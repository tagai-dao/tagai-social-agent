"""Monitor eligible TagAI communities and mirror external callouts."""

from __future__ import annotations

import hashlib
import json
import argparse
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

from tagagent import config
from tagagent.tools.callout_platforms import (
    PLATFORM_READERS,
    browser_batch_for_token,
    fomo_browser_feed,
    fomo_callouts,
    gmgn_browser_feed,
    gmgn_callouts,
    latest_global_feed_status,
    pump_browser_feed,
)
from tagagent.tools.x_read import fetch_tweet, normalize_payload
from tagagent.wiki import append_jsonl, load_json, read_jsonl, save_json
from tagagent.agents.social.feed_collector import collect_all


CHAINS = {"bsc": 56, "robinhood": 4663}
NATIVE_PRICE_IDS = {"bsc": "binancecoin", "robinhood": "ethereum"}
# TagAI is a public HTTPS origin. Sending it through the desktop's optional
# local browsing proxy introduced intermittent TLS EOFs under bursty imports.
# Keep platform browsing on that proxy, but connect to TagAI directly.
TAGAI_DIRECT_PROXIES = {"http": "", "https": ""}


class CalloutMonitorError(RuntimeError):
    pass


class ImportCircuitOpen(CalloutMonitorError):
    """The TagAI import endpoint failed enough records to pause this run."""


_IMPORT_CIRCUIT_LOCK = threading.Lock()
_IMPORT_CIRCUIT_FAILURES = 3
_import_circuit: dict[str, object] = {"failures": 0, "open": False}


def _reset_import_circuit() -> None:
    with _IMPORT_CIRCUIT_LOCK:
        _import_circuit.update({"failures": 0, "open": False})


def _import_circuit_is_open() -> bool:
    with _IMPORT_CIRCUIT_LOCK:
        return bool(_import_circuit["open"])


def _record_import_endpoint_failure() -> None:
    with _IMPORT_CIRCUIT_LOCK:
        failures = int(_import_circuit["failures"]) + 1
        _import_circuit["failures"] = failures
        if failures >= _IMPORT_CIRCUIT_FAILURES:
            _import_circuit["open"] = True


def _dedupe_pending_imports(
    entries: list[tuple[str, dict, dict, str, int]],
) -> list[tuple[str, dict, dict, str, int]]:
    """Submit each idempotency key at most once in one monitor run.

    A post can remain visible in the overlapping Feed while the same key is
    also present in the durable retry ledger. Sending both copies concurrently
    amplifies gateway load and relies unnecessarily on server-side locking.
    Prefer the entry carrying the highest retry count so its history is kept.
    """
    unique: dict[str, tuple[str, dict, dict, str, int]] = {}
    for entry in entries:
        key = entry[3]
        current = unique.get(key)
        if current is None or entry[4] > current[4]:
            unique[key] = entry
    return list(unique.values())


def _effective_lookback(now: int, source: str | None = None) -> int:
    """Return a per-source overlapping window after missed/failed cycles."""
    base = config.SOCIAL_CALLOUT_LOOKBACK_SECONDS
    recovery = config.SOCIAL_CALLOUT_RECOVERY_LOOKBACK_SECONDS
    state = load_json(config.SOCIAL_CALLOUT_WINDOW_STATE, {})
    if not isinstance(state, dict):
        return recovery
    source_success = state.get("sourceSuccessfulAt")
    last_success_value = (
        source_success.get(source)
        if source and isinstance(source_success, dict)
        else state.get("lastSuccessfulAt")
    )
    if not last_success_value:
        return recovery
    try:
        last_success = int(_as_timestamp(last_success_value))
    except (TypeError, ValueError):
        return recovery
    # The normal ten-minute window already overlaps a five-minute cadence by a
    # full cycle. Adding `base` to every elapsed interval made a healthy worker
    # request 15 minutes forever and incorrectly look like recovery mode. Only
    # expand when the successful-cycle gap itself exceeds the normal window.
    return min(recovery, max(base, now - last_success))


def _as_timestamp(value: object) -> float:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()


def _cycle_complete(report: dict) -> bool:
    chains = report.get("chains") if isinstance(report, dict) else None
    if not isinstance(chains, dict) or set(chains) != set(CHAINS):
        return False
    for chain_report in chains.values():
        stats = chain_report.get("sourceStats", {})
        if chain_report.get("eligible", 0) <= 0 or chain_report.get("collectionPending", 0) > 0:
            return False
        if chain_report.get("errors"):
            return False
        for source in PLATFORM_READERS:
            source_stats = stats.get(source, {})
            if source_stats.get("queried", 0) <= 0 or source_stats.get("errors", 0) > 0:
                return False
            if source_stats.get("collectorHealthy") is not True:
                return False
            # A recent complete batch may be inside the asynchronous grace
            # period even when this monitor run did not observe a new scan.
            # It is healthy for alerting/import gating, but it must not move
            # the successful-cycle watermark or recovery cursor.
            if source_stats.get("collectorExecuted") is not True:
                return False
            if source in {"gmgn", "fomo"}:
                feed = source_stats.get("feedBatch", {})
                if feed.get("fresh") is not True or feed.get("coverageComplete") is not True:
                    return False
            if source == "pump":
                covered = int(source_stats.get("browserFeedTargets") or 0) + int(
                    source_stats.get("apiFallbackTargets") or 0
                )
                if covered < int(chain_report.get("eligible") or 0):
                    return False
    return True


def _count_import_result(chain_report: dict, source: str, result: dict) -> None:
    """Only a confirmed create is a newly imported record."""
    counter = {"created": "imported", "duplicate": "duplicate", "dry_run": "dryRun"}.get(
        result.get("status")
    )
    if counter:
        for stats in (chain_report, chain_report["sourceStats"][source]):
            stats[counter] = stats.get(counter, 0) + 1


def _source_cycle_complete(report: dict, source: str) -> bool:
    """Return whether one source safely advanced across both supported chains."""
    chains = report.get("chains") if isinstance(report, dict) else None
    if not isinstance(chains, dict) or set(chains) != set(CHAINS):
        return False
    for chain_report in chains.values():
        if chain_report.get("eligible", 0) <= 0:
            return False
        stats = chain_report.get("sourceStats", {}).get(source, {})
        if (
            stats.get("queried", 0) <= 0
            or stats.get("errors", 0) > 0
            or stats.get("collectionPending", 0) > 0
            or stats.get("collectorHealthy") is not True
            or stats.get("collectorExecuted") is not True
        ):
            return False
        if source in {"gmgn", "fomo"}:
            feed = stats.get("feedBatch", {})
            if feed.get("fresh") is not True or feed.get("coverageComplete") is not True:
                return False
        if source == "pump":
            covered = int(stats.get("browserFeedTargets") or 0) + int(
                stats.get("apiFallbackTargets") or 0
            )
            if covered < int(chain_report.get("eligible") or 0):
                return False
    return True


def _pump_browser_has_token(payload: object, token_ca: str) -> bool:
    """A finite Pump home-feed page only covers CAs actually present in it.

    ``browser_batch_for_token`` synthesizes an empty per-token batch from a
    completed global page.  That is useful for an exhaustive chronological
    feed, but Pump's home-feed is a selected coin snapshot.  Its end-of-page
    marker cannot prove that an absent eligible CA had no callouts.
    """
    if not isinstance(payload, dict):
        return False
    return isinstance(payload.get(token_ca.lower()), dict)


def _headers(chain_id: int, *, idempotency_key: str | None = None) -> dict:
    headers = {
        "Authorization": f"Bearer {config.SOCIAL_CALLOUT_TAGCLAW_KEY}",
        "Content-Type": "application/json",
        "User-Agent": "TagAgentX-Social/1.0",
        "X-Chain-Id": str(chain_id),
    }
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key
    return headers


def _transient_http_error(exc: Exception) -> bool:
    """Return whether a TagAI request is safe to retry idempotently."""
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return status in {408, 425, 429, 500, 502, 503, 504}


def _retry_delay(attempt: int, exc: Exception) -> float:
    response = getattr(exc, "response", None)
    retry_after = getattr(response, "headers", {}).get("Retry-After") if response is not None else None
    try:
        return min(8.0, max(0.0, float(retry_after)))
    except (TypeError, ValueError):
        return min(4.0, 0.5 * (2 ** attempt))


def native_usd_prices() -> dict[str, float]:
    errors: list[str] = []
    try:
        response = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": ",".join(NATIVE_PRICE_IDS.values()), "vs_currencies": "usd"},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        errors.append(f"CoinGecko: {exc}")
    else:
        prices = {
            chain: float(payload.get(price_id, {}).get("usd", 0))
            for chain, price_id in NATIVE_PRICE_IDS.items()
        }
        if all(price > 0 for price in prices.values()):
            return prices
        errors.append("CoinGecko: incomplete price response")

    # CoinGecko's anonymous endpoint is frequently rate-limited. Binance's
    # public ticker is a narrow, independent fallback for the two native assets
    # used only to evaluate TagAI's USD market-cap threshold.
    prices: dict[str, float] = {}
    for chain, symbol in {"bsc": "BNBUSDT", "robinhood": "ETHUSDT"}.items():
        try:
            response = requests.get(
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": symbol},
                timeout=20,
            )
            response.raise_for_status()
            prices[chain] = float(response.json().get("price", 0))
        except (requests.RequestException, ValueError, TypeError) as exc:
            errors.append(f"Binance {symbol}: {exc}")
    if all(prices.get(chain, 0) > 0 for chain in NATIVE_PRICE_IDS):
        return prices
    raise CalloutMonitorError("native USD price request failed: " + "; ".join(errors))


def eligible_communities(chain: str, native_usd: float) -> list[dict]:
    chain_id = CHAINS[chain]
    error: Exception | None = None
    best: list[dict] = []
    previous_tokens: set[str] | None = None
    for attempt in range(5):
        try:
            response = requests.get(
                f"{config.SOCIAL_CALLOUT_API_BASE}/tagclaw/communities/eligible-callouts",
                params={
                    "minMarketCapUsd": config.SOCIAL_CALLOUT_MIN_MARKET_CAP_USD,
                    "nativeUsd": native_usd,
                },
                headers=_headers(chain_id),
                proxies=TAGAI_DIRECT_PROXIES,
                # The API now returns a stale-while-revalidate market-cap
                # snapshot, so a healthy request should finish well inside
                # this deadline. Retry only transient gateway/server failures.
                timeout=(10, 20),
            )
            response.raise_for_status()
            payload = response.json()
            pending = payload.get("marketCapRefreshPending") is True
            pending_communities = payload.get(
                "communities", payload.get("data", {}).get("communities", [])
            )
            if pending and not pending_communities:
                raise requests.ReadTimeout("eligible market-cap snapshot is still warming")
            communities = payload.get("communities", payload.get("data", {}).get("communities", []))
            communities = communities if isinstance(communities, list) else []
            tokens = {
                str(row.get("token") or "").lower()
                for row in communities if isinstance(row, dict) and row.get("token")
            }
            if len(tokens) > len({str(row.get("token") or "").lower() for row in best}):
                best = communities
            # A cold background snapshot sometimes reports pending=false while
            # still exposing only part of the chain. Require the exact token
            # set to stabilize across two successful reads; retain the largest
            # observed response if later probes hit a transient gateway error.
            if tokens and tokens == previous_tokens:
                return best
            previous_tokens = tokens
            if attempt < 4:
                time.sleep(0.25)
                continue
        except (requests.RequestException, ValueError) as exc:
            error = exc
            if best and attempt == 4:
                return best
            if attempt == 4 or not _transient_http_error(exc):
                raise CalloutMonitorError(
                    f"TagAI eligible-community request failed for {chain} after {attempt + 1} attempts: {exc}"
                ) from exc
            time.sleep(_retry_delay(attempt, exc))
    if best:
        return best
    raise CalloutMonitorError(f"TagAI eligible-community request failed for {chain}: {error}")


def _eligible_snapshot_is_implausibly_small(
    current: list[dict], previous: list[dict],
) -> bool:
    """Reject a successful-but-truncated market-cap snapshot.

    The TagAI endpoint can return HTTP 200 while its background snapshot is
    only partly populated.  Persisting that response poisons the Worker's exact
    CA whitelist until the next successful monitor run.  Ordinary market-cap
    movement changes a handful of communities per cycle, not most of a mature
    chain, so retain the last exact whitelist on a large discontinuity.
    """
    def tokens(rows: list[dict]) -> set[str]:
        return {
            str(row.get("token") or row.get("tokenCa") or "").lower()
            for row in rows if isinstance(row, dict)
            and str(row.get("token") or row.get("tokenCa") or "")
        }

    current_tokens = tokens(current)
    previous_tokens = tokens(previous)
    return bool(
        len(previous_tokens) >= 50
        and len(previous_tokens) - len(current_tokens) >= 15
        and len(current_tokens) < len(previous_tokens) * 0.90
    )


def _idempotency_key(chain_id: int, tick: str, callout: dict) -> str:
    raw = f"{chain_id}:{tick}:{callout['source']}:{callout['sourcePostId']}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _semantic_callout_key(chain_id: int, tick: str, callout: dict) -> str:
    """Second-line defense when a platform card lacks a stable native ID."""
    lines: list[str] = []
    skip_value = False
    for raw_line in str(callout.get("content") or "").splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        if line.lower() in {"pnl", "mc"}:
            skip_value = True
            continue
        if skip_value:
            if re.fullmatch(r"[+\-]?\d+(?:\.\d+)?%|\$?[\d,.]+(?:\.\d+)?[KMBT]?", line, re.I):
                skip_value = False
                continue
            skip_value = False
        if re.fullmatch(r"now|\d+\s*(?:s|sec|secs|m|min|mins|h|hr|hrs)|[+\-]?\d+(?:\.\d+)?%", line, re.I):
            continue
        lines.append(line.lower())
    author = callout.get("author") if isinstance(callout.get("author"), dict) else {}
    username = str(author.get("username") or author.get("twitterUsername") or "").lstrip("@").lower()
    raw = "|".join((str(chain_id), tick.lower(), str(callout.get("source") or ""), username, " ".join(lines)))
    return hashlib.sha256(raw.encode()).hexdigest()


def _processed_keys() -> set[str]:
    return {
        str(row.get("key")) for row in read_jsonl(config.SOCIAL_CALLOUT_LEDGER)
        if row.get("status") in {"created", "duplicate"} and row.get("key")
    }


def _retryable_import_failure(exc: Exception) -> bool:
    """Return whether an exhausted import should survive into later cycles."""
    if isinstance(exc, ImportCircuitOpen):
        return True
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, requests.RequestException):
            return _transient_http_error(current)
        # import_callout uses ValueError only for an invalid/truncated 2xx JSON
        # response. The idempotency key makes replaying that uncertain outcome
        # safe and lets the server answer duplicate if it committed already.
        if isinstance(current, ValueError):
            return True
        current = current.__cause__ or current.__context__
    return False


def _invalid_durable_retry_reason(source: str, callout: dict) -> str:
    """Reject legacy browser payloads that can never become valid on retry.

    An older FOMO DOM parser sometimes used the entire card text as the
    username.  Those payloads exceed TagAI's author-field contract and the
    upstream gateway returns 504 while validating them.  Retrying the exact
    same malformed body every cycle delayed all fresh imports by minutes.
    Current collectors emit the real first-line account name; quarantine only
    the unmistakable legacy shape so ordinary transient failures remain
    durable.
    """
    if source != "fomo":
        return ""
    author = callout.get("author") if isinstance(callout.get("author"), dict) else {}
    username = str(author.get("username") or "")
    source_account_id = str(author.get("sourceAccountId") or "")
    content = " ".join(str(callout.get("content") or "").split())
    for field, value in (("username", username), ("sourceAccountId", source_account_id)):
        normalized = " ".join(value.split())
        contains_card = "Thesis" in value or "观点" in value
        contains_content = bool(content and content[:48] in normalized)
        if len(value) > 128 and (contains_card or contains_content):
            return f"legacy FOMO card text leaked into author.{field}"
    return ""


def _pending_retry_imports(
    chain_id: int,
    processed: set[str],
    *,
    now: float | None = None,
) -> list[tuple[str, dict, dict, str, int]]:
    """Load a bounded, due subset of unresolved imports for one chain.

    A TagAI gateway outage can leave dozens of durable retries. Replaying the
    whole backlog before fresh Feed records made one single-channel importer
    run for longer than the collection cadence. Apply an exponential cooldown
    and a per-chain cap; records remain in the ledger until a later cycle.
    Legacy retry rows without a timestamp remain immediately eligible.
    """
    latest: dict[str, dict] = {}
    for row in read_jsonl(config.SOCIAL_CALLOUT_LEDGER):
        key = str(row.get("key") or "")
        if key:
            latest[key] = row
    pending_rows: list[tuple[float, tuple[str, dict, dict, str, int]]] = []
    current_time = time.time() if now is None else now
    for key, row in latest.items():
        if key in processed or row.get("status") != "retry":
            continue
        if int(row.get("chainId") or 0) != chain_id:
            continue
        community = row.get("community")
        callout = row.get("callout")
        source = str(row.get("source") or "")
        if (
            source not in PLATFORM_READERS
            or not isinstance(community, dict)
            or not community.get("tick")
            or not community.get("token")
            or not isinstance(callout, dict)
            or not callout.get("sourcePostId")
        ):
            continue
        invalid_reason = _invalid_durable_retry_reason(source, callout)
        if invalid_reason:
            append_jsonl(config.SOCIAL_CALLOUT_LEDGER, {
                "key": key,
                "status": "discarded_invalid_payload",
                "at": datetime.now(timezone.utc).isoformat(),
                "attempts": int(row.get("attempts") or 1),
                "chainId": chain_id,
                "tick": community.get("tick"),
                "source": source,
                "sourcePostId": callout.get("sourcePostId"),
                "reason": invalid_reason,
            })
            continue
        attempts = int(row.get("attempts") or 1)
        retried_at = row.get("at")
        retry_timestamp = 0.0
        if retried_at:
            try:
                retry_timestamp = _as_timestamp(retried_at)
            except (TypeError, ValueError):
                retry_timestamp = 0.0
        delay = min(
            3600,
            config.SOCIAL_CALLOUT_IMPORT_RETRY_BASE_DELAY_SECONDS
            * (2 ** max(0, attempts - 1)),
        )
        if retry_timestamp and current_time < retry_timestamp + delay:
            continue
        pending_rows.append((
            retry_timestamp,
            (source, community, callout, key, attempts),
        ))
    pending_rows.sort(key=lambda item: item[0])
    limit = config.SOCIAL_CALLOUT_IMPORT_RETRY_LIMIT_PER_CHAIN
    return [item for _, item in pending_rows[:limit]]


def enrich_x_callout(callout: dict) -> dict:
    """Best-effort enrichment for a browser-collected X callout.

    The browser collector already supplies the visible post text and canonical
    status URL.  X API availability is therefore an optional enrichment, not a
    publishing gate.  This keeps GMGN/FOMO X callouts flowing when the X API is
    unavailable while preserving the source URL used by TagAI's X badge.
    """
    if callout.get("contentType") != "x_callout":
        return callout
    if callout.get("content") and callout.get("sourceUrl") and callout.get("originalXTweetId"):
        # Platform-visible text + canonical status URL are already sufficient
        # for TagAI publication and the clickable X source badge. This applies
        # to GMGN, FOMO and Pump and avoids a serial X API round-trip per post.
        return callout
    tweet_id = str(callout.get("originalXTweetId") or "")
    if not tweet_id:
        return callout
    try:
        payload = fetch_tweet(tweet_id)
        rows = normalize_payload(payload)
    except Exception as exc:
        return {
            **callout,
            "raw": {
                "platform": callout.get("raw"),
                "xEnrichment": {"status": "unavailable", "error": str(exc)},
            },
        }
    if not rows or str(rows[0].get("tweet_id") or "") != tweet_id:
        return {
            **callout,
            "raw": {
                "platform": callout.get("raw"),
                "xEnrichment": {"status": "unavailable", "error": "post not returned"},
            },
        }
    row = rows[0]
    x_author = row.get("author") if isinstance(row.get("author"), dict) else {}
    metrics = x_author.get("public_metrics") if isinstance(x_author.get("public_metrics"), dict) else {}
    media = row.get("media") if isinstance(row.get("media"), list) else []
    return {
        **callout,
        "content": str(row.get("expanded_text") or row.get("text") or callout.get("content") or ""),
        "publishedAt": row.get("created_at") or callout.get("publishedAt"),
        "author": {
            **(callout.get("author") or {}),
            "sourceAccountId": str(row.get("author_id") or ""),
            "twitterId": str(row.get("author_id") or ""),
            "twitterUsername": str(row.get("author_handle") or ""),
            "username": str(row.get("author_handle") or ""),
            "displayName": str(x_author.get("name") or row.get("author_handle") or ""),
            "profileImageUrl": str(x_author.get("profile_image_url") or ""),
            "followers": int(metrics.get("followers_count") or 0),
            "followings": int(metrics.get("following_count") or 0),
        },
        "media": [
            {"url": item.get("url") or item.get("preview_image_url"), "type": item.get("type")}
            for item in media if isinstance(item, dict) and (item.get("url") or item.get("preview_image_url"))
        ],
        "raw": {"platform": callout.get("raw"), "x": payload},
    }


def import_callout(chain_id: int, community: dict, callout: dict) -> dict:
    payload = {
        **callout,
        "chainId": chain_id,
        "tick": community["tick"],
        "tokenCa": community["token"],
    }
    if not config.SOCIAL_CALLOUT_EXECUTE:
        return {"status": "dry_run", "payload": payload}
    if _import_circuit_is_open():
        raise ImportCircuitOpen(
            "TagAI import circuit is open after consecutive gateway failures; "
            "deferred to a later cycle"
        )
    error: Exception | None = None
    # The idempotency key makes POST retries safe. TagAI's edge occasionally
    # terminates a TLS stream before headers arrive; treating that as a final
    # post failure left freshly collected cards waiting for the next cycle.
    for attempt in range(4):
        try:
            response = requests.post(
                f"{config.SOCIAL_CALLOUT_API_BASE}/tagclaw/callouts/import",
                headers=_headers(
                    chain_id,
                    idempotency_key=_idempotency_key(chain_id, community["tick"], callout),
                ),
                json=payload,
                proxies=TAGAI_DIRECT_PROXIES,
                timeout=(10, 30),
            )
            response.raise_for_status()
            result = response.json()
            _reset_import_circuit()
            return result
        except requests.RequestException as exc:
            error = exc
            if attempt == 3 or not _transient_http_error(exc):
                break
            time.sleep(_retry_delay(attempt, exc))
        except ValueError as exc:
            # A truncated 2xx JSON body is another transient edge failure.
            error = exc
            if attempt == 3:
                break
            time.sleep(_retry_delay(attempt, exc))
    failure = CalloutMonitorError(
        f"TagAI callout import failed after {attempt + 1} attempts: {error}"
    )
    if isinstance(error, Exception) and _transient_http_error(error):
        _record_import_endpoint_failure()
    raise failure from error


def run_once() -> dict:
    if not config.SOCIAL_CALLOUT_ENABLED:
        return {"status": "disabled"}
    if not config.SOCIAL_CALLOUT_TAGCLAW_KEY:
        return {"status": "skipped", "reason": "TagClaw API key is not configured"}

    _reset_import_circuit()
    now = int(time.time())
    previous_window_state = load_json(config.SOCIAL_CALLOUT_WINDOW_STATE, {})
    if not isinstance(previous_window_state, dict):
        previous_window_state = {}
    previous_source_scans = previous_window_state.get("sourceScannedAt", {})
    if not isinstance(previous_source_scans, dict):
        previous_source_scans = {}
    lookback_seconds = _effective_lookback(now)
    since = now - lookback_seconds
    gmgn_since = now - max(lookback_seconds, config.SOCIAL_GMGN_LOOKBACK_SECONDS)
    # The browser worker must run before feed health is evaluated.  Its errors
    # are retained in the audit report instead of letting stale batches look
    # like a no-callout result.
    collector_run = (
        collect_all(window_seconds=lookback_seconds)
        if config.SOCIAL_FEED_BROWSER_COLLECTOR_COMMAND
        else {"mode": "heartbeat-browser", "results": {}, "errors": {}}
    )
    price_error = ""
    try:
        prices = native_usd_prices()
    except CalloutMonitorError as exc:
        # Eligibility is exact-chain+CA and the previous queue is deliberately
        # retained for transient TagAI outages. Apply the same safe degradation
        # when every public native-price provider is briefly unavailable; the
        # old code raised here before the per-chain whitelist fallback existed.
        prices = {chain: 0.0 for chain in CHAINS}
        price_error = str(exc)
    report = {
        "status": "ok",
        "at": datetime.now(timezone.utc).isoformat(),
        "lookbackSeconds": lookback_seconds,
        "chains": {},
        "feedCollector": collector_run,
    }
    if price_error:
        report["errors"] = [price_error + "; using previous exact whitelist"]
    processed = _processed_keys()
    scheduled_keys = set(processed)
    seen_semantic: set[str] = set()
    previous_queue_payload = load_json(config.SOCIAL_BROWSER_SCAN_QUEUE, {})
    previous_queue_entries = (
        previous_queue_payload.get("entries", [])
        if isinstance(previous_queue_payload, dict) else []
    )
    scan_queue: dict[tuple[str, str], dict] = {}

    def preserve_previous_chain(chain_name: str) -> None:
        """Never erase the worker whitelist during a transient TagAI outage."""
        for entry in previous_queue_entries:
            if not isinstance(entry, dict) or entry.get("chain") != chain_name:
                continue
            token_ca = str(entry.get("tokenCa") or "")
            if token_ca:
                scan_queue[(chain_name, token_ca.lower())] = entry

    def previous_communities(chain_name: str) -> list[dict]:
        """Keep importing against the last exact whitelist during an API outage."""
        rows: list[dict] = []
        seen: set[str] = set()
        for entry in previous_queue_entries:
            if not isinstance(entry, dict) or entry.get("chain") != chain_name:
                continue
            token_ca = str(entry.get("tokenCa") or "")
            if not token_ca or token_ca.lower() in seen:
                continue
            seen.add(token_ca.lower())
            rows.append({
                "tick": entry.get("tick"),
                "token": token_ca,
                "chainId": entry.get("chainId", CHAINS[chain_name]),
                "eligibilitySource": "previous-browser-scan-queue",
            })
        return rows
    for chain, chain_id in CHAINS.items():
        chain_report = {
            "eligible": 0,
            "platformsQueried": 0,
            "collectionPending": 0,
            "found": 0,
            "imported": 0,
            "duplicate": 0,
            "dryRun": 0,
            "sourceStats": {
                source: {
                    "queried": 0, "found": 0, "imported": 0,
                    "duplicate": 0, "dryRun": 0,
                    "errors": 0, "collectionPending": 0,
                }
                for source in PLATFORM_READERS
            },
            "errors": [],
            "warnings": [],
        }
        report["chains"][chain] = chain_report
        pending_imports: list[tuple[str, dict, dict, str, int]] = []
        # Durable retries are appended only after newly discovered Feed
        # records. A stale outage backlog must never delay current callouts.
        retry_imports: list[tuple[str, dict, dict, str, int]] = []
        retry_imports = _pending_retry_imports(chain_id, processed)
        for source, community, callout, key, attempts in retry_imports:
            if key in scheduled_keys:
                continue
            scheduled_keys.add(key)
            chain_report["sourceStats"][source]["retryQueued"] = (
                chain_report["sourceStats"][source].get("retryQueued", 0) + 1
            )
        for source, error in collector_run.get("errors", {}).items():
            chain_report["errors"].append({"source": f"{source}_collector", "error": error})
        communities: list[dict]
        if prices.get(chain, 0) <= 0:
            communities = previous_communities(chain)
            preserve_previous_chain(chain)
            chain_report["eligibleSource"] = "previous-browser-scan-queue"
            message = "native USD price unavailable; using previous exact whitelist"
            (chain_report["warnings"] if communities else chain_report["errors"]).append(message)
        else:
            try:
                communities = eligible_communities(chain, prices[chain])
                previous = previous_communities(chain)
                if _eligible_snapshot_is_implausibly_small(communities, previous):
                    chain_report["warnings"].append(
                        "TagAI eligible-community snapshot shrank implausibly "
                        f"from {len(previous)} to {len(communities)}; using previous exact whitelist"
                    )
                    communities = previous
                    preserve_previous_chain(chain)
                    chain_report["eligibleSource"] = "previous-browser-scan-queue"
                else:
                    chain_report["eligibleSource"] = "tagai-api"
            except CalloutMonitorError as exc:
                communities = previous_communities(chain)
                preserve_previous_chain(chain)
                chain_report["eligibleSource"] = "previous-browser-scan-queue"
                message = f"{exc}; using previous exact whitelist"
                (chain_report["warnings"] if communities else chain_report["errors"]).append(message)
        if not communities:
            continue
        chain_report["eligible"] = len(communities)
        try:
            pump_payload = pump_browser_feed(chain)
        except Exception as exc:
            pump_payload = {}
            chain_report["errors"].append({"source": "pump_feed", "error": str(exc)})
        pump_prefetch: dict[str, tuple[list[dict] | None, Exception | None]] = {}
        pump_reader = PLATFORM_READERS.get("pump")
        if pump_reader and communities:
            with ThreadPoolExecutor(max_workers=config.SOCIAL_PUMP_MAX_WORKERS) as executor:
                futures = {
                    executor.submit(
                        pump_reader,
                        chain,
                        str(community.get("token") or ""),
                        since=since,
                    ): str(community.get("token") or "").lower()
                    for community in communities
                    if str(community.get("token") or "")
                    and not _pump_browser_has_token(
                        pump_payload, str(community.get("token") or "")
                    )
                }
                for future in as_completed(futures):
                    token_key = futures[future]
                    try:
                        pump_prefetch[token_key] = (future.result(), None)
                    except Exception as exc:
                        pump_prefetch[token_key] = (None, exc)
        chain_report["sourceStats"]["pump"]["apiFallbackTargets"] = len(pump_prefetch)
        chain_report["sourceStats"]["pump"]["browserFeedTargets"] = max(
            0, len(communities) - len(pump_prefetch)
        )
        gmgn_payload = None
        fomo_payload = None
        feed_health = {}
        for source in ("gmgn", "fomo", "pump"):
            try:
                health = latest_global_feed_status(source, chain)
            except Exception as exc:
                health = {"present": False, "fresh": False, "error": str(exc)}
            feed_health[source] = health
            chain_report["sourceStats"][source]["feedBatch"] = {
                key: health.get(key)
                for key in (
                    "present", "fresh", "scannedAt", "ageSeconds",
                    "coverageComplete", "recordCount",
                )
            }
            # This is the actual platform-collector signal. `queried` below is
            # intentionally retained as a per-target attempt counter.
            scanned_at = health.get("scannedAt")
            previous_scanned_at = previous_source_scans.get(source)
            try:
                if previous_scanned_at:
                    advanced = bool(scanned_at) and (
                        _as_timestamp(scanned_at) > _as_timestamp(previous_scanned_at)
                    )
                else:
                    # Migrate legacy state by using its cycle watermark. This
                    # still fails closed for an old batch, while allowing the
                    # first genuinely newer browser result to establish the
                    # per-source watermark without a deadlock.
                    legacy_watermark = previous_window_state.get("lastSuccessfulAt")
                    advanced = bool(scanned_at) and (
                        not legacy_watermark
                        or _as_timestamp(scanned_at) > _as_timestamp(legacy_watermark)
                    )
            except (TypeError, ValueError):
                advanced = False
            chain_report["sourceStats"][source]["collectorExecuted"] = bool(
                health.get("fresh") and advanced
            )
            age_seconds = health.get("ageSeconds")
            try:
                within_async_grace = 0 <= float(age_seconds) <= max(
                    config.SOCIAL_CALLOUT_LOOKBACK_SECONDS,
                    config.SOCIAL_CALLOUT_INTERVAL_SECONDS * 2,
                )
            except (TypeError, ValueError):
                within_async_grace = False
            # Browser collection and import are intentionally asynchronous.
            # A heartbeat can fire after it consumed a batch but before the
            # next five-minute browser cycle finishes. Keep collectorExecuted
            # as the strict "advanced since last import" signal, while a fresh
            # complete batch inside the two-cycle grace proves the worker is
            # still healthy and avoids a false failed-run alert.
            chain_report["sourceStats"][source]["collectorHealthy"] = bool(
                health.get("fresh")
                and health.get("coverageComplete") is True
                and (advanced or within_async_grace)
            )
        # Pump's per-token API fallback is an actual collector execution even
        # when its browser Feed batch did not advance in this cycle.
        if chain_report["sourceStats"]["pump"].get("apiFallbackTargets", 0) > 0:
            chain_report["sourceStats"]["pump"]["collectorExecuted"] = True
            chain_report["sourceStats"]["pump"]["collectorHealthy"] = True
        # X checkpoints can be complete while the native GMGN scan failed.
        # Keep importing their valid records, but never advance the whole
        # platform's successful watermark on that partial evidence. Pump's
        # independent exact-CA fallback may still succeed; expose its browser
        # failure separately rather than hiding it behind fallback health.
        worker_health = load_json(
            config.SOCIAL_CALLOUT_WINDOW_STATE.parent / "persistent-worker-health.json", {}
        )
        if isinstance(worker_health, dict):
            for source, error in (worker_health.get("errors") or {}).items():
                if source not in chain_report["sourceStats"]:
                    continue
                stats = chain_report["sourceStats"][source]
                stats["browserCollectionError"] = str(error)
                chain_report["warnings"].append({"source": source, "error": str(error)})
                if source == "gmgn":
                    stats["collectorHealthy"] = False
        try:
            # GMGN's documented Agent API does not contain website GMGN/X
            # callouts. Consume only fresh, complete browser scan batches.
            gmgn_payload = gmgn_browser_feed(chain, since=gmgn_since)
        except Exception as exc:
            chain_report["errors"].append({"source": "gmgn", "error": str(exc)})
        try:
            if not config.SOCIAL_FOMO_CALLOUT_URL_TEMPLATE:
                fomo_payload = fomo_browser_feed(chain)
        except Exception as exc:
            chain_report["errors"].append({"source": "fomo", "error": str(exc)})
        for community in communities:
            token_ca = str(community.get("token") or "")
            if not token_ca:
                continue
            for source, reader in PLATFORM_READERS.items():
                chain_report["platformsQueried"] += 1
                chain_report["sourceStats"][source]["queried"] += 1
                browser_payload = (
                    gmgn_payload if source == "gmgn"
                    else fomo_payload if source == "fomo" and not config.SOCIAL_FOMO_CALLOUT_URL_TEMPLATE
                    else pump_payload if source == "pump"
                    else None
                )
                if source in {"gmgn", "fomo"} and isinstance(browser_payload, dict):
                    batch = browser_batch_for_token(browser_payload, source, chain, token_ca)
                    queued = scan_queue.setdefault((chain, token_ca.lower()), {
                        "chain": chain,
                        "chainId": chain_id,
                        "tick": community.get("tick"),
                        "tokenCa": token_ca,
                        "sources": [],
                        "sourceState": {},
                    })
                    # A degraded run may seed this map with a legacy queue row.
                    # Normalize optional fields before updating it instead of
                    # failing the very fallback intended to survive outages.
                    queued.setdefault("sources", [])
                    queued.setdefault("sourceState", {})
                    queued["sources"].append(source)
                    if source == "gmgn":
                        tabs = batch.get("tabs", {}) if isinstance(batch, dict) else {}
                        record_count = sum(
                            len(tabs.get(name, [])) if isinstance(tabs.get(name), list) else 0
                            for name in ("gmgn", "x")
                        )
                    else:
                        records = batch.get("records", []) if isinstance(batch, dict) else []
                        record_count = len(records) if isinstance(records, list) else 0
                    queued["sourceState"][source] = {
                        "missing": batch is None,
                        "scannedAt": batch.get("scannedAt") if isinstance(batch, dict) else None,
                        "recordCount": record_count,
                        "coverageComplete": (
                            bool(batch.get("coverage", {}).get("complete"))
                            if isinstance(batch, dict) and isinstance(batch.get("coverage"), dict)
                            else batch is not None
                        ),
                        "expectedRecords": (
                            batch.get("coverage", {}).get("expectedRecords")
                            if isinstance(batch, dict) and isinstance(batch.get("coverage"), dict)
                            else None
                        ),
                    }
                    if batch is None:
                        chain_report["collectionPending"] += 1
                        chain_report["sourceStats"][source]["collectionPending"] += 1
                        continue
                    if (
                        isinstance(batch.get("coverage"), dict)
                        and batch["coverage"].get("complete") is not True
                    ):
                        # Partial feeds may contain publishable records, but
                        # they never prove that the cycle covered the entire
                        # requested window. Import what is present while
                        # retaining the recovery cursor for the next cycle.
                        chain_report["collectionPending"] += 1
                        chain_report["sourceStats"][source]["collectionPending"] += 1
                try:
                    if source == "gmgn":
                        callouts = gmgn_callouts(chain, token_ca, since=gmgn_since, payload=gmgn_payload) if gmgn_payload is not None else gmgn_callouts(chain, token_ca, since=gmgn_since)
                    elif source == "fomo" and fomo_payload is not None:
                        callouts = fomo_callouts(chain, token_ca, since=since, payload=fomo_payload)
                    elif source == "pump":
                        if _pump_browser_has_token(pump_payload, token_ca):
                            callouts = reader(chain, token_ca, since=since, payload=pump_payload)
                        else:
                            callouts, pump_error = pump_prefetch.get(token_ca.lower(), ([], None))
                            if pump_error is not None:
                                raise pump_error
                            callouts = callouts or []
                    else:
                        callouts = reader(chain, token_ca, since=since)
                    chain_report["found"] += len(callouts)
                    chain_report["sourceStats"][source]["found"] += len(callouts)
                    for callout in callouts:
                        semantic_key = _semantic_callout_key(chain_id, community["tick"], callout)
                        if semantic_key in seen_semantic:
                            continue
                        seen_semantic.add(semantic_key)
                        key = _idempotency_key(chain_id, community["tick"], callout)
                        if key in scheduled_keys:
                            continue
                        scheduled_keys.add(key)
                        pending_imports.append((source, community, callout, key, 0))
                except Exception as exc:  # isolate one platform/community from the rest of the cycle
                    chain_report["sourceStats"][source]["errors"] += 1
                    chain_report["errors"].append({"tick": community.get("tick"), "source": source, "error": str(exc)})

        pending_imports.extend(retry_imports)
        pending_imports = _dedupe_pending_imports(pending_imports)

        # A single transient TagAI edge timeout previously blocked every later
        # record while it performed four serial idempotent retries. Submit the
        # already-deduplicated records concurrently; ledger writes remain on
        # this thread, and the server/idempotency key preserve exactly-once
        # behavior even when one request's outcome is temporarily uncertain.
        if pending_imports:
            # BSC's larger legacy tables serialize several mirror/feed writes.
            # Even two same-community imports can queue behind the same legacy
            # lookup and expire together at the gateway.  The Robinhood
            # instance showed the same failure mode when an expanded recovery
            # window submitted dozens of records at eight-way concurrency.
            # Keep both chains independently configurable, with conservative
            # single-file defaults; operators can raise either only after the
            # corresponding database path is proven to tolerate it.
            import_workers = (
                config.SOCIAL_CALLOUT_IMPORT_MAX_WORKERS_BSC
                if chain_id == 56
                else config.SOCIAL_CALLOUT_IMPORT_MAX_WORKERS_ROBINHOOD
            )
            with ThreadPoolExecutor(max_workers=import_workers) as executor:
                futures = {
                    executor.submit(
                        import_callout, chain_id, community, enrich_x_callout(callout)
                    ): (source, community, callout, key, durable_attempts)
                    for source, community, callout, key, durable_attempts in pending_imports
                }
                for future in as_completed(futures):
                    source, community, callout, key, durable_attempts = futures[future]
                    try:
                        result = future.result()
                        _count_import_result(chain_report, source, result)
                        status = result.get("status")
                        if config.SOCIAL_CALLOUT_EXECUTE and status in {"created", "duplicate"}:
                            append_jsonl(config.SOCIAL_CALLOUT_LEDGER, {
                                "key": key,
                                "status": status,
                                "at": datetime.now(timezone.utc).isoformat(),
                                "chainId": chain_id,
                                "tick": community["tick"],
                                "source": callout["source"],
                                "sourcePostId": callout["sourcePostId"],
                                "sourceUrl": callout.get("sourceUrl"),
                                "originalXTweetId": callout.get("originalXTweetId"),
                            })
                            processed.add(key)
                    except Exception as exc:
                        if _retryable_import_failure(exc):
                            append_jsonl(config.SOCIAL_CALLOUT_LEDGER, {
                                "key": key,
                                "status": "retry",
                                "at": datetime.now(timezone.utc).isoformat(),
                                "attempts": durable_attempts + 1,
                                "chainId": chain_id,
                                "tick": community.get("tick"),
                                "source": source,
                                "sourcePostId": callout.get("sourcePostId"),
                                "community": {
                                    "tick": community.get("tick"),
                                    "token": community.get("token"),
                                },
                                "callout": callout,
                                "error": str(exc),
                            })
                        chain_report["sourceStats"][source]["errors"] += 1
                        chain_report["errors"].append({
                            "tick": community.get("tick"), "source": source,
                            "sourcePostId": callout.get("sourcePostId"), "error": str(exc),
                        })

        # A stopped browser collector used to look healthy because `queried`
        # counts eligible targets visited by this process, not Feed executions.
        # Report one actionable source/chain failure instead of silently
        # emitting found=0 for every target.
        for source in ("gmgn", "fomo"):
            stats = chain_report["sourceStats"][source]
            health = feed_health[source]
            if stats.get("collectorHealthy") is True:
                continue
            chain_report["collectionPending"] += 1
            stats["collectionPending"] += 1
            age = health.get("ageSeconds")
            if stats.get("browserCollectionError"):
                detail = stats["browserCollectionError"]
            elif health.get("fresh") and health.get("coverageComplete") is not True:
                detail = (
                    "latest batch is fresh but historical coverage continuity is "
                    f"still rebuilding (scannedAt={health.get('scannedAt')})"
                )
            else:
                detail = (
                    f"latest batch is {age}s old (scannedAt={health.get('scannedAt')})"
                    if health.get("present") else "no platform Feed batch exists"
                )
            stats["errors"] += 1
            chain_report["errors"].append({
                "source": source,
                "error": (
                    f"{source.upper()} browser Feed collector has no recent complete coverage; "
                    f"{detail}. queried counts target checks, not collector health"
                ),
            })
    queue_entries = list(scan_queue.values())
    queue_entries.sort(key=lambda entry: (
        # Give every eligible community an initial exact chain+CA scan before
        # spending repeated cycles completing a large partial community.
        -sum(1 for state in entry.get("sourceState", {}).values() if state.get("missing")),
        -sum(
            1 for state in entry.get("sourceState", {}).values()
            if not state.get("missing") and not state.get("coverageComplete", True)
        ),
        min(
            (state.get("scannedAt") or "" for state in entry.get("sourceState", {}).values()),
            default="",
        ),
        entry.get("chain", ""),
        str(entry.get("tokenCa", "")).lower(),
    ))
    save_json(config.SOCIAL_BROWSER_SCAN_QUEUE, {
        "schemaVersion": 1,
        "generatedAt": report["at"],
        "entries": queue_entries,
    })
    previous_state = load_json(config.SOCIAL_CALLOUT_WINDOW_STATE, {})
    previous_state = previous_state if isinstance(previous_state, dict) else {}
    source_success = dict(previous_state.get("sourceSuccessfulAt") or {})
    source_scanned_at = dict(previous_state.get("sourceScannedAt") or {})
    for source in ("gmgn", "fomo", "pump"):
        if not _source_cycle_complete(report, source):
            continue
        source_success[source] = report["at"]
        timestamps = [
            chain_report.get("sourceStats", {}).get(source, {})
            .get("feedBatch", {}).get("scannedAt")
            for chain_report in report["chains"].values()
        ]
        source_scanned_at[source] = next((value for value in timestamps if value), None)
    window_state = {
        **previous_state,
        "schemaVersion": 1,
        "lookbackSeconds": lookback_seconds,
        "sourceSuccessfulAt": source_success,
        "sourceScannedAt": source_scanned_at,
    }
    if _cycle_complete(report):
        window_state["lastSuccessfulAt"] = report["at"]
    if source_success or window_state.get("lastSuccessfulAt"):
        save_json(config.SOCIAL_CALLOUT_WINDOW_STATE, window_state)
    append_jsonl(config.SOCIAL_CALLOUT_AUDIT_LOG, report)
    return report


def run_forever(interval_seconds: int | None = None) -> None:
    """Run on a fixed cadence without overlapping monitor cycles."""
    interval = max(60, int(interval_seconds or config.SOCIAL_CALLOUT_INTERVAL_SECONDS))
    while True:
        started = time.monotonic()
        try:
            print(json.dumps(run_once(), ensure_ascii=False, default=str), flush=True)
        except Exception as exc:  # keep the long-running service alive after one failed cycle
            failure = {
                "status": "error",
                "at": datetime.now(timezone.utc).isoformat(),
                "error": str(exc),
            }
            append_jsonl(config.SOCIAL_CALLOUT_AUDIT_LOG, failure)
            print(json.dumps(failure, ensure_ascii=False), flush=True)
        elapsed = time.monotonic() - started
        time.sleep(max(1.0, interval - elapsed))


def main() -> None:
    parser = argparse.ArgumentParser(description="Mirror GMGN/FOMO/Pump callouts into TagAI")
    parser.add_argument("--loop", action="store_true", help="run continuously on the configured five-minute cadence")
    parser.add_argument("--interval", type=int, default=None, help="loop interval in seconds (minimum 60)")
    parser.add_argument(
        "--require-complete",
        action="store_true",
        help="exit non-zero unless all platform collectors and imports completed",
    )
    args = parser.parse_args()
    if args.loop:
        run_forever(args.interval)
    else:
        report = run_once()
        print(json.dumps(report, ensure_ascii=False, default=str))
        if args.require_complete and not _cycle_complete(report):
            raise SystemExit(2)


if __name__ == "__main__":
    main()
