"""Read-only adapters for GMGN, FOMO and Pump community callouts."""

from __future__ import annotations

import re
import hashlib
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote, urlparse

import requests

from tagagent import config
from tagagent.wiki import load_json, read_jsonl


X_STATUS_RE = re.compile(r"https?://(?:x|twitter)\.com/[^/\s]+/status/(\d{15,22})", re.I)
GMGN_TOKEN_URL_RE = re.compile(
    r"^https://gmgn\.ai/(?P<chain>[a-z0-9_-]+)/token/(?P<ca>0x[a-f0-9]{40})(?:[/?#].*)?$",
    re.I,
)
RELATIVE_TIME_RE = re.compile(r"^(?P<value>\d+)\s*(?P<unit>s|m|h|d)$", re.I)
GLOBAL_FEED_KEY = "__global_feed__"
_GLOBAL_FEED_INDEX_KEY: tuple[str, object] | None = None
_GLOBAL_FEED_INDEX: dict[str, dict[str, tuple[datetime, dict] | None]] = {}
_DEFAULT_READ_JSONL = read_jsonl


class PlatformCalloutError(RuntimeError):
    pass


def _walk(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def _first(row: dict, *names: str):
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return value
    return None


def _timestamp(value: Any) -> str:
    if isinstance(value, (int, float)):
        seconds = value / 1000 if value > 10_000_000_000 else value
        return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat()
    text = str(value or "").strip()
    if text:
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
        except ValueError:
            pass
    return datetime.now(timezone.utc).isoformat()


def _x_id(*values: Any) -> str:
    for value in values:
        match = X_STATUS_RE.search(str(value or ""))
        if match:
            return match.group(1)
    return ""


def _author(row: dict, nested: dict | None = None) -> dict:
    nested = nested or {}
    return {
        "sourceAccountId": str(_first(row, "userId", "user_id", "author_id", "maker") or _first(nested, "id", "userId", "walletAddress") or ""),
        "username": str(_first(row, "userName", "username", "author_name", "maker_name") or _first(nested, "username", "userName") or ""),
        "displayName": str(_first(row, "display_name", "authorName", "userName") or _first(nested, "displayName", "username") or ""),
        "twitterId": str(_first(row, "twitter_user_id", "twitterId") or _first(nested, "twitterId") or ""),
        "twitterUsername": str(_first(row, "xUsername", "twitter_username") or _first(nested, "xUsername", "twitterUsername") or "").replace("@", ""),
        "evmAddress": str(_first(row, "evm_address", "wallet_address") or _first(nested, "evmAddress") or ""),
        "solanaAddress": str(_first(row, "solana_address", "walletAddress") or _first(nested, "solanaAddress") or ""),
        "profileImageUrl": str(_first(row, "profileImage", "avatar_url") or _first(nested, "avatarUrl", "profileImageUrl") or ""),
        "profileUrl": str(_first(row, "profile_url") or ""),
    }


def _normalize_rows(source: str, payload: Any, token_ca: str) -> list[dict]:
    target = token_ca.lower()
    output: list[dict] = []
    seen: set[str] = set()
    for row in _walk(payload):
        row_ca = str(_first(row, "token_address", "tokenAddress", "coinMint", "mint", "contract_address", "address") or "")
        data = row.get("data") if isinstance(row.get("data"), dict) else {}
        row_ca = row_ca or str(_first(data, "token_address", "coinMint", "mint", "address") or "")
        if row_ca.lower() != target:
            continue
        post_id = str(_first(row, "calloutId", "callout_id", "post_id", "id", "signal_id") or "")
        content = str(_first(row, "thesis", "note", "content", "text", "message", "description") or _first(data, "thesis", "note", "content", "text", "message") or "").strip()
        if not post_id or not content or post_id in seen:
            continue
        seen.add(post_id)
        source_url = str(_first(row, "url", "source_url", "permalink") or "")
        original_x_id = _x_id(source_url, content, _first(row, "x_url", "tweet_url"), _first(data, "twitter"))
        caller = row.get("caller") if isinstance(row.get("caller"), dict) else data.get("caller") if isinstance(data.get("caller"), dict) else {}
        output.append({
            "source": source,
            "sourcePostId": post_id,
            "sourceCommunityId": row_ca,
            "sourceUrl": source_url,
            "contentType": "x_callout" if original_x_id else "native_callout",
            "originalXTweetId": original_x_id,
            "content": content,
            "publishedAt": _timestamp(_first(row, "createdAt", "created_at", "published_at", "trigger_at", "timestamp")),
            "author": _author(row, caller),
            "media": row.get("media") if isinstance(row.get("media"), list) else [],
            "raw": row,
        })
    return output


def _as_utc(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _global_feed_index() -> dict[str, dict[str, tuple[datetime, dict] | None]]:
    """Parse the append-only Feed log once per monitor process.

    The log can exceed hundreds of megabytes. A monitor run asks for status and
    payloads repeatedly for three sources and two chains; reparsing the whole
    file for each lookup turned a normal import into a 10+ minute CPU loop.
    Keep only the newest observed and newest complete batch per source.
    """
    global _GLOBAL_FEED_INDEX_KEY, _GLOBAL_FEED_INDEX
    path = config.SOCIAL_CALLOUT_BROWSER_FEED_LOG
    # Keep the reader object itself in the key. Tests and embedders may replace
    # it; using id() allowed a reclaimed lambda's integer id to be reused and
    # returned a stale index.
    key = (str(path), read_jsonl)
    if _GLOBAL_FEED_INDEX_KEY == key:
        return _GLOBAL_FEED_INDEX
    index: dict[str, dict[str, tuple[datetime, dict] | None]] = {
        source: {"latest": None, "complete": None}
        for source in ("gmgn", "fomo", "pump")
    }
    latest_dir = path.parent / "browser-feed-latest"
    # Tests and embedders intentionally replace the audit reader. Respect that
    # source instead of leaking production sidecars into the injected view.
    indexed = read_jsonl is _DEFAULT_READ_JSONL
    for source in index:
        if not indexed:
            break
        for key_name, suffix in (("latest", ""), ("complete", "-complete")):
            batch = load_json(latest_dir / f"{source}{suffix}.json", None)
            if not isinstance(batch, dict) or not isinstance(batch.get("records"), list):
                indexed = False
                break
            try:
                scanned_at = _as_utc(batch.get("scannedAt"))
            except (TypeError, ValueError):
                indexed = False
                break
            index[source][key_name] = (scanned_at, batch)
        if not indexed:
            break
    if indexed:
        _GLOBAL_FEED_INDEX_KEY = key
        _GLOBAL_FEED_INDEX = index
        return index

    # Compatibility bootstrap for installations that predate the bounded
    # latest-checkpoint index. New writes create the sidecars, so this costly
    # full audit-log scan is only a one-time fallback.
    index = {
        source: {"latest": None, "complete": None}
        for source in ("gmgn", "fomo", "pump")
    }
    for batch in read_jsonl(path):
        if not isinstance(batch, dict) or batch.get("schemaVersion") != 1:
            continue
        if batch.get("complete") is not True:
            continue
        source = str(batch.get("source") or "").lower()
        if source not in index:
            continue
        coverage = batch.get("coverage")
        records = batch.get("records")
        if not isinstance(coverage, dict) or not isinstance(records, list):
            continue
        window_seconds = int(coverage.get("windowSeconds") or 0)
        if not 300 <= window_seconds <= config.SOCIAL_CALLOUT_RECOVERY_LOOKBACK_SECONDS:
            continue
        try:
            scanned_at = _as_utc(batch.get("scannedAt"))
        except (TypeError, ValueError):
            continue
        current = index[source]["latest"]
        if current is None or scanned_at > current[0]:
            index[source]["latest"] = (scanned_at, batch)
        complete = index[source]["complete"]
        if coverage.get("complete") is True and (
            complete is None or scanned_at > complete[0]
        ):
            index[source]["complete"] = (scanned_at, batch)
    _GLOBAL_FEED_INDEX_KEY = key
    _GLOBAL_FEED_INDEX = index
    return index


def _relative_published_at(relative_time: str, scanned_at: datetime) -> tuple[str, bool]:
    """Resolve GMGN's visible relative time and identify the ambiguous 3d edge."""
    text = str(relative_time or "").strip().lower()
    if text == "now":
        return scanned_at.isoformat(), False
    match = RELATIVE_TIME_RE.fullmatch(text)
    if not match:
        raise PlatformCalloutError(f"unsupported GMGN relative time: {relative_time!r}")
    value = int(match.group("value"))
    unit = match.group("unit").lower()
    delta = {
        "s": timedelta(seconds=value),
        "m": timedelta(minutes=value),
        "h": timedelta(hours=value),
        "d": timedelta(days=value),
    }[unit]
    return (scanned_at - delta).isoformat(), unit == "d" and value == 3


def latest_global_feed_status(source: str, chain: str) -> dict:
    """Describe the newest valid platform-wide batch, including stale batches.

    The monitor previously discarded a batch as soon as it became stale.  That
    made a stopped browser collector indistinguishable from a healthy feed with
    no matching posts.  Keep the last observation for health reporting while
    still refusing to import stale records.
    """
    now = datetime.now(timezone.utc)
    indexed = _global_feed_index().get(source, {})

    def chain_candidate(entry: tuple[datetime, dict] | None):
        if entry is None:
            return None
        scanned_at, batch = entry
        age = (now - scanned_at).total_seconds()
        if age < 0:
            return None
        records = batch.get("records", [])
        candidate = {
            **batch,
            "records": [
                row for row in records
                if isinstance(row, dict)
                and str(row.get("chain") or "").lower() == chain.lower()
                and re.fullmatch(r"0x[a-f0-9]{40}", str(row.get("tokenCa") or ""), re.I)
            ],
            "ageSeconds": int(age),
            "fresh": age <= config.SOCIAL_CALLOUT_BROWSER_FEED_MAX_AGE_SECONDS,
        }
        return scanned_at, candidate

    latest = chain_candidate(indexed.get("latest"))
    latest_complete = chain_candidate(indexed.get("complete"))
    if latest is None:
        return {
            "present": False,
            "fresh": False,
            "scannedAt": None,
            "ageSeconds": None,
            "coverageComplete": False,
            "recordCount": 0,
        }
    batch = latest[1]
    # GMGN's minute-level X snapshots are deliberately partial: they preserve
    # new posts until the next full browser pass proves the whole lookback
    # boundary.  A fresh partial snapshot must remain available for importing,
    # but it must not hide the most recent full-cycle proof and make every
    # intervening heartbeat report the collector as unhealthy.
    # Liveness comes from the newest observation, even while it is rebuilding
    # historical coverage after an outage. Reporting the old complete sidecar
    # as the newest scan made an actively polling collector look stopped for
    # up to an hour. Completeness is tracked separately below and remains
    # false until continuity is actually proven.
    health_batch = batch
    continuous_overlap = False
    if latest_complete is not None and latest[0] > latest_complete[0]:
        complete_window = int(
            latest_complete[1].get("coverage", {}).get("windowSeconds") or 0
        )
        continuous_overlap = bool(
            batch.get("fresh")
            and complete_window > 0
            and (latest[0] - latest_complete[0]).total_seconds() <= complete_window
        )
    return {
        "present": True,
        "fresh": bool(health_batch["fresh"]),
        "scannedAt": health_batch.get("scannedAt"),
        "ageSeconds": health_batch["ageSeconds"],
        "coverageComplete": bool(
            batch.get("coverage", {}).get("complete") or continuous_overlap
        ),
        "recordCount": len(health_batch["records"]),
        "batch": batch,
        "completeBatch": latest_complete[1] if latest_complete is not None else None,
        "latestObservedScannedAt": batch.get("scannedAt"),
        "latestObservedCoverageComplete": bool(
            batch.get("coverage", {}).get("complete")
        ),
        "continuousOverlap": continuous_overlap,
    }


def _latest_global_feed(source: str, chain: str) -> dict | None:
    """Read the newest fresh platform-wide browser feed for one chain."""
    status = latest_global_feed_status(source, chain)
    batch = status.get("batch")
    # Collector health deliberately remains incomplete while a historical
    # continuity gap is being rebuilt. Do not let that suppress the fresh
    # records actually present in the newest partial snapshot: the monitor
    # imports them while retaining its recovery cursor and pending status.
    if not isinstance(batch, dict) or batch.get("fresh") is not True:
        return None
    complete = status.get("completeBatch")
    if not isinstance(complete, dict):
        return batch
    if batch.get("coverage", {}).get("complete") is True:
        return batch

    # A partial minute snapshot plus an overlapping recent full cycle is a
    # complete continuous view: the full cycle proves the older boundary and
    # the snapshot supplies the new head.  Merge both sets so the importer can
    # consume new X posts without marking every exact-CA target pending until
    # the next ten-minute full browser pass.
    latest_at = _as_utc(batch.get("scannedAt"))
    complete_at = _as_utc(complete.get("scannedAt"))
    complete_window = int(complete.get("coverage", {}).get("windowSeconds") or 0)
    if complete_window <= 0 or (latest_at - complete_at).total_seconds() > complete_window:
        return batch

    rows: dict[str, dict] = {}
    for row in [*complete.get("records", []), *batch.get("records", [])]:
        if not isinstance(row, dict):
            continue
        key = str(row.get("sourcePostId") or row.get("originalXTweetId") or "")
        if not key:
            key = hashlib.sha256(repr(sorted(row.items())).encode()).hexdigest()
        rows[key] = row
    return {
        **complete,
        **batch,
        "records": list(rows.values()),
        "coverage": {
            **complete.get("coverage", {}),
            **batch.get("coverage", {}),
            "complete": True,
            "continuousOverlap": True,
            "completeBaselineScannedAt": complete.get("scannedAt"),
        },
        "verification": {
            **complete.get("verification", {}),
            **batch.get("verification", {}),
            "continuousOverlap": True,
        },
    }


def _global_token_batch(source: str, chain: str, token_ca: str, feed: dict) -> dict:
    records = [
        row for row in feed.get("records", [])
        if str(row.get("chain") or "").lower() == chain.lower()
        and str(row.get("tokenCa") or "").lower() == token_ca.lower()
    ]
    base = {
        "schemaVersion": 1,
        "complete": True,
        "scannedAt": feed.get("scannedAt"),
        "chain": chain,
        "tokenCa": token_ca,
        "pageUrl": feed.get("pageUrl"),
        "coverage": feed.get("coverage", {}),
        "verification": feed.get("verification", {}),
        "feedSource": True,
    }
    if source == "gmgn":
        return {
            **base,
            "tabs": {
                "gmgn": [row for row in records if str(row.get("kind") or "") == "native"],
                "x": [row for row in records if str(row.get("kind") or "") == "x"],
            },
        }
    return {**base, "pageChain": chain, "pageTokenCa": token_ca, "records": records}


def _merge_global_feed(source: str, chain: str, legacy: dict[str, dict]) -> dict[str, dict]:
    feed = _latest_global_feed(source, chain)
    if not feed:
        return legacy
    merged = dict(legacy)
    token_cas = {
        str(row.get("tokenCa") or "").lower()
        for row in feed.get("records", []) if isinstance(row, dict)
    }
    feed_time = _as_utc(feed.get("scannedAt"))
    for token_ca in token_cas:
        candidate = _global_token_batch(source, chain, token_ca, feed)
        current = merged.get(token_ca)
        if current is None or feed_time >= _as_utc(current.get("scannedAt")):
            merged[token_ca] = candidate
    merged[GLOBAL_FEED_KEY] = feed
    return merged


def browser_batch_for_token(payload: object, source: str, chain: str, token_ca: str) -> dict | None:
    if not isinstance(payload, dict):
        return None
    batch = payload.get(token_ca.lower())
    if isinstance(batch, dict):
        return batch
    feed = payload.get(GLOBAL_FEED_KEY)
    if (
        isinstance(feed, dict)
        and isinstance(feed.get("coverage"), dict)
        and feed["coverage"].get("complete") is True
    ):
        return _global_token_batch(source, chain, token_ca, feed)
    return None


def gmgn_browser_feed(chain: str, *, since: int) -> dict[str, dict]:
    """Return the latest valid browser scan per exact CA for one chain.

    An absent scan is not an empty GMGN result. Callers must surface it as a
    collection error so API emptiness can never again be reported as zero
    website callouts.
    """
    now = datetime.now(timezone.utc)
    latest: dict[str, tuple[datetime, dict]] = {}
    for batch in read_jsonl(config.SOCIAL_GMGN_BROWSER_BATCH_LOG):
        if not isinstance(batch, dict) or batch.get("schemaVersion") != 1:
            continue
        if str(batch.get("chain") or "").lower() != chain.lower():
            continue
        ca = str(batch.get("tokenCa") or "")
        page_url = str(batch.get("pageUrl") or "")
        match = GMGN_TOKEN_URL_RE.match(page_url)
        if not match or match.group("chain").lower() != chain.lower() or match.group("ca").lower() != ca.lower():
            continue
        if batch.get("complete") is not True:
            continue
        try:
            scanned_at = _as_utc(batch.get("scannedAt"))
        except (TypeError, ValueError):
            continue
        tabs = batch.get("tabs")
        if not isinstance(tabs, dict) or not all(
            isinstance(tabs.get(name), list) for name in ("gmgn", "x")
        ):
            continue
        if any(
            not isinstance(row, dict)
            or not X_STATUS_RE.search(str(_first(row, "sourceUrl", "url") or ""))
            for row in tabs["x"]
        ):
            # Older browser collectors sometimes captured the author's X
            # profile URL, or a nested quoted post, instead of the top-level
            # card's canonical status URL. Never consume those batches.
            continue
        age = (now - scanned_at).total_seconds()
        if age < 0 or age > config.SOCIAL_GMGN_BROWSER_BATCH_MAX_AGE_SECONDS:
            continue
        key = ca.lower()
        if key not in latest or scanned_at > latest[key][0]:
            latest[key] = (scanned_at, batch)
    return _merge_global_feed(
        "gmgn", chain, {ca: batch for ca, (_, batch) in latest.items()}
    )


def _browser_post_id(chain: str, token_ca: str, kind: str, row: dict, content: str) -> str:
    explicit = _first(row, "sourcePostId", "postId", "id")
    if explicit:
        return str(explicit)
    x_id = _x_id(_first(row, "sourceUrl", "url"), content)
    if x_id:
        return f"x-{x_id}"
    author = str(_first(row, "username", "handle", "authorUsername") or "").lower()
    published = str(_first(row, "publishedAt", "createdAt", "timestamp", "relativeTime") or "")
    raw = (
        f"{chain}:{token_ca.lower()}:{kind}:{author}:{published}:"
        f"{' '.join(content.split()).lower()}"
    )
    return "web-" + hashlib.sha256(raw.encode()).hexdigest()


def _browser_batch_since(batch: dict, since: int) -> int:
    """Keep a completed browser scan's observed window stable while it is consumed.

    A Feed scan can take several minutes.  Filtering its records against the
    later monitor start time drops posts near the original window boundary.
    Completed batches already prove coverage from ``scannedAt-windowSeconds``;
    use that earlier boundary while the batch remains fresh.
    """
    coverage = batch.get("coverage")
    if not isinstance(coverage, dict) or coverage.get("complete") is not True:
        return int(since)
    try:
        window_seconds = int(coverage.get("windowSeconds") or 0)
        scanned_at = int(_as_utc(batch.get("scannedAt")).timestamp())
    except (TypeError, ValueError):
        return int(since)
    if not 300 <= window_seconds <= config.SOCIAL_CALLOUT_RECOVERY_LOOKBACK_SECONDS:
        return int(since)
    return min(int(since), scanned_at - window_seconds)


def _normalize_gmgn_browser_batch(chain: str, token_ca: str, batch: dict, *, since: int) -> list[dict]:
    scanned_at = _as_utc(batch.get("scannedAt"))
    since = _browser_batch_since(batch, since)
    tabs = batch.get("tabs") if isinstance(batch.get("tabs"), dict) else {}
    output: list[dict] = []
    seen: set[str] = set()
    for kind in ("gmgn", "x"):
        rows = tabs.get(kind, [])
        if not isinstance(rows, list):
            raise PlatformCalloutError(f"GMGN browser batch tab {kind!r} is not a list")
        for row in rows:
            if not isinstance(row, dict):
                continue
            content = str(_first(row, "content", "text") or "").strip()
            if not content:
                continue
            absolute_time = _first(row, "publishedAt", "createdAt", "timestamp")
            if absolute_time not in (None, ""):
                published_at = _timestamp(absolute_time)
                boundary_3d = False
            else:
                published_at, boundary_3d = _relative_published_at(
                    str(row.get("relativeTime") or ""), scanned_at
                )
            if boundary_3d and not config.SOCIAL_GMGN_INCLUDE_3D_BOUNDARY:
                continue
            if _as_utc(published_at).timestamp() < int(since):
                continue
            source_url = str(_first(row, "sourceUrl", "url") or batch.get("pageUrl") or "")
            original_x_id = _x_id(source_url, content) if kind == "x" else ""
            if kind == "x" and not original_x_id:
                raise PlatformCalloutError(
                    "GMGN X callout requires an exact X status URL with tweet ID"
                )
            post_id = _browser_post_id(chain, token_ca, kind, row, content)
            dedupe_key = f"{kind}:{post_id}"
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            username = str(_first(row, "username", "handle", "authorUsername") or "").lstrip("@")
            output.append({
                "source": "gmgn",
                "sourcePostId": post_id,
                "sourceCommunityId": token_ca,
                "sourceUrl": source_url,
                "contentType": "x_callout" if kind == "x" else "native_callout",
                "originalXTweetId": original_x_id,
                "content": content,
                "publishedAt": published_at,
                "author": {
                    "sourceAccountId": str(_first(row, "authorId", "accountId") or username),
                    "username": username,
                    "displayName": str(_first(row, "displayName", "authorName") or username),
                    "twitterUsername": username if kind == "x" else "",
                    "profileImageUrl": str(_first(row, "profileImageUrl", "avatarUrl") or ""),
                    "profileUrl": str(_first(row, "profileUrl") or ""),
                },
                "media": row.get("media") if isinstance(row.get("media"), list) else [],
                "raw": {"collector": "gmgn_web", "tab": kind, "boundary3d": boundary_3d, "record": row},
            })
    return output


def gmgn_callouts(chain: str, token_ca: str, *, since: int, payload: object | None = None) -> list[dict]:
    if payload is None:
        payload = gmgn_browser_feed(chain, since=since)
    if not isinstance(payload, dict):
        raise PlatformCalloutError("GMGN browser feed is invalid")
    batch = browser_batch_for_token(payload, "gmgn", chain, token_ca)
    if batch is None:
        raise PlatformCalloutError(
            f"GMGN website scan missing or stale for exact {chain}+{token_ca}"
        )
    return _normalize_gmgn_browser_batch(chain, token_ca, batch, since=since)


def _valid_fomo_page_url(value: Any) -> bool:
    parsed = urlparse(str(value or ""))
    return parsed.scheme == "https" and parsed.hostname in {"fomo.family", "www.fomo.family"}


def fomo_browser_feed(chain: str) -> dict[str, dict]:
    """Return fresh, complete FOMO browser batches keyed by exact CA."""
    now = datetime.now(timezone.utc)
    latest: dict[str, tuple[datetime, dict]] = {}
    for batch in read_jsonl(config.SOCIAL_FOMO_BROWSER_BATCH_LOG):
        if not isinstance(batch, dict) or batch.get("schemaVersion") != 1:
            continue
        if str(batch.get("chain") or "").lower() != chain.lower():
            continue
        ca = str(batch.get("tokenCa") or "")
        if not re.fullmatch(r"0x[a-f0-9]{40}", ca, re.I):
            continue
        if str(batch.get("pageChain") or "").lower() != chain.lower():
            continue
        if str(batch.get("pageTokenCa") or "").lower() != ca.lower():
            continue
        if batch.get("complete") is not True or not _valid_fomo_page_url(batch.get("pageUrl")):
            continue
        records = batch.get("records")
        if not isinstance(records, list):
            continue
        if any(
            not isinstance(row, dict)
            or str(row.get("kind") or "") not in {"native", "x"}
            or (
                str(row.get("kind") or "") == "x"
                and not X_STATUS_RE.search(str(_first(row, "sourceUrl", "url") or ""))
            )
            for row in records
        ):
            continue
        try:
            scanned_at = _as_utc(batch.get("scannedAt"))
        except (TypeError, ValueError):
            continue
        age = (now - scanned_at).total_seconds()
        if age < 0 or age > config.SOCIAL_FOMO_BROWSER_BATCH_MAX_AGE_SECONDS:
            continue
        key = ca.lower()
        if key not in latest or scanned_at > latest[key][0]:
            latest[key] = (scanned_at, batch)
    return _merge_global_feed(
        "fomo", chain, {ca: batch for ca, (_, batch) in latest.items()}
    )


def _fomo_published_at(row: dict, scanned_at: datetime) -> str:
    published = _first(row, "publishedAt", "createdAt", "timestamp")
    if published not in (None, ""):
        return _timestamp(published)
    relative = str(row.get("relativeTime") or "")
    value, _ = _relative_published_at(relative, scanned_at)
    return value


def _normalize_fomo_browser_batch(token_ca: str, batch: dict, *, since: int) -> list[dict]:
    scanned_at = _as_utc(batch.get("scannedAt"))
    since = _browser_batch_since(batch, since)
    output: list[dict] = []
    seen: set[str] = set()
    for row in batch.get("records", []):
        if not isinstance(row, dict):
            continue
        kind = str(row.get("kind") or "")
        content = str(_first(row, "content", "text", "thesis") or "").strip()
        if kind not in {"native", "x"} or not content:
            continue
        published_at = _fomo_published_at(row, scanned_at)
        if _as_utc(published_at).timestamp() < int(since):
            continue
        source_url = str(_first(row, "sourceUrl", "url") or batch.get("pageUrl") or "")
        x_id = _x_id(source_url, content) if kind == "x" else ""
        if kind == "x" and not x_id:
            raise PlatformCalloutError("FOMO X callout requires an exact X status URL with tweet ID")
        username = str(_first(row, "username", "handle", "authorUsername") or "").lstrip("@")
        post_id = str(_first(row, "sourcePostId", "postId", "id") or "")
        if not post_id:
            if x_id:
                post_id = f"x-{x_id}"
            else:
                raw = (
                    f"fomo:{token_ca.lower()}:{username.lower()}:{published_at}:"
                    f"{' '.join(content.split()).lower()}"
                )
                post_id = "web-" + hashlib.sha256(raw.encode()).hexdigest()
        dedupe_key = x_id or post_id
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        output.append({
            "source": "fomo",
            "sourcePostId": post_id,
            "sourceCommunityId": token_ca,
            "sourceUrl": source_url,
            "contentType": "x_callout" if kind == "x" else "native_callout",
            "originalXTweetId": x_id,
            "content": content,
            "publishedAt": published_at,
            "author": {
                "sourceAccountId": str(_first(row, "authorId", "accountId") or username),
                "username": username,
                "displayName": str(_first(row, "displayName", "authorName") or username),
                "twitterUsername": username if kind == "x" else "",
                "profileImageUrl": str(_first(row, "profileImageUrl", "avatarUrl") or ""),
                "profileUrl": str(_first(row, "profileUrl") or ""),
                "evmAddress": str(_first(row, "evmAddress", "walletAddress", "address") or ""),
            },
            "media": row.get("media") if isinstance(row.get("media"), list) else [],
            "raw": {"collector": "fomo_web", "record": row},
        })
    return output


def fomo_callouts(
    chain: str,
    token_ca: str,
    *,
    since: int,
    payload: object | None = None,
) -> list[dict]:
    template = config.SOCIAL_FOMO_CALLOUT_URL_TEMPLATE
    if not template:
        if payload is None:
            payload = fomo_browser_feed(chain)
        if not isinstance(payload, dict):
            raise PlatformCalloutError("FOMO browser feed is invalid")
        batch = browser_batch_for_token(payload, "fomo", chain, token_ca)
        if batch is None:
            raise PlatformCalloutError(
                f"FOMO website scan missing or stale for exact {chain}+{token_ca}"
            )
        return _normalize_fomo_browser_batch(token_ca, batch, since=since)
    url = template.format(chain=quote(chain), token_ca=quote(token_ca), since=int(since))
    headers = {"User-Agent": "TagAgentX/1.0", "Accept": "application/json"}
    if config.SOCIAL_FOMO_ACCESS_TOKEN:
        headers["Authorization"] = f"Bearer {config.SOCIAL_FOMO_ACCESS_TOKEN}"
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise PlatformCalloutError(f"FOMO callout request failed: {exc}") from exc
    rows = _normalize_rows("fomo", payload, token_ca)
    for row in rows:
        if not row["sourceUrl"]:
            row["sourceUrl"] = f"https://fomo.family/coin?address={token_ca}"
    return rows


def pump_browser_feed(chain: str) -> dict[str, dict]:
    """Return a fresh Pump Callouts feed grouped by exact contract."""
    return _merge_global_feed("pump", chain, {})


def pump_callouts(
    chain: str,
    token_ca: str,
    *,
    since: int,
    payload: object | None = None,
) -> list[dict]:
    if payload is None:
        payload = pump_browser_feed(chain)
    batch = browser_batch_for_token(payload, "pump", chain, token_ca)
    if batch is not None:
        rows = _normalize_fomo_browser_batch(token_ca, batch, since=since)
        for row in rows:
            row["source"] = "pump"
            row["raw"] = {"collector": "pump_feed_web", "record": row["raw"].get("record")}
        return rows
    params = {"limit": 100, "sortBy": "TIMESTAMP", "sortOrder": "DESC"}
    url = f"{config.SOCIAL_PUMP_API_BASE}/callout/top/{quote(token_ca, safe='')}"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; TagAgentX/1.0; +https://tagai.fun)",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://pump.fun",
        "Referer": "https://pump.fun/",
    }
    last_error: Exception | None = None
    for attempt in range(config.SOCIAL_PUMP_MAX_ATTEMPTS):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=(10, 30))
            if response.status_code == 404:
                return []
            response.raise_for_status()
            payload = response.json()
            break
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            retryable_status = getattr(getattr(exc, "response", None), "status_code", None)
            retryable = isinstance(exc, (requests.ConnectionError, requests.Timeout)) or retryable_status in {
                408, 425, 429, 500, 502, 503, 504,
            }
            if attempt + 1 >= config.SOCIAL_PUMP_MAX_ATTEMPTS or not retryable:
                raise PlatformCalloutError(
                    f"Pump callout request failed after {attempt + 1} attempt(s): {exc}"
                ) from exc
            delay = config.SOCIAL_PUMP_RETRY_BACKOFF_SECONDS * (2 ** attempt)
            retry_after = getattr(getattr(exc, "response", None), "headers", {}).get("Retry-After")
            if retry_after:
                try:
                    delay = max(delay, float(retry_after))
                except ValueError:
                    pass
            time.sleep(delay)
    else:  # defensive; the loop either breaks or raises
        raise PlatformCalloutError(f"Pump callout request failed: {last_error}")
    rows = _normalize_rows("pump", payload, token_ca)
    for row in rows:
        if not row["sourceUrl"]:
            row["sourceUrl"] = f"https://pump.fun/coin/{token_ca}"
    cutoff = int(since)
    return [row for row in rows if datetime.fromisoformat(row["publishedAt"]).timestamp() >= cutoff]


PLATFORM_READERS = {
    "gmgn": gmgn_callouts,
    "fomo": fomo_callouts,
    "pump": pump_callouts,
}
