"""Validate and persist feed-first browser callout batches.

The browser collector reads the platform-wide GMGN, FOMO or Pump feed, then
resolves every candidate to an exact supported chain and EVM contract before
handing it to Social Agent. Tickers are intentionally not accepted as routing
keys.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from tagagent import config
from tagagent.tools.callout_platforms import X_STATUS_RE, _relative_published_at
from tagagent.wiki import append_jsonl, load_json, read_jsonl, save_json


CHAINS = {"bsc", "robinhood"}
SOURCES = {"gmgn", "fomo", "pump"}
SOURCE_HOSTS = {
    "gmgn": {"gmgn.ai", "www.gmgn.ai"},
    "fomo": {"fomo.family", "www.fomo.family"},
    "pump": {"pump.fun", "www.pump.fun"},
}
WORKER_RELATIVE_RE = re.compile(r"^(?:now|\d+\s*(?:s|sec|secs|m|min|mins|h|hr|hrs))$", re.I)
WORKER_PERCENT_RE = re.compile(r"^[+\-]?\d+(?:\.\d+)?%$")
WORKER_VALUE_RE = re.compile(r"^\$?[\d,.]+(?:\.\d+)?[KMBT]?$", re.I)


def latest_checkpoint_path(source: str, *, complete: bool = False) -> Path:
    """Return the bounded, atomic index used by hot-path collectors/importers."""
    suffix = "-complete" if complete else ""
    return config.SOCIAL_CALLOUT_BROWSER_FEED_LOG.parent / "browser-feed-latest" / f"{source}{suffix}.json"


def _reverse_jsonl(path: Path, *, chunk_size: int = 1024 * 1024):
    """Yield JSONL objects newest-first without loading a multi-GB audit log."""
    if not path.exists():
        return
    with path.open("rb") as handle:
        position = handle.seek(0, 2)
        remainder = b""
        while position:
            size = min(chunk_size, position)
            position -= size
            handle.seek(position)
            block = handle.read(size) + remainder
            lines = block.split(b"\n")
            remainder = lines.pop(0)
            for raw in reversed(lines):
                if not raw.strip():
                    continue
                try:
                    yield json.loads(raw)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue
        if remainder.strip():
            try:
                yield json.loads(remainder)
            except (UnicodeDecodeError, json.JSONDecodeError):
                pass


def _latest_same_source(path: Path, source: str, current_time: datetime) -> tuple[dict | None, datetime | None]:
    if path == config.SOCIAL_CALLOUT_BROWSER_FEED_LOG:
        indexed = load_json(latest_checkpoint_path(source), None)
        if isinstance(indexed, dict) and isinstance(indexed.get("records"), list):
            try:
                indexed_time = _as_utc(indexed.get("scannedAt"))
            except (TypeError, ValueError):
                indexed_time = None
            if indexed_time is not None and indexed_time <= current_time:
                return indexed, indexed_time
    for candidate in _reverse_jsonl(path):
        if not isinstance(candidate, dict) or str(candidate.get("source") or "").lower() != source:
            continue
        if not isinstance(candidate.get("records"), list):
            continue
        try:
            candidate_time = _as_utc(candidate.get("scannedAt"))
        except (TypeError, ValueError):
            continue
        if candidate_time <= current_time:
            return candidate, candidate_time
    return None, None


def rebuild_latest_indexes(path: Path | None = None) -> dict[str, dict[str, str]]:
    """Build bounded hot-path indexes from an existing append-only audit log."""
    target = path or config.SOCIAL_CALLOUT_BROWSER_FEED_LOG
    found: dict[str, dict[str, dict]] = {source: {} for source in SOURCES}
    for candidate in _reverse_jsonl(target):
        if not isinstance(candidate, dict) or not isinstance(candidate.get("records"), list):
            continue
        source = str(candidate.get("source") or "").lower()
        if source not in found:
            continue
        found[source].setdefault("latest", candidate)
        coverage = candidate.get("coverage")
        if isinstance(coverage, dict) and coverage.get("complete") is True:
            found[source].setdefault("complete", candidate)
        if all(set(rows) == {"latest", "complete"} for rows in found.values()):
            break
    summary: dict[str, dict[str, str]] = {}
    if target == config.SOCIAL_CALLOUT_BROWSER_FEED_LOG:
        for source, rows in found.items():
            if "latest" in rows:
                save_json(latest_checkpoint_path(source), rows["latest"])
            if "complete" in rows:
                save_json(latest_checkpoint_path(source, complete=True), rows["complete"])
            summary[source] = {
                key: str(value.get("scannedAt") or "") for key, value in rows.items()
            }
    return summary


class FeedBrowserBatchError(ValueError):
    pass


def _as_utc(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _canonical_record(row: dict, scanned_at: datetime) -> dict:
    """Freeze relative card time before a later checkpoint changes its meaning."""
    record = dict(row)
    published = next(
        (record.get(name) for name in ("publishedAt", "createdAt", "timestamp")
         if record.get(name) not in (None, "")),
        None,
    )
    if published is not None:
        record["publishedAt"] = _as_utc(published).isoformat()
    else:
        record["publishedAt"] = _relative_published_at(
            str(record.get("relativeTime") or ""), scanned_at
        )[0]
    return record


def _worker_stable_text(value: object) -> str:
    stable: list[str] = []
    skip_value = False
    for raw_line in str(value or "").splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        if line.lower() in {"pnl", "mc"}:
            skip_value = True
            continue
        if skip_value:
            if WORKER_PERCENT_RE.fullmatch(line) or WORKER_VALUE_RE.fullmatch(line):
                skip_value = False
                continue
            skip_value = False
        if WORKER_RELATIVE_RE.fullmatch(line) or WORKER_PERCENT_RE.fullmatch(line):
            continue
        stable.append(line.lower())
    return " ".join(stable)


def _record_key(row: dict) -> str:
    post_id = str(row.get("sourcePostId") or row.get("postId") or row.get("id") or "").strip()
    if post_id.startswith(("gmgn-worker:", "fomo-worker:", "pump-worker:")):
        identity = ":".join((
            str(row.get("chain") or "").lower(),
            str(row.get("tokenCa") or "").lower(),
            str(row.get("username") or row.get("handle") or "").lstrip("@").lower(),
            _worker_stable_text(row.get("content") or row.get("text") or row.get("thesis")),
        ))
        return "worker-card:" + hashlib.sha256(identity.encode()).hexdigest()
    if post_id:
        return f"post:{post_id}"
    source_url = str(row.get("sourceUrl") or row.get("url") or "")
    match = X_STATUS_RE.search(source_url)
    if match:
        return f"x:{match.group(1)}"
    normalized = " ".join(
        str(row.get("content") or row.get("text") or row.get("thesis") or "").split()
    ).lower()
    identity = ":".join((
        str(row.get("chain") or "").lower(),
        str(row.get("tokenCa") or "").lower(),
        str(row.get("kind") or "").lower(),
        str(row.get("username") or row.get("handle") or "").lstrip("@").lower(),
        str(row.get("publishedAt") or ""),
        normalized,
    ))
    return "card:" + hashlib.sha256(identity.encode()).hexdigest()


def _merge_record(old: dict, new: dict) -> dict:
    """Prefer new observations without erasing useful fields with empty values."""
    merged = dict(old)
    merged.update({key: value for key, value in new.items() if value not in (None, "", [], {})})
    return merged


def _legacy_broad_fomo_record(row: dict) -> bool:
    """Identify records emitted by the retired whole-pane ancestor matcher."""
    if str(row.get("feedCardType") or "").lower() == "thesis":
        return False
    content = str(row.get("content") or "")
    return (
        re.search(r"(?:^|\n)is up(?:\n|$)", content, re.I) is not None
        or len(re.findall(r"(?:^|\n)Thesis(?:\n|$)", content, re.I)) > 1
        or len(content) > 2500
    )


def _merge_latest_checkpoint(batch: dict, path: Path) -> dict:
    """Merge the latest same-source checkpoint and retain only this scan window."""
    source = str(batch["source"]).lower()
    current_time = _as_utc(batch["scannedAt"])
    previous, previous_time = _latest_same_source(path, source, current_time)

    merged: dict[str, dict] = {}
    if previous is not None and previous_time is not None:
        for row in previous["records"]:
            if isinstance(row, dict):
                # Worker-generated FOMO records must originate from a Thesis
                # Feed card. Drop records produced by the retired broad-link
                # matcher (ticker-strip price fragments) instead of carrying
                # them forever through checkpoint merging.
                if source == "fomo":
                    if _legacy_broad_fomo_record(row):
                        continue
                    if (
                        str(row.get("sourcePostId") or "").startswith("fomo-worker:")
                        and str(row.get("feedCardType") or "").lower() != "thesis"
                        and not re.search(r"(?:^|\n)Thesis(?:\n|$)", str(row.get("content") or ""), re.I)
                    ):
                        continue
                frozen = _canonical_record(row, previous_time)
                merged[_record_key(frozen)] = frozen
    for row in batch["records"]:
        frozen = _canonical_record(row, current_time)
        key = _record_key(frozen)
        merged[key] = _merge_record(merged.get(key, {}), frozen)

    cutoff = current_time.timestamp() - int(batch["coverage"]["windowSeconds"])
    records = [
        row for row in merged.values()
        if _as_utc(row["publishedAt"]).timestamp() >= cutoff
        and _as_utc(row["publishedAt"]) <= current_time
    ]
    records.sort(key=lambda row: _as_utc(row["publishedAt"]), reverse=True)
    verification = dict(batch["verification"])
    if previous_time is not None:
        verification["mergedCheckpointScannedAt"] = previous_time.isoformat()
    return {**batch, "records": records, "verification": verification}


def _has_required_feed_verification(source: str, verification: dict) -> bool:
    if verification.get("feedContainerLocated") is not True:
        return False
    if source == "gmgn":
        return (
            verification.get("calloutPanel") in {"callout", "social_monitor"}
            and set(verification.get("chains") or []) == {"bsc", "robinhood"}
            and set(verification.get("chainFilterSelected") or []) == {"bsc", "robinhood"}
            and verification.get("chainFilterExcludedOthers") is True
        )
    if source == "fomo":
        return verification.get("feedSelected") is True and verification.get("tradesDisabled") is True
    legacy_chain_feed = (
        set(verification.get("chainFilterScanned") or []) == {"bsc", "robinhood"}
        and verification.get("boundaryByChain", {}).get("bsc") is True
        and verification.get("boundaryByChain", {}).get("robinhood") is True
    )
    coin_page_global_feed = (
        verification.get("coinPageFeed") is True
        and verification.get("globalCrossChainFeed") is True
        and (
            verification.get("coinPageMatched") is True
            or verification.get("coinPageNavigationFailed") is True
        )
        and verification.get("browserHomeFeed") is True
        and verification.get("nextPageTokenExhausted") is True
        and verification.get("chainResolution") == "feed-payload"
    )
    return verification.get("calloutsSelected") is True and (
        legacy_chain_feed or coin_page_global_feed
    )


def validate_batch(batch: object, *, check_observed_consistency: bool = True) -> dict:
    if not isinstance(batch, dict):
        raise FeedBrowserBatchError("batch must be a JSON object")
    if batch.get("schemaVersion") != 1:
        raise FeedBrowserBatchError("schemaVersion must be 1")
    if batch.get("complete") is not True:
        raise FeedBrowserBatchError("complete=true is required")
    source = str(batch.get("source") or "").lower()
    if source not in SOURCES:
        raise FeedBrowserBatchError("source must be gmgn, fomo or pump")
    parsed = urlparse(str(batch.get("pageUrl") or ""))
    if parsed.scheme != "https" or parsed.hostname not in SOURCE_HOSTS[source]:
        raise FeedBrowserBatchError(f"pageUrl must be an https://{next(iter(SOURCE_HOSTS[source]))} page")
    try:
        scanned_at = datetime.fromisoformat(str(batch.get("scannedAt") or "").replace("Z", "+00:00"))
    except ValueError as exc:
        raise FeedBrowserBatchError("scannedAt must be ISO-8601") from exc
    if scanned_at.tzinfo is None:
        raise FeedBrowserBatchError("scannedAt must include a timezone")
    if scanned_at.astimezone(timezone.utc) > datetime.now(timezone.utc):
        raise FeedBrowserBatchError("scannedAt cannot be in the future")
    coverage = batch.get("coverage")
    if not isinstance(coverage, dict) or coverage.get("complete") not in {True, False}:
        raise FeedBrowserBatchError("coverage.complete must be a boolean")
    window_seconds = int(coverage.get("windowSeconds") or 0)
    if window_seconds < 300 or window_seconds > config.SOCIAL_CALLOUT_RECOVERY_LOOKBACK_SECONDS:
        raise FeedBrowserBatchError(
            "coverage.windowSeconds must be between 300 and the configured recovery window"
        )
    verification = batch.get("verification")
    if not isinstance(verification, dict) or not _has_required_feed_verification(source, verification):
        raise FeedBrowserBatchError(f"missing {source} feed verification")
    records = batch.get("records")
    if not isinstance(records, list):
        raise FeedBrowserBatchError("records must be a list")
    if (
        coverage.get("complete") is True
        and verification.get("boundaryReached") is not True
        and verification.get("noResults") is not True
    ):
        raise FeedBrowserBatchError(
            "coverage.complete=true requires boundaryReached=true or an explicit noResults=true"
        )
    visible_eligible = int(
        verification.get("eligibleMatchedCards")
        or coverage.get("eligibleMatchedCards")
        or 0
    )
    if (
        check_observed_consistency
        and coverage.get("complete") is True
        and not records
        and visible_eligible > 0
    ):
        raise FeedBrowserBatchError(
            f"{source.upper()} complete empty batch contradicts visible eligible matched cards"
        )
    if (
        coverage.get("complete") is True
        and int(verification.get("scannedCards") or coverage.get("scannedCards") or 0) == 0
        and verification.get("noResults") is not True
    ):
        raise FeedBrowserBatchError(
            f"{source.upper()} complete coverage cannot be proven from zero scanned Feed cards"
        )
    if source == "gmgn":
        page_chain = str(parse_qs(parsed.query).get("chain", [""])[0]).lower()
        record_chains = {
            str(row.get("chain") or "").lower()
            for row in records if isinstance(row, dict)
        }
        detected_route_mismatch = bool(
            page_chain in CHAINS and any(chain != page_chain for chain in record_chains)
        )
        route_mismatch = (
            detected_route_mismatch
            or
            verification.get("routeMismatch") is True
            or verification.get("robinhoodRouteMismatchObserved") is True
        )
        if coverage.get("complete") is True and route_mismatch and verification.get("routeMismatchResolved") is not True:
            raise FeedBrowserBatchError(
                "GMGN route mismatch must be resolved before coverage.complete=true"
            )
    for index, row in enumerate(records):
        if not isinstance(row, dict):
            raise FeedBrowserBatchError(f"records[{index}] must be an object")
        chain = str(row.get("chain") or "").lower()
        token_ca = str(row.get("tokenCa") or "")
        if chain not in CHAINS:
            raise FeedBrowserBatchError(f"records[{index}].chain must be bsc or robinhood")
        if not re.fullmatch(r"0x[a-f0-9]{40}", token_ca, re.I):
            raise FeedBrowserBatchError(f"records[{index}].tokenCa must be an exact EVM contract")
        kind = str(row.get("kind") or "")
        if kind not in {"native", "x"}:
            raise FeedBrowserBatchError(f"records[{index}].kind must be native or x")
        if not str(row.get("content") or row.get("text") or row.get("thesis") or "").strip():
            raise FeedBrowserBatchError(f"records[{index}] requires content")
        if not str(row.get("username") or row.get("handle") or "").strip():
            raise FeedBrowserBatchError(f"records[{index}] requires username")
        if (
            source == "fomo"
            and str(row.get("sourcePostId") or "").startswith("fomo-worker:")
            and str(row.get("feedCardType") or "").lower() != "thesis"
            and not re.search(r"(?:^|\n)Thesis(?:\n|$)", str(row.get("content") or ""), re.I)
        ):
            raise FeedBrowserBatchError(f"records[{index}] is not a FOMO Thesis Feed card")
        evm_address = str(row.get("evmAddress") or row.get("walletAddress") or "").strip()
        if evm_address and not re.fullmatch(r"0x[a-f0-9]{40}", evm_address, re.I):
            raise FeedBrowserBatchError(f"records[{index}] has invalid evmAddress")
        if not any(row.get(name) not in (None, "") for name in ("publishedAt", "createdAt", "timestamp")):
            try:
                _relative_published_at(str(row.get("relativeTime") or ""), scanned_at)
            except Exception as exc:
                raise FeedBrowserBatchError(f"records[{index}] has invalid time") from exc
        if kind == "x" and not X_STATUS_RE.search(str(row.get("sourceUrl") or row.get("url") or "")):
            raise FeedBrowserBatchError(
                f"records[{index}] requires an exact X status URL with tweet ID"
            )
    return batch


def persist_batch(batch: object, path: Path | None = None) -> dict:
    validated = validate_batch(batch)
    target = path or config.SOCIAL_CALLOUT_BROWSER_FEED_LOG
    previous = (
        load_json(latest_checkpoint_path(validated["source"]), None)
        if target == config.SOCIAL_CALLOUT_BROWSER_FEED_LOG else None
    )
    merged = _merge_latest_checkpoint(validated, target)
    # Validate again because merging freezes relative times and can incorporate
    # records from the previous checkpoint.
    # The merged record list is window-filtered, while eligibleMatchedCards is
    # evidence from the current visible-page scan. A visible card can be older
    # than the rolling cutoff and therefore correctly disappear from `records`.
    # The raw batch above was validated strictly; do not compare those different
    # populations a second time here.
    validated = validate_batch(merged, check_observed_consistency=False)
    # Minute-level X snapshots are freshness heartbeats as well as data. Keep
    # every newest snapshot in the atomic sidecar, but do not append another
    # large audit-log line when the actual records and completeness proof are
    # unchanged. This prevents a quiet Feed from growing by tens of MB/hour.
    previous_signature = None
    if isinstance(previous, dict) and isinstance(previous.get("records"), list):
        previous_signature = (
            bool(previous.get("coverage", {}).get("complete")),
            json.dumps(previous["records"], sort_keys=True, ensure_ascii=False),
        )
    current_signature = (
        bool(validated["coverage"].get("complete")),
        json.dumps(validated["records"], sort_keys=True, ensure_ascii=False),
    )
    previous_complete = bool(
        isinstance(previous, dict)
        and previous.get("coverage", {}).get("complete") is True
    )
    current_complete = bool(validated["coverage"].get("complete"))
    previous_audit_at = None
    if isinstance(previous, dict):
        try:
            previous_audit_at = _as_utc(
                previous.get("verification", {}).get("auditCheckpointAt")
            )
        except (TypeError, ValueError):
            previous_audit_at = None
    current_at = _as_utc(validated["scannedAt"])
    audit_due = (
        previous_audit_at is None
        or (current_at - previous_audit_at).total_seconds() >= 600
    )
    audit_appended = bool(
        previous_signature != current_signature
        and (audit_due or previous_complete != current_complete)
    )
    validated["verification"]["auditCheckpointAt"] = (
        current_at.isoformat()
        if audit_appended else previous_audit_at.isoformat()
        if previous_audit_at is not None else current_at.isoformat()
    )
    if audit_appended:
        append_jsonl(target, validated)
    if target == config.SOCIAL_CALLOUT_BROWSER_FEED_LOG:
        save_json(latest_checkpoint_path(validated["source"]), validated)
        if validated["coverage"]["complete"] is True:
            save_json(latest_checkpoint_path(validated["source"], complete=True), validated)
    return {
        "status": "accepted",
        "source": validated["source"],
        "records": len(validated["records"]),
        "coverageComplete": validated["coverage"]["complete"],
        "auditAppended": audit_appended,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Persist a verified platform callout feed batch")
    parser.add_argument("file", nargs="?", help="JSON file; omit to read stdin")
    args = parser.parse_args()
    if args.file:
        payload = json.loads(Path(args.file).read_text(encoding="utf-8"))
    else:
        payload = json.load(sys.stdin)
    print(json.dumps(persist_batch(payload), ensure_ascii=False))


if __name__ == "__main__":
    main()
