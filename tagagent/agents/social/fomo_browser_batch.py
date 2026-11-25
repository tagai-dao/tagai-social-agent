"""Validate and persist FOMO batches collected from a visible browser page."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from tagagent import config
from tagagent.tools.callout_platforms import X_STATUS_RE, _relative_published_at, _valid_fomo_page_url
from tagagent.wiki import append_jsonl


class FOMOBrowserBatchError(ValueError):
    pass


def validate_batch(batch: object) -> dict:
    if not isinstance(batch, dict):
        raise FOMOBrowserBatchError("batch must be a JSON object")
    if batch.get("schemaVersion") != 1:
        raise FOMOBrowserBatchError("schemaVersion must be 1")
    if batch.get("complete") is not True:
        raise FOMOBrowserBatchError("complete=true is required")
    chain = str(batch.get("chain") or "").lower()
    token_ca = str(batch.get("tokenCa") or "")
    if chain not in {"bsc", "robinhood"}:
        raise FOMOBrowserBatchError("chain must be bsc or robinhood")
    if not re.fullmatch(r"0x[a-f0-9]{40}", token_ca, re.I):
        raise FOMOBrowserBatchError("tokenCa must be an EVM contract address")
    if str(batch.get("pageChain") or "").lower() != chain:
        raise FOMOBrowserBatchError("pageChain must exactly match chain")
    if str(batch.get("pageTokenCa") or "").lower() != token_ca.lower():
        raise FOMOBrowserBatchError("pageTokenCa must exactly match tokenCa")
    if not _valid_fomo_page_url(batch.get("pageUrl")):
        raise FOMOBrowserBatchError("pageUrl must be an https://fomo.family page")
    try:
        scanned_at = datetime.fromisoformat(str(batch.get("scannedAt") or "").replace("Z", "+00:00"))
    except ValueError as exc:
        raise FOMOBrowserBatchError("scannedAt must be ISO-8601") from exc
    if scanned_at.tzinfo is None:
        raise FOMOBrowserBatchError("scannedAt must include a timezone")
    if scanned_at.astimezone(timezone.utc) > datetime.now(timezone.utc):
        raise FOMOBrowserBatchError("scannedAt cannot be in the future")
    records = batch.get("records")
    if not isinstance(records, list):
        raise FOMOBrowserBatchError("records must be a list")
    for index, row in enumerate(records):
        if not isinstance(row, dict):
            raise FOMOBrowserBatchError(f"records[{index}] must be an object")
        kind = str(row.get("kind") or "")
        if kind not in {"native", "x"}:
            raise FOMOBrowserBatchError(f"records[{index}].kind must be native or x")
        if not str(row.get("content") or row.get("text") or row.get("thesis") or "").strip():
            raise FOMOBrowserBatchError(f"records[{index}] requires content")
        if not str(row.get("username") or row.get("handle") or "").strip():
            raise FOMOBrowserBatchError(f"records[{index}] requires username")
        evm_address = str(row.get("evmAddress") or row.get("walletAddress") or "").strip()
        if evm_address and not re.fullmatch(r"0x[a-f0-9]{40}", evm_address, re.I):
            raise FOMOBrowserBatchError(f"records[{index}] has invalid evmAddress")
        if not any(row.get(name) not in (None, "") for name in ("publishedAt", "createdAt", "timestamp")):
            try:
                _relative_published_at(str(row.get("relativeTime") or ""), scanned_at)
            except Exception as exc:
                raise FOMOBrowserBatchError(f"records[{index}] has invalid time") from exc
        if kind == "x":
            source_url = str(row.get("sourceUrl") or row.get("url") or "")
            if not X_STATUS_RE.search(source_url):
                raise FOMOBrowserBatchError(
                    f"records[{index}] requires an exact X status URL with tweet ID"
                )
    return batch


def persist_batch(batch: object, path: Path | None = None) -> dict:
    validated = validate_batch(batch)
    append_jsonl(path or config.SOCIAL_FOMO_BROWSER_BATCH_LOG, validated)
    return {
        "status": "accepted",
        "chain": validated["chain"],
        "tokenCa": validated["tokenCa"],
        "records": len(validated["records"]),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Persist a verified FOMO browser scan batch")
    parser.add_argument("file", nargs="?", help="JSON file; omit to read stdin")
    args = parser.parse_args()
    if args.file:
        payload = json.loads(Path(args.file).read_text(encoding="utf-8"))
    else:
        payload = json.load(sys.stdin)
    print(json.dumps(persist_batch(payload), ensure_ascii=False))


if __name__ == "__main__":
    main()
