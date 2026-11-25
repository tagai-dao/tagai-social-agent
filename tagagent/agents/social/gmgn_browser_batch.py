"""Validate and persist GMGN website batches produced by browser automation.

The collector must read only visible page content from both ``GMGN喊单`` and
``X喊单`` tabs. This module deliberately does not accept cookies, tokens or
browser storage; it is a narrow hand-off between the browser and Social Agent.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from tagagent import config
from tagagent.tools.callout_platforms import (
    GMGN_TOKEN_URL_RE,
    X_STATUS_RE,
    _relative_published_at,
)
from tagagent.wiki import append_jsonl


class GMGNBrowserBatchError(ValueError):
    pass


def validate_batch(batch: object) -> dict:
    if not isinstance(batch, dict):
        raise GMGNBrowserBatchError("batch must be a JSON object")
    if batch.get("schemaVersion") != 1:
        raise GMGNBrowserBatchError("schemaVersion must be 1")
    if batch.get("complete") is not True:
        raise GMGNBrowserBatchError("complete=true is required")
    chain = str(batch.get("chain") or "").lower()
    token_ca = str(batch.get("tokenCa") or "")
    match = GMGN_TOKEN_URL_RE.match(str(batch.get("pageUrl") or ""))
    if not match or match.group("chain").lower() != chain or match.group("ca").lower() != token_ca.lower():
        raise GMGNBrowserBatchError("pageUrl must exactly match chain + tokenCa")
    try:
        scanned_at = datetime.fromisoformat(str(batch.get("scannedAt") or "").replace("Z", "+00:00"))
    except ValueError as exc:
        raise GMGNBrowserBatchError("scannedAt must be ISO-8601") from exc
    if scanned_at.tzinfo is None:
        raise GMGNBrowserBatchError("scannedAt must include a timezone")
    if scanned_at.astimezone(timezone.utc) > datetime.now(timezone.utc):
        raise GMGNBrowserBatchError("scannedAt cannot be in the future")
    tabs = batch.get("tabs")
    if not isinstance(tabs, dict) or not all(name in tabs for name in ("gmgn", "x")):
        raise GMGNBrowserBatchError("tabs.gmgn and tabs.x are required")
    for name in ("gmgn", "x"):
        if not isinstance(tabs[name], list):
            raise GMGNBrowserBatchError(f"tabs.{name} must be a list")
        for index, row in enumerate(tabs[name]):
            if not isinstance(row, dict) or not str(row.get("content") or "").strip():
                raise GMGNBrowserBatchError(f"tabs.{name}[{index}] requires content")
            if not str(row.get("username") or row.get("handle") or "").strip():
                raise GMGNBrowserBatchError(f"tabs.{name}[{index}] requires username")
            try:
                _relative_published_at(str(row.get("relativeTime") or ""), scanned_at)
            except Exception as exc:
                raise GMGNBrowserBatchError(
                    f"tabs.{name}[{index}] has invalid relativeTime"
                ) from exc
            if name == "x":
                source_url = str(row.get("sourceUrl") or row.get("url") or "")
                if not X_STATUS_RE.search(source_url):
                    raise GMGNBrowserBatchError(
                        f"tabs.x[{index}] requires an exact X status URL with tweet ID"
                    )
    return batch


def persist_batch(batch: object, path: Path | None = None) -> dict:
    validated = validate_batch(batch)
    append_jsonl(path or config.SOCIAL_GMGN_BROWSER_BATCH_LOG, validated)
    return {
        "status": "accepted",
        "chain": validated["chain"],
        "tokenCa": validated["tokenCa"],
        "gmgn": len(validated["tabs"]["gmgn"]),
        "x": len(validated["tabs"]["x"]),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Persist a verified GMGN browser scan batch")
    parser.add_argument("file", nargs="?", help="JSON file; omit to read stdin")
    args = parser.parse_args()
    if args.file:
        payload = json.loads(Path(args.file).read_text(encoding="utf-8"))
    else:
        payload = json.load(sys.stdin)
    print(json.dumps(persist_batch(payload), ensure_ascii=False))


if __name__ == "__main__":
    main()
