"""Invoke the persistent browser Feed worker and persist its platform batches.

This is deliberately a narrow bridge: the worker owns browser UI state while
this module owns the durable TagAI hand-off.  It prevents the monitor from
mistaking target-query counts for an executed browser collection.
"""

from __future__ import annotations

import argparse
import json
import shlex
import subprocess
from pathlib import Path

from tagagent import config
from tagagent.agents.social.feed_browser_batch import persist_batch


class FeedCollectorError(RuntimeError):
    pass


SOURCES = ("fomo", "gmgn", "pump")


def _command() -> list[str]:
    raw = config.SOCIAL_FEED_BROWSER_COLLECTOR_COMMAND
    if not raw:
        raise FeedCollectorError(
            "SOCIAL_FEED_BROWSER_COLLECTOR_COMMAND is not configured; "
            "no persistent browser Feed worker is available"
        )
    command = shlex.split(raw)
    if not command:
        raise FeedCollectorError("browser Feed worker command is empty")
    return command


def collect(source: str, *, window_seconds: int) -> dict:
    """Run one source worker and persist every returned feed batch.

    The worker receives only explicit collection scope and must print either a
    JSON object or newline-delimited JSON objects.  Shell interpolation is
    intentionally avoided.
    """
    if source not in SOURCES:
        raise FeedCollectorError(f"unsupported Feed source: {source}")
    command = [*_command(), "--source", source, "--window-seconds", str(window_seconds)]
    try:
        result = subprocess.run(
            command,
            check=False,
            text=True,
            capture_output=True,
            timeout=config.SOCIAL_FEED_BROWSER_COLLECTOR_TIMEOUT_SECONDS,
        )
    except OSError as exc:
        raise FeedCollectorError(f"browser Feed worker could not start: {exc}") from exc
    except subprocess.TimeoutExpired as exc:
        raise FeedCollectorError("browser Feed worker timed out") from exc
    if result.returncode:
        detail = (result.stderr or result.stdout).strip()[-1000:]
        raise FeedCollectorError(f"browser Feed worker failed ({result.returncode}): {detail}")
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        raise FeedCollectorError("browser Feed worker returned no batch")
    try:
        batches = [json.loads(line) for line in lines]
    except json.JSONDecodeError as exc:
        raise FeedCollectorError("browser Feed worker returned invalid JSON") from exc
    accepted = []
    for batch in batches:
        if str(batch.get("source") or "").lower() != source:
            raise FeedCollectorError(f"worker returned a {batch.get('source')!r} batch for {source}")
        coverage = batch.get("coverage") if isinstance(batch, dict) else None
        if not isinstance(coverage, dict) or int(coverage.get("windowSeconds") or 0) != window_seconds:
            raise FeedCollectorError("worker batch window does not match requested recovery window")
        accepted.append(persist_batch(batch))
    return {"source": source, "batches": accepted}


def collect_all(*, window_seconds: int) -> dict:
    results, errors = {}, {}
    for source in SOURCES:
        try:
            results[source] = collect(source, window_seconds=window_seconds)
        except FeedCollectorError as exc:
            errors[source] = str(exc)
    return {"results": results, "errors": errors}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the persistent browser Feed collector")
    parser.add_argument("--source", choices=SOURCES)
    parser.add_argument("--window-seconds", type=int, required=True)
    args = parser.parse_args()
    if args.source:
        print(json.dumps(collect(args.source, window_seconds=args.window_seconds), ensure_ascii=False))
    else:
        print(json.dumps(collect_all(window_seconds=args.window_seconds), ensure_ascii=False))


if __name__ == "__main__":
    main()
