"""Small, environment-driven config for the standalone Social Agent package."""
from __future__ import annotations

import os
from pathlib import Path


HOME = Path.home()
PKG_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_ROOT = Path(os.environ.get("TAGAI_SOCIAL_RUNTIME", PKG_ROOT / "runtime")).expanduser()
WIKI_ROOT = Path(os.environ.get("TAGAI_SOCIAL_WIKI", HOME / "wiki")).expanduser()
HERMES_ENV_PATH = Path(os.environ.get("HERMES_ENV_PATH", PKG_ROOT / ".env"))


def _dotenv_value(path: Path, name: str) -> str:
    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                if key.strip() == name:
                    return value.strip().strip("'\"")
    except OSError:
        pass
    return ""


def hermes_env(name: str, default: str = "") -> str:
    return (
        os.environ.get(name, "").strip()
        or _dotenv_value(HERMES_ENV_PATH, name)
        or _dotenv_value(HOME / ".hermes" / ".env", name)
        or default
    )


def _path_env(name: str, default: Path) -> Path:
    return Path(hermes_env(name, str(default))).expanduser()


SOCIAL_CALLOUT_ENABLED = hermes_env("SOCIAL_CALLOUT_ENABLED", "1") == "1"
SOCIAL_CALLOUT_EXECUTE = hermes_env("SOCIAL_CALLOUT_EXECUTE", "0") == "1"
SOCIAL_CALLOUT_MIN_MARKET_CAP_USD = int(hermes_env("SOCIAL_CALLOUT_MIN_MARKET_CAP_USD", "100000"))
SOCIAL_CALLOUT_LOOKBACK_SECONDS = int(hermes_env("SOCIAL_CALLOUT_LOOKBACK_SECONDS", "1200"))
SOCIAL_CALLOUT_INTERVAL_SECONDS = max(60, int(hermes_env("SOCIAL_CALLOUT_INTERVAL_SECONDS", "600")))
SOCIAL_CALLOUT_IMPORT_MAX_WORKERS = max(1, int(hermes_env("SOCIAL_CALLOUT_IMPORT_MAX_WORKERS", "1")))
SOCIAL_CALLOUT_IMPORT_MAX_WORKERS_BSC = max(1, int(hermes_env("SOCIAL_CALLOUT_IMPORT_MAX_WORKERS_BSC", "1")))
SOCIAL_CALLOUT_IMPORT_MAX_WORKERS_ROBINHOOD = max(1, int(hermes_env("SOCIAL_CALLOUT_IMPORT_MAX_WORKERS_ROBINHOOD", "1")))
SOCIAL_CALLOUT_IMPORT_RETRY_LIMIT_PER_CHAIN = max(1, int(hermes_env("SOCIAL_CALLOUT_IMPORT_RETRY_LIMIT_PER_CHAIN", "3")))
SOCIAL_CALLOUT_IMPORT_RETRY_BASE_DELAY_SECONDS = max(60, int(hermes_env("SOCIAL_CALLOUT_IMPORT_RETRY_BASE_DELAY_SECONDS", "300")))
SOCIAL_CALLOUT_RECOVERY_LOOKBACK_SECONDS = max(
    SOCIAL_CALLOUT_LOOKBACK_SECONDS,
    int(hermes_env("SOCIAL_CALLOUT_RECOVERY_LOOKBACK_SECONDS", "3600")),
)
SOCIAL_CALLOUT_API_BASE = hermes_env("SOCIAL_CALLOUT_API_BASE", "https://bsc-api.tagai.fun").rstrip("/")
SOCIAL_CALLOUT_TAGCLAW_KEY = hermes_env("SOCIAL_CALLOUT_TAGCLAW_KEY") or _dotenv_value(
    HOME / ".hermes" / "skills" / "tagclaw" / ".env", "TAGCLAW_API_KEY"
)
SOCIAL_CALLOUT_WINDOW_STATE = _path_env(
    "SOCIAL_CALLOUT_WINDOW_STATE", RUNTIME_ROOT / "social-callouts" / "collection-window-state.json"
)
SOCIAL_CALLOUT_LEDGER = RUNTIME_ROOT / "social-callouts" / "import-ledger.jsonl"
SOCIAL_CALLOUT_AUDIT_LOG = RUNTIME_ROOT / "social-callouts" / "monitor-log.jsonl"
SOCIAL_CALLOUT_BROWSER_FEED_LOG = _path_env(
    "SOCIAL_CALLOUT_BROWSER_FEED_LOG", RUNTIME_ROOT / "social-callouts" / "browser-feed-batches.jsonl"
)
SOCIAL_CALLOUT_BROWSER_FEED_MAX_AGE_SECONDS = int(hermes_env("SOCIAL_CALLOUT_BROWSER_FEED_MAX_AGE_SECONDS", "900"))
SOCIAL_GMGN_BROWSER_BATCH_LOG = _path_env(
    "SOCIAL_GMGN_BROWSER_BATCH_LOG", RUNTIME_ROOT / "social-callouts" / "gmgn-browser-batches.jsonl"
)
SOCIAL_GMGN_BROWSER_BATCH_MAX_AGE_SECONDS = int(hermes_env("SOCIAL_GMGN_BROWSER_BATCH_MAX_AGE_SECONDS", "900"))
SOCIAL_GMGN_LOOKBACK_SECONDS = int(hermes_env("SOCIAL_GMGN_LOOKBACK_SECONDS", "600"))
SOCIAL_GMGN_INCLUDE_3D_BOUNDARY = hermes_env("SOCIAL_GMGN_INCLUDE_3D_BOUNDARY", "0") == "1"
SOCIAL_FOMO_CALLOUT_URL_TEMPLATE = hermes_env("SOCIAL_FOMO_CALLOUT_URL_TEMPLATE")
SOCIAL_FOMO_ACCESS_TOKEN = hermes_env("SOCIAL_FOMO_ACCESS_TOKEN")
SOCIAL_FOMO_BROWSER_BATCH_LOG = _path_env(
    "SOCIAL_FOMO_BROWSER_BATCH_LOG", RUNTIME_ROOT / "social-callouts" / "fomo-browser-batches.jsonl"
)
SOCIAL_FOMO_BROWSER_BATCH_MAX_AGE_SECONDS = int(hermes_env("SOCIAL_FOMO_BROWSER_BATCH_MAX_AGE_SECONDS", "900"))
SOCIAL_FEED_BROWSER_COLLECTOR_COMMAND = hermes_env("SOCIAL_FEED_BROWSER_COLLECTOR_COMMAND").strip()
SOCIAL_FEED_BROWSER_COLLECTOR_TIMEOUT_SECONDS = max(
    30, int(hermes_env("SOCIAL_FEED_BROWSER_COLLECTOR_TIMEOUT_SECONDS", "240"))
)
SOCIAL_PUMP_API_BASE = hermes_env("SOCIAL_PUMP_API_BASE", "https://frontend-api-v3.pump.fun").rstrip("/")
SOCIAL_PUMP_MAX_ATTEMPTS = max(1, int(hermes_env("SOCIAL_PUMP_MAX_ATTEMPTS", "3")))
SOCIAL_PUMP_MAX_WORKERS = max(1, min(12, int(hermes_env("SOCIAL_PUMP_MAX_WORKERS", "6"))))
SOCIAL_PUMP_RETRY_BACKOFF_SECONDS = max(0.0, float(hermes_env("SOCIAL_PUMP_RETRY_BACKOFF_SECONDS", "0.75")))
SOCIAL_BROWSER_SCAN_QUEUE = _path_env(
    "SOCIAL_BROWSER_SCAN_QUEUE", RUNTIME_ROOT / "social-callouts" / "browser-scan-queue.json"
)
TRENDING_IMPORT_FOMO_SNAPSHOT = RUNTIME_ROOT / "trending-import" / "fomo-latest.json"
AGENT_USER_ID = hermes_env("TAGAGENT_X_USER_ID", "1672983517528211457")
AGENT_HANDLE = hermes_env("TAGAGENT_X_HANDLE", "TagAgentX").lstrip("@")
XURL_BIN = hermes_env("TAGAGENT_XURL_BIN", "xurl")
X_API_APP = hermes_env("X_API_APP", "tagagent")
X_API_USERNAME = hermes_env("X_API_USERNAME", AGENT_HANDLE)
DEFAULT_PROXY = hermes_env("DEFAULT_PROXY", "")
