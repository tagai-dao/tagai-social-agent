# TagAI Social Agent

Persistent browser-feed collection and TagAI callout import for FOMO, GMGN, and Pump.fun. It runs a dedicated Chrome profile, records atomic browser-feed batches and health state, then imports eligible communities by exact chain + contract address (minimum market cap defaults to **strictly greater than $100,000**). It does not use ticker matching.

This repository contains only the service code and install scaffolding. Browser profiles, runtime data, health files, import ledgers, credentials, and the originating machine's LaunchAgent are intentionally excluded.

## Requirements

- macOS, Python 3.11+, Google Chrome, and (for X enrichment) the `xurl` CLI configured for the operator's own account.
- A TagAI API credential with callout-import permission to enable imports.
- FOMO/GMGN sessions established by the operator in the worker's dedicated Chrome profile. This package does not automate login, CAPTCHA, terms acceptance, or wallet actions.

## Install on macOS

```sh
git clone https://github.com/tagai-dao/tagai-social-agent.git
cd tagai-social-agent
./scripts/install-macos.sh
```

Edit `.env` locally. Keep `SOCIAL_CALLOUT_EXECUTE=0` while validating configuration and feed coverage. Imports are disabled by default unless you explicitly set `SOCIAL_CALLOUT_EXECUTE=1` and provide `SOCIAL_CALLOUT_TAGCLAW_KEY`. Never commit `.env`.

The LaunchAgent uses a dedicated profile under `runtime/social-callouts/chrome-worker-profile`; the first run opens that profile. Sign into the required feed sites yourself, then leave the worker running. To load/reload it:

```sh
launchctl bootstrap gui/$(id -u) "$HOME/Library/LaunchAgents/com.tagai.social-feed-worker.plist"
# Later updates:
launchctl kickstart -k gui/$(id -u)/com.tagai.social-feed-worker
```

To inspect health and logs:

```sh
cat runtime/social-callouts/persistent-worker-health.json
tail -f runtime/social-callouts/worker.log runtime/social-callouts/worker.err.log
```

The worker runs complete FOMO → GMGN → Pump cycles every 600 seconds. Do not start a second browser worker or importer against the same runtime directory. `callout_monitor` supports dry-run by default and can also be run manually with `python -m tagagent.agents.social.callout_monitor --require-complete` after the persistent browser worker is stopped.

## Configuration

See `.env.example`. `TAGAI_SOCIAL_RUNTIME` and `TAGAI_SOCIAL_WIKI` can relocate generated runtime and community wiki data. FOMO token/template settings are optional. BSC and Robinhood imports default to one worker each; idempotency ledger and retry behavior are maintained per runtime directory.

Use a dedicated TagAI key with the minimum required privileges. Importing makes external API writes. Review eligibility and source attribution before enabling execution.

## Data and privacy

The runtime directory may contain authenticated browser state, tweet content, callout records, and import ledgers. Treat it as private machine data. It is gitignored and should not be uploaded or shared. Keep `source`, `sourceUrl`, and `originalXTweetId` attribution intact in downstream records.
