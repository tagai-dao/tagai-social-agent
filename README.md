# TagAI Social Agent

Persistent browser-feed collection and TagAI callout import for FOMO, GMGN, and Pump.fun. It runs a dedicated Chrome profile, records atomic browser-feed batches and health state, then imports eligible communities by exact chain + contract address (minimum market cap defaults to **strictly greater than $100,000**). It does not use ticker matching.

This repository contains only the service code and install scaffolding. Browser profiles, runtime data, health files, import ledgers, credentials, and the originating machine's LaunchAgent are intentionally excluded.

## Requirements

- Windows 10/11 or macOS, Python 3.11+, Google Chrome, and (for optional X enrichment) the `xurl` CLI configured for the operator's own account. Hermes is not required.
- A TagAI API credential with callout-import permission to enable imports.
- FOMO/GMGN sessions established by the operator in the worker's dedicated Chrome profile. This package does not automate login, CAPTCHA, terms acceptance, or wallet actions.

## Windows 安装与验收

在自己的 Windows 登录账户中打开 PowerShell（不需要以 SYSTEM 运行）：

```powershell
git clone https://github.com/tagai-dao/tagai-social-agent.git
cd tagai-social-agent
.\scripts\install-windows.ps1 -RegisterTask
notepad .env
```

填写自己的 `SOCIAL_CALLOUT_TAGCLAW_KEY`，即使试运行也需要此 key 查询白名单。先保留 `SOCIAL_CALLOUT_EXECUTE=0`。Chrome 非标准安装位置可在 .env 设置 `TAGAI_CHROME_PATH` 为 chrome.exe 的完整路径。如果 python 不在 PATH，给安装脚本传入 `-Python` 和 Python 3.11+ 的完整路径。脚本仅注册当前用户登录时运行的任务，不会立即启动，也不覆盖同名任务。

```powershell
Start-ScheduledTask -TaskName 'TagAI Social Feed Worker'
Get-Content .\runtime\social-callouts\persistent-worker-health.json -Raw
```

在首次打开的专用 Chrome 中手动登录。保持 Windows 登录、电脑不休眠，并为专用浏览器保留本机端口 9223。确认三平台都完成扫描，查看 `runtime/social-callouts/monitor-log.jsonl` 的两链结果，再决定是否设置 `SOCIAL_CALLOUT_EXECUTE=1`。可在下次登录时应用配置。停止服务用 `Stop-ScheduledTask -TaskName 'TagAI Social Feed Worker'`；移除自启动用 `Unregister-ScheduledTask -TaskName 'TagAI Social Feed Worker'`。不要分享 .env 或 runtime。

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
