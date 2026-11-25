#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON="${PYTHON:-python3}"
VENV="$ROOT/.venv"
LABEL="com.tagai.social-feed-worker"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "This LaunchAgent installer is for macOS. On other OSes run the module directly." >&2
  exit 2
fi

"$PYTHON" -m venv "$VENV"
"$VENV/bin/python" -m pip install --upgrade pip
"$VENV/bin/pip" install -e "$ROOT"

if [[ ! -f "$ROOT/.env" ]]; then
  cp "$ROOT/.env.example" "$ROOT/.env"
  chmod 600 "$ROOT/.env"
  echo "Created $ROOT/.env. Add credentials there only on this machine; never commit it."
fi

mkdir -p "$HOME/Library/LaunchAgents" "$ROOT/runtime/social-callouts"
cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key><array>
    <string>/usr/bin/caffeinate</string><string>-i</string>
    <string>$VENV/bin/python</string><string>-u</string><string>-m</string>
    <string>tagagent.agents.social.persistent_feed_worker</string>
    <string>--loop</string><string>--interval</string><string>600</string>
  </array>
  <key>WorkingDirectory</key><string>$ROOT</string>
  <key>RunAtLoad</key><true/><key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>$ROOT/runtime/social-callouts/worker.log</string>
  <key>StandardErrorPath</key><string>$ROOT/runtime/social-callouts/worker.err.log</string>
</dict></plist>
EOF

echo "Installed package and LaunchAgent plist at $PLIST."
echo "Before loading: configure $ROOT/.env, install Google Chrome and xurl, then manually sign in to the dedicated Chrome profile."
echo "Load with: launchctl bootstrap gui/$(id -u) '$PLIST'"
