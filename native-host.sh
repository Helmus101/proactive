#!/bin/bash
# Native Messaging Host Wrapper
# This script is launched by Chrome and forwards stdin/stdout to the Proactive Desktop App.

# Ensure Node is resolvable even when Chrome launches with a minimal PATH.
if command -v node >/dev/null 2>&1; then
  NODE_BIN="$(command -v node)"
elif [ -x "/opt/homebrew/bin/node" ]; then
  NODE_BIN="/opt/homebrew/bin/node"
elif [ -x "/usr/local/bin/node" ]; then
  NODE_BIN="/usr/local/bin/node"
elif [ -x "$HOME/.nvm/versions/node/current/bin/node" ]; then
  NODE_BIN="$HOME/.nvm/versions/node/current/bin/node"
else
  echo "Native host error: node binary not found" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "$NODE_BIN" "$SCRIPT_DIR/native-host.js"
