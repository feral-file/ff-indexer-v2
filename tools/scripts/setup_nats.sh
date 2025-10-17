#!/usr/bin/env bash
set -euo pipefail

STREAM_NAME=${1:-BLOCKCHAIN_EVENTS}
CONFIG_FILE="tools/nats/events.json"

if ! command -v nats >/dev/null 2>&1; then
  echo "❌ Please install the NATS CLI (https://github.com/nats-io/natscli)."
  exit 1
fi

echo "➡️  Ensuring NATS stream '$STREAM_NAME' exists..."

if nats stream info "$STREAM_NAME" >/dev/null 2>&1; then
  echo "✅ Stream already exists. Updating config..."
  yes | nats stream update --config "$CONFIG_FILE"
else
  echo "🆕 Creating stream..."
  yes | nats stream add --config "$CONFIG_FILE"
fi

echo "🎉 Stream '$STREAM_NAME' ready!"
