#!/usr/bin/env bash
set -euo pipefail

NAMESPACE_NAME=${1:-ff-indexer}
RETENTION_DAYS=${2:-14d}

if ! command -v temporal >/dev/null 2>&1; then
  echo "❌ Please install the Temporal CLI (https://docs.temporal.io/cli)."
  exit 1
fi

echo "➡️  Ensuring Temporal namespace '$NAMESPACE_NAME' exists..."

# Check if namespace already exists
if temporal operator namespace describe "$NAMESPACE_NAME" >/dev/null 2>&1; then
  echo "✅ Namespace already exists."
else
  echo "🆕 Creating namespace..."
  temporal operator namespace create \
    --namespace "$NAMESPACE_NAME" \
    --description "FF Indexer namespace" \
    --retention "$RETENTION_DAYS"
  echo "✅ Namespace '$NAMESPACE_NAME' created successfully!"
fi

echo "📋 Namespace details:"
temporal operator namespace describe "$NAMESPACE_NAME"

echo "🎉 Namespace '$NAMESPACE_NAME' ready!"

