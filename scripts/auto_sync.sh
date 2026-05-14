#!/usr/bin/env bash
set -euo pipefail

INTERVAL="${1:-600}"

if ! [[ "$INTERVAL" =~ ^[0-9]+$ ]] || [[ "$INTERVAL" -lt 30 ]]; then
  echo "[error] interval must be an integer >= 30 seconds"
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "[auto-sync] start (interval=${INTERVAL}s)"

while true; do
  TS="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "[$TS] collect start"

  if python scripts/collect.py; then
    git add docs/
    if git diff --cached --quiet; then
      echo "[$TS] no changes"
    else
      git commit -m "Update KRX supply data (local auto-sync)"
      git pull --rebase --autostash origin main
      git push
      echo "[$TS] pushed"
    fi
  else
    echo "[$TS] collect failed"
  fi

  sleep "$INTERVAL"
done
