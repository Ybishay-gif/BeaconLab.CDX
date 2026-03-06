#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="${1:-cloudflared/config.yml}"

if ! command -v cloudflared >/dev/null 2>&1; then
  echo "cloudflared is not installed."
  exit 1
fi

if [[ ! -f "${CONFIG_PATH}" ]]; then
  echo "Missing config file: ${CONFIG_PATH}"
  echo "Create it from cloudflared/config.example.yml"
  exit 1
fi

echo "Starting named tunnel using ${CONFIG_PATH}"
cloudflared tunnel --config "${CONFIG_PATH}" run
