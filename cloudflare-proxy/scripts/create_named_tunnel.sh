#!/usr/bin/env bash
set -euo pipefail

TUNNEL_NAME="${1:-beaconlab-planning-api}"

if ! command -v cloudflared >/dev/null 2>&1; then
  echo "cloudflared is not installed."
  exit 1
fi

echo "Creating named tunnel: ${TUNNEL_NAME}"
echo "If this fails with cert.pem error, run: cloudflared tunnel login"
cloudflared tunnel create "${TUNNEL_NAME}"

echo
echo "Done. Next:"
echo "1) cloudflared tunnel route dns ${TUNNEL_NAME} planning-api.<your-domain>"
echo "2) Copy cloudflared/config.example.yml to cloudflared/config.yml and fill values"
