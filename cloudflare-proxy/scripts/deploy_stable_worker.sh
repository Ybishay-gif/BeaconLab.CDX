#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PRIMARY_HOST="${1:-}"
FALLBACK_HOST="${2:-}"

if [[ -z "${PRIMARY_HOST}" ]]; then
  echo "Usage: $0 <primary-hostname> [fallback-hostname]"
  echo "Example: $0 planning-api.example.com backup-api.example.com"
  exit 1
fi

PRIMARY_URL="https://${PRIMARY_HOST}"
FALLBACK_URL=""
if [[ -n "${FALLBACK_HOST}" ]]; then
  FALLBACK_URL="https://${FALLBACK_HOST}"
fi

echo "Deploying worker with:"
echo "  ORIGIN_BASE=${PRIMARY_URL}"
echo "  ORIGIN_BASE_FALLBACK=${FALLBACK_URL:-<empty>}"

cd "${ROOT_DIR}"
npx wrangler deploy \
  --var "ORIGIN_BASE:${PRIMARY_URL}" \
  --var "ORIGIN_BASE_FALLBACK:${FALLBACK_URL}"
