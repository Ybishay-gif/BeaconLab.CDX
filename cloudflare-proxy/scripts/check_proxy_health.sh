#!/usr/bin/env bash
set -euo pipefail

PRIMARY_HOST="${1:-}"
WORKER_HOST="${2:-}"

if [[ -z "${PRIMARY_HOST}" || -z "${WORKER_HOST}" ]]; then
  echo "Usage: $0 <stable-origin-hostname> <worker-hostname>"
  echo "Example: $0 planning-api.example.com beaconlab.example.workers.dev"
  exit 1
fi

echo "Checking local app..."
curl -fsS "http://localhost:8080/health" >/dev/null
echo "  local: OK"

echo "Checking stable origin..."
curl -fsS "https://${PRIMARY_HOST}/health" >/dev/null
echo "  stable origin: OK"

echo "Checking worker origin routing..."
HEADERS="$(curl -fsS -D - -o /dev/null "https://${WORKER_HOST}/health")"
echo "${HEADERS}" | rg -q "x-proxy-target: https://${PRIMARY_HOST}" || {
  echo "Worker did not route to expected origin host: ${PRIMARY_HOST}"
  echo "${HEADERS}"
  exit 1
}
echo "  worker: OK"

echo "All health checks passed."
