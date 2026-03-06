#!/usr/bin/env bash
set -euo pipefail

command -v npx >/dev/null 2>&1 || {
  echo "npx is required but was not found on PATH." >&2
  exit 1
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"
export PWCLI="${PWCLI:-$CODEX_HOME/skills/playwright/scripts/playwright_cli.sh}"

if [[ ! -x "$PWCLI" ]]; then
  echo "Playwright wrapper not found or not executable: $PWCLI" >&2
  exit 1
fi

BASE_URL="${BASE_URL:-http://localhost:8080}"
TS="$(date +%Y%m%d-%H%M%S)"
SESSION="${PLAYWRIGHT_CLI_SESSION:-qs-${RANDOM}}"
ARTIFACT_DIR="$ROOT_DIR/output/playwright/quickstart-flow/$TS"
RUN_CODE_FILE="$ROOT_DIR/scripts/playwright/quickstart_flow.run-code.js"
PW_TIMEOUT_MS="${PW_TIMEOUT_MS:-45000}"
PLAN_NAME_PREFIX="${PLAN_NAME_PREFIX:-PW Quickstart}"
PLAN_DESCRIPTION="${PLAN_DESCRIPTION:-Playwright automated quickstart flow}"
ADMIN_ACCESS_CODE="${ADMIN_ACCESS_CODE:-}"

if [[ ! "$BASE_URL" =~ ^https?://(localhost|127\.0\.0\.1)(:[0-9]+)?(/|$) ]] && [[ "${PLAYWRIGHT_ALLOW_REMOTE_MUTATIONS:-}" != "yes" ]]; then
  echo "Refusing to run mutating Playwright flow against non-local BASE_URL: $BASE_URL" >&2
  echo "Set PLAYWRIGHT_ALLOW_REMOTE_MUTATIONS=yes to override intentionally." >&2
  exit 1
fi

mkdir -p "$ARTIFACT_DIR"

export BASE_URL
export PW_ARTIFACT_DIR="$ARTIFACT_DIR"

echo "Running quickstart browser flow against: $BASE_URL"
echo "Session: $SESSION"
echo "Artifacts: $ARTIFACT_DIR"

"$PWCLI" --session "$SESSION" open "$BASE_URL"

RENDERED_RUN_CODE="$(node -e '
const fs = require("fs");
const tpl = fs.readFileSync(process.argv[1], "utf8");
const replacements = {
  "__BASE_URL__": JSON.stringify(process.env.BASE_URL || "http://localhost:8080"),
  "__ADMIN_ACCESS_CODE__": JSON.stringify(process.env.ADMIN_ACCESS_CODE || ""),
  "__PW_TIMEOUT_MS__": String(Number(process.env.PW_TIMEOUT_MS || 45000)),
  "__PW_ARTIFACT_DIR__": JSON.stringify(process.env.PW_ARTIFACT_DIR || "output/playwright/quickstart-flow"),
  "__PLAN_NAME_PREFIX__": JSON.stringify(process.env.PLAN_NAME_PREFIX || "PW Quickstart"),
  "__PLAN_DESCRIPTION__": JSON.stringify(process.env.PLAN_DESCRIPTION || "Playwright automated quickstart flow")
};
let out = tpl;
for (const [token, value] of Object.entries(replacements)) {
  out = out.split(token).join(value);
}
process.stdout.write(out);
' "$RUN_CODE_FILE")"

RUN_RESULT="$("$PWCLI" --session "$SESSION" run-code "$RENDERED_RUN_CODE")"
printf '%s\n' "$RUN_RESULT" | tee "$ARTIFACT_DIR/result.json"

if printf '%s\n' "$RUN_RESULT" | rg -q "^### Error"; then
  echo "Playwright flow failed. See: $ARTIFACT_DIR/result.json" >&2
  "$PWCLI" --session "$SESSION" close || true
  exit 1
fi

"$PWCLI" --session "$SESSION" close || true

echo "Done. Result JSON: $ARTIFACT_DIR/result.json"
echo "Done. Screenshot: $ARTIFACT_DIR/final.png"
