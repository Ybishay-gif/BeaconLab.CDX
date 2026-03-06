# Playwright Browser Automation

This folder contains a reliable Playwright CLI automation for the documented web quick start flow:
1. Open app
2. Admin login (if not already logged in)
3. Create plan
4. Save parameter
5. Add decision
6. Trigger run
7. Save screenshot + JSON output

## Files

- `scripts/playwright/quickstart_flow.run-code.js`: Playwright flow logic executed via `pwcli run-code`
- `scripts/playwright/run_quickstart_flow.sh`: Runner script with env checks and artifact management

## Prerequisites

```bash
command -v npx >/dev/null 2>&1
```

From repo root:

```bash
export CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"
export PWCLI="${PWCLI:-$CODEX_HOME/skills/playwright/scripts/playwright_cli.sh}"
"$PWCLI" --help
```

App must be running at `BASE_URL` (default `http://localhost:8080`).

Safety default:
- Mutating flows refuse non-local `BASE_URL` unless `PLAYWRIGHT_ALLOW_REMOTE_MUTATIONS=yes` is explicitly set.
- This prevents accidental writes to production.

## Run

```bash
chmod +x scripts/playwright/run_quickstart_flow.sh
ADMIN_ACCESS_CODE='<your-admin-code>' \
BASE_URL='http://localhost:8080' \
scripts/playwright/run_quickstart_flow.sh
```

## Optional env vars

- `PLAYWRIGHT_CLI_SESSION`: custom session name (default `planning-quickstart`)
- `PLAN_NAME_PREFIX`: plan name prefix (default `PW Quickstart`)
- `PLAN_DESCRIPTION`: plan description text
- `PW_TIMEOUT_MS`: per-step timeout in ms (default `45000`)

## Output

Artifacts are written under:

```bash
output/playwright/quickstart-flow/<timestamp>/
```

- `result.json`: returned execution summary (`planId`, `runId`, statuses)
- `final.png`: final full-page screenshot
