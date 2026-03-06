# RUNBOOK.md

## Overview
Operational guide for local development, BigQuery setup, and stable Cloudflare proxy deployment.

## Prerequisites
- Node.js 20+
- npm
- Google Cloud project with BigQuery enabled
- Credentials with BigQuery read/write access (service account or ADC)
- Optional for external access: `cloudflared`, Cloudflare account, `wrangler`

## Environment Setup
1. Install dependencies:
```bash
npm install
```
2. Create env file:
```bash
cp .env.example .env
```
3. Set required values in `.env`:
```env
PORT=8080
GOOGLE_CLOUD_PROJECT=<your-gcp-project>
BQ_DATASET=planning_app
# Optional analytics dataset override (defaults to BQ_DATASET)
# Use shared analytics dataset while isolating mutable app objects by environment.
# BQ_ANALYTICS_DATASET=planning_app
ADMIN_ACCESS_CODE=<admin-code>
# Optional raw source table override (include backticks)
# BQ_RAW_CROSS_TACTIC_TABLE=`crblx-beacon-prod.Custom_Reports.Cross Tactic Analysis Full Data `
```
`ADMIN_ACCESS_CODE` is required and has no runtime fallback default.

### Recommended Dataset Split (Prod vs Test)
- Use a separate `BQ_DATASET` per environment for all mutable app objects:
  - prod: `planning_app`
  - test/dev: `planning_app_dev`
- Optionally keep analytics reads shared from prod by setting:
```env
BQ_DATASET=planning_app_dev
BQ_ANALYTICS_DATASET=planning_app
```

Clone mutable app tables from prod to test (one-way snapshot clone):
```bash
BQ_SYNC_SOURCE_DATASET=planning_app \
BQ_SYNC_TARGET_DATASET=planning_app_dev \
npx tsx scripts/bq/sync_mutable_tables.ts
```

Dry-run clone statements:
```bash
BQ_SYNC_SOURCE_DATASET=planning_app \
BQ_SYNC_TARGET_DATASET=planning_app_dev \
BQ_SYNC_DRY_RUN=true \
npx tsx scripts/bq/sync_mutable_tables.ts
```

## BigQuery Bootstrap
Run in BigQuery SQL editor:
1. `sql/schema.sql`
2. `sql/source_views.sql`

If upgrading an existing environment, add plan scoping support for targets:
```sql
ALTER TABLE `planning_app.targets`
ADD COLUMN IF NOT EXISTS plan_id STRING;
```

Optional advanced analytics objects:
- `sql/view_state_segment_performance_daily.sql`
- `sql/view_price_exploration_daily.sql`
- `sql/table_price_exploration_daily.sql`
- `sql/table_fn_price_exploration.sql`
- `sql/table_fn_price_exploration_agg.sql`
- `sql/sp_refresh_price_exploration_daily.sql`
- `sql/scheduled_refresh_price_exploration.sql`
- `sql/table_fn_plan_merged_agg.sql`
- `sql/table_targets_perf_daily.sql`
- `sql/sp_refresh_targets_perf_daily.sql`
- `sql/scheduled_refresh_targets_perf_daily.sql`

Seed at least one admin-capable user in `planning_app.users`.

## Local Development
Start dev server:
```bash
npm run dev
```

URLs:
- App UI: `http://localhost:8080`
- Health: `http://localhost:8080/health`
- API base: `http://localhost:8080/api`

Type check:
```bash
npm run check
```

Production build/run:
```bash
npm run build
npm run start
```

Playwright safety:
- Mutating Playwright scripts now refuse non-local `BASE_URL` unless:
```bash
PLAYWRIGHT_ALLOW_REMOTE_MUTATIONS=yes
```
- Policy: all test/QA automation must target the testing site, not production.
- Hard rule: do not run any QA automation against production.

## Authentication Notes
- Admin login: `/api/auth/admin-login` using `ADMIN_ACCESS_CODE`
- User login: `/api/auth/user-login` (password flow)
- Session token expected in `x-session-token` header

## Analytics Guardrail: ROE/COR + QBC
- Every ROE/COR calculation must use the active global filter (`activityLeadType`) scope.
- Every ROE/COR calculation must use the relevant QBC from selected plan context (`plan_context_config`):
  - clicks scope -> `qbcClicks`
  - leads/calls scope -> `qbcLeadsCalls`
- If a new analytics endpoint/screen introduces ROE or COR, validate that `qbc` is passed from UI -> route -> service SQL.
- KPI endpoints that compute ROE/COR now require `qbc` explicitly; requests without `qbc` return `400`.

## Plan Context Guardrail
- Performance/price exploration defaults must come from selected plan context date ranges.
- Settings must not define fallback date-default controls for analytics/price exploration.
- Targets default load range is fixed to last 90 days in UI logic (not a configurable setting).

## Analytics Guardrail: Always Apply Global Filters
- Every analytics metric/KPI/rollup must respect active global filters (`activityLeadType`).
- Do not compare values across screens unless both screens are using equivalent row scope and filters.

## UI Guardrail: Table Design Consistency
- All app data tables must use the shared table design system in `public/styles.css` (`.table-wrap`, `table`, `th`, `td`, sticky headers).
- Avoid per-screen table width/spacing overrides that change the visual layout from other tables unless explicitly approved.
- New analytics tables should match existing table behavior and visual style before merge/deploy.
- All UI work must follow `DESIGN_SYSTEM.md`.

## UI Delivery Checklist (Required)
For every UI change, complete this before deploy:
1. Reuse map prepared:
- reference screen(s) identified
- reused classes/components listed
2. Build with shared patterns from `DESIGN_SYSTEM.md` (no one-off controls unless approved).
3. Run checks:
```bash
npm run check
```
4. Run browser validation on testing site for changed flow(s).
5. Confirm no layout break and control consistency.
6. Include test result summary in handoff response.

Do not hand off UI work without completing this checklist.

## Cloudflare Stable Proxy Deployment
Reference: `cloudflare-proxy/README.md`

Default worker for this repo (use this unless explicitly changed):
- `https://beaconlab.kissterralab.workers.dev/`
- Worker name: `beaconlab`
- Prod Cloud Run origin: `https://planning-app-api-758008223769.us-central1.run.app`
- Test Cloud Run service (isolated mutable dataset): `https://planning-app-api-dev-758008223769.us-central1.run.app`
- Testing site requirement:
  - Use `https://planning-app-api-dev-758008223769.us-central1.run.app` for all tests and automation.
  - Do not run tests on `https://beaconlab.kissterralab.workers.dev/` (production).

One-time steps:
1. Cloudflared auth:
```bash
cloudflared tunnel login
```
2. Create named tunnel:
```bash
cd cloudflare-proxy
./scripts/create_named_tunnel.sh beaconlab-planning-api
```
3. Route DNS:
```bash
cloudflared tunnel route dns beaconlab-planning-api planning-api.<your-domain>
```
4. Configure tunnel file:
- Copy `cloudflared/config.example.yml` to `cloudflared/config.yml`
- Fill `TUNNEL_ID`, `CREDENTIALS_FILE`, `STABLE_HOSTNAME`
5. Start tunnel:
```bash
./scripts/start_named_tunnel.sh cloudflared/config.yml
```
6. Deploy worker with stable origin:
```bash
./scripts/deploy_stable_worker.sh planning-api.<your-domain>
```
Optional backup origin:
```bash
./scripts/deploy_stable_worker.sh planning-api.<your-domain> backup-api.<your-domain>
```
7. Validate:
```bash
./scripts/check_proxy_health.sh planning-api.<your-domain> beaconlab.kissterralab.workers.dev
```

Important:
- Do not use `*.trycloudflare.com` as worker origin for stable environments.

Default execution policy for Codex sessions in this repo:
- After code changes are complete and `npm run check` passes, deploy automatically to Cloud Run (`planning-app-api`) so updates appear at:
  - `https://beaconlab.kissterralab.workers.dev/`
- Do not pause to ask for deploy confirmation unless the user explicitly requests to skip deployment.

## Troubleshooting
1. `530` through Cloudflare:
- Verify named tunnel is running.
- Verify worker origin points to stable hostname, not quick tunnel.
2. BigQuery query failures:
- Confirm `GOOGLE_CLOUD_PROJECT` and `BQ_DATASET`.
- Confirm credential identity has table/view read/write permissions.
3. Login/session issues:
- Confirm `ADMIN_ACCESS_CODE` matches local `.env`.
- Confirm `x-session-token` is passed for authenticated API calls.
4. Missing data in analytics endpoints:
- Confirm source views and optional analytics SQL objects were created.
5. Slow Targets loading:
- Create and refresh `targets_perf_daily` using the SQL files above.
- This enables pre-aggregated reads in `targetsService` instead of raw-table aggregation.
6. ROE/COR mismatch between screens:
- Confirm both screens use the same date range and same global filter.
- Confirm both endpoints receive the same `qbc` value for that filter scope.
- Confirm backend SQL applies `@qbc` in ROE/COR formulas (no default/hardcoded override).
7. KPI mismatch between screens:
- Confirm both screens apply the same global filter and same segment/channel scope.
8. UI change exists locally but not on `*.workers.dev`:
- Root cause pattern: local code is updated, but the Worker is proxying a Cloud Run origin that still serves an older revision.
- Check active origin behind Worker:
```bash
curl -sI https://beaconlab.kissterralab.workers.dev | rg -i "x-proxy-target|x-proxy-failover|date"
```
- Compare rendered markup directly between origin and Worker:
```bash
curl -s https://<origin-host>/ | rg -n "<table id=\"priceExplorationTable\">|Recommended TP|<th>Bids</th>"
curl -s https://beaconlab.kissterralab.workers.dev/ | rg -n "<table id=\"priceExplorationTable\">|Recommended TP|<th>Bids</th>"
```
- If origin is stale, redeploy Cloud Run from this repo root:
```bash
gcloud run deploy planning-app-api \
  --source . \
  --region us-central1 \
  --project crblx-beacon-prod \
  --platform managed \
  --allow-unauthenticated
```
- Re-verify Worker response after deploy and hard-refresh browser.

## Incident Note (2026-03-03)
- Symptom: `Bids` column added to Price Exploration was visible in local repo files but missing at `https://beaconlab.kissterralab.workers.dev/`.
- Confirmed cause: Worker proxied `https://planning-app-api-758008223769.us-central1.run.app`, which was serving an older revision.
- Resolution: deployed `planning-app-api` Cloud Run service from repo source; new revision served updated markup and Worker reflected change.

## Change Management
When setup/deploy behavior changes, update this file in the same PR/commit.
