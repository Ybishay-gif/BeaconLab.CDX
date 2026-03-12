# CURRENT_STATE.md

## Snapshot (as of 2026-03-06)
- Project: `planning-app-mvp`
- Runtime: Node.js + TypeScript + Express
- Data backend: BigQuery dataset `planning_app`
- Test mutable dataset provisioned: `planning_app_dev` (cloned snapshot from `planning_app`)
- App dataset split support: writable objects use `BQ_DATASET`; analytics reads can be overridden via `BQ_ANALYTICS_DATASET`
- UI: Static app served from `public/` by `src/server.ts`
- API base path: `/api`
- Health endpoint: `/health`
- Local default port: `8080`
- API route structure: modularized under `src/routes/api/*` and composed via `src/routes/plans.ts`
- Shared backend activity-scope mapping lives in `src/services/shared/activityScope.ts` and is used across analytics/targets
- Shared backend ROE/COR SQL formula helpers live in `src/services/shared/kpiSql.ts` and are reused across analytics/targets
- Frontend formatting/date helpers are modularized in `public/modules/format.js` and imported by `public/main.js`
- Design system source of truth is `DESIGN_SYSTEM.md` and is required for all new UI work
- CI workflow exists at `.github/workflows/ci.yml` and runs install + type-check + build

## Implemented Capabilities
- Auth flows:
  - Admin code login
  - User password setup/login
  - Session token validation (`x-session-token`)
- Planning flows:
  - Create/list/get plans
  - Clone plan (copies plan parameters/settings + strategy config and decisions into a new draft plan)
  - Upsert plan parameters
  - Append decisions
  - Create/list run data and results retrieval
  - Plan-context defaults persisted per plan (`plan_context_config`) for:
    - performance date range
    - price exploration date range
    - `qbcClicks`
    - `qbcLeadsCalls`
  - Selected plan acts as master context for analytics date defaults
  - Targets default date range is fixed in UI logic to last 90 days (not configurable)
  - Plan Outcome tab under Plan:
    - grouped rows by Tier + recommended testing point (0% excluded)
    - grouped state/channel lists per outcome row
    - expected clicks/binds/CPC/CPB with uplift indicators
- Analytics endpoints:
  - State segment performance
  - Price exploration
  - Plan merged analysis
  - Strategy analysis
  - Plans comparison — two modes:
    - **Plans mode**: compare overall KPIs across all plans (each plan = one row). Uses each plan's own dates, activity type, and strategy. Not filtered by sidebar plan/activity selectors.
    - **Activity & Lead Type mode**: select a single plan, then compare its overall KPIs across all 6 activity/lead type combinations (each combo = one row). Not filtered by sidebar plan/activity selectors.
    - Columns: Plan Name (or Activity & Lead Type), Target COR, Bids, Sold, Total Spend, CPC, WR, Binds, Current CPB, Expected CPB, Q2B, Performance, ROE, COR, Additional Clicks, Additional Binds, WR Uplift, CPC Uplift, CPB Uplift, Expected Cost, Additional Budget
    - Date range picker independent of sidebar (own date selector)
    - Backend endpoint: `GET /api/analytics/plans-comparison` returns pre-computed rows for all plans or activity types
    - This is the only analytics screen that ignores the sidebar plan and activity/lead type filters
- Admin utilities:
  - User management
  - Targets APIs
  - Change log APIs

## Data Wiring Status
- Raw source table is referenced via normalized views.
- Primary source views expected from `sql/source_views.sql`:
  - `v_cross_tactic_raw`
  - `v_performance_state_channel`
  - `v_bid_exploration_curve`
- Additional analytics SQL assets exist under `sql/` and should be validated per environment.
- Targets performance can use pre-aggregated table `targets_perf_daily` when present; backend auto-detects and falls back to raw query if absent.
- Targets API supports plan scoping by `planId`; if `planning_app.targets.plan_id` exists, list/create/update can be isolated per selected plan.
- ROE/COR consistency policy is active:
  - calculations must follow global filter scope
  - calculations must use scope-appropriate QBC from selected plan context (`qbcClicks` or `qbcLeadsCalls`)
  - KPI endpoints that compute ROE/COR enforce `qbc` as a required query param (no silent backend fallback)
- Global-filter consistency policy is active for all analytics metrics:
  - all KPI/rollup calculations must respect `activityLeadType`
  - cross-screen comparisons require aligned segment/channel scope
- Table-design consistency policy is active:
  - analytics/data tables should keep the same shared layout and visual structure across screens
  - avoid one-off table style overrides that diverge from the standard table system
- UI delivery process policy is active:
  - prepare reuse plan before non-trivial UI edits
  - run browser validation for changed flows before handoff
  - do not hand off untested UI changes
  - keep `AGENTS.md`, `RUNBOOK.md`, `CURRENT_STATE.md`, and `DESIGN_SYSTEM.md` aligned for cross-chat context
- Settings screen policy is active:
  - keep Settings focused on global filters table + default targets file management
  - do not re-introduce date-default/QBC configuration controls under Settings
- Security baseline:
  - `ADMIN_ACCESS_CODE` is required at runtime (no default fallback)
  - optional source override is supported via `BQ_RAW_CROSS_TACTIC_TABLE`
- Environment isolation support:
  - mutable app tables can be cloned from prod to test via `scripts/bq/sync_mutable_tables.ts`
  - after clone, each environment writes to its own `BQ_DATASET`
  - analytics can remain shared by setting `BQ_ANALYTICS_DATASET` to prod dataset
  - mutating Playwright scripts refuse non-local URLs unless `PLAYWRIGHT_ALLOW_REMOTE_MUTATIONS=yes`

## Deployment Status Model
- Local dev: ready via `npm run dev`
- Stable external proxy option: available via `cloudflare-proxy/` using:
  - Named Cloudflared tunnel
  - Stable hostname
  - Cloudflare Worker reverse proxy
- Delivery policy in this repo:
  - Codex should deploy automatically after completing requested changes and passing checks.
  - Deploy confirmation should not be requested each turn unless user explicitly opts out.
- Current public Worker endpoint in use:
  - `https://beaconlab.kissterralab.workers.dev/`
  - This is the default deployment/validation URL for future chats in this repo.
  - Proxies to Cloud Run origin via `x-proxy-target` response header
  - Current prod Cloud Run revision: `planning-app-api-00091-jlz` (deployed 2026-03-06)
- Test Cloud Run service (isolated mutable dataset):
  - `https://planning-app-api-dev-758008223769.us-central1.run.app`
  - Runtime env points to:
    - `BQ_DATASET=planning_app_dev`
    - `BQ_ANALYTICS_DATASET=planning_app`
  - This is the required endpoint for all testing/QA/automation runs.
  - Production endpoint must not be used for tests.
  - Current test Cloud Run revision: `planning-app-api-dev-00013-qp8` (deployed 2026-03-06)

## Known Constraints / Risks
- Chat sessions are stateless; context must come from repo docs (`AGENTS.md`, `RUNBOOK.md`, this file).
- Stability risk if quick tunnels (`trycloudflare.com`) are used instead of named stable tunnel.
- Environment-specific values (domains, tunnel IDs, service account details) are not persisted here by default.
- Local vs deployed drift risk: UI updates in `public/` may not appear on Worker URL until Cloud Run service is redeployed.

## Recent Incident (2026-03-03)
- Issue: Price Exploration `Bids` column was added in repo code but not visible on Worker URL.
- Root cause: Worker pointed at Cloud Run service URL that was serving an older revision, so deployed HTML lagged local files.
- Resolution executed:
  1. Verified Worker target using `x-proxy-target` header.
  2. Deployed Cloud Run service `planning-app-api` from repo source.
  3. Confirmed updated table header on both Cloud Run URL and Worker URL.
- Preventive rule: when UI mismatch is reported, always verify active Worker origin and compare rendered HTML before assuming frontend cache issues.

## Recent Incident (2026-03-04)
- Issue: Worker returned `502 All origins failed` with `https://planning-api.example.com returned 530`.
- Root cause: `cloudflare-proxy/wrangler.jsonc` default placeholder origin was deployed to Worker.
- Resolution executed:
  1. Verified Cloud Run origin health at `https://planning-app-api-758008223769.us-central1.run.app/health`.
  2. Redeployed Worker `beaconlab` with `ORIGIN_BASE=https://planning-app-api-758008223769.us-central1.run.app`.
  3. Confirmed Worker health and proxy headers (`x-proxy-target`, `x-proxy-failover`) on `https://beaconlab.kissterralab.workers.dev/health`.

## Immediate Next Steps
1. Fill environment-specific deployment values:
- Stable hostname
- Worker name/domain
- Tunnel ID and credentials path
2. Verify all required BigQuery SQL objects exist in target project.
3. Add a lightweight release checklist (optional, can be appended to `RUNBOOK.md`).

## Update Protocol
- Update this file whenever architecture, deploy flow, environment shape, or production status changes.
- Keep the "Snapshot" date current on every meaningful operational change.
