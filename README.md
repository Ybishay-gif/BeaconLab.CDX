# Planning App MVP (BigQuery-backed)

This repo contains a production-style backend scaffold for your planning web app:
- Access for selected users (allowlist in BigQuery `users` table)
- Plan creation and naming
- Planning parameter storage
- Decision capture (append-only)
- Run creation and results retrieval
- Minimal browser UI for rapid testing
- SQL templates for BigQuery schema and calculations

## 1) Prerequisites

- Node.js 20+
- Google Cloud project with BigQuery enabled
- Service account or local ADC credentials with BigQuery read/write permissions

## 2) Setup

```bash
cp .env.example .env
npm install
```

Set `.env`:

```env
PORT=8080
GOOGLE_CLOUD_PROJECT=your-project-id
BQ_DATASET=planning_app
# Optional: analytics reads can use a different dataset (defaults to BQ_DATASET)
# BQ_ANALYTICS_DATASET=planning_app
ADMIN_ACCESS_CODE=replace-with-strong-random-admin-code
# Optional raw source table override (with backticks)
# BQ_RAW_CROSS_TACTIC_TABLE=`crblx-beacon-prod.Custom_Reports.Cross Tactic Analysis Full Data `
```

## 3) Create BigQuery tables

Run `sql/schema.sql` in BigQuery SQL editor.
Run `sql/source_views.sql` to create normalized source views from your raw reporting table.

Then insert at least one allowed user:

```sql
INSERT INTO `planning_app.users` (user_id, email, role, is_active, created_at, updated_at)
VALUES ('u-admin-1', 'you@company.com', 'admin', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
```

## Environment Isolation (Prod vs Test)

Use separate writable datasets per environment:
- Production app objects: `BQ_DATASET=planning_app`
- Test app objects: `BQ_DATASET=planning_app_dev`

If you want analytics reads shared from production while keeping test objects isolated, set:

```env
BQ_DATASET=planning_app_dev
BQ_ANALYTICS_DATASET=planning_app
```

Clone mutable app objects from prod to test:

```bash
BQ_SYNC_SOURCE_DATASET=planning_app \
BQ_SYNC_TARGET_DATASET=planning_app_dev \
npx tsx scripts/bq/sync_mutable_tables.ts
```

## 4) Start API

```bash
npm run dev
```

Open:
- `http://localhost:8080` for the web UI
- `http://localhost:8080/health` for health check

## Stable Cloudflare Proxy (Recommended)

If you expose the app through Cloudflare, avoid `trycloudflare.com` quick tunnels (they expire and can return 530).

Use the stable setup in:
- `cloudflare-proxy/README.md`

That flow uses a named tunnel + fixed hostname + worker origin variables.

## Testing Site Policy

- All testing, QA, and automation must run on the testing site:
  - `https://planning-app-api-dev-758008223769.us-central1.run.app`
- Do not run tests on production endpoints:
  - `https://beaconlab.kissterralab.workers.dev/`
  - `https://planning-app-api-758008223769.us-central1.run.app`

## 5) Web app quick start

1. Open `http://localhost:8080`.
2. Login:
   - Admin access with the admin code, or
   - User login with email/password.
3. For first user login, enter email and create password.
4. In Settings > User Management (admin only), add users by email and reset passwords.
5. Create a plan, then copy/select its `plan_id`.
6. Add one parameter and one decision.
7. Trigger a run.

## 6) API quick test (optional)

Login as admin and extract `token`:

```bash
curl -X POST http://localhost:8080/api/auth/admin-login \
  -H "Content-Type: application/json" \
  -d '{"code":"<your-admin-access-code>"}'
```

Use token in `x-session-token`:

```bash
curl -H "x-session-token: <token>" http://localhost:8080/api/me
```

## Current auth model

MVP uses:
- Admin code login (`ADMIN_ACCESS_CODE`)
- User email + password (first login requires password setup)
- Session token passed in `x-session-token`

Google OAuth can be added next as an additional login method.

## Next build steps

1. Add real Google OAuth login flow and secure cookie session.
2. Add frontend (Next.js) pages for Plans/Plan Editor/Results.
3. Add async run worker that executes `sql/run_plan.sql` and updates `plan_runs`.
4. Add parameter guardrails and richer decision validation.

## Data source wiring

- Raw source table used for planning inputs:
  - `crblx-beacon-prod.Custom_Reports.Cross Tactic Analysis Full Data `
  - Important: this table name has a trailing space.
- To avoid fragile direct references, app SQL uses:
  - `planning_app.v_cross_tactic_raw`
  - `planning_app.v_performance_state_channel`
  - `planning_app.v_bid_exploration_curve`

## Analytics Consistency Rule (ROE/COR)

For all screens and endpoints that show ROE or COR:
- Always apply the active global filter scope (`activityLeadType`).
- Always use the matching QBC from configuration:
  - `clicks_*` -> `qbcClicks`
  - `leads_*` and `calls_*` -> `qbcLeadsCalls`
- Never compute ROE/COR with a hardcoded or missing QBC.

This rule applies to both SQL-backed backend calculations and any frontend-derived aggregations.

## Analytics Consistency Rule (Global Filters)

For all analytics calculations and KPIs (not only ROE/COR):
- Always apply active global filters (`activityLeadType`) consistently across screens.
- When comparing metrics between screens, ensure row scope is aligned (for example, segment-scoped vs non-segment-scoped channels).
