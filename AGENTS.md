# AGENTS.md

## Purpose
This repository is a BigQuery-backed planning app MVP with:
- Express + TypeScript API
- Static web UI served by the same Node server
- BigQuery storage and analytics queries
- Optional Cloudflare Worker + named tunnel proxy for stable external access

## First-Read Rule For New Chats
At the start of every new task in this repo, read these files first:
1. `RUNBOOK.md`
2. `CURRENT_STATE.md`

Do not assume prior chat history exists. Use those files as the source of truth for setup, deployment, and current status.

## Working Agreement
- Keep changes scoped and production-safe.
- UI consistency policy:
  - `DESIGN_SYSTEM.md` is mandatory for all UI changes.
  - New pages and controls must reuse existing button/table/layout patterns first.
  - Do not introduce one-off visual variants unless explicitly approved and documented in `DESIGN_SYSTEM.md`.
- UI implementation process:
  - For non-trivial UI requests, provide a short reuse/implementation plan before editing.
  - Identify reference screen(s) and exact classes/components to reuse.
  - Execute UI QA (browser validation) before handoff; do not hand off untested UI changes.
- Testing policy:
  - All QA, Playwright runs, training/video captures, and non-production validation must run on the testing site only.
  - Do not run tests against production.
- Prefer existing scripts and documented commands over ad hoc commands.
- Deployment default for this repo:
  - After implementing requested changes and passing checks, deploy automatically.
  - Do not ask for deploy confirmation each turn.
  - Exception: skip deploy only when the user explicitly says not to deploy.
- Do not change deployment topology without updating:
  - `RUNBOOK.md` (process)
  - `CURRENT_STATE.md` (status and impact)
- For process/quality changes that must carry across chats, update all shared context docs:
  - `AGENTS.md`
  - `RUNBOOK.md`
  - `CURRENT_STATE.md`
  - `DESIGN_SYSTEM.md` (for UI conventions)
- If adding new environment variables, update:
  - `.env.example`
  - `RUNBOOK.md` env section
- For analytics changes touching ROE/COR:
  - enforce global filter scope in calculations
  - enforce correct QBC source (`qbcClicks` for clicks, `qbcLeadsCalls` for leads/calls)
  - verify parity across screens/endpoints for same filters and dates
- For all analytics changes:
  - enforce active global filters (`activityLeadType`) in all calculations and KPI rollups
  - ensure row scope parity (segment/channel inclusion) before claiming cross-screen equality

## Key Paths
- API entrypoint: `src/server.ts`
- API routes: `src/routes/plans.ts`, `src/routes/health.ts`
- BigQuery logic: `src/services/*.ts`, `src/db/bigquery.ts`
- SQL schema/views/functions: `sql/`
- Cloudflare proxy setup: `cloudflare-proxy/`

## Core Commands
```bash
npm install
npm run dev
npm run check
npm run build
npm run start
```

## Deploy/Proxy Reference
For stable Cloudflare exposure, use:
- `cloudflare-proxy/README.md`
- `RUNBOOK.md` section "Cloudflare Stable Proxy Deployment"
