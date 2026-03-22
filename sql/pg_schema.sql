-- PostgreSQL schema for Beacon Lab
-- Mirrors BQ planning_app dataset

-- Utility function: BQ-compatible SAFE_DIVIDE (returns NULL when divisor is 0)
CREATE OR REPLACE FUNCTION safe_divide(a double precision, b double precision)
RETURNS double precision LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN b = 0 OR b IS NULL THEN NULL ELSE a / b END;
$$;

-- Config tables
CREATE TABLE IF NOT EXISTS users (
  user_id TEXT PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  name TEXT,
  role TEXT NOT NULL DEFAULT 'planner',
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ
);
-- Idempotent migration: add name column to existing tables
ALTER TABLE users ADD COLUMN IF NOT EXISTS name TEXT;

CREATE TABLE IF NOT EXISTS user_credentials (
  user_id TEXT PRIMARY KEY REFERENCES users(user_id),
  email TEXT NOT NULL,
  password_salt TEXT,
  password_hash TEXT,
  last_login_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS auth_sessions (
  session_token TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  email TEXT NOT NULL,
  role TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_sessions_user ON auth_sessions(user_id);
CREATE INDEX idx_sessions_expires ON auth_sessions(expires_at);

CREATE TABLE IF NOT EXISTS plans (
  plan_id TEXT PRIMARY KEY,
  plan_name TEXT NOT NULL,
  description TEXT,
  status TEXT NOT NULL DEFAULT 'draft',
  created_by TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS plan_parameters (
  plan_id TEXT NOT NULL,
  param_key TEXT NOT NULL,
  param_value TEXT NOT NULL,
  value_type TEXT NOT NULL DEFAULT 'string',
  updated_by TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (plan_id, param_key)
);

CREATE TABLE IF NOT EXISTS plan_decisions (
  decision_id TEXT PRIMARY KEY,
  plan_id TEXT NOT NULL,
  decision_type TEXT NOT NULL,
  state TEXT,
  channel TEXT,
  decision_value TEXT NOT NULL,
  reason TEXT,
  created_by TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_decisions_plan ON plan_decisions(plan_id);

CREATE TABLE IF NOT EXISTS plan_runs (
  run_id TEXT PRIMARY KEY,
  plan_id TEXT NOT NULL,
  triggered_by TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_runs_plan ON plan_runs(plan_id);

CREATE TABLE IF NOT EXISTS plan_results (
  run_id TEXT NOT NULL,
  plan_id TEXT NOT NULL,
  state TEXT,
  channel TEXT,
  metric_name TEXT NOT NULL,
  baseline_value DOUBLE PRECISION,
  simulated_value DOUBLE PRECISION,
  delta_value DOUBLE PRECISION,
  delta_pct DOUBLE PRECISION,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_results_run ON plan_results(run_id, plan_id);

CREATE TABLE IF NOT EXISTS targets (
  target_id TEXT PRIMARY KEY,
  plan_id TEXT,
  state TEXT NOT NULL,
  segment TEXT NOT NULL,
  source TEXT NOT NULL,
  target_value DOUBLE PRECISION NOT NULL,
  target_cor DOUBLE PRECISION NOT NULL DEFAULT 0,
  created_by TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_by TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- Idempotent migration: add target_cor to existing tables
ALTER TABLE targets ADD COLUMN IF NOT EXISTS target_cor DOUBLE PRECISION NOT NULL DEFAULT 0;
-- Idempotent migration: scope targets by activity_lead_type
ALTER TABLE targets ADD COLUMN IF NOT EXISTS activity_lead_type TEXT NOT NULL DEFAULT '';
CREATE INDEX idx_targets_plan ON targets(plan_id);
CREATE INDEX idx_targets_state_seg ON targets(state, segment);
CREATE INDEX IF NOT EXISTS idx_targets_plan_alt ON targets(plan_id, activity_lead_type);

-- Roles & Permissions
CREATE TABLE IF NOT EXISTS roles (
  role_id    TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  name       TEXT NOT NULL UNIQUE,
  is_system  BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS role_permissions (
  role_id        TEXT NOT NULL REFERENCES roles(role_id) ON DELETE CASCADE,
  permission_key TEXT NOT NULL,
  PRIMARY KEY (role_id, permission_key)
);
CREATE INDEX IF NOT EXISTS idx_role_permissions_key ON role_permissions(permission_key);

-- Add role_id FK to users
ALTER TABLE users ADD COLUMN IF NOT EXISTS role_id TEXT REFERENCES roles(role_id);
-- Add role_id to auth_sessions
ALTER TABLE auth_sessions ADD COLUMN IF NOT EXISTS role_id TEXT;

-- Module access per user (join table)
CREATE TABLE IF NOT EXISTS user_modules (
  user_id   TEXT NOT NULL REFERENCES users(user_id),
  module_id TEXT NOT NULL,
  granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (user_id, module_id)
);

CREATE TABLE IF NOT EXISTS change_log (
  change_id TEXT PRIMARY KEY,
  changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  changed_by_user_id TEXT NOT NULL,
  changed_by_email TEXT NOT NULL,
  object_type TEXT NOT NULL,
  object_id TEXT,
  action TEXT NOT NULL,
  before_json TEXT,
  after_json TEXT,
  metadata_json TEXT,
  module TEXT NOT NULL DEFAULT 'planning'
);
CREATE INDEX idx_changelog_object ON change_log(object_type, object_id);

-- Analytics tables (populated daily from BQ)
CREATE TABLE IF NOT EXISTS state_segment_daily (
  event_date DATE NOT NULL,
  state TEXT,
  segment TEXT,
  channel_group_name TEXT,
  activity_type TEXT,
  lead_type TEXT,
  bids DOUBLE PRECISION,
  sold DOUBLE PRECISION,
  total_cost DOUBLE PRECISION,
  quote_started DOUBLE PRECISION,
  quotes DOUBLE PRECISION,
  binds DOUBLE PRECISION,
  scored_policies DOUBLE PRECISION,
  target_cpb_sum DOUBLE PRECISION,
  lifetime_premium_sum DOUBLE PRECISION,
  lifetime_cost_sum DOUBLE PRECISION,
  avg_profit_sum DOUBLE PRECISION,
  avg_equity_sum DOUBLE PRECISION,
  avg_mrltv_sum DOUBLE PRECISION,
  refreshed_at TIMESTAMPTZ
);
CREATE INDEX idx_ssd_date_activity ON state_segment_daily(event_date, activity_type, lead_type);
CREATE INDEX idx_ssd_state_seg ON state_segment_daily(state, segment);

CREATE TABLE IF NOT EXISTS price_exploration_daily (
  date DATE NOT NULL,
  channel_group_name TEXT,
  state TEXT,
  activity_type TEXT DEFAULT '',
  lead_type TEXT DEFAULT '',
  price_adjustment_percent INTEGER,
  opps BIGINT,
  bids DOUBLE PRECISION,
  total_impressions DOUBLE PRECISION,
  avg_position DOUBLE PRECISION,
  sold DOUBLE PRECISION,
  win_rate DOUBLE PRECISION,
  avg_bid DOUBLE PRECISION,
  cpc DOUBLE PRECISION,
  total_spend DOUBLE PRECISION,
  click_to_quote DOUBLE PRECISION,
  quote_start_rate DOUBLE PRECISION,
  number_of_quote_started DOUBLE PRECISION,
  number_of_quotes DOUBLE PRECISION,
  number_of_binds DOUBLE PRECISION,
  stat_sig TEXT,
  stat_sig_channel_group TEXT,
  cpc_uplift DOUBLE PRECISION,
  cpc_uplift_channelgroup DOUBLE PRECISION,
  win_rate_uplift DOUBLE PRECISION,
  win_rate_uplift_channelgroup DOUBLE PRECISION,
  additional_clicks DOUBLE PRECISION,
  refreshed_at TIMESTAMPTZ
);
CREATE INDEX idx_ped_date ON price_exploration_daily(date);
CREATE INDEX idx_ped_state_channel ON price_exploration_daily(state, channel_group_name);
CREATE INDEX idx_ped_activity ON price_exploration_daily(activity_type, lead_type);
-- Idempotent migration: add activity/lead type to price_exploration_daily
ALTER TABLE price_exploration_daily ADD COLUMN IF NOT EXISTS activity_type TEXT DEFAULT '';
ALTER TABLE price_exploration_daily ADD COLUMN IF NOT EXISTS lead_type TEXT DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_ped_activity ON price_exploration_daily(activity_type, lead_type);

CREATE TABLE IF NOT EXISTS targets_perf_daily (
  event_date DATE NOT NULL,
  state TEXT,
  segment TEXT,
  source_key TEXT,
  company_account_id TEXT,
  activity_type TEXT,
  lead_type TEXT,
  sold DOUBLE PRECISION,
  binds DOUBLE PRECISION,
  scored_policies DOUBLE PRECISION,
  price_sum DOUBLE PRECISION,
  target_cpb_sum DOUBLE PRECISION,
  lifetime_premium_sum DOUBLE PRECISION,
  lifetime_cost_sum DOUBLE PRECISION,
  avg_profit_sum DOUBLE PRECISION,
  avg_equity_sum DOUBLE PRECISION,
  refreshed_at TIMESTAMPTZ
);
CREATE INDEX idx_tpd_date_activity ON targets_perf_daily(event_date, activity_type, lead_type);
CREATE INDEX idx_tpd_state_seg ON targets_perf_daily(state, segment);

-- Tickets (bug reports & feature requests)
CREATE TABLE IF NOT EXISTS tickets (
  ticket_id        TEXT PRIMARY KEY,
  ticket_number    SERIAL,
  type             TEXT NOT NULL CHECK (type IN ('bug', 'feature')),
  status           TEXT NOT NULL DEFAULT 'todo' CHECK (status IN ('todo', 'in_progress', 'done')),
  title            TEXT NOT NULL,
  description      TEXT NOT NULL DEFAULT '',
  module           TEXT NOT NULL,
  page             TEXT NOT NULL,
  attachments      JSONB DEFAULT '[]',
  created_by       TEXT NOT NULL,
  created_by_email TEXT NOT NULL,
  assigned_to      TEXT,
  resolved_at      TIMESTAMPTZ,
  resolution_notes TEXT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_tickets_status ON tickets (status, created_at DESC);
CREATE INDEX idx_tickets_created_by ON tickets (created_by);
CREATE INDEX idx_tickets_type ON tickets (type);
CREATE INDEX idx_tickets_module ON tickets (module);

-- Reports (custom report generator)
CREATE TABLE IF NOT EXISTS reports (
  report_id        TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  report_name      TEXT NOT NULL,
  user_id          TEXT NOT NULL,
  date_start       DATE NOT NULL,
  date_end         DATE NOT NULL,
  fixed_filters    JSONB NOT NULL DEFAULT '{}',
  dynamic_filters  JSONB NOT NULL DEFAULT '[]',
  selected_columns JSONB NOT NULL DEFAULT '[]',
  status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending','processing','done','error')),
  file_url         TEXT,
  row_count        INTEGER,
  error_message    TEXT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at     TIMESTAMPTZ
);
CREATE INDEX idx_reports_user ON reports(user_id, created_at DESC);
CREATE INDEX idx_reports_status ON reports(status);

-- ── Report Templates ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS report_templates (
  template_id      TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  template_name    TEXT NOT NULL,
  user_id          TEXT NOT NULL,
  fixed_filters    JSONB NOT NULL DEFAULT '{}',
  dynamic_filters  JSONB NOT NULL DEFAULT '[]',
  selected_columns JSONB NOT NULL DEFAULT '[]',
  include_opps     BOOLEAN NOT NULL DEFAULT false,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_report_templates_user ON report_templates(user_id, created_at DESC);

-- ── SFTP Connections (org-wide) ────────────────────────────────────
CREATE TABLE IF NOT EXISTS sftp_connections (
  connection_id      TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  name               TEXT NOT NULL,
  host               TEXT NOT NULL,
  port               INTEGER NOT NULL DEFAULT 22,
  username           TEXT NOT NULL,
  password_encrypted TEXT NOT NULL,
  remote_path        TEXT NOT NULL DEFAULT '/',
  is_active          BOOLEAN NOT NULL DEFAULT TRUE,
  created_by         TEXT NOT NULL,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_sftp_connections_active ON sftp_connections(is_active);

-- ── SFTP Upload Log ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sftp_uploads (
  upload_id      TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  report_id      TEXT NOT NULL REFERENCES reports(report_id) ON DELETE CASCADE,
  connection_id  TEXT NOT NULL REFERENCES sftp_connections(connection_id) ON DELETE CASCADE,
  status         TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending','uploading','done','error')),
  remote_file    TEXT,
  error_message  TEXT,
  initiated_by   TEXT NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at   TIMESTAMPTZ
);
CREATE INDEX idx_sftp_uploads_report ON sftp_uploads(report_id);
CREATE INDEX idx_sftp_uploads_status ON sftp_uploads(status);

-- ── Column Presets (reusable column sets for reports/exports) ─────
CREATE TABLE IF NOT EXISTS column_presets (
  preset_id   TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  preset_name TEXT NOT NULL,
  user_id     TEXT NOT NULL,
  columns     JSONB NOT NULL DEFAULT '[]',
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_column_presets_user ON column_presets(user_id, created_at DESC);

-- ── Tech Vendors (cost management vendor pricing config) ─────
CREATE TABLE IF NOT EXISTS tech_vendors (
  vendor_id      TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  vendor_name    TEXT NOT NULL,
  pricing_model  TEXT NOT NULL,
  pricing_value  DOUBLE PRECISION NOT NULL,
  pricing_column TEXT,
  is_active      BOOLEAN NOT NULL DEFAULT TRUE,
  created_by     TEXT NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Budget Management ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS budgets (
  budget_id      TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  year           INTEGER NOT NULL,
  month          INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
  activity_type  TEXT NOT NULL,
  lead_type      TEXT NOT NULL,
  amount         DOUBLE PRECISION NOT NULL DEFAULT 0,
  created_by     TEXT NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_by     TEXT NOT NULL,
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (year, month, activity_type, lead_type)
);
CREATE INDEX idx_budgets_period ON budgets(year, month);

CREATE TABLE IF NOT EXISTS budget_allocations (
  allocation_id   TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  budget_id       TEXT NOT NULL REFERENCES budgets(budget_id) ON DELETE CASCADE,
  account_name    TEXT NOT NULL,
  allocation_pct  DOUBLE PRECISION NOT NULL DEFAULT 0
                    CHECK (allocation_pct >= 0 AND allocation_pct <= 100),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (budget_id, account_name)
);
CREATE INDEX idx_alloc_budget ON budget_allocations(budget_id);

-- ── Platform Health ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS sync_history (
  sync_id      TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  started_at   TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ,
  ok           BOOLEAN NOT NULL DEFAULT FALSE,
  total_ms     INTEGER,
  total_rows   BIGINT DEFAULT 0,
  error        TEXT,
  tables_json  JSONB NOT NULL DEFAULT '[]',
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sync_history_started ON sync_history(started_at DESC);

CREATE TABLE IF NOT EXISTS security_test_results (
  result_id      TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  test_type      TEXT NOT NULL CHECK (test_type IN ('auth-security', 'pentest')),
  ran_at         TIMESTAMPTZ NOT NULL,
  environment    TEXT NOT NULL DEFAULT 'production',
  target_url     TEXT,
  passed         INTEGER NOT NULL DEFAULT 0,
  failed         INTEGER NOT NULL DEFAULT 0,
  critical_fails INTEGER NOT NULL DEFAULT 0,
  high_fails     INTEGER NOT NULL DEFAULT 0,
  medium_fails   INTEGER NOT NULL DEFAULT 0,
  low_fails      INTEGER NOT NULL DEFAULT 0,
  findings_json  JSONB NOT NULL DEFAULT '[]',
  passed_checks  JSONB NOT NULL DEFAULT '[]',
  status         TEXT NOT NULL DEFAULT 'no_errors'
                   CHECK (status IN ('no_errors', 'critical_errors', 'minor_errors')),
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_security_tests_ran ON security_test_results(ran_at DESC);
