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
