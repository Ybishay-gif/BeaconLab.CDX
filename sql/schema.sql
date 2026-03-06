CREATE SCHEMA IF NOT EXISTS `planning_app`;

CREATE TABLE IF NOT EXISTS `planning_app.users` (
  user_id STRING NOT NULL,
  email STRING NOT NULL,
  role STRING NOT NULL,
  is_active BOOL NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `planning_app.user_credentials` (
  user_id STRING NOT NULL,
  email STRING NOT NULL,
  password_salt STRING,
  password_hash STRING,
  last_login_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `planning_app.auth_sessions` (
  session_token STRING NOT NULL,
  user_id STRING NOT NULL,
  email STRING NOT NULL,
  role STRING NOT NULL,
  expires_at TIMESTAMP NOT NULL,
  created_at TIMESTAMP NOT NULL,
  last_seen_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS `planning_app.plans` (
  plan_id STRING NOT NULL,
  plan_name STRING NOT NULL,
  description STRING,
  status STRING NOT NULL,
  created_by STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `planning_app.plan_parameters` (
  plan_id STRING NOT NULL,
  param_key STRING NOT NULL,
  param_value STRING NOT NULL,
  value_type STRING NOT NULL,
  updated_by STRING NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS `planning_app.plan_decisions` (
  decision_id STRING NOT NULL,
  plan_id STRING NOT NULL,
  decision_type STRING NOT NULL,
  state STRING,
  channel STRING,
  decision_value STRING NOT NULL,
  reason STRING,
  created_by STRING NOT NULL,
  created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS `planning_app.plan_runs` (
  run_id STRING NOT NULL,
  plan_id STRING NOT NULL,
  triggered_by STRING NOT NULL,
  status STRING NOT NULL,
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  error_message STRING,
  created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS `planning_app.plan_results` (
  run_id STRING NOT NULL,
  plan_id STRING NOT NULL,
  state STRING,
  channel STRING,
  metric_name STRING NOT NULL,
  baseline_value FLOAT64,
  simulated_value FLOAT64,
  delta_value FLOAT64,
  delta_pct FLOAT64,
  created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS `planning_app.targets` (
  target_id STRING NOT NULL,
  plan_id STRING,
  state STRING NOT NULL,
  segment STRING NOT NULL,
  source STRING NOT NULL,
  target_value FLOAT64 NOT NULL,
  created_by STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_by STRING NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS `planning_app.change_log` (
  change_id STRING NOT NULL,
  changed_at TIMESTAMP NOT NULL,
  changed_by_user_id STRING NOT NULL,
  changed_by_email STRING NOT NULL,
  object_type STRING NOT NULL,
  object_id STRING,
  action STRING NOT NULL,
  before_json STRING,
  after_json STRING,
  metadata_json STRING
);
