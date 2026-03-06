CREATE TABLE IF NOT EXISTS `crblx-beacon-prod.planning_app.targets_perf_daily` (
  event_date DATE,
  state STRING,
  segment STRING,
  source_key STRING,
  company_account_id STRING,
  activity_type STRING,
  lead_type STRING,
  sold FLOAT64,
  binds FLOAT64,
  scored_policies FLOAT64,
  price_sum FLOAT64,
  target_cpb_sum FLOAT64,
  lifetime_premium_sum FLOAT64,
  lifetime_cost_sum FLOAT64,
  avg_profit_sum FLOAT64,
  avg_equity_sum FLOAT64,
  refreshed_at TIMESTAMP
)
PARTITION BY event_date
CLUSTER BY state, segment, source_key, company_account_id;
