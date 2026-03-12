CREATE TABLE IF NOT EXISTS `crblx-beacon-prod.planning_app.state_segment_daily` (
  event_date DATE,
  state STRING,
  segment STRING,
  channel_group_name STRING,
  activity_type STRING,
  lead_type STRING,
  bids FLOAT64,
  sold FLOAT64,
  total_cost FLOAT64,
  quote_started FLOAT64,
  quotes FLOAT64,
  binds FLOAT64,
  scored_policies FLOAT64,
  target_cpb_sum FLOAT64,
  lifetime_premium_sum FLOAT64,
  lifetime_cost_sum FLOAT64,
  avg_profit_sum FLOAT64,
  avg_equity_sum FLOAT64,
  avg_mrltv_sum FLOAT64,
  refreshed_at TIMESTAMP
)
PARTITION BY event_date
CLUSTER BY state, segment, channel_group_name;
