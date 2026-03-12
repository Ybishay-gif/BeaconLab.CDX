import { pgExec, pgClose } from "../src/db/postgres.js";

async function main() {
  const sql = `
CREATE TABLE IF NOT EXISTS pe_cache (
  cache_key TEXT NOT NULL,
  channel_group_name TEXT, state TEXT, testing_point DOUBLE PRECISION,
  opps DOUBLE PRECISION, bids DOUBLE PRECISION, win_rate DOUBLE PRECISION,
  sold DOUBLE PRECISION, binds DOUBLE PRECISION, quotes DOUBLE PRECISION,
  click_to_quote DOUBLE PRECISION, channel_quote DOUBLE PRECISION,
  click_to_channel_quote DOUBLE PRECISION, q2b DOUBLE PRECISION,
  channel_binds DOUBLE PRECISION, channel_q2b DOUBLE PRECISION,
  cpc DOUBLE PRECISION, avg_bid DOUBLE PRECISION,
  win_rate_uplift_state DOUBLE PRECISION, cpc_uplift_state DOUBLE PRECISION,
  win_rate_uplift_channel DOUBLE PRECISION, cpc_uplift_channel DOUBLE PRECISION,
  win_rate_uplift DOUBLE PRECISION, cpc_uplift DOUBLE PRECISION,
  additional_clicks DOUBLE PRECISION, expected_bind_change DOUBLE PRECISION,
  additional_budget_needed DOUBLE PRECISION, current_cpb DOUBLE PRECISION,
  expected_cpb DOUBLE PRECISION, cpb_uplift DOUBLE PRECISION,
  performance DOUBLE PRECISION, roe DOUBLE PRECISION,
  combined_ratio DOUBLE PRECISION, recommended_testing_point DOUBLE PRECISION,
  stat_sig TEXT, stat_sig_channel_group TEXT, stat_sig_source TEXT,
  cached_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pe_cache_key ON pe_cache(cache_key);

CREATE TABLE IF NOT EXISTS pm_cache (
  cache_key TEXT NOT NULL,
  start_date TEXT, end_date TEXT, channel_group_name TEXT,
  state TEXT, segment TEXT, price_adjustment_percent DOUBLE PRECISION,
  stat_sig TEXT, stat_sig_channel_group TEXT,
  cpc_uplift DOUBLE PRECISION, win_rate_uplift DOUBLE PRECISION,
  additional_clicks DOUBLE PRECISION, expected_total_clicks DOUBLE PRECISION,
  expected_cpc DOUBLE PRECISION, expected_total_cost DOUBLE PRECISION,
  expected_total_binds DOUBLE PRECISION, additional_expected_binds DOUBLE PRECISION,
  expected_cpb DOUBLE PRECISION, ss_performance DOUBLE PRECISION,
  expected_performance DOUBLE PRECISION, performance_uplift DOUBLE PRECISION,
  cached_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pm_cache_key ON pm_cache(cache_key);

CREATE TABLE IF NOT EXISTS query_cache (
  cache_key TEXT PRIMARY KEY,
  result_json JSONB NOT NULL,
  cached_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`;
  try {
    await pgExec(sql);
    console.log("Cache tables created successfully");
  } catch (err) {
    console.error("Failed:", err);
    process.exit(1);
  } finally {
    await pgClose();
  }
}

main();
