-- Parameterized template for calculation workers.
-- Required params:
--   @planId STRING
--   @runId STRING

UPDATE `planning_app.plan_runs`
SET status = 'running', started_at = CURRENT_TIMESTAMP()
WHERE run_id = @runId;

CREATE TEMP TABLE latest_decisions AS
SELECT * EXCEPT(rn)
FROM (
  SELECT
    d.*,
    ROW_NUMBER() OVER (
      PARTITION BY d.plan_id, COALESCE(d.state, 'ALL'), COALESCE(d.channel, 'ALL'), d.decision_type
      ORDER BY d.created_at DESC
    ) rn
  FROM `planning_app.plan_decisions` d
  WHERE d.plan_id = @planId
)
WHERE rn = 1;

CREATE TEMP TABLE baseline AS
SELECT
  p.state,
  p.channel,
  SUM(p.spend) AS spend,
  SUM(p.conversions) AS conversions,
  SAFE_DIVIDE(SUM(p.spend), NULLIF(SUM(p.conversions), 0)) AS cpa
FROM `crblx-beacon-prod.planning_app.v_performance_state_channel` p
GROUP BY 1, 2;

CREATE TEMP TABLE exploration_lift AS
SELECT
  e.state,
  e.channel,
  e.price_adjustment_percent,
  SAFE_DIVIDE(e.conv_rate - b.base_conv_rate, NULLIF(b.base_conv_rate, 0)) AS conv_lift_pct
FROM `crblx-beacon-prod.planning_app.v_bid_exploration_curve` e
LEFT JOIN (
  SELECT
    state,
    channel,
    AVG(conv_rate) AS base_conv_rate
  FROM `crblx-beacon-prod.planning_app.v_bid_exploration_curve`
  WHERE price_adjustment_percent = 0
  GROUP BY 1, 2
) b
  ON b.state = e.state
 AND b.channel = e.channel;

CREATE TEMP TABLE simulated AS
SELECT
  b.state,
  b.channel,
  b.spend AS baseline_spend,
  b.conversions AS baseline_conversions,
  b.cpa AS baseline_cpa,
  b.spend * (1 + COALESCE(CAST(ld.decision_value AS FLOAT64), 0)) AS simulated_spend,
  b.conversions * (1 + COALESCE(e.conv_lift_pct, 0)) AS simulated_conversions
FROM baseline b
LEFT JOIN latest_decisions ld
  ON ld.state = b.state
 AND ld.channel = b.channel
 AND ld.decision_type = 'bid_adjustment'
LEFT JOIN exploration_lift e
  ON e.state = b.state
 AND e.channel = b.channel
 AND e.price_adjustment_percent = CAST(ROUND(COALESCE(CAST(ld.decision_value AS FLOAT64), 0) * 100) AS INT64);

INSERT INTO `planning_app.plan_results`
(run_id, plan_id, state, channel, metric_name, baseline_value, simulated_value, delta_value, delta_pct, created_at)
SELECT @runId, @planId, state, channel, 'spend',
       baseline_spend, simulated_spend,
       simulated_spend - baseline_spend,
       SAFE_DIVIDE(simulated_spend - baseline_spend, NULLIF(baseline_spend, 0)),
       CURRENT_TIMESTAMP()
FROM simulated
UNION ALL
SELECT @runId, @planId, state, channel, 'conversions',
       baseline_conversions, simulated_conversions,
       simulated_conversions - baseline_conversions,
       SAFE_DIVIDE(simulated_conversions - baseline_conversions, NULLIF(baseline_conversions, 0)),
       CURRENT_TIMESTAMP()
FROM simulated
UNION ALL
SELECT @runId, @planId, state, channel, 'cpa',
       baseline_cpa,
       SAFE_DIVIDE(simulated_spend, NULLIF(simulated_conversions, 0)),
       SAFE_DIVIDE(simulated_spend, NULLIF(simulated_conversions, 0)) - baseline_cpa,
       SAFE_DIVIDE(
         SAFE_DIVIDE(simulated_spend, NULLIF(simulated_conversions, 0)) - baseline_cpa,
         NULLIF(baseline_cpa, 0)
       ),
       CURRENT_TIMESTAMP()
FROM simulated;

UPDATE `planning_app.plan_runs`
SET status = 'success', finished_at = CURRENT_TIMESTAMP()
WHERE run_id = @runId;
