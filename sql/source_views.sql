-- Canonical source views for planning calculations.
-- NOTE: Raw table name includes a trailing space: `Cross Tactic Analysis Full Data `

CREATE OR REPLACE VIEW `crblx-beacon-prod.planning_app.v_cross_tactic_raw` AS
SELECT
  DATE(COALESCE(createdate_utc, Data_DateCreated, DateCreated)) AS event_date,
  Data_State AS state,
  ChannelGroupName AS channel,
  SAFE_CAST(PriceAdjustmentPercent AS INT64) AS price_adjustment_percent,
  SAFE_CAST(bid_price AS FLOAT64) AS bid_price,
  SAFE_CAST(Final_Price AS FLOAT64) AS final_price,
  SAFE_CAST(CostBid AS FLOAT64) AS cost_bid,
  SAFE_CAST(bid_count AS INT64) AS bid_count,
  SAFE_CAST(TransactionSold AS INT64) AS transaction_sold,
  SAFE_CAST(Transaction_sold AS INT64) AS transaction_sold_alt,
  SAFE_CAST(TotalBinds AS INT64) AS total_binds,
  SAFE_CAST(NumberOfPosts AS INT64) AS number_of_posts,
  BidResultStatus AS bid_result_status,
  Click_Post_ID AS click_post_id,
  Lead_LeadID AS lead_id
FROM `crblx-beacon-prod.Custom_Reports.Cross Tactic Analysis Full Data `
WHERE Data_State IS NOT NULL
  AND ChannelGroupName IS NOT NULL;

CREATE OR REPLACE VIEW `crblx-beacon-prod.planning_app.v_performance_state_channel` AS
SELECT
  event_date,
  state,
  channel,
  COUNT(*) AS records,
  SUM(COALESCE(NULLIF(cost_bid, 0), NULLIF(final_price, 0), NULLIF(bid_price, 0), 0)) AS spend,
  SUM(COALESCE(bid_count, 0)) AS bids,
  SUM(CASE WHEN click_post_id IS NOT NULL THEN 1 ELSE 0 END) AS clicks,
  SUM(COALESCE(total_binds, COALESCE(transaction_sold, transaction_sold_alt, 0))) AS conversions,
  SAFE_DIVIDE(SUM(CASE WHEN click_post_id IS NOT NULL THEN 1 ELSE 0 END), COUNT(*)) AS ctr,
  SAFE_DIVIDE(SUM(COALESCE(total_binds, COALESCE(transaction_sold, transaction_sold_alt, 0))), COUNT(*)) AS conv_rate
FROM `crblx-beacon-prod.planning_app.v_cross_tactic_raw`
GROUP BY 1, 2, 3;

CREATE OR REPLACE VIEW `crblx-beacon-prod.planning_app.v_bid_exploration_curve` AS
SELECT
  event_date,
  state,
  channel,
  price_adjustment_percent,
  COUNT(*) AS records,
  AVG(COALESCE(bid_price, 0)) AS avg_bid_price,
  AVG(COALESCE(final_price, 0)) AS avg_final_price,
  SAFE_DIVIDE(
    SUM(
      CASE
        WHEN REGEXP_CONTAINS(LOWER(COALESCE(bid_result_status, '')), r'win')
          OR LOWER(COALESCE(bid_result_status, '')) IN ('sold', 'pre_sold')
        THEN 1
        ELSE 0
      END
    ),
    COUNT(*)
  ) AS win_rate,
  SAFE_DIVIDE(SUM(CASE WHEN click_post_id IS NOT NULL THEN 1 ELSE 0 END), COUNT(*)) AS ctr,
  SAFE_DIVIDE(
    SUM(COALESCE(total_binds, COALESCE(transaction_sold, transaction_sold_alt, 0))),
    COUNT(*)
  ) AS conv_rate
FROM `crblx-beacon-prod.planning_app.v_cross_tactic_raw`
WHERE price_adjustment_percent IS NOT NULL
GROUP BY 1, 2, 3, 4;
