CREATE OR REPLACE VIEW `crblx-beacon-prod.planning_app.v_price_exploration_daily` AS
WITH base AS (
  SELECT
    DATE(COALESCE(createdate_utc, Data_DateCreated, DateCreated)) AS event_date,
    ChannelGroupName AS channel_group_name,
    Data_State AS state,
    CASE
      WHEN LOWER(COALESCE(activitytype, '')) LIKE 'click%' THEN 'clicks'
      WHEN LOWER(COALESCE(activitytype, '')) LIKE 'lead%' THEN 'leads'
      WHEN LOWER(COALESCE(activitytype, '')) LIKE 'call%' THEN 'calls'
      ELSE ''
    END AS activity_type,
    CASE
      WHEN LOWER(COALESCE(Leadtype, '')) LIKE '%car%' THEN 'auto'
      WHEN LOWER(COALESCE(Leadtype, '')) LIKE '%home%' THEN 'home'
      ELSE ''
    END AS lead_type,
    SAFE_CAST(PriceAdjustmentPercent AS INT64) AS price_adjustment_percent,
    Lead_LeadID,
    SAFE_CAST(bid_count AS FLOAT64) AS bid_count,
    SAFE_CAST(ExtraBidData_ReturnedAdsCount AS FLOAT64) AS returned_ads_count,
    SAFE_CAST(ExtraBidData_OriginalAdData_Position AS FLOAT64) AS ad_position,
    SAFE_CAST(Transaction_sold AS FLOAT64) AS transaction_sold,
    SAFE_CAST(TransactionSold AS FLOAT64) AS transaction_sold_alt,
    SAFE_CAST(bid_price AS FLOAT64) AS bid_price,
    SAFE_CAST(Price AS FLOAT64) AS price,
    SAFE_CAST(AutoOnlineQuotesStart AS FLOAT64) AS quote_started,
    SAFE_CAST(TotalQuotes AS FLOAT64) AS total_quotes,
    SAFE_CAST(TotalBinds AS FLOAT64) AS total_binds
  FROM `crblx-beacon-prod.Custom_Reports.Cross Tactic Analysis Full Data `
  WHERE Data_State IS NOT NULL
    AND ChannelGroupName IS NOT NULL
    AND SAFE_CAST(PriceAdjustmentPercent AS INT64) IS NOT NULL
),
state_tp AS (
  SELECT
    event_date,
    channel_group_name,
    state,
    activity_type,
    lead_type,
    price_adjustment_percent,
    COUNT(DISTINCT Lead_LeadID) AS opps,
    SUM(COALESCE(bid_count, 0)) AS bids,
    SUM(COALESCE(returned_ads_count, 0)) AS total_impressions,
    AVG(COALESCE(ad_position, 0)) AS avg_position,
    SUM(COALESCE(transaction_sold, transaction_sold_alt, 0)) AS sold,
    SAFE_DIVIDE(
      SUM(COALESCE(transaction_sold, transaction_sold_alt, 0)),
      NULLIF(SUM(COALESCE(bid_count, 0)), 0)
    ) AS win_rate,
    AVG(COALESCE(bid_price, 0)) AS avg_bid,
    SAFE_DIVIDE(
      SUM(COALESCE(price, 0)),
      NULLIF(SUM(COALESCE(transaction_sold, transaction_sold_alt, 0)), 0)
    ) AS cpc,
    SUM(COALESCE(price, 0)) AS total_spend,
    SAFE_DIVIDE(
      SUM(COALESCE(total_quotes, 0)),
      NULLIF(SUM(COALESCE(transaction_sold, transaction_sold_alt, 0)), 0)
    ) AS click_to_quote,
    SAFE_DIVIDE(
      SUM(COALESCE(quote_started, 0)),
      NULLIF(SUM(COALESCE(transaction_sold, transaction_sold_alt, 0)), 0)
    ) AS quote_start_rate,
    SUM(COALESCE(quote_started, 0)) AS number_of_quote_started,
    SUM(COALESCE(total_quotes, 0)) AS number_of_quotes,
    SUM(COALESCE(total_binds, 0)) AS number_of_binds
  FROM base
  GROUP BY 1, 2, 3, 4, 5, 6
),
channel_tp AS (
  SELECT
    event_date,
    channel_group_name,
    activity_type,
    lead_type,
    price_adjustment_percent,
    SUM(bids) AS channel_bids,
    SUM(sold) AS channel_sold,
    SAFE_DIVIDE(SUM(sold), NULLIF(SUM(bids), 0)) AS channel_win_rate,
    SAFE_DIVIDE(SUM(total_spend), NULLIF(SUM(sold), 0)) AS channel_cpc
  FROM state_tp
  GROUP BY 1, 2, 3, 4, 5
),
joined AS (
  SELECT
    s.*,
    b.win_rate AS baseline_win_rate,
    b.cpc AS baseline_cpc,
    b.bids AS baseline_bids,
    b.sold AS baseline_sold,
    c.channel_bids,
    c.channel_sold,
    c.channel_win_rate,
    c.channel_cpc,
    -- channel_ex_bids: channel bids excluding this state (for 600-bid threshold)
    (c.channel_bids - s.bids) AS channel_ex_bids,
    cb.channel_win_rate AS channel_baseline_win_rate,
    cb.channel_cpc AS channel_baseline_cpc,
    cb.channel_bids AS channel_baseline_bids,
    cb.channel_sold AS channel_baseline_sold
  FROM state_tp s
  LEFT JOIN state_tp b
    ON b.event_date = s.event_date
   AND b.channel_group_name = s.channel_group_name
   AND b.state = s.state
   AND b.activity_type = s.activity_type
   AND b.lead_type = s.lead_type
   AND b.price_adjustment_percent = 0
  LEFT JOIN channel_tp c
    ON c.event_date = s.event_date
   AND c.channel_group_name = s.channel_group_name
   AND c.activity_type = s.activity_type
   AND c.lead_type = s.lead_type
   AND c.price_adjustment_percent = s.price_adjustment_percent
  LEFT JOIN channel_tp cb
    ON cb.event_date = s.event_date
   AND cb.channel_group_name = s.channel_group_name
   AND cb.activity_type = s.activity_type
   AND cb.lead_type = s.lead_type
   AND cb.price_adjustment_percent = 0
)
SELECT
  event_date AS date,
  channel_group_name,
  state,
  activity_type,
  lead_type,
  price_adjustment_percent,

  opps,
  bids,
  total_impressions,
  avg_position,
  sold,
  win_rate,
  avg_bid,
  cpc,
  total_spend,
  click_to_quote,
  quote_start_rate,
  number_of_quote_started,
  number_of_quotes,
  number_of_binds,

  -- stat_sig: bid-count thresholds matching live PE query
  CASE
    WHEN price_adjustment_percent = 0 THEN 'baseline'
    WHEN bids >= 200 THEN 'state'
    WHEN bids >= 50 AND COALESCE(channel_ex_bids, 0) >= 600 THEN 'channel'
    ELSE 'disqualified'
  END AS stat_sig,

  CASE
    WHEN price_adjustment_percent = 0 THEN 'baseline'
    WHEN channel_bids >= 200 THEN 'state'
    ELSE 'disqualified'
  END AS stat_sig_channel_group,

  CASE
    WHEN price_adjustment_percent = 0 THEN NULL
    ELSE SAFE_DIVIDE(cpc - baseline_cpc, NULLIF(baseline_cpc, 0))
  END AS cpc_uplift,

  CASE
    WHEN price_adjustment_percent = 0 THEN NULL
    ELSE SAFE_DIVIDE(channel_cpc - channel_baseline_cpc, NULLIF(channel_baseline_cpc, 0))
  END AS cpc_uplift_channelgroup,

  CASE
    WHEN price_adjustment_percent = 0 THEN NULL
    ELSE SAFE_DIVIDE(win_rate - baseline_win_rate, NULLIF(baseline_win_rate, 0))
  END AS win_rate_uplift,

  CASE
    WHEN price_adjustment_percent = 0 THEN NULL
    ELSE SAFE_DIVIDE(channel_win_rate - channel_baseline_win_rate, NULLIF(channel_baseline_win_rate, 0))
  END AS win_rate_uplift_channelgroup,

  -- additional_clicks: uses bid-count stat_sig
  CASE
    WHEN price_adjustment_percent = 0 THEN NULL
    ELSE (
      (
        CASE
          WHEN bids >= 200 THEN win_rate
          WHEN bids >= 50 AND COALESCE(channel_ex_bids, 0) >= 600 THEN channel_win_rate
          ELSE win_rate
        END
        - baseline_win_rate
      ) * bids
    )
  END AS additional_clicks
FROM joined;
