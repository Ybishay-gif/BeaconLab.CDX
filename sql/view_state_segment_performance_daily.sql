CREATE OR REPLACE VIEW `crblx-beacon-prod.planning_app.v_state_segment_performance_daily` AS
WITH base AS (
  SELECT
    DATE(COALESCE(createdate_utc, Data_DateCreated, DateCreated)) AS event_date,
    Data_State AS state,
    UPPER(
      COALESCE(
        NULLIF(TRIM(Segments), ''),
        REGEXP_EXTRACT(UPPER(COALESCE(ChannelGroupName, '')), r'(MCH|MCR|SCH|SCR)')
      )
    ) AS segment,
    Lead_LeadID,
    bid_count,
    bid_price,
    ExtraBidData_ReturnedAdsCount,
    ExtraBidData_OriginalAdData_Position,
    NumberOfPosts,
    Transaction_sold,
    TransactionSold,
    Price,
    TotalCalls,
    TotalQuotes,
    TotalBinds,
    AutoOnlineQuotesStart,
    ScoredPolicies,
    LifetimePremium,
    LifeTimeCost,
    CustomValues_Mrltv,
    CustomValues_Profit,
    Equity,
    Target_TargetCPB
  FROM `crblx-beacon-prod.Custom_Reports.Cross Tactic Analysis Full Data `
)
SELECT
  event_date,
  state,
  segment,

  COUNT(DISTINCT Lead_LeadID) AS opps,
  SUM(COALESCE(CAST(bid_count AS FLOAT64), 0)) AS binds,
  AVG(COALESCE(CAST(bid_price AS FLOAT64), 0)) AS avg_bid,
  SUM(COALESCE(CAST(ExtraBidData_ReturnedAdsCount AS FLOAT64), 0)) AS number_of_impressions,
  AVG(COALESCE(CAST(ExtraBidData_OriginalAdData_Position AS FLOAT64), 0)) AS avg_position,
  SUM(COALESCE(CAST(NumberOfPosts AS FLOAT64), 0)) AS number_of_posts,
  SUM(COALESCE(CAST(Transaction_sold AS FLOAT64), CAST(TransactionSold AS FLOAT64), 0)) AS sold,
  SUM(COALESCE(CAST(Price AS FLOAT64), 0)) AS total_price,
  SAFE_DIVIDE(
    SUM(COALESCE(CAST(Price AS FLOAT64), 0)),
    NULLIF(SUM(COALESCE(CAST(Transaction_sold AS FLOAT64), CAST(TransactionSold AS FLOAT64), 0)), 0)
  ) AS cpc,

  SUM(COALESCE(CAST(NumberOfPosts AS FLOAT64), 0)) AS post,
  SUM(COALESCE(CAST(TotalCalls AS FLOAT64), 0)) AS calls,
  SUM(COALESCE(CAST(TotalQuotes AS FLOAT64), 0)) AS total_quote,
  SUM(COALESCE(CAST(TotalBinds AS FLOAT64), 0)) AS total_binds,
  SAFE_DIVIDE(
    SUM(COALESCE(CAST(TotalBinds AS FLOAT64), 0)),
    NULLIF(SUM(COALESCE(CAST(TotalQuotes AS FLOAT64), 0)), 0)
  ) AS quote_to_bind,
  SUM(COALESCE(CAST(AutoOnlineQuotesStart AS FLOAT64), 0)) AS quote_started,
  SAFE_DIVIDE(
    SUM(COALESCE(CAST(AutoOnlineQuotesStart AS FLOAT64), 0)),
    NULLIF(SUM(COALESCE(CAST(Transaction_sold AS FLOAT64), CAST(TransactionSold AS FLOAT64), 0)), 0)
  ) AS qsr,
  SAFE_DIVIDE(
    SUM(COALESCE(CAST(TotalQuotes AS FLOAT64), 0)),
    NULLIF(SUM(COALESCE(CAST(Transaction_sold AS FLOAT64), CAST(TransactionSold AS FLOAT64), 0)), 0)
  ) AS click_to_quote,

  SUM(COALESCE(CAST(ScoredPolicies AS FLOAT64), 0)) AS scored_policies,
  SUM(COALESCE(CAST(LifetimePremium AS FLOAT64), 0)) AS lifetime_premium,
  SUM(COALESCE(CAST(LifeTimeCost AS FLOAT64), 0)) AS lifetime_cost,
  SAFE_DIVIDE(
    SUM(COALESCE(CAST(CustomValues_Mrltv AS FLOAT64), 0)),
    NULLIF(SUM(COALESCE(CAST(ScoredPolicies AS FLOAT64), 0)), 0)
  ) AS avg_mrltv,
  SAFE_DIVIDE(
    SUM(COALESCE(CAST(CustomValues_Profit AS FLOAT64), 0)),
    NULLIF(SUM(COALESCE(CAST(ScoredPolicies AS FLOAT64), 0)), 0)
  ) AS avg_profit,
  SAFE_DIVIDE(
    SUM(COALESCE(CAST(Equity AS FLOAT64), 0)),
    NULLIF(SUM(COALESCE(CAST(ScoredPolicies AS FLOAT64), 0)), 0)
  ) AS avg_equity,

  SAFE_DIVIDE(
    SUM(COALESCE(CAST(Price AS FLOAT64), 0)),
    NULLIF(SUM(COALESCE(CAST(TotalBinds AS FLOAT64), 0)), 0)
  ) AS cpb,
  AVG(COALESCE(CAST(Target_TargetCPB AS FLOAT64), 0)) AS target_cpb,
  SAFE_DIVIDE(
    AVG(COALESCE(CAST(Target_TargetCPB AS FLOAT64), 0)),
    SAFE_DIVIDE(
      SUM(COALESCE(CAST(Price AS FLOAT64), 0)),
      NULLIF(SUM(COALESCE(CAST(TotalBinds AS FLOAT64), 0)), 0)
    )
  ) AS performance
FROM base
WHERE state IS NOT NULL
  AND segment IN ('MCH', 'MCR', 'SCH', 'SCR')
GROUP BY event_date, state, segment;
