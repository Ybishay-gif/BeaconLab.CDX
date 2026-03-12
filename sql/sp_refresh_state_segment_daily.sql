CREATE OR REPLACE PROCEDURE `crblx-beacon-prod.planning_app.sp_refresh_state_segment_daily`(
  start_date DATE,
  end_date DATE
)
BEGIN
  -- Delete existing rows for the refresh window
  DELETE FROM `crblx-beacon-prod.planning_app.state_segment_daily`
  WHERE event_date BETWEEN start_date AND end_date;

  -- Insert pre-aggregated daily rows
  INSERT INTO `crblx-beacon-prod.planning_app.state_segment_daily` (
    event_date,
    state,
    segment,
    channel_group_name,
    activity_type,
    lead_type,
    bids,
    sold,
    total_cost,
    quote_started,
    quotes,
    binds,
    scored_policies,
    target_cpb_sum,
    lifetime_premium_sum,
    lifetime_cost_sum,
    avg_profit_sum,
    avg_equity_sum,
    avg_mrltv_sum,
    refreshed_at
  )
  SELECT
    DATE(COALESCE(createdate_utc, Data_DateCreated, DateCreated)) AS event_date,
    Data_State AS state,
    UPPER(
      COALESCE(
        NULLIF(TRIM(Segments), ''),
        REGEXP_EXTRACT(UPPER(COALESCE(ChannelGroupName, '')), r'(MCH|MCR|SCH|SCR)')
      )
    ) AS segment,
    COALESCE(
      NULLIF(TRIM(ChannelGroupName), ''),
      NULLIF(TRIM(CAST(Account_Name AS STRING)), ''),
      ''
    ) AS channel_group_name,
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
    SUM(COALESCE(SAFE_CAST(bid_count AS FLOAT64), 0)) AS bids,
    SUM(COALESCE(SAFE_CAST(Transaction_sold AS FLOAT64), SAFE_CAST(TransactionSold AS FLOAT64), 0)) AS sold,
    SUM(COALESCE(SAFE_CAST(Price AS FLOAT64), 0)) AS total_cost,
    SUM(COALESCE(SAFE_CAST(AutoOnlineQuotesStart AS FLOAT64), 0)) AS quote_started,
    SUM(COALESCE(SAFE_CAST(TotalQuotes AS FLOAT64), 0)) AS quotes,
    SUM(COALESCE(SAFE_CAST(TotalBinds AS FLOAT64), 0)) AS binds,
    SUM(COALESCE(SAFE_CAST(ScoredPolicies AS FLOAT64), 0)) AS scored_policies,
    SUM(COALESCE(SAFE_CAST(Target_TargetCPB AS FLOAT64), 0)) AS target_cpb_sum,
    SUM(COALESCE(SAFE_CAST(LifetimePremium AS FLOAT64), 0)) AS lifetime_premium_sum,
    SUM(COALESCE(SAFE_CAST(LifeTimeCost AS FLOAT64), 0)) AS lifetime_cost_sum,
    SUM(COALESCE(SAFE_CAST(CustomValues_Profit AS FLOAT64), 0)) AS avg_profit_sum,
    SUM(COALESCE(SAFE_CAST(Equity AS FLOAT64), 0)) AS avg_equity_sum,
    SUM(COALESCE(SAFE_CAST(CustomValues_Mrltv AS FLOAT64), 0) * COALESCE(SAFE_CAST(ScoredPolicies AS FLOAT64), 0)) AS avg_mrltv_sum,
    CURRENT_TIMESTAMP() AS refreshed_at
  FROM `crblx-beacon-prod.Custom_Reports.Cross Tactic Analysis Full Data `
  WHERE DATE(COALESCE(createdate_utc, Data_DateCreated, DateCreated)) BETWEEN start_date AND end_date
    AND Data_State IS NOT NULL
    AND UPPER(
      COALESCE(
        NULLIF(TRIM(Segments), ''),
        REGEXP_EXTRACT(UPPER(COALESCE(ChannelGroupName, '')), r'(MCH|MCR|SCH|SCR)')
      )
    ) IN ('MCH', 'MCR', 'SCH', 'SCR')
  GROUP BY 1, 2, 3, 4, 5, 6;
END;
