CREATE OR REPLACE PROCEDURE `crblx-beacon-prod.planning_app.sp_refresh_targets_perf_daily`(
  start_date DATE,
  end_date DATE
)
BEGIN
  DELETE FROM `crblx-beacon-prod.planning_app.targets_perf_daily`
  WHERE event_date BETWEEN start_date AND end_date;

  INSERT INTO `crblx-beacon-prod.planning_app.targets_perf_daily` (
    event_date,
    state,
    segment,
    source_key,
    company_account_id,
    activity_type,
    lead_type,
    sold,
    binds,
    scored_policies,
    price_sum,
    target_cpb_sum,
    lifetime_premium_sum,
    lifetime_cost_sum,
    avg_profit_sum,
    avg_equity_sum,
    refreshed_at
  )
  SELECT
    DATE(COALESCE(createdate_utc, Data_DateCreated, DateCreated)) AS event_date,
    UPPER(Data_State) AS state,
    UPPER(
      COALESCE(
        NULLIF(TRIM(Segments), ''),
        REGEXP_EXTRACT(UPPER(COALESCE(ChannelGroupName, '')), r'(MCH|MCR|SCH|SCR)')
      )
    ) AS segment,
    REGEXP_REPLACE(LOWER(COALESCE(CAST(Account_Name AS STRING), '')), r'[^a-z0-9]+', '') AS source_key,
    COALESCE(CAST(CompanyAccountId AS STRING), '') AS company_account_id,
    CASE
      WHEN LOWER(COALESCE(activitytype, '')) LIKE 'click%' THEN 'Click'
      WHEN LOWER(COALESCE(activitytype, '')) LIKE 'lead%' THEN 'Lead'
      WHEN LOWER(COALESCE(activitytype, '')) LIKE 'call%' THEN 'Call'
      ELSE ''
    END AS activity_type,
    CASE
      WHEN LOWER(COALESCE(Leadtype, '')) LIKE '%car%' THEN 'CAR_INSURANCE_LEAD'
      WHEN LOWER(COALESCE(Leadtype, '')) LIKE '%home%' THEN 'HOME_INSURANCE_LEAD'
      ELSE ''
    END AS lead_type,
    SUM(COALESCE(SAFE_CAST(Transaction_sold AS FLOAT64), SAFE_CAST(TransactionSold AS FLOAT64), 0)) AS sold,
    SUM(COALESCE(SAFE_CAST(TotalBinds AS FLOAT64), 0)) AS binds,
    SUM(COALESCE(SAFE_CAST(ScoredPolicies AS FLOAT64), 0)) AS scored_policies,
    SUM(COALESCE(SAFE_CAST(Price AS FLOAT64), 0)) AS price_sum,
    SUM(COALESCE(SAFE_CAST(Target_TargetCPB AS FLOAT64), 0)) AS target_cpb_sum,
    SUM(COALESCE(SAFE_CAST(LifetimePremium AS FLOAT64), 0)) AS lifetime_premium_sum,
    SUM(COALESCE(SAFE_CAST(LifeTimeCost AS FLOAT64), 0)) AS lifetime_cost_sum,
    SUM(COALESCE(SAFE_CAST(CustomValues_Profit AS FLOAT64), 0)) AS avg_profit_sum,
    SUM(COALESCE(SAFE_CAST(Equity AS FLOAT64), 0)) AS avg_equity_sum,
    CURRENT_TIMESTAMP() AS refreshed_at
  FROM `crblx-beacon-prod.Custom_Reports.Cross Tactic Analysis Full Data `
  WHERE DATE(COALESCE(createdate_utc, Data_DateCreated, DateCreated)) BETWEEN start_date AND end_date
    AND Data_State IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6, 7;
END;
