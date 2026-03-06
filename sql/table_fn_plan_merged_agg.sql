CREATE OR REPLACE TABLE FUNCTION `crblx-beacon-prod.planning_app.fn_state_segment_performance_agg`(
  start_date DATE,
  end_date DATE
)
AS (
  SELECT
    state,
    segment,
    SUM(opps) AS opps,
    SUM(binds) AS binds,
    AVG(avg_bid) AS avg_bid,
    SUM(number_of_impressions) AS number_of_impressions,
    AVG(avg_position) AS avg_position,
    SUM(number_of_posts) AS number_of_posts,
    SUM(sold) AS sold,
    SUM(total_price) AS total_price,
    SAFE_DIVIDE(SUM(total_price), NULLIF(SUM(sold), 0)) AS cpc,
    SUM(post) AS post,
    SUM(calls) AS calls,
    SUM(total_quote) AS total_quote,
    SUM(total_binds) AS total_binds,
    SAFE_DIVIDE(SUM(total_binds), NULLIF(SUM(total_quote), 0)) AS quote_to_bind,
    SUM(quote_started) AS quote_started,
    SAFE_DIVIDE(SUM(quote_started), NULLIF(SUM(sold), 0)) AS qsr,
    SAFE_DIVIDE(SUM(total_quote), NULLIF(SUM(sold), 0)) AS click_to_quote,
    SUM(scored_policies) AS scored_policies,
    SUM(lifetime_premium) AS lifetime_premium,
    SUM(lifetime_cost) AS lifetime_cost,
    SAFE_DIVIDE(SUM(lifetime_cost), NULLIF(SUM(lifetime_premium), 0)) AS combine_ratio,
    SAFE_DIVIDE(SUM(avg_mrltv * scored_policies), NULLIF(SUM(scored_policies), 0)) AS avg_mrltv,
    SAFE_DIVIDE(SUM(avg_profit * scored_policies), NULLIF(SUM(scored_policies), 0)) AS avg_profit,
    SAFE_DIVIDE(SUM(avg_equity * scored_policies), NULLIF(SUM(scored_policies), 0)) AS avg_equity,
    SAFE_DIVIDE(SUM(avg_profit * scored_policies), NULLIF(SUM(avg_equity * scored_policies), 0)) AS roe,
    SAFE_DIVIDE(SUM(total_price), NULLIF(SUM(total_binds), 0)) AS cpb,
    AVG(target_cpb) AS target_cpb,
    SAFE_DIVIDE(AVG(target_cpb), SAFE_DIVIDE(SUM(total_price), NULLIF(SUM(total_binds), 0))) AS performance,
    SAFE_DIVIDE(SUM(total_binds), NULLIF(SUM(sold), 0)) AS sold_to_bind
  FROM `crblx-beacon-prod.planning_app.v_state_segment_performance_daily`
  WHERE event_date BETWEEN start_date AND end_date
  GROUP BY state, segment
);

CREATE OR REPLACE TABLE FUNCTION `crblx-beacon-prod.planning_app.fn_plan_merged_agg`(
  start_date DATE,
  end_date DATE
)
AS (
  WITH pe AS (
    SELECT
      p.*, 
      REGEXP_EXTRACT(UPPER(p.channel_group_name), r'(MCH|MCR|SCH|SCR)') AS segment
    FROM `crblx-beacon-prod.planning_app.fn_price_exploration_agg`(start_date, end_date) p
  ),
  filtered_pe AS (
    SELECT *
    FROM pe
    WHERE segment IS NOT NULL
  ),
  perf AS (
    SELECT *
    FROM `crblx-beacon-prod.planning_app.fn_state_segment_performance_agg`(start_date, end_date)
  )
  SELECT
    start_date,
    end_date,
    fp.channel_group_name,
    fp.state,
    fp.segment,
    fp.price_adjustment_percent,

    -- Exploration-side metrics
    fp.opps AS pe_opps,
    fp.bids AS pe_bids,
    fp.total_impressions AS pe_total_impressions,
    fp.avg_position AS pe_avg_position,
    fp.sold AS pe_sold,
    fp.win_rate AS pe_win_rate,
    fp.avg_bid AS pe_avg_bid,
    fp.cpc AS pe_cpc,
    fp.total_spend AS pe_total_spend,
    fp.click_to_quote AS pe_click_to_quote,
    fp.quote_start_rate AS pe_quote_start_rate,
    fp.number_of_quote_started AS pe_number_of_quote_started,
    fp.number_of_quotes AS pe_number_of_quotes,
    fp.stat_sig,
    fp.stat_sig_channel_group,
    fp.cpc_uplift,
    fp.cpc_uplift_channelgroup,
    fp.win_rate_uplift,
    fp.win_rate_uplift_channelgroup,
    fp.additional_clicks,

    -- State+segment performance metrics
    pf.opps AS ss_opps,
    pf.sold AS ss_sold,
    pf.total_quote AS ss_total_quote,
    pf.total_binds AS ss_total_binds,
    pf.quote_to_bind AS ss_quote_to_bind,
    pf.cpb AS ss_cpb,
    pf.target_cpb AS ss_target_cpb,
    pf.performance AS ss_performance,
    pf.roe AS ss_roe,
    pf.combine_ratio AS ss_combine_ratio,
    pf.avg_mrltv AS ss_avg_mrltv,
    pf.avg_equity AS ss_avg_equity,
    pf.avg_profit AS ss_avg_profit,
    pf.sold_to_bind AS ss_sold_to_bind,

    -- Expected scenario calculations
    GREATEST((fp.sold + COALESCE(fp.additional_clicks, 0)), 0) AS expected_total_clicks,
    fp.cpc AS expected_cpc,
    GREATEST((fp.sold + COALESCE(fp.additional_clicks, 0)), 0) * fp.cpc AS expected_total_cost,
    (GREATEST((fp.sold + COALESCE(fp.additional_clicks, 0)), 0) * pf.sold_to_bind) AS expected_total_binds,
    (COALESCE(fp.additional_clicks, 0) * pf.sold_to_bind) AS additional_expected_binds,
    SAFE_DIVIDE(
      GREATEST((fp.sold + COALESCE(fp.additional_clicks, 0)), 0) * fp.cpc,
      NULLIF((GREATEST((fp.sold + COALESCE(fp.additional_clicks, 0)), 0) * pf.sold_to_bind), 0)
    ) AS expected_cpb,
    SAFE_DIVIDE(
      pf.target_cpb,
      NULLIF(
        SAFE_DIVIDE(
          GREATEST((fp.sold + COALESCE(fp.additional_clicks, 0)), 0) * fp.cpc,
          NULLIF((GREATEST((fp.sold + COALESCE(fp.additional_clicks, 0)), 0) * pf.sold_to_bind), 0)
        ),
        0
      )
    ) AS expected_performance,
    SAFE_DIVIDE(
      pf.target_cpb,
      NULLIF(
        SAFE_DIVIDE(
          GREATEST((fp.sold + COALESCE(fp.additional_clicks, 0)), 0) * fp.cpc,
          NULLIF((GREATEST((fp.sold + COALESCE(fp.additional_clicks, 0)), 0) * pf.sold_to_bind), 0)
        ),
        0
      )
    ) - pf.performance AS performance_uplift
  FROM filtered_pe fp
  LEFT JOIN perf pf
    ON pf.state = fp.state
   AND pf.segment = fp.segment
);
