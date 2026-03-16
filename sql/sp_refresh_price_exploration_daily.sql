CREATE OR REPLACE PROCEDURE `crblx-beacon-prod.planning_app.sp_refresh_price_exploration_daily`(
  start_date DATE,
  end_date DATE
)
BEGIN
  DELETE FROM `crblx-beacon-prod.planning_app.price_exploration_daily`
  WHERE date BETWEEN start_date AND end_date;

  INSERT INTO `crblx-beacon-prod.planning_app.price_exploration_daily` (
    date,
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
    stat_sig,
    stat_sig_channel_group,
    cpc_uplift,
    cpc_uplift_channelgroup,
    win_rate_uplift,
    win_rate_uplift_channelgroup,
    additional_clicks,
    refreshed_at
  )
  SELECT
    date,
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
    stat_sig,
    stat_sig_channel_group,
    cpc_uplift,
    cpc_uplift_channelgroup,
    win_rate_uplift,
    win_rate_uplift_channelgroup,
    additional_clicks,
    CURRENT_TIMESTAMP()
  FROM `crblx-beacon-prod.planning_app.fn_price_exploration`(start_date, end_date);
END;
