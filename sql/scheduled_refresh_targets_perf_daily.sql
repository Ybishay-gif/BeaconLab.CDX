-- Run periodically (for example every 4-6 hours) in BigQuery Scheduled Queries.
CALL `crblx-beacon-prod.planning_app.sp_refresh_targets_perf_daily`(
  DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY),
  CURRENT_DATE()
);
