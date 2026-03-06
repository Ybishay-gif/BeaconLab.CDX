CREATE OR REPLACE TABLE FUNCTION `crblx-beacon-prod.planning_app.fn_price_exploration`(
  start_date DATE,
  end_date DATE
)
AS (
  SELECT *
  FROM `crblx-beacon-prod.planning_app.v_price_exploration_daily`
  WHERE date BETWEEN start_date AND end_date
);
