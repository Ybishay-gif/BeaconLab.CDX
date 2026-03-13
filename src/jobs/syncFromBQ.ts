/**
 * Daily sync job: BQ → PostgreSQL.
 *
 * Pulls analytics data from BigQuery pre-aggregated tables
 * and streams it into the PostgreSQL runtime database.
 * Uses BQ createQueryStream() to avoid loading all rows into memory at once.
 *
 * Tables synced:
 *   - state_segment_daily
 *   - targets_perf_daily
 *   - price_exploration_daily
 *   - pe_computed_daily      (shadow-mode: full PE CTE output)
 *   - plan_merged_daily      (shadow-mode: fn_plan_merged_agg output)
 */

import { bigquery, table as bqTable, analyticsTable as bqAnalyticsTable, analyticsRoutine as bqAnalyticsRoutine } from "../db/bigquery.js";
import { pgExec } from "../db/postgres.js";
import { config } from "../config.js";
import { buildRoeSql, buildCombinedRatioSql } from "../services/shared/kpiSql.js";
import { splitCombinedFilter } from "../services/shared/activityScope.js";

type SyncTableResult = {
  table: string;
  rows: number;
  ms: number;
  error?: string;
};

type SyncResult = {
  ok: boolean;
  totalMs: number;
  tables: SyncTableResult[];
};

const BATCH_SIZE = 500;

// ── Column definitions for each synced table ─────────────────────────

const STATE_SEGMENT_DAILY_COLS = [
  "event_date", "state", "segment", "channel_group_name", "activity_type",
  "lead_type", "bids", "sold", "total_cost", "quote_started", "quotes",
  "binds", "scored_policies", "target_cpb_sum", "lifetime_premium_sum",
  "lifetime_cost_sum", "avg_profit_sum", "avg_equity_sum", "avg_mrltv_sum",
  "refreshed_at",
] as const;

const TARGETS_PERF_DAILY_COLS = [
  "event_date", "state", "segment", "source_key", "company_account_id",
  "activity_type", "lead_type", "sold", "binds", "scored_policies",
  "price_sum", "target_cpb_sum", "lifetime_premium_sum", "lifetime_cost_sum",
  "avg_profit_sum", "avg_equity_sum", "refreshed_at",
] as const;

const PRICE_EXPLORATION_DAILY_COLS = [
  "date", "channel_group_name", "state", "price_adjustment_percent",
  "opps", "bids", "total_impressions", "avg_position", "sold",
  "win_rate", "avg_bid", "cpc", "total_spend", "click_to_quote",
  "quote_start_rate", "number_of_quote_started", "number_of_quotes",
  "stat_sig", "stat_sig_channel_group", "cpc_uplift",
  "cpc_uplift_channelgroup", "win_rate_uplift",
  "win_rate_uplift_channelgroup", "additional_clicks", "refreshed_at",
] as const;

const PE_COMPUTED_DAILY_COLS = [
  "activity_lead_type", "channel_group_name", "state", "testing_point",
  "opps", "bids", "win_rate", "sold", "binds", "quotes",
  "click_to_quote", "channel_quote", "click_to_channel_quote",
  "q2b", "channel_binds", "channel_q2b",
  "cpc", "avg_bid",
  "win_rate_uplift_state", "cpc_uplift_state",
  "win_rate_uplift_channel", "cpc_uplift_channel",
  "win_rate_uplift", "cpc_uplift",
  "additional_clicks", "expected_bind_change", "additional_budget_needed",
  "current_cpb", "expected_cpb", "cpb_uplift",
  "performance", "roe", "combined_ratio",
  "recommended_testing_point", "stat_sig", "stat_sig_channel_group", "stat_sig_source",
  "scf_avg_profit", "scf_avg_equity", "scf_avg_lifetime_premium", "scf_avg_lifetime_cost",
  "ssd_binds", "ssd_quotes", "ssd_q2b", "ssd_performance", "ssd_roe", "ssd_combined_ratio",
  "refreshed_at",
] as const;

const PLAN_MERGED_DAILY_COLS = [
  "start_date", "end_date", "channel_group_name", "state", "segment",
  "price_adjustment_percent", "stat_sig", "stat_sig_channel_group",
  "cpc_uplift", "win_rate_uplift", "additional_clicks",
  "expected_total_clicks", "expected_cpc", "expected_total_cost",
  "expected_total_binds", "additional_expected_binds", "expected_cpb",
  "ss_performance", "expected_performance", "performance_uplift",
  "refreshed_at",
] as const;

// ── Helpers ──────────────────────────────────────────────────────────

/** Normalise a BQ row value for PG insertion. */
function pgLiteral(value: unknown): string {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "number" || typeof value === "bigint") return String(value);
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";

  // BQ date/timestamp objects have a .value property
  if (typeof value === "object" && value !== null && "value" in value) {
    return pgLiteral((value as { value: unknown }).value);
  }

  // BigQueryDate objects
  if (typeof value === "object" && value !== null && typeof (value as { toISOString?: unknown }).toISOString === "function") {
    return `'${(value as { toISOString: () => string }).toISOString()}'`;
  }

  const str = String(value).replace(/'/g, "''");
  return `'${str}'`;
}

function buildInsertBatch(tableName: string, cols: readonly string[], rows: Record<string, unknown>[]): string {
  const colList = cols.join(", ");
  const valueSets = rows.map((row) => {
    const vals = cols.map((col) => pgLiteral(row[col]));
    return `(${vals.join(", ")})`;
  });
  return `INSERT INTO ${tableName} (${colList}) VALUES\n${valueSets.join(",\n")}`;
}

// ── Per-table sync (streaming) ────────────────────────────────────────

/**
 * Streams rows from BQ and inserts into PG in small batches.
 * Never holds more than BATCH_SIZE rows in memory at once.
 * Truncates first, then streams inserts — no wrapping transaction needed
 * because each batch is independent and the data is refreshed daily.
 */
async function syncTable(
  tableName: string,
  bqSql: string,
  cols?: readonly string[],
): Promise<SyncTableResult> {
  const t0 = Date.now();
  try {
    const colDefs = cols
      ?? (tableName === "targets_perf_daily" ? TARGETS_PERF_DAILY_COLS
        : tableName === "price_exploration_daily" ? PRICE_EXPLORATION_DAILY_COLS
        : STATE_SEGMENT_DAILY_COLS);

    await pgExec(`TRUNCATE ${tableName}`);

    const stream = bigquery.createQueryStream({ query: bqSql, useLegacySql: false });
    let batch: Record<string, unknown>[] = [];
    let totalRows = 0;

    for await (const row of stream) {
      batch.push(row as Record<string, unknown>);
      if (batch.length >= BATCH_SIZE) {
        await pgExec(buildInsertBatch(tableName, colDefs, batch));
        totalRows += batch.length;
        batch = [];
      }
    }
    // Flush remaining rows
    if (batch.length > 0) {
      await pgExec(buildInsertBatch(tableName, colDefs, batch));
      totalRows += batch.length;
    }

    return { table: tableName, rows: totalRows, ms: Date.now() - t0 };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return { table: tableName, rows: 0, ms: Date.now() - t0, error: message };
  }
}

// ── PE Computed sync (shadow-mode) ────────────────────────────────────

/**
 * Build the full PE CTE SQL for a given activity/lead scope.
 * This mirrors the logic in analyticsService.getPriceExplorationBQ() exactly,
 * but adds raw financial columns (scf_*, ssd_*) for ROE/COR recomputation
 * at PG query time with the runtime qbc param.
 *
 * Key differences from the runtime CTE:
 *   - 90-day date window (hardcoded)
 *   - No state/channel filters (syncs ALL pairs)
 *   - qbc=0 placeholder (ROE/COR recomputed at PG query time)
 *   - Extra financial columns in output
 */
function buildPeSyncSql(activityType: string, leadType: string): string {
  const RAW = config.rawCrossTacticTable;
  const SSD = bqAnalyticsTable("state_segment_daily");

  // Activity/lead type filter conditions (embedded, not parameterized)
  const actWhere = activityType
    ? `AND (
        CASE
          WHEN activity_type_raw LIKE 'click%' THEN 'clicks'
          WHEN activity_type_raw LIKE 'lead%' THEN 'leads'
          WHEN activity_type_raw LIKE 'call%' THEN 'calls'
          ELSE ''
        END
      ) = '${activityType}'`
    : "";
  const leadWhere = leadType
    ? `AND (
        CASE
          WHEN lead_type_raw LIKE '%car%' THEN 'auto'
          WHEN lead_type_raw LIKE '%home%' THEN 'home'
          ELSE ''
        END
      ) = '${leadType}'`
    : "";

  // Map PE activity/lead to SSD activity_type/lead_type
  const ssdActFilter = activityType === "clicks" ? "Click"
    : activityType === "leads" ? "Lead"
    : activityType === "calls" ? "Call"
    : "";
  const ssdLeadFilter = leadType === "auto" ? "CAR_INSURANCE_LEAD"
    : leadType === "home" ? "HOME_INSURANCE_LEAD"
    : "";

  const ssdActWhere = ssdActFilter ? `AND activity_type = '${ssdActFilter}'` : "";
  const ssdLeadWhere = ssdLeadFilter ? `AND lead_type = '${ssdLeadFilter}'` : "";

  return `
    WITH raw_all AS (
      SELECT
        ChannelGroupName AS channel_group_name,
        Data_State AS state,
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
        SAFE_CAST(TotalBinds AS FLOAT64) AS total_binds,
        SAFE_CAST(ScoredPolicies AS FLOAT64) AS scored_policies,
        SAFE_CAST(Target_TargetCPB AS FLOAT64) AS target_cpb,
        SAFE_CAST(CustomValues_Profit AS FLOAT64) AS avg_profit,
        SAFE_CAST(Equity AS FLOAT64) AS avg_equity,
        SAFE_CAST(LifetimePremium AS FLOAT64) AS lifetime_premium,
        SAFE_CAST(LifeTimeCost AS FLOAT64) AS lifetime_cost,
        LOWER(COALESCE(activitytype, '')) AS activity_type_raw,
        LOWER(COALESCE(Leadtype, '')) AS lead_type_raw
      FROM ${RAW}
      WHERE DATE(COALESCE(createdate_utc, Data_DateCreated, DateCreated))
            BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND CURRENT_DATE()
        AND Data_State IS NOT NULL
        AND ChannelGroupName IS NOT NULL
        AND SAFE_CAST(PriceAdjustmentPercent AS INT64) IS NOT NULL
    ),
    base_all AS (
      SELECT
        channel_group_name,
        state,
        price_adjustment_percent,
        Lead_LeadID,
        bid_count,
        returned_ads_count,
        ad_position,
        transaction_sold,
        transaction_sold_alt,
        bid_price,
        price,
        quote_started,
        total_quotes,
        total_binds,
        scored_policies,
        target_cpb,
        avg_profit,
        avg_equity,
        lifetime_premium,
        lifetime_cost,
        CASE
          WHEN activity_type_raw LIKE 'click%' THEN 'clicks'
          WHEN activity_type_raw LIKE 'lead%' THEN 'leads'
          WHEN activity_type_raw LIKE 'call%' THEN 'calls'
          ELSE ''
        END AS activity_group,
        CASE
          WHEN lead_type_raw LIKE '%car%' THEN 'auto'
          WHEN lead_type_raw LIKE '%home%' THEN 'home'
          ELSE ''
        END AS lead_group
      FROM raw_all
      WHERE TRUE
        ${actWhere}
        ${leadWhere}
    ),
    base_filtered AS (
      SELECT * FROM base_all
    ),
    state_tp AS (
      SELECT
        channel_group_name,
        state,
        activity_group,
        lead_group,
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
        SUM(COALESCE(total_binds, 0)) AS number_of_binds,
        SUM(COALESCE(scored_policies, 0)) AS scored_policies,
        SUM(COALESCE(target_cpb, 0)) AS target_cpb_total,
        SUM(COALESCE(avg_profit, 0)) AS avg_profit_total,
        SUM(COALESCE(avg_equity, 0)) AS avg_equity_total,
        SUM(COALESCE(lifetime_premium, 0)) AS lifetime_premium_total,
        SUM(COALESCE(lifetime_cost, 0)) AS lifetime_cost_total
      FROM base_filtered
      GROUP BY 1, 2, 3, 4, 5
    ),
    channel_tp AS (
      SELECT
        channel_group_name,
        activity_group,
        lead_group,
        price_adjustment_percent,
        SUM(bids) AS channel_bids,
        SUM(sold) AS channel_sold,
        SUM(total_spend) AS channel_total_spend,
        SAFE_DIVIDE(SUM(sold), NULLIF(SUM(bids), 0)) AS channel_win_rate,
        SAFE_DIVIDE(SUM(total_spend), NULLIF(SUM(sold), 0)) AS channel_cpc
      FROM (
        SELECT
          channel_group_name,
          activity_group,
          lead_group,
          price_adjustment_percent,
          SUM(COALESCE(bid_count, 0)) AS bids,
          SUM(COALESCE(transaction_sold, transaction_sold_alt, 0)) AS sold,
          SUM(COALESCE(price, 0)) AS total_spend
        FROM base_all
        GROUP BY 1, 2, 3, 4
      )
      GROUP BY 1, 2, 3, 4
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
        cb.channel_win_rate AS channel_baseline_win_rate,
        cb.channel_cpc AS channel_baseline_cpc,
        cb.channel_bids AS channel_baseline_bids,
        cb.channel_sold AS channel_baseline_sold,
        SAFE_DIVIDE(
          c.channel_sold - COALESCE(s.sold, 0),
          NULLIF(c.channel_bids - COALESCE(s.bids, 0), 0)
        ) AS channel_ex_win_rate,
        SAFE_DIVIDE(
          c.channel_total_spend - COALESCE(s.total_spend, 0),
          NULLIF(c.channel_sold - COALESCE(s.sold, 0), 0)
        ) AS channel_ex_cpc,
        SAFE_DIVIDE(
          cb.channel_sold - COALESCE(b.sold, 0),
          NULLIF(cb.channel_bids - COALESCE(b.bids, 0), 0)
        ) AS channel_baseline_ex_win_rate,
        SAFE_DIVIDE(
          cb.channel_total_spend - COALESCE(b.total_spend, 0),
          NULLIF(cb.channel_sold - COALESCE(b.sold, 0), 0)
        ) AS channel_baseline_ex_cpc,
        (c.channel_bids - COALESCE(s.bids, 0)) AS channel_ex_bids
      FROM state_tp s
      LEFT JOIN state_tp b
        ON b.channel_group_name = s.channel_group_name
       AND b.state = s.state
       AND b.activity_group = s.activity_group
       AND b.lead_group = s.lead_group
       AND b.price_adjustment_percent = 0
      LEFT JOIN channel_tp c
        ON c.channel_group_name = s.channel_group_name
       AND c.activity_group = s.activity_group
       AND c.lead_group = s.lead_group
       AND c.price_adjustment_percent = s.price_adjustment_percent
      LEFT JOIN channel_tp cb
        ON cb.channel_group_name = s.channel_group_name
       AND cb.activity_group = s.activity_group
       AND cb.lead_group = s.lead_group
       AND cb.price_adjustment_percent = 0
    ),
    per_group AS (
      SELECT
        channel_group_name,
        state,
        activity_group,
        lead_group,
        price_adjustment_percent AS testing_point,
        opps,
        bids,
        sold,
        number_of_binds,
        scored_policies,
        target_cpb_total,
        avg_profit_total,
        avg_equity_total,
        lifetime_premium_total,
        lifetime_cost_total,
        number_of_quotes,
        avg_bid,
        cpc,
        total_spend,
        CASE
          WHEN price_adjustment_percent = 0          THEN 'baseline'
          WHEN bids < 50                             THEN 'disqualified'
          WHEN sold < 15                             THEN 'disqualified'
          WHEN bids >= 200                           THEN 'state'
          WHEN COALESCE(channel_ex_bids, 0) < 600   THEN 'disqualified'
          ELSE                                            'channel'
        END AS stat_sig,
        CASE
          WHEN price_adjustment_percent = 0 THEN NULL
          ELSE SAFE_DIVIDE(cpc - baseline_cpc, NULLIF(baseline_cpc, 0))
        END AS cpc_uplift_state,
        CASE
          WHEN price_adjustment_percent = 0 THEN NULL
          ELSE SAFE_DIVIDE(win_rate - baseline_win_rate, NULLIF(baseline_win_rate, 0))
        END AS win_rate_uplift_state,
        CASE
          WHEN price_adjustment_percent = 0 THEN NULL
          ELSE SAFE_DIVIDE(
            COALESCE(SAFE_DIVIDE(cpc - baseline_cpc, NULLIF(baseline_cpc, 0)), 0) * bids
            + COALESCE(SAFE_DIVIDE(channel_ex_cpc - channel_baseline_ex_cpc, NULLIF(channel_baseline_ex_cpc, 0)), 0) * COALESCE(channel_ex_bids, 0),
            NULLIF(bids + COALESCE(channel_ex_bids, 0), 0)
          )
        END AS cpc_uplift_channel,
        CASE
          WHEN price_adjustment_percent = 0 THEN NULL
          ELSE SAFE_DIVIDE(
            COALESCE(SAFE_DIVIDE(win_rate - baseline_win_rate, NULLIF(baseline_win_rate, 0)), 0) * bids
            + COALESCE(SAFE_DIVIDE(channel_ex_win_rate - channel_baseline_ex_win_rate, NULLIF(channel_baseline_ex_win_rate, 0)), 0) * COALESCE(channel_ex_bids, 0),
            NULLIF(bids + COALESCE(channel_ex_bids, 0), 0)
          )
        END AS win_rate_uplift_channel,
        CASE
          WHEN price_adjustment_percent = 0          THEN NULL
          WHEN bids < 50                             THEN NULL
          WHEN sold < 15                             THEN NULL
          WHEN bids >= 200                           THEN (win_rate - baseline_win_rate) * bids
          WHEN COALESCE(channel_ex_bids, 0) < 600   THEN NULL
          ELSE
            SAFE_DIVIDE(
              (win_rate - COALESCE(baseline_win_rate, 0)) * bids
              + (COALESCE(channel_ex_win_rate, 0) - COALESCE(channel_baseline_ex_win_rate, 0)) * COALESCE(channel_ex_bids, 0),
              NULLIF(bids + COALESCE(channel_ex_bids, 0), 0)
            ) * bids
        END AS additional_clicks,
        channel_ex_bids
      FROM joined
    ),
    final_agg AS (
      SELECT
        channel_group_name,
        state,
        testing_point,
        SUM(opps) AS opps,
        SUM(bids) AS bids,
        SAFE_DIVIDE(SUM(sold), NULLIF(SUM(bids), 0)) AS win_rate,
        SUM(sold) AS sold,
        SUM(number_of_binds) AS binds,
        SUM(scored_policies) AS scored_policies,
        SUM(target_cpb_total) AS target_cpb_total,
        SUM(avg_profit_total) AS avg_profit_total,
        SUM(avg_equity_total) AS avg_equity_total,
        SUM(lifetime_premium_total) AS lifetime_premium_total,
        SUM(lifetime_cost_total) AS lifetime_cost_total,
        SUM(number_of_quotes) AS quotes,
        SAFE_DIVIDE(SUM(number_of_quotes), NULLIF(SUM(bids), 0)) AS click_to_quote,
        SAFE_DIVIDE(SUM(total_spend), NULLIF(SUM(sold), 0)) AS cpc,
        SAFE_DIVIDE(SUM(avg_bid * bids), NULLIF(SUM(bids), 0)) AS avg_bid,
        SUM(total_spend) AS total_spend,
        CASE
          WHEN SUM(number_of_binds) = 0 THEN 0
          ELSE SAFE_DIVIDE(SUM(target_cpb_total), SUM(number_of_binds))
        END AS target_cpb,
        SAFE_DIVIDE(
          CASE
            WHEN SUM(number_of_binds) = 0 THEN 0
            ELSE SAFE_DIVIDE(SUM(target_cpb_total), SUM(number_of_binds))
          END,
          SAFE_DIVIDE(SUM(total_spend), NULLIF(SUM(number_of_binds), 0))
        ) AS performance,
        ${buildRoeSql({
          zeroConditions: [
            "SUM(scored_policies) = 0",
            "SAFE_DIVIDE(SUM(avg_equity_total), NULLIF(SUM(scored_policies), 0)) = 0"
          ],
          avgProfitExpr: "SAFE_DIVIDE(SUM(avg_profit_total), NULLIF(SUM(scored_policies), 0))",
          cpbExpr: "SAFE_DIVIDE(SUM(total_spend), NULLIF(SUM(number_of_binds), 0))",
          avgEquityExpr: "SAFE_DIVIDE(SUM(avg_equity_total), NULLIF(SUM(scored_policies), 0))",
          qbcExpr: "0"
        })} AS roe,
        ${buildCombinedRatioSql({
          zeroConditions: [
            "SUM(scored_policies) = 0",
            "SAFE_DIVIDE(SUM(lifetime_premium_total), NULLIF(SUM(scored_policies), 0)) = 0"
          ],
          cpbExpr: "SAFE_DIVIDE(SUM(total_spend), NULLIF(SUM(number_of_binds), 0))",
          avgLifetimeCostExpr: "SAFE_DIVIDE(SUM(lifetime_cost_total), NULLIF(SUM(scored_policies), 0))",
          avgLifetimePremiumExpr: "SAFE_DIVIDE(SUM(lifetime_premium_total), NULLIF(SUM(scored_policies), 0))",
          qbcExpr: "0"
        })} AS combined_ratio,
        SAFE_DIVIDE(
          SUM(IF(win_rate_uplift_state IS NULL, 0, win_rate_uplift_state * bids)),
          NULLIF(SUM(IF(win_rate_uplift_state IS NULL, 0, bids)), 0)
        ) AS win_rate_uplift_state,
        SAFE_DIVIDE(
          SUM(IF(cpc_uplift_state IS NULL, 0, cpc_uplift_state * sold)),
          NULLIF(SUM(IF(cpc_uplift_state IS NULL, 0, sold)), 0)
        ) AS cpc_uplift_state,
        SAFE_DIVIDE(
          SUM(IF(win_rate_uplift_channel IS NULL, 0, win_rate_uplift_channel * bids)),
          NULLIF(SUM(IF(win_rate_uplift_channel IS NULL, 0, bids)), 0)
        ) AS win_rate_uplift_channel,
        SAFE_DIVIDE(
          SUM(IF(cpc_uplift_channel IS NULL, 0, cpc_uplift_channel * sold)),
          NULLIF(SUM(IF(cpc_uplift_channel IS NULL, 0, sold)), 0)
        ) AS cpc_uplift_channel,
        SUM(COALESCE(additional_clicks, 0)) AS additional_clicks,
        MAX(COALESCE(channel_ex_bids, 0)) AS channel_ex_bids,
        CASE
          WHEN testing_point = 0                             THEN 'baseline'
          WHEN SUM(bids) < 50                                THEN 'disqualified'
          WHEN SUM(sold) < 15                                THEN 'disqualified'
          WHEN SUM(bids) >= 200                              THEN 'state'
          WHEN MAX(COALESCE(channel_ex_bids, 0)) < 600      THEN 'disqualified'
          ELSE                                                    'channel'
        END AS stat_sig
      FROM per_group
      GROUP BY channel_group_name, state, testing_point
    ),
    with_budget AS (
      SELECT
        *,
        SUM(bids) OVER (PARTITION BY channel_group_name, state) AS total_bids_channel_state,
        SUM(sold) OVER (PARTITION BY channel_group_name, state) AS current_sold_channel_state,
        SUM(quotes) OVER (PARTITION BY channel_group_name, state) AS state_ch_quotes,
        SUM(total_spend) OVER (PARTITION BY channel_group_name, state) AS current_spend_channel_state
      FROM final_agg
    ),
    with_expected AS (
      SELECT
        *,
        MAX(IF(testing_point = 0, win_rate, NULL)) OVER (PARTITION BY channel_group_name, state)
          AS baseline_win_rate_channel_state,
        MAX(IF(testing_point = 0, cpc, NULL)) OVER (PARTITION BY channel_group_name, state)
          AS baseline_cpc_channel_state,
        (win_rate * total_bids_channel_state) AS expected_clicks,
        (win_rate * total_bids_channel_state * cpc) AS expected_total_cost,
        MAX(IF(testing_point = 0, win_rate * total_bids_channel_state, NULL))
          OVER (PARTITION BY channel_group_name, state) AS baseline_expected_clicks,
        MAX(IF(testing_point = 0, win_rate * total_bids_channel_state * cpc, NULL))
          OVER (PARTITION BY channel_group_name, state) AS baseline_expected_cost
      FROM with_budget
    ),
    state_channel_binds AS (
      SELECT
        channel_group_name,
        state,
        SUM(COALESCE(total_binds, 0)) AS binds_state_channel
      FROM base_filtered
      GROUP BY channel_group_name, state
    ),
    state_channel_financials AS (
      SELECT
        channel_group_name,
        state,
        SAFE_DIVIDE(SUM(COALESCE(avg_profit, 0)), NULLIF(SUM(COALESCE(scored_policies, 0)), 0)) AS avg_profit,
        SAFE_DIVIDE(SUM(COALESCE(avg_equity, 0)), NULLIF(SUM(COALESCE(scored_policies, 0)), 0)) AS avg_equity,
        SAFE_DIVIDE(
          SUM(COALESCE(lifetime_premium, 0)),
          NULLIF(SUM(COALESCE(scored_policies, 0)), 0)
        ) AS avg_lifetime_premium,
        SAFE_DIVIDE(SUM(COALESCE(lifetime_cost, 0)), NULLIF(SUM(COALESCE(scored_policies, 0)), 0)) AS avg_lifetime_cost
      FROM base_filtered
      GROUP BY channel_group_name, state
    ),
    ssd_metrics AS (
      SELECT
        channel_group_name,
        state,
        SUM(binds) AS ssd_binds,
        SUM(quotes) AS ssd_quotes,
        SAFE_DIVIDE(SUM(binds), NULLIF(SUM(quotes), 0)) AS ssd_q2b,
        SAFE_DIVIDE(
          CASE WHEN SUM(binds) = 0 THEN 0
            ELSE SAFE_DIVIDE(SUM(target_cpb_sum), SUM(binds))
          END,
          SAFE_DIVIDE(SUM(total_cost), NULLIF(SUM(binds), 0))
        ) AS ssd_performance,
        ${buildRoeSql({
          zeroConditions: [
            "SUM(scored_policies) = 0",
            "SAFE_DIVIDE(SUM(avg_equity_sum), NULLIF(SUM(scored_policies), 0)) = 0"
          ],
          avgProfitExpr: "SAFE_DIVIDE(SUM(avg_profit_sum), NULLIF(SUM(scored_policies), 0))",
          cpbExpr: "SAFE_DIVIDE(SUM(total_cost), NULLIF(SUM(binds), 0))",
          avgEquityExpr: "SAFE_DIVIDE(SUM(avg_equity_sum), NULLIF(SUM(scored_policies), 0))",
          qbcExpr: "0"
        })} AS ssd_roe,
        ${buildCombinedRatioSql({
          zeroConditions: [
            "SUM(scored_policies) = 0",
            "SAFE_DIVIDE(SUM(lifetime_premium_sum), NULLIF(SUM(scored_policies), 0)) = 0"
          ],
          cpbExpr: "SAFE_DIVIDE(SUM(total_cost), NULLIF(SUM(binds), 0))",
          avgLifetimeCostExpr: "SAFE_DIVIDE(SUM(lifetime_cost_sum), NULLIF(SUM(scored_policies), 0))",
          avgLifetimePremiumExpr: "SAFE_DIVIDE(SUM(lifetime_premium_sum), NULLIF(SUM(scored_policies), 0))",
          qbcExpr: "0"
        })} AS ssd_combined_ratio
      FROM ${SSD}
      WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND CURRENT_DATE()
        ${ssdActWhere}
        ${ssdLeadWhere}
      GROUP BY channel_group_name, state
    ),
    channel_binds AS (
      SELECT
        channel_group_name,
        SUM(COALESCE(total_binds, 0)) AS channel_binds
      FROM base_all
      GROUP BY channel_group_name
    ),
    channel_quotes_all AS (
      SELECT
        channel_group_name,
        SUM(COALESCE(total_quotes, 0)) AS channel_quote,
        SAFE_DIVIDE(
          SUM(COALESCE(total_quotes, 0)),
          NULLIF(SUM(COALESCE(transaction_sold, transaction_sold_alt, 0)), 0)
        ) AS click_to_channel_quote
      FROM base_all
      GROUP BY channel_group_name
    ),
    q2b_source AS (
      SELECT
        channel_group_name,
        state,
        SAFE_DIVIDE(SUM(COALESCE(binds, 0)), NULLIF(SUM(COALESCE(quotes, 0)), 0)) AS q2b
      FROM ${SSD}
      WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND CURRENT_DATE()
      GROUP BY channel_group_name, state
    ),
    q2b_channel AS (
      SELECT
        channel_group_name,
        SAFE_DIVIDE(SUM(COALESCE(binds, 0)), NULLIF(SUM(COALESCE(quotes, 0)), 0)) AS channel_q2b
      FROM ${SSD}
      WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND CURRENT_DATE()
      GROUP BY channel_group_name
    ),
    q2b_state AS (
      SELECT
        state,
        SAFE_DIVIDE(SUM(COALESCE(binds, 0)), NULLIF(SUM(COALESCE(quotes, 0)), 0)) AS state_q2b
      FROM ${SSD}
      WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND CURRENT_DATE()
      GROUP BY state
    ),
    quote_rate_calc AS (
      SELECT
        with_expected.*,
        CASE
          WHEN COALESCE(state_ch_quotes, 0) >= 50
            THEN SAFE_DIVIDE(state_ch_quotes, NULLIF(current_sold_channel_state, 0))
          ELSE COALESCE(channel_quotes_all.click_to_channel_quote, 0)
        END AS quote_rate,
        CASE
          WHEN COALESCE(state_channel_binds.binds_state_channel, 0) >= 5 AND q2b_source.q2b IS NOT NULL
            THEN q2b_source.q2b
          ELSE COALESCE(q2b_state.state_q2b, 0)
        END AS q2b_rate,
        state_channel_binds.binds_state_channel,
        q2b_source.q2b,
        channel_binds.channel_binds,
        q2b_channel.channel_q2b,
        channel_quotes_all.channel_quote,
        channel_quotes_all.click_to_channel_quote
      FROM with_expected
      LEFT JOIN q2b_source
        ON q2b_source.channel_group_name = with_expected.channel_group_name
       AND q2b_source.state = with_expected.state
      LEFT JOIN state_channel_binds
        ON state_channel_binds.channel_group_name = with_expected.channel_group_name
       AND state_channel_binds.state = with_expected.state
      LEFT JOIN channel_binds
        ON channel_binds.channel_group_name = with_expected.channel_group_name
      LEFT JOIN channel_quotes_all
        ON channel_quotes_all.channel_group_name = with_expected.channel_group_name
      LEFT JOIN q2b_channel
        ON q2b_channel.channel_group_name = with_expected.channel_group_name
      LEFT JOIN q2b_state
        ON q2b_state.state = with_expected.state
    ),
    with_expected_binds AS (
      SELECT
        *,
        (expected_clicks * quote_rate * q2b_rate) AS expected_binds,
        MAX(IF(testing_point = 0, expected_clicks * quote_rate * q2b_rate, NULL))
          OVER (PARTITION BY channel_group_name, state) AS baseline_expected_binds,
        MAX(IF(testing_point = 0,
          SAFE_DIVIDE(
            expected_clicks * cpc,
            NULLIF(binds_state_channel, 0)
          ), NULL))
          OVER (PARTITION BY channel_group_name, state) AS baseline_expected_cpb
      FROM quote_rate_calc
    ),
    final_rows AS (
      SELECT
        channel_group_name,
        state,
        testing_point,
        opps,
        bids,
        win_rate,
        sold,
        binds_state_channel AS binds,
        quotes,
        click_to_quote,
        channel_quote,
        click_to_channel_quote,
        q2b,
        channel_binds,
        channel_q2b,
        cpc,
        avg_bid,
        win_rate_uplift_state,
        cpc_uplift_state,
        win_rate_uplift_channel,
        cpc_uplift_channel,
        performance,
        roe,
        combined_ratio,
        CASE
          WHEN testing_point = 0         THEN NULL
          WHEN stat_sig = 'disqualified' THEN NULL
          WHEN stat_sig = 'channel'      THEN win_rate_uplift_channel
          ELSE                                win_rate_uplift_state
        END AS win_rate_uplift,
        CASE
          WHEN testing_point = 0         THEN NULL
          WHEN stat_sig = 'disqualified' THEN NULL
          WHEN stat_sig = 'channel'      THEN cpc_uplift_channel
          ELSE                                cpc_uplift_state
        END AS cpc_uplift,
        CASE
          WHEN testing_point = 0         THEN 0
          WHEN stat_sig = 'disqualified' THEN 0
          ELSE (expected_clicks - COALESCE(baseline_expected_clicks, 0))
        END AS additional_clicks,
        CASE
          WHEN testing_point = 0         THEN 0
          WHEN stat_sig = 'disqualified' THEN 0
          ELSE (expected_binds - COALESCE(baseline_expected_binds, 0))
        END AS expected_bind_change,
        CASE
          WHEN testing_point = 0         THEN 0
          WHEN stat_sig = 'disqualified' THEN 0
          ELSE (expected_total_cost - COALESCE(baseline_expected_cost, 0))
        END AS additional_budget_needed,
        SAFE_DIVIDE(
          current_spend_channel_state,
          NULLIF(binds_state_channel, 0)
        ) AS current_cpb,
        SAFE_DIVIDE(
          expected_total_cost,
          NULLIF(
            COALESCE(binds_state_channel, 0) +
            CASE
              WHEN testing_point = 0         THEN 0
              WHEN stat_sig = 'disqualified' THEN 0
              ELSE (expected_binds - COALESCE(baseline_expected_binds, 0))
            END,
            0
          )
        ) AS expected_cpb,
        CASE
          WHEN testing_point = 0         THEN NULL
          WHEN stat_sig = 'disqualified' THEN NULL
          ELSE SAFE_DIVIDE(
            SAFE_DIVIDE(
              expected_total_cost,
              NULLIF(
                COALESCE(binds_state_channel, 0) +
                (expected_binds - COALESCE(baseline_expected_binds, 0)),
                0
              )
            ) - baseline_expected_cpb,
            NULLIF(baseline_expected_cpb, 0)
          )
        END AS cpb_uplift,
        stat_sig,
        CASE
          WHEN testing_point = 0         THEN 'baseline'
          WHEN stat_sig = 'disqualified' THEN 'disqualified'
          WHEN stat_sig = 'channel'      THEN 'channel only'
          ELSE                                'channel & state'
        END AS stat_sig_source
      FROM with_expected_binds
    ),
    final_rows_scoped AS (
      SELECT
        final_rows.* EXCEPT (roe, combined_ratio),
        ${buildRoeSql({
          zeroConditions: [
            "final_rows.expected_cpb IS NULL",
            "state_channel_financials.avg_equity IS NULL",
            "state_channel_financials.avg_equity = 0"
          ],
          avgProfitExpr: "state_channel_financials.avg_profit",
          cpbExpr: "final_rows.expected_cpb",
          avgEquityExpr: "state_channel_financials.avg_equity",
          qbcExpr: "0"
        })} AS roe,
        ${buildCombinedRatioSql({
          zeroConditions: [
            "final_rows.expected_cpb IS NULL",
            "state_channel_financials.avg_lifetime_premium IS NULL",
            "state_channel_financials.avg_lifetime_premium = 0"
          ],
          cpbExpr: "final_rows.expected_cpb",
          avgLifetimeCostExpr: "state_channel_financials.avg_lifetime_cost",
          avgLifetimePremiumExpr: "state_channel_financials.avg_lifetime_premium",
          qbcExpr: "0"
        })} AS combined_ratio,
        -- Raw financial columns for ROE/COR recomputation at PG query time
        state_channel_financials.avg_profit AS scf_avg_profit,
        state_channel_financials.avg_equity AS scf_avg_equity,
        state_channel_financials.avg_lifetime_premium AS scf_avg_lifetime_premium,
        state_channel_financials.avg_lifetime_cost AS scf_avg_lifetime_cost,
        ssd_metrics.ssd_binds,
        ssd_metrics.ssd_quotes,
        ssd_metrics.ssd_q2b,
        ssd_metrics.ssd_performance,
        ssd_metrics.ssd_roe,
        ssd_metrics.ssd_combined_ratio
      FROM final_rows
      LEFT JOIN state_channel_financials
        ON state_channel_financials.channel_group_name = final_rows.channel_group_name
       AND state_channel_financials.state = final_rows.state
      LEFT JOIN ssd_metrics
        ON ssd_metrics.channel_group_name = final_rows.channel_group_name
       AND ssd_metrics.state = final_rows.state
    ),
    ranked_rows AS (
      SELECT
        *,
        FIRST_VALUE(testing_point) OVER (
          PARTITION BY channel_group_name, state
          ORDER BY
            CASE
              WHEN testing_point != 0
                AND stat_sig != 'disqualified'
                AND cpb_uplift IS NOT NULL
                AND cpb_uplift <= 0.10
                AND additional_clicks > 0
                THEN 0
              WHEN testing_point = 0 THEN 1
              ELSE 2
            END,
            CASE
              WHEN testing_point != 0
                AND stat_sig != 'disqualified'
                AND cpb_uplift IS NOT NULL
                AND cpb_uplift <= 0.10
                AND additional_clicks > 0
                THEN additional_clicks
              ELSE -1e18
            END DESC,
            testing_point
        ) AS recommended_testing_point
      FROM final_rows_scoped
    )
    SELECT
      channel_group_name,
      state,
      testing_point,
      opps,
      bids,
      win_rate,
      sold,
      binds,
      quotes,
      click_to_quote,
      channel_quote,
      click_to_channel_quote,
      q2b,
      channel_binds,
      channel_q2b,
      cpc,
      avg_bid,
      win_rate_uplift_state,
      cpc_uplift_state,
      win_rate_uplift_channel,
      cpc_uplift_channel,
      win_rate_uplift,
      cpc_uplift,
      additional_clicks,
      expected_bind_change,
      additional_budget_needed,
      current_cpb,
      expected_cpb,
      cpb_uplift,
      performance,
      roe,
      combined_ratio,
      recommended_testing_point,
      stat_sig,
      CAST(NULL AS STRING) AS stat_sig_channel_group,
      stat_sig_source,
      scf_avg_profit,
      scf_avg_equity,
      scf_avg_lifetime_premium,
      scf_avg_lifetime_cost,
      ssd_binds,
      ssd_quotes,
      ssd_q2b,
      ssd_performance,
      ssd_roe,
      ssd_combined_ratio,
      CURRENT_TIMESTAMP() AS refreshed_at
    FROM ranked_rows
    ORDER BY channel_group_name, state, testing_point
  `;
}

/**
 * Sync PE computed data for a given activity/lead scope.
 * Uses BQ query (not streaming) since the PE CTE is parameterized.
 */
async function syncPeComputedForScope(
  scope: string,
  activityType: string,
  leadType: string
): Promise<SyncTableResult> {
  const t0 = Date.now();
  try {
    const sql = buildPeSyncSql(activityType, leadType);
    const stream = bigquery.createQueryStream({ query: sql, useLegacySql: false });
    let batch: Record<string, unknown>[] = [];
    let totalRows = 0;

    for await (const row of stream) {
      const r = row as Record<string, unknown>;
      r.activity_lead_type = scope;
      batch.push(r);
      if (batch.length >= BATCH_SIZE) {
        await pgExec(buildInsertBatch("pe_computed_daily", PE_COMPUTED_DAILY_COLS, batch));
        totalRows += batch.length;
        batch = [];
      }
    }
    if (batch.length > 0) {
      await pgExec(buildInsertBatch("pe_computed_daily", PE_COMPUTED_DAILY_COLS, batch));
      totalRows += batch.length;
    }

    return { table: `pe_computed_daily[${scope}]`, rows: totalRows, ms: Date.now() - t0 };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`PE sync error for scope ${scope}:`, message);
    return { table: `pe_computed_daily[${scope}]`, rows: 0, ms: Date.now() - t0, error: message };
  }
}

/**
 * Sync PE computed data for all active plan scopes.
 * Truncates once, then inserts per scope.
 */
async function syncPeComputed(): Promise<SyncTableResult[]> {
  const t0 = Date.now();
  const results: SyncTableResult[] = [];

  try {
    // Determine which scopes to sync by looking at active plans
    const { pgQuery } = await import("../db/postgres.js");
    const plans = await pgQuery<{ plan_context_json: string }>(
      `SELECT p.plan_context_json
       FROM plans p
       WHERE p.status != 'archived'
         AND p.plan_context_json IS NOT NULL`
    );

    const scopes = new Set<string>();
    for (const plan of plans) {
      try {
        const ctx = JSON.parse(plan.plan_context_json || "{}");
        const activity = String(ctx.activity || "clicks");
        const lt = String(ctx.leadType || "auto");
        const alt = ctx.activityLeadType ? String(ctx.activityLeadType) : `${activity}_${lt}`;
        scopes.add(alt);
      } catch { /* skip malformed */ }
    }

    // Always include the default scope
    if (scopes.size === 0) scopes.add("clicks_auto");

    // Truncate once before syncing all scopes
    await pgExec("TRUNCATE pe_computed_daily");

    // Sync each scope sequentially
    for (const scope of scopes) {
      const parts = splitCombinedFilter(scope);
      const result = await syncPeComputedForScope(scope, parts.activityType, parts.leadType);
      results.push(result);
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    results.push({ table: "pe_computed_daily", rows: 0, ms: Date.now() - t0, error: message });
  }

  return results;
}

/**
 * Sync plan-merged data from BQ fn_plan_merged_agg() stored procedure.
 */
async function syncPlanMerged(): Promise<SyncTableResult> {
  const t0 = Date.now();
  try {
    const sql = `
      SELECT
        start_date,
        end_date,
        channel_group_name,
        state,
        segment,
        price_adjustment_percent,
        stat_sig,
        stat_sig_channel_group,
        cpc_uplift,
        win_rate_uplift,
        additional_clicks,
        expected_total_clicks,
        expected_cpc,
        expected_total_cost,
        expected_total_binds,
        additional_expected_binds,
        expected_cpb,
        ss_performance,
        expected_performance,
        performance_uplift,
        CURRENT_TIMESTAMP() AS refreshed_at
      FROM ${bqAnalyticsRoutine("fn_plan_merged_agg")}(
        DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY),
        CURRENT_DATE()
      )
    `;

    await pgExec("TRUNCATE plan_merged_daily");

    const stream = bigquery.createQueryStream({ query: sql, useLegacySql: false });
    let batch: Record<string, unknown>[] = [];
    let totalRows = 0;

    for await (const row of stream) {
      batch.push(row as Record<string, unknown>);
      if (batch.length >= BATCH_SIZE) {
        await pgExec(buildInsertBatch("plan_merged_daily", PLAN_MERGED_DAILY_COLS, batch));
        totalRows += batch.length;
        batch = [];
      }
    }
    if (batch.length > 0) {
      await pgExec(buildInsertBatch("plan_merged_daily", PLAN_MERGED_DAILY_COLS, batch));
      totalRows += batch.length;
    }

    return { table: "plan_merged_daily", rows: totalRows, ms: Date.now() - t0 };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error("Plan-merged sync error:", message);
    return { table: "plan_merged_daily", rows: 0, ms: Date.now() - t0, error: message };
  }
}

// ── Public API ───────────────────────────────────────────────────────

export async function syncAllFromBQ(): Promise<SyncResult> {
  const t0 = Date.now();

  // Run sequentially to halve peak memory usage
  const ssd = await syncTable(
    "state_segment_daily",
    `SELECT
       event_date, state, segment, channel_group_name, activity_type,
       lead_type, bids, sold, total_cost, quote_started, quotes,
       binds, scored_policies, target_cpb_sum, lifetime_premium_sum,
       lifetime_cost_sum, avg_profit_sum, avg_equity_sum, avg_mrltv_sum,
       CURRENT_TIMESTAMP() AS refreshed_at
     FROM ${bqTable("state_segment_daily")}
     WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)`
  );

  // Sync targets_perf_daily directly from the raw cross-tactic table.
  // This bypasses the BQ stored procedure / intermediate BQ table.
  const tpd = await syncTable(
    "targets_perf_daily",
    `SELECT
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
     FROM ${config.rawCrossTacticTable}
     WHERE DATE(COALESCE(createdate_utc, Data_DateCreated, DateCreated))
           >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
       AND Data_State IS NOT NULL
     GROUP BY 1, 2, 3, 4, 5, 6, 7`
  );

  // Sync price_exploration_daily from BQ pre-aggregated table
  const ped = await syncTable(
    "price_exploration_daily",
    `SELECT
       date, channel_group_name, state, price_adjustment_percent,
       opps, bids, total_impressions, avg_position, sold,
       win_rate, avg_bid, cpc, total_spend, click_to_quote,
       quote_start_rate, number_of_quote_started, number_of_quotes,
       stat_sig, stat_sig_channel_group, cpc_uplift,
       cpc_uplift_channelgroup, win_rate_uplift,
       win_rate_uplift_channelgroup, additional_clicks,
       CURRENT_TIMESTAMP() AS refreshed_at
     FROM ${bqTable("price_exploration_daily")}
     WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)`
  );

  // Shadow-mode: sync PE computed and plan-merged tables
  const peComputedResults = await syncPeComputed();
  const planMergedResult = await syncPlanMerged();

  const results = [ssd, tpd, ped, ...peComputedResults, planMergedResult];
  const ok = results.every((r) => !r.error);
  return { ok, totalMs: Date.now() - t0, tables: results };
}
