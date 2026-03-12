/**
 * Daily sync job: BQ → PostgreSQL.
 *
 * Pulls analytics data from BigQuery pre-aggregated tables
 * and streams it into the PostgreSQL runtime database.
 *
 * Tables synced:
 *   - state_segment_daily
 *   - targets_perf_daily
 *   - price_exploration_daily
 *   - pe_computed_daily      (shadow-mode: calls getPriceExplorationBQ, stores output)
 *   - plan_merged_daily      (shadow-mode: fn_plan_merged_agg output)
 */

import { bigquery, table as bqTable, analyticsRoutine as bqAnalyticsRoutine } from "../db/bigquery.js";
import { pgExec } from "../db/postgres.js";
import { config } from "../config.js";
import { splitCombinedFilter } from "../services/shared/activityScope.js";
import { cacheClear } from "../cache.js";

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

/** Insert rows from an in-memory array into PG in batches of BATCH_SIZE. */
async function insertRowsBatched(
  tableName: string,
  cols: readonly string[],
  rows: Record<string, unknown>[]
): Promise<void> {
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    await pgExec(buildInsertBatch(tableName, cols, batch));
  }
}

// ── Per-table sync (streaming from BQ) ──────────────────────────────

/**
 * Streams rows from BQ and inserts into PG in small batches.
 * Never holds more than BATCH_SIZE rows in memory at once.
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
 * Sync PE computed data by calling getPriceExplorationBQ() from analyticsService.
 * This uses the same cached BQ query the API uses — fast if cache is warm.
 * Stores fully-computed rows (ROE/COR at qbc=0) for comparison.
 */
async function syncPeComputed(): Promise<SyncTableResult[]> {
  const t0 = Date.now();
  const results: SyncTableResult[] = [];

  try {
    // Lazy import to avoid circular deps at module load time
    const { getPriceExplorationBQ, normalizePriceExplorationFilters } =
      await import("../services/analyticsService.js");

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
    if (scopes.size === 0) scopes.add("clicks_auto");

    // Truncate once before syncing all scopes
    await pgExec("TRUNCATE pe_computed_daily");

    for (const scope of scopes) {
      const scopeT0 = Date.now();
      try {
        const normalized = normalizePriceExplorationFilters({
          activityLeadType: scope,
          qbc: 0,
          limit: 200000,
          topPairs: 0,
        });

        // This calls the cached BQ CTE — instant if warm, ~30s if cold
        const rows = await getPriceExplorationBQ(normalized);

        if (!rows.length) {
          results.push({ table: `pe_computed_daily[${scope}]`, rows: 0, ms: Date.now() - scopeT0 });
          continue;
        }

        // Tag rows with scope and add refreshed_at
        const now = new Date().toISOString();
        const tagged = rows.map((row) => ({
          ...row,
          activity_lead_type: scope,
          refreshed_at: now,
        }));

        await insertRowsBatched("pe_computed_daily", PE_COMPUTED_DAILY_COLS, tagged as unknown as Record<string, unknown>[]);
        results.push({ table: `pe_computed_daily[${scope}]`, rows: tagged.length, ms: Date.now() - scopeT0 });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`PE sync error for scope ${scope}:`, message);
        results.push({ table: `pe_computed_daily[${scope}]`, rows: 0, ms: Date.now() - scopeT0, error: message });
      }
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    results.push({ table: "pe_computed_daily", rows: 0, ms: Date.now() - t0, error: message });
  }

  return results;
}

// ── Plan-Merged sync (shadow-mode) ──────────────────────────────────

/**
 * Sync plan-merged data from BQ fn_plan_merged_agg() stored procedure.
 * Streams rows from BQ in batches.
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

  // Clear in-memory BQ cache so next API requests get fresh data
  cacheClear();

  const results = [ssd, tpd, ped, ...peComputedResults, planMergedResult];
  const ok = results.every((r) => !r.error);
  return { ok, totalMs: Date.now() - t0, tables: results };
}
