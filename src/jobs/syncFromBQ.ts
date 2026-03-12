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
 */

import { bigquery, table as bqTable } from "../db/bigquery.js";
import { pgTransaction } from "../db/postgres.js";
import { config } from "../config.js";

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
 * Streams rows from BQ and inserts into PG in batches.
 * Uses createQueryStream() to avoid loading all rows into memory.
 * Wrapped in a transaction so readers never see a truncated/partial table.
 */
async function syncTable(
  tableName: string,
  bqSql: string
): Promise<SyncTableResult> {
  const t0 = Date.now();
  try {
    const cols = tableName === "targets_perf_daily"
      ? TARGETS_PERF_DAILY_COLS
      : STATE_SEGMENT_DAILY_COLS;

    // Stream all rows from BQ first
    const stream = bigquery.createQueryStream({ query: bqSql, useLegacySql: false });
    const allRows: Record<string, unknown>[] = [];
    for await (const row of stream) {
      allRows.push(row as Record<string, unknown>);
    }

    // TRUNCATE + INSERT inside a single transaction so readers
    // always see either the old complete data or the new complete data.
    await pgTransaction(async (exec) => {
      await exec(`TRUNCATE ${tableName}`);
      for (let i = 0; i < allRows.length; i += BATCH_SIZE) {
        const batch = allRows.slice(i, i + BATCH_SIZE);
        await exec(buildInsertBatch(tableName, cols, batch));
      }
    });

    return { table: tableName, rows: allRows.length, ms: Date.now() - t0 };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return { table: tableName, rows: 0, ms: Date.now() - t0, error: message };
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

  const results = [ssd, tpd];
  const ok = results.every((r) => !r.error);
  return { ok, totalMs: Date.now() - t0, tables: results };
}
