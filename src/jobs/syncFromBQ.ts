/**
 * Daily sync job: BQ → PostgreSQL.
 *
 * Pulls analytics data from BigQuery pre-aggregated tables
 * and streams it into the PostgreSQL runtime database.
 *
 * Key design:
 *   - Fire-and-forget: POST /admin/sync-from-bq returns 202 immediately.
 *     The sync runs as a background async task — no HTTP timeout dependency.
 *   - True streaming: rows are flushed in batches of 2000, never buffering
 *     all rows in memory. Peak memory: ~400KB per table.
 *   - Atomic table swap: inserts go into a staging table (_new), then an
 *     atomic RENAME swaps it in. Readers never see partial data.
 *   - Progress tracking: GET /admin/sync-status returns per-table row counts.
 *
 * Tables synced:
 *   - state_segment_daily        (~1M rows / 90 days)
 *   - targets_perf_daily         (~400K rows / 90 days)
 *   - price_exploration_daily    (~3.2M rows / 90 days)
 */

import { bigquery, table as bqTable } from "../db/bigquery.js";
import { pgExec, pgWithClient, pgTransaction } from "../db/postgres.js";
import { cacheClear } from "../cache.js";
import { snapshotSuggestedCpb } from "./snapshotSuggestedCpb.js";
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

// ── Sync status (module-level singleton) ─────────────────────────────

type SyncStatus = {
  running: boolean;
  startedAt: string | null;
  completedAt: string | null;
  result: SyncResult | null;
  error: string | null;
  progress: Record<string, { rows: number; done: boolean }>;
};

let syncStatus: SyncStatus = {
  running: false,
  startedAt: null,
  completedAt: null,
  result: null,
  error: null,
  progress: {},
};

export function getSyncStatus(): SyncStatus {
  return { ...syncStatus, progress: { ...syncStatus.progress } };
}

/**
 * Kicks off the BQ→PG sync in the background.
 * Returns immediately — poll GET /admin/sync-status for progress.
 */
export function startSyncInBackground(): { started: boolean; message: string } {
  if (syncStatus.running) {
    return { started: false, message: "Sync already running" };
  }

  syncStatus = {
    running: true,
    startedAt: new Date().toISOString(),
    completedAt: null,
    result: null,
    error: null,
    progress: {},
  };

  // Fire and forget — no await
  syncAllFromBQ()
    .then(async (result) => {
      syncStatus.result = result;
      syncStatus.running = false;
      syncStatus.completedAt = new Date().toISOString();
      // Clear analytics cache now that fresh data is in PG
      cacheClear();
      console.log(`[sync] completed in ${result.totalMs}ms — cache cleared`);
      // Chain: snapshot suggested CPB after fresh data is available
      try {
        await snapshotSuggestedCpb();
        console.log(`[sync] suggested CPB snapshot complete`);
      } catch (e) {
        console.error(`[sync] suggested CPB snapshot failed:`, e);
      }
    })
    .catch((err) => {
      syncStatus.error = err instanceof Error ? err.message : String(err);
      syncStatus.running = false;
      syncStatus.completedAt = new Date().toISOString();
      console.error(`[sync] failed:`, err);
    });

  return { started: true, message: "Sync started in background" };
}

const BATCH_SIZE = 2000;

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
  "number_of_binds",
  "stat_sig", "stat_sig_channel_group", "cpc_uplift",
  "cpc_uplift_channelgroup", "win_rate_uplift",
  "win_rate_uplift_channelgroup", "additional_clicks", "refreshed_at",
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

// ── Per-table sync (true streaming + atomic swap) ────────────────────

/**
 * Streams rows from BQ into a PG staging table, then atomically swaps it in.
 *
 * Flow:
 *   1. CREATE staging table (tablename_new) with same schema + indexes
 *   2. Stream BQ rows, flushing in batches of 500 — never more than 500 rows in memory
 *   3. Atomic RENAME: old → _old, staging → live (inside a transaction)
 *   4. DROP the old table
 *
 * Readers always see either the complete old data or the complete new data.
 */
async function syncTable(
  tableName: string,
  bqSql: string,
  cols?: readonly string[],
): Promise<SyncTableResult> {
  const t0 = Date.now();
  const stagingTable = `${tableName}_new`;
  const oldTable = `${tableName}_old`;

  try {
    const colDefs = cols
      ?? (tableName === "targets_perf_daily" ? TARGETS_PERF_DAILY_COLS
        : tableName === "price_exploration_daily" ? PRICE_EXPLORATION_DAILY_COLS
        : STATE_SEGMENT_DAILY_COLS);

    // 1. Create staging table with same schema + indexes
    await pgExec(`DROP TABLE IF EXISTS ${stagingTable}`);
    await pgExec(`CREATE TABLE ${stagingTable} (LIKE ${tableName} INCLUDING INDEXES)`);

    // 2. Stream from BQ and flush in batches — never buffer all rows
    let rowCount = 0;
    syncStatus.progress[tableName] = { rows: 0, done: false };
    const stream = bigquery.createQueryStream({ query: bqSql, useLegacySql: false });
    console.log(`[sync] ${tableName}: starting BQ stream...`);

    await pgWithClient(async (exec) => {
      let batch: Record<string, unknown>[] = [];

      for await (const row of stream) {
        batch.push(row as Record<string, unknown>);
        if (batch.length >= BATCH_SIZE) {
          await exec(buildInsertBatch(stagingTable, colDefs, batch));
          rowCount += batch.length;
          syncStatus.progress[tableName] = { rows: rowCount, done: false };
          batch = [];
          if (rowCount % 50000 === 0) {
            console.log(`[sync] ${tableName}: ${rowCount.toLocaleString()} rows inserted...`);
          }
        }
      }
      // Flush remaining rows
      if (batch.length > 0) {
        await exec(buildInsertBatch(stagingTable, colDefs, batch));
        rowCount += batch.length;
      }
    });
    syncStatus.progress[tableName] = { rows: rowCount, done: true };
    console.log(`[sync] ${tableName}: ${rowCount.toLocaleString()} rows total, swapping...`);

    // 3. Atomic swap: staging → live
    await pgTransaction(async (exec) => {
      // Drop any leftover old table from a previous failed swap
      await exec(`DROP TABLE IF EXISTS ${oldTable}`);
      await exec(`ALTER TABLE ${tableName} RENAME TO ${oldTable}`);
      await exec(`ALTER TABLE ${stagingTable} RENAME TO ${tableName}`);
    });

    // 4. Cleanup: drop old table (outside transaction — non-critical)
    await pgExec(`DROP TABLE IF EXISTS ${oldTable}`);

    return { table: tableName, rows: rowCount, ms: Date.now() - t0 };
  } catch (err) {
    // Cleanup staging table on failure
    try { await pgExec(`DROP TABLE IF EXISTS ${stagingTable}`); } catch { /* ignore */ }
    const message = err instanceof Error ? err.message : String(err);
    return { table: tableName, rows: 0, ms: Date.now() - t0, error: message };
  }
}

// ── Public API ───────────────────────────────────────────────────────

export async function syncAllFromBQ(): Promise<SyncResult> {
  const t0 = Date.now();

  // Run sequentially to minimize peak memory usage
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
       number_of_binds,
       stat_sig, stat_sig_channel_group, cpc_uplift,
       cpc_uplift_channelgroup, win_rate_uplift,
       win_rate_uplift_channelgroup, additional_clicks,
       CURRENT_TIMESTAMP() AS refreshed_at
     FROM ${bqTable("price_exploration_daily")}
     WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)`
  );

  const results = [ssd, tpd, ped];
  const ok = results.every((r) => !r.error);
  return { ok, totalMs: Date.now() - t0, tables: results };
}
