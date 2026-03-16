/**
 * Daily sync job: BQ → PostgreSQL.
 *
 * Pulls analytics data from BigQuery pre-aggregated tables
 * and streams it into the PostgreSQL runtime database.
 *
 * Key design:
 *   - Fire-and-forget: POST /admin/sync-from-bq returns 202 immediately.
 *     The sync runs as a background async task — no HTTP timeout dependency.
 *   - COPY protocol: streams CSV directly to PG storage via pg-copy-streams,
 *     bypassing SQL parsing. 20-50x faster than INSERT batching.
 *   - Parallel: all 3 tables sync concurrently (~50KB total memory).
 *   - Atomic table swap: staging table + RENAME. Readers never see partial data.
 *   - Progress tracking: GET /admin/sync-status returns per-table row counts.
 *
 * Tables synced (90-day window):
 *   - state_segment_daily        (~1.04M rows)
 *   - targets_perf_daily         (~389K rows)
 *   - price_exploration_daily    (~3.19M rows)
 */

import { from as copyFrom } from "pg-copy-streams";
import { Storage } from "@google-cloud/storage";
import { bigquery, table as bqTable } from "../db/bigquery.js";
import { pgExec, pgWithRawClient, pgTransaction } from "../db/postgres.js";
import { cacheClear } from "../cache.js";
import { snapshotSuggestedCpb } from "./snapshotSuggestedCpb.js";
import { config } from "../config.js";

const storage = new Storage();
const SYNC_BUCKET = "beacon-lab-sync";

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
  "date", "channel_group_name", "state", "activity_type", "lead_type",
  "price_adjustment_percent",
  "opps", "bids", "total_impressions", "avg_position", "sold",
  "win_rate", "avg_bid", "cpc", "total_spend", "click_to_quote",
  "quote_start_rate", "number_of_quote_started", "number_of_quotes",
  "number_of_binds",
  "stat_sig", "stat_sig_channel_group", "cpc_uplift",
  "cpc_uplift_channelgroup", "win_rate_uplift",
  "win_rate_uplift_channelgroup", "additional_clicks", "refreshed_at",
] as const;

// ── Index definitions (recreated AFTER atomic swap for speed) ─────────

const TABLE_INDEXES: Record<string, string[]> = {
  state_segment_daily: [
    "CREATE INDEX IF NOT EXISTS idx_ssd_event_date ON state_segment_daily (event_date, activity_type, lead_type)",
    "CREATE INDEX IF NOT EXISTS idx_ssd_state_segment ON state_segment_daily (state, segment)",
  ],
  targets_perf_daily: [
    "CREATE INDEX IF NOT EXISTS idx_tpd_event_date ON targets_perf_daily (event_date, activity_type, lead_type)",
    "CREATE INDEX IF NOT EXISTS idx_tpd_state_segment ON targets_perf_daily (state, segment)",
  ],
  price_exploration_daily: [
    "CREATE INDEX IF NOT EXISTS idx_ped_date ON price_exploration_daily (date)",
    "CREATE INDEX IF NOT EXISTS idx_ped_state_channel ON price_exploration_daily (state, channel_group_name)",
    "CREATE INDEX IF NOT EXISTS idx_ped_activity ON price_exploration_daily (activity_type, lead_type)",
  ],
};

// ── CSV helpers for COPY protocol ────────────────────────────────────

/** Convert a single value to CSV format for PG COPY. */
function csvValue(value: unknown): string {
  if (value === null || value === undefined) return "\\N";

  // BQ date/timestamp objects have a .value property
  if (typeof value === "object" && value !== null && "value" in value) {
    return csvValue((value as { value: unknown }).value);
  }

  // Date objects with toISOString
  if (
    typeof value === "object" &&
    value !== null &&
    typeof (value as { toISOString?: unknown }).toISOString === "function"
  ) {
    return (value as { toISOString: () => string }).toISOString();
  }

  if (typeof value === "number" || typeof value === "bigint") return String(value);
  if (typeof value === "boolean") return value ? "t" : "f";

  const str = String(value);
  // Must quote if contains comma, double-quote, newline, or backslash
  if (str.includes(",") || str.includes('"') || str.includes("\n") || str.includes("\\")) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

// ── Per-table sync (GCS export + COPY + atomic swap) ─────────────────

/**
 * Syncs a table from BQ to PG via GCS CSV export:
 *
 *   BQ EXPORT DATA → GCS (CSV shards) → stream → PG COPY FROM STDIN
 *
 * Why GCS export instead of BQ API pagination:
 *   - BQ writes CSV to GCS at ~100MB/s (seconds, not minutes)
 *   - GCS download → PG COPY is a simple stream pipe (no pagination hang)
 *   - Total: ~15s export + ~30-60s COPY = under 2 min per table
 */
async function syncTable(
  tableName: string,
  bqSql: string,
  cols?: readonly string[],
): Promise<SyncTableResult> {
  const t0 = Date.now();
  const stagingTable = `${tableName}_new`;
  const oldTable = `${tableName}_old`;
  const gcsPrefix = `sync/${tableName}/${t0}`;

  try {
    const colDefs = cols
      ?? (tableName === "targets_perf_daily" ? TARGETS_PERF_DAILY_COLS
        : tableName === "price_exploration_daily" ? PRICE_EXPLORATION_DAILY_COLS
        : STATE_SEGMENT_DAILY_COLS);

    // 1. Create UNLOGGED staging table WITHOUT indexes
    await pgExec(`DROP TABLE IF EXISTS ${stagingTable}`);
    await pgExec(`CREATE UNLOGGED TABLE ${stagingTable} (LIKE ${tableName})`);

    syncStatus.progress[tableName] = { rows: 0, done: false };

    // 2. Export BQ query results to GCS as CSV (no header)
    //    BQ writes CSV shards in parallel — typically finishes in 10-20s
    console.log(`[sync] ${tableName}: exporting to GCS...`);
    const exportSql = `EXPORT DATA OPTIONS(
      uri='gs://${SYNC_BUCKET}/${gcsPrefix}/*.csv',
      format='CSV',
      overwrite=true,
      header=false
    ) AS ${bqSql}`;
    await bigquery.query({ query: exportSql, useLegacySql: false });
    console.log(`[sync] ${tableName}: GCS export done (${Date.now() - t0}ms)`);

    // 3. List exported CSV shard files
    const [files] = await storage.bucket(SYNC_BUCKET).getFiles({ prefix: gcsPrefix + "/" });
    const csvFiles = files.filter((f) => f.name.endsWith(".csv"));
    console.log(`[sync] ${tableName}: ${csvFiles.length} CSV shards, streaming to PG...`);

    // 4. Stream each CSV shard into PG via COPY
    //    NULLs in BQ CSV export appear as empty fields → NULL '' in COPY
    let rowCount = 0;
    await pgWithRawClient(async (client) => {
      const copySql = `COPY ${stagingTable} (${colDefs.join(", ")}) FROM STDIN WITH (FORMAT csv, NULL '')`;
      const pgStream = client.query(copyFrom(copySql));

      for (const file of csvFiles) {
        await new Promise<void>((resolve, reject) => {
          const gcsStream = file.createReadStream();
          gcsStream.on("data", (chunk: Buffer) => {
            // Count newlines for progress tracking
            for (let i = 0; i < chunk.length; i++) {
              if (chunk[i] === 10) rowCount++; // 10 = '\n'
            }
            if (!pgStream.write(chunk)) {
              gcsStream.pause();
              pgStream.once("drain", () => gcsStream.resume());
            }
          });
          gcsStream.on("end", resolve);
          gcsStream.on("error", reject);
        });
        syncStatus.progress[tableName] = { rows: rowCount, done: false };
        console.log(`[sync] ${tableName}: ${rowCount.toLocaleString()} rows streamed`);
      }

      // Signal end-of-data and wait for PG to finish
      await new Promise<void>((resolve, reject) => {
        pgStream.on("finish", resolve);
        pgStream.on("error", reject);
        pgStream.end();
      });
      console.log(`[sync] ${tableName}: COPY completed, ${rowCount.toLocaleString()} rows`);
    });

    syncStatus.progress[tableName] = { rows: rowCount, done: true };

    // 5. Atomic swap: staging → live
    await pgTransaction(async (exec) => {
      await exec(`DROP TABLE IF EXISTS ${oldTable}`);
      await exec(`ALTER TABLE ${tableName} RENAME TO ${oldTable}`);
      await exec(`ALTER TABLE ${stagingTable} RENAME TO ${tableName}`);
    });

    // 6. Recreate indexes (after swap — no index overhead during COPY)
    const indexes = TABLE_INDEXES[tableName] ?? [];
    for (const ddl of indexes) {
      await pgExec(ddl);
    }
    if (indexes.length > 0) {
      console.log(`[sync] ${tableName}: ${indexes.length} indexes recreated`);
    }

    // 7. Make table LOGGED again (for crash recovery in production)
    await pgExec(`ALTER TABLE ${tableName} SET LOGGED`);

    // 8. Cleanup: drop old table + delete GCS shards (non-critical)
    await pgExec(`DROP TABLE IF EXISTS ${oldTable}`);
    try {
      await storage.bucket(SYNC_BUCKET).deleteFiles({ prefix: gcsPrefix + "/" });
    } catch { /* non-critical */ }

    return { table: tableName, rows: rowCount, ms: Date.now() - t0 };
  } catch (err) {
    // Cleanup staging table on failure
    try { await pgExec(`DROP TABLE IF EXISTS ${stagingTable}`); } catch { /* ignore */ }
    try { await storage.bucket(SYNC_BUCKET).deleteFiles({ prefix: gcsPrefix + "/" }); } catch { /* ignore */ }
    const message = err instanceof Error ? err.message : String(err);
    console.error(`[sync] ${tableName} failed:`, message);
    return { table: tableName, rows: 0, ms: Date.now() - t0, error: message };
  }
}

// ── Public API ───────────────────────────────────────────────────────

export async function syncAllFromBQ(): Promise<SyncResult> {
  const t0 = Date.now();

  // Run all 3 tables in parallel — each gets its own GCS prefix + PG client
  console.log("[sync] starting parallel sync (GCS export → COPY)...");

  const [ssd, tpd, ped] = await Promise.all([
  syncTable(
    "state_segment_daily",
    `SELECT
       event_date, state, segment, channel_group_name, activity_type,
       lead_type, bids, sold, total_cost, quote_started, quotes,
       binds, scored_policies, target_cpb_sum, lifetime_premium_sum,
       lifetime_cost_sum, avg_profit_sum, avg_equity_sum, avg_mrltv_sum,
       CURRENT_TIMESTAMP() AS refreshed_at
     FROM ${bqTable("state_segment_daily")}
     WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)`
  ),

  syncTable(
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
  ),

  syncTable(
    "price_exploration_daily",
    `SELECT
       date, channel_group_name, state, activity_type, lead_type,
       price_adjustment_percent,
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
  ),
  ]);

  const results = [ssd, tpd, ped];
  const ok = results.every((r) => !r.error);
  return { ok, totalMs: Date.now() - t0, tables: results };
}
