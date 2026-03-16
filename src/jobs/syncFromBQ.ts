/**
 * Daily sync job: BQ → PostgreSQL via Cloud SQL Import.
 *
 * Uses Cloud SQL's native CSV import for server-side data loading.
 * No streaming through the application — Cloud SQL pulls directly from GCS.
 *
 * Flow per table:
 *   1. BQ query → temp BQ table (with 90-day filter + aggregations)
 *   2. BQ extract → single CSV in GCS (no header)
 *   3. Cloud SQL Import CSV → PG staging table (server-side, ~seconds)
 *   4. Atomic swap: staging → live table
 *   5. Recreate indexes, SET LOGGED, cleanup
 *
 * Tables synced (90-day window):
 *   - state_segment_daily        (~1.04M rows)
 *   - targets_perf_daily         (~389K rows)
 *   - price_exploration_daily    (~3.19M rows)
 */

import { Storage } from "@google-cloud/storage";
import { GoogleAuth } from "google-auth-library";
import { bigquery, table as bqTable } from "../db/bigquery.js";
import { pgExec, pgTransaction } from "../db/postgres.js";
import { cacheClear } from "../cache.js";
import { snapshotSuggestedCpb } from "./snapshotSuggestedCpb.js";
import { config } from "../config.js";

const storage = new Storage();
const auth = new GoogleAuth({ scopes: ["https://www.googleapis.com/auth/cloud-platform"] });

const SYNC_BUCKET = "beacon-lab-sync";
const CLOUD_SQL_INSTANCE = "beacon-lab-db";
const PG_DATABASE = "beacon_lab";
const PG_USER = "beacon";

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
  progress: Record<string, { rows: number; done: boolean; phase?: string }>;
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
      cacheClear();
      console.log(`[sync] completed in ${result.totalMs}ms — cache cleared`);
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

// ── BQ helpers ─────────────────────────────────────────────────────────

/** Poll a BQ job until it completes. Throws on error. */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function waitForBqJob(job: any): Promise<void> {
  while (true) {
    const [meta] = await job.getMetadata();
    if (meta.status?.state === "DONE") {
      if (meta.status?.errorResult) {
        throw new Error(meta.status.errorResult.message);
      }
      return;
    }
    await new Promise(r => setTimeout(r, 1000));
  }
}

/** Run a BQ query and write results to a temp table. Returns row count. */
async function bqQueryToTemp(tableName: string, sql: string): Promise<number> {
  const tmpTableId = `_sync_tmp_${tableName}`;
  const dataset = bigquery.dataset(config.dataset);

  const [job] = await bigquery.createQueryJob({
    query: sql,
    destination: dataset.table(tmpTableId),
    writeDisposition: "WRITE_TRUNCATE",
    useLegacySql: false,
  });
  await waitForBqJob(job);

  const [meta] = await dataset.table(tmpTableId).getMetadata();
  return parseInt(meta.numRows || "0", 10);
}

/** Extract a BQ temp table to a single CSV in GCS (no header). */
async function bqExtractToGcs(tableName: string): Promise<string> {
  const tmpTableId = `_sync_tmp_${tableName}`;
  const gcsUri = `gs://${SYNC_BUCKET}/sync/${tableName}.csv`;

  const [job] = await bigquery.createJob({
    configuration: {
      extract: {
        sourceTable: {
          projectId: config.projectId,
          datasetId: config.dataset,
          tableId: tmpTableId,
        },
        destinationUris: [gcsUri],
        destinationFormat: "CSV",
        printHeader: false,
      },
    },
  });
  await waitForBqJob(job);
  return gcsUri;
}

// ── Cloud SQL Import ───────────────────────────────────────────────────

/** Import a CSV from GCS into PG via Cloud SQL Admin API (server-side). */
async function cloudSqlImportCsv(
  gcsUri: string,
  table: string,
  columns: readonly string[],
): Promise<void> {
  const client = await auth.getClient();

  const res = await client.request({
    url: `https://sqladmin.googleapis.com/v1/projects/${config.projectId}/instances/${CLOUD_SQL_INSTANCE}/import`,
    method: "POST",
    data: {
      importContext: {
        fileType: "CSV",
        uri: gcsUri,
        database: PG_DATABASE,
        importUser: PG_USER,
        csvImportOptions: {
          table,
          columns: [...columns],
        },
      },
    },
  });

  // Poll the operation until complete
  const opName = (res.data as Record<string, unknown>).name as string;
  while (true) {
    const opRes = await client.request({
      url: `https://sqladmin.googleapis.com/v1/projects/${config.projectId}/operations/${opName}`,
      method: "GET",
    });
    const op = opRes.data as Record<string, unknown>;
    if (op.status === "DONE") {
      if (op.error) {
        const errors = (op.error as Record<string, unknown>).errors;
        throw new Error(`Cloud SQL import failed: ${JSON.stringify(errors)}`);
      }
      return;
    }
    await new Promise(r => setTimeout(r, 2000));
  }
}

// ── Per-table sync ─────────────────────────────────────────────────────

async function syncTable(
  tableName: string,
  bqSql: string,
  cols: readonly string[],
): Promise<SyncTableResult> {
  const t0 = Date.now();
  const stagingTable = `${tableName}_new`;
  const oldTable = `${tableName}_old`;

  try {
    // 1. BQ query → temp BQ table
    syncStatus.progress[tableName] = { rows: 0, done: false, phase: "query" };
    console.log(`[sync] ${tableName}: BQ query → temp table...`);
    const rowCount = await bqQueryToTemp(tableName, bqSql);
    console.log(`[sync] ${tableName}: ${rowCount.toLocaleString()} rows queried (${Date.now() - t0}ms)`);

    // 2. BQ extract → single CSV in GCS
    syncStatus.progress[tableName] = { rows: rowCount, done: false, phase: "extract" };
    console.log(`[sync] ${tableName}: extracting to GCS...`);
    const gcsUri = await bqExtractToGcs(tableName);
    console.log(`[sync] ${tableName}: extract done (${Date.now() - t0}ms)`);

    // 3. Create PG staging table
    await pgExec(`DROP TABLE IF EXISTS ${stagingTable}`);
    await pgExec(`CREATE UNLOGGED TABLE ${stagingTable} (LIKE ${tableName})`);

    // 4. Cloud SQL import CSV → PG staging table
    syncStatus.progress[tableName] = { rows: rowCount, done: false, phase: "import" };
    console.log(`[sync] ${tableName}: Cloud SQL importing...`);
    await cloudSqlImportCsv(gcsUri, stagingTable, cols);
    console.log(`[sync] ${tableName}: import done (${Date.now() - t0}ms)`);

    // 5. Atomic swap: staging → live
    await pgTransaction(async (exec) => {
      await exec(`DROP TABLE IF EXISTS ${oldTable}`);
      await exec(`ALTER TABLE ${tableName} RENAME TO ${oldTable}`);
      await exec(`ALTER TABLE ${stagingTable} RENAME TO ${tableName}`);
    });

    // 6. Recreate indexes + SET LOGGED
    const indexes = TABLE_INDEXES[tableName] ?? [];
    for (const ddl of indexes) await pgExec(ddl);
    if (indexes.length > 0) console.log(`[sync] ${tableName}: ${indexes.length} indexes recreated`);
    await pgExec(`ALTER TABLE ${tableName} SET LOGGED`);

    // 7. Cleanup (non-critical)
    await pgExec(`DROP TABLE IF EXISTS ${oldTable}`);
    try { await storage.bucket(SYNC_BUCKET).file(`sync/${tableName}.csv`).delete(); } catch { /* ok */ }
    try { await bigquery.dataset(config.dataset).table(`_sync_tmp_${tableName}`).delete(); } catch { /* ok */ }

    syncStatus.progress[tableName] = { rows: rowCount, done: true, phase: "done" };
    console.log(`[sync] ${tableName}: complete — ${rowCount.toLocaleString()} rows in ${Date.now() - t0}ms`);
    return { table: tableName, rows: rowCount, ms: Date.now() - t0 };
  } catch (err) {
    try { await pgExec(`DROP TABLE IF EXISTS ${stagingTable}`); } catch { /* ignore */ }
    try { await storage.bucket(SYNC_BUCKET).file(`sync/${tableName}.csv`).delete(); } catch { /* ignore */ }
    try { await bigquery.dataset(config.dataset).table(`_sync_tmp_${tableName}`).delete(); } catch { /* ignore */ }
    const message = err instanceof Error ? err.message : String(err);
    console.error(`[sync] ${tableName} failed:`, message);
    return { table: tableName, rows: 0, ms: Date.now() - t0, error: message };
  }
}

// ── Public API ───────────────────────────────────────────────────────

export async function syncAllFromBQ(): Promise<SyncResult> {
  const t0 = Date.now();

  // Sequential: Cloud SQL only supports one import operation at a time
  console.log("[sync] starting sync (BQ → GCS → Cloud SQL Import)...");

  const ssd = await syncTable(
    "state_segment_daily",
    `SELECT
       event_date, state, segment, channel_group_name, activity_type,
       lead_type, bids, sold, total_cost, quote_started, quotes,
       binds, scored_policies, target_cpb_sum, lifetime_premium_sum,
       lifetime_cost_sum, avg_profit_sum, avg_equity_sum, avg_mrltv_sum,
       CURRENT_TIMESTAMP() AS refreshed_at
     FROM ${bqTable("state_segment_daily")}
     WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)`,
    STATE_SEGMENT_DAILY_COLS,
  );

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
       GROUP BY 1, 2, 3, 4, 5, 6, 7`,
    TARGETS_PERF_DAILY_COLS,
  );

  const ped = await syncTable(
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
     WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)`,
    PRICE_EXPLORATION_DAILY_COLS,
  );

  const results = [ssd, tpd, ped];
  const ok = results.every((r) => !r.error);
  return { ok, totalMs: Date.now() - t0, tables: results };
}
