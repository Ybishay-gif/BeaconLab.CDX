/**
 * Unified DB layer.
 *
 * When USE_PG=true, all runtime queries go to PostgreSQL.
 * BQ functions are still exported for the daily sync job.
 *
 * SQL compatibility notes (BQ → PG):
 *  - UNNEST(@arr)                → ANY($n)          (handled in service SQL)
 *  - GENERATE_UUID()             → gen_random_uuid()
 *  - CURRENT_TIMESTAMP()         → NOW()
 *  - SAFE_DIVIDE(a, b)           → CASE WHEN b=0 THEN NULL ELSE a/b END
 *  - MERGE ... WHEN MATCHED      → INSERT ... ON CONFLICT DO UPDATE
 *  - `project.dataset.table`     → just table name
 *  - Named params @foo           → auto-converted to $1 by pgQuery
 */

import { config } from "../config.js";
import { query as bqQuery, table as bqTable, analyticsTable as bqAnalyticsTableFn, analyticsRoutine as bqAnalyticsRoutineFn } from "./bigquery.js";
import { pgQuery as pgQueryFn } from "./postgres.js";

/**
 * query<T>() — runs against PG or BQ depending on USE_PG flag.
 */
export async function query<T>(sql: string, params: Record<string, unknown> = {}): Promise<T[]> {
  if (config.usePg) {
    return pgQueryFn<T & Record<string, unknown>>(sql, params);
  }
  return bqQuery<T>(sql, params);
}

/**
 * table() — returns just the table name for PG, or fully-qualified for BQ.
 */
export function table(tableName: string): string {
  if (config.usePg) {
    return tableName;
  }
  return bqTable(tableName);
}

/**
 * analyticsTable() — for PG, analytics tables live in the same schema.
 */
export function analyticsTable(tableName: string): string {
  if (config.usePg) {
    return tableName;
  }
  return bqAnalyticsTableFn(tableName);
}

/**
 * analyticsRoutine() — PG doesn't use BQ routines; returns plain name.
 */
export function analyticsRoutine(routineName: string): string {
  if (config.usePg) {
    return routineName;
  }
  return bqAnalyticsRoutineFn(routineName);
}

// Re-export BQ-specific functions for the sync job
export { query as bqQuery, table as bqTable, analyticsTable as bqAnalyticsTable, analyticsRoutine as bqAnalyticsRoutine } from "./bigquery.js";
export { pgQuery, pgExec, pgTransaction, pgClose } from "./postgres.js";
