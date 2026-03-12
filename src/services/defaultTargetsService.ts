import { config } from "../config.js";
import { query, table } from "../db/index.js";
import { pgExec, pgTransaction } from "../db/postgres.js";

export const VALID_TARGET_KEYS = [
  "clicks_auto",
  "leads_auto",
  "calls_auto",
  "clicks_home",
  "leads_home",
  "calls_home",
] as const;

export type TargetKey = (typeof VALID_TARGET_KEYS)[number];

export type DefaultTargetRow = {
  id?: number;
  state: string;
  segment: string;
  source: string;
  target_value: number;
  account_id?: number;
  company_id?: number;
  original_id?: string;
  segment_name?: string;
  attributes?: string;
};

export type DefaultTargetSummary = {
  target_key: string;
  row_count: number;
};

/* ------------------------------------------------------------------ */
/*  Lazy table creation (PG only — BQ table must exist via schema)    */
/* ------------------------------------------------------------------ */

let tableReady: Promise<void> | null = null;

function ensureTable(): Promise<void> {
  if (!config.usePg) return Promise.resolve();
  if (!tableReady) {
    tableReady = pgExec(`
      CREATE TABLE IF NOT EXISTS default_targets (
        id SERIAL PRIMARY KEY,
        target_key TEXT NOT NULL,
        state TEXT NOT NULL,
        segment TEXT NOT NULL,
        source TEXT NOT NULL,
        target_value DOUBLE PRECISION NOT NULL,
        account_id INTEGER NOT NULL DEFAULT 0,
        company_id INTEGER NOT NULL DEFAULT 0,
        original_id TEXT NOT NULL DEFAULT '',
        segment_name TEXT NOT NULL DEFAULT '',
        attributes TEXT NOT NULL DEFAULT '',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `).then(() =>
      pgExec(
        `CREATE INDEX IF NOT EXISTS idx_default_targets_key ON default_targets (target_key)`
      )
    ).then(() =>
      Promise.all([
        pgExec(`ALTER TABLE default_targets ADD COLUMN IF NOT EXISTS account_id INTEGER NOT NULL DEFAULT 0`).catch(() => {}),
        pgExec(`ALTER TABLE default_targets ADD COLUMN IF NOT EXISTS company_id INTEGER NOT NULL DEFAULT 0`).catch(() => {}),
        pgExec(`ALTER TABLE default_targets ADD COLUMN IF NOT EXISTS original_id TEXT NOT NULL DEFAULT ''`).catch(() => {}),
        pgExec(`ALTER TABLE default_targets ADD COLUMN IF NOT EXISTS segment_name TEXT NOT NULL DEFAULT ''`).catch(() => {}),
        pgExec(`ALTER TABLE default_targets ADD COLUMN IF NOT EXISTS attributes TEXT NOT NULL DEFAULT ''`).catch(() => {}),
      ])
    ).then(() => undefined);
  }
  return tableReady;
}

/* ------------------------------------------------------------------ */
/*  Public API                                                        */
/* ------------------------------------------------------------------ */

export async function getDefaultTargetsSummary(): Promise<DefaultTargetSummary[]> {
  await ensureTable();
  const castExpr = config.usePg ? "COUNT(*)::int" : "CAST(COUNT(*) AS INT64)";
  return query<DefaultTargetSummary>(
    `SELECT target_key, ${castExpr} AS row_count
     FROM ${table("default_targets")}
     GROUP BY target_key
     ORDER BY target_key`
  );
}

export async function getDefaultTargets(key: TargetKey): Promise<DefaultTargetRow[]> {
  await ensureTable();
  const coalesce = config.usePg ? "COALESCE" : "IFNULL";
  return query<DefaultTargetRow>(
    `SELECT id, state, segment, source, target_value,
       ${coalesce}(account_id, 0) AS account_id,
       ${coalesce}(company_id, 0) AS company_id,
       ${coalesce}(original_id, '') AS original_id,
       ${coalesce}(segment_name, '') AS segment_name,
       ${coalesce}(attributes, '') AS attributes
     FROM ${table("default_targets")}
     WHERE target_key = @key
     ORDER BY state, segment, source`,
    { key }
  );
}

function escapeVal(s: string): string {
  return String(s).replace(/'/g, "''").trim();
}

export async function setDefaultTargets(
  key: TargetKey,
  rows: DefaultTargetRow[]
): Promise<{ count: number }> {
  await ensureTable();

  const ALL_COLS = "target_key, state, segment, source, target_value, account_id, company_id, original_id, segment_name, attributes, created_at";

  function rowToValues(r: DefaultTargetRow, now: string): string {
    const st = escapeVal(r.state).toUpperCase();
    const seg = escapeVal(r.segment).toUpperCase();
    const src = escapeVal(r.source);
    const val = Number(r.target_value) || 0;
    const aid = Number(r.account_id) || 0;
    const cid = Number(r.company_id) || 0;
    const oid = escapeVal(r.original_id || "");
    const sn = escapeVal(r.segment_name || "");
    const attr = escapeVal(r.attributes || "");
    return `('${key}', '${st}', '${seg}', '${src}', ${val}, ${aid}, ${cid}, '${oid}', '${sn}', '${attr}', ${now})`;
  }

  if (config.usePg) {
    await pgTransaction(async (exec) => {
      await exec(`DELETE FROM default_targets WHERE target_key = '${key}'`);
      const BATCH = 500;
      for (let i = 0; i < rows.length; i += BATCH) {
        const batch = rows.slice(i, i + BATCH);
        const vals = batch.map((r) => rowToValues(r, "NOW()")).join(",\n");
        await exec(`INSERT INTO default_targets (${ALL_COLS}) VALUES ${vals}`);
      }
    });
  } else {
    await query(
      `DELETE FROM ${table("default_targets")} WHERE target_key = @key`,
      { key }
    );
    const BATCH = 500;
    for (let i = 0; i < rows.length; i += BATCH) {
      const batch = rows.slice(i, i + BATCH);
      const vals = batch.map((r) => rowToValues(r, "CURRENT_TIMESTAMP()")).join(",\n");
      await query(`INSERT INTO ${table("default_targets")} (${ALL_COLS}) VALUES ${vals}`);
    }
  }
  return { count: rows.length };
}

export async function updateDefaultTargetValue(
  key: TargetKey,
  id: number,
  targetValue: number
): Promise<void> {
  await ensureTable();
  await query(
    `UPDATE ${table("default_targets")}
     SET target_value = @targetValue
     WHERE target_key = @key AND id = @id`,
    { key, id, targetValue }
  );
}

export async function clearDefaultTargets(key: TargetKey): Promise<void> {
  await ensureTable();
  await query(
    `DELETE FROM ${table("default_targets")} WHERE target_key = @key`,
    { key }
  );
}
