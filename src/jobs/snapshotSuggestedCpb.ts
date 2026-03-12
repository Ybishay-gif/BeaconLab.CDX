/**
 * Daily snapshot: suggested Target CPB → BigQuery.
 *
 * For every active plan, computes the "Calc. Max CPB" that appears on the
 * Target COR tab and writes it into a BQ table (`suggested_target_cpb`).
 * The table is fully replaced each run (TRUNCATE + INSERT).
 *
 * Formula (same as frontend):
 *   suggested_max_cpb = 0.81 * (target_cor * avg_lifetime_premium - qbc - avg_lifetime_cost)
 */

import { bigquery, table as bqTable } from "../db/bigquery.js";
import { config } from "../config.js";
import { listPlans } from "../services/plansService.js";
import { listTargets, type TargetRow } from "../services/targetsService.js";

const BQ_TABLE = `${config.projectId}.${config.dataset}.suggested_target_cpb`;
const BATCH_SIZE = 500;

export type SnapshotResult = {
  ok: boolean;
  rows: number;
  plans: number;
  ms: number;
  error?: string;
};

type SuggestedRow = {
  plan_id: string;
  plan_name: string;
  state: string;
  segment: string;
  target_cor: number;
  avg_lifetime_premium: number;
  avg_lifetime_cost: number;
  qbc: number;
  suggested_max_cpb: number;
  current_cpb: number | null;
  current_target_value: number;
  snapshot_date: string; // YYYY-MM-DD
};

// ── Helpers ──────────────────────────────────────────────────────────

function todayDate(): string {
  return new Date().toISOString().slice(0, 10);
}

function bqLiteral(v: unknown): string {
  if (v === null || v === undefined) return "NULL";
  if (typeof v === "number") return Number.isFinite(v) ? String(v) : "NULL";
  const s = String(v).replace(/'/g, "''");
  return `'${s}'`;
}

// ── Ensure BQ table exists ───────────────────────────────────────────

async function ensureTable(): Promise<void> {
  const ddl = `
    CREATE TABLE IF NOT EXISTS \`${BQ_TABLE}\` (
      plan_id STRING,
      plan_name STRING,
      state STRING,
      segment STRING,
      target_cor FLOAT64,
      avg_lifetime_premium FLOAT64,
      avg_lifetime_cost FLOAT64,
      qbc FLOAT64,
      suggested_max_cpb FLOAT64,
      current_cpb FLOAT64,
      current_target_value FLOAT64,
      snapshot_date DATE
    )`;
  await bigquery.query({ query: ddl, useLegacySql: false });
}

// ── Core logic ───────────────────────────────────────────────────────

function computeSuggested(
  row: TargetRow,
  qbc: number,
  planId: string,
  planName: string,
  snapshotDate: string
): SuggestedRow | null {
  const tCor = row.target_cor;
  const ltv = row.avg_lifetime_premium ?? 0;
  const ltc = row.avg_lifetime_cost ?? 0;

  // Skip rows where COR target or LTV is missing/zero (no suggestion possible)
  if (!tCor || !ltv) return null;

  const suggested = 0.81 * (tCor * ltv - qbc - ltc);
  if (!Number.isFinite(suggested) || suggested <= 0) return null;

  return {
    plan_id: planId,
    plan_name: planName,
    state: row.state,
    segment: row.segment,
    target_cor: tCor,
    avg_lifetime_premium: ltv,
    avg_lifetime_cost: ltc,
    qbc,
    suggested_max_cpb: Math.round(suggested),
    current_cpb: row.cpb,
    current_target_value: row.target_value,
    snapshot_date: snapshotDate,
  };
}

// ── Public API ───────────────────────────────────────────────────────

export async function snapshotSuggestedCpb(): Promise<SnapshotResult> {
  const t0 = Date.now();
  try {
    await ensureTable();

    const plans = await listPlans();
    const activePlans = plans.filter((p) => p.status !== "archived");

    const snapshotDate = todayDate();
    const allRows: SuggestedRow[] = [];

    for (const plan of activePlans) {
      let ctx: Record<string, unknown> = {};
      try {
        ctx = plan.plan_context_json ? JSON.parse(plan.plan_context_json) : {};
      } catch { /* ignore */ }

      const perfStartDate = String(ctx.perfStartDate || ctx.performanceStartDate || "");
      const perfEndDate = String(ctx.perfEndDate || ctx.performanceEndDate || "");
      const qbc = Number(ctx.qbcClicks) || 0;
      const activity = String(ctx.activity || "clicks");
      const leadType = String(ctx.leadType || "auto");
      const activityLeadType = ctx.activityLeadType
        ? String(ctx.activityLeadType)
        : `${activity}_${leadType}`;

      if (!perfStartDate || !perfEndDate) continue;

      try {
        const targets = await listTargets({
          planId: plan.plan_id,
          startDate: perfStartDate,
          endDate: perfEndDate,
          activityLeadType,
          qbc,
        });

        // Deduplicate by state+segment (listTargets returns one row per source)
        const seen = new Set<string>();
        for (const row of targets) {
          const key = `${row.state}|${row.segment}`;
          if (seen.has(key)) continue;
          seen.add(key);
          const suggested = computeSuggested(row, qbc, plan.plan_id, plan.plan_name, snapshotDate);
          if (suggested) allRows.push(suggested);
        }
      } catch {
        // Non-fatal: skip plan on error
      }
    }

    // Truncate + insert into BQ
    await bigquery.query({
      query: `DELETE FROM \`${BQ_TABLE}\` WHERE TRUE`,
      useLegacySql: false,
    });

    const COLS = [
      "plan_id", "plan_name", "state", "segment", "target_cor",
      "avg_lifetime_premium", "avg_lifetime_cost", "qbc",
      "suggested_max_cpb", "current_cpb", "current_target_value", "snapshot_date",
    ] as const;

    for (let i = 0; i < allRows.length; i += BATCH_SIZE) {
      const batch = allRows.slice(i, i + BATCH_SIZE);
      const values = batch
        .map((r) => {
          const vals = COLS.map((c) => {
            if (c === "snapshot_date") return `DATE '${r.snapshot_date}'`;
            return bqLiteral(r[c]);
          });
          return `(${vals.join(", ")})`;
        })
        .join(",\n");

      await bigquery.query({
        query: `INSERT INTO \`${BQ_TABLE}\` (${COLS.join(", ")}) VALUES\n${values}`,
        useLegacySql: false,
      });
    }

    return { ok: true, rows: allRows.length, plans: activePlans.length, ms: Date.now() - t0 };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return { ok: false, rows: 0, plans: 0, ms: Date.now() - t0, error: message };
  }
}
