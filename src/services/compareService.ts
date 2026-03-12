/**
 * Shadow-mode comparison engine.
 *
 * Runs both BQ (cached) and PG query paths for the 6 analytics functions,
 * compares results row-by-row, and produces a detailed report.
 *
 * Usage:
 *   POST /admin/compare { startDate, endDate, activityLeadType, qbc, functions? }
 */

import {
  // BQ path (existing)
  getStateSegmentPerformanceFromDaily,
  listStateSegmentFiltersFromDaily,
  getPriceExplorationBQ,
  listPriceExplorationFilters,
  listPlanMergedFilters,
  getPlanMergedAnalytics,
  normalizeFilters,
  normalizePriceExplorationFilters,
  normalizePlanMergedFilters,
  type StateSegmentFilters,
  type PriceExplorationFilters,
  type PlanMergedFilters,
} from "./analyticsService.js";

import {
  // PG path (new)
  getSSPFromDailyPG,
  listSSPFiltersPG,
  listPEFiltersPG,
  getPEBQviaPG,
  listPlanMergedFiltersPG,
  getPlanMergedPG,
} from "./pgAnalyticsService.js";

// ── Types ────────────────────────────────────────────────────────────

export type CompareFilters = {
  startDate?: string;
  endDate?: string;
  activityLeadType?: string;
  qbc?: number;
  functions?: string[];
};

type ColumnMismatch = {
  rowKey: string;
  column: string;
  bqValue: unknown;
  pgValue: unknown;
  diff: number | null;
  relDiff: number | null;
};

type FunctionResult = {
  name: string;
  status: "pass" | "fail" | "error" | "skipped";
  error?: string;
  bqRowCount: number;
  pgRowCount: number;
  missingInPg: number;
  extraInPg: number;
  matchedRows: number;
  mismatchedRows: number;
  mismatches: ColumnMismatch[];
  durationBqMs: number;
  durationPgMs: number;
};

export type CompareReport = {
  timestamp: string;
  overallStatus: "pass" | "fail" | "error";
  functions: FunctionResult[];
};

// ── Tolerance ────────────────────────────────────────────────────────

const ABS_TOL = 0.01;
const REL_TOL = 0.001; // 0.1%

function valuesMatch(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a == null && b == null) return true;

  // Both numeric → floating-point tolerance
  const na = Number(a);
  const nb = Number(b);
  if (Number.isFinite(na) && Number.isFinite(nb)) {
    const absDiff = Math.abs(na - nb);
    if (absDiff < ABS_TOL) return true;
    const denom = Math.max(Math.abs(na), Math.abs(nb));
    if (denom > 0 && absDiff / denom < REL_TOL) return true;
    return false;
  }

  // String comparison (case-insensitive trim)
  if (typeof a === "string" && typeof b === "string") {
    return a.trim().toLowerCase() === b.trim().toLowerCase();
  }

  // Arrays (for filter comparisons)
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    const sortedA = [...a].sort();
    const sortedB = [...b].sort();
    return sortedA.every((v, i) => valuesMatch(v, sortedB[i]));
  }

  return String(a) === String(b);
}

function numericDiff(a: unknown, b: unknown): { diff: number | null; relDiff: number | null } {
  const na = Number(a);
  const nb = Number(b);
  if (!Number.isFinite(na) || !Number.isFinite(nb)) return { diff: null, relDiff: null };
  const diff = nb - na;
  const denom = Math.max(Math.abs(na), Math.abs(nb));
  const relDiff = denom > 0 ? diff / denom : 0;
  return { diff, relDiff };
}

// ── Row-key builders ─────────────────────────────────────────────────

function buildRowKey(row: Record<string, unknown>, keyCols: string[]): string {
  return keyCols.map((c) => String(row[c] ?? "")).join("|");
}

// ── Generic row comparator ───────────────────────────────────────────

function compareRows(
  bqRows: Record<string, unknown>[],
  pgRows: Record<string, unknown>[],
  keyCols: string[],
  compareCols: string[]
): Pick<FunctionResult, "matchedRows" | "mismatchedRows" | "missingInPg" | "extraInPg" | "mismatches"> {
  const bqMap = new Map<string, Record<string, unknown>>();
  for (const row of bqRows) {
    bqMap.set(buildRowKey(row, keyCols), row);
  }

  const pgMap = new Map<string, Record<string, unknown>>();
  for (const row of pgRows) {
    pgMap.set(buildRowKey(row, keyCols), row);
  }

  let missingInPg = 0;
  let extraInPg = 0;
  let matchedRows = 0;
  let mismatchedRows = 0;
  const mismatches: ColumnMismatch[] = [];

  // Check BQ rows against PG
  for (const [key, bqRow] of bqMap) {
    const pgRow = pgMap.get(key);
    if (!pgRow) {
      missingInPg++;
      if (mismatches.length < 50) {
        mismatches.push({ rowKey: key, column: "__MISSING__", bqValue: "(exists)", pgValue: "(missing)", diff: null, relDiff: null });
      }
      continue;
    }

    let rowHasMismatch = false;
    for (const col of compareCols) {
      if (!valuesMatch(bqRow[col], pgRow[col])) {
        const { diff, relDiff } = numericDiff(bqRow[col], pgRow[col]);
        if (mismatches.length < 200) {
          mismatches.push({ rowKey: key, column: col, bqValue: bqRow[col], pgValue: pgRow[col], diff, relDiff });
        }
        rowHasMismatch = true;
      }
    }
    if (rowHasMismatch) {
      mismatchedRows++;
    } else {
      matchedRows++;
    }
  }

  // Check for extra rows in PG
  for (const key of pgMap.keys()) {
    if (!bqMap.has(key)) {
      extraInPg++;
      if (mismatches.length < 50) {
        mismatches.push({ rowKey: key, column: "__EXTRA__", bqValue: "(missing)", pgValue: "(exists)", diff: null, relDiff: null });
      }
    }
  }

  return { matchedRows, mismatchedRows, missingInPg, extraInPg, mismatches };
}

// ── Individual function comparators ──────────────────────────────────

async function compareSSP(filters: StateSegmentFilters): Promise<FunctionResult> {
  const name = "getStateSegmentPerformance";
  const normalized = normalizeFilters(filters);
  let bqRows: Record<string, unknown>[] = [];
  let pgRows: Record<string, unknown>[] = [];
  let durationBqMs = 0;
  let durationPgMs = 0;

  try {
    const bqStart = Date.now();
    bqRows = (await getStateSegmentPerformanceFromDaily(filters, normalized)) as unknown as Record<string, unknown>[];
    durationBqMs = Date.now() - bqStart;

    const pgStart = Date.now();
    pgRows = (await getSSPFromDailyPG(filters, normalized)) as unknown as Record<string, unknown>[];
    durationPgMs = Date.now() - pgStart;
  } catch (err) {
    return { name, status: "error", error: String(err), bqRowCount: bqRows.length, pgRowCount: pgRows.length, missingInPg: 0, extraInPg: 0, matchedRows: 0, mismatchedRows: 0, mismatches: [], durationBqMs, durationPgMs };
  }

  const keyCols = ["state", "segment", "channel_group_name"];
  const compareCols = [
    "bids", "sold", "total_cost", "quote_started", "quotes", "binds",
    "q2b_score", "scored_policies", "cpb", "target_cpb", "performance",
    "roe", "combined_ratio", "mrltv", "profit", "equity",
  ];

  const result = compareRows(bqRows, pgRows, keyCols, compareCols);
  const status = result.missingInPg === 0 && result.extraInPg === 0 && result.mismatchedRows === 0 ? "pass" : "fail";

  return { name, status, bqRowCount: bqRows.length, pgRowCount: pgRows.length, ...result, durationBqMs, durationPgMs };
}

async function compareSSPFilters(filters: StateSegmentFilters): Promise<FunctionResult> {
  const name = "listStateSegmentFilters";
  const normalized = normalizeFilters(filters);
  let durationBqMs = 0;
  let durationPgMs = 0;

  try {
    const bqStart = Date.now();
    const bqResult = await listStateSegmentFiltersFromDaily(normalized);
    durationBqMs = Date.now() - bqStart;

    const pgStart = Date.now();
    const pgResult = await listSSPFiltersPG(normalized);
    durationPgMs = Date.now() - pgStart;

    const mismatches: ColumnMismatch[] = [];
    if (!valuesMatch(bqResult.states, pgResult.states)) {
      mismatches.push({ rowKey: "filters", column: "states", bqValue: `[${bqResult.states.length}]`, pgValue: `[${pgResult.states.length}]`, diff: null, relDiff: null });
    }
    if (!valuesMatch(bqResult.segments, pgResult.segments)) {
      mismatches.push({ rowKey: "filters", column: "segments", bqValue: `[${bqResult.segments.length}]`, pgValue: `[${pgResult.segments.length}]`, diff: null, relDiff: null });
    }
    if (!valuesMatch(bqResult.channel_groups, pgResult.channel_groups)) {
      mismatches.push({ rowKey: "filters", column: "channel_groups", bqValue: `[${bqResult.channel_groups.length}]`, pgValue: `[${pgResult.channel_groups.length}]`, diff: null, relDiff: null });
    }

    const status = mismatches.length === 0 ? "pass" : "fail";
    return { name, status, bqRowCount: 1, pgRowCount: 1, missingInPg: 0, extraInPg: 0, matchedRows: mismatches.length === 0 ? 1 : 0, mismatchedRows: mismatches.length > 0 ? 1 : 0, mismatches, durationBqMs, durationPgMs };
  } catch (err) {
    return { name, status: "error", error: String(err), bqRowCount: 0, pgRowCount: 0, missingInPg: 0, extraInPg: 0, matchedRows: 0, mismatchedRows: 0, mismatches: [], durationBqMs, durationPgMs };
  }
}

async function comparePEFilters(filters: PriceExplorationFilters): Promise<FunctionResult> {
  const name = "listPriceExplorationFilters";
  const normalized = normalizePriceExplorationFilters(filters);
  let durationBqMs = 0;
  let durationPgMs = 0;

  try {
    const bqStart = Date.now();
    const bqResult = await listPriceExplorationFilters(filters);
    durationBqMs = Date.now() - bqStart;

    const pgStart = Date.now();
    const pgResult = await listPEFiltersPG(normalized);
    durationPgMs = Date.now() - pgStart;

    const mismatches: ColumnMismatch[] = [];
    if (!valuesMatch(bqResult.states, pgResult.states)) {
      mismatches.push({ rowKey: "filters", column: "states", bqValue: `[${bqResult.states.length}]`, pgValue: `[${pgResult.states.length}]`, diff: null, relDiff: null });
    }
    if (!valuesMatch(bqResult.channelGroups, pgResult.channelGroups)) {
      mismatches.push({ rowKey: "filters", column: "channelGroups", bqValue: `[${bqResult.channelGroups.length}]`, pgValue: `[${pgResult.channelGroups.length}]`, diff: null, relDiff: null });
    }

    const status = mismatches.length === 0 ? "pass" : "fail";
    return { name, status, bqRowCount: 1, pgRowCount: 1, missingInPg: 0, extraInPg: 0, matchedRows: mismatches.length === 0 ? 1 : 0, mismatchedRows: mismatches.length > 0 ? 1 : 0, mismatches, durationBqMs, durationPgMs };
  } catch (err) {
    return { name, status: "error", error: String(err), bqRowCount: 0, pgRowCount: 0, missingInPg: 0, extraInPg: 0, matchedRows: 0, mismatchedRows: 0, mismatches: [], durationBqMs, durationPgMs };
  }
}

async function comparePE(filters: PriceExplorationFilters): Promise<FunctionResult> {
  const name = "getPriceExplorationBQ";
  const normalized = normalizePriceExplorationFilters(filters);
  let bqRows: Record<string, unknown>[] = [];
  let pgRows: Record<string, unknown>[] = [];
  let durationBqMs = 0;
  let durationPgMs = 0;

  try {
    const bqStart = Date.now();
    bqRows = (await getPriceExplorationBQ(normalized)) as unknown as Record<string, unknown>[];
    durationBqMs = Date.now() - bqStart;

    const pgStart = Date.now();
    pgRows = (await getPEBQviaPG(normalized)) as unknown as Record<string, unknown>[];
    durationPgMs = Date.now() - pgStart;
  } catch (err) {
    return { name, status: "error", error: String(err), bqRowCount: bqRows.length, pgRowCount: pgRows.length, missingInPg: 0, extraInPg: 0, matchedRows: 0, mismatchedRows: 0, mismatches: [], durationBqMs, durationPgMs };
  }

  const keyCols = ["state", "channel_group_name", "testing_point"];
  const compareCols = [
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
    "recommended_testing_point",
    "stat_sig", "stat_sig_channel_group", "stat_sig_source",
  ];

  const result = compareRows(bqRows, pgRows, keyCols, compareCols);
  const status = result.missingInPg === 0 && result.extraInPg === 0 && result.mismatchedRows === 0 ? "pass" : "fail";

  return { name, status, bqRowCount: bqRows.length, pgRowCount: pgRows.length, ...result, durationBqMs, durationPgMs };
}

async function comparePlanMergedFilters(filters: PlanMergedFilters): Promise<FunctionResult> {
  const name = "listPlanMergedFilters";
  const normalized = normalizePlanMergedFilters(filters);
  let durationBqMs = 0;
  let durationPgMs = 0;

  try {
    const bqStart = Date.now();
    const bqResult = await listPlanMergedFilters(filters);
    durationBqMs = Date.now() - bqStart;

    const pgStart = Date.now();
    const pgResult = await listPlanMergedFiltersPG(normalized);
    durationPgMs = Date.now() - pgStart;

    const mismatches: ColumnMismatch[] = [];
    if (!valuesMatch(bqResult.states, pgResult.states)) {
      mismatches.push({ rowKey: "filters", column: "states", bqValue: `[${bqResult.states.length}]`, pgValue: `[${pgResult.states.length}]`, diff: null, relDiff: null });
    }
    if (!valuesMatch(bqResult.segments, pgResult.segments)) {
      mismatches.push({ rowKey: "filters", column: "segments", bqValue: `[${bqResult.segments.length}]`, pgValue: `[${pgResult.segments.length}]`, diff: null, relDiff: null });
    }
    if (!valuesMatch(bqResult.channelGroups, pgResult.channelGroups)) {
      mismatches.push({ rowKey: "filters", column: "channelGroups", bqValue: `[${bqResult.channelGroups.length}]`, pgValue: `[${pgResult.channelGroups.length}]`, diff: null, relDiff: null });
    }
    if (!valuesMatch(bqResult.testingPoints, pgResult.testingPoints)) {
      mismatches.push({ rowKey: "filters", column: "testingPoints", bqValue: `[${bqResult.testingPoints.length}]`, pgValue: `[${pgResult.testingPoints.length}]`, diff: null, relDiff: null });
    }
    if (!valuesMatch(bqResult.statSig, pgResult.statSig)) {
      mismatches.push({ rowKey: "filters", column: "statSig", bqValue: `[${bqResult.statSig.length}]`, pgValue: `[${pgResult.statSig.length}]`, diff: null, relDiff: null });
    }

    const status = mismatches.length === 0 ? "pass" : "fail";
    return { name, status, bqRowCount: 1, pgRowCount: 1, missingInPg: 0, extraInPg: 0, matchedRows: mismatches.length === 0 ? 1 : 0, mismatchedRows: mismatches.length > 0 ? 1 : 0, mismatches, durationBqMs, durationPgMs };
  } catch (err) {
    return { name, status: "error", error: String(err), bqRowCount: 0, pgRowCount: 0, missingInPg: 0, extraInPg: 0, matchedRows: 0, mismatchedRows: 0, mismatches: [], durationBqMs, durationPgMs };
  }
}

async function comparePlanMerged(filters: PlanMergedFilters): Promise<FunctionResult> {
  const name = "getPlanMergedAnalytics";
  const normalized = normalizePlanMergedFilters(filters);
  let bqRows: Record<string, unknown>[] = [];
  let pgRows: Record<string, unknown>[] = [];
  let durationBqMs = 0;
  let durationPgMs = 0;

  try {
    const bqStart = Date.now();
    bqRows = (await getPlanMergedAnalytics(filters)) as unknown as Record<string, unknown>[];
    durationBqMs = Date.now() - bqStart;

    const pgStart = Date.now();
    pgRows = (await getPlanMergedPG(normalized)) as unknown as Record<string, unknown>[];
    durationPgMs = Date.now() - pgStart;
  } catch (err) {
    return { name, status: "error", error: String(err), bqRowCount: bqRows.length, pgRowCount: pgRows.length, missingInPg: 0, extraInPg: 0, matchedRows: 0, mismatchedRows: 0, mismatches: [], durationBqMs, durationPgMs };
  }

  const keyCols = ["state", "channel_group_name", "segment", "price_adjustment_percent"];
  const compareCols = [
    "start_date", "end_date",
    "stat_sig", "stat_sig_channel_group",
    "cpc_uplift", "win_rate_uplift",
    "additional_clicks", "expected_total_clicks",
    "expected_cpc", "expected_total_cost",
    "expected_total_binds", "additional_expected_binds",
    "expected_cpb", "ss_performance", "expected_performance", "performance_uplift",
  ];

  const result = compareRows(bqRows, pgRows, keyCols, compareCols);
  const status = result.missingInPg === 0 && result.extraInPg === 0 && result.mismatchedRows === 0 ? "pass" : "fail";

  return { name, status, bqRowCount: bqRows.length, pgRowCount: pgRows.length, ...result, durationBqMs, durationPgMs };
}

// ── Main entry point ─────────────────────────────────────────────────

const ALL_FUNCTION_NAMES = [
  "getStateSegmentPerformance",
  "listStateSegmentFilters",
  "listPriceExplorationFilters",
  "getPriceExplorationBQ",
  "listPlanMergedFilters",
  "getPlanMergedAnalytics",
];

export async function compareAll(opts: CompareFilters): Promise<CompareReport> {
  const requested = (opts.functions && opts.functions.length > 0)
    ? opts.functions
    : ALL_FUNCTION_NAMES;

  const sspFilters: StateSegmentFilters = {
    startDate: opts.startDate,
    endDate: opts.endDate,
    activityLeadType: opts.activityLeadType,
    qbc: opts.qbc,
  };

  const peFilters: PriceExplorationFilters = {
    startDate: opts.startDate,
    endDate: opts.endDate,
    activityLeadType: opts.activityLeadType,
    qbc: opts.qbc ?? 0,
    limit: 50000,
    topPairs: 0,
  };

  const pmFilters: PlanMergedFilters = {
    startDate: opts.startDate,
    endDate: opts.endDate,
    activityLeadType: opts.activityLeadType,
  };

  const results: FunctionResult[] = [];

  // Run comparisons sequentially to avoid overloading BQ
  for (const fn of requested) {
    const skip = (): FunctionResult => ({
      name: fn, status: "skipped", bqRowCount: 0, pgRowCount: 0,
      missingInPg: 0, extraInPg: 0, matchedRows: 0, mismatchedRows: 0,
      mismatches: [], durationBqMs: 0, durationPgMs: 0,
    });

    switch (fn) {
      case "getStateSegmentPerformance":
        results.push(await compareSSP(sspFilters));
        break;
      case "listStateSegmentFilters":
        results.push(await compareSSPFilters(sspFilters));
        break;
      case "listPriceExplorationFilters":
        results.push(await comparePEFilters(peFilters));
        break;
      case "getPriceExplorationBQ":
        results.push(await comparePE(peFilters));
        break;
      case "listPlanMergedFilters":
        results.push(await comparePlanMergedFilters(pmFilters));
        break;
      case "getPlanMergedAnalytics":
        results.push(await comparePlanMerged(pmFilters));
        break;
      default:
        results.push(skip());
    }
  }

  const overallStatus = results.some((r) => r.status === "error")
    ? "error"
    : results.some((r) => r.status === "fail")
      ? "fail"
      : "pass";

  return {
    timestamp: new Date().toISOString(),
    overallStatus,
    functions: results,
  };
}
