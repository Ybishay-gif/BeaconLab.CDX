import { bqQuery, bqTable } from "../db/index.js";

/* ------------------------------------------------------------------ */
/*  Types                                                              */
/* ------------------------------------------------------------------ */

export type ChannelParamFilters = {
  tactics: string[];
  verticals: string[];
  segments: string[];
};

/** The subset of columns editable in the UI. */
export type ChannelParamValues = {
  // ROE (6)
  roe_poor: number;
  roe_minimal: number;
  roe_0: number;
  roe_good: number;
  roe_excellent: number;
  roe_amazing: number;
  // Performance (5)
  per_poor: number;
  per_minimal: number;
  per_good: number;
  per_excellent: number;
  per_amazing: number;
  // Win Rate (5)
  Poor_WR: number;
  Low_WR: number;
  OK_WR: number;
  High_WR: number;
  VHigh_WR: number;
  // Quote Rate (5)
  QuoteRate_poor: number;
  QuoteRate_minimal: number;
  QuoteRate_good: number;
  QuoteRate_excellent: number;
  QuoteRate_amazing: number;
  // Date Ranges (6)
  early_funnel_start_days: number;
  early_funnel_end_days: number;
  early_cmp_funnel_start_days: number;
  early_cmp_funnel_end_days: number;
  perf_start_days: number;
  perf_end_days: number;
  // Cost (5)
  QBC: number;
  minimal_cost: number;
  mid_cost: number;
  high_cost: number;
  vhigh_cost: number;
};

export type ChannelParamRow = {
  Tactic: string;
  Vertical: string;
  Segment: string;
} & ChannelParamValues;

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

const TBL = () => "`crblx-beacon-prod.LM_Customan_Analysis.Channel_param_Blab`";

/** All editable column names (used in SET clause). */
const EDITABLE_COLS: (keyof ChannelParamValues)[] = [
  "roe_poor", "roe_minimal", "roe_0", "roe_good", "roe_excellent", "roe_amazing",
  "per_poor", "per_minimal", "per_good", "per_excellent", "per_amazing",
  "Poor_WR", "Low_WR", "OK_WR", "High_WR", "VHigh_WR",
  "QuoteRate_poor", "QuoteRate_minimal", "QuoteRate_good", "QuoteRate_excellent", "QuoteRate_amazing",
  "early_funnel_start_days", "early_funnel_end_days",
  "early_cmp_funnel_start_days", "early_cmp_funnel_end_days",
  "perf_start_days", "perf_end_days",
  "QBC", "minimal_cost", "mid_cost", "high_cost", "vhigh_cost",
];

/* ------------------------------------------------------------------ */
/*  Public API                                                         */
/* ------------------------------------------------------------------ */

export async function getChannelParamFilters(
  tactic?: string,
  vertical?: string
): Promise<ChannelParamFilters> {
  // Tactics are always global
  const tacticsP = bqQuery<{ Tactic: string }>(
    `SELECT DISTINCT Tactic FROM ${TBL()} ORDER BY Tactic`
  );

  // Verticals filtered by tactic when provided
  const verticalsP = tactic
    ? bqQuery<{ Vertical: string }>(
        `SELECT DISTINCT Vertical FROM ${TBL()} WHERE Tactic = @tactic ORDER BY Vertical`,
        { tactic }
      )
    : bqQuery<{ Vertical: string }>(
        `SELECT DISTINCT Vertical FROM ${TBL()} ORDER BY Vertical`
      );

  // Segments filtered by tactic + vertical when both provided
  let segmentsP: Promise<{ Segment: string }[]>;
  if (tactic && vertical) {
    segmentsP = bqQuery<{ Segment: string }>(
      `SELECT DISTINCT Segment FROM ${TBL()} WHERE Tactic = @tactic AND Vertical = @vertical ORDER BY Segment`,
      { tactic, vertical }
    );
  } else {
    segmentsP = bqQuery<{ Segment: string }>(
      `SELECT DISTINCT Segment FROM ${TBL()} ORDER BY Segment`
    );
  }

  const [tactics, verticals, segments] = await Promise.all([tacticsP, verticalsP, segmentsP]);
  return {
    tactics: tactics.map((r) => r.Tactic),
    verticals: verticals.map((r) => r.Vertical),
    segments: segments.map((r) => r.Segment),
  };
}

/**
 * Fetch channel param row(s).
 * When segment is "__ALL__", returns the first matching row (for display).
 */
export async function getChannelParams(
  tactic: string,
  vertical: string,
  segment?: string
): Promise<ChannelParamRow[]> {
  const cols = ["Tactic", "Vertical", "Segment", ...EDITABLE_COLS].join(", ");

  if (!segment || segment === "__ALL__") {
    return bqQuery<ChannelParamRow>(
      `SELECT ${cols} FROM ${TBL()}
       WHERE Tactic = @tactic AND Vertical = @vertical
       ORDER BY Segment
       LIMIT 1`,
      { tactic, vertical }
    );
  }

  return bqQuery<ChannelParamRow>(
    `SELECT ${cols} FROM ${TBL()}
     WHERE Tactic = @tactic AND Vertical = @vertical AND Segment = @segment`,
    { tactic, vertical, segment }
  );
}

/**
 * Update a single segment row.
 */
export async function updateChannelParam(
  tactic: string,
  vertical: string,
  segment: string,
  values: ChannelParamValues
): Promise<{ ok: boolean }> {
  const setClauses = EDITABLE_COLS.map((c) => `${c} = @${c}`).join(", ");
  const params: Record<string, unknown> = { tactic, vertical, segment, ...values };

  await bqQuery(
    `UPDATE ${TBL()}
     SET ${setClauses}
     WHERE Tactic = @tactic AND Vertical = @vertical AND Segment = @segment`,
    params
  );
  return { ok: true };
}

/**
 * Bulk-update ALL segments under a tactic+vertical.
 */
export async function updateChannelParamAll(
  tactic: string,
  vertical: string,
  values: ChannelParamValues
): Promise<{ ok: boolean }> {
  const setClauses = EDITABLE_COLS.map((c) => `${c} = @${c}`).join(", ");
  const params: Record<string, unknown> = { tactic, vertical, ...values };

  await bqQuery(
    `UPDATE ${TBL()}
     SET ${setClauses}
     WHERE Tactic = @tactic AND Vertical = @vertical`,
    params
  );
  return { ok: true };
}
