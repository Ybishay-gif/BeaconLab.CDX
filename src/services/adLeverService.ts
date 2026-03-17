/**
 * Ad Lever Service
 * Calculates 1-10 lever scores per state+segment using percentile ranking of KPIs.
 */
import { query, table } from "../db/index.js";
import { cached, buildCacheKey } from "../cache.js";
import { getParameterValues, upsertParameters } from "./plansService.js";
import { buildCombinedRatioSql, buildRoeSql } from "./shared/kpiSql.js";
import { splitCombinedFilter } from "./shared/activityScope.js";

// ── Types ─────────────────────────────────────────────────────────────────────

export type RetentionRow = { state: string; segment: string; nblr: number; nb_lt_prem?: number };

export type AdLeverOverrides = Record<string, number | string>; // key = "STATE|SEGMENT"

export type AdLeverPerformanceRow = {
  state: string;
  segment: string;
  bids: number;
  avg_bid: number;
  cpc: number;
  sold: number;
  win_rate: number;
  total_cost: number;
  calls: number;
  quotes: number;
  q2b: number;
  binds: number;
  combined_ratio: number;
  roe: number;
  performance: number;
};

export type AdLeverRow = AdLeverPerformanceRow & {
  retention_nblr: number | null;
  cor_score: number | null;
  q2b_score: number | null;
  retention_score: number | null;
  wr_score: number | null;
  final_score: number | null;
  lever: number | null;
  lever_override: number | string | null;
  is_low_volume: boolean;
};

export type AdLeverFilters = {
  planId: string;
  startDate?: string;
  endDate?: string;
  activityLeadType?: string;
  qbc?: number;
};

// ── PERCENTRANK.INC (matches Excel exactly) ───────────────────────────────────

function percentRankInc(sorted: number[], x: number): number {
  const n = sorted.length;
  if (n <= 1) return 0;
  let countLess = 0;
  for (const v of sorted) {
    if (v < x) countLess++;
  }
  return countLess / (n - 1);
}

/** Lower is better: =MAX(1,MIN(10,ROUNDUP((1-PERCENTRANK.INC(range,val))*10,0))) */
function scoreLowerIsBetter(allValues: number[], x: number): number {
  const sorted = [...allValues].sort((a, b) => a - b);
  const raw = (1 - percentRankInc(sorted, x)) * 10;
  return Math.max(1, Math.min(10, Math.ceil(raw)));
}

/** Higher is better: =MAX(1,MIN(10,ROUNDUP(PERCENTRANK.INC(range,val)*10,0))) */
function scoreHigherIsBetter(allValues: number[], x: number): number {
  const sorted = [...allValues].sort((a, b) => a - b);
  const raw = percentRankInc(sorted, x) * 10;
  return Math.max(1, Math.min(10, Math.ceil(raw)));
}

// ── BQ Query ──────────────────────────────────────────────────────────────────

async function queryAdLeverPerformance(
  startDate: string,
  endDate: string,
  activityType: string,
  leadType: string,
  qbc: number
): Promise<AdLeverPerformanceRow[]> {
  const cacheKey = buildCacheKey("ad-lever", { startDate, endDate, activityType, leadType, qbc });

  return cached(cacheKey, () =>
    query<AdLeverPerformanceRow>(
      `
      SELECT
        state,
        segment,
        SUM(bids) AS bids,
        SAFE_DIVIDE(SUM(total_cost), NULLIF(SUM(bids), 0)) AS avg_bid,
        SAFE_DIVIDE(SUM(total_cost), NULLIF(SUM(sold), 0)) AS cpc,
        SUM(sold) AS sold,
        SAFE_DIVIDE(SUM(sold), NULLIF(SUM(bids), 0)) AS win_rate,
        SUM(total_cost) AS total_cost,
        SUM(quote_started) AS calls,
        SUM(quotes) AS quotes,
        SAFE_DIVIDE(SUM(binds), NULLIF(SUM(quotes), 0)) AS q2b,
        SUM(binds) AS binds,
        ${buildCombinedRatioSql({
          zeroConditions: [
            "SUM(scored_policies) = 0",
            "SAFE_DIVIDE(SUM(lifetime_premium_sum), NULLIF(SUM(scored_policies), 0)) = 0",
          ],
          cpbExpr: "SAFE_DIVIDE(SUM(total_cost), NULLIF(SUM(binds), 0))",
          avgLifetimeCostExpr: "SAFE_DIVIDE(SUM(lifetime_cost_sum), NULLIF(SUM(scored_policies), 0))",
          avgLifetimePremiumExpr: "SAFE_DIVIDE(SUM(lifetime_premium_sum), NULLIF(SUM(scored_policies), 0))",
        })} AS combined_ratio,
        ${buildRoeSql({
          zeroConditions: [
            "SUM(scored_policies) = 0",
            "SAFE_DIVIDE(SUM(avg_equity_sum), NULLIF(SUM(scored_policies), 0)) = 0",
          ],
          avgProfitExpr: "SAFE_DIVIDE(SUM(avg_profit_sum), NULLIF(SUM(scored_policies), 0))",
          cpbExpr: "SAFE_DIVIDE(SUM(total_cost), NULLIF(SUM(binds), 0))",
          avgEquityExpr: "SAFE_DIVIDE(SUM(avg_equity_sum), NULLIF(SUM(scored_policies), 0))",
        })} AS roe,
        SAFE_DIVIDE(
          CASE WHEN SUM(binds) = 0 THEN 0
            ELSE SAFE_DIVIDE(SUM(target_cpb_sum), SUM(binds))
          END,
          SAFE_DIVIDE(SUM(total_cost), NULLIF(SUM(binds), 0))
        ) AS performance
      FROM ${table("state_segment_daily")}
      WHERE event_date BETWEEN @startDate::date AND @endDate::date
        AND (@activityType = '' OR activity_type = @activityType)
        AND (@leadType = '' OR lead_type = @leadType)
      GROUP BY state, segment
      ORDER BY state, segment
      `,
      { startDate, endDate, activityType, leadType, qbc }
    )
  );
}

// ── Core Lever Calculation ────────────────────────────────────────────────────

export async function getAdLeverData(filters: AdLeverFilters): Promise<AdLeverRow[]> {
  const startDate = filters.startDate || "2024-01-01";
  const endDate = filters.endDate || "2099-12-31";
  const scope = splitCombinedFilter(filters.activityLeadType);
  const qbc = filters.qbc ?? 0;

  // 1. Fetch performance data
  const perfRows = await queryAdLeverPerformance(
    startDate,
    endDate,
    scope.activityType,
    scope.leadType,
    qbc
  );

  // 2. Load retention data (plan-specific or default)
  const retentionMap = await loadRetentionMap(filters.planId);

  // 3. Load overrides
  const overrides = await loadOverrides(filters.planId);

  // 4. Join retention NBLR to perf rows
  const rows: AdLeverRow[] = perfRows.map((r) => {
    const key = `${r.state}|${r.segment}`;
    const nblr = retentionMap.get(key) ?? null;
    return {
      ...r,
      retention_nblr: nblr,
      cor_score: null,
      q2b_score: null,
      retention_score: null,
      wr_score: null,
      final_score: null,
      lever: null,
      lever_override: overrides[key] ?? null,
      is_low_volume: (r.binds ?? 0) < 2,
    };
  });

  // 5. Identify qualifying rows (binds >= 2)
  const qualifying = rows.filter((r) => !r.is_low_volume);
  if (qualifying.length === 0) return rows;

  // 6. Collect value arrays for percentile scoring
  const allCOR = qualifying.map((r) => r.combined_ratio);
  const allQ2B = qualifying.map((r) => r.q2b);
  const allWR = qualifying.map((r) => r.win_rate);
  const qualifyingWithNBLR = qualifying.filter((r) => r.retention_nblr != null);
  const allNBLR = qualifyingWithNBLR.map((r) => r.retention_nblr!);

  // 7. Score each qualifying row
  for (const row of qualifying) {
    row.cor_score = scoreLowerIsBetter(allCOR, row.combined_ratio);
    row.q2b_score = scoreHigherIsBetter(allQ2B, row.q2b);
    row.wr_score = scoreHigherIsBetter(allWR, row.win_rate);

    if (row.retention_nblr != null && allNBLR.length > 1) {
      row.retention_score = scoreLowerIsBetter(allNBLR, row.retention_nblr);
    }

    // Final score = average of available scores
    const scores = [row.cor_score, row.q2b_score, row.wr_score];
    if (row.retention_score != null) scores.push(row.retention_score);
    row.final_score = scores.reduce((a, b) => a + b, 0) / scores.length;
  }

  // 8. Lever = percentile of final scores → 1-10
  const allFinals = qualifying.map((r) => r.final_score!);
  for (const row of qualifying) {
    row.lever = scoreHigherIsBetter(allFinals, row.final_score!);
  }

  // 9. Apply overrides — override replaces lever for display
  for (const row of rows) {
    const key = `${row.state}|${row.segment}`;
    if (overrides[key] != null) {
      row.lever_override = overrides[key];
    }
  }

  return rows;
}

// ── Retention Data Helpers ────────────────────────────────────────────────────

async function loadRetentionMap(planId: string): Promise<Map<string, number>> {
  const params = await getParameterValues(planId, ["ad_lever_retention_data"]);
  let data: RetentionRow[];

  if (params.ad_lever_retention_data) {
    try {
      data = JSON.parse(params.ad_lever_retention_data);
    } catch {
      data = DEFAULT_RETENTION_DATA;
    }
  } else {
    data = DEFAULT_RETENTION_DATA;
  }

  const map = new Map<string, number>();
  for (const row of data) {
    const key = `${String(row.state).trim().toUpperCase()}|${String(row.segment).trim().toUpperCase()}`;
    map.set(key, row.nblr);
  }
  return map;
}

async function loadOverrides(planId: string): Promise<AdLeverOverrides> {
  const params = await getParameterValues(planId, ["ad_lever_overrides"]);
  if (params.ad_lever_overrides) {
    try {
      return JSON.parse(params.ad_lever_overrides);
    } catch {
      return {};
    }
  }
  return {};
}

export async function getRetentionData(planId: string): Promise<RetentionRow[]> {
  const params = await getParameterValues(planId, ["ad_lever_retention_data"]);
  if (params.ad_lever_retention_data) {
    try {
      return JSON.parse(params.ad_lever_retention_data);
    } catch {
      return DEFAULT_RETENTION_DATA;
    }
  }
  return DEFAULT_RETENTION_DATA;
}

// ── Persistence ───────────────────────────────────────────────────────────────

export async function saveAdLeverOverrides(
  planId: string,
  userId: string,
  overrides: AdLeverOverrides
): Promise<void> {
  await upsertParameters(planId, userId, [
    { key: "ad_lever_overrides", value: JSON.stringify(overrides), valueType: "json" },
  ]);
}

export async function saveRetentionData(
  planId: string,
  userId: string,
  data: RetentionRow[]
): Promise<void> {
  await upsertParameters(planId, userId, [
    { key: "ad_lever_retention_data", value: JSON.stringify(data), valueType: "json" },
  ]);
}

export async function resetRetentionData(
  planId: string,
  userId: string
): Promise<void> {
  await upsertParameters(planId, userId, [
    { key: "ad_lever_retention_data", value: JSON.stringify(DEFAULT_RETENTION_DATA), valueType: "json" },
  ]);
}

export function getDefaultRetentionData(): RetentionRow[] {
  return DEFAULT_RETENTION_DATA;
}

// ── Default Retention Data (154 rows from provided XLSX) ──────────────────────

const DEFAULT_RETENTION_DATA: RetentionRow[] = [
  { state: "WI", segment: "MCH", nblr: 0.857 },
  { state: "WI", segment: "MCR", nblr: 0.849 },
  { state: "WI", segment: "SCH", nblr: 0.857 },
  { state: "WI", segment: "SCR", nblr: 0.856 },
  { state: "NE", segment: "MCH", nblr: 0.814 },
  { state: "NE", segment: "MCR", nblr: 0.813 },
  { state: "NE", segment: "SCH", nblr: 0.811 },
  { state: "NE", segment: "SCR", nblr: 0.792 },
  { state: "KS", segment: "MCH", nblr: 0.771 },
  { state: "KS", segment: "MCR", nblr: 0.786 },
  { state: "KS", segment: "SCH", nblr: 0.755 },
  { state: "KS", segment: "SCR", nblr: 0.765 },
  { state: "OK", segment: "MCH", nblr: 0.769 },
  { state: "OK", segment: "MCR", nblr: 0.77 },
  { state: "OK", segment: "SCH", nblr: 0.77 },
  { state: "OK", segment: "SCR", nblr: 0.761 },
  { state: "AZ", segment: "MCH", nblr: 0.773 },
  { state: "AZ", segment: "MCR", nblr: 0.767 },
  { state: "AZ", segment: "SCH", nblr: 0.765 },
  { state: "AZ", segment: "SCR", nblr: 0.75 },
  { state: "AR", segment: "MCH", nblr: 0.762 },
  { state: "AR", segment: "MCR", nblr: 0.765 },
  { state: "AR", segment: "SCH", nblr: 0.758 },
  { state: "AR", segment: "SCR", nblr: 0.766 },
  { state: "IN", segment: "MCH", nblr: 0.763 },
  { state: "IN", segment: "MCR", nblr: 0.747 },
  { state: "IN", segment: "SCH", nblr: 0.762 },
  { state: "IN", segment: "SCR", nblr: 0.757 },
  { state: "DE", segment: "MCH", nblr: 0.743 },
  { state: "DE", segment: "MCR", nblr: 0.754 },
  { state: "DE", segment: "SCH", nblr: 0.739 },
  { state: "DE", segment: "SCR", nblr: 0.743 },
  { state: "NV", segment: "MCH", nblr: 0.741 },
  { state: "NV", segment: "MCR", nblr: 0.744 },
  { state: "NV", segment: "SCH", nblr: 0.734 },
  { state: "NV", segment: "SCR", nblr: 0.751 },
  { state: "KY", segment: "MCH", nblr: 0.739 },
  { state: "KY", segment: "MCR", nblr: 0.732 },
  { state: "KY", segment: "SCH", nblr: 0.761 },
  { state: "KY", segment: "SCR", nblr: 0.738 },
  { state: "MN", segment: "MCH", nblr: 0.738 },
  { state: "MN", segment: "MCR", nblr: 0.753 },
  { state: "MN", segment: "SCH", nblr: 0.738 },
  { state: "MN", segment: "SCR", nblr: 0.739 },
  { state: "PA", segment: "MCH", nblr: 0.729 },
  { state: "PA", segment: "MCR", nblr: 0.737 },
  { state: "PA", segment: "SCH", nblr: 0.749 },
  { state: "PA", segment: "SCR", nblr: 0.74 },
  { state: "IA", segment: "MCH", nblr: 0.741 },
  { state: "IA", segment: "MCR", nblr: 0.722 },
  { state: "IA", segment: "SCH", nblr: 0.736 },
  { state: "IA", segment: "SCR", nblr: 0.735 },
  { state: "UT", segment: "MCH", nblr: 0.737 },
  { state: "UT", segment: "MCR", nblr: 0.731 },
  { state: "UT", segment: "SCH", nblr: 0.739 },
  { state: "UT", segment: "SCR", nblr: 0.718 },
  { state: "ID", segment: "MCH", nblr: 0.714 },
  { state: "ID", segment: "MCR", nblr: 0.733 },
  { state: "ID", segment: "SCH", nblr: 0.715 },
  { state: "ID", segment: "SCR", nblr: 0.717 },
  { state: "TN", segment: "MCH", nblr: 0.71 },
  { state: "TN", segment: "MCR", nblr: 0.712 },
  { state: "TN", segment: "SCH", nblr: 0.713 },
  { state: "TN", segment: "SCR", nblr: 0.707 },
  { state: "MS", segment: "MCH", nblr: 0.714 },
  { state: "MS", segment: "MCR", nblr: 0.704 },
  { state: "MS", segment: "SCH", nblr: 0.701 },
  { state: "MS", segment: "SCR", nblr: 0.682 },
  { state: "MA", segment: "MCH", nblr: 0.721 },
  { state: "MA", segment: "MCR", nblr: 0.697 },
  { state: "MA", segment: "SCH", nblr: 0.711 },
  { state: "MA", segment: "SCR", nblr: 0.686 },
  { state: "MO", segment: "MCH", nblr: 0.702 },
  { state: "MO", segment: "MCR", nblr: 0.69 },
  { state: "MO", segment: "SCH", nblr: 0.698 },
  { state: "MO", segment: "SCR", nblr: 0.681 },
  { state: "NH", segment: "MCH", nblr: 0.684 },
  { state: "NH", segment: "MCR", nblr: 0.687 },
  { state: "NH", segment: "SCH", nblr: 0.681 },
  { state: "NH", segment: "SCR", nblr: 0.684 },
  { state: "GA", segment: "MCH", nblr: 0.66 },
  { state: "GA", segment: "MCR", nblr: 0.646 },
  { state: "GA", segment: "SCH", nblr: 0.679 },
  { state: "GA", segment: "SCR", nblr: 0.671 },
  { state: "OR", segment: "MCH", nblr: 0.653 },
  { state: "OR", segment: "MCR", nblr: 0.655 },
  { state: "OR", segment: "SCH", nblr: 0.654 },
  { state: "OR", segment: "SCR", nblr: 0.65 },
  { state: "VT", segment: "MCH", nblr: 0.657 },
  { state: "VT", segment: "MCR", nblr: 0.643 },
  { state: "VT", segment: "SCH", nblr: 0.65 },
  { state: "VT", segment: "SCR", nblr: 0.629 },
  { state: "NM", segment: "MCH", nblr: 0.647 },
  { state: "NM", segment: "MCR", nblr: 0.629 },
  { state: "NM", segment: "SCH", nblr: 0.637 },
  { state: "NM", segment: "SCR", nblr: 0.617 },
  { state: "ME", segment: "MCH", nblr: 0.638 },
  { state: "ME", segment: "MCR", nblr: 0.636 },
  { state: "ME", segment: "SCH", nblr: 0.636 },
  { state: "ME", segment: "SCR", nblr: 0.622 },
  { state: "WA", segment: "MCH", nblr: 0.634 },
  { state: "WA", segment: "MCR", nblr: 0.63 },
  { state: "WA", segment: "SCH", nblr: 0.641 },
  { state: "WA", segment: "SCR", nblr: 0.629 },
  { state: "NC", segment: "MCH", nblr: 0.666 },
  { state: "NC", segment: "MCR", nblr: 0.665 },
  { state: "NC", segment: "SCH", nblr: 0.601 },
  { state: "NC", segment: "SCR", nblr: 0.607 },
  { state: "NJ", segment: "MCH", nblr: 0.616 },
  { state: "NJ", segment: "MCR", nblr: 0.645 },
  { state: "NJ", segment: "SCH", nblr: 0.628 },
  { state: "NJ", segment: "SCR", nblr: 0.638 },
  { state: "TX", segment: "MCH", nblr: 0.631 },
  { state: "TX", segment: "MCR", nblr: 0.616 },
  { state: "TX", segment: "SCH", nblr: 0.627 },
  { state: "TX", segment: "SCR", nblr: 0.614 },
  { state: "RI", segment: "MCH", nblr: 0.629 },
  { state: "RI", segment: "MCR", nblr: 0.616 },
  { state: "RI", segment: "SCH", nblr: 0.631 },
  { state: "RI", segment: "SCR", nblr: 0.618 },
  { state: "VA", segment: "MCH", nblr: 0.628 },
  { state: "VA", segment: "MCR", nblr: 0.611 },
  { state: "VA", segment: "SCH", nblr: 0.62 },
  { state: "VA", segment: "SCR", nblr: 0.605 },
  { state: "MI", segment: "MCH", nblr: 0.6 },
  { state: "MI", segment: "MCR", nblr: 0.644 },
  { state: "MI", segment: "SCH", nblr: 0.609 },
  { state: "MI", segment: "SCR", nblr: 0.637 },
  { state: "WV", segment: "MCH", nblr: 0.62 },
  { state: "WV", segment: "MCR", nblr: 0.618 },
  { state: "WV", segment: "SCH", nblr: 0.608 },
  { state: "WV", segment: "SCR", nblr: 0.602 },
  { state: "IL", segment: "MCH", nblr: 0.596 },
  { state: "IL", segment: "MCR", nblr: 0.6 },
  { state: "IL", segment: "SCH", nblr: 0.621 },
  { state: "IL", segment: "SCR", nblr: 0.618 },
  { state: "AL", segment: "MCH", nblr: 0.604 },
  { state: "AL", segment: "MCR", nblr: 0.605 },
  { state: "AL", segment: "SCH", nblr: 0.599 },
  { state: "AL", segment: "SCR", nblr: 0.601 },
  { state: "OH", segment: "MCH", nblr: 0.597 },
  { state: "OH", segment: "MCR", nblr: 0.594 },
  { state: "OH", segment: "SCH", nblr: 0.597 },
  { state: "OH", segment: "SCR", nblr: 0.586 },
  { state: "FL", segment: "MCH", nblr: 0.579 },
  { state: "FL", segment: "MCR", nblr: 0.595 },
  { state: "FL", segment: "SCH", nblr: 0.58 },
  { state: "FL", segment: "SCR", nblr: 0.591 },
  { state: "CT", segment: "MCH", nblr: 0.575 },
  { state: "CT", segment: "MCR", nblr: 0.59 },
  { state: "CT", segment: "SCH", nblr: 0.571 },
  { state: "CT", segment: "SCR", nblr: 0.573 },
  { state: "MD", segment: "MCH", nblr: 0.547 },
  { state: "MD", segment: "MCR", nblr: 0.55 },
  { state: "MD", segment: "SCH", nblr: 0.551 },
  { state: "MD", segment: "SCR", nblr: 0.559 },
  { state: "SC", segment: "MCH", nblr: 0.555 },
  { state: "SC", segment: "MCR", nblr: 0.538 },
  { state: "SC", segment: "SCH", nblr: 0.55 },
  { state: "SC", segment: "SCR", nblr: 0.531 },
  { state: "CO", segment: "MCH", nblr: 0.543 },
  { state: "CO", segment: "MCR", nblr: 0.548 },
  { state: "CO", segment: "SCH", nblr: 0.535 },
  { state: "CO", segment: "SCR", nblr: 0.536 },
  { state: "NY", segment: "MCH", nblr: 0.482 },
  { state: "NY", segment: "MCR", nblr: 0.505 },
  { state: "NY", segment: "SCH", nblr: 0.494 },
  { state: "NY", segment: "SCR", nblr: 0.501 },
  { state: "DC", segment: "MCH", nblr: 0.495 },
  { state: "DC", segment: "MCR", nblr: 0.506 },
  { state: "DC", segment: "SCH", nblr: 0.487 },
  { state: "DC", segment: "SCR", nblr: 0.482 },
  { state: "HI", segment: "MCH", nblr: 0.483 },
  { state: "HI", segment: "MCR", nblr: 0.492 },
  { state: "HI", segment: "SCH", nblr: 0.491 },
  { state: "HI", segment: "SCR", nblr: 0.485 },
  { state: "MT", segment: "MCH", nblr: 0.483 },
  { state: "MT", segment: "SCH", nblr: 0.457 },
  { state: "MT", segment: "SCR", nblr: 0.411 },
];
