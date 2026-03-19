import { query, table } from "../db/index.js";
import { config } from "../config.js";
import { normalizeActivityScopeKey, splitCombinedFilter, resolveQbc } from "./shared/activityScope.js";
import { buildCombinedRatioSql, buildRoeSql } from "./shared/kpiSql.js";
import { cached, buildCacheKey } from "../cache.js";

const RAW_CROSS_TACTIC_TABLE = config.rawCrossTacticTable;

export type StateSegmentFilters = {
  startDate?: string;
  endDate?: string;
  states?: string[];
  segments?: string[];
  channelGroups?: string[];
  activityLeadType?: string;
  qbc?: number;
  groupBy?: string;
};

export type PriceExplorationFilters = {
  planId?: string;
  startDate?: string;
  endDate?: string;
  q2bStartDate?: string;
  q2bEndDate?: string;
  states?: string[];
  channelGroups?: string[];
  activityLeadType?: string;
  qbc?: number;
  limit?: number;
  topPairs?: number;
};

export type PlanMergedFilters = {
  startDate?: string;
  endDate?: string;
  states?: string[];
  segments?: string[];
  channelGroups?: string[];
  testingPoints?: string[];
  statSig?: string[];
  activityLeadType?: string;
};

export type StateSegmentPerformanceRow = {
  state: string;
  segment: string;
  channel_group_name: string;
  bids: number;
  sold: number;
  total_cost: number;
  quote_started: number;
  quotes: number;
  binds: number;
  q2b_score: number;
  scored_policies: number;
  cpb: number;
  target_cpb: number;
  performance: number;
  roe: number;
  combined_ratio: number;
  mrltv: number;
  profit: number;
  equity: number;
  /* raw additive _sum columns for correct re-aggregation */
  target_cpb_sum: number;
  avg_profit_sum: number;
  avg_equity_sum: number;
  lifetime_cost_sum: number;
  lifetime_premium_sum: number;
  avg_mrltv_sum: number;
};

export type PriceExplorationRow = {
  channel_group_name: string;
  state: string;
  testing_point: number;
  opps: number;
  bids: number;
  win_rate: number;
  sold: number;
  binds: number;
  quotes: number;
  click_to_quote: number | null;
  channel_quote: number;
  click_to_channel_quote: number | null;
  q2b: number | null;
  channel_binds: number | null;
  channel_q2b: number | null;
  cpc: number;
  avg_bid: number;
  win_rate_uplift_state: number | null;
  cpc_uplift_state: number | null;
  win_rate_uplift_channel: number | null;
  cpc_uplift_channel: number | null;
  win_rate_uplift: number | null;
  cpc_uplift: number | null;
  additional_clicks: number | null;
  expected_bind_change: number | null;
  additional_budget_needed: number | null;
  current_cpb: number | null;
  expected_cpb: number | null;
  cpb_uplift: number | null;
  performance: number | null;
  roe: number | null;
  combined_ratio: number | null;
  recommended_testing_point: number | null;
  algorithm_recommended_tp: number | null;
  is_override: boolean;
  stat_sig: string;
  stat_sig_channel_group: string;
  stat_sig_source: string;
  is_valid_tp: boolean;
};

export type PlanMergedRow = {
  start_date: string;
  end_date: string;
  channel_group_name: string;
  state: string;
  segment: string;
  price_adjustment_percent: number;
  stat_sig: string;
  stat_sig_channel_group: string;
  cpc_uplift: number | null;
  win_rate_uplift: number | null;
  additional_clicks: number | null;
  expected_total_clicks: number | null;
  expected_cpc: number | null;
  expected_total_cost: number | null;
  expected_total_binds: number | null;
  additional_expected_binds: number | null;
  expected_cpb: number | null;
  ss_performance: number | null;
  expected_performance: number | null;
  performance_uplift: number | null;
};

type FilterOptionsRow = {
  states: string[];
  segments: string[];
  channel_groups: string[];
};

type PriceExplorationFilterOptionsRow = {
  states: string[];
  channel_groups: string[];
};

type PlanMergedFilterOptionsRow = {
  states: string[];
  segments: string[];
  channel_groups: string[];
  testing_points: number[];
  stat_sig: string[];
};

export type StrategyRule = {
  id: number;
  name: string;
  states: string[];
  segments: string[];
  maxCpcUplift: number;
  maxCpbUplift: number;
  corTarget: number;
  growthStrategy: string;
  leverScore: number | null;
};

type PriceDecisionOverride = {
  state: string;
  channelGroupName: string;
  segment?: string;
  testingPoint: number;
};

type StrategyAnalysisFilters = {
  planId: string;
  startDate?: string;
  endDate?: string;
  activityLeadType?: string;
  qbc?: number;
};

type StrategyBaselineRow = {
  state: string;
  segment: string;
  bids: number | null;
  sold: number | null;
  total_cost: number | null;
  quotes: number | null;
  binds: number | null;
  scored_policies: number | null;
  q2b: number | null;
  performance: number | null;
  roe: number | null;
  combined_ratio: number | null;
  /* raw additive columns for correct re-aggregation */
  target_cpb_sum: number;
  avg_profit_sum: number;
  avg_equity_sum: number;
  lifetime_cost_sum: number;
  lifetime_premium_sum: number;
};

type StrategyPlanMergedRow = {
  channel_group_name: string;
  state: string;
  segment: string;
  price_adjustment_percent: number | null;
  pe_bids: number | null;
  pe_sold: number | null;
  pe_number_of_quotes: number | null;
  pe_win_rate: number | null;
  pe_total_spend: number | null;
  ss_cpb: number | null;
  expected_cpb: number | null;
  additional_clicks: number | null;
  additional_expected_binds: number | null;
  expected_total_cost: number | null;
};

export type StrategyAnalysisRow = {
  rule_name: string;
  states: string[];
  segments: string[];
  target_cor: number | null;
  bids: number;
  sold: number;
  total_spend: number;
  cpc: number | null;
  wr: number | null;
  quotes: number;
  binds: number;
  current_cpb: number | null;
  expected_cpb: number | null;
  q2b: number | null;
  performance: number | null;
  roe: number | null;
  cor: number | null;
  additional_clicks: number;
  additional_binds: number;
  wr_uplift: number | null;
  cpc_uplift: number | null;
  cpb_uplift: number | null;
  expected_total_cost: number;
  additional_budget: number;
  /* raw sums for frontend re-aggregation (COR view) */
  _scored_policies: number;
  _lifetime_cost_sum: number;
  _lifetime_premium_sum: number;
  _target_cpb_sum: number;
  _avg_profit_sum: number;
  _avg_equity_sum: number;
  _wr_rollup_current: number;
  _wr_rollup_expected: number;
  _cpb_rollup_current: number;
  _cpb_rollup_expected: number;
};

export type StateAnalysisSegmentRow = {
  segment: string;
  bids: number;
  sold: number;
  wr: number | null;
  total_spend: number;
  quotes: number;
  sold_to_quotes: number | null;
  binds: number;
  q2b: number | null;
  cor: number | null;
  roe: number | null;
  cpb: number | null;
  performance: number | null;
};

export type StateAnalysisRuleRow = {
  rule_name: string;
  tier: number;
  strategy_key: "aggressive" | "robustic" | "cautious";
  strategy_label: string;
  states: string[];
  segments: string[];
  kpis: StrategyAnalysisRow & { ltv: number | null };
  segment_rows: StateAnalysisSegmentRow[];
};

export type StateAnalysisStateRow = {
  state: string;
  rule_name: string | null;
  tier: number | null;
  strategy_key: "aggressive" | "robustic" | "cautious" | null;
  strategy_label: string | null;
  total_spend: number;
  cor: number | null;
  roe: number | null;
  binds: number;
  performance: number | null;
  additional_clicks: number;
  additional_binds: number;
  additional_budget: number;
  cpc_uplift: number | null;
  cpb_uplift: number | null;
};

export type StateAnalysisResponse = {
  overall: StrategyAnalysisRow & { ltv: number | null };
  states: StateAnalysisStateRow[];
  state_details: Array<{
    state: string;
    rule_name: string | null;
    tier: number | null;
    strategy_key: "aggressive" | "robustic" | "cautious" | null;
    strategy_label: string | null;
    kpis: StrategyAnalysisRow & { ltv: number | null };
    segment_rows: StateAnalysisSegmentRow[];
  }>;
  rules: StateAnalysisRuleRow[];
};

const ALL_US_STATE_CODES = [
  "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL",
  "GA", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA",
  "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE",
  "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI",
  "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"
];

/** BQ ARRAY(SELECT ...) returns [{value: x}, ...] — unwrap to plain values. */
function unwrapBqArray<T = string>(arr: unknown[] | undefined): T[] {
  return (arr ?? []).map((x: unknown) =>
    typeof x === "object" && x !== null && "value" in x ? (x as { value: T }).value : (x as T)
  );
}

function withAllStateCodes(states?: unknown[]): string[] {
  const normalized = unwrapBqArray<string>(states).map((value) => String(value || "").trim().toUpperCase()).filter(Boolean);
  return [...new Set([...ALL_US_STATE_CODES, ...normalized])].sort();
}

function parseStrategyRules(raw: string, activityLeadType?: string): StrategyRule[] {
  try {
    const parsed = JSON.parse(raw);
    const scopeKey = normalizeActivityScopeKey(activityLeadType);
    let scopedRules: unknown[] = [];
    if (parsed?.scopes && typeof parsed.scopes === "object") {
      const selectedScope = parsed.scopes?.[scopeKey] || parsed.scopes?.all;
      scopedRules = Array.isArray(selectedScope?.rules) ? selectedScope.rules : [];
    } else if (Array.isArray(parsed?.rules)) {
      if (scopeKey === "clicks_auto" || scopeKey === "all") {
        scopedRules = parsed.rules;
      }
    }
    if (!Array.isArray(scopedRules)) {
      return [];
    }

    // Build a leverScore lookup from clicks_auto scope to backfill missing values
    // in other scopes (e.g., calls_auto, leads_auto that were saved before leverScore existed)
    let clicksAutoLeverScores: Map<string, number> | null = null;
    if (scopeKey !== "clicks_auto" && parsed?.scopes?.clicks_auto?.rules) {
      clicksAutoLeverScores = new Map();
      for (const rule of parsed.scopes.clicks_auto.rules) {
        const name = String(rule?.name || "").trim();
        if (name && rule?.leverScore != null && Number(rule.leverScore) > 0) {
          clicksAutoLeverScores.set(name, Number(rule.leverScore));
        }
      }
    }

    return scopedRules
      .map((rule: any, index: number) => {
        let leverScore: number | null = rule?.leverScore != null && Number(rule.leverScore) > 0 ? Number(rule.leverScore) : null;
        // Backfill from clicks_auto if missing
        if (leverScore == null && clicksAutoLeverScores) {
          const name = String(rule?.name || "").trim();
          leverScore = clicksAutoLeverScores.get(name) ?? null;
        }
        return {
          id: Number(rule?.id) || index + 1,
          name: String(rule?.name || "").trim(),
          states: Array.isArray(rule?.states)
            ? rule.states.map((value: string) => String(value || "").trim().toUpperCase()).filter(Boolean)
            : [],
          segments: Array.isArray(rule?.segments)
            ? rule.segments.map((value: string) => String(value || "").trim().toUpperCase()).filter(Boolean)
            : [],
          maxCpcUplift: Number(rule?.maxCpcUplift),
          maxCpbUplift: Number(rule?.maxCpbUplift),
          corTarget: normalizeCorTargetInput(rule?.corTarget),
          growthStrategy: String(rule?.growthStrategy || "balanced").trim().toLowerCase(),
          leverScore
        };
      })
      .filter((rule: StrategyRule) => rule.name && rule.states.length > 0 && rule.segments.length > 0);
  } catch {
    return [];
  }
}

function normalizeCorTargetInput(value: unknown): number {
  const raw = Number(value);
  if (!Number.isFinite(raw) || raw <= 0) {
    return 0;
  }
  // UI label is "(%)" so values like 103.5 are expected and must be normalized to ratio.
  return raw > 2 ? raw / 100 : raw;
}

function extractSegmentFromChannelGroup(channelGroup: string): string {
  const upper = String(channelGroup || "").toUpperCase();
  const match = upper.match(/\b(MCH|MCR|SCH|SCR)\b/);
  if (match) return match[1];
  if (upper.includes("HOME")) return "HOME";
  if (upper.includes("RENT")) return "RENT";
  return "";
}

function buildPriceDecisionOverrideKey(channelGroupName: string, state: string, segment?: string): string {
  return `${String(channelGroupName || "")}|${String(state || "").toUpperCase()}|${String(segment || "").toUpperCase()}`;
}

function parsePriceDecisionOverrides(raw: string, activityLeadType?: string): Map<string, number> {
  const result = new Map<string, number>();
  if (!String(raw || "").trim()) {
    return result;
  }

  try {
    const parsed = JSON.parse(raw);
    const scopeKey = normalizeActivityScopeKey(activityLeadType);
    let scopedOverrides: unknown[] = [];
    if (parsed?.scopes && typeof parsed.scopes === "object") {
      const selectedScope = parsed.scopes?.[scopeKey] || parsed.scopes?.all;
      scopedOverrides = Array.isArray(selectedScope?.overrides) ? selectedScope.overrides : [];
    } else if (Array.isArray(parsed?.overrides)) {
      if (scopeKey === "clicks_auto" || scopeKey === "all") {
        scopedOverrides = parsed.overrides;
      }
    }

    for (const item of scopedOverrides) {
      const entry = item as PriceDecisionOverride;
      const channelGroupName = String(entry?.channelGroupName || "").trim();
      const state = String(entry?.state || "").trim().toUpperCase();
      const segment = String(entry?.segment || "").trim().toUpperCase();
      const testingPoint = Number(entry?.testingPoint);
      if (!channelGroupName || !state || !Number.isFinite(testingPoint)) {
        continue;
      }
      const key = buildPriceDecisionOverrideKey(channelGroupName, state, segment);
      result.set(key, testingPoint);
      if (!segment) {
        const fallbackKey = buildPriceDecisionOverrideKey(channelGroupName, state, "");
        result.set(fallbackKey, testingPoint);
      }
    }
  } catch {
    return result;
  }

  return result;
}

function normalizeUpliftLimit(raw: number, fallback = 0.1): number {
  if (!Number.isFinite(raw)) {
    return fallback;
  }
  const value = raw > 1 ? raw / 100 : raw;
  if (value <= 0) {
    return fallback;
  }
  return value;
}

function toFiniteNumberOrNull(value: unknown): number | null {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

// ---------------------------------------------------------------------------
// Weighted-scoring TP selection
// ---------------------------------------------------------------------------
// Strategy weight profiles: each metric is normalized to [0,1] (higher=better)
// then multiplied by its weight. CPC and CPB are inverted (lower uplift = better).
//
//   aggressive : maximize growth (WR, binds), accept higher costs
//   balanced   : genuine blend of growth + cost efficiency
//   cautious   : minimize cost (CPC, CPB), accept less growth
// ---------------------------------------------------------------------------
const STRATEGY_WEIGHTS: Record<string, { wr: number; binds: number; cpc: number; cpb: number }> = {
  aggressive: { wr: 0.35, binds: 0.35, cpc: 0.15, cpb: 0.15 },
  balanced:   { wr: 0.25, binds: 0.20, cpc: 0.30, cpb: 0.25 },
  cautious:   { wr: 0.10, binds: 0.15, cpc: 0.45, cpb: 0.30 },
};

// Confidence multiplier based on stat_sig tier
const STAT_SIG_CONFIDENCE: Record<string, number> = {
  state: 1.0,
  channel: 0.85,
};

function resolveStrategyKey(raw: string): string {
  const s = String(raw || "").trim().toLowerCase();
  if (s.includes("aggressive") || s.includes("high") || s === "growth") return "aggressive";
  if (s.includes("cautious") || s.includes("cost") || s.includes("low")) return "cautious";
  return "balanced";
}

function chooseRecommendedTestingPoint(
  rows: PriceExplorationRow[],
  rule: StrategyRule | null
): number {
  if (!rows.length) {
    return 0;
  }

  const baselineTestingPoint = Number(
    rows.find((row) => Number(row.testing_point) === 0)?.testing_point ?? 0
  );

  if (!rule) {
    const fallbackByFlag = rows.find(
      (row) =>
        Number.isFinite(Number(row.recommended_testing_point)) &&
        Number(row.testing_point) === Number(row.recommended_testing_point)
    );
    return Number.isFinite(Number(fallbackByFlag?.testing_point))
      ? Number(fallbackByFlag?.testing_point)
      : baselineTestingPoint;
  }

  // --- Step 1: hard constraint filter ---
  const maxCpc = normalizeUpliftLimit(rule.maxCpcUplift, 0.1);
  const maxCpb = normalizeUpliftLimit(rule.maxCpbUplift, 0.1);

  const candidates = rows.filter((row) => {
    const tp = Number(row.testing_point);
    const additionalClicks = Number(row.additional_clicks) || 0;
    const cpcUplift = toFiniteNumberOrNull(row.cpc_uplift);
    const cpbUplift = toFiniteNumberOrNull(row.cpb_uplift);
    return (
      tp !== 0 &&
      row.is_valid_tp !== false &&
      additionalClicks > 0 &&
      cpcUplift !== null &&
      cpbUplift !== null &&
      cpcUplift <= maxCpc &&
      cpbUplift <= maxCpb
    );
  });

  if (!candidates.length) {
    return baselineTestingPoint;
  }

  // Single candidate — skip scoring
  if (candidates.length === 1) {
    return Number(candidates[0].testing_point) || baselineTestingPoint;
  }

  // --- Step 2: extract raw metric arrays for min-max normalization ---
  const wrArr = candidates.map((r) => toFiniteNumberOrNull(r.win_rate_uplift) ?? 0);
  const bindsArr = candidates.map((r) => Number(r.expected_bind_change) || 0);
  const cpcArr = candidates.map((r) => toFiniteNumberOrNull(r.cpc_uplift) ?? 0);
  const cpbArr = candidates.map((r) => toFiniteNumberOrNull(r.cpb_uplift) ?? 0);

  const minMax = (arr: number[]) => {
    let min = arr[0], max = arr[0];
    for (const v of arr) { if (v < min) min = v; if (v > max) max = v; }
    return { min, max, range: max - min };
  };

  const wrBounds = minMax(wrArr);
  const bindsBounds = minMax(bindsArr);
  const cpcBounds = minMax(cpcArr);
  const cpbBounds = minMax(cpbArr);

  // Normalize to [0,1]: higher = better
  // WR & binds: higher value → higher norm
  // CPC & CPB: lower value → higher norm (inverted)
  const norm = (val: number, bounds: { min: number; range: number }, invert: boolean): number => {
    if (bounds.range === 0) return 0.5;
    const n = (val - bounds.min) / bounds.range;
    return invert ? 1 - n : n;
  };

  // --- Step 3: weighted scoring ---
  // COR override: if state's combined_ratio exceeds the tier's corTarget,
  // force cautious weights to prioritize cost reduction
  let strategyKey = resolveStrategyKey(rule.growthStrategy);
  const baselineRow = rows.find((r) => Number(r.testing_point) === 0);
  const stateCor = toFiniteNumberOrNull(baselineRow?.combined_ratio ?? null);
  const corTarget = rule.corTarget;
  if (stateCor !== null && Number.isFinite(corTarget) && corTarget > 0 && stateCor > corTarget) {
    strategyKey = "cautious";
  }
  const w = STRATEGY_WEIGHTS[strategyKey] || STRATEGY_WEIGHTS.balanced;

  const scored = candidates.map((row, i) => {
    const nWr = norm(wrArr[i], wrBounds, false);
    const nBinds = norm(bindsArr[i], bindsBounds, false);
    const nCpc = norm(cpcArr[i], cpcBounds, true);    // inverted: lower CPC = higher score
    const nCpb = norm(cpbArr[i], cpbBounds, true);    // inverted: lower CPB = higher score

    const rawScore = w.wr * nWr + w.binds * nBinds + w.cpc * nCpc + w.cpb * nCpb;

    // Confidence from stat_sig tier
    const confidence = STAT_SIG_CONFIDENCE[String(row.stat_sig || "")] ?? 0.5;

    return {
      tp: Number(row.testing_point) || 0,
      score: rawScore * confidence,
      // Tiebreakers aligned to strategy
      binds: Number(row.expected_bind_change) || 0,
      cpc: toFiniteNumberOrNull(row.cpc_uplift) ?? Infinity,
    };
  });

  // Sort by score DESC, then strategy-aligned tiebreaker
  scored.sort((a, b) => {
    if (a.score !== b.score) return b.score - a.score;
    if (strategyKey === "cautious") {
      return a.cpc - b.cpc;  // lower CPC wins
    }
    return b.binds - a.binds; // more binds wins (aggressive & balanced)
  });

  return scored[0]?.tp || baselineTestingPoint;
}

export async function getStrategyRulesForPlan(
  planId: string | undefined,
  activityLeadType?: string
): Promise<StrategyRule[]> {
  const normalizedPlanId = String(planId || "").trim();
  if (!normalizedPlanId) {
    return [];
  }
  const paramRows = await query<{ param_value: string }>(
    `
      SELECT param_value
      FROM ${table("plan_parameters")}
      WHERE plan_id = @planId
        AND param_key = 'plan_strategy_config'
      LIMIT 1
    `,
    { planId: normalizedPlanId }
  );
  return parseStrategyRules(paramRows[0]?.param_value || "", activityLeadType);
}

async function getPriceDecisionOverridesForPlan(
  planId: string | undefined,
  activityLeadType?: string
): Promise<Map<string, number>> {
  const normalizedPlanId = String(planId || "").trim();
  if (!normalizedPlanId) {
    return new Map();
  }
  const paramRows = await query<{ param_value: string }>(
    `
      SELECT param_value
      FROM ${table("plan_parameters")}
      WHERE plan_id = @planId
        AND param_key = 'price_exploration_decisions'
      LIMIT 1
    `,
    { planId: normalizedPlanId }
  );
  return parsePriceDecisionOverrides(paramRows[0]?.param_value || "", activityLeadType);
}


function normalizeFilters(filters: StateSegmentFilters) {
  const states = (filters.states ?? []).map((value) => value.trim()).filter(Boolean);
  const segments = (filters.segments ?? []).map((value) => value.trim().toUpperCase()).filter(Boolean);
  const channelGroups = (filters.channelGroups ?? []).map((value) => value.trim()).filter(Boolean);
  const combined = splitCombinedFilter(filters.activityLeadType);

  return {
    startDate: filters.startDate || "",
    endDate: filters.endDate || "",
    states: states.length > 0 ? states : ["__ALL__"],
    segments: segments.length > 0 ? segments : ["__ALL__"],
    channelGroups: channelGroups.length > 0 ? channelGroups : ["__ALL__"],
    activityType: combined.activityType,
    leadType: combined.leadType,
    activityPattern: combined.activityPattern,
    leadPattern: combined.leadPattern,
    stateSegmentActivityType: combined.stateSegmentActivityType,
    stateSegmentLeadType: combined.stateSegmentLeadType,
    qbc: Number.isFinite(Number(filters.qbc)) ? Number(filters.qbc) : 0
  };
}

function normalizePriceExplorationFilters(filters: PriceExplorationFilters) {
  const states = (filters.states ?? []).map((value) => value.trim()).filter(Boolean);
  const channelGroups = (filters.channelGroups ?? []).map((value) => value.trim()).filter(Boolean);
  const combined = splitCombinedFilter(filters.activityLeadType);

  return {
    startDate: filters.startDate || "",
    endDate: filters.endDate || "",
    q2bStartDate: filters.q2bStartDate || filters.startDate || "",
    q2bEndDate: filters.q2bEndDate || filters.endDate || "",
    states: states.length > 0 ? states : ["__ALL__"],
    channelGroups: channelGroups.length > 0 ? channelGroups : ["__ALL__"],
    activityType: combined.activityType,
    leadType: combined.leadType,
    activityPattern: combined.activityPattern,
    leadPattern: combined.leadPattern,
    stateSegmentActivityType: combined.stateSegmentActivityType,
    stateSegmentLeadType: combined.stateSegmentLeadType,
    qbc: Number.isFinite(Number(filters.qbc)) ? Number(filters.qbc) : 0,
    limit: Number.isFinite(Number(filters.limit)) ? Math.min(Math.max(Number(filters.limit), 1), 200000) : 10000,
    topPairs: Number.isFinite(Number(filters.topPairs)) ? Math.max(Number(filters.topPairs), 0) : 0
  };
}

function normalizePlanMergedFilters(filters: PlanMergedFilters) {
  const states = (filters.states ?? []).map((value) => value.trim().toUpperCase()).filter(Boolean);
  const segments = (filters.segments ?? []).map((value) => value.trim().toUpperCase()).filter(Boolean);
  const channelGroups = (filters.channelGroups ?? []).map((value) => value.trim()).filter(Boolean);
  const testingPoints = (filters.testingPoints ?? [])
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value));
  const statSig = (filters.statSig ?? []).map((value) => value.trim().toLowerCase()).filter(Boolean);
  const combined = splitCombinedFilter(filters.activityLeadType);

  return {
    startDate: filters.startDate || "",
    endDate: filters.endDate || "",
    states: states.length > 0 ? states : ["__ALL__"],
    segments: segments.length > 0 ? segments : ["__ALL__"],
    channelGroups: channelGroups.length > 0 ? channelGroups : ["__ALL__"],
    testingPoints: testingPoints.length > 0 ? testingPoints : [999999999],
    statSig: statSig.length > 0 ? statSig : ["__ALL__"],
    activityType: combined.activityType,
    leadType: combined.leadType,
    activityPattern: combined.activityPattern,
    leadPattern: combined.leadPattern
  };
}

export async function listStateSegmentFilters(filters: StateSegmentFilters): Promise<FilterOptionsRow> {
  const normalized = normalizeFilters(filters);
  return listStateSegmentFiltersFromDaily(normalized);
}

const VIEW_DIMENSIONS: Record<string, string[]> = {
  state: ["state"],
  segment: ["segment"],
  channel_group: ["channel_group_name"],
  state_segment: ["state", "segment"],
  state_channel_group: ["state", "channel_group_name"],
  state_segment_channel: ["state", "segment", "channel_group_name"],
};

async function getStateSegmentPerformanceFromDaily(
  filters: StateSegmentFilters,
  normalized: ReturnType<typeof normalizeFilters>
): Promise<StateSegmentPerformanceRow[]> {
  const dims = VIEW_DIMENSIONS[filters.groupBy || "state_segment_channel"]
    || VIEW_DIMENSIONS.state_segment_channel;

  const selectDims = [
    dims.includes("state") ? "state" : "'ALL' AS state",
    dims.includes("segment") ? "segment" : "'ALL' AS segment",
    dims.includes("channel_group_name") ? "channel_group_name" : "'ALL' AS channel_group_name",
  ].join(",\n        ");
  const groupByClause = dims.join(", ");

  const cacheKey = buildCacheKey("ssp", {
    startDate: normalized.startDate,
    endDate: normalized.endDate,
    states: normalized.states,
    segments: normalized.segments,
    channelGroups: normalized.channelGroups,
    activityType: normalized.activityType,
    leadType: normalized.leadType,
    qbc: normalized.qbc,
    groupBy: filters.groupBy || "state_segment_channel"
  });

  return cached(cacheKey, () => query<StateSegmentPerformanceRow>(
    `
      SELECT
        ${selectDims},
        SUM(bids) AS bids,
        SUM(sold) AS sold,
        SUM(total_cost) AS total_cost,
        SUM(quote_started) AS quote_started,
        SUM(quotes) AS quotes,
        SUM(binds) AS binds,
        SAFE_DIVIDE(SUM(binds), NULLIF(SUM(quotes), 0)) AS q2b_score,
        SUM(scored_policies) AS scored_policies,
        SAFE_DIVIDE(SUM(total_cost), NULLIF(SUM(binds), 0)) AS cpb,
        CASE WHEN SUM(binds) = 0 THEN 0
          ELSE SAFE_DIVIDE(SUM(target_cpb_sum), SUM(binds))
        END AS target_cpb,
        SAFE_DIVIDE(
          CASE WHEN SUM(binds) = 0 THEN 0
            ELSE SAFE_DIVIDE(SUM(target_cpb_sum), SUM(binds))
          END,
          SAFE_DIVIDE(SUM(total_cost), NULLIF(SUM(binds), 0))
        ) AS performance,
        ${buildRoeSql({
          zeroConditions: [
            "SUM(scored_policies) = 0",
            "SAFE_DIVIDE(SUM(avg_equity_sum), NULLIF(SUM(scored_policies), 0)) = 0"
          ],
          avgProfitExpr: "SAFE_DIVIDE(SUM(avg_profit_sum), NULLIF(SUM(scored_policies), 0))",
          cpbExpr: "SAFE_DIVIDE(SUM(total_cost), NULLIF(SUM(binds), 0))",
          avgEquityExpr: "SAFE_DIVIDE(SUM(avg_equity_sum), NULLIF(SUM(scored_policies), 0))"
        })} AS roe,
        ${buildCombinedRatioSql({
          zeroConditions: [
            "SUM(scored_policies) = 0",
            "SAFE_DIVIDE(SUM(lifetime_premium_sum), NULLIF(SUM(scored_policies), 0)) = 0"
          ],
          cpbExpr: "SAFE_DIVIDE(SUM(total_cost), NULLIF(SUM(binds), 0))",
          avgLifetimeCostExpr: "SAFE_DIVIDE(SUM(lifetime_cost_sum), NULLIF(SUM(scored_policies), 0))",
          avgLifetimePremiumExpr: "SAFE_DIVIDE(SUM(lifetime_premium_sum), NULLIF(SUM(scored_policies), 0))"
        })} AS combined_ratio,
        SAFE_DIVIDE(SUM(avg_mrltv_sum), NULLIF(SUM(scored_policies), 0)) AS mrltv,
        SAFE_DIVIDE(SUM(avg_profit_sum), NULLIF(SUM(scored_policies), 0)) AS profit,
        SAFE_DIVIDE(SUM(avg_equity_sum), NULLIF(SUM(scored_policies), 0)) AS equity,
        SUM(target_cpb_sum) AS target_cpb_sum,
        SUM(avg_profit_sum) AS avg_profit_sum,
        SUM(avg_equity_sum) AS avg_equity_sum,
        SUM(lifetime_cost_sum) AS lifetime_cost_sum,
        SUM(lifetime_premium_sum) AS lifetime_premium_sum,
        SUM(avg_mrltv_sum) AS avg_mrltv_sum
      FROM ${table("state_segment_daily")}
      WHERE event_date BETWEEN @startDate::date AND @endDate::date
        AND ('__ALL__' = ANY(@states) OR state = ANY(@states))
        AND ('__ALL__' = ANY(@segments) OR segment = ANY(@segments))
        AND ('__ALL__' = ANY(@channelGroups) OR channel_group_name = ANY(@channelGroups))
        AND (@activityType = '' OR activity_type = @activityType)
        AND (@leadType = '' OR lead_type = @leadType)
      GROUP BY ${groupByClause}
      ORDER BY ${groupByClause}
    `,
    normalized
  ));
}

async function listStateSegmentFiltersFromDaily(
  normalized: ReturnType<typeof normalizeFilters>
): Promise<FilterOptionsRow> {
  const cacheKey = buildCacheKey("ssp-filters", {
    startDate: normalized.startDate,
    endDate: normalized.endDate,
    activityType: normalized.activityType,
    leadType: normalized.leadType
  });

  return cached(cacheKey, async () => {
    const rows = await query<FilterOptionsRow>(
      `
        WITH scoped AS (
          SELECT state, segment, channel_group_name
          FROM ${table("state_segment_daily")}
          WHERE (@startDate = '' OR event_date >= @startDate::date)
            AND (@endDate = '' OR event_date <= @endDate::date)
            AND (@activityType = '' OR activity_type = @activityType)
            AND (@leadType = '' OR lead_type = @leadType)
        )
        SELECT
          ARRAY(
            SELECT DISTINCT state
            FROM scoped
            WHERE state IS NOT NULL
            ORDER BY state
          ) AS states,
          ARRAY(
            SELECT DISTINCT segment
            FROM scoped
            WHERE segment IS NOT NULL AND segment IN ('MCH', 'MCR', 'SCH', 'SCR', 'HOME', 'RENT')
            ORDER BY segment
          ) AS segments,
          ARRAY(
            SELECT DISTINCT channel_group_name
            FROM scoped
            WHERE channel_group_name IS NOT NULL AND channel_group_name != ''
            ORDER BY channel_group_name
          ) AS channel_groups
      `,
      normalized
    );

    const first = rows[0];
    return {
      states: withAllStateCodes(first?.states),
      segments: unwrapBqArray<string>(first?.segments),
      channel_groups: unwrapBqArray<string>(first?.channel_groups)
    };
  });
}

export async function getStateSegmentPerformance(
  filters: StateSegmentFilters
): Promise<StateSegmentPerformanceRow[]> {
  const normalized = normalizeFilters(filters);
  return getStateSegmentPerformanceFromDaily(filters, normalized);
}

export async function listPriceExplorationFilters(
  filters: Pick<PriceExplorationFilters, "startDate" | "endDate" | "activityLeadType">
): Promise<{ states: string[]; channelGroups: string[] }> {
  const normalized = normalizePriceExplorationFilters(filters);

  const cacheKey = buildCacheKey("pe-filters", {
    startDate: normalized.startDate,
    endDate: normalized.endDate,
    activityType: normalized.activityType,
    leadType: normalized.leadType
  });

  return cached(cacheKey, async () => {
    const stateRows = await query<{ state: string }>(
      `SELECT DISTINCT state
       FROM ${table("price_exploration_daily")}
       WHERE state IS NOT NULL
         AND (@startDate = '' OR date >= @startDate::date)
         AND (@endDate = '' OR date <= @endDate::date)
         AND (@activityType = '' OR activity_type = @activityType)
         AND (@leadType = '' OR lead_type = @leadType)
       ORDER BY state`,
      { startDate: normalized.startDate, endDate: normalized.endDate,
        activityType: normalized.activityType, leadType: normalized.leadType }
    );

    const channelRows = await query<{ channel_group_name: string }>(
      `SELECT DISTINCT channel_group_name
       FROM ${table("price_exploration_daily")}
       WHERE channel_group_name IS NOT NULL
         AND (@startDate = '' OR date >= @startDate::date)
         AND (@endDate = '' OR date <= @endDate::date)
         AND (@activityType = '' OR activity_type = @activityType)
         AND (@leadType = '' OR lead_type = @leadType)
       ORDER BY channel_group_name`,
      { startDate: normalized.startDate, endDate: normalized.endDate,
        activityType: normalized.activityType, leadType: normalized.leadType }
    );

    return {
      states: withAllStateCodes(stateRows.map(r => r.state)),
      channelGroups: channelRows.map(r => r.channel_group_name)
    };
  });
}

/**
 * Price Exploration data from PG tables.
 * Reads from price_exploration_daily + state_segment_daily (both synced daily from BQ).
 * Returns raw rows with SQL-level recommended_testing_point (basic cpb_uplift heuristic).
 * Strategy rules and PE decisions are applied AFTER this function returns, so
 * user changes take effect immediately without cache invalidation.
 */
async function getPriceExplorationBQ(
  normalized: ReturnType<typeof normalizePriceExplorationFilters>
): Promise<PriceExplorationRow[]> {
  const cacheKey = buildCacheKey("pe-bq", {
    startDate: normalized.startDate,
    endDate: normalized.endDate,
    q2bStartDate: normalized.q2bStartDate,
    q2bEndDate: normalized.q2bEndDate,
    states: normalized.states,
    channelGroups: normalized.channelGroups,
    activityType: normalized.activityType,
    leadType: normalized.leadType,
    qbc: normalized.qbc,
    limit: normalized.limit,
    topPairs: normalized.topPairs
  });

  return cached(cacheKey, () => query<PriceExplorationRow>(
    `
      WITH
      /* ═══════════════════════════════════════════════════════════════
         Part A: PE metrics from price_exploration_daily
         Aggregate daily rows across date range
         ═══════════════════════════════════════════════════════════════ */

      -- All states (for channel-level metrics), filtered by activity/lead/channel/date
      pe_all AS (
        SELECT
          channel_group_name,
          state,
          activity_type AS activity_group,
          lead_type AS lead_group,
          price_adjustment_percent,
          SUM(opps)::bigint AS opps,
          SUM(bids) AS bids,
          SUM(total_impressions) AS total_impressions,
          CASE WHEN SUM(bids) > 0 THEN SUM(avg_position * bids) / SUM(bids) ELSE 0 END AS avg_position,
          SUM(sold) AS sold,
          SUM(total_spend) AS total_spend,
          CASE WHEN SUM(bids) > 0 THEN SUM(avg_bid * bids) / SUM(bids) ELSE 0 END AS avg_bid,
          SUM(number_of_quote_started) AS number_of_quote_started,
          SUM(number_of_quotes) AS number_of_quotes,
          SUM(number_of_binds) AS number_of_binds
        FROM price_exploration_daily
        WHERE date BETWEEN
            CASE WHEN @startDate = '' THEN CURRENT_DATE - 14 ELSE @startDate::date END
            AND CASE WHEN @endDate = '' THEN CURRENT_DATE ELSE @endDate::date END
          AND ('__ALL__' = ANY(@channelGroups) OR channel_group_name = ANY(@channelGroups))
          AND (@activityType = '' OR activity_type = @activityType)
          AND (@leadType = '' OR lead_type = @leadType)
        GROUP BY 1, 2, 3, 4, 5
      ),
      -- State-filtered subset with computed ratios
      state_tp AS (
        SELECT
          channel_group_name, state, activity_group, lead_group, price_adjustment_percent,
          opps, bids, total_impressions, avg_position, sold, total_spend, avg_bid,
          number_of_quote_started, number_of_quotes, number_of_binds,
          SAFE_DIVIDE(sold, NULLIF(bids, 0)) AS win_rate,
          SAFE_DIVIDE(total_spend, NULLIF(sold, 0)) AS cpc,
          SAFE_DIVIDE(number_of_quotes, NULLIF(sold, 0)) AS click_to_quote,
          SAFE_DIVIDE(number_of_quote_started, NULLIF(sold, 0)) AS quote_start_rate
        FROM pe_all
        WHERE ('__ALL__' = ANY(@states) OR state = ANY(@states))
      ),
      -- Channel aggregation from pe_all (ALL states, not just filtered)
      channel_tp AS (
        SELECT
          channel_group_name,
          activity_group,
          lead_group,
          price_adjustment_percent,
          SUM(bids) AS channel_bids,
          SUM(sold) AS channel_sold,
          SUM(total_spend) AS channel_total_spend,
          SAFE_DIVIDE(SUM(sold), NULLIF(SUM(bids), 0)) AS channel_win_rate,
          SAFE_DIVIDE(SUM(total_spend), NULLIF(SUM(sold), 0)) AS channel_cpc
        FROM pe_all
        GROUP BY 1, 2, 3, 4
      ),
      joined AS (
        SELECT
          s.*,
          b.win_rate AS baseline_win_rate,
          b.cpc AS baseline_cpc,
          b.bids AS baseline_bids,
          b.sold AS baseline_sold,
          c.channel_bids,
          c.channel_sold,
          c.channel_win_rate,
          c.channel_cpc,
          cb.channel_win_rate AS channel_baseline_win_rate,
          cb.channel_cpc AS channel_baseline_cpc,
          cb.channel_bids AS channel_baseline_bids,
          cb.channel_sold AS channel_baseline_sold,
          -- Channel metrics excluding this state (subtraction: channel_total - this_state)
          -- Used for the 50-199 bids fallback so the state doesn't inflate its own channel benchmark
          SAFE_DIVIDE(
            c.channel_sold - COALESCE(s.sold, 0),
            NULLIF(c.channel_bids - COALESCE(s.bids, 0), 0)
          ) AS channel_ex_win_rate,
          SAFE_DIVIDE(
            c.channel_total_spend - COALESCE(s.total_spend, 0),
            NULLIF(c.channel_sold - COALESCE(s.sold, 0), 0)
          ) AS channel_ex_cpc,
          SAFE_DIVIDE(
            cb.channel_sold - COALESCE(b.sold, 0),
            NULLIF(cb.channel_bids - COALESCE(b.bids, 0), 0)
          ) AS channel_baseline_ex_win_rate,
          SAFE_DIVIDE(
            cb.channel_total_spend - COALESCE(b.total_spend, 0),
            NULLIF(cb.channel_sold - COALESCE(b.sold, 0), 0)
          ) AS channel_baseline_ex_cpc,
          -- Channel bids excluding this state (for 600-bid channel threshold)
          (c.channel_bids - COALESCE(s.bids, 0)) AS channel_ex_bids
        FROM state_tp s
        LEFT JOIN state_tp b
          ON b.channel_group_name = s.channel_group_name
         AND b.state = s.state
         AND b.activity_group = s.activity_group
         AND b.lead_group = s.lead_group
         AND b.price_adjustment_percent = 0
        LEFT JOIN channel_tp c
          ON c.channel_group_name = s.channel_group_name
         AND c.activity_group = s.activity_group
         AND c.lead_group = s.lead_group
         AND c.price_adjustment_percent = s.price_adjustment_percent
        LEFT JOIN channel_tp cb
          ON cb.channel_group_name = s.channel_group_name
         AND cb.activity_group = s.activity_group
         AND cb.lead_group = s.lead_group
         AND cb.price_adjustment_percent = 0
      ),
      per_group AS (
        SELECT
          channel_group_name,
          state,
          activity_group,
          lead_group,
          price_adjustment_percent AS testing_point,
          opps,
          bids,
          sold,
          number_of_binds,
          number_of_quotes,
          avg_bid,
          cpc,
          total_spend,
          -- Sample-size classification:
          -- 'baseline'     : testing_point = 0
          -- 'disqualified' : bids < 50, or sold < 15, or channel fallback with < 600 channel_ex_bids
          -- 'channel'      : 50-199 bids + >= 600 channel_ex_bids — blend state + channel uplifts
          -- 'state'        : >= 200 bids + >= 15 sold — use state-level directly
          CASE
            WHEN price_adjustment_percent = 0          THEN 'baseline'
            WHEN bids < 50                             THEN 'disqualified'
            WHEN sold < 15                             THEN 'disqualified'
            WHEN bids >= 200                           THEN 'state'
            WHEN COALESCE(channel_ex_bids, 0) < 600   THEN 'disqualified'
            ELSE                                            'channel'
          END AS stat_sig,
          -- State-level uplifts (used when stat_sig = 'state')
          CASE
            WHEN price_adjustment_percent = 0 THEN NULL
            ELSE SAFE_DIVIDE(cpc - baseline_cpc, NULLIF(baseline_cpc, 0))
          END AS cpc_uplift_state,
          CASE
            WHEN price_adjustment_percent = 0 THEN NULL
            ELSE SAFE_DIVIDE(win_rate - baseline_win_rate, NULLIF(baseline_win_rate, 0))
          END AS win_rate_uplift_state,
          -- Channel-level uplifts: weighted blend of state + channel (excl. state)
          -- Gives the state's own signal proportional weight even at 50-199 bids
          CASE
            WHEN price_adjustment_percent = 0 THEN NULL
            ELSE SAFE_DIVIDE(
              COALESCE(SAFE_DIVIDE(cpc - baseline_cpc, NULLIF(baseline_cpc, 0)), 0) * bids
              + COALESCE(SAFE_DIVIDE(channel_ex_cpc - channel_baseline_ex_cpc, NULLIF(channel_baseline_ex_cpc, 0)), 0) * COALESCE(channel_ex_bids, 0),
              NULLIF(bids + COALESCE(channel_ex_bids, 0), 0)
            )
          END AS cpc_uplift_channel,
          CASE
            WHEN price_adjustment_percent = 0 THEN NULL
            ELSE SAFE_DIVIDE(
              COALESCE(SAFE_DIVIDE(win_rate - baseline_win_rate, NULLIF(baseline_win_rate, 0)), 0) * bids
              + COALESCE(SAFE_DIVIDE(channel_ex_win_rate - channel_baseline_ex_win_rate, NULLIF(channel_baseline_ex_win_rate, 0)), 0) * COALESCE(channel_ex_bids, 0),
              NULLIF(bids + COALESCE(channel_ex_bids, 0), 0)
            )
          END AS win_rate_uplift_channel,
          CASE
            WHEN price_adjustment_percent = 0          THEN NULL
            WHEN bids < 50                             THEN NULL
            WHEN sold < 15                             THEN NULL
            WHEN bids >= 200                           THEN (win_rate - baseline_win_rate) * bids
            WHEN COALESCE(channel_ex_bids, 0) < 600   THEN NULL
            ELSE
              -- Blended absolute WR diff × state bids
              SAFE_DIVIDE(
                (win_rate - COALESCE(baseline_win_rate, 0)) * bids
                + (COALESCE(channel_ex_win_rate, 0) - COALESCE(channel_baseline_ex_win_rate, 0)) * COALESCE(channel_ex_bids, 0),
                NULLIF(bids + COALESCE(channel_ex_bids, 0), 0)
              ) * bids
          END AS additional_clicks,
          channel_ex_bids
        FROM joined
      ),
      final_agg AS (
        SELECT
          channel_group_name,
          state,
          testing_point,
          SUM(opps) AS opps,
          SUM(bids) AS bids,
          SAFE_DIVIDE(SUM(sold), NULLIF(SUM(bids), 0)) AS win_rate,
          SUM(sold) AS sold,
          SUM(number_of_binds) AS binds,
          SUM(number_of_quotes) AS quotes,
          SAFE_DIVIDE(SUM(number_of_quotes), NULLIF(SUM(bids), 0)) AS click_to_quote,
          SAFE_DIVIDE(SUM(total_spend), NULLIF(SUM(sold), 0)) AS cpc,
          SAFE_DIVIDE(SUM(avg_bid * bids), NULLIF(SUM(bids), 0)) AS avg_bid,
          SUM(total_spend) AS total_spend,
          -- Weighted uplifts across activity/lead groups
          SAFE_DIVIDE(
            SUM(CASE WHEN win_rate_uplift_state IS NOT NULL THEN win_rate_uplift_state * bids ELSE 0 END),
            NULLIF(SUM(CASE WHEN win_rate_uplift_state IS NOT NULL THEN bids ELSE 0 END), 0)
          ) AS win_rate_uplift_state,
          SAFE_DIVIDE(
            SUM(CASE WHEN cpc_uplift_state IS NOT NULL THEN cpc_uplift_state * sold ELSE 0 END),
            NULLIF(SUM(CASE WHEN cpc_uplift_state IS NOT NULL THEN sold ELSE 0 END), 0)
          ) AS cpc_uplift_state,
          SAFE_DIVIDE(
            SUM(CASE WHEN win_rate_uplift_channel IS NOT NULL THEN win_rate_uplift_channel * bids ELSE 0 END),
            NULLIF(SUM(CASE WHEN win_rate_uplift_channel IS NOT NULL THEN bids ELSE 0 END), 0)
          ) AS win_rate_uplift_channel,
          SAFE_DIVIDE(
            SUM(CASE WHEN cpc_uplift_channel IS NOT NULL THEN cpc_uplift_channel * sold ELSE 0 END),
            NULLIF(SUM(CASE WHEN cpc_uplift_channel IS NOT NULL THEN sold ELSE 0 END), 0)
          ) AS cpc_uplift_channel,
          SUM(COALESCE(additional_clicks, 0)) AS additional_clicks,
          MAX(COALESCE(channel_ex_bids, 0)) AS channel_ex_bids,
          -- Re-apply thresholds on the aggregated totals
          CASE
            WHEN testing_point = 0                             THEN 'baseline'
            WHEN SUM(bids) < 50                                THEN 'disqualified'
            WHEN SUM(sold) < 15                                THEN 'disqualified'
            WHEN SUM(bids) >= 200                              THEN 'state'
            WHEN MAX(COALESCE(channel_ex_bids, 0)) < 600      THEN 'disqualified'
            ELSE                                                    'channel'
          END AS stat_sig
        FROM per_group
        GROUP BY channel_group_name, state, testing_point
      ),
      with_budget AS (
        SELECT
          *,
          SUM(bids) OVER (PARTITION BY channel_group_name, state) AS total_bids_channel_state,
          SUM(sold) OVER (PARTITION BY channel_group_name, state) AS current_sold_channel_state,
          SUM(quotes) OVER (PARTITION BY channel_group_name, state) AS state_ch_quotes,
          SUM(total_spend) OVER (PARTITION BY channel_group_name, state) AS current_spend_channel_state
        FROM final_agg
      ),
      with_expected AS (
        SELECT
          *,
          MAX(CASE WHEN testing_point = 0 THEN win_rate END) OVER (PARTITION BY channel_group_name, state)
            AS baseline_win_rate_channel_state,
          MAX(CASE WHEN testing_point = 0 THEN cpc END) OVER (PARTITION BY channel_group_name, state)
            AS baseline_cpc_channel_state,
          (win_rate * total_bids_channel_state) AS expected_clicks,
          (win_rate * total_bids_channel_state * cpc) AS expected_total_cost,
          -- Baseline expected clicks for delta calculations
          MAX(CASE WHEN testing_point = 0 THEN win_rate * total_bids_channel_state END)
            OVER (PARTITION BY channel_group_name, state) AS baseline_expected_clicks,
          -- Baseline expected cost for delta calculations
          MAX(CASE WHEN testing_point = 0 THEN win_rate * total_bids_channel_state * cpc END)
            OVER (PARTITION BY channel_group_name, state) AS baseline_expected_cost
        FROM with_budget
      ),
      -- Financial metrics per state+channel from state_segment_daily
      state_channel_financials AS (
        SELECT
          channel_group_name,
          state,
          SAFE_DIVIDE(SUM(COALESCE(avg_profit_sum, 0)), NULLIF(SUM(COALESCE(scored_policies, 0)), 0)) AS avg_profit,
          SAFE_DIVIDE(SUM(COALESCE(avg_equity_sum, 0)), NULLIF(SUM(COALESCE(scored_policies, 0)), 0)) AS avg_equity,
          SAFE_DIVIDE(
            SUM(COALESCE(lifetime_premium_sum, 0)),
            NULLIF(SUM(COALESCE(scored_policies, 0)), 0)
          ) AS avg_lifetime_premium,
          SAFE_DIVIDE(SUM(COALESCE(lifetime_cost_sum, 0)), NULLIF(SUM(COALESCE(scored_policies, 0)), 0)) AS avg_lifetime_cost
        FROM state_segment_daily
        WHERE event_date BETWEEN
            CASE WHEN @startDate = '' THEN CURRENT_DATE - 14 ELSE @startDate::date END
            AND CASE WHEN @endDate = '' THEN CURRENT_DATE ELSE @endDate::date END
          AND ('__ALL__' = ANY(@states) OR state = ANY(@states))
          AND (@activityType = '' OR activity_type = @activityType)
          AND (@leadType = '' OR lead_type = @leadType)
        GROUP BY channel_group_name, state
      ),
      ssd_metrics AS (
        SELECT
          channel_group_name,
          state,
          SUM(binds) AS ssd_binds,
          SUM(quotes) AS ssd_quotes,
          SAFE_DIVIDE(SUM(binds), NULLIF(SUM(quotes), 0)) AS ssd_q2b,
          SAFE_DIVIDE(
            CASE WHEN SUM(binds) = 0 THEN 0
              ELSE SAFE_DIVIDE(SUM(target_cpb_sum), SUM(binds))
            END,
            SAFE_DIVIDE(SUM(total_cost), NULLIF(SUM(binds), 0))
          ) AS ssd_performance,
          ${buildRoeSql({
            zeroConditions: [
              "SUM(scored_policies) = 0",
              "SAFE_DIVIDE(SUM(avg_equity_sum), NULLIF(SUM(scored_policies), 0)) = 0"
            ],
            avgProfitExpr: "SAFE_DIVIDE(SUM(avg_profit_sum), NULLIF(SUM(scored_policies), 0))",
            cpbExpr: "SAFE_DIVIDE(SUM(total_cost), NULLIF(SUM(binds), 0))",
            avgEquityExpr: "SAFE_DIVIDE(SUM(avg_equity_sum), NULLIF(SUM(scored_policies), 0))"
          })} AS ssd_roe,
          ${buildCombinedRatioSql({
            zeroConditions: [
              "SUM(scored_policies) = 0",
              "SAFE_DIVIDE(SUM(lifetime_premium_sum), NULLIF(SUM(scored_policies), 0)) = 0"
            ],
            cpbExpr: "SAFE_DIVIDE(SUM(total_cost), NULLIF(SUM(binds), 0))",
            avgLifetimeCostExpr: "SAFE_DIVIDE(SUM(lifetime_cost_sum), NULLIF(SUM(scored_policies), 0))",
            avgLifetimePremiumExpr: "SAFE_DIVIDE(SUM(lifetime_premium_sum), NULLIF(SUM(scored_policies), 0))"
          })} AS ssd_combined_ratio
        FROM state_segment_daily
        WHERE event_date BETWEEN
            CASE WHEN @startDate = '' THEN CURRENT_DATE - 14 ELSE @startDate::date END
            AND CASE WHEN @endDate = '' THEN CURRENT_DATE ELSE @endDate::date END
          AND ('__ALL__' = ANY(@states) OR state = ANY(@states))
          AND ('__ALL__' = ANY(@channelGroups) OR channel_group_name = ANY(@channelGroups))
          AND (@activityType = '' OR activity_type = @activityType)
          AND (@leadType = '' OR lead_type = @leadType)
        GROUP BY channel_group_name, state
      ),
      -- Binds per state+channel from PE data (state-filtered)
      state_channel_binds AS (
        SELECT
          channel_group_name,
          state,
          SUM(number_of_binds) AS binds_state_channel
        FROM pe_all
        WHERE ('__ALL__' = ANY(@states) OR state = ANY(@states))
        GROUP BY channel_group_name, state
      ),
      -- Channel-level binds from PE (all states)
      channel_binds AS (
        SELECT
          channel_group_name,
          SUM(number_of_binds) AS channel_binds
        FROM pe_all
        GROUP BY channel_group_name
      ),
      -- Channel-wide quotes and click-to-quote rate (all states, all TPs) for display
      channel_quotes_all AS (
        SELECT
          channel_group_name,
          SUM(number_of_quotes) AS channel_quote,
          SAFE_DIVIDE(
            SUM(number_of_quotes),
            NULLIF(SUM(sold), 0)
          ) AS click_to_channel_quote
        FROM pe_all
        GROUP BY channel_group_name
      ),
      q2b_source AS (
        SELECT
          channel_group_name,
          state,
          SAFE_DIVIDE(SUM(COALESCE(binds, 0)), NULLIF(SUM(COALESCE(quotes, 0)), 0)) AS q2b
        FROM state_segment_daily
        WHERE (@q2bStartDate = '' OR event_date >= @q2bStartDate::date)
          AND (@q2bEndDate = '' OR event_date <= @q2bEndDate::date)
          AND (@activityType = '' OR activity_type = @activityType)
          AND (@leadType = '' OR lead_type = @leadType)
        GROUP BY channel_group_name, state
      ),
      q2b_channel AS (
        SELECT
          channel_group_name,
          SAFE_DIVIDE(SUM(COALESCE(binds, 0)), NULLIF(SUM(COALESCE(quotes, 0)), 0)) AS channel_q2b
        FROM state_segment_daily
        WHERE (@q2bStartDate = '' OR event_date >= @q2bStartDate::date)
          AND (@q2bEndDate = '' OR event_date <= @q2bEndDate::date)
          AND (@activityType = '' OR activity_type = @activityType)
          AND (@leadType = '' OR lead_type = @leadType)
        GROUP BY channel_group_name
      ),
      -- Q2B state-level fallback: product-market fit is state-driven
      q2b_state AS (
        SELECT
          state,
          SAFE_DIVIDE(SUM(COALESCE(binds, 0)), NULLIF(SUM(COALESCE(quotes, 0)), 0)) AS state_q2b
        FROM state_segment_daily
        WHERE (@q2bStartDate = '' OR event_date >= @q2bStartDate::date)
          AND (@q2bEndDate = '' OR event_date <= @q2bEndDate::date)
          AND (@activityType = '' OR activity_type = @activityType)
          AND (@leadType = '' OR lead_type = @leadType)
        GROUP BY state
      ),
      -- Quote rate: state+channel (≥50 quotes) → channel fallback
      -- Quote rate = intent signal → driven by channel/segment
      quote_rate_calc AS (
        SELECT
          with_expected.*,
          CASE
            WHEN COALESCE(state_ch_quotes, 0) >= 50
              THEN SAFE_DIVIDE(state_ch_quotes, NULLIF(current_sold_channel_state, 0))
            ELSE COALESCE(channel_quotes_all.click_to_channel_quote, 0)
          END AS quote_rate,
          -- Q2B: product-market fit → state-driven fallback
          CASE
            WHEN COALESCE(state_channel_binds.binds_state_channel, 0) >= 5 AND q2b_source.q2b IS NOT NULL
              THEN q2b_source.q2b
            ELSE COALESCE(q2b_state.state_q2b, 0)
          END AS q2b_rate,
          state_channel_binds.binds_state_channel,
          q2b_source.q2b,
          channel_binds.channel_binds,
          q2b_channel.channel_q2b,
          channel_quotes_all.channel_quote,
          channel_quotes_all.click_to_channel_quote
        FROM with_expected
        LEFT JOIN q2b_source
          ON q2b_source.channel_group_name = with_expected.channel_group_name
         AND q2b_source.state = with_expected.state
        LEFT JOIN state_channel_binds
          ON state_channel_binds.channel_group_name = with_expected.channel_group_name
         AND state_channel_binds.state = with_expected.state
        LEFT JOIN channel_binds
          ON channel_binds.channel_group_name = with_expected.channel_group_name
        LEFT JOIN channel_quotes_all
          ON channel_quotes_all.channel_group_name = with_expected.channel_group_name
        LEFT JOIN q2b_channel
          ON q2b_channel.channel_group_name = with_expected.channel_group_name
        LEFT JOIN q2b_state
          ON q2b_state.state = with_expected.state
      ),
      -- Compute expected_binds from scratch: expected_clicks × quote_rate × Q2B
      with_expected_binds AS (
        SELECT
          *,
          (expected_clicks * quote_rate * q2b_rate) AS expected_binds,
          -- Baseline expected binds for delta calculations
          MAX(CASE WHEN testing_point = 0 THEN expected_clicks * quote_rate * q2b_rate END)
            OVER (PARTITION BY channel_group_name, state) AS baseline_expected_binds,
          -- Baseline expected CPB: baseline_expected_cost / actual_binds
          MAX(CASE WHEN testing_point = 0 THEN
            SAFE_DIVIDE(
              expected_clicks * cpc,
              NULLIF(binds_state_channel, 0)
            ) END)
            OVER (PARTITION BY channel_group_name, state) AS baseline_expected_cpb
        FROM quote_rate_calc
      ),
      final_rows AS (
        SELECT
          channel_group_name,
          state,
          testing_point,
          opps,
          bids,
          win_rate,
          sold,
          binds_state_channel AS binds,
          quotes,
          click_to_quote,
          channel_quote,
          click_to_channel_quote,
          q2b,
          channel_binds,
          channel_q2b,
          cpc,
          avg_bid,
          win_rate_uplift_state,
          cpc_uplift_state,
          win_rate_uplift_channel,
          cpc_uplift_channel,
          CASE
            WHEN testing_point = 0         THEN NULL
            WHEN stat_sig = 'disqualified' THEN NULL
            WHEN stat_sig = 'channel'      THEN win_rate_uplift_channel
            ELSE                                win_rate_uplift_state   -- 'state'
          END AS win_rate_uplift,
          CASE
            WHEN testing_point = 0         THEN NULL
            WHEN stat_sig = 'disqualified' THEN NULL
            WHEN stat_sig = 'channel'      THEN cpc_uplift_channel
            ELSE                                cpc_uplift_state        -- 'state'
          END AS cpc_uplift,
          -- Additional clicks: delta from baseline
          -- For channel fallback, use blended WR uplift × baseline WR × total bids
          -- (raw state WR can diverge from blended channel WR, giving wrong sign)
          CASE
            WHEN testing_point = 0         THEN 0
            WHEN stat_sig = 'disqualified' THEN 0
            WHEN stat_sig = 'channel'      THEN
              COALESCE(win_rate_uplift_channel, 0)
              * COALESCE(baseline_win_rate_channel_state, 0)
              * total_bids_channel_state
            ELSE (expected_clicks - COALESCE(baseline_expected_clicks, 0))
          END AS additional_clicks,
          -- Expected bind change: delta from baseline
          -- Same blended approach for channel fallback
          CASE
            WHEN testing_point = 0         THEN 0
            WHEN stat_sig = 'disqualified' THEN 0
            WHEN stat_sig = 'channel'      THEN
              COALESCE(win_rate_uplift_channel, 0)
              * COALESCE(baseline_win_rate_channel_state, 0)
              * total_bids_channel_state
              * quote_rate * q2b_rate
            ELSE (expected_binds - COALESCE(baseline_expected_binds, 0))
          END AS expected_bind_change,
          -- Additional budget: delta from baseline projected cost (not actual spend)
          CASE
            WHEN testing_point = 0         THEN 0
            WHEN stat_sig = 'disqualified' THEN 0
            ELSE (expected_total_cost - COALESCE(baseline_expected_cost, 0))
          END AS additional_budget_needed,
          -- Current CPB (actual spend / actual binds)
          SAFE_DIVIDE(
            current_spend_channel_state,
            NULLIF(binds_state_channel, 0)
          ) AS current_cpb,
          -- Expected CPB: expected_cost / (actual_binds + additional_binds)
          -- Guard: suppress when projected total binds < 1.0 (near-zero denominator produces absurd values)
          CASE
            WHEN testing_point = 0         THEN SAFE_DIVIDE(expected_total_cost, NULLIF(COALESCE(binds_state_channel, 0), 0))
            WHEN stat_sig = 'disqualified' THEN NULL
            WHEN (COALESCE(binds_state_channel, 0) + (expected_binds - COALESCE(baseline_expected_binds, 0))) < 1.0 THEN NULL
            ELSE SAFE_DIVIDE(
              expected_total_cost,
              (COALESCE(binds_state_channel, 0) + (expected_binds - COALESCE(baseline_expected_binds, 0)))
            )
          END AS expected_cpb,
          -- CPB uplift: (expected_cpb - baseline_expected_cpb) / baseline_expected_cpb
          CASE
            WHEN testing_point = 0         THEN NULL
            WHEN stat_sig = 'disqualified' THEN NULL
            WHEN (COALESCE(binds_state_channel, 0) + (expected_binds - COALESCE(baseline_expected_binds, 0))) < 1.0 THEN NULL
            ELSE SAFE_DIVIDE(
              SAFE_DIVIDE(
                expected_total_cost,
                (COALESCE(binds_state_channel, 0) + (expected_binds - COALESCE(baseline_expected_binds, 0)))
              ) - baseline_expected_cpb,
              NULLIF(baseline_expected_cpb, 0)
            )
          END AS cpb_uplift,
          stat_sig,
          '' AS stat_sig_channel_group,
          CASE
            WHEN testing_point = 0         THEN 'baseline'
            WHEN stat_sig = 'disqualified' THEN 'disqualified'
            WHEN stat_sig = 'channel'      THEN 'channel only'
            ELSE                                'channel & state'
          END AS stat_sig_source
        FROM with_expected_binds
      ),
      -- Median bids per state+channel for is_valid_tp (PG percentile_cont)
      median_bids AS (
        SELECT
          channel_group_name,
          state,
          percentile_cont(0.5) WITHIN GROUP (ORDER BY bids) AS median_bids
        FROM final_rows
        WHERE testing_point != 0
        GROUP BY channel_group_name, state
      ),
      final_rows_scoped AS (
        SELECT
          final_rows.*,
          -- performance from SSD: target_cpb / cpb
          ssd_metrics.ssd_performance AS performance,
          ${buildRoeSql({
            zeroConditions: [
              "final_rows.expected_cpb IS NULL",
              "state_channel_financials.avg_equity IS NULL",
              "state_channel_financials.avg_equity = 0"
            ],
            avgProfitExpr: "state_channel_financials.avg_profit",
            cpbExpr: "final_rows.expected_cpb",
            avgEquityExpr: "state_channel_financials.avg_equity"
          })} AS roe,
          ${buildCombinedRatioSql({
            zeroConditions: [
              "final_rows.expected_cpb IS NULL",
              "state_channel_financials.avg_lifetime_premium IS NULL",
              "state_channel_financials.avg_lifetime_premium = 0"
            ],
            cpbExpr: "final_rows.expected_cpb",
            avgLifetimeCostExpr: "state_channel_financials.avg_lifetime_cost",
            avgLifetimePremiumExpr: "state_channel_financials.avg_lifetime_premium"
          })} AS combined_ratio,
          ssd_metrics.ssd_binds,
          ssd_metrics.ssd_quotes,
          ssd_metrics.ssd_q2b,
          ssd_metrics.ssd_performance,
          ssd_metrics.ssd_roe,
          ssd_metrics.ssd_combined_ratio,
          CASE
            WHEN final_rows.testing_point = 0 THEN TRUE
            WHEN median_bids.median_bids IS NULL THEN TRUE
            WHEN final_rows.bids >= 0.5 * median_bids.median_bids THEN TRUE
            ELSE FALSE
          END AS is_valid_tp
        FROM final_rows
        LEFT JOIN state_channel_financials
          ON state_channel_financials.channel_group_name = final_rows.channel_group_name
         AND state_channel_financials.state = final_rows.state
        LEFT JOIN ssd_metrics
          ON ssd_metrics.channel_group_name = final_rows.channel_group_name
         AND ssd_metrics.state = final_rows.state
        LEFT JOIN median_bids
          ON median_bids.channel_group_name = final_rows.channel_group_name
         AND median_bids.state = final_rows.state
      ),
      ranked_rows AS (
        SELECT
          *,
          FIRST_VALUE(testing_point) OVER (
            PARTITION BY channel_group_name, state
            ORDER BY
              CASE
                WHEN testing_point != 0
                  AND stat_sig != 'disqualified'
                  AND is_valid_tp = TRUE
                  AND cpb_uplift IS NOT NULL
                  AND cpb_uplift <= 0.10
                  AND additional_clicks > 0
                  THEN 0
                WHEN testing_point = 0 THEN 1
                ELSE 2
              END,
              CASE
                WHEN testing_point != 0
                  AND stat_sig != 'disqualified'
                  AND is_valid_tp = TRUE
                  AND cpb_uplift IS NOT NULL
                  AND cpb_uplift <= 0.10
                  AND additional_clicks > 0
                  THEN additional_clicks
                ELSE -1e18
              END DESC,
              testing_point
          ) AS recommended_testing_point
        FROM final_rows_scoped
      )
      ${normalized.topPairs > 0 ? `
      , pair_bind_scores AS (
        SELECT state, channel_group_name,
          MAX(CASE WHEN testing_point = recommended_testing_point
              THEN COALESCE(expected_bind_change, 0) ELSE 0 END) as pair_bind_score
        FROM ranked_rows
        GROUP BY state, channel_group_name
        ORDER BY pair_bind_score DESC
        LIMIT ${normalized.topPairs}
      )
      SELECT r.*
      FROM ranked_rows r
      INNER JOIN pair_bind_scores p
        ON r.state = p.state AND r.channel_group_name = p.channel_group_name
      ORDER BY p.pair_bind_score DESC, r.state, r.channel_group_name, r.testing_point
      LIMIT @limit
      ` : `
      SELECT *
      FROM ranked_rows
      ORDER BY channel_group_name, state, testing_point
      LIMIT @limit
      `}
    `,
    normalized
  ));
}

export async function getPriceExploration(
  filters: PriceExplorationFilters
): Promise<PriceExplorationRow[]> {
  const normalized = normalizePriceExplorationFilters(filters);

  // BQ data from cache (or fresh on miss); strategy rules always read fresh from PG
  const cachedRows = await getPriceExplorationBQ(normalized);
  // Deep-copy so the cached array is never mutated (recommended_testing_point is set in-place)
  const rows = cachedRows.map((row) => ({ ...row }));

  const [strategyRules, manualOverrides] = await Promise.all([
    getStrategyRulesForPlan(filters.planId, filters.activityLeadType),
    getPriceDecisionOverridesForPlan(filters.planId, filters.activityLeadType)
  ]);
  if (!strategyRules.length) {
    if (!manualOverrides.size) {
      return rows;
    }
    const groupedWithoutRules = new Map<string, PriceExplorationRow[]>();
    for (const row of rows) {
      const state = String(row.state || "").toUpperCase();
      const segment = extractSegmentFromChannelGroup(String(row.channel_group_name || ""));
      const key = `${String(row.channel_group_name || "")}|${state}|${segment || "__UNSEGMENTED__"}`;
      const bucket = groupedWithoutRules.get(key) || [];
      bucket.push(row);
      groupedWithoutRules.set(key, bucket);
    }
    for (const groupRows of groupedWithoutRules.values()) {
      if (!groupRows.length) {
        continue;
      }
      const sample = groupRows[0];
      const state = String(sample.state || "").toUpperCase();
      const segment = extractSegmentFromChannelGroup(String(sample.channel_group_name || ""));
      const overrideKey = buildPriceDecisionOverrideKey(String(sample.channel_group_name || ""), state, segment);
      const overrideTp = manualOverrides.get(overrideKey) ?? manualOverrides.get(
        buildPriceDecisionOverrideKey(String(sample.channel_group_name || ""), state, "")
      );
      if (!Number.isFinite(Number(overrideTp))) {
        continue;
      }
      const normalizedOverride = Number(overrideTp);
      if (!groupRows.some((item) => Number(item.testing_point) === normalizedOverride)) {
        continue;
      }
      for (const row of groupRows) {
        const sqlRecommended = Number(row.recommended_testing_point) || 0;
        row.algorithm_recommended_tp = sqlRecommended;
        row.is_override = normalizedOverride !== sqlRecommended;
        row.recommended_testing_point = normalizedOverride;
      }
    }
    return rows;
  }

  const grouped = new Map<string, PriceExplorationRow[]>();
  for (const row of rows) {
    const state = String(row.state || "").toUpperCase();
    const segment = extractSegmentFromChannelGroup(String(row.channel_group_name || ""));
    const key = `${String(row.channel_group_name || "")}|${state}|${segment || "__UNSEGMENTED__"}`;
    const bucket = grouped.get(key) || [];
    bucket.push(row);
    grouped.set(key, bucket);
  }

  for (const groupRows of grouped.values()) {
    if (!groupRows.length) {
      continue;
    }
    const sample = groupRows[0];
    const state = String(sample.state || "").toUpperCase();
    const segment = extractSegmentFromChannelGroup(String(sample.channel_group_name || ""));
    const matchedRule = strategyRules.find(
      (rule) => rule.states.includes(state) && rule.segments.includes(segment)
    ) || null;
    const overrideKey = buildPriceDecisionOverrideKey(String(sample.channel_group_name || ""), state, segment);
    const overrideTp = manualOverrides.get(overrideKey) ?? manualOverrides.get(
      buildPriceDecisionOverrideKey(String(sample.channel_group_name || ""), state, "")
    );
    const hasValidOverride =
      Number.isFinite(Number(overrideTp)) &&
      groupRows.some((row) => Number(row.testing_point) === Number(overrideTp));
    const algorithmTp = chooseRecommendedTestingPoint(groupRows, matchedRule);
    const recommendedTp = hasValidOverride ? Number(overrideTp) : algorithmTp;
    for (const row of groupRows) {
      row.recommended_testing_point = recommendedTp;
      row.algorithm_recommended_tp = algorithmTp;
      row.is_override = hasValidOverride;
    }
  }

  return rows;
}

export async function listPlanMergedFilters(
  filters: Pick<PlanMergedFilters, "startDate" | "endDate" | "activityLeadType">
): Promise<{
  states: string[];
  segments: string[];
  channelGroups: string[];
  testingPoints: number[];
  statSig: string[];
}> {
  const normalized = normalizePlanMergedFilters(filters);

  const cacheKey = buildCacheKey("pm-filters", {
    startDate: normalized.startDate,
    endDate: normalized.endDate,
    activityType: normalized.activityType,
    leadType: normalized.leadType
  });

  return cached(cacheKey, async () => {
    const rows = await query<PlanMergedFilterOptionsRow>(
      `
        WITH pe_agg AS (
          SELECT
            channel_group_name,
            state,
            price_adjustment_percent,
            UPPER(SUBSTRING(channel_group_name FROM '(?i)(MCH|MCR|SCH|SCR)')) AS segment,
            SUM(bids) AS bids
          FROM ${table("price_exploration_daily")}
          WHERE date BETWEEN
            CASE WHEN @startDate = '' THEN CURRENT_DATE - 14 ELSE @startDate::date END
            AND CASE WHEN @endDate = '' THEN CURRENT_DATE ELSE @endDate::date END
          GROUP BY 1, 2, 3, 4
          HAVING UPPER(SUBSTRING(channel_group_name FROM '(?i)(MCH|MCR|SCH|SCR)')) IS NOT NULL
        ),
        pe_channel AS (
          SELECT channel_group_name, price_adjustment_percent,
            SUM(bids) AS channel_bids
          FROM pe_agg
          GROUP BY 1, 2
        ),
        pe_sig AS (
          SELECT a.*,
            CASE
              WHEN a.price_adjustment_percent = 0 THEN 'baseline'
              WHEN a.bids >= 200 THEN 'state'
              WHEN a.bids >= 50 AND COALESCE(c.channel_bids - a.bids, 0) >= 600 THEN 'channel'
              ELSE 'disqualified'
            END AS stat_sig
          FROM pe_agg a
          LEFT JOIN pe_channel c
            ON c.channel_group_name = a.channel_group_name
           AND c.price_adjustment_percent = a.price_adjustment_percent
        )
        SELECT
          ARRAY(SELECT DISTINCT state FROM pe_sig WHERE state IS NOT NULL ORDER BY state) AS states,
          ARRAY(SELECT DISTINCT segment FROM pe_sig WHERE segment IS NOT NULL ORDER BY segment) AS segments,
          ARRAY(SELECT DISTINCT channel_group_name FROM pe_sig WHERE channel_group_name IS NOT NULL ORDER BY channel_group_name) AS channel_groups,
          ARRAY(SELECT DISTINCT price_adjustment_percent FROM pe_sig ORDER BY price_adjustment_percent) AS testing_points,
          ARRAY(SELECT DISTINCT stat_sig FROM pe_sig WHERE stat_sig IS NOT NULL ORDER BY stat_sig) AS stat_sig
      `,
      normalized
    );

    const first = rows[0];
    return {
      states: withAllStateCodes(first?.states),
      segments: unwrapBqArray<string>(first?.segments),
      channelGroups: unwrapBqArray<string>(first?.channel_groups),
      testingPoints: unwrapBqArray<number>(first?.testing_points),
      statSig: unwrapBqArray<string>(first?.stat_sig)
    };
  });
}

export async function getPlanMergedAnalytics(
  filters: PlanMergedFilters
): Promise<PlanMergedRow[]> {
  const normalized = normalizePlanMergedFilters(filters);

  const cacheKey = buildCacheKey("pm", {
    startDate: normalized.startDate,
    endDate: normalized.endDate,
    states: normalized.states,
    segments: normalized.segments,
    channelGroups: normalized.channelGroups,
    testingPoints: normalized.testingPoints,
    statSig: normalized.statSig,
    activityType: normalized.activityType,
    leadType: normalized.leadType
  });

  return cached(cacheKey, async () => {
    const rows = await query<PlanMergedRow>(
      `
        WITH pe_raw AS (
          SELECT
            channel_group_name,
            state,
            price_adjustment_percent,
            SUM(opps) AS opps,
            SUM(bids) AS bids,
            SUM(total_impressions) AS total_impressions,
            SUM(sold) AS sold,
            SUM(total_spend) AS total_spend,
            SUM(number_of_quote_started) AS number_of_quote_started,
            SUM(number_of_quotes) AS number_of_quotes,
            SUM(number_of_binds) AS number_of_binds
          FROM ${table("price_exploration_daily")}
          WHERE date BETWEEN
            CASE WHEN @startDate = '' THEN CURRENT_DATE - 14 ELSE @startDate::date END
            AND CASE WHEN @endDate = '' THEN CURRENT_DATE ELSE @endDate::date END
          GROUP BY 1, 2, 3
        ),
        pe_state_tp AS (
          SELECT *,
            CASE WHEN bids = 0 OR bids IS NULL THEN NULL ELSE sold::double precision / bids END AS win_rate,
            CASE WHEN sold = 0 OR sold IS NULL THEN NULL ELSE total_spend / sold END AS cpc
          FROM pe_raw
        ),
        pe_channel_tp AS (
          SELECT
            channel_group_name,
            price_adjustment_percent,
            SUM(bids) AS channel_bids,
            SUM(sold) AS channel_sold,
            SUM(total_spend) AS channel_total_spend,
            CASE WHEN SUM(bids) = 0 THEN NULL ELSE SUM(sold)::double precision / SUM(bids) END AS channel_win_rate,
            CASE WHEN SUM(sold) = 0 THEN NULL ELSE SUM(total_spend) / SUM(sold) END AS channel_cpc
          FROM pe_state_tp
          GROUP BY 1, 2
        ),
        pe_joined AS (
          SELECT
            s.*,
            UPPER(SUBSTRING(s.channel_group_name FROM '(?i)(MCH|MCR|SCH|SCR)')) AS segment,
            b.win_rate AS baseline_win_rate,
            b.cpc AS baseline_cpc,
            b.bids AS baseline_bids,
            b.sold AS baseline_sold,
            c.channel_bids,
            c.channel_sold,
            c.channel_win_rate,
            c.channel_cpc,
            (c.channel_bids - s.bids) AS channel_ex_bids,
            cb.channel_win_rate AS channel_baseline_win_rate,
            cb.channel_cpc AS channel_baseline_cpc
          FROM pe_state_tp s
          LEFT JOIN pe_state_tp b
            ON b.channel_group_name = s.channel_group_name
           AND b.state = s.state
           AND b.price_adjustment_percent = 0
          LEFT JOIN pe_channel_tp c
            ON c.channel_group_name = s.channel_group_name
           AND c.price_adjustment_percent = s.price_adjustment_percent
          LEFT JOIN pe_channel_tp cb
            ON cb.channel_group_name = s.channel_group_name
           AND cb.price_adjustment_percent = 0
        ),
        pe_final AS (
          SELECT
            channel_group_name,
            state,
            segment,
            price_adjustment_percent,
            sold,
            bids,
            win_rate,
            cpc,
            CASE
              WHEN price_adjustment_percent = 0 THEN 'baseline'
              WHEN bids >= 200 THEN 'state'
              WHEN bids >= 50 AND COALESCE(channel_ex_bids, 0) >= 600 THEN 'channel'
              ELSE 'disqualified'
            END AS stat_sig,
            CASE
              WHEN price_adjustment_percent = 0 THEN 'baseline'
              WHEN channel_bids >= 200 THEN 'state'
              ELSE 'disqualified'
            END AS stat_sig_channel_group,
            CASE WHEN price_adjustment_percent = 0 THEN NULL
              ELSE CASE WHEN baseline_cpc = 0 OR baseline_cpc IS NULL THEN NULL ELSE (cpc - baseline_cpc) / baseline_cpc END
            END AS cpc_uplift,
            CASE WHEN price_adjustment_percent = 0 THEN NULL
              ELSE CASE WHEN baseline_win_rate = 0 OR baseline_win_rate IS NULL THEN NULL ELSE (win_rate - baseline_win_rate) / baseline_win_rate END
            END AS win_rate_uplift,
            CASE
              WHEN price_adjustment_percent = 0 THEN NULL
              ELSE (
                CASE
                  WHEN bids >= 200 THEN win_rate
                  WHEN bids >= 50 AND COALESCE(channel_ex_bids, 0) >= 600 THEN channel_win_rate
                  ELSE win_rate
                END - baseline_win_rate
              ) * bids
            END AS additional_clicks
          FROM pe_joined
          WHERE UPPER(SUBSTRING(channel_group_name FROM '(?i)(MCH|MCR|SCH|SCR)')) IS NOT NULL
        ),
        perf_daily AS (
          SELECT
            event_date, state, segment,
            SUM(sold) AS sold,
            SUM(total_cost) AS total_cost,
            SUM(binds) AS binds,
            SUM(target_cpb_sum) AS target_cpb
          FROM ${table("state_segment_daily")}
          WHERE event_date BETWEEN
            CASE WHEN @startDate = '' THEN CURRENT_DATE - 14 ELSE @startDate::date END
            AND CASE WHEN @endDate = '' THEN CURRENT_DATE ELSE @endDate::date END
          GROUP BY 1, 2, 3
        ),
        perf AS (
          SELECT
            state, segment,
            SUM(sold) AS ss_sold,
            SUM(total_cost) AS ss_total_cost,
            SUM(binds) AS ss_binds,
            AVG(target_cpb) AS ss_target_cpb,
            CASE WHEN SUM(binds) = 0 THEN NULL ELSE SUM(total_cost) / SUM(binds) END AS ss_cpb,
            CASE WHEN SUM(total_cost) = 0 OR SUM(binds) = 0 THEN NULL
              ELSE AVG(target_cpb) / (SUM(total_cost) / SUM(binds))
            END AS ss_performance,
            CASE WHEN SUM(sold) = 0 THEN NULL ELSE SUM(binds)::double precision / SUM(sold) END AS sold_to_bind
          FROM perf_daily
          GROUP BY 1, 2
        ),
        merged AS (
          SELECT
            fp.channel_group_name, fp.state, fp.segment, fp.price_adjustment_percent,
            fp.stat_sig, fp.stat_sig_channel_group,
            fp.cpc_uplift, fp.win_rate_uplift, fp.additional_clicks,
            GREATEST(fp.sold + COALESCE(fp.additional_clicks, 0), 0) AS expected_total_clicks,
            fp.cpc AS expected_cpc,
            GREATEST(fp.sold + COALESCE(fp.additional_clicks, 0), 0) * fp.cpc AS expected_total_cost,
            GREATEST(fp.sold + COALESCE(fp.additional_clicks, 0), 0) * COALESCE(pf.sold_to_bind, 0) AS expected_total_binds,
            COALESCE(fp.additional_clicks, 0) * COALESCE(pf.sold_to_bind, 0) AS additional_expected_binds,
            pf.ss_performance,
            pf.ss_target_cpb
          FROM pe_final fp
          LEFT JOIN perf pf ON pf.state = fp.state AND pf.segment = fp.segment
          WHERE ('__ALL__' = ANY(@states) OR fp.state = ANY(@states))
            AND ('__ALL__' = ANY(@segments) OR fp.segment = ANY(@segments))
            AND ('__ALL__' = ANY(@channelGroups) OR fp.channel_group_name = ANY(@channelGroups))
            AND (999999999 = ANY(@testingPoints) OR fp.price_adjustment_percent = ANY(@testingPoints))
            AND ('__ALL__' = ANY(@statSig) OR fp.stat_sig = ANY(@statSig))
        )
        SELECT
          CASE WHEN @startDate = '' THEN (CURRENT_DATE - 14)::text ELSE @startDate END AS start_date,
          CASE WHEN @endDate = '' THEN CURRENT_DATE::text ELSE @endDate END AS end_date,
          channel_group_name, state, segment, price_adjustment_percent,
          stat_sig, stat_sig_channel_group,
          cpc_uplift, win_rate_uplift, additional_clicks,
          expected_total_clicks,
          expected_cpc,
          expected_total_cost,
          expected_total_binds,
          additional_expected_binds,
          CASE WHEN expected_total_binds = 0 OR expected_total_binds IS NULL THEN NULL
            ELSE expected_total_cost / expected_total_binds
          END AS expected_cpb,
          ss_performance,
          CASE WHEN expected_total_cost = 0 OR expected_total_cost IS NULL
                 OR expected_total_binds = 0 OR expected_total_binds IS NULL
                 OR ss_target_cpb IS NULL THEN NULL
            ELSE ss_target_cpb / (expected_total_cost / NULLIF(expected_total_binds, 0))
          END AS expected_performance,
          CASE WHEN expected_total_cost = 0 OR expected_total_cost IS NULL
                 OR expected_total_binds = 0 OR expected_total_binds IS NULL
                 OR ss_target_cpb IS NULL OR ss_performance IS NULL THEN NULL
            ELSE (ss_target_cpb / (expected_total_cost / NULLIF(expected_total_binds, 0))) - ss_performance
          END AS performance_uplift
        FROM merged
        ORDER BY channel_group_name, state, price_adjustment_percent
      `,
      normalized
    );

    return rows;
  });
}

export async function getStrategyAnalysis(
  filters: StrategyAnalysisFilters
): Promise<StrategyAnalysisRow[]> {
  const paramRows = await query<{ param_value: string }>(
    `
      SELECT param_value
      FROM ${table("plan_parameters")}
      WHERE plan_id = @planId
        AND param_key = 'plan_strategy_config'
      LIMIT 1
    `,
    { planId: filters.planId }
  );

  const rules = parseStrategyRules(paramRows[0]?.param_value || "", filters.activityLeadType);
  if (!rules.length) {
    return [];
  }

  const uniqueStates = [...new Set(rules.flatMap((rule) => rule.states))];
  const uniqueSegments = [...new Set(rules.flatMap((rule) => rule.segments))];
  const qbc = Number.isFinite(Number(filters.qbc)) ? Number(filters.qbc) : 0;
  const [rawBaselineRows, priceRows] = await Promise.all([
    getStateSegmentPerformance({
      startDate: filters.startDate,
      endDate: filters.endDate,
      states: uniqueStates,
      segments: uniqueSegments,
      activityLeadType: filters.activityLeadType,
      qbc,
      groupBy: "state_segment"
    }),
    getPriceExploration({
      planId: filters.planId,
      startDate: filters.startDate,
      endDate: filters.endDate,
      q2bStartDate: filters.startDate,
      q2bEndDate: filters.endDate,
      states: uniqueStates,
      activityLeadType: filters.activityLeadType,
      qbc,
      limit: 200000
    })
  ]);

  /* BQ already grouped by (state, segment) — map directly */
  const baselineRows: StrategyBaselineRow[] = rawBaselineRows.map((row) => ({
    state: String(row.state || "").toUpperCase(),
    segment: String(row.segment || "").toUpperCase(),
    bids: Number(row.bids) || 0,
    sold: Number(row.sold) || 0,
    total_cost: Number(row.total_cost) || 0,
    quotes: Number(row.quotes) || 0,
    binds: Number(row.binds) || 0,
    scored_policies: Number(row.scored_policies) || 0,
    q2b: Number.isFinite(Number(row.q2b_score)) ? Number(row.q2b_score) : null,
    performance: Number.isFinite(Number(row.performance)) ? Number(row.performance) : null,
    roe: Number.isFinite(Number(row.roe)) ? Number(row.roe) : null,
    combined_ratio: Number.isFinite(Number(row.combined_ratio)) ? Number(row.combined_ratio) : null,
    /* raw _sum columns for correct re-aggregation across segments */
    target_cpb_sum: Number(row.target_cpb_sum) || 0,
    avg_profit_sum: Number(row.avg_profit_sum) || 0,
    avg_equity_sum: Number(row.avg_equity_sum) || 0,
    lifetime_cost_sum: Number(row.lifetime_cost_sum) || 0,
    lifetime_premium_sum: Number(row.lifetime_premium_sum) || 0,
  }));

  function extractSegment(channelGroup: string): string {
    const upper = String(channelGroup || "").toUpperCase();
    const match = upper.match(/\b(MCH|MCR|SCH|SCR)\b/);
    if (match) return match[1];
    if (upper.includes("HOME")) return "HOME";
    if (upper.includes("RENT")) return "RENT";
    return "";
  }

  type RecommendedSummary = {
    state: string;
    segment: string;
    additional_clicks: number;
    additional_binds: number;
    additional_budget: number;
    current_wr: number | null;
    expected_wr: number | null;
    wr_weight: number;
    current_cpc: number | null;
    expected_cpc: number | null;
    cpc_weight: number;
    current_cpb: number | null;
    expected_cpb: number | null;
    cpb_weight: number;
  };

  const rowsByChannelStateSegment = new Map<string, PriceExplorationRow[]>();
  for (const row of priceRows) {
    const state = String(row.state || "").toUpperCase();
    const segment = extractSegment(String(row.channel_group_name || "")) || "__UNSEGMENTED__";
    if (!state) {
      continue;
    }
    const key = `${String(row.channel_group_name || "")}|${state}|${segment}`;
    const group = rowsByChannelStateSegment.get(key) || [];
    group.push(row);
    rowsByChannelStateSegment.set(key, group);
  }

  const recommendedSummaries: RecommendedSummary[] = [];
  for (const groupRows of rowsByChannelStateSegment.values()) {
    const baseline = groupRows.find((row) => Number(row.testing_point) === 0) || null;
    const recommendedByFlag = groupRows.find((row) =>
      Number.isFinite(Number(row.recommended_testing_point)) &&
      Number(row.testing_point) === Number(row.recommended_testing_point)
    );
    const recommended =
      recommendedByFlag ||
      [...groupRows].sort((a, b) => {
        const aTp = Number(a.testing_point) || 0;
        const bTp = Number(b.testing_point) || 0;
        const aCpbUplift = Number(a.cpb_uplift);
        const bCpbUplift = Number(b.cpb_uplift);
        const aAdditionalClicks = Number(a.additional_clicks) || 0;
        const bAdditionalClicks = Number(b.additional_clicks) || 0;
        const aPriority =
          aTp !== 0 && Number.isFinite(aCpbUplift) && aCpbUplift <= 0.10 && aAdditionalClicks > 0
            ? 0
            : aTp === 0
              ? 1
              : 2;
        const bPriority =
          bTp !== 0 && Number.isFinite(bCpbUplift) && bCpbUplift <= 0.10 && bAdditionalClicks > 0
            ? 0
            : bTp === 0
              ? 1
              : 2;
        if (aPriority !== bPriority) {
          return aPriority - bPriority;
        }
        if (aPriority === 0 && aAdditionalClicks !== bAdditionalClicks) {
          return bAdditionalClicks - aAdditionalClicks;
        }
        return aTp - bTp;
      })[0];

    if (!recommended) {
      continue;
    }

    const state = String(recommended.state || "").toUpperCase();
    const segment = extractSegment(String(recommended.channel_group_name || "")) || "__UNSEGMENTED__";
    if (!state) {
      continue;
    }

    const baselineBids = Number(baseline?.bids);
    const baselineSold = Number(baseline?.sold);
    const baselineBinds = Number(baseline?.binds);
    const baselineWr = Number(baseline?.win_rate);
    const baselineCpc = Number(baseline?.cpc);
    const expectedWr = Number(recommended.win_rate);
    const expectedCpc = Number(recommended.cpc);
    const wrUplift = Number(recommended.win_rate_uplift);
    const cpcUplift = Number(recommended.cpc_uplift);

    const derivedBaselineWr =
      Number.isFinite(expectedWr) && Number.isFinite(wrUplift) && Math.abs(1 + wrUplift) > 1e-9
        ? expectedWr / (1 + wrUplift)
        : null;
    const derivedBaselineCpc =
      Number.isFinite(expectedCpc) && Number.isFinite(cpcUplift) && Math.abs(1 + cpcUplift) > 1e-9
        ? expectedCpc / (1 + cpcUplift)
        : null;

    const currentWr = Number.isFinite(baselineWr) ? baselineWr : derivedBaselineWr;
    const currentCpc = Number.isFinite(baselineCpc) ? baselineCpc : derivedBaselineCpc;
    const currentCpb = Number(recommended.current_cpb);
    const expectedCpb = Number(recommended.expected_cpb);

    recommendedSummaries.push({
      state,
      segment,
      additional_clicks: Number(recommended.additional_clicks) || 0,
      additional_binds: Number(recommended.expected_bind_change) || 0,
      additional_budget: Number(recommended.additional_budget_needed) || 0,
      current_wr: Number.isFinite(currentWr) ? currentWr : null,
      expected_wr: Number.isFinite(expectedWr) ? expectedWr : null,
      wr_weight:
        Number.isFinite(baselineBids) && baselineBids > 0
          ? baselineBids
          : Number(recommended.bids) || 0,
      current_cpc: Number.isFinite(currentCpc) ? currentCpc : null,
      expected_cpc: Number.isFinite(expectedCpc) ? expectedCpc : null,
      cpc_weight:
        Number.isFinite(baselineSold) && baselineSold > 0
          ? baselineSold
          : Number(recommended.sold) || 0,
      current_cpb: Number.isFinite(currentCpb) ? currentCpb : null,
      expected_cpb: Number.isFinite(expectedCpb) ? expectedCpb : null,
      cpb_weight:
        Number.isFinite(baselineBinds) && baselineBinds > 0
          ? baselineBinds
          : Number(recommended.binds) || 0
    });
  }

  const result = rules.map((rule) => {
    const stateSet = new Set(rule.states);
    const segmentSet = new Set(rule.segments);
    const includesAllCoreSegments =
      (segmentSet.has("MCH") && segmentSet.has("MCR") && segmentSet.has("SCH") && segmentSet.has("SCR")) ||
      (segmentSet.has("HOME") && segmentSet.has("RENT"));

    const matchingBaseline = baselineRows.filter(
      (row) => stateSet.has(String(row.state || "").toUpperCase()) && segmentSet.has(String(row.segment || "").toUpperCase())
    );
    const matchingRecommended = recommendedSummaries.filter(
      (row) =>
        stateSet.has(String(row.state || "").toUpperCase()) &&
        (includesAllCoreSegments || segmentSet.has(String(row.segment || "").toUpperCase()))
    );

    const bids = matchingBaseline.reduce((sum, row) => sum + (Number(row.bids) || 0), 0);
    const sold = matchingBaseline.reduce((sum, row) => sum + (Number(row.sold) || 0), 0);
    const quotes = matchingBaseline.reduce((sum, row) => sum + (Number(row.quotes) || 0), 0);
    const binds = matchingBaseline.reduce((sum, row) => sum + (Number(row.binds) || 0), 0);
    const totalSpend = matchingBaseline.reduce((sum, row) => sum + (Number(row.total_cost) || 0), 0);
    const scoredPolicies = matchingBaseline.reduce((sum, row) => sum + (Number(row.scored_policies) || 0), 0);
    const wr = bids > 0 ? sold / bids : null;
    const cpc = sold > 0 ? totalSpend / sold : null;
    const q2b = quotes > 0 ? binds / quotes : null;

    /* Aggregate raw _sum columns, then compute ratios (same as BQ SQL) */
    const targetCpbSum = matchingBaseline.reduce((sum, row) => sum + (Number(row.target_cpb_sum) || 0), 0);
    const avgProfitSum = matchingBaseline.reduce((sum, row) => sum + (Number(row.avg_profit_sum) || 0), 0);
    const avgEquitySum = matchingBaseline.reduce((sum, row) => sum + (Number(row.avg_equity_sum) || 0), 0);
    const lifetimeCostSum = matchingBaseline.reduce((sum, row) => sum + (Number(row.lifetime_cost_sum) || 0), 0);
    const lifetimePremiumSum = matchingBaseline.reduce((sum, row) => sum + (Number(row.lifetime_premium_sum) || 0), 0);

    const ruleCpb = binds > 0 ? totalSpend / binds : 0;
    const ruleTargetCpb = binds > 0 ? targetCpbSum / binds : 0;
    const ruleAvgProfit = scoredPolicies > 0 ? avgProfitSum / scoredPolicies : 0;
    const ruleAvgEquity = scoredPolicies > 0 ? avgEquitySum / scoredPolicies : 0;
    const ruleAvgLifetimeCost = scoredPolicies > 0 ? lifetimeCostSum / scoredPolicies : 0;
    const ruleAvgLifetimePremium = scoredPolicies > 0 ? lifetimePremiumSum / scoredPolicies : 0;

    const rulePerformance = ruleCpb > 0 ? ruleTargetCpb / ruleCpb : null;
    const ruleRoe = scoredPolicies > 0 && ruleAvgEquity !== 0
      ? (ruleAvgProfit - 0.8 * (ruleCpb / 0.81 + qbc)) / ruleAvgEquity
      : null;
    const ruleCor = scoredPolicies > 0 && ruleAvgLifetimePremium !== 0
      ? (ruleCpb / 0.81 + qbc + ruleAvgLifetimeCost) / ruleAvgLifetimePremium
      : null;

    const additionalClicks = matchingRecommended.reduce((sum, row) => sum + (Number(row.additional_clicks) || 0), 0);
    const additionalBinds = matchingRecommended.reduce(
      (sum, row) => sum + (Number(row.additional_binds) || 0),
      0
    );
    const additionalBudget = matchingRecommended.reduce((sum, row) => sum + (Number(row.additional_budget) || 0), 0);
    const expectedTotalCost = totalSpend + additionalBudget;
    const currentCpb = binds > 0 ? totalSpend / binds : null;
    const expectedCpb = binds + additionalBinds > 0 ? expectedTotalCost / (binds + additionalBinds) : null;

    const wrRollup = matchingRecommended.reduce(
      (acc, row) => {
        const current = Number(row.current_wr);
        const expected = Number(row.expected_wr);
        const weight = Number(row.wr_weight) || 0;
        if (weight > 0 && Number.isFinite(current) && Number.isFinite(expected)) {
          acc.currentWins += current * weight;
          acc.expectedWins += expected * weight;
          acc.weight += weight;
        }
        return acc;
      },
      { currentWins: 0, expectedWins: 0, weight: 0 }
    );
    const cpbRollup = matchingRecommended.reduce(
      (acc, row) => {
        const current = Number(row.current_cpb);
        const expected = Number(row.expected_cpb);
        const weight = Number(row.cpb_weight) || 0;
        if (weight > 0 && Number.isFinite(current) && Number.isFinite(expected)) {
          acc.currentCost += current * weight;
          acc.expectedCost += expected * weight;
        }
        return acc;
      },
      { currentCost: 0, expectedCost: 0 }
    );

    const wr_uplift =
      wrRollup.weight > 0 && wrRollup.currentWins > 0
        ? (wrRollup.expectedWins - wrRollup.currentWins) / wrRollup.currentWins
        : null;
    const currentCpc = bids > 0 ? totalSpend / bids : null;
    const expectedCost = expectedTotalCost;
    const expectedClicks = bids + additionalClicks;
    const expectedCpc = expectedClicks > 0 ? expectedCost / expectedClicks : null;
    const cpc_uplift =
      Number.isFinite(currentCpc) && Number.isFinite(expectedCpc) && Number(currentCpc) > 0
        ? (Number(expectedCpc) - Number(currentCpc)) / Number(currentCpc)
        : null;
    const cpb_uplift =
      cpbRollup.currentCost > 0
        ? (cpbRollup.expectedCost - cpbRollup.currentCost) / cpbRollup.currentCost
        : null;

    return {
      rule_name: rule.name,
      states: rule.states,
      segments: rule.segments,
      target_cor: rule.corTarget > 0 ? rule.corTarget : null,
      bids,
      sold,
      total_spend: totalSpend,
      cpc,
      wr,
      quotes,
      binds,
      current_cpb: currentCpb,
      expected_cpb: expectedCpb,
      q2b,
      performance: rulePerformance,
      roe: ruleRoe,
      cor: ruleCor,
      additional_clicks: additionalClicks,
      additional_binds: additionalBinds,
      wr_uplift,
      cpc_uplift,
      cpb_uplift,
      expected_total_cost: expectedTotalCost,
      additional_budget: additionalBudget,
      _scored_policies: scoredPolicies,
      _lifetime_cost_sum: lifetimeCostSum,
      _lifetime_premium_sum: lifetimePremiumSum,
      _target_cpb_sum: targetCpbSum,
      _avg_profit_sum: avgProfitSum,
      _avg_equity_sum: avgEquitySum,
      _wr_rollup_current: wrRollup.currentWins,
      _wr_rollup_expected: wrRollup.expectedWins,
      _cpb_rollup_current: cpbRollup.currentCost,
      _cpb_rollup_expected: cpbRollup.expectedCost,
    };
  });
  return result;
}

function mapGrowthStrategy(
  growthStrategy: string
): { key: "aggressive" | "robustic" | "cautious"; label: string } {
  const value = String(growthStrategy || "").trim().toLowerCase();
  if (value.includes("aggressive") || value.includes("high")) {
    return { key: "aggressive", label: "Aggressive growth" };
  }
  if (value.includes("cautious") || value.includes("cost") || value.includes("low")) {
    return { key: "cautious", label: "Cautious growth" };
  }
  return { key: "robustic", label: "Robustic growth" };
}

type StrategyBaselineExtRow = {
  state: string;
  segment: string;
  bids: number;
  sold: number;
  total_cost: number;
  quotes: number;
  binds: number;
  scored_policies: number;
  performance: number | null;
  roe: number | null;
  combined_ratio: number | null;
  mrltv: number | null;
  /* raw additive columns for correct re-aggregation */
  target_cpb_sum: number;
  avg_profit_sum: number;
  avg_equity_sum: number;
  lifetime_cost_sum: number;
  lifetime_premium_sum: number;
  avg_mrltv_sum: number;
};

type StrategyRecommendedSummary = {
  state: string;
  segment: string;
  additional_clicks: number;
  additional_binds: number;
  additional_budget: number;
  current_wr: number | null;
  expected_wr: number | null;
  wr_weight: number;
  current_cpb: number | null;
  expected_cpb: number | null;
  cpb_weight: number;
};

function aggregateStrategySlice(
  baselineRows: StrategyBaselineExtRow[],
  recommendedRows: StrategyRecommendedSummary[],
  qbc: number = 0
): StrategyAnalysisRow & { ltv: number | null } {
  const bids = baselineRows.reduce((sum, row) => sum + (Number(row.bids) || 0), 0);
  const sold = baselineRows.reduce((sum, row) => sum + (Number(row.sold) || 0), 0);
  const quotes = baselineRows.reduce((sum, row) => sum + (Number(row.quotes) || 0), 0);
  const binds = baselineRows.reduce((sum, row) => sum + (Number(row.binds) || 0), 0);
  const totalSpend = baselineRows.reduce((sum, row) => sum + (Number(row.total_cost) || 0), 0);
  const scoredPolicies = baselineRows.reduce((sum, row) => sum + (Number(row.scored_policies) || 0), 0);
  const wr = bids > 0 ? sold / bids : null;
  const cpc = sold > 0 ? totalSpend / sold : null;
  const q2b = quotes > 0 ? binds / quotes : null;

  /* Aggregate raw _sum columns, then compute ratios from sums
     (same formulas as the BQ SQL — ratio of aggregated sums, NOT weighted avg of ratios) */
  const targetCpbSum = baselineRows.reduce((sum, row) => sum + (Number(row.target_cpb_sum) || 0), 0);
  const avgProfitSum = baselineRows.reduce((sum, row) => sum + (Number(row.avg_profit_sum) || 0), 0);
  const avgEquitySum = baselineRows.reduce((sum, row) => sum + (Number(row.avg_equity_sum) || 0), 0);
  const lifetimeCostSum = baselineRows.reduce((sum, row) => sum + (Number(row.lifetime_cost_sum) || 0), 0);
  const lifetimePremiumSum = baselineRows.reduce((sum, row) => sum + (Number(row.lifetime_premium_sum) || 0), 0);
  const avgMrltvSum = baselineRows.reduce((sum, row) => sum + (Number(row.avg_mrltv_sum) || 0), 0);

  const cpb = binds > 0 ? totalSpend / binds : 0;
  const targetCpb = binds > 0 ? targetCpbSum / binds : 0;
  const avgProfit = scoredPolicies > 0 ? avgProfitSum / scoredPolicies : 0;
  const avgEquity = scoredPolicies > 0 ? avgEquitySum / scoredPolicies : 0;
  const avgLifetimeCost = scoredPolicies > 0 ? lifetimeCostSum / scoredPolicies : 0;
  const avgLifetimePremium = scoredPolicies > 0 ? lifetimePremiumSum / scoredPolicies : 0;

  /* Performance = target_cpb / cpb (same as BQ) */
  const performance = cpb > 0 ? targetCpb / cpb : null;

  /* ROE = (avg_profit - 0.8*(cpb/0.81 + qbc)) / avg_equity (same as BQ buildRoeSql) */
  const roe = scoredPolicies > 0 && avgEquity !== 0
    ? (avgProfit - 0.8 * (cpb / 0.81 + qbc)) / avgEquity
    : null;

  /* COR = (cpb/0.81 + qbc + avg_lifetime_cost) / avg_lifetime_premium (same as BQ buildCombinedRatioSql) */
  const cor = scoredPolicies > 0 && avgLifetimePremium !== 0
    ? (cpb / 0.81 + qbc + avgLifetimeCost) / avgLifetimePremium
    : null;

  /* LTV = avg_mrltv */
  const ltv = scoredPolicies > 0 ? avgMrltvSum / scoredPolicies : null;

  const additionalClicks = recommendedRows.reduce((sum, row) => sum + (Number(row.additional_clicks) || 0), 0);
  const additionalBinds = recommendedRows.reduce((sum, row) => sum + (Number(row.additional_binds) || 0), 0);
  const additionalBudget = recommendedRows.reduce((sum, row) => sum + (Number(row.additional_budget) || 0), 0);
  const expectedTotalCost = totalSpend + additionalBudget;
  const currentCpb = binds > 0 ? totalSpend / binds : null;
  const expectedCpb = binds + additionalBinds > 0 ? expectedTotalCost / (binds + additionalBinds) : null;

  const wrRollup = recommendedRows.reduce(
    (acc, row) => {
      const current = Number(row.current_wr);
      const expected = Number(row.expected_wr);
      const weight = Number(row.wr_weight) || 0;
      if (weight > 0 && Number.isFinite(current) && Number.isFinite(expected)) {
        acc.currentWins += current * weight;
        acc.expectedWins += expected * weight;
      }
      return acc;
    },
    { currentWins: 0, expectedWins: 0 }
  );
  const cpbRollup = recommendedRows.reduce(
    (acc, row) => {
      const current = Number(row.current_cpb);
      const expected = Number(row.expected_cpb);
      const weight = Number(row.cpb_weight) || 0;
      if (weight > 0 && Number.isFinite(current) && Number.isFinite(expected)) {
        acc.currentCost += current * weight;
        acc.expectedCost += expected * weight;
      }
      return acc;
    },
    { currentCost: 0, expectedCost: 0 }
  );

  const wrUplift =
    wrRollup.currentWins > 0 ? (wrRollup.expectedWins - wrRollup.currentWins) / wrRollup.currentWins : null;
  const expectedClicks = bids + additionalClicks;
  const expectedCpc = expectedClicks > 0 ? expectedTotalCost / expectedClicks : null;
  const cpcUplift =
    Number.isFinite(Number(cpc)) && Number.isFinite(Number(expectedCpc)) && Number(cpc) > 0
      ? (Number(expectedCpc) - Number(cpc)) / Number(cpc)
      : null;
  const cpbUplift =
    cpbRollup.currentCost > 0 ? (cpbRollup.expectedCost - cpbRollup.currentCost) / cpbRollup.currentCost : null;

  return {
    rule_name: "Aggregate",
    states: [],
    segments: [],
    target_cor: null,
    bids,
    sold,
    total_spend: totalSpend,
    cpc,
    wr,
    quotes,
    binds,
    current_cpb: currentCpb,
    expected_cpb: expectedCpb,
    q2b,
    performance,
    roe,
    cor,
    additional_clicks: additionalClicks,
    additional_binds: additionalBinds,
    wr_uplift: wrUplift,
    cpc_uplift: cpcUplift,
    cpb_uplift: cpbUplift,
    expected_total_cost: expectedTotalCost,
    additional_budget: additionalBudget,
    _scored_policies: scoredPolicies,
    _lifetime_cost_sum: lifetimeCostSum,
    _lifetime_premium_sum: lifetimePremiumSum,
    _target_cpb_sum: targetCpbSum,
    _avg_profit_sum: avgProfitSum,
    _avg_equity_sum: avgEquitySum,
    _wr_rollup_current: wrRollup.currentWins,
    _wr_rollup_expected: wrRollup.expectedWins,
    _cpb_rollup_current: cpbRollup.currentCost,
    _cpb_rollup_expected: cpbRollup.expectedCost,
    ltv
  };
}

export async function getStateAnalysis(
  filters: StrategyAnalysisFilters
): Promise<StateAnalysisResponse> {
  const paramRows = await query<{ param_value: string }>(
    `
      SELECT param_value
      FROM ${table("plan_parameters")}
      WHERE plan_id = @planId
        AND param_key = 'plan_strategy_config'
      LIMIT 1
    `,
    { planId: filters.planId }
  );

  const rules = parseStrategyRules(paramRows[0]?.param_value || "", filters.activityLeadType);
  if (!rules.length) {
    return {
      overall: {
        ...aggregateStrategySlice([], []),
        rule_name: "All rules",
        states: [],
        segments: []
      },
      states: ALL_US_STATE_CODES.map((stateCode) => ({
        state: stateCode,
        rule_name: null,
        tier: null,
        strategy_key: null,
        strategy_label: null,
        total_spend: 0,
        cor: null,
        roe: null,
        binds: 0,
        performance: null,
        additional_clicks: 0,
        additional_binds: 0,
        additional_budget: 0,
        cpc_uplift: null,
        cpb_uplift: null
      })),
      state_details: [],
      rules: []
    };
  }

  const uniqueStates = [...new Set(rules.flatMap((rule) => rule.states))];
  const uniqueSegments = [...new Set(rules.flatMap((rule) => rule.segments))];
  const qbc = Number.isFinite(Number(filters.qbc)) ? Number(filters.qbc) : 0;
  const [rawBaselineRows, priceRows] = await Promise.all([
    getStateSegmentPerformance({
      startDate: filters.startDate,
      endDate: filters.endDate,
      states: uniqueStates,
      segments: uniqueSegments,
      activityLeadType: filters.activityLeadType,
      qbc,
      groupBy: "state_segment"
    }),
    getPriceExploration({
      planId: filters.planId,
      startDate: filters.startDate,
      endDate: filters.endDate,
      q2bStartDate: filters.startDate,
      q2bEndDate: filters.endDate,
      states: uniqueStates,
      activityLeadType: filters.activityLeadType,
      qbc,
      limit: 200000
    })
  ]);

  /* BQ already grouped by (state, segment) — map directly, no channel re-grouping needed */
  const baselineRows: StrategyBaselineExtRow[] = rawBaselineRows.map((row) => ({
    state: String(row.state || "").toUpperCase(),
    segment: String(row.segment || "").toUpperCase(),
    bids: Number(row.bids) || 0,
    sold: Number(row.sold) || 0,
    total_cost: Number(row.total_cost) || 0,
    quotes: Number(row.quotes) || 0,
    binds: Number(row.binds) || 0,
    scored_policies: Number(row.scored_policies) || 0,
    performance: Number.isFinite(Number(row.performance)) ? Number(row.performance) : null,
    roe: Number.isFinite(Number(row.roe)) ? Number(row.roe) : null,
    combined_ratio: Number.isFinite(Number(row.combined_ratio)) ? Number(row.combined_ratio) : null,
    mrltv: Number.isFinite(Number(row.mrltv)) ? Number(row.mrltv) : null,
    target_cpb_sum: Number(row.target_cpb_sum) || 0,
    avg_profit_sum: Number(row.avg_profit_sum) || 0,
    avg_equity_sum: Number(row.avg_equity_sum) || 0,
    lifetime_cost_sum: Number(row.lifetime_cost_sum) || 0,
    lifetime_premium_sum: Number(row.lifetime_premium_sum) || 0,
    avg_mrltv_sum: Number(row.avg_mrltv_sum) || 0
  }));

  const rowsByChannelStateSegment = new Map<string, PriceExplorationRow[]>();
  for (const row of priceRows) {
    const state = String(row.state || "").toUpperCase();
    const segment = extractSegmentFromChannelGroup(String(row.channel_group_name || "")) || "__UNSEGMENTED__";
    if (!state) {
      continue;
    }
    const key = `${String(row.channel_group_name || "")}|${state}|${segment}`;
    const bucket = rowsByChannelStateSegment.get(key) || [];
    bucket.push(row);
    rowsByChannelStateSegment.set(key, bucket);
  }

  const recommendedSummaries: StrategyRecommendedSummary[] = [];
  for (const groupRows of rowsByChannelStateSegment.values()) {
    const baseline = groupRows.find((row) => Number(row.testing_point) === 0) || null;
    const recommendedByFlag = groupRows.find(
      (row) =>
        Number.isFinite(Number(row.recommended_testing_point)) &&
        Number(row.testing_point) === Number(row.recommended_testing_point)
    );
    const recommended = recommendedByFlag || groupRows[0];
    if (!recommended) {
      continue;
    }

    const state = String(recommended.state || "").toUpperCase();
    const segment = extractSegmentFromChannelGroup(String(recommended.channel_group_name || "")) || "__UNSEGMENTED__";
    if (!state) {
      continue;
    }

    const baselineBids = Number(baseline?.bids);
    const baselineBinds = Number(baseline?.binds);
    const baselineWr = Number(baseline?.win_rate);
    const expectedWr = Number(recommended.win_rate);
    const wrUplift = Number(recommended.win_rate_uplift);
    const derivedBaselineWr =
      Number.isFinite(expectedWr) && Number.isFinite(wrUplift) && Math.abs(1 + wrUplift) > 1e-9
        ? expectedWr / (1 + wrUplift)
        : null;
    const currentWr = Number.isFinite(baselineWr) ? baselineWr : derivedBaselineWr;

    const currentCpb = Number(recommended.current_cpb);
    const expectedCpb = Number(recommended.expected_cpb);

    recommendedSummaries.push({
      state,
      segment,
      additional_clicks: Number(recommended.additional_clicks) || 0,
      additional_binds: Number(recommended.expected_bind_change) || 0,
      additional_budget: Number(recommended.additional_budget_needed) || 0,
      current_wr: Number.isFinite(currentWr) ? currentWr : null,
      expected_wr: Number.isFinite(expectedWr) ? expectedWr : null,
      wr_weight: Number.isFinite(baselineBids) && baselineBids > 0 ? baselineBids : Number(recommended.bids) || 0,
      current_cpb: Number.isFinite(currentCpb) ? currentCpb : null,
      expected_cpb: Number.isFinite(expectedCpb) ? expectedCpb : null,
      cpb_weight:
        Number.isFinite(baselineBinds) && baselineBinds > 0 ? baselineBinds : Number(recommended.binds) || 0
    });
  }

  const overallAgg = aggregateStrategySlice(baselineRows, recommendedSummaries, qbc);

  const ruleRows: StateAnalysisRuleRow[] = rules.map((rule) => {
    const stateSet = new Set(rule.states);
    const segmentSet = new Set(rule.segments);
    const includesAllCoreSegments =
      (segmentSet.has("MCH") && segmentSet.has("MCR") && segmentSet.has("SCH") && segmentSet.has("SCR")) ||
      (segmentSet.has("HOME") && segmentSet.has("RENT"));
    const strategy = mapGrowthStrategy(rule.growthStrategy);

    const baselineSlice = baselineRows.filter(
      (row) => stateSet.has(row.state) && segmentSet.has(row.segment)
    );
    const recommendedSlice = recommendedSummaries.filter(
      (row) => stateSet.has(row.state) && (includesAllCoreSegments || segmentSet.has(row.segment))
    );
    const ruleAgg = aggregateStrategySlice(baselineSlice, recommendedSlice, qbc);

    const segmentRows: StateAnalysisSegmentRow[] = rule.segments.map((segment) => {
      const baselineSegment = baselineRows.filter((row) => stateSet.has(row.state) && row.segment === segment);
      const recommendedSegment = recommendedSummaries.filter(
        (row) => stateSet.has(row.state) && row.segment === segment
      );
      const agg = aggregateStrategySlice(baselineSegment, recommendedSegment, qbc);
      return {
        segment,
        bids: agg.bids,
        sold: agg.sold,
        wr: agg.wr,
        total_spend: agg.total_spend,
        quotes: agg.quotes,
        sold_to_quotes: agg.quotes > 0 ? agg.sold / agg.quotes : null,
        binds: agg.binds,
        q2b: agg.q2b,
        cor: agg.cor,
        roe: agg.roe,
        cpb: agg.current_cpb,
        performance: agg.performance
      };
    });

    return {
      rule_name: rule.name,
      tier: Number(rule.id) || 999,
      strategy_key: strategy.key,
      strategy_label: strategy.label,
      states: rule.states,
      segments: rule.segments,
      kpis: {
        ...ruleAgg,
        rule_name: rule.name,
        states: rule.states,
        segments: rule.segments
      },
      segment_rows: segmentRows
    };
  });

  const stateRows: StateAnalysisStateRow[] = ALL_US_STATE_CODES.map((stateCode) => {
    const matchingRules = ruleRows
      .filter((rule) => rule.states.includes(stateCode))
      .sort((a, b) => (a.tier || 999) - (b.tier || 999));
    const primaryRule = matchingRules[0] || null;
    const segmentSet = primaryRule ? new Set(primaryRule.segments) : null;

    const baselineSlice = baselineRows.filter(
      (row) => row.state === stateCode && (!segmentSet || segmentSet.has(row.segment))
    );
    const recommendedSlice = recommendedSummaries.filter(
      (row) => row.state === stateCode && (!segmentSet || segmentSet.has(row.segment))
    );
    const agg = aggregateStrategySlice(baselineSlice, recommendedSlice, qbc);

    return {
      state: stateCode,
      rule_name: primaryRule?.rule_name || null,
      tier: primaryRule?.tier ?? null,
      strategy_key: primaryRule?.strategy_key ?? null,
      strategy_label: primaryRule?.strategy_label ?? null,
      total_spend: agg.total_spend,
      cor: agg.cor,
      roe: agg.roe,
      binds: agg.binds,
      performance: agg.performance,
      additional_clicks: agg.additional_clicks,
      additional_binds: agg.additional_binds,
      additional_budget: agg.additional_budget,
      cpc_uplift: agg.cpc_uplift,
      cpb_uplift: agg.cpb_uplift
    };
  });

  const stateDetails = stateRows.map((stateRow) => {
    const segmentSet = stateRow.rule_name
      ? new Set(
          (ruleRows.find((rule) => rule.rule_name === stateRow.rule_name)?.segments || []).map((value) =>
            String(value || "").toUpperCase()
          )
        )
      : null;
    const baselineSlice = baselineRows.filter(
      (row) => row.state === stateRow.state && (!segmentSet || segmentSet.has(String(row.segment || "").toUpperCase()))
    );
    const recommendedSlice = recommendedSummaries.filter(
      (row) => row.state === stateRow.state && (!segmentSet || segmentSet.has(String(row.segment || "").toUpperCase()))
    );
    const agg = aggregateStrategySlice(baselineSlice, recommendedSlice, qbc);

    const segments = segmentSet ? [...segmentSet] : [...new Set(baselineSlice.map((row) => String(row.segment || "").toUpperCase()))];
    segments.sort();
    const segmentRows: StateAnalysisSegmentRow[] = segments.map((segment) => {
      const baselineSegment = baselineSlice.filter((row) => String(row.segment || "").toUpperCase() === segment);
      const recommendedSegment = recommendedSlice.filter((row) => String(row.segment || "").toUpperCase() === segment);
      const segmentAgg = aggregateStrategySlice(baselineSegment, recommendedSegment, qbc);
      return {
        segment,
        bids: segmentAgg.bids,
        sold: segmentAgg.sold,
        wr: segmentAgg.wr,
        total_spend: segmentAgg.total_spend,
        quotes: segmentAgg.quotes,
        sold_to_quotes: segmentAgg.quotes > 0 ? segmentAgg.sold / segmentAgg.quotes : null,
        binds: segmentAgg.binds,
        q2b: segmentAgg.q2b,
        cor: segmentAgg.cor,
        roe: segmentAgg.roe,
        cpb: segmentAgg.current_cpb,
        performance: segmentAgg.performance
      };
    });

    return {
      state: stateRow.state,
      rule_name: stateRow.rule_name,
      tier: stateRow.tier,
      strategy_key: stateRow.strategy_key,
      strategy_label: stateRow.strategy_label,
      kpis: {
        ...agg,
        rule_name: stateRow.rule_name || "State",
        states: [stateRow.state],
        segments
      },
      segment_rows: segmentRows
    };
  });

  const result = {
    overall: {
      ...overallAgg,
      rule_name: "All rules",
      states: uniqueStates,
      segments: uniqueSegments
    },
    states: stateRows,
    state_details: stateDetails,
    rules: ruleRows
  };
  return result;
}

// ── Plans Comparison ──────────────────────────────────────────────────
// Returns one row per plan (plans mode) or one row per activity/lead type (activity mode).
// Each row is the "overall" KPI from getStateAnalysis.

const ACTIVITY_LEAD_TYPES = [
  "clicks_auto", "clicks_home", "leads_auto", "leads_home", "calls_auto", "calls_home"
] as const;

const ACTIVITY_LEAD_LABELS: Record<string, string> = {
  clicks_auto: "Clicks / Auto",
  clicks_home: "Clicks / Home",
  leads_auto: "Leads / Auto",
  leads_home: "Leads / Home",
  calls_auto: "Calls / Auto",
  calls_home: "Calls / Home",
};

export type PlansComparisonRow = {
  label: string;
  plan_id?: string;
  activity_lead_type?: string;
  target_cor: number | null;
  bids: number;
  sold: number;
  total_spend: number;
  cpc: number | null;
  wr: number | null;
  binds: number;
  current_cpb: number | null;
  expected_cpb: number | null;
  q2b: number | null;
  performance: number | null;
  roe: number | null;
  cor: number | null;
  additional_clicks: number;
  additional_binds: number;
  wr_uplift: number | null;
  cpc_uplift: number | null;
  cpb_uplift: number | null;
  expected_total_cost: number;
  additional_budget: number;
};

export async function getPlansComparison(opts: {
  mode: "plans" | "activity";
  planId?: string;
  startDate?: string;
  endDate?: string;
  plans: Array<{ plan_id: string; plan_name: string; plan_context_json?: string | null; status?: string }>;
}): Promise<PlansComparisonRow[]> {
  const { mode, plans } = opts;

  if (mode === "plans") {
    // One row per plan — use each plan's own dates/activity/qbc
    const results = await Promise.all(
      plans
        .filter((p) => p.status !== "archived")
        .map(async (plan) => {
          let ctx: Record<string, unknown> = {};
          try { ctx = plan.plan_context_json ? JSON.parse(plan.plan_context_json) : {}; } catch { /* ignore */ }

          const startDate = opts.startDate || String(ctx.perfStartDate || ctx.performanceStartDate || "");
          const endDate = opts.endDate || String(ctx.perfEndDate || ctx.performanceEndDate || "");
          const activity = String(ctx.activity || "clicks");
          const leadType = String(ctx.leadType || "auto");
          const activityLeadType = ctx.activityLeadType ? String(ctx.activityLeadType) : `${activity}_${leadType}`;
          const qbc = resolveQbc(activityLeadType, Number(ctx.qbcClicks) || 0, Number(ctx.qbcLeadsCalls) || 0);

          if (!startDate || !endDate || !qbc) {
            return { label: plan.plan_name, plan_id: plan.plan_id, empty: true } as PlansComparisonRow & { empty?: boolean };
          }

          try {
            const sa = await getStateAnalysis({ planId: plan.plan_id, startDate, endDate, activityLeadType, qbc });
            return {
              label: plan.plan_name,
              plan_id: plan.plan_id,
              target_cor: sa.overall.target_cor,
              bids: sa.overall.bids,
              sold: sa.overall.sold,
              total_spend: sa.overall.total_spend,
              cpc: sa.overall.cpc,
              wr: sa.overall.wr,
              binds: sa.overall.binds,
              current_cpb: sa.overall.current_cpb,
              expected_cpb: sa.overall.expected_cpb,
              q2b: sa.overall.q2b,
              performance: sa.overall.performance,
              roe: sa.overall.roe,
              cor: sa.overall.cor,
              additional_clicks: sa.overall.additional_clicks,
              additional_binds: sa.overall.additional_binds,
              wr_uplift: sa.overall.wr_uplift,
              cpc_uplift: sa.overall.cpc_uplift,
              cpb_uplift: sa.overall.cpb_uplift,
              expected_total_cost: sa.overall.expected_total_cost,
              additional_budget: sa.overall.additional_budget,
            } satisfies PlansComparisonRow;
          } catch {
            return { label: plan.plan_name, plan_id: plan.plan_id, empty: true } as PlansComparisonRow & { empty?: boolean };
          }
        })
    );
    return results.filter((r) => !(r as { empty?: boolean }).empty) as PlansComparisonRow[];
  }

  // Activity mode — one row per activity/lead type for a single plan
  const planId = opts.planId;
  if (!planId) return [];

  const plan = plans.find((p) => p.plan_id === planId);
  let ctx: Record<string, unknown> = {};
  try { ctx = plan?.plan_context_json ? JSON.parse(plan.plan_context_json) : {}; } catch { /* ignore */ }

  const startDate = opts.startDate || String(ctx.perfStartDate || ctx.performanceStartDate || "");
  const endDate = opts.endDate || String(ctx.perfEndDate || ctx.performanceEndDate || "");
  const qbcClicks = Number(ctx.qbcClicks) || 0;
  const qbcLeadsCalls = Number(ctx.qbcLeadsCalls) || 0;
  if (!startDate || !endDate || (!qbcClicks && !qbcLeadsCalls)) return [];

  const results = await Promise.all(
    ACTIVITY_LEAD_TYPES.map(async (alt) => {
      const qbc = resolveQbc(alt, qbcClicks, qbcLeadsCalls);
      try {
        const sa = await getStateAnalysis({ planId, startDate, endDate, activityLeadType: alt, qbc });
        return {
          label: ACTIVITY_LEAD_LABELS[alt],
          activity_lead_type: alt,
          target_cor: sa.overall.target_cor,
          bids: sa.overall.bids,
          sold: sa.overall.sold,
          total_spend: sa.overall.total_spend,
          cpc: sa.overall.cpc,
          wr: sa.overall.wr,
          binds: sa.overall.binds,
          current_cpb: sa.overall.current_cpb,
          expected_cpb: sa.overall.expected_cpb,
          q2b: sa.overall.q2b,
          performance: sa.overall.performance,
          roe: sa.overall.roe,
          cor: sa.overall.cor,
          additional_clicks: sa.overall.additional_clicks,
          additional_binds: sa.overall.additional_binds,
          wr_uplift: sa.overall.wr_uplift,
          cpc_uplift: sa.overall.cpc_uplift,
          cpb_uplift: sa.overall.cpb_uplift,
          expected_total_cost: sa.overall.expected_total_cost,
          additional_budget: sa.overall.additional_budget,
        } satisfies PlansComparisonRow;
      } catch {
        return null;
      }
    })
  );
  return results.filter(Boolean) as PlansComparisonRow[];
}
