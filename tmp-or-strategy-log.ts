import "dotenv/config";
import { getStrategyAnalysis, getStateSegmentPerformance, getPriceExploration } from "./src/services/analyticsService.js";

const PLAN_ID = "089bd324-1dd9-47cf-8704-d590123860bd";
const START = "2026-01-01";
const END = "2026-01-31";
const ACTIVITY = "clicks_auto";
const RULE_NAME = "Tier 1 - Robust Growth";

function segFromChannel(channel: string): string {
  const m = String(channel || "").toUpperCase().match(/\b(MCH|MCR|SCH|SCR)\b/);
  return m ? m[1] : "";
}

(async () => {
  const rows = await getStrategyAnalysis({
    planId: PLAN_ID,
    startDate: START,
    endDate: END,
    activityLeadType: ACTIVITY
  });
  const rule = rows.find((r) => r.rule_name === RULE_NAME);
  if (!rule) throw new Error("Rule not found");

  const baseline = await getStateSegmentPerformance({
    startDate: START,
    endDate: END,
    states: rule.states,
    segments: rule.segments,
    activityLeadType: ACTIVITY
  });

  const baselineBySegment: Record<string, any> = {};
  for (const seg of ["MCH", "MCR", "SCH", "SCR"]) {
    const rowsSeg = baseline.filter((r) => String(r.segment || "").toUpperCase() === seg);
    const bids = rowsSeg.reduce((s, r) => s + (Number(r.bids) || 0), 0);
    const sold = rowsSeg.reduce((s, r) => s + (Number(r.sold) || 0), 0);
    const total_cost = rowsSeg.reduce((s, r) => s + (Number(r.total_cost) || 0), 0);
    const quotes = rowsSeg.reduce((s, r) => s + (Number(r.quotes) || 0), 0);
    const binds = rowsSeg.reduce((s, r) => s + (Number(r.binds) || 0), 0);
    baselineBySegment[seg] = {
      bids, sold, total_cost, quotes, binds,
      wr: bids > 0 ? sold / bids : null,
      cpc: sold > 0 ? total_cost / sold : null,
      q2b: quotes > 0 ? binds / quotes : null,
      click_to_quote: sold > 0 ? quotes / sold : null
    };
  }

  const pe = await getPriceExploration({
    planId: PLAN_ID,
    startDate: START,
    endDate: END,
    q2bStartDate: START,
    q2bEndDate: END,
    states: rule.states,
    activityLeadType: ACTIVITY,
    limit: 200000
  });

  const byKey = new Map<string, any[]>();
  for (const r of pe) {
    const state = String(r.state || "").toUpperCase();
    const seg = segFromChannel(String(r.channel_group_name || "")) || "__UNSEGMENTED__";
    const key = `${String(r.channel_group_name || "")}|${state}|${seg}`;
    const arr = byKey.get(key) || [];
    arr.push(r);
    byKey.set(key, arr);
  }

  const recommendedRows: any[] = [];
  for (const arr of byKey.values()) {
    const pick = arr.find((r) => Number(r.testing_point) === Number(r.recommended_testing_point));
    if (pick) recommendedRows.push(pick);
  }

  const matchedRecommended = recommendedRows.filter((r) => rule.states.includes(String(r.state || "").toUpperCase()));

  const recBySegment: Record<string, any> = {};
  for (const seg of ["MCH", "MCR", "SCH", "SCR"]) {
    const rowsSeg = matchedRecommended.filter((r) => segFromChannel(String(r.channel_group_name || "")) === seg);
    recBySegment[seg] = {
      rows: rowsSeg.length,
      additional_clicks: rowsSeg.reduce((s, r) => s + (Number(r.additional_clicks) || 0), 0),
      additional_binds: rowsSeg.reduce((s, r) => s + (Number(r.expected_bind_change) || 0), 0),
      additional_budget: rowsSeg.reduce((s, r) => s + (Number(r.additional_budget_needed) || 0), 0),
      cpb_uplift_weighted_current_cost: rowsSeg.reduce((s, r) => s + ((Number(r.current_cpb) || 0) * (Number(r.binds) || 0)), 0),
      cpb_uplift_weighted_expected_cost: rowsSeg.reduce((s, r) => s + ((Number(r.expected_cpb) || 0) * (Number(r.binds) || 0)), 0),
      cpb_uplift_weight: rowsSeg.reduce((s, r) => s + (Number(r.binds) || 0), 0)
    };
  }

  const recTotals = {
    rows: matchedRecommended.length,
    additional_clicks: matchedRecommended.reduce((s, r) => s + (Number(r.additional_clicks) || 0), 0),
    additional_binds: matchedRecommended.reduce((s, r) => s + (Number(r.expected_bind_change) || 0), 0),
    additional_budget: matchedRecommended.reduce((s, r) => s + (Number(r.additional_budget_needed) || 0), 0)
  };

  const baselineTotals = {
    bids: baseline.reduce((s, r) => s + (Number(r.bids) || 0), 0),
    sold: baseline.reduce((s, r) => s + (Number(r.sold) || 0), 0),
    total_spend: baseline.reduce((s, r) => s + (Number(r.total_cost) || 0), 0),
    quotes: baseline.reduce((s, r) => s + (Number(r.quotes) || 0), 0),
    binds: baseline.reduce((s, r) => s + (Number(r.binds) || 0), 0)
  };

  const wr_rollup = matchedRecommended.reduce((acc, r) => {
    const cur = Number(r.win_rate);
    const wrUplift = Number(r.win_rate_uplift);
    const curWins = Number(r.bids) || 0;
    const exp = Number.isFinite(cur) && Number.isFinite(wrUplift) ? cur : NaN;
    const expected = Number.isFinite(cur) && Number.isFinite(wrUplift) ? cur * (1 + wrUplift) : NaN;
    if (Number.isFinite(exp) && Number.isFinite(expected) && curWins > 0) {
      acc.current += exp * curWins;
      acc.expected += expected * curWins;
      acc.weight += curWins;
    }
    return acc;
  }, { current: 0, expected: 0, weight: 0 });

  const cpb_rollup = matchedRecommended.reduce((acc, r) => {
    const cur = Number(r.current_cpb);
    const exp = Number(r.expected_cpb);
    const w = Number(r.binds) || 0;
    if (Number.isFinite(cur) && Number.isFinite(exp) && w > 0) {
      acc.current += cur * w;
      acc.expected += exp * w;
      acc.weight += w;
    }
    return acc;
  }, { current: 0, expected: 0, weight: 0 });

  const currentCpcTier = baselineTotals.bids > 0 ? baselineTotals.total_spend / baselineTotals.bids : null;
  const expectedCostTier = baselineTotals.total_spend + recTotals.additional_budget;
  const expectedClicksTier = baselineTotals.bids + recTotals.additional_clicks;
  const expectedCpcTier = expectedClicksTier > 0 ? expectedCostTier / expectedClicksTier : null;

  const out = {
    meta: { planId: PLAN_ID, startDate: START, endDate: END, activityLeadType: ACTIVITY, ruleName: RULE_NAME },
    strategyRowFromApi: rule,
    baselineTotals,
    baselineBySegment,
    recommendedTotals: recTotals,
    recommendedBySegment: recBySegment,
    derivedChecks: {
      wr_uplift: wr_rollup.current > 0 ? (wr_rollup.expected - wr_rollup.current) / wr_rollup.current : null,
      cpc_uplift: currentCpcTier && expectedCpcTier ? (expectedCpcTier - currentCpcTier) / currentCpcTier : null,
      cpb_uplift: cpb_rollup.current > 0 ? (cpb_rollup.expected - cpb_rollup.current) / cpb_rollup.current : null,
      current_cpc_tier: currentCpcTier,
      expected_cpc_tier: expectedCpcTier,
      expected_total_cost_tier: expectedCostTier,
      expected_total_binds_tier: baselineTotals.binds + recTotals.additional_binds
    },
    sampleRecommendedRows: matchedRecommended
      .sort((a, b) => String(a.channel_group_name).localeCompare(String(b.channel_group_name)))
      .slice(0, 20)
      .map((r) => ({
        channel_group_name: r.channel_group_name,
        state: r.state,
        segment: segFromChannel(String(r.channel_group_name || "")),
        testing_point: r.testing_point,
        recommended_testing_point: r.recommended_testing_point,
        stat_sig: r.stat_sig,
        additional_clicks: r.additional_clicks,
        expected_bind_change: r.expected_bind_change,
        additional_budget_needed: r.additional_budget_needed,
        current_cpb: r.current_cpb,
        expected_cpb: r.expected_cpb
      }))
  };

  console.log(JSON.stringify(out, null, 2));
})();
