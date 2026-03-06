import "dotenv/config";
import { getStrategyAnalysis, getStateSegmentPerformance, getPriceExploration } from "./src/services/analyticsService.js";
import fs from "node:fs";

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
  const rows = await getStrategyAnalysis({ planId: PLAN_ID, startDate: START, endDate: END, activityLeadType: ACTIVITY });
  const rule = rows.find((r) => r.rule_name === RULE_NAME);
  if (!rule) throw new Error("Rule not found");

  const baseline = await getStateSegmentPerformance({
    startDate: START,
    endDate: END,
    states: rule.states,
    segments: rule.segments,
    activityLeadType: ACTIVITY
  });

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

  const matchedRecommended = recommendedRows
    .filter((r) => rule.states.includes(String(r.state || "").toUpperCase()))
    .map((r) => ({
      channel_group_name: r.channel_group_name,
      state: r.state,
      segment: segFromChannel(String(r.channel_group_name || "")),
      testing_point: r.testing_point,
      recommended_testing_point: r.recommended_testing_point,
      stat_sig: r.stat_sig,
      stat_sig_source: r.stat_sig_source,
      sold: r.sold,
      quotes: r.quotes,
      binds: r.binds,
      click_to_quote: r.click_to_quote,
      q2b: r.q2b,
      channel_q2b: r.channel_q2b,
      additional_clicks: r.additional_clicks,
      expected_bind_change: r.expected_bind_change,
      cpc: r.cpc,
      cpc_uplift: r.cpc_uplift,
      current_cpb: r.current_cpb,
      expected_cpb: r.expected_cpb,
      cpb_uplift: r.cpb_uplift,
      additional_budget_needed: r.additional_budget_needed
    }))
    .sort((a, b) => `${a.channel_group_name}|${a.segment}`.localeCompare(`${b.channel_group_name}|${b.segment}`));

  const result = {
    meta: { planId: PLAN_ID, startDate: START, endDate: END, activityLeadType: ACTIVITY, ruleName: RULE_NAME },
    strategyRowFromApi: rule,
    baselineRows: baseline,
    recommendedRowsUsed: matchedRecommended,
    checks: {
      additional_clicks_sum: matchedRecommended.reduce((s, r) => s + (Number(r.additional_clicks) || 0), 0),
      additional_binds_sum: matchedRecommended.reduce((s, r) => s + (Number(r.expected_bind_change) || 0), 0),
      additional_budget_sum: matchedRecommended.reduce((s, r) => s + (Number(r.additional_budget_needed) || 0), 0)
    }
  };

  fs.mkdirSync("debug", { recursive: true });
  const outPath = "debug/or_strategy_full_log_2026-01.json";
  fs.writeFileSync(outPath, JSON.stringify(result, null, 2));
  console.log(outPath);
  console.log(JSON.stringify(result.checks, null, 2));
})();
