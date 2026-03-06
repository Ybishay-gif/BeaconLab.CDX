import "dotenv/config";
import fs from "node:fs";
import { getStrategyAnalysis, getStateSegmentPerformance, getPriceExploration } from "./src/services/analyticsService.js";

const PLAN_ID = "089bd324-1dd9-47cf-8704-d590123860bd";
const START = "2026-01-01";
const END = "2026-01-31";
const ACTIVITY = "clicks_auto";
const RULE_NAME = "Tier 1 - Robust Growth";
const OUT_DIR = "debug/or_plan_inputs_2026-01";

function segFromChannel(channel: string): string {
  const m = String(channel || "").toUpperCase().match(/\b(MCH|MCR|SCH|SCR)\b/);
  return m ? m[1] : "";
}

function toCsv(rows: Record<string, unknown>[]): string {
  if (!rows.length) return "";
  const keys = Array.from(rows.reduce((set, row) => {
    for (const key of Object.keys(row)) set.add(key);
    return set;
  }, new Set<string>()));

  const escape = (value: unknown) => {
    if (value === null || value === undefined) return "";
    const text = typeof value === "object" ? JSON.stringify(value) : String(value);
    if (/[",\n]/.test(text)) {
      return `"${text.replace(/"/g, '""')}"`;
    }
    return text;
  };

  const header = keys.join(",");
  const body = rows.map((row) => keys.map((k) => escape(row[k])).join(",")).join("\n");
  return `${header}\n${body}\n`;
}

(async () => {
  fs.mkdirSync(OUT_DIR, { recursive: true });

  const strategyRows = await getStrategyAnalysis({
    planId: PLAN_ID,
    startDate: START,
    endDate: END,
    activityLeadType: ACTIVITY
  });
  const strategyRow = strategyRows.find((r) => r.rule_name === RULE_NAME);
  if (!strategyRow) throw new Error("Rule not found");

  const baselineRows = await getStateSegmentPerformance({
    startDate: START,
    endDate: END,
    states: strategyRow.states,
    segments: strategyRow.segments,
    activityLeadType: ACTIVITY
  });

  const peRows = await getPriceExploration({
    planId: PLAN_ID,
    startDate: START,
    endDate: END,
    q2bStartDate: START,
    q2bEndDate: END,
    states: strategyRow.states,
    activityLeadType: ACTIVITY,
    limit: 200000
  });

  const peRowsOr = peRows
    .filter((r) => strategyRow.states.includes(String(r.state || "").toUpperCase()))
    .map((r) => ({ ...r, segment: segFromChannel(String(r.channel_group_name || "")) }));

  const byKey = new Map<string, typeof peRowsOr>();
  for (const r of peRowsOr) {
    const key = `${String(r.channel_group_name || "")}|${String(r.state || "").toUpperCase()}|${String(r.segment || "") || "__UNSEGMENTED__"}`;
    const arr = byKey.get(key) || [];
    arr.push(r);
    byKey.set(key, arr);
  }

  const recommendedRowsUsed = Array.from(byKey.values())
    .map((arr) => arr.find((r) => Number(r.testing_point) === Number(r.recommended_testing_point)))
    .filter(Boolean)
    .map((r) => ({ ...r!, used_in_rollup: true }));

  const checks = {
    additional_clicks_sum: recommendedRowsUsed.reduce((s, r) => s + (Number(r.additional_clicks) || 0), 0),
    additional_binds_sum: recommendedRowsUsed.reduce((s, r) => s + (Number(r.expected_bind_change) || 0), 0),
    additional_budget_sum: recommendedRowsUsed.reduce((s, r) => s + (Number(r.additional_budget_needed) || 0), 0)
  };

  const meta = {
    planId: PLAN_ID,
    startDate: START,
    endDate: END,
    activityLeadType: ACTIVITY,
    ruleName: RULE_NAME,
    states: strategyRow.states,
    segments: strategyRow.segments,
    counts: {
      baselineRows: baselineRows.length,
      priceExplorationRowsOr: peRowsOr.length,
      recommendedRowsUsed: recommendedRowsUsed.length
    },
    checks,
    strategyRow
  };

  fs.writeFileSync(`${OUT_DIR}/00_meta.json`, JSON.stringify(meta, null, 2));
  fs.writeFileSync(`${OUT_DIR}/01_strategy_row.json`, JSON.stringify(strategyRow, null, 2));

  fs.writeFileSync(`${OUT_DIR}/02_state_segment_performance_rows.json`, JSON.stringify(baselineRows, null, 2));
  fs.writeFileSync(`${OUT_DIR}/02_state_segment_performance_rows.csv`, toCsv(baselineRows as unknown as Record<string, unknown>[]));

  fs.writeFileSync(`${OUT_DIR}/03_price_exploration_rows_or.json`, JSON.stringify(peRowsOr, null, 2));
  fs.writeFileSync(`${OUT_DIR}/03_price_exploration_rows_or.csv`, toCsv(peRowsOr as unknown as Record<string, unknown>[]));

  fs.writeFileSync(`${OUT_DIR}/04_recommended_rows_used.json`, JSON.stringify(recommendedRowsUsed, null, 2));
  fs.writeFileSync(`${OUT_DIR}/04_recommended_rows_used.csv`, toCsv(recommendedRowsUsed as unknown as Record<string, unknown>[]));

  const formulaLog = {
    formulas: {
      additional_clicks: "(win_rate_at_tp * total_bids_channel_state) - (baseline_win_rate * total_bids_channel_state)",
      click_to_quote_ratio: "if channel+state quotes>=10 then quotes/sold at channel+state else state_quotes/state_sold (fallback to channel_quote/total_bids_channel_state)",
      additional_quotes: "additional_clicks * click_to_quote_ratio",
      q2b: "if binds_state_channel>=5 then state_segment_q2b else channel_segment_q2b",
      additional_binds: "additional_quotes * q2b",
      additional_budget: "expected_total_cost - current_spend_channel_state"
    },
    checks,
    strategyRow
  };
  fs.writeFileSync(`${OUT_DIR}/05_formula_log.json`, JSON.stringify(formulaLog, null, 2));

  console.log(JSON.stringify({ outDir: OUT_DIR, files: fs.readdirSync(OUT_DIR).sort() }, null, 2));
})();
