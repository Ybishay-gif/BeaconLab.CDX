import "dotenv/config";
import { getStrategyAnalysis, getStateSegmentPerformance } from "./src/services/analyticsService.js";

(async () => {
  const planId = "089bd324-1dd9-47cf-8704-d590123860bd";
  const startDate = "2026-01-01";
  const endDate = "2026-01-31";
  const activityLeadType = "clicks_auto";
  const ruleName = "Tier 1 - Robust Growth";

  const rows = await getStrategyAnalysis({ planId, startDate, endDate, activityLeadType });
  const tier = rows.find((r) => r.rule_name === ruleName);
  if (!tier) {
    console.log(JSON.stringify({ error: "Tier not found", available: rows.map((r) => r.rule_name) }, null, 2));
    process.exit(1);
  }

  const baseline = await getStateSegmentPerformance({
    startDate,
    endDate,
    states: tier.states,
    segments: tier.segments,
    activityLeadType
  });

  const currentSpend = baseline.reduce((sum, r) => sum + (Number(r.total_cost) || 0), 0);
  const additionalBudget = Number(tier.additional_budget) || 0;
  const expectedTotalBudget = currentSpend + additionalBudget;

  console.log(JSON.stringify({
    ruleName,
    states: tier.states,
    segments: tier.segments,
    currentSpend,
    additionalBudget,
    expectedTotalBudget
  }, null, 2));
})();
