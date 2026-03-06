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
  if (!tier) throw new Error("tier not found");

  const baseline = await getStateSegmentPerformance({
    startDate,
    endDate,
    states: tier.states,
    segments: tier.segments,
    activityLeadType
  });

  const currentClicks = baseline.reduce((sum, r) => sum + (Number(r.bids) || 0), 0);
  const currentBinds = baseline.reduce((sum, r) => sum + (Number(r.binds) || 0), 0);
  const additionalClicks = Number(tier.additional_clicks) || 0;
  const additionalBinds = Number(tier.additional_binds) || 0;

  console.log(JSON.stringify({
    ruleName,
    currentClicks,
    additionalClicks,
    expectedClicks: currentClicks + additionalClicks,
    currentBinds,
    additionalBinds,
    expectedBinds: currentBinds + additionalBinds
  }, null, 2));
})();
