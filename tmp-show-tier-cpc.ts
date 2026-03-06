import "dotenv/config";
import { getStrategyAnalysis } from "./src/services/analyticsService.js";

(async () => {
  const rows = await getStrategyAnalysis({
    planId: "089bd324-1dd9-47cf-8704-d590123860bd",
    startDate: "2026-01-01",
    endDate: "2026-01-31",
    activityLeadType: "clicks_auto"
  });
  const r = rows.find((x) => x.rule_name === "Tier 1 - Robust Growth");
  console.log(JSON.stringify(r, null, 2));
})();
