import "dotenv/config";
import { getPriceExploration } from "./src/services/analyticsService.js";

(async () => {
  const rows = await getPriceExploration({
    planId: "089bd324-1dd9-47cf-8704-d590123860bd",
    startDate: "2026-01-01",
    endDate: "2026-01-31",
    q2bStartDate: "2026-01-01",
    q2bEndDate: "2026-01-31",
    states: ["OR"],
    activityLeadType: "clicks_auto",
    limit: 200000
  });

  const target = rows
    .filter((r) => String(r.state).toUpperCase() === "OR" && String(r.channel_group_name) === "Group 40 MCH")
    .sort((a,b) => Number(a.testing_point)-Number(b.testing_point))
    .map((r) => ({
      testing_point: r.testing_point,
      is_recommended: Number(r.testing_point) === Number(r.recommended_testing_point) ? 1 : 0,
      recommended_testing_point: r.recommended_testing_point,
      bids: r.bids,
      sold: r.sold,
      win_rate: r.win_rate,
      cpc: r.cpc,
      cpb_uplift: r.cpb_uplift,
      cpc_uplift: r.cpc_uplift,
      additional_clicks: r.additional_clicks,
      expected_bind_change: r.expected_bind_change,
      stat_sig: r.stat_sig,
      stat_sig_source: r.stat_sig_source
    }));

  console.log(JSON.stringify(target, null, 2));
})();
