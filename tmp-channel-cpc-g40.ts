import "dotenv/config";
import { getPriceExploration } from "./src/services/analyticsService.js";

(async () => {
  const rows = await getPriceExploration({
    planId: "089bd324-1dd9-47cf-8704-d590123860bd",
    startDate: "2026-01-01",
    endDate: "2026-01-31",
    q2bStartDate: "2026-01-01",
    q2bEndDate: "2026-01-31",
    activityLeadType: "clicks_auto",
    limit: 200000
  });

  const ch = rows.filter(r => String(r.channel_group_name) === "Group 40 MCH" && (Number(r.testing_point) === 0 || Number(r.testing_point) === 10));

  const byTp = new Map<number, {sold:number; spend:number}>();
  for (const r of ch) {
    const tp = Number(r.testing_point);
    const sold = Number(r.sold) || 0;
    const cpc = Number(r.cpc);
    const spend = Number.isFinite(cpc) ? sold * cpc : 0;
    const cur = byTp.get(tp) || { sold: 0, spend: 0 };
    cur.sold += sold;
    cur.spend += spend;
    byTp.set(tp, cur);
  }

  const base = byTp.get(0) || {sold:0, spend:0};
  const t10 = byTp.get(10) || {sold:0, spend:0};
  const baseCpc = base.sold > 0 ? base.spend / base.sold : null;
  const t10Cpc = t10.sold > 0 ? t10.spend / t10.sold : null;
  const uplift = (baseCpc && t10Cpc) ? (t10Cpc - baseCpc) / baseCpc : null;

  console.log(JSON.stringify({
    channel_group: "Group 40 MCH",
    baseline: { sold: base.sold, spend: base.spend, cpc: baseCpc },
    tp10: { sold: t10.sold, spend: t10.spend, cpc: t10Cpc },
    cpc_uplift_channel: uplift
  }, null, 2));
})();
