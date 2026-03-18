import { Router } from "express";
import { z } from "zod";
import { requirePermission } from "../../middleware/auth.js";
import {
  getChannelParamFilters,
  getChannelParams,
  updateChannelParam,
  updateChannelParamAll,
  type ChannelParamValues,
} from "../../services/channelParamService.js";
import { appendChangeLog } from "../../services/changeLogService.js";

const valuesSchema = z.object({
  // ROE
  roe_poor: z.number().finite(),
  roe_minimal: z.number().finite(),
  roe_0: z.number().finite(),
  roe_good: z.number().finite(),
  roe_excellent: z.number().finite(),
  roe_amazing: z.number().finite(),
  // Performance
  per_poor: z.number().finite(),
  per_minimal: z.number().finite(),
  per_good: z.number().finite(),
  per_excellent: z.number().finite(),
  per_amazing: z.number().finite(),
  // Win Rate
  Poor_WR: z.number().finite(),
  Low_WR: z.number().finite(),
  OK_WR: z.number().finite(),
  High_WR: z.number().finite(),
  VHigh_WR: z.number().finite(),
  // Quote Rate
  QuoteRate_poor: z.number().finite(),
  QuoteRate_minimal: z.number().finite(),
  QuoteRate_good: z.number().finite(),
  QuoteRate_excellent: z.number().finite(),
  QuoteRate_amazing: z.number().finite(),
  // Date Ranges
  early_funnel_start_days: z.number().int(),
  early_funnel_end_days: z.number().int(),
  early_cmp_funnel_start_days: z.number().int(),
  early_cmp_funnel_end_days: z.number().int(),
  perf_start_days: z.number().int(),
  perf_end_days: z.number().int(),
  // Cost
  QBC: z.number().int(),
  minimal_cost: z.number().int(),
  mid_cost: z.number().int(),
  high_cost: z.number().int(),
  vhigh_cost: z.number().int(),
});

const saveSchema = z.object({
  tactic: z.string().min(1),
  vertical: z.string().min(1),
  segment: z.string().min(1), // "__ALL__" for bulk
  values: valuesSchema,
});

/** Map BQ tactic/vertical to the activityLeadType key used by audit log */
function deriveActivityLeadType(tactic: string, vertical: string): string | undefined {
  const actMap: Record<string, string> = { Click: "clicks", Lead: "leads", Call: "calls" };
  const ltMap: Record<string, string> = { CAR_INSURANCE_LEAD: "auto", HOME_INSURANCE_LEAD: "home" };
  const act = actMap[tactic];
  const lt = ltMap[vertical];
  if (act && lt) return `${act}_${lt}`;
  return undefined;
}

/** Human-readable labels for channel_param field keys */
const CHANNEL_PARAM_FIELD_LABELS: Record<string, string> = {
  roe_poor: "ROE Poor",
  roe_minimal: "ROE Minimal",
  roe_0: "ROE 0",
  roe_good: "ROE Good",
  roe_excellent: "ROE Excellent",
  roe_amazing: "ROE Amazing",
  per_poor: "Performance Poor",
  per_minimal: "Performance Minimal",
  per_good: "Performance Good",
  per_excellent: "Performance Excellent",
  per_amazing: "Performance Amazing",
  Poor_WR: "Win Rate Poor",
  Low_WR: "Win Rate Low",
  OK_WR: "Win Rate OK",
  High_WR: "Win Rate High",
  VHigh_WR: "Win Rate VHigh",
  QuoteRate_poor: "Quote Rate Poor",
  QuoteRate_minimal: "Quote Rate Minimal",
  QuoteRate_good: "Quote Rate Good",
  QuoteRate_excellent: "Quote Rate Excellent",
  QuoteRate_amazing: "Quote Rate Amazing",
  early_funnel_start_days: "Undeveloped Period Start",
  early_funnel_end_days: "Undeveloped Period End",
  early_cmp_funnel_start_days: "Compared Date Start",
  early_cmp_funnel_end_days: "Compared Date End",
  perf_start_days: "Developed Period Start",
  perf_end_days: "Developed Period End",
  QBC: "QBC",
  minimal_cost: "Minimal Cost",
  mid_cost: "Mid Cost",
  high_cost: "High Cost",
  vhigh_cost: "Very High Cost",
};

export const channelParamRoutes = Router();

/* GET /channel-params/filters?tactic=...&vertical=... */
channelParamRoutes.get("/channel-params/filters", async (req, res, next) => {
  try {
    const tactic = req.query.tactic ? String(req.query.tactic) : undefined;
    const vertical = req.query.vertical ? String(req.query.vertical) : undefined;
    const filters = await getChannelParamFilters(tactic, vertical);
    res.json(filters);
  } catch (err) {
    next(err);
  }
});

/* GET /channel-params?tactic=...&vertical=...&segment=... */
channelParamRoutes.get("/channel-params", async (req, res, next) => {
  try {
    const tactic = String(req.query.tactic || "");
    const vertical = String(req.query.vertical || "");
    if (!tactic || !vertical) {
      res.status(400).json({ error: "tactic and vertical are required" });
      return;
    }
    const segment = req.query.segment ? String(req.query.segment) : undefined;
    const rows = await getChannelParams(tactic, vertical, segment);
    res.json({ rows });
  } catch (err) {
    next(err);
  }
});

/* PUT /channel-params — save (admin/planner only) */
channelParamRoutes.put(
  "/channel-params",
  requirePermission("channel_recommendations:edit"),
  async (req, res, next) => {
    try {
      const { tactic, vertical, segment, values } = saveSchema.parse(req.body);

      // Fetch current values before update (for audit diff)
      const beforeRows = await getChannelParams(tactic, vertical, segment === "__ALL__" ? undefined : segment);
      const before: Partial<ChannelParamValues> = beforeRows[0] ?? {};

      if (segment === "__ALL__") {
        await updateChannelParamAll(tactic, vertical, values);
      } else {
        await updateChannelParam(tactic, vertical, segment, values);
      }

      // Log each changed field as a separate audit row
      const user = { userId: req.user!.userId, email: req.user!.email };
      const objectId = `${tactic}/${vertical}/${segment}`;
      const activityLeadType = deriveActivityLeadType(tactic, vertical);
      const changedFields: string[] = [];

      for (const key of Object.keys(values) as (keyof ChannelParamValues)[]) {
        const oldVal = Number(before[key]);
        const newVal = Number(values[key]);
        if (oldVal !== newVal) {
          changedFields.push(key);
          const fieldLabel = CHANNEL_PARAM_FIELD_LABELS[key] ?? key;
          appendChangeLog(user, {
            objectType: "channel_param",
            objectId,
            action: "update_field",
            before: { field: key, fieldLabel, value: oldVal },
            after: { field: key, fieldLabel, value: newVal },
            metadata: { tactic, vertical, segment, activityLeadType },
            module: "channel_recommendations",
          }).catch(console.error);
        }
      }

      res.json({
        ok: true,
        mode: segment === "__ALL__" ? "all_segments" : "single_segment",
        changedFields: changedFields.length,
      });
    } catch (err) {
      next(err);
    }
  }
);
