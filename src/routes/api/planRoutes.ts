import { Router } from "express";
import { z } from "zod";
import { requirePermission } from "../../middleware/auth.js";
import {
  appendDecisions,
  clonePlan,
  createPlan,
  createRun,
  deletePlan,
  getParameterValues,
  getPlan,
  getRun,
  getRunResults,
  listPlanParameters,
  listPlans,
  updatePlan,
  upsertParameters,
  upsertPlanContext
} from "../../services/plansService.js";
import { appendChangeLog } from "../../services/changeLogService.js";
import { getPriceExploration } from "../../services/analyticsService.js";
import { buildPlanOutcome } from "../../services/planOutcomeService.js";
import { parseOptionalNumber, parseQueryArray } from "./queryParsers.js";

const planContextSchema = z.object({
  perf_start_date: z.string().optional(),
  perf_end_date: z.string().optional(),
  price_start_date: z.string().optional(),
  price_end_date: z.string().optional(),
  qbc_clicks: z.number().optional(),
  qbc_leads_calls: z.number().optional()
});

const createPlanSchema = z.object({
  planName: z.string().min(1).optional(),
  plan_name: z.string().min(1).optional(),
  description: z.string().optional()
}).merge(planContextSchema).refine(
  (v) => !!(v.planName || v.plan_name),
  { message: "planName or plan_name is required" }
);

const clonePlanSchema = z.object({
  planName: z.string().min(1).optional(),
  description: z.string().optional()
});

const updatePlanSchema = z.object({
  planName: z.string().min(1).optional(),
  plan_name: z.string().min(1).optional(),
  description: z.string().optional()
}).merge(planContextSchema);

const parametersSchema = z.object({
  parameters: z.array(
    z.object({
      key: z.string().min(1),
      value: z.string(),
      valueType: z.enum(["int", "float", "bool", "string", "json"])
    })
  ),
  activityLeadType: z.string().optional(),
});

const decisionsSchema = z.object({
  decisions: z.array(
    z.object({
      decisionType: z.string().min(1),
      decisionValue: z.string().min(1),
      state: z.string().optional(),
      channel: z.string().optional(),
      reason: z.string().optional()
    })
  )
});

export const planRoutes = Router();

planRoutes.get("/me", (req, res) => {
  res.json({ user: req.user });
});

planRoutes.get("/plans", async (_req, res, next) => {
  try {
    const rawPlans = await listPlans();
    // Flatten plan_context_json fields onto each plan for the frontend
    const plans = rawPlans.map((p) => {
      let ctx: Record<string, unknown> = {};
      if (p.plan_context_json) {
        try { ctx = JSON.parse(p.plan_context_json); } catch { /* ignore */ }
      }
      return {
        ...p,
        // Support both new (perfStartDate) and legacy (performanceStartDate) field names
        perf_start_date: ctx.perfStartDate ?? ctx.performanceStartDate ?? "",
        perf_end_date: ctx.perfEndDate ?? ctx.performanceEndDate ?? "",
        price_start_date: ctx.priceStartDate ?? ctx.priceExplorationStartDate ?? "",
        price_end_date: ctx.priceEndDate ?? ctx.priceExplorationEndDate ?? "",
        qbc_clicks: Number(ctx.qbcClicks) || 0,
        qbc_leads_calls: Number(ctx.qbcLeadsCalls) || 0
      };
    });
    res.json({ plans });
  } catch (error) {
    next(error);
  }
});

planRoutes.post("/plans", requirePermission("plan_builder:edit"), async (req, res, next) => {
  try {
    const parsed = createPlanSchema.parse(req.body);
    const planName = (parsed.planName || parsed.plan_name)!;
    const result = await createPlan({
      planName,
      description: parsed.description,
      createdBy: req.user!.userId
    });

    // Persist plan context if any context fields are provided
    if (parsed.perf_start_date || parsed.perf_end_date || parsed.qbc_clicks) {
      await upsertPlanContext(result.planId, req.user!.userId, {
        perfStartDate: parsed.perf_start_date ?? "",
        perfEndDate: parsed.perf_end_date ?? "",
        priceStartDate: parsed.price_start_date ?? "",
        priceEndDate: parsed.price_end_date ?? "",
        qbcClicks: parsed.qbc_clicks ?? 0,
        qbcLeadsCalls: parsed.qbc_leads_calls ?? 0,
      });
    }

    appendChangeLog(
      { userId: req.user!.userId, email: req.user!.email },
      { objectType: "plan", objectId: result.planId, action: "create", after: { planName, description: parsed.description } }
    ).catch(console.error);

    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

planRoutes.post("/plans/:planId/clone", requirePermission("plan_builder:edit"), async (req, res, next) => {
  try {
    const parsed = clonePlanSchema.parse(req.body || {});
    const result = await clonePlan(req.params.planId, req.user!.userId, {
      planName: parsed.planName,
      description: parsed.description
    });
    appendChangeLog(
      { userId: req.user!.userId, email: req.user!.email },
      { objectType: "plan", objectId: result.planId, action: "clone", after: { planName: parsed.planName }, metadata: { sourcePlanId: req.params.planId } }
    ).catch(console.error);
    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

planRoutes.get("/plans/:planId", async (req, res, next) => {
  try {
    const plan = await getPlan(req.params.planId);
    if (!plan) {
      res.status(404).json({ error: "Plan not found" });
      return;
    }
    res.json({ plan });
  } catch (error) {
    next(error);
  }
});

planRoutes.put("/plans/:planId", requirePermission("plan_builder:edit"), async (req, res, next) => {
  try {
    const parsed = updatePlanSchema.parse(req.body || {});
    const planName = parsed.planName || parsed.plan_name;
    const before = await getPlan(req.params.planId);

    // Update plan name/description if provided
    if (planName || parsed.description !== undefined) {
      await updatePlan(req.params.planId, {
        planName,
        description: parsed.description
      });
    }

    // Persist plan context if any context fields are provided
    if (
      parsed.perf_start_date !== undefined ||
      parsed.perf_end_date !== undefined ||
      parsed.qbc_clicks !== undefined
    ) {
      await upsertPlanContext(req.params.planId, req.user!.userId, {
        perfStartDate: parsed.perf_start_date ?? "",
        perfEndDate: parsed.perf_end_date ?? "",
        priceStartDate: parsed.price_start_date ?? "",
        priceEndDate: parsed.price_end_date ?? "",
        qbcClicks: parsed.qbc_clicks ?? 0,
        qbcLeadsCalls: parsed.qbc_leads_calls ?? 0,
      });
    }

    appendChangeLog(
      { userId: req.user!.userId, email: req.user!.email },
      { objectType: "plan", objectId: req.params.planId, action: "update", before: before ? { plan_name: before.plan_name, description: before.description } : undefined, after: { planName, description: parsed.description } }
    ).catch(console.error);

    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

planRoutes.delete("/plans/:planId", requirePermission("plan_builder:edit"), async (req, res, next) => {
  try {
    const before = await getPlan(req.params.planId);
    await deletePlan(req.params.planId);
    appendChangeLog(
      { userId: req.user!.userId, email: req.user!.email },
      { objectType: "plan", objectId: req.params.planId, action: "delete", before: before ? { plan_name: before.plan_name, description: before.description, status: before.status } : undefined }
    ).catch(console.error);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

planRoutes.get("/plans/:planId/parameters", async (req, res, next) => {
  try {
    const rows = await listPlanParameters(req.params.planId);
    const parameters = rows.map((r) => ({
      key: r.param_key,
      value: r.param_value,
      valueType: r.value_type,
    }));
    res.json({ parameters });
  } catch (error) {
    next(error);
  }
});

planRoutes.put("/plans/:planId/parameters", requirePermission("plan_builder:edit"), async (req, res, next) => {
  try {
    const parsed = parametersSchema.parse(req.body);
    const paramKeys = parsed.parameters.map((p) => p.key);
    const beforeValues = await getParameterValues(req.params.planId, paramKeys);
    await upsertParameters(req.params.planId, req.user!.userId, parsed.parameters);
    const afterValues: Record<string, string> = {};
    for (const p of parsed.parameters) afterValues[p.key] = p.value;
    const meta: Record<string, unknown> = { paramKeys };
    if (parsed.activityLeadType) meta.activityLeadType = parsed.activityLeadType;
    appendChangeLog(
      { userId: req.user!.userId, email: req.user!.email },
      { objectType: "plan_parameters", objectId: req.params.planId, action: "update", before: beforeValues, after: afterValues, metadata: meta }
    ).catch(console.error);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

planRoutes.post("/plans/:planId/decisions", requirePermission("plan_builder:edit"), async (req, res, next) => {
  try {
    const parsed = decisionsSchema.parse(req.body);
    const result = await appendDecisions(req.params.planId, req.user!.userId, parsed.decisions);
    appendChangeLog(
      { userId: req.user!.userId, email: req.user!.email },
      { objectType: "plan_decision", objectId: req.params.planId, action: "create", after: parsed.decisions }
    ).catch(console.error);
    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

planRoutes.post("/plans/:planId/runs", requirePermission("plan_builder:edit"), async (req, res, next) => {
  try {
    const result = await createRun(req.params.planId, req.user!.userId);
    res.status(202).json({ ...result, status: "queued" });
  } catch (error) {
    next(error);
  }
});

planRoutes.get("/plans/:planId/runs/:runId", async (req, res, next) => {
  try {
    const run = await getRun(req.params.planId, req.params.runId);
    if (!run) {
      res.status(404).json({ error: "Run not found" });
      return;
    }

    res.json({ run });
  } catch (error) {
    next(error);
  }
});

planRoutes.get("/plans/:planId/runs/:runId/results", async (req, res, next) => {
  try {
    const results = await getRunResults(req.params.planId, req.params.runId);
    res.json({ results });
  } catch (error) {
    next(error);
  }
});

/* ── Plan Outcome — generate grouped outcome from PE recommended TPs ── */

planRoutes.post("/plans/:planId/outcome/generate", requirePermission("plan_builder:edit"), async (req, res, next) => {
  try {
    const planId = req.params.planId;
    const qbc = typeof req.body.qbc === "number" ? req.body.qbc : parseOptionalNumber(req.body.qbc);
    if (!Number.isFinite(qbc)) {
      res.status(400).json({ error: "qbc is required" });
      return;
    }

    const peRows = await getPriceExploration({
      planId,
      startDate: typeof req.body.startDate === "string" ? req.body.startDate : undefined,
      endDate: typeof req.body.endDate === "string" ? req.body.endDate : undefined,
      q2bStartDate: typeof req.body.q2bStartDate === "string" ? req.body.q2bStartDate : undefined,
      q2bEndDate: typeof req.body.q2bEndDate === "string" ? req.body.q2bEndDate : undefined,
      states: Array.isArray(req.body.states) ? req.body.states : [],
      channelGroups: Array.isArray(req.body.channelGroups) ? req.body.channelGroups : [],
      activityLeadType: typeof req.body.activityLeadType === "string" ? req.body.activityLeadType : undefined,
      qbc,
    });

    const outcome = buildPlanOutcome(peRows);

    await upsertParameters(planId, req.user!.userId, [{
      key: "plan_outcome_json",
      value: JSON.stringify(outcome),
      valueType: "json"
    }]);

    appendChangeLog(
      { userId: req.user!.userId, email: req.user!.email },
      {
        objectType: "plan_outcome",
        objectId: planId,
        action: "generate",
        after: { groups: outcome.summary.total_groups, remainder: outcome.summary.total_remainder },
      }
    ).catch(console.error);

    res.json({ outcome });
  } catch (error) {
    next(error);
  }
});
