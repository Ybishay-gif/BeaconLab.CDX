import { Router } from "express";
import { z } from "zod";
import { requireRole } from "../../middleware/auth.js";
import {
  appendDecisions,
  clonePlan,
  createPlan,
  createRun,
  deletePlan,
  getPlan,
  getRun,
  getRunResults,
  listPlanParameters,
  listPlans,
  updatePlan,
  upsertParameters
} from "../../services/plansService.js";

const createPlanSchema = z.object({
  planName: z.string().min(1),
  description: z.string().optional()
});

const clonePlanSchema = z.object({
  planName: z.string().min(1).optional(),
  description: z.string().optional()
});

const updatePlanSchema = z
  .object({
    planName: z.string().min(1).optional(),
    description: z.string().optional()
  })
  .refine((value) => value.planName !== undefined || value.description !== undefined, {
    message: "At least one field must be provided"
  });

const parametersSchema = z.object({
  parameters: z.array(
    z.object({
      key: z.string().min(1),
      value: z.string(),
      valueType: z.enum(["int", "float", "bool", "string", "json"])
    })
  )
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
    const plans = await listPlans();
    res.json({ plans });
  } catch (error) {
    next(error);
  }
});

planRoutes.post("/plans", requireRole(["admin", "planner"]), async (req, res, next) => {
  try {
    const parsed = createPlanSchema.parse(req.body);
    const result = await createPlan({
      planName: parsed.planName,
      description: parsed.description,
      createdBy: req.user!.userId
    });
    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

planRoutes.post("/plans/:planId/clone", requireRole(["admin", "planner"]), async (req, res, next) => {
  try {
    const parsed = clonePlanSchema.parse(req.body || {});
    const result = await clonePlan(req.params.planId, req.user!.userId, {
      planName: parsed.planName,
      description: parsed.description
    });
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

planRoutes.put("/plans/:planId", requireRole(["admin", "planner"]), async (req, res, next) => {
  try {
    const parsed = updatePlanSchema.parse(req.body || {});
    await updatePlan(req.params.planId, {
      planName: parsed.planName,
      description: parsed.description
    });
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

planRoutes.delete("/plans/:planId", requireRole(["admin", "planner"]), async (req, res, next) => {
  try {
    await deletePlan(req.params.planId);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

planRoutes.get("/plans/:planId/parameters", async (req, res, next) => {
  try {
    const parameters = await listPlanParameters(req.params.planId);
    res.json({ parameters });
  } catch (error) {
    next(error);
  }
});

planRoutes.put("/plans/:planId/parameters", requireRole(["admin", "planner"]), async (req, res, next) => {
  try {
    const parsed = parametersSchema.parse(req.body);
    await upsertParameters(req.params.planId, req.user!.userId, parsed.parameters);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

planRoutes.post("/plans/:planId/decisions", requireRole(["admin", "planner"]), async (req, res, next) => {
  try {
    const parsed = decisionsSchema.parse(req.body);
    const result = await appendDecisions(req.params.planId, req.user!.userId, parsed.decisions);
    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

planRoutes.post("/plans/:planId/runs", requireRole(["admin", "planner"]), async (req, res, next) => {
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
