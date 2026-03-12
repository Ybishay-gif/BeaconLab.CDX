import { Router } from "express";
import type { NextFunction, Request, Response } from "express";
import { z } from "zod";
import { requireRole } from "../../middleware/auth.js";
import { createTarget, getTarget, getTargetsMetrics, listTargets, updateTarget, batchCreateTargets } from "../../services/targetsService.js";
import { appendChangeLog } from "../../services/changeLogService.js";
import { getDefaultTargets, VALID_TARGET_KEYS } from "../../services/defaultTargetsService.js";
import { parseOptionalNumber } from "./queryParsers.js";

const updateTargetSchema = z
  .object({
    state: z.string().optional(),
    segment: z.string().optional(),
    source: z.string().optional(),
    targetValue: z.number().finite().optional(),
    target_cpb: z.number().finite().optional(),
    target_roe: z.number().finite().optional(),
    target_cor: z.number().finite().optional(),
  })
  .refine((value) => Object.values(value).some((entry) => entry !== undefined), {
    message: "At least one field must be provided"
  });

const targetsMetricsSchema = z.object({
  rows: z.array(
    z.object({
      state: z.string(),
      segment: z.string(),
      source: z.string(),
      accountId: z.string().optional()
    })
  )
});

const populateDefaultsSchema = z.object({
  planId: z.string().min(1),
  defaultTargetKey: z.enum(VALID_TARGET_KEYS),
  activityLeadType: z.string().optional(),
});

export const targetsRoutes = Router();

function parsePlanId(query: Request["query"]): string | undefined {
  const raw = typeof query.planId === "string" ? query.planId.trim() : "";
  return raw || undefined;
}

function parseActivityLeadType(query: Request["query"]): string | undefined {
  const raw = typeof query.activityLeadType === "string" ? query.activityLeadType.trim() : "";
  return raw || undefined;
}

function timedRoute(
  name: string,
  handler: (req: Request, res: Response, next: NextFunction) => Promise<void>
) {
  return async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    const started = process.hrtime.bigint();
    let rowCount: number | undefined;
    const originalJson = res.json.bind(res);
    res.json = ((body: unknown) => {
      if (typeof body === "object" && body !== null && "rows" in body) {
        const rows = (body as { rows?: unknown }).rows;
        rowCount = Array.isArray(rows) ? rows.length : undefined;
      }
      return originalJson(body);
    }) as Response["json"];

    try {
      await handler(req, res, next);
    } finally {
      const elapsedMs = Number(process.hrtime.bigint() - started) / 1_000_000;
      const status = res.statusCode;
      const querySummary = {
        planId: parsePlanId(req.query),
        startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
        endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
        activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined,
        qbc: typeof req.query.qbc === "string" ? req.query.qbc : undefined
      };

      console.info(
        JSON.stringify({
          kind: "targets_api_timing",
          endpoint: name,
          method: req.method,
          path: req.originalUrl || req.path,
          status,
          duration_ms: Number(elapsedMs.toFixed(2)),
          row_count: rowCount,
          query: querySummary
        })
      );
    }
  };
}

targetsRoutes.get("/targets", timedRoute("list_targets", async (req, res, next) => {
  try {
    const qbc = parseOptionalNumber(req.query.qbc);
    if (!Number.isFinite(qbc)) {
      res.status(400).json({ error: "qbc is required" });
      return;
    }

    const rawRows = await listTargets({
      planId: parsePlanId(req.query),
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined,
      qbc
    });
    // Map backend fields to frontend Target interface
    const rows = rawRows.map((r) => ({
      target_id: r.target_id,
      state: r.state,
      segment: r.segment,
      source: r.source,
      sold: r.sold ?? 0,
      binds: r.binds ?? 0,
      target_cpb: r.target_value ?? 0,
      current_cpb: r.cpb ?? 0,
      performance: r.performance ?? 0,
      target_roe: 0,
      current_roe: r.roe ?? 0,
      target_cor: r.target_cor ?? 0,
      current_cor: r.combined_ratio ?? 0,
      suggested_max_cpb: r.current_target ?? undefined,
      avg_lifetime_premium: r.avg_lifetime_premium ?? 0,
      avg_lifetime_cost: r.avg_lifetime_cost ?? 0,
      fallback_level: undefined,
    }));
    res.json({ rows });
  } catch (error) {
    next(error);
  }
}));

targetsRoutes.post("/targets", requireRole(["admin", "planner"]), async (req, res, next) => {
  try {
    const planId = parsePlanId(req.query);
    const alt = parseActivityLeadType(req.query);
    const result = await createTarget(req.user!.userId, planId, alt);
    appendChangeLog(
      { userId: req.user!.userId, email: req.user!.email },
      { objectType: "target", objectId: result.targetId, action: "create", after: { targetId: result.targetId, planId } }
    ).catch(console.error);
    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

targetsRoutes.put("/targets/:targetId", requireRole(["admin", "planner"]), async (req, res, next) => {
  try {
    const parsed = updateTargetSchema.parse(req.body);
    const before = await getTarget(req.params.targetId);
    // target_cor is stored in its own column; other fields map to target_value
    const targetValue = parsed.targetValue ?? parsed.target_cpb ?? parsed.target_roe;
    const input = {
      state: parsed.state,
      segment: parsed.segment,
      source: parsed.source,
      targetValue,
      targetCor: parsed.target_cor,
    };
    await updateTarget(req.params.targetId, input, req.user!.userId, parsePlanId(req.query), parseActivityLeadType(req.query));
    const after = await getTarget(req.params.targetId);
    appendChangeLog(
      { userId: req.user!.userId, email: req.user!.email },
      { objectType: "target", objectId: req.params.targetId, action: "update", before, after, metadata: { planId: parsePlanId(req.query), activityLeadType: req.query.activityLeadType || undefined } }
    ).catch(console.error);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

targetsRoutes.post("/targets/metrics", timedRoute("targets_metrics", async (req, res, next) => {
  try {
    const parsed = targetsMetricsSchema.parse(req.body);
    const qbc = parseOptionalNumber(req.query.qbc);
    if (!Number.isFinite(qbc)) {
      res.status(400).json({ error: "qbc is required" });
      return;
    }

    const rows = await getTargetsMetrics(parsed.rows, {
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined,
      qbc
    });
    res.json({ rows });
  } catch (error) {
    next(error);
  }
}));

/* POST /targets/populate-defaults — copy default targets into plan's targets */
targetsRoutes.post("/targets/populate-defaults", requireRole(["admin", "planner"]), async (req, res, next) => {
  try {
    const { planId, defaultTargetKey, activityLeadType } = populateDefaultsSchema.parse(req.body);
    const alt = activityLeadType || defaultTargetKey;
    const defaults = await getDefaultTargets(defaultTargetKey);
    if (defaults.length === 0) {
      res.json({ ok: true, count: 0 });
      return;
    }
    const count = await batchCreateTargets(
      defaults.map((d) => ({
        state: d.state,
        segment: d.segment,
        source: d.source,
        targetValue: d.target_value,
      })),
      req.user!.userId,
      planId,
      alt
    );
    appendChangeLog(
      { userId: req.user!.userId, email: req.user!.email },
      { objectType: "target", objectId: planId, action: "populate_defaults", metadata: { planId, defaultTargetKey, activityLeadType: alt, count } }
    ).catch(console.error);
    res.json({ ok: true, count });
  } catch (error) {
    next(error);
  }
});
