import { Router } from "express";
import { z } from "zod";
import { requireRole } from "../../middleware/auth.js";
import {
  VALID_TARGET_KEYS,
  getDefaultTargetsSummary,
  getDefaultTargets,
  setDefaultTargets,
  clearDefaultTargets,
  updateDefaultTargetValue,
} from "../../services/defaultTargetsService.js";
import { listDefaultTargetsWithPerf } from "../../services/targetsService.js";
import { appendChangeLog } from "../../services/changeLogService.js";
import { parseOptionalNumber } from "./queryParsers.js";

const keySchema = z.enum(VALID_TARGET_KEYS);

const rowsSchema = z.object({
  rows: z
    .array(
      z.object({
        state: z.string().min(1),
        segment: z.string().min(1),
        source: z.string().min(1),
        target_value: z.number().finite(),
        account_id: z.number().int().optional().default(0),
        company_id: z.number().int().optional().default(0),
        original_id: z.string().optional().default(""),
        segment_name: z.string().optional().default(""),
        attributes: z.string().optional().default(""),
      })
    )
    .min(1)
    .max(50_000),
});

export const defaultTargetsRoutes = Router();

/* GET /default-targets — summary of all keys */
defaultTargetsRoutes.get("/default-targets", async (_req, res, next) => {
  try {
    const summary = await getDefaultTargetsSummary();
    res.json({ summary });
  } catch (err) {
    next(err);
  }
});

/* GET /default-targets/:key — rows for a specific key */
defaultTargetsRoutes.get("/default-targets/:key", async (req, res, next) => {
  try {
    const key = keySchema.parse(req.params.key);
    const rows = await getDefaultTargets(key);
    res.json({ rows, count: rows.length });
  } catch (err) {
    next(err);
  }
});

/* PUT /default-targets/:key — replace rows (admin/planner) */
defaultTargetsRoutes.put(
  "/default-targets/:key",
  requireRole(["admin", "planner"]),
  async (req, res, next) => {
    try {
      const key = keySchema.parse(req.params.key);
      const { rows } = rowsSchema.parse(req.body);
      const currentRows = await getDefaultTargets(key);
      const result = await setDefaultTargets(key, rows);
      appendChangeLog(
        { userId: req.user!.userId, email: req.user!.email },
        { objectType: "default_targets", objectId: key, action: "update", before: { count: currentRows.length }, after: { count: result.count }, metadata: { key, activityLeadType: key, planId: req.query.planId || undefined } }
      ).catch(console.error);
      res.json({ ok: true, count: result.count });
    } catch (err) {
      next(err);
    }
  }
);

/* GET /default-targets/:key/perf — rows with BQ perf data appended */
defaultTargetsRoutes.get("/default-targets/:key/perf", async (req, res, next) => {
  try {
    const key = keySchema.parse(req.params.key);
    const qbc = parseOptionalNumber(req.query.qbc);
    if (!Number.isFinite(qbc)) {
      res.status(400).json({ error: "qbc is required" });
      return;
    }

    const rawRows = await listDefaultTargetsWithPerf(key, {
      planId: typeof req.query.planId === "string" ? req.query.planId.trim() : undefined,
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      activityLeadType: key,
      qbc,
    });

    // Map to frontend Target interface
    const rows = rawRows.map((r) => ({
      target_id: String(r.target_id),
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
      account_id: r.account_id ?? 0,
      company_id: r.company_id ?? 0,
      original_id: r.original_id ?? "",
      segment_name: r.segment_name ?? "",
      attributes: r.attributes ?? "",
    }));

    res.json({ rows });
  } catch (err) {
    console.error("[default-targets/:key/perf] ERROR:", err);
    next(err);
  }
});

/* PUT /default-targets/:key/:id — update single row value (admin/planner) */
defaultTargetsRoutes.put(
  "/default-targets/:key/:id",
  requireRole(["admin", "planner"]),
  async (req, res, next) => {
    try {
      const key = keySchema.parse(req.params.key);
      const id = Number(req.params.id);
      if (!Number.isFinite(id) || id <= 0) {
        res.status(400).json({ error: "Invalid id" });
        return;
      }
      const { target_value } = z.object({ target_value: z.number().finite() }).parse(req.body);
      await updateDefaultTargetValue(key, id, target_value);
      appendChangeLog(
        { userId: req.user!.userId, email: req.user!.email },
        { objectType: "default_targets", objectId: `${key}/${id}`, action: "update_value", metadata: { key, id, target_value } }
      ).catch(console.error);
      res.json({ ok: true });
    } catch (err) {
      next(err);
    }
  }
);

/* DELETE /default-targets/:key — clear rows (admin/planner) */
defaultTargetsRoutes.delete(
  "/default-targets/:key",
  requireRole(["admin", "planner"]),
  async (req, res, next) => {
    try {
      const key = keySchema.parse(req.params.key);
      const currentRows = await getDefaultTargets(key);
      await clearDefaultTargets(key);
      appendChangeLog(
        { userId: req.user!.userId, email: req.user!.email },
        { objectType: "default_targets", objectId: key, action: "delete", before: { count: currentRows.length }, metadata: { key } }
      ).catch(console.error);
      res.json({ ok: true });
    } catch (err) {
      next(err);
    }
  }
);
