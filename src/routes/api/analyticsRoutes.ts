import { timingSafeEqual } from "node:crypto";
import { Router } from "express";
import type { Request, Response, NextFunction } from "express";
import {
  getPlanMergedAnalytics,
  getPlansComparison,
  getPriceExploration,
  getStateAnalysis,
  getStateSegmentPerformance,
  getStrategyAnalysis,
  listPlanMergedFilters,
  listPriceExplorationFilters,
  listStateSegmentFilters
} from "../../services/analyticsService.js";
import { listPlans } from "../../services/plansService.js";
import { startSyncInBackground, getSyncStatus } from "../../jobs/syncFromBQ.js";
import { snapshotSuggestedCpb } from "../../jobs/snapshotSuggestedCpb.js";
import { parseOptionalNumber, parseQueryArray } from "./queryParsers.js";
import { resolveQbc } from "../../services/shared/activityScope.js";
import { cacheClear, cacheStats } from "../../cache.js";
import { requireUser } from "../../middleware/auth.js";
import { config } from "../../config.js";
import {
  getCrossTacticSchema,
  getCrossTacticAggregation,
  getCrossTacticComparison,
  INVERSE_MEASURES,
  getFilterValues as getCrossTacticFilterValues,
  type CrossTacticRequest,
  type DrillStep,
  type DynamicFilter,
} from "../../services/crossTacticService.js";
import {
  listPresets,
  createPreset,
  deletePreset,
  type PresetConfig,
} from "../../services/crossTacticPresetsService.js";

export const analyticsRoutes = Router();

// ── Admin endpoints — require auth OR internal scheduler secret ─────────────
export const adminRoutes = Router();

/**
 * Allow requests that present the internal scheduler secret (via X-Scheduler-Secret header)
 * OR authenticated users with admin role.
 *
 * Cloud Scheduler jobs should be configured to send:
 *   X-Scheduler-Secret: <value of SCHEDULER_SECRET env var>
 *
 * This replaces the old spoofable X-CloudScheduler header check.
 */
function requireAdminOrScheduler(req: Request, res: Response, next: NextFunction): void {
  const schedulerSecret = config.schedulerSecret;
  const providedSecret = req.header("x-scheduler-secret");
  if (
    schedulerSecret &&
    providedSecret &&
    schedulerSecret.length === providedSecret.length &&
    timingSafeEqual(Buffer.from(schedulerSecret), Buffer.from(providedSecret))
  ) {
    next();
    return;
  }
  // Fall through to session-based auth + admin role check
  requireUser(req, res, (err?: unknown) => {
    if (err) { next(err); return; }
    if (res.headersSent) return; // requireUser already sent 401
    if (req.user?.role !== "admin") {
      res.status(403).json({ error: "Admin access required" });
      return;
    }
    next();
  });
}

// Cache warming — called by Cloud Scheduler daily to pre-warm the analytics cache.
// Also callable manually by admins.
adminRoutes.post("/admin/warm-cache", requireAdminOrScheduler, async (_req, res) => {
  const start = Date.now();
  try {
    const plans = await listPlans();
    const activePlans = plans.filter((p) => p.status !== "archived");
    const results: Array<{ planId: string; planName: string; ok: boolean; ms: number }> = [];

    for (const plan of activePlans) {
      const planStart = Date.now();
      try {
        // Parse plan_context_config to get dates and qbc
        let ctx: Record<string, unknown> = {};
        try {
          ctx = plan.plan_context_json ? JSON.parse(plan.plan_context_json) : {};
        } catch { /* ignore */ }

        // Support both new and legacy field names
        const perfStartDate = String(ctx.perfStartDate || ctx.performanceStartDate || "");
        const perfEndDate = String(ctx.perfEndDate || ctx.performanceEndDate || "");
        // Derive activityLeadType from stored activity + leadType, or use direct value if present
        const activity = String(ctx.activity || "clicks");
        const leadType = String(ctx.leadType || "auto");
        const activityLeadType = ctx.activityLeadType
          ? String(ctx.activityLeadType)
          : `${activity}_${leadType}`;
        const qbc = resolveQbc(activityLeadType, Number(ctx.qbcClicks) || 0, Number(ctx.qbcLeadsCalls) || 0);

        if (!perfStartDate || !perfEndDate || !qbc) {
          results.push({ planId: plan.plan_id, planName: plan.plan_name, ok: false, ms: 0 });
          continue;
        }

        // Pre-warm the heaviest queries in parallel
        await Promise.all([
          getStateAnalysis({ planId: plan.plan_id, startDate: perfStartDate, endDate: perfEndDate, activityLeadType, qbc }).catch(() => null),
          getStrategyAnalysis({ planId: plan.plan_id, startDate: perfStartDate, endDate: perfEndDate, activityLeadType, qbc }).catch(() => null),
          getStateSegmentPerformance({ startDate: perfStartDate, endDate: perfEndDate, activityLeadType, qbc }).catch(() => null),
          listStateSegmentFilters({ startDate: perfStartDate, endDate: perfEndDate, activityLeadType }).catch(() => null),
          listPriceExplorationFilters({ startDate: perfStartDate, endDate: perfEndDate, activityLeadType }).catch(() => null),
          listPlanMergedFilters({ startDate: perfStartDate, endDate: perfEndDate, activityLeadType }).catch(() => null),
        ]);

        results.push({ planId: plan.plan_id, planName: plan.plan_name, ok: true, ms: Date.now() - planStart });
      } catch {
        results.push({ planId: plan.plan_id, planName: plan.plan_name, ok: false, ms: Date.now() - planStart });
      }
    }

    res.json({
      totalMs: Date.now() - start,
      plans: results
    });
  } catch (error) {
    res.status(500).json({ error: "Cache warming failed", detail: String(error) });
  }
});

// BQ → PG sync — fire-and-forget. Returns 202 immediately.
// The sync streams data in the background — no HTTP timeout dependency.
// Poll GET /admin/sync-status for progress.
adminRoutes.post("/admin/sync-from-bq", requireAdminOrScheduler, (_req, res) => {
  const result = startSyncInBackground();
  res.status(result.started ? 202 : 409).json(result);
});

// Sync progress — returns per-table row counts and completion status.
adminRoutes.get("/admin/sync-status", requireAdminOrScheduler, (_req, res) => {
  res.json(getSyncStatus());
});

// Cache diagnostics
adminRoutes.get("/admin/cache-stats", requireAdminOrScheduler, (_req, res) => {
  res.json(cacheStats());
});

// Manual cache clear
adminRoutes.post("/admin/clear-cache", requireAdminOrScheduler, (_req, res) => {
  cacheClear();
  res.json({ ok: true, message: "Analytics BQ cache cleared" });
});

// Ingest security test results from scheduled tasks (auth-test / pentest).
adminRoutes.post("/admin/security-test-result", requireAdminOrScheduler, async (req, res) => {
  try {
    const { insertSecurityTestResult } = await import("../../services/platformHealthService.js");
    const data = req.body;
    if (!data?.suite || !data?.summary) {
      res.status(400).json({ error: "Missing required fields: suite, summary" });
      return;
    }
    const resultId = await insertSecurityTestResult(data);
    res.status(201).json({ ok: true, resultId });
  } catch (error) {
    res.status(500).json({ error: "Failed to ingest test result", detail: String(error) });
  }
});

// Standalone snapshot of suggested Target CPB → BQ (manual trigger).
adminRoutes.post("/admin/snapshot-suggested-cpb", requireAdminOrScheduler, async (_req, res) => {
  try {
    const result = await snapshotSuggestedCpb();
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: "Snapshot failed", detail: String(error) });
  }
});

analyticsRoutes.get("/analytics/state-segment-performance/filters", async (req, res, next) => {
  try {
    const filters = await listStateSegmentFilters({
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined
    });
    res.json(filters);
  } catch (error) {
    next(error);
  }
});

analyticsRoutes.get("/analytics/state-segment-performance", async (req, res, next) => {
  try {
    const qbc = parseOptionalNumber(req.query.qbc);
    if (!Number.isFinite(qbc)) {
      res.status(400).json({ error: "qbc is required" });
      return;
    }

    const rows = await getStateSegmentPerformance({
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      states: parseQueryArray(req.query.states),
      segments: parseQueryArray(req.query.segments),
      channelGroups: parseQueryArray(req.query.channelGroups),
      activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined,
      groupBy: typeof req.query.groupBy === "string" ? req.query.groupBy : undefined,
      qbc
    });
    res.json({ rows });
  } catch (error) {
    next(error);
  }
});

analyticsRoutes.get("/analytics/price-exploration/filters", async (req, res, next) => {
  try {
    const filters = await listPriceExplorationFilters({
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined
    });
    res.json(filters);
  } catch (error) {
    next(error);
  }
});

analyticsRoutes.get("/analytics/price-exploration", async (req, res, next) => {
  try {
    const qbc = parseOptionalNumber(req.query.qbc);
    if (!Number.isFinite(qbc)) {
      res.status(400).json({ error: "qbc is required" });
      return;
    }
    const limit = parseOptionalNumber(req.query.limit);
    const topPairs = parseOptionalNumber(req.query.topPairs);

    const rows = await getPriceExploration({
      planId: typeof req.query.planId === "string" ? req.query.planId : undefined,
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      q2bStartDate: typeof req.query.q2bStartDate === "string" ? req.query.q2bStartDate : undefined,
      q2bEndDate: typeof req.query.q2bEndDate === "string" ? req.query.q2bEndDate : undefined,
      states: parseQueryArray(req.query.states),
      channelGroups: parseQueryArray(req.query.channelGroups),
      activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined,
      qbc,
      limit,
      topPairs
    });
    res.json({ rows });
  } catch (error) {
    next(error);
  }
});

analyticsRoutes.get("/analytics/plan-merged/filters", async (req, res, next) => {
  try {
    const filters = await listPlanMergedFilters({
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined
    });
    res.json(filters);
  } catch (error) {
    next(error);
  }
});

analyticsRoutes.get("/analytics/plan-merged", async (req, res, next) => {
  try {
    const rows = await getPlanMergedAnalytics({
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      states: parseQueryArray(req.query.states),
      segments: parseQueryArray(req.query.segments),
      channelGroups: parseQueryArray(req.query.channelGroups),
      testingPoints: parseQueryArray(req.query.testingPoints),
      statSig: parseQueryArray(req.query.statSig),
      activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined
    });
    res.json({ rows });
  } catch (error) {
    next(error);
  }
});

analyticsRoutes.get("/analytics/plans-comparison", async (req, res, next) => {
  try {
    const mode = req.query.mode === "activity" ? "activity" : "plans";
    const planId = typeof req.query.planId === "string" ? req.query.planId.trim() : undefined;
    const startDate = typeof req.query.startDate === "string" ? req.query.startDate : undefined;
    const endDate = typeof req.query.endDate === "string" ? req.query.endDate : undefined;

    const plans = await listPlans();
    const rows = await getPlansComparison({ mode, planId, startDate, endDate, plans });
    res.json({ rows });
  } catch (error) {
    next(error);
  }
});

analyticsRoutes.get("/analytics/strategy-analysis", async (req, res, next) => {
  try {
    const planId = typeof req.query.planId === "string" ? req.query.planId.trim() : "";
    const qbc = parseOptionalNumber(req.query.qbc);
    if (!Number.isFinite(qbc)) {
      res.status(400).json({ error: "qbc is required" });
      return;
    }

    if (!planId) {
      res.status(400).json({ error: "planId is required" });
      return;
    }

    const rows = await getStrategyAnalysis({
      planId,
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined,
      qbc
    });
    res.json({ rows });
  } catch (error) {
    next(error);
  }
});

analyticsRoutes.get("/analytics/state-analysis", async (req, res, next) => {
  try {
    const planId = typeof req.query.planId === "string" ? req.query.planId.trim() : "";
    const qbc = parseOptionalNumber(req.query.qbc);
    if (!Number.isFinite(qbc)) {
      res.status(400).json({ error: "qbc is required" });
      return;
    }

    if (!planId) {
      res.status(400).json({ error: "planId is required" });
      return;
    }

    const payload = await getStateAnalysis({
      planId,
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined,
      qbc
    });
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

// ── Cross Tactic Analytics Explorer ─────────────────────────────────

analyticsRoutes.get("/analytics/cross-tactic/schema", async (_req, res, next) => {
  try {
    const schema = await getCrossTacticSchema();
    res.json(schema);
  } catch (error) {
    next(error);
  }
});

analyticsRoutes.get("/analytics/cross-tactic/filter-values", async (req, res, next) => {
  try {
    const column = typeof req.query.column === "string" ? req.query.column : "";
    if (!column) {
      res.status(400).json({ error: "column query param is required" });
      return;
    }
    const values = await getCrossTacticFilterValues(column);
    res.json({ values });
  } catch (error) {
    next(error);
  }
});

/** Parse the shared cross-tactic request body */
function parseCrossTacticBody(body: Partial<CrossTacticRequest>) {
  const dimensions = Array.isArray(body.dimensions) ? body.dimensions : [];
  const metrics = Array.isArray(body.metrics) ? body.metrics : ["opps", "bids", "sold", "total_cost"];
  const filters: Record<string, string[]> = typeof body.filters === "object" && body.filters ? body.filters : {};
  const dynamicFilters: DynamicFilter[] = Array.isArray(body.dynamicFilters) ? body.dynamicFilters : [];
  const startDate = typeof body.startDate === "string" ? body.startDate : "";
  const endDate = typeof body.endDate === "string" ? body.endDate : "";
  const drillPath: DrillStep[] = Array.isArray(body.drillPath) ? body.drillPath : [];
  const qbc = typeof body.qbc === "number" ? body.qbc : 0;
  const qbcClicks = typeof body.qbcClicks === "number" ? body.qbcClicks : undefined;
  const qbcLeadsCalls = typeof body.qbcLeadsCalls === "number" ? body.qbcLeadsCalls : undefined;
  const compareStartDate = typeof body.compareStartDate === "string" ? body.compareStartDate : undefined;
  const compareEndDate = typeof body.compareEndDate === "string" ? body.compareEndDate : undefined;
  return { dimensions, metrics, filters, dynamicFilters, startDate, endDate, drillPath, qbc, qbcClicks, qbcLeadsCalls, compareStartDate, compareEndDate };
}

analyticsRoutes.post("/analytics/cross-tactic", async (req, res, next) => {
  try {
    const parsed = parseCrossTacticBody(req.body as Partial<CrossTacticRequest>);

    if (!parsed.dimensions.length) {
      res.status(400).json({ error: "dimensions array is required (1-10 items)" });
      return;
    }
    if (!parsed.startDate || !parsed.endDate) {
      res.status(400).json({ error: "startDate and endDate are required" });
      return;
    }

    const result = await getCrossTacticAggregation(parsed);
    res.json(result);
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("Invalid")) {
      res.status(400).json({ error: error.message });
      return;
    }
    next(error);
  }
});

analyticsRoutes.post("/analytics/cross-tactic/compare", async (req, res, next) => {
  try {
    const parsed = parseCrossTacticBody(req.body as Partial<CrossTacticRequest>);

    if (!parsed.dimensions.length) {
      res.status(400).json({ error: "dimensions array is required" });
      return;
    }
    if (!parsed.startDate || !parsed.endDate || !parsed.compareStartDate || !parsed.compareEndDate) {
      res.status(400).json({ error: "startDate, endDate, compareStartDate, and compareEndDate are required" });
      return;
    }

    const result = await getCrossTacticComparison(parsed);
    res.json({ ...result, inverseMeasures: [...INVERSE_MEASURES] });
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("Invalid")) {
      res.status(400).json({ error: error.message });
      return;
    }
    next(error);
  }
});

// ── Presets ──

analyticsRoutes.get("/analytics/cross-tactic/presets", async (req, res, next) => {
  try {
    const userId = req.user?.userId;
    if (!userId) { res.status(401).json({ error: "Not authenticated" }); return; }
    const presets = await listPresets(userId);
    res.json({ presets });
  } catch (error) { next(error); }
});

analyticsRoutes.post("/analytics/cross-tactic/presets", async (req, res, next) => {
  try {
    const userId = req.user?.userId;
    if (!userId) { res.status(401).json({ error: "Not authenticated" }); return; }
    const { presetName, config } = req.body as { presetName: string; config: PresetConfig };
    if (!presetName || !config) { res.status(400).json({ error: "presetName and config are required" }); return; }
    const result = await createPreset(userId, presetName, config);
    res.json(result);
  } catch (error) { next(error); }
});

analyticsRoutes.delete("/analytics/cross-tactic/presets/:id", async (req, res, next) => {
  try {
    await deletePreset(req.params.id);
    res.json({ ok: true });
  } catch (error) { next(error); }
});
