import { Router } from "express";
import { getPlanMergedAnalytics, getPlansComparison, getPriceExploration, getStateAnalysis, getStateSegmentPerformance, getStrategyAnalysis, listPlanMergedFilters, listPriceExplorationFilters, listStateSegmentFilters } from "../../services/analyticsService.js";
import { listPlans } from "../../services/plansService.js";
import { syncAllFromBQ } from "../../jobs/syncFromBQ.js";
import { snapshotSuggestedCpb } from "../../jobs/snapshotSuggestedCpb.js";
import { parseOptionalNumber, parseQueryArray } from "./queryParsers.js";
import { cacheClear, cacheStats } from "../../cache.js";
import { requireUser } from "../../middleware/auth.js";
import { config } from "../../config.js";
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
function requireAdminOrScheduler(req, res, next) {
    const schedulerSecret = config.schedulerSecret;
    const providedSecret = req.header("x-scheduler-secret");
    if (schedulerSecret && providedSecret && schedulerSecret === providedSecret) {
        next();
        return;
    }
    // Fall through to session-based auth + admin role check
    requireUser(req, res, (err) => {
        if (err) {
            next(err);
            return;
        }
        if (res.headersSent)
            return; // requireUser already sent 401
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
        const results = [];
        for (const plan of activePlans) {
            const planStart = Date.now();
            try {
                // Parse plan_context_config to get dates and qbc
                let ctx = {};
                try {
                    ctx = plan.plan_context_json ? JSON.parse(plan.plan_context_json) : {};
                }
                catch { /* ignore */ }
                // Support both new and legacy field names
                const perfStartDate = String(ctx.perfStartDate || ctx.performanceStartDate || "");
                const perfEndDate = String(ctx.perfEndDate || ctx.performanceEndDate || "");
                const qbc = Number(ctx.qbcClicks) || 0;
                // Derive activityLeadType from stored activity + leadType, or use direct value if present
                const activity = String(ctx.activity || "clicks");
                const leadType = String(ctx.leadType || "auto");
                const activityLeadType = ctx.activityLeadType
                    ? String(ctx.activityLeadType)
                    : `${activity}_${leadType}`;
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
            }
            catch {
                results.push({ planId: plan.plan_id, planName: plan.plan_name, ok: false, ms: Date.now() - planStart });
            }
        }
        res.json({
            totalMs: Date.now() - start,
            plans: results
        });
    }
    catch (error) {
        res.status(500).json({ error: "Cache warming failed", detail: String(error) });
    }
});
// BQ → PG sync — called by Cloud Scheduler daily (or manually by admins).
// Also triggers the suggested-CPB snapshot after sync completes.
// Clears the analytics BQ cache so next request gets fresh data.
adminRoutes.post("/admin/sync-from-bq", requireAdminOrScheduler, async (_req, res) => {
    try {
        const syncResult = await syncAllFromBQ();
        // Chain: snapshot suggested CPB into BQ after fresh perf data is available
        const snapshotResult = await snapshotSuggestedCpb();
        // Clear analytics BQ cache — fresh data now available
        cacheClear();
        res.json({ ...syncResult, suggestedCpbSnapshot: snapshotResult, cacheCleared: true });
    }
    catch (error) {
        res.status(500).json({ error: "Sync failed", detail: String(error) });
    }
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
// Standalone snapshot of suggested Target CPB → BQ (manual trigger).
adminRoutes.post("/admin/snapshot-suggested-cpb", requireAdminOrScheduler, async (_req, res) => {
    try {
        const result = await snapshotSuggestedCpb();
        res.json(result);
    }
    catch (error) {
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
    }
    catch (error) {
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
    }
    catch (error) {
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
    }
    catch (error) {
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
    }
    catch (error) {
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
    }
    catch (error) {
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
    }
    catch (error) {
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
    }
    catch (error) {
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
    }
    catch (error) {
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
    }
    catch (error) {
        next(error);
    }
});
