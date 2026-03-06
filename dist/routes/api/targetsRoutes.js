import { Router } from "express";
import { z } from "zod";
import { requireRole } from "../../middleware/auth.js";
import { createTarget, getTargetsMetrics, listTargets, updateTarget } from "../../services/targetsService.js";
import { parseOptionalNumber } from "./queryParsers.js";
const updateTargetSchema = z
    .object({
    state: z.string().optional(),
    segment: z.string().optional(),
    source: z.string().optional(),
    targetValue: z.number().finite().optional()
})
    .refine((value) => Object.values(value).some((entry) => entry !== undefined), {
    message: "At least one field must be provided"
});
const targetsMetricsSchema = z.object({
    rows: z.array(z.object({
        state: z.string(),
        segment: z.string(),
        source: z.string(),
        accountId: z.string().optional()
    }))
});
export const targetsRoutes = Router();
function parsePlanId(query) {
    const raw = typeof query.planId === "string" ? query.planId.trim() : "";
    return raw || undefined;
}
function timedRoute(name, handler) {
    return async (req, res, next) => {
        const started = process.hrtime.bigint();
        let rowCount;
        const originalJson = res.json.bind(res);
        res.json = ((body) => {
            if (typeof body === "object" && body !== null && "rows" in body) {
                const rows = body.rows;
                rowCount = Array.isArray(rows) ? rows.length : undefined;
            }
            return originalJson(body);
        });
        try {
            await handler(req, res, next);
        }
        finally {
            const elapsedMs = Number(process.hrtime.bigint() - started) / 1_000_000;
            const status = res.statusCode;
            const querySummary = {
                planId: parsePlanId(req.query),
                startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
                endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
                activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined,
                qbc: typeof req.query.qbc === "string" ? req.query.qbc : undefined
            };
            console.info(JSON.stringify({
                kind: "targets_api_timing",
                endpoint: name,
                method: req.method,
                path: req.originalUrl || req.path,
                status,
                duration_ms: Number(elapsedMs.toFixed(2)),
                row_count: rowCount,
                query: querySummary
            }));
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
        const rows = await listTargets({
            planId: parsePlanId(req.query),
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
}));
targetsRoutes.post("/targets", requireRole(["admin", "planner"]), async (req, res, next) => {
    try {
        const result = await createTarget(req.user.userId, parsePlanId(req.query));
        res.status(201).json(result);
    }
    catch (error) {
        next(error);
    }
});
targetsRoutes.put("/targets/:targetId", requireRole(["admin", "planner"]), async (req, res, next) => {
    try {
        const parsed = updateTargetSchema.parse(req.body);
        await updateTarget(req.params.targetId, parsed, req.user.userId, parsePlanId(req.query));
        res.json({ ok: true });
    }
    catch (error) {
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
    }
    catch (error) {
        next(error);
    }
}));
