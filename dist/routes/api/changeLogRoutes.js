import { Router } from "express";
import { z } from "zod";
import { appendChangeLog, listChangeLogs } from "../../services/changeLogService.js";
const changeLogCreateSchema = z.object({
    objectType: z.string().min(1),
    objectId: z.string().optional(),
    action: z.string().min(1),
    before: z.unknown().optional(),
    after: z.unknown().optional(),
    metadata: z.unknown().optional(),
    module: z.string().optional()
});
export const changeLogRoutes = Router();
changeLogRoutes.get("/change-log", async (req, res, next) => {
    try {
        const rawLimit = typeof req.query.limit === "string" ? Number(req.query.limit.trim()) : 200;
        const objectType = typeof req.query.objectType === "string" ? req.query.objectType.trim() : undefined;
        const objectId = typeof req.query.objectId === "string" ? req.query.objectId.trim() : undefined;
        const userId = typeof req.query.userId === "string" ? req.query.userId.trim() : undefined;
        const module = typeof req.query.module === "string" ? req.query.module.trim() : undefined;
        const rows = await listChangeLogs({
            limit: Number.isFinite(rawLimit) ? rawLimit : 200,
            objectType: objectType || undefined,
            objectId: objectId || undefined,
            userId: userId || undefined,
            module: module || undefined,
        });
        res.json({ rows });
    }
    catch (error) {
        next(error);
    }
});
changeLogRoutes.post("/change-log", async (req, res, next) => {
    try {
        const parsed = changeLogCreateSchema.parse(req.body);
        const result = await appendChangeLog({
            userId: req.user.userId,
            email: req.user.email
        }, {
            objectType: parsed.objectType,
            objectId: parsed.objectId,
            action: parsed.action,
            before: parsed.before,
            after: parsed.after,
            metadata: parsed.metadata,
            module: parsed.module
        });
        res.status(201).json(result);
    }
    catch (error) {
        next(error);
    }
});
