import { Router } from "express";
import { z } from "zod";
import { appendChangeLog, listChangeLogs } from "../../services/changeLogService.js";

const changeLogCreateSchema = z.object({
  objectType: z.string().min(1),
  objectId: z.string().optional(),
  action: z.string().min(1),
  before: z.unknown().optional(),
  after: z.unknown().optional(),
  metadata: z.unknown().optional()
});

export const changeLogRoutes = Router();

changeLogRoutes.get("/change-log", async (req, res, next) => {
  try {
    const rawLimit = typeof req.query.limit === "string" ? Number(req.query.limit.trim()) : 200;
    const rows = await listChangeLogs(Number.isFinite(rawLimit) ? rawLimit : 200);
    res.json({ rows });
  } catch (error) {
    next(error);
  }
});

changeLogRoutes.post("/change-log", async (req, res, next) => {
  try {
    const parsed = changeLogCreateSchema.parse(req.body);
    const result = await appendChangeLog(
      {
        userId: req.user!.userId,
        email: req.user!.email
      },
      {
        objectType: parsed.objectType,
        objectId: parsed.objectId,
        action: parsed.action,
        before: parsed.before,
        after: parsed.after,
        metadata: parsed.metadata
      }
    );
    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});
