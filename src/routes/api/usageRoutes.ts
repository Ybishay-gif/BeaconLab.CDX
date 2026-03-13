import { Router } from "express";
import { z } from "zod";
import { requirePermission } from "../../middleware/auth.js";
import { recordUsageEvents, queryUsageEvents } from "../../services/usageService.js";

const eventSchema = z.object({
  sessionId: z.string().min(1),
  eventType: z.enum(["login", "page_visit", "view_change", "interaction"]),
  page: z.string().optional(),
  action: z.string().optional(),
  metadata: z.record(z.unknown()).optional(),
  module: z.string().optional(),
});

const trackSchema = z.object({
  events: z.array(eventSchema).min(1).max(50),
});

export const usageRoutes = Router();

// POST /api/usage/track — receives batched events from the frontend
usageRoutes.post("/usage/track", async (req, res, next) => {
  try {
    const { events } = trackSchema.parse(req.body);
    const user = req.user!;
    // Fire-and-forget: respond immediately, write in background
    recordUsageEvents(user.userId, user.email, events).catch((err) =>
      console.error("Usage tracking error:", err)
    );
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

// GET /api/usage/events — admin-only query endpoint
usageRoutes.get("/usage/events", requirePermission("usage_analytics:view"), async (req, res, next) => {
  try {
    const result = await queryUsageEvents({
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      userId: typeof req.query.userId === "string" ? req.query.userId : undefined,
      eventType: typeof req.query.eventType === "string" ? req.query.eventType : undefined,
      page: typeof req.query.page === "string" ? req.query.page : undefined,
      module: typeof req.query.module === "string" ? req.query.module : undefined,
      limit: req.query.limit ? Number(req.query.limit) : undefined,
      offset: req.query.offset ? Number(req.query.offset) : undefined,
    });
    res.json(result);
  } catch (error) {
    next(error);
  }
});
