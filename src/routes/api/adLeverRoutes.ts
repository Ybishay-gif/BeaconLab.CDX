import { Router } from "express";
import type { Request, Response, NextFunction } from "express";
import {
  getAdLeverData,
  saveAdLeverOverrides,
  saveRetentionData,
  resetRetentionData,
  getDefaultRetentionData,
} from "../../services/adLeverService.js";
import { parseOptionalNumber } from "./queryParsers.js";

export const adLeverRoutes = Router();

// GET /analytics/ad-levers — main lever calculation endpoint
adLeverRoutes.get("/analytics/ad-levers", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const planId = typeof req.query.planId === "string" ? req.query.planId : "";
    if (!planId) {
      res.status(400).json({ error: "planId is required" });
      return;
    }

    const rows = await getAdLeverData({
      planId,
      startDate: typeof req.query.startDate === "string" ? req.query.startDate : undefined,
      endDate: typeof req.query.endDate === "string" ? req.query.endDate : undefined,
      activityLeadType: typeof req.query.activityLeadType === "string" ? req.query.activityLeadType : undefined,
      qbc: parseOptionalNumber(req.query.qbc) ?? 0,
    });

    res.json({ rows });
  } catch (error) {
    next(error);
  }
});

// PUT /analytics/ad-levers/overrides — save lever overrides
adLeverRoutes.put("/analytics/ad-levers/overrides", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { planId, overrides } = req.body || {};
    if (!planId || typeof planId !== "string") {
      res.status(400).json({ error: "planId is required" });
      return;
    }
    if (!overrides || typeof overrides !== "object") {
      res.status(400).json({ error: "overrides object is required" });
      return;
    }

    const userId = (req as any).user?.id || "system";
    await saveAdLeverOverrides(planId, userId, overrides);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

// POST /analytics/ad-levers/retention — upload retention data
adLeverRoutes.post("/analytics/ad-levers/retention", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { planId, data } = req.body || {};
    if (!planId || typeof planId !== "string") {
      res.status(400).json({ error: "planId is required" });
      return;
    }
    if (!Array.isArray(data) || data.length === 0) {
      res.status(400).json({ error: "data array is required" });
      return;
    }

    const userId = (req as any).user?.id || "system";
    await saveRetentionData(planId, userId, data);
    res.json({ ok: true, count: data.length });
  } catch (error) {
    next(error);
  }
});

// POST /analytics/ad-levers/retention/reset — reset to default retention data
adLeverRoutes.post("/analytics/ad-levers/retention/reset", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { planId } = req.body || {};
    if (!planId || typeof planId !== "string") {
      res.status(400).json({ error: "planId is required" });
      return;
    }

    const userId = (req as any).user?.id || "system";
    await resetRetentionData(planId, userId);
    res.json({ ok: true, count: getDefaultRetentionData().length });
  } catch (error) {
    next(error);
  }
});
