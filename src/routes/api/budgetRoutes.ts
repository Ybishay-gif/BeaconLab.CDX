import { Router } from "express";
import type { NextFunction, Request, Response } from "express";
import { z } from "zod";
import { requirePermission } from "../../middleware/auth.js";
import {
  listBudgets,
  upsertBudget,
  deleteBudget,
  upsertAllocations,
  listAccountNames,
  getActualsVsPlanned,
  getForecast,
} from "../../services/budgetService.js";
import { appendChangeLog } from "../../services/changeLogService.js";

export const budgetRoutes = Router();

function timedRoute(
  name: string,
  handler: (req: Request, res: Response, next: NextFunction) => Promise<void>
) {
  return async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    const started = process.hrtime.bigint();
    try {
      await handler(req, res, next);
    } finally {
      const elapsedMs = Number(process.hrtime.bigint() - started) / 1_000_000;
      console.info(
        JSON.stringify({
          kind: "budget_api_timing",
          endpoint: name,
          method: req.method,
          path: req.originalUrl || req.path,
          status: res.statusCode,
          duration_ms: Number(elapsedMs.toFixed(2)),
        })
      );
    }
  };
}

// ── Schemas ──────────────────────────────────────────────────────────

const upsertBudgetSchema = z.object({
  year: z.number().int().min(2020).max(2100),
  month: z.number().int().min(1).max(12),
  activityType: z.enum(["clicks", "leads", "calls"]),
  leadType: z.enum(["auto", "home"]),
  amount: z.number().nonnegative(),
});

const upsertAllocationsSchema = z.object({
  allocations: z
    .array(
      z.object({
        accountName: z.string().min(1),
        allocationPct: z.number().min(0).max(100),
      })
    )
    .refine(
      (arr) => arr.reduce((sum, a) => sum + a.allocationPct, 0) <= 100.001,
      { message: "Total allocation percentage cannot exceed 100%" }
    ),
});

const bulkUpsertSchema = z.object({
  year: z.number().int().min(2020).max(2100),
  budgets: z.array(
    z.object({
      month: z.number().int().min(1).max(12),
      activityType: z.enum(["clicks", "leads", "calls"]),
      leadType: z.enum(["auto", "home"]),
      amount: z.number().nonnegative(),
    })
  ),
});

// ── Endpoints ────────────────────────────────────────────────────────

// Bulk upsert budgets (for import)
budgetRoutes.put(
  "/budgets/bulk",
  requirePermission("budgets:edit"),
  timedRoute("bulk_upsert_budgets", async (req, res, next) => {
    try {
      const parsed = bulkUpsertSchema.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: parsed.error.issues });
        return;
      }
      const { year, budgets: entries } = parsed.data;
      const user = (req as any).user;
      let count = 0;
      for (const entry of entries) {
        await upsertBudget(year, entry.month, entry.activityType, entry.leadType, entry.amount, user.userId);
        count++;
      }

      await appendChangeLog(
        { userId: user.userId, email: user.email },
        {
          objectType: "budget",
          action: "bulk_upsert",
          after: { year, count },
          module: "cross_tactic",
        }
      );

      res.json({ ok: true, count });
    } catch (error) {
      next(error);
    }
  })
);

// List budgets for a year
budgetRoutes.get(
  "/budgets",
  requirePermission("budgets:view"),
  timedRoute("list_budgets", async (req, res, next) => {
    try {
      const year = Number(req.query.year);
      if (!Number.isFinite(year)) {
        res.status(400).json({ error: "year is required" });
        return;
      }
      const rows = await listBudgets(year);
      res.json({ rows });
    } catch (error) {
      next(error);
    }
  })
);

// Upsert a budget
budgetRoutes.put(
  "/budgets",
  requirePermission("budgets:edit"),
  timedRoute("upsert_budget", async (req, res, next) => {
    try {
      const parsed = upsertBudgetSchema.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: parsed.error.issues });
        return;
      }
      const { year, month, activityType, leadType, amount } = parsed.data;
      const user = (req as any).user;
      const { budgetId } = await upsertBudget(year, month, activityType, leadType, amount, user.userId);

      await appendChangeLog(
        { userId: user.userId, email: user.email },
        {
          objectType: "budget",
          objectId: budgetId,
          action: "upsert",
          after: { year, month, activityType, leadType, amount },
          module: "cross_tactic",
        }
      );

      res.json({ ok: true, budgetId });
    } catch (error) {
      next(error);
    }
  })
);

// Delete a budget
budgetRoutes.delete(
  "/budgets/:budgetId",
  requirePermission("budgets:edit"),
  timedRoute("delete_budget", async (req, res, next) => {
    try {
      const { budgetId } = req.params;
      const user = (req as any).user;

      await deleteBudget(budgetId);

      await appendChangeLog(
        { userId: user.userId, email: user.email },
        {
          objectType: "budget",
          objectId: budgetId,
          action: "delete",
          module: "cross_tactic",
        }
      );

      res.json({ ok: true });
    } catch (error) {
      next(error);
    }
  })
);

// Upsert allocations for a budget
budgetRoutes.put(
  "/budgets/:budgetId/allocations",
  requirePermission("budgets:edit"),
  timedRoute("upsert_allocations", async (req, res, next) => {
    try {
      const { budgetId } = req.params;
      const parsed = upsertAllocationsSchema.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: parsed.error.issues });
        return;
      }
      const user = (req as any).user;

      await upsertAllocations(budgetId, parsed.data.allocations);

      await appendChangeLog(
        { userId: user.userId, email: user.email },
        {
          objectType: "budget_allocation",
          objectId: budgetId,
          action: "upsert",
          after: parsed.data.allocations,
          module: "cross_tactic",
        }
      );

      res.json({ ok: true });
    } catch (error) {
      next(error);
    }
  })
);

// List account names from actuals
budgetRoutes.get(
  "/budgets/account-names",
  requirePermission("budgets:view"),
  timedRoute("list_account_names", async (req, res, next) => {
    try {
      const rows = await listAccountNames();
      res.json({ rows });
    } catch (error) {
      next(error);
    }
  })
);

// Actual vs planned
budgetRoutes.get(
  "/budgets/actuals",
  requirePermission("budgets:view"),
  timedRoute("get_actuals", async (req, res, next) => {
    try {
      const year = Number(req.query.year);
      const month = Number(req.query.month);
      if (!Number.isFinite(year) || !Number.isFinite(month)) {
        res.status(400).json({ error: "year and month are required" });
        return;
      }
      const data = await getActualsVsPlanned(year, month);
      res.json(data);
    } catch (error) {
      next(error);
    }
  })
);

// Forecast
budgetRoutes.get(
  "/budgets/forecast",
  requirePermission("budgets:view"),
  timedRoute("get_forecast", async (req, res, next) => {
    try {
      const year = Number(req.query.year);
      const month = Number(req.query.month);
      if (!Number.isFinite(year) || !Number.isFinite(month)) {
        res.status(400).json({ error: "year and month are required" });
        return;
      }
      const data = await getForecast(year, month);
      res.json(data);
    } catch (error) {
      next(error);
    }
  })
);
