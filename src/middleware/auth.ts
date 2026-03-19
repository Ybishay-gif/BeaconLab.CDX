import type { NextFunction, Request, Response } from "express";
import { validateSessionToken } from "../services/authService.js";

declare global {
  namespace Express {
    interface Request {
      user?: {
        userId: string;
        email: string;
        role: string;
        roleId: string;
        roleName: string;
        permissions: string[];
      };
    }
  }
}

export async function requireUser(req: Request, res: Response, next: NextFunction): Promise<void> {
  try {
    const sessionToken = req.header("x-session-token")?.trim();
    if (!sessionToken) {
      res.status(401).json({ error: "Missing x-session-token header" });
      return;
    }

    const user = await validateSessionToken(sessionToken);
    if (!user) {
      res.status(401).json({ error: "Session expired or invalid" });
      return;
    }

    req.user = {
      userId: user.userId,
      email: user.email,
      role: user.role,
      roleId: user.roleId,
      roleName: user.roleName,
      permissions: user.permissions,
    };

    // Send current role + permissions so the frontend can detect changes without re-login
    res.setHeader("x-user-role", JSON.stringify({
      roleId: user.roleId,
      roleName: user.roleName,
      permissions: user.permissions,
    }));

    next();
  } catch (error) {
    next(error);
  }
}

/** Check that the user has ALL of the specified permissions */
export function requirePermission(...required: string[]) {
  return (req: Request, res: Response, next: NextFunction): void => {
    const perms = req.user?.permissions ?? [];
    const hasAll = required.every((p) => perms.includes(p));
    if (!hasAll) {
      res.status(403).json({ error: "Insufficient permissions" });
      return;
    }
    next();
  };
}

/** @deprecated Use requirePermission instead. Kept for backward compat during transition. */
export function requireRole(allowed: string[]) {
  return (req: Request, res: Response, next: NextFunction): void => {
    const role = req.user?.role;
    if (!role || !allowed.includes(role)) {
      res.status(403).json({ error: "Insufficient permissions" });
      return;
    }
    next();
  };
}
