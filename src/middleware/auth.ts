import type { NextFunction, Request, Response } from "express";
import { validateSessionToken } from "../services/authService.js";

declare global {
  namespace Express {
    interface Request {
      user?: {
        userId: string;
        email: string;
        role: "admin" | "planner" | "viewer";
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
      role: user.role
    };

    next();
  } catch (error) {
    next(error);
  }
}

export function requireRole(allowed: Array<"admin" | "planner" | "viewer">) {
  return (req: Request, res: Response, next: NextFunction): void => {
    const role = req.user?.role;
    if (!role || !allowed.includes(role)) {
      res.status(403).json({ error: "Insufficient permissions" });
      return;
    }

    next();
  };
}
