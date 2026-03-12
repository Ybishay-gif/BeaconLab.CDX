import { Router } from "express";
import { z } from "zod";
import {
  getUserLoginState,
  getUserModules,
  loginAdminWithCode,
  loginUser,
  logoutSession,
  setupUserPassword
} from "../../services/authService.js";
import { VALID_MODULE_IDS } from "../../modules.js";

const adminLoginSchema = z.object({
  code: z.string().min(1)
});

const userStatusSchema = z.object({
  email: z.string().email()
});

const userLoginSchema = z.object({
  email: z.string().email(),
  password: z.string().min(1)
});

const userSetupPasswordSchema = z.object({
  email: z.string().email(),
  password: z.string().min(8)
});

export const authRoutes = Router();

authRoutes.post("/auth/admin-login", async (req, res, next) => {
  try {
    const parsed = adminLoginSchema.parse(req.body);
    const session = await loginAdminWithCode(parsed.code);
    // Admin gets access to all modules
    res.json({ ...session, user: { ...session.user, modules: VALID_MODULE_IDS } });
  } catch (error) {
    next(error);
  }
});

authRoutes.post("/auth/user-status", async (req, res, next) => {
  try {
    const parsed = userStatusSchema.parse(req.body);
    const state = await getUserLoginState(parsed.email);
    res.json(state);
  } catch (error) {
    next(error);
  }
});

authRoutes.post("/auth/user-setup-password", async (req, res, next) => {
  try {
    const parsed = userSetupPasswordSchema.parse(req.body);
    const session = await setupUserPassword(parsed.email, parsed.password);
    const modules = await getUserModules(session.user.userId);
    res.json({ ...session, user: { ...session.user, modules } });
  } catch (error) {
    next(error);
  }
});

authRoutes.post("/auth/user-login", async (req, res, next) => {
  try {
    const parsed = userLoginSchema.parse(req.body);
    const session = await loginUser(parsed.email, parsed.password);
    const modules = await getUserModules(session.user.userId);
    res.json({ ...session, user: { ...session.user, modules } });
  } catch (error) {
    next(error);
  }
});

authRoutes.post("/auth/logout", async (req, res, next) => {
  try {
    const sessionToken = req.header("x-session-token")?.trim();
    if (sessionToken) {
      await logoutSession(sessionToken);
    }
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});
