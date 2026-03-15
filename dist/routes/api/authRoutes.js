import { Router } from "express";
import { z } from "zod";
import rateLimit from "express-rate-limit";
import { getUserLoginState, getUserModules, loginAdminWithCode, loginUser, logoutSession, setupUserPassword } from "../../services/authService.js";
import { VALID_MODULE_IDS } from "../../modules.js";
// Rate limit: max 10 login attempts per IP per 15-minute window
const loginLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 10,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: "Too many login attempts. Please try again later." },
});
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
authRoutes.post("/auth/admin-login", loginLimiter, async (req, res, next) => {
    try {
        const parsed = adminLoginSchema.parse(req.body);
        const session = await loginAdminWithCode(parsed.code);
        // Admin gets access to all modules
        res.json({ ...session, user: { ...session.user, modules: VALID_MODULE_IDS } });
    }
    catch (error) {
        next(error);
    }
});
authRoutes.post("/auth/user-status", loginLimiter, async (req, res, next) => {
    try {
        const parsed = userStatusSchema.parse(req.body);
        const state = await getUserLoginState(parsed.email);
        res.json(state);
    }
    catch (error) {
        next(error);
    }
});
authRoutes.post("/auth/user-setup-password", loginLimiter, async (req, res, next) => {
    try {
        const parsed = userSetupPasswordSchema.parse(req.body);
        const session = await setupUserPassword(parsed.email, parsed.password);
        const modules = await getUserModules(session.user.userId);
        res.json({ ...session, user: { ...session.user, modules } });
    }
    catch (error) {
        next(error);
    }
});
authRoutes.post("/auth/user-login", loginLimiter, async (req, res, next) => {
    try {
        const parsed = userLoginSchema.parse(req.body);
        const session = await loginUser(parsed.email, parsed.password);
        const modules = await getUserModules(session.user.userId);
        res.json({ ...session, user: { ...session.user, modules } });
    }
    catch (error) {
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
    }
    catch (error) {
        next(error);
    }
});
