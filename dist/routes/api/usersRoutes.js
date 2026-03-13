import { Router } from "express";
import { z } from "zod";
import { requireRole } from "../../middleware/auth.js";
import { addManagedUser, listManagedUsers, resetManagedUserPassword } from "../../services/authService.js";
const addUserSchema = z.object({
    email: z.string().email()
});
export const usersRoutes = Router();
usersRoutes.get("/users", requireRole(["admin"]), async (_req, res, next) => {
    try {
        const rows = await listManagedUsers();
        res.json({ users: rows });
    }
    catch (error) {
        next(error);
    }
});
usersRoutes.post("/users", requireRole(["admin"]), async (req, res, next) => {
    try {
        const parsed = addUserSchema.parse(req.body);
        const created = await addManagedUser(parsed.email);
        res.status(201).json(created);
    }
    catch (error) {
        next(error);
    }
});
usersRoutes.post("/users/:userId/reset-password", requireRole(["admin"]), async (req, res, next) => {
    try {
        await resetManagedUserPassword(req.params.userId);
        res.json({ ok: true });
    }
    catch (error) {
        next(error);
    }
});
