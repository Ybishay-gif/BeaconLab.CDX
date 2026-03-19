import { Router } from "express";
import { z } from "zod";
import { requirePermission } from "../../middleware/auth.js";
import { addManagedUser, listManagedUsers, resetManagedUserPassword, setUserModules, updateUserRole, setUserActive } from "../../services/authService.js";
import { VALID_MODULE_IDS } from "../../modules.js";
const addUserSchema = z.object({
    email: z.string().email(),
    name: z.string().optional(),
    role: z.string().optional(),
    roleId: z.string().optional(),
    modules: z.array(z.string()).optional(),
});
const updateModulesSchema = z.object({
    modules: z.array(z.string()).min(1),
});
const updateRoleSchema = z.object({
    roleId: z.string().min(1),
});
const updateActiveSchema = z.object({
    active: z.boolean(),
});
export const usersRoutes = Router();
usersRoutes.get("/users", requirePermission("user_management:view"), async (_req, res, next) => {
    try {
        const rows = await listManagedUsers();
        res.json({ users: rows });
    }
    catch (error) {
        next(error);
    }
});
usersRoutes.post("/users", requirePermission("user_management:edit"), async (req, res, next) => {
    try {
        const parsed = addUserSchema.parse(req.body);
        const created = await addManagedUser(parsed.email, {
            name: parsed.name,
            role: parsed.role,
            roleId: parsed.roleId,
        });
        // Set module access if provided (otherwise addManagedUser defaults to 'planning')
        if (parsed.modules && parsed.modules.length > 0) {
            await setUserModules(created.userId, parsed.modules);
        }
        res.status(201).json(created);
    }
    catch (error) {
        next(error);
    }
});
usersRoutes.post("/users/:userId/reset-password", requirePermission("user_management:edit"), async (req, res, next) => {
    try {
        await resetManagedUserPassword(req.params.userId);
        res.json({ ok: true });
    }
    catch (error) {
        next(error);
    }
});
usersRoutes.put("/users/:userId/modules", requirePermission("user_management:edit"), async (req, res, next) => {
    try {
        const parsed = updateModulesSchema.parse(req.body);
        await setUserModules(req.params.userId, parsed.modules);
        res.json({ ok: true });
    }
    catch (error) {
        next(error);
    }
});
usersRoutes.put("/users/:userId/role", requirePermission("user_management:edit"), async (req, res, next) => {
    try {
        const parsed = updateRoleSchema.parse(req.body);
        await updateUserRole(req.params.userId, parsed.roleId);
        res.json({ ok: true });
    }
    catch (error) {
        next(error);
    }
});
usersRoutes.put("/users/:userId/active", requirePermission("user_management:edit"), async (req, res, next) => {
    try {
        // Prevent deactivating yourself
        if (req.params.userId === req.user?.userId) {
            res.status(400).json({ error: "You cannot deactivate yourself." });
            return;
        }
        const parsed = updateActiveSchema.parse(req.body);
        await setUserActive(req.params.userId, parsed.active);
        res.json({ ok: true });
    }
    catch (error) {
        next(error);
    }
});
// Return available modules for admin UI
usersRoutes.get("/modules", requirePermission("user_management:view"), (_req, res) => {
    res.json({ modules: VALID_MODULE_IDS });
});
