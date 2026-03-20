import { Router } from "express";
import { z } from "zod";
import { requirePermission } from "../../middleware/auth.js";
import { addManagedUser, listManagedUsers, resetManagedUserPassword, setUserModules, updateUserRole, setUserActive, getUserModules } from "../../services/authService.js";
import { VALID_MODULE_IDS } from "../../modules.js";
import { appendChangeLog } from "../../services/changeLogService.js";
import { query, table } from "../../db/index.js";
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
async function fetchUserSnapshot(userId) {
    const rows = await query(`SELECT u.user_id, u.email, u.role, u.role_id, u.is_active, u.name, r.name AS role_name
     FROM ${table("users")} u LEFT JOIN ${table("roles")} r ON r.role_id = u.role_id
     WHERE u.user_id = @userId LIMIT 1`, { userId });
    return rows[0] ?? null;
}
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
        const modules = parsed.modules && parsed.modules.length > 0 ? parsed.modules : ["planning"];
        if (parsed.modules && parsed.modules.length > 0) {
            await setUserModules(created.userId, parsed.modules);
        }
        appendChangeLog({ userId: req.user.userId, email: req.user.email }, {
            objectType: "user",
            objectId: created.userId,
            action: "create",
            after: { email: created.email, name: parsed.name ?? null, role: parsed.role ?? "planner", role_id: parsed.roleId ?? null, modules },
            module: "admin",
        }).catch(console.error);
        res.status(201).json(created);
    }
    catch (error) {
        next(error);
    }
});
usersRoutes.post("/users/:userId/reset-password", requirePermission("user_management:edit"), async (req, res, next) => {
    try {
        const snapshot = await fetchUserSnapshot(req.params.userId);
        await resetManagedUserPassword(req.params.userId);
        appendChangeLog({ userId: req.user.userId, email: req.user.email }, {
            objectType: "user",
            objectId: req.params.userId,
            action: "reset_password",
            metadata: { target_email: snapshot?.email ?? "unknown" },
            module: "admin",
        }).catch(console.error);
        res.json({ ok: true });
    }
    catch (error) {
        next(error);
    }
});
usersRoutes.put("/users/:userId/modules", requirePermission("user_management:edit"), async (req, res, next) => {
    try {
        const parsed = updateModulesSchema.parse(req.body);
        const beforeModules = await getUserModules(req.params.userId);
        await setUserModules(req.params.userId, parsed.modules);
        appendChangeLog({ userId: req.user.userId, email: req.user.email }, {
            objectType: "user",
            objectId: req.params.userId,
            action: "update_modules",
            before: { modules: beforeModules },
            after: { modules: parsed.modules },
            module: "admin",
        }).catch(console.error);
        res.json({ ok: true });
    }
    catch (error) {
        next(error);
    }
});
usersRoutes.put("/users/:userId/role", requirePermission("user_management:edit"), async (req, res, next) => {
    try {
        const parsed = updateRoleSchema.parse(req.body);
        const snapshot = await fetchUserSnapshot(req.params.userId);
        await updateUserRole(req.params.userId, parsed.roleId);
        appendChangeLog({ userId: req.user.userId, email: req.user.email }, {
            objectType: "user",
            objectId: req.params.userId,
            action: "update_role",
            before: { role: snapshot?.role, role_id: snapshot?.role_id, role_name: snapshot?.role_name },
            after: { role_id: parsed.roleId },
            module: "admin",
        }).catch(console.error);
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
        const snapshot = await fetchUserSnapshot(req.params.userId);
        await setUserActive(req.params.userId, parsed.active);
        appendChangeLog({ userId: req.user.userId, email: req.user.email }, {
            objectType: "user",
            objectId: req.params.userId,
            action: parsed.active ? "activate" : "deactivate",
            before: { is_active: snapshot?.is_active, email: snapshot?.email },
            after: { is_active: parsed.active },
            module: "admin",
        }).catch(console.error);
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
