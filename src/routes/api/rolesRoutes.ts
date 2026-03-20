import { Router } from "express";
import { z } from "zod";
import { requirePermission } from "../../middleware/auth.js";
import { listRoles, getRoleById, createRole, updateRole, deleteRole } from "../../services/roleService.js";
import { PERMISSION_GROUPS, ALL_PERMISSIONS } from "../../permissions.js";
import { appendChangeLog } from "../../services/changeLogService.js";
import { invalidateSessionsForRole } from "../../services/authService.js";

const createRoleSchema = z.object({
  name: z.string().min(1),
  permissions: z.array(z.string()),
});

const updateRoleSchema = z.object({
  name: z.string().min(1).optional(),
  permissions: z.array(z.string()).optional(),
});

export const rolesRoutes = Router();

// List all roles with their permissions
rolesRoutes.get("/roles", requirePermission("roles_permissions:edit"), async (_req, res, next) => {
  try {
    const roles = await listRoles();
    res.json({ roles });
  } catch (error) {
    next(error);
  }
});

// Get single role
rolesRoutes.get("/roles/:roleId", requirePermission("roles_permissions:edit"), async (req, res, next) => {
  try {
    const role = await getRoleById(req.params.roleId);
    if (!role) {
      res.status(404).json({ error: "Role not found" });
      return;
    }
    res.json(role);
  } catch (error) {
    next(error);
  }
});

// Create a new role
rolesRoutes.post("/roles", requirePermission("roles_permissions:edit"), async (req, res, next) => {
  try {
    const parsed = createRoleSchema.parse(req.body);
    const role = await createRole(parsed.name, parsed.permissions);

    appendChangeLog(
      { userId: req.user!.userId, email: req.user!.email },
      {
        objectType: "role",
        objectId: role.role_id,
        action: "create",
        after: { name: role.name, permissions: role.permissions },
        module: "admin",
      }
    ).catch(console.error);

    res.status(201).json(role);
  } catch (error) {
    next(error);
  }
});

// Update a role
rolesRoutes.put("/roles/:roleId", requirePermission("roles_permissions:edit"), async (req, res, next) => {
  try {
    const parsed = updateRoleSchema.parse(req.body);
    const before = await getRoleById(req.params.roleId);
    const role = await updateRole(req.params.roleId, parsed);

    // Flush cached sessions so users with this role pick up new permissions immediately
    if (parsed.permissions !== undefined) {
      const flushed = invalidateSessionsForRole(req.params.roleId);
      if (flushed > 0) console.log(`Flushed ${flushed} cached session(s) after role ${req.params.roleId} permissions update`);
    }

    const permsBefore = before?.permissions ?? [];
    const permsAfter = role.permissions;
    const added = permsAfter.filter((p) => !permsBefore.includes(p));
    const removed = permsBefore.filter((p) => !permsAfter.includes(p));

    appendChangeLog(
      { userId: req.user!.userId, email: req.user!.email },
      {
        objectType: "role",
        objectId: req.params.roleId,
        action: "update",
        before: { name: before?.name, permissions: permsBefore },
        after: { name: role.name, permissions: permsAfter },
        metadata: { permissions_added: added, permissions_removed: removed },
        module: "admin",
      }
    ).catch(console.error);

    res.json(role);
  } catch (error) {
    next(error);
  }
});

// Delete a role
rolesRoutes.delete("/roles/:roleId", requirePermission("roles_permissions:edit"), async (req, res, next) => {
  try {
    const before = await getRoleById(req.params.roleId);
    await deleteRole(req.params.roleId);

    appendChangeLog(
      { userId: req.user!.userId, email: req.user!.email },
      {
        objectType: "role",
        objectId: req.params.roleId,
        action: "delete",
        before: { name: before?.name, permissions: before?.permissions },
        module: "admin",
      }
    ).catch(console.error);

    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

// Return permission groups for the roles editor UI
rolesRoutes.get("/permissions", requirePermission("roles_permissions:edit"), (_req, res) => {
  res.json({ groups: PERMISSION_GROUPS, all: ALL_PERMISSIONS });
});
