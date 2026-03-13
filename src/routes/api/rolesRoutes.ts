import { Router } from "express";
import { z } from "zod";
import { requirePermission } from "../../middleware/auth.js";
import { listRoles, getRoleById, createRole, updateRole, deleteRole } from "../../services/roleService.js";
import { PERMISSION_GROUPS, ALL_PERMISSIONS } from "../../permissions.js";

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
    res.status(201).json(role);
  } catch (error) {
    next(error);
  }
});

// Update a role
rolesRoutes.put("/roles/:roleId", requirePermission("roles_permissions:edit"), async (req, res, next) => {
  try {
    const parsed = updateRoleSchema.parse(req.body);
    const role = await updateRole(req.params.roleId, parsed);
    res.json(role);
  } catch (error) {
    next(error);
  }
});

// Delete a role
rolesRoutes.delete("/roles/:roleId", requirePermission("roles_permissions:edit"), async (req, res, next) => {
  try {
    await deleteRole(req.params.roleId);
    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

// Return permission groups for the roles editor UI
rolesRoutes.get("/permissions", requirePermission("roles_permissions:edit"), (_req, res) => {
  res.json({ groups: PERMISSION_GROUPS, all: ALL_PERMISSIONS });
});
