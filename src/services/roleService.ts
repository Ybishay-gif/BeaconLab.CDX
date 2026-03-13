import { query, table } from "../db/index.js";
import { ALL_PERMISSIONS } from "../permissions.js";

type RoleRow = {
  role_id: string;
  name: string;
  is_system: boolean;
  created_at: string;
  updated_at: string;
};

type RolePermRow = {
  role_id: string;
  permission_key: string;
};

export type RoleWithPermissions = RoleRow & { permissions: string[] };

function fail(status: number, message: string): never {
  const error = new Error(message) as Error & { status: number };
  error.status = status;
  throw error;
}

export async function listRoles(): Promise<RoleWithPermissions[]> {
  const roles = await query<RoleRow>(
    `SELECT role_id, name, is_system, created_at::text, updated_at::text FROM ${table("roles")} ORDER BY is_system DESC, name`
  );
  if (roles.length === 0) return [];

  const perms = await query<RolePermRow>(
    `SELECT role_id, permission_key FROM ${table("role_permissions")}`
  );
  const permMap = new Map<string, string[]>();
  for (const p of perms) {
    const arr = permMap.get(p.role_id) ?? [];
    arr.push(p.permission_key);
    permMap.set(p.role_id, arr);
  }

  return roles.map((r) => ({
    ...r,
    permissions: permMap.get(r.role_id) ?? [],
  }));
}

export async function getRoleById(roleId: string): Promise<RoleWithPermissions | null> {
  const rows = await query<RoleRow>(
    `SELECT role_id, name, is_system, created_at::text, updated_at::text FROM ${table("roles")} WHERE role_id = @roleId`,
    { roleId }
  );
  const role = rows[0];
  if (!role) return null;

  const perms = await query<RolePermRow>(
    `SELECT role_id, permission_key FROM ${table("role_permissions")} WHERE role_id = @roleId`,
    { roleId }
  );
  return { ...role, permissions: perms.map((p) => p.permission_key) };
}

export async function getRolePermissions(roleId: string): Promise<string[]> {
  const rows = await query<RolePermRow>(
    `SELECT permission_key FROM ${table("role_permissions")} WHERE role_id = @roleId`,
    { roleId }
  );
  return rows.map((r) => r.permission_key);
}

export async function getRoleByName(name: string): Promise<RoleRow | null> {
  const rows = await query<RoleRow>(
    `SELECT role_id, name, is_system, created_at::text, updated_at::text FROM ${table("roles")} WHERE LOWER(name) = LOWER(@name)`,
    { name }
  );
  return rows[0] ?? null;
}

export async function createRole(
  name: string,
  permissions: string[]
): Promise<RoleWithPermissions> {
  const trimmed = name.trim();
  if (!trimmed) fail(400, "Role name is required.");

  const existing = await getRoleByName(trimmed);
  if (existing) fail(409, `A role named "${trimmed}" already exists.`);

  const validPerms = permissions.filter((p) => (ALL_PERMISSIONS as readonly string[]).includes(p));

  const rows = await query<RoleRow>(
    `INSERT INTO ${table("roles")} (name, is_system) VALUES (@name, FALSE) RETURNING role_id, name, is_system, created_at::text, updated_at::text`,
    { name: trimmed }
  );
  const role = rows[0];

  for (const perm of validPerms) {
    await query(
      `INSERT INTO ${table("role_permissions")} (role_id, permission_key) VALUES (@roleId, @perm) ON CONFLICT DO NOTHING`,
      { roleId: role.role_id, perm }
    );
  }

  return { ...role, permissions: validPerms };
}

export async function updateRole(
  roleId: string,
  updates: { name?: string; permissions?: string[] }
): Promise<RoleWithPermissions> {
  const role = await getRoleById(roleId);
  if (!role) fail(404, "Role not found.");

  if (updates.name !== undefined) {
    const trimmed = updates.name.trim();
    if (!trimmed) fail(400, "Role name is required.");
    if (trimmed.toLowerCase() !== role.name.toLowerCase()) {
      const dup = await getRoleByName(trimmed);
      if (dup) fail(409, `A role named "${trimmed}" already exists.`);
    }
    await query(
      `UPDATE ${table("roles")} SET name = @name, updated_at = NOW() WHERE role_id = @roleId`,
      { name: trimmed, roleId }
    );
    role.name = trimmed;
  }

  if (updates.permissions !== undefined) {
    const validPerms = updates.permissions.filter((p) =>
      (ALL_PERMISSIONS as readonly string[]).includes(p)
    );
    await query(`DELETE FROM ${table("role_permissions")} WHERE role_id = @roleId`, { roleId });
    for (const perm of validPerms) {
      await query(
        `INSERT INTO ${table("role_permissions")} (role_id, permission_key) VALUES (@roleId, @perm)`,
        { roleId, perm }
      );
    }
    role.permissions = validPerms;
  }

  return role;
}

export async function deleteRole(roleId: string): Promise<void> {
  const role = await getRoleById(roleId);
  if (!role) fail(404, "Role not found.");
  if (role.is_system) fail(400, "Cannot delete a system role.");

  // Check if any users are assigned this role
  const users = await query<{ cnt: string }>(
    `SELECT COUNT(*)::text AS cnt FROM ${table("users")} WHERE role_id = @roleId`,
    { roleId }
  );
  const count = parseInt(users[0]?.cnt ?? "0", 10);
  if (count > 0) {
    fail(400, `Cannot delete role "${role.name}" — ${count} user(s) are still assigned to it.`);
  }

  await query(`DELETE FROM ${table("roles")} WHERE role_id = @roleId`, { roleId });
}
