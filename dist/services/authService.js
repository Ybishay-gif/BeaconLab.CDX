import { randomBytes, randomUUID, scryptSync, timingSafeEqual } from "node:crypto";
import { config } from "../config.js";
import { query, table } from "../db/index.js";
import { VALID_MODULE_IDS, isValidModuleId } from "../modules.js";
import { getRolePermissions, getRoleByName } from "./roleService.js";
import { ALL_PERMISSIONS_LIST } from "../permissions.js";
let authTablesReady = null;
function isSerializableConflict(error) {
    const message = typeof error === "object" && error !== null && "message" in error ? String(error.message) : "";
    return message.toLowerCase().includes("could not serialize access to table");
}
async function withSerializableRetry(operation, retries = 2) {
    let attempt = 0;
    while (true) {
        try {
            return await operation();
        }
        catch (error) {
            attempt += 1;
            if (attempt > retries || !isSerializableConflict(error)) {
                throw error;
            }
            await new Promise((resolve) => setTimeout(resolve, 100 * attempt));
        }
    }
}
function fail(status, message) {
    const error = new Error(message);
    error.status = status;
    throw error;
}
function normalizeEmail(email) {
    return String(email || "").trim().toLowerCase();
}
function hashPassword(password, salt) {
    return scryptSync(password, salt, 64).toString("hex");
}
function validatePasswordStrength(password) {
    if (password.length < 8) {
        fail(400, "Password must be at least 8 characters.");
    }
    if (!/[a-z]/.test(password)) {
        fail(400, "Password must contain at least one lowercase letter.");
    }
    if (!/[A-Z]/.test(password)) {
        fail(400, "Password must contain at least one uppercase letter.");
    }
    if (!/[0-9]/.test(password)) {
        fail(400, "Password must contain at least one number.");
    }
}
async function ensureAuthTablesExist() {
    // PG schema created via migration — no runtime DDL needed
    if (config.usePg)
        return;
    if (!authTablesReady) {
        authTablesReady = (async () => {
            await query(`
          CREATE TABLE IF NOT EXISTS ${table("users")} (
            user_id STRING NOT NULL,
            email STRING NOT NULL,
            name STRING,
            role STRING NOT NULL,
            is_active BOOL NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP
          )
        `);
            await query(`
          CREATE TABLE IF NOT EXISTS ${table("user_credentials")} (
            user_id STRING NOT NULL,
            email STRING NOT NULL,
            password_salt STRING,
            password_hash STRING,
            last_login_at TIMESTAMP,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP
          )
        `);
            await query(`
          CREATE TABLE IF NOT EXISTS ${table("auth_sessions")} (
            session_token STRING NOT NULL,
            user_id STRING NOT NULL,
            email STRING NOT NULL,
            role STRING NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP NOT NULL,
            last_seen_at TIMESTAMP NOT NULL
          )
        `);
        })();
    }
    return authTablesReady;
}
async function getUserByEmail(email) {
    const roleIdCol = config.usePg ? "role_id," : "";
    const rows = await query(`
      SELECT user_id, email, role, ${roleIdCol} is_active
      FROM ${table("users")}
      WHERE LOWER(email) = @email
      LIMIT 1
    `, { email: normalizeEmail(email) });
    return rows[0] || null;
}
async function getCredentialsByUserId(userId) {
    const castLogin = config.usePg ? "last_login_at::text" : "CAST(last_login_at AS STRING)";
    const rows = await query(`
      SELECT
        user_id,
        email,
        password_salt,
        password_hash,
        ${castLogin} AS last_login_at
      FROM ${table("user_credentials")}
      WHERE user_id = @userId
      LIMIT 1
    `, { userId });
    return rows[0] || null;
}
/** Resolve role_id -> permissions. For BQ mode or missing role_id, falls back based on role text. */
async function resolvePermissions(roleId, roleFallback) {
    if (config.usePg && roleId) {
        const perms = await getRolePermissions(roleId);
        const rows = await query(`SELECT name FROM ${table("roles")} WHERE role_id = @roleId`, { roleId });
        const roleName = rows[0]?.name ?? roleFallback;
        return { roleId, roleName, permissions: perms };
    }
    // BQ fallback or no role_id: admin gets all, others get nothing
    if (roleFallback === "admin") {
        return { roleId: roleId ?? "admin", roleName: "Admin", permissions: [...ALL_PERMISSIONS_LIST] };
    }
    return { roleId: roleId ?? "", roleName: roleFallback, permissions: [] };
}
const SESSION_TTL_DAYS = 14;
async function createSession(user) {
    const token = randomBytes(32).toString("hex");
    const expiresAt = new Date(Date.now() + SESSION_TTL_DAYS * 24 * 60 * 60 * 1000).toISOString();
    const expiresExpr = config.usePg
        ? "NOW() + INTERVAL '14 days'"
        : "TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)";
    const now = config.usePg ? "NOW()" : "CURRENT_TIMESTAMP()";
    const roleIdCol = config.usePg ? "role_id," : "";
    const roleIdVal = config.usePg ? "@roleId," : "";
    await withSerializableRetry(() => query(`
        INSERT INTO ${table("auth_sessions")} (
          session_token,
          user_id,
          email,
          role,
          ${roleIdCol}
          expires_at,
          created_at,
          last_seen_at
        )
        VALUES (
          @token,
          @userId,
          @email,
          @role,
          ${roleIdVal}
          ${expiresExpr},
          ${now},
          ${now}
        )
      `, {
        token,
        userId: user.userId,
        email: user.email,
        role: user.role,
        roleId: user.roleId,
    }));
    return { token, expiresAt, user };
}
export async function getUserLoginState(email) {
    await ensureAuthTablesExist();
    const user = await getUserByEmail(email);
    if (!user || !user.is_active) {
        return { exists: false, requiresPasswordSetup: false };
    }
    const credential = await getCredentialsByUserId(user.user_id);
    const requiresPasswordSetup = !credential?.password_hash || !credential?.password_salt;
    return { exists: true, requiresPasswordSetup };
}
export async function setupUserPassword(email, password) {
    await ensureAuthTablesExist();
    validatePasswordStrength(password);
    const normalizedEmail = normalizeEmail(email);
    const user = await getUserByEmail(normalizedEmail);
    if (!user || !user.is_active) {
        fail(404, "User not found or inactive.");
    }
    const credential = await getCredentialsByUserId(user.user_id);
    if (credential?.password_hash && credential.password_salt) {
        fail(400, "Password is already set for this user.");
    }
    const salt = randomBytes(16).toString("hex");
    const hashed = hashPassword(password, salt);
    if (config.usePg) {
        await query(`
        INSERT INTO ${table("user_credentials")} (
          user_id, email, password_salt, password_hash, last_login_at, created_at, updated_at
        ) VALUES (
          @userId, @email, @salt, @hashed, NOW(), NOW(), NOW()
        )
        ON CONFLICT (user_id) DO UPDATE SET
          email = EXCLUDED.email,
          password_salt = EXCLUDED.password_salt,
          password_hash = EXCLUDED.password_hash,
          last_login_at = NOW(),
          updated_at = NOW()
      `, { userId: user.user_id, email: user.email, salt, hashed });
    }
    else {
        await query(`
        MERGE ${table("user_credentials")} AS target
        USING (
          SELECT @userId AS user_id, @email AS email, @salt AS password_salt, @hashed AS password_hash
        ) AS source
        ON target.user_id = source.user_id
        WHEN MATCHED THEN
          UPDATE SET
            email = source.email,
            password_salt = source.password_salt,
            password_hash = source.password_hash,
            last_login_at = CURRENT_TIMESTAMP(),
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (user_id, email, password_salt, password_hash, last_login_at, created_at, updated_at)
          VALUES (source.user_id, source.email, source.password_salt, source.password_hash,
                  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
      `, { userId: user.user_id, email: user.email, salt, hashed });
    }
    const resolved = await resolvePermissions(user.role_id, user.role);
    return createSession({
        userId: user.user_id,
        email: user.email,
        role: user.role,
        roleId: resolved.roleId,
        roleName: resolved.roleName,
        permissions: resolved.permissions,
    });
}
export async function loginUser(email, password) {
    await ensureAuthTablesExist();
    const normalizedEmail = normalizeEmail(email);
    const user = await getUserByEmail(normalizedEmail);
    if (!user || !user.is_active) {
        fail(403, "Access denied.");
    }
    const credential = await getCredentialsByUserId(user.user_id);
    if (!credential?.password_hash || !credential.password_salt) {
        fail(400, "Password has not been set yet for this user.");
    }
    const expected = Buffer.from(credential.password_hash, "hex");
    const actual = Buffer.from(hashPassword(password, credential.password_salt), "hex");
    const isValid = expected.length === actual.length && timingSafeEqual(expected, actual);
    if (!isValid) {
        fail(401, "Invalid credentials.");
    }
    await query(`
      UPDATE ${table("user_credentials")}
      SET
        last_login_at = CURRENT_TIMESTAMP(),
        updated_at = CURRENT_TIMESTAMP()
      WHERE user_id = @userId
    `, { userId: user.user_id });
    const resolved = await resolvePermissions(user.role_id, user.role);
    return createSession({
        userId: user.user_id,
        email: user.email,
        role: user.role,
        roleId: resolved.roleId,
        roleName: resolved.roleName,
        permissions: resolved.permissions,
    });
}
export async function loginAdminWithCode(code) {
    await ensureAuthTablesExist();
    if (String(code || "").trim() !== config.adminAccessCode) {
        fail(401, "Invalid admin access code.");
    }
    // Resolve the Admin system role
    let roleId = "admin-code";
    let permissions = [...ALL_PERMISSIONS_LIST];
    if (config.usePg) {
        const adminRole = await getRoleByName("Admin");
        if (adminRole) {
            roleId = adminRole.role_id;
            permissions = await getRolePermissions(adminRole.role_id);
        }
    }
    return createSession({
        userId: "admin-code",
        email: "admin@local",
        role: "admin",
        roleId,
        roleName: "Admin",
        permissions,
    });
}
// ── Session cache: avoids a BQ round-trip on every authenticated request ──
const sessionCache = new Map();
const SESSION_CACHE_TTL_MS = 2 * 60 * 1000; // 2 minutes
export async function validateSessionToken(token) {
    // Fast-path: return from in-memory cache
    const cached = sessionCache.get(token);
    if (cached && cached.expiresAt > Date.now()) {
        return cached.user;
    }
    await ensureAuthTablesExist();
    const roleIdSelect = config.usePg ? ", role_id" : "";
    const rows = await query(`
      SELECT session_token, user_id, email, role${roleIdSelect}
      FROM ${table("auth_sessions")}
      WHERE session_token = @token
        AND expires_at > CURRENT_TIMESTAMP()
      ORDER BY created_at DESC
      LIMIT 1
    `, { token });
    const session = rows[0];
    if (!session) {
        sessionCache.delete(token);
        return null;
    }
    void withSerializableRetry(() => query(`
        UPDATE ${table("auth_sessions")}
        SET last_seen_at = CURRENT_TIMESTAMP()
        WHERE session_token = @token
      `, { token })).catch(() => undefined);
    const resolved = await resolvePermissions(session.role_id, session.role);
    const user = {
        userId: session.user_id,
        email: session.email,
        role: session.role,
        roleId: resolved.roleId,
        roleName: resolved.roleName,
        permissions: resolved.permissions,
    };
    sessionCache.set(token, { user, expiresAt: Date.now() + SESSION_CACHE_TTL_MS });
    return user;
}
export async function logoutSession(token) {
    sessionCache.delete(token);
    await ensureAuthTablesExist();
    await query(`
      DELETE FROM ${table("auth_sessions")}
      WHERE session_token = @token
    `, { token });
}
// ── Module access ────────────────────────────────────────────────
export async function getUserModules(userId) {
    if (!config.usePg)
        return VALID_MODULE_IDS; // BQ: no module table, grant all
    const rows = await query(`SELECT module_id FROM ${table("user_modules")} WHERE user_id = @userId`, { userId });
    return rows.map((r) => r.module_id);
}
export async function setUserModules(userId, modules) {
    const valid = modules.filter(isValidModuleId);
    if (valid.length === 0) {
        const error = new Error("At least one valid module is required.");
        error.status = 400;
        throw error;
    }
    await query(`DELETE FROM ${table("user_modules")} WHERE user_id = @userId`, { userId });
    for (const moduleId of valid) {
        await query(`INSERT INTO ${table("user_modules")} (user_id, module_id) VALUES (@userId, @moduleId)`, { userId, moduleId });
    }
}
export async function listManagedUsers() {
    await ensureAuthTablesExist();
    const castLogin = config.usePg ? "c.last_login_at::text" : "CAST(c.last_login_at AS STRING)";
    const castCreated = config.usePg ? "u.created_at::text" : "CAST(u.created_at AS STRING)";
    if (config.usePg) {
        const rows = await query(`
        SELECT
          u.user_id,
          u.email,
          u.name,
          u.role,
          u.role_id,
          r.name AS role_name,
          u.is_active AS active,
          ${castLogin} AS last_login,
          ${castCreated} AS created_at,
          COALESCE(array_agg(m.module_id) FILTER (WHERE m.module_id IS NOT NULL), '{}') AS modules
        FROM ${table("users")} AS u
        LEFT JOIN ${table("user_credentials")} AS c
          ON c.user_id = u.user_id
        LEFT JOIN ${table("user_modules")} AS m
          ON m.user_id = u.user_id
        LEFT JOIN ${table("roles")} AS r
          ON r.role_id = u.role_id
        GROUP BY u.user_id, u.email, u.name, u.role, u.role_id, r.name, u.is_active, c.last_login_at, u.created_at
        ORDER BY LOWER(u.email)
      `);
        return rows.map((r) => ({ ...r, modules: r.modules ?? [] }));
    }
    // BQ fallback: no module table, grant all modules
    const rows = await query(`
      SELECT
        u.user_id,
        u.email,
        u.name,
        u.role,
        u.is_active AS active,
        ${castLogin} AS last_login,
        ${castCreated} AS created_at
      FROM ${table("users")} AS u
      LEFT JOIN ${table("user_credentials")} AS c
        ON c.user_id = u.user_id
      ORDER BY LOWER(u.email)
    `);
    return rows.map((r) => ({ ...r, role_id: null, role_name: null, modules: VALID_MODULE_IDS }));
}
export async function addManagedUser(email, opts) {
    await ensureAuthTablesExist();
    const normalizedEmail = normalizeEmail(email);
    if (!normalizedEmail) {
        fail(400, "Email is required.");
    }
    const name = opts?.name?.trim() || null;
    // Determine role_id: prefer explicit roleId, otherwise resolve from role name
    let roleId = opts?.roleId ?? null;
    let roleText = opts?.role ?? "planner";
    if (!roleId && config.usePg) {
        const roleName = roleText === "admin" ? "Admin" : roleText === "viewer" ? "Viewer" : "Planner";
        const role = await getRoleByName(roleName);
        roleId = role?.role_id ?? null;
        roleText = role?.name?.toLowerCase() ?? roleText;
    }
    const existing = await getUserByEmail(normalizedEmail);
    const userId = existing?.user_id || randomUUID();
    if (!existing) {
        if (config.usePg) {
            await query(`
          INSERT INTO ${table("users")} (
            user_id, email, name, role, role_id, is_active, created_at, updated_at
          )
          VALUES (
            @userId, @email, @name, @role, @roleId, TRUE, NOW(), NOW()
          )
        `, { userId, email: normalizedEmail, name, role: roleText, roleId });
        }
        else {
            await query(`
          INSERT INTO ${table("users")} (
            user_id, email, name, role, is_active, created_at, updated_at
          )
          VALUES (
            @userId, @email, @name, @role, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
          )
        `, { userId, email: normalizedEmail, name, role: roleText });
        }
    }
    else if (config.usePg && roleId) {
        await query(`UPDATE ${table("users")} SET role = @role, role_id = @roleId, updated_at = NOW() WHERE user_id = @userId`, { role: roleText, roleId, userId });
    }
    if (config.usePg) {
        await query(`
        INSERT INTO ${table("user_credentials")} (
          user_id, email, password_salt, password_hash, last_login_at, created_at, updated_at
        ) VALUES (
          @userId, @email, NULL, NULL, NULL, NOW(), NOW()
        )
        ON CONFLICT (user_id) DO UPDATE SET
          email = EXCLUDED.email,
          updated_at = NOW()
      `, { userId, email: normalizedEmail });
    }
    else {
        await query(`
        MERGE ${table("user_credentials")} AS target
        USING (
          SELECT @userId AS user_id, @email AS email
        ) AS source
        ON target.user_id = source.user_id
        WHEN MATCHED THEN
          UPDATE SET
            email = source.email,
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (user_id, email, password_salt, password_hash, last_login_at, created_at, updated_at)
          VALUES (source.user_id, source.email, NULL, NULL, NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
      `, { userId, email: normalizedEmail });
    }
    // Grant default module access (planning)
    if (config.usePg) {
        await query(`INSERT INTO ${table("user_modules")} (user_id, module_id) VALUES (@userId, 'planning') ON CONFLICT DO NOTHING`, { userId });
    }
    return { userId, email: normalizedEmail };
}
export async function updateUserRole(userId, roleId) {
    if (!config.usePg)
        fail(400, "Role management requires PostgreSQL mode.");
    const rows = await query(`SELECT name FROM ${table("roles")} WHERE role_id = @roleId`, { roleId });
    if (rows.length === 0)
        fail(404, "Role not found.");
    await query(`UPDATE ${table("users")} SET role = LOWER(@roleName), role_id = @roleId, updated_at = NOW() WHERE user_id = @userId`, { roleName: rows[0].name, roleId, userId });
    // Invalidate cached sessions for this user so they pick up new permissions
    for (const [token, cached] of sessionCache.entries()) {
        if (cached.user.userId === userId) {
            sessionCache.delete(token);
        }
    }
}
export async function resetManagedUserPassword(userId) {
    await ensureAuthTablesExist();
    const rows = await query(`
      SELECT user_id, email, role, is_active
      FROM ${table("users")}
      WHERE user_id = @userId
      LIMIT 1
    `, { userId });
    const user = rows[0];
    if (!user) {
        fail(404, "User not found.");
    }
    if (config.usePg) {
        await query(`
        INSERT INTO ${table("user_credentials")} (
          user_id, email, password_salt, password_hash, last_login_at, created_at, updated_at
        ) VALUES (
          @userId, @email, NULL, NULL, NULL, NOW(), NOW()
        )
        ON CONFLICT (user_id) DO UPDATE SET
          email = EXCLUDED.email,
          password_salt = NULL,
          password_hash = NULL,
          updated_at = NOW()
      `, { userId: user.user_id, email: user.email });
    }
    else {
        await query(`
        MERGE ${table("user_credentials")} AS target
        USING (
          SELECT @userId AS user_id, @email AS email
        ) AS source
        ON target.user_id = source.user_id
        WHEN MATCHED THEN
          UPDATE SET
            email = source.email,
            password_salt = NULL,
            password_hash = NULL,
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (user_id, email, password_salt, password_hash, last_login_at, created_at, updated_at)
          VALUES (source.user_id, source.email, NULL, NULL, NULL, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
      `, { userId: user.user_id, email: user.email });
    }
}
