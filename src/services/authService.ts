import { randomBytes, randomUUID, scryptSync, timingSafeEqual } from "node:crypto";
import { config } from "../config.js";
import { query, table } from "../db/index.js";
import { VALID_MODULE_IDS, isValidModuleId } from "../modules.js";

type UserRecord = {
  user_id: string;
  email: string;
  role: "admin" | "planner" | "viewer";
  is_active: boolean;
};

type UserCredentialRecord = {
  user_id: string;
  email: string;
  password_salt: string | null;
  password_hash: string | null;
  last_login_at: string | null;
};

type SessionRecord = {
  session_token: string;
  user_id: string;
  email: string;
  role: "admin" | "planner" | "viewer";
};

export type SessionUser = {
  userId: string;
  email: string;
  role: "admin" | "planner" | "viewer";
};

type ManagedUserRow = {
  user_id: string;
  email: string;
  name: string | null;
  role: "admin" | "planner" | "viewer";
  active: boolean;
  last_login: string | null;
  created_at: string;
  modules: string[];
};

let authTablesReady: Promise<void> | null = null;

function isSerializableConflict(error: unknown): boolean {
  const message =
    typeof error === "object" && error !== null && "message" in error ? String((error as { message: unknown }).message) : "";
  return message.toLowerCase().includes("could not serialize access to table");
}

async function withSerializableRetry<T>(operation: () => Promise<T>, retries = 2): Promise<T> {
  let attempt = 0;
  while (true) {
    try {
      return await operation();
    } catch (error) {
      attempt += 1;
      if (attempt > retries || !isSerializableConflict(error)) {
        throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, 100 * attempt));
    }
  }
}

function fail(status: number, message: string): never {
  const error = new Error(message) as Error & { status: number };
  error.status = status;
  throw error;
}

function normalizeEmail(email: string): string {
  return String(email || "").trim().toLowerCase();
}

function hashPassword(password: string, salt: string): string {
  return scryptSync(password, salt, 64).toString("hex");
}

function validatePasswordStrength(password: string): void {
  if (password.length < 8) {
    fail(400, "Password must be at least 8 characters.");
  }
}

async function ensureAuthTablesExist(): Promise<void> {
  // PG schema created via migration — no runtime DDL needed
  if (config.usePg) return;

  if (!authTablesReady) {
    authTablesReady = (async () => {
      await query(
        `
          CREATE TABLE IF NOT EXISTS ${table("users")} (
            user_id STRING NOT NULL,
            email STRING NOT NULL,
            name STRING,
            role STRING NOT NULL,
            is_active BOOL NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP
          )
        `
      );
      await query(
        `
          CREATE TABLE IF NOT EXISTS ${table("user_credentials")} (
            user_id STRING NOT NULL,
            email STRING NOT NULL,
            password_salt STRING,
            password_hash STRING,
            last_login_at TIMESTAMP,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP
          )
        `
      );
      await query(
        `
          CREATE TABLE IF NOT EXISTS ${table("auth_sessions")} (
            session_token STRING NOT NULL,
            user_id STRING NOT NULL,
            email STRING NOT NULL,
            role STRING NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP NOT NULL,
            last_seen_at TIMESTAMP NOT NULL
          )
        `
      );
    })();
  }
  return authTablesReady;
}

async function getUserByEmail(email: string): Promise<UserRecord | null> {
  const rows = await query<UserRecord>(
    `
      SELECT user_id, email, role, is_active
      FROM ${table("users")}
      WHERE LOWER(email) = @email
      LIMIT 1
    `,
    { email: normalizeEmail(email) }
  );
  return rows[0] || null;
}

async function getCredentialsByUserId(userId: string): Promise<UserCredentialRecord | null> {
  const castLogin = config.usePg ? "last_login_at::text" : "CAST(last_login_at AS STRING)";
  const rows = await query<UserCredentialRecord>(
    `
      SELECT
        user_id,
        email,
        password_salt,
        password_hash,
        ${castLogin} AS last_login_at
      FROM ${table("user_credentials")}
      WHERE user_id = @userId
      LIMIT 1
    `,
    { userId }
  );
  return rows[0] || null;
}

async function createSession(user: SessionUser): Promise<{ token: string; user: SessionUser }> {
  const token = randomBytes(32).toString("hex");
  const expiresExpr = config.usePg
    ? "NOW() + INTERVAL '14 days'"
    : "TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)";
  const now = config.usePg ? "NOW()" : "CURRENT_TIMESTAMP()";
  await withSerializableRetry(() =>
    query(
      `
        INSERT INTO ${table("auth_sessions")} (
          session_token,
          user_id,
          email,
          role,
          expires_at,
          created_at,
          last_seen_at
        )
        VALUES (
          @token,
          @userId,
          @email,
          @role,
          ${expiresExpr},
          ${now},
          ${now}
        )
      `,
      {
        token,
        userId: user.userId,
        email: user.email,
        role: user.role
      }
    )
  );
  return { token, user };
}

export async function getUserLoginState(
  email: string
): Promise<{ exists: boolean; requiresPasswordSetup: boolean }> {
  await ensureAuthTablesExist();
  const user = await getUserByEmail(email);
  if (!user || !user.is_active) {
    return { exists: false, requiresPasswordSetup: false };
  }
  const credential = await getCredentialsByUserId(user.user_id);
  const requiresPasswordSetup = !credential?.password_hash || !credential?.password_salt;
  return { exists: true, requiresPasswordSetup };
}

export async function setupUserPassword(
  email: string,
  password: string
): Promise<{ token: string; user: SessionUser }> {
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
    await query(
      `
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
      `,
      { userId: user.user_id, email: user.email, salt, hashed }
    );
  } else {
    await query(
      `
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
      `,
      { userId: user.user_id, email: user.email, salt, hashed }
    );
  }

  return createSession({
    userId: user.user_id,
    email: user.email,
    role: user.role
  });
}

export async function loginUser(email: string, password: string): Promise<{ token: string; user: SessionUser }> {
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

  await query(
    `
      UPDATE ${table("user_credentials")}
      SET
        last_login_at = CURRENT_TIMESTAMP(),
        updated_at = CURRENT_TIMESTAMP()
      WHERE user_id = @userId
    `,
    { userId: user.user_id }
  );

  return createSession({
    userId: user.user_id,
    email: user.email,
    role: user.role
  });
}

export async function loginAdminWithCode(code: string): Promise<{ token: string; user: SessionUser }> {
  await ensureAuthTablesExist();
  if (String(code || "").trim() !== config.adminAccessCode) {
    fail(401, "Invalid admin access code.");
  }
  return createSession({
    userId: "admin-code",
    email: "admin@local",
    role: "admin"
  });
}

// ── Session cache: avoids a BQ round-trip on every authenticated request ──
const sessionCache = new Map<string, { user: SessionUser; expiresAt: number }>();
const SESSION_CACHE_TTL_MS = 2 * 60 * 1000; // 2 minutes

export async function validateSessionToken(token: string): Promise<SessionUser | null> {
  // Fast-path: return from in-memory cache
  const cached = sessionCache.get(token);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.user;
  }

  await ensureAuthTablesExist();
  const rows = await query<SessionRecord>(
    `
      SELECT session_token, user_id, email, role
      FROM ${table("auth_sessions")}
      WHERE session_token = @token
        AND expires_at > CURRENT_TIMESTAMP()
      ORDER BY created_at DESC
      LIMIT 1
    `,
    { token }
  );

  const session = rows[0];
  if (!session) {
    sessionCache.delete(token);
    return null;
  }

  void withSerializableRetry(() =>
    query(
      `
        UPDATE ${table("auth_sessions")}
        SET last_seen_at = CURRENT_TIMESTAMP()
        WHERE session_token = @token
      `,
      { token }
    )
  ).catch(() => undefined);

  const user: SessionUser = {
    userId: session.user_id,
    email: session.email,
    role: session.role
  };

  sessionCache.set(token, { user, expiresAt: Date.now() + SESSION_CACHE_TTL_MS });
  return user;
}

export async function logoutSession(token: string): Promise<void> {
  sessionCache.delete(token);
  await ensureAuthTablesExist();
  await query(
    `
      DELETE FROM ${table("auth_sessions")}
      WHERE session_token = @token
    `,
    { token }
  );
}

// ── Module access ────────────────────────────────────────────────
export async function getUserModules(userId: string): Promise<string[]> {
  if (!config.usePg) return VALID_MODULE_IDS; // BQ: no module table, grant all
  const rows = await query<{ module_id: string }>(
    `SELECT module_id FROM ${table("user_modules")} WHERE user_id = @userId`,
    { userId }
  );
  return rows.map((r) => r.module_id);
}

export async function setUserModules(userId: string, modules: string[]): Promise<void> {
  const valid = modules.filter(isValidModuleId);
  if (valid.length === 0) {
    const error = new Error("At least one valid module is required.") as Error & { status: number };
    error.status = 400;
    throw error;
  }
  await query(`DELETE FROM ${table("user_modules")} WHERE user_id = @userId`, { userId });
  for (const moduleId of valid) {
    await query(
      `INSERT INTO ${table("user_modules")} (user_id, module_id) VALUES (@userId, @moduleId)`,
      { userId, moduleId }
    );
  }
}

export async function listManagedUsers(): Promise<ManagedUserRow[]> {
  await ensureAuthTablesExist();
  const castLogin = config.usePg ? "c.last_login_at::text" : "CAST(c.last_login_at AS STRING)";
  const castCreated = config.usePg ? "u.created_at::text" : "CAST(u.created_at AS STRING)";

  if (config.usePg) {
    // PG: use array_agg to include modules in a single query
    type RawRow = Omit<ManagedUserRow, "modules"> & { modules: string[] | null };
    const rows = await query<RawRow>(
      `
        SELECT
          u.user_id,
          u.email,
          u.name,
          u.role,
          u.is_active AS active,
          ${castLogin} AS last_login,
          ${castCreated} AS created_at,
          COALESCE(array_agg(m.module_id) FILTER (WHERE m.module_id IS NOT NULL), '{}') AS modules
        FROM ${table("users")} AS u
        LEFT JOIN ${table("user_credentials")} AS c
          ON c.user_id = u.user_id
        LEFT JOIN ${table("user_modules")} AS m
          ON m.user_id = u.user_id
        GROUP BY u.user_id, u.email, u.name, u.role, u.is_active, c.last_login_at, u.created_at
        ORDER BY LOWER(u.email)
      `
    );
    return rows.map((r) => ({ ...r, modules: r.modules ?? [] }));
  }

  // BQ fallback: no module table, grant all modules
  const rows = await query<Omit<ManagedUserRow, "modules">>(
    `
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
    `
  );
  return rows.map((r) => ({ ...r, modules: VALID_MODULE_IDS }));
}

export async function addManagedUser(
  email: string,
  opts?: { name?: string; role?: string }
): Promise<{ userId: string; email: string }> {
  await ensureAuthTablesExist();
  const normalizedEmail = normalizeEmail(email);
  if (!normalizedEmail) {
    fail(400, "Email is required.");
  }

  const name = opts?.name?.trim() || null;
  const role = opts?.role === "admin" ? "admin" : "planner";

  const existing = await getUserByEmail(normalizedEmail);
  const userId = existing?.user_id || randomUUID();
  if (!existing) {
    await query(
      `
        INSERT INTO ${table("users")} (
          user_id,
          email,
          name,
          role,
          is_active,
          created_at,
          updated_at
        )
        VALUES (
          @userId,
          @email,
          @name,
          @role,
          TRUE,
          CURRENT_TIMESTAMP(),
          CURRENT_TIMESTAMP()
        )
      `,
      { userId, email: normalizedEmail, name, role }
    );
  }

  if (config.usePg) {
    await query(
      `
        INSERT INTO ${table("user_credentials")} (
          user_id, email, password_salt, password_hash, last_login_at, created_at, updated_at
        ) VALUES (
          @userId, @email, NULL, NULL, NULL, NOW(), NOW()
        )
        ON CONFLICT (user_id) DO UPDATE SET
          email = EXCLUDED.email,
          updated_at = NOW()
      `,
      { userId, email: normalizedEmail }
    );
  } else {
    await query(
      `
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
      `,
      { userId, email: normalizedEmail }
    );
  }

  // Grant default module access (planning)
  if (config.usePg) {
    await query(
      `INSERT INTO ${table("user_modules")} (user_id, module_id) VALUES (@userId, 'planning') ON CONFLICT DO NOTHING`,
      { userId }
    );
  }

  return { userId, email: normalizedEmail };
}

export async function resetManagedUserPassword(userId: string): Promise<void> {
  await ensureAuthTablesExist();
  const rows = await query<UserRecord>(
    `
      SELECT user_id, email, role, is_active
      FROM ${table("users")}
      WHERE user_id = @userId
      LIMIT 1
    `,
    { userId }
  );
  const user = rows[0];
  if (!user) {
    fail(404, "User not found.");
  }

  if (config.usePg) {
    await query(
      `
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
      `,
      { userId: user.user_id, email: user.email }
    );
  } else {
    await query(
      `
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
      `,
      { userId: user.user_id, email: user.email }
    );
  }
}
