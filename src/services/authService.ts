import { randomBytes, randomUUID, scryptSync, timingSafeEqual } from "node:crypto";
import { config } from "../config.js";
import { query, table } from "../db/bigquery.js";

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
  role: "admin" | "planner" | "viewer";
  is_active: boolean;
  last_login_at: string | null;
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
  if (!authTablesReady) {
    authTablesReady = (async () => {
      await query(
        `
          CREATE TABLE IF NOT EXISTS ${table("users")} (
            user_id STRING NOT NULL,
            email STRING NOT NULL,
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
  const rows = await query<UserCredentialRecord>(
    `
      SELECT
        user_id,
        email,
        password_salt,
        password_hash,
        CAST(last_login_at AS STRING) AS last_login_at
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
          TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY),
          CURRENT_TIMESTAMP(),
          CURRENT_TIMESTAMP()
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
        INSERT (
          user_id,
          email,
          password_salt,
          password_hash,
          last_login_at,
          created_at,
          updated_at
        )
        VALUES (
          source.user_id,
          source.email,
          source.password_salt,
          source.password_hash,
          CURRENT_TIMESTAMP(),
          CURRENT_TIMESTAMP(),
          CURRENT_TIMESTAMP()
        )
    `,
    {
      userId: user.user_id,
      email: user.email,
      salt,
      hashed
    }
  );

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

export async function validateSessionToken(token: string): Promise<SessionUser | null> {
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

  return {
    userId: session.user_id,
    email: session.email,
    role: session.role
  };
}

export async function logoutSession(token: string): Promise<void> {
  await ensureAuthTablesExist();
  await query(
    `
      DELETE FROM ${table("auth_sessions")}
      WHERE session_token = @token
    `,
    { token }
  );
}

export async function listManagedUsers(): Promise<ManagedUserRow[]> {
  await ensureAuthTablesExist();
  return query<ManagedUserRow>(
    `
      SELECT
        u.user_id,
        u.email,
        u.role,
        u.is_active,
        CAST(c.last_login_at AS STRING) AS last_login_at
      FROM ${table("users")} AS u
      LEFT JOIN ${table("user_credentials")} AS c
        ON c.user_id = u.user_id
      ORDER BY LOWER(u.email)
    `
  );
}

export async function addManagedUser(email: string): Promise<{ userId: string; email: string }> {
  await ensureAuthTablesExist();
  const normalizedEmail = normalizeEmail(email);
  if (!normalizedEmail) {
    fail(400, "Email is required.");
  }

  const existing = await getUserByEmail(normalizedEmail);
  const userId = existing?.user_id || randomUUID();
  if (!existing) {
    await query(
      `
        INSERT INTO ${table("users")} (
          user_id,
          email,
          role,
          is_active,
          created_at,
          updated_at
        )
        VALUES (
          @userId,
          @email,
          'planner',
          TRUE,
          CURRENT_TIMESTAMP(),
          CURRENT_TIMESTAMP()
        )
      `,
      { userId, email: normalizedEmail }
    );
  }

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
        INSERT (
          user_id,
          email,
          password_salt,
          password_hash,
          last_login_at,
          created_at,
          updated_at
        )
        VALUES (
          source.user_id,
          source.email,
          NULL,
          NULL,
          NULL,
          CURRENT_TIMESTAMP(),
          CURRENT_TIMESTAMP()
        )
    `,
    { userId, email: normalizedEmail }
  );

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
        INSERT (
          user_id,
          email,
          password_salt,
          password_hash,
          last_login_at,
          created_at,
          updated_at
        )
        VALUES (
          source.user_id,
          source.email,
          NULL,
          NULL,
          NULL,
          CURRENT_TIMESTAMP(),
          CURRENT_TIMESTAMP()
        )
    `,
    { userId: user.user_id, email: user.email }
  );
}
