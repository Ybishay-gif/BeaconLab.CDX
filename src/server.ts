import express from "express";
import compression from "compression";
import cors from "cors";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { ZodError } from "zod";
import { config } from "./config.js";
import { healthRouter } from "./routes/health.js";
import { plansRouter } from "./routes/plans.js";
import { ALL_PERMISSIONS, DEFAULT_ROLE_PERMISSIONS } from "./permissions.js";

const app = express();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Trust proxy headers (Cloud Run sits behind a load balancer)
app.set("trust proxy", 1);

// Hide server technology fingerprint
app.disable("x-powered-by");

// Gzip / Brotli compression for JSON responses
app.use(compression());

// Security headers
app.use((_req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("X-XSS-Protection", "0"); // Modern browsers: rely on CSP instead
  res.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");
  res.setHeader("Strict-Transport-Security", "max-age=31536000; includeSubDomains");
  res.setHeader("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self'; frame-ancestors 'none'");
  next();
});

// CORS — allow Cloudflare Pages frontend and local dev
app.use(
  cors({
    origin: [
      "https://beacon-lab-v2.pages.dev",
      /\.beacon-lab-v2\.pages\.dev$/,
      "http://localhost:5173",
      "http://localhost:5174",
    ],
    credentials: true,
    methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization", "x-session-token"],
  })
);

app.use(express.json({ limit: "15mb" }));
app.use(express.static(path.resolve(__dirname, "../public")));

app.use(healthRouter);
app.use("/api", plansRouter);

app.use((err: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  // JSON parse errors from express.json() — return generic message, don't leak parser details
  if (err instanceof SyntaxError && "type" in err && (err as { type?: string }).type === "entity.parse.failed") {
    res.status(400).json({ error: "Invalid request body" });
    return;
  }

  if (err instanceof ZodError) {
    const summary = err.issues.map((i) => `${i.path.join(".")}: ${i.message}`).join("; ");
    console.error(`[zod] Validation failed: ${summary}`);
    res.status(400).json({ error: `Invalid request: ${summary}`, details: err.issues });
    return;
  }

  const status = typeof err === "object" && err !== null ? (err as { status?: unknown }).status : undefined;
  if (typeof status === "number" && status >= 400 && status < 600) {
    const message =
      typeof err === "object" && err !== null && "message" in err ? String((err as { message: unknown }).message) : "";
    res.status(status).json({ error: message || "Request failed" });
    return;
  }

  console.error(err);
  res.status(500).json({ error: "Internal server error" });
});

app.get("*", (_req, res) => {
  res.sendFile(path.resolve(__dirname, "../public/index.html"));
});

async function runMigrations() {
  if (!config.usePg) return;
  try {
    const { pgExec } = await import("./db/postgres.js");
    await pgExec("ALTER TABLE targets ADD COLUMN IF NOT EXISTS target_cor DOUBLE PRECISION NOT NULL DEFAULT 0");
    await pgExec("ALTER TABLE users ADD COLUMN IF NOT EXISTS name TEXT");

    // Add activity_type / lead_type to price_exploration_daily (pre-existing rows get '')
    await pgExec("ALTER TABLE price_exploration_daily ADD COLUMN IF NOT EXISTS activity_type TEXT DEFAULT ''");
    await pgExec("ALTER TABLE price_exploration_daily ADD COLUMN IF NOT EXISTS lead_type TEXT DEFAULT ''");
    await pgExec("CREATE INDEX IF NOT EXISTS idx_ped_activity ON price_exploration_daily (activity_type, lead_type)").catch(() => {});

    // Usage analytics table
    await pgExec(`
      CREATE TABLE IF NOT EXISTS usage_events (
        id            BIGSERIAL PRIMARY KEY,
        session_id    TEXT        NOT NULL,
        user_id       TEXT        NOT NULL,
        user_email    TEXT        NOT NULL,
        event_type    TEXT        NOT NULL,
        page          TEXT,
        action        TEXT,
        metadata      JSONB       DEFAULT '{}',
        created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pgExec("CREATE INDEX IF NOT EXISTS idx_usage_events_created_at ON usage_events (created_at DESC)");
    await pgExec("CREATE INDEX IF NOT EXISTS idx_usage_events_user_id ON usage_events (user_id, created_at DESC)");
    await pgExec("CREATE INDEX IF NOT EXISTS idx_usage_events_event_type ON usage_events (event_type, created_at DESC)");
    await pgExec("CREATE INDEX IF NOT EXISTS idx_usage_events_session_id ON usage_events (session_id)");

    // Module access per user
    await pgExec(`
      CREATE TABLE IF NOT EXISTS user_modules (
        user_id   TEXT NOT NULL REFERENCES users(user_id),
        module_id TEXT NOT NULL,
        granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (user_id, module_id)
      )
    `);
    // Backfill: give all existing users access to 'planning'
    await pgExec(`
      INSERT INTO user_modules (user_id, module_id)
      SELECT user_id, 'planning' FROM users
      ON CONFLICT DO NOTHING
    `);

    // Add module column to change_log and usage_events (DEFAULT backfills existing rows)
    await pgExec("ALTER TABLE change_log ADD COLUMN IF NOT EXISTS module TEXT NOT NULL DEFAULT 'planning'");
    await pgExec("ALTER TABLE usage_events ADD COLUMN IF NOT EXISTS module TEXT NOT NULL DEFAULT 'planning'");
    await pgExec("CREATE INDEX IF NOT EXISTS idx_usage_events_module ON usage_events (module, created_at DESC)");

    // Chat tables
    await pgExec(`
      CREATE TABLE IF NOT EXISTS chat_rooms (
        room_id   BIGSERIAL PRIMARY KEY,
        room_type TEXT NOT NULL CHECK (room_type IN ('general', 'dm')),
        room_name TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pgExec(`
      INSERT INTO chat_rooms (room_id, room_type, room_name)
      VALUES (1, 'general', 'General')
      ON CONFLICT (room_id) DO NOTHING
    `);
    await pgExec(`SELECT setval('chat_rooms_room_id_seq', GREATEST(1, (SELECT MAX(room_id) FROM chat_rooms)))`);

    await pgExec(`
      CREATE TABLE IF NOT EXISTS chat_room_members (
        room_id  BIGINT NOT NULL REFERENCES chat_rooms(room_id) ON DELETE CASCADE,
        user_id  TEXT   NOT NULL,
        joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (room_id, user_id)
      )
    `);

    await pgExec(`
      CREATE TABLE IF NOT EXISTS chat_messages (
        message_id   BIGSERIAL PRIMARY KEY,
        room_id      BIGINT NOT NULL REFERENCES chat_rooms(room_id) ON DELETE CASCADE,
        sender_id    TEXT   NOT NULL,
        sender_email TEXT   NOT NULL,
        content      JSONB  NOT NULL,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pgExec("CREATE INDEX IF NOT EXISTS idx_chat_messages_room_created ON chat_messages (room_id, created_at DESC)");

    await pgExec(`
      CREATE TABLE IF NOT EXISTS chat_read_status (
        room_id      BIGINT NOT NULL REFERENCES chat_rooms(room_id) ON DELETE CASCADE,
        user_id      TEXT   NOT NULL,
        last_read_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (room_id, user_id)
      )
    `);

    // Tickets table (bug reports & feature requests)
    await pgExec(`
      CREATE TABLE IF NOT EXISTS tickets (
        ticket_id        TEXT PRIMARY KEY,
        ticket_number    SERIAL,
        type             TEXT NOT NULL CHECK (type IN ('bug', 'feature')),
        status           TEXT NOT NULL DEFAULT 'todo' CHECK (status IN ('todo', 'in_progress', 'done')),
        title            TEXT NOT NULL,
        description      TEXT NOT NULL DEFAULT '',
        module           TEXT NOT NULL,
        page             TEXT NOT NULL,
        attachments      JSONB DEFAULT '[]',
        created_by       TEXT NOT NULL,
        created_by_email TEXT NOT NULL,
        assigned_to      TEXT,
        resolved_at      TIMESTAMPTZ,
        resolution_notes TEXT,
        created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pgExec("CREATE INDEX IF NOT EXISTS idx_tickets_status ON tickets (status, created_at DESC)");
    await pgExec("CREATE INDEX IF NOT EXISTS idx_tickets_created_by ON tickets (created_by)");
    await pgExec("CREATE INDEX IF NOT EXISTS idx_tickets_type ON tickets (type)");
    await pgExec("CREATE INDEX IF NOT EXISTS idx_tickets_module ON tickets (module)");

    // v2: Approval-based workflow statuses + test_results column
    await pgExec(`
      DO $$ BEGIN
        ALTER TABLE tickets DROP CONSTRAINT IF EXISTS tickets_status_check;
        ALTER TABLE tickets ADD CONSTRAINT tickets_status_check
          CHECK (status IN ('todo','approved','coded','pending_review','deploy_approved','deployed'));
        -- Migrate old statuses
        UPDATE tickets SET status = 'deployed' WHERE status = 'done';
        UPDATE tickets SET status = 'coded' WHERE status = 'in_progress';
      EXCEPTION WHEN others THEN NULL;
      END $$
    `);
    await pgExec(`
      ALTER TABLE tickets ADD COLUMN IF NOT EXISTS test_results TEXT
    `);
    await pgExec(`
      ALTER TABLE tickets ADD COLUMN IF NOT EXISTS documentation JSONB DEFAULT '[]'
    `);
    await pgExec(`
      ALTER TABLE tickets ADD COLUMN IF NOT EXISTS test_checklist JSONB DEFAULT '[]'
    `);

    // v3: Spec-driven workflow statuses + new columns + activity/comments tables
    // First drop old constraint and migrate rows, then add new constraint
    await pgExec(`
      DO $$ BEGIN
        ALTER TABLE tickets DROP CONSTRAINT IF EXISTS tickets_status_check;
      EXCEPTION WHEN others THEN NULL;
      END $$
    `);
    await pgExec("UPDATE tickets SET status = 'pending_spec' WHERE status = 'approved'");
    await pgExec("UPDATE tickets SET status = 'pending_deployment' WHERE status IN ('coded', 'pending_review')");
    await pgExec("UPDATE tickets SET status = 'deployment_approved' WHERE status = 'deploy_approved'");
    await pgExec(`
      DO $$ BEGIN
        ALTER TABLE tickets ADD CONSTRAINT tickets_status_check
          CHECK (status IN (
            'todo','pending_spec','pending_spec_approval','spec_approved',
            'adjusted_spec','pending_deployment','deployment_approved','deployed'
          ));
      EXCEPTION WHEN others THEN NULL;
      END $$
    `);
    // Spec phase columns
    await pgExec("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS complexity TEXT DEFAULT 'medium'");
    await pgExec("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS functional_spec TEXT");
    await pgExec("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS design_notes TEXT");
    await pgExec("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS ui_mockup JSONB");
    await pgExec("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS testing_scenarios JSONB DEFAULT '[]'");
    // Dev phase columns
    await pgExec("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS dev_summary TEXT");
    await pgExec("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS code_changes JSONB DEFAULT '[]'");
    await pgExec("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS dev_test_results TEXT");
    await pgExec("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS dev_evidence JSONB DEFAULT '[]'");
    // Deploy phase columns
    await pgExec("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS deploy_info TEXT");
    await pgExec("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS prod_test_results TEXT");
    await pgExec("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS prod_evidence JSONB DEFAULT '[]'");

    // Activity log table
    await pgExec(`
      CREATE TABLE IF NOT EXISTS ticket_activity_log (
        log_id         TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        ticket_id      TEXT NOT NULL REFERENCES tickets(ticket_id) ON DELETE CASCADE,
        action         TEXT NOT NULL,
        old_value      TEXT,
        new_value      TEXT,
        details        TEXT,
        user_id        TEXT NOT NULL,
        user_email     TEXT NOT NULL,
        created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pgExec("CREATE INDEX IF NOT EXISTS idx_ticket_activity_ticket ON ticket_activity_log (ticket_id, created_at)");

    // Comments table
    await pgExec(`
      CREATE TABLE IF NOT EXISTS ticket_comments (
        comment_id     TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        ticket_id      TEXT NOT NULL REFERENCES tickets(ticket_id) ON DELETE CASCADE,
        user_id        TEXT NOT NULL,
        user_email     TEXT NOT NULL,
        body           TEXT NOT NULL,
        created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pgExec("CREATE INDEX IF NOT EXISTS idx_ticket_comments_ticket ON ticket_comments (ticket_id, created_at)");

    // v4: Add done + reopened statuses
    await pgExec(`
      DO $$ BEGIN
        ALTER TABLE tickets DROP CONSTRAINT IF EXISTS tickets_status_check;
      EXCEPTION WHEN others THEN NULL;
      END $$
    `);
    await pgExec(`
      DO $$ BEGIN
        ALTER TABLE tickets ADD CONSTRAINT tickets_status_check
          CHECK (status IN (
            'todo','pending_spec','pending_spec_approval','spec_approved',
            'adjusted_spec','pending_deployment','deployment_approved','deployed',
            'done','reopened'
          ));
      EXCEPTION WHEN others THEN NULL;
      END $$
    `);

    // Reports table (custom report generator)
    await pgExec(`
      CREATE TABLE IF NOT EXISTS reports (
        report_id        TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        report_name      TEXT NOT NULL,
        user_id          TEXT NOT NULL,
        date_start       DATE NOT NULL,
        date_end         DATE NOT NULL,
        fixed_filters    JSONB NOT NULL DEFAULT '{}',
        dynamic_filters  JSONB NOT NULL DEFAULT '[]',
        selected_columns JSONB NOT NULL DEFAULT '[]',
        status           TEXT NOT NULL DEFAULT 'pending'
                           CHECK (status IN ('pending','processing','done','error')),
        file_url         TEXT,
        row_count        INTEGER,
        error_message    TEXT,
        created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        completed_at     TIMESTAMPTZ
      )
    `);
    await pgExec("CREATE INDEX IF NOT EXISTS idx_reports_user ON reports(user_id, created_at DESC)");
    await pgExec("CREATE INDEX IF NOT EXISTS idx_reports_status ON reports(status)");
    await pgExec("ALTER TABLE reports ADD COLUMN IF NOT EXISTS include_opps BOOLEAN NOT NULL DEFAULT false");

    // Report templates table
    await pgExec(`
      CREATE TABLE IF NOT EXISTS report_templates (
        template_id      TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        user_id          TEXT NOT NULL,
        template_name    TEXT NOT NULL,
        fixed_filters    JSONB NOT NULL DEFAULT '{}',
        dynamic_filters  JSONB NOT NULL DEFAULT '[]',
        selected_columns JSONB NOT NULL DEFAULT '[]',
        include_opps     BOOLEAN NOT NULL DEFAULT false,
        created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pgExec("CREATE INDEX IF NOT EXISTS idx_report_templates_user ON report_templates(user_id, created_at DESC)");

    // ── Roles & Permissions ────────────────────────────────────────
    await pgExec(`
      CREATE TABLE IF NOT EXISTS roles (
        role_id    TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        name       TEXT NOT NULL UNIQUE,
        is_system  BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pgExec(`
      CREATE TABLE IF NOT EXISTS role_permissions (
        role_id        TEXT NOT NULL REFERENCES roles(role_id) ON DELETE CASCADE,
        permission_key TEXT NOT NULL,
        PRIMARY KEY (role_id, permission_key)
      )
    `);
    await pgExec("CREATE INDEX IF NOT EXISTS idx_role_permissions_key ON role_permissions(permission_key)");

    // Add role_id FK to users and auth_sessions
    await pgExec("ALTER TABLE users ADD COLUMN IF NOT EXISTS role_id TEXT REFERENCES roles(role_id)");
    await pgExec("ALTER TABLE auth_sessions ADD COLUMN IF NOT EXISTS role_id TEXT");

    // Seed system roles (idempotent) — uses query() from db layer for SELECT/INSERT RETURNING
    const { query: dbQuery } = await import("./db/index.js");
    for (const [roleName, permissions] of Object.entries(DEFAULT_ROLE_PERMISSIONS)) {
      const rows = await dbQuery<{ role_id: string }>(
        `SELECT role_id FROM roles WHERE name = @roleName`,
        { roleName }
      );
      let roleId: string;
      if (rows.length === 0) {
        const inserted = await dbQuery<{ role_id: string }>(
          `INSERT INTO roles (name, is_system) VALUES (@roleName, TRUE) RETURNING role_id`,
          { roleName }
        );
        roleId = inserted[0].role_id;
      } else {
        roleId = rows[0].role_id;
      }
      // Sync permissions for this role
      await dbQuery(`DELETE FROM role_permissions WHERE role_id = @roleId`, { roleId });
      for (const perm of permissions) {
        await dbQuery(
          `INSERT INTO role_permissions (role_id, permission_key) VALUES (@roleId, @perm) ON CONFLICT DO NOTHING`,
          { roleId, perm }
        );
      }
    }

    // Backfill: assign role_id to existing users based on role text column
    await pgExec(`
      UPDATE users SET role_id = (
        SELECT role_id FROM roles WHERE LOWER(roles.name) = LOWER(users.role)
      )
      WHERE role_id IS NULL AND role IS NOT NULL
    `);

    // Backfill: assign role_id to existing sessions based on role text column
    await pgExec(`
      UPDATE auth_sessions SET role_id = (
        SELECT role_id FROM roles WHERE LOWER(roles.name) = LOWER(auth_sessions.role)
      )
      WHERE role_id IS NULL AND role IS NOT NULL
    `);

    // AI Chat sessions & messages — persistent conversation history
    await pgExec(`
      CREATE TABLE IF NOT EXISTS ai_chat_sessions (
        session_id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        title TEXT NOT NULL DEFAULT 'New conversation',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pgExec("CREATE INDEX IF NOT EXISTS idx_ai_chat_sessions_user ON ai_chat_sessions(user_id, updated_at DESC)");

    await pgExec(`
      CREATE TABLE IF NOT EXISTS ai_chat_messages (
        message_id BIGSERIAL PRIMARY KEY,
        session_id TEXT NOT NULL REFERENCES ai_chat_sessions(session_id) ON DELETE CASCADE,
        role TEXT NOT NULL CHECK (role IN ('user', 'model')),
        content TEXT NOT NULL,
        sql_query TEXT,
        action JSONB,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pgExec("CREATE INDEX IF NOT EXISTS idx_ai_chat_messages_session ON ai_chat_messages(session_id, created_at ASC)");

    // SFTP Connections & Uploads
    await pgExec(`
      CREATE TABLE IF NOT EXISTS sftp_connections (
        connection_id      TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        name               TEXT NOT NULL,
        host               TEXT NOT NULL,
        port               INTEGER NOT NULL DEFAULT 22,
        username           TEXT NOT NULL,
        password_encrypted TEXT NOT NULL,
        remote_path        TEXT NOT NULL DEFAULT '/',
        is_active          BOOLEAN NOT NULL DEFAULT TRUE,
        created_by         TEXT NOT NULL,
        created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);
    await pgExec("CREATE INDEX IF NOT EXISTS idx_sftp_connections_active ON sftp_connections(is_active)");
    await pgExec(`
      CREATE TABLE IF NOT EXISTS sftp_uploads (
        upload_id      TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
        report_id      TEXT NOT NULL REFERENCES reports(report_id) ON DELETE CASCADE,
        connection_id  TEXT NOT NULL REFERENCES sftp_connections(connection_id) ON DELETE CASCADE,
        status         TEXT NOT NULL DEFAULT 'pending'
                         CHECK (status IN ('pending','uploading','done','error')),
        remote_file    TEXT,
        error_message  TEXT,
        initiated_by   TEXT NOT NULL,
        created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        completed_at   TIMESTAMPTZ
      )
    `);
    await pgExec("CREATE INDEX IF NOT EXISTS idx_sftp_uploads_report ON sftp_uploads(report_id)");
    await pgExec("CREATE INDEX IF NOT EXISTS idx_sftp_uploads_status ON sftp_uploads(status)");

    console.log("Migrations OK");
  } catch (err) {
    console.warn("Migration warning (non-fatal):", err);
  }
}

async function main() {
  await runMigrations();
  app.listen(config.port, () => {
    console.log(`planning-app-api listening on port ${config.port}`);
  });
}

main().catch((err) => {
  console.error("Startup failed:", err);
  process.exit(1);
});
