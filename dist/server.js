import express from "express";
import compression from "compression";
import cors from "cors";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { ZodError } from "zod";
import { config } from "./config.js";
import { healthRouter } from "./routes/health.js";
import { plansRouter } from "./routes/plans.js";
const app = express();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
// Gzip / Brotli compression for JSON responses
app.use(compression());
// CORS — allow Cloudflare Pages frontend and local dev
app.use(cors({
    origin: [
        "https://beacon-lab-v2.pages.dev",
        /\.beacon-lab-v2\.pages\.dev$/,
        "http://localhost:5173",
        "http://localhost:5174",
    ],
    credentials: true,
    methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization", "x-session-token"],
}));
app.use(express.json({ limit: "15mb" }));
app.use(express.static(path.resolve(__dirname, "../public")));
app.use(healthRouter);
app.use("/api", plansRouter);
app.use((err, _req, res, _next) => {
    if (err instanceof ZodError) {
        const summary = err.issues.map((i) => `${i.path.join(".")}: ${i.message}`).join("; ");
        console.error(`[zod] Validation failed: ${summary}`);
        res.status(400).json({ error: `Invalid request: ${summary}`, details: err.issues });
        return;
    }
    const status = typeof err === "object" && err !== null ? err.status : undefined;
    if (typeof status === "number" && status >= 400 && status < 600) {
        const message = typeof err === "object" && err !== null && "message" in err ? String(err.message) : "";
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
    if (!config.usePg)
        return;
    try {
        const { pgExec } = await import("./db/postgres.js");
        await pgExec("ALTER TABLE targets ADD COLUMN IF NOT EXISTS target_cor DOUBLE PRECISION NOT NULL DEFAULT 0");
        await pgExec("ALTER TABLE users ADD COLUMN IF NOT EXISTS name TEXT");
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
        // Shadow-mode migration: PE computed daily (full PE CTE output from BQ)
        await pgExec(`
      CREATE TABLE IF NOT EXISTS pe_computed_daily (
        activity_lead_type         TEXT NOT NULL DEFAULT '',
        channel_group_name         TEXT,
        state                      TEXT,
        testing_point              INTEGER,
        opps                       DOUBLE PRECISION,
        bids                       DOUBLE PRECISION,
        win_rate                   DOUBLE PRECISION,
        sold                       DOUBLE PRECISION,
        binds                      DOUBLE PRECISION,
        quotes                     DOUBLE PRECISION,
        click_to_quote             DOUBLE PRECISION,
        channel_quote              DOUBLE PRECISION,
        click_to_channel_quote     DOUBLE PRECISION,
        q2b                        DOUBLE PRECISION,
        channel_binds              DOUBLE PRECISION,
        channel_q2b                DOUBLE PRECISION,
        cpc                        DOUBLE PRECISION,
        avg_bid                    DOUBLE PRECISION,
        win_rate_uplift_state      DOUBLE PRECISION,
        cpc_uplift_state           DOUBLE PRECISION,
        win_rate_uplift_channel    DOUBLE PRECISION,
        cpc_uplift_channel         DOUBLE PRECISION,
        win_rate_uplift            DOUBLE PRECISION,
        cpc_uplift                 DOUBLE PRECISION,
        additional_clicks          DOUBLE PRECISION,
        expected_bind_change       DOUBLE PRECISION,
        additional_budget_needed   DOUBLE PRECISION,
        current_cpb                DOUBLE PRECISION,
        expected_cpb               DOUBLE PRECISION,
        cpb_uplift                 DOUBLE PRECISION,
        performance                DOUBLE PRECISION,
        roe                        DOUBLE PRECISION,
        combined_ratio             DOUBLE PRECISION,
        recommended_testing_point  INTEGER,
        stat_sig                   TEXT,
        stat_sig_channel_group     TEXT,
        stat_sig_source            TEXT,
        scf_avg_profit             DOUBLE PRECISION,
        scf_avg_equity             DOUBLE PRECISION,
        scf_avg_lifetime_premium   DOUBLE PRECISION,
        scf_avg_lifetime_cost      DOUBLE PRECISION,
        ssd_binds                  DOUBLE PRECISION,
        ssd_quotes                 DOUBLE PRECISION,
        ssd_q2b                    DOUBLE PRECISION,
        ssd_performance            DOUBLE PRECISION,
        ssd_roe                    DOUBLE PRECISION,
        ssd_combined_ratio         DOUBLE PRECISION,
        refreshed_at               TIMESTAMPTZ DEFAULT NOW()
      )
    `);
        await pgExec("CREATE INDEX IF NOT EXISTS idx_pe_computed_scope ON pe_computed_daily (activity_lead_type, state, channel_group_name)");
        // Shadow-mode migration: Plan merged daily (output of BQ fn_plan_merged_agg)
        await pgExec(`
      CREATE TABLE IF NOT EXISTS plan_merged_daily (
        start_date                 TEXT,
        end_date                   TEXT,
        channel_group_name         TEXT,
        state                      TEXT,
        segment                    TEXT,
        price_adjustment_percent   DOUBLE PRECISION,
        stat_sig                   TEXT,
        stat_sig_channel_group     TEXT,
        cpc_uplift                 DOUBLE PRECISION,
        win_rate_uplift            DOUBLE PRECISION,
        additional_clicks          DOUBLE PRECISION,
        expected_total_clicks      DOUBLE PRECISION,
        expected_cpc               DOUBLE PRECISION,
        expected_total_cost        DOUBLE PRECISION,
        expected_total_binds       DOUBLE PRECISION,
        additional_expected_binds  DOUBLE PRECISION,
        expected_cpb               DOUBLE PRECISION,
        ss_performance             DOUBLE PRECISION,
        expected_performance       DOUBLE PRECISION,
        performance_uplift         DOUBLE PRECISION,
        refreshed_at               TIMESTAMPTZ DEFAULT NOW()
      )
    `);
        await pgExec("CREATE INDEX IF NOT EXISTS idx_plan_merged_daily_state ON plan_merged_daily (state, channel_group_name)");
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
        console.log("Migrations OK");
    }
    catch (err) {
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
