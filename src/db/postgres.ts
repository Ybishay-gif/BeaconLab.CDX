import pg from "pg";

const { Pool } = pg;

let pool: pg.Pool | null = null;

function getPool(): pg.Pool {
  if (!pool) {
    const pgHost = process.env.PGHOST || "/cloudsql/" + process.env.CLOUD_SQL_CONNECTION_NAME;
    const isUnixSocket = pgHost.startsWith("/");

    pool = new Pool({
      ...(isUnixSocket
        ? { host: pgHost }
        : { host: pgHost, port: Number(process.env.PGPORT || 5432) }),
      database: process.env.PGDATABASE || "beacon_lab",
      user: process.env.PGUSER || "beacon",
      password: process.env.PGPASSWORD,
      max: 10,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 5_000,
    });
  }
  return pool;
}

/**
 * Run a parameterized query against PostgreSQL.
 *
 * BQ uses named params (@startDate), PG uses positional ($1).
 * This helper accepts named params and converts them automatically.
 */
export async function pgQuery<T extends pg.QueryResultRow = Record<string, unknown>>(
  sql: string,
  params: Record<string, unknown> = {}
): Promise<T[]> {
  const { text, values } = namedToPositional(sql, params);
  const result = await getPool().query<T>(text, values);
  return result.rows;
}

/**
 * Run raw SQL (no named params). Used for DDL, migrations, etc.
 */
export async function pgExec(sql: string): Promise<void> {
  await getPool().query(sql);
}

/**
 * Convert BQ-style named params (@paramName) to PG-style positional ($1, $2, ...).
 * Handles array params by expanding them for ANY() usage.
 */
function namedToPositional(
  sql: string,
  params: Record<string, unknown>
): { text: string; values: unknown[] } {
  const values: unknown[] = [];
  const paramIndex = new Map<string, number>();

  // BQ → PG syntax fixups applied automatically:
  let normalized = sql
    .replace(/\bCURRENT_TIMESTAMP\(\)/g, "NOW()")
    .replace(/\bGENERATE_UUID\(\)/g, "gen_random_uuid()")
    .replace(/"(__ALL__|)"/g, "'$1'") // BQ double-quoted strings → PG single-quoted
    .replace(/= ""/g, "= ''")         // empty-string comparisons
    .replace(/!= ""/g, "!= ''");

  // SAFE_DIVIDE(a, b) → (CASE WHEN (b) = 0 THEN NULL ELSE (a)::double precision / (b) END)
  normalized = replaceSafeDivide(normalized);

  const text = normalized.replace(/@(\w+)/g, (_match, name: string) => {
    if (paramIndex.has(name)) {
      return `$${paramIndex.get(name)}`;
    }
    values.push(params[name] ?? null);
    const idx = values.length;
    paramIndex.set(name, idx);
    return `$${idx}`;
  });

  return { text, values };
}

/**
 * Run a callback inside a PG transaction (single client).
 * Automatically BEGINs, COMMITs on success, ROLLBACKs on error.
 */
export async function pgTransaction<T>(
  fn: (exec: (sql: string) => Promise<void>) => Promise<T>
): Promise<T> {
  const client = await getPool().connect();
  try {
    await client.query("BEGIN");
    const exec = async (sql: string) => { await client.query(sql); };
    const result = await fn(exec);
    await client.query("COMMIT");
    return result;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Replace SAFE_DIVIDE(a, b) with PG-compatible CASE expression.
 * Handles nested SAFE_DIVIDE calls by finding balanced parentheses.
 */
function replaceSafeDivide(sql: string): string {
  let result = sql;
  // Process innermost SAFE_DIVIDE calls first, looping until none remain
  while (true) {
    const idx = result.search(/\bSAFE_DIVIDE\s*\(/i);
    if (idx === -1) break;

    // Find the opening paren
    const openParen = result.indexOf("(", idx);
    // Find the matching closing paren and the comma that splits args
    let depth = 1;
    let commaPos = -1;
    let i = openParen + 1;
    for (; i < result.length && depth > 0; i++) {
      if (result[i] === "(") depth++;
      else if (result[i] === ")") depth--;
      else if (result[i] === "," && depth === 1 && commaPos === -1) commaPos = i;
    }
    if (commaPos === -1 || depth !== 0) break; // malformed, bail out

    const arg1 = result.substring(openParen + 1, commaPos).trim();
    const arg2 = result.substring(commaPos + 1, i - 1).trim();
    const replacement = `(CASE WHEN (${arg2}) = 0 THEN NULL ELSE (${arg1})::double precision / (${arg2}) END)`;
    result = result.substring(0, idx) + replacement + result.substring(i);
  }
  return result;
}

/**
 * Run a callback with a dedicated PG client from the pool.
 * The client is released when the callback completes (or throws).
 * Unlike pgTransaction, this does NOT wrap in BEGIN/COMMIT.
 */
export async function pgWithClient(
  fn: (exec: (sql: string) => Promise<void>) => Promise<void>
): Promise<void> {
  const client = await getPool().connect();
  try {
    const exec = async (sql: string) => { await client.query(sql); };
    await fn(exec);
  } finally {
    client.release();
  }
}

/** Gracefully close the pool (for shutdown). */
export async function pgClose(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}
