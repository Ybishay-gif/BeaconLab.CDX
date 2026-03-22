import pg from "pg";

const client = new pg.Client({
  host: "35.202.110.87",
  database: "beacon_lab",
  user: "beacon",
  password: "BeaconLab2024",
});

async function run() {
  await client.connect();
  console.log("Connected to PG");

  // Check current state
  const before = await client.query("SELECT module_id, COUNT(*) as cnt FROM user_modules GROUP BY module_id ORDER BY module_id");
  console.log("Before migration:", before.rows);

  await client.query("BEGIN");

  // planning → all 4 new modules
  const r1 = await client.query(`
    INSERT INTO user_modules (user_id, module_id)
    SELECT user_id, unnest(ARRAY['lm_tools','beacon_lite_tactic','beacon_lite_cross_tactic','kissterra_tools'])
    FROM user_modules WHERE module_id = 'planning'
    ON CONFLICT DO NOTHING
  `);
  console.log("planning expansion:", r1.rowCount, "rows inserted");

  // cross_tactic → beacon_lite_cross_tactic
  const r2 = await client.query(`
    INSERT INTO user_modules (user_id, module_id)
    SELECT user_id, 'beacon_lite_cross_tactic'
    FROM user_modules WHERE module_id = 'cross_tactic'
    ON CONFLICT DO NOTHING
  `);
  console.log("cross_tactic expansion:", r2.rowCount, "rows inserted");

  // channel_recommendations → lm_tools
  const r3 = await client.query(`
    INSERT INTO user_modules (user_id, module_id)
    SELECT user_id, 'lm_tools'
    FROM user_modules WHERE module_id = 'channel_recommendations'
    ON CONFLICT DO NOTHING
  `);
  console.log("channel_recommendations expansion:", r3.rowCount, "rows inserted");

  await client.query("COMMIT");

  // Check after state
  const after = await client.query("SELECT module_id, COUNT(*) as cnt FROM user_modules GROUP BY module_id ORDER BY module_id");
  console.log("After migration:", after.rows);

  await client.end();
  console.log("Migration complete");
}

run().catch((e) => { console.error(e); process.exit(1); });
