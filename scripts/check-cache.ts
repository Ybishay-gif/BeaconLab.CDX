import { pgQuery } from "../src/db/postgres.js";

async function main() {
  const pe = await pgQuery<{cnt: string}>("SELECT COUNT(*) as cnt FROM pe_cache", {});
  const pm = await pgQuery<{cnt: string}>("SELECT COUNT(*) as cnt FROM pm_cache", {});
  const qc = await pgQuery<{cnt: string}>("SELECT COUNT(*) as cnt FROM query_cache", {});
  console.log("pe_cache:", pe[0]?.cnt, "pm_cache:", pm[0]?.cnt, "query_cache:", qc[0]?.cnt);
  process.exit(0);
}
main().catch(e => { console.error(e); process.exit(1); });
