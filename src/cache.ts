/**
 * In-memory LRU cache with TTL expiry.
 *
 * IMPORTANT: This caches only raw BQ query results, NOT computed analytics.
 * Strategy rules and PE decisions are applied fresh on every request so that
 * user changes take effect immediately.
 *
 * BQ data refreshes once daily → 24 h TTL.
 * Cache is cleared automatically by /admin/sync-from-bq after daily refresh.
 */

type CacheEntry<T = unknown> = {
  data: T;
  expiresAt: number;
  lastAccess: number;
  sizeEstimate: number; // rough byte estimate for diagnostics
};

const DEFAULT_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours
const MAX_ENTRIES = 200;

const store = new Map<string, CacheEntry>();

let stats = { hits: 0, misses: 0, evictions: 0 };

function estimateSize(data: unknown): number {
  if (data === null || data === undefined) return 0;
  if (Array.isArray(data)) return data.length * 200; // rough row estimate
  if (typeof data === "string") return data.length * 2;
  if (typeof data === "object") return JSON.stringify(data).length * 2;
  return 100;
}

function evictIfNeeded(): void {
  if (store.size <= MAX_ENTRIES) return;
  const entries = [...store.entries()].sort(
    (a, b) => a[1].lastAccess - b[1].lastAccess
  );
  const toEvict = entries.slice(0, store.size - MAX_ENTRIES);
  for (const [key] of toEvict) {
    store.delete(key);
    stats.evictions++;
  }
}

export function cacheGet<T>(key: string): T | undefined {
  const entry = store.get(key);
  if (!entry) {
    stats.misses++;
    return undefined;
  }
  if (Date.now() > entry.expiresAt) {
    store.delete(key);
    stats.misses++;
    return undefined;
  }
  entry.lastAccess = Date.now();
  stats.hits++;
  return entry.data as T;
}

export function cacheSet<T>(key: string, data: T, ttlMs = DEFAULT_TTL_MS): void {
  const now = Date.now();
  store.set(key, {
    data,
    expiresAt: now + ttlMs,
    lastAccess: now,
    sizeEstimate: estimateSize(data)
  });
  evictIfNeeded();
}

/** Delete all entries whose key starts with `prefix`. */
export function invalidateByPrefix(prefix: string): number {
  let count = 0;
  for (const key of store.keys()) {
    if (key.startsWith(prefix)) {
      store.delete(key);
      count++;
    }
  }
  return count;
}

/** Delete a single cache entry. */
export function invalidateKey(key: string): boolean {
  return store.delete(key);
}

/** Clear the entire cache (called after daily BQ sync). */
export function cacheClear(): void {
  store.clear();
}

/** Diagnostics: entry count, hit/miss stats, estimated memory. */
export function cacheStats(): {
  entries: number;
  hits: number;
  misses: number;
  hitRate: string;
  evictions: number;
  estimatedMB: string;
  keys: string[];
} {
  const total = stats.hits + stats.misses;
  const estimatedBytes = [...store.values()].reduce((sum, e) => sum + e.sizeEstimate, 0);
  return {
    entries: store.size,
    hits: stats.hits,
    misses: stats.misses,
    hitRate: total > 0 ? `${((stats.hits / total) * 100).toFixed(1)}%` : "N/A",
    evictions: stats.evictions,
    estimatedMB: `${(estimatedBytes / 1024 / 1024).toFixed(2)} MB`,
    keys: [...store.keys()]
  };
}

/**
 * Build a deterministic cache key from a prefix and params object.
 * Sorts keys and normalizes arrays for consistent hashing.
 */
export function buildCacheKey(prefix: string, params: Record<string, unknown>): string {
  const parts: string[] = [prefix];
  const sortedKeys = Object.keys(params).sort();
  for (const key of sortedKeys) {
    const val = params[key];
    if (val === undefined || val === null) continue;
    if (Array.isArray(val)) {
      parts.push(`${key}=${[...val].sort().join(",")}`);
    } else {
      parts.push(`${key}=${String(val)}`);
    }
  }
  return parts.join("|");
}

/**
 * Wrap an async function with cache.
 * Returns cached value if available, otherwise calls fn, caches, and returns.
 */
export async function cached<T>(
  key: string,
  fn: () => Promise<T>,
  ttlMs = DEFAULT_TTL_MS
): Promise<T> {
  const hit = cacheGet<T>(key);
  if (hit !== undefined) return hit;
  const result = await fn();
  cacheSet(key, result, ttlMs);
  return result;
}

/** Reset stats counters (for testing). */
export function cacheResetStats(): void {
  stats = { hits: 0, misses: 0, evictions: 0 };
}
