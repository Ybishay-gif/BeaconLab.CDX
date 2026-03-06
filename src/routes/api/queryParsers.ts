export function parseQueryArray(value: unknown): string[] {
  if (!value) {
    return [];
  }

  const parts = Array.isArray(value) ? value : [value];
  return parts
    .flatMap((entry) => String(entry).split(","))
    .map((entry) => entry.trim())
    .filter(Boolean);
}

export function parseOptionalNumber(value: unknown): number | undefined {
  const raw = typeof value === "string" ? value.trim() : "";
  if (!raw) {
    return undefined;
  }

  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : undefined;
}
