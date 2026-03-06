export function formatInt(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "-";
  }
  return Number(value).toLocaleString("en-US", { maximumFractionDigits: 0 });
}

function getAdaptiveFractionDigits(value) {
  return Math.abs(Number(value) || 0) < 10 ? 1 : 0;
}

export function formatDecimal(value, decimals = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "-";
  }
  const numeric = Number(value);
  const fractionDigits = getAdaptiveFractionDigits(numeric);
  return numeric.toLocaleString("en-US", {
    minimumFractionDigits: fractionDigits,
    maximumFractionDigits: fractionDigits
  });
}

export function formatPercent(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "-";
  }
  return `${formatDecimal((Number(value) || 0) * 100)}%`;
}

export function formatPercentFixed(value, decimals = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "-";
  }
  return `${formatDecimal((Number(value) || 0) * 100)}%`;
}

export function formatPercentOrDash(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "-";
  }
  return formatPercent(value);
}

export function formatCurrency(value, decimals = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "-";
  }
  return `$${formatDecimal(value)}`;
}

export function clampDays(fromDays, toDays) {
  const normalizedFrom = Math.max(1, Math.floor(Number(fromDays) || 30));
  const normalizedTo = Math.max(0, Math.floor(Number(toDays) || 7));
  if (normalizedFrom <= normalizedTo) {
    return { fromDays: normalizedTo + 1, toDays: normalizedTo };
  }
  return { fromDays: normalizedFrom, toDays: normalizedTo };
}

function toIsoDate(date) {
  return date.toISOString().slice(0, 10);
}

export function computeRangeFromToday(fromDays, toDays) {
  const today = new Date();
  const start = new Date(today);
  const end = new Date(today);
  start.setDate(today.getDate() - fromDays);
  end.setDate(today.getDate() - toDays);
  return { startIso: toIsoDate(start), endIso: toIsoDate(end) };
}
