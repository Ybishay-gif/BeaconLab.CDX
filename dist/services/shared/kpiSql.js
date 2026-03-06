function buildZeroCondition(conditions) {
    const normalized = conditions.map((condition) => condition.trim()).filter(Boolean);
    return normalized.length ? normalized.join("\n              OR ") : "FALSE";
}
export function buildRoeSql(args) {
    const qbcExpr = args.qbcExpr || "@qbc";
    return `
          CASE
            WHEN ${buildZeroCondition(args.zeroConditions)} THEN 0
            ELSE SAFE_DIVIDE(
              (
                ${args.avgProfitExpr}
                - (0.8 * (SAFE_DIVIDE(${args.cpbExpr}, 0.81) + ${qbcExpr}))
              ),
              ${args.avgEquityExpr}
            )
          END
  `.trim();
}
export function buildCombinedRatioSql(args) {
    const qbcExpr = args.qbcExpr || "@qbc";
    return `
          CASE
            WHEN ${buildZeroCondition(args.zeroConditions)} THEN 0
            ELSE SAFE_DIVIDE(
              (
                SAFE_DIVIDE(${args.cpbExpr}, 0.81)
                + ${qbcExpr}
                + ${args.avgLifetimeCostExpr}
              ),
              ${args.avgLifetimePremiumExpr}
            )
          END
  `.trim();
}
