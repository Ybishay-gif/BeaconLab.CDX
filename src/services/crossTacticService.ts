/**
 * Cross Tactic Analytics Explorer — queries the raw BQ table directly
 * with user-selected dimensions and metrics, caching aggregated results.
 *
 * Full spec from the PDF field list:
 * - ~100 dimensions across 15 categories (user selects up to 10)
 * - ~30 measures with specific SQL formulas and derived ratios
 * - Virtual dimensions (date, hour, day_of_week) from Data_DateCreated
 * - LeadType value renaming (CAR_INSURANCE_LEAD → Auto, HOME_INSURANCE_LEAD → Home)
 * - ROE/COR computed using QBC from plan context
 */

import { query as bqQuery } from "../db/bigquery.js";
import { config } from "../config.js";
import { buildCacheKey, cached } from "../cache.js";
import { getTableSchema, getFilterValues } from "./reportService.js";

// ── Types ────────────────────────────────────────────────────────────

export type DimensionCategory =
  | "campaign" | "bidding" | "rejection" | "date"
  | "lead_geo" | "driver" | "insurance" | "vehicle" | "home"
  | "attribution" | "rate_call" | "predictive_caller"
  | "third_party" | "compliance" | "repetition";

export type DimensionDef = {
  column: string;
  label: string;
  category: DimensionCategory;
  /** If set, use this SQL expression instead of `\`column\`` in SELECT/GROUP BY */
  sqlExpr?: string;
  /** If set, apply CASE WHEN renaming in SELECT */
  valueMap?: Record<string, string>;
};

export type MeasureFormat = "integer" | "currency" | "currency_2dp" | "percent_0dp" | "percent_1dp" | "decimal_2dp";

export type BqMeasureDef = {
  label: string;
  sqlExpr: string;
  format: MeasureFormat;
  /** If true, not shown to user — only used as dependency for derived measures */
  hidden?: boolean;
};

export type DerivedMeasureDef = {
  label: string;
  format: MeasureFormat;
  /** Keys of BQ measures this depends on (auto-included in query) */
  deps: string[];
  /** JS compute function: (row) => number | null */
  compute: (row: Record<string, number>, qbc: number) => number | null;
};

export type DrillStep = {
  dimension: string;
  value: string;
};

export type DynamicFilter = {
  column: string;
  operator: string;
  value: string | number | (string | number)[];
};

export type CrossTacticRequest = {
  dimensions: string[];
  metrics: string[];
  filters: Record<string, string[]>;
  dynamicFilters?: DynamicFilter[];
  startDate: string;
  endDate: string;
  drillPath: DrillStep[];
  qbc?: number;
  // Compare mode
  compareStartDate?: string;
  compareEndDate?: string;
};

export type CrossTacticResult = {
  rows: Record<string, unknown>[];
  metadata: {
    rowCount: number;
    dimensions: string[];
    metrics: string[];
  };
};

// ── Dimension Catalog (~100 dimensions, 15 categories) ──────────────

const DIMENSIONS: DimensionDef[] = [
  // ── Campaign ──
  { column: "Company_Name", label: "Company Name", category: "campaign" },
  { column: "Account_Name", label: "Partner Name", category: "campaign" },
  { column: "Campaign_Name", label: "Campaign Name", category: "campaign" },
  { column: "Origin_ActivityType", label: "Activity Type", category: "campaign" },
  { column: "LeadType", label: "Lead Type", category: "campaign",
    valueMap: { CAR_INSURANCE_LEAD: "Auto", HOME_INSURANCE_LEAD: "Home" } },
  { column: "ChannelGroupName", label: "Channel Group", category: "campaign" },
  { column: "StrategyGroupName", label: "Bidding Group", category: "campaign" },

  // ── Bidding ──
  { column: "PriceAdjustmentPercent", label: "Testing Point", category: "bidding" },
  { column: "ExtraBidData_Ads_0_CreativeId", label: "Creative ID", category: "bidding" },

  // ── Rejection ──
  { column: "TrackingVariables_reject_reason", label: "Reject Reason", category: "rejection" },
  { column: "CampaignFilteredReason", label: "Campaign Filter Reason", category: "rejection" },

  // ── Date (virtual — computed from Data_DateCreated) ──
  { column: "date_only", label: "Date", category: "date",
    sqlExpr: "FORMAT_DATETIME('%Y-%m-%d', `Data_DateCreated`)" },
  { column: "hour_of_day", label: "Hour", category: "date",
    sqlExpr: "FORMAT_DATETIME('%H', `Data_DateCreated`)" },
  { column: "day_of_week", label: "Day of Week", category: "date",
    sqlExpr: "FORMAT_DATETIME('%A', `Data_DateCreated`)" },

  // ── Lead / Geographic ──
  { column: "Data_State", label: "State", category: "lead_geo" },
  { column: "Data_City", label: "City", category: "lead_geo" },
  { column: "Segments", label: "Segment", category: "lead_geo" },
  { column: "Data_OwnHome", label: "Home Owner", category: "lead_geo" },
  { column: "Data_ZipCode", label: "Zip Code", category: "lead_geo" },
  { column: "ZipCodeGroupId", label: "Zip Group ID", category: "lead_geo" },
  { column: "Exclusionzipgroupname", label: "Exclusion Zip Group", category: "lead_geo" },
  { column: "Suppressionszipgroupname", label: "Suppressions Zip Group", category: "lead_geo" },
  { column: "Biddingzipgroupname", label: "Bidding Zip Group", category: "lead_geo" },

  // ── Driver ──
  { column: "Data_DriversCount", label: "Drivers Count", category: "driver" },
  { column: "Data_Drivers_0_Occupation", label: "Occupation", category: "driver" },
  { column: "Data_Drivers_0_DUI", label: "DUI", category: "driver" },
  { column: "Data_Drivers_0_SR22", label: "SR22", category: "driver" },
  { column: "Data_Drivers_0_GoodStudent", label: "Good Student", category: "driver" },
  { column: "Data_Drivers_0_MilitaryServiceMember", label: "Military Service Member", category: "driver" },
  { column: "Data_Drivers_0_ServedInMilitary", label: "Served In Military", category: "driver" },
  { column: "Data_Drivers_0_Gender", label: "Gender", category: "driver" },
  { column: "Data_Drivers_0_Education", label: "Education", category: "driver" },
  { column: "Data_Drivers_0_LicenseStatus", label: "License Status", category: "driver" },
  { column: "Data_Drivers_0_MaritalStatus", label: "Marital Status", category: "driver" },
  { column: "Data_Drivers_0_CreditRating", label: "Credit Rating", category: "driver" },
  { column: "driver_Age", label: "Age", category: "driver" },

  // ── Insurance ──
  { column: "Data_IsCurrentInsurance", label: "Currently Insured", category: "insurance" },
  { column: "Data_CurrentInsuranceYearsInsured", label: "Years Insured", category: "insurance" },
  { column: "Data_CurrentCoverageType", label: "Current Coverage Type", category: "insurance" },
  { column: "Data_BodilyInjuryPerAccident", label: "Bodily Injury Per Accident", category: "insurance" },
  { column: "Data_BodilyInjuryPerPeson", label: "Bodily Injury Per Person", category: "insurance" },
  { column: "Data_Drivers_0_AccidentsCount", label: "Number of Accidents", category: "insurance" },

  // ── Vehicle ──
  { column: "Data_CarsCount", label: "Number of Vehicles", category: "vehicle" },
  { column: "Data_HasMultipleVehicles", label: "Multi-car", category: "vehicle" },
  { column: "Data_Cars_0_Ownership", label: "1st Car Ownership", category: "vehicle" },
  { column: "Data_Cars_0_Type", label: "1st Car Type", category: "vehicle" },
  { column: "Data_Cars_0_PrimaryUse", label: "1st Car Primary Use", category: "vehicle" },
  { column: "Data_Cars_0_IsAlarmed", label: "1st Car Alarmed", category: "vehicle" },
  { column: "Data_Cars_0_Year", label: "1st Car Year", category: "vehicle" },
  { column: "Data_Cars_0_Make", label: "1st Car Make", category: "vehicle" },
  { column: "Data_Cars_0_Model", label: "1st Car Model", category: "vehicle" },
  { column: "Data_Cars_0_SubModel", label: "1st Car SubModel", category: "vehicle" },

  // ── Home ──
  { column: "Data_ResidenceCategory", label: "Residence Category", category: "home" },
  { column: "Data_ResidenceInMonths", label: "Residence In Months", category: "home" },
  { column: "Data_Properties_0_PropertyInformation_YearBuilt", label: "Residence Year Built", category: "home" },

  // ── Attribution ──
  { column: "Attribution_Source", label: "Source", category: "attribution" },
  { column: "Attribution_Channel", label: "Channel", category: "attribution" },
  { column: "Attribution_SubChannel1", label: "Sub Channel 1", category: "attribution" },
  { column: "Attribution_SubChannel2", label: "Sub Channel 2", category: "attribution" },
  { column: "Attribution_SubChannel3", label: "Sub Channel 3", category: "attribution" },
  { column: "Attribution_CampaignName", label: "Attribution Campaign", category: "attribution" },
  { column: "UserAgentInfo_IsMobile", label: "Is Mobile", category: "attribution" },
  { column: "UserAgentInfo_DeviceType", label: "Device Type", category: "attribution" },
  { column: "UserAgentInfo_DeviceBrand", label: "Device Brand", category: "attribution" },
  { column: "UserAgentInfo_deviceModel", label: "Device Model", category: "attribution" },
  { column: "UserAgentinfo_OS", label: "OS", category: "attribution" },
  { column: "Data_TcpaUrl", label: "Submission URL", category: "attribution" },

  // ── Rate Call 1 ──
  { column: "RateCall1Data_UnderwritingStatus", label: "RC1 Status", category: "rate_call" },
  { column: "RateCall1Data_UnderwritingStatusRemarkType", label: "RC1 Remark Type", category: "rate_call" },
  { column: "RC1_Reson_Description", label: "RC1 Description", category: "rate_call" },
  { column: "RateCall1Data_BillingFrequency", label: "RC1 Billing Frequency", category: "rate_call" },
  { column: "RateCall1Data_BillingMethod", label: "RC1 Billing Method", category: "rate_call" },

  // ── Predictive Caller ──
  { column: "PredictiveCallerData_FoundInBlackList", label: "BLA Status", category: "predictive_caller" },
  { column: "PredictiveCallerData_FoundInBlockList", label: "DNC Status", category: "predictive_caller" },
  { column: "PredictiveCallerData_BlocklistTier", label: "DNC Tier", category: "predictive_caller" },
  { column: "PredictiveCallerData_BlocklistPassedDays", label: "DNC Days", category: "predictive_caller" },

  // ── 3rd Party Scores ──
  { column: "MerkleData_LTVModelVentile", label: "MD LTV Ventile", category: "third_party" },
  { column: "TransUnionData_FullScore", label: "TU Full Score", category: "third_party" },
  { column: "TransUnionData_PhoneScore", label: "TU Phone Score", category: "third_party" },
  { column: "TransUnionDNData_TUPhoneType", label: "TU Phone Type", category: "third_party" },
  { column: "TransUnionDNData_TUPhoneActivity", label: "TU Phone Activity", category: "third_party" },
  { column: "TransUnionDNData_TUPhoneContactabilityScore", label: "TU Contactability Score", category: "third_party" },
  { column: "TransUnionDNData_TUPhonelinkage", label: "TU Phone Linkage", category: "third_party" },
  { column: "TransUnionDNData_TULTVdecile", label: "TU LTV Decile", category: "third_party" },
  { column: "TransUnionDNData_VerificationScore", label: "TU Verification Score", category: "third_party" },

  // ── Compliance (AP + Jornaya) ──
  { column: "ActiveProspectValidationData_Domain", label: "AP Domain", category: "compliance" },
  { column: "ActiveProspectValidationData_APCertificationStatus", label: "AP Certification Status", category: "compliance" },
  { column: "JornayaValidationData_AuthenticationStatus", label: "Jornaya Auth Status", category: "compliance" },
  { column: "JornayaValidationData_Consent", label: "Jornaya Consent", category: "compliance" },
  { column: "JornayaValidationData_DataIntegrity", label: "Jornaya Data Integrity", category: "compliance" },
  { column: "JornayaValidationData_VisibilityLevel", label: "Jornaya Visibility Level", category: "compliance" },
  { column: "JornayaValidationData_Disclosure", label: "Jornaya Disclosure", category: "compliance" },
  { column: "JornayaValidationData_Stored", label: "Jornaya Stored", category: "compliance" },
  { column: "JornayaValidationData_LeadAge", label: "Jornaya Lead Age", category: "compliance" },
  { column: "JornayaValidationData_LeadDuration", label: "Jornaya Lead Duration", category: "compliance" },
  { column: "JornayaValidationData_RiskFlagSummary", label: "Jornaya Risk Flag", category: "compliance" },
  { column: "JornayaValidationData_LinkageSummary", label: "Jornaya Linkage", category: "compliance" },
  { column: "JornayaValidationData_IDVerifyScore", label: "Jornaya ID Verify Score", category: "compliance" },
  { column: "JornayaValidationData_ValidationSummary", label: "Jornaya Validation", category: "compliance" },

  // ── Repetition ──
  { column: "NumofLeadsByJornaya", label: "Leads By Jornaya", category: "repetition" },
  { column: "NumofCompByJornaya", label: "Partners By Jornaya", category: "repetition" },
  { column: "NumofTacticsByJornaya", label: "Tactics By Jornaya", category: "repetition" },
  { column: "NumofLeadsByShaPhone", label: "Leads By Sha256 Phone", category: "repetition" },
  { column: "NumofCompByShaphone", label: "Partners By Sha256 Phone", category: "repetition" },
  { column: "NumofTacticsByShaphone", label: "Tactics By Sha256 Phone", category: "repetition" },
  { column: "NumofLeadsByShaEmail", label: "Leads By Sha256 Email", category: "repetition" },
  { column: "NumofCompByShaemail", label: "Partners By Sha256 Email", category: "repetition" },
  { column: "NumofTacticsByShaemail", label: "Tactics By Sha256 Email", category: "repetition" },
  { column: "NumofSoldByShaPhone", label: "Sold By Sha256 Phone", category: "repetition" },
  { column: "NumofSoldCompByShaphone", label: "Sold Partners By Sha256 Phone", category: "repetition" },
  { column: "NumofSoldTacticsByShaphone", label: "Sold Tactics By Sha256 Phone", category: "repetition" },
  { column: "NumofSoldByShaEmail", label: "Sold By Sha256 Email", category: "repetition" },
  { column: "NumofSoldCompByShaemail", label: "Sold Partners By Sha256 Email", category: "repetition" },
  { column: "NumofSoldTacticsByShaemail", label: "Sold Tactics By Sha256 Email", category: "repetition" },
  { column: "NumofLeadsByUA_IP_Key", label: "Leads By UA_IP Key", category: "repetition" },
];

// ── Category display labels ──

export const CATEGORY_LABELS: Record<DimensionCategory, string> = {
  campaign: "Campaign",
  bidding: "Bidding",
  rejection: "Rejection",
  date: "Date",
  lead_geo: "Lead / Geographic",
  driver: "Driver",
  insurance: "Insurance",
  vehicle: "Vehicle",
  home: "Home",
  attribution: "Attribution",
  rate_call: "Rate Call 1",
  predictive_caller: "Predictive Caller",
  third_party: "3rd Party Scores",
  compliance: "Compliance (AP / Jornaya)",
  repetition: "Repetition",
};

// ── Measures Catalog ─────────────────────────────────────────────────

/** BQ-aggregated measures (computed in SQL) */
const BQ_MEASURES: Record<string, BqMeasureDef> = {
  opps:             { label: "Opps",             sqlExpr: "COUNT(DISTINCT `Lead_LeadID`)",                                               format: "integer" },
  bids:             { label: "Bids",             sqlExpr: "SUM(`bid_count`)",                                                            format: "integer" },
  avg_bid:          { label: "Avg Bid",          sqlExpr: "AVG(CASE WHEN `bid_count` = 1 AND `bid_price` > 0 THEN `bid_price` END)",     format: "currency_2dp" },
  impressions:      { label: "Impressions",      sqlExpr: "COUNTIF(`ExtraBidData_Ads_0_Used` = true)",                                   format: "integer" },
  avg_position:     { label: "Avg Position",     sqlExpr: "AVG(CASE WHEN `ExtraBidData_Ads_0_Position` > 0 THEN `ExtraBidData_Ads_0_Position` END)", format: "decimal_2dp" },
  sold:             { label: "Sold",             sqlExpr: "SUM(`Transaction_sold`)",                                                     format: "integer" },
  cpc:              { label: "CPC",              sqlExpr: "AVG(CASE WHEN `Price` > 0 THEN `Price` END)",                                 format: "currency_2dp" },
  total_cost:       { label: "Total Cost",       sqlExpr: "SUM(`Price`)",                                                                format: "currency" },
  calls:            { label: "Calls",            sqlExpr: "SUM(`TotalCalls`)",                                                           format: "integer" },
  qs:               { label: "QS",               sqlExpr: "SUM(`AutoOnlineQuotesStart`)",                                                format: "integer" },
  quotes:           { label: "Quotes",           sqlExpr: "SUM(`TotalQuotes`)",                                                          format: "integer" },
  binds:            { label: "Binds",            sqlExpr: "SUM(`TotalBinds`)",                                                           format: "integer" },
  scored_policies:  { label: "Scored Policies",  sqlExpr: "SUM(`ScoredPolicies`)",                                                       format: "integer" },
  sc_cor:           { label: "ScCor",            sqlExpr: "SUM(`ScCor`)",                                                                format: "integer", hidden: true },
  // Hidden aggregations — used as building blocks for derived measures
  sum_target_cpb:       { label: "",  sqlExpr: "SUM(`Target_TargetCPB`)",       format: "integer", hidden: true },
  sum_mrltv:            { label: "",  sqlExpr: "SUM(`CustomValues_Mrltv`)",     format: "integer", hidden: true },
  sum_profit:           { label: "",  sqlExpr: "SUM(`CustomValues_Profit`)",    format: "integer", hidden: true },
  sum_equity:           { label: "",  sqlExpr: "SUM(`Equity`)",                 format: "integer", hidden: true },
  sum_premium:          { label: "",  sqlExpr: "SUM(`CustomValues_Premium`)",   format: "integer", hidden: true },
  sum_lifetime_premium: { label: "",  sqlExpr: "SUM(`LifetimePremium`)",        format: "integer", hidden: true },
  sum_lifetime_cost:    { label: "",  sqlExpr: "SUM(`LifeTimeCost`)",           format: "integer", hidden: true },
};

/** Derived measures (computed in JS after BQ query returns) */
const DERIVED_MEASURES: Record<string, DerivedMeasureDef> = {
  sov:              { label: "SOV",              format: "percent_0dp", deps: ["impressions", "bids"],
    compute: (r) => r.bids > 0 ? r.impressions / r.bids : null },
  win_rate:         { label: "Win Rate",         format: "percent_1dp", deps: ["sold", "bids"],
    compute: (r) => r.bids > 0 ? r.sold / r.bids : null },
  bid_rate:         { label: "Bid Rate",         format: "percent_1dp", deps: ["bids", "opps"],
    compute: (r) => r.opps > 0 ? r.bids / r.opps : null },
  engagement_rate:  { label: "Engagement Rate",  format: "percent_1dp", deps: ["calls", "sold"],
    compute: (r) => r.sold > 0 ? r.calls / r.sold : null },
  qsr:              { label: "QSR",              format: "percent_1dp", deps: ["qs", "sold"],
    compute: (r) => r.sold > 0 ? r.qs / r.sold : null },
  sold_to_quote:    { label: "Sold to Quote",    format: "percent_1dp", deps: ["quotes", "sold"],
    compute: (r) => r.sold > 0 ? r.quotes / r.sold : null },
  q2b:              { label: "Q2B",              format: "percent_1dp", deps: ["quotes", "binds"],
    compute: (r) => r.binds > 0 ? r.quotes / r.binds : null },
  sold_to_bind:     { label: "Sold to Bind",     format: "percent_1dp", deps: ["binds", "sold"],
    compute: (r) => r.sold > 0 ? r.binds / r.sold : null },
  avg_target_cpb:   { label: "Avg Target CPB",   format: "currency",    deps: ["sum_target_cpb", "binds"],
    compute: (r) => r.binds > 0 ? r.sum_target_cpb / r.binds : null },
  cpb:              { label: "CPB",              format: "currency",    deps: ["total_cost", "binds"],
    compute: (r) => r.binds > 0 ? r.total_cost / r.binds : null },
  performance:      { label: "Performance",      format: "percent_1dp", deps: ["sum_target_cpb", "total_cost", "binds"],
    compute: (r) => {
      const avgTargetCpb = r.binds > 0 ? r.sum_target_cpb / r.binds : 0;
      const cpb = r.binds > 0 ? r.total_cost / r.binds : 0;
      return cpb > 0 ? avgTargetCpb / cpb : null;
    }},
  avg_mrltv:        { label: "Avg MRLTV",        format: "integer",     deps: ["sum_mrltv", "scored_policies"],
    compute: (r) => r.scored_policies > 0 ? r.sum_mrltv / r.scored_policies : null },
  avg_profit:       { label: "Avg Profit",       format: "integer",     deps: ["sum_profit", "scored_policies"],
    compute: (r) => r.scored_policies > 0 ? r.sum_profit / r.scored_policies : null },
  avg_equity:       { label: "Avg Equity",       format: "integer",     deps: ["sum_equity", "scored_policies"],
    compute: (r) => r.scored_policies > 0 ? r.sum_equity / r.scored_policies : null },
  avg_premium:      { label: "Avg Premium",      format: "integer",     deps: ["sum_premium", "scored_policies"],
    compute: (r) => r.scored_policies > 0 ? r.sum_premium / r.scored_policies : null },
  avg_lifetime_premium: { label: "Avg Lifetime Premium", format: "integer", deps: ["sum_lifetime_premium", "scored_policies"],
    compute: (r) => r.scored_policies > 0 ? r.sum_lifetime_premium / r.scored_policies : null },
  avg_lifetime_cost:    { label: "Avg Lifetime Cost",    format: "integer", deps: ["sum_lifetime_cost", "sc_cor"],
    compute: (r) => r.sc_cor > 0 ? r.sum_lifetime_cost / r.sc_cor : null },
  // ROE and COR use QBC from plan context (same formulas as analyticsService.ts)
  roe: { label: "ROE", format: "percent_1dp",
    deps: ["sum_profit", "sum_equity", "total_cost", "binds", "scored_policies"],
    compute: (r, qbc) => {
      const avgProfit = r.scored_policies > 0 ? r.sum_profit / r.scored_policies : 0;
      const avgEquity = r.scored_policies > 0 ? r.sum_equity / r.scored_policies : 0;
      const cpb = r.binds > 0 ? r.total_cost / r.binds : 0;
      return avgEquity !== 0 ? (avgProfit - 0.8 * (cpb / 0.81 + qbc)) / avgEquity : null;
    }},
  cor: { label: "COR", format: "percent_1dp",
    deps: ["sum_lifetime_cost", "sum_lifetime_premium", "total_cost", "binds", "scored_policies", "sc_cor"],
    compute: (r, qbc) => {
      const avgLifetimeCost = r.sc_cor > 0 ? r.sum_lifetime_cost / r.sc_cor : 0;
      const avgLifetimePremium = r.scored_policies > 0 ? r.sum_lifetime_premium / r.scored_policies : 0;
      const cpb = r.binds > 0 ? r.total_cost / r.binds : 0;
      return avgLifetimePremium !== 0 ? (cpb / 0.81 + qbc + avgLifetimeCost) / avgLifetimePremium : null;
    }},
};

// ── Lookups ──

const DIMENSION_MAP = new Map(DIMENSIONS.map((d) => [d.column, d]));
const VALID_DIMENSIONS = new Set(DIMENSIONS.map((d) => d.column));
const VALID_BQ_MEASURES = new Set(Object.keys(BQ_MEASURES));
const VALID_DERIVED_MEASURES = new Set(Object.keys(DERIVED_MEASURES));
const VALID_ALL_MEASURES = new Set([...VALID_BQ_MEASURES, ...VALID_DERIVED_MEASURES]);

const MAX_DIMENSIONS = 10;
const MAX_RESULT_ROWS = 10_000;
const CACHE_TTL = 24 * 60 * 60 * 1000; // 24h

// ── Schema Endpoint ──────────────────────────────────────────────────

export async function getCrossTacticSchema(): Promise<{
  dimensions: DimensionDef[];
  categoryLabels: Record<string, string>;
  bqMeasures: Array<{ key: string; label: string; format: MeasureFormat }>;
  derivedMeasures: Array<{ key: string; label: string; format: MeasureFormat; deps: string[] }>;
}> {
  // Validate dimensions against actual BQ schema
  const schema = await getTableSchema(false);
  const bqColumns = new Set(schema.map((c) => c.column_name));

  // Virtual dimensions (date_only, hour_of_day, day_of_week) don't exist as columns
  const virtualDims = new Set(DIMENSIONS.filter((d) => d.sqlExpr).map((d) => d.column));
  const validDimensions = DIMENSIONS.filter((d) => virtualDims.has(d.column) || bqColumns.has(d.column));

  const visibleBqMeasures = Object.entries(BQ_MEASURES)
    .filter(([, def]) => !def.hidden)
    .map(([key, def]) => ({ key, label: def.label, format: def.format }));

  const derivedMeasures = Object.entries(DERIVED_MEASURES)
    .map(([key, def]) => ({ key, label: def.label, format: def.format, deps: def.deps }));

  return {
    dimensions: validDimensions,
    categoryLabels: CATEGORY_LABELS,
    bqMeasures: visibleBqMeasures,
    derivedMeasures,
  };
}

// ── Filter Values (delegates to reportService) ───────────────────────

export { getFilterValues } from "./reportService.js";

// ── Core Aggregation ─────────────────────────────────────────────────

export async function getCrossTacticAggregation(
  req: CrossTacticRequest
): Promise<CrossTacticResult> {
  // ── Validate ──
  if (!req.dimensions.length || req.dimensions.length > MAX_DIMENSIONS) {
    throw new Error(`dimensions must have 1-${MAX_DIMENSIONS} items`);
  }
  for (const d of req.dimensions) {
    if (!VALID_DIMENSIONS.has(d)) throw new Error(`Invalid dimension: ${d}`);
  }
  for (const m of req.metrics) {
    if (!VALID_ALL_MEASURES.has(m)) throw new Error(`Invalid metric: ${m}`);
  }
  for (const col of Object.keys(req.filters)) {
    if (!VALID_DIMENSIONS.has(col)) throw new Error(`Invalid filter dimension: ${col}`);
  }
  for (const step of req.drillPath) {
    if (!VALID_DIMENSIONS.has(step.dimension)) throw new Error(`Invalid drill dimension: ${step.dimension}`);
  }
  if (!req.startDate || !req.endDate) throw new Error("startDate and endDate are required");

  // Resolve which BQ measures are actually needed (including hidden deps for derived)
  const neededBqMeasures = resolveNeededBqMeasures(req.metrics);

  // ── Cache ──
  const cacheKey = buildCacheKey("cross-tactic", {
    dimensions: req.dimensions,
    metrics: req.metrics,
    filters: JSON.stringify(req.filters),
    dynamicFilters: JSON.stringify(req.dynamicFilters ?? []),
    startDate: req.startDate,
    endDate: req.endDate,
    drillPath: JSON.stringify(req.drillPath),
  });

  const rawRows = await cached<Record<string, unknown>[]>(cacheKey, async () => {
    const { sql, params } = buildAggregationSql(req, neededBqMeasures);
    return bqQuery<Record<string, unknown>>(sql, params);
  }, CACHE_TTL);

  // Deep copy so we don't mutate cache
  const rows = rawRows.map((r) => ({ ...r }));

  // Compute derived measures
  const qbc = req.qbc ?? 0;
  const selectedDerived = req.metrics.filter((m) => VALID_DERIVED_MEASURES.has(m));
  for (const row of rows) {
    const numRow = row as Record<string, number>;
    for (const key of selectedDerived) {
      const def = DERIVED_MEASURES[key];
      row[key] = def.compute(numRow, qbc);
    }
  }

  return {
    rows,
    metadata: {
      rowCount: rows.length,
      dimensions: req.dimensions,
      metrics: req.metrics,
    },
  };
}

// ── Resolve BQ measure dependencies ──

function resolveNeededBqMeasures(selectedMetrics: string[]): string[] {
  const needed = new Set<string>();

  for (const m of selectedMetrics) {
    if (VALID_BQ_MEASURES.has(m)) {
      needed.add(m);
    } else if (DERIVED_MEASURES[m]) {
      for (const dep of DERIVED_MEASURES[m].deps) {
        needed.add(dep);
      }
    }
  }

  return [...needed];
}

// ── SQL Builder ──────────────────────────────────────────────────────

function buildAggregationSql(
  req: CrossTacticRequest,
  bqMeasures: string[]
): { sql: string; params: Record<string, unknown> } {
  const params: Record<string, unknown> = {};
  let paramIdx = 0;

  // SELECT: dimensions
  const selectParts: string[] = [];
  const groupByParts: string[] = [];

  for (const dimKey of req.dimensions) {
    const dimDef = DIMENSION_MAP.get(dimKey)!;

    if (dimDef.valueMap) {
      // CASE WHEN renaming (e.g. LeadType)
      const cases = Object.entries(dimDef.valueMap)
        .map(([from, to]) => `WHEN \`${dimKey}\` = '${from}' THEN '${to}'`)
        .join(" ");
      selectParts.push(`CASE ${cases} ELSE \`${dimKey}\` END AS \`${dimKey}\``);
      groupByParts.push(`CASE ${cases} ELSE \`${dimKey}\` END`);
    } else if (dimDef.sqlExpr) {
      // Virtual dimension (date_only, hour_of_day, etc.)
      selectParts.push(`${dimDef.sqlExpr} AS \`${dimKey}\``);
      groupByParts.push(dimDef.sqlExpr);
    } else {
      selectParts.push(`\`${dimKey}\``);
      groupByParts.push(`\`${dimKey}\``);
    }
  }

  // SELECT: measures
  for (const measureKey of bqMeasures) {
    const def = BQ_MEASURES[measureKey];
    selectParts.push(`${def.sqlExpr} AS \`${measureKey}\``);
  }

  // WHERE
  const conditions: string[] = [];
  params.startDate = req.startDate;
  params.endDate = req.endDate;
  conditions.push("`Data_DateCreated` >= @startDate");
  conditions.push("`Data_DateCreated` <= @endDate");

  // Drill-down constraints
  for (const step of req.drillPath) {
    const dimDef = DIMENSION_MAP.get(step.dimension);
    const key = `drill_${paramIdx++}`;
    params[key] = step.value;

    if (dimDef?.sqlExpr) {
      conditions.push(`${dimDef.sqlExpr} = @${key}`);
    } else if (dimDef?.valueMap) {
      // Reverse map: user sends "Auto", we need to match "CAR_INSURANCE_LEAD"
      const reverseMap = Object.entries(dimDef.valueMap);
      const original = reverseMap.find(([, v]) => v === step.value)?.[0];
      if (original) params[key] = original;
      conditions.push(`\`${step.dimension}\` = @${key}`);
    } else {
      conditions.push(`\`${step.dimension}\` = @${key}`);
    }
  }

  // User filters
  for (const [col, values] of Object.entries(req.filters)) {
    if (!values?.length) continue;
    const dimDef = DIMENSION_MAP.get(col);
    const key = `filter_${paramIdx++}`;

    if (dimDef?.valueMap) {
      // Reverse-map filter values
      const reverseMap = Object.entries(dimDef.valueMap);
      const mapped = values.map((v) => reverseMap.find(([, display]) => display === v)?.[0] ?? v);
      params[key] = mapped;
    } else {
      params[key] = values;
    }

    if (dimDef?.sqlExpr) {
      conditions.push(`${dimDef.sqlExpr} IN UNNEST(@${key})`);
    } else {
      conditions.push(`\`${col}\` IN UNNEST(@${key})`);
    }
  }

  // Dynamic filters (column + operator + value)
  const ALLOWED_OPERATORS = new Set(["=", "!=", ">", "<", ">=", "<=", "BETWEEN", "LIKE", "IN"]);
  for (const f of req.dynamicFilters ?? []) {
    if (!ALLOWED_OPERATORS.has(f.operator)) continue;
    const colRef = `\`${f.column}\``;

    if (f.operator === "BETWEEN" && Array.isArray(f.value) && f.value.length === 2) {
      const k1 = `dp${paramIdx++}`;
      const k2 = `dp${paramIdx++}`;
      params[k1] = f.value[0];
      params[k2] = f.value[1];
      conditions.push(`${colRef} BETWEEN @${k1} AND @${k2}`);
    } else if (f.operator === "IN") {
      const vals = Array.isArray(f.value) ? f.value : String(f.value).split(",").map((s) => s.trim());
      const key = `dp${paramIdx++}`;
      params[key] = vals;
      conditions.push(`${colRef} IN UNNEST(@${key})`);
    } else if (f.operator === "LIKE") {
      const key = `dp${paramIdx++}`;
      params[key] = `%${f.value}%`;
      conditions.push(`${colRef} LIKE @${key}`);
    } else {
      const key = `dp${paramIdx++}`;
      params[key] = f.value;
      conditions.push(`${colRef} ${f.operator} @${key}`);
    }
  }

  const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

  // ORDER BY: first non-hidden BQ measure, descending
  const orderByMeasure = bqMeasures.find((m) => !BQ_MEASURES[m].hidden) ?? bqMeasures[0] ?? "opps";

  const sql = `SELECT ${selectParts.join(",\n  ")}
FROM ${config.rawCrossTacticTable}
${whereClause}
GROUP BY ${groupByParts.join(", ")}
ORDER BY \`${orderByMeasure}\` DESC
LIMIT ${MAX_RESULT_ROWS}`;

  return { sql, params };
}

// ── Measures where lower = better (for color coding) ─────────────────

export const INVERSE_MEASURES = new Set([
  "cpc", "avg_bid", "total_cost", "cpb", "cor", "avg_lifetime_cost",
]);

// ── Compare Mode ─────────────────────────────────────────────────────

export async function getCrossTacticComparison(
  req: CrossTacticRequest
): Promise<CrossTacticResult> {
  if (!req.compareStartDate || !req.compareEndDate) {
    throw new Error("compareStartDate and compareEndDate are required for comparison");
  }

  // Validate same as aggregation
  if (!req.dimensions.length || req.dimensions.length > MAX_DIMENSIONS) {
    throw new Error(`dimensions must have 1-${MAX_DIMENSIONS} items`);
  }
  for (const d of req.dimensions) {
    if (!VALID_DIMENSIONS.has(d)) throw new Error(`Invalid dimension: ${d}`);
  }
  for (const m of req.metrics) {
    if (!VALID_ALL_MEASURES.has(m)) throw new Error(`Invalid metric: ${m}`);
  }
  if (!req.startDate || !req.endDate) throw new Error("startDate and endDate are required");

  const neededBqMeasures = resolveNeededBqMeasures(req.metrics);

  // Build main + compare requests
  const mainReq = { ...req };
  const compareReq = { ...req, startDate: req.compareStartDate!, endDate: req.compareEndDate! };

  // Cache keys
  const mainCacheKey = buildCacheKey("cross-tactic", {
    dimensions: req.dimensions, metrics: req.metrics,
    filters: JSON.stringify(req.filters), dynamicFilters: JSON.stringify(req.dynamicFilters ?? []),
    startDate: req.startDate, endDate: req.endDate,
    drillPath: JSON.stringify(req.drillPath),
  });
  const compareCacheKey = buildCacheKey("cross-tactic", {
    dimensions: req.dimensions, metrics: req.metrics,
    filters: JSON.stringify(req.filters), dynamicFilters: JSON.stringify(req.dynamicFilters ?? []),
    startDate: req.compareStartDate!, endDate: req.compareEndDate!,
    drillPath: JSON.stringify(req.drillPath),
  });

  // Run both in parallel
  const [mainRows, compareRows] = await Promise.all([
    cached<Record<string, unknown>[]>(mainCacheKey, async () => {
      const { sql, params } = buildAggregationSql(mainReq, neededBqMeasures);
      return bqQuery<Record<string, unknown>>(sql, params);
    }, CACHE_TTL),
    cached<Record<string, unknown>[]>(compareCacheKey, async () => {
      const { sql, params } = buildAggregationSql(compareReq, neededBqMeasures);
      return bqQuery<Record<string, unknown>>(sql, params);
    }, CACHE_TTL),
  ]);

  // Build lookup for compare rows by dimension key
  const dimCols = req.dimensions;
  const dimKey = (row: Record<string, unknown>) => dimCols.map((d) => String(row[d] ?? "")).join("|");
  const compareMap = new Map<string, Record<string, unknown>>();
  for (const row of compareRows) {
    compareMap.set(dimKey(row), row);
  }

  // Compute derived + diffs
  const qbc = req.qbc ?? 0;
  const selectedDerived = req.metrics.filter((m) => VALID_DERIVED_MEASURES.has(m));

  const resultRows = mainRows.map((mainRow) => {
    const row = { ...mainRow };
    const compareRow = compareMap.get(dimKey(mainRow));

    // Compute derived measures on main
    const numRow = row as Record<string, number>;
    for (const key of selectedDerived) {
      const def = DERIVED_MEASURES[key];
      row[key] = def.compute(numRow, qbc);
    }

    // Compute derived on compare
    const compareVals: Record<string, number | null> = {};
    if (compareRow) {
      const numCompare = { ...compareRow } as Record<string, number>;
      for (const key of selectedDerived) {
        const def = DERIVED_MEASURES[key];
        compareVals[key] = def.compute(numCompare, qbc);
      }
    }

    // Add compare + diff for each visible metric
    for (const mKey of req.metrics) {
      const mainVal = Number(row[mKey] ?? 0);
      let compareVal: number;

      if (VALID_DERIVED_MEASURES.has(mKey)) {
        compareVal = Number(compareVals[mKey] ?? 0);
      } else {
        compareVal = compareRow ? Number(compareRow[mKey] ?? 0) : 0;
      }

      row[`${mKey}_compare`] = compareVal;
      row[`${mKey}_diff`] = mainVal - compareVal;
      row[`${mKey}_diff_pct`] = compareVal !== 0 ? (mainVal - compareVal) / compareVal : null;
    }

    return row;
  });

  return {
    rows: resultRows,
    metadata: {
      rowCount: resultRows.length,
      dimensions: req.dimensions,
      metrics: req.metrics,
    },
  };
}
