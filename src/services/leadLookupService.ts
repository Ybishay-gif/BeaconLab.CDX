/**
 * Lead Lookup Service — query individual lead-level data from the
 * Cross Tactic Analysis BQ table.
 *
 * Column mappings are hardcoded from the authoritative BQ field spec PDF.
 */

import { randomUUID } from "node:crypto";
import { Storage } from "@google-cloud/storage";
import PDFDocument from "pdfkit";
import { query as bqQuery } from "../db/bigquery.js";
import { config } from "../config.js";

const storage = new Storage({ projectId: config.projectId });

/* ------------------------------------------------------------------ */
/*  Identifier types (filter columns)                                  */
/* ------------------------------------------------------------------ */

export type IdentifierType =
  | "beacon_id"
  | "sha256_email"
  | "sha256_phone"
  | "jornaya_id"
  | "rc1_quote_id"
  | "ap_form_id";

const IDENTIFIER_COLUMN_MAP: Record<IdentifierType, string> = {
  beacon_id: "Lead_LeadID",
  sha256_email: "Sha256Email",
  sha256_phone: "Data_Sha256Phone",
  jornaya_id: "JornayaLeadId",
  rc1_quote_id: "RateCall1Data_QuoteId",
  ap_form_id: "TrustedFormId",
};

export const IDENTIFIER_DISPLAY: Record<IdentifierType, string> = {
  beacon_id: "Beacon ID",
  sha256_email: "Sha256 Email",
  sha256_phone: "Sha256 Phone",
  jornaya_id: "Jornaya Lead Id",
  rc1_quote_id: "RC1 QuoteID",
  ap_form_id: "AP Form ID",
};

/* ------------------------------------------------------------------ */
/*  Section → column mapping                                           */
/* ------------------------------------------------------------------ */

export type SectionKey =
  | "campaign_details"
  | "bidding_info"
  | "bid_rejection"
  | "lead_info"
  | "drivers"
  | "insurance"
  | "vehicles"
  | "home"
  | "attribution"
  | "rc1"
  | "predictive_caller"
  | "merkle"
  | "transunion"
  | "activeprospect"
  | "jornaya"
  | "performance"
  | "repetition";

export interface ColumnDef {
  bq: string;
  display: string;
}

export const SECTION_DISPLAY: Record<SectionKey, string> = {
  campaign_details: "Campaign Details",
  bidding_info: "Bidding Info",
  bid_rejection: "Bid Rejection Details",
  lead_info: "Lead Info",
  drivers: "Drivers Information",
  insurance: "Insurance Details",
  vehicles: "Vehicle Details",
  home: "Home Information",
  attribution: "Attribution Details",
  rc1: "Rate Call 1 (RC1)",
  predictive_caller: "Predictive Caller",
  merkle: "Merkle",
  transunion: "TransUnion",
  activeprospect: "ActiveProspect",
  jornaya: "Jornaya Details",
  performance: "Performance Data",
  repetition: "Repetition Data",
};

const c = (bq: string, display: string): ColumnDef => ({ bq, display });

export const SECTION_COLUMNS: Record<SectionKey, ColumnDef[]> = {
  campaign_details: [
    c("BrokerCompanyId", "Company ID"),
    c("Company_Name", "Company Name"),
    c("Origin_CompanyAccountId", "Partner ID"),
    c("Account_Name", "Partner Name"),
    c("BrokerId", "Campaign ID"),
    c("Campaign_Name", "Campaign name"),
    c("Origin_ActivityType", "Activity Type"),
    c("LeadType", "Lead Type"),
    c("BrokerChannelId", "Ad group ID"),
    c("ChannelGroupName", "Channel Group"),
    c("StrategyGroupName", "Bidding Group"),
    c("CallInfo_IsBillable", "Billable Call"),
    c("CallInfo_CallDurationSeconds", "Call Duration"),
    c("Account_Time Zone", "Time zone"),
    c("Budget_Limits", "Campaign Budget limits"),
    c("Account_Monthly_Budget", "Partner Monthly Budget"),
    c("Account_Daily_Budget", "Partner Daily Budget"),
    c("Account_Daily_Cap", "Partner Daily Cap"),
    c("Campaign_Monthly_Budget", "Campaign Monthly Budget"),
    c("Campaign_Daily_Budget", "Campaign Daily Budget"),
    c("Campaign_Monthly_Cap", "Campaign Monthly Cap"),
    c("Campaign_Daily_Cap", "Campaign Daily Cap"),
    c("Campaign_URL", "Campaign URL"),
    c("Is_Account_Active", "Partner Active Status"),
    c("Is_Campaign_Active", "Campaign Active status"),
    c("Last_Modified_UTC", "Last Modified UTC"),
  ],
  bidding_info: [
    c("bid_count", "Bid count"),
    c("bid_price", "Bid Price"),
    c("PriceAdjustmentPercent", "Testing point"),
    c("ExtraBidData_Ads_0_CreativeId", "Creative ID"),
    c("ExtraBidData_Ads_0_Used", "Impression"),
    c("ExtraBidData_Ads_0_Position", "Position"),
    c("ExtraBidData_Ads_0_ImpressionPixel", "Pixel URL"),
    c("Prefill_timeout", "Prefill timeout"),
    c("Prefill_Error", "Prefill Error"),
    c("Prefill_Empty_URL", "Prefill Empty_URL"),
    c("Transaction_sold", "Sold"),
    c("Price", "Price"),
  ],
  bid_rejection: [
    c("TrackingVariables_reject_reason", "Reject reason"),
    c("CampaignFilteredReason", "Campaign filter reason"),
  ],
  lead_info: [
    c("Data_DateCreated", "Date"),
    c("Data_OwnHome", "Home owner"),
    c("Data_City", "City"),
    c("Data_State", "State"),
    c("Segments", "Segment"),
    c("Lead_LeadID", "Beacon ID"),
    c("Sha256Email", "Sha256Email"),
    c("Data_Sha256Phone", "Sha256Phone"),
    c("Data_ZipCode", "Zip Code"),
    c("ZipCodeGroupId", "Zip Group ID"),
    c("Exclusionzipgroupname", "Exclusion zip group name"),
    c("Suppressionszipgroupname", "Suppressions zip group name"),
    c("Biddingzipgroupname", "Bidding zip group name"),
  ],
  drivers: [
    c("Data_DriversCount", "DriversCount"),
    c("Data_Drivers_0_Occupation", "Occupation"),
    c("Data_Drivers_0_DUI", "DUI"),
    c("Data_Drivers_0_SR22", "SR22"),
    c("Data_Drivers_0_GoodStudent", "Good Student"),
    c("Data_Drivers_0_MilitaryServiceMember", "Military Service Member"),
    c("Data_Drivers_0_ServedInMilitary", "Served In Military"),
    c("Data_Drivers_0_Gender", "Gender"),
    c("Data_Drivers_0_Education", "Education"),
    c("Data_Drivers_0_LicenseStatus", "License Status"),
    c("Data_Drivers_0_MaritalStatus", "Marital Status"),
    c("Data_Drivers_0_CreditRating", "CreditRating"),
    c("driver_Age", "Age"),
  ],
  insurance: [
    c("Data_IsCurrentInsurance", "Currently insured"),
    c("Data_CurrentInsuranceYearsInsured", "Years insured"),
    c("Data_CurrentInsuranceCompany", "Current Insurance Company"),
    c("Data_CurrentInsuranceExpiration", "Current Insurance Expiration"),
    c("Data_CurrentInsuranceStartDate", "Current Insurance Start Date"),
    c("Data_ContinuousCoverageInMonths", "Continuous Coverage InMonth"),
    c("Data_CurrentCoverageType", "Current Coverage Type"),
    c("Data_BodilyInjuryPerAccident", "Bodily Injury Per Accident"),
    c("Data_BodilyInjuryPerPeson", "Bodily Injury Per Person"),
    c("Data_Drivers_0_AccidentsCount", "Number of Accidents"),
    c("Data_Drivers_0_TicketOrClaimInTheLastThreeYears", "Number of Tickets"),
    c("Data_PropertyDamagePerAccident", "Property Damage Per Accident"),
    c("Data_InterestedInHomeInsurance", "Home bundle"),
  ],
  vehicles: [
    c("Data_CarsCount", "Number of Vehicles"),
    c("Data_HasMultipleVehicles", "Multi-car"),
    c("Data_Cars_0_Ownership", "1st car Ownership"),
    c("Data_Cars_0_CoverageDesired", "1st car Coverage Desired"),
    c("Data_Cars_0_AnnualMileage", "1st car Annual Mileage"),
    c("Data_Cars_0_Collision", "1st car Collision"),
    c("Data_Cars_0_Type", "1st car Type"),
    c("Data_Cars_0_CommuteDaysPerWeek", "1st car Commute Days Per Week"),
    c("Data_Cars_0_PrimaryUse", "1st car PrimaryUse"),
    c("Data_Cars_0_IsAlarmed", "1st car Is Alarmed"),
    c("Data_Cars_0_VIN", "1st car VIN"),
    c("Data_Cars_0_Year", "1st car Year"),
    c("Data_Cars_0_Make", "1st car Make"),
    c("Data_Cars_0_Model", "1st car Model"),
    c("Data_Cars_0_SubModel", "1st car SubModel"),
    c("Data_Cars_1_VIN", "2nd car VIN"),
    c("Data_Cars_1_Year", "2nd car Year"),
    c("Data_Cars_1_Make", "2nd car Make"),
    c("Data_Cars_1_Model", "2nd car Model"),
    c("Data_Cars_1_SubModel", "2nd car SubModel"),
    c("Data_Cars_2_VIN", "3rd car VIN"),
    c("Data_Cars_2_Year", "3rd car Year"),
    c("Data_Cars_2_Make", "3rd car Make"),
    c("Data_Cars_2_Model", "3rd car Model"),
    c("Data_Cars_2_SubModel", "3rd car SubModel"),
  ],
  home: [
    c("Data_ResidenceCategory", "Residence Category"),
    c("Data_ResidenceInMonths", "Residence In Months"),
    c("Data_Properties_0_PropertyInformation_YearBuilt", "Residence Year Built"),
  ],
  attribution: [
    c("Attribution_Source", "Source"),
    c("Attribution_Channel", "Channel"),
    c("Attribution_SubChannel1", "Sub Channel 1"),
    c("Attribution_SubChannel2", "Sub Channel 2"),
    c("Attribution_SubChannel3", "Sub Channel 3"),
    c("Attribution_CampaignId", "Attribution campaign ID"),
    c("Attribution_CampaignName", "Attribution campaign name"),
    c("UniqueId", "External ID"),
    c("UserAgent", "User Agent"),
    c("UserIp", "User Ip"),
    c("UserAgentInfo_IsMobile", "Is Mobile"),
    c("UserAgentInfo_DeviceType", "Device Type"),
    c("UserAgentInfo_DeviceBrand", "DeviceBrand"),
    c("UserAgentInfo_deviceModel", "Device Model"),
    c("UserAgentinfo_OS", "OS"),
    c("UserAgentInfo_Browser", "Browser"),
    c("UserAgentInfo_BrowserVersion", "Browser Version"),
    c("EmailISP", "EmailISP"),
    c("Data_EmailConsent", "Email Consent"),
    c("Data_TcpaCall", "Call consent"),
    c("Data_TcpaSms", "SMS consent"),
    c("Data_TcpaUrl", "Submission URL"),
  ],
  rc1: [
    c("RateCall1Data_UnderwritingStatus", "RC1 status"),
    c("RateCall1Data_UnderwritingStatusRemarkType", "RC1 Remark Type"),
    c("RC1_Reson_Description", "RC1 Description"),
    c("RateCall1Data_QuoteId", "RC1 QuoteID"),
    c("RateCall1Data_RecallUrl", "RC1 URL"),
    c("RateCall1Data_MonthlyPrice", "Monthly Price"),
    c("RateCall1Data_BillingFrequency", "RC1 Billing Frequency"),
    c("RateCall1Data_BillingMethod", "RC1 Billing Method"),
  ],
  predictive_caller: [
    c("PredictiveCallerData_FoundInBlackList", "BLA status"),
    c("PredictiveCallerData_FoundInBlockList", "DNC status"),
    c("PredictiveCallerData_BlocklistTier", "SNC tier"),
    c("PredictiveCallerData_BlocklistPassedDays", "DNC Days"),
  ],
  merkle: [
    c("MerkleData_LTVModelScore", "MD LTV Score"),
    c("MerkleData_LTVModelVentile", "MD LTV Ventile"),
  ],
  transunion: [
    c("TransUnionData_FullScore", "TU Full Score"),
    c("TransUnionData_PhoneScore", "TU Phone Score"),
    c("TransUnionDNData_TUPhoneType", "TU Phone Type"),
    c("TransUnionDNData_TUPhoneActivity", "TU Phone Activity"),
    c("TransUnionDNData_TUPhoneContactabilityScore", "TU Contactability Score"),
    c("TransUnionDNData_TUPhonelinkage", "TU Phone linkage"),
    c("TransUnionDNData_TULtvScore", "TU LTV Score"),
    c("TransUnionDNData_TULTVdecile", "TU LTV decile"),
    c("TransUnionDNData_VerificationScore", "TU Verification Score"),
  ],
  activeprospect: [
    c("ActiveProspectValidationData_Domain", "AP Domain"),
    c("ActiveProspectValidationData_AgeSeconds", "AP Age Seconds"),
    c("ActiveProspectValidationData_FormDuration", "AP Form Duration"),
    c("ActiveProspectValidationData_APCertificationStatus", "AP Certification Status"),
    c("ActiveProspectValidationData_Ip", "AP IP"),
    c("TrustedFormCertificateClaimed", "TF Certificate Claimed"),
    c("Data_TrustedFormCertificateClaimed", "AP Certificate Claimed"),
    c("TrustedFormId", "AP Form ID"),
  ],
  jornaya: [
    c("JornayaLeadId", "Jornaya Lead Id"),
    c("JornayaValidationData_AuthenticationStatus", "Jornaya Authentication Status"),
    c("JornayaValidationData_Consent", "Jornaya Consent"),
    c("JornayaValidationData_DataIntegrity", "Jornaya Data Integrity"),
    c("JornayaValidationData_VisibilityLevel", "Jornaya Visibility Level"),
    c("JornayaValidationData_Disclosure", "Jornaya Disclosure"),
    c("JornayaValidationData_Stored", "Jornaya Stored"),
    c("JornayaValidationData_LeadAge", "Jornaya Lead Age"),
    c("JornayaValidationData_LeadDuration", "Jornaya Lead Duration"),
    c("JornayaValidationData_RiskFlagSummary", "Jornaya Risk Flag Summary"),
    c("JornayaValidationData_LinkageSummary", "Jornaya Linkage Summary"),
    c("JornayaValidationData_IDVerifyScore", "Jornaya ID Verify Score"),
    c("JornayaValidationData_ValidationSummary", "Jornaya Validation Summary"),
  ],
  performance: [
    c("Conv_MC", "Conv MultiCar"),
    c("Conv_Homeonership", "Conv Homeonership"),
    c("Conv_segement", "Conv segment"),
    c("CallCount", "Call Count"),
    c("NumofCalls", "Numof Calls"),
    c("TotalCalls", "Total Calls"),
    c("TalkTime", "Talk Time"),
    c("AutoQuotes", "Auto Quotes"),
    c("TenantQuotes", "Tenant Quotes"),
    c("CondoQuotes", "Condo Quotes"),
    c("HomeQuotes", "Home Quotes"),
    c("AutoOnlineQuotesStart", "Quotes Started"),
    c("TotalQuotes", "Total Quotes"),
    c("AutoBinds", "Auto Binds"),
    c("CondoBinds", "CondoBinds"),
    c("TenantBinds", "Tenant Binds"),
    c("HomeBinds", "Home Binds"),
    c("OtherBinds", "OtherBinds"),
    c("AutoOnlineBinds", "Online Binds"),
    c("TotalBinds", "TotalBinds"),
    c("AutoRejects", "Auto Rejects"),
    c("TenantRejects", "Tenant Rejects"),
    c("CondoRejects", "Condo Rejects"),
    c("HomeRejects", "Home Rejects"),
    c("ScoredPolicies", "Scored Policies"),
    c("ScCor", "ScCor"),
    c("Target_TargetCPB_original", "Origin Target CPB"),
    c("Target_TargetCPB", "Target CPB"),
    c("CustomValues_Mrltv", "MRTLV"),
    c("CustomValues_Profit", "Profit"),
    c("Equity", "Equity"),
    c("CustomValues_Premium", "Premium"),
    c("ClickLossAdj", "ClickLossAdj"),
    c("LifetimePremium", "Lifetime Premium"),
    c("LifeTimeCost", "LifeTime Cost"),
    c("CreditScore", "Conv Credit Score"),
    c("BillingFrequency", "Conv Billing Frequency"),
    c("BillingMethod", "Conv Billing Method"),
  ],
  repetition: [
    c("NumofLeadsByJornaya", "Num of Leads By Jornaya"),
    c("NumofCompByJornaya", "Num of Partners By Jornaya"),
    c("NumofTacticsByJornaya", "Num of Tactics By Jornaya"),
    c("NumofLeadsByShaPhone", "Num of Leads By Sha256 Phone"),
    c("NumofCompByShaphone", "Num of Partners By Sha256 Phone"),
    c("NumofTacticsByShaphone", "Num of Tactics By Sha256 Phone"),
    c("NumofLeadsByShaEmail", "Num of Leads By Sha256 Email"),
    c("NumofCompByShaemail", "Num of Partners By Sha256 Email"),
    c("NumofTacticsByShaemail", "Num of Tactics By Sha256 Email"),
    c("NumofSoldByShaPhone", "Num of Sold By Sha256 Phone"),
    c("NumofSoldCompByShaphone", "Num of Partners Comp By Sha256 Phone"),
    c("NumofSoldTacticsByShaphone", "Num of Sold Tactics By Sha256 Phone"),
    c("NumofSoldByShaEmail", "Num of Sold By Sha256 Email"),
    c("NumofSoldCompByShaemail", "Num of Partners Comp By Sha256 Email"),
    c("NumofSoldTacticsByShaemail", "Num of Sold Tactics By Sha256 Email"),
    c("UA_IP_Key", "UA_IP_Key"),
    c("NumofLeadsByUA_IP_Key", "Num of Leads By UA_IP Key"),
    c("SoldClickKey", "UA_IP_Zip_Year_make Key"),
  ],
};

const ALL_SECTION_KEYS = Object.keys(SECTION_COLUMNS) as SectionKey[];

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

/** Backtick-quote a column name for BQ (handles spaces) */
function bqCol(name: string): string {
  return `\`${name}\``;
}

function resolveIdentifierColumn(type: string): string {
  const col = IDENTIFIER_COLUMN_MAP[type as IdentifierType];
  if (!col) throw new Error(`Unknown identifier type: ${type}`);
  return col;
}

function resolveSections(keys: string[]): SectionKey[] {
  if (keys.includes("all")) return ALL_SECTION_KEYS;
  const valid: SectionKey[] = [];
  for (const k of keys) {
    if (k in SECTION_COLUMNS) valid.push(k as SectionKey);
  }
  if (valid.length === 0) throw new Error("No valid sections specified");
  return valid;
}

function buildWhere(
  identifierCol: string,
  filters?: { account_name?: string; segment?: string },
): { clause: string; params: Record<string, unknown> } {
  const conditions = [`${bqCol(identifierCol)} = @identifier_value`];
  const params: Record<string, unknown> = {};
  if (filters?.account_name) {
    conditions.push(`Account_Name = @account_name`);
    params.account_name = filters.account_name;
  }
  if (filters?.segment) {
    conditions.push(`Segments = @segment`);
    params.segment = filters.segment;
  }
  return { clause: conditions.join(" AND "), params };
}

/* ------------------------------------------------------------------ */
/*  Search leads                                                       */
/* ------------------------------------------------------------------ */

export interface LeadSearchResult {
  Lead_LeadID: string;
  Account_Name: string;
  Segments: string;
  Data_DateCreated: string;
  Data_State: string;
  Attribution_Channel: string;
  LeadType: string;
  Transaction_sold: number;
}

export async function searchLeads(
  identifierType: string,
  identifierValue: string,
  filters?: { account_name?: string; segment?: string },
): Promise<{ total: number; rows: LeadSearchResult[] }> {
  const idCol = resolveIdentifierColumn(identifierType);
  const { clause, params } = buildWhere(idCol, filters);

  const sql = `
    SELECT
      Lead_LeadID, Account_Name, Segments,
      CAST(Data_DateCreated AS STRING) AS Data_DateCreated,
      Data_State, Attribution_Channel, LeadType, Transaction_sold
    FROM ${config.rawCrossTacticTable}
    WHERE ${clause}
    ORDER BY Data_DateCreated DESC
    LIMIT 100
  `;

  const results = await bqQuery<LeadSearchResult>(sql, {
    identifier_value: identifierValue,
    ...params,
  });

  return { total: results.length, rows: results };
}

/* ------------------------------------------------------------------ */
/*  Get lead details                                                   */
/* ------------------------------------------------------------------ */

export type RowSelection = "first" | "last" | "all";

export interface LeadDetailResult {
  section: string;
  sectionDisplay: string;
  data: Record<string, unknown>[];
}

export async function getLeadDetails(
  identifierType: string,
  identifierValue: string,
  sectionKeys: string[],
  filters?: { account_name?: string; segment?: string },
  rowSelection: RowSelection = "all",
): Promise<{ totalRows: number; sections: LeadDetailResult[] }> {
  const idCol = resolveIdentifierColumn(identifierType);
  const sections = resolveSections(sectionKeys);
  const { clause, params } = buildWhere(idCol, filters);

  // Collect all unique columns across requested sections
  const allColumns = new Set<string>();
  for (const sk of sections) {
    for (const col of SECTION_COLUMNS[sk]) {
      allColumns.add(col.bq);
    }
  }

  const selectCols = Array.from(allColumns).map(bqCol).join(", ");
  const orderDir = rowSelection === "first" ? "ASC" : "DESC";
  const limitClause = rowSelection === "all" ? "LIMIT 100" : "LIMIT 1";

  const sql = `
    SELECT ${selectCols}
    FROM ${config.rawCrossTacticTable}
    WHERE ${clause}
    ORDER BY Data_DateCreated ${orderDir}
    ${limitClause}
  `;

  const rawRows = await bqQuery<Record<string, unknown>>(sql, {
    identifier_value: identifierValue,
    ...params,
  });

  // Group results by section for structured response
  const result: LeadDetailResult[] = [];
  for (const sk of sections) {
    const cols = SECTION_COLUMNS[sk];
    const sectionData = rawRows.map((row) => {
      const obj: Record<string, unknown> = {};
      for (const col of cols) {
        obj[col.display] = row[col.bq] ?? null;
      }
      return obj;
    });
    result.push({
      section: sk,
      sectionDisplay: SECTION_DISPLAY[sk],
      data: sectionData,
    });
  }

  return { totalRows: rawRows.length, sections: result };
}

/* ------------------------------------------------------------------ */
/*  Export lead data (CSV or PDF)                                       */
/* ------------------------------------------------------------------ */

export interface ExportResult {
  exportId: string;
  fileName: string;
  rowCount: number;
  format: string;
  gcsPath: string;
}

/** Query lead data for export — shared by CSV and PDF */
async function queryForExport(
  identifierType: string,
  identifierValue: string,
  sectionKeys: string[],
  filters?: { account_name?: string; segment?: string },
) {
  const idCol = resolveIdentifierColumn(identifierType);
  const sections = resolveSections(sectionKeys);
  const { clause, params } = buildWhere(idCol, filters);

  // Collect columns grouped by section (for PDF) and flat (for CSV)
  const sectionColMap: { section: SectionKey; display: string; cols: ColumnDef[] }[] = [];
  const allColumns: ColumnDef[] = [];
  for (const sk of sections) {
    const cols = SECTION_COLUMNS[sk];
    sectionColMap.push({ section: sk, display: SECTION_DISPLAY[sk], cols });
    for (const col of cols) {
      if (!allColumns.find((c) => c.bq === col.bq)) allColumns.push(col);
    }
  }

  const selectCols = allColumns.map((col) => bqCol(col.bq)).join(", ");
  const sql = `
    SELECT ${selectCols}
    FROM ${config.rawCrossTacticTable}
    WHERE ${clause}
    ORDER BY Data_DateCreated DESC
    LIMIT 1000
  `;

  const rows = await bqQuery<Record<string, unknown>>(sql, {
    identifier_value: identifierValue,
    ...params,
  });

  return { rows, allColumns, sectionColMap };
}

/** Build CSV content */
function buildCsv(rows: Record<string, unknown>[], columns: ColumnDef[]): string {
  const headers = columns.map((c) => c.display);
  const lines = [headers.join(",")];
  for (const row of rows) {
    const values = columns.map((col) => {
      const val = row[col.bq];
      if (val === null || val === undefined) return "";
      const str = String(val);
      if (str.includes(",") || str.includes('"') || str.includes("\n")) {
        return `"${str.replace(/"/g, '""')}"`;
      }
      return str;
    });
    lines.push(values.join(","));
  }
  return lines.join("\n");
}

/** Build PDF buffer — one section per group, key-value layout for single rows, table for multiple */
function buildPdf(
  rows: Record<string, unknown>[],
  sectionColMap: { section: SectionKey; display: string; cols: ColumnDef[] }[],
  identifierValue: string,
): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const doc = new PDFDocument({ size: "A4", margin: 40 });
    const chunks: Buffer[] = [];
    doc.on("data", (chunk: Buffer) => chunks.push(chunk));
    doc.on("end", () => resolve(Buffer.concat(chunks)));
    doc.on("error", reject);

    // Title
    doc.fontSize(16).font("Helvetica-Bold").text("Lead Data Export", { align: "center" });
    doc.moveDown(0.3);
    doc.fontSize(9).font("Helvetica").text(`ID: ${identifierValue}  |  ${rows.length} row(s)  |  ${new Date().toISOString().slice(0, 10)}`, { align: "center" });
    doc.moveDown(0.8);

    for (const sec of sectionColMap) {
      // Section header
      doc.fontSize(11).font("Helvetica-Bold").fillColor("#2563eb").text(sec.display);
      doc.moveDown(0.2);
      doc.fillColor("#000000");

      if (rows.length === 1) {
        // Single row: key-value pairs
        const row = rows[0];
        for (const col of sec.cols) {
          const val = row[col.bq];
          if (val === null || val === undefined || val === "") continue;
          doc.fontSize(8).font("Helvetica-Bold").text(`${col.display}: `, { continued: true });
          doc.font("Helvetica").text(String(val));
        }
      } else {
        // Multiple rows: compact table
        const visibleCols = sec.cols.filter((col) =>
          rows.some((r) => r[col.bq] != null && r[col.bq] !== ""),
        ).slice(0, 8); // max 8 cols to fit on page

        if (visibleCols.length > 0) {
          // Header row
          const colWidth = (doc.page.width - 80) / visibleCols.length;
          const startX = 40;
          let y = doc.y;

          doc.fontSize(7).font("Helvetica-Bold");
          visibleCols.forEach((col, i) => {
            doc.text(col.display, startX + i * colWidth, y, {
              width: colWidth - 4,
              align: "left",
            });
          });
          y = doc.y + 2;
          doc.moveTo(startX, y).lineTo(doc.page.width - 40, y).stroke();
          y += 3;

          // Data rows
          doc.font("Helvetica").fontSize(7);
          for (const row of rows.slice(0, 50)) {
            if (y > doc.page.height - 60) {
              doc.addPage();
              y = 40;
            }
            visibleCols.forEach((col, i) => {
              const val = row[col.bq];
              doc.text(
                val != null ? String(val).slice(0, 30) : "",
                startX + i * colWidth,
                y,
                { width: colWidth - 4, align: "left" },
              );
            });
            y = doc.y + 1;
          }
          if (rows.length > 50) {
            doc.fontSize(7).font("Helvetica-Oblique").text(`... and ${rows.length - 50} more rows`);
          }
        }
      }

      doc.moveDown(0.6);

      // Page break if near bottom
      if (doc.y > doc.page.height - 100) {
        doc.addPage();
      }
    }

    doc.end();
  });
}

export async function exportLeadData(
  identifierType: string,
  identifierValue: string,
  sectionKeys: string[],
  filters?: { account_name?: string; segment?: string },
  format: "csv" | "pdf" = "pdf",
): Promise<ExportResult> {
  const { rows, allColumns, sectionColMap } = await queryForExport(
    identifierType, identifierValue, sectionKeys, filters,
  );

  const exportId = randomUUID();
  const ext = format === "pdf" ? "pdf" : "csv";
  const fileName = `lead-export-${exportId}.${ext}`;
  const gcsPath = `exports/${fileName}`;
  const bucket = storage.bucket(config.reportsBucket);
  const file = bucket.file(gcsPath);

  if (format === "pdf") {
    const pdfBuffer = await buildPdf(rows, sectionColMap, identifierValue);
    await file.save(pdfBuffer, {
      contentType: "application/pdf",
      metadata: { contentDisposition: `attachment; filename="${fileName}"` },
    });
  } else {
    const csvContent = buildCsv(rows, allColumns);
    await file.save(csvContent, {
      contentType: "text/csv",
      metadata: { contentDisposition: `attachment; filename="${fileName}"` },
    });
  }

  return { exportId, fileName, rowCount: rows.length, format: ext, gcsPath };
}

/** Get a signed download URL for an export */
export async function getExportDownloadUrl(exportId: string): Promise<string> {
  // Try PDF first, then CSV
  const bucket = storage.bucket(config.reportsBucket);
  for (const ext of ["pdf", "csv"]) {
    const gcsPath = `exports/lead-export-${exportId}.${ext}`;
    const [exists] = await bucket.file(gcsPath).exists();
    if (exists) {
      const [url] = await bucket.file(gcsPath).getSignedUrl({
        version: "v4",
        action: "read",
        expires: Date.now() + 15 * 60 * 1000,
      });
      return url;
    }
  }
  throw new Error("Export file not found");
}
