import {
  clampDays,
  computeRangeFromToday,
  formatCurrency,
  formatDecimal,
  formatInt,
  formatPercent,
  formatPercentFixed,
  formatPercentOrDash
} from "./modules/format.js";
import { renderDateRangeControl } from "./modules/ui.js";

function mountDateRangeComponents() {
  const mounts = [
    {
      mountId: "planPerformanceDateControlMount",
      inputId: "planPerformanceDateRange",
      startInputId: "planPerformanceStartDate",
      endInputId: "planPerformanceEndDate",
      presetsId: "planPerformanceDatePresets",
      labelText: "Performance Date Range"
    },
    {
      mountId: "planPriceDateControlMount",
      inputId: "planPriceDateRange",
      startInputId: "planPriceStartDate",
      endInputId: "planPriceEndDate",
      presetsId: "planPriceDatePresets",
      labelText: "Price Exploration Date Range"
    },
    {
      mountId: "targetsDateControlMount",
      inputId: "targetsDateRange",
      startInputId: "targetsStartDate",
      endInputId: "targetsEndDate",
      presetsId: "targetsDatePresets"
    },
    {
      mountId: "priceDecisionDateControlMount",
      inputId: "priceDecisionDateRange",
      startInputId: "priceDecisionStartDate",
      endInputId: "priceDecisionEndDate",
      presetsId: "priceDecisionDatePresets",
      labelClass: "sr-only",
      ariaLabel: "Price exploration decisions date range"
    },
    {
      mountId: "stateSegmentDateControlMount",
      inputId: "stateSegmentDateRange",
      startInputId: "startDate",
      endInputId: "endDate",
      presetsId: "stateSegmentDatePresets",
      labelClass: "sr-only",
      ariaLabel: "State and channel performance date range"
    },
    {
      mountId: "priceDateControlMount",
      inputId: "priceDateRange",
      startInputId: "priceStartDate",
      endInputId: "priceEndDate",
      presetsId: "priceDatePresets",
      labelClass: "sr-only",
      ariaLabel: "Price exploration date range"
    },
    {
      mountId: "strategyAnalysisDateControlMount",
      inputId: "strategyAnalysisDateRange",
      startInputId: "strategyAnalysisStartDate",
      endInputId: "strategyAnalysisEndDate",
      presetsId: "strategyAnalysisDatePresets",
      labelClass: "sr-only",
      ariaLabel: "Strategy analysis date range"
    },
    {
      mountId: "plansComparisonDateControlMount",
      inputId: "plansComparisonDateRange",
      startInputId: "plansComparisonStartDate",
      endInputId: "plansComparisonEndDate",
      presetsId: "plansComparisonDatePresets",
      labelClass: "sr-only",
      ariaLabel: "Plans comparison date range"
    },
    {
      mountId: "stateAnalysisDateControlMount",
      inputId: "stateAnalysisDateRange",
      startInputId: "stateAnalysisStartDate",
      endInputId: "stateAnalysisEndDate",
      presetsId: "stateAnalysisDatePresets",
      labelClass: "sr-only",
      ariaLabel: "State analysis date range"
    },
    {
      mountId: "statePlanAnalysisDateControlMount",
      inputId: "statePlanAnalysisDateRange",
      startInputId: "statePlanAnalysisStartDate",
      endInputId: "statePlanAnalysisEndDate",
      presetsId: "statePlanAnalysisDatePresets",
      labelClass: "sr-only",
      ariaLabel: "State plan analysis date range"
    }
  ];

  mounts.forEach((mountConfig) => {
    const mount = document.getElementById(mountConfig.mountId);
    if (!mount) {
      return;
    }
    renderDateRangeControl(mount, mountConfig);
  });
}

mountDateRangeComponents();

const state = {
  email: localStorage.getItem("planning_user_email") || "",
  role: localStorage.getItem("planning_user_role") || "",
  sessionToken: localStorage.getItem("planning_session_token") || "",
  activeSection: "plan",
  activePlanTab: "builder",
  activeAnalyticsTab: "state-segment",
  strategyAnalysisViewMode: localStorage.getItem("planning_strategy_analysis_view_mode") || "rule",
  activeSettingsTab: "global-filters",
  sidebarCollapsed: localStorage.getItem("planning_sidebar_collapsed") === "1",
  sidebarPinned: localStorage.getItem("planning_sidebar_pinned") !== "0",
  authEmailCandidate: "",
  activityLeadType: localStorage.getItem("planning_activity_lead_type") || "all",
  multiSelectValues: {
    states: [],
    segments: [],
    stateSegmentChannels: [],
    priceStates: [],
    priceChannels: [],
    priceDecisionStates: [],
    priceDecisionChannels: [],
    priceDecisionSegments: []
  },
  stateSegmentRawRows: [],
  stateSegmentDisplayRows: [],
  targetsRows: [],
  targetsMode: "bq",
  uploadedTargetsFile: null,
  targetsDefaultLoaded: false,
  defaultTargetsFile: null,
  settingsDefaultTargetsByScope: {},
  pendingDefaultTargetsUploadScope: "",
  targetsGoalMode: "cpb",
  derivedTargetRules: [],
  derivedRuleIdCounter: 1,
  derivedTargetStateOptions: [],
  derivedTargetSegmentOptions: ["MCH", "MCR", "SCH", "SCR"],
  planStrategyRules: [],
  planStrategyRuleIdCounter: 1,
  planStrategyStateOptions: [],
  planStrategySegmentOptions: ["MCH", "MCR", "SCH", "SCR"],
  strategyAnalysisRows: [],
  plansComparisonRows: [],
  plansComparisonMode: "plans",
  plansComparisonPlanId: "",
  planOutcomeRows: [],
  priceExplorationRows: [],
  priceExplorationKpiRows: [],
  priceDecisionRows: [],
  priceDecisionAllChannels: [],
  priceDecisionSelectedKey: "",
  priceDecisionManualTestingPoints: {},
  stateAnalysisData: null,
  statePlanAnalysisData: null,
  statePlanSelectedState: "",
  usStatesGeoJson: null,
  planTableRows: [],
  planContext: {
    performanceStartDate: "",
    performanceEndDate: "",
    priceExplorationStartDate: "",
    priceExplorationEndDate: "",
    qbcClicks: 1,
    qbcLeadsCalls: 1
  }
};

const DEFAULT_TARGETS_FILE_URL = "/assets/Targets-default.xlsx";
const DEFAULT_TARGETS_FILE_STORAGE_KEY = "planning_default_targets_file";
const DEFAULT_TARGETS_FILE_DB_NAME = "planning_app_settings";
const DEFAULT_TARGETS_FILE_DB_STORE = "settings";
const DEFAULT_TARGETS_FILE_DB_KEY = "default_targets_file";
const PRICE_EXPLORATION_TABLE_MAX_ROWS = 5000;
const PRICE_EXPLORATION_KPI_MAX_ROWS = 200000;
const PRICE_DECISION_TOP_GROUPS_DEFAULT = 100;
const PRICE_DECISION_FULL_LIMIT = 200000;
const DEFAULT_TARGETS_TABLE_COL_COUNT = 12;
const DERIVED_RULES_STORAGE_PREFIX = "planning_targets_rules_";
let targetsEnrichmentSeq = 0;
const PLAN_STRATEGY_PARAM_KEY = "plan_strategy_config";
const TARGETS_DERIVED_RULES_PARAM_KEY = "targets_derived_rules_config";
const TARGETS_DEFAULT_FILE_PARAM_KEY = "targets_default_file_config";
const APP_SETTINGS_PARAM_KEY = "app_settings_config";
const PLAN_CONTEXT_PARAM_KEY = "plan_context_config";
const PRICE_DECISION_PARAM_KEY = "price_exploration_decisions";
const SIDEBAR_COLLAPSED_KEY = "planning_sidebar_collapsed";
const SIDEBAR_PINNED_KEY = "planning_sidebar_pinned";
const SELECTED_PLAN_ID_STORAGE_KEY = "planning_selected_plan_id";
const SELECTED_PLAN_ID_STORAGE_PREFIX = "planning_selected_plan_id_";
const PLAN_STRATEGY_LOCAL_PREFIX = "planning_plan_strategy_local_";
const PLAN_STRATEGY_LOCAL_SCOPE_PREFIX = "planning_plan_strategy_local_scope_";
const ACTIVITY_SCOPE_KEYS = new Set([
  "all",
  "clicks_auto",
  "clicks_home",
  "leads_auto",
  "leads_home",
  "calls_auto",
  "calls_home"
]);
const DEFAULT_TARGET_SCOPE_ROWS = [
  { key: "clicks_auto", label: "Auto Clicks" },
  { key: "clicks_home", label: "Home Clicks" },
  { key: "leads_auto", label: "Auto Leads" },
  { key: "leads_home", label: "Home Leads" },
  { key: "calls_auto", label: "Auto Calls" },
  { key: "calls_home", label: "Home Calls" }
];
const COMPARISON_SCOPE_KEYS = DEFAULT_TARGET_SCOPE_ROWS.map((item) => item.key);
const ACTIVITY_SCOPE_LABELS = DEFAULT_TARGET_SCOPE_ROWS.reduce((acc, item) => {
  acc[item.key] = item.label;
  return acc;
}, {});
const STRATEGY_SEGMENT_REGEX = /\b(MCH|MCR|SCH|SCR)\b/i;
const TOAST_SUCCESS_STATUS_IDS = new Set([
  "createStatus",
  "actionStatus",
  "planStrategyStatus",
  "derivedTargetRulesStatus",
  "targetsStatus",
  "defaultTargetsFileStatus",
  "priceDecisionStatus"
]);
const TOAST_SUCCESS_REGEX = /\b(saved?|updated?|created?|added|applied|adjusted|queued|cleared|reset|set)\b/i;
const TOAST_EXCLUDE_REGEX =
  /\b(loading|loaded|load|refresh|failed|error|select|enter|available|prepared|signing|signed in|access granted)\b/i;
const TOAST_DURATION_MS = 3200;
const PLAN_MUTATION_TIMEOUT_MS = 90000;
const PLAN_LIST_TIMEOUT_MS = 45000;
const TARGETS_DEFAULT_FROM_DAYS = 90;
const TARGETS_DEFAULT_TO_DAYS = 0;
const DEFAULT_PLAN_QBC_CLICKS = 1;
const DEFAULT_PLAN_QBC_LEADS_CALLS = 1;
const migratedPlanQbcIds = new Set();
const TABLE_ENHANCER_IDS = [
  "derivedTargetRulesTable",
  "derivedTargetPreviewTable",
  "targetsTable",
  "planStrategyRulesTable",
  "planStrategySettingsTable",
  "planOutcomeTable",
  "stateSegmentTable",
  "priceExplorationTable",
  "priceDecisionTable",
  "strategyAnalysisTable",
  "plansComparisonTable",
  "statePlanStateSegmentsTable",
  "usersTable"
];
const ALL_US_STATE_CODES = [
  "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL",
  "GA", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA",
  "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE",
  "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI",
  "SC", "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"
];
const FIPS_TO_STATE_CODE = {
  1: "AL", 2: "AK", 4: "AZ", 5: "AR", 6: "CA", 8: "CO", 9: "CT", 10: "DE", 11: "DC", 12: "FL",
  13: "GA", 15: "HI", 16: "ID", 17: "IL", 18: "IN", 19: "IA", 20: "KS", 21: "KY", 22: "LA", 23: "ME",
  24: "MD", 25: "MA", 26: "MI", 27: "MN", 28: "MS", 29: "MO", 30: "MT", 31: "NE", 32: "NV", 33: "NH",
  34: "NJ", 35: "NM", 36: "NY", 37: "NC", 38: "ND", 39: "OH", 40: "OK", 41: "OR", 42: "PA", 44: "RI",
  45: "SC", 46: "SD", 47: "TN", 48: "TX", 49: "UT", 50: "VT", 51: "VA", 53: "WA", 54: "WV", 55: "WI",
  56: "WY"
};

const el = {
  appLayout: document.getElementById("appLayout"),
  mainLoadingOverlay: document.getElementById("mainLoadingOverlay"),
  sidebar: document.querySelector(".sidebar"),
  sidebarToggleBtn: document.getElementById("sidebarToggleBtn"),
  sidebarPinBtn: document.getElementById("sidebarPinBtn"),
  loginScreen: document.getElementById("loginScreen"),
  authStatus: document.getElementById("authStatus"),
  adminAccessCode: document.getElementById("adminAccessCode"),
  adminLoginBtn: document.getElementById("adminLoginBtn"),
  authEmail: document.getElementById("authEmail"),
  userContinueBtn: document.getElementById("userContinueBtn"),
  userPasswordLoginWrap: document.getElementById("userPasswordLoginWrap"),
  userPasswordSetupWrap: document.getElementById("userPasswordSetupWrap"),
  authPassword: document.getElementById("authPassword"),
  userLoginBtn: document.getElementById("userLoginBtn"),
  authCreatePassword: document.getElementById("authCreatePassword"),
  authConfirmPassword: document.getElementById("authConfirmPassword"),
  userSetPasswordBtn: document.getElementById("userSetPasswordBtn"),
  logoutBtn: document.getElementById("logoutBtn"),
  meStatus: document.getElementById("meStatus"),
  planName: document.getElementById("planName"),
  planDesc: document.getElementById("planDesc"),
  createPlan: document.getElementById("createPlan"),
  createStatus: document.getElementById("createStatus"),
  refreshPlans: document.getElementById("refreshPlans"),
  addPlanRowBtn: document.getElementById("addPlanRowBtn"),
  plansTableBody: document.getElementById("plansTableBody"),
  plansTableStatus: document.getElementById("plansTableStatus"),
  planDeleteModal: document.getElementById("planDeleteModal"),
  planDeleteModalMessage: document.getElementById("planDeleteModalMessage"),
  planDeleteCancelBtn: document.getElementById("planDeleteCancelBtn"),
  planDeleteConfirmBtn: document.getElementById("planDeleteConfirmBtn"),
  plansList: document.getElementById("plansList"),
  selectedPlanId: document.getElementById("selectedPlanId"),
  planPerformanceDateRange: document.getElementById("planPerformanceDateRange"),
  planPerformanceStartDate: document.getElementById("planPerformanceStartDate"),
  planPerformanceEndDate: document.getElementById("planPerformanceEndDate"),
  planPerformanceDatePresets: document.getElementById("planPerformanceDatePresets"),
  planPriceDateRange: document.getElementById("planPriceDateRange"),
  planPriceStartDate: document.getElementById("planPriceStartDate"),
  planPriceEndDate: document.getElementById("planPriceEndDate"),
  planPriceDatePresets: document.getElementById("planPriceDatePresets"),
  savePlanDateRanges: document.getElementById("savePlanDateRanges"),
  planDateRangesStatus: document.getElementById("planDateRangesStatus"),
  paramKey: document.getElementById("paramKey"),
  paramValue: document.getElementById("paramValue"),
  paramType: document.getElementById("paramType"),
  saveParameter: document.getElementById("saveParameter"),
  decisionType: document.getElementById("decisionType"),
  decisionValue: document.getElementById("decisionValue"),
  decisionState: document.getElementById("decisionState"),
  decisionChannel: document.getElementById("decisionChannel"),
  addDecision: document.getElementById("addDecision"),
  runPlan: document.getElementById("runPlan"),
  actionStatus: document.getElementById("actionStatus"),
  planSelector: document.getElementById("planSelector"),
  activityLeadTypeFilter: document.getElementById("activityLeadTypeFilter"),
  menuItems: document.querySelectorAll(".menu-item[data-section]"),
  planSectionBtn: document.getElementById("planSectionBtn"),
  planMenuSubmenu: document.getElementById("planMenuSubmenu"),
  planTabBuilder: document.getElementById("planTabBuilder"),
  planTabTargets: document.getElementById("planTabTargets"),
  planTabStrategy: document.getElementById("planTabStrategy"),
  planTabPriceDecision: document.getElementById("planTabPriceDecision"),
  planTabOutcome: document.getElementById("planTabOutcome"),
  planBuilderPanel: document.getElementById("planBuilderPanel"),
  targetsPanel: document.getElementById("targetsPanel"),
  planStrategyPanel: document.getElementById("planStrategyPanel"),
  priceDecisionPanel: document.getElementById("priceDecisionPanel"),
  planOutcomePanel: document.getElementById("planOutcomePanel"),
  analyticsSectionBtn: document.getElementById("analyticsSectionBtn"),
  analyticsMenuSubmenu: document.getElementById("analyticsMenuSubmenu"),
  settingsMenuSubmenu: document.getElementById("settingsMenuSubmenu"),
  settingsSubGlobalFilters: document.getElementById("settingsSubGlobalFilters"),
  settingsSubUsers: document.getElementById("settingsSubUsers"),
  settingsGlobalFiltersPanel: document.getElementById("settingsGlobalFiltersPanel"),
  settingsUsersPanel: document.getElementById("settingsUsersPanel"),
  newUserEmail: document.getElementById("newUserEmail"),
  addUserBtn: document.getElementById("addUserBtn"),
  refreshUsersBtn: document.getElementById("refreshUsersBtn"),
  usersStatus: document.getElementById("usersStatus"),
  usersTableBody: document.getElementById("usersTableBody"),
  sectionPanels: document.querySelectorAll("[data-section-panel]"),
  analyticsTabStateSegment: document.getElementById("analyticsTabStateSegment"),
  analyticsTabPriceExploration: document.getElementById("analyticsTabPriceExploration"),
  analyticsTabStrategyAnalysis: document.getElementById("analyticsTabStrategyAnalysis"),
  analyticsTabPlansComparison: document.getElementById("analyticsTabPlansComparison"),
  analyticsTabStateAnalysis: document.getElementById("analyticsTabStateAnalysis"),
  analyticsTabStatePlanAnalysis: document.getElementById("analyticsTabStatePlanAnalysis"),
  stateSegmentPanel: document.getElementById("stateSegmentPanel"),
  priceExplorationPanel: document.getElementById("priceExplorationPanel"),
  strategyAnalysisPanel: document.getElementById("strategyAnalysisPanel"),
  plansComparisonPanel: document.getElementById("plansComparisonPanel"),
  stateAnalysisPanel: document.getElementById("stateAnalysisPanel"),
  statePlanAnalysisPanel: document.getElementById("statePlanAnalysisPanel"),
  stateSegmentDateRange: document.getElementById("stateSegmentDateRange"),
  startDate: document.getElementById("startDate"),
  endDate: document.getElementById("endDate"),
  stateSegmentDatePresets: document.getElementById("stateSegmentDatePresets"),
  stateSegmentViewMode: document.getElementById("stateSegmentViewMode"),
  applyAnalyticsFilters: document.getElementById("applyAnalyticsFilters"),
  clearAnalyticsFilters: document.getElementById("clearAnalyticsFilters"),
  stateSegmentLoading: document.getElementById("stateSegmentLoading"),
  analyticsStatus: document.getElementById("analyticsStatus"),
  stateSegmentTableBody: document.getElementById("stateSegmentTableBody"),
  statesFilterToggle: document.getElementById("statesFilterToggle"),
  statesFilterMenu: document.getElementById("statesFilterMenu"),
  segmentsFilterToggle: document.getElementById("segmentsFilterToggle"),
  segmentsFilterMenu: document.getElementById("segmentsFilterMenu"),
  stateSegmentChannelsFilterToggle: document.getElementById("stateSegmentChannelsFilterToggle"),
  stateSegmentChannelsFilterMenu: document.getElementById("stateSegmentChannelsFilterMenu"),
  priceDateRange: document.getElementById("priceDateRange"),
  priceStartDate: document.getElementById("priceStartDate"),
  priceEndDate: document.getElementById("priceEndDate"),
  priceDatePresets: document.getElementById("priceDatePresets"),
  priceStatesFilterToggle: document.getElementById("priceStatesFilterToggle"),
  priceStatesFilterMenu: document.getElementById("priceStatesFilterMenu"),
  priceChannelGroupsFilterToggle: document.getElementById("priceChannelGroupsFilterToggle"),
  priceChannelGroupsFilterMenu: document.getElementById("priceChannelGroupsFilterMenu"),
  applyPriceExplorationFilters: document.getElementById("applyPriceExplorationFilters"),
  clearPriceExplorationFilters: document.getElementById("clearPriceExplorationFilters"),
  showOnlyRecommendedTp: document.getElementById("showOnlyRecommendedTp"),
  priceExplorationLoading: document.getElementById("priceExplorationLoading"),
  priceExplorationStatus: document.getElementById("priceExplorationStatus"),
  priceExplorationTableBody: document.getElementById("priceExplorationTableBody"),
  kpiBids: document.getElementById("kpiBids"),
  kpiWinRate: document.getElementById("kpiWinRate"),
  kpiSold: document.getElementById("kpiSold"),
  kpiCpc: document.getElementById("kpiCpc"),
  kpiAvgBid: document.getElementById("kpiAvgBid"),
  kpiWinRateUplift: document.getElementById("kpiWinRateUplift"),
  kpiCpcUplift: document.getElementById("kpiCpcUplift"),
  kpiAdditionalClicks: document.getElementById("kpiAdditionalClicks"),
  kpiAdditionalBudget: document.getElementById("kpiAdditionalBudget"),
  kpiAdditionalBinds: document.getElementById("kpiAdditionalBinds"),
  priceDecisionDateRange: document.getElementById("priceDecisionDateRange"),
  priceDecisionStartDate: document.getElementById("priceDecisionStartDate"),
  priceDecisionEndDate: document.getElementById("priceDecisionEndDate"),
  priceDecisionDatePresets: document.getElementById("priceDecisionDatePresets"),
  priceDecisionStatesFilterToggle: document.getElementById("priceDecisionStatesFilterToggle"),
  priceDecisionStatesFilterMenu: document.getElementById("priceDecisionStatesFilterMenu"),
  priceDecisionChannelsFilterToggle: document.getElementById("priceDecisionChannelsFilterToggle"),
  priceDecisionChannelsFilterMenu: document.getElementById("priceDecisionChannelsFilterMenu"),
  priceDecisionSegmentsFilterToggle: document.getElementById("priceDecisionSegmentsFilterToggle"),
  priceDecisionSegmentsFilterMenu: document.getElementById("priceDecisionSegmentsFilterMenu"),
  applyPriceDecisionFilters: document.getElementById("applyPriceDecisionFilters"),
  clearPriceDecisionFilters: document.getElementById("clearPriceDecisionFilters"),
  priceDecisionLoading: document.getElementById("priceDecisionLoading"),
  priceDecisionStatus: document.getElementById("priceDecisionStatus"),
  priceDecisionCards: document.getElementById("priceDecisionCards"),
  priceDecisionDetailTitle: document.getElementById("priceDecisionDetailTitle"),
  priceDecisionDetailRule: document.getElementById("priceDecisionDetailRule"),
  priceDecisionDetailEvidence: document.getElementById("priceDecisionDetailEvidence"),
  priceDecisionTestingPointSelect: document.getElementById("priceDecisionTestingPointSelect"),
  priceDecisionKpiBids: document.getElementById("priceDecisionKpiBids"),
  priceDecisionKpiSold: document.getElementById("priceDecisionKpiSold"),
  priceDecisionKpiBinds: document.getElementById("priceDecisionKpiBinds"),
  priceDecisionKpiRoe: document.getElementById("priceDecisionKpiRoe"),
  priceDecisionKpiCor: document.getElementById("priceDecisionKpiCor"),
  priceDecisionKpiWrUplift: document.getElementById("priceDecisionKpiWrUplift"),
  priceDecisionKpiCpcUplift: document.getElementById("priceDecisionKpiCpcUplift"),
  priceDecisionKpiCpbUplift: document.getElementById("priceDecisionKpiCpbUplift"),
  priceDecisionKpiAdditionalClicks: document.getElementById("priceDecisionKpiAdditionalClicks"),
  priceDecisionKpiAdditionalBinds: document.getElementById("priceDecisionKpiAdditionalBinds"),
  priceDecisionKpiAdditionalBudget: document.getElementById("priceDecisionKpiAdditionalBudget"),
  priceDecisionImpactChart: document.getElementById("priceDecisionImpactChart"),
  priceDecisionTableBody: document.getElementById("priceDecisionTableBody"),
  planOutcomeLoading: document.getElementById("planOutcomeLoading"),
  planOutcomeStatus: document.getElementById("planOutcomeStatus"),
  planOutcomeTableBody: document.getElementById("planOutcomeTableBody"),
  strategyAnalysisDateRange: document.getElementById("strategyAnalysisDateRange"),
  strategyAnalysisStartDate: document.getElementById("strategyAnalysisStartDate"),
  strategyAnalysisEndDate: document.getElementById("strategyAnalysisEndDate"),
  strategyAnalysisDatePresets: document.getElementById("strategyAnalysisDatePresets"),
  strategyAnalysisViewMode: document.getElementById("strategyAnalysisViewMode"),
  strategyAnalysisPrimaryHeader: document.getElementById("strategyAnalysisPrimaryHeader"),
  applyStrategyAnalysisFilters: document.getElementById("applyStrategyAnalysisFilters"),
  strategyAnalysisLoading: document.getElementById("strategyAnalysisLoading"),
  strategyAnalysisStatus: document.getElementById("strategyAnalysisStatus"),
  strategyAnalysisTableBody: document.getElementById("strategyAnalysisTableBody"),
  plansComparisonDateRange: document.getElementById("plansComparisonDateRange"),
  plansComparisonStartDate: document.getElementById("plansComparisonStartDate"),
  plansComparisonEndDate: document.getElementById("plansComparisonEndDate"),
  plansComparisonDatePresets: document.getElementById("plansComparisonDatePresets"),
  plansComparisonMode: document.getElementById("plansComparisonMode"),
  plansComparisonPlanWrap: document.getElementById("plansComparisonPlanWrap"),
  plansComparisonPlanId: document.getElementById("plansComparisonPlanId"),
  plansComparisonPrimaryHeader: document.getElementById("plansComparisonPrimaryHeader"),
  applyPlansComparisonFilters: document.getElementById("applyPlansComparisonFilters"),
  plansComparisonLoading: document.getElementById("plansComparisonLoading"),
  plansComparisonStatus: document.getElementById("plansComparisonStatus"),
  plansComparisonTableBody: document.getElementById("plansComparisonTableBody"),
  stateAnalysisDateRange: document.getElementById("stateAnalysisDateRange"),
  stateAnalysisStartDate: document.getElementById("stateAnalysisStartDate"),
  stateAnalysisEndDate: document.getElementById("stateAnalysisEndDate"),
  stateAnalysisDatePresets: document.getElementById("stateAnalysisDatePresets"),
  applyStateAnalysisFilters: document.getElementById("applyStateAnalysisFilters"),
  stateAnalysisLoading: document.getElementById("stateAnalysisLoading"),
  stateAnalysisStatus: document.getElementById("stateAnalysisStatus"),
  stateAnalysisKpiBids: document.getElementById("stateAnalysisKpiBids"),
  stateAnalysisKpiWr: document.getElementById("stateAnalysisKpiWr"),
  stateAnalysisKpiTotalSpend: document.getElementById("stateAnalysisKpiTotalSpend"),
  stateAnalysisKpiQ2b: document.getElementById("stateAnalysisKpiQ2b"),
  stateAnalysisKpiBinds: document.getElementById("stateAnalysisKpiBinds"),
  stateAnalysisKpiCpb: document.getElementById("stateAnalysisKpiCpb"),
  stateAnalysisKpiRoe: document.getElementById("stateAnalysisKpiRoe"),
  stateAnalysisKpiCor: document.getElementById("stateAnalysisKpiCor"),
  stateAnalysisKpiLtv: document.getElementById("stateAnalysisKpiLtv"),
  stateAnalysisKpiAdditionalClicks: document.getElementById("stateAnalysisKpiAdditionalClicks"),
  stateAnalysisKpiAdditionalBinds: document.getElementById("stateAnalysisKpiAdditionalBinds"),
  stateAnalysisKpiAdditionalBudget: document.getElementById("stateAnalysisKpiAdditionalBudget"),
  stateAnalysisMapSvg: document.getElementById("stateAnalysisMapSvg"),
  stateAnalysisMapTooltip: document.getElementById("stateAnalysisMapTooltip"),
  stateAnalysisRulesContainer: document.getElementById("stateAnalysisRulesContainer"),
  statePlanAnalysisDateRange: document.getElementById("statePlanAnalysisDateRange"),
  statePlanAnalysisStartDate: document.getElementById("statePlanAnalysisStartDate"),
  statePlanAnalysisEndDate: document.getElementById("statePlanAnalysisEndDate"),
  statePlanAnalysisDatePresets: document.getElementById("statePlanAnalysisDatePresets"),
  applyStatePlanAnalysisFilters: document.getElementById("applyStatePlanAnalysisFilters"),
  statePlanAnalysisLoading: document.getElementById("statePlanAnalysisLoading"),
  statePlanAnalysisStatus: document.getElementById("statePlanAnalysisStatus"),
  statePlanAnalysisKpiBids: document.getElementById("statePlanAnalysisKpiBids"),
  statePlanAnalysisKpiWr: document.getElementById("statePlanAnalysisKpiWr"),
  statePlanAnalysisKpiTotalSpend: document.getElementById("statePlanAnalysisKpiTotalSpend"),
  statePlanAnalysisKpiQ2b: document.getElementById("statePlanAnalysisKpiQ2b"),
  statePlanAnalysisKpiBinds: document.getElementById("statePlanAnalysisKpiBinds"),
  statePlanAnalysisKpiCpb: document.getElementById("statePlanAnalysisKpiCpb"),
  statePlanAnalysisKpiRoe: document.getElementById("statePlanAnalysisKpiRoe"),
  statePlanAnalysisKpiCor: document.getElementById("statePlanAnalysisKpiCor"),
  statePlanAnalysisKpiLtv: document.getElementById("statePlanAnalysisKpiLtv"),
  statePlanAnalysisKpiAdditionalClicks: document.getElementById("statePlanAnalysisKpiAdditionalClicks"),
  statePlanAnalysisKpiAdditionalBinds: document.getElementById("statePlanAnalysisKpiAdditionalBinds"),
  statePlanAnalysisKpiAdditionalBudget: document.getElementById("statePlanAnalysisKpiAdditionalBudget"),
  statePlanAnalysisMapSvg: document.getElementById("statePlanAnalysisMapSvg"),
  statePlanAnalysisMapTooltip: document.getElementById("statePlanAnalysisMapTooltip"),
  statePlanAnalysisDetailTitle: document.getElementById("statePlanAnalysisDetailTitle"),
  statePlanAnalysisDetailMeta: document.getElementById("statePlanAnalysisDetailMeta"),
  statePlanStateKpiBids: document.getElementById("statePlanStateKpiBids"),
  statePlanStateKpiWr: document.getElementById("statePlanStateKpiWr"),
  statePlanStateKpiTotalSpend: document.getElementById("statePlanStateKpiTotalSpend"),
  statePlanStateKpiQ2b: document.getElementById("statePlanStateKpiQ2b"),
  statePlanStateKpiBinds: document.getElementById("statePlanStateKpiBinds"),
  statePlanStateKpiCpb: document.getElementById("statePlanStateKpiCpb"),
  statePlanStateKpiRoe: document.getElementById("statePlanStateKpiRoe"),
  statePlanStateKpiCor: document.getElementById("statePlanStateKpiCor"),
  statePlanStateKpiLtv: document.getElementById("statePlanStateKpiLtv"),
  statePlanStateKpiAdditionalClicks: document.getElementById("statePlanStateKpiAdditionalClicks"),
  statePlanStateKpiAdditionalBinds: document.getElementById("statePlanStateKpiAdditionalBinds"),
  statePlanStateKpiAdditionalBudget: document.getElementById("statePlanStateKpiAdditionalBudget"),
  statePlanStateSegmentsBody: document.getElementById("statePlanStateSegmentsBody"),
  targetsDateRange: document.getElementById("targetsDateRange"),
  targetsStartDate: document.getElementById("targetsStartDate"),
  targetsEndDate: document.getElementById("targetsEndDate"),
  targetsDatePresets: document.getElementById("targetsDatePresets"),
  uploadTargetsFile: document.getElementById("uploadTargetsFile"),
  downloadTargetsFile: document.getElementById("downloadTargetsFile"),
  downloadDerivedTargetsFile: document.getElementById("downloadDerivedTargetsFile"),
  targetsFileInput: document.getElementById("targetsFileInput"),
  targetsStatus: document.getElementById("targetsStatus"),
  targetsTableBody: document.getElementById("targetsTableBody"),
  targetsModeCpb: document.getElementById("targetsModeCpb"),
  targetsModeRoe: document.getElementById("targetsModeRoe"),
  targetsModeCor: document.getElementById("targetsModeCor"),
  derivedTargetPanel: document.getElementById("derivedTargetPanel"),
  derivedTargetTitle: document.getElementById("derivedTargetTitle"),
  derivedTargetMetricHeader: document.getElementById("derivedTargetMetricHeader"),
  derivedTargetPreviewMetricHeader: document.getElementById("derivedTargetPreviewMetricHeader"),
  addDerivedTargetRule: document.getElementById("addDerivedTargetRule"),
  adjustDerivedTargetBtn: document.getElementById("adjustDerivedTargetBtn"),
  derivedTargetRulesBody: document.getElementById("derivedTargetRulesBody"),
  derivedTargetRulesStatus: document.getElementById("derivedTargetRulesStatus"),
  derivedTargetPreviewWrap: document.getElementById("derivedTargetPreviewWrap"),
  derivedTargetPreviewBody: document.getElementById("derivedTargetPreviewBody"),
  addPlanStrategyRule: document.getElementById("addPlanStrategyRule"),
  planStrategyRulesBody: document.getElementById("planStrategyRulesBody"),
  planStrategySettingsBody: document.getElementById("planStrategySettingsBody"),
  savePlanStrategyBtn: document.getElementById("savePlanStrategyBtn"),
  planStrategyStatus: document.getElementById("planStrategyStatus"),
  settingsGlobalFiltersBody: document.getElementById("settingsGlobalFiltersBody"),
  settingsGlobalFiltersStatus: document.getElementById("settingsGlobalFiltersStatus"),
  defaultTargetsFileInput: document.getElementById("defaultTargetsFileInput"),
  defaultTargetsFileStatus: document.getElementById("defaultTargetsFileStatus"),
  askAiSectionBtn: document.getElementById("askAiSectionBtn"),
  askAiPanel: document.getElementById("askAiPanel"),
  askAiMessages: document.getElementById("askAiMessages"),
  askAiInput: document.getElementById("askAiInput"),
  askAiSendBtn: document.getElementById("askAiSendBtn"),
  askAiNewSessionBtn: document.getElementById("askAiNewSessionBtn"),
  askAiSessionList: document.getElementById("askAiSessionList"),
};

const tableEnhancers = new Map();
const dateRangeControllers = new Map();
const mainContentLoadTokens = new Set();
let mainContentLoadTokenSeq = 0;
const mainContentLoadTimerByToken = new Map();

const multiSelectMeta = {
  states: {
    toggle: el.statesFilterToggle,
    menu: el.statesFilterMenu,
    allLabel: "All states"
  },
  segments: {
    toggle: el.segmentsFilterToggle,
    menu: el.segmentsFilterMenu,
    allLabel: "All segments"
  },
  stateSegmentChannels: {
    toggle: el.stateSegmentChannelsFilterToggle,
    menu: el.stateSegmentChannelsFilterMenu,
    allLabel: "All channel groups"
  },
  priceStates: {
    toggle: el.priceStatesFilterToggle,
    menu: el.priceStatesFilterMenu,
    allLabel: "All states"
  },
  priceChannels: {
    toggle: el.priceChannelGroupsFilterToggle,
    menu: el.priceChannelGroupsFilterMenu,
    allLabel: "All channels"
  },
  priceDecisionStates: {
    toggle: el.priceDecisionStatesFilterToggle,
    menu: el.priceDecisionStatesFilterMenu,
    allLabel: "All states"
  },
  priceDecisionChannels: {
    toggle: el.priceDecisionChannelsFilterToggle,
    menu: el.priceDecisionChannelsFilterMenu,
    allLabel: "All channel groups"
  },
  priceDecisionSegments: {
    toggle: el.priceDecisionSegmentsFilterToggle,
    menu: el.priceDecisionSegmentsFilterMenu,
    allLabel: "All segments"
  }
};

function toIsoDateAtLocalMidnight(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    return "";
  }
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function getPresetDateRange(presetKey) {
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  if (presetKey === "this_month") {
    const start = new Date(today.getFullYear(), today.getMonth(), 1);
    return { startIso: toIsoDateAtLocalMidnight(start), endIso: toIsoDateAtLocalMidnight(today) };
  }
  if (presetKey === "last_month") {
    const firstOfThisMonth = new Date(today.getFullYear(), today.getMonth(), 1);
    const end = new Date(firstOfThisMonth.getTime() - 24 * 60 * 60 * 1000);
    const start = new Date(end.getFullYear(), end.getMonth(), 1);
    return { startIso: toIsoDateAtLocalMidnight(start), endIso: toIsoDateAtLocalMidnight(end) };
  }
  if (presetKey === "last_14_days") {
    const start = new Date(today);
    start.setDate(start.getDate() - 13);
    return { startIso: toIsoDateAtLocalMidnight(start), endIso: toIsoDateAtLocalMidnight(today) };
  }
  if (presetKey === "last_30_days") {
    const start = new Date(today);
    start.setDate(start.getDate() - 29);
    return { startIso: toIsoDateAtLocalMidnight(start), endIso: toIsoDateAtLocalMidnight(today) };
  }
  return { startIso: "", endIso: "" };
}

function formatRangeDisplayText(startIso, endIso) {
  if (!startIso || !endIso) {
    return "";
  }
  const formatUs = (iso) => {
    const match = String(iso || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!match) {
      return String(iso || "");
    }
    return `${match[2]}/${match[3]}/${match[1]}`;
  };
  return `${formatUs(startIso)} - ${formatUs(endIso)}`;
}

function applyDateRange(key, startIso, endIso, options = {}) {
  const controller = dateRangeControllers.get(key);
  if (!controller) {
    return;
  }

  const nextStart = String(startIso || "");
  const nextEnd = String(endIso || "");
  const currentStart = String(controller.startInput?.value || "");
  const currentEnd = String(controller.endInput?.value || "");
  const changed = currentStart !== nextStart || currentEnd !== nextEnd;

  if (controller.startInput) {
    controller.startInput.value = nextStart;
  }
  if (controller.endInput) {
    controller.endInput.value = nextEnd;
  }
  if (controller.input) {
    controller.input.value = formatRangeDisplayText(nextStart, nextEnd);
  }

  if (controller.picker && options.syncPicker !== false) {
    controller.suppressPickerChange = true;
    if (nextStart && nextEnd) {
      controller.picker.setDate([nextStart, nextEnd], false, "Y-m-d");
    } else {
      controller.picker.clear(false);
    }
    controller.suppressPickerChange = false;
  }

  if (changed && options.trigger !== false && typeof controller.onChange === "function") {
    void controller.onChange();
  }
}

function initializeDateRangePicker({
  key,
  input,
  startInput,
  endInput,
  presetsWrap,
  onChange
}) {
  if (!input || !startInput || !endInput) {
    return;
  }

  const controller = {
    input,
    startInput,
    endInput,
    presetsWrap,
    onChange,
    picker: null,
    suppressPickerChange: false
  };
  dateRangeControllers.set(key, controller);

  if (window.flatpickr) {
    controller.picker = window.flatpickr(input, {
      mode: "range",
      dateFormat: "m/d/Y",
      allowInput: false,
      onChange: (selectedDates) => {
        if (controller.suppressPickerChange || selectedDates.length !== 2) {
          return;
        }
        const [first, second] = selectedDates;
        const start = first <= second ? first : second;
        const end = first <= second ? second : first;
        applyDateRange(key, toIsoDateAtLocalMidnight(start), toIsoDateAtLocalMidnight(end), {
          syncPicker: false,
          trigger: true
        });
      }
    });
  }

  if (presetsWrap) {
    presetsWrap.addEventListener("click", (event) => {
      const target = event.target instanceof HTMLElement ? event.target : null;
      if (!target) {
        return;
      }
      const trigger = target.closest(".date-preset-trigger");
      if (trigger) {
        document.querySelectorAll(".date-range-presets.open").forEach((wrap) => {
          if (wrap !== presetsWrap) {
            wrap.classList.remove("open");
          }
        });
        presetsWrap.classList.toggle("open");
        return;
      }
      const button = target.closest(".date-preset-btn");
      if (!button) {
        return;
      }
      const preset = String(button.dataset.preset || "");
      const range = getPresetDateRange(preset);
      applyDateRange(key, range.startIso, range.endIso, { trigger: true });
      presetsWrap.classList.remove("open");
    });
  }

  const startValue = String(startInput.value || "");
  const endValue = String(endInput.value || "");
  applyDateRange(key, startValue, endValue, { trigger: false });
}

document.addEventListener("click", (event) => {
  const target = event.target;
  if (!(target instanceof Node)) {
    return;
  }
  document.querySelectorAll(".date-range-presets.open").forEach((wrap) => {
    if (wrap.contains(target)) {
      return;
    }
    wrap.classList.remove("open");
  });
});

el.activityLeadTypeFilter.value = state.activityLeadType;
if (el.selectedPlanId) {
  el.selectedPlanId.value = getStoredSelectedPlanId();
}

function setStatus(node, message, isError = false) {
  if (!node) {
    return;
  }
  const text = String(message || "");
  node.textContent = text;
  node.style.color = isError ? "#b00020" : "";
  maybeShowStatusToast(node, text, isError);
}

function ensureToastHost() {
  let host = document.getElementById("toastHost");
  if (host) {
    return host;
  }
  host = document.createElement("div");
  host.id = "toastHost";
  host.className = "toast-host";
  host.setAttribute("aria-live", "polite");
  host.setAttribute("aria-atomic", "true");
  document.body.appendChild(host);
  return host;
}

function showToast(message, variant = "success") {
  const text = String(message || "").trim();
  if (!text) {
    return;
  }
  const host = ensureToastHost();
  const toast = document.createElement("div");
  toast.className = `toast toast-${variant}`;
  toast.textContent = text;
  host.appendChild(toast);
  requestAnimationFrame(() => {
    toast.classList.add("show");
  });
  const hideTimer = setTimeout(() => {
    toast.classList.remove("show");
    setTimeout(() => {
      if (toast.parentNode) {
        toast.parentNode.removeChild(toast);
      }
    }, 220);
    clearTimeout(hideTimer);
  }, TOAST_DURATION_MS);
}

function maybeShowStatusToast(node, message, isError) {
  if (isError || !node) {
    return;
  }
  const nodeId = String(node.id || "");
  if (!TOAST_SUCCESS_STATUS_IDS.has(nodeId)) {
    return;
  }
  if (!TOAST_SUCCESS_REGEX.test(message) || TOAST_EXCLUDE_REGEX.test(message)) {
    return;
  }
  showToast(message, "success");
}

function confirmPlanDelete(planName) {
  return new Promise((resolve) => {
    if (!el.planDeleteModal || !el.planDeleteConfirmBtn || !el.planDeleteCancelBtn) {
      resolve(window.confirm(`Delete plan "${planName}"?`));
      return;
    }

    const modal = el.planDeleteModal;
    const messageNode = el.planDeleteModalMessage;
    if (messageNode) {
      messageNode.textContent = `Delete plan "${planName}"?`;
    }

    const cleanup = () => {
      modal.hidden = true;
      el.planDeleteConfirmBtn.removeEventListener("click", onConfirm);
      el.planDeleteCancelBtn.removeEventListener("click", onCancel);
      modal.removeEventListener("click", onBackdrop);
    };

    const onConfirm = () => {
      cleanup();
      resolve(true);
    };
    const onCancel = () => {
      cleanup();
      resolve(false);
    };
    const onBackdrop = (event) => {
      if (event.target === modal) {
        onCancel();
      }
    };

    el.planDeleteConfirmBtn.addEventListener("click", onConfirm);
    el.planDeleteCancelBtn.addEventListener("click", onCancel);
    modal.addEventListener("click", onBackdrop);
    modal.hidden = false;
  });
}

function setPanelLoading(node, visible) {
  if (!node) {
    return;
  }
  node.hidden = !visible;
}

function updateMainContentLoadingOverlay() {
  if (!el.mainLoadingOverlay) {
    return;
  }
  const visible = mainContentLoadTokens.size > 0 && state.sessionToken && !el.appLayout?.hidden;
  el.mainLoadingOverlay.hidden = !visible;
}

function startMainContentLoading() {
  const token = ++mainContentLoadTokenSeq;
  mainContentLoadTokens.add(token);
  const timer = setTimeout(() => {
    mainContentLoadTokens.delete(token);
    mainContentLoadTimerByToken.delete(token);
    updateMainContentLoadingOverlay();
  }, 60000);
  mainContentLoadTimerByToken.set(token, timer);
  updateMainContentLoadingOverlay();
  return token;
}

function stopMainContentLoading(token) {
  if (token) {
    const timer = mainContentLoadTimerByToken.get(token);
    if (timer) {
      clearTimeout(timer);
      mainContentLoadTimerByToken.delete(token);
    }
    mainContentLoadTokens.delete(token);
  } else {
    for (const timer of mainContentLoadTimerByToken.values()) {
      clearTimeout(timer);
    }
    mainContentLoadTimerByToken.clear();
    mainContentLoadTokens.clear();
  }
  updateMainContentLoadingOverlay();
}

let panelLoadTokenSeq = 0;
const panelLoadTokensByNode = new Map();
const panelLoadTimerByToken = new Map();

function startPanelLoading(node) {
  if (!node) {
    return 0;
  }
  panelLoadTokenSeq += 1;
  const token = panelLoadTokenSeq;
  const tokens = panelLoadTokensByNode.get(node) || new Set();
  tokens.add(token);
  panelLoadTokensByNode.set(node, tokens);
  setPanelLoading(node, true);
  const timer = setTimeout(() => {
    stopPanelLoading(node, token);
  }, 45000);
  panelLoadTimerByToken.set(token, timer);
  return token;
}

function stopPanelLoading(node, token) {
  if (!node) {
    return;
  }
  const timer = panelLoadTimerByToken.get(token);
  if (timer) {
    clearTimeout(timer);
    panelLoadTimerByToken.delete(token);
  }
  const tokens = panelLoadTokensByNode.get(node);
  if (!tokens) {
    setPanelLoading(node, false);
    return;
  }
  tokens.delete(token);
  if (tokens.size === 0) {
    panelLoadTokensByNode.delete(node);
    setPanelLoading(node, false);
    return;
  }
  panelLoadTokensByNode.set(node, tokens);
}

function ensurePriceExplorationBidsColumn() {
  const headerRow = document.querySelector("#priceExplorationTable thead tr");
  if (!headerRow) {
    return;
  }
  const headers = Array.from(headerRow.querySelectorAll("th"));
  const recommendedIdx = headers.findIndex((th) => String(th.textContent || "").trim() === "Recommended TP");
  if (recommendedIdx < 0) {
    return;
  }
  const nextText = String(headers[recommendedIdx + 1]?.textContent || "").trim();
  if (nextText === "Bids") {
    return;
  }
  const th = document.createElement("th");
  th.textContent = "Bids";
  headerRow.insertBefore(th, headers[recommendedIdx + 1] || null);
}

function getTableHeaderCells(table) {
  const thead = table.querySelector("thead");
  if (!thead) {
    return [];
  }
  const rows = Array.from(thead.querySelectorAll("tr")).filter((row) => !row.classList.contains("column-filter-row"));
  if (!rows.length) {
    return [];
  }
  return Array.from(rows[0].querySelectorAll("th"));
}

function parseSortableValue(text) {
  const cleaned = String(text || "")
    .replace(/[$,%]/g, "")
    .replace(/,/g, "")
    .trim();
  const asNumber = Number(cleaned);
  if (Number.isFinite(asNumber)) {
    return asNumber;
  }
  return String(text || "").trim().toLowerCase();
}

function exportTableToExcel(table) {
  const xlsx = window.XLSX;
  if (!xlsx) {
    return;
  }
  const headers = getTableHeaderCells(table).map((th) => String(th.textContent || "").trim());
  if (!headers.length) {
    return;
  }

  const tbody = table.querySelector("tbody");
  const rows = tbody ? Array.from(tbody.querySelectorAll("tr")) : [];
  const body = rows
    .filter((row) => row.style.display !== "none")
    .filter((row) => {
      const cells = row.querySelectorAll("td");
      if (!cells.length) {
        return false;
      }
      if (cells.length === 1 && Number(cells[0].getAttribute("colspan")) > 1) {
        return false;
      }
      return true;
    })
    .map((row) => {
      const cells = Array.from(row.querySelectorAll("td"));
      return headers.map((_, idx) => String(cells[idx]?.innerText || "").trim());
    });

  const worksheet = xlsx.utils.aoa_to_sheet([headers, ...body]);
  const workbook = xlsx.utils.book_new();
  xlsx.utils.book_append_sheet(workbook, worksheet, "Data");
  xlsx.writeFile(workbook, `${table.id || "table"}_${new Date().toISOString().slice(0, 10)}.xlsx`);
}

function ensureTableExportButton(table) {
  const wrap = table.closest(".table-wrap");
  if (!wrap || !wrap.parentElement) {
    return;
  }
  const toolsSelector = `.table-tools[data-table-id="${table.id}"]`;
  let tools = wrap.parentElement.querySelector(toolsSelector);
  if (!tools) {
    tools = document.createElement("div");
    tools.className = "table-tools";
    tools.dataset.tableId = table.id;
    wrap.parentElement.insertBefore(tools, wrap);
  }

  let exportBtn = tools.querySelector(`.table-export-btn[data-table-id="${table.id}"]`);
  if (!exportBtn) {
    exportBtn = document.createElement("button");
    exportBtn.type = "button";
    exportBtn.className = "compact-btn export-btn table-export-btn";
    exportBtn.dataset.tableId = table.id;
    exportBtn.textContent = "Export to Excel";
    exportBtn.addEventListener("click", () => exportTableToExcel(table));
    tools.appendChild(exportBtn);
  }

  if (table.id === "targetsTable") {
    const uploadBtn = document.getElementById("uploadTargetsFile");
    const downloadBtn = document.getElementById("downloadTargetsFile");
    for (const btn of [uploadBtn, downloadBtn]) {
      if (!btn) {
        continue;
      }
      btn.classList.add("compact-btn", "toolbar-btn");
      if (btn.parentElement !== tools) {
        tools.insertBefore(btn, exportBtn);
      }
    }
  }
}

function updateTableSortHeaderUI(headerCells, enhancer) {
  for (const th of headerCells) {
    th.classList.remove("sorted-asc", "sorted-desc");
  }
  if (enhancer.sort.index >= 0 && headerCells[enhancer.sort.index]) {
    headerCells[enhancer.sort.index].classList.add(enhancer.sort.direction === "asc" ? "sorted-asc" : "sorted-desc");
  }
}

function applyTableFilterAndSort(table, enhancer) {
  const tbody = table.querySelector("tbody");
  if (!tbody) {
    return;
  }
  const headerCells = getTableHeaderCells(table);
  const columnCount = headerCells.length;
  if (!columnCount) {
    return;
  }
  updateTableSortHeaderUI(headerCells, enhancer);

  const rows = Array.from(tbody.querySelectorAll("tr"));
  const messageRows = rows.filter((row) => {
    const cells = row.querySelectorAll("td");
    return cells.length === 1 && Number(cells[0].getAttribute("colspan")) > 1;
  });
  const dataRows = rows.filter((row) => !messageRows.includes(row));

  const visibleRows = dataRows.filter((row) => {
    const cells = Array.from(row.querySelectorAll("td"));
    return enhancer.filters.every((filterValue, idx) => {
      const needle = String(filterValue || "").trim().toLowerCase();
      if (!needle) {
        return true;
      }
      const hay = String(cells[idx]?.innerText || "").toLowerCase();
      return hay.includes(needle);
    });
  });

  for (const row of dataRows) {
    row.style.display = visibleRows.includes(row) ? "" : "none";
  }

  if (enhancer.sort.index >= 0) {
    visibleRows.sort((a, b) => {
      const av = parseSortableValue(a.querySelectorAll("td")[enhancer.sort.index]?.innerText || "");
      const bv = parseSortableValue(b.querySelectorAll("td")[enhancer.sort.index]?.innerText || "");
      if (typeof av === "number" && typeof bv === "number") {
        return enhancer.sort.direction === "asc" ? av - bv : bv - av;
      }
      return enhancer.sort.direction === "asc"
        ? String(av).localeCompare(String(bv))
        : String(bv).localeCompare(String(av));
    });
    for (const row of visibleRows) {
      tbody.appendChild(row);
    }
  }

  for (const row of messageRows) {
    row.style.display = visibleRows.length ? "none" : "";
  }
}

function runTableEnhancer(table, enhancer) {
  if (!table || !enhancer || enhancer.isApplying) {
    return;
  }
  enhancer.isApplying = true;
  try {
    buildColumnFilterRow(table, enhancer);
    bindTableSorting(table, enhancer);
    applyTableFilterAndSort(table, enhancer);
  } finally {
    enhancer.isApplying = false;
  }
}

function bindTableSorting(table, enhancer) {
  if (!enhancer.enableSorting) {
    return;
  }
  const headers = getTableHeaderCells(table);
  headers.forEach((th, idx) => {
    if (th.dataset.genericSortBound === "1") {
      return;
    }
    th.dataset.genericSortBound = "1";
    th.classList.add("sortable");
    th.classList.add("generic-sortable");
    th.addEventListener("click", (event) => {
      if (event.target instanceof HTMLInputElement) {
        return;
      }
      if (enhancer.sort.index === idx) {
        enhancer.sort.direction = enhancer.sort.direction === "asc" ? "desc" : "asc";
      } else {
        enhancer.sort.index = idx;
        enhancer.sort.direction = "asc";
      }
      applyTableFilterAndSort(table, enhancer);
    });
  });
}

function buildColumnFilterRow(table, enhancer) {
  const thead = table.querySelector("thead");
  const headers = getTableHeaderCells(table);
  if (!thead || !headers.length) {
    return;
  }

  const old = thead.querySelector("tr.column-filter-row");
  if (old) {
    old.remove();
  }

  const tr = document.createElement("tr");
  tr.className = "column-filter-row";
  headers.forEach((_, idx) => {
    const th = document.createElement("th");
    const input = document.createElement("input");
    input.type = "text";
    input.className = "column-filter-input";
    input.placeholder = "Filter";
    input.value = enhancer.filters[idx] || "";
    input.addEventListener("input", () => {
      enhancer.filters[idx] = input.value;
      applyTableFilterAndSort(table, enhancer);
    });
    th.appendChild(input);
    tr.appendChild(th);
  });
  thead.appendChild(tr);
}

function initializeTableEnhancer(tableId) {
  if (tableEnhancers.has(tableId)) {
    return;
  }
  const table = document.getElementById(tableId);
  if (!table) {
    return;
  }
  const enhancer = {
    filters: [],
    sort: { index: -1, direction: "asc" },
    enableSorting: true,
    observer: null,
    isApplying: false
  };

  ensureTableExportButton(table);
  runTableEnhancer(table, enhancer);
  tableEnhancers.set(tableId, enhancer);
}

function initializeTableEnhancers() {
  for (const tableId of TABLE_ENHANCER_IDS) {
    initializeTableEnhancer(tableId);
  }
}

function isAuthenticated() {
  return Boolean(state.sessionToken);
}

function showLoginScreen(message = "") {
  document.body.dataset.authenticated = "false";
  stopMainContentLoading();
  if (el.loginScreen) {
    el.loginScreen.hidden = false;
    el.loginScreen.style.display = "";
  }
  if (el.appLayout) {
    el.appLayout.hidden = true;
    el.appLayout.style.display = "none";
  }
  if (message) {
    setStatus(el.authStatus, message, false);
  }
}

function showAppLayout() {
  document.body.dataset.authenticated = "true";
  if (el.loginScreen) {
    el.loginScreen.hidden = true;
    el.loginScreen.style.display = "none";
  }
  if (el.appLayout) {
    el.appLayout.hidden = false;
    el.appLayout.style.display = "";
  }
  updateMainContentLoadingOverlay();
}

function setSession(session) {
  state.sessionToken = session.token;
  state.email = session.user.email;
  state.role = session.user.role;
  localStorage.setItem("planning_session_token", state.sessionToken);
  localStorage.setItem("planning_user_email", state.email);
  localStorage.setItem("planning_user_role", state.role);
  showAppLayout();
  // Force UI swap even if browser keeps stale hidden state.
  requestAnimationFrame(() => {
    if (el.loginScreen) {
      el.loginScreen.style.display = "none";
    }
    if (el.appLayout) {
      el.appLayout.hidden = false;
      el.appLayout.style.display = "";
    }
  });
}

function clearSessionLocally() {
  state.sessionToken = "";
  state.email = "";
  state.role = "";
  state.authEmailCandidate = "";
  localStorage.removeItem("planning_session_token");
  localStorage.removeItem("planning_user_email");
  localStorage.removeItem("planning_user_role");
  stopMainContentLoading();
}

function setButtonBusy(button, busy, busyLabel) {
  if (!button) {
    return;
  }
  if (busy) {
    button.dataset.originalLabel = button.textContent || "";
    button.textContent = busyLabel;
    button.disabled = true;
    return;
  }
  button.textContent = button.dataset.originalLabel || button.textContent;
  button.disabled = false;
}

function applyRoleAccessUi() {
  const isAdmin = state.role === "admin";
  if (el.settingsSubUsers) {
    el.settingsSubUsers.hidden = !isAdmin;
  }
  if (!isAdmin && state.activeSettingsTab === "users") {
    setActiveSettingsTab("global-filters");
  }
}

function renderSettingsGlobalFiltersTable() {
  if (!el.settingsGlobalFiltersBody) {
    return;
  }
  el.settingsGlobalFiltersBody.innerHTML = "";
  for (const row of DEFAULT_TARGET_SCOPE_ROWS) {
    const tr = document.createElement("tr");
    const nameTd = document.createElement("td");
    nameTd.textContent = row.label;
    const keyTd = document.createElement("td");
    keyTd.textContent = row.key;
    const fileTd = document.createElement("td");
    const scopeFile = state.settingsDefaultTargetsByScope[row.key];
    fileTd.textContent = scopeFile?.fileName || "bundled Targets-default.xlsx";
    const actionsTd = document.createElement("td");
    const actionWrap = document.createElement("div");
    actionWrap.className = "rule-actions";
    const uploadBtn = document.createElement("button");
    uploadBtn.type = "button";
    uploadBtn.className = "compact-btn";
    uploadBtn.textContent = "Upload";
    uploadBtn.addEventListener("click", () => {
      if (!el.defaultTargetsFileInput) {
        return;
      }
      state.pendingDefaultTargetsUploadScope = row.key;
      el.defaultTargetsFileInput.click();
    });
    const clearBtn = document.createElement("button");
    clearBtn.type = "button";
    clearBtn.className = "compact-btn secondary-btn";
    clearBtn.textContent = "Clear";
    clearBtn.addEventListener("click", async () => {
      try {
        const before = safeLogPayload(state.settingsDefaultTargetsByScope[row.key] || null);
        await clearDefaultTargetsFileForScope(row.key);
        await logChange({
          objectType: "settings_default_target_file",
          action: "clear_default_file",
          before,
          after: null,
          metadata: { scope: row.key }
        });
        renderSettingsGlobalFiltersTable();
        setStatus(el.settingsGlobalFiltersStatus, `Cleared default target file for ${row.label}.`);
      } catch (err) {
        setStatus(el.settingsGlobalFiltersStatus, err.message || "Failed clearing file.", true);
      }
    });
    actionWrap.appendChild(uploadBtn);
    actionWrap.appendChild(clearBtn);
    actionsTd.appendChild(actionWrap);
    tr.appendChild(nameTd);
    tr.appendChild(keyTd);
    tr.appendChild(fileTd);
    tr.appendChild(actionsTd);
    el.settingsGlobalFiltersBody.appendChild(tr);
  }
  setStatus(
    el.settingsGlobalFiltersStatus,
    "Upload default target files per global filter. If empty, bundled Targets-default.xlsx is used."
  );
}

function safeLogPayload(value) {
  try {
    return JSON.parse(JSON.stringify(value));
  } catch {
    return null;
  }
}

async function logChange({ objectType, objectId = "", action, before, after, metadata } = {}) {
  if (!isAuthenticated()) {
    return;
  }
  try {
    await api("/api/change-log", {
      method: "POST",
      body: JSON.stringify({
        objectType,
        objectId: objectId || undefined,
        action,
        before: safeLogPayload(before),
        after: safeLogPayload(after),
        metadata: safeLogPayload(metadata)
      })
    });
  } catch (_err) {
    // Do not block primary action when audit logging fails.
  }
}

function getDerivedRulesStorageKey(mode, activityScope = getActivityScopeKey()) {
  return `${DERIVED_RULES_STORAGE_PREFIX}${activityScope}_${mode}`;
}

function saveDerivedRulesToStorage(mode, rules, activityScope = getActivityScopeKey()) {
  if (mode !== "roe" && mode !== "cor") {
    return;
  }
  const payload = (rules || []).map((rule) => ({
    id: Number(rule.id) || Date.now(),
    name: String(rule.name || "").trim(),
    states: Array.isArray(rule.states) ? rule.states : [],
    segments: Array.isArray(rule.segments) ? rule.segments : [],
    targetValue: Number(rule.targetValue) || 0,
    isEditing: Boolean(rule.isEditing)
  }));
  localStorage.setItem(getDerivedRulesStorageKey(mode, activityScope), JSON.stringify(payload));
}

function loadDerivedRulesFromStorage(mode, activityScope = getActivityScopeKey()) {
  if (mode !== "roe" && mode !== "cor") {
    return [];
  }
  const scopedRaw = localStorage.getItem(getDerivedRulesStorageKey(mode, activityScope));
  const legacyRaw = localStorage.getItem(`${DERIVED_RULES_STORAGE_PREFIX}${mode}`);
  const raw = scopedRaw || legacyRaw;
  if (!raw) {
    return [];
  }
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed.map((rule, index) => ({
      id: Number(rule.id) || index + 1,
      name: String(rule.name || "").trim(),
      states: Array.isArray(rule.states) ? rule.states.map((value) => String(value || "").toUpperCase()).filter(Boolean) : [],
      segments: Array.isArray(rule.segments)
        ? rule.segments.map((value) => String(value || "").toUpperCase()).filter(Boolean)
        : [],
      targetValue: Number(rule.targetValue) || 0,
      isEditing: Boolean(rule.isEditing)
    }));
  } catch {
    return [];
  }
}

function persistDerivedRulesForCurrentMode() {
  if (state.targetsGoalMode === "cpb") {
    return;
  }
  saveDerivedRulesToStorage(state.targetsGoalMode, state.derivedTargetRules);
  if (isAuthenticated()) {
    void persistDerivedRulesSharedForSelectedPlan().catch(() => {
      // Keep UI responsive; local cache remains available.
    });
  }
}

function getSelectedPlanId() {
  return String(el.selectedPlanId?.value || "").trim();
}

function appendSelectedPlanId(params) {
  const planId = getSelectedPlanId();
  if (planId) {
    params.set("planId", planId);
  }
}

function withSelectedPlanId(path) {
  const planId = getSelectedPlanId();
  if (!planId) {
    return path;
  }
  const separator = path.includes("?") ? "&" : "?";
  return `${path}${separator}planId=${encodeURIComponent(planId)}`;
}

function normalizeIsoDateInput(value) {
  const text = String(value || "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(text) ? text : "";
}

function normalizePlanContextPayload(raw) {
  const qbcClicks = Number(raw?.qbcClicks);
  const qbcLeadsCalls = Number(raw?.qbcLeadsCalls);
  return {
    performanceStartDate: normalizeIsoDateInput(raw?.performanceStartDate),
    performanceEndDate: normalizeIsoDateInput(raw?.performanceEndDate),
    priceExplorationStartDate: normalizeIsoDateInput(raw?.priceExplorationStartDate),
    priceExplorationEndDate: normalizeIsoDateInput(raw?.priceExplorationEndDate),
    qbcClicks: Number.isFinite(qbcClicks) && qbcClicks >= 0 ? qbcClicks : DEFAULT_PLAN_QBC_CLICKS,
    qbcLeadsCalls:
      Number.isFinite(qbcLeadsCalls) && qbcLeadsCalls >= 0 ? qbcLeadsCalls : DEFAULT_PLAN_QBC_LEADS_CALLS
  };
}

function parsePlanContextPayload(raw) {
  if (!raw || !String(raw).trim()) {
    return normalizePlanContextPayload({});
  }
  try {
    return normalizePlanContextPayload(JSON.parse(raw));
  } catch {
    return normalizePlanContextPayload({});
  }
}

function getPlanPerformanceRange() {
  const startIso = normalizeIsoDateInput(state.planContext.performanceStartDate);
  const endIso = normalizeIsoDateInput(state.planContext.performanceEndDate);
  if (startIso && endIso) {
    return { startIso, endIso };
  }
  return computeRangeFromToday(30, 7);
}

function getPlanPriceExplorationRange() {
  const startIso = normalizeIsoDateInput(state.planContext.priceExplorationStartDate);
  const endIso = normalizeIsoDateInput(state.planContext.priceExplorationEndDate);
  if (startIso && endIso) {
    return { startIso, endIso };
  }
  return computeRangeFromToday(30, 7);
}

function applySharedPerformanceDateRange(startIso, endIso) {
  const normalizedStart = normalizeIsoDateInput(startIso);
  const normalizedEnd = normalizeIsoDateInput(endIso);
  state.planContext.performanceStartDate = normalizedStart;
  state.planContext.performanceEndDate = normalizedEnd;
  applyDateRange("stateSegment", normalizedStart, normalizedEnd, { trigger: false });
  applyDateRange("strategyAnalysis", normalizedStart, normalizedEnd, { trigger: false });
  applyDateRange("plansComparison", normalizedStart, normalizedEnd, { trigger: false });
  applyDateRange("stateAnalysis", normalizedStart, normalizedEnd, { trigger: false });
  applyDateRange("statePlanAnalysis", normalizedStart, normalizedEnd, { trigger: false });
}

function applySharedPriceExplorationDateRange(startIso, endIso) {
  const normalizedStart = normalizeIsoDateInput(startIso);
  const normalizedEnd = normalizeIsoDateInput(endIso);
  state.planContext.priceExplorationStartDate = normalizedStart;
  state.planContext.priceExplorationEndDate = normalizedEnd;
  applyDateRange("priceExploration", normalizedStart, normalizedEnd, { trigger: false });
  applyDateRange("priceDecision", normalizedStart, normalizedEnd, { trigger: false });
}

function syncPlanDateRangeEditorsFromContext() {
  applyDateRange(
    "planPerformance",
    state.planContext.performanceStartDate,
    state.planContext.performanceEndDate,
    { trigger: false }
  );
  applyDateRange(
    "planPriceExploration",
    state.planContext.priceExplorationStartDate,
    state.planContext.priceExplorationEndDate,
    { trigger: false }
  );
}

function getActivityScopeKey(value = state.activityLeadType) {
  const normalized = String(value || "all").trim().toLowerCase();
  return ACTIVITY_SCOPE_KEYS.has(normalized) ? normalized : "all";
}

function getSelectedPlanStorageKey(activityScope = getActivityScopeKey()) {
  return `${SELECTED_PLAN_ID_STORAGE_PREFIX}${activityScope}`;
}

function getStoredSelectedPlanId(activityScope = getActivityScopeKey()) {
  return localStorage.getItem(getSelectedPlanStorageKey(activityScope)) || "";
}

function setStoredSelectedPlanId(planId, activityScope = getActivityScopeKey()) {
  const value = String(planId || "").trim();
  if (!value) {
    localStorage.removeItem(getSelectedPlanStorageKey(activityScope));
    return;
  }
  localStorage.setItem(getSelectedPlanStorageKey(activityScope), value);
  localStorage.setItem(SELECTED_PLAN_ID_STORAGE_KEY, value);
}

function clearStoredSelectedPlanId(activityScope = getActivityScopeKey()) {
  localStorage.removeItem(getSelectedPlanStorageKey(activityScope));
}

function getPlanStrategyLocalKey(planId) {
  return `${PLAN_STRATEGY_LOCAL_PREFIX}${String(planId || "").trim()}`;
}

function getPlanStrategyLocalScopedKey(planId, activityScope = getActivityScopeKey()) {
  return `${PLAN_STRATEGY_LOCAL_SCOPE_PREFIX}${activityScope}_${String(planId || "").trim()}`;
}

function readPlanStrategyLocalBackup(planId, activityScope = getActivityScopeKey()) {
  if (!planId) {
    return null;
  }
  try {
    const raw =
      localStorage.getItem(getPlanStrategyLocalScopedKey(planId, activityScope)) ||
      localStorage.getItem(getPlanStrategyLocalKey(planId));
    if (!raw) {
      return null;
    }
    const parsed = JSON.parse(raw);
    if (!parsed || !Array.isArray(parsed.rules)) {
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

function writePlanStrategyLocalBackup(planId, payload, activityScope = getActivityScopeKey()) {
  if (!planId || !payload || !Array.isArray(payload.rules)) {
    return;
  }
  const scopedPayload = JSON.stringify({
    ...payload,
    activityScope,
    localSavedAt: new Date().toISOString()
  });
  localStorage.setItem(getPlanStrategyLocalScopedKey(planId, activityScope), scopedPayload);
  localStorage.setItem(
    getPlanStrategyLocalKey(planId),
    scopedPayload
  );
}

function parsePlanStrategyScopedConfig(rawValue, activityScope = getActivityScopeKey()) {
  const fallback = { config: {}, scopePayload: { rules: [], savedAt: "" }, rules: [], savedAt: "" };
  if (!rawValue || !String(rawValue).trim()) {
    return fallback;
  }
  try {
    const parsed = JSON.parse(rawValue);
    const parsedScopes = parsed && typeof parsed === "object" && parsed.scopes && typeof parsed.scopes === "object"
      ? parsed.scopes
      : null;
    if (parsedScopes) {
      const scoped = parsedScopes[activityScope] || parsedScopes.all || { rules: [], savedAt: "" };
      const scopedRules = Array.isArray(scoped?.rules) ? scoped.rules : [];
      return {
        config: parsed,
        scopePayload: {
          rules: scopedRules,
          savedAt: String(scoped?.savedAt || "")
        },
        rules: scopedRules,
        savedAt: String(scoped?.savedAt || "")
      };
    }
    const legacyRules = Array.isArray(parsed?.rules) ? parsed.rules : [];
    const useLegacyForScope = activityScope === "clicks_auto" || activityScope === "all";
    return {
      config: parsed,
      scopePayload: {
        rules: useLegacyForScope ? legacyRules : [],
        savedAt: useLegacyForScope ? String(parsed?.savedAt || "") : ""
      },
      rules: useLegacyForScope ? legacyRules : [],
      savedAt: useLegacyForScope ? String(parsed?.savedAt || "") : ""
    };
  } catch {
    return fallback;
  }
}

function mergePlanStrategyScopedConfig(existingConfig, activityScope, scopePayload) {
  const base = existingConfig && typeof existingConfig === "object" ? existingConfig : {};
  const scopes =
    base.scopes && typeof base.scopes === "object"
      ? { ...base.scopes }
      : {};
  scopes[activityScope] = {
    rules: Array.isArray(scopePayload?.rules) ? scopePayload.rules : [],
    savedAt: String(scopePayload?.savedAt || new Date().toISOString())
  };
  return {
    ...base,
    version: 2,
    scopes,
    updatedAt: new Date().toISOString()
  };
}

function parsePriceDecisionScopedConfig(rawValue, activityScope = getActivityScopeKey()) {
  const fallback = { config: {}, scopePayload: { overrides: [], savedAt: "" }, overrides: [] };
  if (!rawValue || !String(rawValue).trim()) {
    return fallback;
  }
  try {
    const parsed = JSON.parse(rawValue);
    const parsedScopes = parsed && typeof parsed === "object" && parsed.scopes && typeof parsed.scopes === "object"
      ? parsed.scopes
      : null;
    if (parsedScopes) {
      const scoped = parsedScopes[activityScope] || parsedScopes.all || { overrides: [], savedAt: "" };
      const overrides = Array.isArray(scoped?.overrides) ? scoped.overrides : [];
      return {
        config: parsed,
        scopePayload: {
          overrides,
          savedAt: String(scoped?.savedAt || "")
        },
        overrides
      };
    }
    const legacyOverrides = Array.isArray(parsed?.overrides) ? parsed.overrides : [];
    const useLegacyForScope = activityScope === "clicks_auto" || activityScope === "all";
    return {
      config: parsed,
      scopePayload: {
        overrides: useLegacyForScope ? legacyOverrides : [],
        savedAt: useLegacyForScope ? String(parsed?.savedAt || "") : ""
      },
      overrides: useLegacyForScope ? legacyOverrides : []
    };
  } catch {
    return fallback;
  }
}

function mergePriceDecisionScopedConfig(existingConfig, activityScope, scopePayload) {
  const base = existingConfig && typeof existingConfig === "object" ? existingConfig : {};
  const scopes =
    base.scopes && typeof base.scopes === "object"
      ? { ...base.scopes }
      : {};
  scopes[activityScope] = {
    overrides: Array.isArray(scopePayload?.overrides) ? scopePayload.overrides : [],
    savedAt: String(scopePayload?.savedAt || new Date().toISOString())
  };
  return {
    ...base,
    version: 2,
    scopes,
    updatedAt: new Date().toISOString()
  };
}

function sanitizeDerivedRules(rules) {
  return (Array.isArray(rules) ? rules : []).map((rule, index) => ({
    id: Number(rule.id) || index + 1,
    name: String(rule.name || "").trim(),
    states: Array.isArray(rule.states) ? rule.states.map((value) => String(value || "").toUpperCase()).filter(Boolean) : [],
    segments: Array.isArray(rule.segments)
      ? rule.segments.map((value) => String(value || "").toUpperCase()).filter(Boolean)
      : [],
    targetValue: Number(rule.targetValue) || 0,
    isEditing: Boolean(rule.isEditing)
  }));
}

function parseTargetsDerivedRulesScopedConfig(rawValue, activityScope = getActivityScopeKey()) {
  const fallback = { config: {}, scopePayload: { roeRules: [], corRules: [], savedAt: "" } };
  if (!rawValue || !String(rawValue).trim()) {
    return fallback;
  }
  try {
    const parsed = JSON.parse(rawValue);
    const parsedScopes = parsed && typeof parsed === "object" && parsed.scopes && typeof parsed.scopes === "object"
      ? parsed.scopes
      : null;
    if (!parsedScopes) {
      return fallback;
    }
    const scoped = parsedScopes[activityScope] || parsedScopes.all || {};
    return {
      config: parsed,
      scopePayload: {
        roeRules: sanitizeDerivedRules(scoped?.roeRules),
        corRules: sanitizeDerivedRules(scoped?.corRules),
        savedAt: String(scoped?.savedAt || "")
      }
    };
  } catch {
    return fallback;
  }
}

function mergeTargetsDerivedRulesScopedConfig(existingConfig, activityScope, scopePayload) {
  const base = existingConfig && typeof existingConfig === "object" ? existingConfig : {};
  const scopes = base.scopes && typeof base.scopes === "object" ? { ...base.scopes } : {};
  scopes[activityScope] = {
    roeRules: sanitizeDerivedRules(scopePayload?.roeRules),
    corRules: sanitizeDerivedRules(scopePayload?.corRules),
    savedAt: String(scopePayload?.savedAt || new Date().toISOString())
  };
  return {
    ...base,
    version: 1,
    scopes,
    updatedAt: new Date().toISOString()
  };
}

function parseTargetsDefaultFileScopedConfig(rawValue, activityScope = getActivityScopeKey()) {
  const fallback = { config: {}, scopePayload: null };
  if (!rawValue || !String(rawValue).trim()) {
    return fallback;
  }
  try {
    const parsed = JSON.parse(rawValue);
    const parsedScopes = parsed && typeof parsed === "object" && parsed.scopes && typeof parsed.scopes === "object"
      ? parsed.scopes
      : null;
    if (!parsedScopes) {
      return fallback;
    }
    const scoped = parsedScopes[activityScope] || parsedScopes.all || null;
    return {
      config: parsed,
      scopePayload: parseDefaultTargetsFilePayload(scoped)
    };
  } catch {
    return fallback;
  }
}

function mergeTargetsDefaultFileScopedConfig(existingConfig, activityScope, payload) {
  const base = existingConfig && typeof existingConfig === "object" ? existingConfig : {};
  const scopes = base.scopes && typeof base.scopes === "object" ? { ...base.scopes } : {};
  if (payload) {
    scopes[activityScope] = {
      fileName: String(payload.fileName || ""),
      dataUrl: String(payload.dataUrl || ""),
      savedAt: String(payload.savedAt || new Date().toISOString())
    };
  } else {
    delete scopes[activityScope];
  }
  return {
    ...base,
    version: 1,
    scopes,
    updatedAt: new Date().toISOString()
  };
}

async function persistDerivedRulesSharedForSelectedPlan() {
  const planId = getSelectedPlanId() || (await ensureSelectedPlanId());
  if (!planId) {
    return;
  }
  const activityScope = getActivityScopeKey();
  const existingParameters = await api(`/api/plans/${planId}/parameters`);
  const existingParameter = (existingParameters.parameters || []).find(
    (item) => String(item.param_key || "") === TARGETS_DERIVED_RULES_PARAM_KEY
  );
  const parsed = parseTargetsDerivedRulesScopedConfig(String(existingParameter?.param_value || ""), activityScope);
  const mergedPayload = mergeTargetsDerivedRulesScopedConfig(parsed.config, activityScope, {
    roeRules: loadDerivedRulesFromStorage("roe", activityScope),
    corRules: loadDerivedRulesFromStorage("cor", activityScope),
    savedAt: new Date().toISOString()
  });
  await api(`/api/plans/${planId}/parameters`, {
    method: "PUT",
    body: JSON.stringify({
      parameters: [
        {
          key: TARGETS_DERIVED_RULES_PARAM_KEY,
          value: JSON.stringify(mergedPayload),
          valueType: "json"
        }
      ]
    })
  });
}

async function loadDerivedRulesSharedForSelectedPlan() {
  const planId = getSelectedPlanId() || (await ensureSelectedPlanId());
  if (!planId) {
    return;
  }
  const activityScope = getActivityScopeKey();
  const data = await api(`/api/plans/${planId}/parameters`);
  const parameter = (data.parameters || []).find((item) => String(item.param_key || "") === TARGETS_DERIVED_RULES_PARAM_KEY);
  if (!parameter) {
    return;
  }
  const parsed = parseTargetsDerivedRulesScopedConfig(String(parameter.param_value || ""), activityScope);
  saveDerivedRulesToStorage("roe", parsed.scopePayload.roeRules, activityScope);
  saveDerivedRulesToStorage("cor", parsed.scopePayload.corRules, activityScope);
  if (state.targetsGoalMode === "roe" || state.targetsGoalMode === "cor") {
    state.derivedTargetRules = loadDerivedRulesFromStorage(state.targetsGoalMode, activityScope);
    const maxId = state.derivedTargetRules.reduce((acc, rule) => Math.max(acc, Number(rule.id) || 0), 0);
    state.derivedRuleIdCounter = Math.max(maxId + 1, state.derivedRuleIdCounter);
    renderDerivedTargetRules();
  }
}

async function persistDefaultTargetsFileSharedForSelectedPlan(payload, activityScope = getActivityScopeKey()) {
  const planId = getSelectedPlanId() || (await ensureSelectedPlanId());
  if (!planId) {
    return;
  }
  const existingParameters = await api(`/api/plans/${planId}/parameters`);
  const existingParameter = (existingParameters.parameters || []).find(
    (item) => String(item.param_key || "") === TARGETS_DEFAULT_FILE_PARAM_KEY
  );
  const parsed = parseTargetsDefaultFileScopedConfig(String(existingParameter?.param_value || ""), activityScope);
  const mergedPayload = mergeTargetsDefaultFileScopedConfig(parsed.config, activityScope, payload);
  await api(`/api/plans/${planId}/parameters`, {
    method: "PUT",
    body: JSON.stringify({
      parameters: [
        {
          key: TARGETS_DEFAULT_FILE_PARAM_KEY,
          value: JSON.stringify(mergedPayload),
          valueType: "json"
        }
      ]
    })
  });
}

async function loadDefaultTargetsFileSharedForSelectedPlan() {
  const planId = getSelectedPlanId() || (await ensureSelectedPlanId());
  if (!planId) {
    return;
  }
  const activityScope = getActivityScopeKey();
  const data = await api(`/api/plans/${planId}/parameters`);
  const parameter = (data.parameters || []).find((item) => String(item.param_key || "") === TARGETS_DEFAULT_FILE_PARAM_KEY);
  if (!parameter) {
    return;
  }
  const parsed = parseTargetsDefaultFileScopedConfig(String(parameter.param_value || ""), activityScope);
  if (!parsed.scopePayload) {
    return;
  }
  state.defaultTargetsFile = parsed.scopePayload;
  await writeStoredDefaultTargetsFile(parsed.scopePayload);
  updateDefaultTargetsFileStatus();
}

async function loadDefaultTargetsFilesByScopeForSelectedPlan() {
  const planId = getSelectedPlanId() || (await ensureSelectedPlanId());
  if (!planId) {
    state.settingsDefaultTargetsByScope = {};
    return;
  }
  const data = await api(`/api/plans/${planId}/parameters`);
  const parameter = (data.parameters || []).find((item) => String(item.param_key || "") === TARGETS_DEFAULT_FILE_PARAM_KEY);
  if (!parameter) {
    state.settingsDefaultTargetsByScope = {};
    return;
  }
  let parsed;
  try {
    parsed = JSON.parse(String(parameter.param_value || ""));
  } catch {
    state.settingsDefaultTargetsByScope = {};
    return;
  }
  const scopes = parsed && typeof parsed === "object" && parsed.scopes && typeof parsed.scopes === "object"
    ? parsed.scopes
    : {};
  const next = {};
  for (const scopeKey of Object.keys(scopes)) {
    const payload = parseDefaultTargetsFilePayload(scopes[scopeKey]);
    if (payload) {
      next[scopeKey] = payload;
    }
  }
  state.settingsDefaultTargetsByScope = next;
}

async function loadTargetsSharedConfigForSelectedPlan() {
  if (!isAuthenticated()) {
    return;
  }
  await Promise.allSettled([
    loadDerivedRulesSharedForSelectedPlan(),
    loadDefaultTargetsFileSharedForSelectedPlan()
  ]);
}

function getTargetsDefaultRange() {
  return computeRangeFromToday(TARGETS_DEFAULT_FROM_DAYS, TARGETS_DEFAULT_TO_DAYS);
}

function parseLegacyAppSettingsConfig(rawValue) {
  if (!rawValue || !String(rawValue).trim()) {
    return null;
  }
  try {
    const parsed = JSON.parse(String(rawValue));
    const scopes = parsed && typeof parsed === "object" && parsed.scopes && typeof parsed.scopes === "object"
      ? parsed.scopes
      : {};
    const allScope = scopes.all && typeof scopes.all === "object" ? scopes.all : {};
    const clicksScope = scopes.clicks_auto && typeof scopes.clicks_auto === "object" ? scopes.clicks_auto : allScope;
    const leadsScope = scopes.leads_auto && typeof scopes.leads_auto === "object" ? scopes.leads_auto : allScope;
    const qbcClicks = Number(clicksScope.qbcClicks ?? allScope.qbcClicks);
    const qbcLeadsCalls = Number(leadsScope.qbcLeadsCalls ?? allScope.qbcLeadsCalls);
    return {
      qbcClicks: Number.isFinite(qbcClicks) && qbcClicks >= 0 ? qbcClicks : DEFAULT_PLAN_QBC_CLICKS,
      qbcLeadsCalls:
        Number.isFinite(qbcLeadsCalls) && qbcLeadsCalls >= 0 ? qbcLeadsCalls : DEFAULT_PLAN_QBC_LEADS_CALLS
    };
  } catch {
    return null;
  }
}

function getActiveQbcValue() {
  const isClicks = state.activityLeadType.startsWith("clicks_");
  const value = isClicks ? state.planContext.qbcClicks : state.planContext.qbcLeadsCalls;
  const fallback = isClicks ? DEFAULT_PLAN_QBC_CLICKS : DEFAULT_PLAN_QBC_LEADS_CALLS;
  return Number.isFinite(Number(value)) ? Number(value) : fallback;
}

async function persistPlanContextForSelectedPlan() {
  const planId = getSelectedPlanId() || (await ensureSelectedPlanId());
  if (!planId) {
    return;
  }
  const payload = normalizePlanContextPayload(state.planContext);
  await api(`/api/plans/${planId}/parameters`, {
    method: "PUT",
    body: JSON.stringify({
      parameters: [
        {
          key: PLAN_CONTEXT_PARAM_KEY,
          value: JSON.stringify(payload),
          valueType: "json"
        }
      ]
    })
  });
}

async function loadPlanContextForSelectedPlan() {
  if (!isAuthenticated()) {
    return;
  }
  const planId = getSelectedPlanId() || (await ensureSelectedPlanId());
  if (!planId) {
    return;
  }
  const data = await api(`/api/plans/${planId}/parameters`);
  const parameters = Array.isArray(data.parameters) ? data.parameters : [];
  const contextParameter = parameters.find((item) => String(item.param_key || "") === PLAN_CONTEXT_PARAM_KEY);
  const appSettingsParameter = parameters.find((item) => String(item.param_key || "") === APP_SETTINGS_PARAM_KEY);
  const parsedContext = parsePlanContextPayload(String(contextParameter?.param_value || ""));
  const hasQbcInContext =
    String(contextParameter?.param_value || "").includes("qbcClicks") ||
    String(contextParameter?.param_value || "").includes("qbcLeadsCalls");
  if (!hasQbcInContext) {
    const legacyQbc = parseLegacyAppSettingsConfig(String(appSettingsParameter?.param_value || ""));
    if (legacyQbc) {
      parsedContext.qbcClicks = legacyQbc.qbcClicks;
      parsedContext.qbcLeadsCalls = legacyQbc.qbcLeadsCalls;
      state.planContext = parsedContext;
      await persistPlanContextForSelectedPlan();
    } else {
      state.planContext = parsedContext;
    }
  } else {
    state.planContext = parsedContext;
  }
  syncPlanDateRangeEditorsFromContext();
}

async function migrateLegacyQbcForPlan(planId) {
  const normalizedPlanId = String(planId || "").trim();
  if (!normalizedPlanId || migratedPlanQbcIds.has(normalizedPlanId)) {
    return false;
  }
  try {
    const data = await api(`/api/plans/${normalizedPlanId}/parameters`);
    const parameters = Array.isArray(data.parameters) ? data.parameters : [];
    const contextParameter = parameters.find((item) => String(item.param_key || "") === PLAN_CONTEXT_PARAM_KEY);
    const contextRaw = String(contextParameter?.param_value || "");
    if (contextRaw.includes("qbcClicks") || contextRaw.includes("qbcLeadsCalls")) {
      migratedPlanQbcIds.add(normalizedPlanId);
      return false;
    }
    const appSettingsParameter = parameters.find((item) => String(item.param_key || "") === APP_SETTINGS_PARAM_KEY);
    const legacyQbc = parseLegacyAppSettingsConfig(String(appSettingsParameter?.param_value || ""));
    if (!legacyQbc) {
      migratedPlanQbcIds.add(normalizedPlanId);
      return false;
    }
    const nextContext = normalizePlanContextPayload({
      ...parsePlanContextPayload(contextRaw),
      qbcClicks: legacyQbc.qbcClicks,
      qbcLeadsCalls: legacyQbc.qbcLeadsCalls
    });
    await api(`/api/plans/${normalizedPlanId}/parameters`, {
      method: "PUT",
      body: JSON.stringify({
        parameters: [
          {
            key: PLAN_CONTEXT_PARAM_KEY,
            value: JSON.stringify(nextContext),
            valueType: "json"
          }
        ]
      })
    });
    migratedPlanQbcIds.add(normalizedPlanId);
    return true;
  } catch {
    return false;
  }
}

async function migrateLegacyQbcForLoadedPlans() {
  if (!isAuthenticated() || !state.planTableRows.length) {
    return false;
  }
  const pendingPlanIds = state.planTableRows
    .filter((row) => !row.hasQbcConfigured)
    .map((row) => String(row.planId || "").trim())
    .filter(Boolean);
  if (!pendingPlanIds.length) {
    return false;
  }
  const results = await Promise.allSettled(pendingPlanIds.map((planId) => migrateLegacyQbcForPlan(planId)));
  return results.some((result) => result.status === "fulfilled" && result.value === true);
}

function createChecklistPicker(options, selectedValues, onChange, withSelectAll = false) {
  const wrap = document.createElement("div");
  wrap.className = "rule-picker";

  const picked = document.createElement("input");
  picked.type = "text";
  picked.className = "rule-picker-selected";
  picked.readOnly = true;
  picked.value = selectedValues.length ? selectedValues.join(", ") : "";
  picked.placeholder = "Select...";
  wrap.appendChild(picked);

  const list = document.createElement("div");
  list.className = "rule-picker-list";
  const optionCheckboxes = [];

  let selectAllCheckbox = null;
  function isAllSelected() {
    return options.length > 0 && selectedValues.length === options.length;
  }
  function updateSelectAllState() {
    if (!selectAllCheckbox) {
      return;
    }
    selectAllCheckbox.checked = isAllSelected();
  }

  if (withSelectAll) {
    const selectAllLabel = document.createElement("label");
    selectAllLabel.className = "dropdown-option";
    selectAllCheckbox = document.createElement("input");
    selectAllCheckbox.type = "checkbox";
    selectAllCheckbox.checked = isAllSelected();
    selectAllCheckbox.addEventListener("change", () => {
      const values = selectAllCheckbox.checked ? [...options] : [];
      selectedValues.splice(0, selectedValues.length, ...values);
      picked.value = selectedValues.join(", ");
      for (const optionCheckbox of optionCheckboxes) {
        optionCheckbox.checked = selectAllCheckbox.checked;
      }
      onChange(values);
    });
    const selectAllText = document.createElement("span");
    selectAllText.textContent = "Select All";
    selectAllLabel.appendChild(selectAllCheckbox);
    selectAllLabel.appendChild(selectAllText);
    list.appendChild(selectAllLabel);
  }

  for (const value of options) {
    const label = document.createElement("label");
    label.className = "dropdown-option";
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = selectedValues.includes(value);
    optionCheckboxes.push(checkbox);
    checkbox.addEventListener("change", () => {
      const next = new Set(selectedValues);
      if (checkbox.checked) {
        next.add(value);
      } else {
        next.delete(value);
      }
      const values = Array.from(next);
      selectedValues.splice(0, selectedValues.length, ...values);
      picked.value = selectedValues.join(", ");
      updateSelectAllState();
      onChange(values);
    });
    const text = document.createElement("span");
    text.textContent = value;
    label.appendChild(checkbox);
    label.appendChild(text);
    list.appendChild(label);
  }
  updateSelectAllState();
  wrap.appendChild(list);
  return wrap;
}

function createPlanStrategyRule(seed = {}) {
  return {
    id: Number(seed.id) || state.planStrategyRuleIdCounter++,
    name: String(seed.name || "").trim(),
    states: Array.isArray(seed.states) ? seed.states.map((value) => String(value || "").toUpperCase()).filter(Boolean) : [],
    segments: Array.isArray(seed.segments) ? seed.segments.map((value) => String(value || "").toUpperCase()).filter(Boolean) : [],
    maxCpcUplift: Number(seed.maxCpcUplift) || 0,
    maxCpbUplift: Number(seed.maxCpbUplift) || 0,
    corTarget: Number(seed.corTarget) || 0,
    growthStrategy: String(seed.growthStrategy || "balanced"),
    isEditing: seed.isEditing !== undefined ? Boolean(seed.isEditing) : true
  };
}

function getGrowthStrategyOptions() {
  return [
    { value: "high_growth", label: "High Growth" },
    { value: "growth", label: "Growth" },
    { value: "balanced", label: "Balanced" },
    { value: "cost_focused", label: "Cost Focused" },
    { value: "cost_optimized", label: "Cost Optimized" }
  ];
}

function renderPlanStrategySettingsTable() {
  if (!el.planStrategySettingsBody) {
    return;
  }
  el.planStrategySettingsBody.innerHTML = "";

  const savedRules = state.planStrategyRules.filter((rule) => !rule.isEditing);
  if (!savedRules.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 7;
    td.textContent = "No saved rules yet. Save a row in \"States and Segment\" first.";
    tr.appendChild(td);
    el.planStrategySettingsBody.appendChild(tr);
    return;
  }

  for (const rule of savedRules) {
    const settingsTr = document.createElement("tr");
    const nameViewTd = document.createElement("td");
    nameViewTd.textContent = rule.name || "-";
    settingsTr.appendChild(nameViewTd);

    const statesViewTd = document.createElement("td");
    statesViewTd.textContent = rule.states.length ? rule.states.join(", ") : "-";
    settingsTr.appendChild(statesViewTd);

    const segmentsViewTd = document.createElement("td");
    segmentsViewTd.textContent = rule.segments.length ? rule.segments.join(", ") : "-";
    settingsTr.appendChild(segmentsViewTd);

    const maxCpcTd = document.createElement("td");
    const maxCpcInput = document.createElement("input");
    maxCpcInput.type = "number";
    maxCpcInput.step = "0.01";
    maxCpcInput.value = Number(rule.maxCpcUplift || 0).toString();
    maxCpcInput.addEventListener("input", () => {
      rule.maxCpcUplift = Number(maxCpcInput.value) || 0;
    });
    maxCpcTd.appendChild(maxCpcInput);
    settingsTr.appendChild(maxCpcTd);

    const maxCpbTd = document.createElement("td");
    const maxCpbInput = document.createElement("input");
    maxCpbInput.type = "number";
    maxCpbInput.step = "0.01";
    maxCpbInput.value = Number(rule.maxCpbUplift || 0).toString();
    maxCpbInput.addEventListener("input", () => {
      rule.maxCpbUplift = Number(maxCpbInput.value) || 0;
    });
    maxCpbTd.appendChild(maxCpbInput);
    settingsTr.appendChild(maxCpbTd);

    const corTargetTd = document.createElement("td");
    const corTargetInput = document.createElement("input");
    corTargetInput.type = "number";
    corTargetInput.step = "0.01";
    corTargetInput.value = Number(rule.corTarget || 0).toString();
    corTargetInput.addEventListener("input", () => {
      rule.corTarget = Number(corTargetInput.value) || 0;
    });
    corTargetTd.appendChild(corTargetInput);
    settingsTr.appendChild(corTargetTd);

    const growthTd = document.createElement("td");
    const growthSelect = document.createElement("select");
    for (const option of getGrowthStrategyOptions()) {
      const item = document.createElement("option");
      item.value = option.value;
      item.textContent = option.label;
      growthSelect.appendChild(item);
    }
    growthSelect.value = rule.growthStrategy || "balanced";
    growthSelect.addEventListener("change", () => {
      rule.growthStrategy = growthSelect.value;
    });
    growthTd.appendChild(growthSelect);
    settingsTr.appendChild(growthTd);

    el.planStrategySettingsBody.appendChild(settingsTr);
  }
}

function renderPlanStrategyTables() {
  if (!el.planStrategyRulesBody || !el.planStrategySettingsBody) {
    return;
  }

  el.planStrategyRulesBody.innerHTML = "";
  if (!state.planStrategyRules.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 4;
    td.textContent = "No rules yet. Click + to add one.";
    tr.appendChild(td);
    el.planStrategyRulesBody.appendChild(tr);
    renderPlanStrategySettingsTable();
    return;
  }

  for (const rule of state.planStrategyRules) {
    const ruleTr = document.createElement("tr");

    const nameTd = document.createElement("td");
    if (rule.isEditing) {
      const nameInput = document.createElement("input");
      nameInput.type = "text";
      nameInput.placeholder = "Strategy name";
      nameInput.value = rule.name || "";
      nameInput.addEventListener("input", () => {
        rule.name = nameInput.value.trim();
        renderPlanStrategySettingsTable();
      });
      nameTd.appendChild(nameInput);
    } else {
      nameTd.textContent = rule.name || "-";
    }
    ruleTr.appendChild(nameTd);

    const statesTd = document.createElement("td");
    if (rule.isEditing) {
      const statesValue = Array.isArray(rule.states) ? [...rule.states] : [];
      statesTd.appendChild(
        createChecklistPicker(state.planStrategyStateOptions, statesValue, (values) => {
          rule.states = values;
          renderPlanStrategySettingsTable();
        }, true)
      );
    } else {
      statesTd.className = "rule-view-wrap";
      statesTd.textContent = rule.states?.length ? rule.states.join(", ") : "-";
    }
    ruleTr.appendChild(statesTd);

    const segmentsTd = document.createElement("td");
    if (rule.isEditing) {
      const segmentsValue = Array.isArray(rule.segments) ? [...rule.segments] : [];
      segmentsTd.appendChild(
        createChecklistPicker(state.planStrategySegmentOptions, segmentsValue, (values) => {
          rule.segments = values;
          renderPlanStrategySettingsTable();
        })
      );
    } else {
      segmentsTd.className = "rule-view-wrap";
      segmentsTd.textContent = rule.segments?.length ? rule.segments.join(", ") : "-";
    }
    ruleTr.appendChild(segmentsTd);

    const actionTd = document.createElement("td");
    const actions = document.createElement("div");
    actions.className = "rule-actions";
    if (rule.isEditing) {
      const saveBtn = createSaveIconButton("Save Rule", async () => {
        const error = validatePlanStrategyRule(rule);
        if (error) {
          setStatus(el.planStrategyStatus, error, true);
          return;
        }
        const previousEditing = rule.isEditing;
        rule.isEditing = false;
        renderPlanStrategyTables();
        try {
          await persistPlanStrategySavedRules({
            requireSavedRules: true,
            successMessage: `Saved rule "${rule.name}".`
          });
        } catch (err) {
          rule.isEditing = previousEditing;
          renderPlanStrategyTables();
          setStatus(el.planStrategyStatus, err.message || "Failed to persist saved rule.", true);
        }
      });
      actions.appendChild(saveBtn);
    } else {
      const editBtn = createEditIconButton("Edit Rule", () => {
        rule.isEditing = true;
        renderPlanStrategyTables();
      });
      actions.appendChild(editBtn);
    }
    const removeBtn = createDeleteIconButton("Remove Strategy", async () => {
      state.planStrategyRules = state.planStrategyRules.filter((item) => item.id !== rule.id);
      renderPlanStrategyTables();
      try {
        await persistPlanStrategySavedRules({
          requireSavedRules: false,
          successMessage: "Strategy rules updated."
        });
      } catch (err) {
        setStatus(el.planStrategyStatus, err.message || "Failed to persist strategy after delete.", true);
      }
    });
    actions.appendChild(removeBtn);
    actionTd.appendChild(actions);
    ruleTr.appendChild(actionTd);
    el.planStrategyRulesBody.appendChild(ruleTr);
  }
  renderPlanStrategySettingsTable();
}

function validatePlanStrategyRule(rule) {
  if (!String(rule.name || "").trim()) {
    return "Strategy name is required.";
  }
  if (!Array.isArray(rule.states) || !rule.states.length) {
    return `Select at least one state for strategy "${rule.name || "-"}".`;
  }
  if (!Array.isArray(rule.segments) || !rule.segments.length) {
    return `Select at least one segment for strategy "${rule.name || "-"}".`;
  }
  return "";
}

function validatePlanStrategyRules() {
  const savedRules = state.planStrategyRules.filter((rule) => !rule.isEditing);
  if (!savedRules.length) {
    return "Save at least one rule before persisting strategy parameters.";
  }
  for (const rule of savedRules) {
    const baseValidation = validatePlanStrategyRule(rule);
    if (baseValidation) {
      return baseValidation;
    }
    if (!Number.isFinite(Number(rule.maxCpcUplift))) {
      return `Max CPC uplift must be numeric for "${rule.name || "-"}".`;
    }
    if (!Number.isFinite(Number(rule.maxCpbUplift))) {
      return `Max CPB uplift must be numeric for "${rule.name || "-"}".`;
    }
    if (!Number.isFinite(Number(rule.corTarget))) {
      return `COR target must be numeric for "${rule.name || "-"}".`;
    }
  }
  return "";
}

function buildSavedPlanStrategyPayload() {
  return {
    rules: state.planStrategyRules
      .filter((rule) => !rule.isEditing)
      .map((rule) => ({
        id: Number(rule.id) || 0,
        name: String(rule.name || "").trim(),
        states: Array.isArray(rule.states) ? rule.states : [],
        segments: Array.isArray(rule.segments) ? rule.segments : [],
        maxCpcUplift: Number(rule.maxCpcUplift) || 0,
        maxCpbUplift: Number(rule.maxCpbUplift) || 0,
        corTarget: Number(rule.corTarget) || 0,
        growthStrategy: String(rule.growthStrategy || "balanced")
      })),
    savedAt: new Date().toISOString()
  };
}

async function persistPlanStrategySavedRules({ requireSavedRules = true, successMessage } = {}) {
  let planId = getSelectedPlanId();
  if (!planId) {
    planId = await ensureSelectedPlanId();
  }
  if (!planId) {
    throw new Error("Enter plan ID first.");
  }

  const activityScope = getActivityScopeKey();
  const scopePayload = buildSavedPlanStrategyPayload();
  if (requireSavedRules && scopePayload.rules.length === 0) {
    throw new Error("Save at least one rule before persisting strategy parameters.");
  }
  const existingParameters = await api(`/api/plans/${planId}/parameters`);
  const existingParameter = (existingParameters.parameters || []).find(
    (item) => String(item.param_key || "") === PLAN_STRATEGY_PARAM_KEY
  );
  const existingParsed = parsePlanStrategyScopedConfig(String(existingParameter?.param_value || ""), activityScope);
  const mergedPayload = mergePlanStrategyScopedConfig(existingParsed.config, activityScope, scopePayload);

  await api(`/api/plans/${planId}/parameters`, {
    method: "PUT",
    body: JSON.stringify({
      parameters: [
        {
          key: PLAN_STRATEGY_PARAM_KEY,
          value: JSON.stringify(mergedPayload),
          valueType: "json"
        }
      ]
    })
  });
  writePlanStrategyLocalBackup(planId, scopePayload, activityScope);

  if (successMessage) {
    setStatus(el.planStrategyStatus, successMessage);
  } else {
    setStatus(el.planStrategyStatus, `Saved ${scopePayload.rules.length} strategy row(s) for ${activityScope}.`);
  }
}

async function refreshPlanStrategyOptions() {
  const segmentFallback = ["MCH", "MCR", "SCH", "SCR"];
  const params = new URLSearchParams();
  appendGlobalFilter(params);
  if (el.targetsStartDate.value) {
    params.set("startDate", el.targetsStartDate.value);
  }
  if (el.targetsEndDate.value) {
    params.set("endDate", el.targetsEndDate.value);
  }

  try {
    const data = await api(`/api/analytics/state-segment-performance/filters?${params.toString()}`);
    const states = mergeWithAllStateCodes(data.states || []);
    const segments = [...new Set((data.segments || []).map((value) => String(value || "").toUpperCase()).filter(Boolean))];
    state.planStrategyStateOptions = states;
    state.planStrategySegmentOptions = segments.length ? segments : segmentFallback;

    const stateSet = new Set(state.planStrategyStateOptions);
    const segmentSet = new Set(state.planStrategySegmentOptions);
    for (const rule of state.planStrategyRules) {
      rule.states = rule.states.filter((stateCode) => stateSet.has(stateCode));
      rule.segments = rule.segments.filter((segmentCode) => segmentSet.has(segmentCode));
    }
  } catch (_err) {
    state.planStrategyStateOptions = mergeWithAllStateCodes(
      state.targetsRows.map((row) => String(row.state || "").toUpperCase()).filter(Boolean)
    );
    state.planStrategySegmentOptions = segmentFallback;
  }

  renderPlanStrategyTables();
}

async function loadPlanStrategyForSelectedPlan() {
  const activityScope = getActivityScopeKey();
  let planId = getSelectedPlanId();
  if (!planId) {
    try {
      planId = await ensureSelectedPlanId();
    } catch (_err) {
      planId = "";
    }
  }
  if (!planId) {
    state.planStrategyRules = [];
    renderPlanStrategyTables();
    setStatus(el.planStrategyStatus, "Select a plan ID to load strategy.");
    return;
  }

  try {
    const data = await api(`/api/plans/${planId}/parameters`);
    const parameter = (data.parameters || []).find((item) => String(item.param_key || "") === PLAN_STRATEGY_PARAM_KEY);
    const scopedRemote = parsePlanStrategyScopedConfig(String(parameter?.param_value || ""), activityScope);
    let parsedRules = Array.isArray(scopedRemote.rules) ? scopedRemote.rules : [];
    let remoteSavedAt = String(scopedRemote.savedAt || "");

    const localBackup = readPlanStrategyLocalBackup(planId, activityScope);
    if (localBackup?.rules?.length) {
      const localTs = Date.parse(String(localBackup.localSavedAt || localBackup.savedAt || "")) || 0;
      const remoteTs = Date.parse(remoteSavedAt) || 0;
      if (localTs > remoteTs) {
        parsedRules = localBackup.rules;
      }
    }

    state.planStrategyRules = parsedRules.map((rule) =>
      createPlanStrategyRule({
        ...rule,
        isEditing: false
      })
    );
    const maxId = state.planStrategyRules.reduce((acc, rule) => Math.max(acc, Number(rule.id) || 0), 0);
    state.planStrategyRuleIdCounter = Math.max(maxId + 1, state.planStrategyRuleIdCounter, 1);
    await refreshPlanStrategyOptions();
    setStatus(el.planStrategyStatus, `Strategy loaded for ${activityScope}.`);
  } catch (err) {
    setStatus(el.planStrategyStatus, err.message || "Failed to load strategy.", true);
  }
}

async function savePlanStrategyForSelectedPlan() {
  const validationError = validatePlanStrategyRules();
  if (validationError) {
    setStatus(el.planStrategyStatus, validationError, true);
    return;
  }

  try {
    await persistPlanStrategySavedRules({ requireSavedRules: true });
  } catch (err) {
    setStatus(el.planStrategyStatus, err.message || "Failed to save strategy.", true);
  }
}

function renderTargetsLoadingRow(message = "Loading targets...") {
  el.targetsTableBody.innerHTML = "";
  const tr = document.createElement("tr");
  tr.className = "loading-row";
  const td = document.createElement("td");
  const dynamicCount = getTargetsColumns(state.targetsRows || []).columns.length;
  td.colSpan = Math.max(dynamicCount, DEFAULT_TARGETS_TABLE_COL_COUNT);
  td.className = "loading-cell";

  const wrap = document.createElement("div");
  wrap.className = "table-loading";
  const spinner = document.createElement("span");
  spinner.className = "spinner";
  spinner.setAttribute("aria-hidden", "true");
  const text = document.createElement("span");
  text.textContent = message;

  wrap.appendChild(spinner);
  wrap.appendChild(text);
  td.appendChild(wrap);
  tr.appendChild(td);
  el.targetsTableBody.appendChild(tr);
}

function addDerivedTargetRule(seed = {}) {
  state.derivedTargetRules.push({
    id: state.derivedRuleIdCounter++,
    name: String(seed.name || `Rule ${state.derivedRuleIdCounter - 1}`).trim(),
    states: Array.isArray(seed.states) ? [...seed.states] : [],
    segments: Array.isArray(seed.segments) ? [...seed.segments] : [],
    targetValue: Number(seed.targetValue) || 0,
    isEditing: seed.isEditing !== undefined ? Boolean(seed.isEditing) : true
  });
}

function ensureDerivedTargetRuleExists() {
  if (!state.derivedTargetRules.length) {
    addDerivedTargetRule();
  }
}

function setTargetsGoalMode(mode) {
  state.targetsGoalMode = mode;
  el.targetsModeCpb.classList.toggle("active", mode === "cpb");
  el.targetsModeRoe.classList.toggle("active", mode === "roe");
  el.targetsModeCor.classList.toggle("active", mode === "cor");
  if (el.adjustDerivedTargetBtn) {
    el.adjustDerivedTargetBtn.hidden = mode === "cpb";
  }
  el.derivedTargetPanel.hidden = mode === "cpb";

  if (mode === "roe") {
    el.derivedTargetTitle.textContent = "ROE Target Rules";
    el.derivedTargetMetricHeader.textContent = "Target ROE";
    el.derivedTargetPreviewMetricHeader.textContent = "Target ROE";
  } else if (mode === "cor") {
    el.derivedTargetTitle.textContent = "COR Target Rules";
    el.derivedTargetMetricHeader.textContent = "COR Target Source";
    el.derivedTargetPreviewMetricHeader.textContent = "Target COR";
  }

  if (mode === "roe" || mode === "cor") {
    state.derivedTargetRules = loadDerivedRulesFromStorage(mode);
    const maxId = state.derivedTargetRules.reduce((acc, rule) => Math.max(acc, Number(rule.id) || 0), 0);
    state.derivedRuleIdCounter = Math.max(maxId + 1, state.derivedRuleIdCounter);
    ensureDerivedTargetRuleExists();
    renderDerivedTargetRules();
  } else {
    el.derivedTargetPreviewWrap.hidden = true;
  }
  renderTargetsRows(state.targetsRows);
}

function renderDerivedTargetRules() {
  el.derivedTargetRulesBody.innerHTML = "";
  if (state.targetsGoalMode === "cpb") {
    return;
  }
  ensureDerivedTargetRuleExists();

  function validateRule(rule) {
    if (!String(rule.name || "").trim()) {
      return "Rule name is required.";
    }
    if (!Array.isArray(rule.states) || !rule.states.length) {
      return "Select at least one state.";
    }
    if (!Array.isArray(rule.segments) || !rule.segments.length) {
      return "Select at least one segment.";
    }
    if (state.targetsGoalMode === "roe" && !Number.isFinite(Number(rule.targetValue))) {
      return `Target ${state.targetsGoalMode === "roe" ? "ROE" : "COR"} must be a number.`;
    }
    return "";
  }

  for (const rule of state.derivedTargetRules) {
    const tr = document.createElement("tr");

    const tdName = document.createElement("td");
    if (rule.isEditing) {
      const nameInput = document.createElement("input");
      nameInput.type = "text";
      nameInput.placeholder = "Rule name";
      nameInput.value = rule.name || "";
      nameInput.addEventListener("input", () => {
        rule.name = nameInput.value.trim();
      });
      tdName.appendChild(nameInput);
    } else {
      tdName.textContent = rule.name || "-";
    }
    tr.appendChild(tdName);

    const tdStates = document.createElement("td");
    if (rule.isEditing) {
      const statesValue = Array.isArray(rule.states) ? [...rule.states] : [];
      tdStates.appendChild(
        createChecklistPicker(state.derivedTargetStateOptions, statesValue, (values) => {
          rule.states = values;
        }, true)
      );
    } else {
      tdStates.className = "rule-view-wrap";
      tdStates.textContent = rule.states?.length ? rule.states.join(", ") : "-";
    }
    tr.appendChild(tdStates);

    const tdSegments = document.createElement("td");
    if (rule.isEditing) {
      const segmentsValue = Array.isArray(rule.segments) ? [...rule.segments] : [];
      tdSegments.appendChild(
        createChecklistPicker(state.derivedTargetSegmentOptions, segmentsValue, (values) => {
          rule.segments = values;
        })
      );
    } else {
      tdSegments.className = "rule-view-wrap";
      tdSegments.textContent = rule.segments?.length ? rule.segments.join(", ") : "-";
    }
    tr.appendChild(tdSegments);

    const tdTarget = document.createElement("td");
    if (state.targetsGoalMode === "cor") {
      tdTarget.textContent = "From Strategy Rules";
    } else if (rule.isEditing) {
      const targetInput = document.createElement("input");
      targetInput.type = "number";
      targetInput.step = "0.01";
      targetInput.value = Number(rule.targetValue || 0).toString();
      targetInput.addEventListener("input", () => {
        rule.targetValue = Number(targetInput.value) || 0;
      });
      tdTarget.appendChild(targetInput);
    } else {
      tdTarget.textContent = `${formatDecimal(rule.targetValue, 2)}%`;
    }
    tr.appendChild(tdTarget);

    const tdAction = document.createElement("td");
    const actions = document.createElement("div");
    actions.className = "rule-actions";
    if (rule.isEditing) {
      const saveBtn = createSaveIconButton("Save Rule", async () => {
        const errorMessage = validateRule(rule);
        if (errorMessage) {
          setStatus(el.derivedTargetRulesStatus, errorMessage, true);
          return;
        }
        const before = safeLogPayload(rule);
        rule.isEditing = false;
        persistDerivedRulesForCurrentMode();
        await logChange({
          objectType: "targets_rule",
          objectId: String(rule.id),
          action: "save_rule",
          before,
          after: rule,
          metadata: { mode: state.targetsGoalMode }
        });
        setStatus(el.derivedTargetRulesStatus, `Saved rule "${rule.name}".`);
        renderDerivedTargetRules();
      });
      actions.appendChild(saveBtn);
    } else {
      const editBtn = createEditIconButton("Edit Rule", () => {
        const before = safeLogPayload(rule);
        rule.isEditing = true;
        persistDerivedRulesForCurrentMode();
        void logChange({
          objectType: "targets_rule",
          objectId: String(rule.id),
          action: "edit_rule",
          before,
          after: rule,
          metadata: { mode: state.targetsGoalMode }
        });
        renderDerivedTargetRules();
      });
      actions.appendChild(editBtn);
    }

    const removeBtn = createDeleteIconButton("Remove Rule", () => {
      const before = safeLogPayload(rule);
      state.derivedTargetRules = state.derivedTargetRules.filter((item) => item.id !== rule.id);
      ensureDerivedTargetRuleExists();
      persistDerivedRulesForCurrentMode();
      void logChange({
        objectType: "targets_rule",
        objectId: String(rule.id),
        action: "remove_rule",
        before,
        after: null,
        metadata: { mode: state.targetsGoalMode }
      });
      renderDerivedTargetRules();
    });
    actions.appendChild(removeBtn);
    tdAction.appendChild(actions);
    tr.appendChild(tdAction);

    el.derivedTargetRulesBody.appendChild(tr);
  }
}

function getValidDerivedTargetRules() {
  return state.derivedTargetRules
    .filter(
      (rule) =>
        !rule.isEditing &&
        String(rule.name || "").trim() &&
        rule.states.length &&
        Array.isArray(rule.segments) &&
        rule.segments.length &&
        Number.isFinite(Number(rule.targetValue))
    )
    .map((rule) => ({
      ...rule,
      name: String(rule.name || "").trim(),
      states: rule.states.map((stateCode) => String(stateCode || "").toUpperCase()).filter(Boolean),
      segments: rule.segments.map((segmentCode) => String(segmentCode || "").toUpperCase()).filter(Boolean),
      targetValue: Number(rule.targetValue) / 100
    }));
}

function calculateAdjustedCpbFromRoe(row, targetRoe, qbc) {
  const avgProfit = Number(row.avg_profit);
  const avgEquity = Number(row.avg_equity);
  if (!Number.isFinite(avgProfit) || !Number.isFinite(avgEquity) || avgEquity === 0) {
    return null;
  }
  const cpb = 0.81 * (((avgProfit - targetRoe * avgEquity) / 0.8) - qbc);
  if (!Number.isFinite(cpb)) {
    return null;
  }
  return Math.max(0, cpb);
}

function calculateAdjustedCpbFromCor(row, targetCor, qbc) {
  const avgPremium = Number(row.avg_lifetime_premium);
  const avgCost = Number(row.avg_lifetime_cost);
  if (!Number.isFinite(avgPremium) || !Number.isFinite(avgCost)) {
    return null;
  }
  const cpb = 0.81 * ((targetCor * avgPremium) - qbc - avgCost);
  if (!Number.isFinite(cpb)) {
    return null;
  }
  return Math.max(0, cpb);
}

function normalizeTargetRatio(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return null;
  }
  return numeric > 1 ? numeric / 100 : numeric;
}

function toFinite(value, fallback = 0) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

function buildTargetsAggregationProfiles(rows) {
  const byStateSegment = new Map();
  const byState = new Map();
  const bySegment = new Map();

  function ensureProfile(map, key) {
    if (!map.has(key)) {
      map.set(key, {
        binds: 0,
        scoredPolicies: 0,
        weightedCpbCost: 0,
        weightedProfit: 0,
        weightedEquity: 0,
        weightedPremium: 0,
        weightedCost: 0
      });
    }
    return map.get(key);
  }

  for (const row of rows) {
    const stateCode = String(row.state || "").toUpperCase();
    const segmentCode = String(row.segment || "").toUpperCase();
    if (!stateCode || !segmentCode) {
      continue;
    }

    const binds = toFinite(row.binds, 0);
    const scoredPolicies = toFinite(row.scored_policies, 0);
    const avgProfit = toFinite(row.avg_profit, 0);
    const avgEquity = toFinite(row.avg_equity, 0);
    const avgPremium = toFinite(row.avg_lifetime_premium, 0);
    const avgCost = toFinite(row.avg_lifetime_cost, 0);
    const cpb = toFinite(row.cpb, 0);

    const keys = [
      [byStateSegment, `${stateCode}|${segmentCode}`],
      [byState, stateCode],
      [bySegment, segmentCode]
    ];

    for (const [targetMap, key] of keys) {
      const profile = ensureProfile(targetMap, key);
      profile.binds += binds;
      profile.scoredPolicies += scoredPolicies;
      profile.weightedCpbCost += cpb * binds;
      profile.weightedProfit += avgProfit * scoredPolicies;
      profile.weightedEquity += avgEquity * scoredPolicies;
      profile.weightedPremium += avgPremium * scoredPolicies;
      profile.weightedCost += avgCost * scoredPolicies;
    }
  }

  return { byStateSegment, byState, bySegment };
}

function getProfileDerivedRow(profile) {
  if (!profile || profile.scoredPolicies <= 0) {
    return null;
  }
  return {
    binds: profile.binds,
    cpb: profile.binds > 0 ? profile.weightedCpbCost / profile.binds : null,
    avg_profit: profile.weightedProfit / profile.scoredPolicies,
    avg_equity: profile.weightedEquity / profile.scoredPolicies,
    avg_lifetime_premium: profile.weightedPremium / profile.scoredPolicies,
    avg_lifetime_cost: profile.weightedCost / profile.scoredPolicies
  };
}

function getCorTargetFromLanStrategy(stateCode, segmentCode) {
  const savedRules = state.planStrategyRules.filter((rule) => !rule.isEditing);
  const matchedRule = savedRules.find(
    (rule) =>
      Array.isArray(rule.states) &&
      Array.isArray(rule.segments) &&
      rule.states.map((value) => String(value || "").toUpperCase()).includes(stateCode) &&
      rule.segments.map((value) => String(value || "").toUpperCase()).includes(segmentCode)
  );
  if (!matchedRule) {
    return { targetMetric: null, ruleName: null };
  }
  return {
    targetMetric: normalizeTargetRatio(matchedRule.corTarget),
    ruleName: String(matchedRule.name || "LAN Strategy")
  };
}

function calculateSuggestedMaxCpbForProfile(mode, profile, targetMetric, qbc) {
  if (!profile || !Number.isFinite(Number(targetMetric))) {
    return null;
  }
  const profileRow = getProfileDerivedRow(profile);
  if (!profileRow) {
    return null;
  }
  return mode === "roe"
    ? calculateAdjustedCpbFromRoe(profileRow, targetMetric, qbc)
    : calculateAdjustedCpbFromCor(profileRow, targetMetric, qbc);
}

function summarizeProfileForDisplay(profile, qbc) {
  const derived = getProfileDerivedRow(profile);
  if (!derived) {
    return null;
  }
  const cpb = Number(derived.cpb);
  const avgPremium = Number(derived.avg_lifetime_premium);
  const avgCost = Number(derived.avg_lifetime_cost);
  const corValue =
    Number.isFinite(cpb) && Number.isFinite(avgPremium) && avgPremium !== 0 && Number.isFinite(avgCost)
      ? (cpb / 0.81 + qbc + avgCost) / avgPremium
      : null;

  return {
    binds: derived.binds,
    scoredPolicies: Number(profile?.scoredPolicies) || 0,
    profit: derived.avg_profit,
    equity: derived.avg_equity,
    currentCor: corValue,
    cpb: derived.cpb
  };
}

function calculateSuggestedWithFallback({
  mode,
  stateCode,
  segmentCode,
  targetMetric,
  profiles,
  qbc
}) {
  if (!Number.isFinite(Number(targetMetric))) {
    return null;
  }
  const pairProfile = profiles.byStateSegment.get(`${stateCode}|${segmentCode}`);
  const pairBinds = toFinite(pairProfile?.binds, 0);

  if (pairBinds >= 5) {
    return {
      suggestedMaxCpb: calculateSuggestedMaxCpbForProfile(mode, pairProfile, targetMetric, qbc),
      method: "State+Segment",
      pairBinds
    };
  }

  const stateOnly = calculateSuggestedMaxCpbForProfile(
    mode,
    profiles.byState.get(stateCode),
    targetMetric,
    qbc
  );
  const segmentOnly = calculateSuggestedMaxCpbForProfile(
    mode,
    profiles.bySegment.get(segmentCode),
    targetMetric,
    qbc
  );

  const hasStateOnly = Number.isFinite(Number(stateOnly));
  const hasSegmentOnly = Number.isFinite(Number(segmentOnly));
  if (hasStateOnly && hasSegmentOnly) {
    return {
      suggestedMaxCpb: (Number(stateOnly) + Number(segmentOnly)) / 2,
      method: "Fallback Avg (State + Segment)",
      pairBinds
    };
  }
  if (hasStateOnly) {
    return {
      suggestedMaxCpb: Number(stateOnly),
      method: "Fallback State",
      pairBinds
    };
  }
  if (hasSegmentOnly) {
    return {
      suggestedMaxCpb: Number(segmentOnly),
      method: "Fallback Segment",
      pairBinds
    };
  }
  return {
    suggestedMaxCpb: null,
    method: "No Data",
    pairBinds
  };
}

function buildDerivedTargetAdjustments() {
  const rules = getValidDerivedTargetRules();
  const qbc = Number(getActiveQbcValue()) || 0;
  const profiles = buildTargetsAggregationProfiles(state.targetsRows);
  const adjustments = [];

  for (const row of state.targetsRows) {
    const stateCode = String(row.state || "").toUpperCase();
    const segmentCode = String(row.segment || "").toUpperCase();
    if (!stateCode || !segmentCode) {
      continue;
    }

    let targetMetric = null;
    let ruleName = "-";

    if (state.targetsGoalMode === "roe") {
      const matchedRule = rules.find(
        (rule) => rule.segments.includes(segmentCode) && rule.states.includes(stateCode)
      );
      if (!matchedRule) {
        continue;
      }
      targetMetric = matchedRule.targetValue;
      ruleName = matchedRule.name || "-";
    } else {
      const corFromStrategy = getCorTargetFromLanStrategy(stateCode, segmentCode);
      if (!Number.isFinite(Number(corFromStrategy.targetMetric))) {
        continue;
      }
      targetMetric = corFromStrategy.targetMetric;
      ruleName = corFromStrategy.ruleName || "LAN Strategy";
    }

    const result = calculateSuggestedWithFallback({
      mode: state.targetsGoalMode,
      stateCode,
      segmentCode,
      targetMetric,
      profiles,
      qbc
    });

    if (!Number.isFinite(Number(result?.suggestedMaxCpb))) {
      continue;
    }

    adjustments.push({
      row,
      ruleName,
      targetMetricValue: targetMetric,
      adjustedTargetCpb: Number(result.suggestedMaxCpb)
    });
  }

  return adjustments;
}

function renderDerivedTargetPreview(adjustments) {
  el.derivedTargetPreviewBody.innerHTML = "";
  if (!adjustments.length) {
    el.derivedTargetPreviewWrap.hidden = true;
    return;
  }

  for (const item of adjustments) {
    const tr = document.createElement("tr");
    const cells = [
      item.ruleName,
      item.row.source || "-",
      item.row.segment || "-",
      item.row.state || "-",
      formatDecimal(item.row.cpb, 2),
      formatPercent(item.row.roe),
      formatPercent(item.row.combined_ratio),
      formatPercent(item.targetMetricValue),
      formatDecimal(item.adjustedTargetCpb, 2)
    ];
    for (const value of cells) {
      const td = document.createElement("td");
      td.textContent = value;
      tr.appendChild(td);
    }
    el.derivedTargetPreviewBody.appendChild(tr);
  }
  el.derivedTargetPreviewWrap.hidden = false;
}

function parseDefaultTargetsFilePayload(raw) {
  if (!raw) {
    return null;
  }
  const parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
  if (!parsed || typeof parsed !== "object") {
    return null;
  }
  if (!parsed.dataUrl || !parsed.fileName) {
    return null;
  }
  return {
    fileName: String(parsed.fileName),
    dataUrl: String(parsed.dataUrl),
    savedAt: parsed.savedAt ? String(parsed.savedAt) : null
  };
}

function readStoredDefaultTargetsFileFromLocalStorage() {
  const raw = localStorage.getItem(DEFAULT_TARGETS_FILE_STORAGE_KEY);
  if (!raw) {
    return null;
  }
  try {
    return parseDefaultTargetsFilePayload(raw);
  } catch {
    return null;
  }
}

function openDefaultTargetsDb() {
  if (!("indexedDB" in globalThis)) {
    return Promise.resolve(null);
  }
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DEFAULT_TARGETS_FILE_DB_NAME, 1);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(DEFAULT_TARGETS_FILE_DB_STORE)) {
        db.createObjectStore(DEFAULT_TARGETS_FILE_DB_STORE);
      }
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error || new Error("IndexedDB open failed."));
  });
}

async function readStoredDefaultTargetsFile() {
  const localFallback = readStoredDefaultTargetsFileFromLocalStorage();
  try {
    const db = await openDefaultTargetsDb();
    if (!db) {
      return localFallback;
    }
    const payload = await new Promise((resolve, reject) => {
      const tx = db.transaction(DEFAULT_TARGETS_FILE_DB_STORE, "readonly");
      const store = tx.objectStore(DEFAULT_TARGETS_FILE_DB_STORE);
      const request = store.get(DEFAULT_TARGETS_FILE_DB_KEY);
      request.onsuccess = () => resolve(request.result || null);
      request.onerror = () => reject(request.error || new Error("IndexedDB read failed."));
    });
    db.close();
    const parsed = parseDefaultTargetsFilePayload(payload);
    return parsed || localFallback;
  } catch {
    return localFallback;
  }
}

async function writeStoredDefaultTargetsFile(payload) {
  const serialized = JSON.stringify(payload);
  let localStorageSaved = false;
  try {
    localStorage.setItem(DEFAULT_TARGETS_FILE_STORAGE_KEY, serialized);
    localStorageSaved = true;
  } catch {
    // Ignore; IndexedDB path may still succeed for large files.
  }

  try {
    const db = await openDefaultTargetsDb();
    if (!db) {
      if (!localStorageSaved) {
        throw new Error("No storage available for default file.");
      }
      return;
    }
    await new Promise((resolve, reject) => {
      const tx = db.transaction(DEFAULT_TARGETS_FILE_DB_STORE, "readwrite");
      const store = tx.objectStore(DEFAULT_TARGETS_FILE_DB_STORE);
      const request = store.put(payload, DEFAULT_TARGETS_FILE_DB_KEY);
      request.onsuccess = () => resolve(null);
      request.onerror = () => reject(request.error || new Error("IndexedDB write failed."));
    });
    db.close();
  } catch (error) {
    if (!localStorageSaved) {
      throw error;
    }
  }
}

async function clearStoredDefaultTargetsFile() {
  localStorage.removeItem(DEFAULT_TARGETS_FILE_STORAGE_KEY);
  try {
    const db = await openDefaultTargetsDb();
    if (!db) {
      return;
    }
    await new Promise((resolve, reject) => {
      const tx = db.transaction(DEFAULT_TARGETS_FILE_DB_STORE, "readwrite");
      const store = tx.objectStore(DEFAULT_TARGETS_FILE_DB_STORE);
      const request = store.delete(DEFAULT_TARGETS_FILE_DB_KEY);
      request.onsuccess = () => resolve(null);
      request.onerror = () => reject(request.error || new Error("IndexedDB delete failed."));
    });
    db.close();
  } catch {
    // Local storage already cleared; ignore secondary failure.
  }
}

function updateDefaultTargetsFileStatus() {
  if (state.defaultTargetsFile?.fileName) {
    setStatus(el.defaultTargetsFileStatus, `Default file: ${state.defaultTargetsFile.fileName}`);
    return;
  }
  setStatus(el.defaultTargetsFileStatus, "Default file: bundled Targets-default.xlsx");
}

function dataUrlToArrayBuffer(dataUrl) {
  const base64Part = String(dataUrl || "").split(",")[1] || "";
  if (!base64Part) {
    throw new Error("Invalid default targets file data.");
  }
  const binary = atob(base64Part);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes.buffer;
}

function fileToDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error("Failed reading file."));
    reader.readAsDataURL(file);
  });
}

function applyPlanAndTargetDefaultsToInputs() {
  const perfRange = getPlanPerformanceRange();
  const priceRange = getPlanPriceExplorationRange();
  const targetsRange = getTargetsDefaultRange();
  applySharedPerformanceDateRange(perfRange.startIso, perfRange.endIso);
  applySharedPriceExplorationDateRange(priceRange.startIso, priceRange.endIso);
  applyDateRange("targets", targetsRange.startIso, targetsRange.endIso, { trigger: false });
}

function applySidebarUi() {
  if (!el.appLayout) {
    return;
  }
  el.appLayout.classList.toggle("sidebar-collapsed", Boolean(state.sidebarCollapsed));
  el.appLayout.classList.toggle("sidebar-unpinned", !Boolean(state.sidebarPinned));

  if (el.sidebarToggleBtn) {
    el.sidebarToggleBtn.classList.toggle("expanded", !Boolean(state.sidebarCollapsed));
    el.sidebarToggleBtn.title = state.sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar";
    el.sidebarToggleBtn.setAttribute("aria-label", state.sidebarCollapsed ? "Expand sidebar" : "Collapse sidebar");
  }
  if (el.sidebarPinBtn) {
    el.sidebarPinBtn.classList.toggle("active", Boolean(state.sidebarPinned));
    el.sidebarPinBtn.title = state.sidebarPinned ? "Sidebar pinned" : "Pin sidebar";
    el.sidebarPinBtn.setAttribute("aria-label", state.sidebarPinned ? "Sidebar pinned" : "Pin sidebar");
  }
  document.querySelectorAll(".menu-item, .menu-subitem").forEach((node) => {
    const label = String(node.querySelector("span:last-child")?.textContent || "").trim();
    if (!label) {
      return;
    }
    node.setAttribute("title", label);
    node.setAttribute("aria-label", label);
    node.setAttribute("data-tooltip", label);
  });
}

function setSidebarCollapsed(nextValue, persist = true) {
  state.sidebarCollapsed = Boolean(nextValue);
  if (persist) {
    localStorage.setItem(SIDEBAR_COLLAPSED_KEY, state.sidebarCollapsed ? "1" : "0");
  }
  applySidebarUi();
}

function setSidebarPinned(nextValue, persist = true) {
  state.sidebarPinned = Boolean(nextValue);
  if (persist) {
    localStorage.setItem(SIDEBAR_PINNED_KEY, state.sidebarPinned ? "1" : "0");
  }
  if (state.sidebarPinned && state.sidebarCollapsed) {
    state.sidebarCollapsed = false;
    if (persist) {
      localStorage.setItem(SIDEBAR_COLLAPSED_KEY, "0");
    }
  }
  applySidebarUi();
}

function setActiveSection(section) {
  state.activeSection = section;
  for (const item of el.menuItems) {
    item.classList.toggle("active", item.dataset.section === section);
  }
  if (el.analyticsMenuSubmenu) {
    el.analyticsMenuSubmenu.classList.toggle("open", section === "analytics");
  }
  if (el.planMenuSubmenu) {
    el.planMenuSubmenu.classList.toggle("open", section === "plan");
  }
  if (el.settingsMenuSubmenu) {
    el.settingsMenuSubmenu.classList.toggle("open", section === "settings");
  }
  for (const panel of el.sectionPanels) {
    panel.classList.toggle("active", panel.dataset.sectionPanel === section);
  }
  if (section === "ask-ai") {
    initAskAiView();
  }
}

function setActivePlanTab(tabName) {
  state.activePlanTab = tabName;
  const isBuilder = tabName === "builder";
  const isTargets = tabName === "targets";
  const isStrategy = tabName === "strategy";
  const isPriceDecision = tabName === "price-decision";
  const isOutcome = tabName === "outcome";
  el.planTabBuilder.classList.toggle("active", isBuilder);
  el.planTabTargets.classList.toggle("active", isTargets);
  if (el.planTabStrategy) {
    el.planTabStrategy.classList.toggle("active", isStrategy);
  }
  if (el.planTabPriceDecision) {
    el.planTabPriceDecision.classList.toggle("active", isPriceDecision);
  }
  if (el.planTabOutcome) {
    el.planTabOutcome.classList.toggle("active", isOutcome);
  }
  el.planBuilderPanel.classList.toggle("active", isBuilder);
  el.targetsPanel.classList.toggle("active", isTargets);
  if (el.planStrategyPanel) {
    el.planStrategyPanel.classList.toggle("active", isStrategy);
  }
  if (el.priceDecisionPanel) {
    el.priceDecisionPanel.classList.toggle("active", isPriceDecision);
  }
  if (el.planOutcomePanel) {
    el.planOutcomePanel.classList.toggle("active", isOutcome);
  }
  if (isPriceDecision) {
    initializeTableEnhancer("priceDecisionTable");
  }
  if (isOutcome) {
    initializeTableEnhancer("planOutcomeTable");
  }
}

function setActiveAnalyticsTab(tabName) {
  state.activeAnalyticsTab = tabName;
  const isStateSegment = tabName === "state-segment";
  const isPriceExploration = tabName === "price-exploration";
  const isStrategyAnalysis = tabName === "strategy-analysis";
  const isPlansComparison = tabName === "plans-comparison";
  const isStateAnalysis = tabName === "state-analysis";
  const isStatePlanAnalysis = tabName === "state-plan-analysis";
  el.analyticsTabStateSegment.classList.toggle("active", isStateSegment);
  el.analyticsTabPriceExploration.classList.toggle("active", isPriceExploration);
  if (el.analyticsTabStrategyAnalysis) {
    el.analyticsTabStrategyAnalysis.classList.toggle("active", isStrategyAnalysis);
  }
  if (el.analyticsTabPlansComparison) {
    el.analyticsTabPlansComparison.classList.toggle("active", isPlansComparison);
  }
  if (el.analyticsTabStateAnalysis) {
    el.analyticsTabStateAnalysis.classList.toggle("active", isStateAnalysis);
  }
  if (el.analyticsTabStatePlanAnalysis) {
    el.analyticsTabStatePlanAnalysis.classList.toggle("active", isStatePlanAnalysis);
  }
  el.stateSegmentPanel.classList.toggle("active", isStateSegment);
  el.priceExplorationPanel.classList.toggle("active", isPriceExploration);
  if (el.strategyAnalysisPanel) {
    el.strategyAnalysisPanel.classList.toggle("active", isStrategyAnalysis);
  }
  if (el.plansComparisonPanel) {
    el.plansComparisonPanel.classList.toggle("active", isPlansComparison);
  }
  if (el.stateAnalysisPanel) {
    el.stateAnalysisPanel.classList.toggle("active", isStateAnalysis);
  }
  if (el.statePlanAnalysisPanel) {
    el.statePlanAnalysisPanel.classList.toggle("active", isStatePlanAnalysis);
  }

  if (isStateSegment) {
    initializeTableEnhancer("stateSegmentTable");
  }
  if (isPriceExploration) {
    initializeTableEnhancer("priceExplorationTable");
  }
  if (isStrategyAnalysis) {
    initializeTableEnhancer("strategyAnalysisTable");
  }
  if (isPlansComparison) {
    initializeTableEnhancer("plansComparisonTable");
  }
  if (isStatePlanAnalysis) {
    initializeTableEnhancer("statePlanStateSegmentsTable");
  }
}

function setActiveSettingsTab(tabName) {
  const normalized = tabName === "users" ? "users" : "global-filters";
  state.activeSettingsTab = normalized;
  const isGlobalFilters = normalized === "global-filters";
  if (el.settingsSubGlobalFilters) {
    el.settingsSubGlobalFilters.classList.toggle("active", isGlobalFilters);
  }
  if (el.settingsSubUsers) {
    el.settingsSubUsers.classList.toggle("active", !isGlobalFilters);
  }
  if (el.settingsGlobalFiltersPanel) {
    el.settingsGlobalFiltersPanel.classList.toggle("active", isGlobalFilters);
  }
  if (el.settingsUsersPanel) {
    el.settingsUsersPanel.classList.toggle("active", !isGlobalFilters);
  }
}

async function api(path, options = {}) {
  if (!isAuthenticated()) {
    throw new Error("Please log in first.");
  }

  const useGlobalLoading = Boolean(options.useGlobalLoading);
  const loadingToken = useGlobalLoading ? startMainContentLoading() : 0;
  const controller = new AbortController();
  const timeoutMs = Number(options.timeoutMs) > 0 ? Number(options.timeoutMs) : 15000;
  const timeout = setTimeout(() => controller.abort(new Error("Request timeout")), timeoutMs);
  const headers = {
    "Content-Type": "application/json",
    "x-session-token": state.sessionToken,
    ...(options.headers || {})
  };

  try {
    const res = await fetch(path, { ...options, headers, signal: controller.signal });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      if (res.status === 401) {
        clearSessionLocally();
        showLoginScreen("Session expired. Please log in again.");
      }
      throw new Error(data.error || `Request failed: ${res.status}`);
    }
    return data;
  } catch (err) {
    if (err?.name === "AbortError") {
      throw new Error("Request timed out. Please try again.");
    }
    throw err;
  } finally {
    clearTimeout(timeout);
    if (useGlobalLoading) {
      stopMainContentLoading(loadingToken);
    }
  }
}

async function publicApi(path, options = {}) {
  const useGlobalLoading = Boolean(options.useGlobalLoading);
  const loadingToken = useGlobalLoading ? startMainContentLoading() : 0;
  const controller = new AbortController();
  const timeoutMs = Number(options.timeoutMs) > 0 ? Number(options.timeoutMs) : 15000;
  const timeout = setTimeout(() => controller.abort(new Error("Request timeout")), timeoutMs);
  const headers = {
    "Content-Type": "application/json",
    ...(options.headers || {})
  };
  try {
    const res = await fetch(path, { ...options, headers, signal: controller.signal });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(data.error || `Request failed: ${res.status}`);
    }
    return data;
  } catch (err) {
    if (err?.name === "AbortError") {
      throw new Error("Request timed out. Please try again.");
    }
    throw err;
  } finally {
    clearTimeout(timeout);
    if (useGlobalLoading) {
      stopMainContentLoading(loadingToken);
    }
  }
}

function appendGlobalFilter(params) {
  if (state.activityLeadType && state.activityLeadType !== "all") {
    params.set("activityLeadType", state.activityLeadType);
  }
  const activeQbc = getActiveQbcValue();
  if (Number.isFinite(Number(activeQbc))) {
    params.set("qbc", String(Number(activeQbc)));
  }
}

function getMultiValues(key) {
  return state.multiSelectValues[key] || [];
}

function mergeWithAllStateCodes(options) {
  const normalized = [...new Set((options || []).map((value) => String(value).trim().toUpperCase()).filter(Boolean))];
  return [...new Set([...ALL_US_STATE_CODES, ...normalized])].sort();
}

function setMultiOptions(key, options) {
  const normalized =
    key === "states" || key === "priceStates"
      ? mergeWithAllStateCodes(options)
      : [...new Set((options || []).map((value) => String(value).trim()).filter(Boolean))];
  const currentSelected = new Set(state.multiSelectValues[key] || []);
  state.multiSelectValues[key] = normalized.filter((value) => currentSelected.has(value));

  const { menu } = multiSelectMeta[key];
  menu.innerHTML = "";

  for (const value of normalized) {
    const label = document.createElement("label");
    label.className = "dropdown-option";
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.value = value;
    checkbox.checked = state.multiSelectValues[key].includes(value);
    checkbox.addEventListener("change", () => {
      const selected = new Set(state.multiSelectValues[key] || []);
      if (checkbox.checked) {
        selected.add(value);
      } else {
        selected.delete(value);
      }
      state.multiSelectValues[key] = Array.from(selected);
      updateMultiToggleLabel(key);
    });
    const text = document.createElement("span");
    text.textContent = value;
    label.appendChild(checkbox);
    label.appendChild(text);
    menu.appendChild(label);
  }

  updateMultiToggleLabel(key);
}

function updateMultiToggleLabel(key) {
  const { toggle, allLabel } = multiSelectMeta[key];
  const selected = state.multiSelectValues[key] || [];
  if (!selected.length) {
    toggle.textContent = allLabel;
    return;
  }
  if (selected.length <= 2) {
    toggle.textContent = selected.join(", ");
    return;
  }
  toggle.textContent = `${selected.length} selected`;
}

function initializeMultiDropdowns() {
  for (const [key, meta] of Object.entries(multiSelectMeta)) {
    meta.toggle.addEventListener("click", (event) => {
      event.stopPropagation();
      const parent = meta.toggle.closest(".multi-dropdown");
      const open = parent.classList.contains("open");

      document.querySelectorAll(".multi-dropdown.open").forEach((node) => node.classList.remove("open"));
      if (!open) {
        parent.classList.add("open");
      }
    });
    state.multiSelectValues[key] = [];
  }

  document.addEventListener("click", () => {
    document.querySelectorAll(".multi-dropdown.open").forEach((node) => node.classList.remove("open"));
  });
}

function buildStateSegmentAnalyticsQuery() {
  const params = new URLSearchParams();
  appendGlobalFilter(params);
  if (el.startDate.value) {
    params.set("startDate", el.startDate.value);
  }
  if (el.endDate.value) {
    params.set("endDate", el.endDate.value);
  }
  const states = getMultiValues("states");
  const segments = getMultiValues("segments");
  const channelGroups = getMultiValues("stateSegmentChannels");
  if (states.length) {
    params.set("states", states.join(","));
  }
  if (segments.length) {
    params.set("segments", segments.join(","));
  }
  if (channelGroups.length) {
    params.set("channelGroups", channelGroups.join(","));
  }
  return params.toString();
}

function buildPriceExplorationQuery(limit = PRICE_EXPLORATION_KPI_MAX_ROWS) {
  const params = new URLSearchParams();
  appendGlobalFilter(params);
  const planId = String(el.selectedPlanId?.value || "").trim();
  if (planId) {
    params.set("planId", planId);
  }
  if (el.priceStartDate.value) {
    params.set("startDate", el.priceStartDate.value);
  }
  if (el.priceEndDate.value) {
    params.set("endDate", el.priceEndDate.value);
  }
  const states = getMultiValues("priceStates");
  const channels = getMultiValues("priceChannels");
  if (states.length) {
    params.set("states", states.join(","));
  }
  if (channels.length) {
    params.set("channelGroups", channels.join(","));
  }
  const perfRange = getPlanPerformanceRange();
  params.set("q2bStartDate", perfRange.startIso);
  params.set("q2bEndDate", perfRange.endIso);
  params.set("limit", String(limit));
  return params.toString();
}

function buildStrategyAnalysisQuery() {
  const params = new URLSearchParams();
  appendGlobalFilter(params);

  const planId = String(el.selectedPlanId?.value || "").trim();
  if (planId) {
    params.set("planId", planId);
  }
  if (el.strategyAnalysisStartDate?.value) {
    params.set("startDate", el.strategyAnalysisStartDate.value);
  }
  if (el.strategyAnalysisEndDate?.value) {
    params.set("endDate", el.strategyAnalysisEndDate.value);
  }

  return params.toString();
}

function getQbcForScope(planRow, scopeKey) {
  if (String(scopeKey || "").startsWith("clicks_")) {
    const value = Number(planRow?.qbcClicks);
    return Number.isFinite(value) ? value : DEFAULT_PLAN_QBC_CLICKS;
  }
  const value = Number(planRow?.qbcLeadsCalls);
  return Number.isFinite(value) ? value : DEFAULT_PLAN_QBC_LEADS_CALLS;
}

function buildStrategyAnalysisQueryForComparison(planId, scopeKey, qbcValue, startDate, endDate) {
  const params = new URLSearchParams();
  params.set("planId", String(planId || "").trim());
  if (startDate) {
    params.set("startDate", String(startDate));
  }
  if (endDate) {
    params.set("endDate", String(endDate));
  }
  if (scopeKey && scopeKey !== "all") {
    params.set("activityLeadType", String(scopeKey));
  }
  if (Number.isFinite(Number(qbcValue))) {
    params.set("qbc", String(Number(qbcValue)));
  }
  return params.toString();
}

function summarizeStrategyRowsForComparison(rows, primaryLabel) {
  const summary = {
    rule_name: primaryLabel || "-",
    states: new Set(),
    segments: new Set(),
    bids: 0,
    sold: 0,
    total_spend: 0,
    quotes: 0,
    binds: 0,
    additional_clicks: 0,
    additional_binds: 0,
    additional_budget: 0,
    expected_total_cost: 0,
    performanceWeighted: 0,
    roeWeighted: 0,
    corWeighted: 0,
    targetCorWeighted: 0,
    metricWeight: 0
  };

  for (const row of rows || []) {
    const rowBids = Number(row.bids) || 0;
    const rowSold = Number(row.sold) || 0;
    const rowSpend = Number(row.total_spend) || 0;
    const rowQuotes = Number(row.quotes) || 0;
    const rowBinds = Number(row.binds) || 0;
    const rowAdditionalClicks = Number(row.additional_clicks) || 0;
    const rowAdditionalBinds = Number(row.additional_binds) || 0;
    const rowAdditionalBudget = Number(row.additional_budget) || 0;
    const rowExpectedTotalCost = Number(row.expected_total_cost);
    const rowPerformance = Number(row.performance);
    const rowRoe = Number(row.roe);
    const rowCor = Number(row.cor);
    const rowTargetCor = Number(row.target_cor);
    const weight = rowBinds > 0 ? rowBinds : 0;

    summary.bids += rowBids;
    summary.sold += rowSold;
    summary.total_spend += rowSpend;
    summary.quotes += rowQuotes;
    summary.binds += rowBinds;
    summary.additional_clicks += rowAdditionalClicks;
    summary.additional_binds += rowAdditionalBinds;
    summary.additional_budget += rowAdditionalBudget;
    summary.expected_total_cost += Number.isFinite(rowExpectedTotalCost) ? rowExpectedTotalCost : rowSpend + rowAdditionalBudget;

    if (Number.isFinite(rowPerformance) && weight > 0) {
      summary.performanceWeighted += rowPerformance * weight;
    }
    if (Number.isFinite(rowRoe) && weight > 0) {
      summary.roeWeighted += rowRoe * weight;
    }
    if (Number.isFinite(rowCor) && weight > 0) {
      summary.corWeighted += rowCor * weight;
    }
    if (Number.isFinite(rowTargetCor) && weight > 0) {
      summary.targetCorWeighted += rowTargetCor * weight;
    }
    if (weight > 0) {
      summary.metricWeight += weight;
    }

    const states = Array.isArray(row.states) ? row.states : [];
    const segments = Array.isArray(row.segments) ? row.segments : [];
    for (const stateCode of states) {
      summary.states.add(String(stateCode || "").toUpperCase());
    }
    for (const segmentCode of segments) {
      summary.segments.add(String(segmentCode || "").toUpperCase());
    }
  }

  const wr = summary.bids > 0 ? summary.sold / summary.bids : null;
  const cpc = summary.sold > 0 ? summary.total_spend / summary.sold : null;
  const q2b = summary.quotes > 0 ? summary.binds / summary.quotes : null;
  const currentCpb = summary.binds > 0 ? summary.total_spend / summary.binds : null;
  const expectedBinds = summary.binds + summary.additional_binds;
  const expectedCpb = expectedBinds > 0 ? summary.expected_total_cost / expectedBinds : null;
  const expectedClicks = summary.bids + summary.additional_clicks;
  const expectedWr = summary.bids > 0 ? expectedClicks / summary.bids : null;
  const expectedCpc = expectedClicks > 0 ? summary.expected_total_cost / expectedClicks : null;

  const wrUplift =
    Number.isFinite(wr) && Number.isFinite(expectedWr) && Number(wr) > 0
      ? (Number(expectedWr) - Number(wr)) / Number(wr)
      : null;
  const cpcUplift =
    Number.isFinite(cpc) && Number.isFinite(expectedCpc) && Number(cpc) > 0
      ? (Number(expectedCpc) - Number(cpc)) / Number(cpc)
      : null;
  const cpbUplift =
    Number.isFinite(currentCpb) && Number.isFinite(expectedCpb) && Number(currentCpb) > 0
      ? (Number(expectedCpb) - Number(currentCpb)) / Number(currentCpb)
      : null;

  return {
    rule_name: primaryLabel || "-",
    states: Array.from(summary.states).sort(),
    segments: Array.from(summary.segments).sort(),
    target_cor: summary.metricWeight > 0 ? summary.targetCorWeighted / summary.metricWeight : null,
    bids: summary.bids,
    sold: summary.sold,
    total_spend: summary.total_spend,
    cpc,
    wr,
    quotes: summary.quotes,
    binds: summary.binds,
    current_cpb: currentCpb,
    expected_cpb: expectedCpb,
    q2b,
    performance: summary.metricWeight > 0 ? summary.performanceWeighted / summary.metricWeight : null,
    roe: summary.metricWeight > 0 ? summary.roeWeighted / summary.metricWeight : null,
    cor: summary.metricWeight > 0 ? summary.corWeighted / summary.metricWeight : null,
    additional_clicks: summary.additional_clicks,
    additional_binds: summary.additional_binds,
    wr_uplift: wrUplift,
    cpc_uplift: cpcUplift,
    cpb_uplift: cpbUplift,
    expected_total_cost: summary.expected_total_cost,
    additional_budget: summary.additional_budget,
    __primary: primaryLabel || "-",
    __states: Array.from(summary.states).sort(),
    __segments: Array.from(summary.segments).sort()
  };
}

function renderPlansComparisonRows(rows) {
  if (!el.plansComparisonTableBody) {
    return;
  }
  el.plansComparisonTableBody.innerHTML = "";
  if (!rows.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 24;
    td.textContent = "No comparison rows for selected dates.";
    tr.appendChild(td);
    el.plansComparisonTableBody.appendChild(tr);
    return;
  }

  for (const row of rows) {
    const tr = document.createElement("tr");
    const cells = [
      row.__primary || row.rule_name || "-",
      Array.isArray(row.__states || row.states) ? (row.__states || row.states).join(", ") : "-",
      Array.isArray(row.__segments || row.segments) ? (row.__segments || row.segments).join(", ") : "-",
      formatPercent(row.target_cor),
      formatInt(row.bids),
      formatInt(row.sold),
      formatCurrency(row.total_spend, 2),
      formatCurrency(row.cpc, 2),
      formatPercent(row.wr),
      formatInt(row.quotes),
      formatInt(row.binds),
      formatCurrency(row.current_cpb, 2),
      formatCurrency(row.expected_cpb, 2),
      formatPercent(row.q2b),
      formatPercent(row.performance),
      formatPercent(row.roe),
      formatPercent(row.cor),
      formatInt(row.additional_clicks),
      formatDecimal(row.additional_binds, 2),
      formatPercent(row.wr_uplift),
      formatPercent(row.cpc_uplift),
      formatPercent(row.cpb_uplift),
      formatCurrency(row.expected_total_cost, 2),
      formatCurrency(row.additional_budget, 2)
    ];
    for (const [idx, value] of cells.entries()) {
      const td = document.createElement("td");
      td.textContent = value;
      if (idx === 16) {
        const corClass = classifyCorVsTarget(row.cor, row.target_cor);
        if (corClass) {
          td.classList.add(corClass);
        }
      }
      tr.appendChild(td);
    }
    el.plansComparisonTableBody.appendChild(tr);
  }
}

function buildStateAnalysisQuery() {
  const params = new URLSearchParams();
  appendGlobalFilter(params);
  const planId = String(el.selectedPlanId?.value || "").trim();
  if (planId) {
    params.set("planId", planId);
  }
  if (el.stateAnalysisStartDate?.value) {
    params.set("startDate", el.stateAnalysisStartDate.value);
  }
  if (el.stateAnalysisEndDate?.value) {
    params.set("endDate", el.stateAnalysisEndDate.value);
  }
  return params.toString();
}

function buildStatePlanAnalysisQuery() {
  const params = new URLSearchParams();
  appendGlobalFilter(params);
  const planId = String(el.selectedPlanId?.value || "").trim();
  if (planId) {
    params.set("planId", planId);
  }
  if (el.statePlanAnalysisStartDate?.value) {
    params.set("startDate", el.statePlanAnalysisStartDate.value);
  }
  if (el.statePlanAnalysisEndDate?.value) {
    params.set("endDate", el.statePlanAnalysisEndDate.value);
  }
  return params.toString();
}

function buildTargetsQuery() {
  const params = new URLSearchParams();
  appendGlobalFilter(params);
  appendSelectedPlanId(params);
  if (el.targetsStartDate.value) {
    params.set("startDate", el.targetsStartDate.value);
  }
  if (el.targetsEndDate.value) {
    params.set("endDate", el.targetsEndDate.value);
  }
  return params.toString();
}

function buildTargetsMetricsPath() {
  const queryString = buildTargetsQuery();
  return `/api/targets/metrics${queryString ? `?${queryString}` : ""}`;
}

function sanitizeKey(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");
}

function findColumnKey(columns, aliases) {
  const normalized = new Map(columns.map((key) => [sanitizeKey(key), key]));
  for (const alias of aliases) {
    const hit = normalized.get(sanitizeKey(alias));
    if (hit) {
      return hit;
    }
  }
  return null;
}

function parseSegmentState(segmentName) {
  const parts = String(segmentName || "").split("-");
  if (parts.length >= 2) {
    return {
      segment: String(parts[0] || "").trim().toUpperCase(),
      state: String(parts[1] || "").trim().toUpperCase()
    };
  }
  return { segment: "", state: "" };
}

function normalizeAccountId(value) {
  const raw = String(value ?? "").trim();
  if (!raw) {
    return "";
  }
  return raw.replace(/\.0+$/, "");
}

function buildSegmentName(segment, stateCode) {
  const seg = String(segment || "").trim().toUpperCase();
  const st = String(stateCode || "").trim().toUpperCase();
  if (!seg && !st) {
    return "";
  }
  return `${seg}-${st}`;
}

function getXlsxLib() {
  const lib = globalThis.XLSX;
  if (!lib) {
    throw new Error("XLSX parser not loaded");
  }
  return lib;
}

function toFileTargetRows(fileMeta) {
  return fileMeta.rowsRaw.map((rawRow, index) => {
    const segmentName = rawRow[fileMeta.columnKeys.segmentName] || "";
    const parsed = parseSegmentState(segmentName);
    return {
      target_id: `file-${index}`,
      state: parsed.state,
      segment: parsed.segment,
      source: String(rawRow[fileMeta.columnKeys.accountName] || "").trim(),
      account_id: normalizeAccountId(rawRow[fileMeta.columnKeys.accountId]),
      target_value: Number(rawRow[fileMeta.columnKeys.value]) || 0,
      current_target: Number(rawRow[fileMeta.columnKeys.value]) || 0,
      sold: null,
      binds: null,
      scored_policies: null,
      cpb: null,
      target_cpb: null,
      performance: null,
      roe: null,
      combined_ratio: null,
      avg_profit: null,
      avg_equity: null,
      avg_lifetime_premium: null,
      avg_lifetime_cost: null,
      __fromFile: true,
      __fileIndex: index
    };
  });
}

async function importTargetsFile(file) {
  state.targetsDefaultLoaded = false;
  renderTargetsLoadingRow("Uploading target file...");
  const buffer = await file.arrayBuffer();
  await importTargetsWorkbook(buffer, file.name);
}

async function importTargetsWorkbook(buffer, fileName) {
  const xlsx = getXlsxLib();
  const workbook = xlsx.read(buffer, { type: "array" });
  const sheetName = workbook.SheetNames[0];
  const worksheet = workbook.Sheets[sheetName];
  if (!worksheet) {
    throw new Error("No sheet found in file");
  }

  const headerRows = xlsx.utils.sheet_to_json(worksheet, { header: 1, range: 0 });
  const headers = Array.isArray(headerRows[0]) ? headerRows[0].map((value) => String(value || "")) : [];
  const rowsRaw = xlsx.utils.sheet_to_json(worksheet, { defval: "" });
  if (!rowsRaw.length) {
    throw new Error("No data rows in uploaded file");
  }

  const columns = Object.keys(rowsRaw[0] || {});
  const columnKeys = {
    accountId: findColumnKey(columns, ["Account ID", "AccountId"]),
    accountName: findColumnKey(columns, ["Account Name", "Source"]),
    segmentName: findColumnKey(columns, ["Segment Name", "Segment"]),
    value: findColumnKey(columns, ["Value", "Target"]),
    attributes: findColumnKey(columns, ["Attributes"])
  };

  if (!columnKeys.accountId || !columnKeys.accountName || !columnKeys.segmentName || !columnKeys.value) {
    throw new Error("Missing required columns (Account ID, Account Name, Segment Name, Value)");
  }

  state.uploadedTargetsFile = {
    fileName,
    sheetName,
    headers,
    rowsRaw,
    columnKeys
  };
  state.targetsMode = "file";
  state.targetsRows = toFileTargetRows(state.uploadedTargetsFile);
  renderTargetsRows(state.targetsRows);
  setStatus(el.targetsStatus, `Loaded ${state.targetsRows.length} row(s) from ${fileName}. Loading BQ data...`);
  void enrichTargetsRowsInBackground(state.targetsRows, {
    doneMessage: `Loaded ${state.targetsRows.length} row(s) from ${fileName}.`
  });
}

async function loadDefaultTargetsFile() {
  renderTargetsLoadingRow("Loading default target file...");
  if (state.defaultTargetsFile?.dataUrl) {
    try {
      const buffer = dataUrlToArrayBuffer(state.defaultTargetsFile.dataUrl);
      await importTargetsWorkbook(buffer, state.defaultTargetsFile.fileName);
      state.targetsDefaultLoaded = true;
      return;
    } catch (error) {
      setStatus(
        el.targetsStatus,
        `Saved default file failed to load (${state.defaultTargetsFile.fileName}). Falling back to bundled default.`,
        true
      );
    }
  }

  const res = await fetch(DEFAULT_TARGETS_FILE_URL);
  if (!res.ok) {
    throw new Error(`Default targets file not found (${res.status})`);
  }
  const buffer = await res.arrayBuffer();
  await importTargetsWorkbook(buffer, "Targets-default.xlsx");
  state.targetsDefaultLoaded = true;
}

async function ensureTargetsDefaultLoaded() {
  if (state.uploadedTargetsFile) {
    return;
  }
  await loadDefaultTargetsFile();
}

async function setDefaultTargetsFileForScope(file, scopeKey) {
  const normalizedScope = getActivityScopeKey(scopeKey);
  const dataUrl = await fileToDataUrl(file);
  const payload = {
    fileName: file.name,
    dataUrl,
    savedAt: new Date().toISOString()
  };
  if (isAuthenticated()) {
    await persistDefaultTargetsFileSharedForSelectedPlan(payload, normalizedScope);
    state.settingsDefaultTargetsByScope[normalizedScope] = payload;
  }
  if (normalizedScope === getActivityScopeKey()) {
    await writeStoredDefaultTargetsFile(payload);
    state.defaultTargetsFile = payload;
    updateDefaultTargetsFileStatus();
  }
}

async function clearDefaultTargetsFileForScope(scopeKey) {
  const normalizedScope = getActivityScopeKey(scopeKey);
  if (isAuthenticated()) {
    await persistDefaultTargetsFileSharedForSelectedPlan(null, normalizedScope);
    delete state.settingsDefaultTargetsByScope[normalizedScope];
  }
  if (normalizedScope === getActivityScopeKey()) {
    await clearStoredDefaultTargetsFile();
    state.defaultTargetsFile = null;
    updateDefaultTargetsFileStatus();
  }
}

async function refreshTargetsFileMode() {
  if (state.targetsMode !== "file") {
    return;
  }
  resetTargetsBqMetrics(state.targetsRows);
  renderTargetsRows(state.targetsRows);
  setStatus(el.targetsStatus, "Refreshing BQ data...");
  await enrichTargetsRowsInBackground(state.targetsRows, { doneMessage: "BQ data refreshed." });
  await refreshDerivedTargetOptions();
}

async function refreshTargetsCurrentMode() {
  if (state.targetsMode === "bq") {
    await refreshTargetsFromBq();
    return;
  }
  await refreshTargetsFileMode();
}

function syncFileRow(row) {
  const fileMeta = state.uploadedTargetsFile;
  if (!fileMeta || !row.__fromFile) {
    return;
  }
  const raw = fileMeta.rowsRaw[row.__fileIndex];
  if (!raw) {
    return;
  }
  const { accountName, segmentName, value, attributes } = fileMeta.columnKeys;
  raw[accountName] = row.source;
  raw[segmentName] = buildSegmentName(row.segment, row.state);
  raw[value] = Number(row.target_value) || 0;
  if (attributes) {
    raw[attributes] = `segment is ${row.segment}, state code is ${row.state}`;
  }
}

function downloadTargetsFile() {
  try {
    const fileMeta = state.uploadedTargetsFile;
    if (!fileMeta) {
      setStatus(el.targetsStatus, "Upload a target file first.", true);
      return;
    }
    const xlsx = getXlsxLib();
    const workbook = xlsx.utils.book_new();
    const worksheet = xlsx.utils.json_to_sheet(fileMeta.rowsRaw, {
      header: Object.keys(fileMeta.rowsRaw[0] || {})
    });
    xlsx.utils.book_append_sheet(workbook, worksheet, fileMeta.sheetName || "Targets");

    const nextFileName = fileMeta.fileName.replace(/\.(xlsx|xls|csv)$/i, "") + "-updated.xlsx";
    xlsx.writeFile(workbook, nextFileName);
    setStatus(el.targetsStatus, `Downloaded ${nextFileName}`);
  } catch (err) {
    setStatus(el.targetsStatus, err.message || "Failed to download file.", true);
  }
}

function targetMatchKey(row) {
  const accountId = normalizeAccountId(row.account_id);
  if (accountId) {
    return `${String(row.state || "").toUpperCase()}|${String(row.segment || "").toUpperCase()}|acct:${accountId}`;
  }
  return `${String(row.state || "").toUpperCase()}|${String(row.segment || "").toUpperCase()}|src:${sanitizeKey(row.source)}`;
}

function resetTargetsBqMetrics(rows) {
  for (const row of rows) {
    row.sold = null;
    row.binds = null;
    row.scored_policies = null;
    row.cpb = null;
    row.target_cpb = null;
    row.performance = null;
    row.roe = null;
    row.combined_ratio = null;
    row.avg_profit = null;
    row.avg_equity = null;
    row.avg_lifetime_premium = null;
    row.avg_lifetime_cost = null;
  }
}

async function enrichTargetsRowsInBackground(rows, options = {}) {
  const seq = ++targetsEnrichmentSeq;
  const doneMessage = options.doneMessage || "BQ data loaded.";
  try {
    await enrichTargetsRowsFromBq(rows);
    if (seq !== targetsEnrichmentSeq || rows !== state.targetsRows) {
      return;
    }
    renderTargetsRows(rows);
    setStatus(el.targetsStatus, doneMessage);
  } catch (error) {
    if (seq !== targetsEnrichmentSeq || rows !== state.targetsRows) {
      return;
    }
    renderTargetsRows(rows);
    setStatus(el.targetsStatus, error?.message || "Failed loading BQ data for targets.", true);
  }
}

async function enrichTargetsRowsFromBq(rows) {
  if (!rows.length) {
    return;
  }
  if (!isAuthenticated()) {
    return;
  }
  const requestRows = rows.map((row) => ({
    state: row.state || "",
    segment: row.segment || "",
    source: row.source || "",
    accountId: normalizeAccountId(row.account_id)
  }));

  const uniqueMap = new Map();
  for (const requestRow of requestRows) {
    uniqueMap.set(targetMatchKey({ ...requestRow, account_id: requestRow.accountId }), requestRow);
  }
  const uniqueRows = Array.from(uniqueMap.values());

  async function callChunkWithRetry(chunk, attempts = 3) {
    let lastError = null;
    for (let attempt = 1; attempt <= attempts; attempt += 1) {
      try {
        return await api(buildTargetsMetricsPath(), {
          method: "POST",
          body: JSON.stringify({ rows: chunk })
        });
      } catch (error) {
        lastError = error;
        if (attempt < attempts) {
          await new Promise((resolve) => setTimeout(resolve, attempt * 300));
        }
      }
    }
    throw lastError;
  }

  const chunkSize = 100;
  const merged = [];
  let failedChunks = 0;
  const chunks = [];
  for (let offset = 0; offset < uniqueRows.length; offset += chunkSize) {
    chunks.push(uniqueRows.slice(offset, offset + chunkSize));
  }
  const concurrency = Math.min(6, chunks.length);
  let chunkIndex = 0;

  async function worker() {
    while (chunkIndex < chunks.length) {
      const current = chunks[chunkIndex];
      chunkIndex += 1;
      try {
        const data = await callChunkWithRetry(current, 3);
        merged.push(...(data.rows || []));
      } catch (_err) {
        failedChunks += 1;
      }
    }
  }

  await Promise.all(Array.from({ length: Math.max(concurrency, 1) }, () => worker()));

  const map = new Map(merged.map((row) => [targetMatchKey(row), row]));
  for (const row of rows) {
    const matched = map.get(targetMatchKey(row));
    if (!matched) {
      continue;
    }
    row.sold = matched.sold;
    row.binds = matched.binds;
    row.scored_policies = matched.scored_policies;
    row.cpb = matched.cpb;
    row.target_cpb = matched.target_cpb;
    row.performance = matched.performance;
    row.roe = matched.roe;
    row.combined_ratio = matched.combined_ratio;
    row.avg_profit = matched.avg_profit;
    row.avg_equity = matched.avg_equity;
    row.avg_lifetime_premium = matched.avg_lifetime_premium;
    row.avg_lifetime_cost = matched.avg_lifetime_cost;
    if (matched.current_target !== null && matched.current_target !== undefined) {
      row.current_target = matched.current_target;
    }
  }

  if (failedChunks > 0) {
    setStatus(
      el.targetsStatus,
      `Loaded file rows with partial BQ enrichment (${failedChunks} chunk(s) failed).`,
      true
    );
  }
}

function createSaveIconButton(title, onClick) {
  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "icon-btn";
  btn.title = title;
  btn.setAttribute("aria-label", title);
  btn.innerHTML =
    '<svg viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M5 5h12l2 2v12H5zM8 5v6h8V5M8 19h8" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>';
  btn.addEventListener("click", onClick);
  return btn;
}

function createEditIconButton(title, onClick) {
  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "icon-btn icon-btn-secondary";
  btn.title = title;
  btn.setAttribute("aria-label", title);
  btn.innerHTML =
    '<svg viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M4 20h4l10-10-4-4L4 16v4zM12 6l4 4" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>';
  btn.addEventListener("click", onClick);
  return btn;
}

function createCloneIconButton(title, onClick) {
  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "icon-btn icon-btn-secondary";
  btn.title = title;
  btn.setAttribute("aria-label", title);
  btn.innerHTML =
    '<svg viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M9 9h10v10H9zM5 5h10v10" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>';
  btn.addEventListener("click", onClick);
  return btn;
}

function createCancelIconButton(title, onClick) {
  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "icon-btn icon-btn-secondary";
  btn.title = title;
  btn.setAttribute("aria-label", title);
  btn.innerHTML =
    '<svg viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M6 6l12 12M18 6 6 18" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>';
  btn.addEventListener("click", onClick);
  return btn;
}

function createDeleteIconButton(title, onClick) {
  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "icon-btn icon-btn-danger";
  btn.title = title;
  btn.setAttribute("aria-label", title);
  btn.innerHTML =
    '<svg viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M4 7h16M9 7V5h6v2M8 7l1 12h6l1-12M10 11v5M14 11v5" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/></svg>';
  btn.addEventListener("click", onClick);
  return btn;
}

function editableCell({ value, type = "text", onSave }) {
  const wrap = document.createElement("div");
  wrap.className = "inline-edit";
  const input = document.createElement("input");
  input.type = type;
  input.value = value ?? "";
  if (type === "number") {
    input.step = "0.01";
  }
  const saveButton = createSaveIconButton("Save", async () => {
    await onSave(input.value);
  });
  wrap.appendChild(input);
  wrap.appendChild(saveButton);
  return wrap;
}

async function saveTargetField(row, field, rawValue) {
  const value = String(rawValue ?? "").trim();
  const payload = {};
  const beforeSnapshot = safeLogPayload({
    target_id: row.target_id,
    state: row.state,
    segment: row.segment,
    source: row.source,
    target_value: row.target_value
  });

  if (field === "targetValue") {
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) {
      setStatus(el.targetsStatus, "Target must be a number.", true);
      return;
    }
    payload.targetValue = parsed;
  } else {
    return;
  }

  if (row.__fromFile) {
    if (field === "targetValue") {
      row.target_value = payload.targetValue;
    }
    syncFileRow(row);
    renderTargetsRows(state.targetsRows);
    setStatus(el.targetsStatus, "File row updated.");
    await logChange({
      objectType: "target_row",
      objectId: row.target_id,
      action: "update_target_value_file",
      before: beforeSnapshot,
      after: safeLogPayload({
        target_id: row.target_id,
        state: row.state,
        segment: row.segment,
        source: row.source,
        target_value: row.target_value
      })
    });
    return;
  }

  await api(withSelectedPlanId(`/api/targets/${row.target_id}`), {
    method: "PUT",
    body: JSON.stringify(payload)
  });
  await logChange({
    objectType: "target_row",
    objectId: row.target_id,
    action: "update_target_value_bq",
    before: beforeSnapshot,
    after: safeLogPayload({
      ...beforeSnapshot,
      target_value: payload.targetValue
    })
  });
  setStatus(el.targetsStatus, "BQ target updated.");
  await refreshTargetsFromBq();
}

function getTargetsDerivedValuesMap(rows) {
  const byKey = new Map();
  if (state.targetsGoalMode !== "roe" && state.targetsGoalMode !== "cor") {
    return byKey;
  }
  const qbc = Number(getActiveQbcValue()) || 0;
  const rules = getValidDerivedTargetRules();
  const profiles = buildTargetsAggregationProfiles(rows);

  for (const row of rows) {
    const stateCode = String(row.state || "").toUpperCase();
    const segmentCode = String(row.segment || "").toUpperCase();
    if (!stateCode || !segmentCode) {
      byKey.set(targetMatchKey(row), { targetMetric: null, suggestedMaxCpb: null });
      continue;
    }

    let targetMetric = null;
    if (state.targetsGoalMode === "roe") {
      const matchedRule = rules.find(
        (rule) => rule.segments.includes(segmentCode) && rule.states.includes(stateCode)
      );
      targetMetric = matchedRule?.targetValue ?? null;
    } else {
      targetMetric = getCorTargetFromLanStrategy(stateCode, segmentCode).targetMetric;
    }

    const result = calculateSuggestedWithFallback({
      mode: state.targetsGoalMode,
      stateCode,
      segmentCode,
      targetMetric,
      profiles,
      qbc
    });

    const stateSummary = summarizeProfileForDisplay(profiles.byState.get(stateCode), qbc);
    const segmentSummary = summarizeProfileForDisplay(profiles.bySegment.get(segmentCode), qbc);
    const pairSummary = summarizeProfileForDisplay(profiles.byStateSegment.get(`${stateCode}|${segmentCode}`), qbc);

    byKey.set(targetMatchKey(row), {
      targetMetric,
      suggestedMaxCpb:
        Number.isFinite(Number(result?.suggestedMaxCpb)) ? Number(result.suggestedMaxCpb) : null,
      method: result?.method || "No Data",
      pairBinds: pairSummary?.binds ?? null,
      pairScoredPolicies: pairSummary?.scoredPolicies ?? null,
      pairProfit: pairSummary?.profit ?? null,
      pairEquity: pairSummary?.equity ?? null,
      pairCurrentCor: pairSummary?.currentCor ?? null,
      stateBinds: stateSummary?.binds ?? null,
      stateProfit: stateSummary?.profit ?? null,
      stateEquity: stateSummary?.equity ?? null,
      stateCurrentCor: stateSummary?.currentCor ?? null,
      stateCpb: stateSummary?.cpb ?? null,
      segmentBinds: segmentSummary?.binds ?? null,
      segmentProfit: segmentSummary?.profit ?? null,
      segmentEquity: segmentSummary?.equity ?? null,
      segmentCurrentCor: segmentSummary?.currentCor ?? null,
      segmentCpb: segmentSummary?.cpb ?? null
    });
  }
  return byKey;
}

function getTargetsColumns(rows) {
  const derivedMap = getTargetsDerivedValuesMap(rows);
  const common = [
    { key: "state", label: "State", className: "targets-col-state", render: (row) => row.state || "-" },
    { key: "segment", label: "Seg.", className: "targets-col-segment", render: (row) => row.segment || "-" },
    { key: "source", label: "Source", className: "targets-col-source", render: (row) => row.source || "-" },
    {
      key: "current_target",
      label: "Current Target",
      className: "targets-col-current-target",
      render: (row) => formatCurrency(row.current_target, 2)
    },
    {
      key: "target_value",
      label: "Target",
      className: "targets-col-target",
      render: (row) =>
        editableCell({
          value: Number.isFinite(Number(row.target_value)) ? Number(row.target_value).toFixed(2) : "",
          type: "number",
          onSave: (nextValue) => saveTargetField(row, "targetValue", nextValue)
        })
    }
  ];

  if (state.targetsGoalMode === "roe") {
    return {
      columns: [
        ...common,
        {
          key: "binds",
          label: "Binds",
          className: "targets-col-binds",
          render: (row) => formatInt(derivedMap.get(targetMatchKey(row))?.pairBinds)
        },
        {
          key: "scored_policies",
          label: "SC",
          className: "targets-col-sc",
          render: (row) => formatInt(derivedMap.get(targetMatchKey(row))?.pairScoredPolicies)
        },
        {
          key: "avg_profit",
          className: "targets-col-profit",
          label: "Avg Profit",
          render: (row) => formatCurrency(derivedMap.get(targetMatchKey(row))?.pairProfit, 2)
        },
        {
          key: "avg_equity",
          className: "targets-col-equity",
          label: "Avg Equity",
          render: (row) => formatCurrency(derivedMap.get(targetMatchKey(row))?.pairEquity, 2)
        },
        { key: "roe", label: "Current ROE", render: (row) => formatPercent(row.roe) },
        {
          key: "target_metric",
          label: "Target ROE",
          render: (row) => formatPercent(derivedMap.get(targetMatchKey(row))?.targetMetric)
        },
        {
          key: "suggested_max_cpb",
          label: "Sug. Max CPB",
          render: (row) => formatCurrency(derivedMap.get(targetMatchKey(row))?.suggestedMaxCpb, 2)
        },
        {
          key: "method",
          label: "Method",
          render: (row) => derivedMap.get(targetMatchKey(row))?.method || "-"
        },
        {
          key: "state_binds",
          label: "State Binds",
          className: "targets-sep-left",
          render: (row) => formatInt(derivedMap.get(targetMatchKey(row))?.stateBinds)
        },
        {
          key: "state_profit",
          label: "State Profit",
          render: (row) => formatCurrency(derivedMap.get(targetMatchKey(row))?.stateProfit, 2)
        },
        {
          key: "state_equity",
          label: "State Equity",
          render: (row) => formatCurrency(derivedMap.get(targetMatchKey(row))?.stateEquity, 2)
        },
        {
          key: "state_cor",
          label: "State CoR",
          render: (row) => formatPercent(derivedMap.get(targetMatchKey(row))?.stateCurrentCor)
        },
        {
          key: "state_cpb",
          label: "State CPB",
          render: (row) => formatCurrency(derivedMap.get(targetMatchKey(row))?.stateCpb, 2)
        },
        {
          key: "segment_binds",
          label: "Seg. Binds",
          className: "targets-sep-left",
          render: (row) => formatInt(derivedMap.get(targetMatchKey(row))?.segmentBinds)
        },
        {
          key: "segment_profit",
          label: "Seg. Profit",
          render: (row) => formatCurrency(derivedMap.get(targetMatchKey(row))?.segmentProfit, 2)
        },
        {
          key: "segment_equity",
          label: "Seg. Equity",
          render: (row) => formatCurrency(derivedMap.get(targetMatchKey(row))?.segmentEquity, 2)
        },
        {
          key: "segment_cor",
          label: "Seg. COR",
          render: (row) => formatPercent(derivedMap.get(targetMatchKey(row))?.segmentCurrentCor)
        },
        {
          key: "segment_cpb",
          label: "Seg. CPB",
          render: (row) => formatCurrency(derivedMap.get(targetMatchKey(row))?.segmentCpb, 2)
        }
      ],
      derivedMap
    };
  }

  if (state.targetsGoalMode === "cor") {
    return {
      columns: [
        ...common,
        {
          key: "binds",
          label: "Binds",
          className: "targets-col-binds",
          render: (row) => formatInt(derivedMap.get(targetMatchKey(row))?.pairBinds)
        },
        {
          key: "scored_policies",
          label: "SC",
          className: "targets-col-sc",
          render: (row) => formatInt(derivedMap.get(targetMatchKey(row))?.pairScoredPolicies)
        },
        {
          key: "avg_profit",
          className: "targets-col-profit",
          label: "Avg Profit",
          render: (row) => formatCurrency(derivedMap.get(targetMatchKey(row))?.pairProfit, 2)
        },
        {
          key: "avg_equity",
          className: "targets-col-equity",
          label: "Avg Equity",
          render: (row) => formatCurrency(derivedMap.get(targetMatchKey(row))?.pairEquity, 2)
        },
        {
          key: "combined_ratio",
          label: "Current CoR",
          render: (row) => formatPercent(derivedMap.get(targetMatchKey(row))?.pairCurrentCor)
        },
        {
          key: "target_metric",
          label: "Target CoR",
          className: "targets-col-target-cor",
          render: (row) => formatPercent(derivedMap.get(targetMatchKey(row))?.targetMetric)
        },
        {
          key: "suggested_max_cpb",
          label: "Sug. Max CPB",
          render: (row) => formatCurrency(derivedMap.get(targetMatchKey(row))?.suggestedMaxCpb, 2)
        },
        {
          key: "method",
          label: "Method",
          render: (row) => derivedMap.get(targetMatchKey(row))?.method || "-"
        },
        {
          key: "state_binds",
          label: "State Binds",
          className: "targets-sep-left",
          render: (row) => formatInt(derivedMap.get(targetMatchKey(row))?.stateBinds)
        },
        {
          key: "state_profit",
          label: "State Profit",
          render: (row) => formatCurrency(derivedMap.get(targetMatchKey(row))?.stateProfit, 2)
        },
        {
          key: "state_equity",
          label: "State Equity",
          render: (row) => formatCurrency(derivedMap.get(targetMatchKey(row))?.stateEquity, 2)
        },
        {
          key: "state_cor",
          label: "State CoR",
          render: (row) => formatPercent(derivedMap.get(targetMatchKey(row))?.stateCurrentCor)
        },
        {
          key: "state_cpb",
          label: "State CPB",
          render: (row) => formatCurrency(derivedMap.get(targetMatchKey(row))?.stateCpb, 2)
        },
        {
          key: "segment_binds",
          label: "Seg. Binds",
          className: "targets-sep-left",
          render: (row) => formatInt(derivedMap.get(targetMatchKey(row))?.segmentBinds)
        },
        {
          key: "segment_profit",
          label: "Seg. Profit",
          render: (row) => formatCurrency(derivedMap.get(targetMatchKey(row))?.segmentProfit, 2)
        },
        {
          key: "segment_equity",
          label: "Seg. Equity",
          render: (row) => formatCurrency(derivedMap.get(targetMatchKey(row))?.segmentEquity, 2)
        },
        {
          key: "segment_cor",
          label: "Seg. COR",
          render: (row) => formatPercent(derivedMap.get(targetMatchKey(row))?.segmentCurrentCor)
        },
        {
          key: "segment_cpb",
          label: "Seg. CPB",
          render: (row) => formatCurrency(derivedMap.get(targetMatchKey(row))?.segmentCpb, 2)
        }
      ],
      derivedMap
    };
  }

  return {
    columns: [
      ...common,
      { key: "sold", label: "Sold", render: (row) => formatInt(row.sold) },
      { key: "binds", label: "Binds", render: (row) => formatInt(row.binds) },
      { key: "cpb", label: "CPB", render: (row) => formatDecimal(row.cpb, 2) },
      { key: "target_cpb", label: "Target CPB", render: (row) => formatDecimal(row.target_cpb, 2) },
      { key: "performance", label: "Performance", render: (row) => formatPercent(row.performance) },
      { key: "roe", label: "ROE", render: (row) => formatPercent(row.roe) },
      { key: "combined_ratio", label: "COR", render: (row) => formatPercent(row.combined_ratio) }
    ],
    derivedMap
  };
}

function renderTargetsHeader(columns) {
  const headerRow = document.querySelector("#targetsTable thead tr");
  if (!headerRow) {
    return;
  }
  headerRow.innerHTML = "";
  for (const column of columns) {
    const th = document.createElement("th");
    th.textContent = column.label;
    th.dataset.sortKey = column.key;
    th.classList.add("sortable");
    if (column.className) {
      th.classList.add(column.className);
    }
    headerRow.appendChild(th);
  }
}

function renderTargetsRows(rows) {
  el.targetsTableBody.innerHTML = "";
  const { columns } = getTargetsColumns(rows);
  renderTargetsHeader(columns);

  if (!rows.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = Math.max(columns.length, DEFAULT_TARGETS_TABLE_COL_COUNT);
    td.textContent = "No targets found.";
    tr.appendChild(td);
    el.targetsTableBody.appendChild(tr);
    return;
  }

  for (const row of rows) {
    const tr = document.createElement("tr");
    for (const column of columns) {
      const td = document.createElement("td");
      if (column.className) {
        td.classList.add(column.className);
      }
      const rendered = column.render(row);
      if (rendered instanceof Node) {
        td.appendChild(rendered);
      } else {
        td.textContent = rendered;
      }
      tr.appendChild(td);
    }
    el.targetsTableBody.appendChild(tr);
  }
}

function getStateSegmentDimensionsForView(viewMode) {
  switch (viewMode) {
    case "state_segment_channel":
      return ["state", "segment", "channel_group_name"];
    case "state_segment":
      return ["state", "segment"];
    case "state_channel":
      return ["state", "channel_group_name"];
    case "segment_channel":
      return ["segment", "channel_group_name"];
    case "state":
      return ["state"];
    case "segment":
      return ["segment"];
    case "channel":
      return ["channel_group_name"];
    default:
      return ["state", "segment"];
  }
}

function getRowChannelGroup(row) {
  const value =
    row?.channel_group_name ??
    row?.channelGroupName ??
    row?.channelgroupname ??
    row?.channel_group ??
    row?.channelGroup ??
    row?.channel ??
    "";
  const text = String(value || "").trim();
  return text || "Unknown";
}

function getSegmentFromChannelGroup(channelGroupName) {
  const match = String(channelGroupName || "").toUpperCase().match(STRATEGY_SEGMENT_REGEX);
  return match ? String(match[1] || "").toUpperCase() : "";
}

function syncStateSegmentDimensionHeaders(viewMode) {
  const dimensions = getStateSegmentDimensionsForView(viewMode);
  const showState = dimensions.includes("state");
  const showSegment = dimensions.includes("segment");
  const showChannel = dimensions.includes("channel_group_name");

  document.querySelectorAll("#stateSegmentTable .dim-state").forEach((node) => {
    node.style.display = showState ? "" : "none";
  });
  document.querySelectorAll("#stateSegmentTable .dim-segment").forEach((node) => {
    node.style.display = showSegment ? "" : "none";
  });
  document.querySelectorAll("#stateSegmentTable .dim-channel").forEach((node) => {
    node.style.display = showChannel ? "" : "none";
  });
}

function aggregateStateSegmentRows(rows, viewMode) {
  const dimensions = getStateSegmentDimensionsForView(viewMode);
  if (
    dimensions.length === 3 &&
    dimensions.includes("state") &&
    dimensions.includes("segment") &&
    dimensions.includes("channel_group_name")
  ) {
    return rows.map((row) => ({ ...row }));
  }

  const grouped = new Map();
  for (const row of rows) {
    const key = dimensions
      .map((field) => {
        if (field === "channel_group_name") {
          return getRowChannelGroup(row);
        }
        return String(row[field] || "");
      })
      .join("||");
    if (!grouped.has(key)) {
      grouped.set(key, {
        state: dimensions.includes("state") ? row.state : "All",
        segment: dimensions.includes("segment") ? row.segment : "All",
        channel_group_name: dimensions.includes("channel_group_name") ? getRowChannelGroup(row) : "All",
        bids: 0,
        sold: 0,
        total_cost: 0,
        quote_started: 0,
        quotes: 0,
        binds: 0,
        q2b_score: null,
        scored_policies: 0,
        cpb: null,
        target_cpb: null,
        performance: null,
        roe: null,
        combined_ratio: null,
        mrltv: null,
        profit: null,
        equity: null,
        _totalPrice: 0,
        _targetCpbWeighted: 0,
        _targetCpbWeight: 0,
        _performanceWeighted: 0,
        _performanceWeight: 0,
        _roeWeighted: 0,
        _roeWeight: 0,
        _combinedWeighted: 0,
        _combinedWeight: 0,
        _mrltvWeighted: 0,
        _mrltvWeight: 0,
        _profitWeighted: 0,
        _profitWeight: 0,
        _equityWeighted: 0,
        _equityWeight: 0
      });
    }

    const item = grouped.get(key);
    const bids = Number(row.bids) || 0;
    const sold = Number(row.sold) || 0;
    const quoteStarted = Number(row.quote_started) || 0;
    const quotes = Number(row.quotes) || 0;
    const binds = Number(row.binds) || 0;
    const totalCost = Number(row.total_cost) || 0;
    const scoredPolicies = Number(row.scored_policies) || 0;
    const cpb = Number(row.cpb);
    const targetCpb = Number(row.target_cpb);
    const performance = Number(row.performance);
    const roe = Number(row.roe);
    const combinedRatio = Number(row.combined_ratio);
    const mrltv = Number(row.mrltv);
    const profit = Number(row.profit);
    const equity = Number(row.equity);

    item.bids += bids;
    item.sold += sold;
    item.total_cost += totalCost;
    item.quote_started += quoteStarted;
    item.quotes += quotes;
    item.binds += binds;
    item.scored_policies += scoredPolicies;

    if (!Number.isNaN(cpb)) {
      item._totalPrice += cpb * binds;
    }
    if (!Number.isNaN(targetCpb)) {
      item._targetCpbWeighted += targetCpb * binds;
      item._targetCpbWeight += binds;
    }
    if (!Number.isNaN(performance)) {
      item._performanceWeighted += performance * binds;
      item._performanceWeight += binds;
    }
    const adjustedRoe = Number.isFinite(roe) ? roe : null;
    if (adjustedRoe !== null && !Number.isNaN(adjustedRoe)) {
      item._roeWeighted += adjustedRoe * scoredPolicies;
      item._roeWeight += scoredPolicies;
    }
    const adjustedCombine = Number.isFinite(combinedRatio) ? combinedRatio : null;
    if (adjustedCombine !== null && !Number.isNaN(adjustedCombine)) {
      item._combinedWeighted += adjustedCombine * scoredPolicies;
      item._combinedWeight += scoredPolicies;
    }
    if (!Number.isNaN(mrltv)) {
      item._mrltvWeighted += mrltv * scoredPolicies;
      item._mrltvWeight += scoredPolicies;
    }
    if (!Number.isNaN(profit)) {
      item._profitWeighted += profit * scoredPolicies;
      item._profitWeight += scoredPolicies;
    }
    if (!Number.isNaN(equity)) {
      item._equityWeighted += equity * scoredPolicies;
      item._equityWeight += scoredPolicies;
    }
  }

  return Array.from(grouped.values()).map((item) => {
    item.q2b_score = item.quotes ? item.binds / item.quotes : null;
    item.cpb = item.binds ? item._totalPrice / item.binds : null;
    item.target_cpb = item._targetCpbWeight ? item._targetCpbWeighted / item._targetCpbWeight : null;
    item.performance = item._performanceWeight ? item._performanceWeighted / item._performanceWeight : null;
    item.roe = item._roeWeight ? item._roeWeighted / item._roeWeight : null;
    item.combined_ratio = item._combinedWeight ? item._combinedWeighted / item._combinedWeight : null;
    item.mrltv = item._mrltvWeight ? item._mrltvWeighted / item._mrltvWeight : null;
    item.profit = item._profitWeight ? item._profitWeighted / item._profitWeight : null;
    item.equity = item._equityWeight ? item._equityWeighted / item._equityWeight : null;
    return item;
  });
}

function renderStateSegmentRows(rows) {
  el.stateSegmentTableBody.innerHTML = "";
  const viewMode = el.stateSegmentViewMode.value;
  const dimensions = getStateSegmentDimensionsForView(viewMode);

  if (!rows.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = dimensions.length + 16;
    td.textContent = "No data for selected filters.";
    tr.appendChild(td);
    el.stateSegmentTableBody.appendChild(tr);
    return;
  }

  for (const row of rows) {
    const roeAdjusted = row.roe;
    const combineAdjusted = row.combined_ratio;
    const tr = document.createElement("tr");
    const dimensionCells = [];
    if (dimensions.includes("state")) {
      dimensionCells.push(row.state);
    }
    if (dimensions.includes("segment")) {
      dimensionCells.push(row.segment);
    }
    if (dimensions.includes("channel_group_name")) {
      dimensionCells.push(getRowChannelGroup(row));
    }

    const cells = [
      ...dimensionCells,
      formatInt(row.bids),
      formatInt(row.sold),
      formatDecimal(row.total_cost, 2),
      formatInt(row.quote_started),
      formatInt(row.quotes),
      formatInt(row.binds),
      formatPercent(row.q2b_score),
      formatInt(row.scored_policies),
      formatDecimal(row.cpb, 2),
      formatDecimal(row.target_cpb, 2),
      formatPercent(row.performance),
      formatPercent(roeAdjusted),
      formatPercent(combineAdjusted),
      formatDecimal(row.mrltv, 2),
      formatDecimal(row.profit, 2),
      formatDecimal(row.equity, 2)
    ];

    for (const value of cells) {
      const td = document.createElement("td");
      td.textContent = value;
      tr.appendChild(td);
    }
    el.stateSegmentTableBody.appendChild(tr);
  }
}

function applyStateSegmentViewAndRender() {
  const viewMode = el.stateSegmentViewMode.value;
  syncStateSegmentDimensionHeaders(viewMode);
  const aggregated = aggregateStateSegmentRows(state.stateSegmentRawRows, viewMode);
  state.stateSegmentDisplayRows = aggregated;
  renderStateSegmentRows(aggregated);
}

function buildPriceExplorationKpiTotals(rows) {
  return rows.reduce(
    (acc, row) => {
      const opps = Number(row.opps) || 0;
      const bids = Number(row.bids) || 0;
      const sold = Number(row.sold) || 0;
      const cpc = Number(row.cpc) || 0;
      const avgBid = Number(row.avg_bid) || 0;
      const winRateUplift = row.win_rate_uplift;
      const cpcUplift = row.cpc_uplift;
      const additionalClicks = Number(row.additional_clicks) || 0;
      const additionalBinds = Number(row.expected_bind_change) || 0;
      const channelStateKey = `${row.channel_group_name}__${row.state}`;
      const additionalBudgetNeeded = Number(row.additional_budget_needed);

      acc.opps += opps;
      acc.bids += bids;
      acc.sold += sold;
      acc.totalSpend += cpc * sold;
      acc.avgBidWeightedSum += avgBid * bids;
      acc.winRateUpliftWeightedSum += (winRateUplift ?? 0) * bids;
      acc.winRateUpliftWeight += winRateUplift === null || winRateUplift === undefined ? 0 : bids;
      acc.cpcUpliftWeightedSum += (cpcUplift ?? 0) * sold;
      acc.cpcUpliftWeight += cpcUplift === null || cpcUplift === undefined ? 0 : sold;
      acc.additionalClicks += additionalClicks;
      acc.additionalBinds += additionalBinds;
      if (!acc.additionalBudgetByChannelState.has(channelStateKey) && !Number.isNaN(additionalBudgetNeeded)) {
        acc.additionalBudgetByChannelState.set(channelStateKey, additionalBudgetNeeded);
      }
      return acc;
    },
    {
      opps: 0,
      bids: 0,
      sold: 0,
      totalSpend: 0,
      avgBidWeightedSum: 0,
      winRateUpliftWeightedSum: 0,
      winRateUpliftWeight: 0,
      cpcUpliftWeightedSum: 0,
      cpcUpliftWeight: 0,
      additionalClicks: 0,
      additionalBinds: 0,
      additionalBudgetByChannelState: new Map()
    }
  );
}

function renderPriceExplorationKpis(testingRows, expectedRows) {
  const hasTestingRows = Array.isArray(testingRows) && testingRows.length > 0;
  const hasExpectedRows = Array.isArray(expectedRows) && expectedRows.length > 0;

  if (!hasTestingRows) {
    el.kpiBids.textContent = "0";
    el.kpiWinRate.textContent = "-";
    el.kpiSold.textContent = "0";
    el.kpiCpc.textContent = "-";
    el.kpiAvgBid.textContent = "-";
  } else {
    const testingTotals = buildPriceExplorationKpiTotals(testingRows);
    el.kpiBids.textContent = formatInt(testingTotals.bids);
    el.kpiWinRate.textContent = testingTotals.bids ? formatPercent(testingTotals.sold / testingTotals.bids) : "-";
    el.kpiSold.textContent = formatInt(testingTotals.sold);
    el.kpiCpc.textContent = testingTotals.sold ? formatDecimal(testingTotals.totalSpend / testingTotals.sold, 2) : "-";
    el.kpiAvgBid.textContent = testingTotals.bids ? formatDecimal(testingTotals.avgBidWeightedSum / testingTotals.bids, 2) : "-";
  }

  if (!hasExpectedRows) {
    el.kpiWinRateUplift.textContent = "-";
    el.kpiCpcUplift.textContent = "-";
    el.kpiAdditionalClicks.textContent = "0";
    el.kpiAdditionalBudget.textContent = "0.00";
    el.kpiAdditionalBinds.textContent = "0";
    return;
  }

  const expectedTotals = buildPriceExplorationKpiTotals(expectedRows);
  el.kpiWinRateUplift.textContent = expectedTotals.winRateUpliftWeight
    ? formatPercent(expectedTotals.winRateUpliftWeightedSum / expectedTotals.winRateUpliftWeight)
    : "-";
  el.kpiCpcUplift.textContent = expectedTotals.cpcUpliftWeight
    ? formatPercent(expectedTotals.cpcUpliftWeightedSum / expectedTotals.cpcUpliftWeight)
    : "-";
  el.kpiAdditionalClicks.textContent = formatInt(expectedTotals.additionalClicks);
  el.kpiAdditionalBudget.textContent = formatDecimal(
    Array.from(expectedTotals.additionalBudgetByChannelState.values()).reduce((sum, value) => sum + value, 0),
    2
  );
  el.kpiAdditionalBinds.textContent = formatDecimal(expectedTotals.additionalBinds, 1);
}

function getPriceExplorationRecommendedRows(rows) {
  return rows.filter((row) => {
    const isRecommended = Number(row.testing_point) === Number(row.recommended_testing_point);
    const hasStrategySegment = STRATEGY_SEGMENT_REGEX.test(String(row.channel_group_name || ""));
    return isRecommended && hasStrategySegment;
  });
}

function getPriceExplorationVisibleRows(rows) {
  if (!el.showOnlyRecommendedTp?.checked) {
    return rows;
  }
  return getPriceExplorationRecommendedRows(rows);
}

function renderPriceExplorationRows(rows, kpiRows = rows) {
  const visibleRows = getPriceExplorationVisibleRows(rows);
  const visibleKpiRows = getPriceExplorationVisibleRows(kpiRows);
  const recommendedKpiRows = getPriceExplorationRecommendedRows(kpiRows);
  el.priceExplorationTableBody.innerHTML = "";

  if (!visibleRows.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 29;
    td.textContent = "No data for selected filters.";
    tr.appendChild(td);
    el.priceExplorationTableBody.appendChild(tr);
    renderPriceExplorationKpis([], []);
    return;
  }

  for (const row of visibleRows) {
    const tr = document.createElement("tr");
    const cells = [
      row.channel_group_name,
      row.state,
      `${formatDecimal(row.testing_point, 0)}%`,
      `${formatDecimal(row.recommended_testing_point, 0)}%`,
      formatInt(row.bids),
      formatPercentFixed(row.win_rate, 1),
      formatCurrency(row.avg_bid, 2),
      formatCurrency(row.cpc, 2),
      formatInt(row.sold),
      formatPercentFixed(row.win_rate_uplift, 0),
      formatPercentFixed(row.cpc_uplift, 0),
      formatPercentFixed(row.win_rate_uplift_channel, 1),
      formatPercentFixed(row.cpc_uplift_channel, 1),
      formatInt(row.additional_clicks),
      formatCurrency(row.additional_budget_needed, 2),
      formatCurrency(row.current_cpb, 2),
      formatCurrency(row.expected_cpb, 2),
      formatPercentFixed(row.cpb_uplift, 1),
      formatPercentFixed(row.performance, 2),
      formatPercentFixed(row.roe, 2),
      formatPercentFixed(row.combined_ratio, 2),
      `${row.stat_sig} (${row.stat_sig_source || "channel & state"})`,
      formatInt(row.channel_quote),
      formatPercentFixed(row.click_to_channel_quote, 1),
      formatInt(row.binds),
      formatPercentFixed(row.q2b, 1),
      formatInt(row.channel_binds),
      formatPercentFixed(row.channel_q2b, 1),
      formatDecimal(row.expected_bind_change, 1)
    ];

    for (const value of cells) {
      const td = document.createElement("td");
      td.textContent = value;
      tr.appendChild(td);
    }
    el.priceExplorationTableBody.appendChild(tr);
  }

  renderPriceExplorationKpis(visibleKpiRows, recommendedKpiRows);
}

function resetPriceExplorationResults(message = "Choose filters and click Apply Filters.") {
  renderPriceExplorationRows([]);
  setStatus(el.priceExplorationStatus, message);
}

function classifyCorVsTarget(actualCor, targetCor) {
  const actual = Number(actualCor);
  const target = Number(targetCor);
  if (!Number.isFinite(actual) || !Number.isFinite(target) || target <= 0) {
    return "";
  }
  const delta = actual - target;
  if (delta >= 0.03) {
    return "metric-bad";
  }
  if (delta <= -0.03) {
    return "metric-good";
  }
  return "metric-warn";
}

function buildStrategyAnalysisViewRows(rows, viewMode) {
  if (viewMode !== "target_cor") {
    return rows.map((row) => ({
      ...row,
      __primary: row.rule_name || "-",
      __states: Array.isArray(row.states) ? row.states : [],
      __segments: Array.isArray(row.segments) ? row.segments : []
    }));
  }

  const groups = new Map();
  for (const row of rows) {
    const targetCor = Number(row.target_cor);
    const key = Number.isFinite(targetCor) && targetCor > 0 ? targetCor.toFixed(6) : "__NO_TARGET__";
    const current = groups.get(key) || {
      key,
      target_cor: Number.isFinite(targetCor) && targetCor > 0 ? targetCor : null,
      states: new Set(),
      segments: new Set(),
      bids: 0,
      sold: 0,
      total_spend: 0,
      quotes: 0,
      binds: 0,
      additional_clicks: 0,
      additional_binds: 0,
      additional_budget: 0,
      expected_total_cost: 0,
      performanceWeighted: 0,
      roeWeighted: 0,
      corWeighted: 0,
      metricWeight: 0,
      rowCount: 0
    };

    const rowBids = Number(row.bids) || 0;
    const rowSold = Number(row.sold) || 0;
    const rowSpend = Number(row.total_spend) || 0;
    const rowQuotes = Number(row.quotes) || 0;
    const rowBinds = Number(row.binds) || 0;
    const rowAdditionalClicks = Number(row.additional_clicks) || 0;
    const rowAdditionalBinds = Number(row.additional_binds) || 0;
    const rowAdditionalBudget = Number(row.additional_budget) || 0;
    const rowExpectedTotalCost = Number(row.expected_total_cost);
    const rowPerformance = Number(row.performance);
    const rowRoe = Number(row.roe);
    const rowCor = Number(row.cor);
    const weight = rowBinds > 0 ? rowBinds : 0;

    current.bids += rowBids;
    current.sold += rowSold;
    current.total_spend += rowSpend;
    current.quotes += rowQuotes;
    current.binds += rowBinds;
    current.additional_clicks += rowAdditionalClicks;
    current.additional_binds += rowAdditionalBinds;
    current.additional_budget += rowAdditionalBudget;
    current.expected_total_cost += Number.isFinite(rowExpectedTotalCost) ? rowExpectedTotalCost : rowSpend + rowAdditionalBudget;

    if (Number.isFinite(rowPerformance) && weight > 0) {
      current.performanceWeighted += rowPerformance * weight;
    }
    if (Number.isFinite(rowRoe) && weight > 0) {
      current.roeWeighted += rowRoe * weight;
    }
    if (Number.isFinite(rowCor) && weight > 0) {
      current.corWeighted += rowCor * weight;
    }
    if (weight > 0) {
      current.metricWeight += weight;
    }

    const states = Array.isArray(row.states) ? row.states : [];
    const segments = Array.isArray(row.segments) ? row.segments : [];
    for (const stateCode of states) {
      current.states.add(String(stateCode || "").toUpperCase());
    }
    for (const segmentCode of segments) {
      current.segments.add(String(segmentCode || "").toUpperCase());
    }
    current.rowCount += 1;
    groups.set(key, current);
  }

  return Array.from(groups.values()).map((group) => {
    const wr = group.bids > 0 ? group.sold / group.bids : null;
    const cpc = group.sold > 0 ? group.total_spend / group.sold : null;
    const q2b = group.quotes > 0 ? group.binds / group.quotes : null;
    const currentCpb = group.binds > 0 ? group.total_spend / group.binds : null;
    const expectedCpb =
      group.binds + group.additional_binds > 0
        ? group.expected_total_cost / (group.binds + group.additional_binds)
        : null;
    const expectedClicks = group.bids + group.additional_clicks;
    const expectedWr = group.bids > 0 ? expectedClicks / group.bids : null;
    const expectedCpc = expectedClicks > 0 ? group.expected_total_cost / expectedClicks : null;
    const wrUplift =
      Number.isFinite(wr) && Number.isFinite(expectedWr) && Number(wr) > 0
        ? (Number(expectedWr) - Number(wr)) / Number(wr)
        : null;
    const cpcUplift =
      Number.isFinite(cpc) && Number.isFinite(expectedCpc) && Number(cpc) > 0
        ? (Number(expectedCpc) - Number(cpc)) / Number(cpc)
        : null;
    const cpbUplift =
      Number.isFinite(currentCpb) && Number.isFinite(expectedCpb) && Number(currentCpb) > 0
        ? (Number(expectedCpb) - Number(currentCpb)) / Number(currentCpb)
        : null;

    return {
      rule_name: group.rowCount > 1 ? `${group.rowCount} tiers` : "1 tier",
      states: Array.from(group.states).sort(),
      segments: Array.from(group.segments).sort(),
      target_cor: group.target_cor,
      bids: group.bids,
      sold: group.sold,
      total_spend: group.total_spend,
      cpc,
      wr,
      quotes: group.quotes,
      binds: group.binds,
      current_cpb: currentCpb,
      expected_cpb: expectedCpb,
      q2b,
      performance: group.metricWeight > 0 ? group.performanceWeighted / group.metricWeight : null,
      roe: group.metricWeight > 0 ? group.roeWeighted / group.metricWeight : null,
      cor: group.metricWeight > 0 ? group.corWeighted / group.metricWeight : null,
      additional_clicks: group.additional_clicks,
      additional_binds: group.additional_binds,
      wr_uplift: wrUplift,
      cpc_uplift: cpcUplift,
      cpb_uplift: cpbUplift,
      expected_total_cost: group.expected_total_cost,
      additional_budget: group.additional_budget,
      __primary:
        Number.isFinite(group.target_cor) && Number(group.target_cor) > 0
          ? formatPercent(group.target_cor)
          : "No target COR",
      __states: Array.from(group.states).sort(),
      __segments: Array.from(group.segments).sort()
    };
  });
}

function renderStrategyAnalysisRows(rows) {
  if (!el.strategyAnalysisTableBody) {
    return;
  }
  el.strategyAnalysisTableBody.innerHTML = "";
  const viewMode = state.strategyAnalysisViewMode === "target_cor" ? "target_cor" : "rule";
  if (el.strategyAnalysisPrimaryHeader) {
    el.strategyAnalysisPrimaryHeader.textContent = viewMode === "target_cor" ? "Target COR" : "Rule Name";
  }

  const displayRows = buildStrategyAnalysisViewRows(rows, viewMode);

  if (!displayRows.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 24;
    td.textContent = "No strategy analysis rows for selected filters.";
    tr.appendChild(td);
    el.strategyAnalysisTableBody.appendChild(tr);
    return;
  }

  for (const row of displayRows) {
    const tr = document.createElement("tr");
    const cells = [
      row.__primary || row.rule_name || "-",
      Array.isArray(row.__states || row.states) ? (row.__states || row.states).join(", ") : "-",
      Array.isArray(row.__segments || row.segments) ? (row.__segments || row.segments).join(", ") : "-",
      formatPercent(row.target_cor),
      formatInt(row.bids),
      formatInt(row.sold),
      formatCurrency(row.total_spend, 2),
      formatCurrency(row.cpc, 2),
      formatPercent(row.wr),
      formatInt(row.quotes),
      formatInt(row.binds),
      formatCurrency(row.current_cpb, 2),
      formatCurrency(row.expected_cpb, 2),
      formatPercent(row.q2b),
      formatPercent(row.performance),
      formatPercent(row.roe),
      formatPercent(row.cor),
      formatInt(row.additional_clicks),
      formatDecimal(row.additional_binds, 2),
      formatPercent(row.wr_uplift),
      formatPercent(row.cpc_uplift),
      formatPercent(row.cpb_uplift),
      formatCurrency(row.expected_total_cost, 2),
      formatCurrency(row.additional_budget, 2)
    ];

    for (const [idx, value] of cells.entries()) {
      const td = document.createElement("td");
      td.textContent = value;
      if (idx === 16) {
        const corClass = classifyCorVsTarget(row.cor, row.target_cor);
        if (corClass) {
          td.classList.add(corClass);
        }
      }
      tr.appendChild(td);
    }
    el.strategyAnalysisTableBody.appendChild(tr);
  }
}

async function refreshStrategyAnalysisTable() {
  const loadingToken = startPanelLoading(el.strategyAnalysisLoading);
  try {
    const scopeKey = getActivityScopeKey();
    const planId = String(el.selectedPlanId?.value || "").trim();
    if (!planId) {
      renderStrategyAnalysisRows([]);
      setStatus(el.strategyAnalysisStatus, `Select a plan for ${scopeKey} in Plan Builder to load strategy analysis.`);
      return;
    }

    const queryString = buildStrategyAnalysisQuery();
    setStatus(el.strategyAnalysisStatus, "Loading strategy analysis...");
    const data = await api(`/api/analytics/strategy-analysis?${queryString}`, { timeoutMs: 120000 });
    state.strategyAnalysisRows = data.rows || [];
    renderStrategyAnalysisRows(state.strategyAnalysisRows);
    setStatus(el.strategyAnalysisStatus, `Loaded ${state.strategyAnalysisRows.length} row(s) for ${scopeKey} (plan ${planId}).`);
  } catch (err) {
    renderStrategyAnalysisRows([]);
    setStatus(el.strategyAnalysisStatus, err.message, true);
  } finally {
    stopPanelLoading(el.strategyAnalysisLoading, loadingToken);
  }
}

function renderPlansComparisonPlanOptions() {
  if (!el.plansComparisonPlanId) {
    return;
  }
  const selectedPlan = String(getSelectedPlanId() || "").trim();
  const current = String(state.plansComparisonPlanId || selectedPlan || "").trim();
  el.plansComparisonPlanId.innerHTML = "";
  const emptyOption = document.createElement("option");
  emptyOption.value = "";
  emptyOption.textContent = "Select plan";
  el.plansComparisonPlanId.appendChild(emptyOption);
  for (const planRow of state.planTableRows || []) {
    const option = document.createElement("option");
    option.value = planRow.planId;
    option.textContent = planRow.planName || planRow.planId;
    el.plansComparisonPlanId.appendChild(option);
  }
  if (current && (state.planTableRows || []).some((row) => row.planId === current)) {
    state.plansComparisonPlanId = current;
    el.plansComparisonPlanId.value = current;
  } else {
    state.plansComparisonPlanId = "";
    el.plansComparisonPlanId.value = "";
  }
}

function applyPlansComparisonModeUi() {
  const mode = state.plansComparisonMode === "global_filters" ? "global_filters" : "plans";
  state.plansComparisonMode = mode;
  if (el.plansComparisonMode) {
    el.plansComparisonMode.value = mode;
  }
  if (el.plansComparisonPlanWrap) {
    el.plansComparisonPlanWrap.hidden = mode !== "global_filters";
  }
  if (el.plansComparisonPrimaryHeader) {
    el.plansComparisonPrimaryHeader.textContent = mode === "global_filters" ? "Global Filter" : "Plan";
  }
}

async function refreshPlansComparisonTable() {
  const loadingToken = startPanelLoading(el.plansComparisonLoading);
  try {
    if (!state.planTableRows.length) {
      await refreshPlans();
      renderPlansComparisonPlanOptions();
    }
    const startDate = String(el.plansComparisonStartDate?.value || "");
    const endDate = String(el.plansComparisonEndDate?.value || "");
    const mode = state.plansComparisonMode === "global_filters" ? "global_filters" : "plans";
    const outputRows = [];

    if (mode === "plans") {
      const plans = state.planTableRows || [];
      let failedPlanCount = 0;
      for (const planRow of plans) {
        const mergedRows = [];
        let planHadFailure = false;
        for (const scopeKey of COMPARISON_SCOPE_KEYS) {
          try {
            const qbc = getQbcForScope(planRow, scopeKey);
            const queryString = buildStrategyAnalysisQueryForComparison(planRow.planId, scopeKey, qbc, startDate, endDate);
            const data = await api(`/api/analytics/strategy-analysis?${queryString}`, {
              timeoutMs: 120000,
              suppressGlobalLoading: true
            });
            mergedRows.push(...(Array.isArray(data.rows) ? data.rows : []));
          } catch (_err) {
            planHadFailure = true;
          }
        }
        if (planHadFailure) {
          failedPlanCount += 1;
        }
        outputRows.push(summarizeStrategyRowsForComparison(mergedRows, planRow.planName || planRow.planId));
      }
      outputRows.sort((a, b) => (Number(b.additional_binds) || 0) - (Number(a.additional_binds) || 0));
      if (failedPlanCount > 0) {
        setStatus(
          el.plansComparisonStatus,
          `Loaded ${outputRows.length} plan row(s). ${failedPlanCount} plan(s) had partial load failures.`,
          true
        );
      } else {
        setStatus(el.plansComparisonStatus, `Loaded ${outputRows.length} plan row(s).`);
      }
    } else {
      const planId = String(state.plansComparisonPlanId || "").trim();
      if (!planId) {
        renderPlansComparisonRows([]);
        setStatus(el.plansComparisonStatus, "Select a plan to compare global filters.");
        return;
      }
      const planRow = (state.planTableRows || []).find((row) => row.planId === planId);
      if (!planRow) {
        renderPlansComparisonRows([]);
        setStatus(el.plansComparisonStatus, "Selected plan is not available.", true);
        return;
      }

      let failedScopeCount = 0;
      for (const scopeKey of COMPARISON_SCOPE_KEYS) {
        let rows = [];
        try {
          const qbc = getQbcForScope(planRow, scopeKey);
          const queryString = buildStrategyAnalysisQueryForComparison(planId, scopeKey, qbc, startDate, endDate);
          const data = await api(`/api/analytics/strategy-analysis?${queryString}`, {
            timeoutMs: 120000,
            suppressGlobalLoading: true
          });
          rows = Array.isArray(data.rows) ? data.rows : [];
        } catch (_err) {
          failedScopeCount += 1;
        }
        outputRows.push(
          summarizeStrategyRowsForComparison(rows, ACTIVITY_SCOPE_LABELS[scopeKey] || scopeKey)
        );
      }
      outputRows.sort((a, b) => (Number(b.additional_binds) || 0) - (Number(a.additional_binds) || 0));
      if (failedScopeCount > 0) {
        setStatus(
          el.plansComparisonStatus,
          `Loaded ${outputRows.length} global filter row(s) for ${planRow.planName || planId}. ${failedScopeCount} scope(s) failed.`,
          true
        );
      } else {
        setStatus(el.plansComparisonStatus, `Loaded ${outputRows.length} global filter row(s) for ${planRow.planName || planId}.`);
      }
    }

    state.plansComparisonRows = outputRows;
    renderPlansComparisonRows(outputRows);
  } catch (err) {
    state.plansComparisonRows = [];
    renderPlansComparisonRows([]);
    setStatus(el.plansComparisonStatus, err.message || "Failed loading plans comparison.", true);
  } finally {
    stopPanelLoading(el.plansComparisonLoading, loadingToken);
  }
}

function renderStateAnalysisKpis(kpis, prefix = "stateAnalysis") {
  const map = {
    [`${prefix}KpiBids`]: formatInt(kpis?.sold),
    [`${prefix}KpiWr`]: formatPercent(kpis?.wr),
    [`${prefix}KpiTotalSpend`]: formatCurrency(kpis?.total_spend, 0),
    [`${prefix}KpiQ2b`]: formatPercent(kpis?.q2b),
    [`${prefix}KpiBinds`]: formatInt(kpis?.binds),
    [`${prefix}KpiCpb`]: formatCurrency(kpis?.current_cpb, 2),
    [`${prefix}KpiRoe`]: formatPercent(kpis?.roe),
    [`${prefix}KpiCor`]: formatPercent(kpis?.cor),
    [`${prefix}KpiLtv`]: formatCurrency(kpis?.ltv, 0),
    [`${prefix}KpiAdditionalClicks`]: formatInt(kpis?.additional_clicks),
    [`${prefix}KpiAdditionalBinds`]: formatDecimal(kpis?.additional_binds, 1),
    [`${prefix}KpiAdditionalBudget`]: formatCurrency(kpis?.additional_budget, 0)
  };
  for (const [key, value] of Object.entries(map)) {
    if (el[key]) {
      el[key].textContent = value;
    }
  }
}

function createRuleKpiCard(label, value) {
  const article = document.createElement("article");
  article.className = "kpi-card";
  const h3 = document.createElement("h3");
  h3.textContent = label;
  const p = document.createElement("p");
  p.textContent = value;
  article.appendChild(h3);
  article.appendChild(p);
  return article;
}

function renderSegmentRowsIntoTbody(tbody, rows, emptyMessage) {
  if (!tbody) {
    return;
  }
  tbody.innerHTML = "";
  const segmentRows = Array.isArray(rows) ? rows : [];
  if (!segmentRows.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 13;
    td.textContent = emptyMessage;
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  for (const row of segmentRows) {
    const tr = document.createElement("tr");
    const cells = [
      row.segment || "-",
      formatInt(row.bids),
      formatInt(row.sold),
      formatPercent(row.wr),
      formatCurrency(row.total_spend, 2),
      formatInt(row.quotes),
      formatPercent(row.sold_to_quotes),
      formatInt(row.binds),
      formatPercent(row.q2b),
      formatPercent(row.cor),
      formatPercent(row.roe),
      formatCurrency(row.cpb, 2),
      formatPercent(row.performance)
    ];
    for (const value of cells) {
      const td = document.createElement("td");
      td.textContent = value;
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
}

function renderStateAnalysisRules() {
  if (!el.stateAnalysisRulesContainer) {
    return;
  }
  el.stateAnalysisRulesContainer.innerHTML = "";

  const rules = state.stateAnalysisData?.rules || [];
  if (!rules.length) {
    const empty = document.createElement("section");
    empty.className = "state-rule-card";
    const h3 = document.createElement("h3");
    h3.textContent = "Rule Details";
    const p = document.createElement("p");
    p.className = "muted";
    p.textContent = "No strategy rules configured for this scope.";
    empty.appendChild(h3);
    empty.appendChild(p);
    el.stateAnalysisRulesContainer.appendChild(empty);
    return;
  }

  for (const rule of rules) {
    const section = document.createElement("section");
    section.className = "state-rule-card";

    const title = document.createElement("h3");
    title.textContent = `${rule.rule_name} (${rule.strategy_label}, Tier ${rule.tier})`;
    section.appendChild(title);

    const meta = document.createElement("p");
    meta.className = "muted";
    meta.textContent = `States: ${rule.states.join(", ")} | Segments: ${rule.segments.join(", ")}`;
    section.appendChild(meta);

    const kpiGrid = document.createElement("div");
    kpiGrid.className = "kpi-grid";
    const kpiItems = [
      ["Total Clicks", formatInt(rule.kpis?.sold)],
      ["Avg Win Rate", formatPercent(rule.kpis?.wr)],
      ["Cost", formatCurrency(rule.kpis?.total_spend, 0)],
      ["Avg Q2B", formatPercent(rule.kpis?.q2b)],
      ["Binds", formatInt(rule.kpis?.binds)],
      ["CPB", formatCurrency(rule.kpis?.current_cpb, 2)],
      ["ROE", formatPercent(rule.kpis?.roe)],
      ["Combined Ratio", formatPercent(rule.kpis?.cor)],
      ["LTV", formatCurrency(rule.kpis?.ltv, 0)],
      ["Additional Clicks", formatInt(rule.kpis?.additional_clicks)],
      ["Additional Binds", formatDecimal(rule.kpis?.additional_binds, 1)],
      ["Required Budget", formatCurrency(rule.kpis?.additional_budget, 0)]
    ];
    for (const [label, value] of kpiItems) {
      kpiGrid.appendChild(createRuleKpiCard(label, value));
    }
    section.appendChild(kpiGrid);

    const wrap = document.createElement("div");
    wrap.className = "table-wrap";
    const table = document.createElement("table");
    const thead = document.createElement("thead");
    thead.innerHTML = `
      <tr>
        <th>Segment</th>
        <th>Bids</th>
        <th>Sold</th>
        <th>WR</th>
        <th>Total Spend</th>
        <th>Quotes</th>
        <th>Sold to Quotes</th>
        <th>Binds</th>
        <th>Q2B</th>
        <th>COR</th>
        <th>ROE</th>
        <th>CPB</th>
        <th>Performance</th>
      </tr>
    `;
    const tbody = document.createElement("tbody");
    renderSegmentRowsIntoTbody(tbody, rule.segment_rows, "No segment rows for this rule.");
    table.appendChild(thead);
    table.appendChild(tbody);
    wrap.appendChild(table);
    section.appendChild(wrap);
    el.stateAnalysisRulesContainer.appendChild(section);
  }
}

function getStateFillColor(strategyKey, tier) {
  if (!strategyKey) {
    return "#d1d5db";
  }
  const d3Lib = window.d3;
  if (!d3Lib) {
    return "#9ca3af";
  }
  const base =
    strategyKey === "aggressive" ? "#2f9e44" : strategyKey === "cautious" ? "#6b7280" : "#c9a227";
  const numericTier = Number(tier);
  const normalizedTier = Number.isFinite(numericTier) && numericTier > 0 ? numericTier : 4;
  const intensity = Math.min(Math.max((normalizedTier - 1) / 5, 0), 0.8);
  return d3Lib.interpolateRgb(base, "#f8fafc")(intensity);
}

async function ensureUsStatesGeoJson() {
  if (state.usStatesGeoJson) {
    return state.usStatesGeoJson;
  }
  if (!window.d3 || !window.topojson) {
    throw new Error("Map libraries are unavailable.");
  }
  const topo = await window.d3.json("https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json");
  state.usStatesGeoJson = window.topojson.feature(topo, topo.objects.states);
  return state.usStatesGeoJson;
}

function renderStateAnalysisMap() {
  if (!el.stateAnalysisMapSvg) {
    return;
  }
  const d3Lib = window.d3;
  if (!d3Lib || !state.usStatesGeoJson) {
    return;
  }
  const svg = d3Lib.select(el.stateAnalysisMapSvg);
  svg.selectAll("*").remove();

  const stateMap = new Map((state.stateAnalysisData?.states || []).map((item) => [item.state, item]));
  const width = 960;
  const height = 600;
  const projection = d3Lib.geoAlbersUsa().fitSize([width, height], state.usStatesGeoJson);
  const path = d3Lib.geoPath(projection);
  const tooltip = el.stateAnalysisMapTooltip;

  svg
    .append("g")
    .selectAll("path")
    .data(state.usStatesGeoJson.features)
    .join("path")
    .attr("d", path)
    .attr("stroke", "#7aa0da")
    .attr("stroke-width", 0.8)
    .attr("fill", (feature) => {
      const code = FIPS_TO_STATE_CODE[Number(feature.id)] || "";
      const row = stateMap.get(code);
      return getStateFillColor(row?.strategy_key, row?.tier);
    })
    .style("cursor", "default")
    .on("mouseenter", function (event, feature) {
      const code = FIPS_TO_STATE_CODE[Number(feature.id)] || "";
      const row = stateMap.get(code);
      if (!tooltip) {
        return;
      }
      if (!row) {
        tooltip.hidden = false;
        tooltip.innerHTML = `<strong>${feature.properties?.name || code}</strong><div>No configured strategy.</div>`;
        return;
      }
      tooltip.hidden = false;
      tooltip.innerHTML = `
        <strong>${feature.properties?.name || code} (${code})</strong>
        <div>Strategy: ${row.strategy_label || "-"}</div>
        <div>Total spend: ${formatCurrency(row.total_spend, 0)}</div>
        <div>COR: ${formatPercent(row.cor)}</div>
        <div>ROE: ${formatPercent(row.roe)}</div>
        <div>Binds: ${formatInt(row.binds)}</div>
        <div>Performance: ${formatPercent(row.performance)}</div>
        <div>Additional clicks: ${formatInt(row.additional_clicks)}</div>
        <div>Additional binds: ${formatDecimal(row.additional_binds, 1)}</div>
        <div>Additional budget: ${formatCurrency(row.additional_budget, 0)}</div>
        <div>CPC uplift: ${formatPercent(row.cpc_uplift)}</div>
        <div>CPB uplift: ${formatPercent(row.cpb_uplift)}</div>
      `;
    })
    .on("mousemove", function (event) {
      if (!tooltip || tooltip.hidden) {
        return;
      }
      const bounds = el.stateAnalysisMapSvg.getBoundingClientRect();
      tooltip.style.left = `${event.clientX - bounds.left + 14}px`;
      tooltip.style.top = `${event.clientY - bounds.top + 14}px`;
    })
    .on("mouseleave", function () {
      if (tooltip) {
        tooltip.hidden = true;
      }
    });
}

function clearStateAnalysisMap() {
  if (!el.stateAnalysisMapSvg || !window.d3) {
    return;
  }
  window.d3.select(el.stateAnalysisMapSvg).selectAll("*").remove();
  if (el.stateAnalysisMapTooltip) {
    el.stateAnalysisMapTooltip.hidden = true;
  }
}

function renderStatePlanSelectedStateDetails() {
  const details = state.statePlanAnalysisData?.state_details || [];
  const selected =
    details.find((row) => row.state === state.statePlanSelectedState) || null;

  if (!selected) {
    if (el.statePlanAnalysisDetailTitle) {
      el.statePlanAnalysisDetailTitle.textContent = "State Details";
    }
    if (el.statePlanAnalysisDetailMeta) {
      el.statePlanAnalysisDetailMeta.textContent = "Click a state on the map to load details.";
    }
    renderStateAnalysisKpis(null, "statePlanState");
    renderSegmentRowsIntoTbody(
      el.statePlanStateSegmentsBody,
      [],
      "Click a state on the map to load segment breakdown."
    );
    return;
  }

  if (el.statePlanAnalysisDetailTitle) {
    el.statePlanAnalysisDetailTitle.textContent = `${selected.state} | ${selected.rule_name || "No rule"}`;
  }
  if (el.statePlanAnalysisDetailMeta) {
    const strategyText = selected.strategy_label ? `${selected.strategy_label} (Tier ${selected.tier || "-"})` : "No strategy";
    el.statePlanAnalysisDetailMeta.textContent = `Rule: ${selected.rule_name || "-"} | Strategy: ${strategyText}`;
  }
  renderStateAnalysisKpis(selected.kpis, "statePlanState");
  renderSegmentRowsIntoTbody(el.statePlanStateSegmentsBody, selected.segment_rows, "No segment rows for selected state.");
}

function renderStatePlanAnalysisMap() {
  if (!el.statePlanAnalysisMapSvg) {
    return;
  }
  const d3Lib = window.d3;
  if (!d3Lib || !state.usStatesGeoJson) {
    return;
  }
  const svg = d3Lib.select(el.statePlanAnalysisMapSvg);
  svg.selectAll("*").remove();

  const stateMap = new Map((state.statePlanAnalysisData?.states || []).map((item) => [item.state, item]));
  const width = 960;
  const height = 600;
  const projection = d3Lib.geoAlbersUsa().fitSize([width, height], state.usStatesGeoJson);
  const path = d3Lib.geoPath(projection);
  const tooltip = el.statePlanAnalysisMapTooltip;

  svg
    .append("g")
    .selectAll("path")
    .data(state.usStatesGeoJson.features)
    .join("path")
    .attr("d", path)
    .attr("stroke", "#7aa0da")
    .attr("stroke-width", 0.8)
    .attr("fill", (feature) => {
      const code = FIPS_TO_STATE_CODE[Number(feature.id)] || "";
      const row = stateMap.get(code);
      return getStateFillColor(row?.strategy_key, row?.tier);
    })
    .style("cursor", "pointer")
    .on("mouseenter", function (_event, feature) {
      const code = FIPS_TO_STATE_CODE[Number(feature.id)] || "";
      const row = stateMap.get(code);
      if (!tooltip) {
        return;
      }
      if (!row) {
        tooltip.hidden = false;
        tooltip.innerHTML = `<strong>${feature.properties?.name || code}</strong><div>No configured strategy.</div>`;
        return;
      }
      tooltip.hidden = false;
      tooltip.innerHTML = `
        <strong>${feature.properties?.name || code} (${code})</strong>
        <div>Strategy: ${row.strategy_label || "-"}</div>
        <div>Total spend: ${formatCurrency(row.total_spend, 0)}</div>
        <div>COR: ${formatPercent(row.cor)}</div>
        <div>ROE: ${formatPercent(row.roe)}</div>
        <div>Binds: ${formatInt(row.binds)}</div>
        <div>Performance: ${formatPercent(row.performance)}</div>
        <div>Additional clicks: ${formatInt(row.additional_clicks)}</div>
        <div>Additional binds: ${formatDecimal(row.additional_binds, 1)}</div>
        <div>Additional budget: ${formatCurrency(row.additional_budget, 0)}</div>
        <div>CPC uplift: ${formatPercent(row.cpc_uplift)}</div>
        <div>CPB uplift: ${formatPercent(row.cpb_uplift)}</div>
      `;
    })
    .on("mousemove", function (event) {
      if (!tooltip || tooltip.hidden) {
        return;
      }
      const bounds = el.statePlanAnalysisMapSvg.getBoundingClientRect();
      tooltip.style.left = `${event.clientX - bounds.left + 14}px`;
      tooltip.style.top = `${event.clientY - bounds.top + 14}px`;
    })
    .on("mouseleave", function () {
      if (tooltip) {
        tooltip.hidden = true;
      }
    })
    .on("click", function (_event, feature) {
      const code = FIPS_TO_STATE_CODE[Number(feature.id)] || "";
      if (!code) {
        return;
      }
      state.statePlanSelectedState = code;
      renderStatePlanSelectedStateDetails();
    });
}

function clearStatePlanAnalysisMap() {
  if (!el.statePlanAnalysisMapSvg || !window.d3) {
    return;
  }
  window.d3.select(el.statePlanAnalysisMapSvg).selectAll("*").remove();
  if (el.statePlanAnalysisMapTooltip) {
    el.statePlanAnalysisMapTooltip.hidden = true;
  }
}

async function refreshStateAnalysis() {
  const loadingToken = startPanelLoading(el.stateAnalysisLoading);
  try {
    const scopeKey = getActivityScopeKey();
    const planId = String(el.selectedPlanId?.value || "").trim();
    if (!planId) {
      setStatus(el.stateAnalysisStatus, `Select a plan for ${scopeKey} in Plan Builder to load state analysis.`);
      renderStateAnalysisKpis(null, "stateAnalysis");
      state.stateAnalysisData = null;
      renderStateAnalysisRules();
      clearStateAnalysisMap();
      return;
    }

    setStatus(el.stateAnalysisStatus, "Loading state analysis...");
    const queryString = buildStateAnalysisQuery();
    const data = await api(`/api/analytics/state-analysis?${queryString}`, { timeoutMs: 120000 });
    state.stateAnalysisData = data;
    renderStateAnalysisKpis(data.overall, "stateAnalysis");
    renderStateAnalysisRules();
    await ensureUsStatesGeoJson();
    renderStateAnalysisMap();
    setStatus(el.stateAnalysisStatus, `Loaded ${data.states?.length || 0} state row(s) and ${data.rules?.length || 0} rule(s).`);
  } catch (err) {
    state.stateAnalysisData = null;
    renderStateAnalysisKpis(null, "stateAnalysis");
    renderStateAnalysisRules();
    clearStateAnalysisMap();
    setStatus(el.stateAnalysisStatus, err.message, true);
  } finally {
    stopPanelLoading(el.stateAnalysisLoading, loadingToken);
  }
}

async function refreshStatePlanAnalysis() {
  const loadingToken = startPanelLoading(el.statePlanAnalysisLoading);
  try {
    const scopeKey = getActivityScopeKey();
    const planId = String(el.selectedPlanId?.value || "").trim();
    if (!planId) {
      setStatus(el.statePlanAnalysisStatus, `Select a plan for ${scopeKey} in Plan Builder to load state plan analysis.`);
      renderStateAnalysisKpis(null, "statePlanAnalysis");
      state.statePlanAnalysisData = null;
      state.statePlanSelectedState = "";
      renderStatePlanSelectedStateDetails();
      clearStatePlanAnalysisMap();
      return;
    }

    setStatus(el.statePlanAnalysisStatus, "Loading state plan analysis...");
    const queryString = buildStatePlanAnalysisQuery();
    const data = await api(`/api/analytics/state-analysis?${queryString}`, { timeoutMs: 120000 });
    state.statePlanAnalysisData = data;
    renderStateAnalysisKpis(data.overall, "statePlanAnalysis");
    const details = Array.isArray(data.state_details) ? data.state_details : [];
    state.statePlanSelectedState = "";
    renderStatePlanSelectedStateDetails();
    await ensureUsStatesGeoJson();
    renderStatePlanAnalysisMap();
    setStatus(
      el.statePlanAnalysisStatus,
      `Loaded ${data.states?.length || 0} state row(s) and ${details.length} state detail row(s).`
    );
  } catch (err) {
    state.statePlanAnalysisData = null;
    state.statePlanSelectedState = "";
    renderStateAnalysisKpis(null, "statePlanAnalysis");
    renderStatePlanSelectedStateDetails();
    clearStatePlanAnalysisMap();
    setStatus(el.statePlanAnalysisStatus, err.message, true);
  } finally {
    stopPanelLoading(el.statePlanAnalysisLoading, loadingToken);
  }
}

async function checkMe() {
  try {
    const data = await api("/api/me");
    state.email = data.user.email;
    state.role = data.user.role;
    localStorage.setItem("planning_user_email", state.email);
    localStorage.setItem("planning_user_role", state.role);
    applyRoleAccessUi();
    setStatus(el.meStatus, `Access granted as ${data.user.email} (${data.user.role})`);
    return data.user;
  } catch (err) {
    setStatus(el.meStatus, err.message, true);
    throw err;
  }
}

async function tryRestoreSession() {
  if (!isAuthenticated()) {
    return false;
  }
  try {
    await checkMe();
    showAppLayout();
    return true;
  } catch (_err) {
    clearSessionLocally();
    showLoginScreen("Please log in.");
    return false;
  }
}

function resetLoginUiState() {
  state.authEmailCandidate = "";
  if (el.authPassword) {
    el.authPassword.value = "";
  }
  if (el.authCreatePassword) {
    el.authCreatePassword.value = "";
  }
  if (el.authConfirmPassword) {
    el.authConfirmPassword.value = "";
  }
  if (el.userPasswordLoginWrap) {
    el.userPasswordLoginWrap.hidden = true;
  }
  if (el.userPasswordSetupWrap) {
    el.userPasswordSetupWrap.hidden = true;
  }
}

async function loadUserLoginState() {
  const email = (el.authEmail?.value || "").trim().toLowerCase();
  if (!email) {
    setStatus(el.authStatus, "Enter your email.", true);
    return;
  }
  try {
    const data = await publicApi("/api/auth/user-status", {
      method: "POST",
      body: JSON.stringify({ email })
    });
    if (!data.exists) {
      setStatus(el.authStatus, "User not found. Ask admin to add your email first.", true);
      resetLoginUiState();
      return;
    }
    state.authEmailCandidate = email;
    if (el.userPasswordLoginWrap) {
      el.userPasswordLoginWrap.hidden = Boolean(data.requiresPasswordSetup);
    }
    if (el.userPasswordSetupWrap) {
      el.userPasswordSetupWrap.hidden = !data.requiresPasswordSetup;
    }
    const msg = data.requiresPasswordSetup
      ? "First login detected. Create your password."
      : "Enter your password to continue.";
    setStatus(el.authStatus, msg);
  } catch (err) {
    setStatus(el.authStatus, err.message, true);
  }
}

async function loginAsAdmin() {
  const code = (el.adminAccessCode?.value || "").trim();
  if (!code) {
    setStatus(el.authStatus, "Enter admin code.", true);
    return;
  }
  setButtonBusy(el.adminLoginBtn, true, "Signing in...");
  setStatus(el.authStatus, "Signing in as admin...");
  try {
    const session = await publicApi("/api/auth/admin-login", {
      method: "POST",
      body: JSON.stringify({ code })
    });
    setSession(session);
    showAppLayout();
    setStatus(el.authStatus, "Signed in.");
    if (el.adminAccessCode) {
      el.adminAccessCode.value = "";
    }
    void loadAppDataAfterLogin();
  } catch (err) {
    setStatus(el.authStatus, err.message, true);
  } finally {
    setButtonBusy(el.adminLoginBtn, false);
  }
}

async function loginAsUser() {
  const email = state.authEmailCandidate || (el.authEmail?.value || "").trim().toLowerCase();
  const password = el.authPassword?.value || "";
  if (!email || !password) {
    setStatus(el.authStatus, "Enter email and password.", true);
    return;
  }
  setButtonBusy(el.userLoginBtn, true, "Signing in...");
  setStatus(el.authStatus, "Signing in...");
  try {
    const session = await publicApi("/api/auth/user-login", {
      method: "POST",
      body: JSON.stringify({ email, password })
    });
    setSession(session);
    showAppLayout();
    setStatus(el.authStatus, "Signed in.");
    if (el.authPassword) {
      el.authPassword.value = "";
    }
    void loadAppDataAfterLogin();
  } catch (err) {
    setStatus(el.authStatus, err.message, true);
  } finally {
    setButtonBusy(el.userLoginBtn, false);
  }
}

async function setupPasswordAndLogin() {
  const email = state.authEmailCandidate || (el.authEmail?.value || "").trim().toLowerCase();
  const password = el.authCreatePassword?.value || "";
  const confirm = el.authConfirmPassword?.value || "";
  if (!email) {
    setStatus(el.authStatus, "Enter your email first.", true);
    return;
  }
  if (!password) {
    setStatus(el.authStatus, "Enter a password.", true);
    return;
  }
  if (password !== confirm) {
    setStatus(el.authStatus, "Passwords do not match.", true);
    return;
  }
  setButtonBusy(el.userSetPasswordBtn, true, "Saving...");
  setStatus(el.authStatus, "Creating password...");
  try {
    const session = await publicApi("/api/auth/user-setup-password", {
      method: "POST",
      body: JSON.stringify({ email, password })
    });
    setSession(session);
    showAppLayout();
    setStatus(el.authStatus, "Signed in.");
    if (el.authCreatePassword) {
      el.authCreatePassword.value = "";
    }
    if (el.authConfirmPassword) {
      el.authConfirmPassword.value = "";
    }
    void loadAppDataAfterLogin();
  } catch (err) {
    setStatus(el.authStatus, err.message, true);
  } finally {
    setButtonBusy(el.userSetPasswordBtn, false);
  }
}

async function logoutCurrentUser() {
  try {
    if (state.sessionToken) {
      await publicApi("/api/auth/logout", {
        method: "POST",
        headers: { "x-session-token": state.sessionToken }
      });
    }
  } catch (_err) {
    // No-op on logout failure.
  }
  clearSessionLocally();
  resetLoginUiState();
  showLoginScreen("Logged out.");
}

function renderUsersTable(users) {
  el.usersTableBody.innerHTML = "";
  if (!users.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 5;
    td.textContent = "No users found.";
    tr.appendChild(td);
    el.usersTableBody.appendChild(tr);
    return;
  }

  for (const user of users) {
    const tr = document.createElement("tr");

    const emailTd = document.createElement("td");
    emailTd.textContent = user.email || "-";
    tr.appendChild(emailTd);

    const roleTd = document.createElement("td");
    roleTd.textContent = user.role || "-";
    tr.appendChild(roleTd);

    const activeTd = document.createElement("td");
    activeTd.textContent = user.is_active ? "Yes" : "No";
    tr.appendChild(activeTd);

    const lastLoginTd = document.createElement("td");
    lastLoginTd.textContent = user.last_login_at ? new Date(user.last_login_at).toLocaleString() : "Never";
    tr.appendChild(lastLoginTd);

    const actionsTd = document.createElement("td");
    const resetBtn = document.createElement("button");
    resetBtn.type = "button";
    resetBtn.textContent = "Reset Password";
    resetBtn.addEventListener("click", async () => {
      try {
        await api(`/api/users/${user.user_id}/reset-password`, { method: "POST" });
        setStatus(el.usersStatus, `Password reset for ${user.email}. User must create a new password on next login.`);
        await refreshManagedUsers();
      } catch (err) {
        setStatus(el.usersStatus, err.message, true);
      }
    });
    actionsTd.appendChild(resetBtn);
    tr.appendChild(actionsTd);

    el.usersTableBody.appendChild(tr);
  }
}

async function refreshManagedUsers() {
  if (state.role !== "admin") {
    el.usersTableBody.innerHTML = "";
    setStatus(el.usersStatus, "User management is available only for admin access.", true);
    return;
  }

  try {
    const data = await api("/api/users");
    renderUsersTable(data.users || []);
    setStatus(el.usersStatus, `Loaded ${data.users?.length || 0} user(s).`);
  } catch (err) {
    setStatus(el.usersStatus, err.message, true);
  }
}

async function ensureSelectedPlanId() {
  const existing = getSelectedPlanId();
  if (existing) {
    return existing;
  }

  const data = await api("/api/plans", { timeoutMs: PLAN_LIST_TIMEOUT_MS });
  const latestPlanId = data?.plans?.[0]?.plan_id ? String(data.plans[0].plan_id) : "";
  if (!latestPlanId) {
    return "";
  }

  el.selectedPlanId.value = latestPlanId;
  setStoredSelectedPlanId(latestPlanId);
  return latestPlanId;
}

async function addManagedUserFromInput() {
  if (state.role !== "admin") {
    setStatus(el.usersStatus, "Only admin can add users.", true);
    return;
  }
  const email = (el.newUserEmail?.value || "").trim().toLowerCase();
  if (!email) {
    setStatus(el.usersStatus, "Enter user email.", true);
    return;
  }
  try {
    await api("/api/users", {
      method: "POST",
      body: JSON.stringify({ email })
    });
    if (el.newUserEmail) {
      el.newUserEmail.value = "";
    }
    setStatus(el.usersStatus, `User ${email} is ready. They can create a password on first login.`);
    await refreshManagedUsers();
  } catch (err) {
    setStatus(el.usersStatus, err.message, true);
  }
}

function formatPlanDateRange(startDate, endDate) {
  const start = normalizeIsoDateInput(startDate);
  const end = normalizeIsoDateInput(endDate);
  if (!start || !end) {
    return "-";
  }
  return `${start} to ${end}`;
}

function countPlanRulesFromStrategyConfig(rawValue) {
  if (!rawValue || !String(rawValue).trim()) {
    return 0;
  }
  try {
    const parsed = JSON.parse(String(rawValue));
    const scopes = parsed && typeof parsed === "object" && parsed.scopes && typeof parsed.scopes === "object"
      ? parsed.scopes
      : null;
    if (scopes) {
      return Object.values(scopes).reduce((total, scopeValue) => {
        const rules = Array.isArray(scopeValue?.rules) ? scopeValue.rules : [];
        return total + rules.length;
      }, 0);
    }
    const legacyRules = Array.isArray(parsed?.rules) ? parsed.rules : [];
    return legacyRules.length;
  } catch {
    return 0;
  }
}

function renderPlanSelectorOptions(plans) {
  if (!el.planSelector) {
    return;
  }
  const selectedPlanId = getSelectedPlanId();
  el.planSelector.innerHTML = "";
  const placeholder = document.createElement("option");
  placeholder.value = "";
  placeholder.textContent = "Select plan";
  el.planSelector.appendChild(placeholder);
  for (const plan of plans) {
    const option = document.createElement("option");
    option.value = String(plan.plan_id || "");
    option.textContent = String(plan.plan_name || "");
    el.planSelector.appendChild(option);
  }
  el.planSelector.value = selectedPlanId;
}

function createPlanTableRowElement(planRow) {
  const tr = document.createElement("tr");
  const isEdit = planRow.mode === "edit";

  const nameTd = document.createElement("td");
  const descTd = document.createElement("td");
  const rulesTd = document.createElement("td");
  const perfTd = document.createElement("td");
  const priceTd = document.createElement("td");
  const qbcClicksTd = document.createElement("td");
  const qbcLeadsCallsTd = document.createElement("td");
  const createdTd = document.createElement("td");
  const actionsTd = document.createElement("td");
  const actionWrap = document.createElement("div");
  actionWrap.className = "rule-actions";

  const perfWrap = document.createElement("div");
  perfWrap.className = "plan-range-inline";
  const perfStart = document.createElement("input");
  perfStart.type = "date";
  perfStart.value = String(planRow.performanceStartDate || "");
  perfStart.className = "compact-date";
  const perfEnd = document.createElement("input");
  perfEnd.type = "date";
  perfEnd.value = String(planRow.performanceEndDate || "");
  perfEnd.className = "compact-date";

  const priceWrap = document.createElement("div");
  priceWrap.className = "plan-range-inline";
  const priceStart = document.createElement("input");
  priceStart.type = "date";
  priceStart.value = String(planRow.priceStartDate || "");
  priceStart.className = "compact-date";
  const priceEnd = document.createElement("input");
  priceEnd.type = "date";
  priceEnd.value = String(planRow.priceEndDate || "");
  priceEnd.className = "compact-date";

  const qbcClicksInput = document.createElement("input");
  qbcClicksInput.type = "number";
  qbcClicksInput.min = "0";
  qbcClicksInput.step = "0.01";
  qbcClicksInput.value = String(Number(planRow.qbcClicks ?? DEFAULT_PLAN_QBC_CLICKS));

  const qbcLeadsCallsInput = document.createElement("input");
  qbcLeadsCallsInput.type = "number";
  qbcLeadsCallsInput.min = "0";
  qbcLeadsCallsInput.step = "0.01";
  qbcLeadsCallsInput.value = String(Number(planRow.qbcLeadsCalls ?? DEFAULT_PLAN_QBC_LEADS_CALLS));

  if (isEdit) {
    const nameInput = document.createElement("input");
    nameInput.type = "text";
    nameInput.value = String(planRow.planName || "");
    const descInput = document.createElement("input");
    descInput.type = "text";
    descInput.value = String(planRow.description || "");
    perfWrap.appendChild(perfStart);
    perfWrap.appendChild(perfEnd);
    priceWrap.appendChild(priceStart);
    priceWrap.appendChild(priceEnd);
    nameTd.appendChild(nameInput);
    descTd.appendChild(descInput);
    rulesTd.textContent = String(Number(planRow.ruleCount) || 0);
    perfTd.appendChild(perfWrap);
    priceTd.appendChild(priceWrap);
    qbcClicksTd.appendChild(qbcClicksInput);
    qbcLeadsCallsTd.appendChild(qbcLeadsCallsInput);

    const saveBtn = createSaveIconButton("Save Plan", async () => {
      const planName = String(nameInput.value || "").trim();
      if (!planName) {
        setStatus(el.plansTableStatus, "Plan name is required.", true);
        return;
      }
      const payload = {
        planName,
        description: String(descInput.value || "").trim() || undefined
      };
      try {
        let planId = String(planRow.planId || "");
        if (planRow.isNew) {
          const created = await api("/api/plans", {
            method: "POST",
            body: JSON.stringify(payload),
            timeoutMs: PLAN_MUTATION_TIMEOUT_MS
          });
          planId = String(created.planId || "");
        } else {
          await api(`/api/plans/${planId}`, {
            method: "PUT",
            body: JSON.stringify(payload),
            timeoutMs: PLAN_MUTATION_TIMEOUT_MS
          });
        }
        const planContext = normalizePlanContextPayload({
          performanceStartDate: perfStart.value,
          performanceEndDate: perfEnd.value,
          priceExplorationStartDate: priceStart.value,
          priceExplorationEndDate: priceEnd.value,
          qbcClicks: qbcClicksInput.value,
          qbcLeadsCalls: qbcLeadsCallsInput.value
        });
        await api(`/api/plans/${planId}/parameters`, {
          method: "PUT",
          timeoutMs: PLAN_MUTATION_TIMEOUT_MS,
          body: JSON.stringify({
            parameters: [
              {
                key: PLAN_CONTEXT_PARAM_KEY,
                value: JSON.stringify(planContext),
                valueType: "json"
              }
            ]
          })
        });
        setStatus(el.plansTableStatus, planRow.isNew ? "Plan created." : "Plan updated.");
        el.selectedPlanId.value = planId;
        setStoredSelectedPlanId(planId);
        await refreshPlans();
      } catch (err) {
        setStatus(el.plansTableStatus, err.message || "Failed saving plan.", true);
      }
    });

    const cancelBtn = createCancelIconButton("Cancel Edit", () => {
      void refreshPlans();
    });
    actionWrap.appendChild(saveBtn);
    actionWrap.appendChild(cancelBtn);
    actionsTd.appendChild(actionWrap);
  } else {
    nameTd.textContent = String(planRow.planName || "-");
    descTd.textContent = String(planRow.description || "-");
    rulesTd.textContent = String(Number(planRow.ruleCount) || 0);
    perfTd.textContent = formatPlanDateRange(planRow.performanceStartDate, planRow.performanceEndDate);
    priceTd.textContent = formatPlanDateRange(planRow.priceStartDate, planRow.priceEndDate);
    qbcClicksTd.textContent = formatDecimal(Number(planRow.qbcClicks), 2);
    qbcLeadsCallsTd.textContent = formatDecimal(Number(planRow.qbcLeadsCalls), 2);
    createdTd.textContent = planRow.createdAt ? new Date(planRow.createdAt).toLocaleString() : "-";

    const editBtn = createEditIconButton("Edit Plan", () => {
      planRow.mode = "edit";
      renderPlansTable();
    });

    const cloneBtn = createCloneIconButton("Clone Plan", async () => {
      const suggested = `${planRow.planName} (Clone)`;
      const input = window.prompt("Clone plan name:", suggested);
      if (input === null) {
        return;
      }
      try {
        const result = await api(`/api/plans/${planRow.planId}/clone`, {
          method: "POST",
          body: JSON.stringify({ planName: String(input || "").trim() || suggested }),
          timeoutMs: PLAN_MUTATION_TIMEOUT_MS
        });
        el.selectedPlanId.value = result.planId;
        setStoredSelectedPlanId(result.planId);
        await refreshPlans();
      } catch (err) {
        setStatus(el.plansTableStatus, err.message || "Failed cloning plan.", true);
      }
    });

    const deleteBtn = createDeleteIconButton("Delete Plan", async () => {
      const confirmed = await confirmPlanDelete(planRow.planName);
      if (!confirmed) {
        return;
      }
      try {
        await api(`/api/plans/${planRow.planId}`, { method: "DELETE", timeoutMs: PLAN_MUTATION_TIMEOUT_MS });
        if (getSelectedPlanId() === planRow.planId) {
          el.selectedPlanId.value = "";
          clearStoredSelectedPlanId();
        }
        await refreshPlans();
        setStatus(el.plansTableStatus, "Plan deleted.");
        showToast("Plan deleted successfully.", "success");
      } catch (err) {
        setStatus(el.plansTableStatus, err.message || "Failed deleting plan.", true);
      }
    });

    actionWrap.appendChild(editBtn);
    actionWrap.appendChild(cloneBtn);
    actionWrap.appendChild(deleteBtn);
    actionsTd.appendChild(actionWrap);
  }

  tr.appendChild(nameTd);
  tr.appendChild(descTd);
  tr.appendChild(rulesTd);
  tr.appendChild(perfTd);
  tr.appendChild(priceTd);
  tr.appendChild(qbcClicksTd);
  tr.appendChild(qbcLeadsCallsTd);
  tr.appendChild(createdTd);
  tr.appendChild(actionsTd);
  return tr;
}

function renderPlansTable() {
  if (!el.plansTableBody) {
    return;
  }
  el.plansTableBody.innerHTML = "";
  if (!state.planTableRows.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 9;
    td.textContent = "No plans found.";
    tr.appendChild(td);
    el.plansTableBody.appendChild(tr);
    return;
  }
  for (const row of state.planTableRows) {
    el.plansTableBody.appendChild(createPlanTableRowElement(row));
  }
}

async function refreshPlans() {
  try {
    const data = await api("/api/plans", { timeoutMs: PLAN_LIST_TIMEOUT_MS });
    const plans = Array.isArray(data.plans) ? data.plans : [];

    state.planTableRows = plans.map((plan) => {
      const contextRaw = String(plan.plan_context_json || "");
      const context = parsePlanContextPayload(contextRaw);
      return {
        mode: "view",
        isNew: false,
        planId: String(plan.plan_id || ""),
        planName: String(plan.plan_name || ""),
        description: String(plan.description || ""),
        ruleCount: countPlanRulesFromStrategyConfig(String(plan.plan_strategy_json || "")),
        performanceStartDate: context.performanceStartDate,
        performanceEndDate: context.performanceEndDate,
        priceStartDate: context.priceExplorationStartDate,
        priceEndDate: context.priceExplorationEndDate,
        qbcClicks: context.qbcClicks,
        qbcLeadsCalls: context.qbcLeadsCalls,
        hasQbcConfigured: contextRaw.includes("qbcClicks") || contextRaw.includes("qbcLeadsCalls"),
        createdAt: String(plan.created_at?.value || plan.created_at || "")
      };
    });
    renderPlansTable();
    renderPlanSelectorOptions(plans);
    renderPlansComparisonPlanOptions();
    setStatus(el.plansTableStatus, `Loaded ${plans.length} plan(s).`);
  } catch (err) {
    setStatus(el.plansTableStatus, err.message || "Failed loading plans.", true);
  }
}

async function refreshAnalyticsFilters() {
  try {
    const params = new URLSearchParams();
    appendGlobalFilter(params);
    if (el.startDate.value) {
      params.set("startDate", el.startDate.value);
    }
    if (el.endDate.value) {
      params.set("endDate", el.endDate.value);
    }

    const data = await api(`/api/analytics/state-segment-performance/filters?${params.toString()}`);
    setMultiOptions("states", data.states || []);
    setMultiOptions("segments", data.segments || []);
    setMultiOptions("stateSegmentChannels", data.channel_groups || data.channelGroups || []);
  } catch (err) {
    setStatus(el.analyticsStatus, err.message, true);
  }
}

async function refreshStateSegmentTable() {
  const loadingToken = startPanelLoading(el.stateSegmentLoading);
  try {
    const queryString = buildStateSegmentAnalyticsQuery();
    const data = await api(`/api/analytics/state-segment-performance?${queryString}`);
    state.stateSegmentRawRows = data.rows || [];
    applyStateSegmentViewAndRender();
    setStatus(el.analyticsStatus, `Loaded ${state.stateSegmentDisplayRows.length || 0} row(s).`);
  } catch (err) {
    setStatus(el.analyticsStatus, err.message, true);
  } finally {
    stopPanelLoading(el.stateSegmentLoading, loadingToken);
  }
}

async function clearStateSegmentFilters() {
  applyDateRange("stateSegment", "", "", { trigger: false });
  state.multiSelectValues.states = [];
  state.multiSelectValues.segments = [];
  state.multiSelectValues.stateSegmentChannels = [];
  updateMultiToggleLabel("states");
  updateMultiToggleLabel("segments");
  updateMultiToggleLabel("stateSegmentChannels");
  el.stateSegmentViewMode.value = "state_segment";
  await refreshAnalyticsFilters();
  await refreshStateSegmentTable();
}

async function refreshPriceExplorationFilters() {
  try {
    const params = new URLSearchParams();
    appendGlobalFilter(params);
    if (el.priceStartDate.value) {
      params.set("startDate", el.priceStartDate.value);
    }
    if (el.priceEndDate.value) {
      params.set("endDate", el.priceEndDate.value);
    }

    const data = await api(`/api/analytics/price-exploration/filters?${params.toString()}`);
    setMultiOptions("priceStates", data.states || []);
    setMultiOptions("priceChannels", data.channelGroups || []);
  } catch (err) {
    setStatus(el.priceExplorationStatus, err.message, true);
  }
}

async function refreshPriceExplorationTable() {
  const loadingToken = startPanelLoading(el.priceExplorationLoading);
  try {
    const queryString = buildPriceExplorationQuery(PRICE_EXPLORATION_KPI_MAX_ROWS);
    const data = await api(`/api/analytics/price-exploration?${queryString}`);
    state.priceExplorationKpiRows = data.rows || [];
    state.priceExplorationRows = state.priceExplorationKpiRows.slice(0, PRICE_EXPLORATION_TABLE_MAX_ROWS);
    renderPriceExplorationRows(state.priceExplorationRows, state.priceExplorationKpiRows);
    const rowCount = state.priceExplorationRows.length;
    const totalCount = state.priceExplorationKpiRows.length;
    const visibleCount = getPriceExplorationVisibleRows(state.priceExplorationKpiRows).length;
    const suffix =
      totalCount > PRICE_EXPLORATION_TABLE_MAX_ROWS
        ? ` Table shows first ${PRICE_EXPLORATION_TABLE_MAX_ROWS.toLocaleString()} rows. KPIs use ${totalCount.toLocaleString()} loaded rows.`
        : "";
    const visibleNote = el.showOnlyRecommendedTp?.checked ? ` Showing ${visibleCount} recommended row(s).` : "";
    setStatus(el.priceExplorationStatus, `Loaded ${rowCount} row(s) in table.${visibleNote}${suffix}`);
  } catch (err) {
    setStatus(el.priceExplorationStatus, err.message, true);
  } finally {
    stopPanelLoading(el.priceExplorationLoading, loadingToken);
  }
}

async function clearPriceExplorationFilters() {
  applyDateRange("priceExploration", "", "", { trigger: false });
  state.multiSelectValues.priceStates = [];
  state.multiSelectValues.priceChannels = [];
  updateMultiToggleLabel("priceStates");
  updateMultiToggleLabel("priceChannels");
  if (el.showOnlyRecommendedTp) {
    el.showOnlyRecommendedTp.checked = false;
  }
  state.priceExplorationRows = [];
  state.priceExplorationKpiRows = [];
  await refreshPriceExplorationFilters();
  resetPriceExplorationResults();
}

function formatTestingPointLabel(value) {
  const num = Number(value) || 0;
  const prefix = num > 0 ? "+" : "";
  return `${prefix}${formatDecimal(num, 0)}%`;
}

function getPriceDecisionPairKey(channelGroupName, stateCode) {
  return `${String(channelGroupName || "")}||${String(stateCode || "").toUpperCase()}`;
}

function parsePriceDecisionPairKey(pairKey) {
  const [channelGroupName = "", stateCode = ""] = String(pairKey || "").split("||");
  return {
    channelGroupName: String(channelGroupName || ""),
    state: String(stateCode || "").toUpperCase(),
    segment: getSegmentFromChannelGroup(channelGroupName)
  };
}

function getPlanStrategyRuleForStateSegment(stateCode, segmentCode) {
  if (!stateCode || !segmentCode) {
    return null;
  }
  const savedRules = state.planStrategyRules.filter((rule) => !rule.isEditing);
  return (
    savedRules.find(
      (rule) =>
        Array.isArray(rule.states) &&
        Array.isArray(rule.segments) &&
        rule.states.map((value) => String(value || "").toUpperCase()).includes(stateCode) &&
        rule.segments.map((value) => String(value || "").toUpperCase()).includes(segmentCode)
    ) || null
  );
}

function getPriceDecisionSelectedTp(groupRows, pairKey) {
  const manualTp = Number(state.priceDecisionManualTestingPoints[pairKey]);
  if (Number.isFinite(manualTp) && groupRows.some((row) => Number(row.testing_point) === manualTp)) {
    return manualTp;
  }

  const recommendedByRow = groupRows.find(
    (row) =>
      Number.isFinite(Number(row.recommended_testing_point)) &&
      Number(row.recommended_testing_point) === Number(row.testing_point)
  );
  if (recommendedByRow) {
    return Number(recommendedByRow.testing_point);
  }

  const recommendedTp = Number(groupRows[0]?.recommended_testing_point);
  if (Number.isFinite(recommendedTp) && groupRows.some((row) => Number(row.testing_point) === recommendedTp)) {
    return recommendedTp;
  }

  if (groupRows.some((row) => Number(row.testing_point) === 0)) {
    return 0;
  }
  return Number(groupRows[0]?.testing_point) || 0;
}

function getPriceDecisionFilterState() {
  const states = getMultiValues("priceDecisionStates");
  const segments = getMultiValues("priceDecisionSegments")
    .map((value) => String(value || "").toUpperCase())
    .filter(Boolean);
  return {
    hasStateFilter: states.length > 0,
    hasSegmentFilter: segments.length > 0,
    hasStateOrSegmentFilter: states.length > 0 || segments.length > 0
  };
}

function buildPriceDecisionGroups(rows) {
  const grouped = new Map();
  for (const row of rows) {
    const stateCode = String(row.state || "").toUpperCase();
    const channelGroupName = String(row.channel_group_name || "");
    const pairKey = getPriceDecisionPairKey(channelGroupName, stateCode);
    const groupRows = grouped.get(pairKey) || [];
    groupRows.push(row);
    grouped.set(pairKey, groupRows);
  }

  const groups = [];
  for (const [pairKey, groupRows] of grouped.entries()) {
    groupRows.sort((a, b) => (Number(a.testing_point) || 0) - (Number(b.testing_point) || 0));
    const sample = groupRows[0] || {};
    const stateCode = String(sample.state || "").toUpperCase();
    const channelGroupName = String(sample.channel_group_name || "");
    const segment = getSegmentFromChannelGroup(channelGroupName);
    const strategyRule = getPlanStrategyRuleForStateSegment(stateCode, segment);
    const selectedTp = getPriceDecisionSelectedTp(groupRows, pairKey);
    const selectedRow =
      groupRows.find((row) => Number(row.testing_point) === Number(selectedTp)) || groupRows[0] || null;
    const recommendedByFlag = groupRows.find(
      (row) =>
        Number.isFinite(Number(row.recommended_testing_point)) &&
        Number(row.recommended_testing_point) === Number(row.testing_point)
    );
    const recommendedRow = recommendedByFlag || selectedRow || groupRows[0] || null;
    const recommendedTp = Number(selectedRow?.recommended_testing_point);

    groups.push({
      key: pairKey,
      state: stateCode,
      channelGroupName,
      segment,
      strategyRule,
      recommendedTp: Number.isFinite(recommendedTp) ? recommendedTp : null,
      recommendedAdditionalBinds: Number(recommendedRow?.expected_bind_change) || 0,
      testingPoints: groupRows.map((row) => Number(row.testing_point) || 0),
      rows: groupRows,
      selectedTp,
      selectedRow
    });
  }
  return groups;
}

function renderPriceDecisionCards(groups) {
  if (!el.priceDecisionCards) {
    return;
  }
  el.priceDecisionCards.innerHTML = "";
  if (!groups.length) {
    const empty = document.createElement("p");
    empty.className = "muted";
    empty.textContent = "No state/channel pairs for selected filters.";
    el.priceDecisionCards.appendChild(empty);
    return;
  }

  for (const group of groups) {
    const row = group.selectedRow || {};
    const card = document.createElement("button");
    card.type = "button";
    card.className = `pd-card${state.priceDecisionSelectedKey === group.key ? " active" : ""}`;

    const title = document.createElement("h4");
    title.textContent = `${group.state} • ${group.channelGroupName}`;

    const details = document.createElement("p");
    details.className = "pd-card-metrics";
    details.textContent = `Bids: ${formatInt(row.bids)} | Sold: ${formatInt(row.sold)} | Binds: ${formatInt(row.binds)}`;

    const recommended = document.createElement("p");
    recommended.className = "pd-card-recommended";
    recommended.textContent = `Recommended: ${
      Number.isFinite(Number(group.recommendedTp)) ? formatTestingPointLabel(group.recommendedTp) : "-"
    }`;

    const points = document.createElement("p");
    points.className = "pd-card-points";
    points.textContent = `Testing: ${group.testingPoints.map((tp) => formatTestingPointLabel(tp)).join(" | ")}`;

    card.appendChild(title);
    card.appendChild(details);
    card.appendChild(recommended);
    card.appendChild(points);
    card.addEventListener("click", () => {
      state.priceDecisionSelectedKey = group.key;
      renderPriceDecisionView();
    });
    el.priceDecisionCards.appendChild(card);
  }
}

function renderPriceDecisionImpactChart(rows) {
  if (!el.priceDecisionImpactChart) {
    return;
  }
  el.priceDecisionImpactChart.innerHTML = "";
  if (!rows.length) {
    const empty = document.createElement("p");
    empty.className = "muted";
    empty.textContent = "No testing points to chart.";
    el.priceDecisionImpactChart.appendChild(empty);
    return;
  }
  if (!window.d3) {
    const fallback = document.createElement("p");
    fallback.className = "muted";
    fallback.textContent = "Chart library unavailable.";
    el.priceDecisionImpactChart.appendChild(fallback);
    return;
  }

  const d3 = window.d3;
  const sorted = [...rows].sort((a, b) => (Number(a.testing_point) || 0) - (Number(b.testing_point) || 0));
  const labels = sorted.map((row) => formatTestingPointLabel(row.testing_point));
  const series = [
    { key: "win_rate_uplift", label: "Win-Rate Uplift", color: "#35c9de" },
    { key: "cpc_uplift", label: "CPC Uplift", color: "#f4a40d" },
    { key: "cpb_uplift", label: "CPB Uplift", color: "#4cd07a" }
  ];

  const header = document.createElement("div");
  header.className = "pd-chart-header";

  const legendWrap = document.createElement("div");
  legendWrap.className = "pd-chart-legends";

  const metricLegend = document.createElement("div");
  metricLegend.className = "pd-chart-legend metric";
  for (const item of series) {
    const entry = document.createElement("span");
    entry.className = "pd-legend-entry";
    entry.innerHTML = `<span class="pd-legend-swatch" style="background:${item.color}"></span>${item.label}`;
    metricLegend.appendChild(entry);
  }
  legendWrap.appendChild(metricLegend);

  const evidenceLegend = document.createElement("div");
  evidenceLegend.className = "pd-chart-legend evidence";
  evidenceLegend.innerHTML =
    '<span class="pd-legend-entry"><span class="pd-legend-dot green"></span>State+Channel evidence</span>' +
    '<span class="pd-legend-entry"><span class="pd-legend-dot yellow"></span>Channel fallback evidence</span>';
  legendWrap.appendChild(evidenceLegend);
  header.appendChild(legendWrap);
  el.priceDecisionImpactChart.appendChild(header);

  const values = sorted.flatMap((row) => series.map((item) => Number(row[item.key]) || 0));
  const maxAbs = Math.max(0.05, ...values.map((value) => Math.abs(value)));
  const domainMax = Math.ceil(maxAbs * 10) / 10;

  const containerWidth = Math.max(420, Math.floor(el.priceDecisionImpactChart.clientWidth - 8));
  const margin = { top: 20, right: 24, bottom: 74, left: 60 };
  const width = containerWidth;
  const height = Math.max(280, Math.min(430, Math.floor(width * 0.4)));
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const axisFont = Math.max(10, Math.min(13, width / 92));
  const axisTitleFont = Math.max(11, Math.min(14, width / 84));
  const barLabelFont = Math.max(9, Math.min(12, width / 94));

  const svg = d3
    .select(el.priceDecisionImpactChart)
    .append("svg")
    .attr("viewBox", `0 0 ${width} ${height}`)
    .attr("preserveAspectRatio", "xMinYMin meet")
    .attr("class", "pd-bar-chart");

  const root = svg
    .append("g")
    .attr("transform", `translate(${margin.left},${margin.top})`);

  const x0 = d3.scaleBand().domain(labels).range([0, innerWidth]).padding(0.22);
  const x1 = d3.scaleBand().domain(series.map((item) => item.key)).range([0, x0.bandwidth()]).padding(0.1);
  const y = d3.scaleLinear().domain([-domainMax, domainMax]).range([innerHeight, 0]);

  const yAxis = d3.axisLeft(y).ticks(7).tickFormat((value) => `${Math.round(Number(value) * 100)}%`);
  root.append("g").attr("class", "pd-axis y").call(yAxis);
  root
    .append("g")
    .attr("class", "pd-axis x")
    .attr("transform", `translate(0,${y(0)})`)
    .call(d3.axisBottom(x0));
  root.selectAll(".pd-axis text").style("font-size", `${axisFont}px`);

  // Y axis title
  if (width >= 760) {
    root
      .append("text")
      .attr("class", "pd-axis-title")
      .attr("transform", `translate(${-50},${innerHeight / 2}) rotate(-90)`)
      .attr("text-anchor", "middle")
      .style("font-size", `${axisTitleFont}px`)
      .text("Percent Change");
  }

  // X axis title
  if (width >= 760) {
    root
      .append("text")
      .attr("class", "pd-axis-title")
      .attr("x", innerWidth / 2)
      .attr("y", innerHeight + 46)
      .attr("text-anchor", "middle")
      .style("font-size", `${axisTitleFont}px`)
      .text("Bid Adjustment Test Point");
  }

  root
    .selectAll(".pd-grid-line")
    .data(y.ticks(7))
    .enter()
    .append("line")
    .attr("class", "pd-grid-line")
    .attr("x1", 0)
    .attr("x2", innerWidth)
    .attr("y1", (value) => y(value))
    .attr("y2", (value) => y(value));

  const groups = root
    .selectAll(".pd-group")
    .data(sorted)
    .enter()
    .append("g")
    .attr("class", "pd-group")
    .attr("transform", (row) => `translate(${x0(formatTestingPointLabel(row.testing_point))},0)`);

  groups
    .selectAll("rect")
    .data((row) =>
      series.map((item) => ({
        seriesKey: item.key,
        color: item.color,
        value: Number(row[item.key]) || 0
      }))
    )
    .enter()
    .append("rect")
    .attr("x", (d) => x1(d.seriesKey))
    .attr("width", x1.bandwidth())
    .attr("y", (d) => (d.value >= 0 ? y(d.value) : y(0)))
    .attr("height", (d) => Math.max(1, Math.abs(y(d.value) - y(0))))
    .attr("fill", (d) => d.color);

  groups
    .selectAll(".pd-bar-label")
    .data((row) =>
      series.map((item) => ({
        seriesKey: item.key,
        value: Number(row[item.key]) || 0
      }))
    )
    .enter()
    .append("text")
    .attr("class", "pd-bar-label")
    .attr("x", (d) => (x1(d.seriesKey) || 0) + x1.bandwidth() / 2)
    .attr("y", (d) => (d.value >= 0 ? y(d.value) - 7 : y(d.value) + 16))
    .attr("text-anchor", "middle")
    .style("font-size", `${barLabelFont}px`)
    .text((d) => formatPercentFixed(d.value, 1));

  // Evidence marker per testing point (top dots)
  groups
    .append("circle")
    .attr("class", "pd-evidence-dot")
    .attr("cx", x0.bandwidth() / 2)
    .attr("cy", y(domainMax * 0.92))
    .attr("r", (row) => (String(row.stat_sig_source || "").toLowerCase().includes("baseline") ? 0 : 6))
    .attr("fill", (row) => {
      const source = String(row.stat_sig_source || "").toLowerCase();
      return source.includes("channel & state") ? "#3bd44f" : "#f4a40d";
    });
}

function renderPriceDecisionDetail(group) {
  if (!group || !group.selectedRow) {
    if (el.priceDecisionDetailTitle) {
      el.priceDecisionDetailTitle.textContent = "Select a state and channel pair";
    }
    if (el.priceDecisionDetailRule) {
      el.priceDecisionDetailRule.textContent = "Strategy Rule: -";
    }
    if (el.priceDecisionDetailEvidence) {
      el.priceDecisionDetailEvidence.textContent = "Recommended testing point and evidence will appear here.";
    }
    if (el.priceDecisionTestingPointSelect) {
      el.priceDecisionTestingPointSelect.innerHTML = "";
    }
    if (el.priceDecisionTableBody) {
      el.priceDecisionTableBody.innerHTML = "";
    }
    renderPriceDecisionImpactChart([]);
    for (const kpiNode of [
      el.priceDecisionKpiBids,
      el.priceDecisionKpiSold,
      el.priceDecisionKpiBinds,
      el.priceDecisionKpiRoe,
      el.priceDecisionKpiCor,
      el.priceDecisionKpiWrUplift,
      el.priceDecisionKpiCpcUplift,
      el.priceDecisionKpiCpbUplift,
      el.priceDecisionKpiAdditionalClicks,
      el.priceDecisionKpiAdditionalBinds,
      el.priceDecisionKpiAdditionalBudget
    ]) {
      if (kpiNode) {
        kpiNode.textContent = "-";
      }
    }
    return;
  }

  const selected = group.selectedRow;
  const baseline = group.rows.find((row) => Number(row.testing_point) === 0) || selected;
  if (el.priceDecisionDetailTitle) {
    el.priceDecisionDetailTitle.textContent = `${group.state} • ${group.channelGroupName}`;
  }
  if (el.priceDecisionDetailRule) {
    const ruleText = group.strategyRule?.name ? group.strategyRule.name : "No matching plan strategy rule";
    el.priceDecisionDetailRule.textContent = `Strategy Rule (${group.segment || "-"}) : ${ruleText}`;
  }
  if (el.priceDecisionDetailEvidence) {
    el.priceDecisionDetailEvidence.textContent =
      `Recommended: ${Number.isFinite(Number(group.recommendedTp)) ? formatTestingPointLabel(group.recommendedTp) : "-"} | ` +
      `Selected: ${formatTestingPointLabel(selected.testing_point)} | ` +
      `Stat-Sig: ${selected.stat_sig || "-"} (${selected.stat_sig_source || "-"})`;
  }

  if (el.priceDecisionTestingPointSelect) {
    el.priceDecisionTestingPointSelect.innerHTML = "";
    for (const row of group.rows) {
      const option = document.createElement("option");
      option.value = String(row.testing_point);
      option.selected = Number(row.testing_point) === Number(group.selectedTp);
      option.textContent =
        `${formatTestingPointLabel(row.testing_point)} | ` +
        `WR ${formatPercentFixed(row.win_rate_uplift, 1)} | ` +
        `CPC ${formatPercentFixed(row.cpc_uplift, 1)} | ` +
        `Binds ${formatDecimal(row.expected_bind_change, 1)} | ` +
        `${row.stat_sig || "-"} (${row.stat_sig_source || "-"})`;
      el.priceDecisionTestingPointSelect.appendChild(option);
    }
    el.priceDecisionTestingPointSelect.onchange = () => {
      const nextTp = Number(el.priceDecisionTestingPointSelect.value);
      if (Number.isFinite(nextTp)) {
        state.priceDecisionManualTestingPoints[group.key] = nextTp;
        void persistPriceDecisionOverridesForSelectedPlan().catch((err) => {
          setStatus(el.priceDecisionStatus, err?.message || "Failed to save manual testing-point override.", true);
        });
        renderPriceDecisionView();
      }
    };
  }

  if (el.priceDecisionKpiBids) {
    el.priceDecisionKpiBids.textContent = formatInt(baseline.bids);
  }
  if (el.priceDecisionKpiSold) {
    el.priceDecisionKpiSold.textContent = formatInt(baseline.sold);
  }
  if (el.priceDecisionKpiBinds) {
    el.priceDecisionKpiBinds.textContent = formatInt(baseline.binds);
  }
  if (el.priceDecisionKpiRoe) {
    el.priceDecisionKpiRoe.textContent = formatPercentFixed(baseline.roe, 2);
  }
  if (el.priceDecisionKpiCor) {
    el.priceDecisionKpiCor.textContent = formatPercentFixed(baseline.combined_ratio, 2);
  }
  if (el.priceDecisionKpiWrUplift) {
    el.priceDecisionKpiWrUplift.textContent = formatPercentFixed(selected.win_rate_uplift, 1);
  }
  if (el.priceDecisionKpiCpcUplift) {
    el.priceDecisionKpiCpcUplift.textContent = formatPercentFixed(selected.cpc_uplift, 1);
  }
  if (el.priceDecisionKpiCpbUplift) {
    el.priceDecisionKpiCpbUplift.textContent = formatPercentFixed(selected.cpb_uplift, 1);
  }
  if (el.priceDecisionKpiAdditionalClicks) {
    el.priceDecisionKpiAdditionalClicks.textContent = formatInt(selected.additional_clicks);
  }
  if (el.priceDecisionKpiAdditionalBinds) {
    el.priceDecisionKpiAdditionalBinds.textContent = formatDecimal(selected.expected_bind_change, 1);
  }
  if (el.priceDecisionKpiAdditionalBudget) {
    el.priceDecisionKpiAdditionalBudget.textContent = formatCurrency(selected.additional_budget_needed, 2);
  }

  renderPriceDecisionImpactChart(group.rows);

  if (el.priceDecisionTableBody) {
    el.priceDecisionTableBody.innerHTML = "";
    for (const row of group.rows) {
      const tr = document.createElement("tr");
      const cells = [
        formatTestingPointLabel(row.testing_point),
        formatInt(row.bids),
        formatInt(row.sold),
        formatInt(row.binds),
        formatPercentFixed(row.win_rate, 1),
        formatCurrency(row.cpc, 2),
        formatCurrency(row.current_cpb, 2),
        formatCurrency(row.expected_cpb, 2),
        formatPercentFixed(row.win_rate_uplift, 1),
        formatPercentFixed(row.cpc_uplift, 1),
        formatPercentFixed(row.cpb_uplift, 1),
        formatInt(row.additional_clicks),
        formatDecimal(row.expected_bind_change, 1),
        formatCurrency(row.additional_budget_needed, 2),
        row.stat_sig || "-",
        row.stat_sig_source || "-"
      ];
      for (const value of cells) {
        const td = document.createElement("td");
        td.textContent = value;
        tr.appendChild(td);
      }
      el.priceDecisionTableBody.appendChild(tr);
    }
  }
}

function updatePriceDecisionStatusFromGroups(groups) {
  if (!el.priceDecisionStatus || !groups.length) {
    return;
  }
  const totals = groups.reduce(
    (acc, group) => {
      const row = group.selectedRow || {};
      acc.additionalClicks += Number(row.additional_clicks) || 0;
      acc.additionalBinds += Number(row.expected_bind_change) || 0;
      acc.additionalBudget += Number(row.additional_budget_needed) || 0;
      return acc;
    },
    { additionalClicks: 0, additionalBinds: 0, additionalBudget: 0 }
  );
  setStatus(
    el.priceDecisionStatus,
    `Loaded ${state.priceDecisionRows.length} row(s) across ${groups.length} state/channel pair(s). ` +
      `Plan impact (selected TPs): +${formatInt(totals.additionalClicks)} clicks | ` +
      `+${formatDecimal(totals.additionalBinds, 1)} binds | ` +
      `${formatCurrency(totals.additionalBudget, 2)} budget.`
  );
}

function renderPriceDecisionView() {
  const allGroups = buildPriceDecisionGroups(state.priceDecisionRows || []);
  const { hasStateOrSegmentFilter } = getPriceDecisionFilterState();
  const sortedGroups = [...allGroups].sort((a, b) => {
    const bindsDiff = (Number(b.recommendedAdditionalBinds) || 0) - (Number(a.recommendedAdditionalBinds) || 0);
    if (Math.abs(bindsDiff) > 1e-9) {
      return bindsDiff;
    }
    const byState = String(a.state || "").localeCompare(String(b.state || ""));
    if (byState !== 0) {
      return byState;
    }
    return String(a.channelGroupName || "").localeCompare(String(b.channelGroupName || ""));
  });
  const groups = hasStateOrSegmentFilter
    ? sortedGroups
    : sortedGroups.slice(0, PRICE_DECISION_TOP_GROUPS_DEFAULT);

  if (!groups.length) {
    state.priceDecisionSelectedKey = "";
    renderPriceDecisionCards([]);
    renderPriceDecisionDetail(null);
    return;
  }

  if (!groups.some((group) => group.key === state.priceDecisionSelectedKey)) {
    state.priceDecisionSelectedKey = groups[0].key;
  }
  renderPriceDecisionCards(groups);
  renderPriceDecisionDetail(groups.find((group) => group.key === state.priceDecisionSelectedKey) || null);
  updatePriceDecisionStatusFromGroups(groups);
  if (!hasStateOrSegmentFilter && allGroups.length > groups.length && el.priceDecisionStatus) {
    const base = String(el.priceDecisionStatus.textContent || "").trim();
    setStatus(
      el.priceDecisionStatus,
      `${base} Showing top ${PRICE_DECISION_TOP_GROUPS_DEFAULT} pairs by recommended additional binds.`
    );
  }
}

function resetPriceDecisionResults(message = "Choose filters and click Apply Filters.") {
  state.priceDecisionRows = [];
  state.priceDecisionSelectedKey = "";
  renderPriceDecisionView();
  setPanelLoading(el.priceDecisionLoading, false);
  setStatus(el.priceDecisionStatus, message);
}

async function loadPriceDecisionOverridesForSelectedPlan() {
  const planId = getSelectedPlanId();
  if (!planId) {
    state.priceDecisionManualTestingPoints = {};
    return;
  }
  const activityScope = getActivityScopeKey();
  try {
    const data = await api(`/api/plans/${planId}/parameters`);
    const parameter = (data.parameters || []).find((item) => String(item.param_key || "") === PRICE_DECISION_PARAM_KEY);
    const parsed = parsePriceDecisionScopedConfig(String(parameter?.param_value || ""), activityScope);
    const nextMap = {};
    for (const item of parsed.overrides || []) {
      const channelGroupName = String(item?.channelGroupName || "").trim();
      const stateCode = String(item?.state || "").toUpperCase();
      const tp = Number(item?.testingPoint);
      if (!channelGroupName || !stateCode || !Number.isFinite(tp)) {
        continue;
      }
      const key = getPriceDecisionPairKey(channelGroupName, stateCode);
      nextMap[key] = tp;
    }
    state.priceDecisionManualTestingPoints = nextMap;
  } catch (_err) {
    state.priceDecisionManualTestingPoints = {};
  }
}

async function persistPriceDecisionOverridesForSelectedPlan() {
  const planId = getSelectedPlanId();
  if (!planId) {
    return;
  }
  const activityScope = getActivityScopeKey();
  const overrides = Object.entries(state.priceDecisionManualTestingPoints)
    .map(([pairKey, testingPoint]) => {
      const parsedKey = parsePriceDecisionPairKey(pairKey);
      const tp = Number(testingPoint);
      if (!parsedKey.channelGroupName || !parsedKey.state || !Number.isFinite(tp)) {
        return null;
      }
      return {
        channelGroupName: parsedKey.channelGroupName,
        state: parsedKey.state,
        segment: parsedKey.segment || "",
        testingPoint: tp,
        updatedAt: new Date().toISOString()
      };
    })
    .filter(Boolean);

  const existingParameters = await api(`/api/plans/${planId}/parameters`);
  const existingParameter = (existingParameters.parameters || []).find(
    (item) => String(item.param_key || "") === PRICE_DECISION_PARAM_KEY
  );
  const existingParsed = parsePriceDecisionScopedConfig(String(existingParameter?.param_value || ""), activityScope);
  const mergedPayload = mergePriceDecisionScopedConfig(existingParsed.config, activityScope, {
    overrides,
    savedAt: new Date().toISOString()
  });

  await api(`/api/plans/${planId}/parameters`, {
    method: "PUT",
    body: JSON.stringify({
      parameters: [
        {
          key: PRICE_DECISION_PARAM_KEY,
          value: JSON.stringify(mergedPayload),
          valueType: "json"
        }
      ]
    })
  });
}

function buildPriceDecisionQuery() {
  const params = new URLSearchParams();
  appendGlobalFilter(params);
  const planId = String(el.selectedPlanId?.value || "").trim();
  if (planId) {
    params.set("planId", planId);
  }
  if (el.priceDecisionStartDate?.value) {
    params.set("startDate", el.priceDecisionStartDate.value);
  }
  if (el.priceDecisionEndDate?.value) {
    params.set("endDate", el.priceDecisionEndDate.value);
  }

  const states = getMultiValues("priceDecisionStates");
  if (states.length) {
    params.set("states", states.join(","));
  }

  const selectedSegments = getMultiValues("priceDecisionSegments").map((value) => String(value || "").toUpperCase());
  const selectedChannels = getMultiValues("priceDecisionChannels");
  const allChannels = state.priceDecisionAllChannels || [];
  let channelCandidates = selectedChannels.length ? [...selectedChannels] : [...allChannels];
  if (selectedSegments.length) {
    const segmentSet = new Set(selectedSegments);
    channelCandidates = channelCandidates.filter((channel) => segmentSet.has(getSegmentFromChannelGroup(channel)));
  }
  if ((selectedChannels.length || selectedSegments.length) && !channelCandidates.length) {
    params.set("channelGroups", "__NO_MATCH__");
  } else if (channelCandidates.length && channelCandidates.length !== allChannels.length) {
    params.set("channelGroups", channelCandidates.join(","));
  }

  const perfRange = getPlanPerformanceRange();
  params.set("q2bStartDate", perfRange.startIso);
  params.set("q2bEndDate", perfRange.endIso);
  const { hasStateOrSegmentFilter } = getPriceDecisionFilterState();
  params.set("limit", String(hasStateOrSegmentFilter ? PRICE_DECISION_FULL_LIMIT : PRICE_EXPLORATION_TABLE_MAX_ROWS));
  return params.toString();
}

function buildPlanOutcomeQuery() {
  const params = new URLSearchParams();
  appendGlobalFilter(params);
  const planId = String(el.selectedPlanId?.value || "").trim();
  if (planId) {
    params.set("planId", planId);
  }
  if (el.priceDecisionStartDate?.value) {
    params.set("startDate", el.priceDecisionStartDate.value);
  }
  if (el.priceDecisionEndDate?.value) {
    params.set("endDate", el.priceDecisionEndDate.value);
  }
  const perfRange = getPlanPerformanceRange();
  params.set("q2bStartDate", perfRange.startIso);
  params.set("q2bEndDate", perfRange.endIso);
  params.set("limit", String(PRICE_DECISION_FULL_LIMIT));
  return params.toString();
}

function getGrowthStrategyLabel(growthStrategy) {
  const raw = String(growthStrategy || "").trim();
  if (!raw) {
    return "Robustic Growth";
  }
  return raw
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function parseTierFromRule(rule) {
  const name = String(rule?.name || "");
  const match = name.match(/tier\s*(\d+)/i);
  if (match) {
    return Number(match[1]) || 999;
  }
  return Number(rule?.id) || 999;
}

function getRecommendedTestingPointFromGroupRows(groupRows) {
  const byFlag = groupRows.find(
    (row) =>
      Number.isFinite(Number(row.recommended_testing_point)) &&
      Number(row.testing_point) === Number(row.recommended_testing_point)
  );
  if (byFlag) {
    return Number(byFlag.testing_point);
  }
  const recommendedTp = Number(groupRows[0]?.recommended_testing_point);
  if (Number.isFinite(recommendedTp)) {
    return recommendedTp;
  }
  return 0;
}

function formatUpliftInCell(value, uplift) {
  const upliftPct = Number(uplift);
  const upliftText = Number.isFinite(upliftPct)
    ? ` (${upliftPct >= 0 ? "+" : ""}${formatPercentFixed(upliftPct, 1)})`
    : "";
  return `${value}${upliftText}`;
}

function derivePlanOutcomeRows(rows) {
  const groupedPairs = new Map();
  for (const row of rows || []) {
    const stateCode = String(row.state || "").toUpperCase();
    const channelGroupName = String(row.channel_group_name || "");
    if (!stateCode || !channelGroupName) {
      continue;
    }
    const key = getPriceDecisionPairKey(channelGroupName, stateCode);
    const current = groupedPairs.get(key) || [];
    current.push(row);
    groupedPairs.set(key, current);
  }

  const outcomeMap = new Map();
  for (const pairRows of groupedPairs.values()) {
    pairRows.sort((a, b) => (Number(a.testing_point) || 0) - (Number(b.testing_point) || 0));
    const sample = pairRows[0] || {};
    const stateCode = String(sample.state || "").toUpperCase();
    const channelGroupName = String(sample.channel_group_name || "");
    const segment = getSegmentFromChannelGroup(channelGroupName);
    const rule = getPlanStrategyRuleForStateSegment(stateCode, segment);
    if (!rule) {
      continue;
    }
    const recommendedTp = getRecommendedTestingPointFromGroupRows(pairRows);
    if (!Number.isFinite(recommendedTp) || Number(recommendedTp) === 0) {
      continue;
    }
    const selected = pairRows.find((row) => Number(row.testing_point) === Number(recommendedTp));
    if (!selected) {
      continue;
    }

    const tier = parseTierFromRule(rule);
    const strategyLabel = getGrowthStrategyLabel(rule.growthStrategy);
    const key = `${tier}|${strategyLabel}|${recommendedTp}`;
    const agg =
      outcomeMap.get(key) ||
      {
        tier,
        strategyLabel,
        testingPoint: recommendedTp,
        channelGroups: new Set(),
        states: new Set(),
        baselineClicks: 0,
        additionalClicks: 0,
        baselineBinds: 0,
        additionalBinds: 0,
        baselineSpend: 0,
        additionalBudget: 0
      };

    const baselineClicks = Number(selected.bids) || 0;
    const additionalClicks = Number(selected.additional_clicks) || 0;
    const baselineBinds = Number(selected.binds) || 0;
    const additionalBinds = Number(selected.expected_bind_change) || 0;
    const baselineSpendFromCpb = Number(selected.current_cpb) * baselineBinds;
    const baselineSpendFromCpc = (Number(selected.cpc) || 0) * (Number(selected.sold) || 0);
    const baselineSpend = Number.isFinite(baselineSpendFromCpb) && baselineSpendFromCpb > 0
      ? baselineSpendFromCpb
      : baselineSpendFromCpc;
    const additionalBudget = Number(selected.additional_budget_needed) || 0;

    agg.channelGroups.add(channelGroupName);
    agg.states.add(stateCode);
    agg.baselineClicks += baselineClicks;
    agg.additionalClicks += additionalClicks;
    agg.baselineBinds += baselineBinds;
    agg.additionalBinds += additionalBinds;
    agg.baselineSpend += Number.isFinite(baselineSpend) ? baselineSpend : 0;
    agg.additionalBudget += additionalBudget;
    outcomeMap.set(key, agg);
  }

  return Array.from(outcomeMap.values())
    .map((agg) => {
      const expectedClicks = agg.baselineClicks + agg.additionalClicks;
      const expectedBinds = agg.baselineBinds + agg.additionalBinds;
      const expectedSpend = agg.baselineSpend + agg.additionalBudget;
      const baselineCpc = agg.baselineClicks > 0 ? agg.baselineSpend / agg.baselineClicks : null;
      const expectedCpc = expectedClicks > 0 ? expectedSpend / expectedClicks : null;
      const baselineCpb = agg.baselineBinds > 0 ? agg.baselineSpend / agg.baselineBinds : null;
      const expectedCpb = expectedBinds > 0 ? expectedSpend / expectedBinds : null;
      const clicksUplift = agg.baselineClicks > 0 ? agg.additionalClicks / agg.baselineClicks : null;
      const bindsUplift = agg.baselineBinds > 0 ? agg.additionalBinds / agg.baselineBinds : null;
      const cpcUplift =
        Number.isFinite(baselineCpc) && baselineCpc > 0 && Number.isFinite(expectedCpc)
          ? expectedCpc / baselineCpc - 1
          : null;
      const cpbUplift =
        Number.isFinite(baselineCpb) && baselineCpb > 0 && Number.isFinite(expectedCpb)
          ? expectedCpb / baselineCpb - 1
          : null;
      return {
        tier: agg.tier,
        strategyLabel: agg.strategyLabel,
        testingPoint: agg.testingPoint,
        channelGroups: Array.from(agg.channelGroups).sort(),
        states: Array.from(agg.states).sort(),
        expectedClicks,
        clicksUplift,
        expectedBinds,
        bindsUplift,
        expectedCpc,
        cpcUplift,
        expectedCpb,
        cpbUplift
      };
    })
    .sort((a, b) => (a.tier - b.tier) || (a.testingPoint - b.testingPoint));
}

function renderPlanOutcomeRows(rows) {
  if (!el.planOutcomeTableBody) {
    return;
  }
  el.planOutcomeTableBody.innerHTML = "";
  if (!rows.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 7;
    td.textContent = "No outcome rows for selected plan and date ranges.";
    tr.appendChild(td);
    el.planOutcomeTableBody.appendChild(tr);
    return;
  }
  for (const row of rows) {
    const tr = document.createElement("tr");
    const cells = [
      `Tier ${row.tier} - ${row.strategyLabel} | ${formatTestingPointLabel(row.testingPoint)}`,
      row.channelGroups.join(", "),
      row.states.join(", "),
      formatUpliftInCell(formatInt(row.expectedClicks), row.clicksUplift),
      formatUpliftInCell(formatDecimal(row.expectedBinds, 1), row.bindsUplift),
      formatUpliftInCell(formatCurrency(row.expectedCpc, 2), row.cpcUplift),
      formatUpliftInCell(formatCurrency(row.expectedCpb, 2), row.cpbUplift)
    ];
    for (const value of cells) {
      const td = document.createElement("td");
      td.textContent = value;
      tr.appendChild(td);
    }
    el.planOutcomeTableBody.appendChild(tr);
  }
}

async function refreshPlanOutcomeTable() {
  const loadingToken = startPanelLoading(el.planOutcomeLoading);
  try {
    const planId = String(el.selectedPlanId?.value || "").trim();
    if (!planId) {
      state.planOutcomeRows = [];
      renderPlanOutcomeRows([]);
      setStatus(el.planOutcomeStatus, "Select a plan to load outcome.");
      return;
    }
    setStatus(el.planOutcomeStatus, "Loading plan outcome...");
    const queryString = buildPlanOutcomeQuery();
    const data = await api(`/api/analytics/price-exploration?${queryString}`, { timeoutMs: 120000 });
    const rows = Array.isArray(data.rows) ? data.rows : [];
    state.planOutcomeRows = derivePlanOutcomeRows(rows);
    renderPlanOutcomeRows(state.planOutcomeRows);
    setStatus(el.planOutcomeStatus, `Loaded ${state.planOutcomeRows.length} outcome row(s).`);
  } catch (err) {
    state.planOutcomeRows = [];
    renderPlanOutcomeRows([]);
    setStatus(el.planOutcomeStatus, err.message || "Failed loading plan outcome.", true);
  } finally {
    stopPanelLoading(el.planOutcomeLoading, loadingToken);
  }
}

async function refreshPriceDecisionFilters() {
  try {
    const params = new URLSearchParams();
    appendGlobalFilter(params);
    if (el.priceDecisionStartDate?.value) {
      params.set("startDate", el.priceDecisionStartDate.value);
    }
    if (el.priceDecisionEndDate?.value) {
      params.set("endDate", el.priceDecisionEndDate.value);
    }
    const data = await api(`/api/analytics/price-exploration/filters?${params.toString()}`);
    const channels = data.channelGroups || [];
    state.priceDecisionAllChannels = channels;
    const segments = [...new Set(channels.map((channel) => getSegmentFromChannelGroup(channel)).filter(Boolean))].sort();
    setMultiOptions("priceDecisionStates", data.states || []);
    setMultiOptions("priceDecisionChannels", channels);
    setMultiOptions("priceDecisionSegments", segments);
  } catch (err) {
    setStatus(el.priceDecisionStatus, err.message, true);
  }
}

async function refreshPriceDecisionTable() {
  const loadingToken = startPanelLoading(el.priceDecisionLoading);
  try {
    const { hasStateOrSegmentFilter } = getPriceDecisionFilterState();
    const queryString = buildPriceDecisionQuery();
    const timeoutMs = hasStateOrSegmentFilter ? 120000 : 30000;
    const data = await api(`/api/analytics/price-exploration?${queryString}`, { timeoutMs });
    const selectedSegments = new Set(getMultiValues("priceDecisionSegments").map((value) => String(value || "").toUpperCase()));
    const rows = (data.rows || []).filter((row) =>
      !selectedSegments.size || selectedSegments.has(getSegmentFromChannelGroup(row.channel_group_name))
    );
    state.priceDecisionRows = rows;
    renderPriceDecisionView();
  } catch (err) {
    setStatus(el.priceDecisionStatus, err.message, true);
  } finally {
    stopPanelLoading(el.priceDecisionLoading, loadingToken);
  }
}

async function clearPriceDecisionFilters() {
  applyDateRange("priceDecision", "", "", { trigger: false });
  state.multiSelectValues.priceDecisionStates = [];
  state.multiSelectValues.priceDecisionChannels = [];
  state.multiSelectValues.priceDecisionSegments = [];
  updateMultiToggleLabel("priceDecisionStates");
  updateMultiToggleLabel("priceDecisionChannels");
  updateMultiToggleLabel("priceDecisionSegments");
  await refreshPriceDecisionFilters();
  resetPriceDecisionResults();
}

async function refreshDerivedTargetOptions() {
  const segmentFallback = ["MCH", "MCR", "SCH", "SCR"];
  const params = new URLSearchParams();
  appendGlobalFilter(params);
  if (el.targetsStartDate.value) {
    params.set("startDate", el.targetsStartDate.value);
  }
  if (el.targetsEndDate.value) {
    params.set("endDate", el.targetsEndDate.value);
  }

  if (!isAuthenticated()) {
    state.derivedTargetStateOptions = mergeWithAllStateCodes(
      state.targetsRows.map((row) => String(row.state || "").toUpperCase()).filter(Boolean)
    );
    state.derivedTargetSegmentOptions = segmentFallback;
    renderDerivedTargetRules();
    return;
  }

  try {
    const data = await api(`/api/analytics/state-segment-performance/filters?${params.toString()}`);
    const states = mergeWithAllStateCodes(data.states || []);
    const segments = [...new Set((data.segments || []).map((value) => String(value || "").toUpperCase()).filter(Boolean))];
    state.derivedTargetStateOptions = states;
    state.derivedTargetSegmentOptions = segments.length ? segments : segmentFallback;

    const stateSet = new Set(state.derivedTargetStateOptions);
    const segmentSet = new Set(state.derivedTargetSegmentOptions);
    let mutated = false;
    for (const rule of state.derivedTargetRules) {
      const prevStates = rule.states.length;
      const prevSegments = Array.isArray(rule.segments) ? rule.segments.length : 0;
      rule.states = rule.states.filter((stateCode) => stateSet.has(stateCode));
      rule.segments = (Array.isArray(rule.segments) ? rule.segments : []).filter((segmentCode) =>
        segmentSet.has(segmentCode)
      );
      if (rule.states.length !== prevStates || rule.segments.length !== prevSegments) {
        mutated = true;
      }
    }
    if (mutated) {
      persistDerivedRulesForCurrentMode();
    }
    renderDerivedTargetRules();
  } catch (_err) {
    state.derivedTargetStateOptions = mergeWithAllStateCodes(
      state.targetsRows.map((row) => String(row.state || "").toUpperCase()).filter(Boolean)
    );
    state.derivedTargetSegmentOptions = segmentFallback;
    renderDerivedTargetRules();
  }
}

async function refreshTargetsFromBq() {
  try {
    renderTargetsLoadingRow("Loading targets from BQ...");
    const queryString = buildTargetsQuery();
    const data = await api(`/api/targets?${queryString}`);
    state.targetsMode = "bq";
    state.uploadedTargetsFile = null;
    state.targetsDefaultLoaded = false;
    state.targetsRows = data.rows || [];
    renderTargetsRows(state.targetsRows);
    await refreshDerivedTargetOptions();
    setStatus(el.targetsStatus, `Loaded ${state.targetsRows.length || 0} BQ row(s).`);
  } catch (err) {
    setStatus(el.targetsStatus, err.message, true);
  }
}

async function addTargetRow() {
  if (state.targetsMode !== "bq") {
    setStatus(el.targetsStatus, "Add Row is available only in BQ mode.", true);
    return;
  }
  try {
    await api(withSelectedPlanId("/api/targets"), { method: "POST" });
    await logChange({
      objectType: "target_row",
      action: "create_target_row",
      before: null,
      after: { mode: "bq" }
    });
    await refreshTargetsFromBq();
  } catch (err) {
    setStatus(el.targetsStatus, err.message, true);
  }
}

async function saveDerivedTargetRulesPreview() {
  if (state.targetsGoalMode === "cpb") {
    setStatus(el.derivedTargetRulesStatus, "Switch to ROE or COR mode first.", true);
    return;
  }
  if (state.targetsGoalMode === "roe" && state.derivedTargetRules.some((rule) => rule.isEditing)) {
    setStatus(el.derivedTargetRulesStatus, "Save all rule rows first (click save icon).", true);
    return;
  }
  const adjustments = buildDerivedTargetAdjustments();
  el.derivedTargetPreviewWrap.hidden = true;
  if (!adjustments.length) {
    setStatus(el.derivedTargetRulesStatus, "No matching rows found for current rules/date range.", true);
    return;
  }
  setStatus(el.derivedTargetRulesStatus, `Prepared ${adjustments.length} row(s) for adjustment.`);
}

async function applyDerivedTargetAdjustments() {
  if (state.targetsGoalMode === "cpb") {
    setStatus(el.derivedTargetRulesStatus, "Switch to ROE or COR mode first.", true);
    return;
  }
  if (state.targetsGoalMode === "roe" && state.derivedTargetRules.some((rule) => rule.isEditing)) {
    setStatus(el.derivedTargetRulesStatus, "Save all rule rows first (click save icon).", true);
    return;
  }
  const adjustments = buildDerivedTargetAdjustments();
  el.derivedTargetPreviewWrap.hidden = true;
  if (!adjustments.length) {
    setStatus(el.derivedTargetRulesStatus, "No matching rows found for current rules/date range.", true);
    return;
  }

  if (state.targetsMode === "file") {
    const before = adjustments.map((item) => ({
      target_id: item.row.target_id,
      source: item.row.source,
      state: item.row.state,
      segment: item.row.segment,
      target_value: item.row.target_value
    }));
    for (const item of adjustments) {
      item.row.target_value = item.adjustedTargetCpb;
      syncFileRow(item.row);
    }
    renderTargetsRows(state.targetsRows);
    await logChange({
      objectType: "target_adjustment",
      action: "apply_adjustment_file",
      before,
      after: adjustments.map((item) => ({
        target_id: item.row.target_id,
        source: item.row.source,
        state: item.row.state,
        segment: item.row.segment,
        target_value: item.adjustedTargetCpb
      })),
      metadata: { mode: state.targetsGoalMode, adjustedRows: adjustments.length }
    });
    setStatus(el.derivedTargetRulesStatus, `Adjusted ${adjustments.length} file row(s).`);
    return;
  }

  const bqAdjustments = adjustments.filter((item) => !item.row.__fromFile && item.row.target_id);
  if (!bqAdjustments.length) {
    setStatus(el.derivedTargetRulesStatus, "No BQ rows available for update.", true);
    return;
  }

  renderTargetsLoadingRow("Applying adjusted targets...");
  const concurrency = 8;
  let index = 0;
  let updated = 0;
  let failed = 0;

  async function worker() {
    while (index < bqAdjustments.length) {
      const current = bqAdjustments[index];
      index += 1;
      try {
        await api(withSelectedPlanId(`/api/targets/${current.row.target_id}`), {
          method: "PUT",
          body: JSON.stringify({ targetValue: current.adjustedTargetCpb })
        });
        updated += 1;
      } catch (_err) {
        failed += 1;
      }
    }
  }

  await Promise.all(Array.from({ length: Math.min(concurrency, bqAdjustments.length) }, () => worker()));
  await refreshTargetsFromBq();
  await logChange({
    objectType: "target_adjustment",
    action: "apply_adjustment_bq",
    before: bqAdjustments.map((item) => ({
      target_id: item.row.target_id,
      source: item.row.source,
      state: item.row.state,
      segment: item.row.segment,
      target_value: item.row.target_value
    })),
    after: bqAdjustments.map((item) => ({
      target_id: item.row.target_id,
      source: item.row.source,
      state: item.row.state,
      segment: item.row.segment,
      target_value: item.adjustedTargetCpb
    })),
    metadata: { mode: state.targetsGoalMode, updated, failed }
  });
  const message = failed
    ? `Adjusted ${updated} BQ row(s), ${failed} failed.`
    : `Adjusted ${updated} BQ row(s).`;
  setStatus(el.derivedTargetRulesStatus, message, failed > 0);
}

el.adminLoginBtn.addEventListener("click", loginAsAdmin);
el.userContinueBtn.addEventListener("click", loadUserLoginState);
el.userLoginBtn.addEventListener("click", loginAsUser);
el.userSetPasswordBtn.addEventListener("click", setupPasswordAndLogin);
el.adminAccessCode.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    void loginAsAdmin();
  }
});
el.authPassword.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    void loginAsUser();
  }
});
el.authConfirmPassword.addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    void setupPasswordAndLogin();
  }
});
if (el.logoutBtn) {
  el.logoutBtn.addEventListener("click", logoutCurrentUser);
}
if (el.refreshUsersBtn) {
  el.refreshUsersBtn.addEventListener("click", refreshManagedUsers);
}
if (el.addUserBtn) {
  el.addUserBtn.addEventListener("click", addManagedUserFromInput);
}

el.createPlan.addEventListener("click", async () => {
  try {
    const body = {
      planName: el.planName.value.trim(),
      description: el.planDesc.value.trim() || undefined
    };
    const data = await api("/api/plans", {
      method: "POST",
      body: JSON.stringify(body)
    });
    setStatus(el.createStatus, `Created plan: ${data.planId}`);
    el.selectedPlanId.value = data.planId;
    setStoredSelectedPlanId(data.planId);
    el.selectedPlanId.dispatchEvent(new Event("change"));
    await refreshPlans();
  } catch (err) {
    setStatus(el.createStatus, err.message, true);
  }
});

el.refreshPlans.addEventListener("click", refreshPlans);

if (el.addPlanRowBtn) {
  el.addPlanRowBtn.addEventListener("click", () => {
    state.planTableRows.unshift({
      mode: "edit",
      isNew: true,
      planId: "",
      planName: "",
      description: "",
      ruleCount: 0,
      performanceStartDate: "",
      performanceEndDate: "",
      priceStartDate: "",
      priceEndDate: "",
      qbcClicks: DEFAULT_PLAN_QBC_CLICKS,
      qbcLeadsCalls: DEFAULT_PLAN_QBC_LEADS_CALLS,
      createdAt: ""
    });
    renderPlansTable();
    setStatus(el.plansTableStatus, "New row added. Fill details and click Save.");
  });
}

if (el.planSelector) {
  el.planSelector.addEventListener("change", () => {
    const planId = String(el.planSelector.value || "").trim();
    if (planId) {
      el.selectedPlanId.value = planId;
      setStoredSelectedPlanId(planId);
    } else {
      el.selectedPlanId.value = "";
      clearStoredSelectedPlanId();
    }
    el.selectedPlanId.dispatchEvent(new Event("change"));
  });
}

if (el.savePlanDateRanges) {
  el.savePlanDateRanges.addEventListener("click", async () => {
    const planId = getSelectedPlanId();
    if (!planId) {
      setStatus(el.planDateRangesStatus, "Select a plan first.", true);
      return;
    }
    state.planContext.performanceStartDate = normalizeIsoDateInput(el.planPerformanceStartDate?.value);
    state.planContext.performanceEndDate = normalizeIsoDateInput(el.planPerformanceEndDate?.value);
    state.planContext.priceExplorationStartDate = normalizeIsoDateInput(el.planPriceStartDate?.value);
    state.planContext.priceExplorationEndDate = normalizeIsoDateInput(el.planPriceEndDate?.value);

    try {
      await persistPlanContextForSelectedPlan();
      applyPlanAndTargetDefaultsToInputs();
      await refreshAnalyticsFilters();
      if (state.activeSection === "analytics" && state.activeAnalyticsTab === "state-segment") {
        await refreshStateSegmentTable();
      }
      await refreshPriceExplorationFilters();
      resetPriceExplorationResults();
      await refreshPriceDecisionFilters();
      resetPriceDecisionResults("Filters are ready. Click Apply Filters.");
      if (state.activeSection === "analytics" && state.activeAnalyticsTab === "strategy-analysis") {
        await refreshStrategyAnalysisTable();
      }
      if (state.activeSection === "analytics" && state.activeAnalyticsTab === "plans-comparison") {
        setStatus(el.plansComparisonStatus, "Date range saved. Click Apply Filters.");
      }
      if (state.activeSection === "analytics" && state.activeAnalyticsTab === "state-analysis") {
        await refreshStateAnalysis();
      }
      if (state.activeSection === "analytics" && state.activeAnalyticsTab === "state-plan-analysis") {
        await refreshStatePlanAnalysis();
      }
      if (state.activeSection === "plan" && state.activePlanTab === "outcome") {
        await loadPlanStrategyForSelectedPlan();
        await refreshPlanOutcomeTable();
      }
      setStatus(el.planDateRangesStatus, "Plan date ranges saved.");
    } catch (err) {
      setStatus(el.planDateRangesStatus, err.message || "Failed to save plan date ranges.", true);
    }
  });
}

el.saveParameter.addEventListener("click", async () => {
  try {
    const planId = el.selectedPlanId.value.trim();
    await api(`/api/plans/${planId}/parameters`, {
      method: "PUT",
      body: JSON.stringify({
        parameters: [
          {
            key: el.paramKey.value.trim(),
            value: el.paramValue.value.trim(),
            valueType: el.paramType.value
          }
        ]
      })
    });
    setStatus(el.actionStatus, "Parameter saved");
  } catch (err) {
    setStatus(el.actionStatus, err.message, true);
  }
});

el.addDecision.addEventListener("click", async () => {
  try {
    const planId = el.selectedPlanId.value.trim();
    const stateValue = el.decisionState.value.trim();
    const channelValue = el.decisionChannel.value.trim();

    await api(`/api/plans/${planId}/decisions`, {
      method: "POST",
      body: JSON.stringify({
        decisions: [
          {
            decisionType: el.decisionType.value.trim(),
            decisionValue: el.decisionValue.value.trim(),
            state: stateValue || undefined,
            channel: channelValue || undefined
          }
        ]
      })
    });
    setStatus(el.actionStatus, "Decision added");
  } catch (err) {
    setStatus(el.actionStatus, err.message, true);
  }
});

el.runPlan.addEventListener("click", async () => {
  try {
    const planId = el.selectedPlanId.value.trim();
    const data = await api(`/api/plans/${planId}/runs`, {
      method: "POST"
    });
    setStatus(el.actionStatus, `Run queued: ${data.runId}`);
  } catch (err) {
    setStatus(el.actionStatus, err.message, true);
  }
});

for (const item of el.menuItems) {
  item.addEventListener("click", () => {
    setActiveSection(item.dataset.section);
  });
}

if (el.sidebarToggleBtn) {
  el.sidebarToggleBtn.addEventListener("click", (event) => {
    event.stopPropagation();
    setSidebarCollapsed(!state.sidebarCollapsed);
  });
}

if (el.sidebarPinBtn) {
  el.sidebarPinBtn.addEventListener("click", (event) => {
    event.stopPropagation();
    setSidebarPinned(!state.sidebarPinned);
  });
}

document.addEventListener("click", (event) => {
  if (state.sidebarPinned || state.sidebarCollapsed || !el.sidebar) {
    return;
  }
  const target = event.target;
  if (target instanceof Node && el.sidebar.contains(target)) {
    return;
  }
  setSidebarCollapsed(true);
});

el.planTabBuilder.addEventListener("click", () => {
  setActiveSection("plan");
  setActivePlanTab("builder");
});

el.planTabTargets.addEventListener("click", async () => {
  setActiveSection("plan");
  setActivePlanTab("targets");
  const targetsRange = getTargetsDefaultRange();
  applyDateRange("targets", targetsRange.startIso, targetsRange.endIso, { trigger: false });
  await refreshDerivedTargetOptions();
  try {
    await loadPlanContextForSelectedPlan();
    applyPlanAndTargetDefaultsToInputs();
    await loadTargetsSharedConfigForSelectedPlan();
    await ensureTargetsDefaultLoaded();
    await refreshTargetsFileMode();
  } catch (err) {
    setStatus(el.targetsStatus, err.message || "Failed to load targets default file.", true);
  }
});

if (el.planTabStrategy) {
  el.planTabStrategy.addEventListener("click", async () => {
    setActiveSection("plan");
    setActivePlanTab("strategy");
    const targetsRange = getTargetsDefaultRange();
    applyDateRange("targets", targetsRange.startIso, targetsRange.endIso, { trigger: false });
    try {
      await ensureSelectedPlanId();
    } catch (_err) {
      // No-op: load handler below will show relevant status.
    }
    await refreshPlanStrategyOptions();
    await loadPlanStrategyForSelectedPlan();
  });
}

if (el.planTabPriceDecision) {
  el.planTabPriceDecision.addEventListener("click", async () => {
    setActiveSection("plan");
    setActivePlanTab("price-decision");
    const priceRange = getPlanPriceExplorationRange();
    applyDateRange("priceDecision", priceRange.startIso, priceRange.endIso, { trigger: false });
    if (isAuthenticated()) {
      await loadPlanStrategyForSelectedPlan();
      await loadPriceDecisionOverridesForSelectedPlan();
      await refreshPriceDecisionFilters();
      resetPriceDecisionResults("Filters are ready. Click Apply Filters.");
    }
  });
}

if (el.planTabOutcome) {
  el.planTabOutcome.addEventListener("click", async () => {
    setActiveSection("plan");
    setActivePlanTab("outcome");
    if (isAuthenticated()) {
      await loadPlanStrategyForSelectedPlan();
      await refreshPlanOutcomeTable();
    }
  });
}

el.analyticsTabStateSegment.addEventListener("click", async () => {
  setActiveSection("analytics");
  setActiveAnalyticsTab("state-segment");
  const perfRange = getPlanPerformanceRange();
  applySharedPerformanceDateRange(perfRange.startIso, perfRange.endIso);
  if (isAuthenticated()) {
    await refreshAnalyticsFilters();
    await refreshStateSegmentTable();
  }
});
el.analyticsTabPriceExploration.addEventListener("click", async () => {
  setActiveSection("analytics");
  setActiveAnalyticsTab("price-exploration");
  const priceRange = getPlanPriceExplorationRange();
  applySharedPriceExplorationDateRange(priceRange.startIso, priceRange.endIso);
  if (isAuthenticated()) {
    await refreshPriceExplorationFilters();
    resetPriceExplorationResults();
  }
});
if (el.analyticsTabStrategyAnalysis) {
  el.analyticsTabStrategyAnalysis.addEventListener("click", async () => {
    setActiveSection("analytics");
    setActiveAnalyticsTab("strategy-analysis");
    const perfRange = getPlanPerformanceRange();
    applySharedPerformanceDateRange(perfRange.startIso, perfRange.endIso);
    if (isAuthenticated()) {
      await refreshStrategyAnalysisTable();
    }
  });
}
if (el.analyticsTabPlansComparison) {
  el.analyticsTabPlansComparison.addEventListener("click", () => {
    setActiveSection("analytics");
    setActiveAnalyticsTab("plans-comparison");
    const perfRange = getPlanPerformanceRange();
    applyDateRange("plansComparison", perfRange.startIso, perfRange.endIso, { trigger: false });
    applyPlansComparisonModeUi();
    renderPlansComparisonPlanOptions();
    setStatus(el.plansComparisonStatus, "Filters are ready. Click Apply Filters.");
  });
}
if (el.analyticsTabStateAnalysis) {
  el.analyticsTabStateAnalysis.addEventListener("click", async () => {
    setActiveSection("analytics");
    setActiveAnalyticsTab("state-analysis");
    const perfRange = getPlanPerformanceRange();
    applySharedPerformanceDateRange(perfRange.startIso, perfRange.endIso);
    if (isAuthenticated()) {
      await refreshStateAnalysis();
    }
  });
}
if (el.analyticsTabStatePlanAnalysis) {
  el.analyticsTabStatePlanAnalysis.addEventListener("click", async () => {
    setActiveSection("analytics");
    setActiveAnalyticsTab("state-plan-analysis");
    const perfRange = getPlanPerformanceRange();
    applySharedPerformanceDateRange(perfRange.startIso, perfRange.endIso);
    if (isAuthenticated()) {
      await refreshStatePlanAnalysis();
    }
  });
}

if (el.settingsSubGlobalFilters) {
  el.settingsSubGlobalFilters.addEventListener("click", async () => {
    setActiveSection("settings");
    setActiveSettingsTab("global-filters");
    if (isAuthenticated()) {
      await loadDefaultTargetsFilesByScopeForSelectedPlan();
      renderSettingsGlobalFiltersTable();
    }
  });
}

el.settingsSubUsers.addEventListener("click", async () => {
  setActiveSection("settings");
  setActiveSettingsTab("users");
  await refreshManagedUsers();
});

initializeDateRangePicker({
  key: "planPerformance",
  input: el.planPerformanceDateRange,
  startInput: el.planPerformanceStartDate,
  endInput: el.planPerformanceEndDate,
  presetsWrap: el.planPerformanceDatePresets,
  onChange: () => {
    state.planContext.performanceStartDate = normalizeIsoDateInput(el.planPerformanceStartDate?.value);
    state.planContext.performanceEndDate = normalizeIsoDateInput(el.planPerformanceEndDate?.value);
    setStatus(el.planDateRangesStatus, "Performance date range updated. Save to apply.");
  }
});
initializeDateRangePicker({
  key: "planPriceExploration",
  input: el.planPriceDateRange,
  startInput: el.planPriceStartDate,
  endInput: el.planPriceEndDate,
  presetsWrap: el.planPriceDatePresets,
  onChange: () => {
    state.planContext.priceExplorationStartDate = normalizeIsoDateInput(el.planPriceStartDate?.value);
    state.planContext.priceExplorationEndDate = normalizeIsoDateInput(el.planPriceEndDate?.value);
    setStatus(el.planDateRangesStatus, "Price exploration date range updated. Save to apply.");
  }
});
initializeDateRangePicker({
  key: "stateSegment",
  input: el.stateSegmentDateRange,
  startInput: el.startDate,
  endInput: el.endDate,
  presetsWrap: el.stateSegmentDatePresets,
  onChange: async () => {
    applySharedPerformanceDateRange(el.startDate.value, el.endDate.value);
    await refreshAnalyticsFilters();
  }
});
initializeDateRangePicker({
  key: "priceExploration",
  input: el.priceDateRange,
  startInput: el.priceStartDate,
  endInput: el.priceEndDate,
  presetsWrap: el.priceDatePresets,
  onChange: async () => {
    applySharedPriceExplorationDateRange(el.priceStartDate.value, el.priceEndDate.value);
    await refreshPriceExplorationFilters();
    resetPriceExplorationResults();
  }
});
initializeDateRangePicker({
  key: "priceDecision",
  input: el.priceDecisionDateRange,
  startInput: el.priceDecisionStartDate,
  endInput: el.priceDecisionEndDate,
  presetsWrap: el.priceDecisionDatePresets,
  onChange: async () => {
    applySharedPriceExplorationDateRange(el.priceDecisionStartDate.value, el.priceDecisionEndDate.value);
    await refreshPriceDecisionFilters();
    resetPriceDecisionResults();
  }
});
initializeDateRangePicker({
  key: "strategyAnalysis",
  input: el.strategyAnalysisDateRange,
  startInput: el.strategyAnalysisStartDate,
  endInput: el.strategyAnalysisEndDate,
  presetsWrap: el.strategyAnalysisDatePresets,
  onChange: async () => {
    applySharedPerformanceDateRange(el.strategyAnalysisStartDate.value, el.strategyAnalysisEndDate.value);
    if (state.activeSection === "analytics" && state.activeAnalyticsTab === "strategy-analysis") {
      await refreshStrategyAnalysisTable();
    }
  }
});
initializeDateRangePicker({
  key: "plansComparison",
  input: el.plansComparisonDateRange,
  startInput: el.plansComparisonStartDate,
  endInput: el.plansComparisonEndDate,
  presetsWrap: el.plansComparisonDatePresets,
  onChange: () => {
    if (state.activeSection === "analytics" && state.activeAnalyticsTab === "plans-comparison") {
      setStatus(el.plansComparisonStatus, "Filters are ready. Click Apply Filters.");
    }
  }
});
initializeDateRangePicker({
  key: "stateAnalysis",
  input: el.stateAnalysisDateRange,
  startInput: el.stateAnalysisStartDate,
  endInput: el.stateAnalysisEndDate,
  presetsWrap: el.stateAnalysisDatePresets,
  onChange: async () => {
    applySharedPerformanceDateRange(el.stateAnalysisStartDate.value, el.stateAnalysisEndDate.value);
    if (state.activeSection === "analytics" && state.activeAnalyticsTab === "state-analysis") {
      await refreshStateAnalysis();
    }
  }
});
initializeDateRangePicker({
  key: "statePlanAnalysis",
  input: el.statePlanAnalysisDateRange,
  startInput: el.statePlanAnalysisStartDate,
  endInput: el.statePlanAnalysisEndDate,
  presetsWrap: el.statePlanAnalysisDatePresets,
  onChange: async () => {
    applySharedPerformanceDateRange(el.statePlanAnalysisStartDate.value, el.statePlanAnalysisEndDate.value);
    if (state.activeSection === "analytics" && state.activeAnalyticsTab === "state-plan-analysis") {
      await refreshStatePlanAnalysis();
    }
  }
});
initializeDateRangePicker({
  key: "targets",
  input: el.targetsDateRange,
  startInput: el.targetsStartDate,
  endInput: el.targetsEndDate,
  presetsWrap: el.targetsDatePresets,
  onChange: async () => {
    await refreshDerivedTargetOptions();
    if (state.activeSection === "plan" && state.activePlanTab === "strategy") {
      await refreshPlanStrategyOptions();
    }
    if (state.targetsMode === "bq") {
      await refreshTargetsFromBq();
    } else {
      await refreshTargetsFileMode();
    }
  }
});

el.applyAnalyticsFilters.addEventListener("click", refreshStateSegmentTable);
el.clearAnalyticsFilters.addEventListener("click", clearStateSegmentFilters);
el.stateSegmentViewMode.addEventListener("change", applyStateSegmentViewAndRender);

el.applyPriceExplorationFilters.addEventListener("click", refreshPriceExplorationTable);
el.clearPriceExplorationFilters.addEventListener("click", clearPriceExplorationFilters);
if (el.showOnlyRecommendedTp) {
  el.showOnlyRecommendedTp.addEventListener("change", () => {
    renderPriceExplorationRows(state.priceExplorationRows, state.priceExplorationKpiRows);
    const loadedCount = state.priceExplorationRows.length;
    const visibleCount = getPriceExplorationVisibleRows(state.priceExplorationKpiRows).length;
    const visibleNote = el.showOnlyRecommendedTp.checked ? ` Showing ${visibleCount} recommended row(s).` : "";
    const suffix =
      state.priceExplorationKpiRows.length > PRICE_EXPLORATION_TABLE_MAX_ROWS
        ? ` Table shows first ${PRICE_EXPLORATION_TABLE_MAX_ROWS.toLocaleString()} rows. KPIs use ${state.priceExplorationKpiRows.length.toLocaleString()} loaded rows.`
        : "";
    setStatus(el.priceExplorationStatus, `Loaded ${loadedCount} row(s) in table.${visibleNote}${suffix}`);
  });
}
if (el.applyPriceDecisionFilters) {
  el.applyPriceDecisionFilters.addEventListener("click", refreshPriceDecisionTable);
}
if (el.clearPriceDecisionFilters) {
  el.clearPriceDecisionFilters.addEventListener("click", clearPriceDecisionFilters);
}
if (el.applyStrategyAnalysisFilters) {
  el.applyStrategyAnalysisFilters.addEventListener("click", refreshStrategyAnalysisTable);
}
if (el.applyPlansComparisonFilters) {
  el.applyPlansComparisonFilters.addEventListener("click", refreshPlansComparisonTable);
}
if (el.plansComparisonMode) {
  el.plansComparisonMode.addEventListener("change", () => {
    state.plansComparisonMode = String(el.plansComparisonMode.value || "plans");
    applyPlansComparisonModeUi();
    setStatus(el.plansComparisonStatus, "Filters are ready. Click Apply Filters.");
  });
}
if (el.plansComparisonPlanId) {
  el.plansComparisonPlanId.addEventListener("change", () => {
    state.plansComparisonPlanId = String(el.plansComparisonPlanId.value || "").trim();
    if (state.plansComparisonMode === "global_filters") {
      setStatus(el.plansComparisonStatus, "Filters are ready. Click Apply Filters.");
    }
  });
}
if (el.strategyAnalysisViewMode) {
  el.strategyAnalysisViewMode.addEventListener("change", () => {
    const nextView = String(el.strategyAnalysisViewMode.value || "rule");
    state.strategyAnalysisViewMode = nextView === "target_cor" ? "target_cor" : "rule";
    localStorage.setItem("planning_strategy_analysis_view_mode", state.strategyAnalysisViewMode);
    renderStrategyAnalysisRows(state.strategyAnalysisRows || []);
  });
}
if (el.applyStateAnalysisFilters) {
  el.applyStateAnalysisFilters.addEventListener("click", refreshStateAnalysis);
}
if (el.applyStatePlanAnalysisFilters) {
  el.applyStatePlanAnalysisFilters.addEventListener("click", refreshStatePlanAnalysis);
}
el.targetsModeCpb.addEventListener("click", () => setTargetsGoalMode("cpb"));
el.targetsModeRoe.addEventListener("click", async () => {
  setTargetsGoalMode("roe");
  await refreshDerivedTargetOptions();
});
el.targetsModeCor.addEventListener("click", async () => {
  setTargetsGoalMode("cor");
  if (!state.planStrategyRules.filter((rule) => !rule.isEditing).length) {
    try {
      await loadPlanStrategyForSelectedPlan();
    } catch (_err) {
      // Keep UI responsive if strategy load fails; table will show empty target COR values.
    }
  }
  await refreshDerivedTargetOptions();
});
el.addDerivedTargetRule.addEventListener("click", () => {
  addDerivedTargetRule({ segments: state.derivedTargetSegmentOptions[0] ? [state.derivedTargetSegmentOptions[0]] : [] });
  persistDerivedRulesForCurrentMode();
  void logChange({
    objectType: "targets_rule",
    action: "add_rule",
    before: null,
    after: state.derivedTargetRules[state.derivedTargetRules.length - 1] || null,
    metadata: { mode: state.targetsGoalMode }
  });
  renderDerivedTargetRules();
});
el.adjustDerivedTargetBtn.addEventListener("click", applyDerivedTargetAdjustments);
el.uploadTargetsFile.addEventListener("click", () => el.targetsFileInput.click());
el.targetsFileInput.addEventListener("change", async () => {
  const file = el.targetsFileInput.files?.[0];
  if (!file) {
    return;
  }
  try {
    await importTargetsFile(file);
  } catch (err) {
    setStatus(el.targetsStatus, err.message || "Failed to load file.", true);
  } finally {
    el.targetsFileInput.value = "";
  }
});
el.downloadTargetsFile.addEventListener("click", downloadTargetsFile);
if (el.downloadDerivedTargetsFile) {
  el.downloadDerivedTargetsFile.addEventListener("click", downloadTargetsFile);
}
if (el.defaultTargetsFileInput) {
  el.defaultTargetsFileInput.addEventListener("change", async () => {
    const file = el.defaultTargetsFileInput.files?.[0];
    const scopeKey = getActivityScopeKey(state.pendingDefaultTargetsUploadScope);
    if (!file) {
      return;
    }
    try {
      const before = safeLogPayload(state.settingsDefaultTargetsByScope[scopeKey] || null);
      await setDefaultTargetsFileForScope(file, scopeKey);
      await loadDefaultTargetsFilesByScopeForSelectedPlan();
      renderSettingsGlobalFiltersTable();
      const scopeLabel = DEFAULT_TARGET_SCOPE_ROWS.find((item) => item.key === scopeKey)?.label || scopeKey;
      setStatus(el.settingsGlobalFiltersStatus, `Saved default target file for ${scopeLabel}.`);
      await logChange({
        objectType: "settings_default_target_file",
        action: "set_default_file",
        before,
        after: { fileName: file.name, scope: scopeKey }
      });
      if (scopeKey === getActivityScopeKey() && state.activeSection === "plan" && state.activePlanTab === "targets") {
        if (!state.uploadedTargetsFile || state.targetsDefaultLoaded) {
          await loadDefaultTargetsFile();
        }
        await refreshTargetsFileMode();
      }
    } catch (err) {
      setStatus(el.settingsGlobalFiltersStatus, err.message || "Failed to set default file.", true);
    } finally {
      state.pendingDefaultTargetsUploadScope = "";
      el.defaultTargetsFileInput.value = "";
    }
  });
}
el.activityLeadTypeFilter.addEventListener("change", async () => {
  state.activityLeadType = el.activityLeadTypeFilter.value || "all";
  localStorage.setItem("planning_activity_lead_type", state.activityLeadType); // cache only
  const scopedPlanId = getStoredSelectedPlanId();
  if (el.selectedPlanId) {
    el.selectedPlanId.value = scopedPlanId;
  }

  if (!isAuthenticated()) {
    return;
  }

  await loadPlanContextForSelectedPlan();
  applyPlanAndTargetDefaultsToInputs();

  await refreshAnalyticsFilters();
  await refreshStateSegmentTable();
  await refreshPriceExplorationFilters();
  resetPriceExplorationResults();
  await refreshPriceDecisionFilters();
  resetPriceDecisionResults();
  if (state.activeSection === "analytics" && state.activeAnalyticsTab === "strategy-analysis") {
    await refreshStrategyAnalysisTable();
  }
  if (state.activeSection === "analytics" && state.activeAnalyticsTab === "state-analysis") {
    await refreshStateAnalysis();
  }
  if (state.activeSection === "analytics" && state.activeAnalyticsTab === "state-plan-analysis") {
    await refreshStatePlanAnalysis();
  }
  if (state.activeSection === "plan" && state.activePlanTab === "targets") {
    await loadTargetsSharedConfigForSelectedPlan();
    await refreshDerivedTargetOptions();
    await refreshTargetsCurrentMode();
  }
  if (state.activeSection === "plan" && state.activePlanTab === "strategy") {
    await refreshPlanStrategyOptions();
    await loadPlanStrategyForSelectedPlan();
  }
  if (state.activeSection === "plan" && state.activePlanTab === "price-decision") {
    await loadPriceDecisionOverridesForSelectedPlan();
    await refreshPriceDecisionFilters();
    resetPriceDecisionResults("Filters are ready. Click Apply Filters.");
  }
  if (state.activeSection === "plan" && state.activePlanTab === "outcome") {
    await loadPlanStrategyForSelectedPlan();
    await refreshPlanOutcomeTable();
  }
  if (state.activeSection === "settings" && state.activeSettingsTab === "global-filters") {
    await loadDefaultTargetsFilesByScopeForSelectedPlan();
    renderSettingsGlobalFiltersTable();
  }
});

if (el.selectedPlanId) {
  el.selectedPlanId.addEventListener("change", async () => {
    const planId = getSelectedPlanId();
    if (el.planSelector) {
      el.planSelector.value = planId;
    }
    if (planId) {
      setStoredSelectedPlanId(planId);
    } else {
      clearStoredSelectedPlanId();
    }
    if (state.activeSection === "plan" && state.activePlanTab === "strategy") {
      await loadPlanStrategyForSelectedPlan();
    }
    if (state.activeSection === "plan" && state.activePlanTab === "targets") {
      await loadPlanContextForSelectedPlan();
      await loadTargetsSharedConfigForSelectedPlan();
      await refreshTargetsCurrentMode();
    }
    renderPlansComparisonPlanOptions();
    await loadPlanContextForSelectedPlan();
    applyPlanAndTargetDefaultsToInputs();
    await refreshAnalyticsFilters();
    if (state.activeSection === "analytics" && state.activeAnalyticsTab === "state-segment") {
      await refreshStateSegmentTable();
    }
    await refreshPriceExplorationFilters();
    resetPriceExplorationResults();
    await refreshPriceDecisionFilters();
    resetPriceDecisionResults("Filters are ready. Click Apply Filters.");
    if (state.activeSection === "plan" && state.activePlanTab === "price-decision") {
      await loadPriceDecisionOverridesForSelectedPlan();
      await refreshPriceDecisionTable();
    }
    if (state.activeSection === "plan" && state.activePlanTab === "outcome") {
      await loadPlanStrategyForSelectedPlan();
      await refreshPlanOutcomeTable();
    }
    if (state.activeSection === "analytics" && state.activeAnalyticsTab === "strategy-analysis") {
      await refreshStrategyAnalysisTable();
    }
    if (state.activeSection === "analytics" && state.activeAnalyticsTab === "plans-comparison") {
      setStatus(el.plansComparisonStatus, "Plan changed. Click Apply Filters.");
    }
    if (state.activeSection === "analytics" && state.activeAnalyticsTab === "state-analysis") {
      await refreshStateAnalysis();
    }
    if (state.activeSection === "analytics" && state.activeAnalyticsTab === "state-plan-analysis") {
      await refreshStatePlanAnalysis();
    }
  });
}

if (el.addPlanStrategyRule) {
  el.addPlanStrategyRule.addEventListener("click", async () => {
    const planId = getSelectedPlanId() || (await ensureSelectedPlanId());
    if (!planId) {
      setStatus(el.planStrategyStatus, "Select a plan ID before adding rules.", true);
      return;
    }
    state.planStrategyRules.push(
      createPlanStrategyRule({
        name: `Strategy ${state.planStrategyRuleIdCounter}`,
        isEditing: true
      })
    );
    renderPlanStrategyTables();
  });
}

if (el.savePlanStrategyBtn) {
  el.savePlanStrategyBtn.addEventListener("click", savePlanStrategyForSelectedPlan);
}

async function loadAppDataAfterLogin() {
  const pageLoadToken = startMainContentLoading();
  setStatus(el.meStatus, "Loading account and data...");
  try {
    try {
      await checkMe();
    } catch (_err) {
      return;
    }

    try {
      await ensureSelectedPlanId();
      await loadPlanContextForSelectedPlan();
      applyPlanAndTargetDefaultsToInputs();
    } catch (_err) {
      // Fall back to cached/default settings.
    }

    await refreshPlans();
    await loadDefaultTargetsFilesByScopeForSelectedPlan();
    renderSettingsGlobalFiltersTable();
    const settledResults = await Promise.allSettled([
      refreshAnalyticsFilters().then(() => refreshStateSegmentTable()),
      refreshPriceExplorationFilters(),
      refreshPriceDecisionFilters(),
      refreshStrategyAnalysisTable(),
      refreshStateAnalysis(),
      refreshStatePlanAnalysis(),
      migrateLegacyQbcForLoadedPlans()
    ]);
    if (settledResults.some((result) => result.status === "fulfilled" && result.value === true)) {
      await refreshPlans();
    }
    resetPriceExplorationResults("Filters are ready. Click Apply Filters to load price exploration data.");
    resetPriceDecisionResults("Filters are ready. Click Apply Filters.");
    if (state.activeSection === "settings" && state.activeSettingsTab === "users") {
      await refreshManagedUsers();
    }
    if (state.activeSection === "plan" && state.activePlanTab === "targets") {
      try {
        await loadTargetsSharedConfigForSelectedPlan();
        await refreshDerivedTargetOptions();
        await ensureTargetsDefaultLoaded();
        await refreshTargetsFileMode();
      } catch (err) {
        setStatus(el.targetsStatus, err.message || "Failed to load targets default file.", true);
      }
    }
    if (state.activeSection === "plan" && state.activePlanTab === "strategy") {
      try {
        await ensureSelectedPlanId();
      } catch (_err) {
        // Keep going and let strategy loader show status.
      }
      await refreshPlanStrategyOptions();
      await loadPlanStrategyForSelectedPlan();
    }
    if (state.activeSection === "analytics" && state.activeAnalyticsTab === "plans-comparison") {
      applyPlansComparisonModeUi();
      renderPlansComparisonPlanOptions();
      setStatus(el.plansComparisonStatus, "Filters are ready. Click Apply Filters.");
    }
    if (state.activeSection === "plan" && state.activePlanTab === "outcome") {
      await loadPlanStrategyForSelectedPlan();
      await refreshPlanOutcomeTable();
    }
  } finally {
    stopMainContentLoading(pageLoadToken);
  }
}

async function initialize() {
  initializeMultiDropdowns();
  ensurePriceExplorationBidsColumn();
  initializeTableEnhancers();
  applySidebarUi();
  setActiveSection(state.activeSection);
  setActivePlanTab(state.activePlanTab);
  setActiveAnalyticsTab(state.activeAnalyticsTab);
  setActiveSettingsTab(state.activeSettingsTab);
  if (el.strategyAnalysisViewMode) {
    const selectedView = state.strategyAnalysisViewMode === "target_cor" ? "target_cor" : "rule";
    state.strategyAnalysisViewMode = selectedView;
    el.strategyAnalysisViewMode.value = selectedView;
  }
  applyRoleAccessUi();
  renderSettingsGlobalFiltersTable();
  applyPlansComparisonModeUi();
  applyPlanAndTargetDefaultsToInputs();
  setTargetsGoalMode(state.targetsGoalMode);
  renderDerivedTargetRules();
  renderPlanStrategyTables();
  state.defaultTargetsFile = await readStoredDefaultTargetsFile();
  updateDefaultTargetsFileStatus();

  const restored = await tryRestoreSession();
  if (!restored) {
    resetLoginUiState();
    showLoginScreen("Please log in to continue.");
    return;
  }

  await loadAppDataAfterLogin();
}

/* ------------------------------------------------------------------ */
/*  Ask AI — full-page chat                                            */
/* ------------------------------------------------------------------ */

const askAiState = {
  sessionId: "",
  sessions: [],
  sending: false,
  initialized: false,
};

function generateAiSessionId() {
  return "s_" + Date.now().toString(36) + "_" + Math.random().toString(36).slice(2, 8);
}

function initAskAiView() {
  if (askAiState.initialized) return;
  askAiState.initialized = true;
  loadAiSessions();
}

async function loadAiSessions() {
  try {
    const data = await api("/api/ai-chat/sessions", { timeoutMs: 10000 });
    askAiState.sessions = Array.isArray(data) ? data : [];
  } catch {
    askAiState.sessions = [];
  }
  renderAiSessionList();
  if (askAiState.sessions.length > 0 && !askAiState.sessionId) {
    selectAiSession(askAiState.sessions[0].session_id);
  } else if (!askAiState.sessionId) {
    startNewAiSession();
  }
}

function renderAiSessionList() {
  if (!el.askAiSessionList) return;
  el.askAiSessionList.innerHTML = "";
  for (const s of askAiState.sessions) {
    const li = document.createElement("li");
    li.textContent = s.title || "Untitled";
    li.classList.toggle("active", s.session_id === askAiState.sessionId);
    li.addEventListener("click", () => selectAiSession(s.session_id));
    el.askAiSessionList.appendChild(li);
  }
}

async function selectAiSession(sessionId) {
  askAiState.sessionId = sessionId;
  renderAiSessionList();
  renderAiMessages([]);
  try {
    const data = await api(`/api/ai-chat/sessions/${encodeURIComponent(sessionId)}/messages`, { timeoutMs: 10000 });
    const messages = Array.isArray(data) ? data : [];
    renderAiMessages(messages);
  } catch {
    renderAiEmptyState("Could not load messages.");
  }
}

function startNewAiSession() {
  askAiState.sessionId = generateAiSessionId();
  renderAiSessionList();
  renderAiEmptyState("Start a conversation by typing a message below.");
}

function renderAiEmptyState(text) {
  if (!el.askAiMessages) return;
  el.askAiMessages.innerHTML = `<div class="ask-ai-empty">
    <svg viewBox="0 0 24 24" fill="none"><path d="M21 11.5a8.38 8.38 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.38 8.38 0 0 1-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.38 8.38 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5z" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>
    <p>${text}</p>
  </div>`;
}

function renderAiMessages(messages) {
  if (!el.askAiMessages) return;
  if (!messages.length) {
    renderAiEmptyState("Start a conversation by typing a message below.");
    return;
  }
  el.askAiMessages.innerHTML = "";
  for (const m of messages) {
    appendAiMessageBubble(m.role, m.content || m.message || "");
  }
  el.askAiMessages.scrollTop = el.askAiMessages.scrollHeight;
}

function appendAiMessageBubble(role, text) {
  if (!el.askAiMessages) return;
  // Clear empty state if present
  const empty = el.askAiMessages.querySelector(".ask-ai-empty");
  if (empty) empty.remove();

  const div = document.createElement("div");
  div.className = `ask-ai-msg ${role === "user" ? "user" : "assistant"}`;
  div.textContent = text;
  el.askAiMessages.appendChild(div);
  el.askAiMessages.scrollTop = el.askAiMessages.scrollHeight;
}

function buildPlanContext() {
  const ctx = {};
  if (state.planContext.performanceStartDate) ctx.perfStartDate = state.planContext.performanceStartDate;
  if (state.planContext.performanceEndDate) ctx.perfEndDate = state.planContext.performanceEndDate;
  if (state.planContext.priceExplorationStartDate) ctx.priceStartDate = state.planContext.priceExplorationStartDate;
  if (state.planContext.priceExplorationEndDate) ctx.priceEndDate = state.planContext.priceExplorationEndDate;
  if (state.activityLeadType && state.activityLeadType !== "all") ctx.activityLeadType = state.activityLeadType;
  if (state.planContext.qbcClicks > 0) ctx.qbcClicks = state.planContext.qbcClicks;
  if (state.planContext.qbcLeadsCalls > 0) ctx.qbcLeadsCalls = state.planContext.qbcLeadsCalls;
  const planId = el.selectedPlanId?.value?.trim();
  if (planId) ctx.planId = planId;
  return Object.keys(ctx).length ? ctx : undefined;
}

async function sendAiMessage() {
  if (askAiState.sending) return;
  const input = el.askAiInput;
  if (!input) return;
  const message = input.value.trim();
  if (!message) return;

  askAiState.sending = true;
  if (el.askAiSendBtn) el.askAiSendBtn.disabled = true;
  input.value = "";
  input.style.height = "auto";

  appendAiMessageBubble("user", message);

  // Show thinking indicator
  const thinkingDiv = document.createElement("div");
  thinkingDiv.className = "ask-ai-msg assistant";
  thinkingDiv.innerHTML = '<span class="ask-ai-msg-thinking">Thinking...</span>';
  el.askAiMessages.appendChild(thinkingDiv);
  el.askAiMessages.scrollTop = el.askAiMessages.scrollHeight;

  try {
    const body = {
      message,
      sessionId: askAiState.sessionId,
      planContext: buildPlanContext(),
    };
    const data = await api("/api/ai-chat", {
      method: "POST",
      body: JSON.stringify(body),
      timeoutMs: 60000,
    });
    thinkingDiv.remove();
    appendAiMessageBubble("assistant", data.answer || data.message || "No response.");

    // Refresh sessions list (new session might have been created)
    if (!askAiState.sessions.find((s) => s.session_id === askAiState.sessionId)) {
      await loadAiSessions();
      // Re-select current session
      renderAiSessionList();
    }
  } catch (err) {
    thinkingDiv.remove();
    appendAiMessageBubble("assistant", `Error: ${err.message || "Failed to get response."}`);
  } finally {
    askAiState.sending = false;
    if (el.askAiSendBtn) el.askAiSendBtn.disabled = false;
    input.focus();
  }
}

/* Ask AI event listeners */
if (el.askAiSendBtn) {
  el.askAiSendBtn.addEventListener("click", sendAiMessage);
}

if (el.askAiInput) {
  el.askAiInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendAiMessage();
    }
  });
  // Auto-resize textarea
  el.askAiInput.addEventListener("input", () => {
    el.askAiInput.style.height = "auto";
    el.askAiInput.style.height = Math.min(el.askAiInput.scrollHeight, 120) + "px";
  });
}

if (el.askAiNewSessionBtn) {
  el.askAiNewSessionBtn.addEventListener("click", () => {
    startNewAiSession();
  });
}

initialize();
