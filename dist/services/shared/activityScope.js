export const ACTIVITY_SCOPE_KEYS = new Set([
    "all",
    "clicks_auto",
    "clicks_home",
    "leads_auto",
    "leads_home",
    "calls_auto",
    "calls_home"
]);
export function normalizeActivityScopeKey(value) {
    const normalized = String(value || "all").trim().toLowerCase();
    return ACTIVITY_SCOPE_KEYS.has(normalized) ? normalized : "all";
}
export function splitCombinedFilter(value) {
    switch ((value || "").toLowerCase()) {
        case "clicks_auto":
            return {
                activityType: "clicks",
                leadType: "auto",
                activityPattern: "click",
                leadPattern: "auto",
                stateSegmentActivityType: "Click",
                stateSegmentLeadType: "CAR_INSURANCE_LEAD"
            };
        case "clicks_home":
            return {
                activityType: "clicks",
                leadType: "home",
                activityPattern: "click",
                leadPattern: "home",
                stateSegmentActivityType: "Click",
                stateSegmentLeadType: "HOME_INSURANCE_LEAD"
            };
        case "leads_auto":
            return {
                activityType: "leads",
                leadType: "auto",
                activityPattern: "lead",
                leadPattern: "auto",
                stateSegmentActivityType: "Lead",
                stateSegmentLeadType: "CAR_INSURANCE_LEAD"
            };
        case "leads_home":
            return {
                activityType: "leads",
                leadType: "home",
                activityPattern: "lead",
                leadPattern: "home",
                stateSegmentActivityType: "Lead",
                stateSegmentLeadType: "HOME_INSURANCE_LEAD"
            };
        case "calls_auto":
            return {
                activityType: "calls",
                leadType: "auto",
                activityPattern: "call",
                leadPattern: "auto",
                stateSegmentActivityType: "Call",
                stateSegmentLeadType: "CAR_INSURANCE_LEAD"
            };
        case "calls_home":
            return {
                activityType: "calls",
                leadType: "home",
                activityPattern: "call",
                leadPattern: "home",
                stateSegmentActivityType: "Call",
                stateSegmentLeadType: "HOME_INSURANCE_LEAD"
            };
        default:
            return {
                activityType: "",
                leadType: "",
                activityPattern: "",
                leadPattern: "",
                stateSegmentActivityType: "",
                stateSegmentLeadType: ""
            };
    }
}
