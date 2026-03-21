/**
 * Report column metadata — curated from the BQ dimension table specification.
 *
 * Each entry maps a BigQuery field name to its display name, description,
 * data type, category, and whether it can be used as a dimension (column)
 * and/or filter.
 */

export type ColumnMeta = {
  column_name: string;
  display_name: string;
  description: string;
  data_type: string;
  category: string;
  dimension: boolean;
  filter: boolean;
};

export const REPORT_COLUMNS: ColumnMeta[] = [
  // ── Campaign Details ─────────────────────────────────────────────
  { column_name: "BrokerCompanyId", display_name: "Company ID", description: "The customer ID", data_type: "INTEGER", category: "Campaign Details", dimension: false, filter: false },
  { column_name: "Company_Name", display_name: "Company Name", description: "The Beacon customer name", data_type: "STRING", category: "Campaign Details", dimension: true, filter: true },
  { column_name: "Origin_CompanyAccountId", display_name: "Partner ID", description: "Relevant to the table that includes opps (when there is no bid at all you will find the partner ID here)", data_type: "INTEGER", category: "Campaign Details", dimension: false, filter: false },
  { column_name: "Account_Name", display_name: "Partner Name", description: "The name of the partner", data_type: "STRING", category: "Campaign Details", dimension: true, filter: true },
  { column_name: "BrokerId", display_name: "Campaign ID", description: "The ID of the Beacon campaign", data_type: "INTEGER", category: "Campaign Details", dimension: false, filter: false },
  { column_name: "Campaign_Name", display_name: "Campaign Name", description: "The name of the campaign", data_type: "STRING", category: "Campaign Details", dimension: true, filter: true },
  { column_name: "Origin_ActivityType", display_name: "Activity Type", description: "Similar to activity type but for the table with opps", data_type: "STRING", category: "Campaign Details", dimension: true, filter: true },
  { column_name: "LeadType", display_name: "Lead Type", description: "Whether the lead is for Auto or Home insurance (CAR_INSURANCE_LEAD = Auto, HOME_INSURANCE_LEAD = Home)", data_type: "STRING", category: "Campaign Details", dimension: true, filter: true },
  { column_name: "BrokerChannelId", display_name: "Ad Group ID", description: "The ID of the ad group under the campaign that targets the user. Ad groups mostly represent a state", data_type: "INTEGER", category: "Campaign Details", dimension: false, filter: false },
  { column_name: "ChannelGroupName", display_name: "Channel Group", description: "Beacon allows grouping channels and naming them for better and simple optimization", data_type: "STRING", category: "Campaign Details", dimension: true, filter: true },
  { column_name: "StrategyGroupName", display_name: "Bidding Group", description: "Beacon allows grouping ad groups and naming them for better and simple optimization", data_type: "STRING", category: "Campaign Details", dimension: true, filter: true },
  { column_name: "CallInfo_IsBillable", display_name: "Billable Call", description: "Whether a call is billable (a call over 180 seconds is considered billable)", data_type: "BOOLEAN", category: "Campaign Details", dimension: false, filter: false },
  { column_name: "CallInfo_CallDurationSeconds", display_name: "Call Duration", description: "The duration of the call in seconds", data_type: "INTEGER", category: "Campaign Details", dimension: false, filter: false },
  { column_name: "Account_Time Zone", display_name: "Time Zone", description: "The time zone of the campaign", data_type: "STRING", category: "Campaign Details", dimension: false, filter: false },
  { column_name: "Budget_Limits", display_name: "Campaign Budget Limits", description: "The budget limit for the campaign", data_type: "STRING", category: "Campaign Details", dimension: false, filter: false },
  { column_name: "Account_Monthly_Budget", display_name: "Partner Monthly Budget", description: "The monthly budget for the partner", data_type: "STRING", category: "Campaign Details", dimension: false, filter: false },
  { column_name: "Account_Daily_Budget", display_name: "Partner Daily Budget", description: "The daily budget for the partner", data_type: "STRING", category: "Campaign Details", dimension: false, filter: false },
  { column_name: "Account_Daily_Cap", display_name: "Partner Daily Cap", description: "The daily cap for the partner", data_type: "STRING", category: "Campaign Details", dimension: false, filter: false },
  { column_name: "Campaign_Monthly_Budget", display_name: "Campaign Monthly Budget", description: "The monthly budget for the campaign", data_type: "STRING", category: "Campaign Details", dimension: false, filter: false },
  { column_name: "Campaign_Daily_Budget", display_name: "Campaign Daily Budget", description: "The daily budget for the campaign", data_type: "STRING", category: "Campaign Details", dimension: false, filter: false },
  { column_name: "Campaign_Monthly_Cap", display_name: "Campaign Monthly Cap", description: "The monthly cap for the campaign", data_type: "STRING", category: "Campaign Details", dimension: false, filter: false },
  { column_name: "Campaign_Daily_Cap", display_name: "Campaign Daily Cap", description: "The daily cap for the campaign", data_type: "STRING", category: "Campaign Details", dimension: false, filter: false },
  { column_name: "Campaign_URL", display_name: "Campaign URL", description: "The URL to redirect users, for click activity type", data_type: "STRING", category: "Campaign Details", dimension: false, filter: false },
  { column_name: "Is_Account_Active", display_name: "Partner Active Status", description: "Whether the partner is active", data_type: "BOOLEAN", category: "Campaign Details", dimension: false, filter: false },
  { column_name: "Is_Campaign_Active", display_name: "Campaign Active Status", description: "Whether the campaign is active", data_type: "BOOLEAN", category: "Campaign Details", dimension: false, filter: false },
  { column_name: "Last_Modified_UTC", display_name: "Last Modified UTC", description: "Last modified date", data_type: "DATE", category: "Campaign Details", dimension: false, filter: false },

  // ── Bidding Info ─────────────────────────────────────────────────
  { column_name: "bid_count", display_name: "Bid Count", description: "Whether a bid went out (0 or 1)", data_type: "INTEGER", category: "Bidding Info", dimension: false, filter: false },
  { column_name: "bid_price", display_name: "Bid Price", description: "The dollar value of the bid (exists only if there is a bid)", data_type: "FLOAT", category: "Bidding Info", dimension: false, filter: false },
  { column_name: "PriceAdjustmentPercent", display_name: "Testing Point", description: "The platform allows defining testing points that adjust the bid by +/- X% to understand the impact of changing the price", data_type: "INTEGER", category: "Bidding Info", dimension: true, filter: true },
  { column_name: "ExtraBidData_Ads_0_CreativeId", display_name: "Creative ID", description: "The ID of the creative, relevant for click activity type", data_type: "INTEGER", category: "Bidding Info", dimension: true, filter: true },
  { column_name: "ExtraBidData_Ads_0_Used", display_name: "Impression", description: "Whether the creative was presented", data_type: "BOOLEAN", category: "Bidding Info", dimension: false, filter: false },
  { column_name: "ExtraBidData_Ads_0_Position", display_name: "Position", description: "The position in which the creative was presented (if impression is true)", data_type: "INTEGER", category: "Bidding Info", dimension: false, filter: false },
  { column_name: "ExtraBidData_Ads_0_ImpressionPixel", display_name: "Pixel URL", description: "The URL that came back on the bid and represents the impression URL", data_type: "STRING", category: "Bidding Info", dimension: false, filter: false },
  { column_name: "Prefill_timeout", display_name: "Prefill Timeout", description: "If during the carrier prefill process the API does not respond, it is considered a timeout and the user is redirected to the fallback URL", data_type: "INTEGER", category: "Bidding Info", dimension: false, filter: false },
  { column_name: "Prefill_Error", display_name: "Prefill Error", description: "If during the carrier prefill process the API does not get all required information (like PII), it is indicated as a prefill error", data_type: "INTEGER", category: "Bidding Info", dimension: false, filter: false },
  { column_name: "Prefill_Empty_URL", display_name: "Prefill Empty URL", description: "If the carrier prefill API returns an empty URL, the user is redirected to the fallback URL", data_type: "INTEGER", category: "Bidding Info", dimension: false, filter: false },
  { column_name: "Transaction_sold", display_name: "Sold", description: "Whether the bid for the lead/click/call won and the lead was acquired", data_type: "INTEGER", category: "Bidding Info", dimension: false, filter: true },
  { column_name: "Price", display_name: "Price", description: "The final price that was paid for the lead", data_type: "FLOAT", category: "Bidding Info", dimension: false, filter: false },

  // ── Bid Rejection Details ────────────────────────────────────────
  { column_name: "TrackingVariables_reject_reason", display_name: "Reject Reason", description: "The reason the bid was rejected", data_type: "STRING", category: "Bid Rejection Details", dimension: true, filter: true },
  { column_name: "CampaignFilteredReason", display_name: "Campaign Filter Reason", description: "The campaign elements that filtered this lead", data_type: "STRING", category: "Bid Rejection Details", dimension: true, filter: true },

  // ── Lead Info ────────────────────────────────────────────────────
  { column_name: "Data_DateCreated", display_name: "Date", description: "When the lead was offered and the auction started. Used to derive date, hour, and day of week dimensions", data_type: "DATETIME", category: "Lead Info", dimension: true, filter: true },
  { column_name: "Data_OwnHome", display_name: "Home Owner", description: "Whether the lead has a home or not", data_type: "BOOLEAN", category: "Lead Info", dimension: true, filter: true },
  { column_name: "Data_City", display_name: "City", description: "The city of the lead", data_type: "STRING", category: "Lead Info", dimension: true, filter: true },
  { column_name: "Data_State", display_name: "State", description: "The state of the lead", data_type: "STRING", category: "Lead Info", dimension: true, filter: true },
  { column_name: "Segments", display_name: "Segment", description: "The user segment: MCH (multi car + home owner), MCR (multi car + rental), SCR (single car + rental), SCH (single car + home owner), Home, RENT", data_type: "STRING", category: "Lead Info", dimension: true, filter: true },
  { column_name: "Lead_LeadID", display_name: "Beacon ID", description: "The lead ID in Beacon, used to identify a lead", data_type: "STRING", category: "Lead Info", dimension: false, filter: true },
  { column_name: "Sha256Email", display_name: "Sha256 Email", description: "SHA-256 representation of the lead email", data_type: "STRING", category: "Lead Info", dimension: false, filter: true },
  { column_name: "Data_Sha256Phone", display_name: "Sha256 Phone", description: "SHA-256 representation of the lead phone", data_type: "STRING", category: "Lead Info", dimension: false, filter: true },
  { column_name: "Data_ZipCode", display_name: "Zip Code", description: "The zip code of the lead", data_type: "STRING", category: "Lead Info", dimension: true, filter: true },
  { column_name: "ZipCodeGroupId", display_name: "Zip Group ID", description: "Beacon allows creating groups of zip codes for suppression or bid modifying. This is the ID of the relevant zip group", data_type: "INTEGER", category: "Lead Info", dimension: true, filter: true },
  { column_name: "Exclusionzipgroupname", display_name: "Exclusion Zip Group Name", description: "Name of the zip group for exclusion", data_type: "STRING", category: "Lead Info", dimension: true, filter: true },
  { column_name: "Suppressionszipgroupname", display_name: "Suppressions Zip Group Name", description: "Name of the zip group for suppression", data_type: "STRING", category: "Lead Info", dimension: true, filter: true },
  { column_name: "Biddingzipgroupname", display_name: "Bidding Zip Group Name", description: "Name of the zip group for bid", data_type: "STRING", category: "Lead Info", dimension: true, filter: true },

  // ── Drivers Information ──────────────────────────────────────────
  { column_name: "Data_DriversCount", display_name: "Drivers Count", description: "The number of drivers", data_type: "INTEGER", category: "Drivers Information", dimension: true, filter: true },
  { column_name: "Data_Drivers_0_Occupation", display_name: "Occupation", description: "The occupation of the lead", data_type: "STRING", category: "Drivers Information", dimension: true, filter: true },
  { column_name: "Data_Drivers_0_DUI", display_name: "DUI", description: "Whether the driver was caught driving under influence before", data_type: "BOOLEAN", category: "Drivers Information", dimension: true, filter: true },
  { column_name: "Data_Drivers_0_SR22", display_name: "SR22", description: "Whether the driver requires an SR22 certificate", data_type: "BOOLEAN", category: "Drivers Information", dimension: true, filter: true },
  { column_name: "Data_Drivers_0_GoodStudent", display_name: "Good Student", description: "Whether the driver has a good student certificate", data_type: "BOOLEAN", category: "Drivers Information", dimension: true, filter: true },
  { column_name: "Data_Drivers_0_MilitaryServiceMember", display_name: "Military Service Member", description: "Whether the driver served in military", data_type: "BOOLEAN", category: "Drivers Information", dimension: true, filter: true },
  { column_name: "Data_Drivers_0_ServedInMilitary", display_name: "Served In Military", description: "Whether the driver served in military", data_type: "BOOLEAN", category: "Drivers Information", dimension: true, filter: true },
  { column_name: "Data_Drivers_0_Gender", display_name: "Gender", description: "The gender of the lead", data_type: "STRING", category: "Drivers Information", dimension: true, filter: true },
  { column_name: "Data_Drivers_0_Education", display_name: "Education", description: "The education level of the lead", data_type: "STRING", category: "Drivers Information", dimension: true, filter: true },
  { column_name: "Data_Drivers_0_LicenseStatus", display_name: "License Status", description: "The license status of the lead", data_type: "STRING", category: "Drivers Information", dimension: true, filter: true },
  { column_name: "Data_Drivers_0_MaritalStatus", display_name: "Marital Status", description: "The marital status of the lead", data_type: "STRING", category: "Drivers Information", dimension: true, filter: true },
  { column_name: "Data_Drivers_0_CreditRating", display_name: "Credit Rating", description: "The first-party self-reported credit score", data_type: "STRING", category: "Drivers Information", dimension: true, filter: true },
  { column_name: "driver_Age", display_name: "Driver Age", description: "The age of the lead", data_type: "INTEGER", category: "Drivers Information", dimension: true, filter: true },

  // ── Insurance Details ────────────────────────────────────────────
  { column_name: "Data_IsCurrentInsurance", display_name: "Currently Insured", description: "Whether the lead is currently insured by a specific insurer", data_type: "BOOLEAN", category: "Insurance Details", dimension: true, filter: true },
  { column_name: "Data_CurrentInsuranceYearsInsured", display_name: "Years Insured", description: "How many years the lead has insurance with the current carrier", data_type: "INTEGER", category: "Insurance Details", dimension: true, filter: true },
  { column_name: "Data_CurrentInsuranceCompany", display_name: "Current Insurance Company", description: "The current insurance carrier", data_type: "STRING", category: "Insurance Details", dimension: false, filter: false },
  { column_name: "Data_CurrentInsuranceExpiration", display_name: "Current Insurance Expiration", description: "When current insurance expired", data_type: "DATETIME", category: "Insurance Details", dimension: false, filter: false },
  { column_name: "Data_CurrentInsuranceStartDate", display_name: "Current Insurance Start Date", description: "When current insurance started", data_type: "DATETIME", category: "Insurance Details", dimension: false, filter: false },
  { column_name: "Data_ContinuousCoverageInMonths", display_name: "Continuous Coverage (Months)", description: "How long the lead had insurance", data_type: "INTEGER", category: "Insurance Details", dimension: false, filter: false },
  { column_name: "Data_CurrentCoverageType", display_name: "Current Coverage Type", description: "The current coverage type the lead has", data_type: "STRING", category: "Insurance Details", dimension: true, filter: true },
  { column_name: "Data_BodilyInjuryPerAccident", display_name: "Bodily Injury Per Accident", description: "The amount of coverage per accident for bodily injury", data_type: "INTEGER", category: "Insurance Details", dimension: true, filter: true },
  { column_name: "Data_BodilyInjuryPerPeson", display_name: "Bodily Injury Per Person", description: "The amount of coverage per person for bodily injury", data_type: "INTEGER", category: "Insurance Details", dimension: true, filter: true },
  { column_name: "Data_Drivers_0_AccidentsCount", display_name: "Number of Accidents", description: "The number of accidents the lead reported", data_type: "INTEGER", category: "Insurance Details", dimension: true, filter: true },
  { column_name: "Data_Drivers_0_TicketOrClaimInTheLastThreeYears", display_name: "Number of Tickets", description: "The number of tickets the lead reported", data_type: "BOOLEAN", category: "Insurance Details", dimension: false, filter: false },
  { column_name: "Data_PropertyDamagePerAccident", display_name: "Property Damage Per Accident", description: "The expected coverage amount needed per accident for the property", data_type: "INTEGER", category: "Insurance Details", dimension: false, filter: false },
  { column_name: "Data_InterestedInHomeInsurance", display_name: "Home Bundle", description: "Whether the user expects to have home insurance in addition to auto insurance", data_type: "BOOLEAN", category: "Insurance Details", dimension: false, filter: false },

  // ── Vehicle Details ──────────────────────────────────────────────
  { column_name: "Data_CarsCount", display_name: "Number of Vehicles", description: "The number of vehicles", data_type: "INTEGER", category: "Vehicle Details", dimension: true, filter: true },
  { column_name: "Data_HasMultipleVehicles", display_name: "Multi-Car", description: "Whether the lead has multiple cars", data_type: "BOOLEAN", category: "Vehicle Details", dimension: true, filter: true },
  { column_name: "Data_Cars_0_Ownership", display_name: "1st Car Ownership", description: "Whether the first car is owned, leased, or other", data_type: "STRING", category: "Vehicle Details", dimension: true, filter: true },
  { column_name: "Data_Cars_0_CoverageDesired", display_name: "1st Car Coverage Desired", description: "Which type of coverage is needed for the first vehicle", data_type: "STRING", category: "Vehicle Details", dimension: false, filter: false },
  { column_name: "Data_Cars_0_AnnualMileage", display_name: "1st Car Annual Mileage", description: "Annual mileage needed for the first vehicle", data_type: "INTEGER", category: "Vehicle Details", dimension: false, filter: false },
  { column_name: "Data_Cars_0_Collision", display_name: "1st Car Collision", description: "Collision coverage needed for the first vehicle", data_type: "STRING", category: "Vehicle Details", dimension: false, filter: false },
  { column_name: "Data_Cars_0_Type", display_name: "1st Car Type", description: "Which type of vehicle is the first vehicle", data_type: "STRING", category: "Vehicle Details", dimension: true, filter: true },
  { column_name: "Data_Cars_0_CommuteDaysPerWeek", display_name: "1st Car Commute Days/Week", description: "How many days a week this vehicle will be used to commute", data_type: "INTEGER", category: "Vehicle Details", dimension: false, filter: false },
  { column_name: "Data_Cars_0_PrimaryUse", display_name: "1st Car Primary Use", description: "Whether the first vehicle is used as primary vehicle", data_type: "STRING", category: "Vehicle Details", dimension: true, filter: true },
  { column_name: "Data_Cars_0_IsAlarmed", display_name: "1st Car Alarmed", description: "Whether the first vehicle has an alarm system", data_type: "BOOLEAN", category: "Vehicle Details", dimension: true, filter: true },
  { column_name: "Data_Cars_0_VIN", display_name: "1st Car VIN", description: "The VIN of the first car", data_type: "STRING", category: "Vehicle Details", dimension: false, filter: false },
  { column_name: "Data_Cars_0_Year", display_name: "1st Car Year", description: "The year of the first car", data_type: "INTEGER", category: "Vehicle Details", dimension: true, filter: true },
  { column_name: "Data_Cars_0_Make", display_name: "1st Car Make", description: "The make of the first car", data_type: "STRING", category: "Vehicle Details", dimension: true, filter: true },
  { column_name: "Data_Cars_0_Model", display_name: "1st Car Model", description: "The model of the first car", data_type: "STRING", category: "Vehicle Details", dimension: true, filter: true },
  { column_name: "Data_Cars_0_SubModel", display_name: "1st Car SubModel", description: "The sub-model of the first car", data_type: "STRING", category: "Vehicle Details", dimension: true, filter: true },
  { column_name: "Data_Cars_1_VIN", display_name: "2nd Car VIN", description: "The VIN of the second car", data_type: "STRING", category: "Vehicle Details", dimension: false, filter: false },
  { column_name: "Data_Cars_1_Year", display_name: "2nd Car Year", description: "The year of the second car", data_type: "INTEGER", category: "Vehicle Details", dimension: false, filter: false },
  { column_name: "Data_Cars_1_Make", display_name: "2nd Car Make", description: "The make of the second car", data_type: "STRING", category: "Vehicle Details", dimension: false, filter: false },
  { column_name: "Data_Cars_1_Model", display_name: "2nd Car Model", description: "The model of the second car", data_type: "STRING", category: "Vehicle Details", dimension: false, filter: false },
  { column_name: "Data_Cars_1_SubModel", display_name: "2nd Car SubModel", description: "The sub-model of the second car", data_type: "STRING", category: "Vehicle Details", dimension: false, filter: false },
  { column_name: "Data_Cars_2_VIN", display_name: "3rd Car VIN", description: "The VIN of the third car", data_type: "STRING", category: "Vehicle Details", dimension: false, filter: false },
  { column_name: "Data_Cars_2_Year", display_name: "3rd Car Year", description: "The year of the third car", data_type: "INTEGER", category: "Vehicle Details", dimension: false, filter: false },
  { column_name: "Data_Cars_2_Make", display_name: "3rd Car Make", description: "The make of the third car", data_type: "STRING", category: "Vehicle Details", dimension: false, filter: false },
  { column_name: "Data_Cars_2_Model", display_name: "3rd Car Model", description: "The model of the third car", data_type: "STRING", category: "Vehicle Details", dimension: false, filter: false },
  { column_name: "Data_Cars_2_SubModel", display_name: "3rd Car SubModel", description: "The sub-model of the third car", data_type: "STRING", category: "Vehicle Details", dimension: false, filter: false },

  // ── Home Information ─────────────────────────────────────────────
  { column_name: "Data_ResidenceCategory", display_name: "Residence Category", description: "The residence category (single home, family home, hometown, condo, etc.)", data_type: "STRING", category: "Home Information", dimension: true, filter: true },
  { column_name: "Data_ResidenceInMonths", display_name: "Residence (Months)", description: "How many months the lead has lived in the residence", data_type: "INTEGER", category: "Home Information", dimension: true, filter: true },
  { column_name: "Data_Properties_0_PropertyInformation_YearBuilt", display_name: "Residence Year Built", description: "When the residence was built", data_type: "INTEGER", category: "Home Information", dimension: true, filter: true },

  // ── Attribution Details ──────────────────────────────────────────
  { column_name: "Attribution_Source", display_name: "Source", description: "The lead source that provides the lead to the partner. Partners work with multiple lead sources", data_type: "STRING", category: "Attribution Details", dimension: true, filter: true },
  { column_name: "Attribution_Channel", display_name: "Channel", description: "The channel under the source that provided the lead", data_type: "STRING", category: "Attribution Details", dimension: true, filter: true },
  { column_name: "Attribution_SubChannel1", display_name: "Sub Channel 1", description: "Usually the media type used to source the lead (search, social, email, remarketing, SMS, native, display, etc.). For call activity type, indicates warm transfer (WT) or inbound call (IB)", data_type: "STRING", category: "Attribution Details", dimension: true, filter: true },
  { column_name: "Attribution_SubChannel2", display_name: "Sub Channel 2", description: "Placeholder for different attribution details; each source uses it for different purposes", data_type: "STRING", category: "Attribution Details", dimension: true, filter: true },
  { column_name: "Attribution_SubChannel3", display_name: "Sub Channel 3", description: "Placeholder for different attribution details; each source uses it for different purposes", data_type: "STRING", category: "Attribution Details", dimension: true, filter: true },
  { column_name: "Attribution_CampaignId", display_name: "Attribution Campaign ID", description: "The ID of the campaign at the partner platform", data_type: "STRING", category: "Attribution Details", dimension: false, filter: false },
  { column_name: "Attribution_CampaignName", display_name: "Attribution Campaign Name", description: "The name of the campaign at the partner platform", data_type: "STRING", category: "Attribution Details", dimension: true, filter: true },
  { column_name: "UniqueId", display_name: "External ID", description: "The ID of the lead at the partner platform", data_type: "STRING", category: "Attribution Details", dimension: false, filter: false },
  { column_name: "UserAgent", display_name: "User Agent", description: "User agent of the device used to fill in the form", data_type: "STRING", category: "Attribution Details", dimension: false, filter: false },
  { column_name: "UserIp", display_name: "User IP", description: "User IP of the device used to fill in the form", data_type: "STRING", category: "Attribution Details", dimension: false, filter: false },
  { column_name: "UserAgentInfo_IsMobile", display_name: "Is Mobile", description: "Whether the lead used a mobile device to fill in the form", data_type: "BOOLEAN", category: "Attribution Details", dimension: true, filter: true },
  { column_name: "UserAgentInfo_DeviceType", display_name: "Device Type", description: "The type of device used to fill in the form", data_type: "STRING", category: "Attribution Details", dimension: true, filter: true },
  { column_name: "UserAgentInfo_DeviceBrand", display_name: "Device Brand", description: "The brand of the device used to fill in the form", data_type: "STRING", category: "Attribution Details", dimension: true, filter: true },
  { column_name: "UserAgentInfo_deviceModel", display_name: "Device Model", description: "The model of the device used to fill in the form", data_type: "STRING", category: "Attribution Details", dimension: true, filter: true },
  { column_name: "UserAgentinfo_OS", display_name: "OS", description: "The OS of the device used to fill in the form", data_type: "STRING", category: "Attribution Details", dimension: true, filter: true },
  { column_name: "UserAgentInfo_Browser", display_name: "Browser", description: "The browser used to fill in the form", data_type: "STRING", category: "Attribution Details", dimension: false, filter: false },
  { column_name: "UserAgentInfo_BrowserVersion", display_name: "Browser Version", description: "The browser version used to fill in the form", data_type: "STRING", category: "Attribution Details", dimension: false, filter: false },
  { column_name: "EmailISP", display_name: "Email ISP", description: "The service provider for the email reported", data_type: "STRING", category: "Attribution Details", dimension: false, filter: false },
  { column_name: "Data_EmailConsent", display_name: "Email Consent", description: "Whether the lead provided approval to be contacted via email", data_type: "BOOLEAN", category: "Attribution Details", dimension: false, filter: false },
  { column_name: "Data_TcpaCall", display_name: "Call Consent", description: "Whether the lead provided approval to be contacted via phone call", data_type: "BOOLEAN", category: "Attribution Details", dimension: false, filter: false },
  { column_name: "Data_TcpaSms", display_name: "SMS Consent", description: "Whether the lead provided approval to be contacted via SMS", data_type: "BOOLEAN", category: "Attribution Details", dimension: false, filter: false },
  { column_name: "Data_TcpaUrl", display_name: "Submission URL", description: "The URL of the form used to source the lead (reported by the partner). Sometimes partners provide a generic URL", data_type: "STRING", category: "Attribution Details", dimension: true, filter: true },

  // ── Rate Call 1 (RC1) ────────────────────────────────────────────
  { column_name: "RateCall1Data_UnderwritingStatus", display_name: "RC1 Status", description: "Whether the user was approved, declined, or had errors during the RC1 check", data_type: "STRING", category: "Rate Call 1 (RC1)", dimension: true, filter: true },
  { column_name: "RateCall1Data_UnderwritingStatusRemarkType", display_name: "RC1 Remark Type", description: "More details about the RC1 status (like decline reasons)", data_type: "STRING", category: "Rate Call 1 (RC1)", dimension: true, filter: true },
  { column_name: "RC1_Reson_Description", display_name: "RC1 Description", description: "Detailed information about what was missing for the RC1 request", data_type: "STRING", category: "Rate Call 1 (RC1)", dimension: true, filter: true },
  { column_name: "RateCall1Data_QuoteId", display_name: "RC1 Quote ID", description: "The rate call 1 ID with the quote ID", data_type: "STRING", category: "Rate Call 1 (RC1)", dimension: false, filter: true },
  { column_name: "RateCall1Data_RecallUrl", display_name: "RC1 URL", description: "When the lead is planned to be sent to an online flow, the rate system provides a URL for the quote", data_type: "STRING", category: "Rate Call 1 (RC1)", dimension: false, filter: false },
  { column_name: "RateCall1Data_MonthlyPrice", display_name: "RC1 Monthly Price", description: "The monthly price reported for the insurance required", data_type: "FLOAT", category: "Rate Call 1 (RC1)", dimension: true, filter: true },
  { column_name: "RateCall1Data_BillingFrequency", display_name: "RC1 Billing Frequency", description: "The billing frequency reported by RC1 for that lead", data_type: "STRING", category: "Rate Call 1 (RC1)", dimension: true, filter: true },
  { column_name: "RateCall1Data_BillingMethod", display_name: "RC1 Billing Method", description: "The billing method reported by RC1 for that lead", data_type: "STRING", category: "Rate Call 1 (RC1)", dimension: true, filter: true },

  // ── Predictive Caller ────────────────────────────────────────────
  { column_name: "PredictiveCallerData_FoundInBlackList", display_name: "BLA Status", description: "Whether the lead was found in the Black List Alliance (BLA) list", data_type: "BOOLEAN", category: "Predictive Caller", dimension: true, filter: true },
  { column_name: "PredictiveCallerData_FoundInBlockList", display_name: "DNC Status", description: "Whether the lead was found in the Do Not Call (DNC) list", data_type: "BOOLEAN", category: "Predictive Caller", dimension: true, filter: true },
  { column_name: "PredictiveCallerData_BlocklistTier", display_name: "DNC Tier", description: "There are 4 tiers for DNC (0, 1, 2, 3). 0 is the most strict; 2 and 3 are softer opt-outs", data_type: "INTEGER", category: "Predictive Caller", dimension: true, filter: true },
  { column_name: "PredictiveCallerData_BlocklistPassedDays", display_name: "DNC Days", description: "How many days ago the lead was added to the DNC list", data_type: "INTEGER", category: "Predictive Caller", dimension: true, filter: true },

  // ── Merkle ───────────────────────────────────────────────────────
  { column_name: "MerkleData_LTVModelScore", display_name: "MD LTV Score", description: "The LTV score that Merkle had for this lead", data_type: "FLOAT", category: "Merkle", dimension: false, filter: false },
  { column_name: "MerkleData_LTVModelVentile", display_name: "MD LTV Ventile", description: "The ventile score that Merkle had for this lead", data_type: "INTEGER", category: "Merkle", dimension: true, filter: true },

  // ── TransUnion ───────────────────────────────────────────────────
  { column_name: "TransUnionData_FullScore", display_name: "TU Full Score", description: "The final score for the lead based on TransUnion data", data_type: "INTEGER", category: "TransUnion", dimension: true, filter: true },
  { column_name: "TransUnionData_PhoneScore", display_name: "TU Phone Score", description: "The phone score for the lead based on TransUnion data", data_type: "INTEGER", category: "TransUnion", dimension: true, filter: true },
  { column_name: "TransUnionDNData_TUPhoneType", display_name: "TU Phone Type", description: "The phone type for the lead based on TransUnion data", data_type: "STRING", category: "TransUnion", dimension: true, filter: true },
  { column_name: "TransUnionDNData_TUPhoneActivity", display_name: "TU Phone Activity", description: "The phone activity score for the lead based on TransUnion data", data_type: "STRING", category: "TransUnion", dimension: true, filter: true },
  { column_name: "TransUnionDNData_TUPhoneContactabilityScore", display_name: "TU Contactability Score", description: "The contactability score for the lead based on TransUnion data", data_type: "STRING", category: "TransUnion", dimension: true, filter: true },
  { column_name: "TransUnionDNData_TUPhonelinkage", display_name: "TU Phone Linkage", description: "The phone linkage score for the lead based on TransUnion data", data_type: "FLOAT", category: "TransUnion", dimension: true, filter: true },
  { column_name: "TransUnionDNData_TULtvScore", display_name: "TU LTV Score", description: "The LTV score for the lead based on TransUnion data", data_type: "FLOAT", category: "TransUnion", dimension: false, filter: false },
  { column_name: "TransUnionDNData_TULTVdecile", display_name: "TU LTV Decile", description: "The LTV decile for the lead based on TransUnion data", data_type: "FLOAT", category: "TransUnion", dimension: true, filter: true },
  { column_name: "TransUnionDNData_VerificationScore", display_name: "TU Verification Score", description: "The verification score for the lead based on TransUnion data", data_type: "STRING", category: "TransUnion", dimension: true, filter: true },

  // ── Active Prospect ──────────────────────────────────────────────
  { column_name: "ActiveProspectValidationData_Domain", display_name: "AP Domain", description: "The domain of the webpage the lead visited and provided details", data_type: "STRING", category: "Active Prospect", dimension: true, filter: true },
  { column_name: "ActiveProspectValidationData_AgeSeconds", display_name: "AP Age (Seconds)", description: "How many seconds ago the lead filled in the form. Important to understand if the lead is fresh or offered for several hours", data_type: "INTEGER", category: "Active Prospect", dimension: false, filter: false },
  { column_name: "ActiveProspectValidationData_FormDuration", display_name: "AP Form Duration", description: "How long it took the lead to complete the form (30-90 sec is ideal; <10 sec can be a bot; >5 min may lack attention)", data_type: "INTEGER", category: "Active Prospect", dimension: false, filter: false },
  { column_name: "ActiveProspectValidationData_APCertificationStatus", display_name: "AP Certification Status", description: "The status of the certificate, whether the lead meets TCPA rules or not", data_type: "STRING", category: "Active Prospect", dimension: true, filter: true },
  { column_name: "ActiveProspectValidationData_Ip", display_name: "AP IP", description: "The IP of the user as captured by Active Prospect. Sometimes different from what the partner reported", data_type: "STRING", category: "Active Prospect", dimension: false, filter: false },
  { column_name: "TrustedFormCertificateClaimed", display_name: "TF Certificate Claimed", description: "Whether the TrustedForm certificate was claimed", data_type: "BOOLEAN", category: "Active Prospect", dimension: false, filter: false },
  { column_name: "Data_TrustedFormCertificateClaimed", display_name: "AP Certificate Claimed", description: "Whether the Active Prospect certificate was claimed", data_type: "BOOLEAN", category: "Active Prospect", dimension: false, filter: false },
  { column_name: "TrustedFormId", display_name: "AP Form ID", description: "The Active Prospect form ID", data_type: "STRING", category: "Active Prospect", dimension: false, filter: true },

  // ── Jornaya Details ──────────────────────────────────────────────
  { column_name: "JornayaLeadId", display_name: "Jornaya Lead ID", description: "The ID in Jornaya that represents the experience the lead had", data_type: "STRING", category: "Jornaya Details", dimension: false, filter: true },
  { column_name: "JornayaValidationData_AuthenticationStatus", display_name: "Jornaya Authentication Status", description: "Whether the form is authentic or not", data_type: "STRING", category: "Jornaya Details", dimension: true, filter: true },
  { column_name: "JornayaValidationData_Consent", display_name: "Jornaya Consent", description: "Whether the user provided the expected consent", data_type: "STRING", category: "Jornaya Details", dimension: true, filter: true },
  { column_name: "JornayaValidationData_DataIntegrity", display_name: "Jornaya Data Integrity", description: "Whether the lead data matches the details in Jornaya (first name, last name, email, phone, address)", data_type: "STRING", category: "Jornaya Details", dimension: true, filter: true },
  { column_name: "JornayaValidationData_VisibilityLevel", display_name: "Jornaya Visibility Level", description: "The visibility level of the consent language on the partner form", data_type: "STRING", category: "Jornaya Details", dimension: true, filter: true },
  { column_name: "JornayaValidationData_Disclosure", display_name: "Jornaya Disclosure", description: "Whether the disclosure was presented and how on the partner form", data_type: "STRING", category: "Jornaya Details", dimension: true, filter: true },
  { column_name: "JornayaValidationData_Stored", display_name: "Jornaya Stored", description: "Whether the form was stored in the Jornaya platform", data_type: "STRING", category: "Jornaya Details", dimension: true, filter: true },
  { column_name: "JornayaValidationData_LeadAge", display_name: "Jornaya Lead Age", description: "Time lapse from when the lead filled in the form until it was offered. Indicates if the lead is fresh or in the market for a few hours", data_type: "STRING", category: "Jornaya Details", dimension: true, filter: true },
  { column_name: "JornayaValidationData_LeadDuration", display_name: "Jornaya Lead Duration", description: "How long it took the lead to complete the form (30-90 sec is ideal; <10 sec can be a bot; >5 min may lack attention)", data_type: "FLOAT", category: "Jornaya Details", dimension: true, filter: true },
  { column_name: "JornayaValidationData_RiskFlagSummary", display_name: "Jornaya Risk Flag Summary", description: "The risk flag summary by Jornaya for this lead", data_type: "STRING", category: "Jornaya Details", dimension: true, filter: true },
  { column_name: "JornayaValidationData_LinkageSummary", display_name: "Jornaya Linkage Summary", description: "The linkage summary by Jornaya for this lead", data_type: "STRING", category: "Jornaya Details", dimension: true, filter: true },
  { column_name: "JornayaValidationData_IDVerifyScore", display_name: "Jornaya ID Verify Score", description: "The ID verification score by Jornaya for this lead", data_type: "INTEGER", category: "Jornaya Details", dimension: true, filter: true },
  { column_name: "JornayaValidationData_ValidationSummary", display_name: "Jornaya Validation Summary", description: "The validation summary by Jornaya for this lead", data_type: "STRING", category: "Jornaya Details", dimension: true, filter: true },

  // ── Performance Data ─────────────────────────────────────────────
  { column_name: "Conv_MC", display_name: "Conv MultiCar", description: "The number of vehicles captured in the quote details", data_type: "STRING", category: "Performance Data", dimension: false, filter: false },
  { column_name: "Conv_Homeonership", display_name: "Conv Homeownership", description: "The homeownership reported in the quote details", data_type: "STRING", category: "Performance Data", dimension: false, filter: false },
  { column_name: "Conv_segement", display_name: "Conv Segment", description: "The segment reported in the quote details", data_type: "STRING", category: "Performance Data", dimension: false, filter: false },
  { column_name: "CallCount", display_name: "Call Count", description: "Number of calls, relevant for leads and calls", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "NumofCalls", display_name: "Num of Calls", description: "Number of calls, relevant for leads and calls", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "TotalCalls", display_name: "Total Calls", description: "Number of calls, relevant for leads and calls", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "TalkTime", display_name: "Talk Time", description: "The talk time in the contact center (seconds), relevant for leads and calls", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "AutoQuotes", display_name: "Auto Quotes", description: "The number of quotes for auto insurance", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "TenantQuotes", display_name: "Tenant Quotes", description: "The number of quotes for tenant insurance", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "CondoQuotes", display_name: "Condo Quotes", description: "The number of quotes for condo insurance", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "HomeQuotes", display_name: "Home Quotes", description: "The number of quotes for home insurance", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "AutoOnlineQuotesStart", display_name: "Quotes Started", description: "The number of online quotes that started. Relevant for click activity type", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "TotalQuotes", display_name: "Total Quotes", description: "The total number of quotes", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "AutoBinds", display_name: "Auto Binds", description: "The number of binds for auto insurance", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "CondoBinds", display_name: "Condo Binds", description: "The number of binds for condo insurance", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "TenantBinds", display_name: "Tenant Binds", description: "The number of binds for tenant insurance", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "HomeBinds", display_name: "Home Binds", description: "The number of binds for home insurance", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "OtherBinds", display_name: "Other Binds", description: "The number of binds for other insurance", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "AutoOnlineBinds", display_name: "Online Binds", description: "The number of online binds completed. Relevant for click activity type", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "TotalBinds", display_name: "Total Binds", description: "The total number of binds", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "AutoRejects", display_name: "Auto Rejects", description: "The number of rejected binds for auto insurance", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "TenantRejects", display_name: "Tenant Rejects", description: "The number of rejected binds for tenant insurance", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "CondoRejects", display_name: "Condo Rejects", description: "The number of rejected binds for condo insurance", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "HomeRejects", display_name: "Home Rejects", description: "The number of rejected binds for home insurance", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "ScoredPolicies", display_name: "Scored Policies", description: "Out of the binds, how many are considered scored policies for LTV calculations", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "ScCor", display_name: "ScCor", description: "Out of the binds, how many are considered scored policies for combined ratio calculations", data_type: "INTEGER", category: "Performance Data", dimension: false, filter: false },
  { column_name: "Target_TargetCPB_original", display_name: "Origin Target CPB", description: "The original target CPB captured on the bind when it was added to Beacon", data_type: "FLOAT", category: "Performance Data", dimension: false, filter: false },
  { column_name: "Target_TargetCPB", display_name: "Target CPB", description: "The updated target CPB; when users update targets in the platform it gets updated in BQ as well", data_type: "FLOAT", category: "Performance Data", dimension: false, filter: false },
  { column_name: "CustomValues_Mrltv", display_name: "MRLTV", description: "The MRLTV reported, available if scored policies > 0. Represents the margin from a marketing perspective", data_type: "FLOAT", category: "Performance Data", dimension: false, filter: false },
  { column_name: "CustomValues_Profit", display_name: "Profit", description: "The profit reported, available if scored policies > 0. Represents the profit from the lead", data_type: "FLOAT", category: "Performance Data", dimension: false, filter: false },
  { column_name: "Equity", display_name: "Equity", description: "The equity reported, available if scored policies > 0. Represents the amount from the policy that can be invested", data_type: "FLOAT", category: "Performance Data", dimension: false, filter: false },
  { column_name: "CustomValues_Premium", display_name: "Premium", description: "The premium reported, available if scored policies > 0. Represents the total premium charged from the lead", data_type: "FLOAT", category: "Performance Data", dimension: false, filter: false },
  { column_name: "ClickLossAdj", display_name: "Click Loss Adj", description: "A factor used for Auto vs Home leads, available if scored policies > 0", data_type: "FLOAT", category: "Performance Data", dimension: false, filter: false },
  { column_name: "LifetimePremium", display_name: "Lifetime Premium", description: "The modeled lifetime premium, available if scored policies > 0. The total expected amount from that lead", data_type: "FLOAT", category: "Performance Data", dimension: false, filter: false },
  { column_name: "LifeTimeCost", display_name: "Lifetime Cost", description: "The modeled lifetime cost, available if scored policies > 0. The expected claims from that lead", data_type: "FLOAT", category: "Performance Data", dimension: false, filter: false },
  { column_name: "CreditScore", display_name: "Conv Credit Score", description: "The credit score of the lead calculated during the quote process", data_type: "STRING", category: "Performance Data", dimension: false, filter: false },
  { column_name: "BillingFrequency", display_name: "Conv Billing Frequency", description: "The billing frequency agreed for this bind", data_type: "STRING", category: "Performance Data", dimension: false, filter: false },
  { column_name: "BillingMethod", display_name: "Conv Billing Method", description: "The billing method agreed for this bind", data_type: "STRING", category: "Performance Data", dimension: false, filter: false },

  // ── Repetition Data ──────────────────────────────────────────────
  { column_name: "NumofLeadsByJornaya", display_name: "Leads by Jornaya ID", description: "Number of leads that were offered and got a bid with the same Jornaya ID", data_type: "INTEGER", category: "Repetition Data", dimension: true, filter: true },
  { column_name: "NumofCompByJornaya", display_name: "Partners by Jornaya ID", description: "Number of partners that offered this lead with the same Jornaya ID", data_type: "INTEGER", category: "Repetition Data", dimension: true, filter: true },
  { column_name: "NumofTacticsByJornaya", display_name: "Tactics by Jornaya ID", description: "Number of activities that offered this lead with the same Jornaya ID", data_type: "INTEGER", category: "Repetition Data", dimension: true, filter: true },
  { column_name: "NumofLeadsByShaPhone", display_name: "Leads by Sha256 Phone", description: "Number of leads that were offered and got a bid with the same SHA-256 phone", data_type: "INTEGER", category: "Repetition Data", dimension: true, filter: true },
  { column_name: "NumofCompByShaphone", display_name: "Partners by Sha256 Phone", description: "Number of partners that offered this lead with the same SHA-256 phone", data_type: "INTEGER", category: "Repetition Data", dimension: true, filter: true },
  { column_name: "NumofTacticsByShaphone", display_name: "Tactics by Sha256 Phone", description: "Number of activities that offered this lead with the same SHA-256 phone", data_type: "INTEGER", category: "Repetition Data", dimension: true, filter: true },
  { column_name: "NumofLeadsByShaEmail", display_name: "Leads by Sha256 Email", description: "Number of leads that were offered and got a bid with the same SHA-256 email", data_type: "INTEGER", category: "Repetition Data", dimension: true, filter: true },
  { column_name: "NumofCompByShaemail", display_name: "Partners by Sha256 Email", description: "Number of partners that offered this lead with the same SHA-256 email", data_type: "INTEGER", category: "Repetition Data", dimension: true, filter: true },
  { column_name: "NumofTacticsByShaemail", display_name: "Tactics by Sha256 Email", description: "Number of activities that offered this lead with the same SHA-256 email", data_type: "INTEGER", category: "Repetition Data", dimension: true, filter: true },
  { column_name: "NumofSoldByShaPhone", display_name: "Sold by Sha256 Phone", description: "Number of times this lead was acquired based on SHA-256 phone", data_type: "INTEGER", category: "Repetition Data", dimension: true, filter: true },
  { column_name: "NumofSoldCompByShaphone", display_name: "Sold Partners by Sha256 Phone", description: "Number of partners this lead was acquired from based on SHA-256 phone", data_type: "INTEGER", category: "Repetition Data", dimension: true, filter: true },
  { column_name: "NumofSoldTacticsByShaphone", display_name: "Sold Tactics by Sha256 Phone", description: "Number of activity types this lead was acquired from based on SHA-256 phone", data_type: "INTEGER", category: "Repetition Data", dimension: true, filter: true },
  { column_name: "NumofSoldByShaEmail", display_name: "Sold by Sha256 Email", description: "Number of times this lead was acquired based on SHA-256 email", data_type: "INTEGER", category: "Repetition Data", dimension: true, filter: true },
  { column_name: "NumofSoldCompByShaemail", display_name: "Sold Partners by Sha256 Email", description: "Number of partners this lead was acquired from based on SHA-256 email", data_type: "INTEGER", category: "Repetition Data", dimension: true, filter: true },
  { column_name: "NumofSoldTacticsByShaemail", display_name: "Sold Tactics by Sha256 Email", description: "Number of activity types this lead was acquired from based on SHA-256 email", data_type: "INTEGER", category: "Repetition Data", dimension: true, filter: true },
  { column_name: "UA_IP_Key", display_name: "UA + IP Key", description: "A key composed by user agent and user IP", data_type: "STRING", category: "Repetition Data", dimension: false, filter: false },
  { column_name: "NumofLeadsByUA_IP_Key", display_name: "Leads by UA+IP Key", description: "Number of leads that were offered and got a bid with the same UA+IP key", data_type: "INTEGER", category: "Repetition Data", dimension: true, filter: true },
  { column_name: "SoldClickKey", display_name: "UA+IP+Zip+Year+Make Key", description: "A key composed by user agent, user IP, zip, vehicle year, and make", data_type: "STRING", category: "Repetition Data", dimension: false, filter: false },
];

/** Lookup map: column_name → ColumnMeta */
export const COLUMN_MAP = new Map(REPORT_COLUMNS.map((c) => [c.column_name, c]));

/** All unique categories in display order */
export const CATEGORIES = [...new Set(REPORT_COLUMNS.map((c) => c.category))];
