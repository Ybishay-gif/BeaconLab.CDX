async (page) => {
  const baseUrl = __BASE_URL__;
  const adminCode = String(__ADMIN_ACCESS_CODE__).trim();
  const timeoutMs = Number(__PW_TIMEOUT_MS__);
  const artifactDir = __PW_ARTIFACT_DIR__;
  const planNamePrefix = __PLAN_NAME_PREFIX__;
  const planDescription = __PLAN_DESCRIPTION__;

  const shot = async (name, selector = null) => {
    if (selector) {
      await page.locator(selector).first().screenshot({ path: `${artifactDir}/${name}.png` });
      return;
    }
    await page.screenshot({ path: `${artifactDir}/${name}.png`, fullPage: true });
  };

  const waitForAppVisible = async () => {
    await page.waitForFunction(
      () => {
        const app = document.querySelector("#appLayout");
        return Boolean(app && !app.hasAttribute("hidden"));
      },
      {},
      { timeout: timeoutMs }
    );
  };

  const waitForStatusContains = async (selector, text) => {
    await page.waitForFunction(
      ({ sel, t }) => {
        const node = document.querySelector(sel);
        return String(node?.textContent || "").includes(t);
      },
      { sel: selector, t: text },
      { timeout: timeoutMs }
    );
  };

  const waitForNoLoading = async (selector) => {
    await page.waitForFunction(
      (sel) => {
        const node = document.querySelector(sel);
        if (!node) return true;
        return node.hasAttribute("hidden");
      },
      selector,
      { timeout: timeoutMs }
    );
  };

  await page.goto(baseUrl, { waitUntil: "domcontentloaded", timeout: timeoutMs });
  await page.setViewportSize({ width: 1600, height: 1000 });
  await shot("01_login_screen");

  const appVisible = await page
    .locator("#appLayout")
    .evaluate((el) => !el.hasAttribute("hidden"))
    .catch(() => false);

  if (!appVisible) {
    if (!adminCode) {
      throw new Error("ADMIN_ACCESS_CODE is required when not already logged in.");
    }
    await page.fill("#adminAccessCode", adminCode);
    await shot("02_login_filled");
    await Promise.all([waitForAppVisible(), page.click("#adminLoginBtn")]);
  }
  await shot("03_plan_builder_home");

  const now = new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14);
  const planName = `${planNamePrefix} ${now}`;
  await page.fill("#planName", planName);
  await page.fill("#planDesc", planDescription);
  await shot("04_plan_definition");

  await page.click("#createPlan");
  await waitForStatusContains("#createStatus", "Created plan:");
  await shot("05_plan_created");

  const planId = await page.$eval("#selectedPlanId", (el) => String(el.value || "").trim());
  if (!planId) throw new Error("Plan ID missing after creation.");

  await page.fill("#paramKey", "target_budget");
  await page.fill("#paramValue", "250000");
  await page.selectOption("#paramType", "int");
  await page.click("#saveParameter");
  await waitForStatusContains("#actionStatus", "Parameter saved");
  await shot("06_plan_components_parameter");

  await page.fill("#decisionType", "bid_adjustment");
  await page.fill("#decisionValue", "0.15");
  await page.fill("#decisionState", "CA");
  await page.fill("#decisionChannel", "MCH");
  await page.click("#addDecision");
  await waitForStatusContains("#actionStatus", "Decision added");
  await shot("07_plan_components_decision");

  await page.click("#planTabStrategy");
  await page.waitForSelector("#planStrategyPanel.tab-panel.active", { timeout: timeoutMs });
  await shot("08_strategy_panel");

  await page.click("#addPlanStrategyRule");
  await page.waitForSelector("#planStrategyRulesBody tr td input[type='text']", { timeout: timeoutMs });
  await page.fill("#planStrategyRulesBody tr td input[type='text']", "Growth Push");
  await page.waitForFunction(
    () => document.querySelectorAll("#planStrategyRulesBody tr td:nth-child(2) label.dropdown-option").length > 1,
    {},
    { timeout: timeoutMs }
  );
  await page.waitForFunction(
    () => document.querySelectorAll("#planStrategyRulesBody tr td:nth-child(3) label.dropdown-option").length > 0,
    {},
    { timeout: timeoutMs }
  );

  // Pick first available state (excluding Select All), and preferred segment MCH if available.
  const pickerStatus = await page.evaluate(() => {
    const rows = document.querySelectorAll("#planStrategyRulesBody tr");
    if (!rows.length) return { ok: false, reason: "no_row" };
    const row = rows[0];
    const stateLabels = Array.from(row.querySelectorAll("td:nth-child(2) label.dropdown-option"));
    const stateOption = stateLabels.find((label) => {
      const txt = String(label.textContent || "").trim().toUpperCase();
      return txt && txt !== "SELECT ALL";
    });
    const stateCheckbox = stateOption?.querySelector("input[type='checkbox']");
    if (stateCheckbox && !stateCheckbox.checked) {
      stateCheckbox.click();
      stateCheckbox.dispatchEvent(new Event("change", { bubbles: true }));
    }

    const segmentLabels = Array.from(row.querySelectorAll("td:nth-child(3) label.dropdown-option"));
    let segmentOption = segmentLabels.find((label) => String(label.textContent || "").trim().toUpperCase() === "MCH");
    if (!segmentOption) {
      segmentOption = segmentLabels.find((label) => String(label.textContent || "").trim());
    }
    const segmentCheckbox = segmentOption?.querySelector("input[type='checkbox']");
    if (segmentCheckbox && !segmentCheckbox.checked) {
      segmentCheckbox.click();
      segmentCheckbox.dispatchEvent(new Event("change", { bubbles: true }));
    }

    const statePicked = String(row.querySelector("td:nth-child(2) input.rule-picker-selected")?.value || "").trim();
    const segmentPicked = String(row.querySelector("td:nth-child(3) input.rule-picker-selected")?.value || "").trim();
    return { ok: Boolean(statePicked && segmentPicked), statePicked, segmentPicked };
  });
  if (!pickerStatus?.ok) {
    throw new Error(`Could not pick strategy state/segment: ${JSON.stringify(pickerStatus)}`);
  }
  await shot("09_strategy_rule_configured");

  await page.click("#planStrategyRulesBody button[title='Save Rule']");
  await page.waitForFunction(
    () => !document.querySelector("#planStrategyRulesBody tr td input[type='text']"),
    {},
    { timeout: timeoutMs }
  );
  await shot("10_strategy_rule_saved");

  await page.fill("#planStrategySettingsBody tr:first-child td:nth-child(4) input", "18");
  await page.fill("#planStrategySettingsBody tr:first-child td:nth-child(5) input", "12");
  await page.fill("#planStrategySettingsBody tr:first-child td:nth-child(6) input", "72");
  await page.selectOption("#planStrategySettingsBody tr:first-child td:nth-child(7) select", "growth");
  await shot("11_strategy_cor_target_set");

  await page.click("#savePlanStrategyBtn");
  await waitForStatusContains("#planStrategyStatus", "Saved");
  await shot("12_strategy_saved");

  await page.click("#planTabPriceDecision");
  await page.waitForSelector("#priceDecisionPanel.tab-panel.active", { timeout: timeoutMs });
  await page.click("#applyPriceDecisionFilters");
  await waitForNoLoading("#priceDecisionLoading");
  await shot("13_price_decision_overview");

  await page.click("#analyticsSectionBtn");
  await page.waitForSelector(".section-content.active[data-section-panel='analytics']", { timeout: timeoutMs });

  await page.click("#analyticsTabPriceExploration");
  await page.waitForSelector("#priceExplorationPanel.tab-panel.active", { timeout: timeoutMs });
  await page.click("#applyPriceExplorationFilters");
  await waitForNoLoading("#priceExplorationLoading");
  await shot("14_price_exploration_table");

  await page.click("#analyticsTabStatePlanAnalysis");
  await page.waitForSelector("#statePlanAnalysisPanel.tab-panel.active", { timeout: timeoutMs });
  await page.click("#applyStatePlanAnalysisFilters");
  await waitForNoLoading("#statePlanAnalysisLoading");
  await page.waitForSelector("#statePlanAnalysisMapSvg path", { timeout: timeoutMs });
  await shot("15_state_plan_analysis_map");

  const firstPath = page.locator("#statePlanAnalysisMapSvg path").first();
  await firstPath.hover();
  await page.waitForFunction(() => {
    const tooltip = document.querySelector("#statePlanAnalysisMapTooltip");
    return Boolean(tooltip && !tooltip.hasAttribute("hidden") && String(tooltip.textContent || "").trim().length > 0);
  }, {}, { timeout: timeoutMs });
  await shot("16_state_tooltip_full");
  await shot("17_state_tooltip_zoom", "#statePlanAnalysisMapTooltip");

  await page.click("#planSectionBtn");
  await page.waitForSelector(".section-content.active[data-section-panel='plan']", { timeout: timeoutMs });
  await page.click("#planTabBuilder");
  await page.waitForSelector("#planBuilderPanel.tab-panel.active", { timeout: timeoutMs });
  await page.waitForSelector("#runPlan", { timeout: timeoutMs });
  await page.click("#runPlan");
  await waitForStatusContains("#actionStatus", "Run queued:");
  await shot("18_plan_run_queued");

  const actionStatus = await page.$eval("#actionStatus", (el) => String(el.textContent || "").trim());
  const runId = actionStatus.includes("Run queued:") ? actionStatus.split("Run queued:")[1].trim() : "";

  return {
    ok: true,
    baseUrl,
    planName,
    planId,
    runId,
    artifactDir,
    capturedAt: new Date().toISOString()
  };
}
