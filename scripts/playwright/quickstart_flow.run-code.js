async (page) => {
  const baseUrl = __BASE_URL__;
  const adminCode = String(__ADMIN_ACCESS_CODE__).trim();
  const timeoutMs = Number(__PW_TIMEOUT_MS__);
  const artifactDir = __PW_ARTIFACT_DIR__;
  const planNamePrefix = __PLAN_NAME_PREFIX__;
  const planDescription = __PLAN_DESCRIPTION__;

  const waitForText = async (selector, expectedText) => {
    await page.waitForFunction(
      ({ sel, text }) => {
        const node = document.querySelector(sel);
        const content = String(node?.textContent || "").trim();
        return content.includes(text);
      },
      { sel: selector, text: expectedText },
      { timeout: timeoutMs }
    );
  };

  const isAppVisible = async () =>
    page
      .locator("#appLayout")
      .evaluate((el) => !el.hasAttribute("hidden"))
      .catch(() => false);

  await page.goto(baseUrl, { waitUntil: "domcontentloaded", timeout: timeoutMs });
  await page.setViewportSize({ width: 1440, height: 900 });

  if (!(await isAppVisible())) {
    if (!adminCode) {
      throw new Error("ADMIN_ACCESS_CODE is required when not already logged in.");
    }
    await page.waitForSelector("#adminAccessCode", { timeout: timeoutMs });
    await page.fill("#adminAccessCode", adminCode);
    await Promise.all([
      page.waitForFunction(
        () => {
          const app = document.querySelector("#appLayout");
          return Boolean(app && !app.hasAttribute("hidden"));
        },
        {},
        { timeout: timeoutMs }
      ),
      page.click("#adminLoginBtn")
    ]);
  }

  await page.waitForSelector("#planBuilderPanel.tab-panel.active", { timeout: timeoutMs });

  const now = new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14);
  const planName = `${planNamePrefix} ${now}`;

  await page.fill("#planName", planName);
  await page.fill("#planDesc", planDescription);
  await page.click("#createPlan");
  await waitForText("#createStatus", "Created plan:");

  const planId = await page.$eval("#selectedPlanId", (el) => String(el.value || "").trim());
  if (!planId) {
    throw new Error("Plan ID was not set after plan creation.");
  }

  await page.fill("#paramKey", "target_budget");
  await page.fill("#paramValue", "100000");
  await page.selectOption("#paramType", "int");
  await page.click("#saveParameter");
  await waitForText("#actionStatus", "Parameter saved");

  await page.fill("#decisionType", "bid_adjustment");
  await page.fill("#decisionValue", "0.1");
  await page.fill("#decisionState", "CA");
  await page.fill("#decisionChannel", "MCH");
  await page.click("#addDecision");
  await waitForText("#actionStatus", "Decision added");

  await page.click("#runPlan");
  await waitForText("#actionStatus", "Run queued:");

  const createStatus = await page.$eval("#createStatus", (el) => String(el.textContent || "").trim());
  const actionStatus = await page.$eval("#actionStatus", (el) => String(el.textContent || "").trim());
  const runId = actionStatus.includes("Run queued:") ? actionStatus.split("Run queued:")[1].trim() : "";

  await page.screenshot({ path: `${artifactDir}/final.png`, fullPage: true });

  return {
    ok: true,
    baseUrl,
    planName,
    planId,
    runId,
    createStatus,
    actionStatus,
    artifactDir,
    capturedAt: new Date().toISOString()
  };
}
