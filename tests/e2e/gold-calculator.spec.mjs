import { test, expect } from "./fixtures.mjs";

test("gold calculator produces deterministic local results without a price provider", async ({ page, diagnostics }) => {
  await page.goto("/gold", { waitUntil: "networkidle" });
  await page.locator("#goldWeight").fill("19.32");
  await page.locator("#waterDiff").fill("1");

  await expect(page.locator("#goldDensityResult")).toHaveText("19.320");
  await expect(page.locator("#goldPurityResult")).toHaveText("100.00%");
  await expect(page.locator("#goldKResult")).toHaveText("24.00K");
  await expect(page.locator("#goldAuResult")).toHaveText("Au1000");
  expect(diagnostics.blockedRequests).toEqual([]);
  expect(diagnostics.pageErrors).toEqual([]);
});

test("invalid float-weight input is rejected in the UI", async ({ page, diagnostics }) => {
  await page.goto("/gold", { waitUntil: "networkidle" });
  await page.locator("#waterModeFloat").click();
  await page.locator("#goldWeight").fill("10");
  await page.locator("#waterDiff").fill("10");

  await expect(page.locator("#goldGradeResult")).toContainText("水中浮重必须小于黄金重量");
  expect(diagnostics.blockedRequests).toEqual([]);
  expect(diagnostics.pageErrors).toEqual([]);
});
