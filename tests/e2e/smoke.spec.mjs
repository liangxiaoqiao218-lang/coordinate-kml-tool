import { test, expect } from "./fixtures.mjs";

const baselineCommit = "21c2ab031abdbede6ee339ad1d3a4956eee8b017";

test("runtime identity is explicit and regression mode is local", async ({ request }) => {
  const response = await request.get("/api/version");
  expect(response.ok()).toBeTruthy();

  const payload = await response.json();
  expect(payload.brand).toBe("GeoKit Lab");
  expect(payload.runtimeIdentity).toMatchObject({
    commit: baselineCommit,
    branch: "codex/playwright-acceptance-gate",
    regressionTestMode: true,
    workingTreeDirty: true
  });
});

for (const route of [
  { path: "/", selector: "#homePage" },
  { path: "/coordinate", selector: "#coordinatePage" },
  { path: "/judge", selector: "#judgePage" },
  { path: "/gold", selector: "#goldPage" }
]) {
  test(`${route.path} renders its intended application view`, async ({ page, diagnostics }) => {
    const response = await page.goto(route.path, { waitUntil: "networkidle" });
    expect(response?.ok()).toBeTruthy();
    await expect(page.locator(route.selector)).toBeVisible();
    expect(diagnostics.blockedRequests).toEqual([]);
    expect(diagnostics.pageErrors).toEqual([]);
  });
}

test("desktop and mobile layouts remain inside the viewport", async ({ page, diagnostics }, testInfo) => {
  for (const viewport of [
    { name: "desktop", width: 1440, height: 1000 },
    { name: "mobile", width: 390, height: 844 }
  ]) {
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await page.goto("/coordinate", { waitUntil: "networkidle" });
    await expect(page.locator("#coordinatePage")).toBeVisible();

    const overflow = await page.evaluate(() => ({
      body: document.body.scrollWidth - window.innerWidth,
      root: document.documentElement.scrollWidth - window.innerWidth
    }));
    expect(Math.max(overflow.body, overflow.root)).toBeLessThanOrEqual(1);

    await testInfo.attach(`${viewport.name}-coordinate.png`, {
      body: await page.screenshot({ fullPage: true }),
      contentType: "image/png"
    });
  }

  expect(diagnostics.blockedRequests).toEqual([]);
  expect(diagnostics.pageErrors).toEqual([]);
});

test("network guard blocks a non-local request before it leaves Chromium", async ({ page, diagnostics }) => {
  await page.goto("/", { waitUntil: "networkidle" });
  const result = await page.evaluate(async () => {
    try {
      await fetch("https://network-guard.invalid/proof");
      return "unexpected-success";
    } catch {
      return "blocked";
    }
  });

  expect(result).toBe("blocked");
  expect(diagnostics.blockedRequests).toEqual([
    expect.objectContaining({
      method: "GET",
      url: "https://network-guard.invalid/proof"
    })
  ]);
});
