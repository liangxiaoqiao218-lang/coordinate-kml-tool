import fs from "node:fs/promises";
import { test, expect } from "./fixtures.mjs";

const cases = [
  {
    name: "Point",
    input: "-11.178174, 12.319572",
    element: "<Point>",
    coordinate: "-11.178174,12.319572,0"
  },
  {
    name: "LineString",
    input: "-11.178174, 12.319572\n-11.278174, 12.419572",
    element: "<LineString>",
    coordinate: "-11.178174,12.319572,0"
  },
  {
    name: "Polygon",
    input: "-11.178174, 12.319572\n-11.278174, 12.419572\n-11.378174, 12.219572",
    element: "<Polygon>",
    coordinate: "-11.178174,12.319572,0"
  }
];

test("locked WGS84 text fixture is parsed through the local regression API", async ({ request }) => {
  const response = await request.post("/api/regression/parse-coordinate-text", {
    headers: { "x-regression-test": "true" },
    data: { text: "12.319572, -11.178174" }
  });
  expect(response.ok()).toBeTruthy();

  const payload = await response.json();
  expect(payload.success).toBe(true);
  expect(payload.coordinates).toContain("-11.178174,12.319572");
  expect(payload.precisionMode).toBe("wgs84-chat-coordinates");
});

for (const fixture of cases) {
  test(`${fixture.name} manual input downloads a locally finalized KML`, async ({ page, diagnostics }, testInfo) => {
    await page.goto("/coordinate", { waitUntil: "networkidle" });
    await page.locator("#coordinateInput").fill(fixture.input);

    const kmlButton = page.locator("#coordinateKmlAction");
    await expect(kmlButton).toBeEnabled();
    await expect(kmlButton).toHaveAttribute("data-state", "enabled");

    const finalizerPromise = page.waitForResponse(
      (response) => response.url().endsWith("/api/coordinate-manual-finalize") && response.request().method() === "POST"
    );
    const downloadPromise = page.waitForEvent("download", { timeout: 15_000 }).catch(() => null);
    await kmlButton.click();
    const finalizerResponse = await finalizerPromise;
    const finalizerBody = await finalizerResponse.text();

    if (!finalizerResponse.ok()) {
      await testInfo.attach("manual-finalizer-response.txt", {
        body: Buffer.from(finalizerBody),
        contentType: "text/plain"
      });
    }

    expect(
      finalizerResponse.ok(),
      `manual finalizer returned ${finalizerResponse.status()}: ${finalizerBody}`
    ).toBeTruthy();

    const download = await downloadPromise;
    expect(download, "KML download did not start after a successful finalizer response").not.toBeNull();
    const kml = await fs.readFile(await download.path(), "utf8");

    expect(download.suggestedFilename()).toBe("coordinates.kml");
    expect(kml).toContain("<kml xmlns=\"http://www.opengis.net/kml/2.2\">");
    expect(kml).toContain(fixture.element);
    expect(kml).toContain(fixture.coordinate);
    expect(diagnostics.blockedRequests).toEqual([]);
    expect(diagnostics.pageErrors).toEqual([]);
  });
}

test("empty input keeps KML and map actions fail-closed", async ({ page, diagnostics }) => {
  await page.goto("/coordinate", { waitUntil: "networkidle" });

  await expect(page.locator("#coordinateKmlAction")).toBeDisabled();
  await expect(page.locator("#coordinateKmlAction")).toHaveAttribute("data-state", "blocked");
  await expect(page.locator("#mapPreviewAction")).toBeDisabled();
  expect(diagnostics.blockedRequests).toEqual([]);
  expect(diagnostics.pageErrors).toEqual([]);
});
