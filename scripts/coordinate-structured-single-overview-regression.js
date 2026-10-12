#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const scriptDirectory = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(scriptDirectory, "..");
const serverSource = fs.readFileSync(path.join(root, "server.js"), "utf8");
const existingCandidateRegressionSource = fs.readFileSync(path.join(
  root,
  "scripts/coordinate-core-single-charge-candidate-regression.js"
), "utf8");

async function main() {
  const adapter = await import(pathToFileURL(path.join(
    root,
    "server/recognition/qwen38-coordinate-structured-adapter.mjs"
  )).href);
  const oneOverviewItem = {
    type: "image_url",
    image_url: { url: "data:image/png;base64,AA==" }
  };
  const cases = [];

  const single = adapter.buildCoordinateStructuredCall({
    imageItems: [oneOverviewItem],
    stageName: "indonesia_structured_b"
  });
  assert.equal(single.imageItems.length, 1);
  cases.push("adapter_accepts_exactly_one_image");

  for (const imageItems of [[], [oneOverviewItem, oneOverviewItem]]) {
    assert.throws(
      () => adapter.buildCoordinateStructuredCall({
        imageItems,
        stageName: "indonesia_structured_b"
      }),
      error => error?.code === "E_EXACTLY_ONE_OVERVIEW_IMAGE"
    );
  }
  cases.push("adapter_rejects_zero_or_multiple_images");

  const routeStart = serverSource.indexOf("const structuredOverviewImages = recognitionImageAcquisition.images.filter(");
  const providerCall = serverSource.indexOf("structuredProviderResponse = await callAliyunVision", routeStart);
  assert.ok(routeStart >= 0 && providerCall > routeStart);
  const structuredBlock = serverSource.slice(routeStart, providerCall + 600);
  assert.match(structuredBlock, /image\?\.role === "overview"/u);
  assert.match(structuredBlock, /structuredOverviewImages\.length !== 1/u);
  assert.match(structuredBlock, /STRUCTURED_PRODUCT_EXACTLY_ONE_OVERVIEW_REQUIRED/u);
  assert.match(structuredBlock, /imageItems: structuredImageItems/u);
  assert.doesNotMatch(structuredBlock, /role === "detail"|role === "crop"/u);
  cases.push("server_binds_structured_call_to_one_overview_before_provider");

  const genericRouteStart = serverSource.indexOf("const selectedProviderPrompt =", providerCall);
  assert.ok(genericRouteStart > providerCall);
  assert.match(serverSource.slice(genericRouteStart), /\n\s*imageItems,\r?\n/u);
  cases.push("ordinary_provider_path_retains_existing_image_items");

  assert.doesNotMatch(structuredBlock, /50S|32750|coordinateSystemExplicit/u);
  cases.push("overview_binding_does_not_inject_crs_evidence");

  assert.doesNotMatch(
    existingCandidateRegressionSource,
    /if \(indonesiaStructuredRoute\.status === INDONESIA_STRUCTURED_ROUTE_STATUS\.SELECTED\)/u
  );
  assert.match(existingCandidateRegressionSource, /serverSource\.lastIndexOf\("if \(\[", modelEvidenceStatusIndex\)/u);
  const oldSuiteLocator = serverSource.lastIndexOf(
    "if ([",
    serverSource.indexOf("INDONESIA_STRUCTURED_ROUTE_STATUS.MODEL_SOURCE_EVIDENCE_REQUIRED")
  );
  const oldSuiteEnd = serverSource.indexOf("const selectedProviderPrompt", oldSuiteLocator);
  assert.ok(oldSuiteLocator >= 0 && oldSuiteEnd > oldSuiteLocator);
  assert.equal(
    Array.from(serverSource.slice(oldSuiteLocator, oldSuiteEnd).matchAll(/callAliyunVision\(/gu)).length,
    1
  );
  cases.push("existing_candidate_assertion_tracks_multi_status_route_without_rerun");

  console.log(JSON.stringify({
    suite: "COORDINATE_STRUCTURED_SINGLE_OVERVIEW",
    passed: cases.length,
    total: cases.length,
    cases,
    providerCalls: 0,
    databaseWrites: 0,
    network: "OFFLINE_NO_NETWORK"
  }));
}

main().catch(error => {
  console.error(error?.stack || String(error));
  process.exitCode = 1;
});
