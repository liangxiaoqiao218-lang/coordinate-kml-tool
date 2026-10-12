import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  buildIndonesiaStructuredProductBridge,
  INDONESIA_STRUCTURED_PRODUCT_MODE,
  INDONESIA_STRUCTURED_PRODUCT_STATUS,
  INDONESIA_STRUCTURED_ROUTE_STATUS,
  selectIndonesiaStructuredProductRoute
} from "../server/recognition/indonesia-structured-product-bridge.js";
import { STRUCTURED_PROMPT } from "../server/recognition/qwen38-coordinate-structured-adapter.mjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const serverSource = fs.readFileSync(path.join(root, "server.js"), "utf8");
const cases = [];
const test = (name, action) => {
  action();
  cases.push(name);
};

const mode = INDONESIA_STRUCTURED_PRODUCT_MODE;
const route = sourceContextText => selectIndonesiaStructuredProductRoute({
  enabled: true,
  requestedMode: mode,
  sourceContextText
});

test("missing local OCR context requires same-call model source evidence", () => {
  const result = route("");
  assert.equal(result.status, INDONESIA_STRUCTURED_ROUTE_STATUS.MODEL_SOURCE_EVIDENCE_REQUIRED);
  assert.equal(result.reasonCode, "STRUCTURED_PRODUCT_MODEL_SOURCE_EVIDENCE_REQUIRED");
  assert.equal(result.evidence.explicitUtm50s, false);
  assert.equal(result.evidence.projectedColumns, false);
  assert.equal(result.evidence.sourceCrsConflict, false);
});

test("explicit local conflict still blocks before model evidence acquisition", () => {
  const result = route("UTM WGS 1984 ZONA 50 S EPSG:3440 X Y");
  assert.equal(result.status, INDONESIA_STRUCTURED_ROUTE_STATUS.BLOCKED);
  assert.equal(result.reasonCode, "STRUCTURED_PRODUCT_SOURCE_CRS_CONFLICT");
});

test("disabled or unselected product route cannot acquire model evidence", () => {
  assert.equal(selectIndonesiaStructuredProductRoute({
    enabled: false,
    requestedMode: mode,
    sourceContextText: ""
  }).status, INDONESIA_STRUCTURED_ROUTE_STATUS.BLOCKED);
  assert.equal(selectIndonesiaStructuredProductRoute({
    enabled: true,
    requestedMode: "",
    sourceContextText: ""
  }).status, INDONESIA_STRUCTURED_ROUTE_STATUS.NOT_SELECTED);
});

const validStructured = () => ({
  source: {
    title: "visible table",
    coordinateSystemExplicit: "UTM WGS 1984 ZONA 50S",
    datumExplicit: "WGS 1984",
    axisColumnsExplicit: ["X", "Y"],
    pointOrder: ["1", "2", "3"],
    points: [
      { id: "1", order: 1, xRaw: "778807,293", yRaw: "9721476,737", latitudeRaw: "2° 31' 2,805\" S", longitudeRaw: "119° 30' 25,820\" E" },
      { id: "2", order: 2, xRaw: "778981,768", yRaw: "9721477,288", latitudeRaw: "2° 31' 2,776\" S", longitudeRaw: "119° 30' 31,465\" E" },
      { id: "3", order: 3, xRaw: "778982,700", yRaw: "9721182,351", latitudeRaw: "2° 31' 12,373\" S", longitudeRaw: "119° 30' 31,513\" E" }
    ]
  },
  objects: [{
    objectId: "boundary",
    displayName: "",
    geometryType: "Polygon",
    outerRing: ["1", "2", "3"],
    innerRings: []
  }],
  unresolvedItems: []
});

test("post-model explicit CRS datum and axis columns remain review-required", () => {
  const result = buildIndonesiaStructuredProductBridge(validStructured());
  assert.equal(result.status, INDONESIA_STRUCTURED_PRODUCT_STATUS.ACCEPTED_REVIEW_REQUIRED, result.reasonCode);
  assert.equal(result.payload.requiresReview, true);
  assert.equal(result.payload.structuredProductEvidence.modelOutputIsAuthority, false);
  assert.deepEqual(result.payload.structuredProductEvidence.sourceAxisColumnsExplicit, ["X", "Y"]);
  const epsgCandidate = validStructured();
  epsgCandidate.source.coordinateSystemExplicit = "EPSG:32750";
  const epsgResult = buildIndonesiaStructuredProductBridge(epsgCandidate);
  assert.equal(epsgResult.status, INDONESIA_STRUCTURED_PRODUCT_STATUS.ACCEPTED_REVIEW_REQUIRED, epsgResult.reasonCode);
  assert.equal(epsgResult.payload.structuredProductEvidence.sourceCoordinateSystemExplicit, "EPSG:32750");
});

test("missing or conflicting model source evidence produces no geometry", () => {
  const mutations = [
    ["missing CRS", candidate => { candidate.source.coordinateSystemExplicit = null; }, "EXPLICIT_UTM50S_CRS_REQUIRED"],
    ["missing datum", candidate => { candidate.source.datumExplicit = null; }, "EXPLICIT_WGS84_DATUM_REQUIRED"],
    ["conflicting datum", candidate => { candidate.source.datumExplicit = "ITRF 2008"; }, "EXPLICIT_DATUM_CONFLICT"],
    ["missing axes", candidate => { candidate.source.axisColumnsExplicit = []; }, "EXPLICIT_PROJECTED_COLUMNS_REQUIRED"],
    ["conflicting axes", candidate => { candidate.source.axisColumnsExplicit = ["Y", "X"]; }, "EXPLICIT_PROJECTED_COLUMNS_CONFLICT"]
  ];
  for (const [name, mutate, code] of mutations) {
    const candidate = validStructured();
    mutate(candidate);
    const result = buildIndonesiaStructuredProductBridge(candidate);
    assert.equal(result.status, INDONESIA_STRUCTURED_PRODUCT_STATUS.INVALID_REVIEW_REQUIRED, name);
    assert.equal(result.reasonCode, code, name);
    assert.equal(result.payload, null, name);
  }
});

test("server path admits only selected or model-evidence-required states to one call", () => {
  const modelEvidenceStatusIndex = serverSource.indexOf("INDONESIA_STRUCTURED_ROUTE_STATUS.MODEL_SOURCE_EVIDENCE_REQUIRED");
  const start = serverSource.lastIndexOf("if ([", modelEvidenceStatusIndex);
  const end = serverSource.indexOf("const selectedProviderPrompt", start);
  assert.ok(start >= 0 && end > start);
  const block = serverSource.slice(start, end);
  assert.match(block, /MODEL_SOURCE_EVIDENCE_REQUIRED/u);
  assert.equal(Array.from(block.matchAll(/callAliyunVision\(/gu)).length, 1);
  assert.match(block, /buildIndonesiaStructuredProductBridge/u);
  assert.ok(block.indexOf("buildIndonesiaStructuredProductBridge") < block.indexOf("consumeCoordinateUsage"));
  assert.match(block, /本路径不重试，也不调用旧 OCR 或其他模型/u);
});

test("prompt forbids invented CRS datum and axis evidence", () => {
  assert.match(STRUCTURED_PROMPT, /不得猜测或补齐缺失字符、坐标系、基准或轴列表头/u);
  assert.match(STRUCTURED_PROMPT, /axisColumnsExplicit/u);
  assert.match(STRUCTURED_PROMPT, /只能逐字保留原图明确可见内容/u);
});

console.log(JSON.stringify({
  status: "PASS",
  suite: "coordinate-model-source-evidence-gate-regression",
  passed: cases.length,
  total: cases.length,
  evidenceClass: "OFFLINE_NO_NETWORK",
  providerCalls: 0,
  databaseWrites: 0
}));
