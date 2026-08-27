import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import {
  KYRGYZ_PRIMARY_STAGE_CAP_MS,
  MADAGASCAR_PRIMARY_STAGE_CAP_MS,
  WGS84_PRIMARY_STAGE_CAP_MS,
  buildPrimaryRouteDecision,
  detectUploadTableStructure,
  getMadagascarCadastralStrongRouteEvidence,
  getWgs84StrongRouteEvidence,
  shouldRunWgs84TimeoutRescue
} from "../server/recognition/family-primary-routing.js";
import { RecognitionBudget } from "../server/coordinate-finalizer/recognition-deadline.js";
import {
  COORDINATE_CONFIRMATION_STATUS,
  COORDINATE_DECISION_STATE,
  COORDINATE_QUALITY_GATE_STATUS,
  FINALIZED_COORDINATE_CRS,
  finalizeCoordinateResult
} from "../server/coordinate-finalizer/index.js";
import { inferStructuredBoundaryType } from "../server/structured-coordinate-boundary.js";

const tests = [];
const test = (id, name, fn) => tests.push({ id, name, fn });

const strongWgs84 = getWgs84StrongRouteEvidence({
  fileName: "coordinate-table.png",
  rawHint: "WGS84 longitude latitude coordinate table"
});
const unknownWgs84 = getWgs84StrongRouteEvidence({ fileName: "upload.png", rawHint: "" });
const strongMadagascar = getMadagascarCadastralStrongRouteEvidence({
  fileName: "survey.png",
  rawHint: "Madagascar cadastral grid Liste_Carres XV YV"
});
const madagascarFixtureBytes = await readFile(new URL("../regression-samples/fixtures/马达加斯加坐标.png", import.meta.url));

test("W01", "strong WGS84 metadata selects specialized primary and skips generic", () => {
  const decision = buildPrimaryRouteDecision({ family: "wgs84_table", evidence: strongWgs84 });
  assert.equal(strongWgs84.matched, true);
  assert.equal(decision.selected, true);
  assert.equal(decision.genericProviderAllowed, false);
});
test("W02", "unknown WGS84 metadata leaves generic path available", () => {
  const decision = buildPrimaryRouteDecision({ family: "wgs84_table", evidence: unknownWgs84 });
  assert.equal(decision.selected, false);
  assert.equal(decision.genericProviderAllowed, true);
});
test("W03", "WGS84 specialized success preserves decimal structured family", () => {
  assert.equal(inferStructuredBoundaryType({
    precisionMode: "wgs84-table-coordinates",
    wgs84TableCoordinates: { isWgs84TableCoordinates: true }
  }), "decimal_latlon");
});
test("W04", "WGS84 specialized failure is fail-closed and cannot fall through to generic", () => {
  const decision = buildPrimaryRouteDecision({ family: "wgs84_table", evidence: strongWgs84 });
  assert.equal(decision.failClosedOnSpecializedFailure, true);
  assert.equal(decision.genericProviderAllowed, false);
});
test("W05", "WGS84 timeout rescue cannot follow local OCR", () => {
  assert.equal(shouldRunWgs84TimeoutRescue({ localOcrAttempted: true }), false);
  assert.equal(shouldRunWgs84TimeoutRescue({ localOcrAttempted: false }), true);
});

test("K01", "Kyrgyz matched evidence selects a specialized primary", () => {
  const decision = buildPrimaryRouteDecision({ family: "kyrgyzstan_gk", evidence: { matched: true } });
  assert.equal(decision.selected, true);
  assert.equal(decision.genericProviderAllowed, false);
});
test("K02", "Kyrgyz stage cap is bounded below request usable budget", () => {
  assert.equal(KYRGYZ_PRIMARY_STAGE_CAP_MS, 25_000);
  assert.ok(KYRGYZ_PRIMARY_STAGE_CAP_MS < 52_500);
});
test("K03", "Kyrgyz specialized timeout does not authorize unrelated retry", () => {
  const decision = buildPrimaryRouteDecision({ family: "kyrgyzstan_gk", evidence: { matched: true } });
  assert.equal(decision.genericProviderAllowed, false);
  assert.match(decision.genericSkippedReason, /kyrgyzstan_gk_specialized_primary_selected/);
});
test("K04", "Kyrgyz specialized failure policy is safe fail-closed", () => {
  const decision = buildPrimaryRouteDecision({ family: "kyrgyzstan_gk", evidence: { matched: true } });
  assert.equal(decision.failClosedOnSpecializedFailure, true);
});

test("M01", "strong Madagascar cadastral evidence selects specialized primary", () => {
  const decision = buildPrimaryRouteDecision({ family: "madagascar_cadastral_grid", evidence: strongMadagascar });
  assert.equal(strongMadagascar.matched, true);
  assert.equal(decision.genericProviderAllowed, false);
});
test("M01B", "audited Madagascar PNG has OCR-independent table-grid evidence", () => {
  const structuralEvidence = detectUploadTableStructure(madagascarFixtureBytes, "image/png");
  const evidence = getMadagascarCadastralStrongRouteEvidence({
    fileName: "马达加斯加坐标.png",
    structuralEvidence
  });
  assert.equal(structuralEvidence.supported, true);
  assert.equal(structuralEvidence.hasTableGrid, true);
  assert.equal(evidence.matched, true);
});
test("M02", "Madagascar specialized payload remains cadastral family", () => {
  assert.equal(inferStructuredBoundaryType({
    precisionMode: "cadastral-grid-num-xv-yv",
    cadastralGrid: { isCadastralGrid: true }
  }), "madagascar_cadastral_grid");
});
test("M03", "Madagascar specialized evidence cannot become projected_xy", () => {
  assert.notEqual(inferStructuredBoundaryType({
    precisionMode: "utm30n-projected-x-y",
    cadastralGrid: { isCadastralGrid: true }
  }), "projected_xy");
});
test("M04", "Madagascar specialized primary is a single bounded provider stage", () => {
  assert.equal(MADAGASCAR_PRIMARY_STAGE_CAP_MS, 25_000);
  const decision = buildPrimaryRouteDecision({ family: "madagascar_cadastral_grid", evidence: strongMadagascar });
  assert.equal(decision.selected, true);
  assert.equal(decision.genericProviderAllowed, false);
});
test("M05", "Madagascar specialized timeout is fail-closed", () => {
  const decision = buildPrimaryRouteDecision({ family: "madagascar_cadastral_grid", evidence: strongMadagascar });
  assert.equal(decision.failClosedOnSpecializedFailure, true);
});

test("H01", "handwritten metadata cannot select WGS84 primary", () => {
  assert.equal(getWgs84StrongRouteEvidence({ rawHint: "HANDWRITTEN_DMS manuscript" }).matched, false);
});
test("H01B", "handwritten metadata cannot select Madagascar cadastral primary", () => {
  assert.equal(getMadagascarCadastralStrongRouteEvidence({ rawHint: "HANDWRITTEN_DMS manuscript" }).matched, false);
});
test("H02", "handwritten review semantics remain fail-closed", () => {
  const finalized = finalizeCoordinateResult({
    resultId: "sr08f7-handwritten",
    resultRevision: 1,
    currentRevision: 1,
    sourceAuthority: "legacy",
    coordinateType: "handwritten_dms_experimental",
    precisionMode: "handwritten-dms-coordinates",
    crs: FINALIZED_COORDINATE_CRS,
    geometry: { type: "Point", coordinates: [0, 0] },
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.PENDING,
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED,
    requiresReview: true,
    kmlReady: false,
    groups: [{ groupId: "group_1", requiresReview: true, kmlReady: false }],
    warnings: []
  });
  assert.notEqual(finalized.decisionState, COORDINATE_DECISION_STATE.AUTO_EXPORT);
  assert.equal(finalized.requiresReview, true);
  assert.equal(finalized.kmlReady, false);
});

test("A01", "specialized selection records a sanitized generic skip reason", () => {
  const budget = new RecognitionBudget({
    startedAt: 0,
    deadlineMs: 55_000,
    now: () => 10,
    trace: false,
    requestId: "sr08f7"
  });
  const decision = buildPrimaryRouteDecision({ family: "wgs84_table", evidence: strongWgs84 });
  budget.recordSkippedStage("generic_provider", decision.genericSkippedReason, "skipped");
  assert.equal(budget.events.length, 1);
  assert.equal(budget.events[0].result, "skipped");
  assert.equal(budget.events[0].skippedReason, "wgs84_table_specialized_primary_selected");
});
test("A02", "all specialized primary stage caps remain below 30 seconds", () => {
  assert.ok(Math.max(WGS84_PRIMARY_STAGE_CAP_MS, KYRGYZ_PRIMARY_STAGE_CAP_MS, MADAGASCAR_PRIMARY_STAGE_CAP_MS) < 30_000);
});
test("A03", "server retains persisted trace and locked review integration", async () => {
  const source = await readFile(new URL("../server.js", import.meta.url), "utf8");
  assert.match(source, /storeRegressionRecognitionTrace\(recognitionBudget\?\.toSanitizedTrace\(\)\)/);
  assert.match(source, /buildSpecializedFamilyLockedReviewPayload/);
  assert.match(source, /WGS84_STRONG_ROUTE_EVIDENCE/);
  assert.match(source, /MADAGASCAR_CADASTRAL_STRONG_ROUTE_EVIDENCE/);
});

let passed = 0;
for (const entry of tests) {
  try {
    await entry.fn();
    passed += 1;
    console.log(`PASS ${entry.id} ${entry.name}`);
  } catch (error) {
    console.error(`FAIL ${entry.id} ${entry.name}`);
    throw error;
  }
}
console.log(`SR-08F.7 family critical-path regression: ${passed}/${tests.length} PASS`);
