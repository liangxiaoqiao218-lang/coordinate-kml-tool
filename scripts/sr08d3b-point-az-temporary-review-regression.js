import assert from "node:assert/strict";
import {
  COORDINATE_GATE_REASON,
  CoordinateConfirmationRuntime,
  POINT_AZ_TEMPORARY_REVIEW_POLICY,
  createLegacyFinalizerInput,
  finalizeCoordinateResult
} from "../server/coordinate-finalizer/index.js";
import { registerFinalizedCoordinateResult } from "../server/coordinate-finalizer/index.js";
import { evaluatePointAzEvidenceCoverage, POINT_AZ_EVIDENCE_COVERAGE_CONTRACT } from "../server/evidence/point-az-evidence-coverage.js";
import { FinalizedResultSpatialGeometryAdapter } from "../server/spatial/adapters/finalized-result-adapter.js";

const clock = () => "2026-08-26T00:00:00.000Z";
const validPoints = Array.from({ length: 26 }, (_, index) => ({
  label: String.fromCharCode(65 + index),
  lon: 100 + (index % 7) * 0.01,
  lat: 10 + Math.floor(index / 7) * 0.01
}));

function engine(overrides = {}) {
  return {
    coordinate_type: "standard_dms_table",
    precision_mode: "point-az-dms-table",
    requires_review: false,
    groups: [{ group_id: "group_1", geometry: "polygon", requires_review: false, kml_ready: true, points: validPoints }],
    warnings: [],
    ...overrides
  };
}

function pointAzResult({ verification = { status: "PASS", warnings: [], conflicts: [], geometryWarnings: [] }, structured = engine(), revision = {} } = {}) {
  return finalizeCoordinateResult(createLegacyFinalizerInput({
    recognitionResult: { precisionMode: structured.precision_mode },
    coordinateEngineV2: structured,
    verification,
    revision: { resultId: "point-az-policy-result", resultRevision: 1, ...revision }
  }), { clock });
}

const cases = [];
function test(id, fn) { cases.push({ id, fn }); }

test("PAZ-P01", () => {
  const result = pointAzResult();
  assert.equal(result.requiresReview, true);
  assert.ok(["REVIEW_REQUIRED", "BLOCKED"].includes(result.decisionState));
});
test("PAZ-P02", () => assert.equal(pointAzResult().familySafetyPolicy.reasonCode, COORDINATE_GATE_REASON.PROVIDER_EVIDENCE_COVERAGE_INSUFFICIENT));
test("PAZ-P03", () => {
  assert.equal(pointAzResult().kmlReady, true);
  assert.equal(pointAzResult().decisionState, "REVIEW_REQUIRED");
});
test("PAZ-P04", () => {
  const result = registerFinalizedCoordinateResult(pointAzResult());
  const adapted = new FinalizedResultSpatialGeometryAdapter().adapt(result);
  assert.equal(adapted.ok, true);
  assert.equal(adapted.geometry.gate.decisionState, "REVIEW_REQUIRED");
  assert.ok(adapted.geometry.warnings.length > 0);
});
test("PAZ-P05", () => {
  const runtime = new CoordinateConfirmationRuntime();
  const pending = pointAzResult();
  runtime.register(pending);
  const confirmed = runtime.confirm({ resultId: pending.resultId, resultRevision: 1, geometryHash: pending.geometryHash, action: "accept" });
  assert.equal(confirmed.finalizedCoordinateResult.decisionState, "AUTO_EXPORT");
  assert.equal(confirmed.finalizedCoordinateResult.kmlReady, true);
  assert.equal(confirmed.finalizedCoordinateResult.requiresReview, false);
  assert.equal(confirmed.finalizedCoordinateResult.geometryHash, pending.geometryHash);
});
test("PAZ-P06", () => {
  const runtime = new CoordinateConfirmationRuntime();
  const pending = pointAzResult();
  runtime.register(pending);
  assert.equal(runtime.confirm({ resultId: pending.resultId, resultRevision: 0, geometryHash: pending.geometryHash, action: "accept" }).code, COORDINATE_GATE_REASON.STALE_CONFIRMATION_REVISION);
});
test("PAZ-P07", () => {
  const runtime = new CoordinateConfirmationRuntime();
  const pending = pointAzResult();
  const invalid = finalizeCoordinateResult({
    ...pending,
    geometry: { type: "Point", coordinates: [200, 10] }
  }, { clock });
  runtime.register(invalid);
  assert.equal(runtime.confirm({ resultId: invalid.resultId, resultRevision: 1, geometryHash: invalid.geometryHash, action: "accept" }).finalizedCoordinateResult.decisionState, "BLOCKED");
});
test("PAZ-P08", () => {
  const runtime = new CoordinateConfirmationRuntime();
  const failed = pointAzResult({ verification: { status: "BLOCK", warnings: [], conflicts: [], geometryWarnings: [] } });
  runtime.register(failed);
  assert.equal(runtime.confirm({ resultId: failed.resultId, resultRevision: 1, geometryHash: failed.geometryHash, action: "accept" }).finalizedCoordinateResult.decisionState, "BLOCKED");
});
test("PAZ-P09", () => assert.equal(pointAzResult({ structured: engine({ precision_mode: "preserve-original-decimals-and-parse-dms" }) }).familySafetyPolicy, null));
test("PAZ-P10", () => assert.equal(pointAzResult({ structured: engine({ coordinate_type: "wgs84_decimal", precision_mode: "wgs84-table-coordinates" }) }).familySafetyPolicy, null));
test("PAZ-P11", () => assert.equal(pointAzResult({ structured: engine({ coordinate_type: "projected_xy", precision_mode: "utm30n-projected-x-y" }) }).familySafetyPolicy, null));
test("PAZ-P12", () => assert.equal(pointAzResult({ structured: engine({ coordinate_type: "madagascar_cadastral_grid", precision_mode: "cadastral-grid-num-xv-yv" }) }).familySafetyPolicy, null));
test("PAZ-P13", () => {
  const result = pointAzResult({ structured: engine({ coordinate_type: "handwritten_dms_experimental", precision_mode: "handwritten-dms-coordinates" }) });
  assert.equal(result.familySafetyPolicy, null);
  assert.equal(result.confirmationStatus, "pending");
});
test("PAZ-P14", () => {
  const policy = pointAzResult().familySafetyPolicy;
  assert.equal(policy.policyId, "POINT_AZ_TEMPORARY_REVIEW_POLICY");
  assert.equal(policy.policyVersion, "1");
  assert.equal(policy.active, true);
});
test("PAZ-P15", () => assert.equal(POINT_AZ_TEMPORARY_REVIEW_POLICY.removalConditions.length, 10));
test("EC01", () => assert.equal(POINT_AZ_EVIDENCE_COVERAGE_CONTRACT.expectedPointCount, 26));
test("EC02", () => {
  const result = evaluatePointAzEvidenceCoverage({
    generalVision: { parseable: true, rowCount: 22, explicitLabels: [] },
    finalVision: { parseable: true, rowCount: 26, explicitLabels: [] },
    comparison: { ordinalComparableRows: 22, conflictingFields: [] }
  });
  assert.equal(result.status, "partial");
  assert.equal(result.comparison.missingRows, 4);
});
test("EC03", () => {
  const result = evaluatePointAzEvidenceCoverage({
    generalVision: { parseable: true, rowCount: 26, explicitLabels: [] },
    finalVision: { parseable: true, rowCount: 26, explicitLabels: [] },
    comparison: { ordinalComparableRows: 26, conflictingFields: [] }
  });
  assert.equal(result.status, "partial");
  assert.equal(result.comparison.explicitLabelCoverageComplete, false);
});
test("EC04", () => assert.equal(evaluatePointAzEvidenceCoverage({ finalVision: { parseable: true, rowCount: 26 } }).status, "insufficient"));
test("EC05", () => {
  const conflict = { pointId: "P", field: "longitude" };
  const result = evaluatePointAzEvidenceCoverage({
    generalVision: { parseable: true, rowCount: 26, explicitLabels: Array.from({ length: 26 }, (_, index) => String.fromCharCode(65 + index)) },
    finalVision: { parseable: true, rowCount: 26, explicitLabels: Array.from({ length: 26 }, (_, index) => String.fromCharCode(65 + index)) },
    comparison: { ordinalComparableRows: 26, conflictingFields: [conflict] }
  });
  assert.equal(result.status, "partial");
  assert.deepEqual(result.comparison.conflictingFields[0], conflict);
});

let passed = 0;
for (const entry of cases) {
  try {
    await entry.fn();
    passed += 1;
    console.log(`PASS ${entry.id}`);
  } catch (error) {
    console.error(`FAIL ${entry.id}`);
    throw error;
  }
}
console.log(`SR-08D.3B regression: ${passed}/${cases.length} PASS`);
