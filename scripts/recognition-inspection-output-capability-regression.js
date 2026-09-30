import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  COORDINATE_QUALITY_GATE_STATUS,
  FINALIZED_COORDINATE_CRS,
  createGeometryHash,
  finalizeCoordinateResult
} from "../server/coordinate-finalizer/index.js";
import {
  evaluateRecognitionOutputCapability,
  RECOGNITION_OUTPUT_CAPABILITY_VERSION
} from "../server/recognition/recognition-output-capability.js";
import { evaluateUnifiedRecognitionFinalAuthorization } from "../server/recognition/recognition-first-acquisition.js";
import { MapPreviewAdapter } from "../server/spatial/adapters/map-preview-adapter.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const cases = [];
function test(name, fn) {
  fn();
  cases.push(name);
}

function reviewResult(overrides = {}) {
  return finalizeCoordinateResult({
    resultId: "inspection-result-1",
    resultRevision: 1,
    sourceAuthority: "coordinate_engine_v2",
    coordinateType: "generic_coordinate_result",
    precisionMode: "generic-review",
    family: "generic",
    crs: FINALIZED_COORDINATE_CRS,
    geometry: {
      type: "Polygon",
      coordinates: [[[119.5, -2.5], [119.6, -2.5], [119.6, -2.6], [119.5, -2.5]]]
    },
    confirmationStatus: "pending",
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED,
    technicalKmlReady: false,
    requiresReview: true,
    kmlReady: false,
    kmlAuthorityBlocked: true,
    warnings: ["OCR_VALUE_MAY_BE_INACCURATE"],
    ...overrides
  }, { clock: () => "2026-10-01T00:00:00.000Z" });
}

test("valid review geometry enables map and unverified KML without formal authority", () => {
  const result = reviewResult({ explicitAuthorityRejected: true });
  const capability = evaluateRecognitionOutputCapability(result);
  assert.equal(capability.schemaVersion, RECOGNITION_OUTPUT_CAPABILITY_VERSION);
  assert.equal(capability.mapReady, true);
  assert.equal(capability.kmlReady, true);
  assert.equal(capability.unverified, true);
  assert.equal(result.kmlReady, false, "legacy/formal KML gate remains unchanged");
  assert.notEqual(result.decisionState, "AUTO_EXPORT");
});

for (const coordinateType of ["dms", "wgs84_decimal", "projected_xy", "unknown_future_family"]) {
  test(`${coordinateType} uses the same geometry capability rule`, () => {
    const result = reviewResult({ coordinateType, family: coordinateType });
    const capability = evaluateRecognitionOutputCapability(result);
    assert.equal(capability.mapReady, true);
    assert.equal(capability.kmlReady, true);
  });
}

test("review and conflict warnings do not disable technically generatable outputs", () => {
  const base = reviewResult();
  const result = Object.freeze({
    ...base,
    reasonCodes: Object.freeze(["FIELD_DIRECTION_CONFLICT", "SOURCE_LABELS_NONCONTIGUOUS"]),
    blockingReasons: Object.freeze([{ code: "FIELD_DIRECTION_CONFLICT" }])
  });
  const capability = evaluateRecognitionOutputCapability(result);
  assert.equal(capability.mapReady, true);
  assert.equal(capability.kmlReady, true);
  assert.ok(capability.warningReasons.includes("FIELD_DIRECTION_CONFLICT"));
});

test("unified final authorization exposes capability without granting authorization", () => {
  const result = reviewResult({ explicitAuthorityRejected: true });
  const authorization = evaluateUnifiedRecognitionFinalAuthorization({
    body: { finalizedCoordinateResult: result, mapReady: false, kmlReady: false },
    evidence: null,
    decision: null,
    providerCallCount: 0
  });
  assert.equal(authorization.authorized, false);
  assert.equal(authorization.mapReady, true);
  assert.equal(authorization.kmlReady, true);
  assert.equal(authorization.outputCapabilities.unverified, true);
});

test("map adapter accepts current finite review geometry", () => {
  const result = reviewResult({ explicitAuthorityRejected: true, mapReady: false });
  const preview = new MapPreviewAdapter().adapt(result, {
    expectedIdentity: {
      resultId: result.resultId,
      resultRevision: result.resultRevision,
      geometryHash: result.geometryHash
    },
    clock: () => "2026-10-01T00:00:00.000Z"
  });
  assert.equal(preview.previewEligibility.allowed, true);
  assert.equal(preview.geometryType, "Polygon");
  assert.ok(preview.previewWarnings.includes("REVIEW_REQUIRED"));
});

const invalidCases = [
  ["missing result", null, "RESULT_MISSING"],
  ["missing identity", { ...reviewResult(), resultId: null }, "RESULT_IDENTITY_MISSING"],
  ["stale revision", { ...reviewResult(), currentRevision: 2 }, "RESULT_REVISION_STALE"],
  ["invalid CRS", { ...reviewResult(), crs: { id: "EPSG:32750", axisOrder: "easting_northing" } }, "CRS_NOT_WGS84"],
  ["invalid geometry", { ...reviewResult(), geometry: { type: "Point", coordinates: [999, 999] } }, "GEOMETRY_INVALID"],
  ["hash mismatch", { ...reviewResult(), geometryHash: createGeometryHash({ type: "Point", coordinates: [0, 0] }) }, "GEOMETRY_HASH_MISMATCH"]
];

for (const [name, result, reason] of invalidCases) {
  test(`${name} remains blocked`, () => {
    const capability = evaluateRecognitionOutputCapability(result);
    assert.equal(capability.mapReady, false);
    assert.equal(capability.kmlReady, false);
    assert.ok(capability.blockReasons.includes(reason));
  });
}

test("browser consumes output capability instead of formal KML authority", () => {
  const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
  assert.match(html, /const outputCapabilities = payload\?\.outputCapabilities \|\| \{\}/u);
  assert.match(html, /provisionalMapEnabled[\s\S]+outputCapabilities\.technicallyGeneratable === true/u);
  assert.match(html, /outputCapabilities\.geometryHash === finalized\.geometryHash/u);
  assert.doesNotMatch(html, /provisionalMapEnabled[\s\S]{0,500}finalized\.decisionState === "REVIEW_REQUIRED"/u);
  assert.match(html, /provisionalKmlEnabled[\s\S]+outputCapabilities\.kmlReady === true/u);
  assert.doesNotMatch(html, /provisionalKmlEnabled[\s\S]{0,300}finalized\.kmlAuthorityBlocked !== true/u);
});

console.log(`Recognition inspection output capability: ${cases.length}/${cases.length} PASS; REAL_PROVIDER_CALLS=0`);
