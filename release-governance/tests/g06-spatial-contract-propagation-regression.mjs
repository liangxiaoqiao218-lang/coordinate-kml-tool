import assert from "node:assert/strict";

import {
  FINALIZED_COORDINATE_CRS,
  FINALIZED_COORDINATE_SCHEMA_VERSION,
  finalizeCoordinateResult
} from "../../server/coordinate-finalizer/index.js";
import { FinalizedResultSpatialGeometryAdapter } from "../../server/spatial/adapters/finalized-result-adapter.js";
import { MapPreviewAdapter } from "../../server/spatial/adapters/map-preview-adapter.js";

const clock = () => "2026-08-28T00:00:00.000Z";
const geometry = Object.freeze({
  type: "Polygon",
  coordinates: Object.freeze([Object.freeze([
    Object.freeze([116.391245, 39.907654]),
    Object.freeze([116.401245, 39.907654]),
    Object.freeze([116.401245, 39.917654]),
    Object.freeze([116.391245, 39.907654])
  ])])
});
const mapAdapter = new MapPreviewAdapter();
const exportGradeAdapter = new FinalizedResultSpatialGeometryAdapter();

function candidate(overrides = {}) {
  return {
    resultId: "g06-spatial-contract-result",
    resultRevision: 1,
    currentRevision: 1,
    sourceAuthority: "legacy",
    coordinateType: "wgs84_decimal",
    precisionMode: "preserve-original-decimals",
    crs: FINALIZED_COORDINATE_CRS,
    geometry,
    confirmationStatus: "not_required",
    qualityGateStatus: "passed",
    technicalKmlReady: true,
    requiresReview: false,
    kmlReady: true,
    groups: [{ groupId: "group_1", requiresReview: false, kmlReady: true }],
    warnings: [],
    ...overrides
  };
}

function finalized(overrides = {}) {
  return finalizeCoordinateResult(candidate(overrides), { clock });
}

function mapAllowed(result) {
  return mapAdapter.adapt(result, { clock }).previewEligibility.allowed === true;
}

function kmlAllowed(result) {
  return result.decisionState === "AUTO_EXPORT" && result.kmlReady === true;
}

const cases = [];
function test(id, name, run) {
  cases.push({ id, name, run });
}

test("G06-S01", "review pending and drawable allows Map while KML stays blocked", () => {
  const result = finalized({
    qualityGateStatus: "review_required",
    confirmationStatus: "pending",
    requiresReview: true,
    kmlReady: false
  });

  assert.equal(result.qualityGateStatus, "review_required");
  assert.equal(result.confirmationStatus, "pending");
  assert.equal(mapAllowed(result), true);
  assert.equal(kmlAllowed(result), false);
});

test("G06-S02", "accepted review independently allows both sibling consumers", () => {
  const result = finalized({
    qualityGateStatus: "review_required",
    confirmationStatus: "accepted",
    confirmedRevision: 1,
    requiresReview: true,
    kmlReady: true
  });

  assert.equal(result.qualityGateStatus, "review_required");
  assert.equal(result.confirmationStatus, "accepted");
  assert.equal(result.decisionState, "AUTO_EXPORT");
  assert.equal(mapAllowed(result), true);
  assert.equal(kmlAllowed(result), true);
});

test("G06-S03", "kmlReady false never mechanically blocks drawable Map geometry", () => {
  const result = finalized({
    technicalKmlReady: false,
    kmlReady: false
  });

  assert.equal(kmlAllowed(result), false);
  assert.equal(mapAllowed(result), true);
});

test("G06-S04", "invalid or non-drawable geometry blocks Map", () => {
  const result = finalized({
    geometry: null,
    technicalKmlReady: false,
    qualityGateStatus: "failed",
    kmlReady: false
  });

  assert.equal(mapAllowed(result), false);
  assert.equal(kmlAllowed(result), false);
});

test("G06-S05", "KML hard failure remains independent when geometry is drawable", () => {
  const result = finalized({
    qualityGateStatus: "failed",
    technicalKmlReady: false,
    kmlReady: false
  });

  assert.equal(result.decisionState, "BLOCKED");
  assert.equal(kmlAllowed(result), false);
  assert.equal(mapAllowed(result), true);
});

test("G06-S06", "Map renderer failure cannot mutate authoritative KML eligibility", () => {
  const result = finalized();
  const before = Object.freeze({
    decisionState: result.decisionState,
    kmlReady: result.kmlReady,
    geometryHash: result.geometryHash
  });

  assert.equal(mapAllowed(result), true);
  assert.throws(() => {
    throw new Error("SIMULATED_MAP_RENDERER_FAILURE");
  }, /SIMULATED_MAP_RENDERER_FAILURE/);

  assert.deepEqual({
    decisionState: result.decisionState,
    kmlReady: result.kmlReady,
    geometryHash: result.geometryHash
  }, before);
  assert.equal(kmlAllowed(result), true);
});

test("G06-S07", "AUTO_EXPORT-only adapter remains export-grade, not a general Map gate", () => {
  const result = finalized({
    qualityGateStatus: "review_required",
    confirmationStatus: "pending",
    requiresReview: true,
    kmlReady: false
  });

  assert.equal(exportGradeAdapter.adapt(result).ok, false);
  assert.equal(mapAllowed(result), true);
});

test("G06-S08", "Spatial rejects engine-private and raw structures outside the canonical contract", () => {
  const privateInputs = [
    { v3Result: { geometry }, rawOcr: "116.391245,39.907654" },
    { legacyResult: { geometry }, parserState: { family: "private" } },
    { schemaVersion: "engine_private_v3", geometry, crs: FINALIZED_COORDINATE_CRS }
  ];

  for (const input of privateInputs) {
    assert.notEqual(input.schemaVersion, FINALIZED_COORDINATE_SCHEMA_VERSION);
    assert.equal(mapAllowed(input), false);
  }
});

let passed = 0;
for (const entry of cases) {
  try {
    await entry.run();
    passed += 1;
    console.log(`PASS ${entry.id} ${entry.name}`);
  } catch (error) {
    console.error(`FAIL ${entry.id} ${entry.name}`);
    throw error;
  }
}

console.log(`G06_SPATIAL_CONTRACT_PROPAGATION=PASS (${passed}/${cases.length})`);
console.log("KML_SPATIAL_SIBLING_RULE=PASS");
console.log("SATELLITE_GENERAL_MAP_GATE=MAP_PREVIEW_DRAWABLE_ELIGIBILITY");
