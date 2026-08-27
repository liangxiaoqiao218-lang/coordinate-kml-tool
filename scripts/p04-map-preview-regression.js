import assert from "node:assert/strict";
import {
  FINALIZED_COORDINATE_CRS,
  finalizeCoordinateResult
} from "../server/coordinate-finalizer/index.js";
import { FinalizedResultSpatialGeometryAdapter } from "../server/spatial/adapters/finalized-result-adapter.js";
import {
  MAP_PREVIEW_BLOCK_REASON,
  MapPreviewAdapter
} from "../server/spatial/adapters/map-preview-adapter.js";
import { calculateSpatialFacts } from "../server/spatial/spatial-facts.js";

const adapter = new MapPreviewAdapter();
const clock = () => "2026-08-27T00:00:00.000Z";
const point = { type: "Point", coordinates: [103.1, 16.5] };
const line = { type: "LineString", coordinates: [[103.1, 16.5], [103.2, 16.6]] };
const polygon = { type: "Polygon", coordinates: [[[103.1, 16.5], [103.2, 16.5], [103.2, 16.6], [103.1, 16.5]]] };

function candidate(overrides = {}) {
  return {
    resultId: "p04-result",
    resultRevision: 1,
    currentRevision: 1,
    sourceAuthority: "legacy",
    coordinateType: "wgs84_decimal",
    precisionMode: "wgs84-table-coordinates",
    crs: FINALIZED_COORDINATE_CRS,
    geometry: polygon,
    confirmationStatus: "not_required",
    qualityGateStatus: "passed",
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

function identity(result) {
  return { resultId: result.resultId, resultRevision: result.resultRevision, geometryHash: result.geometryHash };
}

const cases = [];
function test(id, name, run) { cases.push({ id, name, run }); }

test("P04-01", "Polygon produces a drawable preview and area facts", () => {
  const result = finalized();
  const preview = adapter.adapt(result, { expectedIdentity: identity(result), clock });
  const facts = calculateSpatialFacts(preview.geometry);
  assert.equal(preview.previewEligibility.allowed, true);
  assert.equal(preview.geometryType, "Polygon");
  assert.ok(facts.areaMeters2 > 0);
  assert.ok(facts.perimeterMeters > 0);
});

test("P04-02", "LineString produces length but no fabricated area", () => {
  const result = finalized({ geometry: line, groups: [{ groupId: "group_1", geometry: "line", requiresReview: false, kmlReady: true }] });
  const facts = calculateSpatialFacts(adapter.adapt(result, { clock }).geometry);
  assert.ok(facts.lengthMeters > 0);
  assert.equal(facts.areaMeters2, null);
});

test("P04-03", "Point produces one point and no fabricated measurements", () => {
  const result = finalized({ geometry: point, groups: [{ groupId: "group_1", geometry: "point", requiresReview: false, kmlReady: true }] });
  const facts = calculateSpatialFacts(adapter.adapt(result, { clock }).geometry);
  assert.equal(facts.pointCount, 1);
  assert.equal(facts.areaMeters2, null);
  assert.equal(facts.lengthMeters, null);
});

test("P04-04", "Review-required drawable result can preview while export remains blocked", () => {
  const result = finalized({ requiresReview: true });
  assert.equal(result.decisionState, "REVIEW_REQUIRED");
  assert.equal(adapter.adapt(result, { clock }).previewEligibility.allowed, true);
  assert.equal(new FinalizedResultSpatialGeometryAdapter().adapt(result).ok, false);
});

test("P04-05", "Pending confirmation can preview and carries a warning", () => {
  const result = finalized({ confirmationStatus: "pending" });
  const preview = adapter.adapt(result, { clock });
  assert.equal(preview.previewEligibility.allowed, true);
  assert.ok(preview.previewWarnings.includes("CONFIRMATION_PENDING"));
});

test("P04-06", "KML-blocked drawable result cannot be upgraded by preview", () => {
  const result = finalized({ kmlReady: false });
  assert.equal(adapter.adapt(result, { clock }).previewEligibility.allowed, true);
  assert.equal(new FinalizedResultSpatialGeometryAdapter().adapt(result).ok, false);
});

test("P04-07", "Self-intersection warning remains visible without changing authority", () => {
  const result = finalized({ warnings: ["SELF_INTERSECTION"], requiresReview: true });
  const preview = adapter.adapt(result, { clock });
  assert.ok(preview.previewWarnings.includes("SELF_INTERSECTION"));
  assert.equal(result.decisionState, "REVIEW_REQUIRED");
});

test("P04-08", "Missing structured result fails closed", () => {
  const preview = adapter.adapt(null, { clock });
  assert.equal(preview.previewEligibility.allowed, false);
  assert.equal(preview.previewReasonCodes[0], MAP_PREVIEW_BLOCK_REASON.NO_STRUCTURED_RESULT);
});

test("P04-09", "No finalized geometry fails closed", () => {
  const result = finalized({ geometry: null, qualityGateStatus: "failed", kmlReady: false });
  const preview = adapter.adapt(result, { clock });
  assert.equal(preview.previewEligibility.allowed, false);
  assert.equal(preview.previewReasonCodes[0], MAP_PREVIEW_BLOCK_REASON.NO_DRAWABLE_GEOMETRY);
});

test("P04-10", "Unavailable family without a result stays blocked", () => {
  const result = finalized({ geometry: null, availabilityStatus: "BLOCKED_BY_PROVIDER", qualityGateStatus: "failed", kmlReady: false });
  const preview = adapter.adapt(result, { clock });
  assert.equal(preview.previewReasonCodes[0], MAP_PREVIEW_BLOCK_REASON.FAMILY_UNAVAILABLE_WITHOUT_RESULT);
});

test("P04-11", "Stale result revision is rejected", () => {
  const result = finalized();
  const preview = adapter.adapt(result, { expectedIdentity: { ...identity(result), resultRevision: 2 }, clock });
  assert.equal(preview.previewReasonCodes[0], MAP_PREVIEW_BLOCK_REASON.STALE_SOURCE_REVISION);
});

test("P04-12", "Geometry hash mismatch and facts failure are isolated", () => {
  const result = finalized();
  const preview = adapter.adapt({ ...result, geometryHash: "sha256:stale" }, { clock });
  assert.equal(preview.previewReasonCodes[0], MAP_PREVIEW_BLOCK_REASON.GEOMETRY_HASH_MISMATCH);
  assert.throws(() => calculateSpatialFacts({ type: "Unsupported", coordinates: [] }), /SPATIAL_FACTS_POSITIONS_REQUIRED/);
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
console.log(`P-04 map preview regression: ${passed}/${cases.length} PASS`);
