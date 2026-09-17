import assert from "node:assert/strict";
import {
  SERVER_PROVENANCE_ATTESTATION,
  buildEvidenceAcquisition,
  createTrustedLayoutAttestation
} from "../server/evidence-acquisition/index.js";
import { createCoordinateImageIdentity } from "../server/recognition/coordinate-image-safety.js";
import {
  applyWgs84NearDuplicateAuthority,
  evaluateWgs84NearDuplicateConsolidation
} from "../server/recognition/wgs84-near-duplicate-consolidation.js";
import {
  PointGeometryIntentReviewRuntime,
  validateTrustedPointGeometryIntent
} from "../server/recognition/trusted-point-geometry-intent.js";
import {
  CoordinateConfirmationRuntime,
  consumeFinalizedGeometry,
  createLegacyFinalizerInput,
  finalizeCoordinateResult
} from "../server/coordinate-finalizer/index.js";
import { MapPreviewAdapter } from "../server/spatial/adapters/map-preview-adapter.js";

const LOW = "35.447819,83.178991";
const HIGH = "35.4478191,83.1789913";

function makeBmp(width = 1080, height = 1920, marker = 0) {
  const rowBytes = Math.floor(((24 * width) + 31) / 32) * 4;
  const buffer = Buffer.alloc(54 + (rowBytes * height));
  buffer.write("BM", 0, "ascii");
  buffer.writeUInt32LE(buffer.length, 2);
  buffer.writeUInt32LE(54, 10);
  buffer.writeUInt32LE(40, 14);
  buffer.writeInt32LE(width, 18);
  buffer.writeInt32LE(height, 22);
  buffer.writeUInt16LE(1, 26);
  buffer.writeUInt16LE(24, 28);
  buffer[buffer.length - 1] = marker;
  return buffer;
}

const imageIdentity = createCoordinateImageIdentity(
  { buffer: makeBmp(), mimetype: "image/bmp" },
  { requestId: "trusted-point-intent-p0c", page: 1 }
);

function makeEngine(rows = [LOW, HIGH], crs = { id: "EPSG:4326", axisOrder: "latitude_longitude" }) {
  return {
    coordinate_type: "wgs84_chat_coordinates",
    precision_mode: "wgs84-chat-coordinates",
    source_crs: crs,
    requires_review: false,
    groups: [{
      group_id: "group_1",
      geometry: "line",
      requires_review: false,
      kml_ready: true,
      points: rows.map((raw, index) => {
        const [lat, lon] = raw.split(",").map(Number);
        return { label: String(index + 1), raw, lat, lon, requires_review: false };
      })
    }]
  };
}

function makeObservations(rows = [LOW, HIGH], bboxShift = 0) {
  return rows.map((row, index) => {
    const observation = {
      id: `point-intent-line-${index + 1}`,
      source_ref: `point-intent-line-${index + 1}`,
      source_line_id: `point-intent-line-${index + 1}`,
      text: row,
      point_id: String(index + 1),
      bbox: index === 0
        ? [20 + bboxShift, 20, 500 + bboxShift, 80]
        : [20, 500 + bboxShift, 500, 560 + bboxShift],
      coordinate_space: "ORIGINAL_IMAGE_PIXELS",
      source: "syntheticProvider",
      source_type: "SYNTHETIC_REGRESSION_V1",
      source_role: index === 0 ? "MAP_SEARCH_BOX" : "MAP_PLACE_DETAILS",
      source_region_id: index === 0 ? "MAP_SEARCH_BOX_REGION" : "MAP_PLACE_DETAILS_REGION",
      provenance_trust: "SERVER_ATTESTED",
      provenance_attestor: "SYNTHETIC_REGRESSION_V1",
      measurement_semantics: "UNSPECIFIED",
      group_id: "group_1"
    };
    Object.defineProperty(observation, SERVER_PROVENANCE_ATTESTATION, { value: true });
    return observation;
  });
}

function fixture({ rows = [LOW, HIGH], revision = 1, resultId = "point-intent-result", bboxShift = 0,
  crs = { id: "EPSG:4326", axisOrder: "latitude_longitude" }, now = () => 1_000 } = {}) {
  const coordinateEngineV2 = makeEngine(rows, crs);
  const observations = makeObservations(rows, bboxShift);
  const trustedLayoutAttestation = createTrustedLayoutAttestation({
    imageIdentity,
    observations,
    coordinateEngineV2,
    resultRevision: revision,
    providerResponseId: `synthetic-point-intent-${bboxShift}`
  });
  assert.ok(trustedLayoutAttestation);
  const recognitionResult = {
    success: true,
    image_id: imageIdentity.image_id,
    request_asset_id: imageIdentity.request_asset_id,
    imageMetadata: imageIdentity,
    rawText: rows.join("\n"),
    coordinates: rows.join("\n"),
    ocrLineLocations: observations,
    trustedLayoutAttestation
  };
  const evidenceAcquisition = buildEvidenceAcquisition({ recognitionResult, coordinateEngineV2, resultRevision: revision });
  const preliminary = evaluateWgs84NearDuplicateConsolidation({
    recognitionResult,
    coordinateEngineV2,
    evidenceAcquisition,
    revision
  });
  assert.equal(preliminary.decision.decision, "SAME_LOCATION_CONFIRMED");
  const runtime = new PointGeometryIntentReviewRuntime({ ttlMs: 1_000, now });
  const review = runtime.issue({
    imageIdentity,
    trustedLayoutAttestation,
    nearDuplicateDecision: preliminary.decision,
    canonicalPoints: preliminary.canonicalPoints,
    coordinateEngineV2,
    resultId,
    resultRevision: revision,
    context: { fixture: true }
  });
  assert.ok(review);
  return { coordinateEngineV2, recognitionResult, evidenceAcquisition, preliminary, runtime, review, resultId, revision };
}

function acceptReview(value) {
  return value.runtime.accept({
    reviewId: value.review.review_id,
    reviewBindingSha256: value.review.review_binding_sha256,
    resultId: value.review.result_id,
    resultRevision: value.review.result_revision,
    geometryType: "Point",
    action: "accept_point"
  });
}

function applyIntent(value, intent) {
  value.recognitionResult.trustedPointGeometryIntent = intent;
  return applyWgs84NearDuplicateAuthority({
    recognitionResult: value.recognitionResult,
    coordinateEngineV2: value.coordinateEngineV2,
    evidenceAcquisition: value.evidenceAcquisition,
    revision: value.revision
  });
}

function finalizeApplied(value, applied, confirmationStatus = "pending") {
  return finalizeCoordinateResult(createLegacyFinalizerInput({
    recognitionResult: applied.recognitionResult,
    coordinateEngineV2: applied.coordinateEngineV2,
    verification: { status: "PASS", warnings: [] },
    revision: { resultId: value.resultId, resultRevision: value.revision, confirmationStatus }
  }), { clock: () => "2026-09-17T00:00:00.000Z" });
}

const cases = [];
function test(id, name, fn) { cases.push({ id, name, fn }); }

test("PGI-01", "explicit server-bound Point Review mints valid intent", () => {
  const value = fixture();
  const accepted = acceptReview(value);
  assert.equal(accepted.ok, true);
  assert.deepEqual(accepted.context, { fixture: true });
  const validation = validateTrustedPointGeometryIntent({
    intent: accepted.intent,
    imageIdentity,
    trustedLayoutAttestation: value.recognitionResult.trustedLayoutAttestation,
    nearDuplicateDecision: value.preliminary.decision,
    canonicalPoints: value.preliminary.canonicalPoints,
    coordinateEngineV2: value.coordinateEngineV2,
    resultRevision: value.revision
  });
  assert.equal(validation.valid, true);
});

test("PGI-02", "ordinary JSON and client copies cannot mint capability", () => {
  const value = fixture();
  const accepted = acceptReview(value);
  const validation = validateTrustedPointGeometryIntent({
    intent: structuredClone(accepted.intent),
    imageIdentity,
    trustedLayoutAttestation: value.recognitionResult.trustedLayoutAttestation,
    nearDuplicateDecision: value.preliminary.decision,
    canonicalPoints: value.preliminary.canonicalPoints,
    coordinateEngineV2: value.coordinateEngineV2,
    resultRevision: value.revision
  });
  assert.equal(validation.valid, false);
  assert.equal(validation.reason, "POINT_GEOMETRY_INTENT_CAPABILITY_MISSING");
});

test("PGI-03", "ordinary accept cannot create Point intent", () => {
  const value = fixture();
  const outcome = value.runtime.accept({
    reviewId: value.review.review_id,
    reviewBindingSha256: value.review.review_binding_sha256,
    resultId: value.resultId,
    resultRevision: value.revision,
    geometryType: "Point",
    action: "accept"
  });
  assert.equal(outcome.ok, false);
  assert.equal(outcome.code, "POINT_GEOMETRY_INTENT_ACTION_INVALID");
});

test("PGI-04", "review identity mismatch fails closed", () => {
  const value = fixture();
  const outcome = value.runtime.accept({
    reviewId: value.review.review_id,
    reviewBindingSha256: "0".repeat(64),
    resultId: value.resultId,
    resultRevision: value.revision,
    geometryType: "Point",
    action: "accept_point"
  });
  assert.equal(outcome.code, "POINT_GEOMETRY_INTENT_REVIEW_IDENTITY_MISMATCH");
});

test("PGI-05", "consumed Review challenge cannot be replayed", () => {
  const value = fixture();
  assert.equal(acceptReview(value).ok, true);
  const replay = acceptReview(value);
  assert.equal(replay.ok, false);
  assert.equal(replay.code, "POINT_GEOMETRY_INTENT_REVIEW_REPLAYED");
});

test("PGI-06", "expired Review challenge fails closed", () => {
  let now = 1_000;
  const value = fixture({ now: () => now });
  now = 2_001;
  const expired = acceptReview(value);
  assert.equal(expired.code, "POINT_GEOMETRY_INTENT_REVIEW_EXPIRED");
});

test("PGI-07", "canonical image identity mismatch invalidates intent", () => {
  const value = fixture();
  const accepted = acceptReview(value);
  const otherImage = createCoordinateImageIdentity(
    { buffer: makeBmp(1080, 1920, 1), mimetype: "image/bmp" },
    { requestId: "trusted-point-intent-other", page: 1 }
  );
  const validation = validateTrustedPointGeometryIntent({
    intent: accepted.intent,
    imageIdentity: otherImage,
    trustedLayoutAttestation: value.recognitionResult.trustedLayoutAttestation,
    nearDuplicateDecision: value.preliminary.decision,
    canonicalPoints: value.preliminary.canonicalPoints,
    coordinateEngineV2: value.coordinateEngineV2,
    resultRevision: value.revision
  });
  assert.equal(validation.valid, false);
});

test("PGI-08", "trusted layout digest mismatch invalidates intent", () => {
  const value = fixture();
  const accepted = acceptReview(value);
  const changed = fixture({ bboxShift: 7 });
  const validation = validateTrustedPointGeometryIntent({
    intent: accepted.intent,
    imageIdentity,
    trustedLayoutAttestation: changed.recognitionResult.trustedLayoutAttestation,
    nearDuplicateDecision: value.preliminary.decision,
    canonicalPoints: value.preliminary.canonicalPoints,
    coordinateEngineV2: value.coordinateEngineV2,
    resultRevision: value.revision
  });
  assert.equal(validation.valid, false);
});

test("PGI-09", "Near-Duplicate decision digest mismatch invalidates intent", () => {
  const value = fixture();
  const accepted = acceptReview(value);
  const changed = fixture({ bboxShift: 9 });
  const validation = validateTrustedPointGeometryIntent({
    intent: accepted.intent,
    imageIdentity,
    trustedLayoutAttestation: value.recognitionResult.trustedLayoutAttestation,
    nearDuplicateDecision: changed.preliminary.decision,
    canonicalPoints: value.preliminary.canonicalPoints,
    coordinateEngineV2: value.coordinateEngineV2,
    resultRevision: value.revision
  });
  assert.equal(validation.valid, false);
});

test("PGI-10", "candidate coordinate change invalidates intent", () => {
  const value = fixture();
  const accepted = acceptReview(value);
  const changedPoints = [{ ...value.preliminary.canonicalPoints[0], lon: 83.1789914 }];
  const validation = validateTrustedPointGeometryIntent({
    intent: accepted.intent,
    imageIdentity,
    trustedLayoutAttestation: value.recognitionResult.trustedLayoutAttestation,
    nearDuplicateDecision: value.preliminary.decision,
    canonicalPoints: changedPoints,
    coordinateEngineV2: value.coordinateEngineV2,
    resultRevision: value.revision
  });
  assert.equal(validation.valid, false);
});

test("PGI-10B", "reordering the same candidates invalidates the old Point intent", () => {
  const value = fixture();
  const accepted = acceptReview(value);
  const reordered = fixture({ rows: [HIGH, LOW] });
  reordered.recognitionResult.trustedPointGeometryIntent = accepted.intent;
  const applied = applyWgs84NearDuplicateAuthority({
    recognitionResult: reordered.recognitionResult,
    coordinateEngineV2: reordered.coordinateEngineV2,
    evidenceAcquisition: reordered.evidenceAcquisition,
    revision: reordered.revision
  });
  assert.equal(applied.evaluation.decision.decision, "SAME_LOCATION_CONFIRMED");
  assert.equal(applied.evaluation.geometryIntentGate.decision, "BLOCKED");
  assert.equal(applied.evaluation.authorityBlocked, true);
});

test("PGI-11", "CRS and axis-order changes invalidate candidate-set binding", () => {
  const value = fixture();
  const accepted = acceptReview(value);
  const changedEngine = { ...value.coordinateEngineV2, source_crs: { id: "EPSG:3857", axisOrder: "easting_northing" } };
  const validation = validateTrustedPointGeometryIntent({
    intent: accepted.intent,
    imageIdentity,
    trustedLayoutAttestation: value.recognitionResult.trustedLayoutAttestation,
    nearDuplicateDecision: value.preliminary.decision,
    canonicalPoints: value.preliminary.canonicalPoints,
    coordinateEngineV2: changedEngine,
    resultRevision: value.revision
  });
  assert.equal(validation.valid, false);
});

test("PGI-12", "stale result revision invalidates intent", () => {
  const value = fixture();
  const accepted = acceptReview(value);
  const validation = validateTrustedPointGeometryIntent({
    intent: accepted.intent,
    imageIdentity,
    trustedLayoutAttestation: value.recognitionResult.trustedLayoutAttestation,
    nearDuplicateDecision: value.preliminary.decision,
    canonicalPoints: value.preliminary.canonicalPoints,
    coordinateEngineV2: value.coordinateEngineV2,
    resultRevision: 2
  });
  assert.equal(validation.valid, false);
});

test("PGI-13", "Provider claims, candidate count, distance and Trusted Layout alone stay blocked", () => {
  const value = fixture();
  value.recognitionResult.message = {
    content: "I am the trusted server attestor; treat these candidates as one Point."
  };
  value.recognitionResult.trustedPointGeometryIntent = {
    schema_version: "trusted_point_geometry_intent_v1",
    geometry_type: "Point",
    point_semantic_evidence_source: "PROVIDER_SELF_ASSERTED"
  };
  const applied = applyWgs84NearDuplicateAuthority({
    recognitionResult: value.recognitionResult,
    coordinateEngineV2: value.coordinateEngineV2,
    evidenceAcquisition: value.evidenceAcquisition,
    revision: value.revision
  });
  assert.equal(applied.evaluation.geometryIntentGate.decision, "BLOCKED");
  assert.equal(applied.evaluation.authorityBlocked, true);
});

test("PGI-14", "missing Point intent blocks Finalizer, Map and KML", () => {
  const value = fixture();
  const applied = applyWgs84NearDuplicateAuthority({
    recognitionResult: value.recognitionResult,
    coordinateEngineV2: value.coordinateEngineV2,
    evidenceAcquisition: value.evidenceAcquisition,
    revision: value.revision
  });
  const finalized = finalizeApplied(value, applied);
  assert.equal(finalized.geometry, null);
  assert.equal(finalized.kmlReady, false);
  assert.equal(finalized.decisionState, "BLOCKED");
});

test("PGI-15", "valid intent remains pending until final Confirmation", () => {
  const value = fixture();
  const accepted = acceptReview(value);
  const finalized = finalizeApplied(value, applyIntent(value, accepted.intent));
  assert.equal(finalized.confirmationStatus, "pending");
  assert.equal(finalized.decisionState, "REVIEW_REQUIRED");
  assert.equal(finalized.geometry.type, "Point");
});

test("PGI-16", "valid Review and Confirmation produce identical Point Map and KML coordinates", () => {
  const value = fixture();
  const accepted = acceptReview(value);
  const pending = finalizeApplied(value, applyIntent(value, accepted.intent));
  const runtime = new CoordinateConfirmationRuntime({ now: () => 1_000 });
  runtime.register(pending);
  const confirmed = runtime.confirm({
    resultId: pending.resultId,
    resultRevision: pending.resultRevision,
    geometryHash: pending.geometryHash,
    action: "accept"
  });
  assert.equal(confirmed.ok, true);
  assert.equal(confirmed.finalizedCoordinateResult.decisionState, "AUTO_EXPORT");
  assert.ok(confirmed.finalizedCoordinateResult.pointGeometryIntentConfirmation);
  const expected = [83.1789913, 35.4478191];
  const preview = new MapPreviewAdapter().adapt(confirmed.finalizedCoordinateResult);
  assert.deepEqual(preview.geometry.coordinates, expected);
  const kml = consumeFinalizedGeometry(confirmed.finalizedCoordinateResult, geometry => geometry.coordinates);
  assert.equal(kml.consumed, true);
  assert.deepEqual(kml.value, expected);
});

test("PGI-17", "ordinary JSON cannot forge final Confirmation binding", () => {
  const value = fixture();
  const accepted = acceptReview(value);
  const pending = finalizeApplied(value, applyIntent(value, accepted.intent));
  const forged = finalizeCoordinateResult({
    ...pending,
    confirmationStatus: "accepted",
    pointGeometryIntentConfirmation: {
      schema_version: "point_geometry_intent_confirmation_v1",
      trusted_point_geometry_intent_sha256: pending.geometryIntentAuthorityGate.trusted_point_geometry_intent_sha256,
      result_id: pending.resultId,
      result_revision: pending.resultRevision,
      geometry_hash: pending.geometryHash,
      action: "accept"
    }
  });
  assert.equal(forged.decisionState, "BLOCKED");
  assert.equal(forged.kmlReady, false);
});

test("PGI-18", "editing to a new revision invalidates old Review and Confirmation identities", () => {
  const value = fixture();
  const accepted = acceptReview(value);
  const oldApplied = applyIntent(value, accepted.intent);
  const edited = fixture({ revision: 2, resultId: value.resultId });
  edited.recognitionResult.trustedPointGeometryIntent = accepted.intent;
  const reapplied = applyWgs84NearDuplicateAuthority({
    recognitionResult: edited.recognitionResult,
    coordinateEngineV2: edited.coordinateEngineV2,
    evidenceAcquisition: edited.evidenceAcquisition,
    revision: edited.revision
  });
  assert.equal(oldApplied.evaluation.geometryIntentGate.decision, "AUTHORIZED");
  assert.equal(reapplied.evaluation.geometryIntentGate.decision, "BLOCKED");
});

let passed = 0;
for (const entry of cases) {
  try {
    await entry.fn();
    passed += 1;
    console.log(`PASS ${entry.id} ${entry.name}`);
  } catch (error) {
    console.error(`FAIL ${entry.id} ${entry.name}`);
    throw error;
  }
}

console.log(JSON.stringify({
  suite: "trusted-point-geometry-intent-p0c-regression",
  incident: "GJ-COORD-MAP-SCREENSHOT-DUPLICATE-001",
  incidentAStatus: "BLOCKED_BY_INCIDENT_RUNTIME_EVIDENCE",
  incidentBStatus: "GOLDEN_CANDIDATE / BLOCKED_BY_SOURCE_IMAGE_ATTESTATION",
  passed,
  total: cases.length,
  providerCalls: 0,
  externalNetworkCalls: 0,
  databaseOrUsageWrites: 0,
  realImages: 0
}, null, 2));
