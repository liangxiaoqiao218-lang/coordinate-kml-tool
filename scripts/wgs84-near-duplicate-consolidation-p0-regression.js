import assert from "node:assert/strict";
import fs from "node:fs";
import {
  SERVER_PROVENANCE_ATTESTATION,
  buildEvidenceAcquisition,
  classifyProviderLayoutRoles,
  collectProviderLayoutProfileQualification,
  extractProviderLayoutCandidates,
  createTrustedLayoutAttestation
} from "../server/evidence-acquisition/index.js";
import { createCoordinateImageIdentity } from "../server/recognition/coordinate-image-safety.js";
import {
  NEAR_DUPLICATE_DECISION,
  applyWgs84NearDuplicateAuthority,
  evaluateWgs84NearDuplicateConsolidation,
  validateWgs84NearDuplicateAuthority
} from "../server/recognition/wgs84-near-duplicate-consolidation.js";
import { PointGeometryIntentReviewRuntime } from "../server/recognition/trusted-point-geometry-intent.js";
import {
  COORDINATE_CONFIRMATION_STATUS,
  CoordinateConfirmationRuntime,
  consumeFinalizedGeometry,
  createLegacyFinalizerInput,
  finalizeCoordinateResult
} from "../server/coordinate-finalizer/index.js";
import { MapPreviewAdapter } from "../server/spatial/adapters/map-preview-adapter.js";

const low = "35.447819,83.178991";
const high = "35.4478191,83.1789913";

function makeBmp(width = 1080, height = 1920) {
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
  return buffer;
}

const syntheticImageIdentity = createCoordinateImageIdentity(
  { buffer: makeBmp(), mimetype: "image/bmp" },
  { requestId: "near-duplicate-p0-regression", page: 1 }
);

function point(raw, label) {
  const [lat, lon] = raw.split(",").map(Number);
  return { label: String(label), raw, lat, lon, confidence: 0.99, requires_review: false, warnings: [] };
}

function engine(rows = [low, high], {
  coordinateType = "wgs84_chat_coordinates",
  precisionMode = "wgs84-chat-coordinates"
} = {}) {
  return {
    coordinate_type: coordinateType,
    precision_mode: precisionMode,
    requires_review: false,
    groups: [{
      group_id: "group_1",
      group_name: "synthetic",
      geometry: rows.length === 1 ? "point" : (rows.length === 2 ? "line" : "polygon"),
      requires_review: false,
      kml_ready: true,
      points: rows.map((row, index) => point(row, index + 1)),
      warnings: []
    }]
  };
}

function payload(rows = [low, high], overrides = {}, coordinateEngineV2 = engine(rows)) {
  const observations = rows.map((row, index) => {
    const observation = {
    id: `line-${index + 1}`,
    source_ref: `line-${index + 1}`,
    source_line_id: `line-${index + 1}`,
    text: row,
    point_id: String(index + 1),
    bbox: index === 0 ? [20, 20, 500, 80] : [20, 500, 500, 560],
    coordinate_space: "ORIGINAL_IMAGE_PIXELS",
    source: "qwenOcr",
    source_type: "SYNTHETIC_REGRESSION_V1",
    source_role: index === 0 ? "MAP_SEARCH_BOX" : "MAP_PLACE_DETAILS",
    source_region_id: index === 0 ? "MAP_SEARCH_BOX_REGION" : "MAP_PLACE_DETAILS_REGION",
    provenance_trust: "SERVER_ATTESTED",
    provenance_attestor: "SYNTHETIC_REGRESSION_V1",
    measurement_semantics: "UNSPECIFIED",
    group_id: "group_1",
    ...overrides.observationOverrides?.[index]
    };
    if (observation.provenance_trust === "SERVER_ATTESTED") {
      Object.defineProperty(observation, SERVER_PROVENANCE_ATTESTATION, { value: true });
    }
    return observation;
  });
  const result = {
    success: true,
    request_asset_id: overrides.request_asset_id || syntheticImageIdentity.request_asset_id,
    image_id: overrides.image_id || syntheticImageIdentity.image_id,
    imageMetadata: syntheticImageIdentity,
    rawText: rows.join("\n"),
    coordinates: rows.join("\n"),
    ocrLineLocations: observations,
    ...overrides.payloadOverrides
  };
  const trustedLayoutAttestation = createTrustedLayoutAttestation({
    imageIdentity: result.imageMetadata,
    observations,
    coordinateEngineV2,
    resultRevision: overrides.revision || 1,
    providerResponseId: "synthetic-near-duplicate-response"
  });
  if (trustedLayoutAttestation) result.trustedLayoutAttestation = trustedLayoutAttestation;
  Object.defineProperty(result, SERVER_PROVENANCE_ATTESTATION, { value: true, enumerable: true });
  return result;
}

function prepare(rows = [low, high], overrides = {}) {
  const coordinateEngineV2 = overrides.coordinateEngineV2 || engine(rows);
  const recognitionResult = payload(rows, overrides, coordinateEngineV2);
  const evidenceAcquisition = buildEvidenceAcquisition({
    recognitionResult,
    coordinateEngineV2,
    resultRevision: overrides.revision || 1
  });
  const baseInput = {
    recognitionResult,
    coordinateEngineV2,
    evidenceAcquisition,
    revision: overrides.revision || 1
  };
  if (overrides.geometryIntent === false) return baseInput;
  const evaluated = evaluateWgs84NearDuplicateConsolidation(baseInput);
  if (evaluated.decision?.decision !== NEAR_DUPLICATE_DECISION.SAME_LOCATION_CONFIRMED) return baseInput;
  const runtime = new PointGeometryIntentReviewRuntime({ now: () => 1_000 });
  const review = runtime.issue({
    imageIdentity: recognitionResult.imageMetadata,
    trustedLayoutAttestation: recognitionResult.trustedLayoutAttestation,
    nearDuplicateDecision: evaluated.decision,
    canonicalPoints: evaluated.canonicalPoints,
    coordinateEngineV2,
    resultId: overrides.resultId || "near-duplicate-fixture",
    resultRevision: overrides.revision || 1
  });
  assert.ok(review);
  const accepted = runtime.accept({
    reviewId: review.review_id,
    reviewBindingSha256: review.review_binding_sha256,
    resultId: review.result_id,
    resultRevision: review.result_revision,
    geometryType: "Point",
    action: "accept_point"
  });
  assert.equal(accepted.ok, true);
  const withIntent = { ...recognitionResult, trustedPointGeometryIntent: accepted.intent };
  return {
    recognitionResult: withIntent,
    coordinateEngineV2,
    evidenceAcquisition,
    revision: overrides.revision || 1
  };
}

const cases = [];
function test(id, name, fn) {
  cases.push({ id, name, fn });
}

test("ND-01", "incident fixture selects the strictly higher precision observation", () => {
  const input = prepare();
  const result = evaluateWgs84NearDuplicateConsolidation(input);
  assert.equal(result.decision.decision, NEAR_DUPLICATE_DECISION.SAME_LOCATION_CONFIRMED);
  assert.equal(result.geometryIntentGate.decision, "AUTHORIZED");
  assert.equal(result.canonicalPoints.length, 1);
  assert.equal(result.canonicalPoints[0].raw, high);
  assert.equal(result.canonicalPoints[0].lat, 35.4478191);
  assert.equal(result.canonicalPoints[0].lon, 83.1789913);
});

test("ND-01B", "Production WGS84 table route reaches the same near-duplicate authority", () => {
  const coordinateEngineV2 = engine([low, high], {
    coordinateType: "decimal_latlon",
    precisionMode: "wgs84-table-coordinates"
  });
  const result = evaluateWgs84NearDuplicateConsolidation(prepare([low, high], {
    coordinateEngineV2
  }));
  assert.equal(result.decision.decision, NEAR_DUPLICATE_DECISION.SAME_LOCATION_CONFIRMED);
  assert.equal(result.geometryIntentGate.decision, "AUTHORIZED");
  assert.equal(result.canonicalPoints.length, 1);
  assert.equal(result.canonicalPoints[0].raw, high);
});

test("ND-01C", "unregistered decimal modes do not enter the WGS84 near-duplicate gate", () => {
  const coordinateEngineV2 = engine([low, high], {
    coordinateType: "decimal_latlon",
    precisionMode: "generic-decimal"
  });
  const result = evaluateWgs84NearDuplicateConsolidation(prepare([low, high], {
    coordinateEngineV2
  }));
  assert.equal(result.applies, false);
  assert.equal(result.canonicalPoints.length, 2);
});

test("ND-01D", "Production WGS84 table near-duplicates cannot become a LineString without trusted layout", () => {
  const coordinateEngineV2 = engine([low, high], {
    coordinateType: "decimal_latlon",
    precisionMode: "wgs84-table-coordinates"
  });
  const input = prepare([low, high], {
    coordinateEngineV2,
    observationOverrides: [{ provenance_trust: "UNTRUSTED" }, { provenance_trust: "UNTRUSTED" }]
  });
  const result = evaluateWgs84NearDuplicateConsolidation(input);
  assert.equal(result.decision.decision, NEAR_DUPLICATE_DECISION.PROVENANCE_INSUFFICIENT);
  assert.equal(result.geometryIntentGate.decision, "BLOCKED");
  assert.equal(result.authorityBlocked, true);
  assert.equal(result.canonicalPoints.length, 2);
});

test("ND-02", "missing trusted provenance fails closed", () => {
  const input = prepare([low, high], {
    observationOverrides: [{ provenance_trust: "UNTRUSTED" }, { provenance_trust: "UNTRUSTED" }]
  });
  const result = evaluateWgs84NearDuplicateConsolidation(input);
  assert.equal(result.decision.decision, NEAR_DUPLICATE_DECISION.PROVENANCE_INSUFFICIENT);
  assert.equal(result.authorityBlocked, true);
});

test("ND-03", "distance never substitutes for exact rounding containment", () => {
  const result = evaluateWgs84NearDuplicateConsolidation(prepare([
    "35.4478190,83.1789910",
    "35.4478194,83.1789914"
  ]));
  assert.equal(result.decision.decision, NEAR_DUPLICATE_DECISION.DISTINCT_POINTS);
  assert.equal(result.authorityBlocked, true);
});

test("ND-03B", "candidate count cannot mint LineString authority", () => {
  const result = evaluateWgs84NearDuplicateConsolidation(prepare([
    "35.4478190,83.1789910",
    "35.4478290,83.1790010"
  ]));
  assert.equal(result.decision.decision, NEAR_DUPLICATE_DECISION.DISTINCT_POINTS);
  assert.equal(result.geometryIntentGate.decision, "BLOCKED");
  assert.equal(result.authorityBlocked, true);
});

test("ND-03C", "LineString intent cannot bypass provenance or CRS identity", () => {
  const input = prepare([
    "35.4478190,83.1789910",
    "35.4478290,83.1790010"
  ], {
    observationOverrides: [{ provenance_trust: "UNTRUSTED" }, {}]
  });
  input.coordinateEngineV2.source_crs = { id: "EPSG:3857", axisOrder: "easting_northing" };
  const result = evaluateWgs84NearDuplicateConsolidation(input);
  assert.equal(result.decision.decision, NEAR_DUPLICATE_DECISION.PROVENANCE_INSUFFICIENT);
  assert.ok(result.decision.reason_codes.includes("CRS_OR_AXIS_CONFLICT"));
  assert.ok(result.decision.reason_codes.includes("PROVENANCE_MISSING_OR_MALFORMED"));
  assert.equal(result.authorityBlocked, true);
});

test("ND-04", "distinct measurement labels veto consolidation", () => {
  const result = evaluateWgs84NearDuplicateConsolidation(prepare([low, high], {
    observationOverrides: [{ semantic_label: "A" }, { semantic_label: "B" }]
  }));
  assert.equal(result.decision.decision, NEAR_DUPLICATE_DECISION.PROVENANCE_INSUFFICIENT);
  assert.ok(result.decision.reason_codes.includes("DISTINCT_MEASUREMENT_SEMANTICS"));
});

test("ND-05", "asset and page identity conflict veto consolidation", () => {
  const result = evaluateWgs84NearDuplicateConsolidation(prepare([low, high], {
    observationOverrides: [{ request_asset_id: "asset-a" }, { request_asset_id: "asset-b" }]
  }));
  assert.ok(result.decision.reason_codes.includes("ASSET_IMAGE_OR_PAGE_CONFLICT"));
});

test("ND-06", "same OCR line across engines cannot authorize sameness", () => {
  const result = evaluateWgs84NearDuplicateConsolidation(prepare([low, high], {
    observationOverrides: [{ source_line_id: "same-line" }, { source_line_id: "same-line" }]
  }));
  assert.ok(result.decision.reason_codes.includes("SAME_OCR_LINE_MULTI_ENGINE"));
});

test("ND-06B", "negative original-image bbox cannot authorize sameness", () => {
  const result = evaluateWgs84NearDuplicateConsolidation(prepare([low, high], {
    observationOverrides: [{ bbox: [-1, 20, 500, 80] }, {}]
  }));
  assert.equal(result.decision.decision, NEAR_DUPLICATE_DECISION.PROVENANCE_INSUFFICIENT);
  assert.ok(result.decision.reason_codes.includes("PROVENANCE_MISSING_OR_MALFORMED"));
});

test("ND-06B2", "zero-area original-image bbox cannot authorize sameness", () => {
  const result = evaluateWgs84NearDuplicateConsolidation(prepare([low, high], {
    observationOverrides: [{ bbox: [20, 20, 20, 80] }, {}]
  }));
  assert.equal(result.decision.decision, NEAR_DUPLICATE_DECISION.PROVENANCE_INSUFFICIENT);
  assert.ok(result.decision.reason_codes.includes("PROVENANCE_MISSING_OR_MALFORMED"));
});

test("ND-06C", "missing explicit page identity cannot authorize sameness", () => {
  const result = evaluateWgs84NearDuplicateConsolidation(prepare([low, high], {
    payloadOverrides: { imageMetadata: { width: 1080, height: 1920 } },
    observationOverrides: [{ page: null }, { page: null }]
  }));
  assert.equal(result.decision.decision, NEAR_DUPLICATE_DECISION.PROVENANCE_INSUFFICIENT);
  assert.ok(result.decision.reason_codes.includes("PROVENANCE_MISSING_OR_MALFORMED"));
});

test("ND-06D", "different source roles without distinct source regions cannot authorize sameness", () => {
  const result = evaluateWgs84NearDuplicateConsolidation(prepare([low, high], {
    observationOverrides: [
      { source_region_id: "MAP_SEARCH_BOX_REGION" },
      { source_region_id: "MAP_SEARCH_BOX_REGION" }
    ]
  }));
  assert.equal(result.decision.decision, NEAR_DUPLICATE_DECISION.PROVENANCE_INSUFFICIENT);
  assert.ok(result.decision.reason_codes.includes("PROVENANCE_MISSING_OR_MALFORMED"));
});

test("ND-06E", "source role and source region must be a strict pair", () => {
  const result = evaluateWgs84NearDuplicateConsolidation(prepare([low, high], {
    observationOverrides: [{ source_region_id: "MAP_PLACE_DETAILS_REGION" }, {}]
  }));
  assert.equal(result.decision.decision, NEAR_DUPLICATE_DECISION.PROVENANCE_INSUFFICIENT);
  assert.ok(result.decision.reason_codes.includes("PROVENANCE_MISSING_OR_MALFORMED"));
});

test("ND-06F", "ordinary provenance strings cannot replace the original-observation capability", () => {
  const input = prepare();
  const evidenceWithoutCapabilities = structuredClone(input.evidenceAcquisition);
  const result = evaluateWgs84NearDuplicateConsolidation({ ...input, evidenceAcquisition: evidenceWithoutCapabilities });
  assert.equal(result.decision.decision, NEAR_DUPLICATE_DECISION.PROVENANCE_INSUFFICIENT);
  assert.ok(result.decision.reason_codes.includes("PROVENANCE_MISSING_OR_MALFORMED"));
});

test("ND-06G", "ordinary geometry-intent attestor strings cannot mint authority", () => {
  const input = prepare();
  const recognitionWithoutCapability = structuredClone(input.recognitionResult);
  const result = evaluateWgs84NearDuplicateConsolidation({
    ...input,
    recognitionResult: recognitionWithoutCapability
  });
  assert.equal(result.decision.decision, NEAR_DUPLICATE_DECISION.SAME_LOCATION_CONFIRMED);
  assert.equal(result.geometryIntentGate.decision, "BLOCKED");
});

test("ND-06H", "different role labels cannot authorize identical source-region bboxes", () => {
  const result = evaluateWgs84NearDuplicateConsolidation(prepare([low, high], {
    observationOverrides: [{ bbox: [20, 20, 500, 80] }, { bbox: [20, 20, 500, 80] }]
  }));
  assert.equal(result.decision.decision, NEAR_DUPLICATE_DECISION.PROVENANCE_INSUFFICIENT);
  assert.ok(result.decision.reason_codes.includes("SOURCE_REGIONS_SPATIALLY_OVERLAP"));
  assert.equal(result.authorityBlocked, true);
});

test("ND-06I", "partially overlapping source-region bboxes fail closed", () => {
  const result = evaluateWgs84NearDuplicateConsolidation(prepare([low, high], {
    observationOverrides: [{ bbox: [20, 20, 500, 100] }, { bbox: [400, 80, 700, 160] }]
  }));
  assert.equal(result.decision.decision, NEAR_DUPLICATE_DECISION.PROVENANCE_INSUFFICIENT);
  assert.ok(result.decision.reason_codes.includes("SOURCE_REGIONS_SPATIALLY_OVERLAP"));
  assert.equal(result.authorityBlocked, true);
});

test("ND-07", "unproven three-observation cluster requires review", () => {
  const result = evaluateWgs84NearDuplicateConsolidation(prepare([low, high, "35.44781912,83.17899131"]));
  assert.equal(result.decision.decision, NEAR_DUPLICATE_DECISION.REVIEW_REQUIRED);
  assert.equal(result.authorityBlocked, true);
});

test("ND-07B", "two observations across groups never consolidate and every group fails closed", () => {
  const input = prepare();
  input.coordinateEngineV2.groups = [
    { ...input.coordinateEngineV2.groups[0], group_id: "group_1", points: [input.coordinateEngineV2.groups[0].points[0]], geometry: "point" },
    { ...input.coordinateEngineV2.groups[0], group_id: "group_2", points: [input.coordinateEngineV2.groups[0].points[1]], geometry: "point" }
  ];
  const result = applyWgs84NearDuplicateAuthority(input);
  assert.equal(result.evaluation.decision.decision, NEAR_DUPLICATE_DECISION.REVIEW_REQUIRED);
  assert.ok(result.evaluation.decision.reason_codes.includes("CROSS_GROUP_NEAR_DUPLICATE_AUTHORITY_UNPROVEN"));
  assert.deepEqual(result.coordinateEngineV2.groups.map(group => group.points.length), [1, 1]);
  assert.ok(result.coordinateEngineV2.groups.every(group => group.requires_review === true && group.kml_ready === false));
});

test("ND-08", "near-duplicate decision cannot decide geometry without independent intent", () => {
  const result = evaluateWgs84NearDuplicateConsolidation(prepare([low, high], { geometryIntent: false }));
  assert.equal(result.decision.decision, NEAR_DUPLICATE_DECISION.SAME_LOCATION_CONFIRMED);
  assert.equal(result.geometryIntentGate.decision, "BLOCKED");
  assert.equal(result.authorityBlocked, true);
});

test("ND-09", "derived authority does not mutate shadow-only evidence", () => {
  const input = prepare();
  const snapshot = structuredClone(input.evidenceAcquisition);
  evaluateWgs84NearDuplicateConsolidation(input);
  assert.deepEqual(input.evidenceAcquisition, snapshot);
  assert.equal(input.evidenceAcquisition.shadow_only, true);
  assert.equal(input.evidenceAcquisition.affects_coordinates, false);
  assert.equal(input.evidenceAcquisition.affects_kml, false);
});

test("ND-10", "forged decision digest fails validation", () => {
  const result = evaluateWgs84NearDuplicateConsolidation(prepare());
  const forged = { ...result.decision, binding: { ...result.decision.binding, canonical_observation_id: "forged" } };
  const validation = validateWgs84NearDuplicateAuthority({
    decision: forged,
    geometryIntentGate: result.geometryIntentGate,
    resultRevision: 1
  });
  assert.equal(validation.valid, false);
  assert.equal(validation.blocked, true);
});

test("ND-11", "revision changes invalidate both same-location and geometry-intent confirmation bindings", () => {
  const result = evaluateWgs84NearDuplicateConsolidation(prepare());
  const validation = validateWgs84NearDuplicateAuthority({
    decision: result.decision,
    geometryIntentGate: result.geometryIntentGate,
    resultRevision: 2
  });
  assert.equal(validation.valid, false);
  assert.equal(validation.blocked, true);
});

test("ND-12", "authorized decision reaches Finalizer as one Point and survives confirmation", () => {
  const input = prepare();
  const applied = applyWgs84NearDuplicateAuthority(input);
  assert.equal(applied.coordinateEngineV2.groups[0].points.length, 1);
  assert.equal(applied.coordinateEngineV2.groups[0].geometry, "point");
  const candidate = createLegacyFinalizerInput({
    recognitionResult: applied.recognitionResult,
    coordinateEngineV2: applied.coordinateEngineV2,
    verification: { status: "PASS", warnings: [] },
    revision: { resultId: "near-duplicate-fixture", resultRevision: 1, confirmationStatus: "pending" }
  });
  const finalized = finalizeCoordinateResult(candidate, { clock: () => "2026-09-17T00:00:00.000Z" });
  assert.equal(finalized.geometry.type, "Point");
  assert.deepEqual(finalized.geometry.coordinates, [83.1789913, 35.4478191]);
  assert.equal(finalized.nearDuplicateDecision.decision, NEAR_DUPLICATE_DECISION.SAME_LOCATION_CONFIRMED);
  const runtime = new CoordinateConfirmationRuntime({ now: () => 1_000 });
  runtime.register(finalized);
  const confirmed = runtime.confirm({
    resultId: finalized.resultId,
    resultRevision: finalized.resultRevision,
    geometryHash: finalized.geometryHash,
    action: "accept"
  });
  assert.equal(confirmed.ok, true);
  assert.equal(confirmed.finalizedCoordinateResult.confirmationStatus, COORDINATE_CONFIRMATION_STATUS.ACCEPTED);
  assert.equal(confirmed.finalizedCoordinateResult.geometry.type, "Point");
  const preview = new MapPreviewAdapter().adapt(confirmed.finalizedCoordinateResult);
  assert.equal(preview.previewEligibility.allowed, true);
  assert.deepEqual(preview.geometry.coordinates, [83.1789913, 35.4478191]);
  const kmlConsumption = consumeFinalizedGeometry(
    confirmed.finalizedCoordinateResult,
    geometry => geometry.coordinates
  );
  assert.equal(kmlConsumption.consumed, true);
  assert.deepEqual(kmlConsumption.value, [83.1789913, 35.4478191]);
});

test("ND-12B", "authority provenance changes invalidate an old confirmation identity", () => {
  const finalizePrepared = (input) => {
    const applied = applyWgs84NearDuplicateAuthority(input);
    return finalizeCoordinateResult(createLegacyFinalizerInput({
      recognitionResult: applied.recognitionResult,
      coordinateEngineV2: applied.coordinateEngineV2,
      verification: { status: "PASS", warnings: [] },
      revision: { resultId: "near-duplicate-authority-rebind", resultRevision: 1, confirmationStatus: "pending" }
    }), { clock: () => "2026-09-17T00:00:00.000Z" });
  };
  const first = finalizePrepared(prepare([low, high], { resultId: "near-duplicate-authority-rebind" }));
  const second = finalizePrepared(prepare([low, high], {
    resultId: "near-duplicate-authority-rebind",
    observationOverrides: [{ bbox: [30, 20, 510, 80] }, {}]
  }));
  assert.deepEqual(first.geometry.coordinates, second.geometry.coordinates);
  assert.notEqual(first.nearDuplicateDecision.binding.provenance_sha256, second.nearDuplicateDecision.binding.provenance_sha256);
  assert.notEqual(first.geometryHash, second.geometryHash);
  const runtime = new CoordinateConfirmationRuntime({ now: () => 1_000 });
  runtime.register(first);
  runtime.register(second);
  const stale = runtime.confirm({
    resultId: first.resultId,
    resultRevision: first.resultRevision,
    geometryHash: first.geometryHash,
    action: "accept"
  });
  assert.equal(stale.ok, false);
  assert.equal(stale.code, "GEOMETRY_HASH_MISMATCH");
});

test("ND-13", "blocked authority never reaches Map or KML geometry", () => {
  const input = prepare([low, high], { geometryIntent: false });
  const applied = applyWgs84NearDuplicateAuthority(input);
  const candidate = createLegacyFinalizerInput({
    recognitionResult: applied.recognitionResult,
    coordinateEngineV2: applied.coordinateEngineV2,
    verification: { status: "REVIEW", warnings: [] },
    revision: { resultId: "blocked-near-duplicate", resultRevision: 1 }
  });
  const finalized = finalizeCoordinateResult(candidate);
  assert.equal(finalized.geometry, null);
  assert.equal(finalized.kmlReady, false);
  assert.equal(finalized.decisionState, "BLOCKED");
});

test("ND-14", "runtime response path applies authority before verification and Finalizer", () => {
  const source = fs.readFileSync("server.js", "utf8");
  const wrapperStart = source.indexOf("function buildCoordinateVerificationResponse(payload = {}, coordinateEngineV2 = null, finalizerOptions = {}) {");
  const aliasStart = source.indexOf("const buildCoordinateVerificationResponseWithoutRecognitionBudget");
  assert.ok(wrapperStart >= 0 && aliasStart > wrapperStart);
  const wrapper = source.slice(wrapperStart, aliasStart);
  assert.match(wrapper, /buildEvidenceAcquisition/);
  assert.match(wrapper, /applyWgs84NearDuplicateAuthority/);
  assert.match(wrapper, /buildCoordinateVerificationResponseBase/);
  assert.match(wrapper, /pointGeometryIntentReviewRuntime\.issue/);
  assert.match(wrapper, /pointGeometryIntentReview/);
  assert.match(source, /runLocalOcrMapLayoutClassification/);
  assert.match(source, /imageBuffer: req\.file\.buffer/);
  assert.match(source, /localOcrStructuredLayoutRows = await/);
  assert.match(wrapper, /observations: payload\.localOcrStructuredLayoutRows/);
});

test("ND-15", "missing Provider layout classifier profile remains shadow-only and authority-blocked", () => {
  const coordinateEngineV2 = engine();
  const providerLayoutCandidates = extractProviderLayoutCandidates({ output: { layout: [
    { id: "search", line_id: "line-search", text: low, bbox: [20, 20, 500, 80] },
    { id: "details", line_id: "line-details", text: high, bbox: [20, 500, 500, 560] }
  ] } });
  const outcome = classifyProviderLayoutRoles({
    profile: null,
    imageIdentity: syntheticImageIdentity,
    candidates: providerLayoutCandidates,
    providerResponseId: "synthetic-no-profile",
    resultRevision: 1
  });
  const recognitionResult = {
    imageMetadata: syntheticImageIdentity,
    providerLayoutCandidates,
    providerLayoutClassificationOutcome: outcome
  };
  const evidenceAcquisition = buildEvidenceAcquisition({ recognitionResult, coordinateEngineV2, resultRevision: 1 });
  assert.equal(evidenceAcquisition.trusted_layout_reason, "TRUSTED_LAYOUT_CLASSIFIER_EVIDENCE_MISSING");
  assert.equal(evidenceAcquisition.trusted_layout_status, "UNATTESTED");
  assert.equal(evidenceAcquisition.shadow_only, true);
  assert.notEqual(evidenceAcquisition.trusted_layout_status, "ATTESTED");
});

test("ND-16", "qualification candidate alone leaves Review, Map and KML fail-closed", () => {
  const qualification = collectProviderLayoutProfileQualification({
    response: { id: "qualification-only", output: { layout: [
      { candidate_schema_version: "provider_layout_candidate_v1", id: "search", line_id: "line-search", source_ref: "search", source_line_id: "line-search", text: low, bbox: [20, 20, 500, 80], coordinate_space: "ORIGINAL_IMAGE_PIXELS", page: 1, image_width: 1080, image_height: 1920 },
      { candidate_schema_version: "provider_layout_candidate_v1", id: "details", line_id: "line-details", source_ref: "details", source_line_id: "line-details", text: high, bbox: [20, 500, 500, 560], coordinate_space: "ORIGINAL_IMAGE_PIXELS", page: 1, image_width: 1080, image_height: 1920 }
    ] } },
    providerId: "ALIYUN_DASHSCOPE",
    modelName: "qwen-vl-plus",
    responseContractId: "DASHSCOPE_STRUCTURED_LAYOUT_V1",
    providerResponseId: "qualification-only",
    imageIdentity: syntheticImageIdentity,
    resultRevision: 1
  });
  assert.equal(qualification.status, "QUALIFICATION_CANDIDATE");
  const input = prepare([low, high], { geometryIntent: false });
  input.recognitionResult = { ...input.recognitionResult, providerLayoutProfileQualification: qualification };
  const applied = applyWgs84NearDuplicateAuthority(input);
  const finalized = finalizeCoordinateResult(createLegacyFinalizerInput({
    recognitionResult: applied.recognitionResult,
    coordinateEngineV2: applied.coordinateEngineV2,
    verification: { status: "REVIEW", warnings: [] },
    revision: { resultId: "qualification-only", resultRevision: 1 }
  }));
  assert.equal(finalized.geometry, null);
  assert.equal(finalized.kmlReady, false);
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
  suite: "wgs84-near-duplicate-consolidation-p0-regression",
  incident: "GJ-COORD-MAP-SCREENSHOT-DUPLICATE-001",
  passed,
  total: cases.length,
  providerCalls: 0,
  productionWrites: 0
}, null, 2));
