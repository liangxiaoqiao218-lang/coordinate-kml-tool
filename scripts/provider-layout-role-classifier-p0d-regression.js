import assert from "node:assert/strict";
import {
  PROVIDER_LAYOUT_CLASSIFICATION_REASON,
  buildEvidenceAcquisition,
  classifyProviderLayoutRoles,
  createServerClassifiedLayoutRows,
  createServerOwnedLayoutClassifierProfile,
  createTrustedLayoutAttestation,
  extractProviderLayoutCandidates,
  getProductionProviderLayoutClassifierProfile,
  getProviderLayoutRoleClassificationFailureReason,
  validateProviderLayoutRoleClassification
} from "../server/evidence-acquisition/index.js";
import { createCoordinateImageIdentity } from "../server/recognition/coordinate-image-safety.js";
import { evaluateWgs84NearDuplicateConsolidation } from "../server/recognition/wgs84-near-duplicate-consolidation.js";

const low = "35.447819,83.178991";
const high = "35.4478191,83.1789913";
const providerResponseId = "synthetic-provider-response-p0d";

function makeBmp(width = 120, height = 100) {
  const rowBytes = Math.floor(((24 * width) + 31) / 32) * 4;
  const pixelBytes = rowBytes * height;
  const buffer = Buffer.alloc(54 + pixelBytes);
  buffer.write("BM", 0, "ascii");
  buffer.writeUInt32LE(buffer.length, 2);
  buffer.writeUInt32LE(54, 10);
  buffer.writeUInt32LE(40, 14);
  buffer.writeInt32LE(width, 18);
  buffer.writeInt32LE(height, 22);
  buffer.writeUInt16LE(1, 26);
  buffer.writeUInt16LE(24, 28);
  buffer.writeUInt32LE(pixelBytes, 34);
  return buffer;
}

const imageIdentity = createCoordinateImageIdentity(
  { buffer: makeBmp(), mimetype: "image/bmp" },
  { requestId: "provider-layout-classifier-p0d", page: 1 }
);

function response(overrides = {}) {
  const rows = overrides.rows || [
    { id: "search", line_id: "line-search", text: low, bbox: [5, 5, 115, 25], source_role: "MAP_SEARCH_BOX" },
    { id: "details", line_id: "line-details", text: high, bbox: [5, 55, 115, 75], source_role: "MAP_PLACE_DETAILS" }
  ];
  return {
    id: providerResponseId,
    message: { content: JSON.stringify({ role: "MAP_SEARCH_BOX", bbox: [0, 0, 120, 100] }) },
    output: { layout: rows }
  };
}

function candidates(overrides = {}) {
  return extractProviderLayoutCandidates(response(overrides));
}

function profile(overrides = {}) {
  return createServerOwnedLayoutClassifierProfile({
    profileId: overrides.profileId ?? "SYNTHETIC_LAYOUT_ROLE_PROFILE_V1",
    classifierVersion: overrides.classifierVersion ?? "P0D_SYNTHETIC_V1",
    classify: overrides.classify || (() => [
      { source_ref: "search", source_role: "MAP_SEARCH_BOX" },
      { source_ref: "details", source_role: "MAP_PLACE_DETAILS" }
    ])
  });
}

function engine(rows = [low, high], overrides = {}) {
  return {
    coordinate_type: "wgs84_chat_coordinates",
    precision_mode: "wgs84-chat-coordinates",
    source_crs: overrides.source_crs || { id: "EPSG:4326", axisOrder: "latitude_longitude" },
    groups: [{
      group_id: "group_1",
      geometry: "line",
      points: rows.map((raw, index) => {
        const [lat, lon] = raw.split(",").map(Number);
        return { label: String(index + 1), raw, lat, lon };
      })
    }]
  };
}

function classify(options = {}) {
  const candidateSet = options.candidates || candidates();
  return classifyProviderLayoutRoles({
    profile: options.profile === undefined ? profile() : options.profile,
    imageIdentity: options.imageIdentity || imageIdentity,
    candidates: candidateSet,
    providerResponseId: options.providerResponseId ?? providerResponseId,
    resultRevision: options.resultRevision || 1
  });
}

function closeLoop(options = {}) {
  const candidateSet = options.candidates || candidates();
  const outcome = classify({ ...options, candidates: candidateSet });
  const rows = outcome.ok ? createServerClassifiedLayoutRows({
    candidates: candidateSet,
    classification: outcome.classification,
    imageIdentity: options.imageIdentity || imageIdentity,
    providerResponseId: options.providerResponseId ?? providerResponseId,
    resultRevision: options.resultRevision || 1
  }) : null;
  const coordinateEngineV2 = options.coordinateEngineV2 || engine();
  const attestation = rows ? createTrustedLayoutAttestation({
    imageIdentity: options.imageIdentity || imageIdentity,
    observations: rows,
    coordinateEngineV2,
    resultRevision: options.resultRevision || 1,
    providerResponseId: options.providerResponseId ?? providerResponseId,
    providerLayoutClassification: outcome.classification
  }) : null;
  return { candidateSet, outcome, rows, attestation, coordinateEngineV2 };
}

const tests = [];
function test(id, name, run) {
  tests.push({ id, name, run });
}

test("P0D-01", "valid in-process server classification capability closes the layout bridge", () => {
  const result = closeLoop();
  assert.equal(result.outcome.ok, true);
  assert.ok(result.rows);
  assert.equal(result.rows.map(row => row.source_role).join(","), "MAP_SEARCH_BOX,MAP_PLACE_DETAILS");
  assert.ok(result.attestation);
});

test("P0D-02", "Production has no preinstalled classifier profile and fails with the fixed reason", () => {
  assert.equal(getProductionProviderLayoutClassifierProfile(), null);
  const outcome = classify({ profile: null });
  assert.equal(outcome.ok, false);
  assert.equal(getProviderLayoutRoleClassificationFailureReason(outcome),
    PROVIDER_LAYOUT_CLASSIFICATION_REASON.EVIDENCE_MISSING);
});

test("P0D-03", "Provider role claims and free-form content remain untrusted", () => {
  const values = candidates();
  assert.equal(values.every(value => value.source_role === null), true);
  assert.equal(values.every(value => value.provenance_trust === "UNTRUSTED"), true);
  assert.equal(values.some(value => value.text.includes("must-not-be-trusted")), false);
});

test("P0D-04", "ordinary classifications array cannot mint server-classified rows", () => {
  assert.equal(createServerClassifiedLayoutRows({
    candidates: candidates(),
    classifications: [
      { source_ref: "search", source_role: "MAP_SEARCH_BOX" },
      { source_ref: "details", source_role: "MAP_PLACE_DETAILS" }
    ],
    imageIdentity,
    providerResponseId,
    resultRevision: 1
  }), null);
});

test("P0D-05", "JSON and structuredClone lose the classification capability", () => {
  const outcome = classify();
  for (const clone of [JSON.parse(JSON.stringify(outcome.classification)), structuredClone(outcome.classification)]) {
    assert.equal(validateProviderLayoutRoleClassification({
      classification: clone,
      imageIdentity,
      candidates: candidates(),
      providerResponseId,
      resultRevision: 1
    }).valid, false);
  }
});

test("P0D-06", "ordinary or cloned profiles cannot invoke the server classifier", () => {
  const ordinary = { profile_id: "SYNTHETIC_LAYOUT_ROLE_PROFILE_V1", classifier_version: "P0D_SYNTHETIC_V1", classify: () => [] };
  assert.equal(classify({ profile: ordinary }).reason, PROVIDER_LAYOUT_CLASSIFICATION_REASON.EVIDENCE_MISSING);
  assert.equal(classify({ profile: structuredClone(profile()) }).reason, PROVIDER_LAYOUT_CLASSIFICATION_REASON.EVIDENCE_MISSING);
});

test("P0D-07", "unknown profile output and classifier version cannot be substituted", () => {
  assert.equal(profile({ classifierVersion: "" }), null);
  assert.equal(profile({ profileId: "UNKNOWN_PROFILE" }), null);
  const invalidOutput = classify({ profile: profile({ classify: () => [{ source_ref: "search", source_role: "UNKNOWN" }] }) });
  assert.equal(invalidOutput.reason, PROVIDER_LAYOUT_CLASSIFICATION_REASON.OUTPUT_INVALID);
});

test("P0D-08", "missing, repeated or ambiguous source references fail closed", () => {
  const missing = candidates({ rows: [
    { id: "", line_id: "line-search", text: low, bbox: [5, 5, 115, 25] },
    { id: "details", line_id: "line-details", text: high, bbox: [5, 55, 115, 75] }
  ] });
  assert.equal(classify({ candidates: missing }).reason, PROVIDER_LAYOUT_CLASSIFICATION_REASON.CANDIDATE_SET_INVALID);
  const repeated = candidates({ rows: [
    { id: "same", line_id: "line-search", text: low, bbox: [5, 5, 115, 25] },
    { id: "same", line_id: "line-details", text: high, bbox: [5, 55, 115, 75] }
  ] });
  assert.equal(classify({ candidates: repeated }).reason, PROVIDER_LAYOUT_CLASSIFICATION_REASON.CANDIDATE_SET_INVALID);
});

test("P0D-09", "Provider response identity mismatch invalidates the bridge", () => {
  const result = closeLoop();
  assert.equal(createServerClassifiedLayoutRows({
    candidates: result.candidateSet,
    classification: result.outcome.classification,
    imageIdentity,
    providerResponseId: "other-provider-response",
    resultRevision: 1
  }), null);
});

test("P0D-10", "image bytes, dimensions, page and revision are bound", () => {
  const outcome = classify();
  const otherImage = createCoordinateImageIdentity(
    { buffer: makeBmp(121, 100), mimetype: "image/bmp" },
    { requestId: "other-image", page: 2 }
  );
  assert.equal(validateProviderLayoutRoleClassification({
    classification: outcome.classification,
    imageIdentity: otherImage,
    candidates: candidates(),
    providerResponseId,
    resultRevision: 1
  }).valid, false);
  assert.equal(validateProviderLayoutRoleClassification({
    classification: outcome.classification,
    imageIdentity,
    candidates: candidates(),
    providerResponseId,
    resultRevision: 2
  }).valid, false);
});

test("P0D-11", "out-of-bounds bbox and same source line fail closed", () => {
  const outside = candidates({ rows: [
    { id: "search", line_id: "line-search", text: low, bbox: [5, 5, 125, 25] },
    { id: "details", line_id: "line-details", text: high, bbox: [5, 55, 115, 75] }
  ] });
  assert.equal(classify({ candidates: outside }).reason, PROVIDER_LAYOUT_CLASSIFICATION_REASON.CANDIDATE_SET_INVALID);
  const sameLine = candidates({ rows: [
    { id: "search", line_id: "same", text: low, bbox: [5, 5, 115, 25] },
    { id: "details", line_id: "same", text: high, bbox: [5, 55, 115, 75] }
  ] });
  assert.equal(classify({ candidates: sameLine }).reason, PROVIDER_LAYOUT_CLASSIFICATION_REASON.CANDIDATE_SET_INVALID);
});

test("P0D-12", "overlapping bboxes and duplicate roles cannot become trusted layout", () => {
  const overlapping = candidates({ rows: [
    { id: "search", line_id: "line-search", text: low, bbox: [5, 5, 115, 60] },
    { id: "details", line_id: "line-details", text: high, bbox: [5, 55, 115, 75] }
  ] });
  const overlappingOutcome = classify({ candidates: overlapping });
  assert.equal(overlappingOutcome.reason, PROVIDER_LAYOUT_CLASSIFICATION_REASON.CANDIDATE_SET_INVALID);
  assert.equal(closeLoop({ candidates: overlapping }).rows, null);
  const duplicateRoles = classify({ profile: profile({ classify: () => [
    { source_ref: "search", source_role: "MAP_SEARCH_BOX" },
    { source_ref: "details", source_role: "MAP_SEARCH_BOX" }
  ] }) });
  assert.equal(duplicateRoles.reason, PROVIDER_LAYOUT_CLASSIFICATION_REASON.OUTPUT_INVALID);
});

test("P0D-13", "candidate text, order and bbox changes invalidate a prior classification", () => {
  const outcome = classify();
  const variants = [
    candidates({ rows: [
      { id: "search", line_id: "line-search", text: `${low}0`, bbox: [5, 5, 115, 25] },
      { id: "details", line_id: "line-details", text: high, bbox: [5, 55, 115, 75] }
    ] }),
    candidates({ rows: response().output.layout.slice().reverse() }),
    candidates({ rows: [
      { id: "search", line_id: "line-search", text: low, bbox: [6, 5, 115, 25] },
      { id: "details", line_id: "line-details", text: high, bbox: [5, 55, 115, 75] }
    ] })
  ];
  variants.forEach(candidateSet => assert.equal(validateProviderLayoutRoleClassification({
    classification: outcome.classification,
    imageIdentity,
    candidates: candidateSet,
    providerResponseId,
    resultRevision: 1
  }).valid, false));
});

test("P0D-14", "CRS, axis order or coordinate changes cannot reuse trusted layout authority", () => {
  const result = closeLoop();
  const recognitionResult = { imageMetadata: imageIdentity, trustedLayoutAttestation: result.attestation };
  const changedCoordinates = engine(["35.4478192,83.178991", high]);
  const changedEvidence = buildEvidenceAcquisition({ recognitionResult, coordinateEngineV2: changedCoordinates, resultRevision: 1 });
  assert.equal(changedEvidence.trusted_layout_status, "UNATTESTED");
  const changedCrs = engine([low, high], { source_crs: { id: "EPSG:3857", axisOrder: "easting_northing" } });
  const crsEvidence = buildEvidenceAcquisition({ recognitionResult, coordinateEngineV2: changedCrs, resultRevision: 1 });
  const decision = evaluateWgs84NearDuplicateConsolidation({
    recognitionResult,
    coordinateEngineV2: changedCrs,
    evidenceAcquisition: crsEvidence,
    revision: 1
  });
  assert.equal(decision.authorityBlocked, true);
});

test("P0D-15", "trusted layout remains shadow-only and cannot create coordinate authority", () => {
  const result = closeLoop();
  const recognitionResult = { imageMetadata: imageIdentity, trustedLayoutAttestation: result.attestation };
  const evidence = buildEvidenceAcquisition({ recognitionResult, coordinateEngineV2: result.coordinateEngineV2, resultRevision: 1 });
  assert.equal(evidence.shadow_only, true);
  assert.equal(evidence.affects_coordinates, false);
  assert.equal(evidence.affects_kml, false);
});

test("P0D-16", "trusted layout cannot replace explicit Point intent or final confirmation", () => {
  const result = closeLoop();
  const recognitionResult = { imageMetadata: imageIdentity, trustedLayoutAttestation: result.attestation };
  const evidenceAcquisition = buildEvidenceAcquisition({ recognitionResult, coordinateEngineV2: result.coordinateEngineV2, resultRevision: 1 });
  const decision = evaluateWgs84NearDuplicateConsolidation({
    recognitionResult,
    coordinateEngineV2: result.coordinateEngineV2,
    evidenceAcquisition,
    revision: 1
  });
  assert.equal(decision.decision.decision, "SAME_LOCATION_CONFIRMED");
  assert.equal(decision.geometryIntentGate.decision, "BLOCKED");
  assert.equal(decision.authorityBlocked, true);
});

test("P0D-17", "trusted classifier failure reason is process-bound and preserved in evidence", () => {
  const outcome = classify({ profile: null });
  const recognitionResult = {
    imageMetadata: imageIdentity,
    providerLayoutCandidates: candidates(),
    providerLayoutClassificationOutcome: outcome
  };
  const evidence = buildEvidenceAcquisition({ recognitionResult, coordinateEngineV2: engine(), resultRevision: 1 });
  assert.equal(evidence.trusted_layout_status, "UNATTESTED");
  assert.equal(evidence.trusted_layout_reason, PROVIDER_LAYOUT_CLASSIFICATION_REASON.EVIDENCE_MISSING);
  const forged = structuredClone(outcome);
  assert.equal(getProviderLayoutRoleClassificationFailureReason(forged), null);
});

test("P0D-18", "server-classified rows cannot cross response, image or revision boundaries", () => {
  const result = closeLoop();
  const otherImage = createCoordinateImageIdentity(
    { buffer: makeBmp(121, 100), mimetype: "image/bmp" },
    { requestId: "cross-stage-other-image", page: 1 }
  );
  const attempts = [
    { imageIdentity, providerResponseId: "other-response", resultRevision: 1 },
    { imageIdentity: otherImage, providerResponseId, resultRevision: 1 },
    { imageIdentity, providerResponseId, resultRevision: 2 }
  ];
  attempts.forEach(attempt => assert.equal(createTrustedLayoutAttestation({
    ...attempt,
    observations: result.rows,
    coordinateEngineV2: result.coordinateEngineV2,
    providerLayoutClassification: result.outcome.classification
  }), null));
});

test("P0D-19", "server-classified rows are immutable and clones lose every in-process capability", () => {
  const result = closeLoop();
  assert.equal(Object.isFrozen(result.rows), true);
  assert.equal(result.rows.every(Object.isFrozen), true);
  assert.throws(() => { result.rows[0].source_role = "MAP_PLACE_DETAILS"; }, TypeError);
  const clonedRows = structuredClone(result.rows);
  assert.equal(createTrustedLayoutAttestation({
    imageIdentity,
    observations: clonedRows,
    coordinateEngineV2: result.coordinateEngineV2,
    providerResponseId,
    resultRevision: 1,
    providerLayoutClassification: result.outcome.classification
  }), null);
  const clonedClassification = structuredClone(result.outcome.classification);
  assert.equal(createTrustedLayoutAttestation({
    imageIdentity,
    observations: result.rows,
    coordinateEngineV2: result.coordinateEngineV2,
    providerResponseId,
    resultRevision: 1,
    providerLayoutClassification: clonedClassification
  }), null);
});

let passed = 0;
for (const item of tests) {
  try {
    item.run();
    passed += 1;
    console.log(`PASS ${item.id} ${item.name}`);
  } catch (error) {
    console.error(`FAIL ${item.id} ${item.name}`);
    throw error;
  }
}

console.log(JSON.stringify({
  suite: "provider-layout-role-classifier-p0d-regression",
  incident: "GJ-COORD-MAP-SCREENSHOT-DUPLICATE-001",
  passed,
  total: tests.length,
  productionClassifierProfile: "UNAVAILABLE_FAIL_CLOSED",
  providerCalls: 0,
  externalNetworkCalls: 0,
  databaseOrUsageWrites: 0,
  goldenStatus: "GOLDEN_CANDIDATE / BLOCKED_BY_SOURCE_IMAGE_ATTESTATION"
}, null, 2));
