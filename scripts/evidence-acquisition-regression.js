import assert from "node:assert/strict";
import { buildRecognitionEvidence } from "../server/evidence/recognition-evidence-adapter.js";
import { buildCoordinateVerification, buildCoordinateVerificationResponse } from "../server/verification/index.js";
import { buildSourceCoordinateRepresentation } from "../server/source-coordinate-representation.js";
import {
  SERVER_PROVENANCE_ATTESTATION,
  TRUSTED_LAYOUT_ATTESTATION_CAPABILITY,
  classifyProviderLayoutRoles,
  collectProviderLayoutProfileQualification,
  extractProviderLayoutCandidates
} from "../server/evidence-acquisition/index.js";

function makePoint(label, raw) {
  return {
    label: String(label),
    raw,
    lat: null,
    lon: null,
    x: null,
    y: null,
    projection: null,
    grid_cell: null,
    requires_review: false,
    warnings: []
  };
}

function makeEngine(text, coordinateType = "standard_dms_table") {
  const requiresReview = coordinateType.includes("handwritten");
  return {
    coordinate_type: coordinateType,
    requires_review: requiresReview,
    warnings: [],
    groups: [{
      group_id: "group_1",
      geometry: "polygon",
      requires_review: requiresReview,
      warnings: [],
      points: text.split("\n").map((line, index) => ({
        ...makePoint(index + 1, line),
        requires_review: requiresReview
      }))
    }]
  };
}

function makeLocatedRows(text) {
  return text.split("\n").map((line, index) => ({
    id: `ocr_row_${index + 1}`,
    text: line,
    bbox: [120, 200 + (index * 70), 920, 250 + (index * 70)],
    coordinate_space: "ORIGINAL_IMAGE_PIXELS",
    source: "qwenOcr",
    point_id: String(index + 1)
  }));
}

const generalVision = [
  `1. 11°28'31.26"N, 08°40'42.13"W`,
  `2. 11°28'31.60"N, 08°40'32.90"W`,
  `3. 11°28'18.01"N, 08°40'31.01"W`,
  `4. 11°28'17.41"N, 08°40'41.36"W`
].join("\n");
const handwrittenVision = generalVision.replace("08°40'41.36", "08°40'47.36");
const handwrittenPayload = {
  success: true,
  image_id: "handwritten-image",
  imageMetadata: { width: 1080, height: 1920, page: 1 },
  rawText: handwrittenVision,
  coordinates: handwrittenVision,
  handwrittenVisionRouting: {
    generalVisionRawText: generalVision,
    handwrittenVisionRawText: handwrittenVision,
    finalRawText: handwrittenVision
  },
  ocrLineLocations: makeLocatedRows(handwrittenVision)
};
const handwrittenResponse = buildCoordinateVerificationResponse(
  handwrittenPayload,
  makeEngine(handwrittenVision, "handwritten_dms_experimental")
);
const point4Binding = handwrittenResponse.evidenceAcquisition.rowBindings.find(binding => binding.point_id === "4");
assert.equal(point4Binding.location_status, "PIXEL_BBOX", "handwritten Point 4 must bind to a pixel row");
assert.deepEqual(point4Binding.bbox, [120, 410, 920, 460]);
assert.ok(point4Binding.match_score >= 0.68);
const point4Conflict = handwrittenResponse.verification.conflicts.find(conflict => (
  conflict.point_id === "4" && conflict.field === "longitude.seconds"
));
assert.ok(point4Conflict, "handwritten Point 4 conflict must remain present");
point4Conflict.sources.forEach(source => {
  assert.ok(source.evidence_id, "conflict source must retain evidence_id");
  const evidence = handwrittenResponse.evidence.items.find(item => item.evidence_id === source.evidence_id);
  assert.ok(evidence, "conflict evidence_id must resolve");
  assert.equal(evidence.location_status, "PIXEL_BBOX");
  assert.deepEqual(evidence.bbox, point4Binding.bbox);
});

const standardDms = [
  `1. 11°00'00.00"N, 08°00'00.00"W`,
  `2. 11°00'30.00"N, 08°00'00.00"W`,
  `3. 11°00'30.00"N, 08°00'30.00"W`,
  `4. 11°00'00.00"N, 08°00'30.00"W`
].join("\n");
const standardPayload = {
  success: true,
  image_id: "standard-dms-image",
  imageMetadata: { width: 1080, height: 1920, page: 1 },
  rawText: standardDms,
  coordinates: standardDms,
  ocrLineLocations: makeLocatedRows(standardDms)
};
const standardResponse = buildCoordinateVerificationResponse(standardPayload, makeEngine(standardDms));
assert.ok(standardResponse.evidenceAcquisition.rowBindings.every(binding => binding.location_status === "PIXEL_BBOX"));
assert.ok(standardResponse.verification.groups[0].points.every(point => point.evidence?.location_status === "PIXEL_BBOX"));

const invalidBboxPayload = {
  success: true,
  rawText: standardDms,
  coordinates: standardDms,
  imageMetadata: { width: 1080, height: 1920, page: 1 },
  ocrLineLocations: standardDms.split("\n").map((text, index) => ({
    text,
    point_id: String(index + 1),
    bbox: [900, 600, 200, 620],
    coordinate_space: "ORIGINAL_IMAGE_PIXELS",
    source: "qwenOcr"
  })),
  visionObservations: [{
    text: standardDms.split("\n")[0],
    point_id: "1",
    bbox: [120, 200, 920, 250],
    source: "generalVision"
  }]
};
const degradedResponse = buildCoordinateVerificationResponse(invalidBboxPayload, makeEngine(standardDms));
assert.equal(degradedResponse.evidenceAcquisition.pixel_bbox_available, false);
assert.ok(degradedResponse.evidenceAcquisition.observations.every(observation => observation.bbox === null));
assert.ok(degradedResponse.evidenceAcquisition.rowBindings.every(binding => (
  binding.location_status === "LOGICAL_ROW_ONLY" && binding.bbox === null
)));

const trustedRegionPayload = {
  success: true,
  request_asset_id: "synthetic-region-asset",
  image_id: "synthetic-region-image",
  imageMetadata: { width: 1080, height: 1920, page: 1 },
  rawText: "35.447819,83.178991",
  coordinates: "35.447819,83.178991",
  ocrLineLocations: [{
    id: "synthetic-region-line",
    source_line_id: "synthetic-region-line",
    text: "35.447819,83.178991",
    point_id: "1",
    bbox: [20, 20, 500, 80],
    coordinate_space: "ORIGINAL_IMAGE_PIXELS",
    source: "qwenOcr",
    source_role: "MAP_SEARCH_BOX",
    source_region_id: "MAP_SEARCH_BOX_REGION",
    provenance_trust: "SERVER_ATTESTED",
    provenance_attestor: "SYNTHETIC_REGRESSION_V1"
  }]
};
Object.defineProperty(trustedRegionPayload, SERVER_PROVENANCE_ATTESTATION, { value: true, enumerable: true });
const trustedRegionEngine = {
  coordinate_type: "wgs84_chat_coordinates",
  groups: [{ group_id: "group_1", geometry: "point", points: [{
    label: "1", raw: trustedRegionPayload.rawText, lat: 35.447819, lon: 83.178991
  }] }]
};
const trustedRegionResponse = buildCoordinateVerificationResponse(trustedRegionPayload, trustedRegionEngine);
const trustedObservation = trustedRegionResponse.evidenceAcquisition.observations[0];
assert.equal(trustedObservation.request_asset_id, "synthetic-region-asset");
assert.equal(trustedObservation.source_role, "MAP_SEARCH_BOX");
assert.equal(trustedObservation.source_region_id, "MAP_SEARCH_BOX_REGION");
assert.equal(trustedObservation.provenance_trust, "SERVER_ATTESTED");
assert.equal(trustedRegionResponse.evidenceAcquisition.shadow_only, true);
assert.equal(trustedRegionResponse.evidenceAcquisition.affects_coordinates, false);
assert.equal(trustedRegionResponse.evidenceAcquisition.affects_kml, false);
assert.equal(trustedRegionResponse.evidenceAcquisition.trusted_layout_status, "UNATTESTED");
assert.notEqual(trustedRegionResponse.evidenceAcquisition[TRUSTED_LAYOUT_ATTESTATION_CAPABILITY], true,
  "legacy provenance strings and markers cannot mint trusted layout capability");

const pseudoRegionPayload = structuredClone(trustedRegionPayload);
const pseudoRegionResponse = buildCoordinateVerificationResponse(pseudoRegionPayload, trustedRegionEngine);
assert.equal(pseudoRegionResponse.evidenceAcquisition.observations[0].source_role, null);
assert.equal(pseudoRegionResponse.evidenceAcquisition.observations[0].provenance_trust, "UNTRUSTED");

const providerLayoutCandidates = extractProviderLayoutCandidates({
  message: { content: JSON.stringify({ bbox: [1, 2, 3, 4], text: "must-not-be-trusted" }) },
  output: {
    layout: [{
      id: "layout-1",
      line_id: "provider-line-1",
      text: "35.447819,83.178991",
      bbox: [20, 20, 500, 80],
      source_role: "MAP_SEARCH_BOX"
    }]
  }
});
assert.equal(providerLayoutCandidates.length, 1, "only allowlisted structured provider layout fields are extracted");
assert.equal(providerLayoutCandidates[0].text, "35.447819,83.178991");
assert.equal(providerLayoutCandidates[0].provenance_trust, "UNTRUSTED");
assert.equal(providerLayoutCandidates[0].source_role, null, "provider self-described roles are discarded");
const missingClassifierOutcome = classifyProviderLayoutRoles({
  profile: null,
  imageIdentity: trustedRegionPayload.imageMetadata,
  candidates: providerLayoutCandidates,
  providerResponseId: "provider-response-shadow-only",
  resultRevision: 1
});
const missingClassifierEvidence = buildCoordinateVerificationResponse({
  ...trustedRegionPayload,
  providerLayoutCandidates,
  providerLayoutClassificationOutcome: missingClassifierOutcome
}, trustedRegionEngine).evidenceAcquisition;
assert.equal(missingClassifierEvidence.trusted_layout_status, "UNATTESTED");
assert.equal(missingClassifierEvidence.trusted_layout_reason, "TRUSTED_LAYOUT_CLASSIFIER_EVIDENCE_MISSING");
assert.equal(missingClassifierEvidence.shadow_only, true);
const qualificationOnly = collectProviderLayoutProfileQualification({
  response: { id: "qualification-only", output: { layout: [] } },
  providerId: "ALIYUN_DASHSCOPE",
  modelName: "qwen-vl-plus",
  responseContractId: "DASHSCOPE_STRUCTURED_LAYOUT_V1",
  providerResponseId: "qualification-only",
  imageIdentity: trustedRegionPayload.imageMetadata,
  resultRevision: 1
});
const qualificationOnlyEvidence = buildCoordinateVerificationResponse({
  ...trustedRegionPayload,
  providerLayoutProfileQualification: qualificationOnly
}, trustedRegionEngine).evidenceAcquisition;
assert.equal(qualificationOnly.status, "RESPONSE_CONTRACT_UNSUPPORTED");
assert.equal(qualificationOnlyEvidence.trusted_layout_status, "UNATTESTED");
assert.equal(qualificationOnlyEvidence.shadow_only, true);

const legacyEngine = makeEngine(standardDms);
const phase2Baseline = {
  success: true,
  rawText: standardDms,
  coordinates: standardDms,
  precisionMode: "dms-coordinates",
  warnings: ["legacy warning"],
  coordinateEngineV2: legacyEngine
};
const legacyPayload = {
  ...phase2Baseline,
  sourceCoordinateRepresentation: buildSourceCoordinateRepresentation(phase2Baseline, legacyEngine)
};
const legacySnapshot = structuredClone(legacyPayload);
const phase2Evidence = buildRecognitionEvidence({
  recognitionResult: legacyPayload,
  coordinateEngineV2: legacyPayload.coordinateEngineV2
});
const phase2Response = {
  ...legacyPayload,
  coordinateEngineV2: legacyPayload.coordinateEngineV2,
  evidence: phase2Evidence,
  verification: buildCoordinateVerification({
    recognitionResult: legacyPayload,
    coordinateEngineV2: legacyPayload.coordinateEngineV2,
    evidence: phase2Evidence
  })
};
const phase3Response = buildCoordinateVerificationResponse(legacyPayload, legacyPayload.coordinateEngineV2);
const { evidenceAcquisition, finalizedCoordinateResult, ...responseWithoutAcquisition } = phase3Response;
assert.ok(evidenceAcquisition, "response must append evidenceAcquisition shadow data");
assert.ok(finalizedCoordinateResult, "response must append the authoritative finalized result");
assert.deepEqual(responseWithoutAcquisition, phase2Response, "removing evidenceAcquisition must restore the Phase 2 response");
assert.deepEqual(legacyPayload, legacySnapshot, "acquisition must not mutate the legacy response");
assert.equal(phase3Response.coordinates, legacySnapshot.coordinates, "coordinates must remain unchanged");

console.log(JSON.stringify({
  suite: "evidence-acquisition-regression",
  passed: 8,
  cases: [
    {
      id: "handwritten_dms_conflict_row_evidence",
      status: "PASS",
      point_id: point4Binding.point_id,
      bbox: point4Binding.bbox,
      match_score: point4Binding.match_score
    },
    {
      id: "standard_dms_row_bbox",
      status: "PASS",
      bindings: standardResponse.evidenceAcquisition.rowBindings.length
    },
    { id: "missing_or_invalid_bbox_degrades_safely", status: "PASS" },
    { id: "trusted_source_role_attestation_and_pseudo_provenance_rejection", status: "PASS" },
    { id: "provider_structured_layout_allowlist_remains_shadow_only", status: "PASS" },
    { id: "missing_server_classifier_profile_fails_closed", status: "PASS" },
    { id: "qualification_evidence_cannot_create_trusted_layout", status: "PASS" },
    { id: "phase2_response_compatibility", status: "PASS" }
  ]
}, null, 2));
