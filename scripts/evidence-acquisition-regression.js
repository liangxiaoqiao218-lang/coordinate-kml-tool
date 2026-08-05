import assert from "node:assert/strict";
import { buildRecognitionEvidence } from "../server/evidence/recognition-evidence-adapter.js";
import { buildCoordinateVerification, buildCoordinateVerificationResponse } from "../server/verification/index.js";

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

const legacyPayload = {
  success: true,
  rawText: standardDms,
  coordinates: standardDms,
  precisionMode: "dms-coordinates",
  warnings: ["legacy warning"],
  coordinateEngineV2: makeEngine(standardDms)
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
const { evidenceAcquisition, ...responseWithoutAcquisition } = phase3Response;
assert.ok(evidenceAcquisition, "response must append evidenceAcquisition shadow data");
assert.deepEqual(responseWithoutAcquisition, phase2Response, "removing evidenceAcquisition must restore the Phase 2 response");
assert.deepEqual(legacyPayload, legacySnapshot, "acquisition must not mutate the legacy response");
assert.equal(phase3Response.coordinates, legacySnapshot.coordinates, "coordinates must remain unchanged");

console.log(JSON.stringify({
  suite: "evidence-acquisition-regression",
  passed: 4,
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
    { id: "phase2_response_compatibility", status: "PASS" }
  ]
}, null, 2));
