import assert from "node:assert/strict";
import { buildCoordinateVerificationResponse } from "../server/verification/index.js";

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
  return {
    coordinate_type: coordinateType,
    requires_review: coordinateType.includes("handwritten"),
    warnings: [],
    groups: [{
      group_id: "group_1",
      geometry: "polygon",
      requires_review: coordinateType.includes("handwritten"),
      warnings: [],
      points: text.split("\n").map((line, index) => makePoint(index + 1, line))
    }]
  };
}

const generalVision = [
  `1. 11°28'31.26"N, 08°40'42.13"W`,
  `2. 11°28'31.60"N, 08°40'32.90"W`,
  `3. 11°28'18.01"N, 08°40'31.01"W`,
  `4. 11°28'17.41"N, 08°40'41.36"W`
].join("\n");
const handwrittenVision = generalVision.replace("08°40'41.36", "08°40'47.36");
const handwrittenEngine = makeEngine(handwrittenVision, "handwritten_dms_experimental");
const rowBoxes = [
  { bbox: [100, 100, 700, 150] },
  { bbox: [100, 160, 700, 210] },
  { bbox: [100, 220, 700, 270] },
  { bbox: [100, 280, 700, 330] }
];
const handwrittenResponse = buildCoordinateVerificationResponse({
  image_id: "handwritten-case-image",
  rawText: handwrittenVision,
  coordinates: handwrittenVision,
  handwrittenVisionRouting: {
    generalVisionRawText: generalVision,
    handwrittenVisionRawText: handwrittenVision,
    finalRawText: handwrittenVision
  },
  imageEvidence: {
    page: 1,
    region_id: "handwritten_coordinate_region",
    sources: {
      generalVision: { rows: rowBoxes },
      handwrittenVision: { rows: rowBoxes }
    }
  }
}, handwrittenEngine);
const handwrittenResult = handwrittenResponse.verification;
const secondsConflict = handwrittenResult.conflicts.find(conflict => (
  conflict.point_id === "4" && conflict.field === "longitude.seconds"
));
assert.ok(secondsConflict, "handwritten Point 4 seconds conflict must be detected");
assert.ok(secondsConflict.sources.every(source => source.evidence_id), "every conflict source must have evidence_id");
assert.ok(secondsConflict.sources.some(source => source.type === "generalVision" && source.value === "41.36"));
assert.ok(secondsConflict.sources.some(source => source.type === "handwrittenVision" && source.value === "47.36"));
secondsConflict.sources.forEach(source => {
  const evidence = handwrittenResponse.evidence.items.find(item => item.evidence_id === source.evidence_id);
  assert.ok(evidence, `evidence ${source.evidence_id} must be resolvable`);
  assert.deepEqual(evidence.bbox, [100, 280, 700, 330], "Point 4 evidence must preserve its image bbox");
  assert.equal(evidence.region_id, "handwritten_coordinate_region");
});

const normalDms = [
  `1. 11°00'00.00"N, 08°00'00.00"W`,
  `2. 11°00'30.00"N, 08°00'00.00"W`,
  `3. 11°00'30.00"N, 08°00'30.00"W`,
  `4. 11°00'00.00"N, 08°00'30.00"W`
].join("\n");
const normalResponse = buildCoordinateVerificationResponse({
  success: true,
  image_id: "normal-dms-image",
  rawText: normalDms,
  coordinates: normalDms,
  precisionMode: "dms-coordinates"
}, makeEngine(normalDms));
const normalPoint = normalResponse.verification.groups[0].points[0];
assert.ok(normalPoint.evidence, "normal DMS point must have primary evidence");
assert.ok(normalPoint.evidence.evidence_id, "normal DMS point evidence must have evidence_id");
assert.ok(normalPoint.evidence_ids.length > 0, "normal DMS point must expose evidence references");
assert.equal(normalPoint.evidence.location_status, "LOGICAL_ROW_ONLY");

const legacyResponse = {
  success: true,
  rawText: normalDms,
  coordinates: normalDms,
  precisionMode: "dms-coordinates",
  warnings: ["legacy warning"],
  coordinateEngineV2: makeEngine(normalDms)
};
const legacySnapshot = structuredClone(legacyResponse);
const evidenceResponse = buildCoordinateVerificationResponse(legacyResponse, legacyResponse.coordinateEngineV2);
const { evidence, verification, ...responseWithoutShadowLayers } = evidenceResponse;
assert.ok(evidence, "response must append evidence shadow data");
assert.ok(verification, "response must preserve verification shadow data");
assert.deepEqual(responseWithoutShadowLayers, legacySnapshot, "removing shadow layers must restore the legacy response");
assert.deepEqual(legacyResponse, legacySnapshot, "evidence adapter must not mutate the legacy response");

console.log(JSON.stringify({
  suite: "evidence-regression",
  passed: 3,
  cases: [
    {
      id: "handwritten_dms_conflict",
      status: "PASS",
      conflict_evidence_ids: secondsConflict.sources.map(source => source.evidence_id),
      bbox: [100, 280, 700, 330]
    },
    {
      id: "normal_dms_point_evidence",
      status: "PASS",
      evidence_id: normalPoint.evidence.evidence_id,
      location_status: normalPoint.evidence.location_status
    },
    { id: "legacy_response_compatibility", status: "PASS" }
  ]
}, null, 2));
