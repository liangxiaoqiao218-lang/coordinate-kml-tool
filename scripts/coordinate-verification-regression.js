import assert from "node:assert/strict";
import { buildCoordinateVerification, buildCoordinateVerificationResponse } from "../server/verification/index.js";

function makeDmsPoint(label, raw) {
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

const normalDmsText = [
  `1. 11°00'00.00"N, 08°00'00.00"W`,
  `2. 11°00'30.00"N, 08°00'00.00"W`,
  `3. 11°00'30.00"N, 08°00'30.00"W`,
  `4. 11°00'00.00"N, 08°00'30.00"W`
].join("\n");

const normalDmsEngine = {
  coordinate_type: "standard_dms_table",
  requires_review: false,
  warnings: [],
  groups: [{
    group_id: "group_1",
    geometry: "polygon",
    requires_review: false,
    warnings: [],
    points: normalDmsText.split("\n").map((line, index) => makeDmsPoint(index + 1, line))
  }]
};

const normalResult = buildCoordinateVerification({
  recognitionResult: { rawText: normalDmsText, coordinates: normalDmsText },
  coordinateEngineV2: normalDmsEngine
});
assert.equal(normalResult.status, "PASS", "normal DMS should pass verification");
assert.ok(normalResult.verification_score >= 0.85, "normal DMS should have a high rule verification score");
assert.equal(normalResult.score_method, "rule_based_v1");
assert.equal(normalResult.score_calibrated, false);
assert.deepEqual(normalResult.supported_scope, ["handwritten_dms", "standard_dms"]);
assert.equal(JSON.stringify(normalResult).includes('"confidence"'), false, "verification output must not expose confidence fields");
assert.equal(normalResult.conflicts.length, 0, "normal DMS should not contain conflicts");

const handwrittenGeneral = [
  `1. 11°28'31.26"N, 08°40'42.13"W`,
  `2. 11°28'31.60"N, 08°40'32.90"W`,
  `3. 11°28'18.01"N, 08°40'31.01"W`,
  `4. 11°28'17.41"N, 08°40'41.36"W`
].join("\n");
const handwrittenVerification = handwrittenGeneral.replace(`08°40'41.36"W`, `08°40'47.36"W`);
const handwrittenEngine = {
  ...normalDmsEngine,
  coordinate_type: "handwritten_dms_experimental",
  requires_review: true,
  groups: [{
    ...normalDmsEngine.groups[0],
    requires_review: true,
    points: handwrittenVerification.split("\n").map((line, index) => ({
      ...makeDmsPoint(index + 1, line),
      requires_review: true
    }))
  }]
};
const handwrittenResult = buildCoordinateVerification({
  recognitionResult: {
    rawText: handwrittenVerification,
    coordinates: handwrittenVerification,
    handwrittenVisionRouting: {
      generalVisionRawText: handwrittenGeneral,
      handwrittenVisionRawText: handwrittenVerification,
      finalRawText: handwrittenVerification
    }
  },
  coordinateEngineV2: handwrittenEngine
});
assert.equal(handwrittenResult.status, "REVIEW", "handwritten digit conflict should require review");
assert.ok(handwrittenResult.conflicts.some(conflict => (
  conflict.point_id === "4"
  && conflict.field === "longitude.seconds"
  && conflict.candidates.includes("41.36")
  && conflict.candidates.includes("47.36")
)), "handwritten seconds conflict should preserve both candidates");

const jumpEngine = {
  coordinate_type: "decimal_latlon",
  requires_review: false,
  warnings: [],
  groups: [{
    group_id: "group_1",
    geometry: "polygon",
    requires_review: false,
    warnings: [],
    points: [
      { label: "1", raw: "-8.00,11.00", lat: 11.00, lon: -8.00 },
      { label: "2", raw: "-8.01,11.00", lat: 11.00, lon: -8.01 },
      { label: "3", raw: "20.00,30.00", lat: 30.00, lon: 20.00 },
      { label: "4", raw: "-8.00,11.01", lat: 11.01, lon: -8.00 }
    ]
  }]
};
const jumpResult = buildCoordinateVerification({
  recognitionResult: { coordinates: jumpEngine.groups[0].points.map(point => point.raw).join("\n") },
  coordinateEngineV2: jumpEngine
});
assert.equal(jumpResult.status, "BLOCK", "obvious coordinate jump should block verification");
assert.ok(jumpResult.geometryWarnings.some(warning => warning.code === "ABNORMAL_JUMP"), "jump warning should identify the abnormal point");

const projectedCases = [
  { id: "utm", coordinate_type: "projected_xy", precisionMode: "utm30n-projected-x-y", projection: "utm30n" },
  { id: "bftm", coordinate_type: "bftm_xy", precisionMode: "bftm-projected-x-y", projection: "bftm" },
  { id: "kyrgyz_gk", coordinate_type: "kyrgyzstan_gk", precisionMode: "kyrgyz-gk-point-x-y", projection: "kyrgyzstan_gk" },
  { id: "other_xy", coordinate_type: "projected_xy", precisionMode: "projected-x-y", projection: "known_projected_crs" }
];
const projectedResults = projectedCases.map(item => {
  const coordinateEngineV2 = {
    coordinate_type: item.coordinate_type,
    precision_mode: item.precisionMode,
    requires_review: false,
    warnings: [],
    groups: [{
      group_id: "group_1",
      geometry: "polygon",
      requires_review: false,
      warnings: [],
      points: [
        { label: "1", raw: "600000,1300000", x: 600000, y: 1300000, projection: item.projection },
        { label: "2", raw: "600100,1300000", x: 600100, y: 1300000, projection: item.projection },
        { label: "3", raw: "600100,1300100", x: 600100, y: 1300100, projection: item.projection }
      ],
      validation: {
        status: "skipped_projected_or_grid",
        order_status: "not_applicable",
        candidates: []
      }
    }]
  };
  const result = buildCoordinateVerification({
    recognitionResult: { precisionMode: item.precisionMode, projection: item.projection },
    coordinateEngineV2
  });
  assert.equal(result.status, "REVIEW", `${item.id} must not pass before projected geometry validation`);
  assert.equal(result.validation_scope, "format_only", `${item.id} must declare format-only validation`);
  assert.equal(result.geometry_validation, "NOT_EVALUATED", `${item.id} geometry must be not evaluated`);
  return { id: item.id, result };
});

const engineGeometryEngine = structuredClone(normalDmsEngine);
engineGeometryEngine.groups[0].validation = {
  status: "scored",
  selected_interpretation: "as_parsed",
  order_status: "resolved",
  candidates: [{
    interpretation: "as_parsed",
    self_intersecting: true,
    warnings: ["polygon 自交。"]
  }]
};
const engineGeometryResult = buildCoordinateVerification({
  recognitionResult: { rawText: normalDmsText, coordinates: normalDmsText },
  coordinateEngineV2: engineGeometryEngine
});
assert.equal(engineGeometryResult.status, "BLOCK", "existing V2 self-intersection must be consumed");
assert.ok(engineGeometryResult.geometryWarnings.some(warning => (
  warning.code === "ENGINE_SELF_INTERSECTION" && warning.source === "coordinate_engine_v2"
)), "geometry warning should identify Coordinate Engine V2 as its source");

const responseBeforeVerification = {
  success: true,
  rawText: normalDmsText,
  coordinates: normalDmsText,
  precisionMode: "dms-coordinates",
  warnings: ["legacy warning remains unchanged"],
  coordinateEngineV2: normalDmsEngine
};
const responseSnapshot = structuredClone(responseBeforeVerification);
const responseAfterVerification = buildCoordinateVerificationResponse(
  responseBeforeVerification,
  responseBeforeVerification.coordinateEngineV2
);
const { verification, ...responseWithoutVerification } = responseAfterVerification;
assert.ok(verification, "response wrapper should append verification");
assert.deepEqual(responseWithoutVerification, responseSnapshot, "removing verification must restore the exact legacy response");
assert.deepEqual(responseBeforeVerification, responseSnapshot, "response wrapper must not mutate the legacy response");

console.log(JSON.stringify({
  suite: "coordinate-verification-regression",
  passed: 9,
  cases: [
    { id: "normal_dms", status: normalResult.status, verification_score: normalResult.verification_score },
    { id: "handwritten_digit_conflict", status: handwrittenResult.status, conflicts: handwrittenResult.conflicts.length },
    { id: "abnormal_jump", status: jumpResult.status, geometryWarnings: jumpResult.geometryWarnings.map(warning => warning.code) },
    ...projectedResults.map(item => ({
      id: item.id,
      status: item.result.status,
      validation_scope: item.result.validation_scope,
      geometry_validation: item.result.geometry_validation
    })),
    { id: "existing_v2_geometry", status: engineGeometryResult.status },
    { id: "legacy_response_compatibility", status: "PASS" }
  ]
}, null, 2));
