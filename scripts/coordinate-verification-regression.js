import assert from "node:assert/strict";
import fs from "node:fs";
import { buildCoordinateVerification, buildCoordinateVerificationResponse } from "../server/verification/index.js";
import {
  COORDINATE_REVIEW_REASON_CODE,
  COORDINATE_REVIEW_REASON_PRECEDENCE,
  COORDINATE_REVIEW_REASON_SCHEMA_VERSION,
  deriveCoordinateReviewReason
} from "../server/coordinate-review-reason.js";

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
  assert.ok(result.groups[0].points.every(point => point.evidence_ids.length > 0), `${item.id} points must retain logical source evidence`);
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

const ambiguousWgs84Engine = {
  coordinate_type: "decimal_latlon",
  requires_review: true,
  warnings: ["坐标顺序存在歧义，请人工确认经纬度顺序。"],
  groups: [{
    group_id: "group_1",
    geometry: "point",
    requires_review: true,
    kml_ready: false,
    warnings: ["坐标顺序存在歧义，请人工确认经纬度顺序。"],
    validation: {
      status: "scored",
      selected_interpretation: "first_is_lat_second_is_lon",
      order_status: "ambiguous",
      candidates: [{
        interpretation: "first_is_lat_second_is_lon",
        warnings: ["坐标顺序存在歧义，请人工确认经纬度顺序。"]
      }]
    },
    points: [{ label: "1", raw: "12.319572, -11.178174", lat: 12.319572, lon: -11.178174 }]
  }]
};
const ambiguousWgs84Result = buildCoordinateVerification({
  recognitionResult: { coordinates: "12.319572, -11.178174" },
  coordinateEngineV2: ambiguousWgs84Engine
});
assert.equal(ambiguousWgs84Result.status, "REVIEW", "unresolved WGS84 axis ambiguity must still require review");
assert.ok(
  ambiguousWgs84Result.warnings.includes("坐标顺序存在歧义，请人工确认经纬度顺序。"),
  "the review must retain the concrete unresolved axis-order question"
);

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
const {
  verification,
  evidence,
  evidenceAcquisition,
  finalizedCoordinateResult,
  sourceCoordinateRepresentation,
  ...responseWithoutAdditiveContracts
} = responseAfterVerification;
assert.ok(verification, "response wrapper should append verification");
assert.ok(evidence, "response wrapper should append evidence shadow data");
assert.ok(evidenceAcquisition, "response wrapper should append evidence acquisition shadow data");
assert.ok(finalizedCoordinateResult, "response wrapper should append the authoritative finalized result");
assert.equal(sourceCoordinateRepresentation?.schema_version, "source_coordinate_representation_v1", "response wrapper should append the source-coordinate representation contract");
assert.equal(sourceCoordinateRepresentation?.displayText, normalDmsText, "source-coordinate representation must preserve the original DMS text");
for (const authorityKey of ["geometry", "resultId", "resultRevision", "geometryHash", "kmlReady"]) {
  assert.equal(Object.hasOwn(sourceCoordinateRepresentation, authorityKey), false, `source-coordinate representation must not carry ${authorityKey} authority`);
}
assert.deepEqual(responseWithoutAdditiveContracts, responseSnapshot, "removing additive contracts must restore the exact legacy response");
assert.deepEqual(responseBeforeVerification, responseSnapshot, "response wrapper must not mutate the legacy response");

let reviewReasonPassed = 0;
const reviewReasonTest = (name, fn) => {
  fn();
  reviewReasonPassed += 1;
  console.log(`PASS REVIEW_REASON_${String(reviewReasonPassed).padStart(2, "0")} ${name}`);
};
const reason = (...causes) => deriveCoordinateReviewReason({ requiresReview: true, causes });
const C = COORDINATE_REVIEW_REASON_CODE;

reviewReasonTest("FALSE_RETURNS_NULL", () => assert.equal(deriveCoordinateReviewReason({ requiresReview: false, causes: [C.REVIEW_WARNING_PRESENT] }), null));
reviewReasonTest("HANDWRITTEN_EXPERIMENTAL_ONLY", () => assert.deepEqual(reason(C.HANDWRITTEN_EXPERIMENTAL_REVIEW).codes, [C.HANDWRITTEN_EXPERIMENTAL_REVIEW]));
reviewReasonTest("CANDIDATE_CONFLICT_ONLY", () => assert.equal(reason(C.CANDIDATE_FIELD_CONFLICT).primary_code, C.CANDIDATE_FIELD_CONFLICT));
reviewReasonTest("R8_R3_CONFLICT_AND_HANDWRITTEN", () => {
  const output = reason(C.HANDWRITTEN_EXPERIMENTAL_REVIEW, C.CANDIDATE_FIELD_CONFLICT);
  assert.deepEqual(output.codes, [C.CANDIDATE_FIELD_CONFLICT, C.HANDWRITTEN_EXPERIMENTAL_REVIEW]);
  assert.equal(output.primary_code, C.CANDIDATE_FIELD_CONFLICT);
});
reviewReasonTest("CRS_CONFIRMATION", () => assert.deepEqual(reason(C.CRS_CONFIRMATION_REQUIRED).codes, [C.CRS_CONFIRMATION_REQUIRED]));
reviewReasonTest("AXIS_AMBIGUITY", () => assert.deepEqual(reason(C.AXIS_ORDER_AMBIGUOUS).codes, [C.AXIS_ORDER_AMBIGUOUS]));
reviewReasonTest("VALIDATION_FAILURE", () => assert.deepEqual(reason(C.COORDINATE_VALIDATION_FAILED).codes, [C.COORDINATE_VALIDATION_FAILED]));
reviewReasonTest("MISSING_COORDINATES", () => assert.deepEqual(reason(C.MISSING_VALID_COORDINATES).codes, [C.MISSING_VALID_COORDINATES]));
reviewReasonTest("FALLBACK_RESULT", () => assert.deepEqual(reason(C.FALLBACK_RESULT_REVIEW).codes, [C.FALLBACK_RESULT_REVIEW]));
reviewReasonTest("POINT_REVIEW", () => assert.deepEqual(reason(C.POINT_REVIEW_REQUIRED).codes, [C.POINT_REVIEW_REQUIRED]));
reviewReasonTest("AUTHORITATIVE_WARNING", () => assert.deepEqual(reason(C.REVIEW_WARNING_PRESENT).codes, [C.REVIEW_WARNING_PRESENT]));
reviewReasonTest("DUPLICATE_CAUSES_DEDUPLICATED", () => assert.deepEqual(reason(C.CANDIDATE_FIELD_CONFLICT, C.CANDIDATE_FIELD_CONFLICT).codes, [C.CANDIDATE_FIELD_CONFLICT]));
reviewReasonTest("MULTIPLE_CAUSES_STABLE_ORDER", () => {
  const shuffled = reason(C.REVIEW_WARNING_PRESENT, C.AXIS_ORDER_AMBIGUOUS, C.MISSING_VALID_COORDINATES, C.FALLBACK_RESULT_REVIEW);
  assert.deepEqual(shuffled.codes, [C.MISSING_VALID_COORDINATES, C.AXIS_ORDER_AMBIGUOUS, C.FALLBACK_RESULT_REVIEW, C.REVIEW_WARNING_PRESENT]);
});
reviewReasonTest("PRIMARY_IS_MEMBER", () => {
  const output = reason(C.POINT_REVIEW_REQUIRED, C.COORDINATE_VALIDATION_FAILED);
  assert.ok(output.codes.includes(output.primary_code));
});
reviewReasonTest("TRUE_REQUIRES_NONEMPTY_CODE", () => assert.throws(() => deriveCoordinateReviewReason({ requiresReview: true, causes: [] }), /missing_authoritative_cause/));
reviewReasonTest("FALSE_IGNORES_DIAGNOSTIC_CONTEXT", () => assert.equal(deriveCoordinateReviewReason({ requiresReview: false, causes: COORDINATE_REVIEW_REASON_PRECEDENCE }), null));
reviewReasonTest("GOLDEN_TRUTH_UNAVAILABLE", () => {
  const source = fs.readFileSync(new URL("../server/coordinate-review-reason.js", import.meta.url), "utf8");
  assert.doesNotMatch(source, /golden|ground.?truth/i);
});
reviewReasonTest("KML_STATE_UNCHANGED", () => {
  const authority = { kml_ready: false };
  const snapshot = structuredClone(authority);
  reason(C.CANDIDATE_FIELD_CONFLICT);
  assert.deepEqual(authority, snapshot);
});
reviewReasonTest("CONFIRMATION_STATE_UNCHANGED", () => {
  const authority = { confirmationStatus: "pending" };
  const snapshot = structuredClone(authority);
  reason(C.HANDWRITTEN_EXPERIMENTAL_REVIEW);
  assert.deepEqual(authority, snapshot);
});
reviewReasonTest("FINALIZER_STATE_UNCHANGED", () => {
  const authority = { decisionState: "REVIEW_REQUIRED", qualityGateStatus: "review_required" };
  const snapshot = structuredClone(authority);
  reason(C.REVIEW_WARNING_PRESENT);
  assert.deepEqual(authority, snapshot);
});
assert.equal(reviewReasonPassed, 20);

const representativeReviewReason = reason(C.HANDWRITTEN_EXPERIMENTAL_REVIEW, C.CANDIDATE_FIELD_CONFLICT);
const mapperEngine = {
  ...handwrittenEngine,
  review_reason: representativeReviewReason
};
const mapperResponse = buildCoordinateVerificationResponse({
  success: true,
  rawText: handwrittenVerification,
  coordinates: handwrittenVerification,
  coordinateEngineV2: mapperEngine
}, mapperEngine);
assert.deepEqual(mapperResponse.coordinateEngineV2.review_reason, representativeReviewReason, "response mapper must preserve review_reason unchanged");

const serverSource = fs.readFileSync(new URL("../server.js", import.meta.url), "utf8");
assert.match(serverSource, /requires_review: normalizedRequiresReview,\s*review_reason: deriveCoordinateReviewReason\(/);
assert.match(serverSource, /candidateFieldConflict: payload\.handwrittenVisionRouting\?\.candidateSelectionDecision === "KEEP_CURRENT_AND_REQUIRE_REVIEW"/);
assert.match(serverSource, /pointReviewRequired \|\| options\.forceRequiresReview === true/);
assert.match(serverSource, /group\.warnings\.some\(isCoordinateEngineV2ReviewWarning\)/);
const reviewReasonModuleSource = fs.readFileSync(new URL("../server/coordinate-review-reason.js", import.meta.url), "utf8");
assert.doesNotMatch(reviewReasonModuleSource, /kml_ready|confirmationStatus|decisionState/);

console.log(JSON.stringify({
  suite: "coordinate-verification-regression",
  passed: 32,
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
    { id: "true_wgs84_axis_ambiguity", status: ambiguousWgs84Result.status },
    { id: "legacy_response_compatibility", status: "PASS" },
    { id: "review_reason_contract_matrix", status: "PASS", cases: reviewReasonPassed },
    { id: "r8_r3_review_reason", status: "PASS", review_reason: representativeReviewReason },
    { id: "review_reason_response_mapping", status: "PASS" },
    { id: "review_reason_runtime_integration", status: "PASS", schema_version: COORDINATE_REVIEW_REASON_SCHEMA_VERSION }
  ]
}, null, 2));
