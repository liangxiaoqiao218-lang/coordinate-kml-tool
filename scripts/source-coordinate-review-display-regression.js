import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildSourceCoordinateRepresentation } from "../server/source-coordinate-representation.js";
import {
  createLegacyFinalizerInput,
  finalizeCoordinateResult
} from "../server/coordinate-finalizer/index.js";
import { MapPreviewAdapter } from "../server/spatial/adapters/map-preview-adapter.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const html = await readFile(path.join(repoRoot, "index.html"), "utf8");

function extractFunctionSource(source, functionName) {
  const marker = `function ${functionName}(`;
  const start = source.indexOf(marker);
  assert.notEqual(start, -1, `${functionName} must exist`);
  const openParen = source.indexOf("(", start);
  let parameterDepth = 0;
  let closeParen = -1;
  for (let index = openParen; index < source.length; index += 1) {
    if (source[index] === "(") parameterDepth += 1;
    if (source[index] === ")" && --parameterDepth === 0) {
      closeParen = index;
      break;
    }
  }
  assert.notEqual(closeParen, -1, `${functionName} parameters must be closed`);
  const openBrace = source.indexOf("{", closeParen);
  let depth = 0;
  for (let index = openBrace; index < source.length; index += 1) {
    if (source[index] === "{") depth += 1;
    if (source[index] === "}" && --depth === 0) return source.slice(start, index + 1);
  }
  throw new Error(`${functionName} body is not closed`);
}

const ordinaryReviewPredicate = Function(`
  ${extractFunctionSource(html, "getFinalizedCoordinateIdentity")}
  ${extractFunctionSource(html, "hasFiniteFinalizedGeometry")}
  ${extractFunctionSource(html, "isOrdinaryReviewOnlyFinalizedResult")}
  return isOrdinaryReviewOnlyFinalizedResult;
`)();

const parseCanonicalLonLat = Function(`
  let internalNormalizedCoordinateGroups = null;
  let internalKmlSourceDirty = true;
  function trimNumber(value) { return String(Number(value)); }
  ${extractFunctionSource(html, "normalizeKmlPair")}
  ${extractFunctionSource(html, "setInternalCanonicalLonLatSourceFromText")}
  return text => {
    setInternalCanonicalLonLatSourceFromText(text);
    return internalNormalizedCoordinateGroups;
  };
`)();

const buildRecognitionDetailEvidence = Function(`
  ${extractFunctionSource(html, "buildRecognitionDetailEvidence")}
  return buildRecognitionDetailEvidence;
`)();
const collectRecognitionSummary = Function(`
  const lines = [];
  function appendDebug(value) { lines.push(String(value)); }
  ${extractFunctionSource(html, "appendRecognitionEvidenceSummary")}
  return (data, detail) => { appendRecognitionEvidenceSummary(data, detail); return lines; };
`)();

const canonicalDmsGroups = parseCanonicalLonLat([
  "-9.020463888888889,11.72123611111111",
  "-9.015563888888888,11.719222222222223",
  "-9.016297222222223,11.717605555555556",
  "-9.02090277777778,11.719805555555556"
].join("\n"));
assert.equal(canonicalDmsGroups.length, 1);
assert.equal(canonicalDmsGroups[0].length, 4);
assert.equal(canonicalDmsGroups[0][0].longitude, "-9.020463888888889");
assert.equal(canonicalDmsGroups[0][0].latitude, "11.72123611111111");

const sourceDms = [
  `P01 11°28'31.26"N,08°40'42.13"W`,
  `P02 11°28'31.60"N,08°40'32.90"W`,
  "",
  `P03 11°28'18.01"N,08°40'31.01"W`,
  `P04 11°28'17.41"N,08°40'41.36"W`
].join("\n");
const points = [
  { label: "P01", lat: 11.47535, lon: -8.678369444444444 },
  { label: "P02", lat: 11.475444444444445, lon: -8.675805555555556 },
  { label: "P03", lat: 11.471669444444445, lon: -8.675280555555556 },
  { label: "P04", lat: 11.471502777777778, lon: -8.678155555555556 }
];

function engineFor(values, overrides = {}) {
  return {
    schema_version: "coordinate_engine_v2",
    coordinate_type: "handwritten_dms_experimental",
    precision_mode: "handwritten-dms-coordinates",
    requires_review: true,
    groups: [{
      group_id: "group_1",
      geometry: "polygon",
      requires_review: true,
      kml_ready: false,
      points: values.map(point => ({ ...point, raw: `${point.lat},${point.lon}` }))
    }],
    warnings: ["review"],
    ...overrides
  };
}

function finalized(values, revision) {
  return finalizeCoordinateResult(createLegacyFinalizerInput({
    recognitionResult: { coordinates: sourceDms, precisionMode: "handwritten-dms-coordinates" },
    coordinateEngineV2: engineFor(values),
    verification: {
      status: "REVIEW",
      validation_scope: "coordinate_and_geometry",
      geometry_validation: "PASSED",
      warnings: ["review"],
      conflicts: [],
      geometryWarnings: []
    },
    revision
  }), { clock: () => "2026-09-04T00:00:00.000Z" });
}

const source = buildSourceCoordinateRepresentation({
  rawText: `provider prose\n${sourceDms}`,
  coordinates: sourceDms,
  precisionMode: "handwritten-dms-coordinates"
}, engineFor(points));
assert.equal(source.schema_version, "source_coordinate_representation_v1");
assert.equal(source.displayText, sourceDms, "handwritten DMS remains source DMS in edit view");
assert.equal(source.axisOrder, "latitude_longitude");
assert.deepEqual(source.pointLabels, ["P01", "P02", "P03", "P04"]);
assert.equal(source.groups.length, 2, "source group boundaries remain explicit");
assert.deepEqual(source.groups.flat().map(line => line.slice(0, 3)), ["P01", "P02", "P03", "P04"]);

const initial = finalized(points, {
  resultId: "source-display-result",
  resultRevision: 1,
  currentRevision: 1,
  confirmedRevision: null,
  confirmationStatus: "pending"
});
assert.equal(initial.geometry.type, "Polygon", "canonical WGS84 remains available internally");
assert.notEqual(source.displayText, initial.geometry.coordinates[0].map(value => value.join(",")).join("\n"));
const responseShapedInitial = JSON.parse(JSON.stringify(initial));
assert.equal(Object.hasOwn(responseShapedInitial, "currentAuthorizedGeometryExportable"), false);
assert.equal(Object.hasOwn(responseShapedInitial, "kmlAuthorityBlocked"), false);
assert.equal(ordinaryReviewPredicate(responseShapedInitial), true, "serialized ordinary-review result omits confirmation dependency");
assert.ok(responseShapedInitial.warnings.length > 0, "ordinary-review warning remains serialized");
for (const blocked of [
  { resultId: null },
  { schemaVersion: null },
  { resultRevision: null },
  { geometryHash: null },
  { geometry: null },
  { geometry: { type: "Point", coordinates: [Infinity, 11] } },
  { geometry: { type: "Point", coordinates: [181, 11] } },
  { sourceAuthority: "coordinate_engine_v3" },
  { crs: null },
  { technicalKmlReady: false },
  { kmlReady: false },
  { blockingReasons: null },
  { blockingReasons: [{ code: "TRANSFORM_FAILED" }] }
]) assert.equal(ordinaryReviewPredicate({ ...responseShapedInitial, ...blocked }), false);
const preview = new MapPreviewAdapter().adapt(initial, {
  expectedIdentity: {
    resultId: initial.resultId,
    resultRevision: initial.resultRevision,
    geometryHash: initial.geometryHash
  }
});
assert.equal(preview.previewEligibility.allowed, true, "Map consumes finalized canonical geometry");
assert.deepEqual(preview.geometry, initial.geometry);

const editedPoints = points.map((point, index) => index === 0 ? { ...point, lat: 11.47536 } : point);
const edited = finalized(editedPoints, {
  resultId: initial.resultId,
  resultRevision: 2,
  currentRevision: 2,
  confirmedRevision: null,
  confirmationStatus: "pending"
});
assert.equal(edited.resultId, initial.resultId);
assert.equal(edited.resultRevision, 2, "source edit creates a new revision");
assert.notEqual(edited.geometryHash, initial.geometryHash, "source edit re-derives canonical geometry hash");

const projectedText = "point | X | Y\n1 | 527190.1200 | 8753910.3400\n2 | 527290.1200 | 8754010.3400";
const projected = buildSourceCoordinateRepresentation({
  coordinates: projectedText,
  precisionMode: "indonesia-utm50s-projected"
}, {
  coordinate_type: "indonesia_utm50_projected",
  precision_mode: "indonesia-utm50s-projected",
  source_crs: { id: "EPSG:32750", axisOrder: "easting_northing" },
  groups: []
});
assert.equal(projected.displayText, projectedText, "projected source remains projected in edit view");
assert.equal(projected.axisOrder, "easting_northing");
assert.equal(projected.sourceCrsEvidence.id, "EPSG:32750");

const preciseWgs84 = "P01 | 11.4700000000, -8.6800000000";
const wgs84 = buildSourceCoordinateRepresentation({ coordinates: preciseWgs84, precisionMode: "wgs84-table-coordinates" }, {
  coordinate_type: "wgs84_table_coordinates",
  precision_mode: "wgs84-table-coordinates",
  groups: []
});
assert.equal(wgs84.displayText, preciseWgs84, "original WGS84 precision remains unchanged");

const completeDetail = buildRecognitionDetailEvidence({
  mapReady: true,
  mapStatus: "ENABLED",
  kmlReady: true,
  kmlStatus: "ENABLED",
  multiRepresentationEvidence: {
    status: "COMPLETE",
    providerMode: "BOTH",
    sourceRowCount: 6,
    labels: ["1", "2", "3", "4", "5", "6"]
  },
  finalizedCoordinateResult: { crs: { id: "EPSG:4326" } },
  coordinateEngineV2: { source_crs: { id: "EPSG:32750" } }
}, {
  rows: ["r1", "r2", "r3", "r4", "r5", "r6"],
  pointLabels: ["1", "2", "3", "4", "5", "6"]
});
assert.equal(completeDetail.acceptedRows.length, 6);
assert.equal(completeDetail.totalRowCount, 6);
assert.equal(completeDetail.representation, "投影 X/Y 与 DMS");
assert.deepEqual(completeDetail.labels, ["1", "2", "3", "4", "5", "6"]);
const completeSummary = collectRecognitionSummary({
  mapReady: true,
  mapStatus: "ENABLED",
  kmlReady: true,
  kmlStatus: "ENABLED"
}, completeDetail);
assert.ok(completeSummary.some(line => line.includes("总计 6，接受 6，未采用 0")));
assert.ok(completeSummary.some(line => line.includes("未确认 KML 可下载")));

const partialDetail = buildRecognitionDetailEvidence({
  coordinates: Array.from({ length: 14 }, (_, index) => `row-${index + 1}`).join("\n"),
  multiRepresentationEvidence: {
    status: "CONFLICT",
    providerMode: "DMS_ONLY",
    sourceRowCount: 16,
    matchedLabels: ["1", "2", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16"],
    missingLabels: ["3", "4"]
  },
  providerReviewEvidence: {
    rejectedRows: [{ lineNumber: 3, text: "unreadable", reason: "DMS_ROW_MALFORMED" }]
  }
}, {});
assert.equal(partialDetail.acceptedRows.length, 14);
assert.equal(partialDetail.totalRowCount, 16);
assert.deepEqual(partialDetail.missingLabels, ["3", "4"]);
assert.equal(partialDetail.rejectedRows.length, 1);
const partialSummary = collectRecognitionSummary({
  mapReady: true,
  mapStatus: "ENABLED",
  kmlReady: false,
  kmlStatus: "CLOSED"
}, partialDetail);
assert.ok(partialSummary.some(line => line.includes("总计 16，接受 14，未采用 2")));
assert.ok(partialSummary.some(line => line.includes("证据不完整")));
assert.ok(partialSummary.some(line => line.includes("未确认 KML 关闭")));

const sourcePriority = html.indexOf("const sourceDisplayText =");
const canonicalFallback = html.indexOf("|| getCanonicalCoordinateDisplayText(data.finalizedCoordinateResult)", sourcePriority);
assert.ok(sourcePriority >= 0 && canonicalFallback > sourcePriority, "canonical display is only a fallback after source display");
assert.match(html, /const ordinaryReviewOnly = isOrdinaryReviewOnlyFinalizedResult\(\)/, "render uses serialized finalized-result predicate");
assert.doesNotMatch(extractFunctionSource(html, "isOrdinaryReviewOnlyFinalizedResult"), /currentAuthorizedGeometryExportable|kmlAuthorityBlocked/);
assert.match(html, /if \(!isConfirmed\)/, "review acknowledgement is offered only while the current result is pending");
assert.match(html, /我已核对，继续使用未确认结果/, "ordinary review exposes a non-authoritative acknowledgement action");
assert.match(html, /结果仍保持待核对状态/, "ordinary review acknowledgement cannot promote formal authority");
assert.match(html, /请对照原图核对坐标；地图和 KML 为未确认输出。/, "ordinary review uses one calm warning");
assert.match(extractFunctionSource(html, "renderProjectedCrsReviewPanel"), /projectedCrsReviewRequested && authorizationBlocked/u,
  "projected recovery actions are hidden when the server enables provisional map review");
assert.match(html, /发现 \$\{activeCoordinateFieldConflictCount\} 处坐标可能存在识别差异/, "field conflict count is user-visible");
assert.match(html, /fetch\("\/api\/coordinate-confirmation"/, "authority-changing confirmation endpoint remains available");
assert.match(html, /getAuthorizedFinalizedGeometryKmlSource/, "KML still consumes finalized canonical geometry");
assert.match(html, /const isTrustedProviderDmsReview = \["COMPLETE", "PROVISIONAL"\]\.includes/u,
  "trusted Provider DMS results have an explicit frontend route");
assert.match(html, /const finalCoordinates = isTrustedProviderDmsReview\s*\? String\(data\.coordinates/u,
  "trusted Provider DMS canonical coordinates drive internal geometry");
assert.match(html, /function setInternalCanonicalLonLatSourceFromText\(text\)/u,
  "canonical Provider coordinates have an order-independent internal parser");
assert.match(html, /if \(isTrustedProviderDmsReview\) \{\s*setInternalCanonicalLonLatSourceFromText\(data\.coordinates\);/u,
  "map and KML consume canonical lon-lat Provider DMS coordinates without applying the display order");
assert.match(html, /appendDebug\(`第 \$\{index \+ 1\} 行已识别：\$\{visibleRows\[index\]\}`\)/u,
  "recognition details retain safe per-row evidence for review and support");
assert.match(html, /await appendRecognizedCoordinateDetails\(detailRows, trustedProviderDmsCoordinateCount\)/u,
  "recognized rows are rendered progressively in the live detail panel");
assert.match(html, /文件名：\$\{safeRecognitionFileName\}/u,
  "recognition details show the selected file name");
assert.match(html, /appendRecognitionEvidenceSummary\(data, recognitionDetailEvidence\)/u,
  "recognition details show row counts, evidence, CRS and final Map\/KML state");
for (const forbidden of ["geometry", "resultId", "resultRevision", "geometryHash", "kmlReady"]) {
  assert.equal(Object.hasOwn(source, forbidden), false, `source display contract cannot become ${forbidden} authority`);
}

console.log(JSON.stringify({
  suite: "source-coordinate-review-display-regression",
  passed: 32,
  cases: [
    "HANDWRITTEN_SOURCE_DMS_PRESERVED",
    "CANONICAL_WGS84_RETAINED_INTERNAL",
    "SOURCE_AND_CANONICAL_MAY_DIFFER",
    "SOURCE_EDIT_REDERIVES_REVISION_AND_HASH",
    "MAP_CONSUMES_CANONICAL_GEOMETRY",
    "KML_CONSUMES_CANONICAL_GEOMETRY",
    "PROJECTED_SOURCE_PRESERVED",
    "WGS84_PRECISION_PRESERVED",
    "POINT_AND_GROUP_ORDER_PRESERVED",
    "ORDINARY_REVIEW_CONFIRM_BUTTON_ABSENT",
    "ORDINARY_REVIEW_WARNING_PRESENT",
    "FIELD_CONFLICT_COUNT_SURFACED",
    "AUTHORITY_CONFIRMATION_PRESERVED",
    "FRONTEND_NOT_PROMOTED_TO_AUTHORITY",
    "SERIALIZED_RESPONSE_SHAPE_ONLY",
    "HARD_BLOCKERS_DO_NOT_BECOME_ORDINARY_REVIEW",
    "TRUSTED_PROVIDER_DMS_FRONTEND_ROUTE",
    "CANONICAL_DMS_INTERNAL_GEOMETRY",
    "CANONICAL_LON_LAT_IGNORES_DISPLAY_ORDER",
    "DISPLAY_TEXT_NOT_REPARSED_FOR_MAP_KML",
    "SAFE_PER_ROW_RECOGNITION_DETAILS",
    "LIVE_PROGRESSIVE_ROW_DETAILS",
    "COMPLETE_TABLE_DETAIL_COUNTS",
    "COMPLETE_TABLE_POINT_ORDER",
    "COMPLETE_TABLE_REPRESENTATION",
    "PARTIAL_TABLE_TOTAL_COUNT",
    "PARTIAL_TABLE_MISSING_LABELS",
    "PARTIAL_TABLE_REJECTED_ROWS",
    "PARTIAL_TABLE_REJECTED_COUNT",
    "PARTIAL_TABLE_COMPLETENESS_WARNING",
    "PARTIAL_TABLE_KML_CLOSED",
    "FINAL_MAP_KML_STATE_DETAILS"
  ]
}, null, 2));
