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
  const openBrace = source.indexOf("{", start);
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

const sourcePriority = html.indexOf("const sourceDisplayText =");
const canonicalFallback = html.indexOf("|| getCanonicalCoordinateDisplayText(data.finalizedCoordinateResult)", sourcePriority);
assert.ok(sourcePriority >= 0 && canonicalFallback > sourcePriority, "canonical display is only a fallback after source display");
assert.match(html, /const ordinaryReviewOnly = isOrdinaryReviewOnlyFinalizedResult\(\)/, "render uses serialized finalized-result predicate");
assert.doesNotMatch(extractFunctionSource(html, "isOrdinaryReviewOnlyFinalizedResult"), /currentAuthorizedGeometryExportable|kmlAuthorityBlocked/);
assert.match(html, /if \(!isConfirmed && !ordinaryReviewOnly\)/, "ordinary review omits redundant confirmation button");
assert.match(html, /建议对照原图核对坐标，部分字符可能存在识别误差。/, "ordinary review warning remains visible");
assert.match(html, /发现 \$\{activeCoordinateFieldConflictCount\} 处坐标可能存在识别差异/, "field conflict count is user-visible");
assert.match(html, /fetch\("\/api\/coordinate-confirmation"/, "authority-changing confirmation endpoint remains available");
assert.match(html, /getAuthorizedFinalizedGeometryKmlSource/, "KML still consumes finalized canonical geometry");
for (const forbidden of ["geometry", "resultId", "resultRevision", "geometryHash", "kmlReady"]) {
  assert.equal(Object.hasOwn(source, forbidden), false, `source display contract cannot become ${forbidden} authority`);
}

console.log(JSON.stringify({
  suite: "source-coordinate-review-display-regression",
  passed: 16,
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
    "HARD_BLOCKERS_DO_NOT_BECOME_ORDINARY_REVIEW"
  ]
}, null, 2));
