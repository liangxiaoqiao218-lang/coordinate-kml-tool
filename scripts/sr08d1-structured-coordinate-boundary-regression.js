import assert from "node:assert/strict";
import {
  CoordinateConfirmationRuntime,
  createLegacyFinalizerInput,
  finalizeCoordinateResult,
  geometryFromStructuredGroups
} from "../server/coordinate-finalizer/index.js";
import { finiteNumberOrNull } from "../server/coordinate-values.js";
import { UTM30N_CRS } from "../server/projection/utm.js";
import {
  getStructuredCoordinateBlocks,
  inferStructuredBoundaryType,
  parseStructuredBoundaryPoint
} from "../server/structured-coordinate-boundary.js";

const clock = () => "2026-08-26T00:00:00.000Z";
const cases = [];
function test(id, name, fn) { cases.push({ id, name, fn }); }

function finalize(points, coordinateType, precisionMode) {
  const group = {
    group_id: "group_1",
    geometry: "polygon",
    requires_review: false,
    kml_ready: true,
    points
  };
  return finalizeCoordinateResult(createLegacyFinalizerInput({
    recognitionResult: { precisionMode },
    coordinateEngineV2: {
      coordinate_type: coordinateType,
      precision_mode: precisionMode,
      requires_review: false,
      groups: [group]
    },
    verification: { status: "PASS", warnings: [] },
    revision: { resultId: `${coordinateType}-specialized`, resultRevision: 1 }
  }), { clock });
}

const alphabet = Array.from({ length: 26 }, (_, index) => String.fromCharCode(65 + index));
const pointAzCoordinateLines = alphabet.map((label, index) => {
  if (index === 0) return `08°16'00.00\"W,10°52'15.00\"N`;
  if (index === 25) return `08°15'16.00\"W,10°52'15.00\"N`;
  return `08°15'${String(40 - index).padStart(2, "0")}.00\"W,10°52'${String(15 + (index % 10)).padStart(2, "0")}.00\"N`;
});
const pointAzRawRows = pointAzCoordinateLines.map((line, index) => `${alphabet[index]} ${line}`);
const pointAzPayload = {
  precisionMode: "point-az-dms-table",
  rawText: ["Point | Ouest | Nord", ...pointAzRawRows].join("\n"),
  coordinates: pointAzCoordinateLines.map((line, index) => index > 0 && index % 4 === 0 ? `\n${line}` : line).join("\n")
};
const pointAzBlocks = getStructuredCoordinateBlocks(pointAzPayload, "standard_dms_table");
const pointAzPoints = pointAzBlocks[0].map((entry, index) => (
  parseStructuredBoundaryPoint(entry.line, "standard_dms_table", index, entry.label)
));

test("A1", "Point A-Z blank lines remain one structured group", () => assert.equal(pointAzBlocks.length, 1));
test("A2", "Point A-Z converts 26/26 DMS rows to ordered structured points", () => {
  assert.equal(pointAzPoints.length, 26);
  assert.equal(pointAzPoints.every(point => Number.isFinite(point?.lon) && Number.isFinite(point?.lat)), true);
  assert.deepEqual(pointAzPoints.map(point => point.label), alphabet);
});
test("A3", "Point A-Z first and last DMS rows match locked WGS84 truth", () => {
  assert.ok(Math.abs(pointAzPoints[0].lon - (-8.266666666666667)) < 1e-12);
  assert.ok(Math.abs(pointAzPoints[0].lat - 10.870833333333334) < 1e-12);
  assert.ok(Math.abs(pointAzPoints[25].lon - (-8.254444444444445)) < 1e-12);
  assert.ok(Math.abs(pointAzPoints[25].lat - 10.870833333333334) < 1e-12);
});
test("A4", "Point A-Z missing coordinate fails closed", () => {
  assert.equal(parseStructuredBoundaryPoint(`A 08°16'00.00\"W`, "standard_dms_table", 0), null);
});
test("A5", "Point A-Z null coordinate never becomes zero", () => {
  const geometry = geometryFromStructuredGroups([{ geometry: "polygon", points: [{ lon: null, lat: 10 }, ...pointAzPoints.slice(1, 3)] }]);
  assert.equal(geometry.ok, false);
  assert.equal(geometry.reasonCode, "CRS_NOT_FINALIZED");
});
test("A6", "Point A-Z blank coordinate never becomes zero", () => {
  assert.equal(finiteNumberOrNull(""), null);
  assert.equal(parseStructuredBoundaryPoint("   ", "standard_dms_table", 0), null);
});
test("A7", "Point A-Z structured polygon reaches AUTO_EXPORT only after exact confirmation", () => {
  const finalized = finalize(pointAzPoints, "standard_dms_table", "point-az-dms-table");
  assert.equal(finalized.geometry.type, "Polygon");
  assert.equal(finalized.geometry.coordinates[0].length, 27);
  assert.equal(finalized.confirmationStatus, "pending");
  assert.equal(finalized.requiresReview, true);
  assert.equal(finalized.kmlReady, false);
  const runtime = new CoordinateConfirmationRuntime();
  runtime.register(finalized);
  const confirmed = runtime.confirm({
    resultId: finalized.resultId,
    resultRevision: finalized.resultRevision,
    geometryHash: finalized.geometryHash,
    action: "accept"
  });
  assert.equal(confirmed.finalizedCoordinateResult.decisionState, "AUTO_EXPORT");
});

const utmRows = [
  [727250, 1219700], [728400, 1219700], [728400, 1219500], [728700, 1219500],
  [728700, 1220000], [729150, 1220000], [729150, 1219500], [729200, 1219500]
];
const utmPoints = utmRows.map(([x, y], index) => parseStructuredBoundaryPoint(`${x},${y}`, "projected_xy", index));
const expectedUtmWgs84 = [
  [-0.9200314965, 11.0265113723], [-0.9095106619, 11.0264389912],
  [-0.9095234349, 11.0246314043], [-0.9067788982, 11.0246124655],
  [-0.9067469196, 11.0291314243], [-0.9026300611, 11.0291029577],
  [-0.9026621025, 11.0245840108], [-0.9022046814, 11.0245808458]
];

test("U1", "UTM30 precision mode maps to canonical projected_xy", () => {
  assert.equal(inferStructuredBoundaryType({ precisionMode: "utm30n-projected-x-y" }), "projected_xy");
});
test("U2", "UTM metadata explicitly resolves zone 30", () => assert.equal(utmPoints[0].source_crs.zone, 30));
test("U3", "UTM metadata explicitly resolves northern hemisphere", () => assert.equal(utmPoints[0].source_crs.hemisphere, "N"));
test("U4", "UTM30 preserves all eight source X/Y rows without axis swap", () => {
  assert.deepEqual(utmPoints.map(point => [point.x, point.y]), utmRows);
  assert.equal(utmPoints.every(point => point.source_crs.id === UTM30N_CRS.id), true);
});
test("U5", "UTM30 converts and validates 8/8 expected WGS84 points", () => {
  assert.equal(utmPoints.length, 8);
  utmPoints.forEach((point, index) => {
    assert.ok(Math.abs(point.lon - expectedUtmWgs84[index][0]) < 1e-10);
    assert.ok(Math.abs(point.lat - expectedUtmWgs84[index][1]) < 1e-10);
  });
});
test("U6", "UTM30 converted rows finalize as a polygon", () => {
  const geometry = geometryFromStructuredGroups([{ geometry: "polygon", points: utmPoints }]);
  assert.equal(geometry.ok, true);
  assert.equal(geometry.geometry.type, "Polygon");
  assert.equal(geometry.geometry.coordinates[0].length, 9);
});
test("U7", "UTM30 converted polygon passes the unified finalizer", () => {
  assert.equal(finalize(utmPoints, "projected_xy", "utm30n-projected-x-y").decisionState, "AUTO_EXPORT");
});
test("U8", "UTM source CRS is converted before EPSG:4326 finalization", () => {
  const finalized = finalize(utmPoints, "projected_xy", "utm30n-projected-x-y");
  assert.equal(finalized.crs.id, "EPSG:4326");
  assert.equal(finalized.crs.axisOrder, "longitude_latitude");
});

test("S1", "null is not coerced to zero", () => assert.equal(finiteNumberOrNull(null), null));
test("S2", "blank text is not coerced to zero", () => {
  assert.equal(finiteNumberOrNull(""), null);
  assert.equal(finiteNumberOrNull("   "), null);
});
test("S3", "undefined cannot become a coordinate", () => assert.equal(finiteNumberOrNull(undefined), null));
test("S4", "NaN and Infinity are rejected", () => {
  assert.equal(finiteNumberOrNull(Number.NaN), null);
  assert.equal(finiteNumberOrNull(Number.POSITIVE_INFINITY), null);
});
test("S5", "structured geometry and finalizer reject missing coordinates", () => {
  const points = [{ lon: null, lat: 10 }, { lon: -8, lat: 11 }, { lon: -7, lat: 10 }];
  const geometry = geometryFromStructuredGroups([{ geometry: "polygon", points }]);
  assert.equal(geometry.ok, false);
  assert.equal(geometry.reasonCode, "CRS_NOT_FINALIZED");
  const finalized = finalize(points, "standard_dms_table", "point-az-dms-table");
  assert.equal(finalized.geometry, null);
  assert.equal(finalized.decisionState, "BLOCKED");
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
console.log(`SR-08D.1 specialized regression: ${passed}/${cases.length} PASS`);
