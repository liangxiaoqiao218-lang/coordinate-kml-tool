import assert from "node:assert/strict";
import { finiteNumberOrNull } from "../server/coordinate-values.js";
import { UTM30N_CRS } from "../server/projection/utm.js";
import {
  getStructuredCoordinateBlocks,
  inferStructuredBoundaryType,
  parseStructuredBoundaryPoint
} from "../server/structured-coordinate-boundary.js";

const cases = [];
function test(id, name, fn) { cases.push({ id, name, fn }); }

const madagascarRows = [
  ...Array.from({ length: 11 }, (_, index) => ({ num: 280 + index, xv: 292812.5, yv: 360937.5 + index * 625 })),
  ...Array.from({ length: 11 }, (_, index) => ({ num: 306 + index, xv: 293437.5, yv: 360937.5 + index * 625 })),
  ...Array.from({ length: 10 }, (_, index) => ({ num: 333 + index, xv: 294062.5, yv: 361562.5 + index * 625 }))
];
const madagascarCoordinates = [
  "num | XV | YV",
  ...madagascarRows.map(row => `${row.num} | ${row.xv} | ${row.yv}`)
].join("\n");
const madagascarPayload = {
  precisionMode: "utm30n-projected-x-y",
  projection: "utm30n",
  coordinates: madagascarCoordinates,
  cadastralGrid: {
    isCadastralGrid: true,
    rows: madagascarRows,
    rowCount: madagascarRows.length
  }
};

test("M1", "positive Madagascar detector outranks UTM-like precision", () => {
  assert.equal(inferStructuredBoundaryType(madagascarPayload), "madagascar_cadastral_grid");
});
test("M2", "Madagascar structured rows preserve the expected cadastral row count", () => {
  const blocks = getStructuredCoordinateBlocks(madagascarPayload, "madagascar_cadastral_grid");
  assert.equal(madagascarPayload.cadastralGrid.rows.length, 32);
  assert.equal(blocks.length, 1);
  assert.equal(blocks[0].filter(entry => !/^num\s*\|/i.test(entry.line)).length, 32);
});
test("M3", "Madagascar positive detector cannot become projected_xy", () => {
  assert.notEqual(inferStructuredBoundaryType(madagascarPayload), "projected_xy");
});
test("M4", "Madagascar existing cadastral structure remains unchanged", () => {
  assert.deepEqual(madagascarPayload.cadastralGrid.rows, madagascarRows);
  assert.deepEqual(madagascarPayload.cadastralGrid.rows[0], { num: 280, xv: 292812.5, yv: 360937.5 });
  assert.deepEqual(madagascarPayload.cadastralGrid.rows.at(-1), { num: 342, xv: 294062.5, yv: 367187.5 });
});

const burkinaRows = [
  [727250, 1219700], [728400, 1219700], [728400, 1219500], [728700, 1219500],
  [728700, 1220000], [729150, 1220000], [729150, 1219500], [729200, 1219500]
];
const burkinaPayload = { precisionMode: "utm30n-projected-x-y", projection: "utm30n" };
const burkinaPoints = burkinaRows.map(([x, y], index) => parseStructuredBoundaryPoint(`${x},${y}`, "projected_xy", index));

test("M5", "Burkina UTM30 without specialized evidence remains projected_xy", () => {
  assert.equal(inferStructuredBoundaryType(burkinaPayload), "projected_xy");
});
test("M6", "Burkina preserves 8/8 valid Zone 30N projected and WGS84 points", () => {
  assert.equal(burkinaPoints.length, 8);
  assert.equal(burkinaPoints.every((point, index) => (
    point.x === burkinaRows[index][0]
    && point.y === burkinaRows[index][1]
    && Number.isFinite(point.lat)
    && Number.isFinite(point.lon)
    && point.source_crs.id === UTM30N_CRS.id
    && point.source_crs.zone === 30
    && point.source_crs.hemisphere === "N"
  )), true);
});
test("M7", "ordinary WGS84 specialized evidence is unaffected", () => {
  assert.equal(inferStructuredBoundaryType({
    precisionMode: "wgs84-table-coordinates",
    wgs84TableCoordinates: { isWgs84TableCoordinates: true }
  }), "decimal_latlon");
});
test("M8", "standard DMS remains on the existing downstream path", () => {
  assert.equal(inferStructuredBoundaryType({ precisionMode: "preserve-original-decimals-and-parse-dms" }), "");
});
test("M9", "Point A-Z remains on its canonical downstream path", () => {
  assert.equal(inferStructuredBoundaryType({ precisionMode: "point-az-dms-table" }), "");
  const blocks = getStructuredCoordinateBlocks({
    precisionMode: "point-az-dms-table",
    rawText: `A 08°16'00\"W,10°52'15\"N\nB 08°15'59\"W,10°52'16\"N`,
    coordinates: `08°16'00\"W,10°52'15\"N\n\n08°15'59\"W,10°52'16\"N`
  }, "standard_dms_table");
  assert.equal(blocks.length, 1);
  assert.equal(blocks[0].length, 2);
});
test("M10", "other positive projected families outrank generic UTM inference", () => {
  assert.equal(inferStructuredBoundaryType({
    precisionMode: "utm30n-projected-x-y",
    projection: "utm30n",
    bftmLongTable: { isBftmLongTable: true }
  }), "bftm_xy");
  assert.equal(inferStructuredBoundaryType({
    precisionMode: "utm30n-projected-x-y",
    projection: "utm30n",
    kyrgyzGk: { isKyrgyzGk: true }
  }), "kyrgyzstan_gk");
});
test("M11", "null-to-zero protection remains fail-closed", () => {
  for (const value of [null, undefined, "", "   ", Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.equal(finiteNumberOrNull(value), null);
  }
});
test("M12", "non-positive or conflicting specialized metadata cannot silently select a specialized family", () => {
  assert.equal(inferStructuredBoundaryType({
    ...burkinaPayload,
    cadastralGrid: { isCadastralGrid: false }
  }), "projected_xy");
  assert.equal(inferStructuredBoundaryType({
    ...burkinaPayload,
    cadastralGrid: { isCadastralGrid: "true" }
  }), "projected_xy");
  assert.equal(inferStructuredBoundaryType({
    ...burkinaPayload,
    cadastralGrid: { isCadastralGrid: true },
    mgrs: { isMgrs: true }
  }), "ambiguous_specialized_family");
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
console.log(`SR-08D.2 specialized precedence regression: ${passed}/${cases.length} PASS`);
