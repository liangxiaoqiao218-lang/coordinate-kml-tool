import assert from "node:assert/strict";

import {
  canHandleKyrgyzGk,
  convertKyrgyzGkToWgs84,
  createDefaultRecognizerRegistry,
  createLatencyBudget,
  KYRGYZ_GK_CRS,
  KYRGYZ_GK_PRECISION_MODE,
  looksLikeKyrgyzGkPair,
  normalizeKyrgyzGk,
  parseKyrgyzGkRows,
  recognizeKyrgyzGk,
  RECOGNIZER_PORT_STATUS,
  toKyrgyzGkKmlCoordinate,
  verifyKyrgyzGk,
} from "../server/coordinate-engine-v3/index.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

const standardTable = [
  "№ points | X | Y",
  "3 | 13261350 | 4607780",
  "1 | 13261341 | 4607777",
  "2 | 13261345 | 4607778",
].join("\n");

const historicalTable = [
  "Координаты угловых точек | № points | X | Y",
  "1 | 13261341 | 4607777",
  "2 | 13261345 | 4607778",
  "65 | 13261317 | 4607721",
].join("\n");

async function parse(text, clock = () => 0) {
  const result = await recognizeKyrgyzGk({ text }, {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock }),
  });
  const normalized = normalizeKyrgyzGk(result);
  const verification = await verifyKyrgyzGk(normalized);
  return { result, normalized, verification };
}

function assertNear(actual, expected, tolerance = 1e-9) {
  assert.equal(Math.abs(Number(actual) - Number(expected)) <= tolerance, true, `${actual} not within ${tolerance} of ${expected}`);
}

test("standard Russian table", async () => {
  const { result } = await parse("№ точек | X | Y\n1 | 13261341 | 4607777\n2 | 13261345 | 4607778\n3 | 13261350 | 4607780");
  assert.equal(result.handled, true);
});

test("point number parsing", () => {
  const rows = parseKyrgyzGkRows({ text: standardTable });
  assert.deepEqual(rows.map((row) => row.point), [1, 2, 3]);
});

test("point order sorting", () => {
  const rows = parseKyrgyzGkRows({ text: standardTable });
  assert.deepEqual(rows.map((row) => row.point), [1, 2, 3]);
});

test("point number preservation", async () => {
  const { normalized } = await parse(standardTable);
  assert.deepEqual(normalized.coordinates.map((point) => point.label), ["1", "2", "3"]);
});

test("X parse", () => {
  const rows = parseKyrgyzGkRows({ text: standardTable });
  assert.equal(rows[0].x, "13261341");
});

test("Y parse", () => {
  const rows = parseKyrgyzGkRows({ text: standardTable });
  assert.equal(rows[0].y, "4607777");
});

test("X/Y axis role", async () => {
  const { normalized } = await parse(standardTable);
  assert.equal(normalized.coordinates[0].sourceProjected.x, 13261341);
  assert.equal(normalized.coordinates[0].sourceProjected.y, 4607777);
  assert.equal(normalized.coordinates[0].sourceProjected.axisSemantics.includes("full easting"), true);
});

test("EPSG resolve", async () => {
  const { result } = await parse(standardTable);
  assert.equal(result.crsDecision.sourceCrs, "EPSG:28413");
});

test("historical EPSG:28413 sample", async () => {
  const { normalized } = await parse(historicalTable);
  assert.equal(normalized.precisionMode, KYRGYZ_GK_PRECISION_MODE);
  assert.equal(normalized.coordinates[0].sourceProjected.sourceCrs, KYRGYZ_GK_CRS);
});

test("projected -> WGS84", () => {
  const converted = convertKyrgyzGkToWgs84(13261341, 4607777);
  assert.equal(converted.crs, "EPSG:4326");
  assert.equal(converted.longitude > 69 && converted.longitude < 80, true);
  assert.equal(converted.latitude > 39 && converted.latitude < 43, true);
});

test("historical ground truth", async () => {
  const { normalized } = await parse(historicalTable);
  assertNear(normalized.coordinates[0].longitude, 72.13791364553406);
  assertNear(normalized.coordinates[0].latitude, 41.56904660823058);
  assertNear(normalized.coordinates[2].longitude, 72.13764850312079);
  assertNear(normalized.coordinates[2].latitude, 41.568535866855164);
});

test("transform tolerance", () => {
  const converted = convertKyrgyzGkToWgs84(13261341, 4607777);
  const maxError = Math.max(
    Math.abs(converted.longitude - 72.13791364553406),
    Math.abs(converted.latitude - 41.56904660823058),
  );
  assert.equal(maxError <= 1e-9, true);
});

test("multiple points", async () => {
  const { normalized } = await parse(standardTable);
  assert.equal(normalized.coordinates.length, 3);
});

test("Point geometry", async () => {
  const { normalized } = await parse("№ points | X | Y\n1 | 13261341 | 4607777");
  assert.equal(normalized.geometryType, "point");
});

test("LineString geometry", async () => {
  const { normalized } = await parse("№ points | X | Y\n1 | 13261341 | 4607777\n2 | 13261345 | 4607778");
  assert.equal(normalized.geometryType, "line");
});

test("Polygon geometry", async () => {
  const { normalized } = await parse(standardTable);
  assert.equal(normalized.geometryType, "polygon");
});

test("KML order", async () => {
  const { normalized } = await parse("№ points | X | Y\n1 | 13261341 | 4607777");
  assert.equal(toKyrgyzGkKmlCoordinate(normalized.coordinates[0]).startsWith("72.13791364553406,41.56904660823058,0"), true);
});

test("invalid X", () => {
  assert.equal(looksLikeKyrgyzGkPair("abc", "4607777"), false);
  assert.equal(canHandleKyrgyzGk({ text: "№ points | X | Y\n1 | abc | 4607777" }), false);
});

test("invalid Y", () => {
  assert.equal(looksLikeKyrgyzGkPair("13261341", "abc"), false);
  assert.equal(canHandleKyrgyzGk({ text: "№ points | X | Y\n1 | 13261341 | abc" }), false);
});

test("missing X", () => {
  assert.equal(canHandleKyrgyzGk({ text: "№ points | X | Y\n1 | | 4607777" }), false);
});

test("missing Y", () => {
  assert.equal(canHandleKyrgyzGk({ text: "№ points | X | Y\n1 | 13261341 |" }), false);
});

test("ambiguous numeric table rejection", () => {
  assert.equal(canHandleKyrgyzGk({ text: "1 | 13261341 | 4607777\n2 | 13261345 | 4607778" }), false);
});

test("Indonesia UTM rejection", () => {
  assert.equal(canHandleKyrgyzGk({ text: "778807.293,9721476.737" }), false);
});

test("WGS84 decimal rejection", () => {
  assert.equal(canHandleKyrgyzGk({ text: "12.319572, -11.178174" }), false);
});

test("MGRS rejection", () => {
  assert.equal(canHandleKyrgyzGk({ text: "47RLH 24469 42832" }), false);
});

test("DMS rejection", () => {
  assert.equal(canHandleKyrgyzGk({ text: "11°27'45\"N 08°36'30\"W" }), false);
});

test("Madagascar-like table rejection", () => {
  assert.equal(canHandleKyrgyzGk({ text: "num | XV | YV\n280 | 292812.5 | 360937.5" }), false);
});

test("provider calls=0", async () => {
  const { result, verification } = await parse(standardTable);
  assert.equal(result.providerCalls, 0);
  assert.equal(result.visionCalls, 0);
  assert.equal(result.ocrCalls, 0);
  assert.equal(verification.providerCalls, 0);
  assert.equal(verification.visionCalls, 0);
  assert.equal(verification.ocrCalls, 0);
});

test("deadline behavior", async () => {
  const { result } = await parse(standardTable, () => 60000);
  assert.equal(result.handled, false);
  assert.equal(result.status, "deadline_exceeded");
  assert.equal(result.providerCalls, 0);
});

test("isolation", () => {
  const registry = createDefaultRecognizerRegistry();
  assert.equal(registry.find((item) => item.coordinateType === "wgs84_decimal").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "mgrs").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "generic_dms").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "kyrgyzstan_gauss_kruger").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "madagascar_cadastral").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry
    .filter((item) => !["wgs84_decimal", "mgrs", "generic_dms", "kyrgyzstan_gauss_kruger", "madagascar_cadastral"].includes(item.coordinateType))
    .every((item) => item.portStatus === RECOGNIZER_PORT_STATUS.NOT_PORTED), true);
});

test("northing narrow repair", () => {
  const rows = parseKyrgyzGkRows({ text: "№ points | X | Y\n1 | 13261341 | 607777" });
  assert.equal(rows[0].y, "4607777");
});

test("duplicate point rejection", () => {
  assert.equal(canHandleKyrgyzGk({ text: "№ points | X | Y\n1 | 13261341 | 4607777\n1 | 13261345 | 4607778" }), false);
});

let passed = 0;
for (const item of tests) {
  try {
    await item.fn();
    passed += 1;
    console.log(`PASS ${item.name}`);
  } catch (error) {
    console.error(`FAIL ${item.name}`);
    throw error;
  }
}

console.log(`Coordinate Engine V3 Kyrgyzstan GK Recognizer Regression: ${passed}/${tests.length} PASS`);
