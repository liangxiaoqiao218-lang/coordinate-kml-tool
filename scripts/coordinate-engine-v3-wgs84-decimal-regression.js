import assert from "node:assert/strict";

import {
  canHandleWgs84Decimal,
  createDefaultRecognizerRegistry,
  createLatencyBudget,
  normalizeWgs84Decimal,
  recognizeWgs84Decimal,
  RECOGNIZER_PORT_STATUS,
  toKmlCoordinate,
  verifyWgs84Decimal,
  wgs84DecimalRecognizer,
} from "../server/coordinate-engine-v3/index.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

async function parse(text) {
  const result = await recognizeWgs84Decimal({ text }, {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock: () => 0 }),
  });
  const normalized = normalizeWgs84Decimal(result);
  const verification = await verifyWgs84Decimal(normalized);
  return { result, normalized, verification };
}

test("single point", async () => {
  const { result, normalized } = await parse("12.319572, -11.178174");
  assert.equal(result.handled, true);
  assert.equal(normalized.coordinates.length, 1);
  assert.equal(normalized.coordinates[0].latitude, 12.319572);
  assert.equal(normalized.coordinates[0].longitude, -11.178174);
});

test("multiple points", async () => {
  const { normalized } = await parse("12.319572, -11.178174\n12.320000, -11.180000\n12.318000, -11.182000");
  assert.equal(normalized.coordinates.length, 3);
});

test("A/B/C labels", async () => {
  const { normalized } = await parse("A: 12.319572, -11.178174\nB: 12.320000, -11.180000\nC: 12.318000, -11.182000");
  assert.deepEqual(normalized.coordinates.map((point) => point.label), ["A", "B", "C"]);
});

test("comma separator", async () => {
  const { result } = await parse("12.319572,-11.178174");
  assert.equal(result.status, "accepted");
});

test("space separator", async () => {
  const { result } = await parse("12.319572 -11.178174");
  assert.equal(result.status, "accepted");
});

test("negative longitude", async () => {
  const { normalized } = await parse("12.319572, -11.178174");
  assert.equal(normalized.coordinates[0].longitude < 0, true);
});

test("negative latitude", async () => {
  const { normalized } = await parse("-12.319572, 11.178174");
  assert.equal(normalized.coordinates[0].latitude < 0, true);
});

test("boundary latitude ±90", async () => {
  const { normalized } = await parse("90, 180\n-90, -180");
  assert.deepEqual(normalized.coordinates.map((point) => point.latitude), [90, -90]);
});

test("boundary longitude ±180", async () => {
  const { normalized } = await parse("90, 180\n-90, -180");
  assert.deepEqual(normalized.coordinates.map((point) => point.longitude), [180, -180]);
});

test("invalid latitude", async () => {
  const { result } = await parse("91, 100");
  assert.equal(result.handled, false);
  assert.equal(result.status, "not_handled");
});

test("invalid longitude", async () => {
  const { result } = await parse("10, 181");
  assert.equal(result.handled, false);
  assert.equal(result.status, "not_handled");
});

test("projected X/Y rejection", async () => {
  const { result } = await parse("778807.293,9721476.737");
  assert.equal(result.handled, false);
  assert.equal(canHandleWgs84Decimal({ text: "778807.293,9721476.737" }), false);
});

test("DMS rejection", async () => {
  const { result } = await parse("11°52'11.93\" N, 08°53'32.66\" W");
  assert.equal(result.handled, false);
  assert.equal(canHandleWgs84Decimal({ text: "11°52'11.93\" N, 08°53'32.66\" W" }), false);
});

test("MGRS rejection", async () => {
  const { result } = await parse("33UXP04 4791");
  assert.equal(result.handled, false);
  assert.equal(canHandleWgs84Decimal({ text: "33UXP04 4791" }), false);
});

test("possible lat/lon swap warning", async () => {
  const { result, normalized } = await parse("119.5082, -2.5224");
  assert.equal(result.status, "rejected");
  assert.equal(result.warnings[0].code, "POSSIBLE_LAT_LON_SWAP");
  assert.equal(normalized.suspectedPoints[0].point, "1");
  assert.equal(normalized.technicalKmlReady, false);
});

test("1 point -> Point", async () => {
  const { normalized } = await parse("12.319572, -11.178174");
  assert.equal(normalized.geometryType, "point");
});

test("2 points -> LineString", async () => {
  const { normalized } = await parse("12.319572, -11.178174\n12.320000, -11.180000");
  assert.equal(normalized.geometryType, "line");
});

test("3+ points -> Polygon", async () => {
  const { normalized } = await parse("12.319572, -11.178174\n12.320000, -11.180000\n12.318000, -11.182000");
  assert.equal(normalized.geometryType, "polygon");
});

test("KML coordinate order", async () => {
  const { normalized } = await parse("12.319572, -11.178174");
  assert.equal(toKmlCoordinate(normalized.coordinates[0]), "-11.178174,12.319572,0");
});

test("no provider call", async () => {
  const { result, verification } = await parse("12.319572, -11.178174");
  assert.equal(result.providerCalls, 0);
  assert.equal(result.visionCalls, 0);
  assert.equal(result.ocrCalls, 0);
  assert.equal(verification.providerCalls, 0);
  assert.equal(verification.visionCalls, 0);
  assert.equal(verification.ocrCalls, 0);
});

test("cross-type registry isolation", () => {
  const registry = createDefaultRecognizerRegistry();
  assert.equal(wgs84DecimalRecognizer.portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "wgs84_decimal").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "wgs84_table").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "mgrs").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "generic_dms").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "kyrgyzstan_gauss_kruger").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "madagascar_cadastral").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "indonesia_utm").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "cote_divoire_dms").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry
    .filter((item) => !["wgs84_decimal", "wgs84_table", "dms_grouped_coordinates", "mgrs", "generic_dms", "kyrgyzstan_gauss_kruger", "madagascar_cadastral", "indonesia_utm", "cote_divoire_dms"].includes(item.coordinateType))
    .every((item) => item.portStatus === RECOGNIZER_PORT_STATUS.NOT_PORTED), true);
});

test("expired latency budget returns without acquisition", async () => {
  const result = await recognizeWgs84Decimal({ text: "12.319572, -11.178174" }, {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock: () => 60000 }),
  });
  assert.equal(result.handled, false);
  assert.equal(result.status, "deadline_exceeded");
  assert.equal(result.providerCalls, 0);
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

console.log(`WGS84 Decimal V3 Recognizer Regression: ${passed}/${tests.length} PASS`);
