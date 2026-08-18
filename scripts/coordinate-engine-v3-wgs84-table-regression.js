import assert from "node:assert/strict";

import {
  canHandleWgs84Table,
  createDefaultRecognizerRegistry,
  createLatencyBudget,
  normalizeWgs84Table,
  parseWgs84Table,
  recognizeWgs84Table,
  RECOGNIZER_PORT_STATUS,
  runCoordinateEngineV3,
  toWgs84TableKmlCoordinate,
  verifyWgs84Table,
  V3_RUNNER_STATUS,
  WGS84_TABLE_CRS,
  WGS84_TABLE_PRECISION_MODE,
  wgs84TableRecognizer,
} from "../server/coordinate-engine-v3/index.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

async function parse(text, clock = () => 0) {
  const result = await recognizeWgs84Table({ text }, {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock }),
  });
  const normalized = normalizeWgs84Table(result);
  const verification = await verifyWgs84Table(normalized);
  return { result, normalized, verification };
}

test("Longitude | Latitude", async () => {
  const { normalized } = await parse("Longitude | Latitude\n16.0320 | 3.7638");
  assert.equal(normalized.coordinates[0].longitude, 16.032);
  assert.equal(normalized.coordinates[0].latitude, 3.7638);
});

test("Latitude | Longitude", async () => {
  const { normalized } = await parse("Latitude | Longitude\n3.7638 | 16.0320");
  assert.equal(normalized.coordinates[0].latitude, 3.7638);
  assert.equal(normalized.coordinates[0].longitude, 16.032);
});

test("经度 | 纬度", async () => {
  const { normalized } = await parse("经度 | 纬度\n16.0320 | 3.7638");
  assert.equal(normalized.coordinates[0].longitude, 16.032);
});

test("纬度 | 经度", async () => {
  const { normalized } = await parse("纬度 | 经度\n3.7638 | 16.0320");
  assert.equal(normalized.coordinates[0].latitude, 3.7638);
});

test("东经 | 北纬", async () => {
  const { normalized } = await parse("东经 | 北纬\n16.0320 | 3.7638");
  assert.equal(normalized.coordinates[0].longitude, 16.032);
  assert.equal(normalized.coordinates[0].latitude, 3.7638);
});

test("西经 | 南纬", async () => {
  const { normalized } = await parse("西经 | 南纬\n8.6085 | 2.5224");
  assert.equal(normalized.coordinates[0].longitude, -8.6085);
  assert.equal(normalized.coordinates[0].latitude, -2.5224);
});

test("Lon | Lat", async () => {
  const { normalized } = await parse("Lon | Lat\n16.0320 | 3.7638");
  assert.equal(normalized.coordinates[0].longitude, 16.032);
});

test("Lat | Lon", async () => {
  const { normalized } = await parse("Lat | Lon\n3.7638 | 16.0320");
  assert.equal(normalized.coordinates[0].longitude, 16.032);
});

test("uppercase headers", async () => {
  const { normalized } = await parse("LONGITUDE | LATITUDE\n16.0320 | 3.7638");
  assert.equal(normalized.coordinates[0].latitude, 3.7638);
});

test("mixed Chinese/English headers", async () => {
  const { normalized } = await parse("Longitude | 北纬\n16.0320 | 3.7638");
  assert.equal(normalized.coordinates[0].latitude, 3.7638);
});

test("point label", async () => {
  const { normalized } = await parse("Point | Longitude | Latitude\nA | 16.0320 | 3.7638");
  assert.equal(normalized.coordinates[0].label, "A");
});

test("numeric label", async () => {
  const { normalized } = await parse("No. | Longitude | Latitude\n1 | 16.0320 | 3.7638");
  assert.equal(normalized.coordinates[0].label, "1");
});

test("multiple rows", async () => {
  const { normalized } = await parse("Point | Longitude | Latitude\nA | 16.0320 | 3.7638\nB | 16.0330 | 3.7648");
  assert.equal(normalized.coordinates.length, 2);
});

test("source order", async () => {
  const { normalized } = await parse("Point | Longitude | Latitude\nB | 16.0330 | 3.7648\nA | 16.0320 | 3.7638");
  assert.deepEqual(normalized.coordinates.map((point) => point.label), ["B", "A"]);
});

test("duplicate exact row removal", async () => {
  const { normalized } = await parse("Point | Longitude | Latitude\nA | 16.0320 | 3.7638\nB | 16.0320 | 3.7638");
  assert.equal(normalized.coordinates.length, 1);
  assert.equal(normalized.coordinates[0].label, "A");
});

test("no fuzzy dedupe", async () => {
  const { normalized } = await parse("Point | Longitude | Latitude\nA | 16.0320 | 3.7638\nB | 16.0321 | 3.7638");
  assert.equal(normalized.coordinates.length, 2);
});

test("negative longitude", async () => {
  const { normalized } = await parse("Longitude | Latitude\n-8.6085 | 2.5224");
  assert.equal(normalized.coordinates[0].longitude, -8.6085);
});

test("negative latitude", async () => {
  const { normalized } = await parse("Longitude | Latitude\n8.6085 | -2.5224");
  assert.equal(normalized.coordinates[0].latitude, -2.5224);
});

test("west header sign", async () => {
  const { normalized } = await parse("西经 | 北纬\n8.6085 | 2.5224");
  assert.equal(normalized.coordinates[0].longitude, -8.6085);
});

test("south header sign", async () => {
  const { normalized } = await parse("东经 | 南纬\n8.6085 | 2.5224");
  assert.equal(normalized.coordinates[0].latitude, -2.5224);
});

test("already-negative west value no double negate", async () => {
  const { normalized } = await parse("西经 | 北纬\n-8.6085 | 2.5224");
  assert.equal(normalized.coordinates[0].longitude, -8.6085);
});

test("inconsistent east + negative warning/reject", async () => {
  const { result, normalized } = await parse("东经 | 北纬\n-8.6085 | 2.5224");
  assert.equal(result.handled, false);
  assert.equal(result.warnings[0].code, "HEADER_SIGN_CONFLICT");
  assert.equal(normalized.coordinates.length, 0);
});

test("inconsistent north + negative warning/reject", async () => {
  const { result, normalized } = await parse("东经 | 北纬\n8.6085 | -2.5224");
  assert.equal(result.handled, false);
  assert.equal(result.warnings[0].code, "HEADER_SIGN_CONFLICT");
  assert.equal(normalized.coordinates.length, 0);
});

test("latitude boundary ±90", async () => {
  const { normalized } = await parse("Longitude | Latitude\n180 | 90\n-180 | -90");
  assert.deepEqual(normalized.coordinates.map((point) => point.latitude), [90, -90]);
});

test("longitude boundary ±180", async () => {
  const { normalized } = await parse("Longitude | Latitude\n180 | 90\n-180 | -90");
  assert.deepEqual(normalized.coordinates.map((point) => point.longitude), [180, -180]);
});

test("invalid latitude", async () => {
  const { result } = await parse("Longitude | Latitude\n10 | 91");
  assert.equal(result.handled, false);
  assert.equal(result.warnings[0].reason, "invalid_latitude");
});

test("invalid longitude", async () => {
  const { result } = await parse("Longitude | Latitude\n181 | 10");
  assert.equal(result.handled, false);
  assert.equal(result.warnings[0].reason, "invalid_longitude");
});

test("NaN rejection", async () => {
  const { result } = await parse("Longitude | Latitude\nNaN | 10");
  assert.equal(result.handled, false);
  assert.equal(result.warnings[0].code, "INVALID_NUMERIC_TOKEN");
});

test("missing field rejection", async () => {
  const { result } = await parse("Longitude | Latitude\n16.0320");
  assert.equal(result.handled, false);
  assert.equal(result.warnings[0].code, "MISSING_COORDINATE_FIELD");
});

test("no-header decimal rejection", () => {
  assert.equal(canHandleWgs84Table({ text: "12.319572, -11.178174" }), false);
});

test("X/Y table rejection", () => {
  assert.equal(canHandleWgs84Table({ text: "X | Y\n778807.293 | 9721476.737" }), false);
});

test("Indonesia UTM rejection", () => {
  assert.equal(canHandleWgs84Table({ text: "UTM Zone 50S\nX | Y\n778807.293 | 9721476.737" }), false);
});

test("Kyrgyz GK rejection", () => {
  assert.equal(canHandleWgs84Table({ text: "№ points | X | Y\n1 | 13261341 | 4607777" }), false);
});

test("Madagascar rejection", () => {
  assert.equal(canHandleWgs84Table({ text: "Liste_Carres\nNC | XV | YV | CM_NOMFIR | num\n1 | 292812,5 | 360937,5 | Ilakaka | 280" }), false);
});

test("MGRS rejection", () => {
  assert.equal(canHandleWgs84Table({ text: "MGRS\n47RLH 24469 42832" }), false);
});

test("DMS rejection", () => {
  assert.equal(canHandleWgs84Table({ text: "Longitude | Latitude\n08°36'30\"W | 11°27'45\"N" }), false);
});

test("frozen 16.0320/3.7638 example", async () => {
  const { normalized } = await parse("经度东 | 北纬\n16.0320 | 3.7638");
  assert.equal(normalized.coordinates[0].latitude, 3.7638);
  assert.equal(normalized.coordinates[0].longitude, 16.032);
});

test("Point geometry", async () => {
  const { normalized } = await parse("Longitude | Latitude\n16.0320 | 3.7638");
  assert.equal(normalized.geometryType, "point");
});

test("LineString geometry", async () => {
  const { normalized } = await parse("Point | Longitude | Latitude\nA | 16.0320 | 3.7638\nB | 16.0330 | 3.7648");
  assert.equal(normalized.geometryType, "line");
});

test("Polygon geometry", async () => {
  const { normalized } = await parse("Point | Longitude | Latitude\nA | 16.0320 | 3.7638\nB | 16.0330 | 3.7648\nC | 16.0340 | 3.7658");
  assert.equal(normalized.geometryType, "polygon");
});

test("KML order", async () => {
  const { normalized } = await parse("经度东 | 北纬\n16.0320 | 3.7638");
  assert.equal(toWgs84TableKmlCoordinate(normalized.coordinates[0]), "16.032,3.7638,0");
});

test("provider calls=0", async () => {
  const { result, verification } = await parse("Longitude | Latitude\n16.0320 | 3.7638");
  assert.equal(result.providerCalls, 0);
  assert.equal(result.visionCalls, 0);
  assert.equal(result.ocrCalls, 0);
  assert.equal(verification.providerCalls, 0);
  assert.equal(verification.visionCalls, 0);
  assert.equal(verification.ocrCalls, 0);
});

test("deadline behavior", async () => {
  const result = await recognizeWgs84Table({ text: "Longitude | Latitude\n16.0320 | 3.7638" }, {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock: () => 60000 }),
  });
  assert.equal(result.handled, false);
  assert.equal(result.status, "deadline_exceeded");
  assert.equal(result.providerCalls, 0);
});

test("isolation", () => {
  const registry = createDefaultRecognizerRegistry();
  assert.equal(wgs84TableRecognizer.portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "wgs84_decimal").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "wgs84_table").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "mgrs").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "generic_dms").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "kyrgyzstan_gauss_kruger").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "madagascar_cadastral").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry
    .filter((item) => !["wgs84_decimal", "wgs84_table", "mgrs", "generic_dms", "kyrgyzstan_gauss_kruger", "madagascar_cadastral"].includes(item.coordinateType))
    .every((item) => item.portStatus === RECOGNIZER_PORT_STATUS.NOT_PORTED), true);
});

test("structured rows", async () => {
  const result = await recognizeWgs84Table({
    rows: [
      { Point: "A", Longitude: "16.0320", Latitude: "3.7638" },
    ],
  }, { latencyBudget: createLatencyBudget({ startedAtMs: 0, clock: () => 0 }) });
  const normalized = normalizeWgs84Table(result);
  assert.equal(normalized.coordinates[0].label, "A");
  assert.equal(normalized.crs, WGS84_TABLE_CRS);
  assert.equal(normalized.precisionMode, WGS84_TABLE_PRECISION_MODE);
});

test("CSV table dispatch not ambiguous", async () => {
  const result = await runCoordinateEngineV3({ text: "Longitude,Latitude\n16.0320,3.7638" }, {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock: () => 0 }),
  });
  assert.equal(result.status, V3_RUNNER_STATUS.MATCHED);
  assert.equal(result.recognizerId, "wgs84_table");
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

console.log(`Coordinate Engine V3 WGS84 Table Recognizer Regression: ${passed}/${tests.length} PASS`);
