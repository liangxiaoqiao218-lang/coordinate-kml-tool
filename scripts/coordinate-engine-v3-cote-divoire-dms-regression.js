import assert from "node:assert/strict";

import {
  canHandleCoteDivoireDms,
  canHandleGenericDms,
  createDefaultRecognizerRegistry,
  createLatencyBudget,
  normalizeCoteDivoireDms,
  parseCoteDivoireDmsTable,
  recognizeCoteDivoireDms,
  RECOGNIZER_PORT_STATUS,
  runCoordinateEngineV3,
  toCoteDivoireDmsKmlCoordinate,
  verifyCoteDivoireDms,
  V3_RUNNER_STATUS,
} from "../server/coordinate-engine-v3/index.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

async function parse(text, clock = () => 0) {
  const result = await recognizeCoteDivoireDms({ text }, {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock }),
  });
  const normalized = normalizeCoteDivoireDms(result);
  const verification = await verifyCoteDivoireDms(normalized);
  return { result, normalized, verification };
}

async function run(text) {
  return runCoordinateEngineV3({ text }, {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock: () => 0 }),
  });
}

function assertNear(actual, expected, tolerance = 1e-12) {
  assert.equal(Math.abs(Number(actual) - Number(expected)) <= tolerance, true, `${actual} not within ${tolerance} of ${expected}`);
}

test("Point | Nord | Est", async () => {
  const { normalized } = await parse("Point | Nord | Est\nA | 10°52'15\" | 08°16'00\"");
  assertNear(normalized.coordinates[0].latitude, 10.870833333333334);
  assertNear(normalized.coordinates[0].longitude, 8.266666666666667);
});

test("Point | Latitude | Longitude with cell hemispheres", async () => {
  const { normalized } = await parse("Point | Latitude | Longitude\nA | 10°52'15\"N | 08°16'00\"W");
  assertNear(normalized.coordinates[0].latitude, 10.870833333333334);
  assertNear(normalized.coordinates[0].longitude, -8.266666666666667);
});

test("title before Point | Latitude Nord | Longitude Ouest header", async () => {
  const { normalized } = await parse("Project coordinates\nPoint | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"");
  assertNear(normalized.coordinates[0].latitude, 10.870833333333334);
  assertNear(normalized.coordinates[0].longitude, -8.266666666666667);
});

test("Longitude Ouest header", async () => {
  const { normalized } = await parse("Point | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"");
  assert.equal(normalized.coordinates[0].longitude < 0, true);
});

test("Latitude Nord header", async () => {
  const { normalized } = await parse("Point | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"");
  assert.equal(normalized.coordinates[0].latitude > 0, true);
});

test("Latitude Sud header", async () => {
  const { normalized } = await parse("Point | Latitude Sud | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"");
  assert.equal(normalized.coordinates[0].latitude < 0, true);
});

test("Longitude Est header", async () => {
  const { normalized } = await parse("Point | Latitude Nord | Longitude Est\nA | 10°52'15\" | 08°16'00\"");
  assert.equal(normalized.coordinates[0].longitude > 0, true);
});

test("header-only N", async () => {
  const { normalized } = await parse("Point | N | O\nA | 10°52'15\" | 08°16'00\"");
  assert.equal(normalized.coordinates[0].latitude > 0, true);
});

test("header-only S", async () => {
  const { normalized } = await parse("Point | S | O\nA | 10°52'15\" | 08°16'00\"");
  assert.equal(normalized.coordinates[0].latitude < 0, true);
});

test("header-only E", async () => {
  const { normalized } = await parse("Point | N | E\nA | 10°52'15\" | 08°16'00\"");
  assert.equal(normalized.coordinates[0].longitude > 0, true);
});

test("header-only W", async () => {
  const { normalized } = await parse("Point | N | W\nA | 10°52'15\" | 08°16'00\"");
  assert.equal(normalized.coordinates[0].longitude < 0, true);
});

test("header-only Ouest", async () => {
  const { normalized } = await parse("Point | Nord | Ouest\nA | 10°52'15\" | 08°16'00\"");
  assert.equal(normalized.coordinates[0].longitude < 0, true);
});

test("cell N", async () => {
  const { normalized } = await parse("Point | Latitude | Longitude Ouest\nA | 10°52'15\"N | 08°16'00\"");
  assert.equal(normalized.coordinates[0].latitude > 0, true);
});

test("cell S", async () => {
  const { normalized } = await parse("Point | Latitude | Longitude Ouest\nA | 10°52'15\"S | 08°16'00\"");
  assert.equal(normalized.coordinates[0].latitude < 0, true);
});

test("cell E", async () => {
  const { normalized } = await parse("Point | Latitude Nord | Longitude\nA | 10°52'15\" | 08°16'00\"E");
  assert.equal(normalized.coordinates[0].longitude > 0, true);
});

test("cell W", async () => {
  const { normalized } = await parse("Point | Latitude Nord | Longitude\nA | 10°52'15\" | 08°16'00\"W");
  assert.equal(normalized.coordinates[0].longitude < 0, true);
});

test("cell O", async () => {
  const { normalized } = await parse("Point | Latitude Nord | Longitude\nA | 10°52'15\" | 08°16'00\"O");
  assert.equal(normalized.coordinates[0].longitude < 0, true);
});

test("header and cell same hemisphere", async () => {
  const { normalized } = await parse("Point | Latitude Nord | Longitude Ouest\nA | 10°52'15\"N | 08°16'00\"W");
  assert.equal(normalized.coordinates.length, 1);
});

test("header/cell longitude conflict", async () => {
  const { result } = await parse("Point | Latitude Nord | Longitude Ouest\nA | 10°52'15\"N | 08°16'00\"E");
  assert.equal(result.handled, false);
  assert.equal(result.warnings[0].code, "HEMISPHERE_CONFLICT");
});

test("header/cell latitude conflict", async () => {
  const { result } = await parse("Point | Latitude Nord | Longitude Ouest\nA | 10°52'15\"S | 08°16'00\"W");
  assert.equal(result.handled, false);
  assert.equal(result.warnings[0].code, "HEMISPHERE_CONFLICT");
});

test("longitude-first columns", async () => {
  const { normalized } = await parse("Point | Longitude Ouest | Latitude Nord\nA | 08°16'00\" | 10°52'15\"");
  assertNear(normalized.coordinates[0].latitude, 10.870833333333334);
  assertNear(normalized.coordinates[0].longitude, -8.266666666666667);
});

test("latitude-first columns", async () => {
  const { normalized } = await parse("Point | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"");
  assertNear(normalized.coordinates[0].latitude, 10.870833333333334);
});

test("A/B/C labels", async () => {
  const { normalized } = await parse("Point | Nord | Ouest\nA | 10°52'15\" | 08°16'00\"\nB | 10°52'16\" | 08°16'01\"\nC | 10°52'17\" | 08°16'02\"");
  assert.deepEqual(normalized.coordinates.map((point) => point.label), ["A", "B", "C"]);
});

test("numeric labels", async () => {
  const { normalized } = await parse("No. | Nord | Ouest\n1 | 10°52'15\" | 08°16'00\"");
  assert.equal(normalized.coordinates[0].label, "1");
});

test("long A-Z style table", async () => {
  const rows = Array.from({ length: 26 }, (_, index) => `${String.fromCharCode(65 + index)} | 10°52'15\" | 08°16'${String(index).padStart(2, "0")}\"`);
  const { normalized } = await parse(`Point | Nord | Ouest\n${rows.join("\n")}`);
  assert.equal(normalized.coordinates.length, 26);
  assert.equal(normalized.coordinates.at(-1).label, "Z");
});

test("source order", async () => {
  const { normalized } = await parse("Point | Nord | Ouest\nC | 10°52'17\" | 08°16'02\"\nA | 10°52'15\" | 08°16'00\"\nB | 10°52'16\" | 08°16'01\"");
  assert.deepEqual(normalized.coordinates.map((point) => point.label), ["C", "A", "B"]);
});

test("duplicate coordinate preservation", async () => {
  const { normalized } = await parse("Point | Nord | Ouest\nA | 10°52'15\" | 08°16'00\"\nB | 10°52'15\" | 08°16'00\"");
  assert.equal(normalized.coordinates.length, 2);
});

test("minutes 59 valid", () => {
  assert.equal(canHandleCoteDivoireDms({ text: "Point | Nord | Ouest\nA | 10°59'00\" | 08°59'00\"" }), true);
});

test("seconds 59.999 valid", () => {
  assert.equal(canHandleCoteDivoireDms({ text: "Point | Nord | Ouest\nA | 10°52'59.999\" | 08°16'59.999\"" }), true);
});

test("minutes 60 reject", () => {
  assert.equal(canHandleCoteDivoireDms({ text: "Point | Nord | Ouest\nA | 10°60'00\" | 08°16'00\"" }), false);
});

test("seconds 60 reject", () => {
  assert.equal(canHandleCoteDivoireDms({ text: "Point | Nord | Ouest\nA | 10°52'60\" | 08°16'00\"" }), false);
});

test("latitude >90 reject", () => {
  assert.equal(canHandleCoteDivoireDms({ text: "Point | Nord | Ouest\nA | 91°00'00\" | 08°16'00\"" }), false);
});

test("longitude >180 reject", () => {
  assert.equal(canHandleCoteDivoireDms({ text: "Point | Nord | Ouest\nA | 10°52'15\" | 181°00'00\"" }), false);
});

test("incomplete DMS reject", () => {
  assert.equal(canHandleCoteDivoireDms({ text: "Point | Nord | Ouest\nA | 10°52' | 08°16'00\"" }), false);
});

test("plain generic DMS rejection", () => {
  assert.equal(canHandleCoteDivoireDms({ text: "10°52'15\"N 08°16'00\"W" }), false);
});

test("generic DMS does not take structured table", () => {
  assert.equal(canHandleGenericDms({ text: "Point | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"" }), false);
});

test("WGS84 decimal rejection", () => {
  assert.equal(canHandleCoteDivoireDms({ text: "12.319572, -11.178174" }), false);
});

test("WGS84 decimal table rejection", () => {
  assert.equal(canHandleCoteDivoireDms({ text: "Point | Latitude | Longitude\nA | 10.8708 | -8.2666" }), false);
});

test("MGRS rejection", () => {
  assert.equal(canHandleCoteDivoireDms({ text: "47RLH 24469 42832" }), false);
});

test("Kyrgyz GK rejection", () => {
  assert.equal(canHandleCoteDivoireDms({ text: "№ points | X | Y\n1 | 13261341 | 4607777" }), false);
});

test("Madagascar rejection", () => {
  assert.equal(canHandleCoteDivoireDms({ text: "Liste_Carres\nNC | XV | YV | CM_NOMFIR | num\n1 | 292812,5 | 360937,5 | Ilakaka | 280" }), false);
});

test("Indonesia UTM rejection", () => {
  assert.equal(canHandleCoteDivoireDms({ text: "SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S\nNo. | X | Y | Latitude | Longitude\n1 | 778807,293 | 9721476,737 | 02°31'01\"S | 119°30'23\"E" }), false);
});

test("projected + geographic verification table rejected", () => {
  assert.equal(canHandleCoteDivoireDms({ text: "No. | X | Y | Latitude | Longitude\n1 | 778807,293 | 9721476,737 | 02°31'01\"S | 119°30'23\"E" }), false);
});

test("frozen Côte d'Ivoire point", async () => {
  const { normalized } = await parse("Point | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"");
  assertNear(normalized.coordinates[0].latitude, 10.870833333333334);
  assertNear(normalized.coordinates[0].longitude, -8.266666666666667);
});

test("header-only hemisphere ground truth", async () => {
  const { normalized } = await parse("Point | Nord | Ouest\nA | 10°52'15\" | 08°16'00\"");
  assertNear(normalized.coordinates[0].latitude, 10.870833333333334);
  assertNear(normalized.coordinates[0].longitude, -8.266666666666667);
});

test("DMS to decimal accuracy", async () => {
  const { normalized } = await parse("Point | Latitude Nord | Longitude Ouest\nA | 11°52'11.93\" | 08°53'32.66\"");
  assertNear(normalized.coordinates[0].latitude, 11.869980555555556);
  assertNear(normalized.coordinates[0].longitude, -8.892405555555555);
});

test("Point geometry", async () => {
  const { normalized } = await parse("Point | Nord | Ouest\nA | 10°52'15\" | 08°16'00\"");
  assert.equal(normalized.geometryType, "point");
});

test("LineString geometry", async () => {
  const { normalized } = await parse("Point | Nord | Ouest\nA | 10°52'15\" | 08°16'00\"\nB | 10°52'16\" | 08°16'01\"");
  assert.equal(normalized.geometryType, "line");
});

test("Polygon geometry", async () => {
  const { normalized } = await parse("Point | Nord | Ouest\nA | 10°52'15\" | 08°16'00\"\nB | 10°52'16\" | 08°16'01\"\nC | 10°52'17\" | 08°16'02\"");
  assert.equal(normalized.geometryType, "polygon");
});

test("KML order", async () => {
  const { normalized } = await parse("Point | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"");
  assert.equal(toCoteDivoireDmsKmlCoordinate(normalized.coordinates[0]), "-8.266666666666667,10.870833333333334,0");
});

test("provider calls=0", async () => {
  const { result, verification } = await parse("Point | Nord | Ouest\nA | 10°52'15\" | 08°16'00\"");
  assert.equal(result.providerCalls, 0);
  assert.equal(result.visionCalls, 0);
  assert.equal(result.ocrCalls, 0);
  assert.equal(verification.providerCalls, 0);
  assert.equal(verification.visionCalls, 0);
  assert.equal(verification.ocrCalls, 0);
});

test("deadline behavior", async () => {
  const { result } = await parse("Point | Nord | Ouest\nA | 10°52'15\" | 08°16'00\"", () => 60000);
  assert.equal(result.handled, false);
  assert.equal(result.status, "deadline_exceeded");
});

test("isolation", async () => {
  const runner = await run("Point | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"");
  assert.equal(runner.status, V3_RUNNER_STATUS.MATCHED);
  assert.equal(runner.recognizerId, "cote_divoire_dms");
  assert.equal(runner.providerCalls, 0);
  assert.equal(runner.visionCalls, 0);
  assert.equal(runner.ocrCalls, 0);
});

test("no ambiguity", async () => {
  const runner = await run("Point | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"");
  assert.notEqual(runner.status, V3_RUNNER_STATUS.AMBIGUOUS);
});

test("registry status", () => {
  const registry = createDefaultRecognizerRegistry();
  assert.equal(registry.find((item) => item.coordinateType === "cote_divoire_dms").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.filter((item) => !["wgs84_decimal", "wgs84_table", "mgrs", "generic_dms", "kyrgyzstan_gauss_kruger", "madagascar_cadastral", "cote_divoire_dms"].includes(item.coordinateType)).every((item) => item.portStatus === RECOGNIZER_PORT_STATUS.NOT_PORTED), true);
});

test("structured rows input", async () => {
  const result = await recognizeCoteDivoireDms({
    rows: [
      { Point: "A", "Latitude Nord": "10°52'15\"", "Longitude Ouest": "08°16'00\"" },
      { Point: "B", "Latitude Nord": "10°52'16\"", "Longitude Ouest": "08°16'01\"" },
    ],
  });
  const normalized = normalizeCoteDivoireDms(result);
  assert.deepEqual(normalized.coordinates.map((point) => point.label), ["A", "B"]);
});

test("parse exposes header mapping", () => {
  const parsed = parseCoteDivoireDmsTable({ text: "Point | Longitude Ouest | Latitude Nord\nA | 08°16'00\" | 10°52'15\"" });
  assert.equal(parsed.mapping.longitudeColumn.index, 1);
  assert.equal(parsed.mapping.latitudeColumn.index, 2);
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

console.log(`Coordinate Engine V3 Côte d'Ivoire DMS Regression: ${passed}/${tests.length} PASS`);
