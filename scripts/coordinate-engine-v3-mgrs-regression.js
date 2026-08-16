import assert from "node:assert/strict";

import {
  canHandleMgrs,
  createDefaultRecognizerRegistry,
  createLatencyBudget,
  normalizeMgrs,
  parseMgrsRows,
  recognizeMgrs,
  RECOGNIZER_PORT_STATUS,
  toMgrsKmlCoordinate,
  verifyMgrs,
} from "../server/coordinate-engine-v3/index.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

async function parse(text, clock = () => 0) {
  const result = await recognizeMgrs({ text }, {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock }),
  });
  const normalized = normalizeMgrs(result);
  const verification = await verifyMgrs(normalized);
  return { result, normalized, verification };
}

function assertNear(actual, expected, tolerance = 1e-5) {
  assert.equal(Math.abs(Number(actual) - Number(expected)) < tolerance, true, `${actual} not within ${tolerance} of ${expected}`);
}

test("standard spaced MGRS", async () => {
  const { result, normalized } = await parse("47RLH 24469 42832");
  assert.equal(result.handled, true);
  assert.equal(result.rows[0].sourceValue, "47RLH 24469 42832");
  assert.equal(normalized.coordinates[0].sourceValue, "47RLH 24469 42832");
  assert.equal(normalized.coordinates[0].mgrs.zone, 47);
  assert.equal(normalized.coordinates[0].mgrs.band, "R");
  assert.equal(normalized.coordinates[0].mgrs.gridSquare, "LH");
});

test("compact MGRS", async () => {
  const { result } = await parse("47RLH2446942832");
  assert.equal(result.handled, true);
});

test("comma MGRS", async () => {
  const { result } = await parse("47RLH,24469,42832");
  assert.equal(result.handled, true);
});

test("label", async () => {
  const { normalized } = await parse("A: 47RLH 24469 42832");
  assert.equal(normalized.coordinates[0].label, "A");
});

test("numeric sequence label", async () => {
  const { normalized } = await parse("1. 47RLH 24469 42832");
  assert.equal(normalized.coordinates[0].label, "1");
});

test("multiple points", async () => {
  const { normalized } = await parse("A: 47RLH 24469 42832\nB: 47RLH 24257 42938");
  assert.equal(normalized.coordinates.length, 2);
});

test("input order", async () => {
  const { normalized } = await parse("B: 47RLH 24257 42938\nA: 47RLH 24469 42832");
  assert.deepEqual(normalized.coordinates.map((point) => point.label), ["B", "A"]);
});

for (const digits of [1, 2, 3, 4, 5]) {
  test(`${digits}-digit precision`, async () => {
    const east = "24469".slice(0, digits);
    const north = "42832".slice(0, digits);
    const { result } = await parse(`47RLH ${east} ${north}`);
    assert.equal(result.handled, true);
    assert.equal(result.rows[0].precisionDigits, digits);
  });
}

test("invalid zone 00", () => {
  assert.equal(canHandleMgrs({ text: "00RLH 24469 42832" }), false);
});

test("invalid zone 61", () => {
  assert.equal(canHandleMgrs({ text: "61RLH 24469 42832" }), false);
});

test("invalid band I", () => {
  assert.equal(canHandleMgrs({ text: "47ILH 24469 42832" }), false);
});

test("invalid band O", () => {
  assert.equal(canHandleMgrs({ text: "47OLH 24469 42832" }), false);
});

test("invalid square I/O", () => {
  assert.equal(canHandleMgrs({ text: "47RIH 24469 42832" }), false);
  assert.equal(canHandleMgrs({ text: "47ROH 24469 42832" }), false);
});

test("mismatched precision", () => {
  assert.equal(canHandleMgrs({ text: "47RLH 2446 42832" }), false);
});

test(">5 digit precision", () => {
  assert.equal(canHandleMgrs({ text: "47RLH 244690 428320" }), false);
});

test("malformed numeric payload", () => {
  assert.equal(canHandleMgrs({ text: "47RLH244694283" }), false);
});

test("WGS84 decimal rejection", () => {
  assert.equal(canHandleMgrs({ text: "12.319572, -11.178174" }), false);
});

test("UTM X/Y rejection", () => {
  assert.equal(canHandleMgrs({ text: "778807.293,9721476.737" }), false);
});

test("DMS rejection", () => {
  assert.equal(canHandleMgrs({ text: "11°27'45\"N 08°36'30\"W" }), false);
});

test("conversion ground truth", async () => {
  const { normalized } = await parse("A: 47RLH 24469 42832");
  assertNear(normalized.coordinates[0].longitude, 97.2636250946);
  assertNear(normalized.coordinates[0].latitude, 24.7901938391);
});

test("error <1e-5°", async () => {
  const { normalized } = await parse("G: 47RLH 24620 42882");
  const lonError = Math.abs(normalized.coordinates[0].longitude - 97.2651119873);
  const latError = Math.abs(normalized.coordinates[0].latitude - 24.7906625174);
  assert.equal(Math.max(lonError, latError) < 1e-5, true);
});

test("1 point geometry", async () => {
  const { normalized } = await parse("47RLH 24469 42832");
  assert.equal(normalized.geometryType, "point");
});

test("2 point geometry", async () => {
  const { normalized } = await parse("A: 47RLH 24469 42832\nB: 47RLH 24257 42938");
  assert.equal(normalized.geometryType, "line");
});

test("3+ point geometry", async () => {
  const { normalized } = await parse("A: 47RLH 24469 42832\nB: 47RLH 24257 42938\nC: 47RLH 24123 42905");
  assert.equal(normalized.geometryType, "polygon");
});

test("KML order", async () => {
  const { normalized } = await parse("A: 47RLH 24469 42832");
  assert.equal(toMgrsKmlCoordinate(normalized.coordinates[0]).startsWith("97.263625"), true);
});

test("provider calls=0", async () => {
  const { result, verification } = await parse("47RLH 24469 42832");
  assert.equal(result.providerCalls, 0);
  assert.equal(result.visionCalls, 0);
  assert.equal(result.ocrCalls, 0);
  assert.equal(verification.providerCalls, 0);
  assert.equal(verification.visionCalls, 0);
  assert.equal(verification.ocrCalls, 0);
});

test("historical A-G validation", async () => {
  const input = [
    "A: 47RLH 24469 42832",
    "B: 47RLH 24257 42938",
    "C: 47RLH 24123 42905",
    "D: 47RLH 24124 43163",
    "E: 47RLH 24386 43228",
    "F: 47RLH 24673 43099",
    "G: 47RLH 24620 42882",
  ].join("\n");
  const { normalized } = await parse(input);
  assert.equal(normalized.coordinates.length, 7);
  assert.deepEqual(normalized.coordinates.map((point) => point.label), ["A", "B", "C", "D", "E", "F", "G"]);
  assertNear(normalized.coordinates[0].longitude, 97.2636250946);
  assertNear(normalized.coordinates[0].latitude, 24.7901938391);
  assertNear(normalized.coordinates[6].longitude, 97.2651119873);
  assertNear(normalized.coordinates[6].latitude, 24.7906625174);
});

test("registry status", () => {
  const registry = createDefaultRecognizerRegistry();
  assert.equal(registry.find((item) => item.coordinateType === "wgs84_decimal").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "mgrs").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry
    .filter((item) => !["wgs84_decimal", "mgrs"].includes(item.coordinateType))
    .every((item) => item.portStatus === RECOGNIZER_PORT_STATUS.NOT_PORTED), true);
});

test("deadline expired does no work", async () => {
  const { result } = await parse("47RLH 24469 42832", () => 60000);
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

console.log(`Coordinate Engine V3 MGRS Recognizer Regression: ${passed}/${tests.length} PASS`);
