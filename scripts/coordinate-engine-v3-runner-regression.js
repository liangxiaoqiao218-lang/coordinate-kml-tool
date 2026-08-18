import assert from "node:assert/strict";

import {
  createDefaultRecognizerRegistry,
  createLatencyBudget,
  createNormalizedCoordinateResult,
  createRecognizerContract,
  RECOGNIZER_PORT_STATUS,
  runCoordinateEngineV3,
  V3_RUNNER_STATUS,
} from "../server/coordinate-engine-v3/index.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

async function run(text, options = {}) {
  return runCoordinateEngineV3({ text }, {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock: () => 0 }),
    ...options,
  });
}

function makeMockRecognizer(id, { canHandle = true, throwCanHandle = false, throwRecognize = false } = {}) {
  return createRecognizerContract({
    recognizerId: id,
    coordinateType: "wgs84_decimal",
    portStatus: RECOGNIZER_PORT_STATUS.IMPLEMENTED,
    canHandle() {
      if (throwCanHandle) throw new Error(`${id} canHandle failed with secret-free message`);
      return canHandle;
    },
    async recognize() {
      if (throwRecognize) throw new Error(`${id} recognize failed with secret-free message`);
      return {
        handled: true,
        rows: [{ label: "1", latitude: 1, longitude: 2 }],
        providerCalls: 0,
        visionCalls: 0,
        ocrCalls: 0,
      };
    },
    normalize(result) {
      return createNormalizedCoordinateResult({
        coordinateType: "wgs84_decimal",
        recognizerId: id,
        coordinates: result.rows,
        geometryType: "point",
        crs: "EPSG:4326",
        precisionMode: "wgs84-decimal",
      });
    },
    async verify() {
      return { verified: true, providerCalls: 0, visionCalls: 0, ocrCalls: 0 };
    },
  });
}

test("WGS84 decimal dispatch", async () => {
  const result = await run("12.319572, -11.178174");
  assert.equal(result.status, V3_RUNNER_STATUS.MATCHED);
  assert.equal(result.recognizerId, "wgs84_decimal");
});

test("WGS84 table dispatch", async () => {
  const result = await run("Longitude | Latitude\n16.0320 | 3.7638");
  assert.equal(result.status, V3_RUNNER_STATUS.MATCHED);
  assert.equal(result.recognizerId, "wgs84_table");
});

test("single point", async () => {
  const result = await run("12.319572, -11.178174");
  assert.equal(result.normalized.geometryType, "point");
  assert.equal(result.normalized.coordinates.length, 1);
});

test("multiple points", async () => {
  const result = await run("12.319572, -11.178174\n12.320000, -11.180000\n12.318000, -11.182000");
  assert.equal(result.normalized.coordinates.length, 3);
});

test("label input", async () => {
  const result = await run("A: 12.319572, -11.178174");
  assert.equal(result.normalized.coordinates[0].label, "A");
});

test("possible swap warning", async () => {
  const result = await run("119.5082, -2.5224");
  assert.equal(result.status, V3_RUNNER_STATUS.MATCHED);
  assert.equal(result.warnings[0].code, "POSSIBLE_LAT_LON_SWAP");
  assert.equal(result.normalized.coordinates.length, 0);
});

test("projected X/Y -> NO_MATCH", async () => {
  const result = await run("778807.293,9721476.737");
  assert.equal(result.status, V3_RUNNER_STATUS.NO_MATCH);
});

test("Indonesia UTM structured table -> indonesia_utm", async () => {
  const result = await run("SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S\nNo. | X | Y | Latitude | Longitude\n1 | 778807,293 | 9721476,737 | 02°31'01\"S | 119°30'23\"E");
  assert.equal(result.status, V3_RUNNER_STATUS.MATCHED);
  assert.equal(result.recognizerId, "indonesia_utm");
});

test("DMS -> generic_dms", async () => {
  const result = await run("11°27'45\"N 08°36'30\"W");
  assert.equal(result.status, V3_RUNNER_STATUS.MATCHED);
  assert.equal(result.recognizerId, "generic_dms");
});

test("MGRS -> mgrs", async () => {
  const result = await run("47RLH 24469 42832");
  assert.equal(result.status, V3_RUNNER_STATUS.MATCHED);
  assert.equal(result.recognizerId, "mgrs");
});

test("Kyrgyzstan GK -> kyrgyzstan_gauss_kruger", async () => {
  const result = await run("№ points | X | Y\n1 | 13261341 | 4607777\n2 | 13261345 | 4607778\n3 | 13261350 | 4607780");
  assert.equal(result.status, V3_RUNNER_STATUS.MATCHED);
  assert.equal(result.recognizerId, "kyrgyzstan_gauss_kruger");
});

test("Madagascar cadastral -> madagascar_cadastral", async () => {
  const result = await run("Liste_Carres\nNC | XV | YV | CM_NOMFIR | num\n1 | 292812,5 | 360937,5 | Ilakaka | 280\n2 | 292812,5 | 361562,5 | Ilakaka | 281\n3 | 292812,5 | 362187,5 | Ilakaka | 282");
  assert.equal(result.status, V3_RUNNER_STATUS.MATCHED);
  assert.equal(result.recognizerId, "madagascar_cadastral");
});

test("Côte d'Ivoire DMS -> cote_divoire_dms", async () => {
  const result = await run("Point | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"");
  assert.equal(result.status, V3_RUNNER_STATUS.MATCHED);
  assert.equal(result.recognizerId, "cote_divoire_dms");
});

test("NOT_PORTED recognizer not called", async () => {
  const notPorted = createRecognizerContract({
    recognizerId: "not_ported_mock",
    coordinateType: "mgrs",
    portStatus: RECOGNIZER_PORT_STATUS.NOT_PORTED,
    canHandle() {
      throw new Error("NOT_PORTED recognizer should not be called");
    },
  });
  const result = await run("47RLH 24469 42832", { registry: [notPorted] });
  assert.equal(result.status, V3_RUNNER_STATUS.NO_MATCH);
  assert.equal(result.errors.length, 0);
});

test("zero match -> NO_MATCH", async () => {
  const result = await run("anything", { registry: [makeMockRecognizer("mock_false", { canHandle: false })] });
  assert.equal(result.status, V3_RUNNER_STATUS.NO_MATCH);
});

test("one match -> MATCHED", async () => {
  const result = await run("anything", { registry: [makeMockRecognizer("mock_one")] });
  assert.equal(result.status, V3_RUNNER_STATUS.MATCHED);
  assert.equal(result.recognizerId, "mock_one");
});

test("multiple match -> AMBIGUOUS", async () => {
  const result = await run("anything", { registry: [makeMockRecognizer("mock_a"), makeMockRecognizer("mock_b")] });
  assert.equal(result.status, V3_RUNNER_STATUS.AMBIGUOUS);
  assert.deepEqual(result.candidates.map((candidate) => candidate.recognizerId), ["mock_a", "mock_b"]);
});

test("expired deadline -> DEADLINE_EXCEEDED", async () => {
  const result = await runCoordinateEngineV3({ text: "12.319572, -11.178174" }, {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock: () => 60000 }),
  });
  assert.equal(result.status, V3_RUNNER_STATUS.DEADLINE_EXCEEDED);
});

test("provider calls = 0", async () => {
  const result = await run("12.319572, -11.178174");
  assert.equal(result.providerCalls, 0);
  assert.equal(result.visionCalls, 0);
  assert.equal(result.ocrCalls, 0);
});

test("recognizer exception isolated", async () => {
  const result = await run("anything", {
    registry: [
      makeMockRecognizer("mock_throw", { throwCanHandle: true }),
      makeMockRecognizer("mock_false", { canHandle: false }),
    ],
  });
  assert.equal(result.status, V3_RUNNER_STATUS.NO_MATCH);
  assert.equal(result.errors.length, 1);
  assert.equal(result.errors[0].phase, "canHandle");
});

test("selected recognizer exception isolated", async () => {
  const result = await run("anything", {
    registry: [makeMockRecognizer("mock_throw_recognize", { throwRecognize: true })],
  });
  assert.equal(result.status, V3_RUNNER_STATUS.RECOGNIZER_ERROR);
  assert.equal(result.errors[0].phase, "recognize_normalize_verify");
});

test("normalized result contract", async () => {
  const result = await run("12.319572, -11.178174");
  assert.equal(result.normalizedContract.valid, true);
  assert.equal(result.normalized.coordinateType, "wgs84_decimal");
  assert.equal(result.normalized.crs, "EPSG:4326");
});

test("forbidden V2 fields absent", async () => {
  const result = await run("12.319572, -11.178174");
  assert.equal(Object.hasOwn(result, "confirmationStatus"), false);
  assert.equal(Object.hasOwn(result, "shadowWinner"), false);
  assert.equal(Object.hasOwn(result, "migrationStatus"), false);
  assert.equal(Object.hasOwn(result, "arbitrationProposal"), false);
  assert.equal(Object.hasOwn(result, "dryRun"), false);
});

test("geometry point", async () => {
  const result = await run("12.319572, -11.178174");
  assert.equal(result.normalized.geometryType, "point");
});

test("geometry line", async () => {
  const result = await run("12.319572, -11.178174\n12.320000, -11.180000");
  assert.equal(result.normalized.geometryType, "line");
});

test("geometry polygon", async () => {
  const result = await run("12.319572, -11.178174\n12.320000, -11.180000\n12.318000, -11.182000");
  assert.equal(result.normalized.geometryType, "polygon");
});

test("default registry keeps wgs84_decimal wgs84_table mgrs generic_dms kyrgyzstan_gauss_kruger madagascar_cadastral indonesia_utm and cote_divoire_dms dispatchable", () => {
  const registry = createDefaultRecognizerRegistry();
  assert.equal(registry.find((item) => item.coordinateType === "wgs84_decimal").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "wgs84_table").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "mgrs").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "generic_dms").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "kyrgyzstan_gauss_kruger").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "madagascar_cadastral").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "indonesia_utm").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "cote_divoire_dms").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.filter((item) => !["wgs84_decimal", "wgs84_table", "mgrs", "generic_dms", "kyrgyzstan_gauss_kruger", "madagascar_cadastral", "indonesia_utm", "cote_divoire_dms"].includes(item.coordinateType)).every((item) => item.portStatus === RECOGNIZER_PORT_STATUS.NOT_PORTED), true);
});

test("standard WGS84 WGS84 table MGRS DMS Kyrgyz GK Madagascar Indonesia and Côte d'Ivoire inputs are not ambiguous", async () => {
  const wgs84 = await run("12.319572, -11.178174");
  const wgs84Table = await run("Longitude | Latitude\n16.0320 | 3.7638");
  const mgrs = await run("47RLH 24469 42832");
  const dms = await run("11°27'45\"N 08°36'30\"W");
  const kyrgyz = await run("№ points | X | Y\n1 | 13261341 | 4607777\n2 | 13261345 | 4607778\n3 | 13261350 | 4607780");
  const madagascar = await run("Liste_Carres\nNC | XV | YV | CM_NOMFIR | num\n1 | 292812,5 | 360937,5 | Ilakaka | 280\n2 | 292812,5 | 361562,5 | Ilakaka | 281\n3 | 292812,5 | 362187,5 | Ilakaka | 282");
  const indonesia = await run("SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S\nNo. | X | Y | Latitude | Longitude\n1 | 778807,293 | 9721476,737 | 02°31'01\"S | 119°30'23\"E");
  const coteDivoire = await run("Point | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"");
  assert.notEqual(wgs84.status, V3_RUNNER_STATUS.AMBIGUOUS);
  assert.notEqual(wgs84Table.status, V3_RUNNER_STATUS.AMBIGUOUS);
  assert.notEqual(mgrs.status, V3_RUNNER_STATUS.AMBIGUOUS);
  assert.notEqual(dms.status, V3_RUNNER_STATUS.AMBIGUOUS);
  assert.notEqual(kyrgyz.status, V3_RUNNER_STATUS.AMBIGUOUS);
  assert.notEqual(madagascar.status, V3_RUNNER_STATUS.AMBIGUOUS);
  assert.notEqual(indonesia.status, V3_RUNNER_STATUS.AMBIGUOUS);
  assert.notEqual(coteDivoire.status, V3_RUNNER_STATUS.AMBIGUOUS);
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

console.log(`Coordinate Engine V3 Runner Regression: ${passed}/${tests.length} PASS`);
