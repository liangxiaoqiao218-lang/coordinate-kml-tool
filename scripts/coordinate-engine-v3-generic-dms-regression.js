import assert from "node:assert/strict";

import {
  canHandleCoteDivoireDms,
  canHandleGenericDms,
  canHandleKyrgyzGk,
  canHandleMadagascarCadastral,
  canHandleMgrs,
  canHandleWgs84Decimal,
  canHandleWgs84Table,
  createDefaultRecognizerRegistry,
  createLatencyBudget,
  normalizeGenericDms,
  parseDmsTokens,
  parseGenericDmsRows,
  recognizeGenericDms,
  RECOGNIZER_PORT_STATUS,
  runCoordinateEngineV3,
  toGenericDmsKmlCoordinate,
  verifyGenericDms,
  V3_RUNNER_STATUS,
} from "../server/coordinate-engine-v3/index.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

async function parse(text, clock = () => 0) {
  const result = await recognizeGenericDms({ text }, {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock }),
  });
  const normalized = normalizeGenericDms(result);
  const verification = await verifyGenericDms(normalized);
  return { result, normalized, verification };
}

function assertNear(actual, expected, tolerance = 1e-12) {
  assert.equal(Math.abs(Number(actual) - Number(expected)) <= tolerance, true, `${actual} not within ${tolerance} of ${expected}`);
}

async function run(text) {
  return runCoordinateEngineV3({ text }, {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock: () => 0 }),
  });
}

test("standard DMS", async () => {
  const { normalized } = await parse("11°27'45.54\"N 08°36'30.76\"W");
  assert.equal(normalized.coordinates.length, 1);
  assertNear(normalized.coordinates[0].latitude, 11.46265);
  assertNear(normalized.coordinates[0].longitude, -8.608544444444444);
});

test("whitespace DMS", async () => {
  const { normalized } = await parse("11 27 45.09 N 08 36 30.76 W");
  assertNear(normalized.coordinates[0].latitude, 11.462525);
});

test("degree + spaced minute/second", async () => {
  const { normalized } = await parse("11°27 45 09 N 08°36 30.76 W");
  assertNear(normalized.coordinates[0].latitude, 11.462525);
});

test("dot-separated DMS", async () => {
  const { normalized } = await parse("11°28.31.26N 08°36.30.76W");
  assertNear(normalized.coordinates[0].latitude, 11.47535);
  assertNear(normalized.coordinates[0].longitude, -8.608544444444444);
});

test("comma-separated pair", async () => {
  const { result } = await parse("11°27'45.54\"N,08°36'30.76\"W");
  assert.equal(result.handled, true);
});

test("N latitude", async () => {
  const { normalized } = await parse("11°27'45\"N 08°36'30\"W");
  assert.equal(normalized.coordinates[0].latitude > 0, true);
});

test("S latitude", async () => {
  const { normalized } = await parse("11°27'45\"S 08°36'30\"W");
  assert.equal(normalized.coordinates[0].latitude < 0, true);
});

test("E longitude", async () => {
  const { normalized } = await parse("11°27'45\"N 08°36'30\"E");
  assert.equal(normalized.coordinates[0].longitude > 0, true);
});

test("W longitude", async () => {
  const { normalized } = await parse("11°27'45\"N 08°36'30\"W");
  assert.equal(normalized.coordinates[0].longitude < 0, true);
});

test("O/Ouest longitude", async () => {
  const o = await parse("11°27'45\"N 08°36'30\"O");
  const ouest = await parse("11°27'45\"N 08°36'30\" Ouest");
  assert.equal(o.normalized.coordinates[0].longitude < 0, true);
  assert.equal(ouest.normalized.coordinates[0].longitude < 0, true);
});

test("Nord", async () => {
  const { normalized } = await parse("11°27'45\" Nord 08°36'30\" Ouest");
  assert.equal(normalized.coordinates[0].latitude > 0, true);
});

test("Sud", async () => {
  const { normalized } = await parse("11°27'45\" Sud 08°36'30\" Ouest");
  assert.equal(normalized.coordinates[0].latitude < 0, true);
});

test("Est", async () => {
  const { normalized } = await parse("11°27'45\" Nord 08°36'30\" Est");
  assert.equal(normalized.coordinates[0].longitude > 0, true);
});

test("longitude-first input", async () => {
  const { normalized } = await parse("08°36'30.76\"W 11°27'45.09\"N");
  assertNear(normalized.coordinates[0].latitude, 11.462525);
  assertNear(normalized.coordinates[0].longitude, -8.608544444444444);
});

test("latitude-first input", async () => {
  const { normalized } = await parse("11°27'45.09\"N 08°36'30.76\"W");
  assertNear(normalized.coordinates[0].latitude, 11.462525);
  assertNear(normalized.coordinates[0].longitude, -8.608544444444444);
});

test("labels", async () => {
  const { normalized } = await parse("A: 11°27'45.54\"N 08°36'30.76\"W");
  assert.equal(normalized.coordinates[0].label, "A");
});

test("numeric labels", async () => {
  const { normalized } = await parse("1. 11°27'45.54\"N 08°36'30.76\"W");
  assert.equal(normalized.coordinates[0].label, "1");
});

test("multiple points", async () => {
  const { normalized } = await parse("A: 11°27'45\"N 08°36'30\"W\nB: 11°28'00\"N 08°37'00\"W");
  assert.equal(normalized.coordinates.length, 2);
});

test("order preservation", async () => {
  const { normalized } = await parse("B: 11°28'00\"N 08°37'00\"W\nA: 11°27'45\"N 08°36'30\"W");
  assert.deepEqual(normalized.coordinates.map((point) => point.label), ["B", "A"]);
});

test("minutes=59 valid", async () => {
  assert.equal(canHandleGenericDms({ text: "11°59'00\"N 08°59'00\"W" }), true);
});

test("seconds=59.999 valid", async () => {
  assert.equal(canHandleGenericDms({ text: "11°27'59.999\"N 08°36'59.999\"W" }), true);
});

test("minutes=60 reject", () => {
  assert.equal(canHandleGenericDms({ text: "11°60'00\"N 08°36'30\"W" }), false);
});

test("seconds=60 reject", () => {
  assert.equal(canHandleGenericDms({ text: "11°27'60\"N 08°36'30\"W" }), false);
});

test("latitude >90 reject", () => {
  assert.equal(canHandleGenericDms({ text: "91°00'00\"N 08°36'30\"W" }), false);
});

test("longitude >180 reject", () => {
  assert.equal(canHandleGenericDms({ text: "11°27'45\"N 181°00'00\"W" }), false);
});

test("missing hemisphere reject", () => {
  assert.equal(canHandleGenericDms({ text: "11°27'45\" 08°36'30\"" }), false);
});

test("duplicate latitude roles reject", () => {
  assert.equal(canHandleGenericDms({ text: "11°27'45\"N 08°36'30\"N" }), false);
});

test("duplicate longitude roles reject", () => {
  assert.equal(canHandleGenericDms({ text: "11°27'45\"W 08°36'30\"W" }), false);
});

test("WGS84 decimal rejection", () => {
  assert.equal(canHandleGenericDms({ text: "12.319572, -11.178174" }), false);
});

test("MGRS rejection", () => {
  assert.equal(canHandleGenericDms({ text: "47RLH 24469 42832" }), false);
});

test("UTM rejection", () => {
  assert.equal(canHandleGenericDms({ text: "778807.293,9721476.737" }), false);
});

test("table header on row 2 rejected", () => {
  assert.equal(canHandleGenericDms({ text: "Project coordinates\nPoint | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"" }), false);
});

test("table header on row 3 rejected", () => {
  assert.equal(canHandleGenericDms({ text: "Project coordinates\nCoordinate appendix\nPoint | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"" }), false);
});

test("CRS preamble + table rejected", () => {
  assert.equal(canHandleGenericDms({ text: "SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S\nNo. | X | Y | Latitude | Longitude\n1 | 778807,293 | 9721476,737 | 02°31'01\"S | 119°30'23\"E" }), false);
});

test("title + table rejected", () => {
  assert.equal(canHandleGenericDms({ text: "Coordinate Report\nPoint | Longitude | Latitude\nA | 16.0320 | 3.7638" }), false);
});

test("Indonesia UTM structured table reject", async () => {
  const text = "SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S\nNo. | X | Y | Latitude | Longitude\n1 | 778807,293 | 9721476,737 | 02°31'01\"S | 119°30'23\"E";
  const runner = await run(text);
  assert.equal(canHandleGenericDms({ text }), false);
  assert.equal(parseGenericDmsRows({ text }).length, 0);
  assert.equal(runner.status, V3_RUNNER_STATUS.NO_MATCH);
});

test("Côte d'Ivoire structured table reject from generic", async () => {
  const text = "Project coordinates\nPoint | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"";
  const runner = await run(text);
  assert.equal(canHandleGenericDms({ text }), false);
  assert.equal(canHandleCoteDivoireDms({ text }), true);
  assert.equal(runner.recognizerId, "cote_divoire_dms");
});

test("WGS84 table reject from generic", () => {
  const text = "Report Header\nPoint | Longitude | Latitude\nA | 16.0320 | 3.7638";
  assert.equal(canHandleGenericDms({ text }), false);
  assert.equal(canHandleWgs84Table({ text }), true);
});

test("plain DMS still accepted after structured-table guard", () => {
  assert.equal(canHandleGenericDms({ text: "11°27'45\"N 08°36'30\"W" }), true);
});

test("labeled plain DMS accepted after structured-table guard", () => {
  assert.equal(canHandleGenericDms({ text: "A: 11°27'45\"N 08°36'30\"W\nB: 11°28'00\"N 08°37'00\"W" }), true);
});

test("multi-line plain DMS accepted after structured-table guard", () => {
  assert.equal(canHandleGenericDms({ text: "11°27'45\"N\n08°36'30\"W" }), true);
});

test("Nord/Ouest prefix plain text accepted", async () => {
  const { normalized } = await parse("Nord 11°27'45\"\nOuest 08°36'30\"");
  assert.equal(normalized.coordinates.length, 1);
  assertNear(normalized.coordinates[0].latitude, 11.4625);
  assertNear(normalized.coordinates[0].longitude, -8.608333333333333);
});

test("metadata-only latitude longitude words do not falsely reject following plain DMS", () => {
  assert.equal(canHandleGenericDms({ text: "Coordinates are latitude and longitude values\n11°27'45\"N 08°36'30\"W" }), true);
});

test("ownership matrix", async () => {
  const cases = [
    {
      name: "Plain WGS84 decimal",
      text: "12.319572, -11.178174",
      only: "wgs84_decimal",
      expectedRunner: "wgs84_decimal",
    },
    {
      name: "WGS84 table",
      text: "Report Header\nPoint | Longitude | Latitude\nA | -8.6085 | 11.4625",
      only: "wgs84_table",
      expectedRunner: "wgs84_table",
    },
    {
      name: "MGRS",
      text: "47RLH 24469 42832",
      only: "mgrs",
      expectedRunner: "mgrs",
    },
    {
      name: "Plain DMS",
      text: "11°27'45\"N 08°36'30\"W",
      only: "generic_dms",
      expectedRunner: "generic_dms",
    },
    {
      name: "Kyrgyz GK",
      text: "№ points | X | Y\n1 | 13261341 | 4607777\n2 | 13261345 | 4607778\n3 | 13261350 | 4607780",
      only: "kyrgyzstan_gauss_kruger",
      expectedRunner: "kyrgyzstan_gauss_kruger",
    },
    {
      name: "Madagascar",
      text: "Liste_Carres\nNC | XV | YV | CM_NOMFIR | num\n1 | 292812,5 | 360937,5 | Ilakaka | 280\n2 | 292812,5 | 361562,5 | Ilakaka | 281\n3 | 292812,5 | 362187,5 | Ilakaka | 282",
      only: "madagascar_cadastral",
      expectedRunner: "madagascar_cadastral",
    },
    {
      name: "Côte d'Ivoire DMS table",
      text: "Project coordinates\nPoint | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"",
      only: "cote_divoire_dms",
      expectedRunner: "cote_divoire_dms",
    },
    {
      name: "Indonesia UTM table",
      text: "SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S\nNo. | X | Y | Latitude | Longitude\n1 | 778807,293 | 9721476,737 | 02°31'01\"S | 119°30'23\"E",
      only: null,
      expectedRunner: null,
    },
  ];

  const recognizers = {
    wgs84_decimal: canHandleWgs84Decimal,
    wgs84_table: canHandleWgs84Table,
    mgrs: canHandleMgrs,
    generic_dms: canHandleGenericDms,
    kyrgyzstan_gauss_kruger: canHandleKyrgyzGk,
    madagascar_cadastral: canHandleMadagascarCadastral,
    cote_divoire_dms: canHandleCoteDivoireDms,
  };

  for (const item of cases) {
    const hits = Object.entries(recognizers)
      .filter(([, canHandle]) => canHandle({ text: item.text }))
      .map(([name]) => name);
    assert.deepEqual(hits, item.only ? [item.only] : [], item.name);
    const runner = await run(item.text);
    if (item.expectedRunner) {
      assert.equal(runner.status, V3_RUNNER_STATUS.MATCHED, item.name);
      assert.equal(runner.recognizerId, item.expectedRunner, item.name);
    } else {
      assert.equal(runner.status, V3_RUNNER_STATUS.NO_MATCH, item.name);
    }
  }
});

test("historical known conversion", async () => {
  const { normalized } = await parse("11°27 45 09 N\n08 36 30.76 W");
  assertNear(normalized.coordinates[0].latitude, 11.462525);
  assertNear(normalized.coordinates[0].longitude, -8.608544444444444);
});

test("conversion precision", async () => {
  const { normalized } = await parse("11°27'45.09\"N 08°36'30.76\"W");
  const latError = Math.abs(normalized.coordinates[0].latitude - 11.462525);
  const lonError = Math.abs(normalized.coordinates[0].longitude - -8.608544444444444);
  assert.equal(Math.max(latError, lonError) <= 1e-12, true);
});

test("Point geometry", async () => {
  const { normalized } = await parse("11°27'45\"N 08°36'30\"W");
  assert.equal(normalized.geometryType, "point");
});

test("LineString geometry", async () => {
  const { normalized } = await parse("A: 11°27'45\"N 08°36'30\"W\nB: 11°28'00\"N 08°37'00\"W");
  assert.equal(normalized.geometryType, "line");
});

test("Polygon geometry", async () => {
  const { normalized } = await parse("A: 11°27'45\"N 08°36'30\"W\nB: 11°28'00\"N 08°37'00\"W\nC: 11°29'00\"N 08°38'00\"W");
  assert.equal(normalized.geometryType, "polygon");
});

test("KML order", async () => {
  const { normalized } = await parse("11°27'45.09\"N 08°36'30.76\"W");
  assert.equal(toGenericDmsKmlCoordinate(normalized.coordinates[0]), "-8.608544444444444,11.462525,0");
});

test("provider calls=0", async () => {
  const { result, verification } = await parse("11°27'45.09\"N 08°36'30.76\"W");
  assert.equal(result.providerCalls, 0);
  assert.equal(result.visionCalls, 0);
  assert.equal(result.ocrCalls, 0);
  assert.equal(verification.providerCalls, 0);
  assert.equal(verification.visionCalls, 0);
  assert.equal(verification.ocrCalls, 0);
});

test("sourceValue sanitized", async () => {
  const { normalized } = await parse("A: 11°27'45.09\"N\t08°36'30.76\"W");
  assert.equal(normalized.coordinates[0].sourceValue.includes("\t"), false);
  assert.equal(normalized.coordinates[0].sourceValue.includes("11°27'45.09\"N"), true);
});

test("deadline behavior", async () => {
  const { result } = await parse("11°27'45.09\"N 08°36'30.76\"W", () => 60000);
  assert.equal(result.handled, false);
  assert.equal(result.status, "deadline_exceeded");
  assert.equal(result.providerCalls, 0);
});

test("registry status", () => {
  const registry = createDefaultRecognizerRegistry();
  assert.equal(registry.find((item) => item.coordinateType === "wgs84_decimal").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "wgs84_table").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "mgrs").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "generic_dms").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "kyrgyzstan_gauss_kruger").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "madagascar_cadastral").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry.find((item) => item.coordinateType === "cote_divoire_dms").portStatus, RECOGNIZER_PORT_STATUS.IMPLEMENTED);
  assert.equal(registry
    .filter((item) => !["wgs84_decimal", "wgs84_table", "mgrs", "generic_dms", "kyrgyzstan_gauss_kruger", "madagascar_cadastral", "cote_divoire_dms"].includes(item.coordinateType))
    .every((item) => item.portStatus === RECOGNIZER_PORT_STATUS.NOT_PORTED), true);
});

test("token parser exposes role by hemisphere", () => {
  const tokens = parseDmsTokens("08°36'30.76\"W 11°27'45.09\"N");
  assert.deepEqual(tokens.map((token) => token.role), ["longitude", "latitude"]);
});

test("two-line pair", async () => {
  const rows = parseGenericDmsRows({ text: "11°27 45 09 N\n08 36 30.76 W" });
  assert.equal(rows.length, 1);
  assert.equal(rows[0].label, "1");
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

console.log(`Coordinate Engine V3 Generic DMS Recognizer Regression: ${passed}/${tests.length} PASS`);
