import assert from "node:assert/strict";

import {
  buildMadagascarCadastralCellPolygons,
  canHandleMadagascarCadastral,
  convertMadagascarCadastralToWgs84,
  createDefaultRecognizerRegistry,
  createLatencyBudget,
  inferMadagascarCadastralCellGeometry,
  MADAGASCAR_CADASTRAL_CELL_SEMANTICS,
  MADAGASCAR_CADASTRAL_OUTPUT_CRS,
  MADAGASCAR_CADASTRAL_PRECISION_MODE,
  MADAGASCAR_CADASTRAL_SOURCE_CRS,
  normalizeMadagascarCadastral,
  parseMadagascarCadastralRows,
  recognizeMadagascarCadastral,
  RECOGNIZER_PORT_STATUS,
  runCoordinateEngineV3,
  toMadagascarCadastralKmlCoordinate,
  verifyMadagascarCadastral,
  V3_RUNNER_STATUS,
} from "../server/coordinate-engine-v3/index.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

const historicalRows = [
  [1, "280", "292812.5", "360937.5", "Ilakaka"],
  [2, "281", "292812.5", "361562.5", "Ilakaka"],
  [3, "282", "292812.5", "362187.5", "Ilakaka"],
  [4, "283", "292812.5", "362812.5", "Ilakaka"],
  [5, "284", "292812.5", "363437.5", "Ilakaka"],
  [6, "285", "292812.5", "364062.5", "Ilakaka"],
  [7, "286", "292812.5", "364687.5", "Ilakaka"],
  [8, "287", "292812.5", "365312.5", "Ilakaka"],
  [9, "288", "292812.5", "365937.5", "Ilakaka"],
  [10, "289", "292812.5", "366562.5", "Ilakaka"],
  [11, "290", "292812.5", "367187.5", "Ilakaka"],
  [12, "306", "293437.5", "360937.5", "Ilakaka"],
  [13, "307", "293437.5", "361562.5", "Ilakaka"],
  [14, "308", "293437.5", "362187.5", "Ilakaka"],
  [15, "309", "293437.5", "362812.5", "Ilakaka"],
  [16, "310", "293437.5", "363437.5", "Ilakaka"],
  [17, "311", "293437.5", "364062.5", "Ilakaka"],
  [18, "312", "293437.5", "364687.5", "Ilakaka"],
  [19, "313", "293437.5", "365312.5", "Ilakaka"],
  [20, "314", "293437.5", "365937.5", "Ilakaka"],
  [21, "315", "293437.5", "366562.5", "Ilakaka"],
  [22, "316", "293437.5", "367187.5", "Ilakaka"],
  [23, "333", "294062.5", "361562.5", "Andriandampy"],
  [24, "334", "294062.5", "362187.5", "Ilakaka"],
  [25, "335", "294062.5", "362812.5", "Ilakaka"],
  [26, "336", "294062.5", "363437.5", "Ilakaka"],
  [27, "337", "294062.5", "364062.5", "Ilakaka"],
  [28, "338", "294062.5", "364687.5", "Ilakaka"],
  [29, "339", "294062.5", "365312.5", "Ilakaka"],
  [30, "340", "294062.5", "365937.5", "Ilakaka"],
  [31, "341", "294062.5", "366562.5", "Ilakaka"],
  [32, "342", "294062.5", "367187.5", "Ilakaka"],
];

const tableText = [
  "Liste_Carrés",
  "NC | XV | YV | CM_NOMFIR | num",
  ...historicalRows.map(([nc, num, xv, yv, name]) => `${nc} | ${xv.replace(".", ",")} | ${yv.replace(".", ",")} | ${name} | ${num}`),
].join("\n");

const tableTextAscii = tableText.replace("Liste_Carrés", "Liste_Carres");
const tableTextDecimalDot = [
  "Liste_Carres",
  "NC | XV | YV | CM_NOMFIR | num",
  "1 | 292812.5 | 360937.5 | Ilakaka | 280",
  "2 | 292812.5 | 361562.5 | Ilakaka | 281",
  "3 | 292812.5 | 362187.5 | Ilakaka | 282",
].join("\n");

async function parse(text, clock = () => 0) {
  const result = await recognizeMadagascarCadastral({ text }, {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock }),
  });
  const normalized = normalizeMadagascarCadastral(result);
  const verification = await verifyMadagascarCadastral(normalized);
  return { result, normalized, verification };
}

test("Liste_Carrés signature", () => {
  assert.equal(canHandleMadagascarCadastral({ text: tableText }), true);
});

test("Liste_Carres signature", () => {
  assert.equal(canHandleMadagascarCadastral({ text: tableTextAscii }), true);
});

test("XV header", () => {
  assert.equal(/\bXV\b/.test(tableText), true);
});

test("YV header", () => {
  assert.equal(/\bYV\b/.test(tableText), true);
});

test("NC parse", () => {
  const rows = parseMadagascarCadastralRows({ text: tableText });
  assert.equal(rows[0].nc, "1");
  assert.equal(rows[31].nc, "32");
});

test("num parse", () => {
  const rows = parseMadagascarCadastralRows({ text: tableText });
  assert.equal(rows[0].num, "280");
  assert.equal(rows[31].num, "342");
});

test("CM_NOMFIR parse", () => {
  const rows = parseMadagascarCadastralRows({ text: tableText });
  assert.equal(rows[0].cmNomfir, "Ilakaka");
});

test("decimal comma", () => {
  const rows = parseMadagascarCadastralRows({ text: tableText });
  assert.equal(rows[0].xv, "292812.5");
  assert.equal(rows[0].yv, "360937.5");
});

test("decimal dot", () => {
  const rows = parseMadagascarCadastralRows({ text: tableTextDecimalDot });
  assert.equal(rows[0].xv, "292812.5");
});

test("32/32 rows", () => {
  assert.equal(parseMadagascarCadastralRows({ text: tableText }).length, 32);
});

test("first row", () => {
  const first = parseMadagascarCadastralRows({ text: tableText })[0];
  assert.deepEqual([first.num, first.xv, first.yv, first.cmNomfir], ["280", "292812.5", "360937.5", "Ilakaka"]);
});

test("last row", () => {
  const last = parseMadagascarCadastralRows({ text: tableText }).at(-1);
  assert.deepEqual([last.num, last.xv, last.yv, last.cmNomfir], ["342", "294062.5", "367187.5", "Ilakaka"]);
});

test("num vs NC distinction", () => {
  const rows = parseMadagascarCadastralRows({ text: tableText });
  assert.equal(rows[0].nc, "1");
  assert.equal(rows[0].num, "280");
});

test("source order", () => {
  const rows = parseMadagascarCadastralRows({ text: tableText });
  assert.deepEqual(rows.map((row) => row.num).slice(0, 12), ["280", "281", "282", "283", "284", "285", "286", "287", "288", "289", "290", "306"]);
});

test("XV exact values", () => {
  const rows = parseMadagascarCadastralRows({ text: tableText });
  assert.equal(rows.filter((row) => row.xv).length, 32);
  assert.equal(rows[22].xv, "294062.5");
});

test("YV exact values", () => {
  const rows = parseMadagascarCadastralRows({ text: tableText });
  assert.equal(rows.filter((row) => row.yv).length, 32);
  assert.equal(rows[31].yv, "367187.5");
});

test("Andriandampy row", () => {
  const rows = parseMadagascarCadastralRows({ text: tableText });
  assert.equal(rows[22].num, "333");
  assert.equal(rows[22].cmNomfir, "Andriandampy");
});

test("Ilakaka rows", () => {
  const rows = parseMadagascarCadastralRows({ text: tableText });
  assert.equal(rows.filter((row) => row.cmNomfir === "Ilakaka").length, 31);
});

test("map tick rejection", () => {
  const mapTicks = "Liste_Carres\nNC | XV | YV | CM_NOMFIR | num\n1 | 290625 | 295625 | Ilakaka | 280";
  assert.equal(canHandleMadagascarCadastral({ text: mapTicks }), false);
});

test("290625/295625 rejection", () => {
  const mapTicks = "Liste_Carres\n290625 295625 300625\n535625 540625 545625";
  assert.equal(parseMadagascarCadastralRows({ text: mapTicks }).length, 0);
});

test("generic projected rejection", () => {
  assert.equal(canHandleMadagascarCadastral({ text: "280 | 292812.5 | 360937.5" }), false);
});

test("Indonesia UTM rejection", () => {
  assert.equal(canHandleMadagascarCadastral({ text: "778807.293,9721476.737\n778808.000,9721477.000" }), false);
});

test("Kyrgyz GK rejection", () => {
  assert.equal(canHandleMadagascarCadastral({ text: "№ points | X | Y\n1 | 13261341 | 4607777" }), false);
});

test("WGS84 decimal rejection", () => {
  assert.equal(canHandleMadagascarCadastral({ text: "12.319572, -11.178174" }), false);
});

test("MGRS rejection", () => {
  assert.equal(canHandleMadagascarCadastral({ text: "47RLH 24469 42832" }), false);
});

test("DMS rejection", () => {
  assert.equal(canHandleMadagascarCadastral({ text: "11°27'45\"N 08°36'30\"W" }), false);
});

test("CRS EPSG:29702", async () => {
  const { result, normalized } = await parse(tableText);
  assert.equal(result.crsDecision.sourceCrs, MADAGASCAR_CADASTRAL_SOURCE_CRS);
  assert.equal(normalized.coordinates[0].sourceProjected.sourceCrs, MADAGASCAR_CADASTRAL_SOURCE_CRS);
});

test("EPSG:29702 -> EPSG:4326", () => {
  const converted = convertMadagascarCadastralToWgs84(292812.5, 360937.5);
  assert.equal(converted.crs, MADAGASCAR_CADASTRAL_OUTPUT_CRS);
  assert.equal(converted.longitude > 42 && converted.longitude < 52, true);
  assert.equal(converted.latitude > -27 && converted.latitude < -10, true);
});

test("historical ground truth transform", async () => {
  const { normalized } = await parse(tableText);
  assert.equal(normalized.coordinates.length, 32);
  assert.equal(normalized.coordinates.every((point) => point.sourceProjected.sourceCrs === "EPSG:29702"), true);
  assert.equal(normalized.coordinates.every((point) => Number(point.longitude) > 42 && Number(point.longitude) < 52), true);
});

test("cadastral cell geometry", () => {
  const rows = parseMadagascarCadastralRows({ text: tableText });
  const geometry = inferMadagascarCadastralCellGeometry(rows);
  assert.equal(geometry.semantics, MADAGASCAR_CADASTRAL_CELL_SEMANTICS);
  assert.equal(geometry.width, 625);
  assert.equal(geometry.height, 625);
});

test("multi-cell geometry", () => {
  const rows = parseMadagascarCadastralRows({ text: tableText });
  const cells = buildMadagascarCadastralCellPolygons(rows);
  assert.equal(cells.length, 32);
  assert.equal(cells[0].wgs84Polygon.length, 5);
  assert.equal(cells[0].sourceProjectedCorners.length, 5);
});

test("KML order", async () => {
  const { normalized } = await parse(tableText);
  const kml = toMadagascarCadastralKmlCoordinate(normalized.coordinates[0]);
  assert.equal(kml.split(",").length, 3);
  assert.equal(kml.startsWith(`${normalized.coordinates[0].longitude},${normalized.coordinates[0].latitude}`), true);
});

test("provider calls=0", async () => {
  const { result, verification } = await parse(tableText);
  assert.equal(result.providerCalls, 0);
  assert.equal(result.visionCalls, 0);
  assert.equal(result.ocrCalls, 0);
  assert.equal(verification.providerCalls, 0);
  assert.equal(verification.visionCalls, 0);
  assert.equal(verification.ocrCalls, 0);
});

test("deadline behavior", async () => {
  const { result } = await parse(tableText, () => 60000);
  assert.equal(result.handled, false);
  assert.equal(result.status, "deadline_exceeded");
  assert.equal(result.providerCalls, 0);
});

test("isolation", () => {
  const registry = createDefaultRecognizerRegistry();
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

test("runner dispatch", async () => {
  const madagascar = await runCoordinateEngineV3({ text: tableText }, {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock: () => 0 }),
  });
  assert.equal(madagascar.status, V3_RUNNER_STATUS.MATCHED);
  assert.equal(madagascar.recognizerId, "madagascar_cadastral");
  assert.equal(madagascar.normalized.precisionMode, MADAGASCAR_CADASTRAL_PRECISION_MODE);
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

console.log(`Coordinate Engine V3 Madagascar Cadastral Recognizer Regression: ${passed}/${tests.length} PASS`);
