import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import {
  canHandleCoteDivoireDms,
  canHandleGenericDms,
  canHandleIndonesiaUtm,
  canHandleKyrgyzGk,
  canHandleMadagascarCadastral,
  canHandleMgrs,
  canHandleWgs84Decimal,
  canHandleWgs84Table,
  createDefaultRecognizerRegistry,
  INDONESIA_UTM_RECOGNIZER_ID,
  INDONESIA_UTM_VERIFICATION_TOLERANCE,
  normalizeIndonesiaUtm,
  parseIndonesiaUtmTable,
  recognizeIndonesiaUtm,
  runCoordinateEngineV3,
  verifyIndonesiaUtm,
  V3_RUNNER_STATUS,
} from "../server/coordinate-engine-v3/index.js";

const FIXTURE_001 = `SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S
No. | X | Y
1 | 779271,176 | 9720912,526
2 | 779554,165 | 9720912,526
3 | 779554,165 | 9720734,464
4 | 779271,176 | 9720734,464`;

const FIXTURE_002 = `SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S
No. | X | Y
1 | 778984,492 | 9721476,737
2 | 779099,680 | 9721476,848
3 | 779099,680 | 9721110,798
4 | 778875,519 | 9721110,798
5 | 778875,519 | 9721180,576
6 | 778984,492 | 9721180,576`;

const FIXTURE_003_ROWS = [
  [1, "778807,293", "9721476,737", "2°31'2.805\" S", "119°30'25.820\" E", -2.517445833, 119.507172222],
  [2, "778981,768", "9721477,288", "2°31'2.776\" S", "119°30'31.465\" E", -2.517437778, 119.508740278],
  [3, "778982,700", "9721182,351", "2°31'12.373\" S", "119°30'31.513\" E", -2.520103611, 119.508753611],
  [4, "778855,308", "9721181,948", "2°31'12.394\" S", "119°30'27.392\" E", -2.520109444, 119.507608889],
  [5, "778855,543", "9721107,284", "2°31'14.823\" S", "119°30'27.404\" E", -2.520784167, 119.507612222],
  [6, "778980,724", "9721107,010", "2°31'14.824\" S", "119°30'31.454\" E", -2.520784444, 119.508737222],
  [7, "778980,920", "9720910,990", "2°31'21.202\" S", "119°30'31.473\" E", -2.522556111, 119.508742500],
  [8, "779100,477", "9720911,109", "2°31'21.191\" S", "119°30'35.340\" E", -2.522553056, 119.509816667],
  [9, "779100,599", "9720788,271", "2°31'25.188\" S", "119°30'35.352\" E", -2.523663333, 119.509820000],
  [10, "778950,926", "9720787,948", "2°31'25.208\" S", "119°30'30.510\" E", -2.523668889, 119.508475000],
  [11, "778950,926", "9720833,787", "2°31'23.716\" S", "119°30'30.507\" E", -2.523254444, 119.508474167],
  [12, "778927,907", "9720833,787", "2°31'23.718\" S", "119°30'29.762\" E", -2.523255000, 119.508267222],
  [13, "778927,907", "9720922,219", "2°31'20.840\" S", "119°30'29.757\" E", -2.522455556, 119.508265833],
  [14, "778906,895", "9720922,219", "2°31'20.842\" S", "119°30'29.077\" E", -2.522456111, 119.508076944],
  [15, "778906,895", "9721078,633", "2°31'15.752\" S", "119°30'29.067\" E", -2.521042222, 119.508074167],
  [16, "778807,082", "9721078,633", "2°31'15.758\" S", "119°30'25.838\" E", -2.521043889, 119.507177222],
];

function makeFixture003(rows = FIXTURE_003_ROWS) {
  return `SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S
No. | X | Y | Latitude | Longitude
${rows.map((row) => `${row[0]} | ${row[1]} | ${row[2]} | ${row[3]} | ${row[4]}`).join("\n")}`;
}

function test(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    throw error;
  }
}

async function testAsync(name, fn) {
  try {
    await fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    throw error;
  }
}

async function parse(text) {
  const result = await recognizeIndonesiaUtm({ text });
  const normalized = normalizeIndonesiaUtm(result);
  const verification = await verifyIndonesiaUtm(normalized, { input: { text } });
  return { result, normalized, verification };
}

function assertRows(parsed, expectedCount) {
  assert.equal(parsed.rows.length, expectedCount);
  parsed.rows.forEach((row, index) => {
    assert.equal(row.point, String(index + 1));
    assert.equal(Number.isFinite(row.easting), true);
    assert.equal(Number.isFinite(row.northing), true);
  });
}

test("Indonesia signature", () => {
  assert.equal(canHandleIndonesiaUtm({ text: makeFixture003() }), true);
});

test("WGS84 UTM wording", () => {
  assert.equal(canHandleIndonesiaUtm({ text: "UTM WGS84 Zone 50S\nNo | X | Y\n1 | 778807.293 | 9721476.737" }), true);
});

test("ZONA 50S", () => {
  const parsed = parseIndonesiaUtmTable({ text: makeFixture003() });
  assert.equal(parsed.crs.zone, 50);
  assert.equal(parsed.crs.hemisphere, "south");
});

test("Zone 50 South", () => {
  const parsed = parseIndonesiaUtmTable({ text: "WGS 84 / UTM Zone 50 South\nNo | X | Y\n1 | 778807.293 | 9721476.737" });
  assert.equal(parsed.crs.zone, 50);
  assert.equal(parsed.crs.hemisphere, "south");
});

test("point labels", () => {
  assert.deepEqual(parseIndonesiaUtmTable({ text: makeFixture003() }).rows.map((row) => row.point), FIXTURE_003_ROWS.map((row) => String(row[0])));
});

test("X=easting", () => {
  assert.equal(parseIndonesiaUtmTable({ text: makeFixture003() }).rows[0].easting, 778807.293);
});

test("Y=northing", () => {
  assert.equal(parseIndonesiaUtmTable({ text: makeFixture003() }).rows[0].northing, 9721476.737);
});

test("no axis swap", () => {
  const row = parseIndonesiaUtmTable({ text: makeFixture003() }).rows[0];
  assert.equal(row.easting < row.northing, true);
});

test("decimal dot", () => {
  const parsed = parseIndonesiaUtmTable({ text: "UTM WGS84 Zone 50S\nNo | X | Y\n1 | 778807.293 | 9721476.737" });
  assert.equal(parsed.rows[0].easting, 778807.293);
});

test("decimal comma", () => {
  const parsed = parseIndonesiaUtmTable({ text: "UTM WGS84 Zone 50S\nNo | X | Y\n1 | 778807,293 | 9721476,737" });
  assert.equal(parsed.rows[0].northing, 9721476.737);
});

test("EPSG:32750 resolve", () => {
  assert.equal(parseIndonesiaUtmTable({ text: makeFixture003() }).crs.epsg, "EPSG:32750");
});

test("missing CRS", async () => {
  const result = await recognizeIndonesiaUtm({ text: "SISTEM KOORDINAT: UTM WGS 1984\nNo | X | Y\n1 | 778807.293 | 9721476.737" });
  const normalized = normalizeIndonesiaUtm(result);
  assert.equal(result.status, "crs_unresolved");
  assert.equal(normalized.technicalKmlReady, false);
});

test("plain X/Y no context NO_MATCH", () => {
  assert.equal(canHandleIndonesiaUtm({ text: "778807.293,9721476.737" }), false);
});

test("#001 rows", () => {
  assertRows(parseIndonesiaUtmTable({ text: FIXTURE_001 }), 4);
});

await testAsync("#001 transform", async () => {
  const { normalized } = await parse(FIXTURE_001);
  assert.equal(normalized.coordinates.length, 4);
  assert.equal(normalized.coordinates.every((point) => point.latitude < 0 && point.longitude > 119), true);
});

test("#002 rows", () => {
  assertRows(parseIndonesiaUtmTable({ text: FIXTURE_002 }), 6);
});

await testAsync("#002 transform", async () => {
  const { normalized } = await parse(FIXTURE_002);
  assert.equal(normalized.coordinates.length, 6);
  assert.equal(normalized.coordinates.every((point) => point.latitude < 0 && point.longitude > 119), true);
});

test("#003 16 rows", () => {
  assertRows(parseIndonesiaUtmTable({ text: makeFixture003() }), 16);
});

test("#003 source order", () => {
  assert.deepEqual(parseIndonesiaUtmTable({ text: makeFixture003() }).rows.map((row) => row.point), Array.from({ length: 16 }, (_, index) => String(index + 1)));
});

await testAsync("#003 Polygon", async () => {
  const { normalized } = await parse(makeFixture003());
  assert.equal(normalized.geometryType, "polygon");
});

await testAsync("#003 transform 16/16", async () => {
  const { normalized } = await parse(makeFixture003());
  assert.equal(normalized.coordinates.length, 16);
  normalized.coordinates.forEach((point, index) => {
    assert.ok(Math.abs(point.latitude - FIXTURE_003_ROWS[index][5]) <= INDONESIA_UTM_VERIFICATION_TOLERANCE);
    assert.ok(Math.abs(point.longitude - FIXTURE_003_ROWS[index][6]) <= INDONESIA_UTM_VERIFICATION_TOLERANCE);
  });
});

await testAsync("#003 max error <=1e-6", async () => {
  const { verification } = await parse(makeFixture003());
  assert.equal(verification.status, "match");
  assert.ok(verification.maximumDifference <= INDONESIA_UTM_VERIFICATION_TOLERANCE);
});

await testAsync("DMS unavailable", async () => {
  const { normalized, verification } = await parse("SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S\nNo | X | Y\n1 | 778807.293 | 9721476.737\n2 | 778981.768 | 9721477.288\n3 | 778982.700 | 9721182.351");
  assert.equal(verification.status, "unavailable");
  assert.equal(normalized.technicalKmlReady, true);
});

await testAsync("DMS all match", async () => {
  const { normalized, verification } = await parse(makeFixture003());
  assert.equal(verification.status, "match");
  assert.equal(normalized.warnings.length, 0);
});

await testAsync("point-label mapping", async () => {
  const swapped = `SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S
No. | X | Y | Latitude | Longitude
13 | 778927,907 | 9720922,219 | 2°31'20.840" S | 119°30'29.757" E
1 | 778807,293 | 9721476,737 | 2°31'2.805" S | 119°30'25.820" E
2 | 778981,768 | 9721477,288 | 2°31'2.776" S | 119°30'31.465" E`;
  const { verification } = await parse(swapped);
  assert.equal(verification.status, "match");
});

await testAsync("P13 wrong reference", async () => {
  const rows = FIXTURE_003_ROWS.map((row) => row[0] === 13 ? [...row.slice(0, 4), "119°30'29.795\" E", row[5], row[6]] : row);
  const { verification } = await parse(makeFixture003(rows));
  assert.equal(verification.status, "mismatch");
  assert.deepEqual(verification.mismatchedPointLabels, ["13"]);
});

await testAsync("P13 warning", async () => {
  const rows = FIXTURE_003_ROWS.map((row) => row[0] === 13 ? [...row.slice(0, 4), "119°30'29.795\" E", row[5], row[6]] : row);
  const { normalized } = await parse(makeFixture003(rows));
  assert.equal(normalized.warnings.some((warning) => warning.code === "UTM_REFERENCE_MISMATCH"), true);
});

await testAsync("P13 KML-ready", async () => {
  const rows = FIXTURE_003_ROWS.map((row) => row[0] === 13 ? [...row.slice(0, 4), "119°30'29.795\" E", row[5], row[6]] : row);
  const { normalized } = await parse(makeFixture003(rows));
  assert.equal(normalized.technicalKmlReady, true);
});

await testAsync("P4 wrong X/Y", async () => {
  const rows = FIXTURE_003_ROWS.map((row) => row[0] === 4 ? [4, "778855,308", "9721188,194", row[3], row[4], row[5], row[6]] : row);
  const { verification } = await parse(makeFixture003(rows));
  assert.equal(verification.status, "mismatch");
  assert.deepEqual(verification.mismatchedPointLabels, ["4"]);
});

await testAsync("P4 suspected point", async () => {
  const rows = FIXTURE_003_ROWS.map((row) => row[0] === 4 ? [4, "778855,308", "9721188,194", row[3], row[4], row[5], row[6]] : row);
  const { normalized } = await parse(makeFixture003(rows));
  assert.deepEqual(normalized.suspectedPoints.map((point) => point.point), ["4"]);
});

await testAsync("P4 KML-ready", async () => {
  const rows = FIXTURE_003_ROWS.map((row) => row[0] === 4 ? [4, "778855,308", "9721188,194", row[3], row[4], row[5], row[6]] : row);
  const { normalized } = await parse(makeFixture003(rows));
  assert.equal(normalized.technicalKmlReady, true);
});

await testAsync("partial reference coverage", async () => {
  const text = `SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S
No. | X | Y | Latitude | Longitude
1 | 778807,293 | 9721476,737 | 2°31'2.805" S | 119°30'25.820" E
2 | 778981,768 | 9721477,288 |  |
3 | 778982,700 | 9721182,351 | 2°31'12.373" S | 119°30'31.513" E`;
  const { verification } = await parse(text);
  assert.equal(verification.status, "partial");
});

test("malformed X", () => {
  const parsed = parseIndonesiaUtmTable({ text: "UTM WGS84 Zone 50S\nNo | X | Y\n1 | bad | 9721476.737" });
  assert.equal(parsed.rows.length, 0);
});

test("malformed Y", () => {
  const parsed = parseIndonesiaUtmTable({ text: "UTM WGS84 Zone 50S\nNo | X | Y\n1 | 778807.293 | bad" });
  assert.equal(parsed.rows.length, 0);
});

test("WGS84 decimal rejection", () => {
  assert.equal(canHandleIndonesiaUtm({ text: "12.319572, -11.178174" }), false);
});

test("WGS84 table rejection", () => {
  assert.equal(canHandleIndonesiaUtm({ text: "Longitude | Latitude\n16.0320 | 3.7638" }), false);
});

test("MGRS rejection", () => {
  assert.equal(canHandleIndonesiaUtm({ text: "47RLH 24469 42832" }), false);
});

test("generic DMS rejection", () => {
  assert.equal(canHandleIndonesiaUtm({ text: "11°27'45\"N 08°36'30\"W" }), false);
});

test("Kyrgyz GK rejection", () => {
  assert.equal(canHandleIndonesiaUtm({ text: "№ points | X | Y\n1 | 13261341 | 4607777\n2 | 13261345 | 4607778\n3 | 13261350 | 4607780" }), false);
});

test("Madagascar rejection", () => {
  assert.equal(canHandleIndonesiaUtm({ text: "Liste_Carres\nNC | XV | YV | CM_NOMFIR | num\n1 | 292812,5 | 360937,5 | Ilakaka | 280" }), false);
});

test("Côte d’Ivoire rejection", () => {
  assert.equal(canHandleIndonesiaUtm({ text: "Point | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"" }), false);
});

await testAsync("Standard ambiguity=0", async () => {
  const result = await runCoordinateEngineV3({ text: makeFixture003() });
  assert.equal(result.status, V3_RUNNER_STATUS.MATCHED);
  assert.equal(result.recognizerId, INDONESIA_UTM_RECOGNIZER_ID);
});

await testAsync("provider=0", async () => {
  const { result, verification } = await parse(makeFixture003());
  assert.equal(result.providerCalls, 0);
  assert.equal(verification.providerCalls, 0);
});

await testAsync("vision=0", async () => {
  const { result, verification } = await parse(makeFixture003());
  assert.equal(result.visionCalls, 0);
  assert.equal(verification.visionCalls, 0);
});

await testAsync("OCR=0", async () => {
  const { result, verification } = await parse(makeFixture003());
  assert.equal(result.ocrCalls, 0);
  assert.equal(verification.ocrCalls, 0);
});

test("no retry", () => {
  const source = readFileSync("server/coordinate-engine-v3/recognizers/indonesia-utm/index.js", "utf8")
    + readFileSync("server/coordinate-engine-v3/recognizers/indonesia-utm/parser.js", "utf8")
    + readFileSync("server/coordinate-engine-v3/recognizers/indonesia-utm/verify.js", "utf8");
  assert.equal(/retry/i.test(source), false);
});

test("no reread", () => {
  const source = readFileSync("server/coordinate-engine-v3/recognizers/indonesia-utm/index.js", "utf8")
    + readFileSync("server/coordinate-engine-v3/recognizers/indonesia-utm/parser.js", "utf8")
    + readFileSync("server/coordinate-engine-v3/recognizers/indonesia-utm/verify.js", "utf8");
  assert.equal(/reread/i.test(source), false);
});

await testAsync("no V2 control fields", async () => {
  const { normalized } = await parse(makeFixture003());
  assert.equal(normalized.confirmationStatus, undefined);
  assert.equal(normalized.shadowWinner, undefined);
  assert.equal(normalized.migrationStatus, undefined);
  assert.equal(normalized.arbitrationProposal, undefined);
  assert.equal(normalized.dryRun, undefined);
});

await testAsync("deadline", async () => {
  const result = await recognizeIndonesiaUtm({ text: makeFixture003() }, {
    latencyBudget: { deadlineExceeded: () => true },
  });
  assert.equal(result.status, "deadline_exceeded");
  assert.equal(result.providerCalls, 0);
});

test("isolation", () => {
  const text = makeFixture003();
  const hits = [
    canHandleWgs84Decimal({ text }),
    canHandleWgs84Table({ text }),
    canHandleMgrs({ text }),
    canHandleGenericDms({ text }),
    canHandleKyrgyzGk({ text }),
    canHandleMadagascarCadastral({ text }),
    canHandleCoteDivoireDms({ text }),
    canHandleIndonesiaUtm({ text }),
  ].filter(Boolean).length;
  assert.equal(hits, 1);
});

const registry = createDefaultRecognizerRegistry();
assert.equal(registry.find((item) => item.coordinateType === "indonesia_utm").portStatus, "IMPLEMENTED");

console.log("Coordinate Engine V3 Indonesia UTM Regression: 50/50 PASS");
