import assert from "node:assert/strict";
import { access, readFile } from "node:fs/promises";
import path from "node:path";

import {
  extractCadastralGridRows,
  formatCadastralGridRows,
  getCadastralGridInfo,
  hasMadagascarCadastralStructuralSignature,
  hasMapGridTickTakeover,
  shouldRunEarlyMadagascarCadastralPriority
} from "../server/coordinate-evidence/cadastral-grid.js";

const expectedRows = [
  ["280", "292812.5", "360937.5"],
  ["281", "292812.5", "361562.5"],
  ["282", "292812.5", "362187.5"],
  ["283", "292812.5", "362812.5"],
  ["284", "292812.5", "363437.5"],
  ["285", "292812.5", "364062.5"],
  ["286", "292812.5", "364687.5"],
  ["287", "292812.5", "365312.5"],
  ["288", "292812.5", "365937.5"],
  ["289", "292812.5", "366562.5"],
  ["290", "292812.5", "367187.5"],
  ["306", "293437.5", "360937.5"],
  ["307", "293437.5", "361562.5"],
  ["308", "293437.5", "362187.5"],
  ["309", "293437.5", "362812.5"],
  ["310", "293437.5", "363437.5"],
  ["311", "293437.5", "364062.5"],
  ["312", "293437.5", "364687.5"],
  ["313", "293437.5", "365312.5"],
  ["314", "293437.5", "365937.5"],
  ["315", "293437.5", "366562.5"],
  ["316", "293437.5", "367187.5"],
  ["333", "294062.5", "361562.5"],
  ["334", "294062.5", "362187.5"],
  ["335", "294062.5", "362812.5"],
  ["336", "294062.5", "363437.5"],
  ["337", "294062.5", "364062.5"],
  ["338", "294062.5", "364687.5"],
  ["339", "294062.5", "365312.5"],
  ["340", "294062.5", "365937.5"],
  ["341", "294062.5", "366562.5"],
  ["342", "294062.5", "367187.5"]
];

const realFixtureCandidates = [
  path.resolve("artifacts", "fixtures", "madagascar_cadastral_candidate_001.png"),
  "C:\\Users\\Mir-1\\Documents\\Codex\\2026-08-05\\coordinate-kml-tool-v11-main-v1.1-clean\\test-fixtures\\coordinate-recognition\\madagascar\\madagascar_cadastral_candidate_001.png"
];

const frozenRightSideTable = `
Liste_Carrés
NC | XV | YV | CM_NOMFIR | num
1 | 292812,5 | 360937,5 | Ilakaka | 280
2 | 292812,5 | 361562,5 | Ilakaka | 281
3 | 292812,5 | 362187,5 | Ilakaka | 282
4 | 292812,5 | 362812,5 | Ilakaka | 283
5 | 292812,5 | 363437,5 | Ilakaka | 284
6 | 292812,5 | 364062,5 | Ilakaka | 285
7 | 292812,5 | 364687,5 | Ilakaka | 286
8 | 292812,5 | 365312,5 | Ilakaka | 287
9 | 292812,5 | 365937,5 | Ilakaka | 288
10 | 292812,5 | 366562,5 | Ilakaka | 289
11 | 292812,5 | 367187,5 | Ilakaka | 290
12 | 293437,5 | 360937,5 | Ilakaka | 306
13 | 293437,5 | 361562,5 | Ilakaka | 307
14 | 293437,5 | 362187,5 | Ilakaka | 308
15 | 293437,5 | 362812,5 | Ilakaka | 309
16 | 293437,5 | 363437,5 | Ilakaka | 310
17 | 293437,5 | 364062,5 | Ilakaka | 311
18 | 293437,5 | 364687,5 | Ilakaka | 312
19 | 293437,5 | 365312,5 | Ilakaka | 313
20 | 293437,5 | 365937,5 | Ilakaka | 314
21 | 293437,5 | 366562,5 | Ilakaka | 315
22 | 293437,5 | 367187,5 | Ilakaka | 316
23 | 294062,5 | 361562,5 | Andriandampy | 333
24 | 294062,5 | 362187,5 | Ilakaka | 334
25 | 294062,5 | 362812,5 | Ilakaka | 335
26 | 294062,5 | 363437,5 | Ilakaka | 336
27 | 294062,5 | 364062,5 | Ilakaka | 337
28 | 294062,5 | 364687,5 | Ilakaka | 338
29 | 294062,5 | 365312,5 | Ilakaka | 339
30 | 294062,5 | 365937,5 | Ilakaka | 340
31 | 294062,5 | 366562,5 | Ilakaka | 341
32 | 294062,5 | 367187,5 | Ilakaka | 342
`;

function assertExpectedRows(rows) {
  assert.equal(rows.length, 32);
  assert.deepEqual(rows.map(row => row.num), expectedRows.map(row => row[0]));
  assert.deepEqual(rows.map(row => row.xv), expectedRows.map(row => row[1]));
  assert.deepEqual(rows.map(row => row.yv), expectedRows.map(row => row[2]));
}

let passed = 0;

assert.equal(hasMadagascarCadastralStructuralSignature(frozenRightSideTable), true);
passed += 1;

const rows = extractCadastralGridRows(frozenRightSideTable);
assertExpectedRows(rows);
passed += 1;

const info = getCadastralGridInfo(frozenRightSideTable);
assert.equal(info.isCadastralGrid, true);
assert.equal(info.rowCount, 32);
assertExpectedRows(info.rows);
passed += 1;

const formatted = formatCadastralGridRows(rows);
assert.match(formatted, /^num \| XV \| YV/m);
assert.match(formatted, /280 \| 292812\.5 \| 360937\.5/);
assert.match(formatted, /342 \| 294062\.5 \| 367187\.5/);
assert.doesNotMatch(formatted, /Ilakaka|Andriandampy|CM_NOMFIR|\bNC\b/);
passed += 1;

const mapTickOnly = `
PROJET PERMIS MINIER AMPASIMAMITAKA
290625
295625
300625
535625
540625
545625
550625
540625,31625
540625,31525
`;
assert.equal(hasMapGridTickTakeover(mapTickOnly), true);
assert.deepEqual(extractCadastralGridRows(`Liste_Carrés\nXV\nYV\n${mapTickOnly}`), []);
passed += 1;

const priority = shouldRunEarlyMadagascarCadastralPriority({
  rawText: mapTickOnly,
  coordinates: "540625,31625\n540625,31525",
  fileName: "马达加斯加坐标.png"
});
assert.equal(priority.candidate, true);
assert.equal(priority.mapTickTakeover, true);
assert.equal(priority.reason, "madagascar_cue_with_map_tick_takeover");
passed += 1;

const fileNameOnlyPriority = shouldRunEarlyMadagascarCadastralPriority({
  rawText: "PROJET PERMIS MINIER AMPASIMAMITAKA\n290625\n295625\n300625",
  coordinates: "290625,345625\n295625,340625\n300625,335625",
  fileName: "马达加斯加坐标.png"
});
assert.equal(fileNameOnlyPriority.candidate, true);
assert.equal(fileNameOnlyPriority.madagascarCue, true);
assert.equal(fileNameOnlyPriority.reason, "madagascar_cue_with_map_tick_takeover");
passed += 1;

const structuralPriority = shouldRunEarlyMadagascarCadastralPriority({
  rawText: frozenRightSideTable,
  coordinates: ""
});
assert.equal(structuralPriority.candidate, true);
assert.equal(structuralPriority.structuralSignature, true);
assert.equal(structuralPriority.reason, "liste_carres_xv_yv_signature");
passed += 1;

let fixtureFound = false;
for (const fixture of realFixtureCandidates) {
  try {
    await access(fixture);
    fixtureFound = true;
    break;
  } catch {
    // Try the next known non-committed fixture location.
  }
}
assert.equal(fixtureFound, true, "Madagascar real fixture image must be available locally for this recovery regression");
passed += 1;

const serverSource = await readFile(new URL("../server.js", import.meta.url), "utf8");
assert.match(serverSource, /stage:\s*"madagascar_legacy_stable_route"/);
assert.match(serverSource, /prompt:\s*cadastralGridLayoutPrompt/);
assert.match(serverSource, /prompt:\s*cadastralGridTablePrompt/);
assert.ok(
  serverSource.indexOf("prompt: cadastralGridLayoutPrompt") < serverSource.indexOf("prompt: cadastralGridTablePrompt"),
  "Madagascar legacy route must restore layout detection before table reading"
);
passed += 1;

assert.match(serverSource, /MADAGASCAR_LEGACY_STABLE_ROUTE:candidate/);
assert.match(serverSource, /MADAGASCAR_LEGACY_STABLE_ROUTE:layout_detected/);
assert.match(serverSource, /MADAGASCAR_LEGACY_STABLE_ROUTE:accepted/);
assert.match(serverSource, /MADAGASCAR_CADASTRAL:projected_fallback_blocked/);
assert.match(serverSource, /CRS_EVIDENCE:skipped_for_madagascar_cadastral/);
assert.match(serverSource, /CRS_EVIDENCE:skipped_for_madagascar_cadastral_candidate/);
assert.match(serverSource, /blocking map-tick projected takeover/);
assert.doesNotMatch(serverSource, /buildMadagascarCadastralTableVisionTiles/);
assert.doesNotMatch(serverSource, /MADAGASCAR_CADASTRAL:right_table_crop/);
assert.ok(
  serverSource.indexOf('stage: "madagascar_legacy_stable_route"') < serverSource.indexOf("const crsVision = await runCrsVisionPass"),
  "Madagascar legacy stable route must run before CRS/UTM routing"
);
passed += 1;

const indexSource = await readFile(new URL("../index.html", import.meta.url), "utf8");
assert.match(indexSource, /MADAGASCAR_CADASTRAL_CRS_CODE\s*=\s*"EPSG:29702"/);
assert.match(indexSource, /convertMadagascarCadastralToWgs84/);
assert.match(indexSource, /buildCadastralGridKml/);
passed += 1;

console.log(`Madagascar Cadastral Stable Path Regression: ${passed}/12 PASS`);
