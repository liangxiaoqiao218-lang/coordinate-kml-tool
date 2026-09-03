import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import { spawn } from "node:child_process";
import { once } from "node:events";
import crypto from "node:crypto";
import * as primaryRouting from "../server/recognition/family-primary-routing.js";
import {
  COORDINATE_CONFIRMATION_STATUS,
  COORDINATE_DECISION_STATE,
  COORDINATE_GATE_REASON,
  COORDINATE_QUALITY_GATE_STATUS,
  FINALIZED_COORDINATE_CRS,
  finalizeCoordinateResult
} from "../server/coordinate-finalizer/index.js";
import { buildFamilyAvailabilityBlockedEngine } from "../server/coordinate-finalizer/family-availability-policy.js";
import {
  buildMadagascarCadastralCellPolygons,
  collapseExactRepeatedCoordinateSequence,
  extractMadagascarCadastralRows,
  getIndonesiaUtm50Info,
  hasMadagascarMapGridTickTakeover,
  hasStrongPrintedProjectedTableEvidence
} from "../server/recognition/family-primary-routing.js";
import { utmToWgs84 } from "../server/projection/utm.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const goldenPath = path.join(root, "regression-samples", "production-recognition-recovery-p0", "golden-records.json");
const golden = JSON.parse(await readFile(goldenPath, "utf8"));
const replay = JSON.parse(await readFile(path.join(root, "release-governance/p0-deterministic-replay-manifest.json"), "utf8"));
const releaseGate = JSON.parse(await readFile(path.join(root, "release-governance/p0-release-gate-governance.json"), "utf8"));
const serverSource = await readFile(path.join(root, "server.js"), "utf8");
// Execute the actual runtime function declarations without app startup or Provider I/O.
const runtime = vm.createContext({ ...primaryRouting, utmToWgs84, Buffer, crypto,
  process: { env: {} }, setTimeout: () => ({ unref() {} }) });
const declarations = [];
for (const start of serverSource.matchAll(/^(?:async )?function \w+\(/gm)) {
  const tail = serverSource.slice(start.index);
  for (const end of tail.matchAll(/^\}/gm)) {
    const candidateSource = tail.slice(0, end.index + 1);
    try { new vm.Script(candidateSource); declarations.push(candidateSource); break; } catch { /* template or inner brace */ }
  }
}
vm.runInContext(declarations.join("\n"), runtime);
for (const name of ['noCoordinatesText', 'MGRS_BANDS', 'MGRS_COLUMN_SETS', 'MGRS_ROW_SETS', 'MOZAMBIQUE_TETE_KNOWN_ROW_TOLERANCE']) {
  vm.runInContext(serverSource.match(new RegExp(`^const ${name} = .+;$`, 'm'))[0], runtime);
}
vm.runInContext('let p0QualificationAcquisition = null; let p0QualificationAcquisitionUsed = false; const aliyunBaseURL = "http://127.0.0.1:1/v1";', runtime);
const structuredText = replay.records.find(record => record.caseId === "indonesia-dms-real-001").approvedAcquisitionLines.join("\n");
const observedText = replay.realAcquisitionObservations[0].observedFinalRawTextLines.join("\n");
if (process.argv[2] === '--http-candidate') {
  const { default: http } = await import('node:http');
  let acquisitions = 0;
  const scenario = process.argv[3];
  globalThis.fetch = async (url, init) => {
    if (String(url) !== 'http://127.0.0.1:1/v1/chat/completions') throw new Error('TEST_EXTERNAL_NETWORK_FORBIDDEN');
    acquisitions += 1;
    if (acquisitions > 1) throw new Error('TEST_UNEXPECTED_SECOND_ACQUISITION');
    const prompt = JSON.parse(init.body).messages.map(message => JSON.stringify(message.content)).join(' ');
    assert.ok(prompt.includes('缺失的 CRS'));
    const content = scenario === 'observed' ? observedText : scenario === 'mismatch'
      ? structuredText.replace('119°30\'40.863" E', '120°30\'40.863" E') : structuredText;
    return new Response(JSON.stringify({ choices: [{ message: { content } }] }), { status: 200, headers: { 'content-type': 'application/json' } });
  };
  const nativeListen = http.Server.prototype.listen;
  http.Server.prototype.listen = function (port, callback) {
    this.once('listening', () => process.send({ port: this.address().port }));
    return nativeListen.call(this, port, '127.0.0.1', callback);
  };
  process.on('message', message => { if (message === 'stats') process.send({ acquisitions }); });
  await import('../server.js');
  await new Promise(() => {});
}

async function runHttpCandidate(scenario) {
  // Process-local module fault injection, before imports. No runtime failure flag or extra file.
  const faultBodies = {
    null: 'return null;', exception: 'throw new Error("PRIVATE_TRANSFORM_ERROR_MUST_NOT_ESCAPE");',
    nonfinite: 'return {lat: NaN, lon: Infinity};', outofrange: 'return {lat: -91, lon: 181};',
    incomplete: 'if (++calls === 3) return {lat: -2}; return originalUtmToWgs84(...args);',
    degenerate: 'return {lat: -2, lon: 119};',
    selfintersection: 'return [{lat:0,lon:0},{lat:2,lon:2},{lat:0,lon:3},{lat:2,lon:0}][calls++];'
  };
  const fault = faultBodies[scenario];
  const preload = fault ? `import { registerHooks } from 'node:module';
    registerHooks({load(url, context, nextLoad) {
      const result = nextLoad(url, context);
      if (!url.endsWith('/server/projection/utm.js')) return result;
      const source = String(result.source).replace('export function utmToWgs84(', 'function originalUtmToWgs84(');
      if (source === String(result.source)) throw new Error('FAULT_INJECTION_NOT_INSTALLED');
      return {...result, source: source + ${JSON.stringify(`\nlet calls=0; export function utmToWgs84(...args) { if(args[0] !== 50) return originalUtmToWgs84(...args); ${fault} }`)}};
    }});` : null;
  const child = spawn(process.execPath, [
    ...(preload ? ['--import', `data:text/javascript,${encodeURIComponent(preload)}`] : []),
    fileURLToPath(import.meta.url), '--http-candidate', scenario], {
    cwd: root, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    env: { SystemRoot: process.env.SystemRoot, PATH: process.env.PATH, NODE_ENV: 'test', PORT: '0',
      ENABLE_REGRESSION_TEST_MODE: 'true', ALIYUN_API_KEY: 'local-mock-only', ALIYUN_BASE_URL: 'http://127.0.0.1:1/v1',
      P0_QUALIFICATION_ACQUISITION_ENABLED: 'true', DOTENV_CONFIG_PATH: path.join(root, '__no_test_env__') }
  });
  // Drain output without retaining provider text or exposing it as production evidence.
  child.stdout.resume(); child.stderr.resume();
  const signal = AbortSignal.timeout(20000);
  try {
    const [{ port }] = await once(child, 'message', { signal });
    const form = new FormData();
    form.set('visitorId', 'coordinate-regression-p0-contract');
    // Synthetic marker bytes: no real-image upload, decoding or Provider execution.
    form.set('image', new Blob([Buffer.from([0xff, 0xd8, 0xff, 0xd9])], { type: 'image/jpeg' }), 'synthetic-coordinate-image.jpg');
    const response = await fetch(`http://127.0.0.1:${port}/api/recognize-coordinates`, { method: 'POST',
      headers: { 'x-regression-test': '1', 'x-regression-case-id': 'indonesia-dms-real-001' }, body: form, signal });
    const payload = await response.json();
    const statsPromise = once(child, 'message', { signal });
    child.send('stats');
    const [stats] = await statsPromise;
    assert.equal(response.status, 200, JSON.stringify(payload));
    assert.equal(stats.acquisitions, 1);
    const traceResponse = await fetch(`http://127.0.0.1:${port}/api/regression/recognition-trace/${response.headers.get('x-recognition-request-id')}`, { headers: { 'x-regression-test': '1' }, signal });
    const trace = await traceResponse.json();
    assert.equal(trace.acquisitionEvidence == null, true, 'synthetic bytes cannot claim real acquisition identity');
    return payload;
  } finally {
    const ended = once(child, 'exit');
    child.kill();
    await ended;
  }
}
const cases = [];
function test(name, fn) { cases.push({ name, fn }); }

const polygon = Object.freeze({
  type: "Polygon",
  coordinates: Object.freeze([Object.freeze([
    Object.freeze([119.50, -2.51]), Object.freeze([119.52, -2.51]),
    Object.freeze([119.52, -2.53]), Object.freeze([119.50, -2.51])
  ])])
});
function candidate(overrides = {}) {
  return {
    resultId: "p0-recovery-result",
    resultRevision: 1,
    currentRevision: 1,
    confirmedRevision: null,
    sourceAuthority: "legacy",
    coordinateType: "indonesia_utm50_projected",
    precisionMode: "indonesia-utm50s-projected",
    family: "indonesia_utm50_projected",
    availabilityStatus: "AVAILABLE",
    crs: FINALIZED_COORDINATE_CRS,
    geometry: polygon,
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.PENDING,
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED,
    technicalKmlReady: true,
    currentAuthorizedGeometryExportable: true,
    requiresReview: true,
    kmlReady: false,
    groups: [{ groupId: "group_1", requiresReview: true, kmlReady: false }],
    ...overrides
  };
}

test("blocked payload engine is locally derived without unavailableEngine ReferenceError", () => {
  const engine = buildFamilyAvailabilityBlockedEngine({
    availability: { family: "madagascar_cadastral_grid" },
    coordinateType: "madagascar_cadastral_grid",
    precisionMode: "cadastral-grid-num-xv-yv"
  });
  assert.equal(engine.coordinate_type, "madagascar_cadastral_grid");
  assert.equal(engine.groups.length, 0);
});

test("Indonesia DMS real fixture preserves E/S signs and four polygon points", () => {
  const record = golden.records.find(item => item.id === "indonesia-dms-real-001");
  const text = `SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S\nNo. | X | Y | Latitude | Longitude\n${record.sourceRows.join("\n")}`;
  const info = getIndonesiaUtm50Info(text, { transform: utmToWgs84 });
  assert.equal(info.rowCount, 4);
  assert.equal(info.crs, "EPSG:32750");
  assert.equal(info.rows.every(row => row.lat < 0 && row.lon > 0), true);
});

test("Indonesia projected duplicate full sequence collapses to six without global dedupe", () => {
  const record = golden.records.find(item => item.id === "indonesia-projected-real-002");
  const duplicated = [...record.sourceRows, ...record.sourceRows];
  const text = `SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S\nNo. | X | Y\n${duplicated.join("\n")}`;
  const info = getIndonesiaUtm50Info(text, { transform: utmToWgs84 });
  assert.equal(info.rowCount, 6);
  assert.equal(info.duplicateSequenceCollapsed, true);
  assert.deepEqual(collapseExactRepeatedCoordinateSequence(["A", "B", "A"]), ["A", "B", "A"]);
  assert.deepEqual(collapseExactRepeatedCoordinateSequence(["A", "B", "C", "A"]), ["A", "B", "C", "A"]);
});

test("printed projected table is strong non-handwritten evidence", async () => {
  const record = golden.records.find(item => item.id === "indonesia-dms-real-001");
  const text = `SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S\nNo. | X | Y | Latitude | Longitude\n${record.sourceRows.join("\n")}`;
  assert.equal(hasStrongPrintedProjectedTableEvidence(text), true);
  assert.equal(hasStrongPrintedProjectedTableEvidence(`1. 11°43'09.20\"N 09°00'56.03\"W\n2. 11°42'09.20\"N 09°01'56.03\"W`), false);
  const serverSource = await readFile(path.join(root, "server.js"), "utf8");
  assert.match(serverSource, /&& !hasStrongPrintedProjectedTable/);
});

test("Madagascar 32-row stable table is parsed and map ticks cannot synthesize rows", () => {
  const record = golden.records.find(item => item.id === "madagascar-cadastral-real-001");
  const text = `Liste_Carrés\nNC | XV | YV | CM_NOMFIR | num\n${record.sourceRows.map((row, index) => `${index + 1} | ${row.split(" | ").slice(1).join(" | ")} | Ilakaka | ${row.split(" | ")[0]}`).join("\n")}`;
  const rows = extractMadagascarCadastralRows(text);
  assert.equal(rows.length, 32);
  assert.deepEqual(rows[0], { num: "280", xv: "292812.5", yv: "360937.5" });
  const cells = buildMadagascarCadastralCellPolygons(rows);
  assert.equal(cells.length, 32);
  assert.equal(cells.every(cell => cell.points.length === 4), true);
  assert.equal(cells.every(cell => cell.points.every(point => Number.isFinite(point.lon) && Number.isFinite(point.lat))), true);
  const ticks = "290625 295625 300625\n535625 540625 545625 550625";
  assert.equal(hasMadagascarMapGridTickTakeover(ticks), true);
  assert.deepEqual(extractMadagascarCadastralRows(ticks), []);
});

test("review and confidence-only quality states do not block current authorized geometry KML", () => {
  const review = finalizeCoordinateResult(candidate(), { clock: () => "2026-09-02T00:00:00.000Z" });
  assert.equal(review.decisionState, COORDINATE_DECISION_STATE.REVIEW_REQUIRED);
  assert.equal(review.kmlReady, true);
  assert.equal(review.blockingReasons.length, 0);
  const quality = finalizeCoordinateResult(candidate({
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.FAILED,
    qualityFailureAuthorityImpact: "confidence_only"
  }), { clock: () => "2026-09-02T00:00:00.000Z" });
  assert.equal(quality.kmlReady, true);
});

test("confidence-only rejection and unavailable status do not retroactively revoke current geometry", () => {
  const rejected = finalizeCoordinateResult(candidate({
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.REJECTED,
    confirmationRejectionAuthorityImpact: "confidence_only"
  }), { clock: () => "2026-09-02T00:00:00.000Z" });
  assert.equal(rejected.kmlReady, true);
  const unavailable = finalizeCoordinateResult(candidate({
    availabilityStatus: "BLOCKED_BY_PROVIDER"
  }), { clock: () => "2026-09-02T00:00:00.000Z" });
  assert.equal(unavailable.kmlReady, true);
});

test("V3 non-authoritative output and invalid CRS confirmation remain hard blocked", () => {
  const v3 = finalizeCoordinateResult(candidate({
    sourceAuthority: "coordinate_engine_v3",
    v3ProductionAuthority: false,
    kmlAuthorityBlocked: true
  }), { clock: () => "2026-09-02T00:00:00.000Z" });
  assert.equal(v3.kmlReady, false);
  const invalidCrs = finalizeCoordinateResult(candidate({
    crs: { id: "UNCONFIRMED", axisOrder: "easting_northing" }
  }), { clock: () => "2026-09-02T00:00:00.000Z" });
  assert.equal(invalidCrs.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
  assert.equal(invalidCrs.kmlReady, false);
  const crsWarning = finalizeCoordinateResult(candidate({
    crs: { id: "UNCONFIRMED", axisOrder: "easting_northing" },
    crsUncertaintyConfidenceOnly: true
  }), { clock: () => "2026-09-02T00:00:00.000Z" });
  assert.equal(crsWarning.kmlReady, true);
});

test("invalid geometry and stale identity remain true KML hard blockers", () => {
  const invalid = finalizeCoordinateResult(candidate({ geometry: { type: "Point", coordinates: [999, 999] } }), { clock: () => "2026-09-02T00:00:00.000Z" });
  assert.equal(invalid.kmlReady, false);
  assert.ok(invalid.blockingReasons.some(reason => reason.code === COORDINATE_GATE_REASON.GEOMETRY_INVALID));
  const stale = finalizeCoordinateResult(candidate({ currentRevision: 2 }), { clock: () => "2026-09-02T00:00:00.000Z" });
  assert.equal(stale.kmlReady, false);
  assert.ok(stale.blockingReasons.some(reason => reason.code === COORDINATE_GATE_REASON.RESULT_REVISION_STALE));
});

test("all three real fixtures are byte-bound to frozen Golden records", async () => {
  assert.equal(golden.records.length, 3);
  for (const record of golden.records) {
    const fixturePath = path.resolve(path.dirname(goldenPath), record.fixture);
    const bytes = await readFile(fixturePath);
    assert.equal(bytes.length, record.bytes, record.id);
    assert.equal(createHash("sha256").update(bytes).digest("hex"), record.sha256, record.id);
  }
});

test("Quick Judge placeholder uses higher-specificity muted styling only", async () => {
  const html = await readFile(path.join(root, "index.html"), "utf8");
  assert.match(html, /\.judge-detail-content \.judge-detail-placeholder\s*\{\s*color:\s*#94a3b8;/);
  assert.match(html, /\.judge-detail-content p\s*\{[^}]*color:\s*#334155;/s);
});

test("actual observed DMS-only text is not Indonesia owner and does not trigger handwritten retry", () => {
  assert.equal(getIndonesiaUtm50Info(observedText, { transform: utmToWgs84 }).isIndonesiaUtm50, false);
  assert.equal(runtime.getHandwrittenDmsVisionRoutingEvidence(observedText, observedText).shouldRetry, false);
  assert.equal(runtime.getHandwrittenDmsInfo(observedText, observedText, { isOcrImage: true }).isHandwrittenDms, false);
  assert.equal(runtime.getHandwrittenDmsVisionRoutingEvidence(observedText, observedText, { file: { originalname: "handwritten_indonesia.jpg" }, hint: "handwritten" }).shouldRetry, false);
});

test("printed projected table cannot trigger handwritten retry even with misleading upload metadata", () => {
  assert.equal(runtime.getHandwrittenDmsVisionRoutingEvidence(structuredText, observedText, { file: { originalname: "handwritten.jpg" } }).shouldRetry, false);
  assert.equal(runtime.getHandwrittenDmsTimeoutRoutingEvidence({ originalname: "handwritten.jpg" }, "", { ocrText: structuredText }).shouldRetry, false);
  assert.equal(runtime.getHandwrittenDmsInfo(structuredText, observedText, { isOcrImage: true }).isHandwrittenDms, false);
});

test("independent EPSG:32750 transform and DMS crosscheck use transformed final coordinates", () => {
  let calls = 0;
  const info = getIndonesiaUtm50Info(structuredText, { transform: (...args) => { calls += 1; return utmToWgs84(...args); } });
  assert.equal(calls, 4);
  assert.equal(info.isIndonesiaUtm50, true);
  assert.equal(info.projectedTransformExecuted, true);
  assert.equal(info.dmsReferenceParsed, true);
  assert.equal(info.projectedDmsCrosscheckExecuted, true);
  assert.equal(info.projectedDmsCrosscheck, "PASS");
  assert.equal(info.crosscheckTolerance, 1e-6);
  for (const row of info.rows) {
    assert.deepEqual({ lat: row.lat, lon: row.lon }, utmToWgs84(50, row.projectedSourceCoordinates.easting, row.projectedSourceCoordinates.northing, false));
    assert.ok(row.maximumDifference <= info.crosscheckTolerance);
    assert.notEqual(row.lon, row.dmsReferenceCoordinates.lon, "must not substitute reference longitude");
  }
  const labeled = structuredText.replace('No | X | Y', 'Point | Easting | Northing').replace(/^([1-4]) \|/gm, (_, n) => `${String.fromCharCode(64 + Number(n))} |`);
  const letterInfo = getIndonesiaUtm50Info(labeled, { transform: utmToWgs84 });
  assert.equal(letterInfo.projectedDmsCrosscheck, 'PASS');
  assert.equal(letterInfo.rows[0].label, 'A');
});

test("DMS mismatch requires review without replacing transformed coordinates", () => {
  const mismatched = structuredText.replace('119°30\'40.863" E', '120°30\'40.863" E');
  assert.notEqual(mismatched, structuredText);
  const info = getIndonesiaUtm50Info(mismatched, { transform: utmToWgs84 });
  assert.equal(info.projectedDmsCrosscheck, "FAIL");
  assert.equal(info.requiresReview, true);
  assert.equal(info.rows[0].lon, utmToWgs84(50, 779271.176, 9720912.526, false).lon);
  assert.match(serverSource, /forceRequiresReview: indonesiaUtm50\.requiresReview/);
});

test("missing or failed transform never succeeds by substituting DMS", () => {
  for (const transform of [undefined, () => null, () => { throw new Error("failed"); }]) {
    const info = getIndonesiaUtm50Info(structuredText, { transform });
    assert.equal(info.isIndonesiaUtm50, true);
    assert.equal(info.structureConfirmed, true);
    assert.equal(info.transformStatus, 'FAILED');
    assert.equal(info.sourceRows.length, 4);
    assert.equal(info.rowCount, 0);
  }
});

test("incomplete DMS reference is review and missing title cannot acquire owner", () => {
  const info = getIndonesiaUtm50Info(structuredText.replace('119°30\'40.863" E', ''), { transform: utmToWgs84 });
  assert.equal(info.projectedDmsCrosscheck, "INCOMPLETE");
  assert.equal(info.requiresReview, true);
  assert.equal(getIndonesiaUtm50Info(structuredText.replace('UTM WGS 1984 ZONA 50S', ''), { transform: utmToWgs84 }).isIndonesiaUtm50, false);
});

test("damaged handwritten morphology retains retry without filename evidence", () => {
  const text = Array.from({ length: 4 }, (_, index) => `${index + 1}. 11°28.3126N 08°40.4213W`).join("\n");
  assert.equal(primaryRouting.getDmsDocumentEvidence(text).handwrittenPositiveSignal, true);
  assert.equal(runtime.getHandwrittenDmsVisionRoutingEvidence(text, text).shouldRetry, true);
});

test("explicit handwritten correction evidence retains retry on complete DMS", () => {
  const text = `${observedText}\n识别提示：手写坐标存在需核对字符`;
  assert.equal(runtime.getHandwrittenDmsVisionRoutingEvidence(text, observedText).shouldRetry, true);
});

test("hash-bound R5-R2 real handwritten stage-1 evidence retains retry and final classification", () => {
  // Exact sanitized stage text; artifact SHA256 1e8883f3c76b6ef9f9d72b2995217f11edae870e4f81db413a56fd5806736d14.
  // This is observed acquisition evidence, not Golden coordinates or a new truth upgrade.
  const text = `11°28'37.26"N,08°40'42.13"W
11°28'31.60"N,08°40'32.90"W
11°28'18.01"N,08°40'31.01"W
11°28'17.41"N,08°40'41.36"W
11°27'57.74"N,08°36'46.30"W
11°28'05.53"N,08°36'40.17"W
11°27'57.19"N,08°36'26.21"W
11°27'48.03"N,08°36'30.35"W
11°27'54.56"N,08°36'21.90"W
11°27'45.54"N,08°36'08.06"W
11°27'37.17"N,08°36'10.87"W
11°27'45.75"N,08°36'25.83"W
11°27'55.13"N,08°36'47.30"W
11°27'46.22"N,08°36'50.51"W
11°27'36.04"N,08°36'33.46"W
11°27'45.09"N,08°36'30.76"W

识别提示：手写坐标存在需核对字符，请结合原图逐行核对。`;
  assert.equal(createHash('sha256').update(text).digest('hex'), '495a43fcb5659a274fb3357fad12e95f7c792550f26daf8cfff6b41c18626444');
  assert.equal(runtime.getHandwrittenDmsVisionRoutingEvidence(text, text).shouldRetry, true);
  assert.equal(runtime.getHandwrittenDmsInfo(text, text, { isOcrImage: true }).isHandwrittenDms, true);
  assert.equal(runtime.getHandwrittenDmsTimeoutRoutingEvidence({ mimetype: 'image/jpeg' }, '', { ocrText: text }).shouldRetry, true);
});

for (const scenario of ['observed', 'structured', 'mismatch']) {
  test(`actual HTTP ${scenario} path uses one mocked acquisition and preserves finalizer`, async () => {
    const payload = await runHttpCandidate(scenario);
    if (scenario === 'observed') {
      assert.equal(payload.indonesiaUtm50?.isIndonesiaUtm50 === true, false);
      assert.equal(payload.coordinateEngineV2.coordinate_type === 'handwritten_dms_experimental', false);
    } else {
      assert.equal(payload.coordinateEngineV2.coordinate_type, 'indonesia_utm50_projected');
      assert.equal(payload.indonesiaUtm50.projectedTransformExecuted, true);
      assert.equal(payload.indonesiaUtm50.transformStatus, 'SUCCESS');
      assert.equal(payload.indonesiaUtm50.projectedDmsCrosscheck, scenario === 'mismatch' ? 'FAIL' : 'PASS');
      const first = payload.coordinateEngineV2.groups[0].points[0];
      assert.ok(Math.abs(first.lon - utmToWgs84(50, 779271.176, 9720912.526, false).lon) < 1e-11);
      assert.ok(Math.abs(payload.finalizedCoordinateResult.geometry.coordinates[0][0][0] - first.lon) < 1e-11);
      if (scenario === 'mismatch') assert.equal(payload.coordinateEngineV2.requires_review, true);
    }
    assert.equal(payload.finalizedCoordinateResult.kmlReady, true);
  });
}

for (const scenario of ['null', 'exception', 'nonfinite', 'outofrange', 'incomplete', 'degenerate', 'selfintersection']) {
  test(`actual HTTP transform ${scenario} preserves owner and evidence without DMS geometry or KML`, async () => {
    const payload = await runHttpCandidate(scenario);
    const info = payload.indonesiaUtm50;
    assert.equal(info.structureConfirmed, true);
    assert.equal(info.ownerIntent, 'indonesia_utm50_projected');
    assert.equal(info.crs, 'EPSG:32750');
    assert.equal(info.axisOrder, 'easting_northing');
    assert.equal(info.transformStatus, 'FAILED');
    assert.equal(info.failureCode, 'INDONESIA_UTM50_TRANSFORM_FAILED');
    assert.ok(info.transformFailureReason);
    assert.equal(info.sourceRows.length, 4);
    assert.ok(info.sourceRows.every(row => row.dmsReferenceParsed && row.dmsReferenceCoordinates && row.projectedSourceCoordinates));
    assert.equal(info.sourceRows[0].projectedSourceCoordinates.easting, 779271.176);
    assert.equal(info.genericDmsFallbackAllowed, false);
    assert.equal(info.finalGeometrySource, 'NONE');
    assert.equal(info.projectedDmsCrosscheck, 'NOT_EXECUTED');
    assert.equal(payload.requiresReview, true);
    assert.equal(payload.coordinates, '');
    assert.equal(payload.coordinateEngineV2.coordinate_type, 'indonesia_utm50_projected');
    assert.equal(payload.coordinateEngineV2.requires_review, true);
    assert.deepEqual(payload.coordinateEngineV2.groups, []);
    assert.equal(payload.coordinateEngineV2.source.fallback_used, false);
    assert.equal(payload.finalizedCoordinateResult.geometry, null);
    assert.equal(typeof payload.finalizedCoordinateResult.decisionState, 'string');
    assert.notEqual(payload.finalizedCoordinateResult.decisionState, 'AUTO_EXPORT');
    assert.equal(payload.finalizedCoordinateResult.requiresReview, true);
    assert.equal(payload.finalizedCoordinateResult.kmlReady, false);
    assert.doesNotMatch(payload.warning, /INDONESIA_UTM50_TRANSFORM_FAILED|PRIVATE_TRANSFORM_ERROR/);
    assert.doesNotMatch(JSON.stringify(payload), /PRIVATE_TRANSFORM_ERROR/);
  });
}

test('transform failure and crosscheck failure have separate governance semantics', () => {
  const failures = releaseGate.indonesiaFailureSemantics;
  assert.equal(failures.TRANSFORM_FAIL.TRANSFORM, 'FAIL');
  assert.equal(failures.TRANSFORM_FAIL.FINAL_COORDINATES, 'UNAVAILABLE');
  assert.equal(failures.TRANSFORM_FAIL.KML, 'BLOCKED');
  assert.equal(failures.CROSSCHECK_FAIL.TRANSFORM, 'PASS');
  assert.equal(failures.CROSSCHECK_FAIL.FINAL_COORDINATES, 'PASS_FROM_PROJECTED');
  assert.equal(replay.transformFailureMayUseDmsSubstitute, false);
});

test("acquisition prompt preserves only visible structure and retains existing family rules", () => {
  assert.match(serverSource, /缺失的 CRS、UTM zone、表头或数值必须保持缺失/);
  assert.match(serverSource, /不要只取 DMS 而丢弃同一表内 X\/Y/);
  for (const text of ['num | XV | YV', 'point | X | Y', 'COORDENADAS GEOGRÁFICAS', 'Latitude nord', '手写 DMS 是逐字符转写任务']) assert.ok(serverSource.includes(text));
});

test("qualification sanitizer excludes credentials unrelated prose binary and full response fields", () => {
  assert.equal(runtime.sanitizeP0AcquisitionText(observedText).text, observedText);
  for (const bad of ['sk-ws-test', 'Bearer abc', 'Authorization: abc', 'api_key=abc', 'data:image/png;base64,AAA', 'https://private.example', 'x'.repeat(17000)]) {
    assert.equal(runtime.sanitizeP0AcquisitionText(`${observedText}\n${bad}`), null);
  }
  assert.equal(runtime.sanitizeP0AcquisitionText(`${observedText}\nunrelated personal note`).omittedLineCount, 1);
  assert.equal(runtime.sanitizeP0AcquisitionText(`${observedText}\n12345678901`).omittedLineCount, 1);
});

test("qualification retention is default-off production-blocked hash-bound and one-request-only", async () => {
  const file = await readFile(path.join(root, replay.records[0].fixture));
  const req = { ip: '127.0.0.1', socket: { remoteAddress: '127.0.0.1' }, get: key => key === 'x-regression-test' ? '1' : '', file: { buffer: file } };
  const response = { choices: [{ message: { content: observedText } }], api_key: "NOT_RETAINED" };
  const budget = { caseId: 'indonesia-dms-real-001', requestId: 'recognition_test_1' };
  assert.equal(runtime.retainP0QualificationAcquisition(req, response, budget), false);
  runtime.process.env = { P0_QUALIFICATION_ACQUISITION_ENABLED: 'true', ENABLE_REGRESSION_TEST_MODE: 'true', NODE_ENV: 'production' };
  assert.equal(runtime.retainP0QualificationAcquisition(req, response, budget), false);
  runtime.process.env.NODE_ENV = 'test';
  assert.equal(runtime.retainP0QualificationAcquisition({ ...req, ip: '192.168.1.1', socket: { remoteAddress: '192.168.1.1' } }, response, budget), false);
  assert.equal(runtime.retainP0QualificationAcquisition({ ...req, file: { buffer: Buffer.from('wrong') } }, response, budget), false);
  assert.equal(runtime.retainP0QualificationAcquisition(req, response, { ...budget, caseId: 'wrong' }), false);
  assert.equal(runtime.retainP0QualificationAcquisition(req, response, budget), true);
  assert.equal(runtime.retainP0QualificationAcquisition(req, response, { ...budget, requestId: 'recognition_test_2' }), false);
  const retained = vm.runInContext('p0QualificationAcquisition', runtime);
  assert.equal(retained.fullResponseJsonRetained, false);
  assert.equal(retained.acquisitionKind, 'LOCAL_REPLAY');
  assert.equal(retained.evidenceClass, 'LOCAL_REPLAY_ACQUISITION_EVIDENCE');
  assert.equal(JSON.stringify(retained).includes('NOT_RETAINED'), false);
});

test("approved replay history is immutable and cannot qualify real acquisition", () => {
  for (const record of replay.records) assert.equal(createHash('sha256').update(record.approvedAcquisitionLines.join('\n')).digest('hex'), record.acquisitionEvidenceSha256);
  assert.equal(replay.provesRealProviderAcquisition, false);
  assert.equal(createHash('sha256').update(observedText).digest('hex'), replay.realAcquisitionObservations[0].observedFinalRawTextSha256);
  assert.equal(replay.realAcquisitionObservations[0].provider1FullResponseJsonRetained, false);
  assert.equal(replay.realAcquisitionObservations[0].kmlSha256, 'c20a403045883707d6528cb34de5de52197763204ac3925437bff5d6581df245');
  assert.equal(releaseGate.deterministicReplayGateScope.mayAuthorizeEndToEndQualification, false);
  assert.equal(releaseGate.realAcquisitionQualification.currentRemediationState, 'REAL_ACQUISITION_NOT_QUALIFIED');
});

let passed = 0;
for (const entry of cases) {
  await entry.fn();
  passed += 1;
  console.log(`PASS ${entry.name}`);
}
console.log(`Production recognition recovery P0 regression: ${passed}/${cases.length} PASS`);
