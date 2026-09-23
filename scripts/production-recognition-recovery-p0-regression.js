import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import { spawn } from "node:child_process";
import { once } from "node:events";
import crypto from "node:crypto";
import { createCoordinateImageIdentity } from "../server/recognition/coordinate-image-safety.js";
import { LOCAL_OCR_FAILURE_CODE, runCancellableOcrJob } from "../server/recognition/cancellable-ocr.js";
import {
  RECOGNITION_COMPLETENESS_DECISION,
  assessRecognitionCompleteness
} from "../server/recognition/recognition-completeness.js";
import {
  createCoordinateUsageCommitController,
  evaluateCoordinateUsageAuthority,
  isRecognitionRequestId
} from "../server/coordinate-usage-atomicity.js";
import * as primaryRouting from "../server/recognition/family-primary-routing.js";
import * as dmsSourceStructure from "../server/recognition/dms-source-structure.js";
import * as familyRetryPolicy from "../server/recognition/family-retry-policy.js";
import * as candidateSelection from "../server/recognition/candidate-selection.js";
import * as structuredCoordinateBoundary from "../server/structured-coordinate-boundary.js";
import {
  COORDINATE_CONFIRMATION_STATUS,
  COORDINATE_DECISION_STATE,
  COORDINATE_GATE_REASON,
  COORDINATE_QUALITY_GATE_STATUS,
  FINALIZED_COORDINATE_CRS,
  consumeFinalizedGeometry,
  finalizeCoordinateResult
} from "../server/coordinate-finalizer/index.js";
import { buildFamilyAvailabilityBlockedEngine } from "../server/coordinate-finalizer/family-availability-policy.js";
import { buildCoordinateVerificationResponse } from "../server/verification/index.js";
import {
  buildMadagascarCadastralCellPolygons,
  collapseExactRepeatedCoordinateSequence,
  extractMadagascarCadastralRows,
  getIndonesiaUtm50Info,
  getWgs84SinglePointEvidence,
  hasMadagascarMapGridTickTakeover,
  hasStrongPrintedProjectedTableEvidence
} from "../server/recognition/family-primary-routing.js";
import { utmToWgs84 } from "../server/projection/utm.js";
import {
  extractProviderProjectedCoordinateEvidence,
  LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS,
  normalizeLocalOcrStructuredEvidence
} from "../server/evidence-acquisition/local-ocr-map-layout-classifier.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const structuredFamilyOnly = process.argv.includes("--structured-family-only");
const goldenPath = path.join(root, "regression-samples", "production-recognition-recovery-p0", "golden-records.json");
// The structured-family-only path is synthetic and must not read historical
// recovery fixtures, replay manifests, or production qualification records.
const golden = structuredFamilyOnly ? null : JSON.parse(await readFile(goldenPath, "utf8"));
const replay = structuredFamilyOnly ? null : JSON.parse(await readFile(path.join(root, "release-governance/p0-deterministic-replay-manifest.json"), "utf8"));
const releaseGate = structuredFamilyOnly ? null : JSON.parse(await readFile(path.join(root, "release-governance/p0-release-gate-governance.json"), "utf8"));
const serverSource = await readFile(path.join(root, "server.js"), "utf8");
// Execute the actual runtime function declarations without app startup or Provider I/O.
const runtime = vm.createContext({ ...primaryRouting, ...dmsSourceStructure, ...familyRetryPolicy, ...candidateSelection, ...structuredCoordinateBoundary, utmToWgs84,
  isRecognitionRequestId, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS, Buffer, crypto,
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
const structuredText = structuredFamilyOnly ? "" : replay.records.find(record => record.caseId === "indonesia-dms-real-001").approvedAcquisitionLines.join("\n");
const observedText = structuredFamilyOnly ? "" : replay.realAcquisitionObservations[0].observedFinalRawTextLines.join("\n");
const syntheticPng = Buffer.from("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=", "base64");

function makeSpatialBmp(width = 900, height = 1400) {
  const rowBytes = Math.floor(((24 * width) + 31) / 32) * 4;
  const pixelBytes = rowBytes * height;
  const buffer = Buffer.alloc(54 + pixelBytes);
  buffer.write("BM", 0, "ascii");
  buffer.writeUInt32LE(buffer.length, 2);
  buffer.writeUInt32LE(54, 10);
  buffer.writeUInt32LE(40, 14);
  buffer.writeInt32LE(width, 18);
  buffer.writeInt32LE(height, 22);
  buffer.writeUInt16LE(1, 26);
  buffer.writeUInt16LE(24, 28);
  buffer.writeUInt32LE(pixelBytes, 34);
  return buffer;
}

const spatialBmp = makeSpatialBmp();
const syntheticSpatialIdentity = createCoordinateImageIdentity(
  { buffer: spatialBmp, mimetype: "image/bmp", size: spatialBmp.length },
  { requestId: "production-recovery-spatial-provenance", page: 1 }
);

function spatialLayoutFor(text, overrides = {}) {
  return String(text).split("\n").filter(Boolean).map((lineText, index) => ({
    text: lineText,
    bbox: [20, 20 + (index * 42), 860, 44 + (index * 42)],
    local_line_index: index,
    page: overrides.page ?? 1,
    resultRevision: overrides.resultRevision ?? 1
  }));
}

function spatialContractFor(source, layoutLines = spatialLayoutFor(source)) {
  const route = primaryRouting.classifyOneShotStructuredFamily({ text: source, layoutLines });
  return primaryRouting.createOneShotAcquisitionContract({
    route,
    sourceText: source,
    layoutLines,
    imageIdentity: syntheticSpatialIdentity,
    resultRevision: 1
  });
}
if (process.argv[2] === '--http-candidate') {
  const { default: http } = await import('node:http');
  let acquisitions = 0;
  const scenario = process.argv[3];
  globalThis.fetch = async (url, init) => {
    try {
      if (String(url) !== 'http://127.0.0.1:1/v1/chat/completions') throw new Error('TEST_EXTERNAL_NETWORK_FORBIDDEN');
      acquisitions += 1;
      if (acquisitions > 1) throw new Error('TEST_UNEXPECTED_SECOND_ACQUISITION');
      const prompt = JSON.parse(init.body).messages.map(message => JSON.stringify(message.content)).join(' ');
      if (scenario === 'generic-dms-review' || scenario === 'generic-dms-review-array'
        || scenario === 'generic-projected-review' || scenario === 'generic-projected-explicit'
        || scenario === 'generic-projected-contextual-utm30') {
        assert.ok(prompt.includes('UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE'));
        assert.ok(prompt.includes('CONTEXT |'));
      } else {
        assert.ok(prompt.includes('Never infer a family, CRS, axis order, direction, missing row, group, or coordinate'));
      }
    const providerText = scenario === 'generic-projected-review' || scenario === 'generic-projected-explicit'
      || scenario === 'generic-projected-contextual-utm30'
      ? [
          ...(scenario === 'generic-projected-explicit' ? ['WGS 84 / UTM 30N'] : []),
          ...(scenario === 'generic-projected-contextual-utm30'
            ? ['CONTEXT | Les coordonnées géographiques en UTM des sommets du site devant abriter l’activité sont consignées dans le tableau ci-dessous.']
            : []),
          'UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE',
          'Sommets | X | Y',
          'A | 727250,1219700',
          'B | 728400,1219700',
          'C | 728400,1219500',
          'D | 728700,1219500',
          'E | 728700,1220000',
          'F | 729150,1220000',
          'G | 729150,1219500',
          'H | 729200,1219500'
        ].join('\n')
      : scenario === 'generic-dms-review' || scenario === 'generic-dms-review-array'
      ? [
          'UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE',
          'Point | Latitude nord | Longitude ouest',
          "1 | 11° 43' 16.45'' | 09° 01' 13.67''",
          "2 | 11° 43' 09.20'' | 09° 00' 56.03''",
          "3 | 11° 43' 03.38'' | 09° 00' 58.67''",
          "4 | 11° 43' 11.30'' | 09° 01' 15.25''"
        ].join('\n')
      : scenario === 'observed' ? observedText : scenario === 'mismatch'
        ? structuredText.replace('119°30\'40.863" E', '120°30\'40.863" E') : structuredText;
    const content = scenario === 'generic-dms-review-array'
      ? providerText.split('\n').map(text => ({ type: 'text', text }))
      : providerText;
      return new Response(JSON.stringify({ choices: [{ message: { content } }] }), { status: 200, headers: { 'content-type': 'application/json' } });
    } catch (error) {
      process.send?.({ stubError: String(error?.message || error) });
      throw error;
    }
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
      P0_QUALIFICATION_ACQUISITION_ENABLED: 'true', SPATIAL_RESULT_ENABLED: 'true',
      DOTENV_CONFIG_PATH: path.join(root, '__no_test_env__') }
  });
  // Drain output without retaining provider text or exposing it as production evidence.
  child.stdout.resume(); child.stderr.resume();
  const signal = AbortSignal.timeout(20000);
  try {
    const [{ port }] = await once(child, 'message', { signal });
    if (scenario === 'manual-projected-review') {
      const coordinateText = [
        'A | 500000 | 1000000',
        'B | 501000 | 1001000',
        'C | 501000 | 1000000',
        'D | 500000 | 1001000'
      ].join('\n');
      const response = await fetch(`http://127.0.0.1:${port}/api/coordinate-manual-finalize`, {
        method: 'POST', headers: { 'content-type': 'application/json' }, signal,
        body: JSON.stringify({ coordinateText, requireConfirmation: false })
      });
      const payload = await response.json();
      assert.equal(response.status, 200, JSON.stringify(payload));
      assert.equal(payload.precisionMode, 'projected-x-y-review');
      assert.equal(payload.finalizedCoordinateResult?.coordinateType, 'projected_xy');
      assert.equal(payload.finalizedCoordinateResult?.kmlReady, false);
      assert.equal(payload.finalizedCoordinateResult?.geometry, null);
      const pending = payload.finalizedCoordinateResult;
      const missingCrsResponse = await fetch(`http://127.0.0.1:${port}/api/coordinate-projection-confirmation`, {
        method: 'POST', headers: { 'content-type': 'application/json' }, signal,
        body: JSON.stringify({ resultId: pending.resultId, resultRevision: pending.resultRevision,
          sourceCrs: '', coordinateText })
      });
      assert.equal(missingCrsResponse.status, 400, 'manual projected rows must not infer a CRS');
      const confirmationResponse = await fetch(`http://127.0.0.1:${port}/api/coordinate-projection-confirmation`, {
        method: 'POST', headers: { 'content-type': 'application/json' }, signal,
        body: JSON.stringify({ resultId: pending.resultId, resultRevision: pending.resultRevision,
          sourceCrs: 'utm30n', coordinateText })
      });
      const confirmation = await confirmationResponse.json();
      assert.equal(confirmationResponse.status, 200, JSON.stringify(confirmation));
      assert.equal(confirmation.geometryMode, 'points_only');
      assert.equal(confirmation.boundaryBlocked, true);
      assert.equal(confirmation.finalizedCoordinateResult?.geometry?.type, 'MultiPoint');
      assert.equal(confirmation.finalizedCoordinateResult?.kmlReady, false);
      assert.notEqual(confirmation.finalizedCoordinateResult?.kmlAuthorityBlocked, true);
      const confirmed = confirmation.finalizedCoordinateResult;
      const mapResponse = await fetch(`http://127.0.0.1:${port}/api/map-preview`, {
        method: 'POST', headers: { 'content-type': 'application/json' }, signal,
        body: JSON.stringify({ resultId: confirmed.resultId, resultRevision: confirmed.resultRevision,
          geometryHash: confirmed.geometryHash })
      });
      const mapPayload = await mapResponse.json();
      assert.equal(mapResponse.status, 200, JSON.stringify(mapPayload));
      assert.equal(mapPayload.mapPreviewObject?.geometry?.type, 'MultiPoint');
      const statsPromise = once(child, 'message', { signal });
      child.send('stats');
      const [stats] = await statsPromise;
      assert.equal(stats.acquisitions, 0, 'manual projected flow must not call a Provider');
      return { ...payload, projectedConfirmation: confirmation, projectedMapPreview: mapPayload,
        providerCallCount: stats.acquisitions };
    }
    const form = new FormData();
    form.set('visitorId', 'coordinate-regression-p0-contract');
    // Valid synthetic image bytes: no customer material or real Provider execution.
    form.set('image', new Blob([syntheticPng], { type: 'image/png' }), 'synthetic-coordinate-image.png');
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
    if (scenario === 'generic-projected-contextual-utm30'
      || scenario === 'generic-dms-review'
      || scenario === 'generic-dms-review-array') {
      const finalized = payload.finalizedCoordinateResult;
      const mapResponse = await fetch(`http://127.0.0.1:${port}/api/map-preview`, {
        method: 'POST', headers: { 'content-type': 'application/json' }, signal,
        body: JSON.stringify({ resultId: finalized.resultId, resultRevision: finalized.resultRevision,
          geometryHash: finalized.geometryHash })
      });
      const mapPreview = await mapResponse.json();
      assert.equal(mapResponse.status, 200, JSON.stringify(mapPreview));
      if (scenario === 'generic-dms-review' || scenario === 'generic-dms-review-array') {
        const confirmationResponse = await fetch(`http://127.0.0.1:${port}/api/coordinate-confirmation`, {
          method: 'POST', headers: { 'content-type': 'application/json' }, signal,
          body: JSON.stringify({ resultId: finalized.resultId, resultRevision: finalized.resultRevision,
            geometryHash: finalized.geometryHash, action: 'accept' })
        });
        const confirmation = await confirmationResponse.json();
        assert.equal(confirmationResponse.status, 200, JSON.stringify(confirmation));
        const confirmed = confirmation.finalizedCoordinateResult;
        const confirmedMapResponse = await fetch(`http://127.0.0.1:${port}/api/map-preview`, {
          method: 'POST', headers: { 'content-type': 'application/json' }, signal,
          body: JSON.stringify({ resultId: confirmed.resultId, resultRevision: confirmed.resultRevision,
            geometryHash: confirmed.geometryHash })
        });
        const confirmedMapPreview = await confirmedMapResponse.json();
        assert.equal(confirmedMapResponse.status, 200, JSON.stringify(confirmedMapPreview));
        return { ...payload, mapPreview, confirmedBoundary: confirmation,
          confirmedMapPreview, providerCallCount: stats.acquisitions };
      }
      return { ...payload, mapPreview, providerCallCount: stats.acquisitions };
    }
    if (payload.providerProjectedReviewEvidence?.status === 'COMPLETE') {
      const pending = payload.finalizedCoordinateResult;
      const rejectedResponse = await fetch(`http://127.0.0.1:${port}/api/coordinate-projection-confirmation`, {
        method: 'POST', headers: { 'content-type': 'application/json' }, signal,
        body: JSON.stringify({ resultId: pending.resultId, resultRevision: pending.resultRevision,
          sourceCrs: '', coordinateText: payload.coordinates })
      });
      assert.equal(rejectedResponse.status, 400, 'missing CRS must remain blocked');
      const crsEvidence = payload.providerProjectedReviewEvidence?.crsEvidence;
      if (crsEvidence?.status !== 'EXPLICIT') {
        return { ...payload, providerCallCount: stats.acquisitions };
      }
      const sourceCrs = `utm${crsEvidence.zone}${String(crsEvidence.hemisphere).toLowerCase()}`;
      const confirmationResponse = await fetch(`http://127.0.0.1:${port}/api/coordinate-projection-confirmation`, {
        method: 'POST', headers: { 'content-type': 'application/json' }, signal,
        body: JSON.stringify({ resultId: pending.resultId, resultRevision: pending.resultRevision,
          sourceCrs, coordinateText: payload.coordinates })
      });
      const confirmation = await confirmationResponse.json();
      if (confirmationResponse.status !== 200) {
        return { ...payload, projectedConfirmationStatus: confirmationResponse.status,
          projectedConfirmationFailure: confirmation };
      }
      const confirmed = confirmation.finalizedCoordinateResult;
      const mapResponse = await fetch(`http://127.0.0.1:${port}/api/map-preview`, {
        method: 'POST', headers: { 'content-type': 'application/json' }, signal,
        body: JSON.stringify({ resultId: confirmed.resultId, resultRevision: confirmed.resultRevision,
          geometryHash: confirmed.geometryHash })
      });
      const mapPayload = await mapResponse.json();
      assert.equal(mapResponse.status, 200, JSON.stringify(mapPayload));
      return { ...payload, projectedConfirmation: confirmation, projectedMapPreview: mapPayload };
    }
    return payload;
  } finally {
    const ended = once(child, 'exit');
    child.kill();
    await ended;
  }
}
const cases = [];
function test(name, fn) { cases.push({ name, fn }); }

function makeUsageBudget({ rejectCommit = false } = {}) {
  const state = { assertions: 0, starts: 0, completions: 0, markedConsumed: 0 };
  return {
    state,
    budget: {
      assertCanContinue({ stageName }) {
        assert.equal(stageName, "usage_commit");
        state.assertions += 1;
        if (rejectCommit) {
          const error = new Error("fixed budget rejection");
          error.code = "RECOGNITION_BUDGET_EXHAUSTED";
          error.reason = "insufficient_remaining_budget";
          throw error;
        }
      },
      stageStarted(stageName) {
        assert.equal(stageName, "usage_commit");
        state.starts += 1;
        return { stageName };
      },
      stageCompleted(stage, { result }) {
        assert.equal(stage.stageName, "usage_commit");
        assert.ok(["success", "failed", "budget_exhausted"].includes(result));
        state.completions += 1;
      },
      markUserUsageConsumed(value) {
        assert.equal(value, true);
        state.markedConsumed += 1;
      }
    }
  };
}

function makeAtomicUsageService({ commitResult = "COMMITTED" } = {}) {
  const state = { prepareCalls: 0, commitCalls: 0 };
  return {
    state,
    service: {
      async prepare() {
        state.prepareCalls += 1;
        return { result: "PREPARED" };
      },
      async commit() {
        state.commitCalls += 1;
        return { result: commitResult, quota: { remaining: 2 } };
      }
    }
  };
}

function makeAuthoritativeUsagePayload() {
  const finalizedCoordinateResult = finalizeCoordinateResult({
    sourceAuthority: "legacy",
    crs: FINALIZED_COORDINATE_CRS,
    geometry: {
      type: "Polygon",
      coordinates: [[[20, 10], [21, 10], [21, 11], [20, 10]]]
    },
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.NOT_REQUIRED,
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.PASSED,
    currentAuthorizedGeometryExportable: true,
    requiresReview: false,
    kmlReady: true
  });
  return { success: true, finalizedCoordinateResult };
}

test("actual usage controller defers and commits a successful authority response exactly once", async () => {
  const { budget, state } = makeUsageBudget();
  const atomic = makeAtomicUsageService();
  const controller = createCoordinateUsageCommitController({
    budget,
    atomicityService: atomic.service,
    recognitionRequestId: "11111111-1111-4111-8111-111111111111",
    userId: "synthetic-user",
    sessionBindingSha256: "1".repeat(64)
  });
  const scheduled = controller.schedule({ note: "authority established" }, { remaining: 3 });
  assert.equal(scheduled.reason, "usage_commit_pending");
  assert.equal(atomic.state.commitCalls, 0);
  const first = await controller.settle({ httpStatus: 200, body: makeAuthoritativeUsagePayload() });
  assert.equal(first.kind, "USAGE_COMMITTED");
  assert.equal(first.committed, true);
  assert.equal(atomic.state.prepareCalls, 1);
  assert.equal(atomic.state.commitCalls, 1);
  assert.equal(state.assertions, 1);
  assert.equal(state.starts, 1);
  assert.equal(state.completions, 1);
  assert.equal(state.markedConsumed, 1);
  const second = await controller.settle({ httpStatus: 200, body: { success: true } });
  assert.equal(second.kind, "DUPLICATE_SETTLEMENT");
  assert.equal(atomic.state.commitCalls, 1);
  assert.equal(state.markedConsumed, 1);
});

test("actual usage controller rejects insufficient final response budget before any charge", async () => {
  const { budget, state } = makeUsageBudget({ rejectCommit: true });
  const atomic = makeAtomicUsageService();
  const controller = createCoordinateUsageCommitController({
    budget,
    atomicityService: atomic.service,
    recognitionRequestId: "22222222-2222-4222-8222-222222222222",
    userId: "synthetic-user",
    sessionBindingSha256: "2".repeat(64)
  });
  controller.schedule({ note: "must remain uncharged" });
  await assert.rejects(
    controller.settle({ httpStatus: 200, body: makeAuthoritativeUsagePayload() }),
    error => error.code === "RECOGNITION_BUDGET_EXHAUSTED"
  );
  assert.equal(atomic.state.prepareCalls, 0);
  assert.equal(atomic.state.commitCalls, 0);
  assert.equal(state.assertions, 1);
  assert.equal(state.starts, 0);
  assert.equal(state.markedConsumed, 0);
  assert.equal(controller.snapshot().committed, false);
});

for (const [name, response] of [
  ["post-Provider pre-Finalizer failure", { httpStatus: 422, body: { success: false, code: "COORDINATE_POST_PROVIDER_PROCESSING_FAILED" } }],
  ["fallback failure", { httpStatus: 422, body: { success: false, code: "COORDINATE_RECOGNITION_FAILED_CLOSED" } }],
  ["budget failure", { httpStatus: 503, body: { success: false, code: "RECOGNITION_BUDGET_EXHAUSTED" } }]
]) {
  test(`actual usage controller keeps ${name} uncharged`, async () => {
    const { budget, state } = makeUsageBudget();
    const atomic = makeAtomicUsageService();
    const controller = createCoordinateUsageCommitController({
      budget,
      atomicityService: atomic.service,
      recognitionRequestId: "33333333-3333-4333-8333-333333333333",
      userId: "synthetic-user",
      sessionBindingSha256: "3".repeat(64)
    });
    controller.schedule({ note: "must remain uncharged" });
    const settlement = await controller.settle(response);
    assert.equal(settlement.kind, "UNCHARGED_RESPONSE");
    assert.equal(atomic.state.prepareCalls, 0);
    assert.equal(atomic.state.commitCalls, 0);
    assert.equal(state.assertions, 0);
    assert.equal(state.markedConsumed, 0);
    assert.equal(controller.snapshot().committed, false);
  });
}

test("production source orders Provider admission before attempt and defers usage to response settlement", async () => {
  const deadlineSource = await readFile(path.join(root, "server/coordinate-finalizer/recognition-deadline.js"), "utf8");
  const indexSource = await readFile(path.join(root, "index.html"), "utf8");
  const providerFunction = serverSource.slice(
    serverSource.indexOf("async function callAliyunVision"),
    serverSource.indexOf("async function callAliyunOcr")
  );
  assert.ok(providerFunction.indexOf("assertCanStartProvider") >= 0);
  assert.ok(providerFunction.indexOf("assertCanStartProvider") < providerFunction.indexOf("markProviderAttempted"));
  assert.equal((providerFunction.match(/await fetch\(/g) || []).length, 1);
  assert.match(deadlineSource, /startIngressUpload\(\)/);
  assert.match(serverSource, /completeIngressUpload\(\)/);
  const usageEligibilityIndex = serverSource.indexOf('runBudgetedStage("usage_eligibility"');
  assert.ok(usageEligibilityIndex >= 0);
  assert.ok(usageEligibilityIndex < serverSource.indexOf("beginExecutionPhase()", usageEligibilityIndex));
  const usageEligibilityEnd = serverSource.indexOf("checkedCoordinateUsageStatus = usageStatus", usageEligibilityIndex);
  assert.ok(usageEligibilityEnd > usageEligibilityIndex);
  const usageEligibilitySource = serverSource.slice(usageEligibilityIndex, usageEligibilityEnd);
  assert.match(usageEligibilitySource, /checkUsage\(visitorId, "convert"\)/);
  assert.doesNotMatch(usageEligibilitySource, /updateSupabaseUserVisitMeta/);
  const recognitionRouteStart = serverSource.indexOf('app.post("/api/recognize-coordinates"');
  const recognitionRouteEnd = serverSource.indexOf('app.get("/admin"', recognitionRouteStart);
  assert.ok(recognitionRouteStart >= 0 && recognitionRouteEnd > recognitionRouteStart);
  const recognitionRouteSource = serverSource.slice(recognitionRouteStart, recognitionRouteEnd);
  assert.doesNotMatch(recognitionRouteSource, /updateSupabaseUserVisitMeta/);
  const configRouteStart = serverSource.indexOf('app.get("/api/config"');
  const configRouteEnd = serverSource.indexOf('\n});', configRouteStart);
  assert.ok(configRouteStart >= 0 && configRouteEnd > configRouteStart);
  assert.match(serverSource.slice(configRouteStart, configRouteEnd), /updateSupabaseUserVisitMeta/);
  assert.match(serverSource, /createCoordinateUsageCommitController/);
  assert.match(serverSource, /assertCanContinue\(\{ stageName: "finalizer" \}\)/);
  assert.match(serverSource, /buildCoordinateVerificationResponseWithoutRecognitionBudget/);
  assert.match(serverSource, /if \(responseCommitPromise\) await responseCommitPromise/);
  assert.match(deadlineSource, /DEFAULT_RECOGNITION_HARD_DEADLINE_MS = 55_000/);
  assert.match(deadlineSource, /MAX_RECOGNITION_HARD_DEADLINE_MS = 59_000/);
  const fixedMessage = "本次识别未完成，未扣除使用次数。你可以直接重新识别；如仍失败，请向支持人员提供本次请求编号。";
  assert.ok(serverSource.includes(fixedMessage));
  assert.ok(indexSource.includes(fixedMessage));
  assert.match(indexSource, /RECOGNITION_BUDGET_EXHAUSTED/);
  assert.match(indexSource, /RECOGNITION_DEADLINE_EXCEEDED/);
  assert.match(indexSource, /upload-message-content/);
  assert.match(indexSource, /isProjectedReview/);
  assert.match(indexSource, /projectionType\.value = "auto"/);
  assert.match(indexSource, /id="mapPreviewAction"[^>]*>查看地图<\/button>/);
  assert.doesNotMatch(indexSource, /确认坐标系后查看地图|确认并启用地图\/KML|id="projectedCrsConfirmAction"/);
  assert.match(indexSource, /图片缺少完整定位信息，暂时无法显示地图/);
  assert.match(indexSource, /重新上传完整图片/);
  assert.match(indexSource, /onclick="openManualSupport\(\)">人工协助<\/button>/);
  assert.match(indexSource, /id="projectedCrsInternalState" hidden aria-hidden="true"/);
  assert.doesNotMatch(indexSource, /<details id="projectedCrsAdvanced"|<summary>我有测量资料，手动设置<\/summary>/);
  assert.match(indexSource, /function chooseCoordinateImage\(\)/);
  assert.match(indexSource, /payload\?\.precisionMode === "projected-x-y-review"/);
  assert.match(indexSource, /点位核对（非矿区边界）/);
  assert.match(indexSource, /矿区轮廓（待核对）/);
  assert.match(indexSource, /id="debugLiveStatus"[^>]*>.*识别中/su);
  assert.match(indexSource, /function setDebugRunning\(running\)/);
  const debugFunctionStart = indexSource.indexOf("function setDebug(text)");
  const debugFunctionEnd = indexSource.indexOf("async function openManualSupport", debugFunctionStart);
  assert.ok(debugFunctionStart >= 0 && debugFunctionEnd > debugFunctionStart);
  assert.doesNotMatch(indexSource.slice(debugFunctionStart, debugFunctionEnd), /coordinateDiagnosticsVisible/);
  assert.match(indexSource, /KML_PROJECTED_BOUNDARY_UNRESOLVED/);
  assert.match(serverSource, /manual-projected-coordinate-input/);
  assert.match(serverSource, /manual_projected_coordinate_rows/);
});

test("generic Provider projected table recovery preserves labels and blocks CRS inference", () => {
  const evidence = extractProviderProjectedCoordinateEvidence({
    sourceText: [
      "UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE",
      "Sommets | X | Y",
      "A | 727250 | 1219700",
      "B | 728400 | 1219700",
      "C | 728400 | 1219500",
      "D | 728700 | 1219500",
      "E | 728700 | 1220000",
      "F | 729150 | 1220000",
      "G | 729150 | 1219500",
      "H | 729200 | 1219500"
    ].join("\n")
  });
  assert.equal(evidence.status, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.COMPLETE);
  assert.equal(evidence.rowCount, 8);
  assert.equal(evidence.rows[0].label, "A");
  assert.equal(evidence.rows[0].x, "727250");
  assert.equal(evidence.rows[0].y, "1219700");
  assert.equal(evidence.rows.at(-1).label, "H");
  assert.equal(evidence.crsEvidence.status, "UNCONFIRMED");
  assert.match(evidence.text, /^A \| 727250 \| 1219700/m);
});

test("projected recovery handles thousand-space rows but rejects ambiguous or unlabeled structures", () => {
  const spaced = extractProviderProjectedCoordinateEvidence({
    sourceText: [
      "Point X Y",
      "1 658 800 1 364 200",
      "2 651 600 1 364 200",
      "3 651 600 1 364 000"
    ].join("\n")
  });
  assert.equal(spaced.status, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.COMPLETE);
  assert.deepEqual([spaced.rows[0].label, spaced.rows[0].x, spaced.rows[0].y], ["1", "658800", "1364200"]);
  const noHeader = extractProviderProjectedCoordinateEvidence({
    sourceText: "A 727250 1219700\nB 728400 1219700\nC 728400 1219500"
  });
  assert.equal(noHeader.status, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.INCOMPLETE);
  const contractBoundNoHeader = extractProviderProjectedCoordinateEvidence({
    sourceText: [
      "UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE",
      "Coordonnées en UTM",
      "A 727250 1219700",
      "B 728400 1219700",
      "C 728400 1219500"
    ].join("\n")
  });
  assert.equal(contractBoundNoHeader.status, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.COMPLETE);
  assert.deepEqual(
    contractBoundNoHeader.rows.map(row => [row.label, row.x, row.y]),
    [["A", "727250", "1219700"], ["B", "728400", "1219700"], ["C", "728400", "1219500"]]
  );
  const labelledCommaRows = extractProviderProjectedCoordinateEvidence({
    sourceText: [
      "UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE",
      "Coordonnées géographiques en UTM",
      "A | 727250,1219700",
      "B | 728400,1219700",
      "C | 728400,1219500"
    ].join("\n")
  });
  assert.equal(labelledCommaRows.status, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.COMPLETE);
  assert.deepEqual(
    labelledCommaRows.rows.map(row => [row.label, row.x, row.y]),
    [["A", "727250", "1219700"], ["B", "728400", "1219700"], ["C", "728400", "1219500"]]
  );
  assert.equal(labelledCommaRows.diagnostics.parsedProjectedRowCount, 3);
  const ambiguous = extractProviderProjectedCoordinateEvidence({
    sourceText: "Point | X | Y\n1 | 727250 | 1219700\n2 | 728400 | 1219700\n3 | 728400 | 1219500 | 99"
  });
  assert.equal(ambiguous.status, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.INCOMPLETE);
});

test("local OCR worker rejection is normalized and sanitized", async () => {
  let terminated = false;
  await assert.rejects(runCancellableOcrJob({
    createWorker: async () => ({
      recognize: async () => { throw new Error("private decoder detail"); },
      terminate: async () => { terminated = true; }
    }),
    image: syntheticPng,
    timeoutMs: 1000
  }), error => {
    assert.equal(error.code, LOCAL_OCR_FAILURE_CODE);
    assert.equal(error.reason, "worker_error");
    assert.equal(error.message, "Local OCR failed.");
    assert.equal(String(error).includes("private decoder detail"), false);
    return true;
  });
  assert.equal(terminated, true);
});

test("local OCR timeout settles even when worker termination hangs", async () => {
  const startedAt = Date.now();
  await assert.rejects(runCancellableOcrJob({
    createWorker: async () => ({
      recognize: async () => new Promise(() => {}),
      terminate: async () => new Promise(() => {})
    }),
    image: syntheticPng,
    timeoutMs: 20,
    terminationTimeoutMs: 30
  }), error => {
    assert.equal(error.code, "RECOGNITION_BUDGET_EXHAUSTED");
    assert.equal(error.reason, "stage_timeout");
    return true;
  });
  assert.ok(Date.now() - startedAt < 250, "termination cleanup must remain bounded");
});

test("synchronous OCR termination failures cannot escape or mask the primary outcome", async () => {
  const successful = await runCancellableOcrJob({
    createWorker: async () => ({
      recognize: async () => ({ data: { text: "synthetic" } }),
      terminate: () => { throw new Error("PRIVATE_TERMINATE_DETAIL"); }
    }),
    image: syntheticPng,
    timeoutMs: 1000
  });
  assert.equal(successful.data.text, "synthetic");

  await assert.rejects(runCancellableOcrJob({
    createWorker: async () => ({
      recognize: async () => { throw new Error("PRIVATE_RECOGNIZE_DETAIL"); },
      terminate: () => { throw new Error("PRIVATE_TERMINATE_DETAIL"); }
    }),
    image: syntheticPng,
    timeoutMs: 1000
  }), error => error.code === LOCAL_OCR_FAILURE_CODE && !String(error).includes("PRIVATE_"));

  await assert.rejects(runCancellableOcrJob({
    createWorker: async () => ({
      recognize: async () => new Promise(() => {}),
      terminate: () => { throw new Error("PRIVATE_TERMINATE_DETAIL"); }
    }),
    image: syntheticPng,
    timeoutMs: 20,
    terminationTimeoutMs: 30
  }), error => error.code === "RECOGNITION_BUDGET_EXHAUSTED" && error.reason === "stage_timeout");
});

test("local OCR forwards explicitly requested structured-output options", async () => {
  const calls = [];
  const successful = await runCancellableOcrJob({
    createWorker: async () => ({
      recognize: async (...args) => {
        calls.push(args);
        return { data: { text: "synthetic", blocks: [] } };
      },
      terminate: async () => {}
    }),
    image: syntheticPng,
    recognizeOptions: { rotateAuto: false },
    recognizeOutput: { text: true, blocks: true },
    timeoutMs: 1000
  });
  assert.equal(successful.data.text, "synthetic");
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0][1], { rotateAuto: false });
  assert.deepEqual(calls[0][2], { text: true, blocks: true });
});

test("production route binds retry classification and fail-closed runtime guards", () => {
  assert.match(serverSource, /DMS_RETRY_ROUTE_CLASSIFICATION[\s\S]*?from "\.\/server\/recognition\/dms-source-structure\.js"/);
  assert.match(serverSource, /assessRecognitionCompleteness[\s\S]*?from "\.\/server\/recognition\/recognition-completeness\.js"/);
  assert.match(serverSource, /authorizeRequestFamilyRetry\s*=\s*targetOwner\s*=>/);
  assert.match(serverSource, /authorizeRequestFamilyRetry\(targetOwner\)/);
  assert.match(serverSource, /familyEvidencePresent:\s*true/);
  assert.match(serverSource, /sourceKind:\s*getCompletenessSourceKind\(\)/);
  assert.match(serverSource, /sourceRoles:\s*getCompletenessSourceRoles\(\)/);
  assert.match(serverSource, /samePlaceNearDuplicate:\s*overrides\.samePlaceNearDuplicate\s*===\s*true/);
  assert.match(serverSource, /layoutGroups:\s*getCompletenessLayoutGroups\(\)/);
  assert.match(serverSource, /crsEvidence:\s*getCompletenessCrsEvidence\(\)/);
  assert.match(serverSource, /handwrittenConflict:\s*handwrittenVisionRouting\?\.reviewRequired\s*===\s*true/);
  assert.match(serverSource, /stageName:\s*"local_ocr_map_layout_completeness"/);
  assert.ok(
    serverSource.indexOf("authorizeRequestFamilyRetry = targetOwner")
      < serverSource.indexOf("prompt: dmsGroupedDirectPrompt"),
    "unified completeness authorization must be installed before success-path family retries"
  );
  assert.doesNotMatch(serverSource, /function shouldRetryRecognition\s*\(/);
  assert.doesNotMatch(serverSource, /claimDownstreamFamilyRetry\("generic_ocr"\)/);
  assert.doesNotMatch(serverSource, /识别结果少于4行/);
  assert.match(serverSource, /COORDINATE_IMAGE_INVALID/);
  assert.match(serverSource, /errorHandler: \(\) => \{\}/);
  assert.match(serverSource, /COORDINATE_POST_PROVIDER_PROCESSING_FAILED/);
  assert.match(serverSource, /LOCAL_OCR_FAILED/);
});

test("recognition completeness makes single points complete and terminal Provider outcomes local-OCR-only", () => {
  const single = assessRecognitionCompleteness({
    coordinates: "98.67370605,26.34265281",
    coordinateRowCount: 1,
    coordinateFormat: "WGS84_DECIMAL"
  });
  assert.equal(single.decision, RECOGNITION_COMPLETENESS_DECISION.COMPLETE_CANDIDATE);
  assert.equal(single.allowGenericProviderRetry, false);
  assert.equal(single.allowLocalOcrEvidence, false);

  for (const providerStatus of ["FAILED", "TIMEOUT", "UNCERTAIN"]) {
    const terminal = assessRecognitionCompleteness({ providerStatus, localOcrAttempted: false });
    assert.equal(terminal.allowGenericProviderRetry, false);
    assert.equal(terminal.allowFamilyProviderRetry, false);
    assert.equal(terminal.allowLocalOcrEvidence, true);
  }
});

test("generic labeled WGS84 decimal and DMS single-point contracts preserve source representation", () => {
  const decimal = getWgs84SinglePointEvidence([
    "WGS84 Single Point",
    "longitude | 121.24681012",
    "latitude | -12.13579111"
  ].join("\n"));
  assert.equal(decimal.matched, true);
  assert.equal(decimal.coordinateFormat, "WGS84_DECIMAL");
  assert.equal(decimal.coordinateLine, "121.24681012,-12.13579111");
  assert.equal(decimal.normalizedCoordinateLine, "121.24681012,-12.13579111");
  assert.equal(decimal.originalLongitude, "121.24681012");
  assert.equal(decimal.originalLatitude, "-12.13579111");
  assert.equal(decimal.originalLongitudeLabel, "longitude");
  assert.equal(decimal.originalLatitudeLabel, "latitude");
  assert.equal(decimal.coordinateType, "decimal_latlon");
  assert.equal(decimal.precisionMode, "wgs84-single-point-decimal");
  assert.equal(decimal.geometryType, "point");
  assert.equal(decimal.forceRequiresReview, true);
  assert.equal(decimal.requiresReview, true);

  const dms = getWgs84SinglePointEvidence([
    "WGS84 Single Point",
    `longitude DMS | 121°14'48.52\"E`,
    `latitude DMS | 12°08'08.84\"S`
  ].join("\n"));
  assert.equal(dms.matched, true);
  assert.equal(dms.coordinateFormat, "WGS84_DMS");
  assert.match(dms.coordinateLine, /121°14'48\.52\"E,12°08'08\.84\"S/);
  assert.equal(dms.originalLongitude, `121°14'48.52\"E`);
  assert.equal(dms.originalLatitude, `12°08'08.84\"S`);
  assert.equal(dms.originalLongitudeLabel, "longitude DMS");
  assert.equal(dms.originalLatitudeLabel, "latitude DMS");
  assert.match(dms.normalizedCoordinateLine, /^121\.2468\d*,-12\.1357\d*$/);
  assert.equal(dms.coordinateType, "decimal_latlon");
  assert.equal(dms.precisionMode, "wgs84-single-point-dms");
  assert.equal(dms.geometryType, "point");
  assert.equal(dms.forceRequiresReview, true);

  const directionalDecimal = getWgs84SinglePointEvidence(
    "WGS84 Single Point\n西经 | -121.2\n南纬 | -12.1"
  );
  assert.equal(directionalDecimal.matched, true);
  assert.equal(directionalDecimal.coordinateFormat, "WGS84_DECIMAL");
  const directionalDms = getWgs84SinglePointEvidence(
    `WGS84 Single Point\n东经 | 121°14'48.52\"E\n北纬 | 12°08'08.84\"N`
  );
  assert.equal(directionalDms.matched, true);
  assert.equal(directionalDms.coordinateFormat, "WGS84_DMS");
});

test("single-point contract fails closed for unlabeled ambiguous projected and multi-value evidence", () => {
  for (const text of [
    "121.24681012,-12.13579111",
    "WGS84 Single Point\nlongitude | 121.2\nlatitude | -12.1\nlatitude | -12.2",
    "WGS84 Single Point\nEasting | 321000\nNorthing | 8650000\nUTM 50S",
    "WGS84 Single Point\nlongitude | 181.2\nlatitude | -12.1",
    "WGS84 Single Point\nlongitude DMS | 121°14'48.52\"\nlatitude DMS | 12°08'08.84\"",
    "WGS84 Single Point\nlongitude | 121.2\nlatitude | -12.1\nother | 35.1,83.2",
    "WGS84 Single Point\nlongitude | 121.2\nlatitude | -12.1\nlocation 2 | 122.0,-13.0",
    "ordinary report preface\nWGS84 Single Point\nlongitude | 121.2\nlatitude | -12.1\nreport footer",
    `WGS84 Single Point\nlongitude DMS | 121°14'48.52\"E 122°00'00.00\"E\nlatitude DMS | 12°08'08.84\"S`,
    "WGS84 Single Point\n东经 | -121.2\n纬度 | -12.1",
    `WGS84 Single Point\n东经 | 121°14'48.52\"W\n纬度 | 12°08'08.84\"S`,
    "WGS84 Single Point\nlongitude DMS | 121.2\nlatitude DMS | -12.1",
    `WGS84 Single Point\nlongitude DMS | 121°14'48.52\"E\nlatitude | 12°08'08.84\"S`
  ]) {
    const evidence = getWgs84SinglePointEvidence(text);
    assert.equal(evidence.matched, false, text);
    assert.equal(evidence.coordinateLine, "", text);
    assert.equal(evidence.normalizedCoordinateLine, "", text);
    assert.equal(evidence.requiresReview, true, text);
  }
});

test("single-point decimal and DMS bindings reach an actual Point parser but remain Finalizer blocked", () => {
  for (const rawText of [
    "WGS84 Single Point\nlongitude | 121.24681012\nlatitude | -12.13579111",
    `WGS84 Single Point\nlongitude DMS | 121°14'48.52\"E\nlatitude DMS | 12°08'08.84\"S`
  ]) {
    const evidence = getWgs84SinglePointEvidence(rawText);
    assert.equal(evidence.matched, true);
    const payload = {
      success: true,
      model: "offline-synthetic",
      rawText,
      coordinates: evidence.normalizedCoordinateLine,
      precisionMode: evidence.precisionMode,
      wgs84SinglePointEvidence: evidence,
      parserTrace: [`WGS84_SINGLE_POINT:${evidence.coordinateFormat}`]
    };
    assert.equal(runtime.inferCoordinateEngineV2Type(payload), "decimal_latlon");
    const point = runtime.parseCoordinateEngineV2PointLine(
      payload.coordinates,
      evidence.coordinateType,
      0
    );
    assert.ok(Number.isFinite(point.lon));
    assert.ok(Number.isFinite(point.lat));
    assert.equal(runtime.getCoordinateEngineV2Geometry([point]), "point");
    assert.equal(evidence.geometryType, "point");
    assert.notEqual(evidence.geometryType, "line");

    const engine = {
      schema_version: "coordinate_engine_v2",
      coordinate_type: evidence.coordinateType,
      precision_mode: evidence.precisionMode,
      groups: [{
        group_id: "wgs84_single_point",
        group_name: "WGS84 single-point review candidate",
        points: [point],
        geometry_type: "point",
        confidence: 0.8,
        requires_review: true,
        kml_ready: false,
        warnings: []
      }],
      requires_review: true,
      kml_ready: false,
      warnings: []
    };
    const response = buildCoordinateVerificationResponse(payload, engine);
    assert.equal(response.coordinateEngineV2.coordinate_type, "decimal_latlon");
    assert.equal(response.coordinateEngineV2.requires_review, true);
    assert.equal(response.coordinateEngineV2.groups[0].geometry_type, "point");
    assert.equal(response.finalizedCoordinateResult.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
    assert.equal(response.finalizedCoordinateResult.geometry, null);
    assert.equal(response.finalizedCoordinateResult.kmlReady, false);
    assert.equal(consumeFinalizedGeometry(response.finalizedCoordinateResult, geometry => geometry).consumed, false);
  }
});

test("production source installs the one-shot single-point acquisition contract before generic parsing", () => {
  assert.match(serverSource, /WGS84 单点快速采集合同/);
  assert.match(serverSource, /WGS84 Single Point/);
  assert.ok(
    serverSource.indexOf("const wgs84SinglePointEvidence = getWgs84SinglePointEvidence(rawText)")
      < serverSource.indexOf("coordinates = wgs84SinglePointEvidence.normalizedCoordinateLine"),
    "single-point evidence must replace only the generic normalized candidate"
  );
  assert.match(serverSource, /setAcquisitionRouteReason\("WGS84_SINGLE_POINT_PRIMARY"\)/);
  assert.match(serverSource, /wgs84SinglePointEvidence\.precisionMode/);
  assert.match(serverSource, /wgs84SinglePointEvidence\.coordinateType/);
  assert.match(serverSource, /wgs84SinglePointEvidence\.forceRequiresReview/);
  assert.match(serverSource, /recognitionBudget\?\.providerAttemptCount \|\| 0\) >= 1/);
  assert.match(serverSource, /RECOGNITION_PROVIDER:one_shot_retry_blocked/);
  assert.doesNotMatch(serverSource, /98\.67370605|26\.34265281/);
});

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

test("image DMS without typed family or complete table structure fails closed", () => {
  const result = dmsSourceStructure.evaluateImageDmsAcquisitionCompleteness({
    isImageInput: true,
    rawText: observedText,
    normalizedCoordinates: runtime.extractCoordinateLines(observedText),
    selectedRoute: dmsSourceStructure.IMAGE_DMS_SELECTED_ROUTE.GENERIC_DMS
  });
  assert.equal(result.allowed, false);
  assert.equal(result.failClosed, true);
  assert.equal(result.state, dmsSourceStructure.IMAGE_DMS_ACQUISITION_COMPLETENESS_STATE.FAIL_CLOSED);
});

test("generic image DMS accepted by the runtime parser but absent from strict source structure fails closed end to end", () => {
  const looseDms = [
    "1. 10.00.00.0N 20.00.00.0E",
    "2. 10.00.01.0N 20.00.01.0E",
    "3. 10.00.02.0N 20.00.02.0E",
    "4. 10.00.03.0N 20.00.03.0E"
  ].join("\n");
  const normalizedCoordinates = runtime.extractCoordinateLines(looseDms);
  assert.equal(runtime.countDmsCoordinateRows(looseDms), 4);
  assert.equal(dmsSourceStructure.extractDmsSourceStructure(looseDms).rowCount, 0);
  const completeness = dmsSourceStructure.evaluateImageDmsAcquisitionCompleteness({
    isImageInput: true,
    rawText: looseDms,
    normalizedCoordinates,
    selectedRoute: dmsSourceStructure.IMAGE_DMS_SELECTED_ROUTE.GENERIC_DMS
  });
  assert.equal(completeness.failClosed, true);
  const patch = runtime.buildImageDmsAcquisitionFailClosedPatch(completeness, ["OCR"]);
  assert.equal(patch.explicitAuthorityRejected, true);
  assert.equal(patch.coordinates, "");
  const blockedPayload = {
    success: true,
    model: "offline-regression",
    rawText: looseDms,
    coordinates: patch.coordinates,
    precisionMode: "preserve-original-decimals-and-parse-dms",
    explicitAuthorityRejected: patch.explicitAuthorityRejected,
    imageDmsSourceCompleteness: completeness,
    parserTrace: patch.parserTrace
  };
  const blockedEngine = {
    schema_version: "coordinate_engine_v2",
    coordinate_type: "standard_dms_table",
    precision_mode: "preserve-original-decimals-and-parse-dms",
    groups: [],
    requires_review: patch.forceRequiresReview,
    kml_ready: false,
    warnings: []
  };
  const response = buildCoordinateVerificationResponse(blockedPayload, blockedEngine);
  assert.deepEqual(response.coordinateEngineV2.groups, []);
  assert.equal(response.explicitAuthorityRejected, true);
  assert.equal(response.finalizedCoordinateResult.geometry, null);
  assert.equal(response.finalizedCoordinateResult.requiresReview, true);
  assert.equal(response.finalizedCoordinateResult.kmlReady, false);
  assert.equal(response.finalizedCoordinateResult.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
  assert.ok(response.finalizedCoordinateResult.blockingReasons.some(reason => reason.code === COORDINATE_GATE_REASON.KML_NOT_READY));
  assert.equal(consumeFinalizedGeometry(response.finalizedCoordinateResult, geometry => geometry).consumed, false);
});

test("complete single-table image DMS requires explicit header exact coverage and row order", () => {
  const complete = [
    "Point | Latitude | Longitude",
    "1 | 10°00'00.0\" N | 20°00'00.0\" E",
    "2 | 10°00'01.0\" N | 20°00'01.0\" E",
    "3 | 10°00'02.0\" N | 20°00'02.0\" E",
    "4 | 10°00'03.0\" N | 20°00'03.0\" E"
  ].join("\n");
  const completePoints = dmsSourceStructure.extractDmsSourceStructure(complete).groups[0].rows
    .map(dmsSourceStructure.parseDmsSourceCoordinateRow);
  const normalized = completePoints.map(point => `${point.longitude},${point.latitude}`).join("\n");
  const accepted = dmsSourceStructure.evaluateImageDmsAcquisitionCompleteness({
    isImageInput: true,
    rawText: complete,
    normalizedCoordinates: normalized,
    selectedRoute: dmsSourceStructure.IMAGE_DMS_SELECTED_ROUTE.GENERIC_DMS
  });
  assert.equal(accepted.allowed, true);
  assert.equal(accepted.failClosed, false);
  assert.equal(accepted.state, dmsSourceStructure.IMAGE_DMS_ACQUISITION_COMPLETENESS_STATE.COMPLETE_GENERIC_TABLE);
  for (const rejected of [
    { text: complete.replace("Point | Latitude | Longitude\n", ""), coordinates: normalized },
    { text: complete.replace("4 |", "7 |"), coordinates: normalized },
    { text: complete, coordinates: normalized.split("\n").slice(0, 3).join("\n") },
    { text: complete, coordinates: [normalized.split("\n")[1], normalized.split("\n")[0], ...normalized.split("\n").slice(2)].join("\n") },
    { text: complete, coordinates: normalized.replace(/^[-+]?\d+(?:\.\d+)?/, "21") },
    { text: complete.replace("Point | Latitude | Longitude", "Point | Longitude | Latitude"), coordinates: normalized },
    { text: complete.replace("Point | Latitude | Longitude", "This note mentions Point, Latitude and Longitude"), coordinates: normalized }
  ]) {
    const result = dmsSourceStructure.evaluateImageDmsAcquisitionCompleteness({
      isImageInput: true,
      rawText: rejected.text,
      normalizedCoordinates: rejected.coordinates,
      selectedRoute: dmsSourceStructure.IMAGE_DMS_SELECTED_ROUTE.GENERIC_DMS
    });
    assert.equal(result.failClosed, true);
  }
});

test("manual DMS and the actually selected strong typed family remain outside the incomplete-image block", () => {
  const manual = dmsSourceStructure.evaluateImageDmsAcquisitionCompleteness({
    isImageInput: false,
    rawText: observedText,
    normalizedCoordinates: runtime.extractCoordinateLines(observedText),
    selectedRoute: dmsSourceStructure.IMAGE_DMS_SELECTED_ROUTE.GENERIC_DMS
  });
  assert.equal(manual.failClosed, false);
  const typed = dmsSourceStructure.evaluateImageDmsAcquisitionCompleteness({
    isImageInput: true,
    rawText: observedText,
    normalizedCoordinates: runtime.extractCoordinateLines(observedText),
    selectedRoute: dmsSourceStructure.IMAGE_DMS_SELECTED_ROUTE.HANDWRITTEN_DMS
  });
  assert.equal(typed.failClosed, false);
  assert.equal(typed.state, dmsSourceStructure.IMAGE_DMS_ACQUISITION_COMPLETENESS_STATE.TYPED_FAMILY_PROVEN);
});

test("complete structured recovery retains the existing projected family authority path", () => {
  const incomplete = dmsSourceStructure.evaluateImageDmsAcquisitionCompleteness({
    isImageInput: true,
    rawText: observedText,
    normalizedCoordinates: runtime.extractCoordinateLines(observedText),
    selectedRoute: dmsSourceStructure.IMAGE_DMS_SELECTED_ROUTE.GENERIC_DMS
  });
  assert.equal(incomplete.failClosed, true);
  const recovered = getIndonesiaUtm50Info(structuredText, { transform: utmToWgs84 });
  assert.equal(recovered.isIndonesiaUtm50, true);
  assert.equal(recovered.structureConfirmed, true);
  assert.equal(recovered.transformStatus, "SUCCESS");
  assert.equal(recovered.ownerIntent, "indonesia_utm50_projected");
});

test("projected image table with DMS references but unresolved CRS cannot bypass the completeness gate", () => {
  const unresolvedProjectedTable = [
    "Point | X | Y | Latitude | Longitude",
    "1 | 500000 | 9000000 | 10°00'00.0\" N | 20°00'00.0\" E",
    "2 | 500100 | 9000100 | 10°00'01.0\" N | 20°00'01.0\" E",
    "3 | 500200 | 9000200 | 10°00'02.0\" N | 20°00'02.0\" E",
    "4 | 500300 | 9000300 | 10°00'03.0\" N | 20°00'03.0\" E"
  ].join("\n");
  const reference = primaryRouting.getPrintedProjectedDmsReference(unresolvedProjectedTable);
  assert.ok(reference);
  assert.equal(reference.projectedSourceStatus, "UNRESOLVED");
  assert.equal(reference.sourceCrs, null);
  const completeness = dmsSourceStructure.evaluateImageDmsAcquisitionCompleteness({
    isImageInput: true,
    rawText: unresolvedProjectedTable,
    normalizedCoordinates: reference.coordinates,
    selectedRoute: dmsSourceStructure.IMAGE_DMS_SELECTED_ROUTE.GENERIC_DMS
  });
  assert.equal(completeness.failClosed, true);
  const patch = runtime.buildImageDmsAcquisitionFailClosedPatch(completeness, [
    "IMAGE_DMS_SOURCE:projected_structure_unresolved"
  ]);
  assert.equal(patch.coordinates, "");
  assert.equal(patch.explicitAuthorityRejected, true);
  const response = buildCoordinateVerificationResponse({
    success: true,
    coordinates: patch.coordinates,
    explicitAuthorityRejected: patch.explicitAuthorityRejected,
    imageDmsSourceCompleteness: completeness,
    rawText: unresolvedProjectedTable,
    projectedSourceStatus: "UNRESOLVED",
    documentReference: reference
  }, {
    schema_version: "coordinate_engine_v2",
    coordinate_type: "standard_dms_table",
    precision_mode: "dms-coordinates",
    groups: [],
    requires_review: true,
    kml_ready: false,
    warnings: []
  });
  assert.equal(response.finalizedCoordinateResult.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
  assert.equal(response.finalizedCoordinateResult.geometry, null);
  assert.equal(response.finalizedCoordinateResult.kmlReady, false);
  assert.equal(consumeFinalizedGeometry(response.finalizedCoordinateResult, geometry => geometry).consumed, false);
});

test("overlapping detectors and hint-only handwriting cannot authorize a generic image DMS route", () => {
  const coordinates = runtime.extractCoordinateLines(observedText);
  for (const ignoredWeakSignal of ["bftm_detector", "wgs84_detector", "handwritten_hint", "filename_hint"]) {
    const result = dmsSourceStructure.evaluateImageDmsAcquisitionCompleteness({
      isImageInput: true,
      rawText: `${observedText}\n${ignoredWeakSignal}`,
      normalizedCoordinates: coordinates,
      selectedRoute: dmsSourceStructure.IMAGE_DMS_SELECTED_ROUTE.GENERIC_DMS
    });
    assert.equal(result.failClosed, true);
  }
  const hintedOnlyEvidence = runtime.hasIndependentHandwrittenDmsAuthorityEvidence({
    initialDocumentEvidence: { explicitHandwrittenSignal: false, dmsPairLineCount: 8 },
    handwrittenDms: { isHandwrittenDms: true, rawDmsRows: 8 },
    handwrittenVisionRouting: { retrySuccess: false, selectedFieldEvidenceCount: 0 },
    activeFamilyOwner: "handwritten_dms"
  });
  assert.equal(hintedOnlyEvidence, false);
  const hintedOnlyRoute = runtime.selectImageDmsCompletenessRoute({
    handwrittenDms: { isHandwrittenDms: true },
    handwrittenAuthorityEvidenceProven: hintedOnlyEvidence,
    dmsAccepted: true
  });
  assert.equal(hintedOnlyRoute, dmsSourceStructure.IMAGE_DMS_SELECTED_ROUTE.GENERIC_DMS);
  const sourceEvidence = runtime.hasIndependentHandwrittenDmsAuthorityEvidence({
    initialDocumentEvidence: { explicitHandwrittenSignal: true, dmsPairLineCount: 8 },
    handwrittenDms: { isHandwrittenDms: true, rawDmsRows: 8 }
  });
  assert.equal(sourceEvidence, true);
  const retryEvidence = runtime.hasIndependentHandwrittenDmsAuthorityEvidence({
    handwrittenDms: { isHandwrittenDms: true, rawDmsRows: 8 },
    handwrittenVisionRouting: {
      retrySuccess: true,
      selectedFieldEvidenceCount: 8,
      candidateSelectionDecision: "REPLACE_CURRENT"
    },
    activeFamilyOwner: "handwritten_dms"
  });
  assert.equal(retryEvidence, true);
  assert.equal(runtime.selectImageDmsCompletenessRoute({
    handwrittenDms: { isHandwrittenDms: true },
    handwrittenAuthorityEvidenceProven: retryEvidence,
    dmsAccepted: true
  }), dmsSourceStructure.IMAGE_DMS_SELECTED_ROUTE.HANDWRITTEN_DMS);
  assert.doesNotMatch(serverSource, /typedFamilyProven:\s*imageDmsTypedFamilyProven/);
  assert.doesNotMatch(serverSource, /explicitHandwrittenEvidence:/);
});

test("retry failure classes prevent fallback and the grouped Provider path has one non-recursive call site", () => {
  for (const reason of [
    "RETRY_ERROR",
    "RETRY_TIMEOUT",
    "MALFORMED_RETRY_OUTPUT",
    "DMS_GROUPED_RETRY_BUDGET_BLOCKED"
  ]) {
    const orchestrator = dmsSourceStructure.createDmsGroupedRetryOrchestrator({ parserTrace: [] });
    const patch = orchestrator.failClose(reason);
    assert.equal(patch.coordinates, "");
    assert.equal(patch.dmsAccepted, false);
    assert.equal(patch.dmsGroupedAccepted, false);
    assert.equal(orchestrator.claim("generic_ocr"), false);
    assert.equal(orchestrator.claim("dms_grouped"), false);
    assert.equal(orchestrator.snapshot().failed, true);
  }
  assert.equal((serverSource.match(/prompt:\s*dmsGroupedDirectPrompt/g) || []).length, 1);
  assert.equal((serverSource.match(/evaluateDmsGroupedRetryEligibility\s*\(/g) || []).length, 1);
  assert.doesNotMatch(serverSource, /function\s+evaluateDmsGroupedRetryEligibility[\s\S]*callAliyunVision/);
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

test("damaged DMS morphology alone cannot claim handwritten retry ownership", () => {
  const text = Array.from({ length: 4 }, (_, index) => `${index + 1}. 11°28.3126N 08°40.4213W`).join("\n");
  assert.equal(primaryRouting.getDmsDocumentEvidence(text).handwrittenPositiveSignal, true);
  const routing = runtime.getHandwrittenDmsVisionRoutingEvidence(text, text);
  assert.equal(routing.shapeRetryCandidate, true);
  assert.equal(routing.explicitHandwrittenContext, false);
  assert.equal(routing.shouldRetry, false);
});

test("Provider-returned handwritten wording cannot acquire retry ownership", () => {
  const text = `${observedText}\n识别提示：手写坐标存在需核对字符`;
  assert.equal(runtime.getHandwrittenDmsVisionRoutingEvidence(text, observedText).shouldRetry, false);
  assert.equal(runtime.getHandwrittenDmsVisionRoutingEvidence(text, observedText, {
    explicitHandwrittenSignal: true
  }).shouldRetry, true);
});

test("eight-row Provider handwriting wording can only request non-authoritative grouped recovery", () => {
  const rows = Array.from({ length: 8 }, (_, index) => `10°00'${String(index + 1).padStart(2, "0")}.0\"N, 20°00'${String(index + 1).padStart(2, "0")}.0\"E`);
  const text = `${rows.join("\n")}\n识别提示：手写坐标存在需核对字符`;
  const evidence = primaryRouting.getDmsDocumentEvidence(text);
  const trust = dmsSourceStructure.resolveDmsRetryTrustBoundary({ documentEvidence: evidence });
  const shape = runtime.getHandwrittenDmsVisionRoutingEvidence(text, text);
  const ownership = dmsSourceStructure.classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: dmsSourceStructure.evaluateDmsGroupedRoutePriority({
      isImageInput: true,
      printedTableSignal: trust.printedDmsCandidateSignal,
      explicitHandwrittenSignal: trust.explicitHandwrittenSignal,
      structureText: text
    }),
    explicitHandwrittenSignal: trust.explicitHandwrittenSignal,
    nonHandwrittenDmsCandidateSignal: trust.nonHandwrittenDmsCandidateSignal,
    // A separate morphology detector may flag the shape, but the Provider
    // wording itself still cannot become trusted handwritten identity.
    handwrittenShapeRetryCandidate: true,
    partialMultisiteRecoveryCandidateSignal: trust.partialMultisiteRecoveryCandidateSignal
  });
  assert.equal(evidence.dmsPairLineCount, 8);
  assert.equal(trust.explicitHandwrittenSignal, false);
  assert.equal(shape.shouldRetry, false);
  assert.equal(ownership.classification, dmsSourceStructure.DMS_RETRY_ROUTE_CLASSIFICATION.DMS_GROUPED_PARTIAL_RECOVERY_ONLY);
  assert.equal(ownership.retryOwner, "dms_grouped");
});

test("ordinary eight-point single-site DMS remains outside partial recovery without morphology risk", () => {
  const rows = Array.from({ length: 8 }, (_, index) => `10°00'${String(index + 1).padStart(2, "0")}.0\"N, 20°00'${String(index + 1).padStart(2, "0")}.0\"E`);
  assert.equal(rows.length, 8);
  const text = rows.join("\n");
  const evidence = primaryRouting.getDmsDocumentEvidence(text);
  const trust = dmsSourceStructure.resolveDmsRetryTrustBoundary({ documentEvidence: evidence });
  const ownership = dmsSourceStructure.classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: dmsSourceStructure.evaluateDmsGroupedRoutePriority({ isImageInput: true, structureText: text }),
    nonHandwrittenDmsCandidateSignal: trust.nonHandwrittenDmsCandidateSignal,
    handwrittenShapeRetryCandidate: false,
    partialMultisiteRecoveryCandidateSignal: trust.partialMultisiteRecoveryCandidateSignal
  });
  assert.equal(ownership.classification, dmsSourceStructure.DMS_RETRY_ROUTE_CLASSIFICATION.NONE);
  assert.equal(ownership.retryOwner, null);
});

test("weak partial three-group evidence gets one dms_grouped-only recovery and never handwritten identity", () => {
  const rows = Array.from({ length: 16 }, (_, index) => {
    const local = index < 8 ? index + 1 : index < 12 ? index - 7 : index - 11;
    return `${local}. 10°00'${String(index + 1).padStart(2, "0")}.0"N, 20°00'${String(index + 1).padStart(2, "0")}.0"E`;
  });
  const stage1 = [
    "Recognition hint: handwritten DMS",
    "SITES1", "POINT | LATITUDE | LONGITUDE", ...rows.slice(0, 4), `5. 10°00'30.0"N, 20°00'30.0"N`, "",
    "SITES2", "POINT | LATITUDE | LONGITUDE", ...rows.slice(8, 12), `5. 10°00'31.0"N, 20°00'31.0"N`, "",
    "SITES3", "POINT | LATITUDE | LONGITUDE", ...rows.slice(12, 15), `4. 10°00'32.0"N, 20°00'32.0"N`
  ].join("\n");
  const retry = [
    "SITES1", "POINT | LATITUDE | LONGITUDE", ...rows.slice(0, 8), "",
    "SITES2", "POINT | LATITUDE | LONGITUDE", ...rows.slice(8, 12), "",
    "SITES3", "POINT | LATITUDE | LONGITUDE", ...rows.slice(12)
  ].join("\n");
  const evidence = primaryRouting.getDmsDocumentEvidence(stage1);
  const trust = dmsSourceStructure.resolveDmsRetryTrustBoundary({ documentEvidence: evidence });
  const weakImageBuffer = Buffer.from("synthetic-weak-partial-image-v1");
  const weak = dmsSourceStructure.evaluateDmsWeakPartialMultisiteRecovery({
    isImageInput: true,
    projectedTableSignal: evidence.projectedTableSignal,
    explicitHandwrittenSignal: trust.explicitHandwrittenSignal,
    candidateSignal: trust.weakPartialMultisiteRecoveryCandidateSignal,
    structureText: stage1,
    imageInputBuffer: weakImageBuffer
  });
  assert.equal(evidence.explicitHandwrittenSignal, true);
  assert.equal(trust.explicitHandwrittenSignal, false);
  assert.equal(runtime.getHandwrittenDmsVisionRoutingEvidence(stage1, stage1).shouldRetry, false);
  assert.equal(weak.accepted, true);
  assert.deepEqual(weak.groupSizes, [4, 4, 3]);
  const ownership = dmsSourceStructure.classifyDmsRetryOwnership({
    isImageInput: true,
    routePriority: dmsSourceStructure.evaluateDmsGroupedRoutePriority({ isImageInput: true, structureText: stage1 }),
    nonHandwrittenDmsCandidateSignal: trust.nonHandwrittenDmsCandidateSignal,
    handwrittenShapeRetryCandidate: true,
    weakPartialMultisiteRecovery: weak
  });
  assert.equal(ownership.classification, dmsSourceStructure.DMS_RETRY_ROUTE_CLASSIFICATION.DMS_GROUPED_WEAK_PARTIAL_RECOVERY_ONLY);
  assert.equal(ownership.retryOwner, "dms_grouped");
  const eligibility = dmsSourceStructure.evaluateDmsGroupedRetryEligibility({
    rawText: stage1,
    isImageInput: true,
    familyRetryAllowed: true,
    imageInputBuffer: weakImageBuffer,
    weakPartialMultisiteRecovery: { ...weak, claimed: true }
  });
  assert.equal(eligibility.allowed, true);
  const expansion = dmsSourceStructure.evaluateDmsGroupedAcquisitionExpansion({
    baselineText: stage1,
    retryText: retry,
    allowWeakPartialMultisiteRecovery: true,
    isImageInput: true,
    projectedTableSignal: false,
    explicitHandwrittenSignal: false,
    weakPartialCandidateSignal: true,
    weakPartialInputEvidence: weak.inputEvidence,
    imageInputBuffer: weakImageBuffer
  });
  assert.equal(expansion.accepted, true);
  assert.equal(expansion.recoveryMode, "STAGE1_WEAK_PARTIAL_MULTISITE_TO_16");
  assert.deepEqual(expansion.groupSizes, [8, 4, 4]);
  assert.equal(expansion.stage1RejectedEvidence.rejectedLineCount, 3);
});

test("weak partial owner claim fails closed when recomputed structure is invalid", () => {
  const invalidWeak = { accepted: false, failClosed: true };
  assert.deepEqual(dmsSourceStructure.evaluateDmsGroupedRetryEligibility({
    rawText: "1. 10°00'01.0\"N, 20°00'01.0\"E",
    isImageInput: true,
    familyRetryAllowed: true,
    weakPartialMultisiteRecovery: { ...invalidWeak, claimed: true }
  }), {
    allowed: false,
    failClosed: true,
    reason: "DMS_GROUPED_WEAK_PARTIAL_RECOVERY_STRUCTURE_UNPROVEN"
  });
  assert.deepEqual(dmsSourceStructure.evaluateDmsGroupedRetryEligibility({
    rawText: "1. 10°00'01.0\"N, 20°00'01.0\"E",
    isImageInput: true,
    familyRetryAllowed: false,
    weakPartialMultisiteRecovery: { accepted: true, failClosed: false, claimed: true }
  }), {
    allowed: false,
    failClosed: true,
    reason: "DMS_GROUPED_WEAK_PARTIAL_RECOVERY_STRUCTURE_UNPROVEN"
  });
});

test("eight-to-sixteen recovery requires exact baseline preservation and ordered 8 4 4", () => {
  const rows = Array.from({ length: 16 }, (_, index) => {
    const local = index < 8 ? index + 1 : index < 12 ? index - 7 : index - 11;
    return `${local}. 10°00'${String(index + 1).padStart(2, "0")}.0\"N, 20°00'${String(index + 1).padStart(2, "0")}.0\"E`;
  });
  const baselineText = rows.slice(0, 8).join("\n");
  const retryText = [
    "SITES1", "POINT | LATITUDE | LONGITUDE", ...rows.slice(0, 8), "",
    "SITES2", "POINT | LATITUDE | LONGITUDE", ...rows.slice(8, 12), "",
    "SITES3", "POINT | LATITUDE | LONGITUDE", ...rows.slice(12)
  ].join("\n");
  const accepted = dmsSourceStructure.evaluateDmsGroupedAcquisitionExpansion({
    baselineText,
    retryText,
    allowPartialMultisiteRecovery: true
  });
  assert.equal(accepted.accepted, true);
  assert.deepEqual(accepted.groupSizes, [8, 4, 4]);
  const baselineCoordinates = accepted.normalizedCoordinates
    .split(/\r?\n/)
    .filter(Boolean)
    .slice(0, 8)
    .join("\n");
  const candidate = dmsSourceStructure.buildDmsGroupedPartialMultisiteRecoveryCandidate({
    stage1RawText: baselineText,
    stage1Coordinates: baselineCoordinates,
    retryRawText: retryText,
    retryCoordinates: accepted.normalizedCoordinates,
    expansion: accepted,
    ownerFamily: "dms_grouped"
  });
  assert.equal(candidate.accepted, true);
  assert.equal(candidate.provenance.candidateRole, "NONAUTHORITATIVE_REVIEW_CANDIDATE");
  assert.equal(candidate.provenance.sourceCandidateSeparate, true);
  assert.equal(candidate.provenance.directCanonicalPromotion, false);
  assert.equal(candidate.provenance.pointwiseBaselineEquivalenceProven, true);
  assert.equal(candidate.provenance.pointwiseAcquisitionDeltaProven, true);
  const direct16Masquerade = dmsSourceStructure.buildDmsGroupedPartialMultisiteRecoveryCandidate({
    stage1RawText: baselineText,
    stage1Coordinates: baselineCoordinates,
    retryRawText: retryText,
    retryCoordinates: accepted.normalizedCoordinates,
    expansion: { ...accepted, recoveryMode: "STAGE1_DIRECT_16" },
    ownerFamily: "dms_grouped"
  });
  assert.equal(direct16Masquerade.accepted, false);
  assert.equal(direct16Masquerade.failClosed, true);
  assert.equal(direct16Masquerade.reason, "DMS_GROUPED_PARTIAL_MULTISITE_RECOVERY_EXPANSION_BINDING_MISMATCH");
  const wrongGrouping = retryText.replace("SITES2", "").replace("SITES3", "");
  assert.equal(dmsSourceStructure.evaluateDmsGroupedAcquisitionExpansion({
    baselineText,
    retryText: wrongGrouping,
    allowPartialMultisiteRecovery: true
  }).accepted, false);
});

test("partial recovery ownership without exact eight-row single-group qualification fails closed", () => {
  const eligibility = dmsSourceStructure.evaluateDmsGroupedRetryEligibility({
    rawText: Array.from({ length: 7 }, (_, index) => `10°00'${String(index + 1).padStart(2, "0")}.0\"N, 20°00'${String(index + 1).padStart(2, "0")}.0\"E`).join("\n"),
    isImageInput: true,
    familyRetryAllowed: true,
    partialMultisiteRecoveryEligible: true
  });
  assert.deepEqual(eligibility, {
    allowed: false,
    failClosed: true,
    reason: "DMS_GROUPED_PARTIAL_MULTISITE_RECOVERY_STRUCTURE_UNPROVEN"
  });
});

test("hash-bound R5-R2 stage text still requires independent handwritten upload evidence", () => {
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
  assert.equal(runtime.getHandwrittenDmsVisionRoutingEvidence(text, text).shouldRetry, false);
  assert.equal(runtime.getHandwrittenDmsInfo(text, text, { isOcrImage: true }).isHandwrittenDms, false);
  assert.equal(runtime.getHandwrittenDmsTimeoutRoutingEvidence({ mimetype: 'image/jpeg' }, '', { ocrText: text }).shouldRetry, false);
  assert.equal(runtime.getHandwrittenDmsVisionRoutingEvidence(text, text, {
    explicitHandwrittenSignal: true
  }).shouldRetry, true);
  assert.equal(runtime.getHandwrittenDmsInfo(text, text, {
    isOcrImage: true,
    hasExplicitHandwrittenDmsContext: true
  }).isHandwrittenDms, true);
  assert.equal(runtime.getHandwrittenDmsTimeoutRoutingEvidence({
    mimetype: 'image/jpeg',
    originalname: 'trusted-handwritten-dms.jpg'
  }, '', { ocrText: text }).shouldRetry, true);
});

for (const scenario of ['observed', 'structured', 'mismatch']) {
  test(`actual HTTP ${scenario} path uses one mocked acquisition and preserves finalizer`, async () => {
    const payload = await runHttpCandidate(scenario);
    if (scenario === 'observed') {
      assert.equal(payload.indonesiaUtm50?.isIndonesiaUtm50 === true, false);
      assert.equal(payload.coordinateEngineV2.coordinate_type === 'handwritten_dms_experimental', false);
      assert.equal(payload.geometryMode, 'boundary_review');
      assert.equal(payload.boundaryBlocked, true);
      assert.equal(payload.sourceCoordinateRepresentation.displayText, observedText);
      assert.equal(payload.sourceCoordinateRepresentation.axisOrder, 'longitude_latitude');
      assert.equal(payload.sourceCoordinateRepresentation.sourceEquivalence, 'pointwise_dms_semantic_match');
      assert.notEqual(payload.finalizedCoordinateResult.decisionState, 'AUTO_EXPORT');
      assert.equal(payload.finalizedCoordinateResult.geometry.type, 'Polygon');
      assert.equal(payload.finalizedCoordinateResult.geometry.coordinates[0].length, 5);
      assert.notEqual(payload.finalizedCoordinateResult.kmlAuthorityBlocked, true);
      assert.equal(payload.finalizedCoordinateResult.kmlReady, false);
    } else {
      assert.equal(payload.coordinateEngineV2.coordinate_type, 'projected_xy');
      assert.equal(payload.providerProjectedReviewEvidence.crsEvidence.status, 'EXPLICIT');
      assert.equal(payload.providerProjectedReviewEvidence.crsEvidence.zone, 50);
      assert.equal(payload.providerProjectedReviewEvidence.crsEvidence.hemisphere, 'S');
      assert.equal(payload.sourceCoordinateRepresentation.displayText, payload.coordinates);
      assert.equal(payload.projectedConfirmation.geometryMode, 'points_only');
      assert.equal(payload.projectedConfirmation.boundaryBlocked, true);
      assert.equal(payload.projectedConfirmation.finalizedCoordinateResult.geometry.type, 'MultiPoint');
      assert.equal(payload.projectedConfirmation.finalizedCoordinateResult.kmlReady, false);
      assert.equal(payload.projectedMapPreview.mapPreviewObject.geometry.type, 'MultiPoint');
    }
    assert.equal(payload.finalizedCoordinateResult.kmlReady, false);
  });
}

for (const scenario of ['null', 'exception', 'nonfinite', 'outofrange', 'incomplete', 'degenerate', 'selfintersection']) {
  test(`actual HTTP transform ${scenario} preserves owner and evidence without DMS geometry or KML`, async () => {
    const payload = await runHttpCandidate(scenario);
    assert.equal(payload.coordinateEngineV2.coordinate_type, 'projected_xy');
    assert.equal(payload.providerProjectedReviewEvidence.crsEvidence.status, 'EXPLICIT');
    assert.equal(payload.providerProjectedReviewEvidence.crsEvidence.zone, 50);
    assert.equal(payload.providerProjectedReviewEvidence.crsEvidence.hemisphere, 'S');
    assert.equal(payload.sourceCoordinateRepresentation.displayText, payload.coordinates);
    assert.equal(payload.requiresReview, true);
    assert.equal(payload.coordinateEngineV2.requires_review, true);
    assert.equal(payload.coordinateEngineV2.groups[0].points.length, 4);
    assert.equal(payload.finalizedCoordinateResult.geometry, null);
    assert.equal(payload.finalizedCoordinateResult.kmlReady, false);
    if (scenario === 'degenerate' || scenario === 'selfintersection') {
      assert.equal(payload.projectedConfirmation.geometryMode, 'points_only');
      assert.equal(payload.projectedConfirmation.boundaryBlocked, true);
      assert.equal(payload.projectedConfirmation.finalizedCoordinateResult.geometry.type, 'MultiPoint');
      assert.equal(payload.projectedConfirmation.finalizedCoordinateResult.kmlReady, false);
    } else {
      assert.equal(payload.projectedConfirmationStatus, 422);
      assert.ok(payload.projectedConfirmationFailure.code);
    }
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
  const budget = { caseId: 'indonesia-dms-real-001', requestId: '11111111-1111-4111-8111-111111111111' };
  assert.equal(runtime.retainP0QualificationAcquisition(req, response, budget), false);
  runtime.process.env = { P0_QUALIFICATION_ACQUISITION_ENABLED: 'true', ENABLE_REGRESSION_TEST_MODE: 'true', NODE_ENV: 'production' };
  assert.equal(runtime.retainP0QualificationAcquisition(req, response, budget), false);
  runtime.process.env.NODE_ENV = 'test';
  assert.equal(runtime.retainP0QualificationAcquisition({ ...req, ip: '192.168.1.1', socket: { remoteAddress: '192.168.1.1' } }, response, budget), false);
  assert.equal(runtime.retainP0QualificationAcquisition({ ...req, file: { buffer: Buffer.from('wrong') } }, response, budget), false);
  assert.equal(runtime.retainP0QualificationAcquisition(req, response, { ...budget, caseId: 'wrong' }), false);
  assert.equal(runtime.retainP0QualificationAcquisition(req, response, budget), true);
  assert.equal(runtime.retainP0QualificationAcquisition(req, response, { ...budget, requestId: '22222222-2222-4222-8222-222222222222' }), false);
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

test("one-shot structured family routing selects general contracts before Provider acquisition", () => {
  const single = primaryRouting.classifyOneShotStructuredFamily({
    text: "Longitude: 73.418205\nLatitude: 18.672914"
  });
  const grouped = primaryRouting.classifyOneShotStructuredFamily({
    text: [
      "GROUP A",
      "Point | Latitude | Longitude",
      `A | 18°40'01.10\"N | 73°25'01.10\"E`,
      `B | 18°40'02.20\"N | 73°25'02.20\"E`,
      "GROUP B",
      "Point | Latitude | Longitude",
      `A | 19°41'03.30\"N | 74°26'03.30\"E`,
      `B | 19°41'04.40\"N | 74°26'04.40\"E`
    ].join("\n")
  });
  assert.equal(single.family, primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.WGS84_SINGLE_POINT);
  assert.equal(grouped.family, primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED);
  assert.match(primaryRouting.buildOneShotStructuredFamilyPrompt({ family: grouped.family }), /Keep groups separate/);
});

test("one-shot structured family routing fails closed without complete structural evidence", () => {
  const incompleteProjected = primaryRouting.classifyOneShotStructuredFamily({
    text: "UTM coordinates\nPoint | X | Y\nA | 500100 | 2065100"
  });
  const metadataOnly = primaryRouting.classifyOneShotStructuredFamily({
    text: "unclassified image",
    fileName: "country-fixed-coordinate.png",
    country: "Exampleland",
    fixedCoordinate: "73.418205,18.672914"
  });
  assert.equal(incompleteProjected.family, primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(incompleteProjected.reason, "projected_crs_evidence_incomplete");
  assert.equal(metadataOnly.family, primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(metadataOnly.matched, false);
});

test("one-shot structured server integration preserves one Provider and one local OCR boundaries", () => {
  const routeStart = serverSource.indexOf('app.post("/api/recognize-coordinates"');
  const classificationIndex = serverSource.indexOf("await runLocalOcrFamilyClassification", routeStart);
  const selectedPromptIndex = serverSource.indexOf("prompt: selectedProviderPrompt", routeStart);
  assert.ok(classificationIndex >= 0 && selectedPromptIndex > classificationIndex);
  assert.match(serverSource, /const useKyrgyzGkPromptFirst = false/);
  assert.match(serverSource, /const useMozambiqueGeographicPromptFirst = false/);
  assert.match(serverSource, /providerAttemptCount \|\| 0\) >= 1/);
  assert.match(serverSource, /oneShotLocalOcrAttempted/);
  assert.match(serverSource, /materializeMapLayoutRowsFromFamilyEvidence/);
  assert.match(serverSource, /prompt:\s*selectedProviderPrompt/);
  const familyFunction = serverSource.slice(
    serverSource.indexOf("async function runLocalOcrFamilyClassification"),
    serverSource.indexOf("function materializeMapLayoutRowsFromFamilyEvidence")
  );
  assert.match(familyFunction, /createOneShotAcquisitionContract\(\{[\s\S]*layoutLines,[\s\S]*imageIdentity,[\s\S]*resultRevision:\s*1/);
});

test("one-shot structured diagnostics are bounded and redact source content", () => {
  const start = serverSource.indexOf('console.log("One-shot structured family route:"');
  const diagnostic = serverSource.slice(start, serverSource.indexOf("const prompt =", start));
  assert.ok(start >= 0);
  assert.match(diagnostic, /coordinateRowCount/);
  assert.match(diagnostic, /localOcrCallCount/);
  assert.doesNotMatch(diagnostic, /rawText|imageDataUrl|providerResponse|authorization|cookie|apiKey|secret/iu);
});

test("one-shot structured acquisition contract rejects post-Provider family drift", () => {
  const source = "Longitude: 64.125001\nLatitude: 12.875002";
  const contract = spatialContractFor(source);
  const conformant = primaryRouting.validateOneShotAcquisitionContract({
    contract,
    providerText: source
  });
  const valueDrifted = primaryRouting.validateOneShotAcquisitionContract({
    contract,
    providerText: "Longitude: 63.500001\nLatitude: 11.500002"
  });
  const drifted = primaryRouting.validateOneShotAcquisitionContract({
    contract,
    providerText: "Longitude: 63.500001\nLatitude: 11.500002\n64.1000 | 12.2000"
  });
  assert.equal(conformant.status, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.CONFORMANT);
  assert.equal(valueDrifted.status, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.REVIEW_REQUIRED);
  assert.equal(
    valueDrifted.reason,
    primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.VALUE_FIDELITY_MISMATCH
  );
  assert.equal(drifted.status, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.REVIEW_REQUIRED);
});

test("one-shot structured generic contract cannot be upgraded by Provider output", () => {
  const route = primaryRouting.classifyOneShotStructuredFamily({ text: "ordinary report" });
  const contract = primaryRouting.createOneShotAcquisitionContract({ route, sourceText: "ordinary report" });
  const prompt = primaryRouting.buildOneShotStructuredFamilyPrompt({ family: route.family });
  assert.match(prompt, /title alone is invalid whenever any coordinate-bearing line is visible/u);
  assert.match(prompt, /Do not stop after the title/u);
  assert.match(prompt, /degree, minute, second, and N\/S\/E\/W\/O direction characters/u);
  const result = primaryRouting.validateOneShotAcquisitionContract({
    contract,
    providerText: "Longitude: 63.500001\nLatitude: 11.500002"
  });
  assert.equal(result.status, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.REVIEW_REQUIRED);
  assert.equal(result.reason, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.GENERIC_REVIEW_ONLY);
});

test("contextual UTM30 boundary preview requires visible UTM site-vertex meaning and bounded labeled rows", () => {
  const sourceText = [
    "CONTEXT | Les coordonnées géographiques en UTM des sommets du site devant abriter l’activité sont consignées dans le tableau ci-dessous.",
    "UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE",
    "Sommets | X | Y",
    "A | 727250 | 1219700",
    "B | 728400 | 1219700",
    "C | 728400 | 1219500",
    "D | 728700 | 1219500"
  ].join("\n");
  const evidence = extractProviderProjectedCoordinateEvidence({ sourceText });
  assert.equal(evidence.status, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.COMPLETE);
  assert.equal(runtime.supportsLegacyUtm30BoundaryPreview({ providerText: sourceText, evidence }), true);
  assert.equal(runtime.supportsLegacyUtm30BoundaryPreview({
    providerText: sourceText.replace(/\bUTM\b/u, "projection"), evidence
  }), false);
  assert.equal(runtime.supportsLegacyUtm30BoundaryPreview({
    providerText: sourceText.replace(/des sommets du site/u, "du rapport"), evidence
  }), false);
  assert.equal(runtime.supportsLegacyUtm30BoundaryPreview({
    providerText: sourceText,
    evidence: { ...evidence, rows: evidence.rows.map((row, index) => ({ ...row, label: index ? row.label : "" })) }
  }), false);
});

test("Provider message text normalization accepts string and text-block envelopes only", () => {
  assert.equal(runtime.extractProviderMessageText({ choices: [{ message: { content: "row 1\nrow 2" } }] }), "row 1\nrow 2");
  assert.equal(runtime.extractProviderMessageText({
    choices: [{ message: { content: ["header", { type: "text", text: "row 1" }, { content: "row 2" }, { type: "image", image_url: "forbidden" }] } }]
  }), "header\nrow 1\nrow 2");
  assert.equal(runtime.extractProviderMessageText({ choices: [{ message: { content: { text: "single object text" } } }] }), "single object text");
  assert.equal(runtime.extractProviderMessageText({ choices: [{ message: { content: { image_url: "forbidden" } } }] }), "");
});

test("one-shot structured complete Provider DMS evidence is recoverable only for explicit review", () => {
  const sourceText = [
    "UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE",
    "Point | Latitude nord | Longitude ouest",
    "1 | 11° 43' 16.45'' | 09° 01' 13.67''",
    "2 | 11° 43' 09.20'' | 09° 00' 56.03''",
    "3 | 11° 43' 03.38'' | 09° 00' 58.67''",
    "4 | 11° 43' 11.30'' | 09° 01' 15.25''"
  ].join("\n");
  const evidence = runtime.extractProviderDmsReviewEvidence(sourceText);
  assert.equal(evidence.status, "COMPLETE");
  assert.equal(evidence.sourceRowCount, 4);
  assert.equal(evidence.coordinateRowCount, 4);
  assert.equal(evidence.axisDirectionBound, true);
  assert.equal(evidence.coordinates.split("\n").length, 4);
  assert.equal(evidence.coordinates.split("\n")[0], "-9.020463888888889,11.72123611111111");

  const ambiguousHeader = sourceText.replace(
    "Point | Latitude nord | Longitude ouest",
    "Point | First coordinate | Second coordinate"
  );
  assert.equal(runtime.extractProviderDmsReviewEvidence(ambiguousHeader).status, "REVIEW_REQUIRED");

  const missingComponent = sourceText.replace("09° 00' 58.67''", "09° 00'");
  const incomplete = runtime.extractProviderDmsReviewEvidence(missingComponent);
  assert.equal(incomplete.status, "REVIEW_REQUIRED");
  assert.equal(incomplete.coordinates, "");
});

test("one-shot structured Provider DMS review accepts labeled rows with per-value directions", () => {
  const sourceText = [
    "1 | 11°43'16.45\"N | 09°01'13.67\"W",
    "2 | 11°43'09.20\"N | 09°00'56.03\"W",
    "3 | 11°43'03.38\"N | 09°00'58.67\"W",
    "4 | 11°43'11.30\"N | 09°01'15.25\"W",
    "识别提示：手写坐标存在需核对字符，请结合原图逐行核对。"
  ].join("\n");
  const evidence = runtime.extractProviderDmsReviewEvidence(sourceText);
  assert.equal(evidence.status, "COMPLETE");
  assert.equal(evidence.sourceRowCount, 4);
  assert.equal(evidence.coordinateRowCount, 4);
  assert.equal(evidence.axisDirectionBound, true);
});

test("one-shot structured Provider DMS review accepts exact whitespace-delimited Provider rows", () => {
  const sourceText = [
    "UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE",
    "Point Latitude nord Longitude ouest",
    "1 11° 43' 16.45'' 09° 01' 13.67''",
    "2 11° 43' 09.20'' 09° 00' 56.03''",
    "3 11° 43' 03.38'' 09° 00' 58.67''",
    "4 11° 43' 11.30'' 09° 01' 15.25''"
  ].join("\n");
  const evidence = runtime.extractProviderDmsReviewEvidence(sourceText);
  assert.equal(evidence.status, "COMPLETE");
  assert.equal(evidence.sourceRowCount, 4);
  assert.equal(evidence.coordinateRowCount, 4);
  assert.equal(evidence.axisDirectionBound, true);
  assert.equal(evidence.coordinates.split("\n")[0], "-9.020463888888889,11.72123611111111");
  const sourceStructure = dmsSourceStructure.extractDmsSourceStructure(sourceText);
  assert.equal(sourceStructure.rowCount, 4);
  assert.equal(sourceStructure.displayText, sourceText.split("\n").slice(2).join("\n"));
  assert.equal(dmsSourceStructure.extractDmsSourceStructure(
    sourceText.replace("Latitude nord Longitude ouest", "Latitude Longitude")
  ).rowCount, 0);
  assert.equal(dmsSourceStructure.extractDmsSourceStructure(
    sourceText.replace("Latitude nord Longitude ouest", "Latitude est Longitude nord")
  ).rowCount, 0);
  assert.equal(runtime.extractProviderDmsReviewEvidence(sourceText.replace("16.45", "60.00")).status, "REVIEW_REQUIRED");
  assert.equal(runtime.extractProviderDmsReviewEvidence(sourceText.replace("43' 16.45", "43'")).status, "REVIEW_REQUIRED");
});

test("one-shot structured Provider DMS review accepts complete triples without seconds marks", () => {
  const sourceText = [
    "UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE",
    "Point Latitude nord Longitude ouest",
    "1 11° 43' 16.45 09° 01' 13.67",
    "2 11° 43' 09.20 09° 00' 56.03",
    "3 11° 43' 03.38 09° 00' 58.67",
    "4 11° 43' 11.30 09° 01' 15.25"
  ].join("\n");
  const evidence = runtime.extractProviderDmsReviewEvidence(sourceText);
  assert.equal(evidence.status, "COMPLETE");
  assert.equal(evidence.sourceRowCount, 4);
  assert.equal(evidence.coordinateRowCount, 4);
  assert.equal(evidence.axisDirectionBound, true);
  assert.equal(evidence.coordinates.split("\n")[0], "-9.020463888888889,11.72123611111111");
});

test("one-shot structured actual HTTP generic DMS recovery remains confirmation gated", async () => {
  const payload = await runHttpCandidate("generic-dms-review");
  const expectedSourceDisplay = [
    "1 | 11° 43' 16.45'' | 09° 01' 13.67''",
    "2 | 11° 43' 09.20'' | 09° 00' 56.03''",
    "3 | 11° 43' 03.38'' | 09° 00' 58.67''",
    "4 | 11° 43' 11.30'' | 09° 01' 15.25''"
  ].join("\n");
  assert.equal(payload.success, true);
  assert.equal(payload.requiresReview, true);
  assert.equal(payload.providerDmsReviewEvidence.status, "COMPLETE");
  assert.equal(payload.providerDmsReviewEvidence.coordinateRowCount, 4);
  assert.equal(payload.coordinates.split("\n").length, 4);
  assert.match(payload.rawText, /Latitude nord \| Longitude ouest/);
  assert.ok(payload.parserTrace.includes("PROVIDER:trusted_dms_rows_recovered"));
  assert.equal(payload.coordinateEngineV2.requires_review, true);
  assert.equal(payload.finalizedCoordinateResult.confirmationStatus, "pending");
  assert.equal(payload.geometryMode, "boundary_review");
  assert.equal(payload.boundaryBlocked, true);
  assert.equal(payload.finalizedCoordinateResult.decisionState, "REVIEW_REQUIRED");
  assert.equal(payload.finalizedCoordinateResult.geometry.type, "Polygon");
  assert.equal(payload.finalizedCoordinateResult.technicalKmlReady, true);
  assert.equal(payload.finalizedCoordinateResult.kmlReady, false);
  assert.equal(payload.mapPreview.mapPreviewObject.geometry.type, "Polygon");
  assert.equal(payload.mapPreview.kmlEligibility.allowed, false);
  assert.equal(payload.sourceCoordinateRepresentation.displayText, expectedSourceDisplay);
  assert.equal(payload.sourceCoordinateRepresentation.sourceEquivalence, "pointwise_dms_semantic_match");
  assert.notEqual(payload.sourceCoordinateRepresentation.displayText, payload.coordinates);
  assert.equal(payload.confirmedBoundary.finalizedCoordinateResult.confirmationStatus, "accepted");
  assert.equal(payload.confirmedBoundary.finalizedCoordinateResult.requiresReview, false);
  assert.equal(payload.confirmedBoundary.finalizedCoordinateResult.kmlReady, true);
  assert.equal(payload.confirmedBoundary.finalizedCoordinateResult.decisionState, "AUTO_EXPORT");
  assert.equal(payload.confirmedMapPreview.kmlEligibility.allowed, true);
  assert.doesNotMatch(
    payload.confirmedMapPreview.mapPreviewObject.previewWarnings.join("\n"),
    /(?:矿区轮廓待核对|CONFIRMATION_PENDING|KML_BLOCKED)/u
  );
  const usageAuthority = evaluateCoordinateUsageAuthority({ httpStatus: 200, body: payload });
  assert.equal(usageAuthority.eligible, true, JSON.stringify({ usageAuthority, finalized: {
    requiresReview: payload.finalizedCoordinateResult.requiresReview,
    kmlReady: payload.finalizedCoordinateResult.kmlReady,
    confirmationStatus: payload.finalizedCoordinateResult.confirmationStatus,
    qualityGateStatus: payload.finalizedCoordinateResult.qualityGateStatus
  } }));
});

test("one-shot structured HTTP generic DMS recovery accepts Provider text-block arrays", async () => {
  const payload = await runHttpCandidate("generic-dms-review-array");
  assert.equal(payload.success, true);
  assert.equal(payload.requiresReview, true);
  assert.equal(payload.providerDmsReviewEvidence.status, "COMPLETE");
  assert.equal(payload.providerDmsReviewEvidence.coordinateRowCount, 4);
  assert.equal(payload.coordinates.split("\n").length, 4);
  assert.equal(payload.finalizedCoordinateResult.confirmationStatus, "pending");
  assert.equal(payload.finalizedCoordinateResult.decisionState, "REVIEW_REQUIRED");
  assert.equal(payload.geometryMode, "boundary_review");
  assert.equal(payload.finalizedCoordinateResult.geometry.type, "Polygon");
  assert.equal(payload.finalizedCoordinateResult.kmlReady, false);
  assert.equal(payload.confirmedBoundary.finalizedCoordinateResult.kmlReady, true);
  assert.equal(payload.mapPreview.mapPreviewObject.geometry.type, "Polygon");
});

test("projected table OCR acquisition hint is generic, structure-bound, and review-only", () => {
  const noisyStructuredOcr = [
    "Les coordonnées géographiques en UTM des sommets du site sont consignées ci-dessous",
    "[Sommets [ Tox Ty",
    "AT 727250 | 1219700",
    "BE 728400 | 1219700",
    "Cc 728400 | 1219500"
  ].join("\n");
  assert.equal(runtime.shouldUseProjectedTableOcrAcquisition(noisyStructuredOcr), true);
  assert.equal(runtime.shouldUseProjectedTableOcrAcquisition(
    "Burkina Faso UTM sample filename without visible table rows"
  ), false);
  assert.equal(runtime.shouldUseProjectedTableOcrAcquisition(
    "Sommets du site\n1 | 11°43'16.45N | 09°01'13.67W\n2 | 11°43'09.20N | 09°00'56.03W"
  ), false);
  const prompt = runtime.buildProjectedTableOcrAcquisitionPrompt();
  assert.match(prompt, /literal OCR transcription/u);
  assert.match(prompt, /UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE/u);
  assert.match(prompt, /Never infer a zone, hemisphere, CRS, missing digit, missing row/u);
  assert.doesNotMatch(prompt, /727250|1219700|Burkina|布基纳/u);
  assert.match(serverSource, /projectedTableOcrAcquisition\s*\?\s*aliyunOcrModel\s*:\s*aliyunVisionModel/u);
});

test("contextual UTM30 site vertices auto-locate while crossed source order remains point review and blocks KML", async () => {
  const payload = await runHttpCandidate("generic-projected-contextual-utm30");
  const expectedCoordinates = [
    "A | 727250 | 1219700",
    "B | 728400 | 1219700",
    "C | 728400 | 1219500",
    "D | 728700 | 1219500",
    "E | 728700 | 1220000",
    "F | 729150 | 1220000",
    "G | 729150 | 1219500",
    "H | 729200 | 1219500"
  ].join("\n");
  assert.equal(payload.success, true);
  assert.equal(payload.precisionMode, "utm30n-projected-x-y");
  assert.equal(payload.coordinates, expectedCoordinates);
  assert.equal(payload.sourceCoordinateRepresentation.displayText, expectedCoordinates);
  assert.ok(payload.parserTrace.includes("UTM30_XY:contextual_boundary_preview"));
  assert.equal(payload.coordinateEngineV2.groups[0].points.length, 8);
  assert.equal(payload.geometryMode, "points_only");
  assert.equal(payload.boundaryBlocked, true);
  assert.equal(payload.finalizedCoordinateResult.geometry.type, "MultiPoint");
  assert.equal(payload.finalizedCoordinateResult.kmlReady, false);
  assert.match(payload.finalizedCoordinateResult.warnings.join("\n"), /交叉/u);
  assert.equal(payload.mapPreview.mapPreviewObject.geometry.type, "MultiPoint");
  assert.equal(payload.mapPreview.mapPreviewObject.previewEligibility.allowed, true);
  assert.equal(payload.mapPreview.kmlEligibility.allowed, false);
  assert.equal(payload.providerCallCount, 1);
});

test("one-shot structured actual HTTP generic projected recovery preserves source X/Y and blocks export", async () => {
  const payload = await runHttpCandidate("generic-projected-review");
  const expectedCoordinates = [
    "A | 727250 | 1219700",
    "B | 728400 | 1219700",
    "C | 728400 | 1219500",
    "D | 728700 | 1219500",
    "E | 728700 | 1220000",
    "F | 729150 | 1220000",
    "G | 729150 | 1219500",
    "H | 729200 | 1219500"
  ].join("\n");
  assert.equal(payload.success, true);
  assert.equal(payload.precisionMode, "projected-x-y-review");
  assert.equal(payload.requiresReview, true);
  assert.equal(payload.providerProjectedReviewEvidence.status, "COMPLETE");
  assert.equal(payload.providerProjectedReviewEvidence.coordinateRowCount, 8);
  assert.equal(payload.providerProjectedReviewEvidence.crsEvidence.status, "UNCONFIRMED");
  assert.equal(payload.coordinates, expectedCoordinates);
  assert.equal(payload.sourceCoordinateRepresentation.displayText, expectedCoordinates);
  assert.ok(payload.parserTrace.includes("PROVIDER:trusted_projected_rows_recovered"));
  assert.equal(payload.coordinateEngineV2.requires_review, true);
  assert.equal(payload.finalizedCoordinateResult.geometry, null);
  assert.equal(payload.finalizedCoordinateResult.kmlReady, false);
  assert.notEqual(payload.finalizedCoordinateResult.decisionState, "AUTO_EXPORT");
  assert.equal(payload.projectedConfirmation, undefined, "unconfirmed projected evidence must not be test-upgraded");
  assert.equal(payload.projectedMapPreview, undefined, "unconfirmed projected evidence must not reach map preview");
  assert.equal(payload.providerCallCount, 1);
  const usageAuthority = evaluateCoordinateUsageAuthority({ httpStatus: 200, body: payload });
  assert.equal(usageAuthority.eligible, true);
  assert.equal(usageAuthority.reason, "PROJECTED_REVIEW_SERVER_AUTHORITY_ESTABLISHED");
});

test("one-shot structured explicit projected evidence auto-locates without a user CRS choice", async () => {
  const payload = await runHttpCandidate("generic-projected-explicit");
  assert.equal(payload.providerProjectedReviewEvidence.crsEvidence.status, "EXPLICIT");
  assert.equal(payload.providerProjectedReviewEvidence.crsEvidence.zone, 30);
  assert.equal(payload.providerProjectedReviewEvidence.crsEvidence.hemisphere, "N");
  assert.equal(payload.projectedConfirmation.selectedCrs, "EPSG:32630");
  assert.equal(payload.projectedConfirmation.sourceRowCount, 8);
  assert.equal(payload.projectedConfirmation.finalizedCoordinateResult.resultId,
    payload.finalizedCoordinateResult.resultId);
  assert.equal(payload.projectedConfirmation.finalizedCoordinateResult.resultRevision,
    payload.finalizedCoordinateResult.resultRevision + 1);
  assert.equal(payload.projectedConfirmation.geometryMode, "points_only");
  assert.equal(payload.projectedConfirmation.boundaryBlocked, true);
  assert.equal(payload.projectedConfirmation.finalizedCoordinateResult.geometry.type, "MultiPoint");
  assert.equal(payload.projectedConfirmation.finalizedCoordinateResult.kmlReady, false);
  assert.equal(payload.projectedMapPreview.mapPreviewObject.previewEligibility.allowed, true);
  assert.equal(payload.projectedMapPreview.kmlEligibility.allowed, false);
});

test("one-shot structured manual projected entry requires location review and keeps crossed boundary KML blocked", async () => {
  const payload = await runHttpCandidate("manual-projected-review");
  assert.equal(payload.success, true);
  assert.equal(payload.precisionMode, "projected-x-y-review");
  assert.equal(payload.projectedCoordinateReviewEvidence.status, "COMPLETE");
  assert.equal(payload.projectedCoordinateReviewEvidence.coordinateRowCount, 4);
  assert.equal(payload.providerCallCount, 0);
  assert.equal(payload.projectedConfirmation.geometryMode, "points_only");
  assert.equal(payload.projectedConfirmation.finalizedCoordinateResult.geometry.type, "MultiPoint");
  assert.equal(payload.projectedConfirmation.finalizedCoordinateResult.kmlReady, false);
  assert.notEqual(payload.projectedConfirmation.finalizedCoordinateResult.kmlAuthorityBlocked, true);
  assert.equal(payload.projectedMapPreview.mapPreviewObject.geometry.type, "MultiPoint");
});

test("one-shot structured server gates conformance before parsing and returns sanitized review", () => {
  const routeStart = serverSource.indexOf('app.post("/api/recognize-coordinates"');
  const conformanceIndex = serverSource.indexOf("validateOneShotAcquisitionContract", routeStart);
  const dmsFormatIndex = serverSource.indexOf("formatHandwrittenDmsRawRows", conformanceIndex);
  const parseIndex = serverSource.indexOf("extractCoordinateLines", conformanceIndex);
  assert.ok(conformanceIndex >= 0 && dmsFormatIndex > conformanceIndex && parseIndex > conformanceIndex);
  const reviewBlock = serverSource.slice(conformanceIndex, dmsFormatIndex);
  assert.match(reviewBlock, /ONE_SHOT_ACQUISITION_CONTRACT_REVIEW_REQUIRED/);
  assert.match(reviewBlock, /forceRequiresReview:\s*true/);
  assert.match(reviewBlock, /extractProviderDmsReviewEvidence/);
  assert.match(reviewBlock, /DMS_AUTHORITY:user_confirmation_required/);
  assert.match(reviewBlock, /rawText:\s*""/);
  assert.match(reviewBlock, /coordinates:\s*""/);
});

test("one-shot structured conformance diagnostics remain bounded", () => {
  const start = serverSource.indexOf('console.log("One-shot acquisition conformance:"');
  const diagnostic = serverSource.slice(start, serverSource.indexOf("if (oneShotAcquisitionConformance.status", start));
  assert.ok(start >= 0);
  assert.match(diagnostic, /family|status|reason|counts|providerCallCount|localOcrCallCount|terminalState/);
  assert.match(diagnostic, /sourceRegionCount/);
  assert.match(diagnostic, /observedCandidateCount|boundCandidateCount|unassignedCandidateCount/);
  assert.doesNotMatch(diagnostic, /rawText|providerRawText|imageDataUrl|authorization|cookie|apiKey|secret|headers/iu);
  assert.doesNotMatch(diagnostic, /image_sha256|request_asset_id|bbox|spatialIdentity|observationIdentity|candidateText|privateBinding/iu);
});

test("one-shot structured spatial capability rejects missing page and overlapping regions", () => {
  const source = "Longitude: 64.125001\nLatitude: 12.875002";
  const route = primaryRouting.classifyOneShotStructuredFamily({ text: source });
  const missing = primaryRouting.createOneShotAcquisitionContract({ route, sourceText: source });
  const wrongPage = spatialContractFor(source, spatialLayoutFor(source, { page: 2 }));
  const overlappingLines = spatialLayoutFor(source).map((line, index) => ({
    ...line,
    bbox: [20, 20 + (index * 5), 860, 60 + (index * 5)]
  }));
  const overlap = spatialContractFor(source, overlappingLines);
  for (const contract of [missing, wrongPage, overlap]) {
    const result = primaryRouting.validateOneShotAcquisitionContract({ contract, providerText: source });
    assert.equal(result.status, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.REVIEW_REQUIRED);
    assert.equal(result.reason, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.SPATIAL_PROVENANCE_UNAVAILABLE);
  }
});

test("one-shot structured public contract and diagnostics redact private spatial provenance", () => {
  const source = "Longitude: 64.125001\nLatitude: 12.875002";
  const contract = spatialContractFor(source);
  const serialized = JSON.stringify(contract);
  assert.doesNotMatch(serialized, /64\.125001|12\.875002|asset_|image_sha256|bbox|sourceRegion|spatialIdentity/u);
  const result = primaryRouting.validateOneShotAcquisitionContract({ contract, providerText: source });
  assert.equal(result.status, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.CONFORMANT);
  assert.ok(result.counts.sourceRegionCount >= 1);
});

test("one-shot structured observation set requires exact one-to-one candidate coverage", () => {
  const source = "Longitude: 64.125001\nLatitude: 12.875002";
  const exact = primaryRouting.validateOneShotAcquisitionContract({
    contract: spatialContractFor(source),
    providerText: source
  });
  assert.equal(exact.status, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.CONFORMANT);
  assert.deepEqual([
    exact.counts.observedCandidateCount,
    exact.counts.boundCandidateCount,
    exact.counts.unassignedCandidateCount
  ], [2, 2, 0]);

  const layoutLines = spatialLayoutFor(source);
  layoutLines.push({
    text: "Z | 65.5000 | 13.5000",
    bbox: [20, 104, 860, 128],
    local_line_index: 2,
    page: 1,
    resultRevision: 1
  });
  const incomplete = primaryRouting.validateOneShotAcquisitionContract({
    contract: spatialContractFor(source, layoutLines),
    providerText: source
  });
  assert.equal(incomplete.status, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.REVIEW_REQUIRED);
  assert.equal(
    incomplete.reason,
    primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.OBSERVATION_SET_INCOMPLETE
  );
  assert.deepEqual([
    incomplete.counts.observedCandidateCount,
    incomplete.counts.boundCandidateCount,
    incomplete.counts.unassignedCandidateCount
  ], [3, 2, 1]);
});

test("one-shot structured map authority rejects a cross-family observed candidate", () => {
  const source = "Search 64.1250,12.8750\nPlace details 64.125001,12.875002\nPlus Code 7JCPTEST+5P";
  const provider = "MAP_SEARCH_BOX | 64.1250,12.8750\nMAP_PLACE_DETAILS | 64.125001,12.875002\nPLUS_CODE | 7JCPTEST+5P";
  const layoutLines = spatialLayoutFor(source);
  layoutLines.push({
    text: "A | 500100 | 2065100",
    bbox: [20, 146, 860, 170],
    local_line_index: 3,
    page: 1,
    resultRevision: 1
  });
  const result = primaryRouting.validateOneShotAcquisitionContract({
    contract: spatialContractFor(source, layoutLines),
    providerText: provider
  });
  assert.equal(result.status, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.REVIEW_REQUIRED);
  assert.equal(result.reason, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.OBSERVATION_SET_AMBIGUOUS);
  assert.equal(result.counts.unassignedCandidateCount, 1);
});

test("one-shot structured projected authority rejects an unassigned projected row", () => {
  const source = "Projection UTM | Datum WGS 84 | Zone 43N | Hemisphere N\nPoint | Easting | Northing\nA | 500100 | 2065100";
  const provider = "Projection UTM | Datum WGS 84 | Zone 43N | Hemisphere N\nAXIS_ORDER | EASTING | NORTHING\nPOINT | A | 500100 | 2065100";
  const layoutLines = spatialLayoutFor(source);
  layoutLines.push({
    text: "B | 500200 | 2065200",
    bbox: [20, 146, 860, 170],
    local_line_index: 3,
    page: 1,
    resultRevision: 1
  });
  const result = primaryRouting.validateOneShotAcquisitionContract({
    contract: spatialContractFor(source, layoutLines),
    providerText: provider
  });
  assert.equal(result.status, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.REVIEW_REQUIRED);
  assert.equal(result.reason, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.OBSERVATION_SET_INCOMPLETE);
  assert.equal(result.counts.unassignedCandidateCount, 1);
});

test("one-shot structured Provider coverage rejects a locally duplicated map role", () => {
  const source = [
    "Search 64.1250,12.8750",
    "Place details 64.125001,12.875002",
    "Plus Code 7JCPTEST+5P",
    "Plus Code 7JCPTEST+5P"
  ].join("\n");
  const provider = [
    "MAP_SEARCH_BOX | 64.1250,12.8750",
    "MAP_PLACE_DETAILS | 64.125001,12.875002",
    "PLUS_CODE | 7JCPTEST+5P"
  ].join("\n");
  const result = primaryRouting.validateOneShotAcquisitionContract({
    contract: spatialContractFor(source),
    providerText: provider
  });
  assert.equal(result.status, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.REVIEW_REQUIRED);
  assert.notEqual(result.reason, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.CONFORMANT);
});

test("one-shot structured WGS84 specialized route gates parsing and usage behind conformance", () => {
  const blockStart = serverSource.indexOf("if (wgs84PrimaryRoute.selected)");
  const blockEnd = serverSource.indexOf("if (madagascarPrimaryRoute.selected)", blockStart);
  const block = serverSource.slice(blockStart, blockEnd);
  const conformance = block.indexOf("validateOneShotAcquisitionContract");
  assert.ok(conformance >= 0);
  assert.ok(block.indexOf("getWgs84TableCoordinatesInfo") > conformance);
  assert.ok(block.indexOf("consumeCoordinateUsage") > conformance);
  assert.match(block, /ONE_SHOT_ACQUISITION_CONTRACT_REVIEW_REQUIRED/);
  assert.doesNotMatch(
    block.slice(block.indexOf('console.log("One-shot WGS84 primary acquisition conformance:"'), block.indexOf("if (wgs84PrimaryConformance.status")),
    /rawText|providerRawText|imageDataUrl|authorization|cookie|apiKey|secret|headers|bbox|observationIdentity/iu
  );
});

test("one-shot structured repeated-header segments reject boundary migration", () => {
  const source = [
    "Point | Longitude | Latitude",
    "A | 64.1001 | 12.1001",
    "B | 64.1002 | 12.1002",
    "Point | Longitude | Latitude",
    "C | 65.1001 | 13.1001",
    "D | 65.1002 | 13.1002"
  ].join("\n");
  const provider = [
    "WGS84 Longitude Latitude Table",
    "Point | Longitude | Latitude",
    "A | 64.1001 | 12.1001",
    "Point | Longitude | Latitude",
    "B | 64.1002 | 12.1002",
    "C | 65.1001 | 13.1001",
    "D | 65.1002 | 13.1002"
  ].join("\n");
  const result = primaryRouting.validateOneShotAcquisitionContract({
    contract: spatialContractFor(source),
    providerText: provider
  });
  assert.equal(result.status, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.REVIEW_REQUIRED);
  assert.equal(result.reason, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.ROW_PROVENANCE_MISMATCH);
});

test("one-shot structured word-box normalization is synthetic and fixture-independent", () => {
  if (structuredFamilyOnly) {
    assert.equal(golden, null);
    assert.equal(replay, null);
    assert.equal(releaseGate, null);
  }
  assert.match(serverSource, /normalizeLocalOcrStructuredEvidence/);
  const rows = [
    ["Point", "Longitude", "Latitude"],
    ["A", "61.1001", "14.1001"],
    ["B", "61.1002", "14.1002"],
    ["C", "61.1003", "14.1003"]
  ];
  const layoutLines = rows.map((fields, lineIndex) => ({
    text: fields.join(" "),
    bbox: [20, 20 + (lineIndex * 40), 860, 44 + (lineIndex * 40)],
    confidence: 96,
    words: fields.flatMap((field, fieldIndex) => String(field).split(/\s+/u).map((word, wordIndex) => ({
      text: word,
      bbox: [30 + (fieldIndex * 270) + (wordIndex * 64), 20 + (lineIndex * 40),
        80 + (fieldIndex * 270) + (wordIndex * 64), 44 + (lineIndex * 40)],
      confidence: 96
    }))),
    word_structure_valid: true,
    local_line_index: lineIndex,
    page: 1,
    resultRevision: 1
  }));
  const normalized = normalizeLocalOcrStructuredEvidence({
    sourceText: rows.map(row => row.join(" ")).join("\n"),
    layoutLines
  });
  const selected = primaryRouting.classifyOneShotStructuredFamily(normalized);
  assert.equal(selected.family, primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE);
  assert.equal(selected.matched, true);
  assert.equal(selected.evidence.coordinateRowCount, 3);
});

test("one-shot structured normalization rejects incomplete OCR coverage before routing", () => {
  assert.match(serverSource, /normalizationComplete/);
  assert.match(serverSource, /LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS\.COMPLETE/);
  const rows = [
    ["Point", "Longitude", "Latitude"],
    ["S", "69.4301", "24.5201"],
    ["T", "69.4302", "24.5202"],
    ["U", "69.4303", "24.5203"]
  ];
  const layoutLines = rows.map((fields, lineIndex) => ({
    text: fields.join(" "),
    bbox: [20, 20 + (lineIndex * 40), 860, 44 + (lineIndex * 40)],
    confidence: 96,
    words: fields.map((field, fieldIndex) => ({
      text: String(field),
      bbox: [30 + (fieldIndex * 270), 20 + (lineIndex * 40),
        110 + (fieldIndex * 270), 44 + (lineIndex * 40)],
      confidence: 96
    })),
    word_structure_valid: true,
    local_line_index: lineIndex,
    page: 1,
    resultRevision: 1
  }));
  const normalized = normalizeLocalOcrStructuredEvidence({
    sourceText: `${rows.map(row => row.join(" ")).join("\n")}\nV 69.4304 24.5204`,
    layoutLines
  });
  assert.equal(normalized.status, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.INCOMPLETE);
  assert.equal(normalized.text, "");
  assert.equal(normalized.layoutLines.length, 0);
  const selected = primaryRouting.classifyOneShotStructuredFamily(normalized);
  assert.equal(selected.family, primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(selected.matched, false);

  const invalidHeader = normalizeLocalOcrStructuredEvidence({
    sourceText: rows.map(row => row.join(" | ")).join("\n"),
    layoutLines: layoutLines.map((line, index) => ({
      ...line,
      text: rows[index].join(" | "),
      words: index === 0 ? [] : line.words,
      word_structure_valid: index !== 0
    }))
  });
  assert.equal(invalidHeader.status, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.INCOMPLETE);
  assert.equal(primaryRouting.classifyOneShotStructuredFamily(invalidHeader).family,
    primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
});

test("one-shot structured fresh per-run eleven-class acceptance is line-break tolerant and contract closed", () => {
  const longitudeSeed = crypto.randomInt(930000, 1670000) / 10000;
  const latitudeSeed = crypto.randomInt(-740000, 740000) / 10000;
  const decimal = (value, offset = 0, precision = 6) => (value + offset).toFixed(precision);
  const zone = crypto.randomInt(11, 54);
  const easting = crypto.randomInt(220000, 780000);
  const northing = crypto.randomInt(1300000, 8700000);
  const mgrsLetters = "ABCDEFGHJKLMNPQRSTUVWXYZ";
  const olcAlphabet = "23456789CFGHJMPQRVWX";
  const pick = value => value[crypto.randomInt(0, value.length)];
  const plusCode = `${pick(olcAlphabet.slice(0, 9))}${pick(olcAlphabet.slice(0, 18))}${Array.from({ length: 6 }, () => pick(olcAlphabet)).join("")}+${pick(olcAlphabet)}${pick(olcAlphabet)}`;
  const grid = `${pick(mgrsLetters)}${pick(mgrsLetters)}`;
  const mgrsEast = String(crypto.randomInt(10000, 99999));
  const mgrsNorth = String(crypto.randomInt(10000, 99999));
  const wordLayout = rows => rows.map((fields, lineIndex) => {
    const values = Array.isArray(fields) ? fields : [fields];
    const top = 24 + (lineIndex * 42);
    const columnWidth = Math.floor(820 / Math.max(1, values.length));
    const words = values.flatMap((field, fieldIndex) => {
      let cursor = 24 + (fieldIndex * columnWidth);
      return String(field).split(/\s+/u).filter(Boolean).map(word => {
        const width = Math.max(38, word.length * 9);
        const entry = { text: word, bbox: [cursor, top, cursor + width, top + 24], confidence: 97 };
        cursor += width + 12;
        return entry;
      });
    });
    return {
      text: values.join(" "),
      bbox: [12, top, 888, top + 28],
      confidence: 97,
      words,
      word_structure_valid: true,
      local_line_index: lineIndex,
      page: 1,
      resultRevision: 1
    };
  });
  const normalizeFresh = rows => normalizeLocalOcrStructuredEvidence({
    sourceText: rows.flatMap(row => row).join(" "),
    layoutLines: wordLayout(rows)
  });
  const makeCase = (rows, family, providerText, matched = true) => ({
    evidence: normalizeFresh(rows), family, providerText, matched
  });

  const decimalSingleRows = [
    ["Longitude"], [decimal(longitudeSeed)], ["Latitude"], [decimal(latitudeSeed)]
  ];
  const dmsSingleRows = [
    ["Longitude"], [`${crypto.randomInt(93, 167)}°${crypto.randomInt(10, 50)}'${crypto.randomInt(10, 50)}.31\"E`],
    ["Latitude"], [`${crypto.randomInt(12, 74)}°${crypto.randomInt(10, 50)}'${crypto.randomInt(10, 50)}.13\"N`]
  ];
  const fourPointRows = [
    ["Point", "Longitude", "Latitude"],
    ...Array.from({ length: 4 }, (_, index) => [
      String.fromCharCode(65 + index), decimal(longitudeSeed, index / 100), decimal(latitudeSeed, index / 100)
    ])
  ];
  const longRows = [];
  for (let index = 0; index < 20; index += 1) {
    if (index === 0 || index === 10) longRows.push(["No", "Longitude", "Latitude"]);
    longRows.push([
      String((index % 10) + 1), decimal(longitudeSeed, index / 1000), decimal(latitudeSeed, index / 1000)
    ]);
  }
  const groupedRows = [
    ["Location Group AlphaFresh"], ["Point", "Latitude DMS", "Longitude DMS"],
    ["A", `23°11'${crypto.randomInt(10, 50)}.11\"N`, `123°21'${crypto.randomInt(10, 50)}.12\"E`],
    ["B", `23°11'${crypto.randomInt(10, 50)}.21\"N`, `123°21'${crypto.randomInt(10, 50)}.22\"E`],
    ["Location Group BetaFresh"], ["Point", "Latitude DMS", "Longitude DMS"],
    ["A", `24°12'${crypto.randomInt(10, 50)}.31\"N`, `124°22'${crypto.randomInt(10, 50)}.32\"E`],
    ["B", `24°12'${crypto.randomInt(10, 50)}.41\"N`, `124°22'${crypto.randomInt(10, 50)}.42\"E`]
  ];
  const mapRows = [
    ["Search"], [decimal(latitudeSeed), decimal(longitudeSeed)],
    ["Place details"], [decimal(latitudeSeed, 0.000037), decimal(longitudeSeed, 0.000037)],
    ["Plus Code"], [plusCode]
  ];
  const utmRows = [
    [`WGS 84 / UTM zone ${zone}N / EPSG:${32600 + zone}`],
    ["Point F"], ["Easting"], [String(easting)], ["Northing"], [String(northing)]
  ];
  const mgrsRows = [
    [`MGRS Datum WGS84 Zone ${zone}N Hemisphere N`],
    ["Axis order X Y"],
    ["MGRS", `${zone}N`, grid, mgrsEast, mgrsNorth]
  ];
  const otherProjectedRows = [
    ["Projected CRS EPSG:3857"],
    ["Point", "Easting", "Northing"], ["F", String(easting), String(crypto.randomInt(100000, 900000))]
  ];
  const groupedProvider = [
    "GROUP | Location Group AlphaFresh", "HEADER | Point | Latitude DMS | Longitude DMS",
    `POINT | A | ${groupedRows[2][1]} | ${groupedRows[2][2]}`,
    `POINT | B | ${groupedRows[3][1]} | ${groupedRows[3][2]}`,
    "GROUP | Location Group BetaFresh", "HEADER | Point | Latitude DMS | Longitude DMS",
    `POINT | A | ${groupedRows[6][1]} | ${groupedRows[6][2]}`,
    `POINT | B | ${groupedRows[7][1]} | ${groupedRows[7][2]}`
  ].join("\n");
  const mapProvider = [
    `MAP_SEARCH_BOX | ${decimal(latitudeSeed)}, ${decimal(longitudeSeed)}`,
    `MAP_PLACE_DETAILS | ${decimal(latitudeSeed, 0.000037)}, ${decimal(longitudeSeed, 0.000037)}`,
    `PLUS_CODE | ${plusCode}`
  ].join("\n");
  const utmProvider = [
    `CRS | WGS 84 EPSG:${32600 + zone}`, `ZONE | ${zone}N`, "HEMISPHERE | N",
    "AXIS_ORDER | EASTING | NORTHING", `POINT | F | ${easting} | ${northing}`
  ].join("\n");
  const mgrsProvider = [
    "CRS | WGS 84 MGRS", `ZONE | ${zone}N`, "HEMISPHERE | N", "AXIS_ORDER | EASTING | NORTHING",
    `MGRS | ${zone}N | ${grid} | ${mgrsEast} | ${mgrsNorth}`
  ].join("\n");
  const otherProjectedProvider = [
    "CRS | EPSG:3857", "AXIS_ORDER | EASTING | NORTHING",
    `POINT | F | ${otherProjectedRows[2][1]} | ${otherProjectedRows[2][2]}`
  ].join("\n");

  const structured = [
    makeCase(decimalSingleRows, primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.WGS84_SINGLE_POINT, null),
    makeCase(dmsSingleRows, primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.WGS84_SINGLE_POINT, null),
    makeCase(fourPointRows, primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE, null),
    makeCase(longRows, primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.WGS84_TABLE, null),
    makeCase(groupedRows, primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.DMS_GROUPED, groupedProvider),
    makeCase(mapRows, primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.MAP_SCREENSHOT, mapProvider),
    makeCase(utmRows, primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE, utmProvider),
    makeCase(mgrsRows, primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE, mgrsProvider),
    makeCase(otherProjectedRows, primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE, otherProjectedProvider)
  ];
  for (const [caseIndex, entry] of structured.entries()) {
    assert.equal(entry.evidence.status, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.COMPLETE,
      `${entry.family}:${entry.evidence.reason}`);
    assert.equal(entry.evidence.sourceLineCount, 1);
    assert.ok(entry.evidence.layoutLineCount > 1);
    const selected = primaryRouting.classifyOneShotStructuredFamily(entry.evidence);
    assert.equal(selected.family, entry.family);
    assert.equal(selected.matched, true);
    const contract = primaryRouting.createOneShotAcquisitionContract({
      route: selected,
      sourceText: entry.evidence.text,
      layoutLines: entry.evidence.layoutLines,
      imageIdentity: syntheticSpatialIdentity,
      resultRevision: 1
    });
    if (entry.family === primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE) {
      const providerContract = primaryRouting.createOneShotAcquisitionContract({
        route: selected,
        sourceText: entry.providerText,
        layoutLines: spatialLayoutFor(entry.providerText),
        imageIdentity: syntheticSpatialIdentity,
        resultRevision: 1
      });
      assert.deepEqual(providerContract.structure.crs, contract.structure.crs,
        `${caseIndex + 1}:projected identity drift`);
    }
    const conformance = primaryRouting.validateOneShotAcquisitionContract({
      contract,
      providerText: entry.providerText || entry.evidence.text
    });
    assert.equal(conformance.status, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.CONFORMANT,
      `${caseIndex + 1}:${entry.family}:${conformance.reason}`);
    assert.equal(conformance.counts.observedCandidateCount, conformance.counts.boundCandidateCount);
    assert.equal(conformance.counts.unassignedCandidateCount, 0);
    console.log(`OFFLINE ACCEPTANCE class ${caseIndex + 1}: PASS`);
  }

  for (const [reviewIndex, rows] of [
    [["Coordinate candidate", decimal(latitudeSeed, 0, 3), "maybe", decimal(longitudeSeed, 0, 3)]],
    [["Report", String(crypto.randomInt(2032, 2098)), "distance", String(crypto.randomInt(10, 99)), "km", "pixels", String(crypto.randomInt(100, 999))]]
  ].entries()) {
    const evidence = normalizeFresh(rows);
    const selected = primaryRouting.classifyOneShotStructuredFamily(evidence);
    assert.equal(selected.family, primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.equal(selected.matched, false);
    const contract = primaryRouting.createOneShotAcquisitionContract({
      route: selected,
      sourceText: evidence.text,
      layoutLines: evidence.layoutLines,
      imageIdentity: syntheticSpatialIdentity,
      resultRevision: 1
    });
    const conformance = primaryRouting.validateOneShotAcquisitionContract({ contract, providerText: evidence.text });
    assert.equal(conformance.status, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_STATUS.REVIEW_REQUIRED);
    assert.equal(conformance.reason, primaryRouting.ONE_SHOT_ACQUISITION_CONFORMANCE_REASON.GENERIC_REVIEW_ONLY);
    console.log(`OFFLINE ACCEPTANCE class ${reviewIndex + 10}: REVIEW`);
  }
});

test("one-shot structured production classification recovery stays contract-bound and synthetic", () => {
  assert.match(serverSource, /format:\s*oneShotAcquisitionContract\.format/u);
  assert.match(primaryRouting.buildOneShotStructuredFamilyPrompt({
    family: primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.WGS84_SINGLE_POINT,
    format: "DMS_SINGLE_POINT"
  }), /original visible DMS notation[\s\S]*Never convert it to decimal degrees/u);

  const compositeUtm = primaryRouting.classifyOneShotStructuredFamily({ text: [
    "WGS 84 / UTM zone 47S / EPSG:32747",
    "Point | Easting | Northing",
    "R | 357910 | 7468020"
  ].join("\n") });
  assert.equal(compositeUtm.family, primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE);
  assert.equal(compositeUtm.evidence.projectedEvidenceComplete, true);

  const controlledMgrs = primaryRouting.classifyOneShotStructuredFamily({
    text: "MGRS | 47J | MT | 35791 | 46802"
  });
  assert.equal(controlledMgrs.family, primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE);
  assert.equal(controlledMgrs.evidence.projectedEvidenceComplete, true);
  const numericImpostor = primaryRouting.classifyOneShotStructuredFamily({ text: "47 35791 46802" });
  assert.equal(numericImpostor.family, primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);

  const explicitOtherProjected = primaryRouting.classifyOneShotStructuredFamily({ text: [
    "Projected CRS WGS 84 / Pseudo-Mercator EPSG:3857",
    "Point | Easting | Northing",
    "R | 357910 | 146802"
  ].join("\n") });
  assert.equal(explicitOtherProjected.family,
    primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE);
  assert.equal(explicitOtherProjected.evidence.projectedEvidenceComplete, true);

  const nad83Projected = primaryRouting.classifyOneShotStructuredFamily({ text: [
    "Projected CRS NAD83 / EPSG:26915",
    "Point | Easting | Northing",
    "R | 357910 | 146802"
  ].join("\n") });
  assert.equal(nad83Projected.family,
    primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(nad83Projected.evidence.projectedEvidenceComplete, false);

  const nad83Utm = primaryRouting.classifyOneShotStructuredFamily({ text: [
    "Projection NAD83 UTM zone 15N EPSG:26915",
    "Point | Easting | Northing",
    "R | 357910 | 146802"
  ].join("\n") });
  assert.equal(nad83Utm.family,
    primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE);
  assert.equal(nad83Utm.evidence.projectedEvidenceComplete, true);

  for (const crsTitle of [
    "Projection WGS84 Transverse Mercator UTM zone 47S EPSG:32747",
    "Projection NAD83 Transverse Mercator UTM zone 15N EPSG:26915"
  ]) {
    const transverseUtm = primaryRouting.classifyOneShotStructuredFamily({ text: [
      crsTitle,
      "Point | Easting | Northing",
      "R | 357910 | 146802"
    ].join("\n") });
    assert.equal(transverseUtm.family,
      primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE);
    assert.equal(transverseUtm.evidence.projectedEvidenceComplete, true);
  }

  const splitMethodConflict = primaryRouting.classifyOneShotStructuredFamily({ text: [
    "Projected CRS WGS84", "Projection", "Mollweide", "EPSG:3857",
    "Point | Easting | Northing", "R | 357910 | 146802"
  ].join("\n") });
  assert.equal(splitMethodConflict.family,
    primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(splitMethodConflict.evidence.projectedEvidenceComplete, false);

  const splitMethodValid = primaryRouting.classifyOneShotStructuredFamily({ text: [
    "Projected CRS WGS84", "Projection", "Pseudo-Mercator", "EPSG:3857",
    "Point | Easting | Northing", "R | 357910 | 146802"
  ].join("\n") });
  assert.equal(splitMethodValid.family,
    primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(splitMethodValid.evidence.projectedEvidenceComplete, false);

  const explicitMethodConflict = primaryRouting.classifyOneShotStructuredFamily({ text: [
    "Projected CRS WGS84", "Method: Mollweide", "EPSG:3857",
    "Point | Easting | Northing", "R | 357910 | 146802"
  ].join("\n") });
  assert.equal(explicitMethodConflict.family,
    primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
  assert.equal(explicitMethodConflict.evidence.projectedEvidenceComplete, false);

  const explicitProjectionMethod = primaryRouting.classifyOneShotStructuredFamily({ text: [
    "Projected CRS WGS84", "Projection Method: Pseudo-Mercator", "EPSG:3857",
    "Point | Easting | Northing", "R | 357910 | 146802"
  ].join("\n") });
  assert.equal(explicitProjectionMethod.family,
    primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE);
  assert.equal(explicitProjectionMethod.evidence.projectedEvidenceComplete, true);

  const delimiterlessMethod = primaryRouting.classifyOneShotStructuredFamily({ text: [
    "Projected CRS WGS84", "Method Pseudo-Mercator", "EPSG:3857",
    "Point | Easting | Northing", "R | 357910 | 146802"
  ].join("\n") });
  assert.equal(delimiterlessMethod.family,
    primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.PROJECTED_CRS_TABLE);
  assert.equal(delimiterlessMethod.evidence.projectedEvidenceComplete, true);

  for (const conflictingCrsTitle of [
    "Projection NAD83 PseudoMercator EPSG:3857",
    "Projection WGS84 EPSG:26915",
    "Projection WGS84 EPSG:22222",
    "Projection WGS84 UTM zone 47S EPSG:3857",
    "Projection NAD83 UTM zone 16N EPSG:26915",
    "Projection WGS84 Transverse Mercator EPSG:3857",
    "Projection WGS84 Lambert Conformal Conic EPSG:3857",
    "Projection: Mollweide / Datum WGS84 / EPSG:3857",
    "Projection WGS84 Lambert Azimuthal Equal Area EPSG:3857",
    "Projection WGS84 Transverse-Mercator EPSG:3857"
  ]) {
    const conflictingProjected = primaryRouting.classifyOneShotStructuredFamily({ text: [
      conflictingCrsTitle,
      "Point | Easting | Northing",
      "R | 357910 | 146802"
    ].join("\n") });
    assert.equal(conflictingProjected.family,
      primaryRouting.ONE_SHOT_STRUCTURED_FAMILY.GENERIC_REVIEW);
    assert.ok(["projected_crs_evidence_inconsistent", "strong_structure_not_established"]
      .includes(conflictingProjected.reason));
    assert.equal(conflictingProjected.evidence.projectedEvidenceComplete, false);
  }
});

let passed = 0;
const noServiceMode = process.argv.includes("--no-service");
const selectedCases = structuredFamilyOnly
  ? cases.filter(entry => entry.name.startsWith("one-shot structured"))
  : noServiceMode
    ? cases.filter(entry => !entry.name.startsWith("actual HTTP "))
    : cases;
for (const entry of selectedCases) {
  await entry.fn();
  passed += 1;
  console.log(`PASS ${entry.name}`);
}
console.log(`Production recognition recovery P0 regression: ${passed}/${selectedCases.length} PASS${structuredFamilyOnly ? " (structured-family synthetic scope)" : noServiceMode ? ` (${cases.length - selectedCases.length} localhost integration cases not started)` : ""}`);
