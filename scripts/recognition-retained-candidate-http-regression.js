// Synthetic local HTTP regression for uncharged candidate retention. No production image or Provider.
import './recognition-audit-offline-guard.cjs';
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const receiptRoot = process.env.RECOGNITION_AUDIT_RECEIPT_ROOT;
if (!receiptRoot) throw new Error('RECOGNITION_AUDIT_RECEIPT_ROOT_REQUIRED');
if (existsSync(receiptRoot)) throw new Error('EXISTING_RECEIPT_NO_RETRY');
mkdirSync(receiptRoot, { recursive: true });

const matrix = path.join(root, 'scripts/production-recognition-recovery-p0-regression.js');
const fixtureMatch = readFileSync(matrix, 'utf8').match(/^const syntheticPng = Buffer\.from\("([A-Za-z0-9+/=]+)", "base64"\);$/m);
if (!fixtureMatch) throw new Error('MATRIX_SYNTHETIC_FIXTURE_NOT_FOUND');
const image = Buffer.from(fixtureMatch[1], 'base64');
const diagnosticUrl = pathToFileURL(path.join(root, 'server/recognition/recognition-diagnostics.js')).href;
const providerRows = Array.from({ length: 16 }, (_, index) => {
  const label = index + 1;
  return `${label} | ${500000 + (index * 20)} | ${9000000 - (index * 15)}`;
});
const providerText = [
  'CONTEXT | Projected coordinate table; source CRS is not stated.',
  'UNCLASSIFIED STRUCTURED COORDINATE EVIDENCE',
  'Point | X | Y',
  ...providerRows
].join('\n');

function publicEvidence(value) {
  if (Array.isArray(value)) return value.map(publicEvidence);
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(Object.entries(value)
    .filter(([key]) => !/^(?:authorization|headers?|cookies?|api[_-]?key|.*secret.*|.*tokens?|password|visitorId|userId|account.*|email|phone|quota)$/i.test(key))
    .map(([key, item]) => [key, publicEvidence(item)]));
}

async function run(mode) {
  const capture = mode === 'captured';
  const preload = [
    "import { registerHooks } from 'node:module';",
    ...(capture ? [
      'import { withRecognitionDiagnostics } from ' + JSON.stringify(diagnosticUrl) + ';',
      "globalThis.__retainedCandidateCapture = (handler, req, res) => withRecognitionDiagnostics(async () => {",
      " let finish; const done = new Promise(resolve => { finish = resolve; }); res.once('finish', finish);",
      " try { const value = await handler(req, res); if (!res.writableFinished) await done; return value; }",
      " finally { res.off('finish', finish); }",
      "}, { retainSourceText: true, origin: 'SYNTHETIC' }).then(({value, diagnostics}) => { process.send?.({diagnostics}); return value; });"
    ] : []),
    "registerHooks({load(url, context, nextLoad) {",
    " const result = nextLoad(url, context); if (!url.endsWith('/server.js')) return result;",
    " let source = String(result.source);",
    ' const replacement = ' + JSON.stringify(providerText) + ';',
    " source = 'const retainedCandidateFixtureFetch = globalThis.fetch;\\nglobalThis.fetch = async (...args) => { const response = await retainedCandidateFixtureFetch(...args); const body = await response.json(); body.choices[0].message.content = ' + JSON.stringify(replacement) + '; return new Response(JSON.stringify(body), {status:response.status, headers:{\\\"content-type\\\":\\\"application/json\\\"}}); };\\n' + source;",
    " const ocrFn = source.indexOf('async function runLocalOcrFamilyClassification');",
    " const ocrCall = source.indexOf('    const result = await runCancellableOcrJob({', ocrFn);",
    " if (ocrFn < 0 || ocrCall < 0) throw new Error('LOCAL_OCR_TIMEOUT_INJECTION_NOT_INSTALLED');",
    " const timeoutInjection = '    { const forcedTimeout = new Error(\\\"FORCED_LOCAL_OCR_TIMEOUT\\\"); forcedTimeout.code = RECOGNITION_BUDGET_CODE; throw forcedTimeout; }\\n';",
    " source = source.slice(0, ocrCall) + timeoutInjection + source.slice(ocrCall);",
    " const handlerStart = source.indexOf('async function recognizeCoordinatesHandler');",
    " const controllerStart = source.indexOf('const usageCommitController = createCoordinateUsageCommitController({', handlerStart);",
    " const regressionFlag = source.indexOf('regressionTestMode: regressionTestMode.active,', controllerStart);",
    " if (handlerStart < 0 || controllerStart < 0 || regressionFlag < 0 || regressionFlag > controllerStart + 1200) throw new Error('USAGE_CONTROLLER_INJECTION_NOT_INSTALLED');",
    " source = source.slice(0, regressionFlag) + 'regressionTestMode: false,' + source.slice(regressionFlag + 'regressionTestMode: regressionTestMode.active,'.length);",
    " const responseWrapper = source.indexOf('res.json = function usageSafeRecognitionJson(body)', controllerStart);",
    " const settlementStart = source.indexOf('const settlement = await usageCommitController.settle({', responseWrapper);",
    " const sourceNewline = source.includes('\\r\\n') ? '\\r\\n' : '\\n';",
    " const settlementBodyToken = '          body' + sourceNewline;",
    " const settlementBody = source.indexOf(settlementBodyToken, settlementStart);",
    " if (responseWrapper < 0 || settlementStart < 0 || settlementBody < 0 || settlementBody > settlementStart + 240) throw new Error('SETTLEMENT_BODY_INJECTION_NOT_INSTALLED');",
    " const forcedBody = '          body: {...body, recognitionAcquisitionReviewAuthority: undefined, projectedCoordinateReviewAuthority: undefined, finalizedCoordinateResult: undefined}\\n';",
    " source = source.slice(0, settlementBody) + forcedBody.replace('\\n', sourceNewline) + source.slice(settlementBody + settlementBodyToken.length);",
    ...(capture ? [
      " const runtimeMarker = 'const recognitionAcquisitionJobRuntime = createRecognitionAcquisitionJobRuntime({';",
      " const runtimeBoundary = source.indexOf(runtimeMarker);",
      " if (runtimeBoundary < 0 || source.indexOf(runtimeMarker, runtimeBoundary + runtimeMarker.length) >= 0) throw new Error('CAPTURE_BOUNDARY_NOT_UNIQUE');",
      " const wrapper = 'const retainedCandidateOriginalHandler = recognizeCoordinatesHandler;\\nrecognizeCoordinatesHandler = (req, res) => globalThis.__retainedCandidateCapture(retainedCandidateOriginalHandler, req, res);\\n';",
      " source = source.slice(0, runtimeBoundary) + wrapper + source.slice(runtimeBoundary);"
    ] : []),
    " return {...result, source};",
    "}});"
  ].join('\n');

  const child = spawn(process.execPath, ['--import', 'data:text/javascript,' + encodeURIComponent(preload), matrix,
    '--http-candidate', 'generic-projected-review-local-ocr-timeout'], {
    cwd: root, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    env: {
      SystemRoot: process.env.SystemRoot, PATH: process.env.PATH, NODE_ENV: 'test', PORT: '0',
      ENABLE_REGRESSION_TEST_MODE: 'true', ALIYUN_API_KEY: 'local-mock-only',
      ALIYUN_BASE_URL: 'http://127.0.0.1:1/v1', SUPABASE_URL: '', SUPABASE_SERVICE_ROLE_KEY: '',
      P0_QUALIFICATION_ACQUISITION_ENABLED: 'true', SPATIAL_RESULT_ENABLED: 'true',
      DOTENV_CONFIG_PATH: path.join(root, '__no_test_env__')
    }
  });
  let stdout = '', stderr = '';
  const messages = [];
  const waiters = [];
  let childTerminalError = null;
  let childExit = null;
  const failWaiters = error => {
    childTerminalError ||= error;
    while (waiters.length > 0) waiters.shift().reject(childTerminalError);
  };
  child.stdout.on('data', bytes => { stdout = (stdout + bytes).slice(-6000); });
  child.stderr.on('data', bytes => { stderr = (stderr + bytes).slice(-6000); });
  child.once('error', error => failWaiters(new Error(`HTTP_CHILD_SPAWN_ERROR: ${error.message}`, { cause: error })));
  child.once('exit', (code, signal) => {
    childExit = { code, signal };
    failWaiters(new Error(`HTTP_CHILD_EXITED_BEFORE_COMPLETION: code=${code} signal=${signal || 'none'}`));
  });
  child.on('message', message => {
    const waiter = waiters.find(item => item.key in message);
    if (waiter) { waiters.splice(waiters.indexOf(waiter), 1); waiter.resolve(message); }
    else messages.push(message);
  });
  const timeoutError = new Error('HTTP_CHILD_BOUNDED_TIMEOUT_30000MS');
  const timeout = setTimeout(() => failWaiters(timeoutError), 30000);
  const signal = AbortSignal.timeout(30000);
  const waitFor = key => new Promise((resolve, reject) => {
    const index = messages.findIndex(message => key in message);
    if (index >= 0) return resolve(messages.splice(index, 1)[0]);
    if (childTerminalError) return reject(childTerminalError);
    waiters.push({ key, resolve, reject });
  });

  const terminateProcessTree = () => {
    if (child.exitCode !== null || child.signalCode !== null) return;
    if (process.platform === 'win32') {
      spawnSync('taskkill', ['/PID', String(child.pid), '/T', '/F'], { windowsHide: true, stdio: 'ignore' });
    } else {
      child.kill('SIGTERM');
    }
  };

  try {
    const { port } = await waitFor('port');
    const form = new FormData();
    form.set('visitorId', 'retained-candidate-contract');
    form.set('image', new Blob([image], { type: 'image/png' }), 'synthetic-coordinate-fixture.png');
    const response = await fetch(`http://127.0.0.1:${port}/api/recognize-coordinates`, {
      method: 'POST', headers: { 'x-regression-test': '1', 'x-regression-case-id': 'retained-candidate-generic' },
      body: form, signal
    });
    const payload = await response.json();
    child.send('stats');
    const stats = await waitFor('acquisitions');
    const diagnostics = capture ? (await waitFor('diagnostics')).diagnostics : null;
    const record = {
      origin: 'SYNTHETIC_NOT_PRODUCTION_REPLAY', mode,
      input: { imageSha256: createHash('sha256').update(image).digest('hex'), imageBytes: image.length,
        providerRows: providerRows.length, sourceCrsExplicit: false },
      httpStatus: response.status, payload: publicEvidence(payload), mockProviderCalls: stats.acquisitions,
      diagnostics: publicEvidence(diagnostics), privateDiagnosticInStdout: stdout.includes('recognition_diagnostic_v1')
    };
    writeFileSync(path.join(receiptRoot, `${mode}-response.json`), JSON.stringify(record, null, 2), { flag: 'wx' });
    return record;
  } catch (error) {
    writeFileSync(path.join(receiptRoot, `${mode}-transport-error.json`), JSON.stringify({
      origin: 'SYNTHETIC_NOT_PRODUCTION_REPLAY', message: error.message,
      childExit, stdout: stdout.slice(-2000), stderr: stderr.slice(-4000)
    }, null, 2), { flag: 'wx' });
    throw error;
  } finally {
    clearTimeout(timeout);
    if (child.exitCode === null && child.signalCode === null) {
      terminateProcessTree();
      await Promise.race([once(child, 'exit'), new Promise(resolve => setTimeout(resolve, 5000))]);
    }
  }
}

function assertRetained(record) {
  const payload = record.payload;
  assert.equal(record.httpStatus, 422);
  assert.equal(payload.success, false);
  assert.equal(payload.usageConsumed, false);
  assert.equal(payload.userUsageConsumed, false);
  assert.equal(payload.candidateEvidenceStatus, 'PRESENT');
  assert.notEqual(payload.failureState, 'FAILED_NO_COORDINATE_EVIDENCE');
  assert.equal(payload.candidatePointCount, 16);
  assert.equal(payload.candidateCoordinates.length, 16);
  assert.equal(payload.candidateCoordinateLines.length, 16);
  assert.equal(payload.coordinates.split('\n').length, 16);
  assert.equal(payload.rawText, '');
  assert.equal(payload.mapReady, false);
  assert.equal(payload.kmlReady, false);
  assert.equal(payload.mapStatus, 'CLOSED');
  assert.equal(payload.kmlStatus, 'CLOSED');
  assert.equal(payload.previewEligibility.allowed, false);
  assert.equal(payload.kmlEligibility.allowed, false);
  assert.equal(record.mockProviderCalls, 1);
  assert.equal(payload.providerCallCount, 1);
  assert.equal(record.privateDiagnosticInStdout, false);
  payload.candidateCoordinates.forEach((candidate, index) => {
    assert.equal(candidate.sourceLabel, String(index + 1));
    assert.equal(candidate.sourceText, providerRows[index]);
    assert.equal(candidate.axisOrder, 'x_y');
    assert.equal(payload.candidateCoordinateLines[index].lineNumber, candidate.sourceLineNumber);
    assert.equal(payload.candidateCoordinateLines[index].text, candidate.sourceText);
  });
  assert.equal(JSON.stringify(payload).includes('Projecting narrative'), false);
}

const captured = await run('captured');
assertRetained(captured);
const disabled = await run('disabled');
assertRetained(disabled);
const businessKeys = ['success', 'reason', 'code', 'failureState', 'authorityReason', 'candidateEvidenceStatus',
  'candidatePointCount', 'candidateCoordinates', 'candidateCoordinateLines', 'candidateCoordinateGroups', 'coordinates',
  'rawText', 'mapReady', 'kmlReady', 'mapStatus', 'kmlStatus', 'previewEligibility', 'kmlEligibility',
  'usageConsumed', 'userUsageConsumed'];
assert.deepEqual(
  Object.fromEntries(businessKeys.map(key => [key, captured.payload[key]])),
  Object.fromEntries(businessKeys.map(key => [key, disabled.payload[key]])),
  'diagnostic toggle must not alter retained-candidate business output'
);
const delivered = captured.diagnostics.sessions.flatMap(session => session.events)
  .filter(event => event.stage === 'delivered_result');
assert.equal(delivered.length, 1);
assert.equal(delivered[0].data.failureState, captured.payload.failureState);
assert.equal(delivered[0].data.authorityReason, captured.payload.authorityReason);
assert.equal(delivered[0].data.candidatePointCount, 16);
writeFileSync(path.join(receiptRoot, 'results.json'), JSON.stringify({
  status: 'PASS', checks: 12, realProviderCalls: 0, mockProviderCallsPerRequest: [1, 1],
  receiptFiles: ['captured-response.json', 'disabled-response.json']
}, null, 2), { flag: 'wx' });
console.log('Recognition retained-candidate HTTP regression: 12/12 PASS; REAL_PROVIDER_CALLS=0');
