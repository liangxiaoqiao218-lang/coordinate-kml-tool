// Local synthetic matrix fixture + actual handler; no production request or Provider.
import './recognition-audit-offline-guard.cjs';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { readFileSync, mkdirSync, writeFileSync, existsSync } from 'node:fs';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { validateFinalizedGeometry } from '../server/coordinate-finalizer/geometry-finalizer.js';
import { createGeometryHash } from '../server/coordinate-finalizer/geometry-hash.js';

const root = fileURLToPath(new URL('../', import.meta.url));
const captureOnly = process.argv.includes('--capture-only');
const negativeOnly = process.argv.includes('--negative-only');
const fromExtra = process.argv.includes('--from-extra');
const directory = process.env.RECOGNITION_AUDIT_RECEIPT_ROOT || path.join(root, 'Temp',
  captureOnly ? 'recognition-table-phase-d1-projected-diagnostic' : 'recognition-table-phase-d1-projected-recovery');
mkdirSync(directory, { recursive: true });
const filename = path.join(directory, captureOnly ? 'capture.json' : 'http-results.json');
if (existsSync(filename)) throw new Error('EXISTING_RECEIPT_NO_RETRY');
const matrix = path.join(root, 'scripts/production-recognition-recovery-p0-regression.js');
const scenario = 'generic-projected-bftm-boundary-local-ocr-timeout';
// Reuse the exact fixture bytes declared in the existing matrix, never a production image.
const fixtureDeclaration = readFileSync(matrix, 'utf8').match(/^const syntheticPng = Buffer.from\("([A-Za-z0-9+/=]+)", "base64"\);$/m);
if (!fixtureDeclaration) throw new Error('MATRIX_SYNTHETIC_FIXTURE_NOT_FOUND');
const image = Buffer.from(fixtureDeclaration[1], 'base64');
const diagnosticUrl = pathToFileURL(path.join(root, 'server/recognition/recognition-diagnostics.js')).href;
const records = [];
const checks = [];
function privateSafe(value) {
  if (Array.isArray(value)) return value.map(privateSafe);
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(Object.entries(value)
    .filter(([key]) => !/^(?:authorization|headers?|cookies?|api[_-]?key|.*secret.*|.*tokens?|password|visitorId|userId|account.*|email|phone|quota)$/i.test(key))
    .map(([key, item]) => [key, privateSafe(item)]));
}
async function request(mode, name = '', providerText = null) {
  const prefix = (name ? name + '-' : '') + mode;
  const preload = [
    "import { registerHooks } from 'node:module';",
    'import { withRecognitionDiagnostics } from ' + JSON.stringify(diagnosticUrl) + ';',
    "globalThis.__projectedCapture = (handler, req, res) => withRecognitionDiagnostics(async () => {",
    " let finish; const done = new Promise(resolve => { finish = resolve; }); res.once('finish', finish);",
    " try { const result = await handler(req, res); if (!res.writableFinished) await done; return result; }",
    " finally { res.off('finish', finish); }",
    "}, { retainSourceText: true, origin: 'SYNTHETIC' }).then(({value, diagnostics}) => { process.send?.({diagnostics}); return value; });",
    "registerHooks({load(url, context, nextLoad) {",
    " const result = nextLoad(url, context); if (!url.endsWith('/server.js')) return result;",
    " let source = String(result.source);",
    ...(providerText === null ? [] : [
      ' const replacement = ' + JSON.stringify(providerText) + ';',
      " source = 'const captureFixtureFetch = globalThis.fetch;\\nglobalThis.fetch = async (...args) => { const response = await captureFixtureFetch(...args); const body = await response.json(); body.choices[0].message.content = ' + JSON.stringify(replacement) + '; return new Response(JSON.stringify(body), {status:response.status}); };\\n' + source;"
    ]),
    " const fn = source.indexOf('async function runLocalOcrFamilyClassification');",
    " const call = source.indexOf('    const result = await runCancellableOcrJob({', fn);",
    " if (fn < 0 || call < 0) throw new Error('LOCAL_OCR_TIMEOUT_INJECTION_NOT_INSTALLED');",
    " const injection = '    { const forcedTimeout = new Error(\"FORCED_LOCAL_OCR_TIMEOUT\"); forcedTimeout.code = RECOGNITION_BUDGET_CODE; throw forcedTimeout; }\\n';",
    " source = source.slice(0, call) + injection + source.slice(call);",
    ...(mode === 'captured' ? [
      " const marker = 'const recognitionAcquisitionJobRuntime = createRecognitionAcquisitionJobRuntime({';",
      " const boundary = source.indexOf(marker);",
      " if (boundary < 0 || source.indexOf(marker, boundary + marker.length) >= 0) throw new Error('CAPTURE_BOUNDARY_NOT_UNIQUE');",
      " const wrapper = 'const projectedCaptureOriginalHandler = recognizeCoordinatesHandler;\\nrecognizeCoordinatesHandler = (req, res) => globalThis.__projectedCapture(projectedCaptureOriginalHandler, req, res);\\n';",
      " source = source.slice(0, boundary) + wrapper + source.slice(boundary);"
    ] : []),
    " return {...result, source};",
    "}});"
  ].join('\n');
  const child = spawn(process.execPath, ['--import', 'data:text/javascript,' + encodeURIComponent(preload),
    matrix, '--http-candidate', scenario], { cwd: root, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
    env: { SystemRoot: process.env.SystemRoot, PATH: process.env.PATH, NODE_ENV: 'test', PORT: '0',
      ENABLE_REGRESSION_TEST_MODE: 'true', ALIYUN_API_KEY: 'local-mock-only', ALIYUN_BASE_URL: 'http://127.0.0.1:1/v1',
      SUPABASE_URL: '', SUPABASE_SERVICE_ROLE_KEY: '', P0_QUALIFICATION_ACQUISITION_ENABLED: 'true',
      SPATIAL_RESULT_ENABLED: 'true', DOTENV_CONFIG_PATH: path.join(root, '__no_test_env__') } });
  const messages = [], waiters = [];
  let stdout = '', stderr = '';
  child.stdout.on('data', bytes => { stdout = (stdout + bytes).slice(-4000); });
  child.stderr.on('data', bytes => { stderr = (stderr + bytes).slice(-4000); });
  child.on('message', message => {
    const waiter = waiters.find(item => item.key in message);
    if (waiter) { waiters.splice(waiters.indexOf(waiter), 1); waiter.resolve(message); }
    else messages.push(message);
  });
  const signal = AbortSignal.timeout(30000);
  const waitFor = key => new Promise((resolve, reject) => {
    const index = messages.findIndex(message => key in message);
    if (index >= 0) return resolve(messages.splice(index, 1)[0]);
    if (signal.aborted) return reject(signal.reason);
    const onAbort = () => reject(signal.reason);
    signal.addEventListener('abort', onAbort, {once:true});
    waiters.push({key, resolve: value => { signal.removeEventListener('abort', onAbort); resolve(value); }});
  });
  try {
    const {port} = await waitFor('port');
    const form = new FormData();
    form.set('visitorId', 'coordinate-regression-p0-contract');
    form.set('image', new Blob([image], {type:'image/png'}), 'synthetic-coordinate-image.png');
    const response = await fetch('http://127.0.0.1:' + port + '/api/recognize-coordinates', {
      method:'POST', headers:{'x-regression-test':'1','x-regression-case-id':'indonesia-dms-real-001'}, body:form, signal});
    const payload = await response.json();
    const record = { origin:'SYNTHETIC_NOT_PRODUCTION_REPLAY', mode, scenario,
      input:{matrix:'scripts/production-recognition-recovery-p0-regression.js',
        imageSha256:createHash('sha256').update(image).digest('hex'), imageBytes:image.length, forcedLocalOcrTimeout:true},
      httpStatus:response.status, payload:privateSafe(payload) };
    // Save before any business assertion. No credentials, account or HTTP headers retained.
    writeFileSync(path.join(directory, prefix + '-response.json'), JSON.stringify(record,null,2), {flag:'wx'});
    if (!captureOnly && payload.finalizedCoordinateResult?.resultId) {
      const f = payload.finalizedCoordinateResult;
      const identity = {resultId:f.resultId,resultRevision:f.resultRevision,geometryHash:f.geometryHash};
      record.mapResponses = [];
      for (const binding of [identity, {...identity,resultRevision:identity.resultRevision+1}]) {
        const map = await fetch('http://127.0.0.1:' + port + '/api/map-preview', {method:'POST',
          headers:{'content-type':'application/json'},body:JSON.stringify(binding),signal});
        record.mapResponses.push({binding,status:map.status,payload:privateSafe(await map.json())});
      }
    }
    child.send('stats');
    const stats = await waitFor('acquisitions');
    record.mockProviderCalls = stats.acquisitions;
    record.requestControl = stats.requestControl;
    if (mode === 'captured') record.diagnostics = (await waitFor('diagnostics')).diagnostics;
    record.privateTraceInPublicLog = stdout.includes('recognition_diagnostic_v1');
    records.push(record);
    writeFileSync(path.join(directory, prefix + '-evidence.json'), JSON.stringify(record,null,2), {flag:'wx'});
    return record;
  } catch (error) {
    writeFileSync(path.join(directory, prefix + '-transport-error.json'), JSON.stringify({message:error.message,
      stderr:stderr.slice(-1000), origin:'SYNTHETIC'},null,2), {flag:'wx'});
    throw error;
  } finally {
    if (child.exitCode === null) { child.kill(); await once(child,'exit'); }
  }
}

let failure = null;
function stages(record) { return record.diagnostics.sessions.flatMap(s => s.events); }
function stage(record, name) {
  const found = stages(record).filter(e => e.stage === name);
  assert.equal(found.length,1, 'one captured stage: ' + name); return found[0].data;
}
function business(record) {
  const p = record.payload, f = p.finalizedCoordinateResult || {};
  const keys = ['success','code','reason','coordinates','rawText','requiresReview','authorizationStatus','resultStatus',
    'mapReady','kmlReady','mapStatus','kmlStatus','usageConsumed','userUsageConsumed','sourceCrs','sourceCoordinateRepresentation',
    'candidateCoordinates','candidateCoordinateGroups','reviewReasons'];
  return {...Object.fromEntries(keys.filter(k => k in p).map(k => [k,p[k]])),
    points:p.coordinateEngineV2?.groups?.flatMap(g => g.points),
    finalized:Object.fromEntries(['resultRevision','geometryHash','geometry','crs','decisionState','confirmationStatus',
      'qualityGateStatus','sourceAuthority','technicalKmlReady','mapReady','kmlReady','kmlAuthorityBlocked','requiresReview']
      .filter(k => k in f).map(k => [k,f[k]]))};
}
function common(record) {
  assert.equal(record.mockProviderCalls,1); assert.equal(record.payload.providerCallCount,1);
  assert.equal(record.payload.usageConsumed,false); assert.equal(record.payload.userUsageConsumed,false);
  assert.equal(record.privateTraceInPublicLog,false);
  assert.notEqual(record.payload.authorizationStatus,'AUTHORIZED');
  assert.notEqual(record.payload.finalizedCoordinateResult?.decisionState,'AUTO_EXPORT');
  const f = record.payload.finalizedCoordinateResult;
  if (f) {
    assert.ok(f.resultId); assert.equal(f.resultRevision,1); assert.equal(f.confirmationStatus,'pending');
    assert.notEqual(record.mapResponses[1].status,200,'stale version may not render');
    assert.equal(record.mapResponses[0].binding.resultId,f.resultId);
    assert.equal(record.mapResponses[0].binding.geometryHash,f.geometryHash);
  }
}
try {
  if (captureOnly) {
    await request('captured');
    console.log('DIAGNOSTIC_CAPTURE_ONLY_NOT_PASS; REAL_PROVIDER_CALLS=0');
  } else {
    // Build the comparison baseline from this run's existing synthetic fixture
    // through the real local HTTP handler. A clean checkout must not depend on
    // an untracked Temp receipt from an earlier diagnostic run.
    const before = await request('captured', negativeOnly ? 'baseline' : 'complete');
    common(before);
    const original = stage(before,'candidate_input_selection').rawProviderText;
    const expected = stage(before,'extractProviderProjectedCoordinateEvidence').result;
    const pointsBefore = before.payload.coordinateEngineV2.groups.flatMap(g => g.points);
    if (!negativeOnly) {
      const disabled = await request('disabled','complete');
      common(disabled);
      const captured = before;
      assert.deepEqual(business(captured),business(disabled),'diagnostics on/off business equivalence');
      const p = captured.payload, f = p.finalizedCoordinateResult;
      assert.equal(p.success,true); assert.equal(captured.httpStatus,200);
      assert.equal(p.precisionMode,'projected-x-y-review'); assert.equal(p.authorizationStatus,'REVIEW_REQUIRED');
      assert.equal(p.resultStatus,'needs_review'); assert.equal(p.requiresReview,true);
      assert.equal(f.decisionState,'REVIEW_REQUIRED'); assert.equal(f.requiresReview,true);
      assert.equal(f.technicalKmlReady,true); assert.equal(f.kmlReady,true);
      assert.notEqual(f.kmlAuthorityBlocked,true); assert.equal(f.geometry.type,'Polygon');
      for (const r of [captured,disabled]) {
        assert.equal(r.payload.mapReady,true); assert.equal(r.payload.kmlReady,true);
        assert.equal(r.payload.mapStatus,'ENABLED'); assert.equal(r.payload.kmlStatus,'ENABLED');
        assert.equal(r.mapResponses[0].status,200); assert.equal(r.mapResponses[0].payload.mapPreviewObject.previewEligibility.allowed,true);
      }
      assert.equal(validateFinalizedGeometry(f.geometry).ok,true);
      assert.equal(createGeometryHash(f.geometry),f.geometryHash);
      assert.deepEqual(f.geometry,before.payload.finalizedCoordinateResult.geometry,'geometry/transform unchanged');
      assert.deepEqual(p.sourceCrs,before.payload.sourceCrs); assert.deepEqual(f.crs,{id:'EPSG:4326',axisOrder:'longitude_latitude'});
      assert.deepEqual(p.coordinateEngineV2.groups.flatMap(g => g.points),pointsBefore,'pointwise numeric/raw/source fidelity');
      assert.equal(p.rawText,original); assert.equal(p.coordinates,before.payload.coordinates);
      assert.equal(stage(captured,'image_identity').image_sha256,captured.input.imageSha256);
      assert.deepEqual(stage(captured,'extractProviderProjectedCoordinateEvidence').result,expected,'old projected parser unchanged');
      const evidence = stage(captured,'unified_candidates').evidence;
      assert.equal(evidence.rawProviderText,original); assert.equal(evidence.candidateCoordinates.length,expected.rows.length);
      assert.deepEqual(evidence.rejectedRows,[]); assert.equal(evidence.normalizationStatus,'COMPLETED');
      const adapter = stage(captured,'table_input_adapter');
      assert.equal(adapter.rows.length,expected.rows.length); assert.equal(adapter.physicalTableIdentity.status,'UNKNOWN');
      evidence.candidateCoordinates.forEach((c,i) => {
        const row = expected.rows[i], fieldRow = adapter.rows[i];
        assert.equal(c.sourceLabel,row.label); assert.equal(c.sourceText,row.sourceText);
        assert.equal(c.x,Number(row.x)); assert.equal(c.y,Number(row.y)); assert.equal(c.axisOrder,'x_y');
        assert.equal(fieldRow.rawRow,row.sourceText); assert.equal(fieldRow.rowOrdinal,i+1); assert.ok(fieldRow.group);
        for (const key of ['x','y']) { assert.equal(fieldRow.fields[key].normalizedValue,c[key]);
          assert.equal(fieldRow.fields[key].source.lineNumber,c.sourceLineNumber); }
      });
      const gate = stage(captured,'evaluateUnifiedRecognitionFinalAuthorization');
      assert.equal(gate.result.authorized,false); assert.equal(gate.result.mapReady,true); assert.equal(gate.result.kmlReady,true);
      const delivered = stage(captured,'delivered_result');
      assert.equal(delivered.resultId,f.resultId);
      assert.equal(delivered.resultRevision,f.resultRevision);
      assert.equal(delivered.geometryHash,f.geometryHash);
      assert.equal(delivered.mapStatus,p.mapStatus); assert.equal(delivered.kmlStatus,p.kmlStatus);
      assert.equal(delivered.mapReady,p.mapReady); assert.equal(delivered.kmlReady,p.kmlReady);
      assert.equal(delivered.usageConsumed,p.usageConsumed);
      assert.equal(delivered.userUsageConsumed,p.userUsageConsumed);
      const preview = captured.mapResponses[0].payload.mapPreviewObject;
      assert.equal(preview.sourceResultId,f.resultId);
      assert.equal(preview.sourceRevision,f.resultRevision);
      assert.equal(preview.sourceGeometryHash,f.geometryHash);
      checks.push({name:'complete_projected_review_and_diagnostic_equivalence',status:'PASS'});
    }
    const row = expected.rows[1].sourceText;
    const negativeCases = [
      ['missing',original.replace(row+'\n',''),'SOURCE_LABELS_NONCONTIGUOUS'],
      ['extra',original.replace(row,row+' | 9'),'COORDINATE_ROW_NOT_FULLY_CONSUMED'],
      ['duplicate',original.replace(row,row.replace(/^2/,'1')),'SOURCE_LABELS_DUPLICATE'],
      ['order',original.replace(expected.rows[0].sourceText+'\n'+row,row+'\n'+expected.rows[0].sourceText),'SOURCE_LABELS_NONCONTIGUOUS']
    ];
    const negatives = fromExtra ? negativeCases.slice(1) : negativeCases;
    for (const [name,text,reason] of negatives) {
      const off = await request('disabled',name,text); common(off);
      const on = await request('captured',name,text); common(on);
      assert.deepEqual(business(on),business(off),name+': diagnostic equivalence');
      const e = stage(on,'unified_candidates').evidence;
      assert.equal(e.rawProviderText,text);
      assert.ok([...e.reviewReasons,...e.rejectedRows.map(r => r.reason)].includes(reason),name+': concrete integrity cause');
      for (const r of [off,on]) {
        assert.notEqual(r.payload.mapReady,true); assert.notEqual(r.payload.kmlReady,true);
        assert.equal(r.payload.mapStatus,'CLOSED'); assert.equal(r.payload.kmlStatus,'CLOSED');
        const blocked = r.payload.finalizedCoordinateResult;
        assert.equal(blocked?.decisionState,'BLOCKED'); assert.equal(blocked?.mapReady,false);
        assert.equal(blocked?.kmlReady,false); assert.equal(blocked?.technicalKmlReady,false);
        assert.ok(Array.isArray(blocked?.blockingReasons) && blocked.blockingReasons.length > 0);
        if (r.mapResponses) {
          const current = r.mapResponses[0], stale = r.mapResponses[1];
          if (!blocked.geometry || !blocked.geometryHash) {
            assert.equal(current.status,409); assert.equal(current.payload.code,'GEOMETRY_HASH_MISMATCH');
            assert.equal(current.payload.mapPreviewObject,undefined);
          } else {
            assert.equal(current.status,422);
            assert.equal(current.payload.mapPreviewObject.previewEligibility.allowed,false);
          }
          assert.equal(stale.status,409); assert.equal(stale.payload.code,'STALE_CONFIRMATION_REVISION');
          assert.equal(stale.payload.mapPreviewObject,undefined);
        }
      }
      checks.push({name:name+'_hard_block_and_diagnostic_equivalence',status:'PASS'});
    }
    console.log(`Projected HTTP ${checks.length}/${checks.length} PASS; REAL_PROVIDER_CALLS=0; mock calls=${records.reduce((n,r)=>n+r.mockProviderCalls,0)}`);
  }
} catch (error) {
  failure = {message:error.message,stack:error.stack}; throw error;
} finally {
  writeFileSync(filename, JSON.stringify({status:captureOnly ? 'DIAGNOSTIC_ONLY_NOT_PASS' : failure ? 'FAIL' : 'PASS',
    realProviderCalls:0,mockProviderCalls:records.reduce((n,r)=>n+r.mockProviderCalls,0),failure,checks,records},null,2), {flag:'wx'});
}
