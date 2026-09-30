// Actual HTTP product route, synthetic local image, mocked OCR/Provider I/O only.
import assert from 'node:assert/strict';
import { spawn, execFileSync } from 'node:child_process';
import { once } from 'node:events';
import { mkdirSync, writeFileSync, readFileSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { withRecognitionDiagnostics } from '../server/recognition/recognition-diagnostics.js';
import { createHash } from 'node:crypto';
import { validateFinalizedGeometry, validateFinalizedCrs } from '../server/coordinate-finalizer/geometry-finalizer.js';
import { createGeometryHash } from '../server/coordinate-finalizer/geometry-hash.js';

const root = fileURLToPath(new URL('../', import.meta.url));
const baseline = '86ea4ac44a6d44a2332d7aaaffb1926504a73dca';
const resumeD1Direction = process.argv.includes('--resume-d1-direction');
const integrityD1 = process.argv.includes('--d1-integrity') || resumeD1Direction;
const resumeD1HistoricalBaseline = process.argv.includes('--resume-d1-historical-baseline') || resumeD1Direction;
const downstreamD1 = process.argv.includes('--d1-downstream') || resumeD1HistoricalBaseline;
const resumeD1State = process.argv.includes('--resume-d1-state') || downstreamD1;
const resumeD1Http = process.argv.includes('--resume-d1-http') || resumeD1State;
const phaseD1 = process.argv.includes('--phase-d1') || resumeD1Http;
if (process.argv[2] === '--child') {
  const { registerHooks } = await import('node:module');
  const { default: http } = await import('node:http');
  registerHooks({ load(url, context, nextLoad) {
    const loaded = nextLoad(url, context);
    if (!url.endsWith('/server.js') && !url.endsWith('/server/recognition/recognition-first-acquisition.js')) return loaded;
    let source = String(loaded.source);
    if (process.env.TEST_BASELINE === '1') {
      const file = url.endsWith('/server.js') ? 'server.js' : 'server/recognition/recognition-first-acquisition.js';
      source = execFileSync('git', ['show', `${baseline}:${file}`], { cwd: root, encoding: 'utf8', maxBuffer: 8 * 1024 * 1024, windowsHide: true });
    }
    if (process.env.TEST_D1_OBSERVE === '1' && url.endsWith('/server/recognition/recognition-first-acquisition.js')) {
      // Observe actual return values, including the old handler's failed path.
      // Never replace a product result or supply a successful decision.
      for (const name of ['buildRecognitionAcquisitionEvidence', 'evaluateUnifiedRecognitionAcquisition',
        'evaluateUnifiedRecognitionFinalAuthorization']) {
        source += `\nconst diagnosticOriginal_${name} = ${name};
${name} = (...args) => {
  const result = diagnosticOriginal_${name}(...args);
  globalThis.__d1Observed.push({ operation: '${name}', result: JSON.parse(JSON.stringify(result)) });
  return result;
};\n`;
      }
    }
    if (url.endsWith('/server.js')) {
      const start = source.indexOf('    const oneShotFamilyClassification = await runLocalOcrFamilyClassification({');
      const end = source.indexOf('    });', start) + 7;
      if (start < 0 || end < 7) throw new Error('OCR_IO_INJECTION_NOT_FOUND');
      const replacement = `    const route = classifyOneShotStructuredFamily();
    const oneShotFamilyClassification = { attempted: true, route, sourceText: '', layoutLines: [],
      axisEvidenceText: 'LATITUDE N | LONGITUDE E\\nX | Y', axisOrderEvidence: null,
      projectedTableOcrAcquisition: true, sourceContextText: process.env.TEST_CONTEXT_TEXT ?? process.env.TEST_PROVIDER_TEXT,
      sourceContextProvenance: { schema_version: 'projected_source_context_v1',
        image_sha256: coordinateImageIdentity.image_sha256, same_request: true, mode: 'source_image' },
      contract: createOneShotAcquisitionContract({ route }) };
`;
      source = source.slice(0, start) + replacement + source.slice(end);
      if (process.env.TEST_CAPTURE === '1') {
        // Install only in this test loader, after the real handler is declared and
        // before Express registers it. Multipart upload completes before capture.
        const marker = 'const recognitionAcquisitionJobRuntime = createRecognitionAcquisitionJobRuntime({';
        const boundary = source.indexOf(marker);
        if (boundary < 0 || source.indexOf(marker, boundary + marker.length) >= 0) {
          throw new Error('HANDLER_CAPTURE_BOUNDARY_NOT_UNIQUE');
        }
        const captureWrapper = `const diagnosticTestOriginalHandler = recognizeCoordinatesHandler;
recognizeCoordinatesHandler = (req, res) => globalThis.__captureRecognitionDiagnosticsForHttpRegression(
  diagnosticTestOriginalHandler, req, res);
`;
        source = source.slice(0, boundary) + captureWrapper + source.slice(boundary);
      }
    }
    return { ...loaded, source };
  } });
  const nativeListen = http.Server.prototype.listen;
  http.Server.prototype.listen = function (_port, callback) {
    return nativeListen.call(this, 0, '127.0.0.1', () => { process.send?.({ type: 'port', port: this.address().port }); callback?.(); });
  };
  let providerCalls = 0;
  globalThis.__d1Observed = [];
  globalThis.fetch = async () => {
    providerCalls += 1;
    if (providerCalls > 1) throw new Error('UNEXPECTED_SECOND_MOCK_CALL');
    return new Response(JSON.stringify({ id: 'synthetic-response', choices: [{ message: { content: process.env.TEST_PROVIDER_TEXT } }] }),
      { status: 200, headers: { 'content-type': 'application/json' } });
  };
  if (process.env.TEST_CAPTURE === '1') {
    globalThis.__captureRecognitionDiagnosticsForHttpRegression = (handler, req, res) =>
      withRecognitionDiagnostics(async () => {
        let finish;
        const responseFinished = new Promise(resolve => { finish = resolve; });
        res.once('finish', finish);
        try {
          const value = await handler(req, res);
          if (!res.writableFinished) await responseFinished;
          return value;
        } finally {
          res.off('finish', finish);
        }
      }, { retainSourceText: true, origin: 'SYNTHETIC' }).then(({ value, diagnostics }) => {
        process.send?.({ type: 'diagnostics', diagnostics });
        return value;
      });
  }
  process.on('message', message => { if (message === 'stats') process.send?.({ type: 'stats', providerCalls,
    observations: globalThis.__d1Observed }); });
  await import('../server.js');
  await new Promise(() => {});
} else {
  const { default: sharp } = await import('sharp');
  const image = await sharp({ create: { width: 800, height: 600, channels: 3, background: '#eeeeee' } }).png().toBuffer();
  const requests = [];
  let requestDirectory = null;
  const clean = value => JSON.parse(JSON.stringify(value));
  function saveRequest(record) {
    requests.push(record);
    writeFileSync(path.join(requestDirectory, `${record.kind}-${record.mode}.json`),
      JSON.stringify(record, null, 2), { flag: 'wx' });
  }
  const cases = [
    ['dms', ['No | Latitude | Longitude', `1 | 18° 1' 1.000" N | 3° 1' 1.000" E`,
      `2 | 18° 1' 1.000" N | 3° 1' 8.000" E`, `3 | 18° 1' 8.000" N | 3° 1' 8.000" E`,
      `4 | 18° 1' 8.000" N | 3° 1' 1.000" E`].join('\n')],
    ['projected', ['UTM WGS 1984 ZONE 31N', 'No | X | Y', '1 | 510000 | 2100000', '2 | 510400 | 2100000',
      '3 | 510400 | 2100400', '4 | 510000 | 2100400'].join('\n')]
  ];
  function business(payload) {
    const keys = ['success', 'code', 'reason', 'coordinates', 'requiresReview', 'authorizationStatus', 'resultStatus',
      'mapStatus', 'mapReady', 'kmlStatus', 'kmlReady', 'userUsageConsumed', 'usageConsumed', 'recoveryRequired',
      'providerCallCount', 'sourceCoordinateRepresentation', 'multiRepresentationEvidence'];
    const selected = Object.fromEntries(keys.filter(key => key in payload).map(key => [key, payload[key]]));
    selected.engine = { sourceCrs: payload.coordinateEngineV2?.source_crs,
      groups: payload.coordinateEngineV2?.groups?.map(group => ({ points: group.points, requiresReview: group.requires_review, kmlReady: group.kml_ready })) };
    const final = payload.finalizedCoordinateResult || {};
    selected.finalized = Object.fromEntries(['resultRevision', 'geometryHash', 'geometry', 'crs', 'decisionState', 'qualityGateStatus',
      'confirmationStatus', 'sourceAuthority', 'requiresReview', 'mapReady', 'kmlReady', 'technicalKmlReady', 'kmlAuthorityBlocked']
      .filter(key => key in final).map(key => [key, final[key]]));
    return JSON.parse(JSON.stringify(selected));
  }
  async function run(kind, text, mode, options = {}) {
    const child = spawn(process.execPath, [fileURLToPath(import.meta.url), '--child'], { cwd: root, windowsHide: true,
      stdio: ['ignore', 'pipe', 'pipe', 'ipc'], env: {
        SystemRoot: process.env.SystemRoot, PATH: process.env.PATH, NODE_ENV: 'test', PORT: '0',
        ENABLE_REGRESSION_TEST_MODE: 'true', ALIYUN_API_KEY: 'local-mock-only', ALIYUN_BASE_URL: 'http://127.0.0.1:1/v1',
        SUPABASE_URL: '', SUPABASE_SERVICE_ROLE_KEY: '', SPATIAL_RESULT_ENABLED: 'true',
        DOTENV_CONFIG_PATH: path.join(root, '__no_test_env__'), TEST_PROVIDER_TEXT: text,
        ...(options.context === undefined ? {} : { TEST_CONTEXT_TEXT: options.context }),
        TEST_D1_OBSERVE: phaseD1 ? '1' : '0',
        TEST_BASELINE: mode === 'baseline' ? '1' : '0', TEST_CAPTURE: mode === 'captured' ? '1' : '0'
      } });
    let stderr = '', stdout = '';
    child.stdout.on('data', chunk => { stdout = (stdout + chunk).slice(-20000); });
    child.stderr.on('data', chunk => { stderr = (stderr + chunk).slice(-4000); });
    const messages = [], waiters = [];
    child.on('message', message => {
      const waiter = waiters.find(entry => entry.type === message.type);
      if (waiter) { waiters.splice(waiters.indexOf(waiter), 1); waiter.resolve(message); }
      else messages.push(message);
    });
    const signal = AbortSignal.timeout(30000);
    const waitFor = type => new Promise((resolve, reject) => {
      const found = messages.findIndex(message => message.type === type);
      if (found >= 0) return resolve(messages.splice(found, 1)[0]);
      if (signal.aborted) return reject(signal.reason);
      const onAbort = () => reject(signal.reason);
      signal.addEventListener('abort', onAbort, { once: true });
      waiters.push({ type, resolve: value => { signal.removeEventListener('abort', onAbort); resolve(value); } });
    });
    try {
      const { port } = await waitFor('port');
      const form = new FormData(); form.set('visitorId', 'diagnostics-synthetic-regression');
      form.set('image', new Blob([image], { type: 'image/png' }), 'synthetic.png');
      const response = await fetch(`http://127.0.0.1:${port}/api/recognize-coordinates`, { method: 'POST', signal,
        headers: { 'x-regression-test': '1', 'x-regression-case-id': `diagnostics-${kind}` }, body: form });
      const payload = await response.json();
      child.send('stats'); const stats = await waitFor('stats');
      let record;
      if (phaseD1) {
        const responseEvidenceKeys = ['recognitionAcquisition', 'candidateCoordinates', 'candidateCoordinateLines',
          'candidateCoordinateGroups', 'candidatePointCount', 'candidateGroupCount', 'visibleCrsEvidence',
          'imageAcquisitionEvidence', 'acquisitionStatus', 'acquisitionContractConformance', 'contractReasons',
          'reviewReasons', 'providerCompletionState', 'previewEligibility', 'kmlEligibility'];
        record = clean({ kind, mode, httpStatus: response.status, business: business(payload),
          evidence: Object.fromEntries(responseEvidenceKeys.filter(k => k in payload).map(k => [k, payload[k]])),
          finalIdentity: payload.finalizedCoordinateResult ? Object.fromEntries(
            ['resultId', 'resultRevision', 'currentRevision', 'confirmedRevision', 'geometryHash']
              .filter(k => k in payload.finalizedCoordinateResult).map(k => [k, payload.finalizedCoordinateResult[k]])) : null,
          mockProviderCalls: stats.providerCalls, observations: stats.observations });
        // Persist the response before any status, candidate or diagnostic assertion.
        saveRequest(record);
      } else if (!options.blocked) assert.equal(response.status, 200, `${kind}/${mode}: HTTP ${response.status}`);
      else assert.ok(response.status < 500, `${kind}/${mode}: negative input must not crash`);
      assert.equal(stats.providerCalls, 1, 'one mock call; no automatic retry');
      assert.ok(!JSON.stringify(payload).includes('recognition_diagnostic_v1'), 'private trace absent from public response');
      assert.ok(!stdout.includes('recognition_diagnostic_v1'), 'private trace absent from public logs');
      if (mode === 'captured') {
        const { diagnostics } = await waitFor('diagnostics');
        if (phaseD1) {
          record.diagnostics = diagnostics;
          writeFileSync(path.join(requestDirectory, `${kind}-${mode}-diagnostics.json`),
            JSON.stringify(diagnostics, null, 2), { flag: 'wx' });
        }
        assert.equal(diagnostics.sessions.length, 1);
        const events = diagnostics.sessions[0].events;
        assert.ok(events.some(event => event.operation === 'extractProviderMessageText'));
        assert.ok(events.some(event => ['extractRecognitionCandidateEvidence', 'adaptRecognitionTableCandidates'].includes(event.operation)));
        const delivered = events.find(event => event.stage === 'delivered_result');
        if (!options.blocked) assert.ok(delivered);
        if (delivered) {
          assert.equal(delivered.data.resultId, payload.finalizedCoordinateResult?.resultId);
          assert.equal(delivered.data.resultRevision, payload.finalizedCoordinateResult?.resultRevision);
          assert.equal(delivered.data.geometryHash, payload.finalizedCoordinateResult?.geometryHash);
          assert.equal(delivered.data.mapReady, payload.mapReady);
          assert.equal(delivered.data.kmlReady, payload.kmlReady);
          assert.equal(delivered.data.userUsageConsumed, payload.userUsageConsumed);
        }
        if (options.adapterExpected) assert.ok(events.some(e => e.operation === 'adaptRecognitionTableCandidates'), 'must reach migrated product entry');
      }
      // Current negative inputs must be blocked. Historical responses are
      // classified and checked separately; an old preview is not a new PASS.
      if (options.blocked && !(phaseD1 && mode === 'baseline')) {
        assert.notEqual(payload.mapReady, true, `${kind}: blocked map`);
        assert.notEqual(payload.kmlReady, true, `${kind}: blocked KML`);
        assert.notEqual(payload.authorizationStatus, 'AUTHORIZED', `${kind}: no authority upgrade`);
      }
      return phaseD1 ? record : business(payload);
    } catch (error) {
      if (stderr) process.stderr.write(stderr); throw error;
    } finally {
      if (child.exitCode === null) { child.kill(); await once(child, 'exit'); }
    }
  }
  if (!phaseD1) for (const [kind, text] of cases) {
    const before = await run(kind, text, 'baseline');
    assert.deepEqual(await run(kind, text, 'disabled'), before, `${kind}: baseline/current HTTP behavior`);
    assert.deepEqual(await run(kind, text, 'captured'), before, `${kind}: diagnostic capture cannot change business outcome`);
  }
  if (!phaseD1) console.log('Phase B HTTP: DMS/XY baseline + disabled + captured equivalent; identities/versions/billing projection unchanged; mock Provider calls=6; REAL_PROVIDER_CALLS=0');
  else {
    const dms = cases[0][1].split('\n').slice(1).map(line => {
      const [point, latitude, longitude] = line.split(' | '); return { point, latitude, longitude };
    });
    const rows = JSON.stringify({ rows: dms });
    const variants = [
      ['plain', cases[0][1], {}],
      ['markdown', ['| No | Latitude | Longitude |', '| --- | --- | --- |', ...cases[0][1].split('\n').slice(1).map(l => `| ${l} |`)].join('\n'), {}],
      // No OCR context: force the raw JSON acquisition entry, not a bound-DMS shortcut.
      ['json_compact', rows, { context: '', adapterExpected: true, expectedRows: dms }],
      ['json_verbose', 'Coordinates follow:\n' + JSON.stringify({ rows: dms }, null, 2) + '\nEnd.', { context: '', adapterExpected: true, expectedRows: dms }],
      ['json_missing', JSON.stringify({ rows: dms.filter((_, i) => i !== 1) }), { context: '', blocked: true, adapterExpected: true,
        expectedRows: dms.filter((_, i) => i !== 1), rejection: 'SOURCE_LABELS_NONCONTIGUOUS' }],
      ['json_extra', JSON.stringify({ rows: dms.map((r, i) => i === 1 ? { ...r, extra: 9 } : r) }), { context: '', blocked: true, adapterExpected: true,
        expectedRows: [], rejection: 'EXTRA_ROW_FIELD' }],
      ['json_direction', rows.replace(' N', ' E'), { context: '', blocked: true, adapterExpected: true,
        expectedRows: [], rejection: 'FIELD_DIRECTION_CONFLICT' }]
    ];
    const results = [], directory = process.env.RECOGNITION_AUDIT_RECEIPT_ROOT || path.join(root, 'Temp', resumeD1Direction
      ? 'recognition-table-phase-d1-direction-recovery' : integrityD1
      ? 'recognition-table-phase-d1-integrity-recovery' : resumeD1HistoricalBaseline
      ? 'recognition-table-phase-d1-historical-baseline-recovery' : downstreamD1
      ? 'recognition-table-phase-d1-downstream' : resumeD1State
      ? 'recognition-table-phase-d1-http-state-recovery' : resumeD1Http
      ? 'recognition-table-phase-d1-http-recovery' : 'recognition-table-phase-d1');
    mkdirSync(directory, { recursive: true });
    if (existsSync(path.join(directory, 'http-results.json'))) throw new Error('EXISTING_RECEIPT_NO_AUTOMATIC_RETRY');
    requestDirectory = path.join(directory, 'requests');
    mkdirSync(requestDirectory, { recursive: true });
    const selected = resumeD1Http
      ? variants.slice(variants.findIndex(([name]) => name === (resumeD1Direction ? 'json_direction' : resumeD1HistoricalBaseline ? 'json_missing' : 'json_compact')))
      : variants;
    function observed(record, name) {
      const values = record.observations.filter(o => o.operation === name);
      assert.ok(values.length, `${record.kind}/${record.mode}: actual ${name} observation required`);
      return values.at(-1).result;
    }
    function candidateEvidence(record) { return observed(record, 'buildRecognitionAcquisitionEvidence'); }
    function hasRetainedDownstreamCandidates(record) {
      return (record.evidence.candidateCoordinates?.length || 0) > 0
        || (record.business.engine.groups || []).some(group => group.points?.length > 0);
    }
    function verifyClosed(record) {
      assert.notEqual(record.business.mapReady, true);
      assert.notEqual(record.business.kmlReady, true);
      assert.notEqual(record.business.authorizationStatus, 'AUTHORIZED');
      assert.notEqual(record.business.finalized.kmlReady, true);
    }
    function verifyOldJsonFailure(record) {
      assert.equal(record.httpStatus, 422, 'specific old JSON candidate-loss baseline');
      assert.equal(record.business.success, false);
      assert.equal(record.business.code, 'COORDINATE_RECOGNITION_FAILED_CLOSED', 'not an arbitrary 422');
      assert.equal(record.business.reason, 'recognition_failed_closed');
      assert.equal(record.business.coordinates, '');
      assert.equal(candidateEvidence(record).candidateCoordinates.length, 0);
      assert.equal(candidateEvidence(record).acquisitionStatus, 'NO_COORDINATE_EVIDENCE');
      assert.equal(hasRetainedDownstreamCandidates(record), false, 'no-candidate failure excludes retained fallback points');
      assert.equal(record.business.finalized.geometry ?? null, null);
      verifyClosed(record);
    }
    function verifyRetainedJsonPointEvidence(record, text, { directionConflict = false } = {}) {
      // Coordinate fidelity is shared; historical and current permissions are
      // checked separately below. This is a fixture assertion, not a parser.
      const b = record.business, final = b.finalized;
      const evidence = candidateEvidence(record);
      assert.equal(evidence.rawProviderText, text);
      assert.equal(evidence.authority, 'EVIDENCE_ONLY');
      assert.equal(final.geometry?.type, 'MultiPoint', 'retained points must not be represented as a complete polygon');
      const allRows = JSON.parse(text).rows;
      if (directionConflict) {
        assert.equal(record.kind, 'json_direction', 'only the specified conflict fixture has this partial category');
        assert.deepEqual(allRows, dms.map((row, index) => index === 0
          ? { ...row, latitude: row.latitude.replace(' N', ' E') } : row));
        assert.equal(b.engine.sourceCrs, null, 'final CRS label must not fill missing source CRS');
        assert.equal(b.sourceCoordinateRepresentation.sourceCrsEvidence, null);
        for (const reason of ['DMS_DIRECTION_CONFLICT', 'SOURCE_LABELS_NONCONTIGUOUS', 'CRS_EVIDENCE_MISSING']) {
          assert.ok(record.evidence.reviewReasons.includes(reason), `retained conflict reason: ${reason}`);
        }
      } else assert.equal(b.engine.sourceCrs, 'EPSG:4326');
      // The dropped row is known from the fixture, not selected by the response.
      const sourceRows = directionConflict ? allRows.slice(1) : allRows;
      assert.ok(Array.isArray(sourceRows) && sourceRows.length > 0);
      const decimal = (literal, axis) => {
        assert.equal(typeof literal, 'string');
        const match = literal.match(/^(\d+)°\s*(\d+)'\s*([\d.]+)"\s*([NSEW])$/);
        assert.ok(match, 'historical preview must retain explicit DMS fields');
        assert.ok((axis === 'latitude' ? /[NS]/ : /[EW]/).test(match[4]));
        const degree = Number(match[1]), minute = Number(match[2]), second = Number(match[3]);
        assert.ok(Number.isFinite(second) && minute < 60 && second < 60);
        const value = (degree + minute / 60 + second / 3600) * (/[SW]/.test(match[4]) ? -1 : 1);
        assert.ok(Number.isFinite(value) && Math.abs(value) <= (axis === 'latitude' ? 90 : 180));
        return value;
      };
      const expectedPoints = sourceRows.map(row => [decimal(row.longitude, 'longitude'), decimal(row.latitude, 'latitude')]);
      const candidates = record.evidence.candidateCoordinates;
      const groups = record.evidence.candidateCoordinateGroups;
      const points = b.engine.groups.flatMap(group => group.points);
      const displayPoints = b.coordinates.split('\n').map(line => line.split(',').map(Number));
      assert.equal(record.evidence.candidatePointCount, sourceRows.length);
      assert.equal(record.evidence.candidateGroupCount, 1);
      assert.equal(candidates.length, sourceRows.length);
      assert.equal(points.length, sourceRows.length);
      assert.equal(groups.length, 1);
      assert.equal(groups[0].boundaryEvidence, 'structured_provider_payload');
      assert.equal(groups[0].orderingEvidence, 'VISIBLE_SOURCE_ROW_ORDER_ONLY');
      assert.deepEqual(groups[0].rows, candidates);
      assert.equal(final.geometry.coordinates.length, sourceRows.length);
      assert.equal(displayPoints.length, sourceRows.length);
      assert.equal(b.sourceCoordinateRepresentation.rawText, text);
      assert.deepEqual(b.sourceCoordinateRepresentation.pointLabels, sourceRows.map(row => row.point));
      sourceRows.forEach((row, i) => {
        const original = `${row.point} | ${row.latitude} | ${row.longitude}`;
        assert.equal(candidates[i].sourceLabel, row.point);
        assert.equal(candidates[i].sourceLineNumber, i + (directionConflict ? 2 : 1));
        assert.equal(candidates[i].candidateOrder, i + 1);
        assert.equal(candidates[i].sourceText, original);
        assert.equal(candidates[i].latitudeSource, row.latitude);
        assert.equal(candidates[i].longitudeSource, row.longitude);
        assert.equal(candidates[i].axisOrder, 'latitude_longitude');
        assert.equal(points[i].label, row.point);
        assert.equal(points[i].raw, original);
        if (directionConflict) assert.equal(points[i].source_crs, null);
        for (const actual of [final.geometry.coordinates[i], displayPoints[i],
          [candidates[i].longitude, candidates[i].latitude], [points[i].lon, points[i].lat]]) {
          assert.equal(actual.length, 2);
          actual.forEach((value, axis) => assert.ok(Number.isFinite(value)
            && Math.abs(value - expectedPoints[i][axis]) < 1e-10, 'historical point and axis fidelity'));
        }
      });
      if (directionConflict) {
        assert.deepEqual(candidates.map(row => row.sourceLabel), ['2', '3', '4']);
        assert.equal(groups[0].sourceStartLine, 2);
        assert.equal(groups[0].sourceEndLine, 4);
        assert.equal(groups[0].sourceLabelsContinuous, false);
        assert.equal(groups[0].sourceLabelState, 'NONCONTIGUOUS');
      }
    }
    function verifyPointReviewState(record) {
      const b = record.business, final = b.finalized;
      assert.equal(record.httpStatus, 200);
      assert.equal(b.success, true);
      assert.equal(b.code, 'ONE_SHOT_ACQUISITION_CONTRACT_REVIEW_REQUIRED');
      assert.equal(b.reason, 'acquisition_completed_review_required');
      assert.equal(b.authorizationStatus, 'REVIEW_REQUIRED');
      assert.equal(b.resultStatus, 'needs_review');
      assert.equal(b.requiresReview, true);
      assert.ok(record.finalIdentity);
      assert.equal(final.decisionState, 'REVIEW_REQUIRED');
      assert.equal(final.kmlReady, false);
      assert.equal(final.technicalKmlReady, false);
      assert.equal(final.kmlAuthorityBlocked, true);
      assert.equal(final.confirmationStatus, 'pending');
      assert.equal(final.sourceAuthority, 'legacy');
      assert.deepEqual(final.crs, { id: 'EPSG:4326', axisOrder: 'longitude_latitude' });
      assert.equal(validateFinalizedGeometry(final.geometry).ok, true);
    }
    function verifyHistoricalPointPreview(record, text) {
      // The old partial preview is a historical risk, never permission for the
      // current path. Its omitted row and missing source CRS stay explicit.
      assert.equal(record.mode, 'baseline');
      verifyPointReviewState(record);
      const partial = record.kind === 'json_direction';
      verifyRetainedJsonPointEvidence(record, text, { directionConflict: partial });
      const evidence = candidateEvidence(record), b = record.business;
      assert.equal(evidence.candidateCoordinates.length, 0);
      assert.equal(evidence.acquisitionStatus, 'NO_COORDINATE_EVIDENCE');
      assert.deepEqual(record.evidence.recognitionAcquisition, evidence);
      assert.equal(b.mapReady, true);
      assert.equal(b.kmlReady, false);
      if (partial) assert.equal(b.sourceCoordinateRepresentation.sourceEquivalence, 'missing_rows_or_engine_points');
      const acquisition = observed(record, 'evaluateUnifiedRecognitionAcquisition');
      const authorization = observed(record, 'evaluateUnifiedRecognitionFinalAuthorization');
      assert.equal(acquisition.shouldReturnFailure, true);
      assert.equal(acquisition.mayProceedToGeometryValidation, false);
      assert.equal(authorization.hasUnifiedEvidence, false);
      assert.equal(authorization.acquisitionIncomplete, true);
      assert.equal(authorization.mapGatePassed, true);
      assert.equal(authorization.kmlGatePassed, false);
      assert.equal(authorization.provisionalKmlReady, false);
      for (const reason of ['ACQUISITION_CONTRACT_NOT_CONFORMANT',
        'UNIFIED_RECOGNITION_ACQUISITION_INCOMPLETE', 'UNIFIED_RECOGNITION_EVIDENCE_INCOMPLETE']) {
        assert.ok(authorization.finalAuthorizationReasons.includes(reason));
        assert.ok(record.evidence.reviewReasons.includes(reason));
      }
      if (record.kind === 'json_missing') {
        assert.equal(record.evidence.candidateCoordinateGroups[0].sourceLabelsContinuous, false);
        assert.equal(record.evidence.candidateCoordinateGroups[0].sourceLabelState, 'NONCONTIGUOUS');
        assert.ok(record.evidence.reviewReasons.includes('SOURCE_LABELS_NONCONTIGUOUS'));
      }
    }
    function verifyBlockedDirectionFallback(record, text) {
      assert.notEqual(record.mode, 'baseline');
      verifyPointReviewState(record);
      verifyRetainedJsonPointEvidence(record, text, { directionConflict: true });
      verifyClosed(record);
      assert.equal(record.business.mapReady, false);
      assert.equal(record.business.kmlReady, false);
      assert.equal(record.business.finalized.mapReady, false);
      const evidence = candidateEvidence(record);
      assert.equal(evidence.candidateCoordinates.length, 0);
      assert.equal(evidence.acquisitionStatus, 'NO_COORDINATE_EVIDENCE');
      assert.ok(evidence.reviewReasons.includes('FIELD_DIRECTION_CONFLICT'));
      assert.ok(evidence.rejectedRows.some(row => row.reason === 'FIELD_DIRECTION_CONFLICT'
        && row.pointer === '/rows/0' && row.field === 'latitude'), 'locate rejected original field');
      const acquisition = observed(record, 'evaluateUnifiedRecognitionAcquisition');
      assert.equal(acquisition.shouldReturnFailure, true, 'acquisition failure is distinct from retained fallback points');
      assert.equal(acquisition.mayProceedToGeometryValidation, false);
      const authorization = observed(record, 'evaluateUnifiedRecognitionFinalAuthorization');
      assert.equal(authorization.hasUnifiedEvidence, false);
      assert.equal(authorization.acquisitionIncomplete, true);
      assert.ok(authorization.finalAuthorizationReasons.includes('FIELD_DIRECTION_CONFLICT'));
      assert.equal(authorization.mapGatePassed, false);
      assert.equal(authorization.kmlGatePassed, false);
      assert.equal(authorization.provisionalKmlReady, false);
    }
    function verifyFinal(record, historicalInput = null) {
      const b = record.business, final = b.finalized;
      const evidence = candidateEvidence(record);
      const acquisition = observed(record, 'evaluateUnifiedRecognitionAcquisition');
      assert.equal(b.usageConsumed, false, 'local regression must never charge');
      assert.equal(b.userUsageConsumed, false);
      assert.equal(record.mockProviderCalls, 1);
      assert.equal(b.providerCallCount, 1);
      assert.notEqual(b.authorizationStatus, 'AUTHORIZED', 'candidate recovery is not formal authority');
      const authorization = observed(record, 'evaluateUnifiedRecognitionFinalAuthorization');
      assert.equal(authorization.authorized, false);
      if ('mapReady' in b) assert.equal(b.mapReady, authorization.mapReady);
      if ('kmlReady' in b) assert.equal(b.kmlReady, authorization.kmlReady);
      if ('mapStatus' in b) assert.equal(b.mapStatus, b.mapReady ? 'ENABLED' : 'CLOSED');
      if ('kmlStatus' in b) assert.equal(b.kmlStatus, b.kmlReady ? 'ENABLED' : 'CLOSED');
      if (record.finalIdentity) {
        const identity = record.finalIdentity;
        assert.ok(identity.resultId);
        assert.ok(Number.isSafeInteger(identity.resultRevision) && identity.resultRevision > 0);
        assert.equal(identity.currentRevision ?? identity.resultRevision, identity.resultRevision);
        assert.equal(identity.confirmedRevision ?? null, null);
        assert.equal(final.confirmationStatus, 'pending');
        if (final.geometry) assert.equal(identity.geometryHash, createGeometryHash(final.geometry));
      }
      // A result object may carry an explicitly blocked, empty geometry. Keep
      // acquisition success, provisional output and final authority separate.
      const geometry = validateFinalizedGeometry(final.geometry);
      const crs = validateFinalizedCrs(final.crs);
      const blockingEvidence = [];
      if (historicalInput !== null && b.mapReady === true) {
        verifyHistoricalPointPreview(record, historicalInput);
        record.stateClassification = record.kind === 'json_direction'
          ? 'HISTORICAL_INCOMPLETE_POINT_PREVIEW' : 'HISTORICAL_POINT_PREVIEW';
      } else if (evidence.candidateCoordinates.length === 0 && hasRetainedDownstreamCandidates(record)) {
        verifyBlockedDirectionFallback(record, evidence.rawProviderText);
        blockingEvidence.push('FIELD_DIRECTION_CONFLICT');
        record.stateClassification = 'RETAINED_FALLBACK_CANDIDATES_WITH_OUTPUT_BLOCKED';
      } else if (evidence.candidateCoordinates.length === 0) {
        verifyOldJsonFailure(record);
        assert.equal(acquisition.shouldReturnFailure, true);
        assert.equal(acquisition.mayProceedToGeometryValidation, false);
        assert.equal(authorization.hasUnifiedEvidence, false);
        assert.equal(authorization.acquisitionIncomplete, true);
        assert.ok(authorization.finalAuthorizationReasons.includes('UNIFIED_RECOGNITION_EVIDENCE_INCOMPLETE'));
        assert.equal(b.authorizationStatus, 'NOT_ESTABLISHED');
        assert.equal(b.resultStatus, 'failed');
        assert.equal(final.geometry ?? null, null, 'no geometry may be synthesized from zero candidates');
        blockingEvidence.push('NO_COORDINATE_EVIDENCE');
        record.stateClassification = 'NO_CANDIDATE_FAILURE';
      } else {
        assert.equal(record.httpStatus, 200);
        assert.equal(b.success, true);
        assert.equal(b.authorizationStatus, 'REVIEW_REQUIRED');
        assert.equal(b.resultStatus, 'needs_review');
        assert.equal(acquisition.shouldReturnFailure, false);
        record.stateClassification = b.mapReady || b.kmlReady
          ? 'PROVISIONAL_REVIEW_OUTPUT' : 'CANDIDATES_WITH_OUTPUT_BLOCKED';
      }
      if (b.mapReady || b.kmlReady) {
        assert.ok(record.finalIdentity, 'output requires a current result identity');
        assert.equal(final.decisionState, 'REVIEW_REQUIRED');
        assert.equal(geometry.ok, true);
        assert.deepEqual(final.crs, { id: 'EPSG:4326', axisOrder: 'longitude_latitude' });
        if (b.kmlReady) assert.equal(b.mapReady, true);
      } else {
        verifyClosed(record);
        if (!geometry.ok) blockingEvidence.push(final.geometry ? geometry.reasonCode : 'STRUCTURED_GEOMETRY_MISSING');
        if (!crs.ok) blockingEvidence.push(crs.reasonCode);
        if (acquisition.mayProceedToGeometryValidation === false) {
          const negativeConditions = ['SOURCE_LABELS_NONCONTIGUOUS', 'EXTRA_ROW_FIELD', 'FIELD_DIRECTION_CONFLICT'];
          blockingEvidence.push(...evidence.reviewReasons.filter(reason => negativeConditions.includes(reason)));
        }
        if (authorization.contractRequiresReview === true
          && record.evidence.acquisitionContractConformance?.status === 'REVIEW_REQUIRED'
          && record.evidence.acquisitionContractConformance?.reason === 'GENERIC_REVIEW_ONLY'
          && authorization.finalAuthorizationReasons.includes('ACQUISITION_CONTRACT_NOT_CONFORMANT')) {
          blockingEvidence.push('ACQUISITION_CONTRACT_NOT_CONFORMANT:GENERIC_REVIEW_ONLY');
        }
        assert.ok(blockingEvidence.length > 0, 'BLOCKED needs an observed failed condition, not just a closed flag');
        if (record.finalIdentity) {
          // The existing point-review route keeps REVIEW_REQUIRED while its
          // contract closes output. This is not authority or export readiness.
          const pointReview = geometry.ok && crs.ok && final.geometry?.type === 'MultiPoint'
            && b.code === 'ONE_SHOT_ACQUISITION_CONTRACT_REVIEW_REQUIRED'
            && record.evidence.acquisitionContractConformance?.reason === 'GENERIC_REVIEW_ONLY';
          assert.equal(final.decisionState, pointReview ? 'REVIEW_REQUIRED' : 'BLOCKED');
          assert.equal(final.technicalKmlReady, false);
        }
      }
      record.stateBlockingEvidence = blockingEvidence;
    }
    function verifyCandidates(record, options, text) {
      const evidence = candidateEvidence(record), expected = options.expectedRows;
      assert.equal(evidence.authority, 'EVIDENCE_ONLY');
      assert.equal(evidence.rawProviderText, text, 'do not rewrite source response');
      assert.equal(evidence.candidateCoordinates.length, expected.length);
      evidence.candidateCoordinates.forEach((row, i) => {
        assert.equal(row.sourceLabel, expected[i].point);
        assert.equal(row.sourceLabelInferred, false);
        assert.equal(row.sourceLineNumber, i + 2, 'normalized source order, never relabel or sort');
        assert.equal(row.format, 'DMS');
        assert.equal(row.axisOrder, 'latitude_longitude');
        assert.equal(row.latitudeSource, expected[i].latitude);
        assert.equal(row.longitudeSource, expected[i].longitude);
      });
      if (options.rejection) {
        assert.ok(evidence.reviewReasons.includes(options.rejection), `${record.kind}: specific rejection retained`);
        assert.equal(observed(record, 'evaluateUnifiedRecognitionAcquisition').mayProceedToGeometryValidation, false);
        verifyClosed(record);
        if (integrityD1) {
          const authorization = observed(record, 'evaluateUnifiedRecognitionFinalAuthorization');
          assert.ok(authorization.finalAuthorizationReasons.includes(options.rejection), 'specific integrity veto reaches final gate');
          assert.equal(authorization.mapGatePassed, false);
          assert.equal(authorization.kmlGatePassed, false);
          if (record.finalIdentity) assert.equal(record.business.finalized.mapReady, false);
          if (expected.length) {
            const points = record.business.engine.groups.flatMap(group => group.points);
            const final = record.business.finalized;
            assert.equal(final.geometry.type, 'MultiPoint', 'blocked preview remains points, not an inferred boundary');
            assert.equal(points.length, expected.length);
            assert.equal(final.geometry.coordinates.length, expected.length);
            const decimal = literal => {
              const m = literal.match(/^(\d+)°\s*(\d+)'\s*([\d.]+)"\s*([NSEW])$/);
              assert.ok(m);
              return (Number(m[1]) + Number(m[2]) / 60 + Number(m[3]) / 3600) * (/[SW]/.test(m[4]) ? -1 : 1);
            };
            points.forEach((point, i) => {
              assert.equal(point.label, expected[i].point);
              assert.equal(point.raw, evidence.candidateCoordinateLines[i].text);
              const pair = [decimal(expected[i].longitude), decimal(expected[i].latitude)];
              [point.lon, point.lat].forEach((value, axis) => {
                assert.ok(Number.isFinite(value) && Math.abs(value - pair[axis]) < 1e-10);
                assert.ok(Math.abs(final.geometry.coordinates[i][axis] - pair[axis]) < 1e-10);
              });
            });
            assert.equal(record.business.sourceCoordinateRepresentation.rawText, text);
          }
        }
        if (expected.length === 0 && hasRetainedDownstreamCandidates(record)) verifyBlockedDirectionFallback(record, text);
        else if (expected.length === 0) verifyOldJsonFailure(record);
        else {
          assert.equal(record.httpStatus, 200);
          assert.equal(record.business.resultStatus, 'needs_review');
        }
      } else {
        assert.equal(record.httpStatus, 200, 'complete JSON candidates must survive the real handler');
        assert.equal(record.business.success, true);
        assert.equal(record.business.authorizationStatus, 'REVIEW_REQUIRED');
        assert.equal(record.business.resultStatus, 'needs_review');
        assert.equal(evidence.acquisitionStatus, 'COMPLETED');
        assert.equal(evidence.normalizationStatus, 'COMPLETED');
        assert.equal(evidence.rejectedRows.length, 0);
        assert.equal(evidence.unboundCandidates.length, 0);
        assert.equal(evidence.candidateCoordinateGroups.length, 1);
        assert.equal(evidence.diagnostics.boundRowCount, expected.length);
        assert.deepEqual(evidence.reviewReasons, [], 'complete explicit directions do not imply a missing-axis CRS reason');
        assert.deepEqual(evidence.geographicCrsEvidence, {
          applicable: true, status: 'EXPLICIT_DMS_AXIS_DIRECTIONS', axisDirectionBound: true,
          complete: true, datumExplicit: false, reviewOnly: true
        }, 'direction-bound DMS is evidence only, not an explicit datum or formal authority');
        assert.ok(record.business.coordinates?.length > 0);
        assert.deepEqual(record.evidence.candidateCoordinates, evidence.candidateCoordinates);
        assert.ok([
          [undefined, undefined],
          ['ONE_SHOT_ACQUISITION_CONTRACT_REVIEW_REQUIRED', 'acquisition_completed_review_required'],
          ['UNIFIED_RECOGNITION_ACQUISITION_REVIEW_REQUIRED', 'acquisition_completed_review_required']
        ].some(([code, reason]) => record.business.code === code && record.business.reason === reason),
        'only established complete/review route reasons are permitted');
        const decimal = value => {
          const match = value.match(/^(\d+)°\s*(\d+)'\s*([\d.]+)"\s*([NSEW])$/);
          assert.ok(match, 'synthetic fixture must have explicit DMS and direction');
          return (Number(match[1]) + Number(match[2]) / 60 + Number(match[3]) / 3600)
            * (/[SW]/.test(match[4]) ? -1 : 1);
        };
        const expectedPoints = expected.map(row => [decimal(row.longitude), decimal(row.latitude)]);
        const geometry = record.business.finalized.geometry;
        if (geometry) {
          const pointReview = record.business.code === 'ONE_SHOT_ACQUISITION_CONTRACT_REVIEW_REQUIRED'
            && record.evidence.acquisitionContractConformance?.reason === 'GENERIC_REVIEW_ONLY';
          assert.equal(geometry.type, pointReview ? 'MultiPoint' : 'Polygon', 'preserve existing boundary policy');
          if (pointReview) verifyClosed(record);
          const ring = pointReview ? geometry.coordinates : geometry.coordinates[0];
          const expectedGeometry = pointReview ? expectedPoints : [...expectedPoints, expectedPoints[0]];
          assert.equal(ring.length, expectedGeometry.length);
          expectedGeometry.forEach((point, i) => point.forEach((value, axis) =>
            assert.ok(Math.abs(ring[i][axis] - value) < 1e-10, `row ${i + 1}: no value replacement or axis swap`)));
        } else verifyClosed(record);
        const coordinates = record.business.coordinates;
        const sourceLines = evidence.candidateCoordinateLines.map(line => line.text).join('\n');
        if (downstreamD1) {
          assert.equal(record.business.sourceCoordinateRepresentation.rawText, text);
          assert.equal(record.business.sourceCoordinateRepresentation.displayText, sourceLines,
            'complete source DMS must not be replaced by numeric prefixes or canonical guesses');
          const points = record.business.engine.groups.flatMap(group => group.points);
          assert.equal(points.length, expectedPoints.length);
          points.forEach((point, i) => {
            assert.equal(point.label, expected[i].point);
            assert.equal(point.raw, evidence.candidateCoordinateLines[i].text);
            assert.ok(Number.isFinite(point.lon) && Number.isFinite(point.lat));
            assert.ok(Math.abs(point.lon - expectedPoints[i][0]) < 1e-10);
            assert.ok(Math.abs(point.lat - expectedPoints[i][1]) < 1e-10);
          });
        }
        if (coordinates !== sourceLines) {
          const points = coordinates.trim().split(/\r?\n/).map(line => line.split(',').map(Number));
          assert.equal(points.length, expectedPoints.length, 'no lost/added coordinate in response');
          points.forEach((point, i) => {
            assert.equal(point.length, 2);
            point.forEach((value, axis) => assert.ok(Number.isFinite(value)
              && Math.abs(value - expectedPoints[i][axis]) < 1e-10, 'response coordinates must match evidence pointwise'));
          });
        }
      }
      if (record.mode === 'captured') {
        const events = record.diagnostics.sessions[0].events;
        const identity = events.find(e => e.stage === 'image_identity');
        assert.equal(identity?.data.image_sha256, createHash('sha256').update(image).digest('hex'));
        const adapter = events.find(e => e.stage === 'table_input_adapter')?.data;
        assert.ok(adapter, 'nonempty actual product adapter evidence required');
        assert.equal(adapter.schemaVersion, 'coordinate_table_input_v1');
        assert.equal(adapter.serialization, 'JSON');
        assert.equal(adapter.adapterInputText, text);
        assert.equal(adapter.selectedInputSource, 'PROVIDER_OUTPUT');
        assert.equal(adapter.physicalTableIdentity.status, 'UNKNOWN');
        const expectedAdapterRows = expected.length ? expected
          : record.kind === 'json_direction' ? JSON.parse(text).rows : null;
        if (expectedAdapterRows) {
          assert.equal(adapter.rows.length, expectedAdapterRows.length, 'retain rejected source fields, not only selected points');
          adapter.rows.forEach((row, i) => {
            assert.equal(row.rowOrdinal, i + 1);
            assert.equal(row.pointLabel, expectedAdapterRows[i].point);
            assert.equal(row.physicalTableIdentity.status, 'UNKNOWN');
            for (const [role, value] of Object.entries(expectedAdapterRows[i])) {
              const field = row.fields[role], span = field.source.characterSpan;
              assert.equal(field.source.document, 'ADAPTER_INPUT');
              assert.equal(field.source.pointer, `/rows/${i}/${role}`);
              assert.equal(field.decodedValue, value);
              assert.equal(text.slice(span.start, span.end), field.rawLiteral);
              assert.equal(JSON.parse(field.rawLiteral), value);
              if (expected.length) assert.equal(field.normalizedValue, value);
              else assert.deepEqual(field.normalizedValue, { status: 'UNKNOWN', reason: 'NOT_YET_PARSED' },
                'rejected raw fields must not masquerade as normalized coordinates');
            }
          });
        }
      }
    }
    function differences(a, b, pointer = '', result = []) {
      if (Object.is(a, b)) return result;
      if (!a || !b || typeof a !== 'object' || typeof b !== 'object') {
        result.push({ pointer, before: a ?? null, after: b ?? null }); return result;
      }
      for (const key of new Set([...Object.keys(a), ...Object.keys(b)])) differences(a[key], b[key], `${pointer}/${key}`, result);
      return result;
    }
    const historicalChecks = [];
    let activeCase = null, activePhase = null, failure = null;
    try {
      for (const [kind, text, options] of selected) {
        activeCase = kind;
        activePhase = 'historical_baseline';
        let before;
        if (resumeD1Direction && kind === 'json_direction') {
          const source = path.join(root, 'Temp', 'recognition-table-phase-d1-integrity-recovery', 'requests', 'json_direction-baseline.json');
          const saved = readFileSync(source);
          const digest = createHash('sha256').update(saved).digest('hex');
          assert.equal(digest, '7a6b1414c7825402c230c8a9d2959e82c29a6db909b11acbfbadfc1d2c0e4bec');
          before = JSON.parse(saved);
          assert.equal(before.kind, kind);
          assert.equal(before.mode, 'baseline');
          assert.equal(candidateEvidence(before).rawProviderText, text);
          before.reusedReceipt = { source, sha256: digest, networkRequestThisRun: false };
          saveRequest(before);
        } else if (resumeD1HistoricalBaseline && kind === 'json_missing') {
          const source = path.join(root, 'Temp', 'recognition-table-phase-d1-markdown-recovery', 'requests', 'json_missing-baseline.json');
          const saved = readFileSync(source);
          const digest = createHash('sha256').update(saved).digest('hex');
          assert.equal(digest, 'cb7b9d42e18e458f6bd8ef943bb137ca0bcdfcea815241ba9c839810b24e6bdd',
            'reuse only the exact authorized historical HTTP response');
          before = JSON.parse(saved);
          assert.equal(before.kind, kind);
          assert.equal(before.mode, 'baseline');
          assert.equal(candidateEvidence(before).rawProviderText, text);
          before.reusedReceipt = { source, sha256: digest, networkRequestThisRun: false };
          saveRequest(before);
        } else if (resumeD1State && kind === 'json_compact') {
          const source = path.join(root, 'Temp', 'recognition-table-phase-d1-http-recovery', 'requests', 'json_compact-baseline.json');
          const saved = readFileSync(source);
          const digest = createHash('sha256').update(saved).digest('hex');
          assert.equal(digest, 'f7d34cf4337d49699d5b355aa48318c81e35b2b62e02501104f1eede8ea51c73',
            'only the exact retained real HTTP baseline may be reused');
          before = JSON.parse(saved);
          assert.equal(before.kind, kind);
          assert.equal(before.mode, 'baseline');
          assert.equal(candidateEvidence(before).rawProviderText, text, 'same synthetic request input');
          before.reusedReceipt = { source, sha256: digest, networkRequestThisRun: false };
          saveRequest(before);
        } else before = await run(kind, text, 'baseline', options);
        if (!options.adapterExpected) assert.equal(before.httpStatus, 200);
        verifyFinal(before, options.adapterExpected ? text : null);
        historicalChecks.push({ kind, status: 'VALIDATED_HISTORICAL_ONLY',
          classification: before.stateClassification, reusedReceipt: before.reusedReceipt ?? null, before });
        activePhase = 'current_without_diagnostics';
        const after = await run(kind, text, 'disabled', options);
        if (options.adapterExpected) verifyCandidates(after, options, text);
        verifyFinal(after);
        activePhase = 'current_with_diagnostics';
        const captured = await run(kind, text, 'captured', options);
        if (options.adapterExpected) verifyCandidates(captured, options, text);
        verifyFinal(captured);
        activePhase = 'differential';
        assert.equal(captured.httpStatus, after.httpStatus);
        assert.deepEqual(captured.business, after.business, `${kind}: capture cannot alter identity revision/geometry/state/billing`);
        assert.deepEqual(captured.evidence, after.evidence, `${kind}: capture cannot alter candidates`);
        assert.deepEqual(captured.observations, after.observations);
        const previous = candidateEvidence(before), current = candidateEvidence(after);
        for (const key of ['version', 'providerCompletionState', 'providerResponseId', 'rawProviderText',
          'imageEvidence', 'visibleCrsEvidence', 'projectedCoordinateEvidence', 'authority']) {
          assert.deepEqual(current[key], previous[key], `${kind}: unchanged ${key}`);
        }
        if (!options.adapterExpected) {
          assert.equal(after.httpStatus, before.httpStatus);
          assert.deepEqual(after.business, before.business, `${kind}: previously successful behavior must stay equal`);
          assert.deepEqual(current, previous);
        } else {
          const explainedFields = new Set(['success', 'code', 'reason', 'coordinates', 'requiresReview',
            'authorizationStatus', 'resultStatus', 'mapStatus', 'mapReady', 'kmlStatus', 'kmlReady',
            'sourceCoordinateRepresentation', 'multiRepresentationEvidence', 'engine', 'finalized']);
          for (const key of new Set([...Object.keys(before.business), ...Object.keys(after.business)])) {
            if (!explainedFields.has(key)) assert.deepEqual(after.business[key], before.business[key],
              `${kind}: unexplained business change in ${key}`);
          }
        }
        results.push({ name: kind, status: 'PASS', classification: !options.adapterExpected ? 'PREVIOUS_SUCCESS_EQUIVALENT'
          : options.rejection ? 'SPECIFIC_NEGATIVE_STILL_BLOCKED' : 'EXPECTED_COMPLETE_JSON_CANDIDATE_RECOVERY',
          before, after, differences: differences({ httpStatus: before.httpStatus, business: before.business, evidence: previous },
            { httpStatus: after.httpStatus, business: after.business, evidence: current }) });
      }
      console.log(`D1 HTTP ${results.length}/${selected.length} PASS; real handler, mocked I/O only; REAL_PROVIDER_CALLS=0`);
    } catch (error) {
      failure = { case: activeCase, phase: activePhase, message: error.message, actual: error.actual, expected: error.expected, stack: error.stack };
      throw error;
    } finally {
      writeFileSync(path.join(directory, 'http-results.json'), JSON.stringify({ baseline,
        origin: 'SYNTHETIC_NOT_PRODUCTION_REPLAY', recovery: resumeD1Direction ? 'HISTORICAL_DIRECTION_CLASSIFICATION' : integrityD1 ? 'INTEGRITY_OUTPUT_GATE' : resumeD1HistoricalBaseline ? 'HISTORICAL_BASELINE_CLASSIFICATION'
          : resumeD1State ? 'FINAL_STATE_CLASSIFICATION' : resumeD1Http, realProviderCalls: 0,
        mockProviderCallsThisRun: requests.filter(record => !record.reusedReceipt).reduce((sum, record) => sum + record.mockProviderCalls, 0),
        skippedPreviouslyPassed: resumeD1Direction
          ? ['39 entry regressions', 'plain HTTP', 'markdown HTTP', '25 downstream cases', '4 historical downstream cases',
            'json_compact HTTP', 'json_verbose HTTP', '13 integrity output cases', 'json_missing HTTP', 'json_extra HTTP']
          : resumeD1HistoricalBaseline
          ? ['39 entry regressions', 'plain HTTP', 'markdown HTTP', '25 downstream cases', '4 historical downstream cases',
            'json_compact HTTP', 'json_verbose HTTP']
          : resumeD1Http ? ['39 entry regressions', 'plain HTTP', 'markdown HTTP'] : [],
        historicalChecks, results, failure, requests }, null, 2), { flag: 'wx' });
    }
  }
}
