// Offline behavioral tests. Real product functions; synthetic evidence only.
import './recognition-audit-offline-guard.cjs';
import assert from 'node:assert/strict';
import { readFileSync, mkdirSync, writeFileSync, existsSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { createProductRuntime, declarations } from './recognition-architecture-probe.js';
import * as finalizer from '../server/coordinate-finalizer/index.js';
import { buildSourceCoordinateRepresentation } from '../server/source-coordinate-representation.js';
import { buildRecognitionAcquisitionEvidence, evaluateUnifiedRecognitionAcquisition } from '../server/recognition/recognition-first-acquisition.js';

const root = fileURLToPath(new URL('../', import.meta.url));
const baseline = '86ea4ac44a6d44a2332d7aaaffb1926504a73dca';
const source = readFileSync(path.join(root, 'server.js'), 'utf8');
const runtime = createProductRuntime(source);
Object.assign(runtime, finalizer, { coordinateConfirmationRuntime: new finalizer.CoordinateConfirmationRuntime() });
const oldRuntime = createProductRuntime(execFileSync('git', ['show', baseline + ':server.js'],
  { cwd: root, encoding: 'utf8', maxBuffer: 8 * 1024 * 1024, windowsHide: true }));
const plain = v => JSON.parse(JSON.stringify(v));
const resumeMarkdown = process.argv.includes('--resume-markdown');
const output = process.env.RECOGNITION_AUDIT_RECEIPT_ROOT || path.join(root, 'Temp',
  resumeMarkdown ? 'recognition-table-phase-d1-markdown-recovery' : 'recognition-table-phase-d1-downstream');
mkdirSync(output, { recursive: true });
if (existsSync(path.join(output, 'downstream-results.json'))) throw new Error('RECEIPT_EXISTS_NO_AUTOMATIC_RETRY');
const receipt = { baseline, origin: 'SYNTHETIC_NOT_PRODUCTION_REPLAY', realProviderCalls: 0,
  historicalPassesNotRerun: [], cases: [] };
const historicallyPassed = [
  'retained_evidence_and_first_null_location', 'json_candidate_geometry_source_preservation',
  'verbose_json_candidate_geometry_source_preservation', 'plain_candidate_geometry_source_preservation'
];
let activeCase = null, checkpointIndex = 0, checkpoints = [];
const serialize = value => JSON.stringify(value, (_key, v) =>
  v === undefined ? { diagnosticType: 'undefined' }
    : typeof v === 'number' && !Number.isFinite(v) ? { diagnosticType: 'nonfinite', value: String(v) } : v, 2);
function checkpoint(stage, data) {
  const file = activeCase + '-' + (++checkpointIndex) + '.json';
  writeFileSync(path.join(output, file), serialize({ origin: receipt.origin, case: activeCase, stage, ...data }), { flag: 'wx' });
  checkpoints.push({ stage, file });
}
function test(name, fn) {
  if (resumeMarkdown && historicallyPassed.includes(name)) return;
  activeCase = name; checkpointIndex = 0; checkpoints = [];
  try { fn(); receipt.cases.push({ name, status: 'PASS', checkpoints }); }
  catch (error) {
    receipt.cases.push({ name, status: 'FAIL', message: error.message, actual: error.actual,
      expected: error.expected, failureLocation: error.stack, checkpoints }); throw error;
  }
}
// Permanent synthetic source fixture. A clean checkout must not depend on an
// untracked Temp receipt produced by an earlier HTTP diagnostic run.
const rows = [
  { point: '1', latitude: '18° 01\' 1.000" N', longitude: '72° 02\' 2.000" E' },
  { point: '2', latitude: '18° 01\' 3.000" N', longitude: '72° 02\' 4.000" E' },
  { point: '3', latitude: '18° 01\' 5.000" N', longitude: '72° 02\' 6.000" E' },
  { point: '4', latitude: '18° 01\' 7.000" N', longitude: '72° 02\' 8.000" E' }
];
const original = JSON.stringify({ rows });
const syntheticFixtureSha256 = 'eacfe2a1e1d8422cdfffca016b51702469e167fb59c391c56068672ecd841131';
const table = values => ['No | Latitude | Longitude', ...values.map(r => [r.point, r.latitude, r.longitude].join(' | '))].join('\n');
const markdown = table(rows).split('\n').map((line, i) =>
  '| ' + line + ' |' + (i === 0 ? '\n| --- | --- | --- |' : '')).join('\n');
const expected = rows.map(r => {
  const decimal = value => { const m = value.match(/^(\d+)° (\d+)' ([\d.]+)" ([NSEW])$/);
    return (Number(m[1]) + Number(m[2]) / 60 + Number(m[3]) / 3600) * (/[SW]/.test(m[4]) ? -1 : 1); };
  return [decimal(r.longitude), decimal(r.latitude)];
});
const acquire = text => buildRecognitionAcquisitionEvidence({ rawText: text, acquisition: {
  width: 800, height: 600, bytes: 2803, images: [{ role: 'overview' }]
} });
function payloadFor(text) {
  const evidence = acquire(text);
  const payload = { rawText: text,
    coordinates: evidence.candidateCoordinateLines.map(row => row.text).join('\n'),
    precisionMode: 'one-shot-acquisition-contract-review',
    candidateCoordinates: evidence.candidateCoordinates, candidateCoordinateGroups: evidence.candidateCoordinateGroups };
  checkpoint('ACQUISITION_BEFORE_ASSERTIONS', { input: text, evidence, payload });
  return { evidence, payload };
}
function priorResult() {
  return finalizer.finalizeCoordinateResult({ resultId: 'synthetic-downstream-result', resultRevision: 7,
    currentRevision: 7, confirmedRevision: null, sourceAuthority: 'legacy',
    crs: finalizer.FINALIZED_COORDINATE_CRS, geometry: null, confirmationStatus: 'pending',
    qualityGateStatus: 'review_required', technicalKmlReady: false, mapReady: false, kmlReady: false,
    kmlAuthorityBlocked: true, requiresReview: true });
}
function pointReview(groups) {
  const prior = priorResult();
  const response = { coordinateEngineV2: { groups }, finalizedCoordinateResult: prior };
  const result = runtime.keepRecognizedCoordinatesAsPointReview(response, '', { blockMap: true });
  checkpoint('FINALIZATION_BEFORE_ASSERTIONS', { groups, prior, result });
  assert.equal(result.finalizedCoordinateResult.resultId, prior.resultId);
  assert.equal(result.finalizedCoordinateResult.resultRevision, prior.resultRevision);
  assert.equal(result.finalizedCoordinateResult.confirmationStatus, 'pending');
  assert.equal(result.finalizedCoordinateResult.mapReady, false);
  assert.equal(result.finalizedCoordinateResult.kmlReady, false);
  return { response, result };
}
try {
  if (resumeMarkdown) {
    const historyPath = path.join(root, 'Temp/recognition-table-phase-d1-downstream/downstream-results.json');
    const bytes = readFileSync(historyPath), history = JSON.parse(bytes);
    const sha256 = createHash('sha256').update(bytes).digest('hex');
    assert.equal(sha256, '7acb416e10263aad682c1db725602631594c900b71d689ef61436739037b56c4',
      'only the exact retained failed run can define the resume boundary');
    assert.equal(history.baseline, baseline);
    assert.deepEqual(history.cases.slice(0, 4), historicallyPassed.map(name => ({ name, status: 'PASS' })));
    assert.equal(history.cases.length, 5);
    assert.equal(history.cases[4].name, 'markdown_candidate_geometry_source_preservation');
    assert.equal(history.cases[4].status, 'FAIL');
    receipt.historicalPassesNotRerun = history.cases.slice(0, 4);
    receipt.resumedFrom = { historyPath, sha256, name: history.cases[4].name };
  }
  test('retained_evidence_and_first_null_location', () => {
    assert.equal(createHash('sha256').update(original).digest('hex'), syntheticFixtureSha256,
      'synthetic input identity must remain stable');
    const { payload } = payloadFor(original);
    assert.equal(oldRuntime.inferCoordinateEngineV2Type(payload), '');
    const oldPoints = oldRuntime.buildCoordinateEngineV2Groups(payload, '').flatMap(g => g.points);
    assert.equal(oldPoints.length, rows.length);
    oldPoints.forEach(p => { assert.equal(p.lat, null); assert.equal(p.lon, null); });
    assert.equal(oldRuntime.parseCoordinateEngineV2PointLine(payload.coordinates.split('\n')[0], '', 0).lat, null);
    assert.ok(Number.isFinite(oldRuntime.parseCoordinateEngineV2PointLine(payload.coordinates.split('\n')[0], 'standard_dms_table', 0).lat));
    assert.deepEqual(oldPoints.map(point => [Number(point.lon), Number(point.lat)]), rows.map(() => [0, 0]),
      'historical null coercion would produce zero coordinates');
  });
  for (const [kind, text] of [
    ['json', original],
    ['verbose_json', 'Coordinates:\n' + JSON.stringify({ rows }, null, 2) + '\nEnd.'],
    ['plain', table(rows)],
    ['markdown', markdown]
  ]) test(kind + '_candidate_geometry_source_preservation', () => {
    const { evidence, payload } = payloadFor(text);
    const before = JSON.stringify(evidence);
    const groups = runtime.buildCoordinateEngineV2Groups(payload, '');
    const points = groups.flatMap(g => g.points);
    const display = buildSourceCoordinateRepresentation(payload, { groups });
    checkpoint('CONSUMPTION_BEFORE_ASSERTIONS', { input: text, evidence, groups, display, expected,
      nextCheck: 'point count, labels, raw text, candidate fields and pointwise coordinates' });
    assert.equal(points.length, rows.length);
    points.forEach((p, i) => {
      assert.equal(p.label, rows[i].point);
      assert.equal(p.raw, evidence.candidateCoordinateLines[i].text);
      assert.equal(evidence.candidateCoordinates[i].latitudeSource, rows[i].latitude);
      assert.equal(evidence.candidateCoordinates[i].longitudeSource, rows[i].longitude);
      assert.ok(Math.abs(p.lon - expected[i][0]) < 1e-10);
      assert.ok(Math.abs(p.lat - expected[i][1]) < 1e-10);
    });
    const result = pointReview(groups).result.finalizedCoordinateResult;
    assert.equal(result.geometry.type, 'MultiPoint', 'preserve existing point-only review policy, not a Polygon upgrade');
    assert.deepEqual(plain(result.geometry.coordinates), expected);
    assert.equal(result.geometryHash, finalizer.createGeometryHash(result.geometry));
    assert.equal(display.rawText, text);
    assert.equal(display.displayText, payload.coordinates, 'no canonical replacement of preserved source rows');
    assert.deepEqual(plain(display.pointLabels), rows.map(row => row.point));
    assert.equal(JSON.stringify(evidence), before, 'consumer must not mutate acquisition or field provenance');
  });
  test('numeric_prefix_and_extra_tokens_rejected', () => {
    const representationSource = readFileSync(path.join(root, 'server/source-coordinate-representation.js'), 'utf8');
    const parserSource = declarations(representationSource).find(d => d.name === 'parseDecimalCoordinateLine').text;
    const parse = Function(parserSource + '; return parseDecimalCoordinateLine;')();
    checkpoint('SOURCE_PREFIX_BEFORE_ASSERTIONS', { inputs: [table(rows).split('\n')[1], '1,18 extra', '1,18,99', '0,0'],
      results: [table(rows).split('\n')[1], '1,18 extra', '1,18,99', '0,0'].map(line => parse(line, 'latitude_longitude')) });
    for (const line of [table(rows).split('\n')[1], '1,18 extra', '1,18,99']) assert.equal(parse(line, 'latitude_longitude'), null);
    assert.deepEqual(parse('0,0', 'latitude_longitude'), { label: '', latitude: 0, longitude: 0 });
    const { payload } = payloadFor(original);
    assert.equal(buildSourceCoordinateRepresentation(payload, { groups: [] }).displayText,
      payload.coordinates, 'missing engine points cannot rewrite DMS as decimal prefixes');
  });
  for (const [name, value] of [['null', null], ['undefined', undefined], ['empty', ''], ['space', '  '],
    ['nan', NaN], ['infinity', Infinity], ['nonnumeric', 'unknown'], ['boolean', false], ['array', []], ['object', {}]]) {
    test('missing_value_' + name, () => {
      for (const field of ['lon', 'lat']) {
        const points = [{ lon: 12, lat: 21 }, { lon: 13, lat: 22, [field]: value }];
        const { response, result } = pointReview([{ points }]);
        assert.equal(result, response, 'no zero-coercion or export of a surviving subset');
        assert.equal(result.finalizedCoordinateResult.geometry, null);
      }
    });
  }
  test('genuine_zero_preserved', () => {
    for (const value of [0, '0']) {
      const result = pointReview([{ points: [{ lon: value, lat: value }] }]).result.finalizedCoordinateResult;
      assert.equal(result.geometry.type, 'Point');
      assert.deepEqual(plain(result.geometry.coordinates), [0, 0]);
    }
  });
  const badInputs = [
    ['missing_row', JSON.stringify({ rows: rows.filter((_, i) => i !== 1) }), 'SOURCE_LABELS_NONCONTIGUOUS'],
    ['duplicate_label', JSON.stringify({ rows: [...rows, rows[0]] }), 'SOURCE_LABELS_DUPLICATE'],
    ['row_order', JSON.stringify({ rows: [...rows].reverse() }), 'SOURCE_LABELS_NONCONTIGUOUS'],
    ['direction', original.replace(' N', ' E'), 'FIELD_DIRECTION_CONFLICT'],
    ['extra_field', JSON.stringify({ rows: rows.map((r, i) => i === 1 ? { ...r, extra: 9 } : r) }), 'EXTRA_ROW_FIELD'],
    ['missing_field', JSON.stringify({ rows: rows.map((r, i) => i === 1 ? { point: r.point, latitude: r.latitude } : r) }), 'MISSING_FIELD']
  ];
  for (const [kind, text, reason] of badInputs) test(kind + '_still_blocked', () => {
    const { evidence, payload } = payloadFor(text);
    const decision = evaluateUnifiedRecognitionAcquisition({ evidence });
    const groups = runtime.buildCoordinateEngineV2Groups(payload, '');
    checkpoint('NEGATIVE_CONSUMPTION_BEFORE_ASSERTIONS', { input: text, evidence, decision, groups, requiredReason: reason });
    assert.ok(evidence.reviewReasons.includes(reason));
    assert.equal(decision.mayProceedToGeometryValidation, false);
    assert.equal(pointReview(groups).result.finalizedCoordinateResult.geometry, null);
  });
  for (const [kind, mutate] of [
    ['value_conflict', p => { p.candidateCoordinates[0].latitudeSource = rows[0].latitude.replace('1.000', '1.001'); }],
    ['group_order_conflict', p => { p.candidateCoordinateGroups[0].rows.reverse(); }],
    ['row_binding_missing', p => { delete p.candidateCoordinateGroups; }],
    ['extra_coordinate_token', p => { p.coordinates += ' | 99'; }],
    ['line_identity_conflict', p => { p.candidateCoordinates[1].sourceLineNumber = p.candidateCoordinates[0].sourceLineNumber; }]
  ]) test(kind + '_not_overridden', () => {
    for (const text of [original, markdown]) {
      const payload = structuredClone(payloadFor(text).payload); mutate(payload);
      const groups = runtime.buildCoordinateEngineV2Groups(payload, '');
      checkpoint('BINDING_CONFLICT_BEFORE_ASSERTIONS', { payload, groups });
      assert.equal(pointReview(groups).result.finalizedCoordinateResult.geometry, null);
    }
    if (kind === 'extra_coordinate_token') for (const token of ['99', '']) {
      // Equal raw strings in both evidence views do not make an extra inner
      // cell acceptable. Exercise field coverage, not merely rawText mismatch.
      const payload = structuredClone(payloadFor(markdown).payload);
      const lines = payload.coordinates.split('\n');
      lines[0] = lines[0].replace(/\|$/, '| ' + token + ' |');
      payload.coordinates = lines.join('\n');
      payload.candidateCoordinates[0].sourceText = lines[0];
      payload.candidateCoordinateGroups[0].rows[0].sourceText = lines[0];
      const groups = runtime.buildCoordinateEngineV2Groups(payload, '');
      checkpoint('EXTRA_CELL_BEFORE_ASSERTIONS', { payload, groups, token });
      assert.equal(pointReview(groups).result.finalizedCoordinateResult.geometry, null);
    }
  });
  test('near_values_and_existing_parser_priority', () => {
    const { payload } = payloadFor(original.replace('1.000', '1.001'));
    const points = runtime.buildCoordinateEngineV2Groups(payload, '').flatMap(g => g.points);
    checkpoint('NEAR_VALUE_BEFORE_ASSERTIONS', { payload, points, expected });
    assert.notEqual(points[0].lat, expected[0][1], 'do not repair a close value to the old sample');
    assert.ok(Math.abs(points[0].lat - (18 + 1 / 60 + 1.001 / 3600)) < 1e-10);
    assert.deepEqual(plain(runtime.buildCoordinateEngineV2Groups(payload, 'standard_dms_table')),
      plain(oldRuntime.buildCoordinateEngineV2Groups(payload, 'standard_dms_table')));
    const unrelated = { ...payload, precisionMode: 'unknown' };
    assert.deepEqual(plain(runtime.buildCoordinateEngineV2Groups(unrelated, '')),
      plain(oldRuntime.buildCoordinateEngineV2Groups(unrelated, '')));
  });
  console.log('D1 downstream ' + receipt.cases.length + '/' + receipt.cases.length
    + ' PASS this run; historical passes not rerun=' + receipt.historicalPassesNotRerun.length + '; REAL_PROVIDER_CALLS=0');
} finally {
  writeFileSync(path.join(output, 'downstream-results.json'), serialize(receipt), { flag: 'wx' });
}

