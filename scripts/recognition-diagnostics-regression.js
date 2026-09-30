import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createProductRuntime, characterize, declarations, retiredNames } from './recognition-architecture-probe.js';
import { createRecognitionDiagnosticSession, withRecognitionDiagnostics, authorizationDiagnosticBody } from '../server/recognition/recognition-diagnostics.js';
import { makeReplayBundle, replayDiagnosticBundle, diagnosticFindings, productFingerprint } from './recognition-diagnostic-replay.js';
import { extractProviderProjectedCoordinateEvidence } from '../server/evidence-acquisition/local-ocr-map-layout-classifier.js';
import { normalizeProviderDmsReviewResult } from '../server/recognition/recognition-review-result.js';
import { bindProjectedEvidenceToSourceContext } from '../server/recognition/projected-source-evidence.js';
import { bindProviderRepresentationsToSource } from '../server/recognition/multi-representation-source-evidence.js';
import { buildRecognitionAcquisitionEvidence as currentAcquisition, buildPreD1RecognitionAcquisitionEvidence,
  evaluateUnifiedRecognitionAcquisition, evaluateUnifiedRecognitionFinalAuthorization } from '../server/recognition/recognition-first-acquisition.js';
import { utmToWgs84 } from '../server/projection/utm.js';

const baselineRef = '86ea4ac44a6d44a2332d7aaaffb1926504a73dca';
const buildRecognitionAcquisitionEvidence = process.argv.includes('--d1-historical') ? buildPreD1RecognitionAcquisitionEvidence : currentAcquisition;
const fromGit = file => execFileSync('git', ['show', `${baselineRef}:${file}`], { encoding: 'utf8', maxBuffer: 8 * 1024 * 1024, windowsHide: true });
const currentSource = readFileSync(new URL('../server.js', import.meta.url), 'utf8');
const beforeSource = fromGit('server.js');
const runtime = createProductRuntime(currentSource);
const plain = value => JSON.parse(JSON.stringify(value));
assert.deepEqual(plain(characterize(currentSource)), plain(characterize(beforeSource)), '18 actual baseline/current safety decisions');
for (const name of retiredNames) assert.equal(declarations(currentSource).some(entry => entry.name === name), false, 'keep Phase A removals');
// Import exact baseline module with imports resolved to the same installed modules.
const oldAcquisitionUrl = new URL('../server/recognition/recognition-first-acquisition.js', import.meta.url);
const baselineAcquisitionSource = fromGit('server/recognition/recognition-first-acquisition.js').replace(
  /from\s+"([^"]+)"/g, (_, specifier) => `from ${JSON.stringify(specifier.startsWith('.')
    ? new URL(specifier, oldAcquisitionUrl).href : import.meta.resolve(specifier))}`);
const oldAcquisition = await import(`data:text/javascript,${encodeURIComponent(baselineAcquisitionSource)}`);

const header = 'No | X | Y | Latitude | Longitude';
const rows = Array.from({ length: 16 }, (_, i) => `${i + 1} | ${510000 + i * 10}.123 | ${9700000 + i * 10}.456 | 2° 42' ${i},123" S | 117° 1' ${i},456" E`);
const formats = [
  [header, ...rows].join('\n'),
  [`| ${header} |`, '| --- | --- | --- | --- | --- |', ...rows.map(row => `| ${row} |`)].join('\n'),
  JSON.stringify({ rows: [{ point: '1', latitude: '2° 42\' 1.123" S', longitude: '117° 1\' 1.456" E' }] }),
  [header, ...rows.filter((_, i) => i !== 1)].join('\n'),
  [header, ...rows, rows[0]].join('\n'),
  [header, ...rows.slice().reverse()].join('\n'),
  [header, ...rows].join('\n').replace(' S |', ' E |'),
  [header, `${rows[0]} | 999`, ...rows.slice(1)].join('\n'),
  '', 'No coordinates available'
];
for (const [index, rawText] of formats.entries()) {
  const input = { rawText, acquisition: { width: 800, height: 600, bytes: 2048, images: [{ role: 'overview' }] } };
  const original = structuredClone(input);
  const captured = await withRecognitionDiagnostics(() => buildRecognitionAcquisitionEvidence({
    ...input, diagnostics: createRecognitionDiagnosticSession()
  }), { retainSourceText: true, origin: 'SYNTHETIC' });
  assert.deepEqual(input, original, `unchanged input ${index}`);
  assert.deepEqual(captured.value, oldAcquisition.buildRecognitionAcquisitionEvidence(input), `baseline acquisition ${index}`);
  assert.deepEqual(captured.value, buildRecognitionAcquisitionEvidence(input), `enabled/disabled acquisition ${index}`);
  assert.ok(captured.diagnostics.sessions[0].events.some(event => event.operation === 'extractRecognitionCandidateEvidence'));
}

const evidenceRows = [[0,0], [400,0], [400,400], [0,400]].map(([dx,dy], index) => {
  const x = 510000 + dx, y = 2100000 + dy, point = utmToWgs84(31, x, y, true);
  return { label: String(index + 1), x: String(x), y: String(y), referenceDms: { latitudeDecimal: point.lat, longitudeDecimal: point.lon } };
});
const evidence = { status: 'COMPLETE', rows: evidenceRows, rowCount: evidenceRows.length, axisOrder: 'easting_northing',
  crsEvidence: { status: 'EXPLICIT', id: 'EPSG:32631', projection: 'utm', zone: 31, hemisphere: 'N' },
  diagnostics: { headerPresent: true, parsedProjectedRowCount: evidenceRows.length, rejectedProjectedCandidateLineCount: 0 } };
for (const kind of ['complete', 'reference_conflict', 'missing_reference', 'missing_axis', 'missing_crs', 'invalid_value', 'wrong_order']) {
  const input = structuredClone(evidence);
  if (kind === 'reference_conflict') input.rows[1].referenceDms.latitudeDecimal += 0.00002;
  if (kind === 'missing_reference') delete input.rows[1].referenceDms;
  if (kind === 'missing_axis') input.axisOrder = null;
  if (kind === 'missing_crs') input.crsEvidence.status = 'UNCONFIRMED';
  if (kind === 'invalid_value') input.rows[1].x = '999999';
  if (kind === 'wrong_order') [input.rows[1], input.rows[2]] = [input.rows[2], input.rows[1]];
  const original = structuredClone(input);
  const captured = await withRecognitionDiagnostics(() => runtime.buildExplicitProjectedBoundaryAutoReleaseEngine({
    evidence: input, diagnostics: createRecognitionDiagnosticSession()
  }), { retainSourceText: true, origin: 'SYNTHETIC' });
  assert.deepEqual(input, original);
  assert.deepEqual(plain(captured.value), plain(runtime.buildExplicitProjectedBoundaryAutoReleaseEngine({ evidence: input })));
  assert.equal(captured.value !== null, kind === 'complete');
  const findings = diagnosticFindings(captured.diagnostics);
  if (kind === 'reference_conflict') {
    assert.ok(findings.some(f => f.pointLabel === '2' && f.field === 'referenceDms.latitudeDecimal' && f.originalCrs.id === 'EPSG:32631'));
    assert.ok(!findings.some(f => f.condition === 'SOURCE_CRS_SELECTION'));
  }
  if (kind === 'missing_axis') assert.ok(findings.some(f => f.field === 'axisOrder' && f.originalCrs.id === 'EPSG:32631'));
}

const raw = ['UTM WGS 1984 ZONE 31N', 'No | X | Y', '1 | 510000 | 2100000', '2 | 510400 | 2100000', '3 | 510400 | 2100400', '4 | 510000 | 2100400'].join('\n');
const full = await withRecognitionDiagnostics(() => {
  const observer = createRecognitionDiagnosticSession();
  function record(name, fn, args) { const result = fn(...args); observer.operation(name, args, result); return result; }
  const response = { choices: [{ message: { content: raw } }] };
  const extracted = record('extractProviderMessageText', runtime.extractProviderMessageText, [response]);
  record('extractProviderDmsReviewEvidence', runtime.extractProviderDmsReviewEvidence, [extracted, { axisEvidenceText: '' }]);
  const dms = record('normalizeProviderDmsReviewResult', normalizeProviderDmsReviewResult, [extracted]);
  const projected = record('extractProviderProjectedCoordinateEvidence', extractProviderProjectedCoordinateEvidence, [{ sourceText: extracted }]);
  const imageIdentity = { image_sha256: 'a'.repeat(64) };
  const sourceContextProvenance = { image_sha256: imageIdentity.image_sha256, same_request: true, mode: 'synthetic' };
  const bound = record('bindProjectedEvidenceToSourceContext', bindProjectedEvidenceToSourceContext, [{ providerEvidence: projected,
    sourceContextText: raw, imageIdentity, sourceContextProvenance }]);
  const multi = record('bindProviderRepresentationsToSource', bindProviderRepresentationsToSource, [{
    providerDmsReviewEvidence: dms, providerProjectedEvidence: bound, sourceContextText: raw, imageIdentity, sourceContextProvenance }]);
  const acquired = buildRecognitionAcquisitionEvidence({ rawText: raw, sourceBoundProjectedEvidence: bound, sourceBoundDmsEvidence: multi, diagnostics: observer });
  const decision = record('evaluateUnifiedRecognitionAcquisition', evaluateUnifiedRecognitionAcquisition, [{ evidence: acquired }]);
  record('evaluateUnifiedRecognitionFinalAuthorization', evaluateUnifiedRecognitionFinalAuthorization, [{ body: authorizationDiagnosticBody({}), evidence: acquired, decision }]);
  runtime.buildExplicitProjectedBoundaryAutoReleaseEngine({ evidence: bound, diagnostics: observer });
  return raw;
}, { retainSourceText: true, origin: 'SYNTHETIC', sourceDigest: productFingerprint().digest });
const bundle = makeReplayBundle(full.diagnostics);
const replay = replayDiagnosticBundle(bundle);
assert.equal(full.value, raw);
assert.equal(replay.status, 'MATCHED_CAPTURED_STAGES', JSON.stringify(replay));
assert.ok(replay.operations.length >= 9);
assert.ok(replay.operations.every(item => item.status === 'MATCH'));
assert.equal(replay.productionReproduction, 'NOT_ESTABLISHED');
assert.equal(replayDiagnosticBundle(null).status, 'UNKNOWN');
const drift = structuredClone(bundle); drift.source.digest = '0'.repeat(64);
assert.equal(replayDiagnosticBundle(drift).reason, 'SOURCE_VERSION_MISSING_OR_MISMATCH');
const tampered = structuredClone(bundle); tampered.diagnostics.sessions[0].events[0].data.result += '\nchanged';
assert.equal(replayDiagnosticBundle(tampered).status, 'DIFFERENT');
const executable = structuredClone(bundle); executable.diagnostics.sessions[0].events[0].operation = 'eval';
assert.throws(() => replayDiagnosticBundle(executable), /OPERATION_NOT_ALLOWLISTED/);

const defaultCapture = await withRecognitionDiagnostics(() => {
  const observer = createRecognitionDiagnosticSession(); observer.operation('normalizeProviderDmsReviewResult', [raw], { rawProviderText: raw });
});
assert.ok(!JSON.stringify(defaultCapture.diagnostics).includes('510000'));
assert.equal(defaultCapture.diagnostics.sessions[0].events[0].retention, 'PARTIAL');
assert.equal(createRecognitionDiagnosticSession().enabled, false);
const secretSentinel = 'DO_NOT_RETAIN_PRIVATE_SENTINEL';
const privacy = await withRecognitionDiagnostics(() => {
  const observer = createRecognitionDiagnosticSession();
  observer.event('privacy_test', { authorization: secretSentinel, account: secretSentinel, email: secretSentinel,
    headers: { authorization: secretSentinel }, rawText: `Bearer ${secretSentinel}` });
  observer.observe('broken_observer', () => { throw new Error(secretSentinel); });
  return 17;
}, { retainSourceText: true, origin: 'SYNTHETIC' });
assert.equal(privacy.value, 17);
assert.equal(privacy.diagnostics.sessions[0].truncated, true);
assert.ok(!JSON.stringify(privacy.diagnostics).includes(secretSentinel));
const unsafe = structuredClone(bundle); unsafe.diagnostics.sessions[0].events[0].data.password = secretSentinel;
assert.throws(() => replayDiagnosticBundle(unsafe), /UNSAFE_OR_OVERSIZED/);
const parallel = await Promise.all(['alpha', 'beta'].map(label => withRecognitionDiagnostics(async () => {
  const observer = createRecognitionDiagnosticSession(); await Promise.resolve(); observer.event('isolation', { label });
}, { origin: 'SYNTHETIC' })));
assert.equal(parallel[0].diagnostics.sessions[0].events[0].data.label, 'alpha');
assert.equal(parallel[1].diagnostics.sessions[0].events[0].data.label, 'beta');
console.log('Phase B diagnostics: 18 baseline safety + 10 acquisition + 7 projected cases, replay/privacy/isolation PASS; REAL_PROVIDER_CALLS=0');
