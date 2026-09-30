import './recognition-audit-offline-guard.cjs';
import assert from 'node:assert/strict';
import { readFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import path from 'node:path';
import { createProductRuntime } from './recognition-architecture-probe.js';
import { adaptDiagnosticBundle, compareShadow, CONTRACT_VERSION } from './coordinate-result-shadow.js';
import { makeReplayBundle, productFingerprint } from './recognition-diagnostic-replay.js';
import { withRecognitionDiagnostics, createRecognitionDiagnosticSession, authorizationDiagnosticBody } from '../server/recognition/recognition-diagnostics.js';
import { extractProviderProjectedCoordinateEvidence } from '../server/evidence-acquisition/local-ocr-map-layout-classifier.js';
import { normalizeProviderDmsReviewResult } from '../server/recognition/recognition-review-result.js';
import { bindProjectedEvidenceToSourceContext } from '../server/recognition/projected-source-evidence.js';
import { bindProviderRepresentationsToSource } from '../server/recognition/multi-representation-source-evidence.js';
import { buildRecognitionAcquisitionEvidence as currentAcquisition, buildPreD1RecognitionAcquisitionEvidence,
  evaluateUnifiedRecognitionAcquisition, evaluateUnifiedRecognitionFinalAuthorization } from '../server/recognition/recognition-first-acquisition.js';
import { utmToWgs84 } from '../server/projection/utm.js';

const root = new URL('../', import.meta.url), baseline = '86ea4ac44a6d44a2332d7aaaffb1926504a73dca';
// Preserve Phase C's exact historical characterization, including the JSON gap.
// The separate D1 regression verifies the new entry and enumerates its changes.
const historicalD1 = process.argv.includes('--d1-historical');
const buildRecognitionAcquisitionEvidence = historicalD1 ? buildPreD1RecognitionAcquisitionEvidence : currentAcquisition;
const runtime = createProductRuntime(readFileSync(new URL('server.js', root), 'utf8'));
const fingerprint = productFingerprint();
const plain = v => JSON.parse(JSON.stringify(v));
const oldFile = 'server/recognition/recognition-first-acquisition.js';
const oldText = execFileSync('git', ['show', `${baseline}:${oldFile}`], { encoding: 'utf8', windowsHide: true });
const oldUrl = new URL(oldFile, root);
const oldAcquisition = await import(`data:text/javascript,${encodeURIComponent(oldText.replace(/from\s+"([^"]+)"/g,
  (_, specifier) => `from ${JSON.stringify(specifier.startsWith('.') ? new URL(specifier, oldUrl).href : import.meta.resolve(specifier))}`))}`);
const sha = createHash('sha256').update('synthetic evidence, no image upload').digest('hex');
const imageIdentity = { image_sha256: sha };
const provenance = { image_sha256: sha, same_request: true, mode: 'synthetic' };
const cases = [], comparisons = [];
const output = process.env.RECOGNITION_AUDIT_RECEIPT_ROOT
  ? pathToFileURL(path.join(process.env.RECOGNITION_AUDIT_RECEIPT_ROOT, 'phase-c') + path.sep)
  : new URL(historicalD1 ? 'Temp/recognition-table-phase-d1/phase-c/' : 'Temp/recognition-result-phase-c/', root);
mkdirSync(output, { recursive: true });

function dms(value, latitude) {
  const magnitude = Math.abs(value), degrees = Math.floor(magnitude), minutes = Math.floor((magnitude - degrees) * 60);
  const seconds = ((magnitude - degrees) * 3600 - minutes * 60).toFixed(6);
  return `${degrees}° ${minutes}' ${seconds}" ${latitude ? value < 0 ? 'S' : 'N' : value < 0 ? 'W' : 'E'}`;
}
function syntheticTable(count, zone = 31, hemisphere = 'N') {
  const rows = Array.from({ length: count }, (_, i) => {
    const angle = 2 * Math.PI * i / count;
    const x = Number((510000 + Math.cos(angle) * 500).toFixed(3));
    const y = Number((2100000 + Math.sin(angle) * 500).toFixed(3));
    const point = utmToWgs84(zone, x, y, hemisphere === 'N');
    return { label: String(i + 1), x, y, latitude: dms(point.lat, true), longitude: dms(point.lon, false),
      referenceDms: { latitudeDecimal: point.lat, longitudeDecimal: point.lon } };
  });
  const header = `UTM WGS 1984 ZONE ${zone}${hemisphere}`;
  const xy = [header, 'No | X | Y', ...rows.map(r => `${r.label} | ${r.x} | ${r.y}`)].join('\n');
  const geographic = ['No | Latitude | Longitude', ...rows.map(r => `${r.label} | ${r.latitude} | ${r.longitude}`)].join('\n');
  const mixed = [header, 'No | X | Y | Latitude | Longitude', ...rows.map(r => `${r.label} | ${r.x} | ${r.y} | ${r.latitude} | ${r.longitude}`)].join('\n');
  return { rows, xy, geographic, mixed, header, zone, hemisphere };
}
const table = syntheticTable(16);
const dimensions = { width: 800, height: 600, bytes: 1024, images: [{ role: 'overview' }] };

async function capture({ text, context = table.mixed, contextProvenance = provenance, projectedOverride = null } = {}) {
  const captured = await withRecognitionDiagnostics(() => {
    const observer = createRecognitionDiagnosticSession();
    const record = (name, fn, args) => { const value = fn(...args); observer.operation(name, args, value); return value; };
    observer.event('image_identity', { ...imageIdentity, page: 1, regionId: null, tableId: null });
    observer.event('local_ocr', { sourceContextText: context, sourceContextProvenance: contextProvenance });
    const raw = record('extractProviderMessageText', runtime.extractProviderMessageText,
      [{ choices: [{ message: { content: text } }] }]);
    const grouped = record('normalizeProviderDmsReviewResult', normalizeProviderDmsReviewResult, [raw]);
    const projected = record('extractProviderProjectedCoordinateEvidence', extractProviderProjectedCoordinateEvidence, [{ sourceText: raw }]);
    const bound = record('bindProjectedEvidenceToSourceContext', bindProjectedEvidenceToSourceContext,
      [{ providerEvidence: projected, sourceContextText: context, imageIdentity, sourceContextProvenance: contextProvenance }]);
    record('bindProviderRepresentationsToSource', bindProviderRepresentationsToSource,
      [{ providerDmsReviewEvidence: grouped, providerProjectedEvidence: bound, sourceContextText: context, imageIdentity, sourceContextProvenance: contextProvenance }]);
    // Characterize the raw acquisition entry independently, not a fabricated full route.
    const input = { rawText: raw, acquisition: dimensions };
    const evidence = buildRecognitionAcquisitionEvidence({ ...input, diagnostics: observer });
    const decision = record('evaluateUnifiedRecognitionAcquisition', evaluateUnifiedRecognitionAcquisition, [{ evidence }]);
    const body = authorizationDiagnosticBody({});
    const final = record('evaluateUnifiedRecognitionFinalAuthorization', evaluateUnifiedRecognitionFinalAuthorization, [{ body, evidence, decision }]);
    runtime.buildExplicitProjectedBoundaryAutoReleaseEngine({ evidence: projectedOverride || bound, diagnostics: observer });
    return { input, evidence, final, projected, bound };
  }, { retainSourceText: true, origin: 'SYNTHETIC', sourceDigest: fingerprint.digest });
  return { ...captured, bundle: makeReplayBundle(captured.diagnostics) };
}
async function test(name, action) {
  await action(); cases.push({ name, status: 'PASS' });
}
function checkProjection(record, name) {
  const before = JSON.stringify(record.bundle);
  const adapted = adaptDiagnosticBundle(record.bundle), comparison = compareShadow(record.bundle, adapted);
  assert.equal(adapted.schemaVersion, CONTRACT_VERSION);
  assert.equal(comparison.status, 'MATCHED_OBSERVATIONS', `${name}: product replay and adapter`);
  assert.equal(comparison.unexplainedSafetyDifferences, 0);
  assert.equal(JSON.stringify(record.bundle), before, 'adapter/replay cannot mutate evidence');
  assert.ok(comparison.operations.length >= 7);
  assert.ok(comparison.operations.every(o => o.status === 'MATCH'));
  assert.deepEqual(record.value.evidence, oldAcquisition.buildRecognitionAcquisitionEvidence(record.value.input), 'exact baseline/current acquisition');
  assert.deepEqual(record.value.evidence, buildRecognitionAcquisitionEvidence(record.value.input), 'diagnostics off/current equivalence');
  const s = adapted.sessions[0];
  const rows = s.rowSets.find(r => r.representation === 'UNIFIED_CANDIDATES').rows;
  assert.equal(rows.length, record.value.evidence.candidateCoordinates.length);
  rows.forEach((row, i) => {
    for (const [key, value] of Object.entries(record.value.evidence.candidateCoordinates[i])) {
      assert.deepEqual(row.fields[key].value, value, `row ${i} ${key} preserved without repair`);
      assert.equal(row.fields[key].source.stage, 'extractRecognitionCandidateEvidence');
    }
    assert.equal(row.rawCharacterSpan.status, 'UNKNOWN');
  });
  assert.equal(s.identity.tableId.status, 'UNKNOWN');
  assert.equal(s.decision.usage.userTaskSuccess.status, 'UNKNOWN');
  assert.equal(s.decision.userReview.acknowledgement.status, 'UNKNOWN');
  assert.equal(s.decision.formalAuthorization.authorized.value, record.value.final.authorized);
  assert.deepEqual(s.crs.providerOriginal.value, plain(record.value.projected.crsEvidence ?? null));
  comparisons.push({ name, comparison });
  return adapted;
}

try {
  const dmsRows = table.geographic.split('\n').slice(1);
  const variants = [
    ['plain_dms', table.geographic], ['plain_xy', table.xy], ['mixed_xy_dms', table.mixed],
    ['markdown_mixed', [table.header, '| No | X | Y | Latitude | Longitude |', '| --- | --- | --- | --- | --- |',
      ...table.mixed.split('\n').slice(2).map(r => `| ${r} |`)].join('\n')],
    ['json_compact', JSON.stringify({ rows: table.rows.map(r => ({ point: r.label, latitude: r.latitude, longitude: r.longitude })) })],
    ['json_verbose', 'Coordinates follow:\n' + JSON.stringify({ rows: table.rows.map(r => ({ point: r.label, latitude: r.latitude, longitude: r.longitude })) }, null, 2) + '\nEnd of table.'],
    ['missing_row', ['No | Latitude | Longitude', ...dmsRows.filter((_, i) => i !== 2)].join('\n')],
    ['duplicate_label', ['No | Latitude | Longitude', ...dmsRows, dmsRows[0]].join('\n')],
    ['order_conflict', ['No | Latitude | Longitude', ...dmsRows.slice().reverse()].join('\n')],
    ['direction_conflict', table.geographic.replace(' N |', ' E |')],
    ['extra_field', ['No | Latitude | Longitude', `${dmsRows[0]} | 987`, ...dmsRows.slice(1)].join('\n')],
    ['xy_missing_crs', table.xy.split('\n').slice(1).join('\n')],
    ['nearly_equal_not_equal', table.mixed.replace(String(table.rows[3].x), String(table.rows[3].x + 0.25))],
    ['empty_response', '']
  ];
  let complete;
  for (const [name, text] of variants) await test(name, async () => {
    const record = await capture({ text, context: name === 'xy_missing_crs' ? 'No | X | Y' : table.mixed });
    const adapted = checkProjection(record, name);
    if (name === 'plain_dms') complete = record;
    if (['plain_dms', 'mixed_xy_dms', 'markdown_mixed'].includes(name)) assert.equal(record.value.evidence.candidateCoordinates.length, table.rows.length);
    if (['json_compact', 'json_verbose'].includes(name)) {
      // Existing raw candidate parser has no JSON entry. Keep the coverage gap
      // visible, not falsely treat grouped-parser success as acquisition success.
      assert.equal(record.value.evidence.candidateCoordinates.length, 0);
      assert.equal(adapted.sessions[0].rowSets.find(r => r.representation === 'DMS_GROUP').rows.length, table.rows.length);
      assert.ok(compareShadow(record.bundle).findings.some(f => f.condition === 'PARSER_COVERAGE_DIFFERS'));
    }
    if (name === 'missing_row') assert.ok(record.value.evidence.reviewReasons.includes('SOURCE_LABELS_NONCONTIGUOUS'));
    if (name === 'duplicate_label') assert.ok(record.value.evidence.reviewReasons.includes('SOURCE_LABELS_DUPLICATE'));
    if (name === 'order_conflict') assert.ok(record.value.evidence.reviewReasons.includes('SOURCE_LABELS_NONCONTIGUOUS'));
    if (['direction_conflict', 'extra_field'].includes(name)) assert.equal(record.value.evidence.rejectedRows.length, 1);
    if (name === 'empty_response') assert.equal(record.value.evidence.candidateCoordinates.length, 0);
    if (name === 'nearly_equal_not_equal') {
      const row = adapted.sessions[0].rowSets.find(s => s.representation === 'PROJECTED_EXTRACTED').rows[3];
      assert.equal(Number(row.fields.x.value), table.rows[3].x + 0.25, 'no replacement with source value');
    }
  });
  for (const [name, context, contextProvenance, expected] of [
    ['crs_conflict', table.mixed.replace('ZONE 31N', 'ZONE 32N'), provenance, 'CRS_CONFLICT'],
    ['axis_conflict', table.mixed.replace('No | X | Y', 'No | Y | X'), provenance, 'AXIS_CONFLICT'],
    ['image_conflict', table.mixed, { ...provenance, image_sha256: 'b'.repeat(64) }, 'IMAGE_IDENTITY_MISMATCH']
  ]) await test(name, async () => {
    const record = await capture({ text: table.xy, context, contextProvenance });
    const adapted = checkProjection(record, name), compared = compareShadow(record.bundle, adapted);
    assert.ok(compared.findings.some(f => f.condition === expected));
    assert.equal(record.value.bound.sourceContextBinding.bound, false);
    assert.equal(adapted.sessions[0].crs.providerOriginal.value.status, 'EXPLICIT', 'do not erase earlier CRS when binding conflicts');
  });
  for (const [count, zone, hemisphere] of [[6, 18, 'S'], [9, 47, 'N'], [16, 31, 'N']]) {
    const sample = syntheticTable(count, zone, hemisphere);
    for (const kind of ['complete', 'near_reference', 'missing_axis', 'missing_crs', 'order_conflict']) await test(`projected_${zone}${hemisphere}_${kind}`, async () => {
      const evidence = { status: 'COMPLETE', rows: sample.rows.map(r => ({ label: r.label, x: String(r.x), y: String(r.y), referenceDms: r.referenceDms })),
        rowCount: count, axisOrder: 'easting_northing', crsEvidence: { status: 'EXPLICIT', projection: 'utm', zone, hemisphere },
        diagnostics: { headerPresent: true, parsedProjectedRowCount: count, rejectedProjectedCandidateLineCount: 0 } };
      if (kind === 'near_reference') evidence.rows[2].referenceDms = { ...evidence.rows[2].referenceDms, latitudeDecimal: evidence.rows[2].referenceDms.latitudeDecimal + 0.00002 };
      if (kind === 'missing_axis') evidence.axisOrder = null;
      if (kind === 'missing_crs') evidence.crsEvidence.status = 'UNCONFIRMED';
      if (kind === 'order_conflict') [evidence.rows[1], evidence.rows[2]] = [evidence.rows[2], evidence.rows[1]];
      const record = await capture({ text: sample.mixed, context: sample.mixed, projectedOverride: evidence });
      const adapted = checkProjection(record, `projected_${zone}${hemisphere}_${kind}`);
      const engineEvent = record.diagnostics.sessions[0].events.find(e => e.operation === 'buildExplicitProjectedBoundaryAutoReleaseEngine');
      assert.equal(engineEvent.data.result !== null, kind === 'complete');
      if (kind === 'near_reference') assert.ok(adapted.sessions[0].findings.some(f => f.rowIndex === 2 && f.field === 'referenceDms.latitudeDecimal'));
      if (kind === 'complete') assert.equal(adapted.sessions[0].conversions[0].targetCrs.value, 'EPSG:4326');
    });
  }
  await test('separate_decisions_and_delivered_revision', async () => {
    const points = [[3,18], [3.01,18], [3.01,18.01], [3,18.01], [3,18]];
    const body = { finalizedCoordinateResult: { coordinateType: 'dms', requiresReview: true,
      decisionState: 'REVIEW_REQUIRED', qualityGateStatus: 'review_required', confirmationStatus: 'pending',
      sourceAuthority: 'ocr', geometry: { type: 'Polygon', coordinates: [points] },
      crs: { id: 'EPSG:4326', axisOrder: 'longitude_latitude' }, mapReady: true, kmlReady: true, technicalKmlReady: true } };
    // The authority must be a real product constant, not an invented success value.
    const { FINALIZED_COORDINATE_SOURCE_AUTHORITIES } = await import('../server/coordinate-finalizer/reason-codes.js');
    body.finalizedCoordinateResult.sourceAuthority = FINALIZED_COORDINATE_SOURCE_AUTHORITIES[0];
    const copy = structuredClone(complete.bundle);
    const events = copy.diagnostics.sessions[0].events;
    const prior = events.find(e => e.operation === 'evaluateUnifiedRecognitionFinalAuthorization');
    prior.data.args[0].body = authorizationDiagnosticBody(body);
    prior.data.result = plain(evaluateUnifiedRecognitionFinalAuthorization(prior.data.args[0]));
    assert.equal(prior.data.result.authorized, false);
    assert.equal(prior.data.result.mapReady, true);
    assert.equal(prior.data.result.kmlReady, true);
    events.push({ sequence: events.length + 1, stage: 'delivered_result', operation: null, retention: 'COMPLETE', withheld: [], data: {
      resultId: 'synthetic-result', resultRevision: 1, geometryHash: 'synthetic-geometry',
      mapReady: true, kmlReady: true, userUsageConsumed: true, finalCrs: body.finalizedCoordinateResult.crs } });
    const adapted = adaptDiagnosticBundle(copy), decision = adapted.sessions[0].decision;
    assert.equal(decision.formalAuthorization.authorized.value, false);
    assert.equal(decision.userReview.confirmationStatus.value, 'pending');
    assert.equal(decision.usage.userUsageConsumed.value, true);
    assert.equal(decision.usage.userTaskSuccess.status, 'UNKNOWN');
    assert.equal(compareShadow(copy).status, 'MATCHED_OBSERVATIONS');
    events.push({ ...structuredClone(events.at(-1)), sequence: events.length + 1, data: { ...events.at(-1).data, resultId: 'different-result', resultRevision: 2 } });
    assert.ok(compareShadow(copy).findings.some(f => f.condition === 'MULTIPLE_DELIVERY_IDENTITIES'));
    assert.equal(adaptDiagnosticBundle(copy).sessions[0].stageRecords.filter(e => e.stage === 'delivered_result').length, 2);
    events.at(-1).data.mapReady = false;
    assert.equal(compareShadow(copy).status, 'DIFFERENT', 'inconsistent delivery is not a pass');
  });
  await test('adversarial_adapter_value_and_authority_changes', () => {
    const adapted = adaptDiagnosticBundle(complete.bundle);
    adapted.sessions[0].rowSets.find(r => r.representation === 'UNIFIED_CANDIDATES').rows[0].fields.latitudeSource.value += ' changed';
    const compared = compareShadow(complete.bundle, adapted);
    assert.equal(compared.status, 'DIFFERENT');
    assert.ok(compared.differences.some(d => d.field.includes('/fields/latitudeSource/value')));
    const authority = adaptDiagnosticBundle(complete.bundle);
    authority.sessions[0].decision.formalAuthorization.authorized.value = true;
    assert.equal(compareShadow(complete.bundle, authority).status, 'DIFFERENT');
  });
  await test('unknown_history_partial_and_source_drift', () => {
    assert.equal(compareShadow(null).status, 'UNKNOWN');
    const drift = structuredClone(complete.bundle); drift.source.digest = '0'.repeat(64);
    assert.equal(compareShadow(drift).status, 'UNKNOWN');
    const partial = structuredClone(complete.bundle); partial.diagnostics.sessions[0].events[0].retention = 'PARTIAL';
    assert.equal(compareShadow(partial).status, 'PARTIAL');
    assert.equal(adaptDiagnosticBundle(partial).sessions[0].identity.image_sha256.status, 'UNKNOWN');
    const tampered = structuredClone(complete.bundle);
    tampered.diagnostics.sessions[0].events.find(e => e.operation === 'extractRecognitionCandidateEvidence').data.result.candidateCoordinates[0].latitudeSource += ' changed';
    assert.equal(compareShadow(tampered).status, 'DIFFERENT', 'actual product replay detects changed evidence');
  });
  await test('privacy_and_multiple_sessions_do_not_merge', () => {
    const sensitive = structuredClone(complete.bundle);
    sensitive.diagnostics.sessions[0].events[0].data.authorization = 'private-sentinel';
    assert.throws(() => adaptDiagnosticBundle(sensitive), /UNSAFE_OR_OVERSIZED/);
    const two = structuredClone(complete.bundle);
    two.diagnostics.sessions.push({ ...structuredClone(two.diagnostics.sessions[0]), sequence: 2 });
    const adapted = adaptDiagnosticBundle(two);
    assert.equal(adapted.sessions.length, 2);
    assert.equal(adapted.sessions[1].rowSets[0].observation.sessionIndex, 1);
    assert.equal(adapted.sessions[1].identity.tableId.status, 'UNKNOWN');
  });
  console.log(`Phase C shadow: ${cases.length}/${cases.length} PASS; product replay + lossless field projection; REAL_PROVIDER_CALLS=0`);
} finally {
  writeFileSync(new URL('comparison-results.json', output), JSON.stringify({ baseline, sourceDigest: fingerprint.digest,
    origin: 'SYNTHETIC', scope: 'PRODUCT_STAGE_CALLS_NOT_FULL_HTTP_OR_PRODUCTION_REPLAY',
    realProviderCalls: 0, cases, comparisons }, null, 2), { flag: 'wx', mode: 0o600 });
}
