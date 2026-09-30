// Synthetic behavioral gate tests: actual product functions, no Provider I/O.
import './recognition-audit-offline-guard.cjs';
import assert from 'node:assert/strict';
import { mkdirSync, writeFileSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import * as finalizer from '../server/coordinate-finalizer/index.js';
import {
  buildRecognitionAcquisitionEvidence, evaluateUnifiedRecognitionAcquisition,
  evaluateUnifiedRecognitionFinalAuthorization, getRecognitionAcquisitionIntegrityBlockReasons
} from '../server/recognition/recognition-first-acquisition.js';

const root = fileURLToPath(new URL('../', import.meta.url));
const baseline = '335ff17e45b349fbbbdf36a3f5f2d59011b546d3';
const output = process.env.RECOGNITION_AUDIT_RECEIPT_ROOT
  || path.join(root, 'Temp/recognition-table-phase-d1-integrity-recovery');
mkdirSync(output, { recursive: true });
const resultPath = path.join(output, 'integrity-results.json');
if (existsSync(resultPath)) throw new Error('EXISTING_RECEIPT_NO_AUTOMATIC_RETRY');
const records = [];
let active = null;
function checkpoint(data) {
  writeFileSync(path.join(output, active + '.json'), JSON.stringify(data, null, 2), { flag: 'wx' });
}
function test(name, action) {
  active = name;
  try { action(); records.push({ name, status: 'PASS' }); }
  catch (error) {
    records.push({ name, status: 'FAIL', message: error.message, actual: error.actual,
      expected: error.expected, stack: error.stack }); throw error;
  }
}
const acquisition = { width: 800, height: 600, bytes: 2803, images: [{ role: 'overview' }] };
const rows = [
  { point: '1', latitude: '18° 1\' 1.000" N', longitude: '3° 1\' 1.000" E' },
  { point: '2', latitude: '18° 1\' 1.000" N', longitude: '3° 1\' 8.000" E' },
  { point: '3', latitude: '18° 1\' 8.000" N', longitude: '3° 1\' 8.000" E' },
  { point: '4', latitude: '18° 1\' 8.000" N', longitude: '3° 1\' 1.000" E' }
];
const json = values => JSON.stringify({ rows: values });
const table = ['No | Latitude | Longitude', ...rows.map(r => [r.point, r.latitude, r.longitude].join(' | '))].join('\n');
const markdown = table.split('\n').map((line, i) => '| ' + line + ' |' + (i === 0 ? '\n| --- | --- | --- |' : '')).join('\n');
const decimal = literal => {
  const m = literal.match(/^(\d+)°\s*(\d+)'\s*([\d.]+)"\s*([NSEW])$/);
  return (Number(m[1]) + Number(m[2]) / 60 + Number(m[3]) / 3600) * (/[SW]/.test(m[4]) ? -1 : 1);
};
const positions = rows.map(row => [decimal(row.longitude), decimal(row.latitude)]);
const geometry = { type: 'Polygon', coordinates: [[...positions, positions[0]]] };
function finalized() {
  // Valid synthetic downstream output deliberately presented to the final gate.
  // Invalid source evidence must veto it, without modifying the coordinates.
  return finalizer.finalizeCoordinateResult({
    resultId: 'synthetic-integrity-output', resultRevision: 7, currentRevision: 7, confirmedRevision: null,
    sourceAuthority: 'legacy', coordinateType: 'standard_dms_table', family: 'standard_dms_table',
    precisionMode: 'dms-coordinates', availabilityStatus: 'AVAILABLE',
    crs: finalizer.FINALIZED_COORDINATE_CRS, geometry, confirmationStatus: 'pending',
    qualityGateStatus: 'review_required', technicalKmlReady: true, currentAuthorizedGeometryExportable: true,
    requiresReview: true, kmlReady: false, groups: [{ groupId: 'group_1', requiresReview: true, kmlReady: false }]
  }, { clock: () => '2026-09-30T00:00:00.000Z' });
}
function inputFor(text, mutate = evidence => evidence) {
  const evidence = mutate(buildRecognitionAcquisitionEvidence({ rawText: text, acquisition, providerResponseId: 'synthetic-response' }));
  const decision = evaluateUnifiedRecognitionAcquisition({ evidence, contractStatus: 'REVIEW_REQUIRED', contractReason: 'GENERIC_REVIEW_ONLY' });
  return { body: { success: true, requiresReview: true, authorizationStatus: 'REVIEW_REQUIRED',
    resultStatus: 'needs_review', mapReady: true, kmlReady: true, usageConsumed: false, userUsageConsumed: false,
    finalizedCoordinateResult: finalized() }, evidence, decision,
    conformance: { status: 'REVIEW_REQUIRED', reason: 'GENERIC_REVIEW_ONLY' }, providerCallCount: 1 };
}
try {
  test('synthetic_incomplete_evidence_preserves_warning_and_exposes_unverified_output', () => {
    const input = inputFor(json(rows.filter((_, i) => i !== 1)));
    const after = evaluateUnifiedRecognitionFinalAuthorization(input);
    checkpoint({ input, after });
    assert.equal(input.decision.mayProceedToGeometryValidation, false);
    assert.deepEqual(getRecognitionAcquisitionIntegrityBlockReasons(input), ['SOURCE_LABELS_NONCONTIGUOUS']);
    assert.equal(after.mapReady, true);
    assert.equal(after.kmlReady, true);
    assert.equal(after.authorized, false);
    assert.equal(after.outputCapabilities.unverified, true);
    assert.ok(after.finalAuthorizationReasons.includes('SOURCE_LABELS_NONCONTIGUOUS'));
  });
  for (const [name, text] of [['json', json(rows)], ['plain', table], ['markdown', markdown]]) {
    test(name + '_complete_review_exposes_unverified_output', () => {
      const input = inputFor(text), beforeInput = JSON.stringify(input);
      const after = evaluateUnifiedRecognitionFinalAuthorization(input);
      checkpoint({ input, after });
      assert.deepEqual(getRecognitionAcquisitionIntegrityBlockReasons(input), []);
      assert.equal(input.decision.mapStatus, 'CLOSED', 'early closed status is not a final hard block');
      assert.equal(input.decision.mayProceedToGeometryValidation, true);
      assert.equal(after.mapReady, true); assert.equal(after.kmlReady, true);
      assert.equal(after.authorized, false);
      assert.equal(after.outputCapabilities.unverified, true);
      assert.equal(JSON.stringify(input), beforeInput, 'identity/version/geometry/source/usage cannot mutate');
    });
  }
  const negatives = [
    ['missing', json(rows.filter((_, i) => i !== 1)), 'SOURCE_LABELS_NONCONTIGUOUS'],
    ['duplicate', json(rows.map((r, i) => i === 1 ? { ...r, point: '1' } : r)), 'SOURCE_LABELS_DUPLICATE'],
    ['order', json([rows[0], rows[2], rows[1], rows[3]]), 'SOURCE_LABELS_NONCONTIGUOUS'],
    ['extra', json(rows.map((r, i) => i === 1 ? { ...r, extra: 9 } : r)), 'EXTRA_ROW_FIELD'],
    ['missing_field', json(rows.map((r, i) => i === 1 ? { point: r.point, latitude: r.latitude } : r)), 'MISSING_FIELD'],
    ['direction', json(rows).replace(' N', ' E'), 'FIELD_DIRECTION_CONFLICT']
  ];
  for (const [name, text, condition] of negatives) test(name + '_warning_does_not_hide_valid_output', () => {
    const input = inputFor(text), snapshot = JSON.stringify(input);
    const after = evaluateUnifiedRecognitionFinalAuthorization(input);
    checkpoint({ input, after, hardReasons: getRecognitionAcquisitionIntegrityBlockReasons(input) });
    assert.ok(input.evidence.reviewReasons.includes(condition));
    assert.equal(input.decision.mayProceedToGeometryValidation, false);
    assert.ok(after.finalAuthorizationReasons.includes(condition));
    assert.equal(after.mapReady, true); assert.equal(after.kmlReady, true); assert.equal(after.authorized, false);
    assert.equal(after.outputCapabilities.unverified, true);
    assert.equal(JSON.stringify(input), snapshot);
  });
  test('unbound_source_warning_does_not_hide_valid_output', () => {
    const input = inputFor(json(rows), e => ({ ...e, unboundCandidates: [e.candidateCoordinates[0]],
      reviewReasons: [...e.reviewReasons, 'COORDINATE_ROW_UNBOUND'] }));
    const after = evaluateUnifiedRecognitionFinalAuthorization(input);
    checkpoint({ input, after });
    assert.equal(after.mapReady, true); assert.equal(after.kmlReady, true);
    assert.equal(after.authorized, false);
    assert.ok(after.finalAuthorizationReasons.includes('COORDINATE_ROW_UNBOUND'));
  });
  test('generic_review_is_not_integrity_failure', () => {
    const input = inputFor(json(rows), e => ({ ...e, reviewReasons: ['CRS_EVIDENCE_MISSING'] }));
    const after = evaluateUnifiedRecognitionFinalAuthorization(input);
    checkpoint({ input, after });
    assert.deepEqual(getRecognitionAcquisitionIntegrityBlockReasons(input), []);
    assert.equal(after.mapReady, true); assert.equal(after.kmlReady, true); assert.equal(after.authorized, false);
  });
  test('invalid_current_geometry_still_blocks_outputs', () => {
    const input = inputFor(json(rows.filter((_, i) => i !== 1)));
    const invalid = finalizer.finalizeCoordinateResult({
      resultId: 'synthetic-invalid-output', resultRevision: 1, currentRevision: 1,
      sourceAuthority: 'legacy', coordinateType: 'standard_dms_table',
      crs: finalizer.FINALIZED_COORDINATE_CRS, geometry: null,
      confirmationStatus: 'pending', qualityGateStatus: 'review_required',
      technicalKmlReady: false, currentAuthorizedGeometryExportable: false,
      requiresReview: true, kmlReady: false
    });
    const after = evaluateUnifiedRecognitionFinalAuthorization({ ...input,
      body: { ...input.body, finalizedCoordinateResult: invalid } });
    checkpoint({ input, after });
    assert.equal(after.mapReady, false); assert.equal(after.kmlReady, false);
    assert.equal(after.outputCapabilities.technicallyGeneratable, false);
    assert.ok(after.outputCapabilities.blockReasons.includes('RESULT_IDENTITY_MISSING')
      || after.outputCapabilities.blockReasons.includes('GEOMETRY_INVALID'));
  });
  console.log('Integrity gate ' + records.length + '/' + records.length + ' PASS; REAL_PROVIDER_CALLS=0');
} finally {
  writeFileSync(resultPath, JSON.stringify({ baseline, origin: 'SYNTHETIC_NOT_PRODUCTION_REPLAY',
    realProviderCalls: 0, cases: records }, null, 2), { flag: 'wx' });
}

