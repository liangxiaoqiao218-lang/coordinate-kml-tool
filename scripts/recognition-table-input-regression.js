import './recognition-audit-offline-guard.cjs';
import assert from 'node:assert/strict';
import { mkdirSync, writeFileSync, readFileSync } from 'node:fs';
import { createProductRuntime } from './recognition-architecture-probe.js';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { adaptRecognitionTableInput, TABLE_INPUT_VERSION } from '../server/recognition/recognition-table-input.js';
import { buildRecognitionAcquisitionEvidence as current, buildPreD1RecognitionAcquisitionEvidence as prior,
  evaluateUnifiedRecognitionAcquisition, evaluateUnifiedRecognitionFinalAuthorization } from '../server/recognition/recognition-first-acquisition.js';
import { normalizeProviderDmsReviewResult } from '../server/recognition/recognition-review-result.js';
import { extractProviderProjectedCoordinateEvidence } from '../server/evidence-acquisition/local-ocr-map-layout-classifier.js';
import { bindProjectedEvidenceToSourceContext } from '../server/recognition/projected-source-evidence.js';
import { bindProviderRepresentationsToSource } from '../server/recognition/multi-representation-source-evidence.js';
import { withRecognitionDiagnostics, createRecognitionDiagnosticSession } from '../server/recognition/recognition-diagnostics.js';
import { makeReplayBundle, productFingerprint } from './recognition-diagnostic-replay.js';
import { adaptDiagnosticBundle, compareShadow } from './coordinate-result-shadow.js';

const output = process.env.RECOGNITION_AUDIT_RECEIPT_ROOT
  ? pathToFileURL(path.join(process.env.RECOGNITION_AUDIT_RECEIPT_ROOT, 'table-input') + path.sep)
  : new URL('../Temp/recognition-table-phase-d1/', import.meta.url);
mkdirSync(output, { recursive: true });
const receipt = { origin: 'SYNTHETIC_NOT_PRODUCTION_REPLAY', baseline: '86ea4ac44a6d44a2332d7aaaffb1926504a73dca',
  realProviderCalls: 0, cases: [], differences: [] };
const dimensions = { width: 800, height: 600, bytes: 2048, images: [{ role: 'overview' }] };
function rows(count) {
  return Array.from({ length: count }, (_, i) => ({ point: String(i + 1), x: `${510000 + i * 13}.1200`, y: `${2100000 + i * 7}.003`,
    latitude: `18° 1' ${i + 1},123" N`, longitude: `3° 1' ${i + 1},456" E` }));
}
const sample = rows(16), dms = sample.map(({ point, latitude, longitude }) => ({ point, latitude, longitude }));
const xy = sample.map(({ point, x, y }) => ({ point, x, y }));
const plainTable = values => [Object.keys(values[0]).map(key => ({ point: 'No', latitude: 'Latitude', longitude: 'Longitude', x: 'X', y: 'Y' })[key]).join(' | '),
  ...values.map(row => Object.values(row).join(' | '))].join('\n');
const markdown = values => plainTable(values).split('\n').flatMap((line, i) => i === 0
  ? [`| ${line} |`, `| ${Object.keys(values[0]).map(() => '---').join(' | ')} |`] : [`| ${line} |`]).join('\n');
function changed(a, b, pointer = '', result = []) {
  if (Object.is(a, b)) return result;
  if (!a || !b || typeof a !== 'object' || typeof b !== 'object') { result.push({ pointer, before: a ?? null, after: b ?? null }); return result; }
  for (const key of new Set([...Object.keys(a), ...Object.keys(b)])) changed(a[key], b[key], `${pointer}/${key}`, result);
  return result;
}
async function test(name, fn) {
  try { await fn(); receipt.cases.push({ name, status: 'PASS' }); }
  catch (error) { receipt.cases.push({ name, status: 'FAIL', message: error.message }); throw error; }
}
function acquisition(text) { return current({ rawText: text, acquisition: dimensions }); }
function safety(evidence) {
  const decision = evaluateUnifiedRecognitionAcquisition({ evidence, contractStatus: 'CONFORMANT' });
  const final = evaluateUnifiedRecognitionFinalAuthorization({ evidence, decision, body: {} });
  assert.equal(final.authorized, false, 'candidate collection is not final authority');
  assert.equal(final.mapReady, false, 'no finalized geometry created by adapter');
  assert.equal(final.kmlReady, false);
  return decision;
}
try {
  for (const [name, values] of [['dms', dms], ['xy', xy], ['mixed', sample]]) {
    for (const [format, text] of [['plain', plainTable(values)], ['markdown', markdown(values)]]) await test(`${name}_${format}_unchanged`, () => {
      const input = { rawText: text, acquisition: dimensions }, before = structuredClone(input);
      assert.deepEqual(current(input), prior(input));
      assert.deepEqual(input, before);
      const adapted = adaptRecognitionTableInput({ rawText: text });
      assert.equal(adapted.evidence.schemaVersion, TABLE_INPUT_VERSION);
      assert.equal(adapted.evidence.rawResponse, text);
      assert.equal(adapted.evidence.rows.length, values.length);
      assert.equal(adapted.evidence.physicalTableIdentity.status, 'UNKNOWN');
      receipt.differences.push({ name: `${name}_${format}`, classification: 'EQUAL', fields: [] });
    });
    for (const [format, text] of [['compact', JSON.stringify({ rows: values })],
      ['verbose', 'Coordinates follow:\n```json\n' + JSON.stringify({ rows: values }, null, 2) + '\n```\nEnd of table.']]) await test(`${name}_json_${format}`, () => {
      const input = { rawText: text, acquisition: dimensions }, newer = current(input), older = prior(input);
      const adapted = adaptRecognitionTableInput({ rawText: text });
      assert.equal(older.candidateCoordinates.length, 0, 'retained entry still characterizes JSON gap');
      assert.equal(newer.candidateCoordinates.length, values.length);
      assert.deepEqual(newer.candidateCoordinates, prior({ rawText: plainTable(values) }).candidateCoordinates);
      assert.equal(newer.rawProviderText, text);
      assert.equal(newer.providerCompletionState, older.providerCompletionState);
      assert.equal(newer.providerResponseId, older.providerResponseId);
      assert.equal(newer.authority, 'EVIDENCE_ONLY');
      assert.deepEqual(newer.imageEvidence, older.imageEvidence);
      assert.equal(adapted.evidence.rows.length, values.length);
      adapted.evidence.rows.forEach((row, i) => {
        assert.equal(row.rowOrdinal, i + 1); assert.equal(row.pointLabel, values[i].point);
        assert.equal(row.physicalTableIdentity.status, 'UNKNOWN');
        for (const [role, expected] of Object.entries(values[i])) {
          const f = row.fields[role], span = f.source.characterSpan;
          assert.equal(f.decodedValue, expected);
          assert.equal(text.slice(span.start, span.end), f.rawLiteral);
          assert.equal(JSON.parse(f.rawLiteral), expected);
          assert.equal(f.source.pointer, `/rows/${i}/${role}`);
        }
      });
      safety(newer);
      receipt.differences.push({ name: `${name}_${format}`, classification: 'EXPECTED_SERIALIZATION_RECOVERY', fields: changed(older, newer),
        unresolved: name === 'xy' ? ['projected raw extractor still consumes original JSON; CRS/authorization not migrated'] : [] });
    });
  }
  for (const count of [3, 6, 9, 23]) await test(`generic_row_count_${count}`, () => {
    const text = JSON.stringify(rows(count).map(({ point, latitude, longitude }) => ({ point, latitude, longitude })));
    assert.equal(acquisition(text).candidateCoordinates.length, count);
  });
  await test('numeric_lexeme_not_reserialized', () => {
    const text = '{"rows":[{"point":1,"x":510000.1200,"y":2100000.0030}]}';
    const a = adaptRecognitionTableInput({ rawText: text });
    assert.equal(a.evidence.rows[0].fields.x.rawLiteral, '510000.1200');
    assert.equal(a.evidence.rows[0].fields.x.normalizedValue, 510000.12);
    assert.ok(a.evidence.normalizedText.includes('510000.1200 | 2100000.0030'));
  });
  const negatives = [
    ['missing_field', (() => { const a = structuredClone(dms); delete a[2].longitude; return JSON.stringify({ rows: a }); })(), 'MISSING_FIELD'],
    ['extra_field', JSON.stringify({ rows: dms.map((r, i) => i === 2 ? { ...r, auxiliary: '987' } : r) }), 'EXTRA_ROW_FIELD'],
    ['alias_conflict', JSON.stringify({ rows: dms.map((r, i) => i === 2 ? { ...r, lat: r.latitude.replace('4', '5') } : r) }), 'DUPLICATE_FIELD_ROLE'],
    ['duplicate_json_key', JSON.stringify({ rows: dms }).replace('"point":"1"', '"point":"1","point":"2"'), 'DUPLICATE_FIELD_ROLE'],
    ['direction_conflict', JSON.stringify({ rows: dms }).replace(' N', ' E'), 'FIELD_DIRECTION_CONFLICT'],
    ['invalid_dms', JSON.stringify({ rows: dms }).replace("1' 1,123", "60' 1,123"), 'DMS_VALUE_NOT_VALIDATED'],
    ['multiple_tables', JSON.stringify({ tables: [{ rows: dms }, { rows: dms }] }), 'TABLE_BOUNDARY_UNRESOLVED'],
    ['field_injection', JSON.stringify({ rows: dms.map((r, i) => i === 2 ? { ...r, latitude: r.latitude + '\n' } : r) }), 'UNSAFE_OR_NONSCALAR_FIELD'],
    ['field_order_conflict', JSON.stringify({ rows: dms.map((r, i) => i === 2 ? { point: r.point, longitude: r.longitude, latitude: r.latitude } : r) }), 'COLUMN_ORDER_CONFLICT'],
    ['coordinates_outside_json', plainTable(dms.slice(0, 1)) + '\n' + JSON.stringify({ rows: dms }), 'COORDINATES_OUTSIDE_JSON_TABLE']
  ];
  for (const [name, text, reason] of negatives) await test(name, () => {
    const adapted = adaptRecognitionTableInput({ rawText: text }), result = acquisition(text);
    assert.ok(adapted.evidence.issues.some(i => i.condition === reason), reason);
    assert.ok(result.rejectedRows.some(r => r.reason === reason));
    assert.equal(result.candidateCoordinates.length, 0, 'invalid JSON is not silently trimmed to a successful subset');
    assert.equal(safety(result).mayProceedToGeometryValidation, false);
  });
  for (const [name, values, reason] of [
    ['missing_row', dms.filter((_, i) => i !== 2), 'SOURCE_LABELS_NONCONTIGUOUS'],
    ['duplicate_label', [...dms, dms[0]], 'SOURCE_LABELS_DUPLICATE'],
    ['row_order_conflict', dms.slice().reverse(), 'SOURCE_LABELS_NONCONTIGUOUS']
  ]) await test(name, () => {
    const result = acquisition(JSON.stringify({ rows: values }));
    assert.deepEqual(result.candidateCoordinates.map(r => r.sourceLabel), values.map(r => r.point), 'never sort or fill rows');
    assert.ok(result.reviewReasons.includes(reason));
    assert.equal(safety(result).mayProceedToGeometryValidation, false);
  });
  await test('near_values_not_replaced', () => {
    const values = structuredClone(xy); values[3].x = '510039.1201';
    const adapted = adaptRecognitionTableInput({ rawText: JSON.stringify({ rows: values }) });
    assert.equal(adapted.candidates.candidateCoordinates[3].x, 510039.1201);
    assert.notEqual(adapted.candidates.candidateCoordinates[3].x, Number(xy[3].x));
    assert.equal(adapted.evidence.rows[3].fields.x.decodedValue, values[3].x);
  });
  const context = 'UTM WGS 1984 ZONE 31N\n' + plainTable(xy);
  const projected = extractProviderProjectedCoordinateEvidence({ sourceText: context, minimumRows: 1 });
  const imageIdentity = { image_sha256: 'a'.repeat(64) }, provenance = { image_sha256: 'a'.repeat(64), same_request: true };
  for (const [name, sourceContextText, sourceContextProvenance] of [
    ['same_image', context, provenance], ['crs_conflict', context.replace('31N', '32N'), provenance],
    ['axis_conflict', context.replace('No | X | Y', 'No | Y | X'), provenance],
    ['image_conflict', context, { ...provenance, image_sha256: 'b'.repeat(64) }]
  ]) await test(`binding_${name}_not_overridden`, () => {
    const bound = bindProjectedEvidenceToSourceContext({ providerEvidence: projected, sourceContextText, imageIdentity, sourceContextProvenance });
    const input = { rawText: context, sourceContextText, sourceBoundProjectedEvidence: bound, acquisition: dimensions };
    assert.deepEqual(current(input), prior(input));
    if (name !== 'same_image') assert.equal(bound.sourceContextBinding.bound, false);
  });
  await test('selected_source_priority_unchanged', () => {
    const bound = bindProjectedEvidenceToSourceContext({ providerEvidence: projected, sourceContextText: context, imageIdentity, sourceContextProvenance: provenance });
    const sourceBoundDmsEvidence = { status: 'COMPLETE', providerMode: 'DMS_ONLY', candidateDmsText: plainTable(dms) };
    for (const sourceBoundProjectedEvidence of [null, bound]) {
      const input = { rawText: JSON.stringify({ rows: dms }), sourceBoundDmsEvidence, sourceBoundProjectedEvidence };
      assert.deepEqual(current(input), prior(input));
    }
  });
  await test('missing_crs_not_invented', () => {
    const result = acquisition(JSON.stringify({ rows: xy }));
    assert.equal(result.visibleCrsEvidence.length, 0);
    assert.equal(safety(result).mayProceedToGeometryValidation, false);
  });
  await test('new_entry_phase_c_shadow_and_replay', async () => {
    const raw = JSON.stringify({ rows: dms }), fingerprint = productFingerprint();
    const runtime = createProductRuntime(readFileSync(new URL('../server.js', import.meta.url), 'utf8'));
    const record = await withRecognitionDiagnostics(() => {
      const observer = createRecognitionDiagnosticSession();
      const envelope = { choices: [{ message: { content: raw } }] };
      observer.operation('extractProviderMessageText', [envelope], runtime.extractProviderMessageText(envelope));
      // Synthetic raw evidence, not a historical image replay.
      observer.operation('normalizeProviderDmsReviewResult', [raw], normalizeProviderDmsReviewResult(raw));
      return current({ rawText: raw, acquisition: dimensions, diagnostics: observer });
    }, { retainSourceText: true, origin: 'SYNTHETIC', sourceDigest: fingerprint.digest });
    assert.deepEqual(record.value, acquisition(raw));
    const bundle = makeReplayBundle(record.diagnostics), adapted = adaptDiagnosticBundle(bundle), comparison = compareShadow(bundle, adapted);
    assert.equal(comparison.status, 'MATCHED_OBSERVATIONS');
    assert.ok(comparison.operations.every(o => o.status === 'MATCH'));
    assert.equal(comparison.unexplainedSafetyDifferences, 0);
    assert.equal(adapted.sessions[0].rowSets.find(s => s.representation === 'UNIFIED_CANDIDATES').rows.length, dms.length);
    const event = record.diagnostics.sessions[0].events.find(e => e.stage === 'table_input_adapter');
    assert.equal(event.data.rows[3].fields.latitude.decodedValue, dms[3].latitude);
    assert.equal(event.data.physicalTableIdentity.status, 'UNKNOWN');
    receipt.newEntryShadow = comparison;
  });
  await test('default_capture_withholds_new_raw_fields', async () => {
    const text = JSON.stringify({ rows: dms });
    const record = await withRecognitionDiagnostics(() => current({ rawText: text, diagnostics: createRecognitionDiagnosticSession() }));
    const event = record.diagnostics.sessions[0].events.find(e => e.stage === 'table_input_adapter');
    assert.equal(event.retention, 'PARTIAL');
    assert.equal(event.data.rawResponse.retention, 'WITHHELD');
    assert.equal(event.data.rows[0].fields.latitude.rawLiteral.retention, 'WITHHELD');
  });
  console.log(`D1 table input: ${receipt.cases.length}/${receipt.cases.length} PASS; REAL_PROVIDER_CALLS=0`);
} finally {
  writeFileSync(new URL('input-differences.json', output), JSON.stringify(receipt, null, 2), { flag: 'wx' });
}
