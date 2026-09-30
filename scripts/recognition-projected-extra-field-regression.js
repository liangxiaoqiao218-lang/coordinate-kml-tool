// Synthetic product-function regression only; no Provider or production I/O.
import './recognition-audit-offline-guard.cjs';
import assert from 'node:assert/strict';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { adaptRecognitionTableInput } from '../server/recognition/recognition-table-input.js';
import { getRecognitionAcquisitionIntegrityBlockReasons,
  buildRecognitionAcquisitionEvidence } from '../server/recognition/recognition-first-acquisition.js';

const root = fileURLToPath(new URL('../', import.meta.url));
const output = process.env.RECOGNITION_AUDIT_RECEIPT_ROOT || path.join(root, 'Temp/recognition-table-phase-d1-projected-extra-field');
mkdirSync(output, { recursive: true });
const receiptPath = path.join(output, 'extra-field-results.json');
if (existsSync(receiptPath)) throw new Error('EXISTING_RECEIPT_NO_RETRY');
const acquisition = { width: 800, height: 600, bytes: 2048, images: [{ role: 'overview' }] };
const cases = [];

function run(name, text, verify) {
  const adapter = adaptRecognitionTableInput({ rawText: text });
  const evidence = buildRecognitionAcquisitionEvidence({ rawText: text, acquisition });
  const reasons = getRecognitionAcquisitionIntegrityBlockReasons({ evidence });
  writeFileSync(path.join(output, `${name}.json`), JSON.stringify({
    origin: 'SYNTHETIC_NOT_PRODUCTION_REPLAY', text, adapter, evidence, reasons
  }, null, 2), { flag: 'wx' });
  try {
    verify({ adapter, evidence, reasons });
    cases.push({ name, status: 'PASS' });
  } catch (error) {
    cases.push({ name, status: 'FAIL', message: error.message, actual: error.actual, expected: error.expected });
    throw error;
  }
}

function assertCompleteRows({ adapter, evidence, reasons }, expected) {
  assert.deepEqual(reasons, []);
  assert.equal(evidence.candidateCoordinates.length, expected.length);
  assert.equal(adapter.evidence.rows.length, expected.length);
  expected.forEach((item, index) => {
    const candidate = evidence.candidateCoordinates[index];
    const row = adapter.evidence.rows[index];
    assert.equal(candidate.sourceText, item.raw); assert.equal(row.rawRow, item.raw);
    assert.equal(candidate.sourceLabel, item.label); assert.equal(row.pointLabel, item.label);
    assert.equal(candidate.x, item.x); assert.equal(candidate.y, item.y);
    assert.equal(row.fields.x.normalizedValue, item.x); assert.equal(row.fields.y.normalizedValue, item.y);
    assert.equal(row.selectedCandidate.sourceLineNumber, candidate.sourceLineNumber);
    assert.ok(row.sourceCells.length >= 2);
  });
}

try {
  run('valid_label_comma_pair', 'No | X | Y\n1 | 516203.1200,2304156.003\n2 | 516204.1201,2304156.004', state =>
    assertCompleteRows(state, [
      { label: '1', x: 516203.12, y: 2304156.003, raw: '1 | 516203.1200,2304156.003' },
      { label: '2', x: 516204.1201, y: 2304156.004, raw: '2 | 516204.1201,2304156.004' }
    ]));
  run('valid_label_x_y', 'No | X | Y\n1 | 516203.1200 | 2304156.003\n2 | 516204.1201 | 2304156.004', state =>
    assertCompleteRows(state, [
      { label: '1', x: 516203.12, y: 2304156.003, raw: '1 | 516203.1200 | 2304156.003' },
      { label: '2', x: 516204.1201, y: 2304156.004, raw: '2 | 516204.1201 | 2304156.004' }
    ]));
  run('valid_decimal_comma_and_zero', 'No | X | Y\n1 | 516203,1200 | 2304156,003\n2 | 0 | 0', state =>
    assertCompleteRows(state, [
      { label: '1', x: 516203.12, y: 2304156.003, raw: '1 | 516203,1200 | 2304156,003' },
      { label: '2', x: 0, y: 0, raw: '2 | 0 | 0' }
    ]));
  run('ambiguous_nested_pair_with_extra_cell', 'No | X | Y\n1 | 655000,1333600\n2 | 654500,1333600 | 9\n3 | 654500,1334100', ({ adapter, evidence, reasons }) => {
    assert.ok(reasons.includes('COORDINATE_ROW_NOT_FULLY_CONSUMED'));
    assert.equal(evidence.candidateCoordinates.length, 2);
    assert.deepEqual(evidence.candidateCoordinates.map(row => row.sourceLabel), ['1', '3']);
    const rejected = evidence.rejectedRows.find(row => row.lineNumber === 3);
    assert.equal(rejected.text, '2 | 654500,1333600 | 9');
    const row = adapter.evidence.rows.find(item => item.rawRow === rejected.text);
    assert.equal(row.selectedCandidate, null); assert.equal(row.rejectionReason, 'COORDINATE_ROW_NOT_FULLY_CONSUMED');
    assert.deepEqual(row.sourceCells.map(cell => cell.rawLiteral.trim()), ['2', '654500,1333600', '9']);
  });
  for (const [name, row] of [
    ['missing_field', '2 | 654500,'],
    ['unambiguous_extra_field', '2 | 654500 | 1333600 | 9']
  ]) run(name, `No | X | Y\n1 | 655000,1333600\n${row}\n3 | 654500,1334100`, ({ adapter, evidence, reasons }) => {
    assert.ok(reasons.includes('COORDINATE_ROW_NOT_FULLY_CONSUMED'));
    const rejected = evidence.rejectedRows.find(item => item.lineNumber === 3);
    assert.equal(rejected.text, row);
    const preserved = adapter.evidence.rows.find(item => item.rawRow === row);
    assert.equal(preserved.selectedCandidate, null);
    assert.ok(preserved.sourceCells.length >= 2);
  });
  console.log(`Projected extra-field ${cases.length}/${cases.length} PASS; REAL_PROVIDER_CALLS=0`);
} finally {
  writeFileSync(receiptPath, JSON.stringify({
    origin: 'SYNTHETIC_NOT_PRODUCTION_REPLAY', realProviderCalls: 0, cases
  }, null, 2), { flag: 'wx' });
}
