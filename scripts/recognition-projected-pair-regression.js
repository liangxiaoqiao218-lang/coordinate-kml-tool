// Actual acquisition/adapter/final gate; synthetic evidence, no Provider I/O.
import './recognition-audit-offline-guard.cjs';
import assert from 'node:assert/strict';
import { readFileSync, mkdirSync, writeFileSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { adaptRecognitionTableInput } from '../server/recognition/recognition-table-input.js';
import { extractProviderProjectedCoordinateEvidence } from '../server/evidence-acquisition/local-ocr-map-layout-classifier.js';
import { buildRecognitionAcquisitionEvidence, evaluateUnifiedRecognitionAcquisition,
  getRecognitionAcquisitionIntegrityBlockReasons }
  from '../server/recognition/recognition-first-acquisition.js';

const root = fileURLToPath(new URL('../', import.meta.url));
const output = process.env.RECOGNITION_AUDIT_RECEIPT_ROOT || path.join(root, 'Temp/recognition-table-phase-d1-projected-recovery');
mkdirSync(output, {recursive:true});
const receiptPath = path.join(output, 'pair-results.json');
if (existsSync(receiptPath)) throw new Error('EXISTING_RECEIPT_NO_RETRY');
const history = JSON.parse(readFileSync(path.join(root, 'Temp/recognition-table-phase-d1-projected-diagnostic/captured-evidence.json')));
const events = history.diagnostics.sessions.flatMap(s => s.events);
const event = stage => events.find(e => e.stage === stage).data;
const source = event('candidate_input_selection').rawProviderText;
const expected = event('extractProviderProjectedCoordinateEvidence').result;
const acquisition = {width:800, height:600, bytes:2048, images:[{role:'overview'}]};
const cases = [];
function test(name, text, action, mutate = state => state) {
  const evidence = buildRecognitionAcquisitionEvidence({rawText:text, acquisition});
  const adapter = adaptRecognitionTableInput({rawText:text});
  const decision = evaluateUnifiedRecognitionAcquisition({evidence, contractStatus:'REVIEW_REQUIRED', contractReason:'GENERIC_REVIEW_ONLY'});
  // This function-level gate deliberately stops before geometry/finalizer.
  // Generic serialization cases do not borrow another fixture's CRS or shape.
  // Output capability, identity, revision and billing are asserted only by
  // the actual local HTTP handler regression.
  const state = mutate({evidence, adapter, decision});
  const before = JSON.stringify(state);
  const reasons = getRecognitionAcquisitionIntegrityBlockReasons(state);
  writeFileSync(path.join(output, 'pair-' + name + '.json'), JSON.stringify({origin:'SYNTHETIC_NOT_PRODUCTION_REPLAY', text,
    adapter:state.adapter, evidence:state.evidence, decision:state.decision, reasons,
    outputCapability:'NOT_EVALUATED_WITHOUT_MATCHING_GEOMETRY'}, null, 2), {flag:'wx'});
  try {
    action({...state, reasons});
    assert.equal(JSON.stringify(state), before, 'source evidence cannot mutate');
    cases.push({name,status:'PASS'});
  } catch (error) { cases.push({name,status:'FAIL',message:error.message,actual:error.actual,expected:error.expected,stack:error.stack}); throw error; }
}
function complete(expectedRows) {
  return ({evidence, adapter, reasons}) => {
    assert.deepEqual(reasons, []);
    assert.equal(evidence.candidateCoordinates.length, expectedRows.length);
    assert.equal(adapter.evidence.rows.length, expectedRows.length);
    assert.equal(adapter.evidence.rawResponse, evidence.rawProviderText);
    evidence.candidateCoordinates.forEach((c,i) => {
      const expected = expectedRows[i], row = adapter.evidence.rows[i];
      assert.equal(c.sourceLabel, expected.label); assert.equal(c.x, Number(expected.x)); assert.equal(c.y, Number(expected.y));
      assert.equal(c.sourceText, expected.raw); assert.equal(row.rawRow, expected.raw);
      assert.equal(row.pointLabel, c.sourceLabel); assert.ok(row.group);
      for (const field of ['x','y']) {
        assert.equal(row.fields[field].normalizedValue, c[field]);
        assert.equal(row.fields[field].source.lineNumber, c.sourceLineNumber);
        assert.equal(row.fields[field].source.candidateField,field);
      }
      assert.equal(row.physicalTableIdentity.status,'UNKNOWN');
    });
  };
}
const values = [
  {label:'1',x:'516203.1200',y:'2304156.003'}, {label:'2',x:'516204.1201',y:'2304156.004'},
  {label:'3',x:'516204.1200',y:'2304176.003'}, {label:'4',x:'516203.1200',y:'2304176.003'}
];
const lines = values.map(v => `${v.label} | ${v.x},${v.y}`);
const table = ['CRS | WGS 84 UTM zone 31N','No | X | Y',...lines].join('\n');
try {
  test('matrix_complete_mixed_delimiter', source, state => {
    complete(expected.rows.map(r => ({...r,raw:r.sourceText})))(state);
    assert.deepEqual(extractProviderProjectedCoordinateEvidence({sourceText:source,minimumRows:3}), expected,
      'existing projected parser semantics must be unchanged');
    assert.equal(state.evidence.normalizationStatus,'COMPLETED');
  });
  test('generic_dot_decimals_near_values', table, complete(values.map((v,i) => ({...v,raw:lines[i]}))));
  const markdownRows = values.map(v => `| ${v.label} | ${v.x} | ${v.y} |`);
  const markdown = ['CRS | WGS 84 UTM zone 31N','| No | X | Y |','| --- | --- | --- |',...markdownRows].join('\n');
  test('markdown', markdown, complete(values.map((v,i) => ({...v,raw:markdownRows[i]}))));
  const reversed = ['No | Y | X',...values.map(v => `${v.label} | ${v.y},${v.x}`)].join('\n');
  test('explicit_reverse_axis', reversed, state => {
    complete(values.map(v => ({...v,raw:`${v.label} | ${v.y},${v.x}`})))(state);
    assert.ok(state.evidence.candidateCoordinates.every(c => c.axisOrder === 'y_x'));
  });
  test('ordinary_decimal_comma_unchanged', 'No | X | Y\n1 | 516203,1200 | 2304156,003\n2 | 0 | 0', complete([
    {label:'1',x:'516203.1200',y:'2304156.003',raw:'1 | 516203,1200 | 2304156,003'},
    {label:'2',x:'0',y:'0',raw:'2 | 0 | 0'}]));
  const negatives = [
    ['missing_row',table.replace(lines[1]+'\n',''),'SOURCE_LABELS_NONCONTIGUOUS'],
    ['duplicate_label',table.replace(lines[1],lines[1].replace(/^2/,'1')),'SOURCE_LABELS_DUPLICATE'],
    ['order_conflict',table.replace(lines[0]+'\n'+lines[1],lines[1]+'\n'+lines[0]),'SOURCE_LABELS_NONCONTIGUOUS'],
    ['missing_field',table.replace(lines[1],'2 | 516204.1201,'),'COORDINATE_ROW_NOT_FULLY_CONSUMED'],
    ['extra_field',table.replace(lines[1],lines[1]+' | 9'),'COORDINATE_ROW_NOT_FULLY_CONSUMED'],
    ['extra_comma',table.replace(lines[1],lines[1]+',9'),'COORDINATE_ROW_NOT_FULLY_CONSUMED'],
    ['empty_cell',table.replace(lines[1],'2 | 516204.1201 | | 2304156.004'),'COORDINATE_ROW_NOT_FULLY_CONSUMED'],
    ['direction_conflict',table.replace(lines[1],'2 | 12° 3\' 4" E | 13° 4\' 5" E'),'COORDINATE_ROW_NOT_FULLY_CONSUMED']
  ];
  for (const [name,text,reason] of negatives) test(name,text,({reasons}) => {
    assert.ok(reasons.includes(reason), reason);
  });
  test('source_binding_conflict',table,({reasons}) => {
    assert.ok(reasons.includes('COORDINATE_ROW_UNBOUND'));
  }, state => ({...state,evidence:{...state.evidence,unboundCandidates:[state.evidence.candidateCoordinates[0]]}}));
  console.log(`Projected pair ${cases.length}/${cases.length} PASS; REAL_PROVIDER_CALLS=0`);
} finally {
  writeFileSync(receiptPath,JSON.stringify({origin:'SYNTHETIC_NOT_PRODUCTION_REPLAY',realProviderCalls:0,cases},null,2),{flag:'wx'});
}
