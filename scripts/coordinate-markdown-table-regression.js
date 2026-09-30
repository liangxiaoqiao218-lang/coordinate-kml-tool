import assert from 'node:assert/strict';
import { extractRecognitionCandidateEvidence } from '../server/recognition/recognition-candidate-evidence.js';
import { buildRecognitionAcquisitionEvidence } from '../server/recognition/recognition-first-acquisition.js';
import { readFileSync } from 'node:fs';

const header = 'No | X | Y | Latitude | Longitude';
for (const count of [4, 16]) {
  const rows = Array.from({ length: count }, (_, i) => `${i + 1} | ${500000 + i * 10}.123 | ${9700000 + i * 10}.456 | 2° 42\' ${i},123" S | 117° 0\' ${i},456" E`);
  const plain = [header, ...rows].join('\n');
  const markdown = [`| ${header} |`, '| --- | ---: | :--- | --- | --- |', ...rows.map(row => `| ${row} |`)].join('\n');
  const expected = extractRecognitionCandidateEvidence({ rawText: plain });
  const actual = extractRecognitionCandidateEvidence({ rawText: markdown });
  assert.equal(actual.candidateCoordinates.length, count);
  assert.equal(actual.rejectedRows.length, 0);
  assert.deepEqual(actual.candidateCoordinates.map(row => [row.sourceLabel, row.latitudeSource, row.longitudeSource]), expected.candidateCoordinates.map(row => [row.sourceLabel, row.latitudeSource, row.longitudeSource]));
  assert.equal(actual.candidateCoordinates[0].sourceText, `| ${rows[0]} |`);
  assert.equal(buildRecognitionAcquisitionEvidence({ rawText: markdown }).acquisitionStatus, 'COMPLETED');
  const extra = extractRecognitionCandidateEvidence({ rawText: markdown.replace(`| ${rows[0]} |`, `| ${rows[0]} | 999 |`) });
  assert.equal(extra.rejectedRows.length, 1);
  const reversed = extractRecognitionCandidateEvidence({ rawText: [`| ${header} |`, ...rows.slice().reverse().map(row => `| ${row} |`)].join('\n') });
  assert.equal(reversed.candidateCoordinateGroups[0].sourceLabelsContinuous, false);
  const missing = extractRecognitionCandidateEvidence({ rawText: [header, ...rows.filter((_, i) => i !== 1)].join('\n') });
  assert.ok(missing.reviewReasons.includes('SOURCE_LABELS_NONCONTIGUOUS'));
  const duplicate = extractRecognitionCandidateEvidence({ rawText: [header, ...rows, rows[0]].join('\n') });
  assert.ok(duplicate.reviewReasons.includes('SOURCE_LABELS_DUPLICATE'));
  const direction = extractRecognitionCandidateEvidence({ rawText: markdown.replace(' S |', ' E |') });
  assert.equal(direction.rejectedRows.length, 1);
}

// Exercise the actual UI status writer in a minimal DOM across transitions.
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const start = html.indexOf('    function setRecognitionStatus(');
const end = html.indexOf('    function setCoordinateOrderReviewVisible(', start);
const panel = { style: { display: 'none' } };
const support = { style: { display: 'none' } };
const progress = { style: {}, replaceChildren() {}, appendChild() {} };
const setStatus = Function('handwrittenDmsReviewPanel', 'uploadMessage', 'recognitionProgress', 'document', `
  const recognitionProgressHideTimer = null;
  const copyContent = () => {};
  ${html.slice(start, end)}
  return setRecognitionStatus;
`)(panel, support, progress, { createElement: () => ({ addEventListener() {} }) });
setStatus('识别完成', 'success');
assert.equal(progress.style.display, 'flex');
panel.style.display = 'flex';
setStatus('请结合原图核对', 'warning');
assert.equal(progress.style.display, 'none');
setStatus('已记录本次核对', 'warning');
assert.equal(progress.style.display, 'none');
panel.style.display = 'none';
support.style.display = 'block';
setStatus('识别失败', 'error');
assert.equal(progress.style.display, 'none');
support.style.display = 'none';
setStatus('缺少坐标证据', 'error');
assert.equal(progress.style.display, 'flex');
console.log('coordinate Markdown table regression: PASS; REAL_PROVIDER_CALLS=0');
