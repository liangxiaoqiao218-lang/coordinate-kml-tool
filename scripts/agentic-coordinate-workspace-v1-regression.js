import assert from 'node:assert/strict';

import {
  acceptAgenticCoordinateRecognition,
  beginAgenticCoordinateUpload,
  bindAgenticCoordinateOutput,
  createAgenticCoordinateWorkspace,
  editAgenticCoordinateText,
  failAgenticCoordinateRecognition,
  getAgenticCoordinateDocument,
} from '../assets/agentic-coordinate/agentic-coordinate-workspace.js';

const recognized = {
  contractVersion: 'agentic-coordinate-recognition/v1',
  resultStatus: 'usable',
  displayText: '11°28\'37.26"N, 08°40\'42.13"W\n11°28\'31.60"N, 08°40\'32.90"W',
  groups: [],
};

let workspace = createAgenticCoordinateWorkspace();
assert.equal(workspace.phase, 'idle');

workspace = beginAgenticCoordinateUpload(workspace, { fileName: 'first.jpg' });
assert.equal(workspace.phase, 'recognizing');
assert.equal(workspace.currentText, '');
assert.equal(workspace.recognitionResult, null);
assert.deepEqual(workspace.derived, { map: null, kml: null });

workspace = acceptAgenticCoordinateRecognition(workspace, recognized);
assert.equal(workspace.phase, 'usable');
assert.equal(workspace.sourceText, recognized.displayText);
assert.equal(workspace.currentText, recognized.displayText);

const firstRevision = workspace.documentRevision;
workspace = bindAgenticCoordinateOutput(workspace, {
  documentRevision: firstRevision,
  map: { geometry: 'first-map' },
  kml: '<kml>first</kml>',
});
assert.equal(workspace.derived.map.documentRevision, firstRevision);
assert.equal(workspace.derived.kml.documentRevision, firstRevision);

const editedText = recognized.displayText.replace('37.26', '31.26');
workspace = editAgenticCoordinateText(workspace, editedText);
assert.equal(workspace.phase, 'edited');
assert.equal(workspace.currentText, editedText);
assert.equal(workspace.sourceText, recognized.displayText);
assert.equal(workspace.documentRevision, firstRevision + 1);
assert.deepEqual(workspace.derived, { map: null, kml: null });

assert.throws(
  () => bindAgenticCoordinateOutput(workspace, {
    documentRevision: firstRevision,
    map: { geometry: 'stale-map' },
  }),
  (error) => error.code === 'STALE_AGENTIC_COORDINATE_REVISION',
);

const editedRevision = workspace.documentRevision;
workspace = bindAgenticCoordinateOutput(workspace, {
  documentRevision: editedRevision,
  map: { geometry: 'edited-map' },
});
assert.equal(workspace.derived.map.value.geometry, 'edited-map');

const unchangedWorkspace = editAgenticCoordinateText(workspace, editedText);
assert.equal(unchangedWorkspace, workspace);

workspace = beginAgenticCoordinateUpload(workspace, { fileName: 'second.jpg' });
assert.equal(workspace.fileName, 'second.jpg');
assert.equal(workspace.phase, 'recognizing');
assert.equal(workspace.currentText, '');
assert.equal(workspace.sourceText, '');
assert.equal(workspace.recognitionResult, null);
assert.deepEqual(workspace.derived, { map: null, kml: null });

workspace = failAgenticCoordinateRecognition(workspace, {
  code: 'NO_COORDINATES_FOUND',
  message: 'No reliable coordinates found',
});
assert.equal(workspace.phase, 'failed');
assert.equal(workspace.currentText, '');
assert.equal(workspace.recognitionResult, null);
assert.equal(workspace.failure.code, 'NO_COORDINATES_FOUND');

assert.throws(
  () => bindAgenticCoordinateOutput(workspace, {
    documentRevision: workspace.documentRevision,
    kml: '<kml>must-not-exist</kml>',
  }),
  /empty coordinate document/,
);

const document = getAgenticCoordinateDocument(workspace);
assert.deepEqual(document, {
  documentRevision: workspace.documentRevision,
  text: '',
});

console.log('agentic coordinate workspace v1 regression: PASS');
