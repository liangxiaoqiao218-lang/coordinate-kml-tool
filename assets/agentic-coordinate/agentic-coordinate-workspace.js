export const AGENTIC_COORDINATE_WORKSPACE_VERSION = 'agentic-coordinate-workspace/v1';

const EMPTY_DERIVED_OUTPUTS = Object.freeze({
  map: null,
  kml: null,
});

function normalizeText(value) {
  return typeof value === 'string' ? value.replace(/\r\n?/g, '\n') : '';
}

function freezeWorkspace(workspace) {
  return Object.freeze({
    ...workspace,
    derived: Object.freeze({ ...workspace.derived }),
  });
}

function nextDocumentRevision(workspace) {
  return workspace.documentRevision + 1;
}

export function createAgenticCoordinateWorkspace() {
  return freezeWorkspace({
    version: AGENTIC_COORDINATE_WORKSPACE_VERSION,
    phase: 'idle',
    uploadRevision: 0,
    documentRevision: 0,
    fileName: '',
    sourceText: '',
    currentText: '',
    recognitionResult: null,
    failure: null,
    derived: EMPTY_DERIVED_OUTPUTS,
  });
}

export function beginAgenticCoordinateUpload(workspace, { fileName = '' } = {}) {
  return freezeWorkspace({
    version: AGENTIC_COORDINATE_WORKSPACE_VERSION,
    phase: 'recognizing',
    uploadRevision: workspace.uploadRevision + 1,
    documentRevision: nextDocumentRevision(workspace),
    fileName: String(fileName),
    sourceText: '',
    currentText: '',
    recognitionResult: null,
    failure: null,
    derived: EMPTY_DERIVED_OUTPUTS,
  });
}

export function acceptAgenticCoordinateRecognition(workspace, recognitionResult) {
  if (!recognitionResult || typeof recognitionResult !== 'object') {
    throw new TypeError('recognitionResult must be an object');
  }

  if (recognitionResult.contractVersion !== 'agentic-coordinate-recognition/v1') {
    throw new Error('recognitionResult uses an unsupported contract version');
  }

  if (!['usable', 'needs_review'].includes(recognitionResult.resultStatus)) {
    throw new Error('Only usable or needs_review recognition results can enter the workspace');
  }

  const displayText = normalizeText(recognitionResult.displayText);
  if (!displayText.trim()) {
    throw new Error('A recognized coordinate document must include displayText');
  }

  return freezeWorkspace({
    ...workspace,
    phase: recognitionResult.resultStatus,
    documentRevision: nextDocumentRevision(workspace),
    sourceText: displayText,
    currentText: displayText,
    recognitionResult,
    failure: null,
    derived: EMPTY_DERIVED_OUTPUTS,
  });
}

export function failAgenticCoordinateRecognition(workspace, failure = {}) {
  return freezeWorkspace({
    ...workspace,
    phase: 'failed',
    documentRevision: nextDocumentRevision(workspace),
    sourceText: '',
    currentText: '',
    recognitionResult: null,
    failure: Object.freeze({
      code: String(failure.code || 'RECOGNITION_FAILED'),
      message: String(failure.message || 'Coordinate recognition failed'),
    }),
    derived: EMPTY_DERIVED_OUTPUTS,
  });
}

export function editAgenticCoordinateText(workspace, text) {
  const currentText = normalizeText(text);
  if (currentText === workspace.currentText) {
    return workspace;
  }

  return freezeWorkspace({
    ...workspace,
    phase: currentText.trim() ? 'edited' : 'empty',
    documentRevision: nextDocumentRevision(workspace),
    currentText,
    failure: null,
    derived: EMPTY_DERIVED_OUTPUTS,
  });
}

export function bindAgenticCoordinateOutput(workspace, {
  documentRevision,
  map = undefined,
  kml = undefined,
} = {}) {
  if (documentRevision !== workspace.documentRevision) {
    const error = new Error('Derived output belongs to a stale coordinate document revision');
    error.code = 'STALE_AGENTIC_COORDINATE_REVISION';
    throw error;
  }

  if (!workspace.currentText.trim()) {
    throw new Error('Cannot bind map or KML output to an empty coordinate document');
  }

  return freezeWorkspace({
    ...workspace,
    derived: {
      map: map === undefined ? workspace.derived.map : Object.freeze({
        documentRevision,
        value: map,
      }),
      kml: kml === undefined ? workspace.derived.kml : Object.freeze({
        documentRevision,
        value: kml,
      }),
    },
  });
}

export function getAgenticCoordinateDocument(workspace) {
  return Object.freeze({
    documentRevision: workspace.documentRevision,
    text: workspace.currentText,
  });
}
