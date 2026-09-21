import { createAgenticGeometryArtifact } from './geometry.js';
import { buildAgenticKml } from './kml.js';
import { runAgenticCoordinateFinalization } from './service.js';
import { transformAgenticCoordinateResult } from './projection.js';
import { assertAgenticCoordinateConsistency } from '../agentic-coordinate-recognition/consistency.js';

function normalizeText(value) {
  return String(value || '').replace(/\r\n?/g, '\n');
}

export async function finalizeAgenticCoordinateDocument({
  documentRevision,
  currentText,
  sourceText,
  recognitionResult,
  name = 'Coordinate result',
  modelName,
  providerCall,
}) {
  const current = normalizeText(currentText);
  if (!current.trim()) throw new Error('currentText must be a non-empty string');

  const canReuseRecognition = recognitionResult
    && normalizeText(sourceText) === current;

  const finalized = canReuseRecognition
    ? {
      documentRevision,
      result: recognitionResult,
      execution: Object.freeze({
        providerCallCount: 0,
        retryCount: 0,
        fallbackCount: 0,
        source: 'initial_recognition',
      }),
    }
    : await runAgenticCoordinateFinalization({
      currentText: current,
      recognitionContext: recognitionResult,
      documentRevision,
      modelName,
      providerCall,
    });

  const consistentResult = assertAgenticCoordinateConsistency(finalized.result);
  const spatialResult = transformAgenticCoordinateResult(consistentResult);
  const geometryArtifact = createAgenticGeometryArtifact({
    documentRevision,
    result: spatialResult,
  });
  const feature = Object.freeze({
    type: 'Feature',
    properties: Object.freeze({
      documentRevision,
      geometryHash: geometryArtifact.geometryHash,
      reviewRequired: geometryArtifact.reviewRequired,
    }),
    geometry: geometryArtifact.geometry,
  });
  const kml = buildAgenticKml({ geometryArtifact, name });

  return Object.freeze({
    documentRevision,
    result: finalized.result,
    spatialResult,
    geometryArtifact,
    map: Object.freeze({
      documentRevision,
      geometryHash: geometryArtifact.geometryHash,
      feature,
    }),
    kml: Object.freeze({
      documentRevision,
      geometryHash: geometryArtifact.geometryHash,
      content: kml,
    }),
    execution: finalized.execution,
  });
}
