import { createAgenticGeometryArtifact } from '../agentic-coordinate-finalization/geometry.js';
import { buildAgenticKml } from '../agentic-coordinate-finalization/kml.js';
import { buildAgenticCoordinateUsageAuthority } from '../agentic-coordinate-usage-authority.js';
import { AGENT_STATE } from './constants.js';
import { assertStrictCoordinateAgentResult } from './schema.js';

function blockedError() {
  const error = new Error('Only a CONFIRMED coordinate agent result may create Map, KML, or usage authority');
  error.code = 'COORDINATE_AGENT_SAFETY_GATE_CLOSED';
  return error;
}

export function buildExistingSafetyArtifacts({
  agentResult,
  documentRevision,
  recognitionRequestId,
  name = 'Coordinate result',
}) {
  const result = assertStrictCoordinateAgentResult(agentResult);
  if (result.terminalState !== AGENT_STATE.CONFIRMED
    || !result.authorization.mapAllowed
    || !result.authorization.kmlAllowed
    || !result.coordinateResult) {
    throw blockedError();
  }
  const geometryArtifact = createAgenticGeometryArtifact({
    documentRevision,
    result: result.coordinateResult,
  });
  return Object.freeze({
    geometryArtifact,
    map: Object.freeze({
      type: 'Feature',
      properties: Object.freeze({
        documentRevision,
        geometryHash: geometryArtifact.geometryHash,
      }),
      geometry: geometryArtifact.geometry,
    }),
    kml: buildAgenticKml({ geometryArtifact, name }),
    usageAuthority: buildAgenticCoordinateUsageAuthority({
      recognitionRequestId,
      result: result.coordinateResult,
    }),
  });
}
