import assert from 'node:assert/strict';

import {
  COORDINATE_AGENT_TURN_SCHEMA,
  CoordinateAgentToolRegistry,
  CoordinateIntelligenceAgentKernel,
  MockCoordinateAgentProviderAdapter,
  assertStrictCoordinateAgentTurn,
} from '../server/coordinate-agent/index.js';

const geometryTypes = COORDINATE_AGENT_TURN_SCHEMA
  .properties.candidate.anyOf[0].properties.geometryType.enum;
assert.deepEqual(geometryTypes, [
  'Point', 'MultiPoint', 'LineString', 'Polygon', 'MultiPolygon', 'Grid', 'Unknown',
]);

const baseTurn = {
  observation: {
    summary: 'Generic whole-image observation',
    orientationDegrees: 0,
    regions: [{
      id: 'whole-1', kind: 'document', bbox: [0, 0, 1, 1], sourceText: '',
      semanticRole: 'whole document', confidence: 0.9, provenance: 'offline_replay',
    }],
  },
  plan: { rationale: 'No tool action needed for this contract test', actions: [] },
  candidate: {
    success: true,
    resultStatus: 'needs_review',
    displayText: 'P1 | source row',
    coordinateSystem: { kind: 'geographic', name: null, epsg: null, status: 'needs_confirmation' },
    geometryType: 'Unknown',
    groups: [{ name: null, points: [{
      label: 'P1', sourceText: 'P1 | source row', x: null, y: null,
      latitude: 10, longitude: 20, needsReview: true,
    }] }],
    warnings: ['Geometry intent requires review'],
  },
  uncertainties: [{
    code: 'GEOMETRY_INTENT_UNCERTAIN', message: 'Geometry intent is not explicit',
    evidenceRegionIds: ['whole-1'], blocking: true,
  }],
  reviewItems: [{
    fieldPath: 'geometryType', question: 'Confirm the visible geometry intent',
    evidenceRegionIds: ['whole-1'], candidates: [],
  }],
};
assert.doesNotThrow(() => assertStrictCoordinateAgentTurn(baseTurn));
assert.doesNotThrow(() => assertStrictCoordinateAgentTurn({ ...baseTurn, candidate: null }));

const invalidGeometryTurn = structuredClone(baseTurn);
invalidGeometryTurn.candidate.geometryType = 'MultiLineString';
assert.throws(
  () => assertStrictCoordinateAgentTurn(invalidGeometryTurn),
  /candidate\.geometryType is not an allowed value/,
);

const invalidEvidenceTurn = structuredClone(baseTurn);
invalidEvidenceTurn.observation.regions[0].kind = 'image';
const failedClosed = await new CoordinateIntelligenceAgentKernel({
  providerAdapter: new MockCoordinateAgentProviderAdapter([invalidEvidenceTurn]),
  toolRegistry: new CoordinateAgentToolRegistry(),
  maxProviderCalls: 1,
  maxIterations: 1,
}).run({ imageRef: 'local-image://offline-contract', requestId: 'offline:phase04c' });
assert.equal(failedClosed.terminalState, 'FAILED_CLOSED');
assert.deepEqual(failedClosed.authorization, { mapAllowed: false, kmlAllowed: false });
assert.equal(failedClosed.evidence.uncertainties[0].code, 'AGENT_TURN_INVALID');
assert.match(failedClosed.evidence.uncertainties[0].message, /kind is not an allowed value/);

console.log(JSON.stringify({
  suite: 'coordinate-agent-phase04c-contract-regression',
  status: 'PASS',
  geometryEnumCount: geometryTypes.length,
  invalidGeometryRejected: true,
  invalidEvidenceTerminalState: failedClosed.terminalState,
  mapAllowed: failedClosed.authorization.mapAllowed,
  kmlAllowed: failedClosed.authorization.kmlAllowed,
  realProviderCalls: 0,
}, null, 2));
