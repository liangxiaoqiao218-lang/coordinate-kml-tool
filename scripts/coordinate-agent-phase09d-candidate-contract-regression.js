import assert from 'node:assert/strict';

import {
  COORDINATE_AGENT_SYSTEM_PROMPT,
  CoordinateAgentToolRegistry,
  CoordinateIntelligenceAgentKernel,
  MockCoordinateAgentProviderAdapter,
  assertStrictCoordinateAgentTurn,
  registerCoordinateMathTools,
} from '../server/coordinate-agent/index.js';
import { summarizeCandidateRepresentation } from '../server/coordinate-agent/candidate-representation-summary.js';

function point(overrides = {}) {
  return {
    label: 'P',
    sourceText: 'visible coordinate row',
    x: null,
    y: null,
    latitude: 10,
    longitude: 20,
    needsReview: false,
    ...overrides,
  };
}

function turn({ kind = 'geographic', status = 'identified', points = [point()], resultStatus = 'usable' } = {}) {
  return {
    observation: {
      summary: 'Whole image observed.',
      orientationDegrees: 0,
      regions: [{
        id: 'whole', kind: 'document', bbox: [0, 0, 1, 1], sourceText: '',
        semanticRole: 'whole image', confidence: 1, provenance: 'offline_mock',
      }],
    },
    plan: { rationale: 'Verify structured candidate.', actions: [] },
    candidate: {
      success: true,
      resultStatus,
      displayText: 'source-faithful rows',
      coordinateSystem: { kind, name: kind === 'geographic' ? 'Explicit geographic CRS' : 'Explicit projected CRS', epsg: null, status },
      geometryType: points.length > 1 ? 'MultiPoint' : 'Point',
      groups: [{ name: 'Visible group', points }],
      warnings: resultStatus === 'needs_review' ? ['Review required'] : [],
    },
    uncertainties: [],
    reviewItems: [],
  };
}

const missingRepresentationFields = turn();
missingRepresentationFields.candidate.groups[0].points[0] = { sourceText: 'visible coordinate row' };
assert.throws(
  () => assertStrictCoordinateAgentTurn(missingRepresentationFields),
  error => error.code === 'COORDINATE_AGENT_SCHEMA_VALIDATION_FAILED'
    && error.path === 'turn.candidate'
    && /points\[0\]\.label is required/u.test(error.message),
);

const geographicTurn = turn({ points: [point(), point({ label: 'Q', latitude: 11, longitude: 21 })] });
const geographicRegistry = new CoordinateAgentToolRegistry();
registerCoordinateMathTools(geographicRegistry);
const geographic = await new CoordinateIntelligenceAgentKernel({
  providerAdapter: new MockCoordinateAgentProviderAdapter([geographicTurn]),
  toolRegistry: geographicRegistry,
  maxProviderCalls: 1,
  maxIterations: 1,
}).run({ imageRef: 'offline-image', requestId: 'offline:phase09d:geographic' });
assert.equal(geographic.terminalState, 'CONFIRMED');
assert.deepEqual(geographic.authorization, { mapAllowed: true, kmlAllowed: true });

const projectedTurn = turn({
  kind: 'projected',
  status: 'identified',
  resultStatus: 'needs_review',
  points: [point({ x: 500000, y: 1200000, latitude: null, longitude: null, needsReview: true })],
});
const projectedRegistry = new CoordinateAgentToolRegistry();
registerCoordinateMathTools(projectedRegistry);
const projected = await new CoordinateIntelligenceAgentKernel({
  providerAdapter: new MockCoordinateAgentProviderAdapter([projectedTurn]),
  toolRegistry: projectedRegistry,
  maxProviderCalls: 1,
  maxIterations: 1,
}).run({ imageRef: 'offline-image', requestId: 'offline:phase09d:projected' });
assert.equal(projected.terminalState, 'REVIEW_REQUIRED');
assert.deepEqual(projected.authorization, { mapAllowed: false, kmlAllowed: false });
assert.ok(projected.evidence.uncertainties.some(item => item.code === 'DETERMINISTIC_GEOGRAPHIC_CRS_UNAVAILABLE'));

const conflictingFamilyTurn = turn({ kind: 'projected', status: 'identified', points: [point()] });
const conflictingRegistry = new CoordinateAgentToolRegistry();
registerCoordinateMathTools(conflictingRegistry);
const conflicting = await new CoordinateIntelligenceAgentKernel({
  providerAdapter: new MockCoordinateAgentProviderAdapter([conflictingFamilyTurn]),
  toolRegistry: conflictingRegistry,
  maxProviderCalls: 1,
  maxIterations: 1,
}).run({ imageRef: 'offline-image', requestId: 'offline:phase09d:conflicting-family' });
assert.equal(conflicting.terminalState, 'FAILED_CLOSED');
assert.deepEqual(conflicting.authorization, { mapAllowed: false, kmlAllowed: false });

const representation = summarizeCandidateRepresentation(projected.coordinateResult);
assert.deepEqual(representation, {
  coordinateSystemKind: 'projected',
  coordinateSystemStatus: 'identified',
  pointCount: 1,
  geographicPairCount: 0,
  projectedPairCount: 1,
  incompleteGeographicPairCount: 0,
  incompleteProjectedPairCount: 0,
  reviewPointCount: 1,
});
assert.equal(JSON.stringify(representation).includes('500000'), false);
assert.match(COORDINATE_AGENT_SYSTEM_PROMPT, /use latitude\/longitude only for normalized signed geographic coordinates/i);
assert.doesNotMatch(COORDINATE_AGENT_SYSTEM_PROMPT, /eval-001|phase9-registered-evaluation/i);

console.log(JSON.stringify({
  suite: 'coordinate-agent-phase09d-candidate-contract-regression',
  status: 'PASS',
  geographicTerminalState: geographic.terminalState,
  projectedTerminalState: projected.terminalState,
  conflictingFamilyTerminalState: conflicting.terminalState,
  representation,
  realProviderCalls: 0,
  mapAllowed: projected.authorization.mapAllowed,
  kmlAllowed: projected.authorization.kmlAllowed,
}, null, 2));
