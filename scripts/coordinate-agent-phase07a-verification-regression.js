import assert from 'node:assert/strict';

import {
  CoordinateAgentToolRegistry,
  CoordinateIntelligenceAgentKernel,
  MockCoordinateAgentProviderAdapter,
  registerCoordinateMathTools,
} from '../server/coordinate-agent/index.js';

function candidateTurn(points) {
  return {
    observation: {
      summary: 'The complete image has been observed and the visible coordinate rows are structured.',
      orientationDegrees: 0,
      regions: [{
        id: 'whole-image', kind: 'document', bbox: [0, 0, 1, 1], sourceText: '',
        semanticRole: 'whole image structure', confidence: 0.95, provenance: 'mock_visual_observation',
      }],
    },
    plan: { rationale: 'Submit the candidate to deterministic safety verification.', actions: [] },
    candidate: {
      success: true,
      resultStatus: 'usable',
      displayText: points.map((point, index) => `P${index + 1} | ${point.latitude} N | ${point.longitude} E`).join('\n'),
      coordinateSystem: { kind: 'geographic', name: 'WGS 84', epsg: '4326', status: 'identified' },
      geometryType: points.length > 1 ? 'MultiPoint' : 'Point',
      groups: [{
        name: 'Visible coordinates',
        points: points.map((point, index) => ({
          label: `P${index + 1}`,
          sourceText: `P${index + 1} | ${point.latitude} N | ${point.longitude} E`,
          x: null, y: null, latitude: point.latitude, longitude: point.longitude, needsReview: false,
        })),
      }],
      warnings: [],
    },
    uncertainties: [],
    reviewItems: [],
  };
}

async function runWithRegistry(registry, points) {
  const kernel = new CoordinateIntelligenceAgentKernel({
    providerAdapter: new MockCoordinateAgentProviderAdapter([candidateTurn(points)]),
    toolRegistry: registry,
    maxProviderCalls: 1,
    maxIterations: 1,
  });
  return kernel.run({ imageRef: 'registered-image', requestId: 'offline:phase07a' });
}

const verifiedRegistry = new CoordinateAgentToolRegistry();
registerCoordinateMathTools(verifiedRegistry);
const verified = await runWithRegistry(verifiedRegistry, [
  { latitude: 10.5, longitude: 20.25 },
  { latitude: 11.5, longitude: 21.25 },
]);
assert.equal(verified.terminalState, 'CONFIRMED');
assert.deepEqual(verified.authorization, { mapAllowed: true, kmlAllowed: true });
assert.deepEqual(verified.evidence.toolResults.map(item => item.toolName), [
  'coordinate_math_check',
  'coordinate_math_check',
  'spatial_consistency_check',
]);
assert.ok(verified.evidence.toolResults.every(item => item.ok));

const missingTools = await runWithRegistry(new CoordinateAgentToolRegistry(), [
  { latitude: 10.5, longitude: 20.25 },
  { latitude: 11.5, longitude: 21.25 },
]);
assert.equal(missingTools.terminalState, 'REVIEW_REQUIRED');
assert.deepEqual(missingTools.authorization, { mapAllowed: false, kmlAllowed: false });
assert.equal(missingTools.coordinateResult.resultStatus, 'needs_review');
assert.equal(missingTools.coordinateResult.geometryType, 'Unknown');
assert.ok(missingTools.evidence.uncertainties.some(
  item => item.code === 'DETERMINISTIC_COORDINATE_VERIFICATION_FAILED' && item.blocking,
));

const projectedTurn = candidateTurn([{ latitude: 10.5, longitude: 20.25 }]);
projectedTurn.candidate.displayText = 'P1 | X 500000 | Y 1200000';
projectedTurn.candidate.coordinateSystem = {
  kind: 'projected', name: 'Visible projected CRS', epsg: null, status: 'needs_confirmation',
};
projectedTurn.candidate.resultStatus = 'needs_review';
projectedTurn.candidate.geometryType = 'Unknown';
projectedTurn.candidate.warnings = ['Geographic conversion requires confirmation'];
projectedTurn.candidate.groups[0].points[0] = {
  label: 'P1', sourceText: 'P1 | X 500000 | Y 1200000',
  x: 500000, y: 1200000, latitude: null, longitude: null, needsReview: true,
};
projectedTurn.candidate.groups[0].points.push({
  label: 'P2', sourceText: 'P2 | X 500100 | Y 1200100',
  x: 500100, y: 1200100, latitude: null, longitude: null, needsReview: true,
});
const projectedRegistry = new CoordinateAgentToolRegistry();
registerCoordinateMathTools(projectedRegistry);
const projected = await new CoordinateIntelligenceAgentKernel({
  providerAdapter: new MockCoordinateAgentProviderAdapter([projectedTurn]),
  toolRegistry: projectedRegistry,
  maxProviderCalls: 1,
  maxIterations: 1,
}).run({ imageRef: 'registered-image', requestId: 'offline:phase07a:projected' });
assert.equal(projected.terminalState, 'REVIEW_REQUIRED');
assert.deepEqual(projected.authorization, { mapAllowed: false, kmlAllowed: false });
assert.ok(projected.evidence.uncertainties.some(
  item => item.code === 'DETERMINISTIC_GEOGRAPHIC_PAIR_UNAVAILABLE' && item.blocking,
));
const projectedSpatialCheck = projected.evidence.toolResults.find(
  item => item.toolName === 'spatial_consistency_check',
);
assert.equal(projectedSpatialCheck.ok, true);
assert.equal(projectedSpatialCheck.output.valid, false);
assert.deepEqual(projectedSpatialCheck.output.invalidIndexes, [0, 1]);

console.log(JSON.stringify({
  suite: 'coordinate-agent-phase07a-verification-regression',
  status: 'PASS',
  verifiedTerminalState: verified.terminalState,
  verifiedToolCalls: verified.execution.toolCallCount,
  missingVerificationTerminalState: missingTools.terminalState,
  missingVerificationMapAllowed: missingTools.authorization.mapAllowed,
  missingVerificationKmlAllowed: missingTools.authorization.kmlAllowed,
  projectedWithoutGeographicPairTerminalState: projected.terminalState,
  realProviderCallCount: 0,
}, null, 2));
