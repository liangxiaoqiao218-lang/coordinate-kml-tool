import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { runCoordinateAgentOfflineScenarios } from './lib/coordinate-agent-offline-evaluator.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const result = await runCoordinateAgentOfflineScenarios({
  scenarioPath: path.join(root, 'regression-samples', 'coordinate-agent-phase03', 'mock-scenarios.v1.json'),
});

assert.equal(result.scenarioCount, 2);
assert.equal(result.realProviderCallCount, 0);
assert.equal(result.mockTurnCount, 4);
assert.equal(result.toolCallCount, 10);
assert.equal(result.sourceMutationCount, 0);
for (const score of Object.values(result.aggregate)) assert.equal(score, 1);

const confirmed = result.scenarios.find(item => item.id === 'generic-confirmed-multigroup');
assert.equal(confirmed.result.terminalState, 'CONFIRMED');
assert.deepEqual(confirmed.result.authorization, { mapAllowed: true, kmlAllowed: true });
assert.deepEqual(confirmed.trace.tools.map(item => item.toolName), [
  'rotate_image', 'crop_region', 'zoom_region',
  'coordinate_math_check', 'coordinate_math_check', 'coordinate_math_check',
  'spatial_consistency_check',
]);
assert.deepEqual(confirmed.trace.states.map(item => item.state), [
  'INITIALIZED', 'OBSERVING', 'PLANNING', 'ACTING', 'RECONCILING', 'OBSERVING', 'PLANNING', 'VERIFYING', 'CONFIRMED',
]);

const review = result.scenarios.find(item => item.id === 'generic-direction-review');
assert.equal(review.result.terminalState, 'REVIEW_REQUIRED');
assert.deepEqual(review.result.authorization, { mapAllowed: false, kmlAllowed: false });
assert.equal(review.result.evidence.reviewItems[0].fieldPath, 'coordinateGroups[0].points[0].longitudeDirection');

console.log('coordinate-agent-phase03-regression: PASS');
