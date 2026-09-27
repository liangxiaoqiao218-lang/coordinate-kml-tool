import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import express from 'express';

import {
  COORDINATE_AGENT_NONPRODUCTION_ROUTE_PREFIX,
  COORDINATE_AGENT_SYSTEM_PROMPT,
  createCoordinateAgentNonProductionRouter,
} from '../server/coordinate-agent/index.js';
import { runCoordinateAgentOfflineScenarios } from './lib/coordinate-agent-offline-evaluator.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const phase3Path = path.join(root, 'regression-samples', 'coordinate-agent-phase03', 'mock-scenarios.v1.json');
const phase5Path = path.join(root, 'regression-samples', 'coordinate-agent-phase05', 'mock-scenarios.v1.json');

for (const requiredPromptConcept of [
  'Observe the complete image',
  'Build a provisional document structure',
  'Decide which listed tools',
  'Read and relate headers, rows, columns',
  'inspect the relevant region again',
  'mathematical and spatial tools',
  'strictly structured coordinate conclusion or precise review questions',
  'Do not bypass this loop with one-shot OCR text',
]) {
  assert.match(COORDINATE_AGENT_SYSTEM_PROMPT, new RegExp(requiredPromptConcept, 'i'));
}

const [phase3, phase5] = await Promise.all([
  runCoordinateAgentOfflineScenarios({ scenarioPath: phase3Path }),
  runCoordinateAgentOfflineScenarios({ scenarioPath: phase5Path }),
]);
const scenarios = [...phase3.scenarios, ...phase5.scenarios];
assert.equal(scenarios.length, 4);
assert.equal(phase3.realProviderCallCount + phase5.realProviderCallCount, 0);
assert.equal(phase3.sourceMutationCount + phase5.sourceMutationCount, 0);
for (const scenario of scenarios) {
  assert.equal(scenario.score.overallScore, 1, `${scenario.id} must satisfy every evaluation dimension`);
}

const rotated = scenarios.find(item => item.id === 'generic-rotated-math-verified');
assert.deepEqual(rotated.trace.tools.map(item => item.toolName), [
  'rotate_image', 'crop_region', 'coordinate_math_check', 'spatial_consistency_check',
]);
assert.deepEqual(rotated.trace.states.map(item => item.state), [
  'INITIALIZED', 'OBSERVING', 'PLANNING', 'ACTING', 'RECONCILING', 'OBSERVING',
  'PLANNING', 'ACTING', 'RECONCILING', 'OBSERVING', 'PLANNING', 'VERIFYING', 'CONFIRMED',
]);

const conflict = scenarios.find(item => item.id === 'generic-layout-conflict-review');
assert.equal(conflict.result.terminalState, 'REVIEW_REQUIRED');
assert.deepEqual(conflict.result.authorization, { mapAllowed: false, kmlAllowed: false });
assert.equal(conflict.result.evidence.reviewItems[0].fieldPath, 'coordinateGroups[0].headerDirectionBinding');

assert.throws(
  () => createCoordinateAgentNonProductionRouter({ evaluateCase: async () => ({}) }),
  /disabled/,
);

const byId = new Map(scenarios.map(item => [item.id, item]));
const router = createCoordinateAgentNonProductionRouter({
  enabled: true,
  evaluateCase: async ({ caseId }) => {
    const scenario = byId.get(caseId);
    if (!scenario) throw new Error('The requested evaluation case is not registered');
    return {
      terminalState: scenario.result.terminalState,
      dimensions: scenario.score.dimensions,
      overallScore: scenario.score.overallScore,
      execution: {
        replayTurnCount: scenario.trace.mockTurnCount,
        toolCallCount: scenario.trace.toolCallCount,
        realProviderCallCount: 0,
      },
      authorization: scenario.result.authorization,
    };
  },
});
const app = express();
app.use(COORDINATE_AGENT_NONPRODUCTION_ROUTE_PREFIX, router);
const server = http.createServer(app);
await new Promise((resolve, reject) => {
  server.once('error', reject);
  server.listen(0, '127.0.0.1', resolve);
});

let routeResponse;
try {
  const address = server.address();
  routeResponse = await fetch(`http://127.0.0.1:${address.port}${COORDINATE_AGENT_NONPRODUCTION_ROUTE_PREFIX}/evaluate`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ caseId: 'generic-layout-conflict-review' }),
  });
  assert.equal(routeResponse.status, 200);
  const payload = await routeResponse.json();
  assert.equal(payload.schemaVersion, 'coordinate-agent-nonproduction-response/v1');
  assert.equal(payload.result.terminalState, 'REVIEW_REQUIRED');
  assert.equal(payload.result.execution.realProviderCallCount, 0);
  assert.deepEqual(payload.result.authorization, { mapAllowed: false, kmlAllowed: false });
} finally {
  await new Promise(resolve => server.close(resolve));
}

const serverSource = await fs.readFile(path.join(root, 'server.js'), 'utf8');
assert.equal(serverSource.includes(COORDINATE_AGENT_NONPRODUCTION_ROUTE_PREFIX), false);

const terminalCounts = scenarios.reduce((counts, scenario) => {
  const terminal = scenario.result.terminalState;
  counts[terminal] = (counts[terminal] || 0) + 1;
  return counts;
}, {});
const toolNames = [...new Set(scenarios.flatMap(item => item.trace.tools.map(tool => tool.toolName)))].sort();
console.log(JSON.stringify({
  suite: 'coordinate-agent-phase05-regression',
  status: 'PASS',
  scenarioCount: scenarios.length,
  dimensions: ['structure', 'rows', 'numeric', 'direction', 'grouping', 'uncertainty', 'mapKmlSafety'],
  minimumOverallScore: Math.min(...scenarios.map(item => item.score.overallScore)),
  terminalCounts,
  toolNames,
  replayTurnCount: scenarios.reduce((sum, item) => sum + item.trace.mockTurnCount, 0),
  toolCallCount: scenarios.reduce((sum, item) => sum + item.trace.toolCallCount, 0),
  realProviderCallCount: 0,
  sourceMutationCount: 0,
  nonProductionRouteMountedInServer: false,
}, null, 2));
