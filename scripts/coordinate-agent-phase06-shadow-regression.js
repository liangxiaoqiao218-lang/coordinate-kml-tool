import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  COORDINATE_AGENT_NONPRODUCTION_ROUTE_PREFIX,
  COORDINATE_AGENT_SHADOW_BOUNDARY,
  createCoordinateAgentShadowApp,
} from '../server/coordinate-agent/index.js';
import { createCoordinateAgentReplayShadowEvaluator } from './lib/coordinate-agent-shadow-evaluator.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const scenarioPaths = [
  path.join(root, 'regression-samples', 'coordinate-agent-phase03', 'mock-scenarios.v1.json'),
  path.join(root, 'regression-samples', 'coordinate-agent-phase05', 'mock-scenarios.v1.json'),
];
const evaluator = await createCoordinateAgentReplayShadowEvaluator({ scenarioPaths });
assert.equal(evaluator.metrics.scenarioCount, 4);
assert.equal(evaluator.metrics.realProviderCallCount, 0);
assert.equal(evaluator.metrics.sourceMutationCount, 0);
assert.deepEqual(COORDINATE_AGENT_SHADOW_BOUNDARY, {
  schemaVersion: 'coordinate-agent-shadow-boundary/v1',
  shadowOnly: true,
  providerMode: 'replay',
  affectsParser: false,
  affectsCoordinates: false,
  affectsMap: false,
  affectsKml: false,
});

const runtimeIdentity = Object.freeze({
  commit: '3405614b227c2268691b3ac121eb6d4e96867201',
  branch: 'codex/coordinate-agent-phase8-shadow-deploy',
});
const app = createCoordinateAgentShadowApp({
  enabled: true,
  evaluateCase: evaluator.evaluateCase,
  runtimeIdentity,
});
const server = http.createServer(app);
await new Promise((resolve, reject) => {
  server.once('error', reject);
  server.listen(0, '127.0.0.1', resolve);
});

const address = server.address();
const baseUrl = `http://127.0.0.1:${address.port}${COORDINATE_AGENT_NONPRODUCTION_ROUTE_PREFIX}`;
try {
  const healthResponse = await fetch(`${baseUrl}/health`);
  assert.equal(healthResponse.status, 200);
  const health = await healthResponse.json();
  assert.equal(health.status, 'READY');
  assert.deepEqual(health.boundary, COORDINATE_AGENT_SHADOW_BOUNDARY);
  assert.deepEqual(health.runtimeIdentity, runtimeIdentity);

  for (const caseId of evaluator.caseIds) {
    const response = await fetch(`${baseUrl}/evaluate`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ caseId }),
    });
    assert.equal(response.status, 200);
    const payload = await response.json();
    assert.equal(payload.caseId, caseId);
    assert.deepEqual(payload.boundary, COORDINATE_AGENT_SHADOW_BOUNDARY);
    assert.equal(payload.result.overallScore, 1);
    assert.equal(payload.result.execution.realProviderCallCount, 0);
    if (payload.result.terminalState !== 'CONFIRMED') {
      assert.deepEqual(payload.result.authorization, { mapAllowed: false, kmlAllowed: false });
    }
  }

  const unknownResponse = await fetch(`${baseUrl}/evaluate`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ caseId: 'not-registered' }),
  });
  assert.equal(unknownResponse.status, 400);
} finally {
  await new Promise(resolve => server.close(resolve));
}

assert.throws(() => createCoordinateAgentShadowApp({ evaluateCase: evaluator.evaluateCase }), /disabled/);
const productionServerSource = await fs.readFile(path.join(root, 'server.js'), 'utf8');
assert.equal(productionServerSource.includes(COORDINATE_AGENT_NONPRODUCTION_ROUTE_PREFIX), false);
assert.equal(productionServerSource.includes('createCoordinateAgentShadowApp'), false);

console.log(JSON.stringify({
  suite: 'coordinate-agent-phase06-shadow-regression',
  status: 'PASS',
  shadowRoute: `${COORDINATE_AGENT_NONPRODUCTION_ROUTE_PREFIX}/evaluate`,
  shadowHealth: `${COORDINATE_AGENT_NONPRODUCTION_ROUTE_PREFIX}/health`,
  caseCount: evaluator.metrics.scenarioCount,
  replayTurnCount: evaluator.metrics.replayTurnCount,
  toolCallCount: evaluator.metrics.toolCallCount,
  realProviderCallCount: 0,
  sourceMutationCount: 0,
  productionRouteMounted: false,
  boundary: COORDINATE_AGENT_SHADOW_BOUNDARY,
}, null, 2));
