import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  COORDINATE_AGENT_NONPRODUCTION_ROUTE_PREFIX,
  createCoordinateAgentShadowApp,
} from '../server/coordinate-agent/index.js';
import { createCoordinateAgentReplayShadowEvaluator } from './lib/coordinate-agent-shadow-evaluator.js';

const args = new Set(process.argv.slice(2));
if (!args.has('--enable-shadow')) {
  throw new Error('Shadow runtime is disabled; pass --enable-shadow explicitly');
}

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const scenarioPaths = [
  path.join(root, 'regression-samples', 'coordinate-agent-phase03', 'mock-scenarios.v1.json'),
  path.join(root, 'regression-samples', 'coordinate-agent-phase05', 'mock-scenarios.v1.json'),
];
const evaluator = await createCoordinateAgentReplayShadowEvaluator({ scenarioPaths });
const app = createCoordinateAgentShadowApp({ enabled: true, evaluateCase: evaluator.evaluateCase });
const server = http.createServer(app);
const requestedPort = Number(process.argv.find(value => value.startsWith('--port='))?.split('=')[1] || 43126);
if (!Number.isInteger(requestedPort) || requestedPort < 0 || requestedPort > 65535) {
  throw new Error('Shadow port is invalid');
}
server.listen(requestedPort, '127.0.0.1', () => {
  const address = server.address();
  console.log(JSON.stringify({
    schemaVersion: 'coordinate-agent-shadow-startup/v1',
    status: 'READY',
    bindAddress: '127.0.0.1',
    port: address.port,
    routePrefix: COORDINATE_AGENT_NONPRODUCTION_ROUTE_PREFIX,
    caseCount: evaluator.metrics.scenarioCount,
    realProviderCallCount: 0,
  }));
});
