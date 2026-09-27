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
const sanitizeRuntimeValue = (value, pattern) => {
  const normalized = String(value || '').trim();
  return pattern.test(normalized) ? normalized : null;
};
const runtimeIdentity = Object.freeze({
  commit: sanitizeRuntimeValue(process.env.RENDER_GIT_COMMIT, /^[0-9a-f]{40}$/i),
  branch: sanitizeRuntimeValue(process.env.RENDER_GIT_BRANCH, /^[a-z0-9._/-]{1,160}$/i),
});
const app = createCoordinateAgentShadowApp({
  enabled: true,
  evaluateCase: evaluator.evaluateCase,
  runtimeIdentity,
});
const server = http.createServer(app);
const requestedPort = Number(
  process.argv.find(value => value.startsWith('--port='))?.split('=')[1]
    || process.env.PORT
    || 43126,
);
if (!Number.isInteger(requestedPort) || requestedPort < 0 || requestedPort > 65535) {
  throw new Error('Shadow port is invalid');
}
const bindAddress = process.argv.find(value => value.startsWith('--bind='))?.split('=')[1] || '127.0.0.1';
if (!['127.0.0.1', '0.0.0.0'].includes(bindAddress)) {
  throw new Error('Shadow bind address must be explicit and local or all-interfaces');
}
server.listen(requestedPort, bindAddress, () => {
  const address = server.address();
  console.log(JSON.stringify({
    schemaVersion: 'coordinate-agent-shadow-startup/v1',
    status: 'READY',
    bindAddress,
    port: address.port,
    routePrefix: COORDINATE_AGENT_NONPRODUCTION_ROUTE_PREFIX,
    caseCount: evaluator.metrics.scenarioCount,
    realProviderCallCount: 0,
    runtimeIdentity,
  }));
});
