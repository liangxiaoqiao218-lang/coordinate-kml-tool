import http from 'node:http';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  COORDINATE_AGENT_NONPRODUCTION_ROUTE_PREFIX,
  createCoordinateAgentShadowApp,
} from '../server/coordinate-agent/index.js';
import { createPhase9QualificationController } from '../server/coordinate-agent-shadow/phase9-qualification-controller.js';
import { runPhase9RealProviderQualification } from '../server/coordinate-agent-shadow/phase9-real-provider-qualification.js';
import { createCoordinateAgentReplayShadowEvaluator } from './lib/coordinate-agent-shadow-evaluator.js';

const args = new Set(process.argv.slice(2));
if (!args.has('--enable-shadow') || !args.has('--execute-real-provider-once')) {
  throw new Error('Phase 9 requires explicit shadow and one-shot Provider flags');
}
const valueArg = name => process.argv.find(value => value.startsWith(`${name}=`))?.split('=').slice(1).join('=');
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const caseId = valueArg('--case-id') || 'eval-001';
const requestedProviderCallLimit = Number(valueArg('--provider-call-limit') || 1);
if (![1, 2].includes(requestedProviderCallLimit)) throw new Error('Provider call limit must be 1 or 2');
// Phase 9A narrows the deployed one-shot qualification budget to one request,
// including services that still carry the earlier Phase 9 "=2" start flag.
const maxProviderCalls = Math.min(requestedProviderCallLimit, 1);
const scenarioPaths = [
  path.join(root, 'regression-samples', 'coordinate-agent-phase03', 'mock-scenarios.v1.json'),
  path.join(root, 'regression-samples', 'coordinate-agent-phase05', 'mock-scenarios.v1.json'),
];
const replayEvaluator = await createCoordinateAgentReplayShadowEvaluator({ scenarioPaths });
const qualification = createPhase9QualificationController({
  runQualification: () => runPhase9RealProviderQualification({ root, caseId, maxProviderCalls }),
});
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
  evaluateCase: replayEvaluator.evaluateCase,
  runtimeIdentity,
  providerMode: 'controlled_real_provider',
  getQualificationStatus: qualification.status,
});
const server = http.createServer(app);
const requestedPort = Number(valueArg('--port') || process.env.PORT || 43129);
if (!Number.isInteger(requestedPort) || requestedPort < 0 || requestedPort > 65535) {
  throw new Error('Shadow port is invalid');
}
const bindAddress = valueArg('--bind') || '127.0.0.1';
if (!['127.0.0.1', '0.0.0.0'].includes(bindAddress)) throw new Error('Shadow bind address is invalid');
if (!runtimeIdentity.commit && bindAddress === '0.0.0.0') {
  throw new Error('A deployed Phase 9 qualification requires a committed runtime identity');
}
server.listen(requestedPort, bindAddress, () => {
  const address = server.address();
  console.log(JSON.stringify({
    schemaVersion: 'coordinate-agent-phase9-shadow-startup/v1',
    status: 'READY',
    bindAddress,
    port: address.port,
    routePrefix: COORDINATE_AGENT_NONPRODUCTION_ROUTE_PREFIX,
    providerCallLimit: maxProviderCalls,
    automaticRetryCount: 0,
    runtimeIdentity,
  }));
  const claimId = `${runtimeIdentity.commit || 'local'}-${caseId}`.replace(/[^a-z0-9._-]/gi, '_');
  const claimPath = path.join(os.tmpdir(), `coordinate-agent-phase9-${claimId}.claim`);
  fs.open(claimPath, 'wx')
    .then(handle => handle.close().then(() => true))
    .catch(error => {
      if (error?.code === 'EEXIST') return false;
      throw error;
    })
    .then(claimed => {
      if (!claimed) {
        console.log(JSON.stringify({
          schemaVersion: 'coordinate-agent-phase9-claim/v1',
          status: 'SKIPPED_ALREADY_CLAIMED',
          runtimeIdentity,
          automaticRetryCount: 0,
        }));
        return null;
      }
      return qualification.runOnce();
    })
    .then(report => {
      if (report) console.log(JSON.stringify(report));
    })
    .catch(() => console.log(JSON.stringify({
      schemaVersion: 'coordinate-agent-phase9-claim/v1',
      status: 'FAILED_CLOSED',
      automaticRetryCount: 0,
    })));
});
