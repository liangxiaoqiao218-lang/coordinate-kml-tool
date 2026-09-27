import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { createPhase9QualificationController } from '../server/coordinate-agent-shadow/phase9-qualification-controller.js';
import { runPhase9RealProviderQualification } from '../server/coordinate-agent-shadow/phase9-real-provider-qualification.js';
import { createCoordinateAgentShadowApp } from '../server/coordinate-agent/index.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

let runCount = 0;
const controller = createPhase9QualificationController({
  runQualification: async () => {
    runCount += 1;
    return {
      schemaVersion: 'coordinate-agent-phase9-qualification/v1',
      status: 'COMPLETED',
      terminalState: 'REVIEW_REQUIRED',
      candidatePointCount: 2,
      realProviderCallCount: 0,
      automaticRetryCount: 0,
      usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 },
      billingStatus: 'UNKNOWN',
      mapAllowed: false,
      kmlAllowed: false,
      stateHistory: [],
      toolCalls: [],
      validationFailureCodes: ['DIRECTION_EVIDENCE_UNRESOLVED'],
      sourceUnchanged: true,
    };
  },
});
const first = controller.runOnce();
const second = controller.runOnce();
assert.equal(first, second);
const report = await first;
assert.equal(runCount, 1);
assert.equal(report.terminalState, 'REVIEW_REQUIRED');
assert.equal(report.mapAllowed, false);
assert.equal(report.kmlAllowed, false);
assert.equal(report.automaticRetryCount, 0);

const scenarioPayload = JSON.parse(await fs.readFile(
  path.join(root, 'regression-samples', 'coordinate-agent-phase03', 'mock-scenarios.v1.json'),
  'utf8',
));
const replayTurns = scenarioPayload.scenarios.find(item => item.id === 'generic-confirmed-multigroup').turns;
let simulatedTransportCalls = 0;
const injectImageRef = (value, imageRef) => {
  if (Array.isArray(value)) return value.map(item => injectImageRef(item, imageRef));
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, injectImageRef(child, imageRef)]));
  }
  return value === '$IMAGE_REF' ? imageRef : value;
};
const replayFetch = async (_url, init) => {
  const body = JSON.parse(init.body);
  const imageLabel = body.messages[1].content.find(item => item.type === 'text' && /^Image asset /.test(item.text));
  const imageRef = imageLabel.text.match(/^Image asset (\S+) /)?.[1];
  const turn = injectImageRef(replayTurns[simulatedTransportCalls], imageRef);
  simulatedTransportCalls += 1;
  return {
    ok: true,
    status: 200,
    json: async () => ({
      choices: [{ message: { content: JSON.stringify(turn) } }],
      usage: { prompt_tokens: 100, completion_tokens: 20, total_tokens: 120 },
    }),
  };
};
const simulatedQualification = await runPhase9RealProviderQualification({
  root,
  caseId: 'eval-001',
  maxProviderCalls: 2,
  env: {
    DASHSCOPE_API_KEY: 'offline-placeholder',
    DASHSCOPE_BASE_URL: 'https://offline.invalid/compatible-mode/v1',
    DASHSCOPE_VISION_MODEL: 'offline-vision-model',
  },
  fetchImpl: replayFetch,
});
assert.equal(simulatedTransportCalls, 2);
assert.equal(simulatedQualification.realProviderCallCount, 2);
assert.equal(simulatedQualification.automaticRetryCount, 0);
assert.equal(simulatedQualification.terminalState, 'CONFIRMED');
assert.equal(simulatedQualification.sourceUnchanged, true);

const app = createCoordinateAgentShadowApp({
  enabled: true,
  evaluateCase: async () => ({ terminalState: 'REVIEW_REQUIRED' }),
  providerMode: 'controlled_real_provider',
  getQualificationStatus: controller.status,
});
const server = app.listen(0, '127.0.0.1');
await new Promise((resolve, reject) => {
  server.once('listening', resolve);
  server.once('error', reject);
});
try {
  const address = server.address();
  const health = await fetch(`http://127.0.0.1:${address.port}/api/nonproduction/coordinate-agent/v1/health`).then(r => r.json());
  const qualification = await fetch(`http://127.0.0.1:${address.port}/api/nonproduction/coordinate-agent/v1/qualification`).then(r => r.json());
  assert.equal(health.boundary.shadowOnly, true);
  assert.equal(health.boundary.providerMode, 'controlled_real_provider');
  assert.equal(health.boundary.affectsParser, false);
  assert.equal(health.boundary.affectsCoordinates, false);
  assert.equal(health.boundary.affectsMap, false);
  assert.equal(health.boundary.affectsKml, false);
  assert.deepEqual(qualification, report);
} finally {
  await new Promise(resolve => server.close(resolve));
}

console.log(JSON.stringify({
  suite: 'coordinate-agent-phase09-shadow-regression',
  status: 'PASS',
  runCount,
  realProviderCallCount: 0,
  automaticRetryCount: 0,
  simulatedTransportCalls,
  networkProviderCalls: 0,
  productionRouteMounted: false,
  mapAllowed: report.mapAllowed,
  kmlAllowed: report.kmlAllowed,
}, null, 2));
