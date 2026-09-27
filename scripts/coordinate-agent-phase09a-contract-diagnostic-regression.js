import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  COORDINATE_AGENT_SYSTEM_PROMPT,
  CoordinateAgentToolRegistry,
  CoordinateIntelligenceAgentKernel,
  assertStrictCoordinateAgentTurn,
  parseCoordinateAgentStructuredOutput,
} from '../server/coordinate-agent/index.js';
import { DashScopeOpenAICompatibleTransport } from '../server/coordinate-agent-transports/dashscope-openai-compatible-transport.js';
import { runPhase9RealProviderQualification } from '../server/coordinate-agent-shadow/phase9-real-provider-qualification.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

const invalidTurn = {
  observation: { regions: [{ id: 'whole', kind: 'document', confidence: 'high' }] },
  plan: { actions: [] },
  candidate: null,
  uncertainties: [],
  reviewItems: [],
};
assert.throws(
  () => assertStrictCoordinateAgentTurn(invalidTurn),
  error => {
    assert.equal(error.code, 'COORDINATE_AGENT_SCHEMA_VALIDATION_FAILED');
    assert.equal(error.path, 'turn.observation.regions[0].confidence');
    assert.equal(error.field, 'confidence');
    assert.equal(error.expectedType, 'number');
    assert.equal(error.actualType, 'string');
    return true;
  },
);

assert.throws(
  () => parseCoordinateAgentStructuredOutput({ structuredOutput: '{bad json' }),
  error => {
    assert.equal(error.code, 'PROVIDER_STRUCTURED_OUTPUT_INVALID_JSON');
    assert.equal(error.path, 'structuredOutput');
    assert.equal(error.expectedType, 'JSON object');
    assert.equal(error.actualType, 'string');
    return true;
  },
);

const transportFailure = new Error('sanitized HTTP failure');
transportFailure.code = 'DASHSCOPE_HTTP_ERROR';
transportFailure.status = 400;
const failedTransportResult = await new CoordinateIntelligenceAgentKernel({
  providerAdapter: { runTurn: async () => { throw transportFailure; } },
  toolRegistry: new CoordinateAgentToolRegistry(),
  maxProviderCalls: 1,
  maxIterations: 1,
}).run({ imageRef: 'local-image://offline-phase09a', requestId: 'offline:phase09a:transport' });
assert.equal(failedTransportResult.terminalState, 'FAILED_CLOSED');
assert.equal(failedTransportResult.evidence.uncertainties[0].code, 'PROVIDER_TRANSPORT_FAILED');
assert.deepEqual(failedTransportResult.execution.diagnostics[0], {
  category: 'transport',
  code: 'DASHSCOPE_HTTP_ERROR',
  path: null,
  field: null,
  expectedType: null,
  actualType: null,
  httpStatus: 400,
});
assert.deepEqual(failedTransportResult.authorization, { mapAllowed: false, kmlAllowed: false });

const schemaFailureResult = await new CoordinateIntelligenceAgentKernel({
  providerAdapter: { runTurn: async () => assertStrictCoordinateAgentTurn(invalidTurn) },
  toolRegistry: new CoordinateAgentToolRegistry(),
  maxProviderCalls: 1,
  maxIterations: 1,
}).run({ imageRef: 'local-image://offline-phase09a', requestId: 'offline:phase09a:schema' });
assert.equal(schemaFailureResult.evidence.uncertainties[0].code, 'AGENT_TURN_INVALID');
assert.deepEqual(schemaFailureResult.execution.diagnostics[0], {
  category: 'schema',
  code: 'COORDINATE_AGENT_SCHEMA_VALIDATION_FAILED',
  path: 'turn.observation.regions[0].confidence',
  field: 'confidence',
  expectedType: 'number',
  actualType: 'string',
  httpStatus: null,
});

const networkTransport = new DashScopeOpenAICompatibleTransport({
  fetchImpl: async () => { throw new Error('offline network failure'); },
  getAccessToken: () => 'offline-placeholder',
  endpoint: 'https://offline.invalid/compatible-mode/v1',
  model: 'offline-vision-model',
});
await assert.rejects(() => networkTransport.complete({
  iteration: 1,
  state: 'OBSERVING',
  systemPrompt: 'Return JSON.',
  responseSchema: { type: 'object' },
  tools: [],
  context: {},
  assets: [],
}), error => error.code === 'DASHSCOPE_NETWORK_ERROR');
assert.deepEqual(networkTransport.telemetry().map(item => ({
  ok: item.ok,
  httpStatus: item.httpStatus,
  errorCode: item.errorCode,
  usageObserved: item.usageObserved,
})), [{
  ok: false,
  httpStatus: null,
  errorCode: 'DASHSCOPE_NETWORK_ERROR',
  usageObserved: false,
}]);

const publicQualificationReport = await runPhase9RealProviderQualification({
  root,
  maxProviderCalls: 1,
  env: {
    DASHSCOPE_API_KEY: 'offline-placeholder',
    DASHSCOPE_VISION_MODEL: 'offline-vision-model',
    DASHSCOPE_BASE_URL: 'https://offline.invalid/compatible-mode/v1',
  },
  fetchImpl: async () => ({
    ok: false,
    status: 400,
    json: async () => ({ error: { message: 'intentionally discarded raw provider body' } }),
  }),
});
assert.equal(publicQualificationReport.realProviderCallCount, 1);
assert.equal(publicQualificationReport.automaticRetryCount, 0);
assert.deepEqual(publicQualificationReport.diagnostics, [{
  category: 'transport',
  code: 'DASHSCOPE_HTTP_ERROR',
  path: null,
  field: null,
  expectedType: null,
  actualType: null,
  httpStatus: 400,
}]);
assert.deepEqual(publicQualificationReport.providerRequests.map(item => ({
  ok: item.ok,
  httpStatus: item.httpStatus,
  errorCode: item.errorCode,
  usageObserved: item.usageObserved,
  timeoutMs: item.timeoutMs,
})), [{
  ok: false,
  httpStatus: 400,
  errorCode: 'DASHSCOPE_HTTP_ERROR',
  usageObserved: false,
  timeoutMs: 90_000,
}]);
assert.ok(publicQualificationReport.providerRequests[0].durationMs >= 0);
assert.equal(publicQualificationReport.mapAllowed, false);
assert.equal(publicQualificationReport.kmlAllowed, false);

assert.match(COORDINATE_AGENT_SYSTEM_PROMPT, /Always include candidate, uncertainties, and reviewItems/);
assert.doesNotMatch(COORDINATE_AGENT_SYSTEM_PROMPT, /eval-001|phase9-registered-evaluation|邓巴/i);

console.log(JSON.stringify({
  suite: 'coordinate-agent-phase09a-contract-diagnostic-regression',
  status: 'PASS',
  transportDiagnostic: failedTransportResult.execution.diagnostics[0],
  schemaDiagnostic: schemaFailureResult.execution.diagnostics[0],
  publicDiagnostic: publicQualificationReport.diagnostics[0],
  realProviderCalls: 0,
  simulatedProviderCalls: publicQualificationReport.realProviderCallCount,
  automaticRetries: 0,
  mapAllowed: false,
  kmlAllowed: false,
}, null, 2));
