import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  CoordinateAgentMultimodalProviderAdapter,
  CoordinateAgentToolRegistry,
  CoordinateIntelligenceAgentKernel,
  LocalImageWorkspace,
  createSharpImageOperations,
  registerCoordinateMathTools,
  registerGenericImageTools,
} from '../server/coordinate-agent/index.js';
import { DashScopeOpenAICompatibleTransport } from '../server/coordinate-agent-transports/dashscope-openai-compatible-transport.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const payload = JSON.parse(await fs.readFile(
  path.join(root, 'regression-samples', 'coordinate-agent-phase03', 'mock-scenarios.v1.json'),
  'utf8',
));
const scenario = payload.scenarios.find(item => item.id === 'generic-confirmed-multigroup');
const source = await fs.readFile(path.join(root, 'regression-samples', scenario.image));
const workspace = new LocalImageWorkspace();
const imageRef = workspace.registerBuffer(source, { label: 'phase04b-offline' });

function injectImageRef(value) {
  if (Array.isArray(value)) return value.map(injectImageRef);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, injectImageRef(child)]));
  }
  return value === '$IMAGE_REF' ? imageRef : value;
}

const turns = injectImageRef(scenario.turns);
const fetchCalls = [];
const fetchImpl = async (url, init) => {
  const callIndex = fetchCalls.length;
  fetchCalls.push({ url, init, body: JSON.parse(init.body) });
  return {
    ok: true,
    status: 200,
    json: async () => ({
      choices: [{ message: { content: JSON.stringify(turns[callIndex]) } }],
      usage: { prompt_tokens: 100 + callIndex, completion_tokens: 20, total_tokens: 120 + callIndex },
    }),
  };
};
const transport = new DashScopeOpenAICompatibleTransport({
  fetchImpl,
  getAccessToken: () => 'offline-placeholder',
  endpoint: 'https://offline.invalid/compatible-mode/v1',
  model: 'offline-vision-model',
  timeoutMs: 5_000,
});
const adapter = new CoordinateAgentMultimodalProviderAdapter({
  transport,
  resolveImage: ref => workspace.resolve(ref),
});
const registry = new CoordinateAgentToolRegistry();
registerGenericImageTools(registry, createSharpImageOperations({ workspace }));
registerCoordinateMathTools(registry);
const result = await new CoordinateIntelligenceAgentKernel({
  providerAdapter: adapter,
  toolRegistry: registry,
  maxProviderCalls: 2,
  maxIterations: 3,
}).run({ imageRef, requestId: 'offline:phase04b' });

assert.equal(result.terminalState, 'CONFIRMED');
assert.equal(fetchCalls.length, 2);
for (const call of fetchCalls) {
  assert.equal(call.url, 'https://offline.invalid/compatible-mode/v1/chat/completions');
  assert.equal(call.init.method, 'POST');
  assert.equal(call.init.headers.Authorization, 'Bearer offline-placeholder');
  assert.equal(call.body.response_format.type, 'json_object');
  assert.equal(call.body.enable_thinking, false);
  assert.equal(call.body.temperature, 0);
  assert.ok(call.body.messages[1].content.some(item => item.type === 'image_url'));
  assert.match(call.body.messages[1].content[0].text, /response schema/i);
  assert.match(call.body.messages[1].content[0].text, /Available tools/i);
}
assert.equal(fetchCalls[0].body.messages[1].content.filter(item => item.type === 'image_url').length, 1);
assert.equal(fetchCalls[1].body.messages[1].content.filter(item => item.type === 'image_url').length, 4);
assert.equal(transport.telemetry().length, 2);
assert.ok(transport.telemetry().every(item => item.usageObserved));

let failedFetchCount = 0;
const failingTransport = new DashScopeOpenAICompatibleTransport({
  fetchImpl: async () => {
    failedFetchCount += 1;
    return { ok: false, status: 500, json: async () => ({}) };
  },
  getAccessToken: () => 'offline-placeholder',
  endpoint: 'https://offline.invalid/compatible-mode/v1',
  model: 'offline-vision-model',
});
await assert.rejects(() => failingTransport.complete({
  iteration: 1,
  state: 'OBSERVING',
  systemPrompt: 'Return JSON.',
  responseSchema: { type: 'object' },
  tools: [],
  context: {},
  assets: [],
}), /HTTP failure/);
assert.equal(failedFetchCount, 1);

console.log(JSON.stringify({
  suite: 'coordinate-agent-phase04b-transport-regression',
  status: 'PASS',
  replayTransportCalls: fetchCalls.length,
  failedRequestAttempts: failedFetchCount,
  automaticRetries: 0,
  realProviderCalls: 0,
  terminalState: result.terminalState,
  mapAllowed: result.authorization.mapAllowed,
  kmlAllowed: result.authorization.kmlAllowed,
}, null, 2));
