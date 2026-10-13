import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  DEFAULT_COORDINATE_AGENT_PROVIDER_TIMEOUT_MS,
  MAX_COORDINATE_AGENT_PROVIDER_TIMEOUT_MS,
  DashScopeOpenAICompatibleTransport,
  normalizeCoordinateAgentProviderTimeoutMs,
} from '../server/coordinate-agent-transports/dashscope-openai-compatible-transport.js';
import { runPhase9RealProviderQualification } from '../server/coordinate-agent-shadow/phase9-real-provider-qualification.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
assert.equal(DEFAULT_COORDINATE_AGENT_PROVIDER_TIMEOUT_MS, 90_000);
assert.equal(normalizeCoordinateAgentProviderTimeoutMs(undefined), 90_000);
assert.equal(normalizeCoordinateAgentProviderTimeoutMs(55_000), 55_000);
assert.equal(normalizeCoordinateAgentProviderTimeoutMs(999_999), MAX_COORDINATE_AGENT_PROVIDER_TIMEOUT_MS);

const timeoutTransport = new DashScopeOpenAICompatibleTransport({
  fetchImpl: async (_url, { signal }) => new Promise((_resolve, reject) => {
    signal.addEventListener('abort', () => {
      const error = new Error('simulated abort');
      error.name = 'AbortError';
      reject(error);
    }, { once: true });
  }),
  getAccessToken: () => 'offline-placeholder',
  endpoint: 'https://offline.invalid/compatible-mode/v1',
  model: 'offline-vision-model',
  timeoutMs: 1_000,
});
const timeoutStartedAt = Date.now();
await assert.rejects(() => timeoutTransport.complete({
  iteration: 1,
  state: 'OBSERVING',
  systemPrompt: 'Return JSON.',
  responseSchema: { type: 'object' },
  tools: [],
  context: {},
  assets: [],
}), error => error.code === 'DASHSCOPE_TIMEOUT');
const timeoutTelemetry = timeoutTransport.telemetry()[0];
assert.equal(timeoutTelemetry.timeoutMs, 1_000);
assert.equal(timeoutTelemetry.errorCode, 'DASHSCOPE_TIMEOUT');
assert.ok(Date.now() - timeoutStartedAt >= 900);

const scenarioPayload = JSON.parse(await fs.readFile(
  path.join(root, 'regression-samples', 'coordinate-agent-phase03', 'mock-scenarios.v1.json'),
  'utf8',
));
const turns = scenarioPayload.scenarios.find(item => item.id === 'generic-confirmed-multigroup').turns;
let replayCallCount = 0;
const delayedReplayFetch = async (_url, init) => {
  await new Promise(resolve => setTimeout(resolve, 25));
  const body = JSON.parse(init.body);
  const imageLabel = body.messages[1].content.find(item => item.type === 'text' && /^Image asset /.test(item.text));
  const imageRef = imageLabel.text.match(/^Image asset (\S+) /)?.[1];
  const injectImageRef = value => {
    if (Array.isArray(value)) return value.map(injectImageRef);
    if (value && typeof value === 'object') {
      return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, injectImageRef(child)]));
    }
    return value === '$IMAGE_REF' ? imageRef : value;
  };
  const turn = injectImageRef(turns[replayCallCount]);
  replayCallCount += 1;
  return {
    ok: true,
    status: 200,
    json: async () => ({
      choices: [{ message: { content: JSON.stringify(turn) } }],
      usage: { prompt_tokens: 100, completion_tokens: 20, total_tokens: 120 },
    }),
  };
};
const report = await runPhase9RealProviderQualification({
  root,
  maxProviderCalls: 2,
  env: {
    DASHSCOPE_API_KEY: 'offline-placeholder',
    DASHSCOPE_BASE_URL: 'https://offline.invalid/compatible-mode/v1',
    DASHSCOPE_VISION_MODEL: 'offline-vision-model',
    COORDINATE_AGENT_PROVIDER_TIMEOUT_MS: '90000',
  },
  fetchImpl: delayedReplayFetch,
});
assert.equal(report.terminalState, 'CONFIRMED');
assert.equal(report.requestTimeoutMs, 90_000);
assert.equal(report.realProviderCallCount, 2);
assert.equal(report.automaticRetryCount, 0);
assert.equal(report.providerRequests.length, 2);
assert.ok(report.providerRequests.every(item => item.ok && item.timeoutMs === 90_000 && item.durationMs >= 20));
assert.equal(report.sourceUnchanged, true);

console.log(JSON.stringify({
  suite: 'coordinate-agent-phase09b-timeout-regression',
  status: 'PASS',
  defaultRequestTimeoutMs: DEFAULT_COORDINATE_AGENT_PROVIDER_TIMEOUT_MS,
  maximumRequestTimeoutMs: MAX_COORDINATE_AGENT_PROVIDER_TIMEOUT_MS,
  simulatedTimeoutMs: timeoutTelemetry.timeoutMs,
  simulatedReplayCalls: replayCallCount,
  realProviderCalls: 0,
  automaticRetries: 0,
  mapAllowed: false,
  kmlAllowed: false,
}, null, 2));
