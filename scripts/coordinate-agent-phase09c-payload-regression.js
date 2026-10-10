import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  PHASE9_QUALIFICATION_TIMEOUT_MS,
  runPhase9RealProviderQualification,
} from '../server/coordinate-agent-shadow/phase9-real-provider-qualification.js';
import { prepareCoordinateAgentProviderImage } from '../server/coordinate-agent-transports/provider-image-budget.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const manifest = JSON.parse(await fs.readFile(
  path.join(root, 'regression-samples', 'coordinate-agent-evaluation-manifest.v1.json'),
  'utf8',
));
const evaluationCase = manifest.cases.find(item => item.id === 'eval-001');
const source = await fs.readFile(path.resolve(root, 'regression-samples', evaluationCase.image));
const prepared = await prepareCoordinateAgentProviderImage({ buffer: source, mimeType: 'image/png' });
assert.equal(prepared.metrics.sourceBytes, source.length);
assert.equal(prepared.metrics.optimized, true);
assert.equal(prepared.metrics.width, 1290);
assert.equal(prepared.metrics.height, 2796);
assert.ok(prepared.metrics.transmittedBytes < prepared.metrics.sourceBytes * 0.25);
assert.equal(prepared.mimeType, 'image/jpeg');

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
  },
  fetchImpl: delayedReplayFetch,
});
assert.equal(report.terminalState, 'CONFIRMED');
assert.equal(report.requestTimeoutMs, PHASE9_QUALIFICATION_TIMEOUT_MS);
assert.equal(report.realProviderCallCount, 2);
assert.equal(report.automaticRetryCount, 0);
assert.equal(report.sourceUnchanged, true);
assert.equal(report.imageMetrics[0].sourceBytes, source.length);
assert.equal(report.imageMetrics[0].width, 1290);
assert.equal(report.imageMetrics[0].height, 2796);
assert.ok(report.imageMetrics[0].transmittedBytes < report.imageMetrics[0].sourceBytes * 0.25);
assert.ok(report.providerRequests.every(item => item.requestBodyBytes > item.imageBytes));
assert.ok(report.providerRequests.every(item => item.imageBytes > 0));
assert.ok(report.providerRequests.every(item => item.schemaBytes > 1_000));
assert.ok(report.providerRequests.every(item => item.requestBuildDurationMs >= 0));
assert.ok(report.providerRequests.every(item => item.durationMs >= 20));
assert.ok(report.providerRequests.every(item => item.timeoutMs === PHASE9_QUALIFICATION_TIMEOUT_MS));

console.log(JSON.stringify({
  suite: 'coordinate-agent-phase09c-payload-regression',
  status: 'PASS',
  sourceImageBytes: prepared.metrics.sourceBytes,
  transmittedImageBytes: prepared.metrics.transmittedBytes,
  imageWidth: prepared.metrics.width,
  imageHeight: prepared.metrics.height,
  requestTimeoutMs: report.requestTimeoutMs,
  requestMetrics: report.providerRequests,
  simulatedReplayCalls: replayCallCount,
  realProviderCalls: 0,
  automaticRetries: 0,
  mapAllowed: false,
  kmlAllowed: false,
}, null, 2));
