import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  COORDINATE_AGENT_MULTIMODAL_REQUEST_VERSION,
  COORDINATE_AGENT_SYSTEM_PROMPT,
  CoordinateAgentMultimodalProviderAdapter,
  CoordinateAgentToolRegistry,
  CoordinateIntelligenceAgentKernel,
  LocalImageWorkspace,
  createSharpImageOperations,
  registerCoordinateMathTools,
  registerGenericImageTools,
} from '../server/coordinate-agent/index.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const scenarioPayload = JSON.parse(await fs.readFile(
  path.join(root, 'regression-samples', 'coordinate-agent-phase03', 'mock-scenarios.v1.json'),
  'utf8',
));
const scenario = scenarioPayload.scenarios.find(item => item.id === 'generic-confirmed-multigroup');
const source = await fs.readFile(path.join(root, 'regression-samples', scenario.image));
const workspace = new LocalImageWorkspace();
const imageRef = workspace.registerBuffer(source, { label: 'phase04a-source' });

function injectImageRef(value) {
  if (Array.isArray(value)) return value.map(injectImageRef);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, injectImageRef(child)]));
  }
  return value === '$IMAGE_REF' ? imageRef : value;
}

const scriptedTurns = injectImageRef(scenario.turns);
const requests = [];
const replayTransport = {
  async complete(request) {
    requests.push(request);
    const turn = scriptedTurns[requests.length - 1];
    if (!turn) throw new Error('Replay exhausted');
    return { structuredOutput: structuredClone(turn) };
  },
};
const providerAdapter = new CoordinateAgentMultimodalProviderAdapter({
  transport: replayTransport,
  resolveImage: ref => workspace.resolve(ref),
});
const registry = new CoordinateAgentToolRegistry();
registerGenericImageTools(registry, createSharpImageOperations({ workspace }));
registerCoordinateMathTools(registry);
const kernel = new CoordinateIntelligenceAgentKernel({
  providerAdapter,
  toolRegistry: registry,
  maxProviderCalls: 2,
  maxIterations: 3,
});
const result = await kernel.run({ imageRef, requestId: 'offline:phase04a' });

assert.equal(result.terminalState, 'CONFIRMED');
assert.deepEqual(result.authorization, { mapAllowed: true, kmlAllowed: true });
assert.equal(requests.length, 2);
assert.equal(requests[0].protocolVersion, COORDINATE_AGENT_MULTIMODAL_REQUEST_VERSION);
assert.equal(requests[0].systemPrompt, COORDINATE_AGENT_SYSTEM_PROMPT);
assert.equal(requests[0].assets.length, 1);
assert.equal(requests[0].assets[0].role, 'source');
assert.match(requests[0].assets[0].bytesBase64, /^[A-Za-z0-9+/]+=*$/);
assert.equal(requests[1].assets.length, 4);
assert.equal(requests[1].assets.filter(asset => asset.role === 'tool_result').length, 3);
assert.deepEqual(requests[0].tools.map(tool => tool.name), registry.definitions().map(tool => tool.name));
assert.equal(requests[0].responseSchema.additionalProperties, false);
assert.ok(requests[0].responseSchema.required.includes('observation'));
assert.ok(requests[0].responseSchema.required.includes('plan'));
assert.doesNotMatch(requests[0].systemPrompt, /10\.5|20\.25|fixture|Group A|Group B/i);
assert.doesNotMatch(JSON.stringify(requests), /api[_-]?key|authorization|secret|dashscope|openai/i);

const malformedAdapter = new CoordinateAgentMultimodalProviderAdapter({
  transport: { complete: async () => ({ structuredOutput: { ...scriptedTurns[0], unexpected: true } }) },
  resolveImage: ref => workspace.resolve(ref),
});
await assert.rejects(
  () => malformedAdapter.runTurn({
    requestId: 'offline:malformed', imageRef, iteration: 1, state: 'OBSERVING',
    tools: registry.definitions(), evidence: { regions: [], uncertainties: [], reviewItems: [], toolResults: [] }, toolResults: [],
  }),
  /unknown properties: unexpected/,
);

const invalidEnvelopeAdapter = new CoordinateAgentMultimodalProviderAdapter({
  transport: { complete: async () => ({ structuredOutput: scriptedTurns[0], usage: {} }) },
  resolveImage: ref => workspace.resolve(ref),
});
await assert.rejects(
  () => invalidEnvelopeAdapter.runTurn({
    requestId: 'offline:envelope', imageRef, iteration: 1, state: 'OBSERVING',
    tools: registry.definitions(), evidence: { regions: [], uncertainties: [], reviewItems: [], toolResults: [] }, toolResults: [],
  }),
  /must contain only structuredOutput/,
);

const invalidTypeTurn = structuredClone(scriptedTurns[0]);
invalidTypeTurn.observation.regions[0].confidence = 'high';
const invalidTypeAdapter = new CoordinateAgentMultimodalProviderAdapter({
  transport: { complete: async () => ({ structuredOutput: invalidTypeTurn }) },
  resolveImage: ref => workspace.resolve(ref),
});
await assert.rejects(
  () => invalidTypeAdapter.runTurn({
    requestId: 'offline:invalid-type', imageRef, iteration: 1, state: 'OBSERVING',
    tools: registry.definitions(), evidence: { regions: [], uncertainties: [], reviewItems: [], toolResults: [] }, toolResults: [],
  }),
  /confidence has an invalid type/,
);

const failedClosedKernel = new CoordinateIntelligenceAgentKernel({
  providerAdapter: invalidTypeAdapter,
  toolRegistry: registry,
  maxProviderCalls: 1,
  maxIterations: 1,
});
const failedClosedResult = await failedClosedKernel.run({ imageRef, requestId: 'offline:fail-closed' });
assert.equal(failedClosedResult.terminalState, 'FAILED_CLOSED');
assert.deepEqual(failedClosedResult.authorization, { mapAllowed: false, kmlAllowed: false });
assert.equal(failedClosedResult.execution.providerCallCount, 1);

console.log(JSON.stringify({
  suite: 'coordinate-agent-phase04a-regression',
  status: 'PASS',
  replayTransportCalls: requests.length,
  realProviderCalls: 0,
  firstTurnAssets: requests[0].assets.length,
  followUpAssets: requests[1].assets.length,
  toolDefinitions: requests[0].tools.length,
  terminalState: result.terminalState,
  mapAllowed: result.authorization.mapAllowed,
  kmlAllowed: result.authorization.kmlAllowed,
  malformedResponseTerminalState: failedClosedResult.terminalState,
}, null, 2));
