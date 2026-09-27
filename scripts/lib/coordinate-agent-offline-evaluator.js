import { createHash } from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

import {
  CoordinateAgentToolRegistry,
  CoordinateIntelligenceAgentKernel,
  LocalImageWorkspace,
  MockCoordinateAgentProviderAdapter,
  buildCoordinateAgentExecutionTrace,
  createSharpImageOperations,
  registerCoordinateMathTools,
  registerGenericImageTools,
  scoreCoordinateAgentEvaluation,
} from '../../server/coordinate-agent/index.js';

function sha256(buffer) {
  return createHash('sha256').update(buffer).digest('hex');
}

function injectImageRef(value, imageRef) {
  if (Array.isArray(value)) return value.map(item => injectImageRef(item, imageRef));
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, injectImageRef(child, imageRef)]));
  }
  return value === '$IMAGE_REF' ? imageRef : value;
}

function safeImagePath(root, relativePath) {
  const candidate = path.resolve(root, String(relativePath || ''));
  const relative = path.relative(root, candidate);
  if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) {
    throw new Error('Offline scenario image escapes the evaluation root');
  }
  return candidate;
}

export async function runCoordinateAgentOfflineScenarios({ scenarioPath, scenarioIds = null } = {}) {
  const absoluteScenarioPath = path.resolve(String(scenarioPath || ''));
  const regressionRoot = path.resolve(path.dirname(absoluteScenarioPath), '..');
  const payload = JSON.parse(await fs.readFile(absoluteScenarioPath, 'utf8'));
  if (payload?.schemaVersion !== 'coordinate-agent-offline-scenarios/v1' || !Array.isArray(payload.scenarios)) {
    throw new Error('Offline scenario file is invalid');
  }

  const requestedIds = scenarioIds === null ? null : new Set(scenarioIds.map(String));
  const selectedScenarios = requestedIds === null
    ? payload.scenarios
    : payload.scenarios.filter(scenario => requestedIds.has(String(scenario.id)));
  if (requestedIds !== null && selectedScenarios.length !== requestedIds.size) {
    throw new Error('One or more requested offline scenarios are not registered');
  }

  const scenarioResults = [];
  for (const scenario of selectedScenarios) {
    const imagePath = safeImagePath(regressionRoot, scenario.image);
    const before = await fs.readFile(imagePath);
    const beforeHash = sha256(before);
    const workspace = new LocalImageWorkspace();
    const imageRef = workspace.registerBuffer(before, { label: scenario.id });
    const registry = new CoordinateAgentToolRegistry();
    registerGenericImageTools(registry, createSharpImageOperations({ workspace }));
    registerCoordinateMathTools(registry);
    const turns = injectImageRef(scenario.turns, imageRef);
    const adapter = new MockCoordinateAgentProviderAdapter(turns);
    const kernel = new CoordinateIntelligenceAgentKernel({
      providerAdapter: adapter,
      toolRegistry: registry,
      maxProviderCalls: Number(scenario.maxProviderCalls || 2),
      maxIterations: Number(scenario.maxIterations || 3),
    });
    const result = await kernel.run({ imageRef, requestId: `offline:${scenario.id}` });
    const score = scoreCoordinateAgentEvaluation({ actual: result, expected: scenario.expected });
    const trace = buildCoordinateAgentExecutionTrace(result);
    const afterHash = sha256(await fs.readFile(imagePath));
    if (beforeHash !== afterHash) throw new Error(`Offline scenario modified its source image: ${scenario.id}`);
    scenarioResults.push(Object.freeze({
      id: String(scenario.id),
      result,
      score,
      trace,
      sourceUnchanged: true,
    }));
  }

  const dimensionNames = ['structure', 'rows', 'numeric', 'direction', 'grouping', 'uncertainty', 'mapKmlSafety'];
  const aggregate = Object.fromEntries(dimensionNames.map(name => {
    const scores = scenarioResults.map(item => item.score.dimensions[name].score);
    return [name, scores.reduce((sum, score) => sum + score, 0) / scores.length];
  }));

  return Object.freeze({
    schemaVersion: 'coordinate-agent-offline-evaluation/v1',
    scenarioCount: scenarioResults.length,
    realProviderCallCount: 0,
    mockTurnCount: scenarioResults.reduce((sum, item) => sum + item.trace.mockTurnCount, 0),
    toolCallCount: scenarioResults.reduce((sum, item) => sum + item.trace.toolCallCount, 0),
    sourceMutationCount: 0,
    aggregate: Object.freeze(aggregate),
    scenarios: Object.freeze(scenarioResults),
  });
}
