import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { runCoordinateAgentOfflineScenarios } from './lib/coordinate-agent-offline-evaluator.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const result = await runCoordinateAgentOfflineScenarios({
  scenarioPath: path.join(root, 'regression-samples', 'coordinate-agent-phase03', 'mock-scenarios.v1.json'),
});

console.log(JSON.stringify({
  schemaVersion: result.schemaVersion,
  scenarioCount: result.scenarioCount,
  realProviderCallCount: result.realProviderCallCount,
  mockTurnCount: result.mockTurnCount,
  toolCallCount: result.toolCallCount,
  sourceMutationCount: result.sourceMutationCount,
  aggregate: result.aggregate,
  scenarios: result.scenarios.map(item => ({
    id: item.id,
    terminalState: item.result.terminalState,
    authorization: item.result.authorization,
    overallScore: item.score.overallScore,
    dimensions: item.score.dimensions,
    trace: item.trace,
    sourceUnchanged: item.sourceUnchanged,
  })),
}, null, 2));
