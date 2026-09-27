import { runCoordinateAgentOfflineScenarios } from './coordinate-agent-offline-evaluator.js';

function publicScenarioResult(scenario) {
  return Object.freeze({
    terminalState: scenario.result.terminalState,
    dimensions: scenario.score.dimensions,
    overallScore: scenario.score.overallScore,
    execution: Object.freeze({
      replayTurnCount: scenario.trace.mockTurnCount,
      toolCallCount: scenario.trace.toolCallCount,
      realProviderCallCount: 0,
    }),
    authorization: scenario.result.authorization,
  });
}

export async function createCoordinateAgentReplayShadowEvaluator({ scenarioPaths = [] } = {}) {
  if (!Array.isArray(scenarioPaths) || scenarioPaths.length === 0) {
    throw new Error('At least one replay scenario path is required');
  }
  const evaluations = await Promise.all(
    scenarioPaths.map(scenarioPath => runCoordinateAgentOfflineScenarios({ scenarioPath })),
  );
  if (evaluations.some(evaluation => evaluation.realProviderCallCount !== 0)) {
    throw new Error('Shadow evaluation cannot use a real Provider');
  }
  const scenarios = evaluations.flatMap(evaluation => evaluation.scenarios);
  const byId = new Map();
  for (const scenario of scenarios) {
    if (byId.has(scenario.id)) throw new Error(`Duplicate shadow scenario id: ${scenario.id}`);
    byId.set(scenario.id, publicScenarioResult(scenario));
  }

  return Object.freeze({
    caseIds: Object.freeze([...byId.keys()].sort()),
    metrics: Object.freeze({
      scenarioCount: scenarios.length,
      replayTurnCount: scenarios.reduce((sum, scenario) => sum + scenario.trace.mockTurnCount, 0),
      toolCallCount: scenarios.reduce((sum, scenario) => sum + scenario.trace.toolCallCount, 0),
      realProviderCallCount: 0,
      sourceMutationCount: 0,
    }),
    async evaluateCase({ caseId } = {}) {
      const result = byId.get(String(caseId || ''));
      if (!result) throw new Error('The requested evaluation case is not registered');
      return result;
    },
  });
}
