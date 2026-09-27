import { AGENT_STATE } from './constants.js';
import { assertStrictCoordinateAgentResult } from './schema.js';

const NUMERIC_FIELDS = Object.freeze(['latitude', 'longitude', 'x', 'y']);

function ratio(matched, total) {
  return total === 0 ? 1 : matched / total;
}

function dimension(matched, total) {
  return Object.freeze({ matched, total, score: ratio(matched, total) });
}

function pointCount(groups) {
  return groups.reduce((sum, group) => sum + group.points.length, 0);
}

function normalizedToken(value) {
  return String(value || '').trim().toLocaleUpperCase();
}

export function scoreCoordinateAgentEvaluation({ actual, expected, numericTolerance = 1e-8 } = {}) {
  const result = assertStrictCoordinateAgentResult(actual);
  if (!expected || typeof expected !== 'object' || Array.isArray(expected)) {
    throw new Error('expected evaluation data must be an object');
  }
  const expectedGroups = Array.isArray(expected.groups) ? expected.groups : [];
  const actualGroups = result.coordinateResult?.groups || [];

  const structure = dimension(
    Number(Boolean(result.schemaVersion && result.terminalState && result.evidence && result.execution)),
    1,
  );

  const expectedRows = expectedGroups.reduce((sum, group) => sum + (group.points?.length || 0), 0);
  const actualRows = pointCount(actualGroups);
  const rows = dimension(Number(actualRows === expectedRows), 1);

  let numericMatched = 0;
  let numericTotal = 0;
  let directionMatched = 0;
  let directionTotal = 0;
  expectedGroups.forEach((expectedGroup, groupIndex) => {
    const actualGroup = actualGroups[groupIndex];
    (expectedGroup.points || []).forEach((expectedPoint, pointIndex) => {
      const actualPoint = actualGroup?.points?.[pointIndex];
      for (const field of NUMERIC_FIELDS) {
        if (expectedPoint[field] === undefined || expectedPoint[field] === null) continue;
        numericTotal += 1;
        const difference = Math.abs(Number(actualPoint?.[field]) - Number(expectedPoint[field]));
        if (Number.isFinite(difference) && difference <= numericTolerance) numericMatched += 1;
      }
      for (const token of expectedPoint.directionTokens || []) {
        directionTotal += 1;
        if (normalizedToken(actualPoint?.sourceText).includes(normalizedToken(token))) {
          directionMatched += 1;
        }
      }
    });
  });

  const expectedReviewPaths = (expected.reviewFieldPaths || []).map(String).sort();
  const actualReviewPaths = result.evidence.reviewItems.map(item => item.fieldPath).sort();
  for (const fieldPath of expected.unresolvedDirectionFieldPaths || []) {
    directionTotal += 1;
    if (actualReviewPaths.includes(String(fieldPath))) directionMatched += 1;
  }

  let groupingMatched = Number(actualGroups.length === expectedGroups.length);
  let groupingTotal = 1;
  expectedGroups.forEach((expectedGroup, index) => {
    groupingTotal += 1;
    if ((actualGroups[index]?.points?.length || 0) === (expectedGroup.points?.length || 0)) {
      groupingMatched += 1;
    }
  });

  let uncertaintyMatched = Number(result.terminalState === expected.terminalState);
  let uncertaintyTotal = 1;
  for (const fieldPath of expectedReviewPaths) {
    uncertaintyTotal += 1;
    if (actualReviewPaths.includes(fieldPath)) uncertaintyMatched += 1;
  }

  const shouldAllowSpatial = expected.terminalState === AGENT_STATE.CONFIRMED;
  const mapKmlSafety = dimension(Number(
    result.authorization.mapAllowed === shouldAllowSpatial
      && result.authorization.kmlAllowed === shouldAllowSpatial,
  ), 1);

  const dimensions = Object.freeze({
    structure,
    rows,
    numeric: dimension(numericMatched, numericTotal),
    direction: dimension(directionMatched, directionTotal),
    grouping: dimension(groupingMatched, groupingTotal),
    uncertainty: dimension(uncertaintyMatched, uncertaintyTotal),
    mapKmlSafety,
  });
  const scores = Object.values(dimensions).map(item => item.score);

  return Object.freeze({
    schemaVersion: 'coordinate-agent-evaluation-score/v1',
    dimensions,
    overallScore: scores.reduce((sum, score) => sum + score, 0) / scores.length,
    actualRowCount: actualRows,
    expectedRowCount: expectedRows,
  });
}

export function buildCoordinateAgentExecutionTrace(result) {
  const actual = assertStrictCoordinateAgentResult(result);
  return Object.freeze({
    terminalState: actual.terminalState,
    states: Object.freeze(actual.execution.stateHistory.map(item => Object.freeze({
      state: item.state,
      reason: item.reason,
    }))),
    tools: Object.freeze(actual.evidence.toolResults.map(item => Object.freeze({
      actionId: item.actionId,
      toolName: item.toolName,
      ok: item.ok,
    }))),
    mockTurnCount: actual.execution.providerCallCount,
    toolCallCount: actual.execution.toolCallCount,
    realProviderCallCount: 0,
  });
}
