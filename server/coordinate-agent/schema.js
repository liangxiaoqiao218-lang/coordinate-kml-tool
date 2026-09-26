import { normalizeAgenticCoordinateResult } from '../agentic-coordinate-recognition/contract.js';
import { AGENT_STATE, COORDINATE_AGENT_SCHEMA_VERSION, TERMINAL_STATES } from './constants.js';

const BBOX_SCHEMA = { type: ['array', 'null'], minItems: 4, maxItems: 4, items: { type: 'number', minimum: 0, maximum: 1 } };
const REGION_SCHEMA = {
  type: 'object', additionalProperties: false, required: ['id', 'kind', 'confidence'],
  properties: { id: { type: 'string' }, kind: { type: 'string' }, bbox: BBOX_SCHEMA, sourceText: { type: 'string' }, semanticRole: { type: 'string' }, confidence: { type: 'number', minimum: 0, maximum: 1 }, provenance: { type: 'string' } },
};
const UNCERTAINTY_SCHEMA = {
  type: 'object', additionalProperties: false, required: ['code', 'message', 'evidenceRegionIds', 'blocking'],
  properties: { code: { type: 'string' }, message: { type: 'string' }, evidenceRegionIds: { type: 'array', items: { type: 'string' } }, blocking: { type: 'boolean' } },
};
const REVIEW_ITEM_SCHEMA = {
  type: 'object', additionalProperties: false, required: ['fieldPath', 'question', 'evidenceRegionIds', 'candidates'],
  properties: { fieldPath: { type: 'string' }, question: { type: 'string' }, evidenceRegionIds: { type: 'array', items: { type: 'string' } }, candidates: { type: 'array', items: { type: 'string' } } },
};
const ACTION_SCHEMA = {
  type: 'object', additionalProperties: false, required: ['id', 'toolName', 'objective', 'args'],
  properties: { id: { type: 'string' }, toolName: { type: 'string' }, objective: { type: 'string' }, args: { type: 'object' } },
};
const POINT_SCHEMA = {
  type: 'object', additionalProperties: false, required: ['sourceText'],
  properties: { label: { type: ['string', 'null'] }, sourceText: { type: 'string' }, x: { type: ['number', 'null'] }, y: { type: ['number', 'null'] }, latitude: { type: ['number', 'null'] }, longitude: { type: ['number', 'null'] }, needsReview: { type: 'boolean' } },
};
const CANDIDATE_SCHEMA = {
  type: 'object', additionalProperties: false,
  required: ['success', 'resultStatus', 'displayText', 'coordinateSystem', 'geometryType', 'groups', 'warnings'],
  properties: {
    contractVersion: { type: 'string' }, success: { type: 'boolean' }, resultStatus: { enum: ['usable', 'needs_review', 'failed'] }, displayText: { type: 'string' }, geometryType: { type: 'string' },
    coordinateSystem: { type: 'object', additionalProperties: false, required: ['kind', 'status'], properties: { kind: { enum: ['geographic', 'projected', 'unknown'] }, name: { type: ['string', 'null'] }, epsg: { type: ['string', 'null'] }, status: { enum: ['identified', 'needs_confirmation', 'unknown'] } } },
    groups: { type: 'array', items: { type: 'object', additionalProperties: false, required: ['points'], properties: { name: { type: ['string', 'null'] }, points: { type: 'array', items: POINT_SCHEMA } } } },
    warnings: { type: 'array', items: { type: 'string' } },
    summary: { type: 'object', additionalProperties: false, required: ['groupCount', 'pointCount'], properties: { groupCount: { type: 'integer', minimum: 0 }, pointCount: { type: 'integer', minimum: 0 } } },
  },
};

export const COORDINATE_AGENT_RESULT_SCHEMA = Object.freeze({
  $id: COORDINATE_AGENT_SCHEMA_VERSION,
  type: 'object',
  additionalProperties: false,
  required: ['schemaVersion', 'terminalState', 'coordinateResult', 'evidence', 'authorization', 'execution'],
  properties: Object.freeze({
    schemaVersion: { const: COORDINATE_AGENT_SCHEMA_VERSION },
    terminalState: { enum: [...TERMINAL_STATES] },
    coordinateResult: { anyOf: [CANDIDATE_SCHEMA, { type: 'null' }] },
    evidence: {
      type: 'object',
      additionalProperties: false,
      required: ['regions', 'uncertainties', 'reviewItems', 'toolResults'],
      properties: {
        regions: { type: 'array', items: REGION_SCHEMA },
        uncertainties: { type: 'array', items: UNCERTAINTY_SCHEMA },
        reviewItems: { type: 'array', items: REVIEW_ITEM_SCHEMA },
        toolResults: { type: 'array' },
      },
    },
    authorization: { type: 'object', additionalProperties: false, required: ['mapAllowed', 'kmlAllowed'], properties: { mapAllowed: { type: 'boolean' }, kmlAllowed: { type: 'boolean' } } },
    execution: {
      type: 'object',
      additionalProperties: false,
      required: ['iterations', 'providerCallCount', 'toolCallCount', 'stateHistory'],
      properties: {
        iterations: { type: 'integer', minimum: 0 },
        providerCallCount: { type: 'integer', minimum: 0 },
        toolCallCount: { type: 'integer', minimum: 0 },
        stateHistory: { type: 'array' },
      },
    },
  }),
});

export const COORDINATE_AGENT_TURN_SCHEMA = Object.freeze({
  $id: 'coordinate-intelligence-agent-turn/v1',
  type: 'object',
  additionalProperties: false,
  required: ['observation', 'plan'],
  properties: {
    observation: { type: 'object', additionalProperties: false, required: ['regions'], properties: { summary: { type: 'string' }, orientationDegrees: { type: ['number', 'null'] }, regions: { type: 'array', items: REGION_SCHEMA } } },
    plan: { type: 'object', additionalProperties: false, required: ['actions'], properties: { rationale: { type: 'string' }, actions: { type: 'array', items: ACTION_SCHEMA } } },
    candidate: CANDIDATE_SCHEMA,
    uncertainties: { type: 'array', items: UNCERTAINTY_SCHEMA },
    reviewItems: { type: 'array', items: REVIEW_ITEM_SCHEMA },
  },
});

function assertExactKeys(value, allowed, path) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`${path} must be an object`);
  const unknown = Object.keys(value).filter(key => !allowed.includes(key));
  if (unknown.length) throw new Error(`${path} contains unknown properties: ${unknown.join(', ')}`);
}

function assertArray(value, path) {
  if (!Array.isArray(value)) throw new Error(`${path} must be an array`);
}

export function assertStrictAgenticCandidate(candidate) {
  assertExactKeys(candidate, ['contractVersion', 'success', 'resultStatus', 'displayText', 'coordinateSystem', 'geometryType', 'groups', 'warnings', 'summary'], 'candidate');
  assertExactKeys(candidate.coordinateSystem, ['kind', 'name', 'epsg', 'status'], 'candidate.coordinateSystem');
  assertArray(candidate.groups, 'candidate.groups');
  candidate.groups.forEach((group, groupIndex) => {
    assertExactKeys(group, ['name', 'points'], `candidate.groups[${groupIndex}]`);
    assertArray(group.points, `candidate.groups[${groupIndex}].points`);
    group.points.forEach((point, pointIndex) => {
      assertExactKeys(
        point,
        ['label', 'sourceText', 'x', 'y', 'latitude', 'longitude', 'needsReview'],
        `candidate.groups[${groupIndex}].points[${pointIndex}]`,
      );
    });
  });
  assertArray(candidate.warnings, 'candidate.warnings');
  if (candidate.summary !== undefined) {
    assertExactKeys(candidate.summary, ['groupCount', 'pointCount'], 'candidate.summary');
  }
  return normalizeAgenticCoordinateResult(candidate);
}

export function assertStrictCoordinateAgentTurn(turn) {
  assertExactKeys(turn, ['observation', 'plan', 'candidate', 'uncertainties', 'reviewItems'], 'turn');
  assertExactKeys(turn.observation, ['summary', 'orientationDegrees', 'regions'], 'turn.observation');
  assertArray(turn.observation.regions, 'turn.observation.regions');
  turn.observation.regions.forEach((region, index) => {
    assertExactKeys(region, ['id', 'kind', 'bbox', 'sourceText', 'semanticRole', 'confidence', 'provenance'], `turn.observation.regions[${index}]`);
  });
  assertExactKeys(turn.plan, ['rationale', 'actions'], 'turn.plan');
  assertArray(turn.plan.actions, 'turn.plan.actions');
  turn.plan.actions.forEach((action, index) => {
    assertExactKeys(action, ['id', 'toolName', 'objective', 'args'], `turn.plan.actions[${index}]`);
  });
  if (turn.uncertainties !== undefined) {
    assertArray(turn.uncertainties, 'turn.uncertainties');
    turn.uncertainties.forEach((item, index) => {
      assertExactKeys(item, ['code', 'message', 'evidenceRegionIds', 'blocking'], `turn.uncertainties[${index}]`);
    });
  }
  if (turn.reviewItems !== undefined) {
    assertArray(turn.reviewItems, 'turn.reviewItems');
    turn.reviewItems.forEach((item, index) => {
      assertExactKeys(item, ['fieldPath', 'question', 'evidenceRegionIds', 'candidates'], `turn.reviewItems[${index}]`);
    });
  }
  if (turn.candidate !== undefined) assertStrictAgenticCandidate(turn.candidate);
  return Object.freeze({ ...turn });
}

export function assertStrictCoordinateAgentResult(value) {
  assertExactKeys(value, ['schemaVersion', 'terminalState', 'coordinateResult', 'evidence', 'authorization', 'execution'], 'result');
  if (value.schemaVersion !== COORDINATE_AGENT_SCHEMA_VERSION) throw new Error('schemaVersion is invalid');
  if (!TERMINAL_STATES.has(value.terminalState)) throw new Error('terminalState is invalid');
  assertExactKeys(value.authorization, ['mapAllowed', 'kmlAllowed'], 'authorization');
  if (typeof value.authorization.mapAllowed !== 'boolean' || typeof value.authorization.kmlAllowed !== 'boolean') {
    throw new Error('authorization flags must be boolean');
  }
  const confirmed = value.terminalState === AGENT_STATE.CONFIRMED;
  if (value.authorization.mapAllowed !== confirmed || value.authorization.kmlAllowed !== confirmed) {
    throw new Error('Map and KML authorization must fail closed unless terminalState is CONFIRMED');
  }
  const coordinateResult = value.coordinateResult === null
    ? null
    : assertStrictAgenticCandidate(value.coordinateResult);
  if (confirmed && (!coordinateResult?.success || coordinateResult.resultStatus !== 'usable')) {
    throw new Error('CONFIRMED requires a usable coordinate result');
  }
  if (!confirmed && coordinateResult?.resultStatus === 'usable') {
    throw new Error('A usable result cannot bypass a non-confirmed terminal state');
  }
  assertExactKeys(value.evidence, ['regions', 'uncertainties', 'reviewItems', 'toolResults'], 'evidence');
  for (const key of ['regions', 'uncertainties', 'reviewItems', 'toolResults']) {
    if (!Array.isArray(value.evidence[key])) throw new Error(`evidence.${key} must be an array`);
  }
  assertExactKeys(value.execution, ['iterations', 'providerCallCount', 'toolCallCount', 'stateHistory'], 'execution');
  for (const key of ['iterations', 'providerCallCount', 'toolCallCount']) {
    if (!Number.isInteger(value.execution[key]) || value.execution[key] < 0) {
      throw new Error(`execution.${key} must be a non-negative integer`);
    }
  }
  if (!Array.isArray(value.execution.stateHistory)) throw new Error('execution.stateHistory must be an array');
  return Object.freeze({
    ...value,
    coordinateResult,
    authorization: Object.freeze({ ...value.authorization }),
    evidence: Object.freeze(value.evidence),
    execution: Object.freeze(value.execution),
  });
}
