import { AGENT_STATE, COORDINATE_AGENT_SCHEMA_VERSION, GENERIC_TOOL_NAMES } from './constants.js';
import { CoordinateEvidenceBoard } from './evidence-board.js';
import { CoordinateAgentPlanner } from './planner.js';
import { assertStrictCoordinateAgentResult, assertStrictCoordinateAgentTurn } from './schema.js';
import { buildFailClosedAuthorization, validateCoordinateCandidate } from './safety-bridge.js';
import { CoordinateAgentStateMachine } from './state-machine.js';

function addTurnEvidence(board, turn) {
  for (const region of turn?.observation?.regions || []) board.addRegion(region);
  for (const item of turn?.uncertainties || []) board.addUncertainty(item);
  for (const item of turn?.reviewItems || []) board.addReviewItem(item);
}

function chooseTerminalState(candidate, board) {
  if (!candidate?.success) return AGENT_STATE.FAILED_CLOSED;
  if (candidate.resultStatus === 'needs_review' || board.hasBlockingUncertainty()) {
    return AGENT_STATE.REVIEW_REQUIRED;
  }
  return candidate.resultStatus === 'usable'
    ? AGENT_STATE.CONFIRMED
    : AGENT_STATE.FAILED_CLOSED;
}

function safeFailureDiagnostic(error) {
  const code = String(error?.code || 'AGENT_ADAPTER_ERROR').slice(0, 120);
  const transport = code.startsWith('DASHSCOPE_');
  const structuredOutput = code.startsWith('PROVIDER_STRUCTURED_OUTPUT_');
  const schema = code === 'COORDINATE_AGENT_SCHEMA_VALIDATION_FAILED';
  const path = typeof error?.path === 'string' ? error.path.slice(0, 240) : null;
  return Object.freeze({
    category: transport ? 'transport' : structuredOutput ? 'structured_output' : schema ? 'schema' : 'adapter',
    code,
    path,
    field: typeof error?.field === 'string'
      ? error.field.slice(0, 120)
      : path?.split('.').at(-1)?.replace(/\[\d+\]$/u, '') || null,
    expectedType: typeof error?.expectedType === 'string' ? error.expectedType.slice(0, 160) : null,
    actualType: typeof error?.actualType === 'string' ? error.actualType.slice(0, 80) : null,
    httpStatus: Number.isInteger(error?.status) ? error.status : null,
  });
}

function candidatePoints(candidate) {
  return (candidate?.groups || []).flatMap(group => group.points || []);
}

function downgradeCandidateForReview(candidate) {
  if (!candidate || candidate.resultStatus !== 'usable') return candidate;
  return validateCoordinateCandidate({
    ...candidate,
    resultStatus: 'needs_review',
    geometryType: 'Unknown',
    groups: candidate.groups.map(group => ({
      ...group,
      points: group.points.map(point => ({ ...point, needsReview: true })),
    })),
    warnings: [...new Set([
      ...(candidate.warnings || []),
      'Deterministic coordinate verification requires review',
    ])],
  });
}

function promoteProjectedCandidate(candidate, transformedPoints) {
  let index = 0;
  return validateCoordinateCandidate({
    ...candidate,
    resultStatus: 'usable',
    groups: candidate.groups.map(group => ({
      ...group,
      points: group.points.map(point => {
        const transformed = transformedPoints[index];
        index += 1;
        return {
          ...point,
          latitude: transformed.latitude,
          longitude: transformed.longitude,
          needsReview: false,
        };
      }),
    })),
  });
}

async function prepareProjectedCandidate({ candidate, board, toolRegistry, imageRef, requestId }) {
  if (candidate?.coordinateSystem?.kind !== 'projected'
    || candidate.coordinateSystem.status !== 'identified') {
    return Object.freeze({ candidate, toolCallCount: 0, projectionVerified: false });
  }
  const points = candidatePoints(candidate);
  const providerEvidence = board.snapshot();
  const action = {
    id: 'safety-projection-transform',
    toolName: GENERIC_TOOL_NAMES.PROJECTED_COORDINATE_TRANSFORM_CHECK,
    args: {
      coordinateSystem: {
        name: candidate.coordinateSystem.name,
        epsg: candidate.coordinateSystem.epsg,
      },
      // The provider-neutral candidate contract defines x as easting and y as northing.
      axisOrder: 'easting_northing',
      points: points.map(point => ({ x: point.x, y: point.y })),
    },
  };
  let output;
  try {
    output = await toolRegistry.execute(action, { imageRef, requestId, safetyVerification: true });
    board.addToolResult({ actionId: action.id, toolName: action.toolName, ok: true, output });
  } catch (error) {
    board.addToolResult({ actionId: action.id, toolName: action.toolName, ok: false, error: error?.message });
    board.addUncertainty({
      code: 'PROJECTED_TRANSFORM_EXECUTION_FAILED',
      message: 'The explicit projected coordinate candidate could not complete deterministic transformation',
      blocking: true,
    });
    return Object.freeze({ candidate: downgradeCandidateForReview(candidate), toolCallCount: 1, projectionVerified: false });
  }

  const canPromote = output?.valid === true
    && output.transformedPointCount === points.length
    && output.roundTripVerifiedPointCount === points.length
    && providerEvidence.uncertainties.every(item => item.blocking === false)
    && providerEvidence.reviewItems.length === 0
    && candidate.geometryType !== 'Unknown';
  if (!canPromote) {
    board.addUncertainty({
      code: String(output?.failureCode || 'PROJECTED_DETERMINISTIC_VERIFICATION_FAILED'),
      message: 'The projected coordinate candidate did not satisfy every deterministic CRS, axis, transformation, and round-trip gate',
      blocking: true,
    });
    return Object.freeze({ candidate: downgradeCandidateForReview(candidate), toolCallCount: 1, projectionVerified: false });
  }
  return Object.freeze({
    candidate: promoteProjectedCandidate(candidate, output.transformedPoints),
    toolCallCount: 1,
    projectionVerified: true,
  });
}

async function runDeterministicCandidateVerification({ candidate, board, toolRegistry, imageRef, requestId }) {
  if (!candidate?.success) return Object.freeze({ candidate, toolCallCount: 0 });
  const projectedPreparation = await prepareProjectedCandidate({
    candidate,
    board,
    toolRegistry,
    imageRef,
    requestId,
  });
  candidate = projectedPreparation.candidate;
  const points = candidatePoints(candidate);
  let toolCallCount = projectedPreparation.toolCallCount;
  let verified = points.length > 0;

  const coordinateIdentityVerified = (
    candidate.coordinateSystem?.kind === 'geographic'
    && candidate.coordinateSystem?.status === 'identified'
  ) || projectedPreparation.projectionVerified;
  if (!coordinateIdentityVerified) {
    verified = false;
    board.addUncertainty({
      code: 'DETERMINISTIC_GEOGRAPHIC_CRS_UNAVAILABLE',
      message: 'Candidate authorization requires an explicitly identified geographic CRS or a fully verified projected-to-geographic conversion',
      blocking: true,
    });
  }

  if (points.length === 0) {
    board.addUncertainty({
      code: 'DETERMINISTIC_COORDINATE_VERIFICATION_MISSING',
      message: 'No candidate coordinate is available for deterministic verification',
      blocking: true,
    });
  }

  for (const [index, point] of points.entries()) {
    const latitude = point?.latitude;
    const longitude = point?.longitude;
    if (typeof latitude !== 'number' || !Number.isFinite(latitude)
      || typeof longitude !== 'number' || !Number.isFinite(longitude)) {
      verified = false;
      board.addUncertainty({
        code: 'DETERMINISTIC_GEOGRAPHIC_PAIR_UNAVAILABLE',
        message: `Candidate point ${index + 1} cannot be authorized without an explicit geographic pair`,
        blocking: true,
      });
      continue;
    }
    const action = {
      id: `safety-math-${index + 1}`,
      toolName: GENERIC_TOOL_NAMES.COORDINATE_MATH_CHECK,
      args: { latitude, longitude },
    };
    try {
      const output = await toolRegistry.execute(action, { imageRef, requestId, safetyVerification: true });
      board.addToolResult({ actionId: action.id, toolName: action.toolName, ok: true, output });
      if (output?.valid !== true) verified = false;
    } catch (error) {
      verified = false;
      board.addToolResult({ actionId: action.id, toolName: action.toolName, ok: false, error: error?.message });
    }
    toolCallCount += 1;
  }

  if (points.length > 1) {
    const action = {
      id: 'safety-spatial-set',
      toolName: GENERIC_TOOL_NAMES.SPATIAL_CONSISTENCY_CHECK,
      args: { points: points.map(point => ({ latitude: point.latitude, longitude: point.longitude })) },
    };
    try {
      const output = await toolRegistry.execute(action, { imageRef, requestId, safetyVerification: true });
      board.addToolResult({ actionId: action.id, toolName: action.toolName, ok: true, output });
      if (output?.valid !== true || output?.pointCount !== points.length) verified = false;
    } catch (error) {
      verified = false;
      board.addToolResult({ actionId: action.id, toolName: action.toolName, ok: false, error: error?.message });
    }
    toolCallCount += 1;
  }

  if (!verified) {
    board.addUncertainty({
      code: 'DETERMINISTIC_COORDINATE_VERIFICATION_FAILED',
      message: 'Candidate coordinates did not complete deterministic mathematical and spatial verification',
      blocking: true,
    });
    board.addReviewItem({
      fieldPath: 'coordinateGroups',
      question: 'Review the candidate coordinates that could not complete deterministic verification',
      evidenceRegionIds: [],
      candidates: [],
    });
  }

  return Object.freeze({
    candidate: verified ? candidate : downgradeCandidateForReview(candidate),
    toolCallCount,
  });
}

export class CoordinateIntelligenceAgentKernel {
  constructor({ providerAdapter, toolRegistry, planner = new CoordinateAgentPlanner(), maxProviderCalls = 2, maxIterations = 3 } = {}) {
    if (typeof providerAdapter?.runTurn !== 'function') throw new Error('providerAdapter.runTurn is required');
    if (typeof toolRegistry?.execute !== 'function') throw new Error('toolRegistry is required');
    this.providerAdapter = providerAdapter;
    this.toolRegistry = toolRegistry;
    this.planner = planner;
    this.maxProviderCalls = Math.max(1, Number(maxProviderCalls) || 2);
    this.maxIterations = Math.max(1, Number(maxIterations) || 3);
  }

  async run({ imageRef, requestId = null } = {}) {
    if (!String(imageRef || '').trim()) throw new Error('imageRef is required');
    const state = new CoordinateAgentStateMachine();
    const evidence = new CoordinateEvidenceBoard();
    let providerCallCount = 0;
    let toolCallCount = 0;
    let iterations = 0;
    let candidate = null;
    let pendingToolResults = [];
    const diagnostics = [];

    state.transition(AGENT_STATE.OBSERVING, 'begin_whole_image_observation');
    while (!state.terminal && iterations < this.maxIterations) {
      iterations += 1;
      if (providerCallCount >= this.maxProviderCalls) {
        state.transition(
          candidate?.success ? AGENT_STATE.REVIEW_REQUIRED : AGENT_STATE.FAILED_CLOSED,
          'provider_call_budget_exhausted',
        );
        break;
      }

      let turn;
      try {
        turn = assertStrictCoordinateAgentTurn(await this.providerAdapter.runTurn(Object.freeze({
          requestId,
          imageRef: String(imageRef),
          iteration: iterations,
          state: state.state,
          tools: this.toolRegistry.definitions(),
          evidence: evidence.snapshot(),
          toolResults: Object.freeze(pendingToolResults),
        })));
        providerCallCount += 1;
      } catch (error) {
        providerCallCount += 1;
        const diagnostic = safeFailureDiagnostic(error);
        diagnostics.push(diagnostic);
        evidence.addUncertainty({
          code: diagnostic.category === 'transport' ? 'PROVIDER_TRANSPORT_FAILED' : 'AGENT_TURN_INVALID',
          message: diagnostic.category === 'transport'
            ? `Provider transport failed (${diagnostic.code}${diagnostic.httpStatus ? ` HTTP ${diagnostic.httpStatus}` : ''})`
            : diagnostic.category === 'schema' || diagnostic.category === 'structured_output'
              ? String(error?.message || `Agent turn failed strict validation at ${diagnostic.path || 'unknown path'}`)
              : `Agent adapter failed at ${diagnostic.path || 'unknown path'}`,
          blocking: true,
        });
        state.transition(
          AGENT_STATE.FAILED_CLOSED,
          diagnostic.category === 'transport' ? 'provider_transport_failed' : 'agent_turn_validation_failed',
        );
        break;
      }
      try {
        addTurnEvidence(evidence, turn);
        if (turn?.candidate) candidate = validateCoordinateCandidate(turn.candidate);
      } catch (error) {
        evidence.addUncertainty({
          code: 'AGENT_EVIDENCE_OR_CANDIDATE_INVALID',
          message: String(error?.message || 'Agent evidence or candidate validation failed'),
          blocking: true,
        });
        state.transition(AGENT_STATE.FAILED_CLOSED, 'evidence_or_candidate_validation_failed');
        break;
      }

      state.transition(AGENT_STATE.PLANNING, 'observation_received');
      let plan;
      try {
        plan = this.planner.createPlan(turn, this.toolRegistry);
      } catch (error) {
        evidence.addUncertainty({
          code: 'AGENT_PLAN_INVALID',
          message: String(error?.message || 'The agent plan failed validation'),
          blocking: true,
        });
        state.transition(AGENT_STATE.FAILED_CLOSED, 'tool_plan_validation_failed');
        break;
      }
      if (plan.actions.length > 0) {
        if (providerCallCount >= this.maxProviderCalls) {
          state.transition(
            candidate?.success ? AGENT_STATE.REVIEW_REQUIRED : AGENT_STATE.FAILED_CLOSED,
            'tool_plan_requires_unavailable_follow_up',
          );
          break;
        }
        state.transition(AGENT_STATE.ACTING, 'execute_generic_local_tools');
        pendingToolResults = [];
        for (const action of plan.actions) {
          try {
            const output = await this.toolRegistry.execute(action, { imageRef, requestId });
            const recorded = evidence.addToolResult({ actionId: action.id, toolName: action.toolName, ok: true, output });
            pendingToolResults.push(recorded);
          } catch (error) {
            const recorded = evidence.addToolResult({ actionId: action.id, toolName: action.toolName, ok: false, error: error?.message });
            pendingToolResults.push(recorded);
          }
          toolCallCount += 1;
        }
        state.transition(AGENT_STATE.RECONCILING, 'local_tool_results_collected');
        state.transition(AGENT_STATE.OBSERVING, 'conditional_visual_follow_up');
        continue;
      }

      state.transition(AGENT_STATE.VERIFYING, 'no_more_tool_actions');
      const deterministicVerification = await runDeterministicCandidateVerification({
        candidate,
        board: evidence,
        toolRegistry: this.toolRegistry,
        imageRef,
        requestId,
      });
      candidate = deterministicVerification.candidate;
      toolCallCount += deterministicVerification.toolCallCount;
      const terminalState = chooseTerminalState(candidate, evidence);
      state.transition(terminalState, 'deterministic_safety_verification');
    }

    if (!state.terminal) {
      state.transition(
        candidate?.success ? AGENT_STATE.REVIEW_REQUIRED : AGENT_STATE.FAILED_CLOSED,
        'iteration_budget_exhausted',
      );
    }

    const snapshot = state.snapshot();
    return assertStrictCoordinateAgentResult({
      schemaVersion: COORDINATE_AGENT_SCHEMA_VERSION,
      terminalState: snapshot.state,
      coordinateResult: candidate,
      evidence: evidence.snapshot(),
      authorization: buildFailClosedAuthorization(snapshot.state),
      execution: Object.freeze({
        iterations,
        providerCallCount,
        toolCallCount,
        stateHistory: snapshot.history,
        diagnostics: Object.freeze(diagnostics),
      }),
    });
  }
}
