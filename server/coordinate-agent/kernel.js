import { AGENT_STATE, COORDINATE_AGENT_SCHEMA_VERSION } from './constants.js';
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
        evidence.addUncertainty({
          code: 'AGENT_TURN_INVALID',
          message: String(error?.message || 'The agent turn failed strict validation'),
          blocking: true,
        });
        state.transition(AGENT_STATE.FAILED_CLOSED, 'provider_or_turn_validation_failed');
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
      }),
    });
  }
}
