import { AGENT_STATE, TERMINAL_STATES } from './constants.js';

const ALLOWED_TRANSITIONS = Object.freeze({
  [AGENT_STATE.INITIALIZED]: new Set([AGENT_STATE.OBSERVING]),
  [AGENT_STATE.OBSERVING]: new Set([
    AGENT_STATE.PLANNING,
    AGENT_STATE.REVIEW_REQUIRED,
    AGENT_STATE.FAILED_CLOSED,
  ]),
  [AGENT_STATE.PLANNING]: new Set([
    AGENT_STATE.ACTING,
    AGENT_STATE.VERIFYING,
    AGENT_STATE.REVIEW_REQUIRED,
    AGENT_STATE.FAILED_CLOSED,
  ]),
  [AGENT_STATE.ACTING]: new Set([AGENT_STATE.RECONCILING, AGENT_STATE.FAILED_CLOSED]),
  [AGENT_STATE.RECONCILING]: new Set([
    AGENT_STATE.OBSERVING,
    AGENT_STATE.VERIFYING,
    AGENT_STATE.REVIEW_REQUIRED,
    AGENT_STATE.FAILED_CLOSED,
  ]),
  [AGENT_STATE.VERIFYING]: new Set([
    AGENT_STATE.CONFIRMED,
    AGENT_STATE.REVIEW_REQUIRED,
    AGENT_STATE.FAILED_CLOSED,
  ]),
  [AGENT_STATE.CONFIRMED]: new Set(),
  [AGENT_STATE.REVIEW_REQUIRED]: new Set(),
  [AGENT_STATE.FAILED_CLOSED]: new Set(),
});

export class CoordinateAgentStateMachine {
  #state = AGENT_STATE.INITIALIZED;
  #history = [{ state: AGENT_STATE.INITIALIZED, reason: 'kernel_created' }];

  get state() {
    return this.#state;
  }

  get terminal() {
    return TERMINAL_STATES.has(this.#state);
  }

  transition(nextState, reason) {
    if (this.terminal) throw new Error(`Cannot transition terminal state ${this.#state}`);
    if (!ALLOWED_TRANSITIONS[this.#state]?.has(nextState)) {
      throw new Error(`Invalid coordinate agent transition ${this.#state} -> ${nextState}`);
    }
    this.#state = nextState;
    this.#history.push({ state: nextState, reason: String(reason || 'unspecified') });
    return this.#state;
  }

  snapshot() {
    return Object.freeze({
      state: this.#state,
      terminal: this.terminal,
      history: Object.freeze(this.#history.map(entry => Object.freeze({ ...entry }))),
    });
  }
}
