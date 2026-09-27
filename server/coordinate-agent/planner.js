export class CoordinateAgentPlanner {
  constructor({ maxActionsPerTurn = 4 } = {}) {
    this.maxActionsPerTurn = Math.max(1, Number(maxActionsPerTurn) || 4);
  }

  createPlan(turn, toolRegistry) {
    const requested = Array.isArray(turn?.plan?.actions) ? turn.plan.actions : [];
    if (requested.length > this.maxActionsPerTurn) {
      throw new Error(`Agent requested ${requested.length} tools; limit is ${this.maxActionsPerTurn}`);
    }
    const actions = requested.map((action, index) => {
      const toolName = String(action?.toolName || '');
      if (!toolRegistry.has(toolName)) throw new Error(`Agent requested unavailable tool: ${toolName}`);
      return Object.freeze({
        id: String(action?.id || `action-${index + 1}`),
        toolName,
        objective: String(action?.objective || ''),
        args: Object.freeze({ ...(action?.args || {}) }),
      });
    });
    return Object.freeze({
      rationale: String(turn?.plan?.rationale || ''),
      actions: Object.freeze(actions),
    });
  }
}
