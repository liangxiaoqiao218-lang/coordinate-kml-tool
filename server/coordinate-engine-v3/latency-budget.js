export const COORDINATE_ENGINE_V3_TARGET_MS = 30000;
export const COORDINATE_ENGINE_V3_HARD_DEADLINE_MS = 60000;

function nowMs(clock) {
  return typeof clock === "function" ? Number(clock()) : Date.now();
}

export function createLatencyBudget({
  targetMs = COORDINATE_ENGINE_V3_TARGET_MS,
  hardDeadlineMs = COORDINATE_ENGINE_V3_HARD_DEADLINE_MS,
  startedAtMs,
  clock,
} = {}) {
  const safeTargetMs = Number.isFinite(Number(targetMs)) && Number(targetMs) > 0
    ? Number(targetMs)
    : COORDINATE_ENGINE_V3_TARGET_MS;
  const safeHardDeadlineMs = Number.isFinite(Number(hardDeadlineMs)) && Number(hardDeadlineMs) > 0
    ? Number(hardDeadlineMs)
    : COORDINATE_ENGINE_V3_HARD_DEADLINE_MS;
  const start = Number.isFinite(Number(startedAtMs)) ? Number(startedAtMs) : nowMs(clock);
  return Object.freeze({
    targetMs: safeTargetMs,
    hardDeadlineMs: safeHardDeadlineMs,
    startedAtMs: start,
    elapsedMs(currentClock = clock) {
      return Math.max(0, nowMs(currentClock) - start);
    },
    remainingMs(currentClock = clock) {
      return Math.max(0, safeHardDeadlineMs - Math.max(0, nowMs(currentClock) - start));
    },
    targetExceeded(currentClock = clock) {
      return Math.max(0, nowMs(currentClock) - start) > safeTargetMs;
    },
    deadlineExceeded(currentClock = clock) {
      return Math.max(0, nowMs(currentClock) - start) >= safeHardDeadlineMs;
    },
  });
}

export function allocateRecognizerBudget(parentBudget, requestedMs, { reserveMs = 1000, minimumMs = 1000 } = {}) {
  const requested = Number.isFinite(Number(requestedMs)) && Number(requestedMs) > 0
    ? Number(requestedMs)
    : parentBudget?.targetMs ?? COORDINATE_ENGINE_V3_TARGET_MS;
  const remaining = typeof parentBudget?.remainingMs === "function"
    ? parentBudget.remainingMs()
    : COORDINATE_ENGINE_V3_HARD_DEADLINE_MS;
  const available = Math.max(0, remaining - Math.max(0, Number(reserveMs) || 0));
  const allocatedMs = Math.min(requested, available);
  return Object.freeze({
    allocatedMs,
    canCallProvider: allocatedMs >= Math.max(0, Number(minimumMs) || 0),
    reason: allocatedMs >= Math.max(0, Number(minimumMs) || 0)
      ? "budget_available"
      : "recognizer_budget_exhausted",
  });
}

