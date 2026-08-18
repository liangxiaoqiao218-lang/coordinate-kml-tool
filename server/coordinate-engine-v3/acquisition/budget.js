export const ACQUISITION_TARGET_MS = 30000;
export const ACQUISITION_HARD_DEADLINE_MS = 60000;
export const ACQUISITION_MAX_PROVIDER_CALLS = 2;

function nowMs(clock) {
  return typeof clock === "function" ? Number(clock()) : Date.now();
}

function cleanPositiveNumber(value, fallback) {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? number : fallback;
}

export function createAcquisitionBudget({
  targetMs = ACQUISITION_TARGET_MS,
  hardDeadlineMs = ACQUISITION_HARD_DEADLINE_MS,
  maxProviderCalls = ACQUISITION_MAX_PROVIDER_CALLS,
  providerCalls = 0,
  startedAtMs,
  clock,
} = {}) {
  const safeTargetMs = cleanPositiveNumber(targetMs, ACQUISITION_TARGET_MS);
  const safeHardDeadlineMs = cleanPositiveNumber(hardDeadlineMs, ACQUISITION_HARD_DEADLINE_MS);
  const safeMaxProviderCalls = Math.max(0, Math.floor(cleanPositiveNumber(maxProviderCalls, ACQUISITION_MAX_PROVIDER_CALLS)));
  const safeProviderCalls = Math.max(0, Math.floor(Number(providerCalls) || 0));
  const start = Number.isFinite(Number(startedAtMs)) ? Number(startedAtMs) : nowMs(clock);

  return Object.freeze({
    targetMs: safeTargetMs,
    hardDeadlineMs: safeHardDeadlineMs,
    maxProviderCalls: safeMaxProviderCalls,
    providerCalls: safeProviderCalls,
    startedAtMs: start,
    elapsedMs(currentClock = clock) {
      return Math.max(0, nowMs(currentClock) - start);
    },
    remainingMs(currentClock = clock) {
      return Math.max(0, safeHardDeadlineMs - Math.max(0, nowMs(currentClock) - start));
    },
    canStartProviderCall({ minimumMs = 1000, currentClock = clock } = {}) {
      if (safeProviderCalls >= safeMaxProviderCalls) {
        return Object.freeze({
          allowed: false,
          reason: "PROVIDER_CALL_LIMIT_EXCEEDED",
          providerCalls: safeProviderCalls,
          maxProviderCalls: safeMaxProviderCalls,
        });
      }
      const remainingMs = Math.max(0, safeHardDeadlineMs - Math.max(0, nowMs(currentClock) - start));
      const requiredMs = Math.max(0, Number(minimumMs) || 0);
      if (remainingMs < requiredMs) {
        return Object.freeze({
          allowed: false,
          reason: "ACQUISITION_DEADLINE_INSUFFICIENT",
          remainingMs,
          minimumMs: requiredMs,
        });
      }
      return Object.freeze({
        allowed: true,
        reason: "ACQUISITION_BUDGET_AVAILABLE",
        remainingMs,
        providerCalls: safeProviderCalls,
        maxProviderCalls: safeMaxProviderCalls,
      });
    },
    recordProviderCall({ currentClock = clock } = {}) {
      const gate = this.canStartProviderCall({ currentClock });
      if (!gate.allowed) {
        return Object.freeze({
          accepted: false,
          reason: gate.reason,
          budget: this,
        });
      }
      return Object.freeze({
        accepted: true,
        reason: "PROVIDER_CALL_RECORDED",
        budget: createAcquisitionBudget({
          targetMs: safeTargetMs,
          hardDeadlineMs: safeHardDeadlineMs,
          maxProviderCalls: safeMaxProviderCalls,
          providerCalls: safeProviderCalls + 1,
          startedAtMs: start,
          clock,
        }),
      });
    },
  });
}
