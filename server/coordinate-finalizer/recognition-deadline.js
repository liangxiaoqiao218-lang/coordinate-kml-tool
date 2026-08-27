import { AsyncLocalStorage } from "node:async_hooks";

export const RECOGNITION_DEADLINE_CODE = "RECOGNITION_DEADLINE_EXCEEDED";
export const RECOGNITION_BUDGET_CODE = "RECOGNITION_BUDGET_EXHAUSTED";
export const DEFAULT_RECOGNITION_HARD_DEADLINE_MS = 55_000;
export const MAX_RECOGNITION_HARD_DEADLINE_MS = 59_000;
export const DEFAULT_RECOGNITION_RESPONSE_RESERVE_MS = 2_500;
export const DEFAULT_LOW_VALUE_FALLBACK_CUTOFF_MS = 45_000;
export const DEFAULT_MIN_RECOGNITION_STAGE_MS = 500;
export const RECOGNITION_STAGE_RESULTS = Object.freeze(new Set([
  "success",
  "failed",
  "timeout",
  "aborted",
  "skipped",
  "budget_exhausted",
  "not_started"
]));

const recognitionDeadlineStorage = new AsyncLocalStorage();

export function getRecognitionHardDeadlineMs(env = process.env) {
  const configured = Number(env.RECOGNITION_HARD_DEADLINE_MS);
  if (!Number.isFinite(configured) || configured <= 0) return DEFAULT_RECOGNITION_HARD_DEADLINE_MS;
  return Math.min(Math.floor(configured), MAX_RECOGNITION_HARD_DEADLINE_MS);
}

export function getRecognitionDeadlineContext() {
  return recognitionDeadlineStorage.getStore() || null;
}

export function getRecognitionDeadlineSignal() {
  return getRecognitionDeadlineContext()?.signal || null;
}

export function activateRecognitionDeadlineContext(req) {
  const context = req?.recognitionDeadlineContext || null;
  if (context) recognitionDeadlineStorage.enterWith(context);
  return context;
}

export function getRecognitionBudget() {
  return getRecognitionDeadlineContext()?.budget || null;
}

export function isRecognitionStopError(error) {
  return error?.code === RECOGNITION_DEADLINE_CODE
    || error?.code === RECOGNITION_BUDGET_CODE;
}

function createRecognitionBudgetError(message, details = {}) {
  const error = new Error(message);
  error.code = details.code || RECOGNITION_BUDGET_CODE;
  error.reason = details.reason || "budget_exhausted";
  Object.assign(error, details);
  return error;
}

export class RecognitionBudget {
  constructor({
    signal,
    startedAt = Date.now(),
    deadlineMs = DEFAULT_RECOGNITION_HARD_DEADLINE_MS,
    responseReserveMs = DEFAULT_RECOGNITION_RESPONSE_RESERVE_MS,
    lowValueFallbackCutoffMs = DEFAULT_LOW_VALUE_FALLBACK_CUTOFF_MS,
    now = () => Date.now(),
    trace = true,
    requestId = `recognition_${startedAt}_${Math.random().toString(36).slice(2, 10)}`,
    caseId = null
  } = {}) {
    this.signal = signal || null;
    this.startedAt = startedAt;
    this.deadlineMs = deadlineMs;
    this.hardDeadlineAt = startedAt + deadlineMs;
    this.responseReserveMs = responseReserveMs;
    this.softFallbackCutoffAt = startedAt + Math.min(lowValueFallbackCutoffMs, deadlineMs);
    this.now = now;
    this.traceEnabled = trace;
    this.requestId = requestId;
    this.caseId = caseId;
    this.events = [];
    this.nextStageId = 1;
    this.responseSentAt = null;
    this.responseHttpStatus = null;
    this.responseCode = null;
    this.handlerCompletedAt = null;
  }

  elapsedMs() {
    return Math.max(0, this.now() - this.startedAt);
  }

  remainingMs() {
    return Math.max(0, this.hardDeadlineAt - this.now());
  }

  isAborted() {
    return Boolean(this.signal?.aborted);
  }

  canStartStage(minRequiredMs = DEFAULT_MIN_RECOGNITION_STAGE_MS, { lowValue = false } = {}) {
    if (this.isAborted() || this.responseSentAt !== null) return false;
    if (lowValue && this.now() >= this.softFallbackCutoffAt) return false;
    return this.remainingMs() - this.responseReserveMs >= minRequiredMs;
  }

  effectiveTimeout(stageCapMs) {
    const cap = Math.max(0, Number(stageCapMs) || 0);
    const usableRemaining = Math.max(0, this.remainingMs() - this.responseReserveMs);
    return Math.min(cap, usableRemaining);
  }

  assertCanContinue({
    stageName = "recognition",
    minRequiredMs = DEFAULT_MIN_RECOGNITION_STAGE_MS,
    lowValue = false
  } = {}) {
    if (this.isAborted()) {
      this.recordSkippedStage(stageName, "request_aborted", "aborted");
      throw createRecognitionBudgetError("Recognition request was aborted.", {
        code: RECOGNITION_DEADLINE_CODE,
        reason: "request_aborted",
        stageName
      });
    }
    if (this.responseSentAt !== null) {
      this.recordSkippedStage(stageName, "response_already_sent", "skipped");
      throw createRecognitionBudgetError("Recognition response was already sent.", {
        reason: "response_already_sent",
        stageName
      });
    }
    if (lowValue && this.now() >= this.softFallbackCutoffAt) {
      this.recordSkippedStage(stageName, "soft_fallback_cutoff", "budget_exhausted");
      throw createRecognitionBudgetError("Low-value recognition fallback cutoff reached.", {
        reason: "soft_fallback_cutoff",
        stageName
      });
    }
    if (!this.canStartStage(minRequiredMs, { lowValue })) {
      this.recordSkippedStage(stageName, "insufficient_remaining_budget", "budget_exhausted");
      throw createRecognitionBudgetError("Insufficient recognition request budget.", {
        reason: "insufficient_remaining_budget",
        stageName,
        remainingMs: this.remainingMs()
      });
    }
    return true;
  }

  stageStarted(stageName, {
    attempt = 1,
    configuredTimeoutMs = null,
    effectiveTimeoutMs = null
  } = {}) {
    const event = {
      id: this.nextStageId++,
      requestId: this.requestId,
      stageName,
      attempt,
      stageStartElapsedMs: this.elapsedMs(),
      stageEndElapsedMs: null,
      configuredTimeoutMs,
      effectiveTimeoutMs,
      remainingBudgetAtStartMs: this.remainingMs(),
      remainingBudgetAtEndMs: null,
      result: "not_started",
      abortObserved: this.isAborted(),
      skippedReason: null,
      responseSent: this.responseSentAt !== null,
      handlerCompleted: false
    };
    this.events.push(event);
    if (this.traceEnabled) console.log("[RecognitionStage]", event);
    return event;
  }

  stageCompleted(event, { abortObserved = this.isAborted(), result = "success", skippedReason = null } = {}) {
    if (!event || event.stageEndElapsedMs !== null) return;
    event.stageEndElapsedMs = this.elapsedMs();
    event.remainingBudgetAtEndMs = this.remainingMs();
    event.result = RECOGNITION_STAGE_RESULTS.has(result) ? result : "failed";
    event.abortObserved = Boolean(abortObserved);
    event.skippedReason = skippedReason || null;
    event.responseSent = this.responseSentAt !== null;
    if (this.traceEnabled) console.log("[RecognitionStage]", event);
  }

  recordSkippedStage(stageName, skippedReason, result = "skipped") {
    const elapsedMs = this.elapsedMs();
    const remainingMs = this.remainingMs();
    const event = {
      id: this.nextStageId++,
      requestId: this.requestId,
      stageName,
      attempt: 1,
      stageStartElapsedMs: elapsedMs,
      stageEndElapsedMs: elapsedMs,
      configuredTimeoutMs: null,
      effectiveTimeoutMs: null,
      remainingBudgetAtStartMs: remainingMs,
      remainingBudgetAtEndMs: remainingMs,
      result: RECOGNITION_STAGE_RESULTS.has(result) ? result : "skipped",
      abortObserved: this.isAborted(),
      skippedReason: skippedReason || "not_started",
      responseSent: this.responseSentAt !== null,
      handlerCompleted: false
    };
    this.events.push(event);
    if (this.traceEnabled) console.log("[RecognitionStage]", event);
    return event;
  }

  markResponseSent({ httpStatus = null, responseCode = null } = {}) {
    if (this.responseSentAt !== null) return;
    this.responseSentAt = this.elapsedMs();
    this.responseHttpStatus = Number.isInteger(Number(httpStatus)) ? Number(httpStatus) : null;
    this.responseCode = responseCode ? String(responseCode).slice(0, 120) : null;
    const event = {
      id: this.nextStageId++,
      requestId: this.requestId,
      stageName: "response",
      attempt: 1,
      stageStartElapsedMs: this.responseSentAt,
      stageEndElapsedMs: this.responseSentAt,
      configuredTimeoutMs: null,
      effectiveTimeoutMs: null,
      remainingBudgetAtStartMs: this.remainingMs(),
      remainingBudgetAtEndMs: this.remainingMs(),
      result: "success",
      abortObserved: this.isAborted(),
      skippedReason: null,
      responseSent: true,
      handlerCompleted: false
    };
    this.events.push(event);
    if (this.traceEnabled) console.log("[RecognitionStage]", event);
  }

  markHandlerCompleted() {
    if (this.handlerCompletedAt === null) this.handlerCompletedAt = this.elapsedMs();
    for (const event of this.events) event.handlerCompleted = true;
    if (this.traceEnabled) {
      console.log("[RecognitionHandler]", {
        requestId: this.requestId,
        responseSent: this.responseSentAt !== null,
        responseSentElapsedMs: this.responseSentAt,
        handlerCompleted: true,
        handlerCompletedElapsedMs: this.handlerCompletedAt,
        abortObserved: this.isAborted()
      });
    }
  }

  toSanitizedTrace() {
    const responseElapsedMs = this.responseSentAt;
    const handlerCompletedElapsedMs = this.handlerCompletedAt;
    const stages = this.events.map(event => Object.freeze({
      stageName: String(event.stageName || "unknown").slice(0, 80),
      attempt: Number.isInteger(event.attempt) && event.attempt > 0 ? event.attempt : 1,
      stageStartElapsedMs: Number.isFinite(event.stageStartElapsedMs) ? event.stageStartElapsedMs : null,
      stageEndElapsedMs: Number.isFinite(event.stageEndElapsedMs) ? event.stageEndElapsedMs : null,
      durationMs: Number.isFinite(event.stageStartElapsedMs) && Number.isFinite(event.stageEndElapsedMs)
        ? Math.max(0, event.stageEndElapsedMs - event.stageStartElapsedMs)
        : null,
      configuredTimeoutMs: Number.isFinite(event.configuredTimeoutMs) ? event.configuredTimeoutMs : null,
      effectiveTimeoutMs: Number.isFinite(event.effectiveTimeoutMs) ? event.effectiveTimeoutMs : null,
      remainingBudgetAtStartMs: Number.isFinite(event.remainingBudgetAtStartMs) ? event.remainingBudgetAtStartMs : null,
      remainingBudgetAtEndMs: Number.isFinite(event.remainingBudgetAtEndMs) ? event.remainingBudgetAtEndMs : null,
      result: RECOGNITION_STAGE_RESULTS.has(event.result) ? event.result : "failed",
      abortObserved: event.abortObserved === true,
      skippedReason: event.skippedReason ? String(event.skippedReason).slice(0, 120) : null
    }));
    const postResponseStageCount = responseElapsedMs === null
      ? 0
      : stages.filter(stage => stage.stageName !== "response"
        && Number.isFinite(stage.stageStartElapsedMs)
        && stage.stageStartElapsedMs > responseElapsedMs).length;
    return Object.freeze({
      schemaVersion: "recognition_stage_trace_v1",
      requestId: this.requestId,
      caseId: this.caseId,
      requestStartedAt: new Date(this.startedAt).toISOString(),
      stages,
      response: Object.freeze({
        httpStatus: this.responseHttpStatus,
        responseCode: this.responseCode,
        responseElapsedMs
      }),
      handler: Object.freeze({
        handlerCompletedElapsedMs,
        handlerCompletionDeltaMs: Number.isFinite(responseElapsedMs) && Number.isFinite(handlerCompletedElapsedMs)
          ? Math.max(0, handlerCompletedElapsedMs - responseElapsedMs)
          : null
      }),
      postResponseStageCount,
      postDeadlineWorkStatus: handlerCompletedElapsedMs !== null && postResponseStageCount === 0
        ? "PROVEN_NONE"
        : "UNPROVEN"
    });
  }
}

export function assertRecognitionCanContinue(options = {}) {
  const budget = getRecognitionBudget();
  if (budget) budget.assertCanContinue(options);
  return budget;
}

export function composeAbortSignals(signals = []) {
  const active = signals.filter(Boolean);
  const controller = new AbortController();
  const listeners = [];
  const abort = signal => {
    if (!controller.signal.aborted) controller.abort(signal?.reason || new Error(RECOGNITION_DEADLINE_CODE));
  };
  for (const signal of active) {
    if (signal.aborted) {
      abort(signal);
      break;
    }
    const listener = () => abort(signal);
    signal.addEventListener("abort", listener, { once: true });
    listeners.push([signal, listener]);
  }
  return {
    signal: controller.signal,
    cleanup() {
      for (const [signal, listener] of listeners) signal.removeEventListener("abort", listener);
    }
  };
}

export function recognitionDeadlineMiddleware({ deadlineMs = getRecognitionHardDeadlineMs() } = {}) {
  if (!Number.isFinite(deadlineMs) || deadlineMs <= 0 || deadlineMs >= 60_000) {
    throw new RangeError("recognition_deadline_must_be_below_60000ms");
  }
  return function enforceRecognitionDeadline(req, res, next) {
    const controller = new AbortController();
    const startedAt = Date.now();
    const deadlineAt = startedAt + deadlineMs;
    const rawCaseId = String(req.get?.("x-regression-case-id") || "").trim();
    const caseId = /^[a-z0-9][a-z0-9_-]{0,119}$/i.test(rawCaseId) ? rawCaseId : null;
    const budget = new RecognitionBudget({ signal: controller.signal, startedAt, deadlineMs, caseId });
    res.setHeader?.("X-Recognition-Request-Id", budget.requestId);
    let deadlineResponseSent = false;
    const originalJson = res.json.bind(res);
    res.json = function deadlineSafeJson(body) {
      if (deadlineResponseSent && res.headersSent) return res;
      budget.markResponseSent({
        httpStatus: res.statusCode,
        responseCode: body?.code || body?.error_code || body?.reason || null
      });
      return originalJson(body);
    };
    const timer = setTimeout(() => {
      controller.abort(new Error(RECOGNITION_DEADLINE_CODE));
      if (!res.headersSent) {
        deadlineResponseSent = true;
        budget.markResponseSent({ httpStatus: 504, responseCode: RECOGNITION_DEADLINE_CODE });
        res.status(504);
        originalJson({
          success: false,
          reason: "timeout",
          code: RECOGNITION_DEADLINE_CODE,
          error: "Coordinate recognition exceeded the request deadline.",
          deadlineMs,
          elapsedMs: Date.now() - startedAt,
          terminationReason: "request_hard_deadline",
          providerCancellationState: "aborted"
        });
      }
    }, deadlineMs);
    timer.unref?.();
    const cleanup = () => clearTimeout(timer);
    res.once?.("finish", cleanup);
    res.once?.("close", cleanup);
    const context = Object.freeze({
      signal: controller.signal,
      startedAt,
      deadlineAt,
      deadlineMs,
      budget
    });
    Object.defineProperty(req, "recognitionDeadlineContext", {
      configurable: false,
      enumerable: false,
      writable: false,
      value: context
    });
    recognitionDeadlineStorage.run(context, next);
  };
}
