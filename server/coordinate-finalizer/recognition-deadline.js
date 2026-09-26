import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";

export const RECOGNITION_DEADLINE_CODE = "RECOGNITION_DEADLINE_EXCEEDED";
export const RECOGNITION_BUDGET_CODE = "RECOGNITION_BUDGET_EXHAUSTED";
export const RECOGNITION_PROVIDER_ATTEMPT_LIMIT_CODE = "RECOGNITION_PROVIDER_ATTEMPT_LIMIT_REACHED";
export const RECOGNITION_LOCAL_OCR_ATTEMPT_LIMIT_CODE = "RECOGNITION_LOCAL_OCR_ATTEMPT_LIMIT_REACHED";
export const DEFAULT_RECOGNITION_HARD_DEADLINE_MS = 55_000;
export const MAX_RECOGNITION_HARD_DEADLINE_MS = 59_000;
export const MAX_ASYNC_RECOGNITION_HARD_DEADLINE_MS = 180_000;
export const DEFAULT_RECOGNITION_PREFLIGHT_DEADLINE_MS = 12_500;
export const DEFAULT_RECOGNITION_EXECUTION_DEADLINE_MS = 42_500;
export const DEFAULT_RECOGNITION_RESPONSE_RESERVE_MS = 2_500;
export const DEFAULT_LOW_VALUE_FALLBACK_CUTOFF_MS = 45_000;
export const DEFAULT_MIN_RECOGNITION_STAGE_MS = 500;
export const RECOGNITION_BUDGET_PHASE = Object.freeze({
  INGRESS_PREFLIGHT: "INGRESS_PREFLIGHT_BUDGET",
  EXECUTION: "RECOGNITION_EXECUTION_BUDGET"
});
export const RECOGNITION_STAGE_RESULTS = Object.freeze(new Set([
  "success",
  "failed",
  "timeout",
  "aborted",
  "skipped",
  "budget_exhausted",
  "not_started"
]));

const SANITIZED_STAGE_NAMES = Object.freeze(new Set([
  "upload",
  "image_prepare",
  "image_safety",
  "permissions",
  "usage_eligibility",
  "pre_route",
  "generic_provider",
  "family_retry",
  "local_ocr",
  "local_ocr_map_layout",
  "local_ocr_map_layout_completeness",
  "handwritten_retry",
  "wgs84_retry",
  "mgrs_retry",
  "kyrgyz_retry",
  "cadastral_retry",
  "cadastral_layout",
  "cadastral_grid",
  "cote_divoire_retry",
  "parser",
  "crs",
  "geometry",
  "verification",
  "finalizer",
  "usage_commit",
  "response"
]));
const SANITIZED_STAGE_REASONS = Object.freeze(new Set([
  "request_aborted",
  "response_already_sent",
  "soft_fallback_cutoff",
  "insufficient_remaining_budget",
  "provider_admission_budget_insufficient",
  "provider_attempt_limit_reached",
  "local_ocr_attempt_limit_reached",
  "stage_timeout",
  "not_started"
]));
const SANITIZED_RESPONSE_CODES = Object.freeze(new Set([
  "OK",
  RECOGNITION_DEADLINE_CODE,
  RECOGNITION_BUDGET_CODE,
  "COORDINATE_IMAGE_INVALID",
  "COORDINATE_POST_PROVIDER_PROCESSING_FAILED",
  "COORDINATE_RECOGNITION_FAILED_CLOSED",
  "PROVIDER_TIMEOUT_AFTER_LOCAL_OCR",
  "ONE_SHOT_ACQUISITION_CONTRACT_REVIEW_REQUIRED",
  "CONVERT_QUOTA_EXHAUSTED",
  "CONVERT_QUOTA_CONSUME_FAILED",
  "USAGE_COMMIT_OUTCOME_UNKNOWN",
  "limit_exceeded",
  "invalid_image",
  "missing_user",
  "regression_test_forbidden",
  "recognition_failed_closed",
  "post_provider_processing_failed",
  "local_ocr_failed",
  "timeout"
]));
const PROVIDER_COMPLETION_STATES = Object.freeze(new Set([
  "NOT_STARTED",
  "SUCCEEDED",
  "FAILED",
  "TIMED_OUT",
  "ABORTED"
]));
const PROVIDER_COST_STATES = Object.freeze(new Set([
  "NOT_INCURRED",
  "POSSIBLY_INCURRED",
  "USAGE_REPORTED"
]));
const USAGE_COMMIT_STATES = Object.freeze(new Set([
  "NOT_STARTED",
  "PREPARING",
  "PREPARED",
  "COMMITTING",
  "COMMITTED",
  "FAILED"
]));
const RECOGNITION_BUDGET_PHASES = Object.freeze(new Set(Object.values(RECOGNITION_BUDGET_PHASE)));
const ACQUISITION_ROUTE_REASONS = Object.freeze(new Set([
  "UNCLASSIFIED",
  "GENERIC_PRIMARY",
  "SPECIALIZED_FAMILY_PRIMARY",
  "WGS84_STRUCTURED_PRIMARY",
  "WGS84_SINGLE_POINT_PRIMARY",
  "LOCAL_OCR_EVIDENCE_ONLY",
  "FAIL_CLOSED"
]));
const ACQUISITION_TERMINAL_STATES = Object.freeze(new Set([
  "NOT_COMPLETED",
  "COMPLETED",
  "FAILED_CLOSED",
  "BUDGET_EXHAUSTED",
  "TIMED_OUT",
  "ABORTED"
]));

function sanitizeEnum(value, allowed, sentinel) {
  return allowed.has(value) ? value : sentinel;
}

function sanitizeRuntimeCommit(value) {
  const normalized = String(value || "").trim();
  return /^[a-f0-9]{40}(?:[a-f0-9]{24})?$/i.test(normalized)
    ? normalized.toLowerCase()
    : "UNLISTED_RUNTIME_COMMIT";
}

function sanitizeRuntimeBranch(value) {
  const normalized = String(value || "").trim();
  return /^[a-z0-9][a-z0-9._/-]{0,119}$/i.test(normalized)
    ? normalized
    : "UNLISTED_RUNTIME_BRANCH";
}

function getUploadSizeBucket(size) {
  const bytes = Number(size);
  if (!Number.isFinite(bytes) || bytes < 0) return "UNLISTED_UPLOAD_SIZE_BUCKET";
  if (bytes === 0) return "EMPTY";
  if (bytes <= 256 * 1024) return "LE_256_KIB";
  if (bytes <= 1024 * 1024) return "LE_1_MIB";
  if (bytes <= 4 * 1024 * 1024) return "LE_4_MIB";
  if (bytes <= 12 * 1024 * 1024) return "LE_12_MIB";
  return "GT_12_MIB";
}

function getDurationBucket(value) {
  const durationMs = Number(value);
  if (!Number.isFinite(durationMs) || durationMs < 0) return "UNLISTED_DURATION";
  if (durationMs <= 1_000) return "LE_1S";
  if (durationMs <= 5_000) return "LE_5S";
  if (durationMs <= 15_000) return "LE_15S";
  if (durationMs <= 30_000) return "LE_30S";
  if (durationMs <= 45_000) return "LE_45S";
  if (durationMs <= 60_000) return "LE_60S";
  return "GT_60S";
}

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
    preflightDeadlineMs = DEFAULT_RECOGNITION_PREFLIGHT_DEADLINE_MS,
    executionDeadlineMs = DEFAULT_RECOGNITION_EXECUTION_DEADLINE_MS,
    responseReserveMs = DEFAULT_RECOGNITION_RESPONSE_RESERVE_MS,
    lowValueFallbackCutoffMs = DEFAULT_LOW_VALUE_FALLBACK_CUTOFF_MS,
    now = () => Date.now(),
    trace = true,
    requestId = randomUUID(),
    caseId = null
  } = {}) {
    this.signal = signal || null;
    this.startedAt = startedAt;
    this.deadlineMs = deadlineMs;
    this.hardDeadlineAt = startedAt + deadlineMs;
    this.preflightDeadlineMs = Math.min(
      Math.max(1, Number(preflightDeadlineMs) || DEFAULT_RECOGNITION_PREFLIGHT_DEADLINE_MS),
      Math.max(1, Math.floor(deadlineMs * 0.25))
    );
    this.executionDeadlineMs = Math.min(
      Math.max(1, Number(executionDeadlineMs) || DEFAULT_RECOGNITION_EXECUTION_DEADLINE_MS),
      Math.max(1, deadlineMs - this.preflightDeadlineMs)
    );
    this.preflightDeadlineAt = startedAt + this.preflightDeadlineMs;
    this.executionDeadlineAt = null;
    this.responseReserveMs = responseReserveMs;
    this.lowValueFallbackCutoffMs = Math.min(lowValueFallbackCutoffMs, deadlineMs);
    this.softFallbackCutoffAt = startedAt + this.lowValueFallbackCutoffMs;
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
    this.budgetPhase = RECOGNITION_BUDGET_PHASE.INGRESS_PREFLIGHT;
    this.executionStartedAt = null;
    this.preflightCompletedAt = null;
    this.postProviderReserveMs = DEFAULT_MIN_RECOGNITION_STAGE_MS * 3;
    this.runtimeCommit = "UNLISTED_RUNTIME_COMMIT";
    this.runtimeBranch = "UNLISTED_RUNTIME_BRANCH";
    this.uploadSizeBucket = "UNLISTED_UPLOAD_SIZE_BUCKET";
    this.providerAttempted = false;
    this.providerAttemptCount = 0;
    this.localOcrAttemptCount = 0;
    this.providerCompletionState = "NOT_STARTED";
    this.providerUsageObserved = false;
    this.providerCostState = "NOT_INCURRED";
    this.userUsageConsumed = false;
    this.usageCommitState = "NOT_STARTED";
    this.initialUploadStage = null;
    this.acquisitionRouteReason = "UNCLASSIFIED";
    this.acquisitionTerminalState = "NOT_COMPLETED";
  }

  elapsedMs() {
    return Math.max(0, this.now() - this.startedAt);
  }

  remainingMs() {
    const phaseDeadlineAt = this.budgetPhase === RECOGNITION_BUDGET_PHASE.EXECUTION
      ? this.executionDeadlineAt
      : this.preflightDeadlineAt;
    return Math.max(0, Math.min(this.hardDeadlineAt, phaseDeadlineAt || this.hardDeadlineAt) - this.now());
  }

  overallRemainingMs() {
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

  setIngressMetadata({ runtimeCommit = null, runtimeBranch = null, uploadSize = null } = {}) {
    this.runtimeCommit = sanitizeRuntimeCommit(runtimeCommit);
    this.runtimeBranch = sanitizeRuntimeBranch(runtimeBranch);
    this.uploadSizeBucket = getUploadSizeBucket(uploadSize);
  }

  startIngressUpload() {
    if (!this.initialUploadStage) this.initialUploadStage = this.stageStarted("upload");
    return this.initialUploadStage;
  }

  completeIngressUpload({ result = "success" } = {}) {
    this.stageCompleted(this.initialUploadStage, { result });
  }

  beginExecutionPhase() {
    if (this.budgetPhase === RECOGNITION_BUDGET_PHASE.EXECUTION) return false;
    if (this.isAborted()) {
      this.recordSkippedStage("pre_route", "request_aborted", "aborted");
      throw createRecognitionBudgetError("Recognition request aborted before execution could start.", {
        code: RECOGNITION_DEADLINE_CODE,
        reason: "request_aborted",
        stageName: "pre_route"
      });
    }
    if (this.responseSentAt !== null) {
      this.recordSkippedStage("pre_route", "response_already_sent", "skipped");
      throw createRecognitionBudgetError("Recognition response was already sent.", {
        reason: "response_already_sent",
        stageName: "pre_route"
      });
    }
    if (this.now() >= Math.min(this.preflightDeadlineAt, this.hardDeadlineAt)) {
      this.recordSkippedStage("pre_route", "insufficient_remaining_budget", "skipped");
      throw createRecognitionBudgetError("Recognition preflight budget was exhausted before execution.", {
        reason: "insufficient_remaining_budget",
        stageName: "pre_route"
      });
    }
    this.preflightCompletedAt = this.elapsedMs();
    this.executionStartedAt = this.now();
    this.executionDeadlineAt = Math.min(
      this.hardDeadlineAt,
      this.executionStartedAt + this.executionDeadlineMs
    );
    this.softFallbackCutoffAt = Math.min(
      this.executionDeadlineAt,
      this.executionStartedAt + this.lowValueFallbackCutoffMs
    );
    this.budgetPhase = RECOGNITION_BUDGET_PHASE.EXECUTION;
    return true;
  }

  assertCanStartProvider({
    stageName = "generic_provider",
    minRequiredMs = DEFAULT_MIN_RECOGNITION_STAGE_MS,
    lowValue = false
  } = {}) {
    if (this.providerAttemptCount >= 1) {
      this.recordSkippedStage(stageName, "provider_attempt_limit_reached", "skipped");
      throw createRecognitionBudgetError("Provider attempt limit reached for this recognition request.", {
        code: RECOGNITION_PROVIDER_ATTEMPT_LIMIT_CODE,
        reason: "provider_attempt_limit_reached",
        stageName
      });
    }
    const providerAndPostProcessingMinimum = Math.max(DEFAULT_MIN_RECOGNITION_STAGE_MS, Number(minRequiredMs) || 0)
      + this.postProviderReserveMs;
    try {
      return this.assertCanContinue({
        stageName,
        minRequiredMs: providerAndPostProcessingMinimum,
        lowValue
      });
    } catch (error) {
      if (error?.code === RECOGNITION_BUDGET_CODE && error?.reason === "insufficient_remaining_budget") {
        error.reason = "provider_admission_budget_insufficient";
        const lastEvent = this.events.at(-1);
        if (lastEvent?.skippedReason === "insufficient_remaining_budget") {
          lastEvent.skippedReason = "provider_admission_budget_insufficient";
        }
      }
      throw error;
    }
  }

  effectiveProviderTimeout(stageCapMs) {
    const cap = Math.max(0, Number(stageCapMs) || 0);
    const usableRemaining = Math.max(
      0,
      this.remainingMs() - this.responseReserveMs - this.postProviderReserveMs
    );
    return Math.min(cap, usableRemaining);
  }

  markProviderAttempted() {
    if (this.providerAttemptCount >= 1) {
      throw createRecognitionBudgetError("Provider attempt limit reached for this recognition request.", {
        code: RECOGNITION_PROVIDER_ATTEMPT_LIMIT_CODE,
        reason: "provider_attempt_limit_reached"
      });
    }
    this.providerAttempted = true;
    this.providerAttemptCount += 1;
    this.providerCompletionState = "NOT_STARTED";
    this.providerCostState = "POSSIBLY_INCURRED";
  }

  markProviderCompleted({ state = "FAILED", usageObserved = false } = {}) {
    this.providerCompletionState = sanitizeEnum(state, PROVIDER_COMPLETION_STATES, "FAILED");
    this.providerUsageObserved = this.providerUsageObserved || usageObserved === true;
    this.providerCostState = this.providerUsageObserved ? "USAGE_REPORTED" : "POSSIBLY_INCURRED";
  }

  assertCanStartLocalOcr({
    stageName = "local_ocr",
    minRequiredMs = DEFAULT_MIN_RECOGNITION_STAGE_MS,
    lowValue = false
  } = {}) {
    if (this.localOcrAttemptCount >= 1) {
      this.recordSkippedStage(stageName, "local_ocr_attempt_limit_reached", "skipped");
      throw createRecognitionBudgetError("Local OCR evidence attempt limit reached for this recognition request.", {
        code: RECOGNITION_LOCAL_OCR_ATTEMPT_LIMIT_CODE,
        reason: "local_ocr_attempt_limit_reached",
        stageName
      });
    }
    this.assertCanContinue({ stageName, minRequiredMs, lowValue });
    this.localOcrAttemptCount += 1;
    return true;
  }

  setAcquisitionRouteReason(reason) {
    this.acquisitionRouteReason = sanitizeEnum(
      String(reason || "").trim(),
      ACQUISITION_ROUTE_REASONS,
      "UNCLASSIFIED"
    );
  }

  setAcquisitionTerminalState(state) {
    this.acquisitionTerminalState = sanitizeEnum(
      String(state || "").trim(),
      ACQUISITION_TERMINAL_STATES,
      "FAILED_CLOSED"
    );
  }

  markUserUsageConsumed(consumed = true) {
    this.userUsageConsumed = consumed === true;
  }

  markUsageCommitState(state) {
    this.usageCommitState = sanitizeEnum(state, USAGE_COMMIT_STATES, "FAILED");
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
      handlerCompleted: false,
      budgetPhase: this.budgetPhase
    };
    this.events.push(event);
    if (this.traceEnabled) console.log("[RecognitionStage]", this.sanitizeLedgerStage(event));
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
    if (this.traceEnabled) console.log("[RecognitionStage]", this.sanitizeLedgerStage(event));
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
      handlerCompleted: false,
      budgetPhase: this.budgetPhase
    };
    this.events.push(event);
    if (this.traceEnabled) console.log("[RecognitionStage]", this.sanitizeLedgerStage(event));
    return event;
  }

  markResponseSent({ httpStatus = null, responseCode = null } = {}) {
    if (this.responseSentAt !== null) return;
    this.responseSentAt = this.elapsedMs();
    this.responseHttpStatus = Number.isInteger(Number(httpStatus)) ? Number(httpStatus) : null;
    const normalizedResponseCode = responseCode ? String(responseCode).slice(0, 120) : null;
    this.responseCode = normalizedResponseCode === null
      ? null
      : sanitizeEnum(normalizedResponseCode, SANITIZED_RESPONSE_CODES, "UNLISTED_RESPONSE_CODE");
    const statusCode = Number(httpStatus);
    if (this.responseCode === RECOGNITION_DEADLINE_CODE || statusCode === 504) {
      this.setAcquisitionTerminalState("TIMED_OUT");
    } else if (this.responseCode === RECOGNITION_BUDGET_CODE) {
      this.setAcquisitionTerminalState("BUDGET_EXHAUSTED");
    } else if (this.isAborted()) {
      this.setAcquisitionTerminalState("ABORTED");
    } else if (statusCode >= 200 && statusCode < 400) {
      this.setAcquisitionTerminalState("COMPLETED");
    } else {
      this.setAcquisitionTerminalState("FAILED_CLOSED");
    }
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
      handlerCompleted: false,
      budgetPhase: this.budgetPhase
    };
    this.events.push(event);
    if (this.traceEnabled) console.log("[RecognitionStage]", this.sanitizeLedgerStage(event));
  }

  sanitizeLedgerStage(event) {
    const traceStage = this.sanitizeStageEvent(event);
    return Object.freeze({
      stageName: traceStage.stageName,
      budgetPhase: traceStage.budgetPhase,
      durationMs: traceStage.durationMs,
      remainingBudgetAtStartMs: traceStage.remainingBudgetAtStartMs,
      remainingBudgetAtEndMs: traceStage.remainingBudgetAtEndMs,
      result: traceStage.result,
      reasonCode: traceStage.skippedReason
    });
  }

  sanitizeStageEvent(event) {
    const stageName = sanitizeEnum(event?.stageName, SANITIZED_STAGE_NAMES, "UNLISTED_STAGE");
    const result = sanitizeEnum(event?.result, RECOGNITION_STAGE_RESULTS, "failed");
    return Object.freeze({
      stageName,
      budgetPhase: sanitizeEnum(
        event?.budgetPhase,
        RECOGNITION_BUDGET_PHASES,
        "UNLISTED_BUDGET_PHASE"
      ),
      attempt: Number.isInteger(event?.attempt) && event.attempt > 0 ? event.attempt : 1,
      stageStartElapsedMs: Number.isFinite(event?.stageStartElapsedMs) ? event.stageStartElapsedMs : null,
      stageEndElapsedMs: Number.isFinite(event?.stageEndElapsedMs) ? event.stageEndElapsedMs : null,
      durationMs: Number.isFinite(event?.stageStartElapsedMs) && Number.isFinite(event?.stageEndElapsedMs)
        ? Math.max(0, event.stageEndElapsedMs - event.stageStartElapsedMs)
        : null,
      configuredTimeoutMs: Number.isFinite(event?.configuredTimeoutMs) ? event.configuredTimeoutMs : null,
      effectiveTimeoutMs: Number.isFinite(event?.effectiveTimeoutMs) ? event.effectiveTimeoutMs : null,
      remainingBudgetAtStartMs: Number.isFinite(event?.remainingBudgetAtStartMs) ? event.remainingBudgetAtStartMs : null,
      remainingBudgetAtEndMs: Number.isFinite(event?.remainingBudgetAtEndMs) ? event.remainingBudgetAtEndMs : null,
      result,
      abortObserved: event?.abortObserved === true,
      skippedReason: event?.skippedReason
        ? sanitizeEnum(event.skippedReason, SANITIZED_STAGE_REASONS, "UNLISTED_STAGE_REASON")
        : null
    });
  }

  markHandlerCompleted() {
    if (this.handlerCompletedAt === null) this.handlerCompletedAt = this.elapsedMs();
    for (const event of this.events) event.handlerCompleted = true;
    if (this.traceEnabled) console.log("[RecognitionLedger]", this.toSanitizedLedger());
    if (this.traceEnabled) console.log("[RecognitionAcquisition]", this.toSanitizedAcquisitionDiagnostic());
  }

  toSanitizedAcquisitionDiagnostic() {
    const executionElapsedMs = this.executionStartedAt === null
      ? 0
      : Math.max(0, this.now() - this.executionStartedAt);
    return Object.freeze({
      schemaVersion: "recognition_acquisition_liveness_v1",
      stageNames: Object.freeze(Array.from(new Set(
        this.events.map(event => this.sanitizeStageEvent(event).stageName)
      ))),
      routeReason: sanitizeEnum(this.acquisitionRouteReason, ACQUISITION_ROUTE_REASONS, "UNCLASSIFIED"),
      timingBucket: getDurationBucket(executionElapsedMs),
      providerCallCount: Math.min(1, Math.max(0, Number(this.providerAttemptCount) || 0)),
      localOcrCallCount: Math.min(1, Math.max(0, Number(this.localOcrAttemptCount) || 0)),
      terminalState: sanitizeEnum(
        this.acquisitionTerminalState,
        ACQUISITION_TERMINAL_STATES,
        "FAILED_CLOSED"
      )
    });
  }

  toSanitizedTrace() {
    const responseElapsedMs = this.responseSentAt;
    const handlerCompletedElapsedMs = this.handlerCompletedAt;
    const stages = this.events.map(event => this.sanitizeStageEvent(event));
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

  toSanitizedLedger() {
    const executionElapsedMs = this.executionStartedAt === null
      ? null
      : Math.max(0, this.now() - this.executionStartedAt);
    return Object.freeze({
      schemaVersion: "recognition_stage_ledger_v2",
      requestId: /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(this.requestId)
        ? this.requestId
        : "UNLISTED_REQUEST_ID",
      runtimeCommit: this.runtimeCommit,
      runtimeBranch: this.runtimeBranch,
      uploadSizeBucket: this.uploadSizeBucket,
      budgetPhase: sanitizeEnum(this.budgetPhase, RECOGNITION_BUDGET_PHASES, "UNLISTED_BUDGET_PHASE"),
      preflightDurationMs: Number.isFinite(this.preflightCompletedAt) ? this.preflightCompletedAt : null,
      executionElapsedMs,
      overallElapsedMs: this.elapsedMs(),
      stages: this.events.map(event => this.sanitizeLedgerStage(event)),
      providerAttempted: this.providerAttempted,
      providerCompletionState: sanitizeEnum(
        this.providerCompletionState,
        PROVIDER_COMPLETION_STATES,
        "FAILED"
      ),
      providerCostState: sanitizeEnum(this.providerCostState, PROVIDER_COST_STATES, "NOT_INCURRED"),
      userUsageConsumed: this.userUsageConsumed,
      usageCommitState: sanitizeEnum(this.usageCommitState, USAGE_COMMIT_STATES, "FAILED"),
      httpStatus: Number.isInteger(this.responseHttpStatus)
        && this.responseHttpStatus >= 100
        && this.responseHttpStatus <= 599
        ? this.responseHttpStatus
        : null,
      responseCode: sanitizeEnum(
        this.responseCode,
        SANITIZED_RESPONSE_CODES,
        this.responseCode === null ? null : "UNLISTED_RESPONSE_CODE"
      )
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

export function recognitionDeadlineMiddleware({
  deadlineMs = getRecognitionHardDeadlineMs(),
  profile = "interactive",
  preflightDeadlineMs,
  executionDeadlineMs,
  responseReserveMs,
  lowValueFallbackCutoffMs
} = {}) {
  const maximumDeadlineMs = profile === "async"
    ? MAX_ASYNC_RECOGNITION_HARD_DEADLINE_MS
    : MAX_RECOGNITION_HARD_DEADLINE_MS;
  if (!Number.isFinite(deadlineMs) || deadlineMs <= 0 || deadlineMs > maximumDeadlineMs) {
    throw new RangeError(profile === "async"
      ? "async_recognition_deadline_must_not_exceed_180000ms"
      : "recognition_deadline_must_be_below_60000ms");
  }
  return function enforceRecognitionDeadline(req, res, next) {
    const controller = new AbortController();
    const startedAt = Date.now();
    const deadlineAt = startedAt + deadlineMs;
    const rawCaseId = String(req.get?.("x-regression-case-id") || "").trim();
    const caseId = /^[a-z0-9][a-z0-9_-]{0,119}$/i.test(rawCaseId) ? rawCaseId : null;
    const suppliedRequestId = String(req.get?.("x-recognition-request-id") || "").trim();
    const requestId = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(suppliedRequestId)
      ? suppliedRequestId.toLowerCase()
      : randomUUID();
    const budget = new RecognitionBudget({
      signal: controller.signal,
      startedAt,
      deadlineMs,
      caseId,
      requestId,
      ...(Number.isFinite(preflightDeadlineMs) ? { preflightDeadlineMs } : {}),
      ...(Number.isFinite(executionDeadlineMs) ? { executionDeadlineMs } : {}),
      ...(Number.isFinite(responseReserveMs) ? { responseReserveMs } : {}),
      ...(Number.isFinite(lowValueFallbackCutoffMs) ? { lowValueFallbackCutoffMs } : {})
    });
    budget.startIngressUpload();
    res.setHeader?.("X-Recognition-Request-Id", budget.requestId);
    let deadlineResponseSent = false;
    const originalJson = res.json.bind(res);
    res.json = function deadlineSafeJson(body) {
      if (deadlineResponseSent && res.headersSent) return res;
      budget.markResponseSent({
        httpStatus: res.statusCode,
        responseCode: body?.code
          || body?.error_code
          || body?.reason
          || (res.statusCode >= 200 && res.statusCode < 400 ? "OK" : null)
      });
      return originalJson(body);
    };
    const timer = setTimeout(() => {
      controller.abort(new Error(RECOGNITION_DEADLINE_CODE));
      if (!res.headersSent) {
        deadlineResponseSent = true;
        const commitOutcomeUnknown = ["PREPARING", "PREPARED", "COMMITTING"].includes(budget.usageCommitState);
        budget.markResponseSent({
          httpStatus: 504,
          responseCode: commitOutcomeUnknown ? "USAGE_COMMIT_OUTCOME_UNKNOWN" : RECOGNITION_DEADLINE_CODE
        });
        res.status(504);
        originalJson(commitOutcomeUnknown ? {
          success: false,
          reason: "usage_commit_outcome_unknown",
          code: "USAGE_COMMIT_OUTCOME_UNKNOWN",
          error: "本次识别已完成，扣次状态仍在确认中。请使用本次请求编号恢复结果，不要重新上传图片。",
          requestId: budget.requestId,
          providerCompletionState: budget.providerCompletionState,
          providerCallCount: Math.min(1, Math.max(0, Number(budget.providerAttemptCount) || 0)),
          usageConsumed: null,
          userUsageConsumed: null,
          recoveryRequired: true,
          retryAllowed: false,
          deadlineMs,
          elapsedMs: Date.now() - startedAt,
          terminationReason: "request_hard_deadline_during_usage_commit"
        } : {
          success: false,
          reason: "timeout",
          code: RECOGNITION_DEADLINE_CODE,
          error: "本次识别未完成，未扣除使用次数。你可以直接重新识别；如仍失败，请向支持人员提供本次请求编号。",
          requestId: budget.requestId,
          providerCompletionState: budget.providerCompletionState,
          providerCallCount: Math.min(1, Math.max(0, Number(budget.providerAttemptCount) || 0)),
          usageConsumed: false,
          userUsageConsumed: false,
          recoveryRequired: false,
          retryAllowed: true,
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
      profile,
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
