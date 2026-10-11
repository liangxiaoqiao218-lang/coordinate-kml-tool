export const LOCAL_OCR_DIAGNOSTIC_SCHEMA = "local_ocr_diagnostics_v1";

const PHASES = new Set([
  "NOT_STARTED",
  "BUDGET_PRECHECK_SKIPPED",
  "WRAPPER_ENTERED",
  "PREPROCESSING",
  "WORKER_CREATE_STARTED",
  "WORKER_READY",
  "RECOGNIZE_STARTED",
  "RESULT_RECEIVED",
  "POSTPROCESS_STARTED",
  "POSTPROCESS_COMPLETED",
  "CONTEXT_RETURNED"
]);

const OUTCOMES = new Set([
  "NOT_ATTEMPTED",
  "INPUT_NOT_AVAILABLE",
  "BUDGET_PRECHECK_SKIPPED",
  "IN_PROGRESS",
  "SUCCESS_WITH_CONTEXT",
  "SUCCESS_EMPTY",
  "WORKER_CREATE_ERROR",
  "RECOGNIZE_ERROR",
  "POSTPROCESS_ERROR_AFTER_RESULT",
  "TIMEOUT",
  "DEADLINE_ABORTED"
]);

const PREPROCESSING_MODES = new Set([
  "NOT_STARTED",
  "SOURCE_IMAGE",
  "WIDE_HEADER_CROP",
  "OVERVIEW_FOOTER_COMPOSITE",
  "FALLBACK_SOURCE_IMAGE",
  "UNKNOWN"
]);

function safeEnum(value, allowed, fallback) {
  const normalized = String(value || "").trim().toUpperCase();
  return allowed.has(normalized) ? normalized : fallback;
}

function safeBoolean(value) {
  return value === true;
}

export function sanitizeLocalOcrDiagnostics(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)
    || value.schemaVersion !== LOCAL_OCR_DIAGNOSTIC_SCHEMA) return null;
  return Object.freeze({
    schemaVersion: LOCAL_OCR_DIAGNOSTIC_SCHEMA,
    phase: safeEnum(value.phase, PHASES, "NOT_STARTED"),
    outcome: safeEnum(value.outcome, OUTCOMES, "NOT_ATTEMPTED"),
    preprocessingMode: safeEnum(value.preprocessingMode, PREPROCESSING_MODES, "UNKNOWN"),
    attempted: safeBoolean(value.attempted),
    wrapperEntered: safeBoolean(value.wrapperEntered),
    workerCreateStarted: safeBoolean(value.workerCreateStarted),
    workerReady: safeBoolean(value.workerReady),
    recognizeStarted: safeBoolean(value.recognizeStarted),
    resultReceived: safeBoolean(value.resultReceived),
    postprocessStarted: safeBoolean(value.postprocessStarted),
    postprocessCompleted: safeBoolean(value.postprocessCompleted),
    contextReturned: safeBoolean(value.contextReturned),
    rawTextPresent: safeBoolean(value.rawTextPresent)
  });
}

export function createLocalOcrDiagnosticTracker({ inputAvailable = true } = {}) {
  const state = {
    schemaVersion: LOCAL_OCR_DIAGNOSTIC_SCHEMA,
    phase: "NOT_STARTED",
    outcome: inputAvailable ? "NOT_ATTEMPTED" : "INPUT_NOT_AVAILABLE",
    preprocessingMode: "NOT_STARTED",
    attempted: false,
    wrapperEntered: false,
    workerCreateStarted: false,
    workerReady: false,
    recognizeStarted: false,
    resultReceived: false,
    postprocessStarted: false,
    postprocessCompleted: false,
    contextReturned: false,
    rawTextPresent: false
  };

  const markPhase = phase => {
    state.phase = safeEnum(phase, PHASES, state.phase);
    if (state.phase === "BUDGET_PRECHECK_SKIPPED") state.outcome = "BUDGET_PRECHECK_SKIPPED";
    if (state.phase === "WRAPPER_ENTERED") {
      state.attempted = true;
      state.wrapperEntered = true;
      state.outcome = "IN_PROGRESS";
    }
    if (state.phase === "WORKER_CREATE_STARTED") state.workerCreateStarted = true;
    if (state.phase === "WORKER_READY") state.workerReady = true;
    if (state.phase === "RECOGNIZE_STARTED") state.recognizeStarted = true;
    if (state.phase === "RESULT_RECEIVED") state.resultReceived = true;
    if (state.phase === "POSTPROCESS_STARTED") state.postprocessStarted = true;
    if (state.phase === "POSTPROCESS_COMPLETED") state.postprocessCompleted = true;
    if (state.phase === "CONTEXT_RETURNED") state.contextReturned = true;
  };

  return Object.freeze({
    markPhase,
    setPreprocessingMode(mode) {
      state.preprocessingMode = safeEnum(mode, PREPROCESSING_MODES, "UNKNOWN");
    },
    setRawTextPresent(value) {
      state.rawTextPresent = value === true;
    },
    complete({ contextPresent = false } = {}) {
      markPhase("CONTEXT_RETURNED");
      state.outcome = contextPresent === true ? "SUCCESS_WITH_CONTEXT" : "SUCCESS_EMPTY";
    },
    fail(outcome) {
      state.outcome = safeEnum(outcome, OUTCOMES, "RECOGNIZE_ERROR");
    },
    snapshot() {
      return sanitizeLocalOcrDiagnostics(state);
    }
  });
}
