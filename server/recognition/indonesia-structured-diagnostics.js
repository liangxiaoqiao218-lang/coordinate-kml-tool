export const INDONESIA_STRUCTURED_DIAGNOSTIC_SCHEMA = "indonesia_structured_preflight_diagnostics_v1";

const ROUTE_FAILURE_CODES = new Set([
  "NONE",
  "STRUCTURED_PRODUCT_FEATURE_DISABLED",
  "STRUCTURED_PRODUCT_UTM50S_EVIDENCE_REQUIRED",
  "STRUCTURED_PRODUCT_PROJECTED_COLUMNS_EVIDENCE_REQUIRED",
  "STRUCTURED_PRODUCT_SOURCE_EVIDENCE_REQUIRED"
]);

const PREFLIGHT_FAILURE_CODES = new Set([
  "NONE",
  "INDONESIA_STRUCTURED_B_RC_PREFLIGHT_NOT_APPLICABLE",
  "INDONESIA_STRUCTURED_B_RC_PREFLIGHT_NOT_ATTEMPTED",
  "INDONESIA_STRUCTURED_B_RC_PREFLIGHT_UNKNOWN",
  "INDONESIA_STRUCTURED_B_RC_JOB_PREFLIGHT_UNAVAILABLE",
  "INDONESIA_STRUCTURED_B_RC_JOB_PREFLIGHT_REJECTED",
  "INDONESIA_STRUCTURED_B_RC_ADMISSION_UNAVAILABLE",
  "INDONESIA_STRUCTURED_B_RC_NOT_ACTIVATED"
]);

function safeBoolean(value) {
  return value === true;
}

function safeEnum(value, allowed, fallback) {
  const normalized = String(value || "").trim().toUpperCase();
  return allowed.has(normalized) ? normalized : fallback;
}

export function classifyIndonesiaStructuredProviderFailure({
  providerAttemptCount = 0,
  errorCode = ""
} = {}) {
  const providerCallCount = Math.min(1, Math.max(0, Number(providerAttemptCount) || 0));
  if (providerCallCount === 0) {
    return Object.freeze({
      providerCallCount: 0,
      providerCompletionState: "NOT_STARTED",
      settlementOutcome: "FAILED_PRE_PROVIDER"
    });
  }
  const code = String(errorCode || "").trim().toUpperCase();
  return Object.freeze({
    providerCallCount: 1,
    providerCompletionState: code === "ALIYUN_TIMEOUT"
      ? "TIMED_OUT"
      : code === "RECOGNITION_DEADLINE_EXCEEDED" ? "ABORTED" : "FAILED",
    settlementOutcome: "OUTCOME_UNKNOWN"
  });
}

export function classifyIndonesiaStructuredRouteFailure(route) {
  if (route?.status !== "BLOCKED") return "NONE";
  if (route?.reasonCode === "STRUCTURED_PRODUCT_FEATURE_DISABLED") {
    return "STRUCTURED_PRODUCT_FEATURE_DISABLED";
  }
  if (route?.evidence?.explicitUtm50s !== true) {
    return "STRUCTURED_PRODUCT_UTM50S_EVIDENCE_REQUIRED";
  }
  if (route?.evidence?.projectedColumns !== true
    && route?.evidence?.projectedColumnsHintOnly !== true) {
    return "STRUCTURED_PRODUCT_PROJECTED_COLUMNS_EVIDENCE_REQUIRED";
  }
  return "STRUCTURED_PRODUCT_SOURCE_EVIDENCE_REQUIRED";
}

export function buildIndonesiaStructuredPreflightDiagnostics({
  route,
  localOcrAttempted = false,
  sourceContextPresent = false,
  preflightChecked = false,
  preflightPassed = false,
  preflightFailureCode = "INDONESIA_STRUCTURED_B_RC_PREFLIGHT_NOT_APPLICABLE",
  controlledRcProjectedColumnsHintAuthorized = false
} = {}) {
  return Object.freeze({
    schemaVersion: INDONESIA_STRUCTURED_DIAGNOSTIC_SCHEMA,
    localOcrAttempted: safeBoolean(localOcrAttempted),
    sourceContextPresent: safeBoolean(sourceContextPresent),
    localOcrContextPresent: safeBoolean(sourceContextPresent),
    explicitUtm50s: safeBoolean(route?.evidence?.explicitUtm50s),
    projectedColumns: safeBoolean(route?.evidence?.projectedColumns),
    controlledRcHintAuthorized: safeBoolean(controlledRcProjectedColumnsHintAuthorized),
    controlledRcHintApplied: safeBoolean(route?.evidence?.projectedColumnsHintOnly),
    preflightChecked: safeBoolean(preflightChecked),
    preflightPassed: safeBoolean(preflightPassed),
    preflightFailureCode: safeEnum(preflightFailureCode, PREFLIGHT_FAILURE_CODES, "INDONESIA_STRUCTURED_B_RC_PREFLIGHT_UNKNOWN"),
    routeSelected: route?.status === "SELECTED",
    routeFailureCode: classifyIndonesiaStructuredRouteFailure(route)
  });
}

export function sanitizeIndonesiaStructuredPreflightDiagnostics(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)
    || value.schemaVersion !== INDONESIA_STRUCTURED_DIAGNOSTIC_SCHEMA) return null;
  return Object.freeze({
    schemaVersion: INDONESIA_STRUCTURED_DIAGNOSTIC_SCHEMA,
    localOcrAttempted: safeBoolean(value.localOcrAttempted),
    sourceContextPresent: safeBoolean(value.sourceContextPresent),
    localOcrContextPresent: safeBoolean(value.localOcrContextPresent),
    explicitUtm50s: safeBoolean(value.explicitUtm50s),
    projectedColumns: safeBoolean(value.projectedColumns),
    controlledRcHintAuthorized: safeBoolean(value.controlledRcHintAuthorized),
    controlledRcHintApplied: safeBoolean(value.controlledRcHintApplied),
    preflightChecked: safeBoolean(value.preflightChecked),
    preflightPassed: safeBoolean(value.preflightPassed),
    preflightFailureCode: safeEnum(value.preflightFailureCode, PREFLIGHT_FAILURE_CODES, "INDONESIA_STRUCTURED_B_RC_PREFLIGHT_UNKNOWN"),
    routeSelected: safeBoolean(value.routeSelected),
    routeFailureCode: safeEnum(value.routeFailureCode, ROUTE_FAILURE_CODES, "STRUCTURED_PRODUCT_SOURCE_EVIDENCE_REQUIRED")
  });
}
