const REQUEST_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const COMMIT_PATTERN = /^[0-9a-f]{40}$/u;
const SAFE_JOB_ID_PATTERN = /^[A-Za-z0-9_.:-]{1,160}$/u;
const SAFE_ERROR_CODE_PATTERN = /^[A-Z0-9_]{1,160}$/u;

export const COORDINATE_RECOGNITION_ISSUE_EVENT = Object.freeze({
  FAILED_CLOSED: "FAILED_CLOSED",
  REVIEW_REQUIRED: "REVIEW_REQUIRED",
  MANUAL_ASSISTANCE_REQUESTED: "MANUAL_ASSISTANCE_REQUESTED"
});

const COORDINATE_TYPES = new Set(["UNKNOWN", "DECIMAL_DEGREES", "DMS", "UTM", "PROJECTED", "MIXED"]);

function safeRequestId(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return REQUEST_ID_PATTERN.test(normalized) ? normalized : null;
}

function safeJobId(value) {
  const normalized = String(value || "").trim();
  return SAFE_JOB_ID_PATTERN.test(normalized) ? normalized : null;
}

function safeRuntimeCommit(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return COMMIT_PATTERN.test(normalized) ? normalized : null;
}

function safeErrorCode(value, fallback) {
  const normalized = String(value || "").trim().toUpperCase();
  return SAFE_ERROR_CODE_PATTERN.test(normalized) ? normalized : fallback;
}

function coordinateTypeFromBody(body = {}) {
  const candidates = [
    body.coordinateType,
    body.coordinate_type,
    body.finalizedCoordinateResult?.coordinateType,
    body.finalizedCoordinateResult?.coordinate_type,
    body.coordinateEngineV2?.coordinateType,
    body.coordinateEngineV2?.coordinate_type,
    body.coordinateEngineV2?.source_format
  ];
  for (const candidate of candidates) {
    const normalized = String(candidate || "").trim().toUpperCase().replace(/[ -]+/gu, "_");
    if (COORDINATE_TYPES.has(normalized)) return normalized;
    if (normalized.includes("DMS")) return "DMS";
    if (normalized.includes("UTM")) return "UTM";
    if (normalized.includes("DECIMAL") || normalized === "WGS84") return "DECIMAL_DEGREES";
    if (normalized.includes("PROJECTED")) return "PROJECTED";
  }
  return "UNKNOWN";
}

function eventFromBody(body = {}, explicitEvent = "") {
  if (explicitEvent === COORDINATE_RECOGNITION_ISSUE_EVENT.MANUAL_ASSISTANCE_REQUESTED) {
    return explicitEvent;
  }
  if (String(body.code || "").toUpperCase() === "COORDINATE_RECOGNITION_FAILED_CLOSED") {
    return COORDINATE_RECOGNITION_ISSUE_EVENT.FAILED_CLOSED;
  }
  const reviewRequired = [
    body.authorizationStatus,
    body.resultStatus,
    body.qualityGateStatus,
    body.finalizedCoordinateResult?.authorizationStatus,
    body.finalizedCoordinateResult?.qualityGateStatus,
    body.finalizedCoordinateResult?.decisionState
  ].some(value => String(value || "").toUpperCase() === "REVIEW_REQUIRED");
  return reviewRequired ? COORDINATE_RECOGNITION_ISSUE_EVENT.REVIEW_REQUIRED : null;
}

function stageFor(event, body = {}) {
  if (event === COORDINATE_RECOGNITION_ISSUE_EVENT.MANUAL_ASSISTANCE_REQUESTED) return "MANUAL_ASSISTANCE";
  const acquisitionStatus = String(body.acquisitionStatus || body.recognitionAcquisition?.status || "").toUpperCase();
  if (event === COORDINATE_RECOGNITION_ISSUE_EVENT.FAILED_CLOSED || acquisitionStatus.includes("NO_COORDINATE_EVIDENCE")) {
    return "ACQUISITION";
  }
  if (body.finalizedCoordinateResult && typeof body.finalizedCoordinateResult === "object") return "FINALIZATION";
  return "UNKNOWN";
}

export function buildCoordinateRecognitionIssue({
  body = {},
  explicitEvent = "",
  requestId,
  jobId,
  runtimeCommit,
  occurredAt = new Date().toISOString()
} = {}) {
  const event = eventFromBody(body, explicitEvent);
  if (!event) return null;
  const normalizedRequestId = safeRequestId(requestId || body.requestId);
  if (!normalizedRequestId) return null;
  const failedClosed = event === COORDINATE_RECOGNITION_ISSUE_EVENT.FAILED_CLOSED;
  const manualAssistance = event === COORDINATE_RECOGNITION_ISSUE_EVENT.MANUAL_ASSISTANCE_REQUESTED;
  const errorCode = failedClosed
    ? "COORDINATE_RECOGNITION_FAILED_CLOSED"
    : manualAssistance
      ? "MANUAL_ASSISTANCE_REQUESTED"
      : safeErrorCode(body.code, "REVIEW_REQUIRED");
  const source = manualAssistance ? "MANUAL_ASSISTANCE" : "COORDINATE_IMAGE_UPLOAD";
  const stage = stageFor(event, body);
  const coordinateType = coordinateTypeFromBody(body);
  return Object.freeze({
    recognitionRequestId: normalizedRequestId,
    jobId: safeJobId(jobId),
    issueType: "recognition_failed",
    issueSummary: `${source}:${stage}:${errorCode}:${coordinateType}`,
    progressStatus: "NEW",
    deliveryStatus: failedClosed ? "FAIL" : "BLOCKED",
    problemResolutionStatus: "UNKNOWN",
    peerValidationStatus: "UNKNOWN",
    productionStatus: "UNKNOWN",
    crsEvidenceStatus: "UNKNOWN",
    geometryRepresentation: "UNKNOWN",
    originalArtifactStatus: "NOT_SAVED",
    source,
    failureStage: stage,
    errorCode,
    coordinateType,
    runtimeCommit: safeRuntimeCommit(runtimeCommit),
    occurredAt
  });
}

export async function recordCoordinateRecognitionIssue({ store, ...input } = {}) {
  const issue = buildCoordinateRecognitionIssue(input);
  if (!issue) return Object.freeze({ recorded: false, reason: "NOT_ELIGIBLE" });
  if (!store || typeof store.createAutomatic !== "function") {
    return Object.freeze({ recorded: false, reason: "STORE_UNAVAILABLE" });
  }
  const result = await store.createAutomatic(issue);
  return Object.freeze({ recorded: result?.setupRequired !== true, issue, ...result });
}
