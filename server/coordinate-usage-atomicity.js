import {
  createCipheriv,
  createDecipheriv,
  createHash,
  randomBytes,
  randomUUID,
  timingSafeEqual
} from "node:crypto";
import {
  COORDINATE_CONFIRMATION_STATUS,
  COORDINATE_DECISION_STATE,
  COORDINATE_GATE_REASON,
  COORDINATE_QUALITY_GATE_STATUS,
  FINALIZED_COORDINATE_CRS,
  FINALIZED_COORDINATE_SCHEMA_VERSION,
  FINALIZED_COORDINATE_SOURCE_AUTHORITIES
} from "./coordinate-finalizer/reason-codes.js";
import { FAMILY_AVAILABILITY_STATUS } from "./coordinate-finalizer/family-availability-policy.js";
import { createGeometryHash } from "./coordinate-finalizer/geometry-hash.js";
import { validateFinalizedGeometry } from "./coordinate-finalizer/geometry-finalizer.js";
import { evaluateAgenticCoordinateUsageAuthority } from "./agentic-coordinate-usage-authority.js";

export const COORDINATE_USAGE_ATOMICITY_VERSION = "coordinate_usage_atomicity_p0_v1";
export const COORDINATE_USAGE_SEAL_VERSION = "AES_256_GCM_V1";
export const PROJECTED_COORDINATE_REVIEW_USAGE_AUTHORITY_VERSION = "projected_coordinate_review_usage_authority/v1";
export const COORDINATE_USAGE_SESSION_COOKIE = "__Host-geokit_coordinate_usage_session";
export const COORDINATE_USAGE_COMMIT_STATE = Object.freeze({
  PREPARED: "PREPARED",
  COMMITTED: "COMMITTED",
  FAILED: "FAILED",
  EXPIRED: "EXPIRED"
});
export const COORDINATE_USAGE_COMMIT_RESULT = Object.freeze({
  PREPARED: "PREPARED",
  ALREADY_PREPARED: "ALREADY_PREPARED",
  COMMITTED: "COMMITTED",
  ALREADY_COMMITTED: "ALREADY_COMMITTED",
  COMMITTED_RESULT_UNAVAILABLE: "COMMITTED_RESULT_UNAVAILABLE",
  QUOTA_EXHAUSTED: "QUOTA_EXHAUSTED",
  NOT_FOUND: "NOT_FOUND",
  EXPIRED: "EXPIRED",
  FAILED: "FAILED",
  OUTCOME_UNKNOWN: "OUTCOME_UNKNOWN"
});
export const COORDINATE_USAGE_ERROR_CODE = Object.freeze({
  AUTHORITY_NOT_ESTABLISHED: "COORDINATE_AUTHORITY_NOT_ESTABLISHED",
  ATOMICITY_UNAVAILABLE: "COORDINATE_USAGE_ATOMICITY_UNAVAILABLE",
  COMMIT_FAILED: "COORDINATE_USAGE_COMMIT_FAILED",
  COMMIT_OUTCOME_UNKNOWN: "USAGE_COMMIT_OUTCOME_UNKNOWN",
  ENVELOPE_INVALID: "COORDINATE_RESULT_ENVELOPE_INVALID",
  REQUEST_ID_INVALID: "RECOGNITION_REQUEST_ID_INVALID",
  SESSION_BINDING_INVALID: "COORDINATE_USAGE_SESSION_BINDING_INVALID"
});
export const COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS = Object.freeze({
  READY: "READY",
  SUPABASE_URL_MISSING: "SUPABASE_URL_MISSING",
  SUPABASE_PROJECT_REF_MISMATCH: "SUPABASE_PROJECT_REF_MISMATCH",
  SERVICE_ROLE_KEY_MISSING: "SERVICE_ROLE_KEY_MISSING",
  SEAL_KEY_INVALID: "SEAL_KEY_INVALID",
  RPC_SCHEMA_CACHE_ERROR: "RPC_SCHEMA_CACHE_ERROR",
  RPC_PERMISSION_ERROR: "RPC_PERMISSION_ERROR",
  RPC_AUTH_ERROR: "RPC_AUTH_ERROR",
  RPC_NETWORK_ERROR: "RPC_NETWORK_ERROR",
  RPC_UNKNOWN_ERROR: "RPC_UNKNOWN_ERROR"
});

const SAFE_REVIEW_BLOCKERS = new Set([
  COORDINATE_GATE_REASON.CONFIRMATION_REQUIRED,
  COORDINATE_GATE_REASON.REVIEW_REQUIRED,
  COORDINATE_GATE_REASON.QUALITY_GATE_REVIEW_REQUIRED,
  COORDINATE_GATE_REASON.KML_NOT_READY
]);
const SAFE_PROVIDER_COST_STATES = new Set(["NOT_INCURRED", "POSSIBLY_INCURRED", "USAGE_REPORTED"]);
const SAFE_RPC_RESULTS = new Set(Object.values(COORDINATE_USAGE_COMMIT_RESULT));
const MAX_SEALED_RESULT_BYTES = 1024 * 1024;
const UNCHARGED_FAILURE_MESSAGE = "本次识别未完成，未扣除使用次数。你可以直接重新识别；如仍失败，请向支持人员提供本次请求编号。";
const SAFE_QUOTA_NUMBER_FIELDS = Object.freeze([
  "free_convert_count",
  "paid_convert_count",
  "free_judge_count",
  "paid_judge_count",
  "freeConvertCount",
  "paidConvertCount",
  "freeJudgeCount",
  "paidJudgeCount",
  "convert_remaining",
  "judge_remaining"
]);

export function isRecognitionRequestId(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(String(value || ""));
}

function safeIdentifier(value, maximum = 160) {
  const text = String(value || "").trim();
  return text.length > 0 && text.length <= maximum && /^[a-z0-9][a-z0-9._:-]*$/i.test(text);
}

function constantTimeEqual(left, right) {
  const a = Buffer.from(String(left || ""));
  const b = Buffer.from(String(right || ""));
  return a.length === b.length && timingSafeEqual(a, b);
}

function sanitizeQuota(quota) {
  if (!quota || typeof quota !== "object" || Array.isArray(quota)) return null;
  const safe = {};
  for (const key of SAFE_QUOTA_NUMBER_FIELDS) {
    const value = Number(quota[key]);
    if (Number.isFinite(value) && value >= 0) safe[key] = value;
  }
  if (typeof quota.is_vip === "boolean") safe.is_vip = quota.is_vip;
  if (typeof quota.isVip === "boolean") safe.isVip = quota.isVip;
  return Object.keys(safe).length > 0 ? Object.freeze(safe) : null;
}

export function buildUnchargedCoordinateFailureResponse({ body = null, recognitionRequestId = null } = {}) {
  const explicitFailure = body?.success === false;
  const originalReason = String(body?.reason || "");
  const originalCode = String(body?.code || "");
  const reason = explicitFailure && /^[a-z0-9_]{1,80}$/.test(originalReason)
    ? originalReason
    : "coordinate_authority_not_established";
  const code = explicitFailure && /^[A-Z0-9_]{1,100}$/.test(originalCode)
    ? originalCode
    : COORDINATE_USAGE_ERROR_CODE.AUTHORITY_NOT_ESTABLISHED;
  const quota = sanitizeQuota(body?.quota);
  return Object.freeze({
    success: false,
    reason,
    code,
    ...(body?.type === "convert" ? { type: "convert" } : {}),
    ...(quota ? { quota } : {}),
    error: UNCHARGED_FAILURE_MESSAGE,
    requestId: isRecognitionRequestId(recognitionRequestId) ? String(recognitionRequestId).toLowerCase() : null,
    usageConsumed: false,
    retryAllowed: true,
    rawText: "",
    coordinates: ""
  });
}

export function hashCoordinateUsageSession(value) {
  const token = String(value || "");
  if (!/^[A-Za-z0-9_-]{43}$/.test(token)) return null;
  return createHash("sha256").update(token).digest("hex");
}

export function createCoordinateUsageSessionToken() {
  return randomBytes(32).toString("base64url");
}

export function parseCoordinateUsageSealKey(value) {
  const source = String(value || "").trim();
  let key = null;
  if (/^[a-f0-9]{64}$/i.test(source)) key = Buffer.from(source, "hex");
  else {
    try {
      const decoded = Buffer.from(source, "base64");
      if (decoded.length === 32 && decoded.toString("base64").replace(/=+$/, "") === source.replace(/=+$/, "")) key = decoded;
    } catch {
      key = null;
    }
  }
  return key?.length === 32 ? key : null;
}

function coordinateUsageProjectRefMatches(value, expectedProjectRef) {
  try {
    const url = new URL(String(value || "").trim());
    return url.protocol === "https:"
      && url.username === ""
      && url.password === ""
      && url.port === ""
      && url.pathname === "/"
      && url.search === ""
      && url.hash === ""
      && url.hostname === `${expectedProjectRef}.supabase.co`;
  } catch {
    return false;
  }
}

function classifyCoordinateUsageRuntimeDiagnosticError(error, responseStatus) {
  const code = String(error?.code || "").trim().toUpperCase();
  const outerStatus = Number(responseStatus);
  const status = Number.isFinite(outerStatus) ? outerStatus : Number(error?.status);
  const name = String(error?.name || "").trim();
  if (["PGRST002", "PGRST106", "PGRST202"].includes(code)) {
    return COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_SCHEMA_CACHE_ERROR;
  }
  if (code === "42501") return COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_PERMISSION_ERROR;
  if ([401, 403].includes(status) || ["PGRST301", "PGRST302", "PGRST303"].includes(code) || /^28[A-Z0-9]{3}$/.test(code)) {
    return COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_AUTH_ERROR;
  }
  if (status === 0
    || ["ECONNABORTED", "ECONNREFUSED", "ECONNRESET", "ENETUNREACH", "ENOTFOUND", "ETIMEDOUT"].includes(code)
    || ["AbortError", "TimeoutError", "TypeError"].includes(name)) {
    return COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_NETWORK_ERROR;
  }
  return COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_UNKNOWN_ERROR;
}

export function createCoordinateUsageRuntimeDiagnostic({
  supabase = null,
  supabaseUrl = "",
  serviceRoleKeyPresent = false,
  sealKey = null,
  expectedProjectRef,
  timeoutMs = 8000,
  randomRequestId = randomUUID,
  randomSessionBinding = () => randomBytes(32).toString("hex")
} = {}) {
  const hasSupabaseUrl = String(supabaseUrl || "").trim().length > 0;
  const productionProjectRefMatch = hasSupabaseUrl
    && /^[a-z0-9]{20}$/i.test(String(expectedProjectRef || ""))
    && coordinateUsageProjectRefMatches(supabaseUrl, String(expectedProjectRef));
  const hasServiceRoleKey = serviceRoleKeyPresent === true;
  const validSealKey = Buffer.isBuffer(sealKey) && sealKey.length === 32;
  const boundedTimeoutMs = Number.isFinite(Number(timeoutMs))
    ? Math.min(Math.max(Math.trunc(Number(timeoutMs)), 1), 30000)
    : 8000;
  const initialStatus = !hasSupabaseUrl
    ? COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.SUPABASE_URL_MISSING
    : !productionProjectRefMatch
      ? COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.SUPABASE_PROJECT_REF_MISMATCH
      : !hasServiceRoleKey
        ? COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.SERVICE_ROLE_KEY_MISSING
        : !validSealKey
          ? COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.SEAL_KEY_INVALID
          : COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_UNKNOWN_ERROR;
  let snapshot = Object.freeze({
    status: initialStatus,
    probeComplete: false,
    productionProjectRefMatch
  });
  let probePromise = null;

  const complete = status => {
    snapshot = Object.freeze({
      status,
      probeComplete: true,
      productionProjectRefMatch
    });
    return snapshot;
  };

  const runProbe = async () => {
    if (initialStatus !== COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_UNKNOWN_ERROR) {
      return complete(initialStatus);
    }
    if (!supabase || typeof supabase.rpc !== "function") {
      return complete(COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_UNKNOWN_ERROR);
    }

    let recognitionRequestId;
    let sessionBindingSha256;
    try {
      recognitionRequestId = String(randomRequestId() || "").toLowerCase();
      sessionBindingSha256 = String(randomSessionBinding() || "").toLowerCase();
    } catch {
      return complete(COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_UNKNOWN_ERROR);
    }
    if (!isRecognitionRequestId(recognitionRequestId) || !/^[a-f0-9]{64}$/.test(sessionBindingSha256)) {
      return complete(COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_UNKNOWN_ERROR);
    }

    let timeoutHandle = null;
    let abortController = null;
    try {
      abortController = typeof AbortController === "function" ? new AbortController() : null;
      const builder = supabase.rpc("get_coordinate_recognition_commit_state", {
        p_recognition_request_id: recognitionRequestId,
        p_user_id: "synthetic-runtime-diagnostic",
        p_session_binding_sha256: sessionBindingSha256
      });
      const request = abortController && typeof builder?.abortSignal === "function"
        ? builder.abortSignal(abortController.signal)
        : builder;
      const timeoutResult = Symbol("coordinate_usage_runtime_diagnostic_timeout");
      const timeoutPromise = new Promise(resolve => {
        timeoutHandle = setTimeout(() => resolve(timeoutResult), boundedTimeoutMs);
      });
      const response = await Promise.race([Promise.resolve(request), timeoutPromise]);
      if (response === timeoutResult) {
        abortController?.abort();
        return complete(COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_NETWORK_ERROR);
      }
      if (response?.error) return complete(classifyCoordinateUsageRuntimeDiagnosticError(response.error, response.status));
      const row = Array.isArray(response?.data) ? response.data[0] : response?.data;
      return complete(row?.result === COORDINATE_USAGE_COMMIT_RESULT.NOT_FOUND
        ? COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.READY
        : COORDINATE_USAGE_RUNTIME_DIAGNOSTIC_STATUS.RPC_UNKNOWN_ERROR);
    } catch (error) {
      return complete(classifyCoordinateUsageRuntimeDiagnosticError(error));
    } finally {
      if (timeoutHandle) clearTimeout(timeoutHandle);
    }
  };

  return Object.freeze({
    runOnce() {
      if (!probePromise) probePromise = runProbe();
      return probePromise;
    },
    snapshot() {
      return snapshot;
    }
  });
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

export function sealCoordinateResult(payload, { key, recognitionRequestId, random = randomBytes } = {}) {
  if (!Buffer.isBuffer(key) || key.length !== 32) throw fixedError(COORDINATE_USAGE_ERROR_CODE.ATOMICITY_UNAVAILABLE);
  if (!isRecognitionRequestId(recognitionRequestId)) throw fixedError(COORDINATE_USAGE_ERROR_CODE.REQUEST_ID_INVALID);
  const serializedPayload = JSON.stringify(payload);
  if (typeof serializedPayload !== "string") throw fixedError(COORDINATE_USAGE_ERROR_CODE.ENVELOPE_INVALID);
  const plaintext = Buffer.from(serializedPayload, "utf8");
  if (plaintext.length === 0 || plaintext.length > MAX_SEALED_RESULT_BYTES) {
    throw fixedError(COORDINATE_USAGE_ERROR_CODE.ENVELOPE_INVALID);
  }
  const iv = random(12);
  if (!Buffer.isBuffer(iv) || iv.length !== 12) throw fixedError(COORDINATE_USAGE_ERROR_CODE.ENVELOPE_INVALID);
  const cipher = createCipheriv("aes-256-gcm", key, iv);
  cipher.setAAD(Buffer.from(`${COORDINATE_USAGE_SEAL_VERSION}:${recognitionRequestId}`, "utf8"));
  const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  const envelope = Object.freeze({
    version: COORDINATE_USAGE_SEAL_VERSION,
    iv: iv.toString("base64"),
    ciphertext: ciphertext.toString("base64"),
    authTag: cipher.getAuthTag().toString("base64")
  });
  return Object.freeze({
    envelope,
    envelopeSha256: createHash("sha256").update(canonicalJson(envelope)).digest("hex")
  });
}

export function unsealCoordinateResult(envelope, { key, recognitionRequestId } = {}) {
  if (!Buffer.isBuffer(key) || key.length !== 32) throw fixedError(COORDINATE_USAGE_ERROR_CODE.ATOMICITY_UNAVAILABLE);
  if (!isRecognitionRequestId(recognitionRequestId) || envelope?.version !== COORDINATE_USAGE_SEAL_VERSION) {
    throw fixedError(COORDINATE_USAGE_ERROR_CODE.ENVELOPE_INVALID);
  }
  try {
    const iv = Buffer.from(String(envelope.iv || ""), "base64");
    const ciphertext = Buffer.from(String(envelope.ciphertext || ""), "base64");
    const authTag = Buffer.from(String(envelope.authTag || ""), "base64");
    if (iv.length !== 12 || authTag.length !== 16 || ciphertext.length === 0 || ciphertext.length > MAX_SEALED_RESULT_BYTES) {
      throw new Error("invalid_envelope");
    }
    const decipher = createDecipheriv("aes-256-gcm", key, iv);
    decipher.setAAD(Buffer.from(`${COORDINATE_USAGE_SEAL_VERSION}:${recognitionRequestId}`, "utf8"));
    decipher.setAuthTag(authTag);
    const plaintext = Buffer.concat([decipher.update(ciphertext), decipher.final()]);
    return JSON.parse(plaintext.toString("utf8"));
  } catch {
    throw fixedError(COORDINATE_USAGE_ERROR_CODE.ENVELOPE_INVALID);
  }
}

function fixedError(code, details = {}) {
  const error = new Error(code);
  error.code = code;
  Object.assign(error, details);
  return error;
}

function normalizeProjectedCoordinateReviewAuthorityEvidence(body = null) {
  if (!body || body.success !== true || body.precisionMode !== "projected-x-y-review" || body.requiresReview !== true) {
    return null;
  }
  const evidence = body.providerProjectedReviewEvidence;
  const rowCount = Number(evidence?.coordinateRowCount);
  const crsStatus = String(evidence?.crsEvidence?.status || "");
  const crsProjection = String(evidence?.crsEvidence?.projection || "");
  const crsZone = evidence?.crsEvidence?.zone == null ? null : Number(evidence.crsEvidence.zone);
  const crsHemisphere = String(evidence?.crsEvidence?.hemisphere || "");
  if (evidence?.status !== "COMPLETE"
    || !Number.isSafeInteger(rowCount)
    || rowCount < 3
    || rowCount > 500
    || !["UNCONFIRMED", "EXPLICIT"].includes(crsStatus)) {
    return null;
  }
  if ((crsStatus === "UNCONFIRMED" && (crsProjection !== "" || crsZone !== null || crsHemisphere !== ""))
    || (crsStatus === "EXPLICIT"
      && !((crsProjection === "bftm" && crsZone === null && crsHemisphere === "")
        || (crsProjection === "utm" && Number.isInteger(crsZone) && crsZone >= 1 && crsZone <= 60 && ["N", "S"].includes(crsHemisphere))))) {
    return null;
  }
  const coordinates = String(body.coordinates || "").trim();
  const lines = coordinates.split(/\r?\n/u).map(line => line.trim()).filter(Boolean);
  if (lines.length !== rowCount) return null;
  const coordinateRows = [];
  const labels = new Set();
  for (const line of lines) {
    const match = line.match(/^([A-Z][A-Z0-9_.-]{0,15}|\d{1,3}) \| ([+-]?\d{4,10}(?:\.\d+)?) \| ([+-]?\d{4,10}(?:\.\d+)?)$/iu);
    const labelIdentity = String(match?.[1] || "").toUpperCase();
    if (!match || labels.has(labelIdentity)) return null;
    labels.add(labelIdentity);
    coordinateRows.push(Object.freeze({ label: match[1], x: match[2], y: match[3] }));
  }
  if (body.sourceCoordinateRepresentation?.displayText !== coordinates) return null;
  const engine = body.coordinateEngineV2;
  const groups = Array.isArray(engine?.groups) ? engine.groups : [];
  const points = groups.length === 1 && Array.isArray(groups[0]?.points) ? groups[0].points : [];
  if (engine?.coordinate_type !== "projected_xy"
    || engine?.requires_review !== true
    || groups[0]?.requires_review !== true
    || groups[0]?.kml_ready !== false
    || points.length !== coordinateRows.length) {
    return null;
  }
  for (let index = 0; index < coordinateRows.length; index += 1) {
    const expected = coordinateRows[index];
    const point = points[index];
    if (String(point?.label || "") !== expected.label
      || Number(point?.x) !== Number(expected.x)
      || Number(point?.y) !== Number(expected.y)
      || point?.lat != null
      || point?.lon != null
      || point?.source_crs != null
      || point?.requires_review !== true) {
      return null;
    }
  }
  const finalized = body.finalizedCoordinateResult;
  if (!finalized
    || finalized.schemaVersion !== FINALIZED_COORDINATE_SCHEMA_VERSION
    || !safeIdentifier(finalized.resultId)
    || !Number.isSafeInteger(finalized.resultRevision)
    || finalized.resultRevision < 1
    || finalized.decisionState === COORDINATE_DECISION_STATE.AUTO_EXPORT
    || finalized.geometry != null
    || finalized.geometryHash != null
    || finalized.kmlReady !== false
    || finalized.requiresReview !== true) {
    return null;
  }
  return Object.freeze({
    precisionMode: body.precisionMode,
    coordinates,
    coordinateRows: Object.freeze(coordinateRows),
    coordinateRowCount: rowCount,
    crsEvidence: Object.freeze({
      status: crsStatus,
      projection: crsProjection,
      zone: crsZone,
      hemisphere: crsHemisphere
    }),
    finalizedResultId: finalized.resultId,
    finalizedResultRevision: finalized.resultRevision,
    finalizedDecisionState: finalized.decisionState
  });
}

export function hashProjectedCoordinateReviewResult(body) {
  const evidence = normalizeProjectedCoordinateReviewAuthorityEvidence(body);
  if (!evidence) return null;
  return createHash("sha256").update(canonicalJson(evidence)).digest("hex");
}

export function buildProjectedCoordinateReviewUsageAuthority({ recognitionRequestId, body } = {}) {
  const requestId = String(recognitionRequestId || "").trim().toLowerCase();
  const resultHash = hashProjectedCoordinateReviewResult(body);
  if (!isRecognitionRequestId(requestId) || !resultHash) {
    throw fixedError(COORDINATE_USAGE_ERROR_CODE.AUTHORITY_NOT_ESTABLISHED);
  }
  return Object.freeze({
    schemaVersion: PROJECTED_COORDINATE_REVIEW_USAGE_AUTHORITY_VERSION,
    recognitionRequestId: requestId,
    resultId: `projected-review:${requestId}`,
    resultRevision: 1,
    decisionState: COORDINATE_DECISION_STATE.REVIEW_REQUIRED,
    resultHash
  });
}

export function evaluateProjectedCoordinateReviewUsageAuthority({ httpStatus = 200, body = null } = {}) {
  const reject = reason => Object.freeze({ eligible: false, reason, identity: null });
  if (!Number.isInteger(Number(httpStatus)) || Number(httpStatus) < 200 || Number(httpStatus) >= 300 || body?.success !== true) {
    return reject("HTTP_OR_BODY_NOT_SUCCESSFUL");
  }
  const authority = body?.projectedCoordinateReviewAuthority;
  const requestId = String(body?.requestId || "").trim().toLowerCase();
  if (!authority || authority.schemaVersion !== PROJECTED_COORDINATE_REVIEW_USAGE_AUTHORITY_VERSION) {
    return reject("PROJECTED_REVIEW_AUTHORITY_MISSING");
  }
  if (!isRecognitionRequestId(requestId)
    || authority.recognitionRequestId !== requestId
    || authority.resultId !== `projected-review:${requestId}`
    || authority.resultRevision !== 1
    || authority.decisionState !== COORDINATE_DECISION_STATE.REVIEW_REQUIRED) {
    return reject("PROJECTED_REVIEW_AUTHORITY_IDENTITY_INVALID");
  }
  const resultHash = hashProjectedCoordinateReviewResult(body);
  if (!resultHash
    || !/^[a-f0-9]{64}$/i.test(String(authority.resultHash || ""))
    || !constantTimeEqual(authority.resultHash, resultHash)) {
    return reject("PROJECTED_REVIEW_RESULT_HASH_MISMATCH");
  }
  return Object.freeze({
    eligible: true,
    reason: "PROJECTED_REVIEW_SERVER_AUTHORITY_ESTABLISHED",
    identity: Object.freeze({
      resultId: authority.resultId,
      resultRevision: authority.resultRevision,
      decisionState: authority.decisionState,
      geometryHash: `sha256:${resultHash}`
    })
  });
}

export function evaluateCoordinateUsageAuthority({ httpStatus = 200, body = null } = {}) {
  if (body?.agenticCoordinateAuthority) {
    return evaluateAgenticCoordinateUsageAuthority({ httpStatus, body });
  }
  if (body?.projectedCoordinateReviewAuthority) {
    return evaluateProjectedCoordinateReviewUsageAuthority({ httpStatus, body });
  }
  const result = body?.finalizedCoordinateResult;
  const reject = reason => Object.freeze({ eligible: false, reason, identity: null });
  if (!Number.isInteger(Number(httpStatus)) || Number(httpStatus) < 200 || Number(httpStatus) >= 300 || body?.success !== true) {
    return reject("HTTP_OR_BODY_NOT_SUCCESSFUL");
  }
  if (!result || result.schemaVersion !== FINALIZED_COORDINATE_SCHEMA_VERSION) return reject("FINALIZER_IDENTITY_MISSING");
  if (!safeIdentifier(result.resultId) || !Number.isSafeInteger(result.resultRevision) || result.resultRevision < 1) {
    return reject("FINALIZER_IDENTITY_MALFORMED");
  }
  const gate = result.gate;
  if (![COORDINATE_DECISION_STATE.AUTO_EXPORT, COORDINATE_DECISION_STATE.REVIEW_REQUIRED].includes(result.decisionState)
    || !gate || typeof gate !== "object"
    || gate.decisionState !== result.decisionState
    || gate.qualityGateStatus !== result.qualityGateStatus
    || gate.confirmationStatus !== result.confirmationStatus
    || gate.availabilityStatus !== result.availabilityStatus
    || gate.availabilityReasonCode !== result.availabilityReasonCode) {
    return reject("FINALIZER_DECISION_BLOCKED_OR_MALFORMED");
  }
  if (!FINALIZED_COORDINATE_SOURCE_AUTHORITIES.includes(result.sourceAuthority) || result.explicitAuthorityRejected !== false) {
    return reject("SOURCE_AUTHORITY_NOT_ESTABLISHED");
  }
  if (result.crs?.id !== FINALIZED_COORDINATE_CRS.id || result.crs?.axisOrder !== FINALIZED_COORDINATE_CRS.axisOrder) {
    return reject("FINALIZED_CRS_INVALID");
  }
  if (validateFinalizedGeometry(result.geometry).ok !== true) return reject("FINALIZED_GEOMETRY_INVALID");
  const recomputedGeometryHash = createGeometryHash(result.geometry, result.schemaVersion);
  if (!constantTimeEqual(recomputedGeometryHash, result.geometryHash)) return reject("FINALIZED_GEOMETRY_HASH_MISMATCH");
  if (result.availabilityStatus !== FAMILY_AVAILABILITY_STATUS.AVAILABLE) {
    return reject("FAMILY_AVAILABILITY_BLOCKED");
  }
  if (![COORDINATE_QUALITY_GATE_STATUS.PASSED, COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED].includes(result.qualityGateStatus)) {
    return reject("QUALITY_AUTHORITY_NOT_ESTABLISHED");
  }
  const blockers = Array.isArray(result.blockingReasons)
    ? result.blockingReasons.map(item => String(item?.code || ""))
    : null;
  if (!blockers || blockers.some(code => !SAFE_REVIEW_BLOCKERS.has(code))) return reject("INDEPENDENT_AUTHORITY_BLOCKER_PRESENT");
  if (result.decisionState === COORDINATE_DECISION_STATE.AUTO_EXPORT
    && (blockers.length !== 0
      || result.requiresReview !== false
      || result.kmlReady !== true
      || ![COORDINATE_CONFIRMATION_STATUS.NOT_REQUIRED, COORDINATE_CONFIRMATION_STATUS.ACCEPTED].includes(result.confirmationStatus))) {
    return reject("AUTO_EXPORT_IDENTITY_CONTRADICTORY");
  }
  if (result.decisionState === COORDINATE_DECISION_STATE.REVIEW_REQUIRED) {
    const technicallyReadyReview = result.kmlReady === true
      && result.technicalKmlReady === true
      && result.kmlAuthorityBlocked !== true
      && !blockers.includes(COORDINATE_GATE_REASON.KML_NOT_READY);
    const safelyBlockedReview = result.kmlReady === false;
    if (result.requiresReview !== true
      || result.confirmationStatus !== COORDINATE_CONFIRMATION_STATUS.PENDING
      || result.qualityGateStatus !== COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED
      || (!technicallyReadyReview && !safelyBlockedReview)) {
      return reject("REVIEW_IDENTITY_CONTRADICTORY");
    }
  }
  return Object.freeze({
    eligible: true,
    reason: "FINAL_SERVER_AUTHORITY_ESTABLISHED",
    identity: Object.freeze({
      resultId: result.resultId,
      resultRevision: result.resultRevision,
      decisionState: result.decisionState,
      geometryHash: result.geometryHash
    })
  });
}

function normalizeRpcRow(data) {
  const row = Array.isArray(data) ? data[0] : data;
  if (!row || typeof row !== "object") return null;
  const result = String(row.result || row.commit_result || row.status || "");
  if (!SAFE_RPC_RESULTS.has(result)) return null;
  return Object.freeze({
    result,
    state: Object.values(COORDINATE_USAGE_COMMIT_STATE).includes(row.state) ? row.state : null,
    consumeType: ["free", "paid", "none"].includes(row.consume_type) ? row.consume_type : null,
    quota: row.quota && typeof row.quota === "object" ? row.quota : null,
    sealedResult: row.sealed_result && typeof row.sealed_result === "object" ? row.sealed_result : null,
    sealedResultSha256: /^[a-f0-9]{64}$/i.test(String(row.sealed_result_sha256 || ""))
      ? String(row.sealed_result_sha256).toLowerCase()
      : null
  });
}

function classifyRpcFailure(error) {
  const code = String(error?.code || "");
  if (["22023", "23503", "23514", "42501"].includes(code)) return "KNOWN_FAILURE";
  return "OUTCOME_UNKNOWN";
}

export class CoordinateUsageAtomicityService {
  constructor({ supabase, sealKey, clock = () => new Date().toISOString() } = {}) {
    this.supabase = supabase || null;
    this.sealKey = sealKey || null;
    this.clock = clock;
  }

  assertAvailable() {
    if (!this.supabase || typeof this.supabase.rpc !== "function" || !Buffer.isBuffer(this.sealKey) || this.sealKey.length !== 32) {
      throw fixedError(COORDINATE_USAGE_ERROR_CODE.ATOMICITY_UNAVAILABLE);
    }
  }

  async prepare({ recognitionRequestId, userId, sessionBindingSha256, responsePayload, providerCostState = "NOT_INCURRED" } = {}) {
    this.assertAvailable();
    const authority = evaluateCoordinateUsageAuthority({ httpStatus: 200, body: responsePayload });
    if (!authority.eligible) throw fixedError(COORDINATE_USAGE_ERROR_CODE.AUTHORITY_NOT_ESTABLISHED, { reason: authority.reason });
    if (!isRecognitionRequestId(recognitionRequestId)) throw fixedError(COORDINATE_USAGE_ERROR_CODE.REQUEST_ID_INVALID);
    if (!safeIdentifier(userId, 256) || !/^[a-f0-9]{64}$/i.test(String(sessionBindingSha256 || ""))) {
      throw fixedError(COORDINATE_USAGE_ERROR_CODE.SESSION_BINDING_INVALID);
    }
    const costState = SAFE_PROVIDER_COST_STATES.has(providerCostState) ? providerCostState : "POSSIBLY_INCURRED";
    const sealed = sealCoordinateResult(responsePayload, { key: this.sealKey, recognitionRequestId });
    const { data, error } = await this.supabase.rpc("prepare_coordinate_recognition_result", {
      p_recognition_request_id: recognitionRequestId,
      p_user_id: userId,
      p_session_binding_sha256: String(sessionBindingSha256).toLowerCase(),
      p_authority_result_id: authority.identity.resultId,
      p_authority_result_revision: authority.identity.resultRevision,
      p_authority_decision_state: authority.identity.decisionState,
      p_authority_geometry_hash: authority.identity.geometryHash,
      p_sealed_result: sealed.envelope,
      p_sealed_result_sha256: sealed.envelopeSha256,
      p_encryption_version: COORDINATE_USAGE_SEAL_VERSION,
      p_provider_cost_state: costState
    });
    if (error) {
      if (classifyRpcFailure(error) === "OUTCOME_UNKNOWN") throw fixedError(COORDINATE_USAGE_ERROR_CODE.COMMIT_OUTCOME_UNKNOWN);
      throw fixedError(COORDINATE_USAGE_ERROR_CODE.COMMIT_FAILED);
    }
    const row = normalizeRpcRow(data);
    if (!row || ![COORDINATE_USAGE_COMMIT_RESULT.PREPARED, COORDINATE_USAGE_COMMIT_RESULT.ALREADY_PREPARED, COORDINATE_USAGE_COMMIT_RESULT.ALREADY_COMMITTED].includes(row.result)) {
      throw fixedError(COORDINATE_USAGE_ERROR_CODE.COMMIT_FAILED);
    }
    return Object.freeze({ ...row, authority: authority.identity, sealed });
  }

  async commit({ recognitionRequestId, userId, sessionBindingSha256 } = {}) {
    this.assertAvailable();
    const { data, error } = await this.supabase.rpc("commit_coordinate_recognition_usage", {
      p_recognition_request_id: recognitionRequestId,
      p_user_id: userId,
      p_session_binding_sha256: sessionBindingSha256
    });
    if (error) {
      if (classifyRpcFailure(error) === "OUTCOME_UNKNOWN") return Object.freeze({ result: COORDINATE_USAGE_COMMIT_RESULT.OUTCOME_UNKNOWN });
      return Object.freeze({ result: COORDINATE_USAGE_COMMIT_RESULT.FAILED });
    }
    return normalizeRpcRow(data) || Object.freeze({ result: COORDINATE_USAGE_COMMIT_RESULT.OUTCOME_UNKNOWN });
  }

  async getState({ recognitionRequestId, userId, sessionBindingSha256 } = {}) {
    this.assertAvailable();
    const { data, error } = await this.supabase.rpc("get_coordinate_recognition_commit_state", {
      p_recognition_request_id: recognitionRequestId,
      p_user_id: userId,
      p_session_binding_sha256: sessionBindingSha256
    });
    if (error) {
      if (classifyRpcFailure(error) === "OUTCOME_UNKNOWN") return Object.freeze({ result: COORDINATE_USAGE_COMMIT_RESULT.OUTCOME_UNKNOWN });
      return Object.freeze({ result: COORDINATE_USAGE_COMMIT_RESULT.FAILED });
    }
    return normalizeRpcRow(data) || Object.freeze({ result: COORDINATE_USAGE_COMMIT_RESULT.OUTCOME_UNKNOWN });
  }

  async recover({ recognitionRequestId, userId, sessionBindingSha256 } = {}) {
    let state = await this.getState({ recognitionRequestId, userId, sessionBindingSha256 });
    if (state.result === COORDINATE_USAGE_COMMIT_RESULT.PREPARED) {
      const committed = await this.commit({ recognitionRequestId, userId, sessionBindingSha256 });
      if (![COORDINATE_USAGE_COMMIT_RESULT.COMMITTED, COORDINATE_USAGE_COMMIT_RESULT.ALREADY_COMMITTED].includes(committed.result)) {
        return committed;
      }
      state = await this.getState({ recognitionRequestId, userId, sessionBindingSha256 });
    }
    if (![COORDINATE_USAGE_COMMIT_RESULT.COMMITTED, COORDINATE_USAGE_COMMIT_RESULT.ALREADY_COMMITTED].includes(state.result)
      || !state.sealedResult || !state.sealedResultSha256) return state;
    const actualHash = createHash("sha256").update(canonicalJson(state.sealedResult)).digest("hex");
    if (!constantTimeEqual(actualHash, state.sealedResultSha256)) {
      return Object.freeze({
        result: COORDINATE_USAGE_COMMIT_RESULT.COMMITTED_RESULT_UNAVAILABLE,
        state: COORDINATE_USAGE_COMMIT_STATE.COMMITTED,
        reason: "SEALED_RESULT_HASH_MISMATCH"
      });
    }
    try {
      return Object.freeze({
        ...state,
        responsePayload: unsealCoordinateResult(state.sealedResult, { key: this.sealKey, recognitionRequestId })
      });
    } catch {
      return Object.freeze({
        result: COORDINATE_USAGE_COMMIT_RESULT.COMMITTED_RESULT_UNAVAILABLE,
        state: COORDINATE_USAGE_COMMIT_STATE.COMMITTED,
        reason: "SEALED_RESULT_DECRYPTION_FAILED"
      });
    }
  }
}

export function createCoordinateUsageCommitController({
  regressionTestMode = false,
  budget = null,
  atomicityService = null,
  recognitionRequestId,
  userId,
  sessionBindingSha256,
  providerCostState = () => "NOT_INCURRED"
} = {}) {
  let pending = null;
  let settled = false;
  let committed = false;
  return Object.freeze({
    schedule(metadata = {}, quota = null) {
      if (regressionTestMode) return { success: true, reason: "regression_test", skipped: true, quota };
      if (pending || settled || committed) return { success: false, reason: "duplicate_usage_commit", quota };
      pending = Object.freeze({ metadata: Object.freeze({ ...metadata }), quota });
      return { success: true, reason: "usage_commit_pending", quota };
    },
    async settle({ httpStatus = 200, body = null } = {}) {
      if (settled) return { kind: "DUPLICATE_SETTLEMENT", committed };
      settled = true;
      const authority = evaluateCoordinateUsageAuthority({ httpStatus, body });
      if (!pending || regressionTestMode || !authority.eligible) {
        return {
          kind: "UNCHARGED_RESPONSE",
          committed: false,
          authorityReason: regressionTestMode
            ? "REGRESSION_TEST"
            : !pending
              ? "USAGE_NOT_SCHEDULED"
              : authority.reason
        };
      }
      budget?.assertCanContinue({ stageName: "usage_commit" });
      const stage = budget?.stageStarted("usage_commit");
      let stageResult = "success";
      try {
        budget?.markUsageCommitState?.("PREPARING");
        const prepared = await atomicityService.prepare({
          recognitionRequestId,
          userId,
          sessionBindingSha256,
          responsePayload: body,
          providerCostState: providerCostState()
        });
        budget?.markUsageCommitState?.("PREPARED");
        if (prepared.result === COORDINATE_USAGE_COMMIT_RESULT.ALREADY_COMMITTED) {
          committed = true;
          budget?.markUserUsageConsumed(true);
          budget?.markUsageCommitState?.("COMMITTED");
          return { kind: "USAGE_COMMITTED", committed: true, commitResult: prepared };
        }
        budget?.markUsageCommitState?.("COMMITTING");
        const commitResult = await atomicityService.commit({ recognitionRequestId, userId, sessionBindingSha256 });
        if ([COORDINATE_USAGE_COMMIT_RESULT.COMMITTED, COORDINATE_USAGE_COMMIT_RESULT.ALREADY_COMMITTED].includes(commitResult.result)) {
          committed = true;
          budget?.markUserUsageConsumed(true);
          budget?.markUsageCommitState?.("COMMITTED");
          return { kind: "USAGE_COMMITTED", committed: true, commitResult };
        }
        if (commitResult.result === COORDINATE_USAGE_COMMIT_RESULT.OUTCOME_UNKNOWN) {
          stageResult = "failed";
          return { kind: "USAGE_COMMIT_OUTCOME_UNKNOWN", committed: false, commitResult };
        }
        stageResult = "failed";
        budget?.markUsageCommitState?.("FAILED");
        return { kind: "USAGE_COMMIT_FAILED", committed: false, commitResult };
      } catch (error) {
        stageResult = error?.code === "RECOGNITION_BUDGET_EXHAUSTED" || error?.code === "RECOGNITION_DEADLINE_EXCEEDED"
          ? "budget_exhausted"
          : "failed";
        if (error?.code === COORDINATE_USAGE_ERROR_CODE.COMMIT_OUTCOME_UNKNOWN) {
          return { kind: "USAGE_COMMIT_OUTCOME_UNKNOWN", committed: false };
        }
        budget?.markUsageCommitState?.("FAILED");
        throw error;
      } finally {
        budget?.stageCompleted(stage, { result: stageResult });
      }
    },
    snapshot() {
      return Object.freeze({ scheduled: Boolean(pending), settled, committed });
    }
  });
}
