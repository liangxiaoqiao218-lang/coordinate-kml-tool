import crypto from "node:crypto";

export const USAGE_POLICY_VERSION = "usage_policy_v1";
export const FREE_TRIAL_DAILY_MAX = 3;
export const FREE_TRIAL_LIFETIME_MAX = 12;
export const FREE_DAY_TIMEZONE = "Asia/Shanghai";
export const USAGE_IDENTITY_COOKIE = "geokit_usage_identity_v1";
export const USAGE_OPERATION_HEADER = "x-usage-operation-token";

export const USAGE_EVENT_TYPE = Object.freeze({
  COORDINATE: "SUCCESSFUL_NEW_COORDINATE_RESULT",
  JUDGE: "SUCCESSFUL_NEW_JUDGE_RESULT"
});

const formatter = new Intl.DateTimeFormat("en-CA", {
  timeZone: FREE_DAY_TIMEZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit"
});

export function getFreeDayKey(value = new Date()) {
  const date = value instanceof Date ? value : new Date(value);
  if (!Number.isFinite(date.getTime())) throw new TypeError("Invalid free-day timestamp");
  const parts = Object.fromEntries(formatter.formatToParts(date).map(part => [part.type, part.value]));
  return `${parts.year}-${parts.month}-${parts.day}`;
}

export function createServiceOperationId() {
  return crypto.randomUUID();
}

export function isServiceOperationId(value) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(String(value || ""));
}

export class UsageTokenError extends Error {
  constructor(code, message) {
    super(message);
    this.name = "UsageTokenError";
    this.code = code;
    this.status = 401;
  }
}

function encodeTokenPart(value) {
  return Buffer.from(value).toString("base64url");
}

function decodeTokenPart(value) {
  return Buffer.from(String(value || ""), "base64url").toString("utf8");
}

function safeEqual(left, right) {
  const a = Buffer.from(String(left || ""));
  const b = Buffer.from(String(right || ""));
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

export function createUsageTokenAuthority({
  secret,
  now = () => Date.now(),
  identityTtlMs = 365 * 24 * 60 * 60 * 1000,
  operationTtlMs = 24 * 60 * 60 * 1000
} = {}) {
  const signingKey = Buffer.isBuffer(secret)
    ? Buffer.from(secret)
    : Buffer.from(String(secret || ""), "utf8");
  const key = signingKey.length >= 32 ? signingKey : crypto.randomBytes(32);

  function sign(kind, payload) {
    const body = encodeTokenPart(JSON.stringify({ version: 1, kind, ...payload }));
    const signature = crypto.createHmac("sha256", key).update(`uq01.${body}`).digest("base64url");
    return `uq01.${body}.${signature}`;
  }

  function verify(token, expectedKind) {
    const [prefix, body, signature, extra] = String(token || "").split(".");
    if (prefix !== "uq01" || !body || !signature || extra !== undefined) {
      throw new UsageTokenError("USAGE_TOKEN_INVALID", "Usage token is invalid");
    }
    const expected = crypto.createHmac("sha256", key).update(`uq01.${body}`).digest("base64url");
    if (!safeEqual(signature, expected)) {
      throw new UsageTokenError("USAGE_TOKEN_INVALID_SIGNATURE", "Usage token signature is invalid");
    }
    let payload;
    try {
      payload = JSON.parse(decodeTokenPart(body));
    } catch {
      throw new UsageTokenError("USAGE_TOKEN_INVALID_PAYLOAD", "Usage token payload is invalid");
    }
    if (payload?.version !== 1 || payload?.kind !== expectedKind || Number(payload.expiresAt) <= now()) {
      throw new UsageTokenError("USAGE_TOKEN_EXPIRED_OR_WRONG_KIND", "Usage token is expired or has the wrong kind");
    }
    return payload;
  }

  function issueIdentity() {
    const usageIdentity = `visitor:${crypto.randomUUID()}`;
    const issuedAt = now();
    return Object.freeze({
      usageIdentity,
      token: sign("identity", { usageIdentity, issuedAt, expiresAt: issuedAt + identityTtlMs })
    });
  }

  function resolveIdentity(identityToken) {
    const payload = verify(identityToken, "identity");
    if (!/^visitor:[0-9a-f-]{36}$/i.test(String(payload.usageIdentity || ""))) {
      throw new UsageTokenError("USAGE_IDENTITY_INVALID", "Usage identity is invalid");
    }
    return payload.usageIdentity;
  }

  function issueOperation({ identityToken, usageEventType, requestFingerprint = "" } = {}) {
    if (!Object.values(USAGE_EVENT_TYPE).includes(usageEventType)) {
      throw new UsageTokenError("USAGE_EVENT_TYPE_INVALID", "Usage event type is invalid");
    }
    const usageIdentity = resolveIdentity(identityToken);
    const serviceOperationId = createServiceOperationId();
    const issuedAt = now();
    return Object.freeze({
      usageIdentity,
      serviceOperationId,
      token: sign("operation", {
        usageIdentity,
        usageEventType,
        serviceOperationId,
        requestFingerprint: String(requestFingerprint || "").trim().toLowerCase(),
        issuedAt,
        expiresAt: issuedAt + operationTtlMs
      })
    });
  }

  function resolveOperation({ identityToken, operationToken, usageEventType, requestFingerprint = "" } = {}) {
    const usageIdentity = resolveIdentity(identityToken);
    const operation = verify(operationToken, "operation");
    if (operation.usageIdentity !== usageIdentity || operation.usageEventType !== usageEventType) {
      throw new UsageTokenError("USAGE_OPERATION_BINDING_MISMATCH", "Usage operation is not bound to this identity and tool");
    }
    if (!isServiceOperationId(operation.serviceOperationId)) {
      throw new UsageTokenError("USAGE_OPERATION_ID_INVALID", "Usage operation ID is invalid");
    }
    const expectedFingerprint = String(operation.requestFingerprint || "");
    const actualFingerprint = String(requestFingerprint || "").trim().toLowerCase();
    if (expectedFingerprint && expectedFingerprint !== actualFingerprint) {
      throw new UsageTokenError("USAGE_OPERATION_REQUEST_MISMATCH", "Usage operation is not bound to this request payload");
    }
    return Object.freeze({ usageIdentity, serviceOperationId: operation.serviceOperationId });
  }

  return Object.freeze({ issueIdentity, resolveIdentity, issueOperation, resolveOperation });
}

function finiteCoordinate(value) {
  return Number.isFinite(Number(value));
}

function usableGeometry(geometry) {
  if (!geometry || typeof geometry !== "object") return false;
  const groups = Array.isArray(geometry.coordinates) ? geometry.coordinates.flat(3) : [];
  return groups.length >= 2 && groups.every(finiteCoordinate);
}

export function isCoordinateResultChargeable(response = {}) {
  const result = response.finalizedCoordinateResult;
  if (!result || typeof result !== "object") return false;
  if (!String(result.resultId || "").trim() || !Number.isSafeInteger(result.resultRevision)) return false;
  if (!String(result.geometryHash || "").trim() || !usableGeometry(result.geometry)) return false;
  if (result.decisionState === "BLOCKED" || result.qualityStatus === "FAILED") return false;
  return result.kmlReady === true || result.mapPreviewReady === true;
}

export function isJudgeResultChargeable({ normalizedResult, persistedCaseId } = {}) {
  return Boolean(String(normalizedResult || "").trim() && String(persistedCaseId || "").trim());
}

export function toSharedQuotaPayload(payload = {}, product = "coordinate") {
  const dailyMax = Math.max(0, Number(payload.free_trial_daily_max ?? FREE_TRIAL_DAILY_MAX));
  const lifetimeMax = Math.max(0, Number(payload.free_trial_lifetime_max ?? FREE_TRIAL_LIFETIME_MAX));
  const freeDailyRemaining = Math.max(0, Number(payload.free_daily_remaining || 0));
  const freeLifetimeRemaining = Math.max(0, Number(payload.free_lifetime_remaining || 0));
  const freeRemaining = Math.min(freeDailyRemaining, freeLifetimeRemaining);
  const paidConvert = Math.max(0, Number(payload.paid_convert_count || 0));
  const paidJudge = Math.max(0, Number(payload.paid_judge_count || 0));
  return {
    usage_policy_version: USAGE_POLICY_VERSION,
    free_day_timezone: FREE_DAY_TIMEZONE,
    free_trial_daily_max: dailyMax,
    free_trial_lifetime_max: lifetimeMax,
    free_daily_used: Math.max(0, Number(payload.free_daily_used || 0)),
    free_lifetime_used: Math.max(0, Number(payload.free_lifetime_used || 0)),
    free_daily_remaining: freeDailyRemaining,
    free_lifetime_remaining: freeLifetimeRemaining,
    free_shared_remaining: freeRemaining,
    paid_convert_count: paidConvert,
    paid_judge_count: paidJudge,
    convert_remaining: freeRemaining + paidConvert,
    judge_remaining: freeRemaining + paidJudge,
    requested_product: product,
    freeTrial: {
      todayRemaining: freeDailyRemaining,
      lifetimeRemaining: freeLifetimeRemaining,
      exhausted: freeLifetimeRemaining === 0
    },
    paid: {
      coordinateRemaining: paidConvert,
      judgeRemaining: paidJudge
    }
  };
}
