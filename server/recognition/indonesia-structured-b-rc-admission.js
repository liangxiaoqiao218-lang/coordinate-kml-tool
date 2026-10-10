import crypto from "node:crypto";

export const INDONESIA_STRUCTURED_B_RC = Object.freeze({
  mode: "indonesia_utm50s_structured_b",
  batchId: "indonesia-ab-20261010-v1",
  runId: "indonesia-b-rc-enablement-20261010-01",
  imageSha256: "41f2b2117667fb92f6a4eb703822b1893e29c985be2e14f7b20fbda103b66cf2",
  rcProjectRef: "thjojitdafxfarhxhoyo",
  rcServiceName: "coordinate-kml-tool-rc",
  providerHostname: "dashscope.aliyuncs.com",
  model: "qwen3.8-flash",
  runWindowSeconds: 1800,
  claimTtlSeconds: 600
});

export class IndonesiaStructuredBRcAdmissionError extends Error {
  constructor(code, httpStatus = 422) {
    super(code);
    this.name = "IndonesiaStructuredBRcAdmissionError";
    this.code = code;
    this.httpStatus = httpStatus;
  }
}
function fail(code, httpStatus) {
  throw new IndonesiaStructuredBRcAdmissionError(code, httpStatus);
}

function exactBoolean(value) {
  return value === "true";
}

function safeUuid(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u.test(normalized)
    ? normalized
    : "";
}

function safeSha256(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return /^[0-9a-f]{64}$/u.test(normalized) ? normalized : "";
}

function safeInteger(value) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isInteger(parsed) ? parsed : null;
}

function projectRefFromUrl(value) {
  try {
    const parsed = new URL(String(value || ""));
    const match = parsed.hostname.toLowerCase().match(/^([a-z0-9]{20})\.supabase\.co$/u);
    return parsed.protocol === "https:" && match ? match[1] : "";
  } catch {
    return "";
  }
}

function providerHostname(value) {
  try {
    const parsed = new URL(String(value || ""));
    return parsed.protocol === "https:" && !parsed.username && !parsed.password
      ? parsed.hostname.toLowerCase()
      : "";
  } catch {
    return "";
  }
}

function constantTimeTextEqual(left, right) {
  const actual = Buffer.from(String(left || ""), "utf8");
  const expected = Buffer.from(String(right || ""), "utf8");
  return actual.length > 0
    && actual.length === expected.length
    && crypto.timingSafeEqual(actual, expected);
}

function bearerToken(value) {
  const match = String(value || "").match(/^Bearer ([A-Za-z0-9_-]{32,256})$/u);
  return match ? match[1] : "";
}

function tokenHash(value) {
  return crypto.createHash("sha256").update(value, "utf8").digest("hex");
}

function imageHash(buffer) {
  return Buffer.isBuffer(buffer)
    ? crypto.createHash("sha256").update(buffer).digest("hex")
    : "";
}

function rpcData(result, fallbackCode) {
  if (result?.error) fail(fallbackCode, 503);
  const data = Array.isArray(result?.data) ? result.data[0] : result?.data;
  if (!data || typeof data !== "object") fail(fallbackCode, 503);
  return data;
}

function normalizeUsage(usage) {
  const promptTokens = Number(usage?.promptTokens ?? usage?.prompt_tokens);
  const completionTokens = Number(usage?.completionTokens ?? usage?.completion_tokens);
  const totalTokens = Number(usage?.totalTokens ?? usage?.total_tokens);
  if (![promptTokens, completionTokens, totalTokens].every(value => Number.isInteger(value) && value >= 0)
    || totalTokens !== promptTokens + completionTokens) {
    return Object.freeze({ promptTokens: null, completionTokens: null, totalTokens: null, estimatedCostCny: null });
  }
  const estimatedCostCny = Number(((promptTokens * 0.8 + completionTokens * 2.7) / 1_000_000).toFixed(10));
  return Object.freeze({ promptTokens, completionTokens, totalTokens, estimatedCostCny });
}

export function readIndonesiaStructuredBRcConfig(env = process.env) {
  const masterEnabled = exactBoolean(env.INDONESIA_STRUCTURED_B_PRODUCT_ENABLED);
  const singleRunEnabled = exactBoolean(env.INDONESIA_STRUCTURED_B_RC_SINGLE_RUN_ENABLED);
  const runId = String(env.INDONESIA_STRUCTURED_B_RC_SINGLE_RUN_ID || "").trim();
  const allowedImageSha256 = safeSha256(env.INDONESIA_STRUCTURED_B_RC_ALLOWED_IMAGE_SHA256);
  const runWindowSeconds = safeInteger(env.INDONESIA_STRUCTURED_B_RC_SINGLE_RUN_WINDOW_SECONDS);
  const claimTtlSeconds = safeInteger(env.INDONESIA_STRUCTURED_B_RC_CLAIM_TTL_SECONDS);
  const databaseProjectRef = projectRefFromUrl(env.SUPABASE_URL);
  const expectedProjectRef = String(env.RC_SUPABASE_PROJECT_REF || "").trim();
  const serviceName = String(env.RENDER_SERVICE_NAME || "").trim().toLowerCase();
  const rcServiceScoped = serviceName === INDONESIA_STRUCTURED_B_RC.rcServiceName;
  const singleRunConfigured = singleRunEnabled
    || Boolean(runId)
    || Boolean(allowedImageSha256)
    || runWindowSeconds !== null
    || claimTtlSeconds !== null;
  const guardRequired = rcServiceScoped || singleRunConfigured;
  const model = String(env.ALIYUN_VISION_MODEL || env.DASHSCOPE_VISION_MODEL || "qwen3.8-flash").trim();
  const endpointHostname = providerHostname(
    env.ALIYUN_BASE_URL || env.DASHSCOPE_BASE_URL || "https://dashscope.aliyuncs.com/compatible-mode/v1"
  );
  const configured = masterEnabled || singleRunEnabled;
  const ready = configured
    && masterEnabled
    && singleRunEnabled
    && runId === INDONESIA_STRUCTURED_B_RC.runId
    && allowedImageSha256 === INDONESIA_STRUCTURED_B_RC.imageSha256
    && runWindowSeconds === INDONESIA_STRUCTURED_B_RC.runWindowSeconds
    && claimTtlSeconds === INDONESIA_STRUCTURED_B_RC.claimTtlSeconds
    && rcServiceScoped
    && databaseProjectRef === INDONESIA_STRUCTURED_B_RC.rcProjectRef
    && expectedProjectRef === INDONESIA_STRUCTURED_B_RC.rcProjectRef
    && model === INDONESIA_STRUCTURED_B_RC.model
    && endpointHostname === INDONESIA_STRUCTURED_B_RC.providerHostname;
  return Object.freeze({
    configured,
    ready,
    masterEnabled,
    singleRunEnabled,
    runId,
    allowedImageSha256,
    runWindowSeconds,
    claimTtlSeconds,
    databaseProjectRef,
    expectedProjectRef,
    serviceName,
    rcServiceScoped,
    singleRunConfigured,
    guardRequired,
    model,
    endpointHostname
  });
}

export function createIndonesiaStructuredBRcAdmission({
  supabase,
  adminPassword,
  env = process.env,
  randomToken = () => crypto.randomBytes(32).toString("base64url")
} = {}) {
  const config = readIndonesiaStructuredBRcConfig(env);
  let activationState = config.singleRunEnabled ? "PENDING" : "DISABLED";
  let activationPromise = null;

  function requireReady() {
    if (!config.ready || !supabase || typeof supabase.rpc !== "function") {
      fail("INDONESIA_STRUCTURED_B_RC_ADMISSION_UNAVAILABLE", 503);
    }
  }

  async function activateOnStartup() {
    if (!config.singleRunEnabled) return Object.freeze({ activated: false, reason: "DISABLED" });
    if (activationPromise) return activationPromise;
    activationPromise = (async () => {
      requireReady();
      const result = await supabase.rpc("claim_indonesia_structured_b_single_run", {
        p_project_ref: INDONESIA_STRUCTURED_B_RC.rcProjectRef,
        p_run_id: INDONESIA_STRUCTURED_B_RC.runId,
        p_batch_id: INDONESIA_STRUCTURED_B_RC.batchId,
        p_image_sha256: INDONESIA_STRUCTURED_B_RC.imageSha256,
        p_recognition_request_id: null,
        p_token_sha256: null,
        p_activation_only: true
      });
      const data = rpcData(result, "INDONESIA_STRUCTURED_B_RC_ACTIVATION_FAILED");
      if (data.accepted !== true || data.action !== "ACTIVATED") {
        fail(String(data.code || "INDONESIA_STRUCTURED_B_RC_ACTIVATION_REJECTED"), 503);
      }
      activationState = "READY";
      return Object.freeze({ activated: true });
    })().catch(error => {
      activationState = "FAILED";
      throw error;
    });
    return activationPromise;
  }

  function authorizeAdmin(headerValue) {
    requireReady();
    if (!constantTimeTextEqual(headerValue, adminPassword)) {
      fail("INDONESIA_STRUCTURED_B_RC_ADMIN_REQUIRED", 401);
    }
    return true;
  }

  async function claim({ adminHeader, runId, batchId, imageSha256, recognitionRequestId } = {}) {
    authorizeAdmin(adminHeader);
    await activateOnStartup();
    const requestId = safeUuid(recognitionRequestId);
    if (runId !== INDONESIA_STRUCTURED_B_RC.runId
      || batchId !== INDONESIA_STRUCTURED_B_RC.batchId
      || safeSha256(imageSha256) !== INDONESIA_STRUCTURED_B_RC.imageSha256
      || !requestId) {
      fail("INDONESIA_STRUCTURED_B_RC_CLAIM_BINDING_INVALID", 422);
    }
    const claimToken = randomToken();
    if (!/^[A-Za-z0-9_-]{32,256}$/u.test(claimToken)) fail("INDONESIA_STRUCTURED_B_RC_TOKEN_GENERATION_FAILED", 500);
    const result = await supabase.rpc("claim_indonesia_structured_b_single_run", {
      p_project_ref: INDONESIA_STRUCTURED_B_RC.rcProjectRef,
      p_run_id: runId,
      p_batch_id: batchId,
      p_image_sha256: INDONESIA_STRUCTURED_B_RC.imageSha256,
      p_recognition_request_id: requestId,
      p_token_sha256: tokenHash(claimToken),
      p_activation_only: false
    });
    const data = rpcData(result, "INDONESIA_STRUCTURED_B_RC_CLAIM_FAILED");
    if (data.accepted !== true || data.action !== "CLAIMED") {
      fail(String(data.code || "INDONESIA_STRUCTURED_B_RC_CLAIM_REJECTED"), 409);
    }
    return Object.freeze({
      accepted: true,
      claimToken,
      recognitionRequestId: requestId,
      expiresAt: typeof data.expires_at === "string" ? data.expires_at : null
    });
  }

  function preflightJob({ mode, recognitionRequestId, imageBuffer, authorization } = {}) {
    requireReady();
    if (activationState !== "READY") fail("INDONESIA_STRUCTURED_B_RC_NOT_ACTIVATED", 503);
    const requestId = safeUuid(recognitionRequestId);
    const actualImageSha256 = imageHash(imageBuffer);
    const token = bearerToken(authorization);
    if (mode !== INDONESIA_STRUCTURED_B_RC.mode
      || !requestId
      || actualImageSha256 !== INDONESIA_STRUCTURED_B_RC.imageSha256
      || !token) {
      fail("INDONESIA_STRUCTURED_B_RC_JOB_PREFLIGHT_REJECTED", 422);
    }
    return Object.freeze({ requestId, imageSha256: actualImageSha256 });
  }

  async function dispatch({ mode, recognitionRequestId, imageBuffer, authorization, asyncAuthorized } = {}) {
    const preflight = preflightJob({ mode, recognitionRequestId, imageBuffer, authorization });
    if (asyncAuthorized !== true) fail("INDONESIA_STRUCTURED_B_RC_ASYNC_JOB_REQUIRED", 404);
    const token = bearerToken(authorization);
    const result = await supabase.rpc("dispatch_indonesia_structured_b_single_run", {
      p_project_ref: INDONESIA_STRUCTURED_B_RC.rcProjectRef,
      p_run_id: INDONESIA_STRUCTURED_B_RC.runId,
      p_batch_id: INDONESIA_STRUCTURED_B_RC.batchId,
      p_image_sha256: preflight.imageSha256,
      p_recognition_request_id: preflight.requestId,
      p_token_sha256: tokenHash(token)
    });
    const data = rpcData(result, "INDONESIA_STRUCTURED_B_RC_DISPATCH_FAILED");
    if (data.accepted !== true || data.action !== "DISPATCHED") {
      fail(String(data.code || "INDONESIA_STRUCTURED_B_RC_DISPATCH_REJECTED"), 409);
    }
    return Object.freeze({ dispatched: true, requestId: preflight.requestId, imageSha256: preflight.imageSha256 });
  }

  async function settle({ recognitionRequestId, authorization, outcome, providerCallCount, usage } = {}) {
    requireReady();
    const requestId = safeUuid(recognitionRequestId);
    const token = bearerToken(authorization);
    const allowedOutcomes = new Set(["COMPLETED", "FAILED_AFTER_PROVIDER", "OUTCOME_UNKNOWN", "FAILED_PRE_PROVIDER"]);
    if (!requestId || !token || !allowedOutcomes.has(outcome) || ![0, 1].includes(providerCallCount)) {
      fail("INDONESIA_STRUCTURED_B_RC_SETTLEMENT_INPUT_INVALID", 500);
    }
    const normalizedUsage = normalizeUsage(usage);
    const result = await supabase.rpc("settle_indonesia_structured_b_single_run", {
      p_project_ref: INDONESIA_STRUCTURED_B_RC.rcProjectRef,
      p_run_id: INDONESIA_STRUCTURED_B_RC.runId,
      p_batch_id: INDONESIA_STRUCTURED_B_RC.batchId,
      p_recognition_request_id: requestId,
      p_token_sha256: tokenHash(token),
      p_outcome: outcome,
      p_provider_call_count: providerCallCount,
      p_prompt_tokens: normalizedUsage.promptTokens,
      p_completion_tokens: normalizedUsage.completionTokens,
      p_total_tokens: normalizedUsage.totalTokens,
      p_estimated_cost_cny: normalizedUsage.estimatedCostCny
    });
    const data = rpcData(result, "INDONESIA_STRUCTURED_B_RC_SETTLEMENT_FAILED");
    if (data.accepted !== true || data.action !== "SETTLED") {
      fail(String(data.code || "INDONESIA_STRUCTURED_B_RC_SETTLEMENT_REJECTED"), 503);
    }
    return Object.freeze({ settled: true, outcome, usage: normalizedUsage });
  }

  async function terminatePreProviderClaim({ recognitionRequestId, authorization } = {}) {
    return settle({
      recognitionRequestId,
      authorization,
      outcome: "FAILED_PRE_PROVIDER",
      providerCallCount: 0
    });
  }

  async function preflightJobOrTerminate(input = {}) {
    try {
      return preflightJob(input);
    } catch (error) {
      try {
        await terminatePreProviderClaim(input);
      } catch {
        // Preserve the preflight result; only a correctly bound reserved claim can be terminated.
      }
      throw error;
    }
  }

  async function close() {
    requireReady();
    const result = await supabase.rpc("close_indonesia_structured_b_single_run", {
      p_project_ref: INDONESIA_STRUCTURED_B_RC.rcProjectRef,
      p_run_id: INDONESIA_STRUCTURED_B_RC.runId,
      p_batch_id: INDONESIA_STRUCTURED_B_RC.batchId
    });
    const data = rpcData(result, "INDONESIA_STRUCTURED_B_RC_CLOSE_FAILED");
    if (data.accepted !== true || data.action !== "CLOSED") {
      fail(String(data.code || "INDONESIA_STRUCTURED_B_RC_CLOSE_REJECTED"), 503);
    }
    activationState = "CLOSED";
    return Object.freeze({ closed: true });
  }

  return Object.freeze({
    config,
    activateOnStartup,
    authorizeAdmin,
    claim,
    preflightJob,
    preflightJobOrTerminate,
    terminatePreProviderClaim,
    dispatch,
    settle,
    close,
    getActivationState: () => activationState
  });
}
