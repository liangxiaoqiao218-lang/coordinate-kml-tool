import crypto from "node:crypto";

export const COORDINATE_THREE_CASE_RC = Object.freeze({
  batchId: "coordinate-three-case-rc-20261011-v1",
  runId: "coordinate-three-case-rc-20261011-r1",
  manifestSha256: "0485cf93fe4c43695ea34f803595a704ca8a27b81978fc4a3436edd9013e255f",
  rcProjectRef: "thjojitdafxfarhxhoyo",
  rcServiceName: "coordinate-kml-tool-rc",
  providerHostname: "dashscope.aliyuncs.com",
  model: "qwen3.8-flash",
  runWindowSeconds: 1800,
  claimTtlSeconds: 600,
  directOcrCalls: 0,
  automaticRetries: 0,
  cases: Object.freeze({
    indonesia: Object.freeze({
      caseId: "indonesia",
      imageSha256: "41f2b2117667fb92f6a4eb703822b1893e29c985be2e14f7b20fbda103b66cf2",
      productMode: "indonesia_utm50s_structured_b",
      budgetPool: "INDONESIA_HISTORICAL_6_CALL_5_CNY",
      reserveCny: 0.816
    }),
    mgrs: Object.freeze({
      caseId: "mgrs",
      imageSha256: "af999328e3232af304e03901c5e8d58cad794ea588ee31709aef6668974f4004",
      productMode: "",
      budgetPool: "MGRS_BFTM_SHARED_2_CNY",
      reserveCny: 1
    }),
    bftm: Object.freeze({
      caseId: "bftm",
      imageSha256: "4567ce9889c47d65414b19e543c06b5322b86c53b76bb53e87da761bc0988e1d",
      productMode: "",
      budgetPool: "MGRS_BFTM_SHARED_2_CNY",
      reserveCny: 1
    })
  })
});

const CASE_IDS = Object.freeze(Object.keys(COORDINATE_THREE_CASE_RC.cases));
const OUTCOMES = Object.freeze(new Set([
  "COMPLETED",
  "FAILED_AFTER_PROVIDER",
  "FAILED_PRE_PROVIDER",
  "OUTCOME_UNKNOWN"
]));

export class CoordinateThreeCaseRcAdmissionError extends Error {
  constructor(code, httpStatus = 422) {
    super(code);
    this.name = "CoordinateThreeCaseRcAdmissionError";
    this.code = code;
    this.httpStatus = httpStatus;
  }
}
function fail(code, httpStatus) {
  throw new CoordinateThreeCaseRcAdmissionError(code, httpStatus);
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

function finiteNumberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function integerOrNull(value) {
  const parsed = finiteNumberOrNull(value);
  return Number.isInteger(parsed) ? parsed : null;
}

function safeCaseId(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return CASE_IDS.includes(normalized) ? normalized : "";
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
    && expected.length > 0
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
  const rawPromptTokens = usage?.promptTokens ?? usage?.prompt_tokens;
  const rawCompletionTokens = usage?.completionTokens ?? usage?.completion_tokens;
  const rawTotalTokens = usage?.totalTokens ?? usage?.total_tokens;
  const rawValues = [rawPromptTokens, rawCompletionTokens, rawTotalTokens];
  if (!rawValues.every(value => typeof value === "number" && Number.isSafeInteger(value) && value >= 0)
    || rawTotalTokens !== rawPromptTokens + rawCompletionTokens) {
    return Object.freeze({
      promptTokens: null,
      completionTokens: null,
      totalTokens: null,
      estimatedCostCny: null,
      costEvidence: "UNKNOWN"
    });
  }
  const promptTokens = rawPromptTokens;
  const completionTokens = rawCompletionTokens;
  const totalTokens = rawTotalTokens;
  const estimatedCostCny = Number(((promptTokens * 0.8 + completionTokens * 2.7) / 1_000_000).toFixed(10));
  return Object.freeze({
    promptTokens,
    completionTokens,
    totalTokens,
    estimatedCostCny,
    costEvidence: "USAGE_REPORTED"
  });
}

function sanitizeStatus(data) {
  const cases = Array.isArray(data?.cases) ? data.cases : [];
  return Object.freeze({
    accepted: data?.accepted === true,
    action: String(data?.action || "STATUS"),
    runStatus: String(data?.run_status || "UNKNOWN"),
    runActivated: data?.run_activated === true,
    runClosed: data?.run_closed === true,
    activeDispatches: integerOrNull(data?.active_dispatches),
    totalDispatched: integerOrNull(data?.total_dispatched),
    indonesiaHistoricalDispatched: integerOrNull(data?.indonesia_historical_dispatched),
    indonesiaHistoricalReservedCny: finiteNumberOrNull(data?.indonesia_historical_reserved_cny),
    nonIndonesiaDispatched: integerOrNull(data?.non_indonesia_dispatched),
    nonIndonesiaReservedCny: finiteNumberOrNull(data?.non_indonesia_reserved_cny),
    nonIndonesiaEstimatedCostCny: finiteNumberOrNull(data?.non_indonesia_estimated_cost_cny),
    cases: Object.freeze(cases.map(entry => Object.freeze({
      caseId: safeCaseId(entry?.case_id) || "UNKNOWN",
      status: String(entry?.status || "UNKNOWN"),
      claimCount: integerOrNull(entry?.claim_count),
      dispatchCount: integerOrNull(entry?.dispatch_count),
      providerCallCount: integerOrNull(entry?.provider_call_count),
      outcome: String(entry?.outcome || "NONE"),
      costEvidence: String(entry?.cost_evidence || "UNKNOWN"),
      estimatedCostCny: finiteNumberOrNull(entry?.estimated_cost_cny)
    })))
  });
}

export function readCoordinateThreeCaseRcConfig(env = process.env) {
  const enabled = exactBoolean(env.COORDINATE_THREE_CASE_RC_ENABLED);
  const runId = String(env.COORDINATE_THREE_CASE_RC_RUN_ID || "").trim();
  const runWindowSeconds = safeInteger(env.COORDINATE_THREE_CASE_RC_WINDOW_SECONDS);
  const claimTtlSeconds = safeInteger(env.COORDINATE_THREE_CASE_RC_CLAIM_TTL_SECONDS);
  const databaseProjectRef = projectRefFromUrl(env.SUPABASE_URL);
  const expectedProjectRef = String(env.RC_SUPABASE_PROJECT_REF || "").trim();
  const serviceName = String(env.RENDER_SERVICE_NAME || "").trim().toLowerCase();
  const model = String(env.ALIYUN_VISION_MODEL || env.DASHSCOPE_VISION_MODEL || "qwen3.8-flash").trim();
  const endpointHostname = providerHostname(
    env.ALIYUN_BASE_URL || env.DASHSCOPE_BASE_URL || "https://dashscope.aliyuncs.com/compatible-mode/v1"
  );
  const ready = enabled
    && runId === COORDINATE_THREE_CASE_RC.runId
    && runWindowSeconds === COORDINATE_THREE_CASE_RC.runWindowSeconds
    && claimTtlSeconds === COORDINATE_THREE_CASE_RC.claimTtlSeconds
    && serviceName === COORDINATE_THREE_CASE_RC.rcServiceName
    && databaseProjectRef === COORDINATE_THREE_CASE_RC.rcProjectRef
    && expectedProjectRef === COORDINATE_THREE_CASE_RC.rcProjectRef
    && model === COORDINATE_THREE_CASE_RC.model
    && endpointHostname === COORDINATE_THREE_CASE_RC.providerHostname;
  // A malformed RC configuration must never reopen an ordinary, unclaimed
  // Provider path on the controlled RC service.
  const explicitRunConfigurationPresent = enabled
    || Boolean(runId)
    || runWindowSeconds !== null
    || claimTtlSeconds !== null;
  const guardRequired = serviceName === COORDINATE_THREE_CASE_RC.rcServiceName
    || explicitRunConfigurationPresent;
  return Object.freeze({
    enabled,
    ready,
    guardRequired,
    runId,
    runWindowSeconds,
    claimTtlSeconds,
    databaseProjectRef,
    expectedProjectRef,
    serviceName,
    model,
    endpointHostname
  });
}

export function createCoordinateThreeCaseRcAdmission({
  supabase,
  adminPassword,
  env = process.env,
  randomToken = () => crypto.randomBytes(32).toString("base64url"),
  hashImage = imageHash
} = {}) {
  const config = readCoordinateThreeCaseRcConfig(env);
  const sessions = new WeakMap();
  let activationState = config.enabled ? "PENDING" : "DISABLED";
  let activationPromise = null;

  function requireReady() {
    if (!config.ready || !supabase || typeof supabase.rpc !== "function") {
      fail("COORDINATE_THREE_CASE_RC_ADMISSION_UNAVAILABLE", 503);
    }
  }

  function authorizeAdmin(headerValue) {
    requireReady();
    if (!constantTimeTextEqual(headerValue, adminPassword)) {
      fail("COORDINATE_THREE_CASE_RC_ADMIN_REQUIRED", 401);
    }
    return true;
  }

  async function activateOnStartup() {
    if (!config.enabled) return Object.freeze({ activated: false, reason: "DISABLED" });
    if (activationPromise) return activationPromise;
    activationPromise = (async () => {
      requireReady();
      const data = rpcData(await supabase.rpc("activate_coordinate_three_case_rc_run", {
        p_project_ref: COORDINATE_THREE_CASE_RC.rcProjectRef,
        p_run_id: COORDINATE_THREE_CASE_RC.runId,
        p_batch_id: COORDINATE_THREE_CASE_RC.batchId,
        p_manifest_sha256: COORDINATE_THREE_CASE_RC.manifestSha256
      }), "COORDINATE_THREE_CASE_RC_ACTIVATION_FAILED");
      if (data.accepted !== true || !["ACTIVATED", "ALREADY_ACTIVE"].includes(data.action)) {
        fail(String(data.code || "COORDINATE_THREE_CASE_RC_ACTIVATION_REJECTED"), 503);
      }
      activationState = "READY";
      return Object.freeze({ activated: true, action: data.action });
    })().catch(error => {
      activationState = "FAILED";
      throw error;
    });
    return activationPromise;
  }

  async function claim({ adminHeader, runId, batchId, caseId, imageSha256, recognitionRequestId } = {}) {
    authorizeAdmin(adminHeader);
    await activateOnStartup();
    const normalizedCaseId = safeCaseId(caseId);
    const definition = COORDINATE_THREE_CASE_RC.cases[normalizedCaseId];
    const requestId = safeUuid(recognitionRequestId);
    if (runId !== COORDINATE_THREE_CASE_RC.runId
      || batchId !== COORDINATE_THREE_CASE_RC.batchId
      || !definition
      || safeSha256(imageSha256) !== definition.imageSha256
      || !requestId) {
      fail("COORDINATE_THREE_CASE_RC_CLAIM_BINDING_INVALID", 422);
    }
    const claimToken = randomToken();
    if (!/^[A-Za-z0-9_-]{32,256}$/u.test(claimToken)) {
      fail("COORDINATE_THREE_CASE_RC_TOKEN_GENERATION_FAILED", 500);
    }
    const data = rpcData(await supabase.rpc("claim_coordinate_three_case_rc", {
      p_project_ref: COORDINATE_THREE_CASE_RC.rcProjectRef,
      p_run_id: runId,
      p_batch_id: batchId,
      p_manifest_sha256: COORDINATE_THREE_CASE_RC.manifestSha256,
      p_case_id: normalizedCaseId,
      p_image_sha256: definition.imageSha256,
      p_recognition_request_id: requestId,
      p_token_sha256: tokenHash(claimToken)
    }), "COORDINATE_THREE_CASE_RC_CLAIM_FAILED");
    if (data.accepted !== true || data.action !== "CLAIMED") {
      fail(String(data.code || "COORDINATE_THREE_CASE_RC_CLAIM_REJECTED"), 409);
    }
    return Object.freeze({
      accepted: true,
      caseId: normalizedCaseId,
      claimToken,
      recognitionRequestId: requestId,
      expiresAt: typeof data.expires_at === "string" ? data.expires_at : null
    });
  }

  function preflightJob({ caseId, productMode = "", recognitionRequestId, imageBuffer, authorization } = {}) {
    requireReady();
    if (activationState !== "READY") fail("COORDINATE_THREE_CASE_RC_NOT_ACTIVATED", 503);
    const normalizedCaseId = safeCaseId(caseId);
    const definition = COORDINATE_THREE_CASE_RC.cases[normalizedCaseId];
    const requestId = safeUuid(recognitionRequestId);
    const actualImageSha256 = hashImage(imageBuffer);
    const token = bearerToken(authorization);
    if (!definition
      || !requestId
      || actualImageSha256 !== definition.imageSha256
      || String(productMode || "").trim() !== definition.productMode
      || !token) {
      fail("COORDINATE_THREE_CASE_RC_JOB_PREFLIGHT_REJECTED", 422);
    }
    return Object.freeze({
      caseId: normalizedCaseId,
      requestId,
      imageSha256: actualImageSha256,
      tokenHash: tokenHash(token)
    });
  }

  function bindBudget({ budget, ...input } = {}) {
    if (!config.guardRequired) return Object.freeze({ bound: false, reason: "DISABLED" });
    if (!budget || typeof budget !== "object") fail("COORDINATE_THREE_CASE_RC_BUDGET_REQUIRED", 503);
    const preflight = preflightJob(input);
    const existing = sessions.get(budget);
    if (existing) {
      if (existing.caseId !== preflight.caseId || existing.requestId !== preflight.requestId) {
        fail("COORDINATE_THREE_CASE_RC_BUDGET_REBIND_REJECTED", 409);
      }
      return Object.freeze({ bound: true, caseId: existing.caseId });
    }
    sessions.set(budget, {
      ...preflight,
      dispatched: false,
      settled: false,
      usage: normalizeUsage(null),
      providerCompletionState: "NOT_STARTED"
    });
    return Object.freeze({ bound: true, caseId: preflight.caseId });
  }

  async function dispatchForBudget({ budget, modelName, maxTokens, enableThinking } = {}) {
    if (!config.guardRequired) return Object.freeze({ dispatched: false, reason: "DISABLED" });
    requireReady();
    const session = sessions.get(budget);
    if (!session || session.settled) fail("COORDINATE_THREE_CASE_RC_PROVIDER_CONTEXT_MISSING", 503);
    if (session.dispatched) fail("COORDINATE_THREE_CASE_RC_DUPLICATE_PROVIDER_DISPATCH", 409);
    if (String(modelName || "").trim() !== COORDINATE_THREE_CASE_RC.model) {
      fail("COORDINATE_THREE_CASE_RC_MODEL_MISMATCH", 422);
    }
    const outputTokenLimit = Number(maxTokens);
    if (!Number.isInteger(outputTokenLimit)
      || outputTokenLimit <= 0
      || outputTokenLimit > 8_000
      || enableThinking !== false) {
      fail("COORDINATE_THREE_CASE_RC_PROVIDER_PARAMETERS_REJECTED", 422);
    }
    const data = rpcData(await supabase.rpc("dispatch_coordinate_three_case_rc", {
      p_project_ref: COORDINATE_THREE_CASE_RC.rcProjectRef,
      p_run_id: COORDINATE_THREE_CASE_RC.runId,
      p_batch_id: COORDINATE_THREE_CASE_RC.batchId,
      p_manifest_sha256: COORDINATE_THREE_CASE_RC.manifestSha256,
      p_case_id: session.caseId,
      p_image_sha256: session.imageSha256,
      p_recognition_request_id: session.requestId,
      p_token_sha256: session.tokenHash,
      p_model: COORDINATE_THREE_CASE_RC.model
    }), "COORDINATE_THREE_CASE_RC_DISPATCH_FAILED");
    if (data.accepted !== true || data.action !== "DISPATCHED") {
      fail(String(data.code || "COORDINATE_THREE_CASE_RC_DISPATCH_REJECTED"), 409);
    }
    session.dispatched = true;
    return Object.freeze({ dispatched: true, caseId: session.caseId });
  }

  function recordProviderResultForBudget({ budget, completionState, usage } = {}) {
    if (!config.guardRequired) return Object.freeze({ recorded: false, reason: "DISABLED" });
    const session = sessions.get(budget);
    if (!session || !session.dispatched || session.settled) {
      return Object.freeze({ recorded: false, reason: "CONTEXT_UNAVAILABLE" });
    }
    session.providerCompletionState = ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"].includes(completionState)
      ? completionState
      : "FAILED";
    session.usage = normalizeUsage(usage);
    return Object.freeze({ recorded: true, costEvidence: session.usage.costEvidence });
  }

  async function settleForBudget({ budget, httpStatus = null, body = null, forceOutcome = "" } = {}) {
    if (!config.guardRequired) return Object.freeze({ settled: false, reason: "DISABLED" });
    requireReady();
    const session = sessions.get(budget);
    if (!session) fail("COORDINATE_THREE_CASE_RC_SETTLEMENT_CONTEXT_MISSING", 503);
    if (session.settled) return Object.freeze({ settled: true, alreadySettled: true, caseId: session.caseId });
    const providerCallCount = Math.min(1, Math.max(0, Number(budget?.providerAttemptCount) || 0));
    let outcome = OUTCOMES.has(forceOutcome) ? forceOutcome : "";
    if (!outcome) {
      if (providerCallCount === 0) outcome = "FAILED_PRE_PROVIDER";
      else if (session.providerCompletionState === "SUCCEEDED") {
        outcome = body?.success === true && Number(httpStatus) >= 200 && Number(httpStatus) < 400
          ? "COMPLETED"
          : "FAILED_AFTER_PROVIDER";
      } else if (["FAILED", "TIMED_OUT", "ABORTED"].includes(session.providerCompletionState)) {
        outcome = "FAILED_AFTER_PROVIDER";
      } else {
        outcome = "OUTCOME_UNKNOWN";
      }
    }
    const usage = providerCallCount === 0
      ? Object.freeze({
          promptTokens: null,
          completionTokens: null,
          totalTokens: null,
          estimatedCostCny: null,
          costEvidence: "NOT_INCURRED"
        })
      : session.usage || normalizeUsage(null);
    const data = rpcData(await supabase.rpc("settle_coordinate_three_case_rc", {
      p_project_ref: COORDINATE_THREE_CASE_RC.rcProjectRef,
      p_run_id: COORDINATE_THREE_CASE_RC.runId,
      p_batch_id: COORDINATE_THREE_CASE_RC.batchId,
      p_manifest_sha256: COORDINATE_THREE_CASE_RC.manifestSha256,
      p_case_id: session.caseId,
      p_recognition_request_id: session.requestId,
      p_token_sha256: session.tokenHash,
      p_outcome: outcome,
      p_provider_call_count: providerCallCount,
      p_prompt_tokens: usage.promptTokens,
      p_completion_tokens: usage.completionTokens,
      p_total_tokens: usage.totalTokens,
      p_estimated_cost_cny: usage.estimatedCostCny,
      p_cost_evidence: usage.costEvidence
    }), "COORDINATE_THREE_CASE_RC_SETTLEMENT_FAILED");
    if (data.accepted !== true || data.action !== "SETTLED") {
      fail(String(data.code || "COORDINATE_THREE_CASE_RC_SETTLEMENT_REJECTED"), 503);
    }
    session.settled = true;
    return Object.freeze({
      settled: true,
      caseId: session.caseId,
      outcome,
      providerCallCount,
      costEvidence: usage.costEvidence,
      estimatedCostCny: usage.estimatedCostCny
    });
  }

  async function terminatePreProviderClaim(input = {}) {
    const budget = input.budget;
    if (budget && sessions.has(budget)) {
      return settleForBudget({ budget, forceOutcome: "FAILED_PRE_PROVIDER" });
    }
    requireReady();
    const preflight = preflightJob(input);
    const data = rpcData(await supabase.rpc("settle_coordinate_three_case_rc", {
      p_project_ref: COORDINATE_THREE_CASE_RC.rcProjectRef,
      p_run_id: COORDINATE_THREE_CASE_RC.runId,
      p_batch_id: COORDINATE_THREE_CASE_RC.batchId,
      p_manifest_sha256: COORDINATE_THREE_CASE_RC.manifestSha256,
      p_case_id: preflight.caseId,
      p_recognition_request_id: preflight.requestId,
      p_token_sha256: preflight.tokenHash,
      p_outcome: "FAILED_PRE_PROVIDER",
      p_provider_call_count: 0,
      p_prompt_tokens: null,
      p_completion_tokens: null,
      p_total_tokens: null,
      p_estimated_cost_cny: null,
      p_cost_evidence: "NOT_INCURRED"
    }), "COORDINATE_THREE_CASE_RC_PRE_PROVIDER_SETTLEMENT_FAILED");
    if (data.accepted !== true || data.action !== "SETTLED") {
      fail(String(data.code || "COORDINATE_THREE_CASE_RC_PRE_PROVIDER_SETTLEMENT_REJECTED"), 503);
    }
    return Object.freeze({ settled: true, outcome: "FAILED_PRE_PROVIDER", providerCallCount: 0 });
  }

  async function close({ adminHeader } = {}) {
    authorizeAdmin(adminHeader);
    const data = rpcData(await supabase.rpc("close_coordinate_three_case_rc_run", {
      p_project_ref: COORDINATE_THREE_CASE_RC.rcProjectRef,
      p_run_id: COORDINATE_THREE_CASE_RC.runId,
      p_batch_id: COORDINATE_THREE_CASE_RC.batchId,
      p_manifest_sha256: COORDINATE_THREE_CASE_RC.manifestSha256
    }), "COORDINATE_THREE_CASE_RC_CLOSE_FAILED");
    if (data.accepted !== true || data.action !== "CLOSED") {
      fail(String(data.code || "COORDINATE_THREE_CASE_RC_CLOSE_REJECTED"), 503);
    }
    activationState = "CLOSED";
    return Object.freeze({ closed: true });
  }

  async function status({ adminHeader } = {}) {
    authorizeAdmin(adminHeader);
    return sanitizeStatus(rpcData(await supabase.rpc("read_coordinate_three_case_rc_status", {
      p_project_ref: COORDINATE_THREE_CASE_RC.rcProjectRef,
      p_run_id: COORDINATE_THREE_CASE_RC.runId,
      p_batch_id: COORDINATE_THREE_CASE_RC.batchId,
      p_manifest_sha256: COORDINATE_THREE_CASE_RC.manifestSha256
    }), "COORDINATE_THREE_CASE_RC_STATUS_FAILED"));
  }

  return Object.freeze({
    config,
    activateOnStartup,
    authorizeAdmin,
    claim,
    preflightJob,
    bindBudget,
    dispatchForBudget,
    recordProviderResultForBudget,
    settleForBudget,
    terminatePreProviderClaim,
    close,
    status,
    getBoundCaseId: budget => sessions.get(budget)?.caseId || null,
    getActivationState: () => activationState
  });
}
