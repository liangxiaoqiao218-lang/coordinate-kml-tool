import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import { runCancellableOcrJob } from "../server/recognition/cancellable-ocr.js";
import {
  createLocalOcrDiagnosticTracker,
  sanitizeLocalOcrDiagnostics
} from "../server/recognition/local-ocr-diagnostics.js";
import {
  buildIndonesiaStructuredPreflightDiagnostics,
  classifyIndonesiaStructuredProviderFailure,
  sanitizeIndonesiaStructuredPreflightDiagnostics
} from "../server/recognition/indonesia-structured-diagnostics.js";
import {
  INDONESIA_STRUCTURED_PRODUCT_MODE,
  INDONESIA_STRUCTURED_PRODUCT_STATUS,
  INDONESIA_STRUCTURED_ROUTE_STATUS,
  buildIndonesiaStructuredProductBridge,
  selectIndonesiaStructuredProductRoute
} from "../server/recognition/indonesia-structured-product-bridge.js";
import {
  COORDINATE_THREE_CASE_RC,
  createCoordinateThreeCaseRcAdmission
} from "../server/recognition/coordinate-three-case-rc-admission.js";
import {
  prepareCoordinateProviderRequest,
  validateCoordinateProviderResponse
} from "../server/recognition/model-provider-compatibility.js";
import {
  COORDINATE_USAGE_ERROR_CODE,
  buildUnchargedCoordinateFailureResponse,
  createCoordinateUsageCommitController
} from "../server/coordinate-usage-atomicity.js";
import { createRecognitionAcquisitionJobRuntime } from "../server/recognition/recognition-acquisition-job-runtime.js";
import {
  activateRecognitionDeadlineContext,
  getRecognitionBudget,
  recognitionDeadlineMiddleware
} from "../server/coordinate-finalizer/recognition-deadline.js";
import {
  createCoordinateThreeCaseRcExistingUserLookup,
  createCoordinateThreeCaseRcInternalBindingMiddleware,
  createCoordinateThreeCaseRcJobForwarder,
  createCoordinateThreeCaseRcJobIngressMiddleware,
  createCoordinateThreeCaseRcJobPreflightMiddleware,
  executeCoordinateThreeCaseRcPermissionBoundary,
  settleCoordinateThreeCaseRcPermissionFailure
} from "../server/recognition/coordinate-three-case-rc-runtime.js";
import {
  CAPABILITY as INDONESIA_STRUCTURED_PRODUCT_CAPABILITY,
  MODEL as INDONESIA_STRUCTURED_PRODUCT_MODEL,
  STRUCTURED_RESULT_STATUS as INDONESIA_STRUCTURED_PROVIDER_STATUS,
  buildCoordinateStructuredCall,
  classifyCoordinateStructuredProviderResult
} from "../server/recognition/qwen38-coordinate-structured-adapter.mjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const serverSource = fs.readFileSync(path.join(root, "server.js"), "utf8");
const results = [];
let stubbedProviderSendBoundaries = 0;

async function test(name, action) {
  try {
    await action();
    results.push({ name, status: "PASS" });
  } catch (error) {
    results.push({ name, status: "FAIL", code: error?.message || String(error) });
  }
}

function extractFunctionDeclaration(source, name) {
  const start = source.search(new RegExp(`^\\s*(?:async )?function ${name}\\(`, "m"));
  assert.ok(start >= 0, `missing function ${name}`);
  const tail = source.slice(start);
  for (const end of tail.matchAll(/^\s*\}/gm)) {
    const candidate = tail.slice(0, end.index + end[0].length);
    try {
      new vm.Script(candidate);
      return candidate;
    } catch {
      // Continue until the declaration is balanced.
    }
  }
  throw new Error(`unable to extract function ${name}`);
}

function extractAsyncRouteCallback(source, routeLiteral) {
  const routeStart = source.indexOf(routeLiteral);
  assert.ok(routeStart >= 0, `missing route ${routeLiteral}`);
  const callbackStart = source.indexOf("async (req, res) => {", routeStart);
  assert.ok(callbackStart > routeStart, `missing async callback ${routeLiteral}`);
  const tail = source.slice(callbackStart);
  for (const end of tail.matchAll(/^\s*\}/gm)) {
    const candidate = tail.slice(0, end.index + end[0].length);
    try {
      new vm.Script(`(${candidate})`);
      return candidate;
    } catch {
      // Continue until the callback is balanced.
    }
  }
  throw new Error(`unable to extract route callback ${routeLiteral}`);
}

function readyEnvironment() {
  return {
    COORDINATE_THREE_CASE_RC_ENABLED: "true",
    COORDINATE_THREE_CASE_RC_RUN_ID: COORDINATE_THREE_CASE_RC.runId,
    COORDINATE_THREE_CASE_RC_WINDOW_SECONDS: String(COORDINATE_THREE_CASE_RC.runWindowSeconds),
    COORDINATE_THREE_CASE_RC_CLAIM_TTL_SECONDS: String(COORDINATE_THREE_CASE_RC.claimTtlSeconds),
    SUPABASE_URL: `https://${COORDINATE_THREE_CASE_RC.rcProjectRef}.supabase.co`,
    RC_SUPABASE_PROJECT_REF: COORDINATE_THREE_CASE_RC.rcProjectRef,
    RENDER_SERVICE_NAME: COORDINATE_THREE_CASE_RC.rcServiceName,
    ALIYUN_VISION_MODEL: COORDINATE_THREE_CASE_RC.model,
    ALIYUN_BASE_URL: `https://${COORDINATE_THREE_CASE_RC.providerHostname}/compatible-mode/v1`
  };
}

async function createBoundAdmission() {
  const calls = [];
  const supabase = {
    async rpc(name) {
      calls.push(name);
      if (name === "activate_coordinate_three_case_rc_run") {
        return { data: { accepted: true, action: "ACTIVATED" }, error: null };
      }
      if (name === "claim_coordinate_three_case_rc") {
        return { data: { accepted: true, action: "CLAIMED" }, error: null };
      }
      if (name === "dispatch_coordinate_three_case_rc") {
        return { data: { accepted: true, action: "DISPATCHED" }, error: null };
      }
      if (name === "settle_coordinate_three_case_rc") {
        return { data: { accepted: true, action: "SETTLED" }, error: null };
      }
      throw new Error(`unexpected_rpc:${name}`);
    }
  };
  const admission = createCoordinateThreeCaseRcAdmission({
    supabase,
    adminPassword: "offline-admin",
    env: readyEnvironment(),
    randomToken: () => "A".repeat(43),
    hashImage: () => COORDINATE_THREE_CASE_RC.cases.indonesia.imageSha256
  });
  await admission.activateOnStartup();
  const requestId = "11111111-1111-4111-8111-111111111111";
  const visitorId = "offline-controlled-visitor";
  const claim = await admission.claim({
    adminHeader: "offline-admin",
    runId: COORDINATE_THREE_CASE_RC.runId,
    batchId: COORDINATE_THREE_CASE_RC.batchId,
    caseId: "indonesia",
    imageSha256: COORDINATE_THREE_CASE_RC.cases.indonesia.imageSha256,
    recognitionRequestId: requestId,
    visitorId
  });
  const budget = {
    requestId,
    providerAttemptCount: 0,
    providerAttempted: false,
    assertCanStartProvider() {},
    effectiveProviderTimeout() { return 1_000; },
    stageStarted() { return {}; },
    stageCompleted() {},
    markProviderAttempted() {
      this.providerAttemptCount += 1;
      this.providerAttempted = true;
    },
    markProviderCompleted() {}
  };
  admission.bindBudget({
    budget,
    caseId: "indonesia",
    productMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
    recognitionRequestId: requestId,
    imageBuffer: Buffer.from("offline-image"),
    authorization: `Bearer ${claim.claimToken}`,
    visitorId
  });
  return { admission, budget, calls, claim, requestId, visitorId };
}

async function createClaimedAdmission({ env = readyEnvironment(), activate = true } = {}) {
  const calls = [];
  const users = new Set(["offline-controlled-visitor"]);
  const supabase = {
    async rpc(name) {
      calls.push(name);
      if (name === "activate_coordinate_three_case_rc_run") {
        return { data: { accepted: true, action: "ACTIVATED" }, error: null };
      }
      if (name === "claim_coordinate_three_case_rc") {
        return { data: { accepted: true, action: "CLAIMED" }, error: null };
      }
      if (name === "dispatch_coordinate_three_case_rc") {
        return { data: { accepted: true, action: "DISPATCHED" }, error: null };
      }
      if (name === "settle_coordinate_three_case_rc") {
        return { data: { accepted: true, action: "SETTLED" }, error: null };
      }
      if (name === "terminate_coordinate_three_case_rc_pre_provider") {
        return { data: { accepted: true, action: "TERMINATED" }, error: null };
      }
      throw new Error(`unexpected_rpc:${name}`);
    },
    from(table) {
      assert.equal(table, "users");
      let visitorId = "";
      const builder = {
        select() { return builder; },
        eq(_field, value) { visitorId = String(value || ""); return builder; },
        async maybeSingle() {
          return { data: users.has(visitorId) ? { user_id: visitorId } : null, error: null };
        }
      };
      return builder;
    }
  };
  const admission = createCoordinateThreeCaseRcAdmission({
    supabase,
    adminPassword: "offline-admin",
    env,
    randomToken: () => "A".repeat(43),
    hashImage: () => COORDINATE_THREE_CASE_RC.cases.indonesia.imageSha256
  });
  if (activate) await admission.activateOnStartup();
  const requestId = "33333333-3333-4333-8333-333333333333";
  const visitorId = "offline-controlled-visitor";
  const claim = activate ? await admission.claim({
    adminHeader: "offline-admin",
    runId: COORDINATE_THREE_CASE_RC.runId,
    batchId: COORDINATE_THREE_CASE_RC.batchId,
    caseId: "indonesia",
    imageSha256: COORDINATE_THREE_CASE_RC.cases.indonesia.imageSha256,
    recognitionRequestId: requestId,
    visitorId
  }) : null;
  return { admission, calls, claim, requestId, visitorId, supabase };
}

function createRouteResponse() {
  const listeners = [];
  return {
    statusCode: 200,
    headersSent: false,
    payload: null,
    setHeader() {},
    once(name, listener) {
      if (name === "finish" || name === "close") listeners.push(listener);
    },
    status(value) { this.statusCode = Number(value); return this; },
    json(body) { this.payload = body; this.headersSent = true; return this; },
    finish() { for (const listener of listeners) listener(); }
  };
}

async function waitForJobTerminal(runtime, accepted) {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    const snapshot = runtime.get(accepted.jobId, accepted.jobAccessToken);
    if (["SUCCEEDED", "FAILED"].includes(snapshot?.status)) return snapshot;
    await new Promise(resolve => setImmediate(resolve));
  }
  throw new Error("offline_job_did_not_settle");
}

function createLocalOcrRuntime({ recognize, postprocessFailure = false, workerCreateFailure = false } = {}) {
  let postprocessInvocationCount = 0;
  const runtime = vm.createContext({
    Buffer,
    Object,
    String,
    Number,
    Boolean,
    createLocalOcrDiagnosticTracker,
    runCancellableOcrJob,
    classifyOneShotStructuredFamily() {
      return { family: "none", matched: false, reason: "offline", evidence: {} };
    },
    createOneShotAcquisitionContract() {
      postprocessInvocationCount += 1;
      if (postprocessFailure && postprocessInvocationCount === 1) {
        throw new Error("offline_postprocess_failure");
      }
      return { schemaVersion: "offline" };
    },
    getRecognitionBudget() { return null; },
    getRecognitionDeadlineSignal() { return null; },
    RECOGNITION_BUDGET_CODE: "RECOGNITION_BUDGET_EXHAUSTED",
    RECOGNITION_DEADLINE_CODE: "RECOGNITION_DEADLINE_EXCEEDED",
    sharp() {
      return { metadata: async () => ({ width: 100, height: 100 }) };
    },
    createLocalOcrClassificationImage: async ({ imageBuffer, imageIdentity }) => ({
      image: imageBuffer,
      layoutSafe: true,
      provenance: Object.freeze({
        schema_version: "projected_source_context_v1",
        image_sha256: imageIdentity.image_sha256,
        same_request: true,
        mode: "source_image"
      })
    }),
    Tesseract: {
      createWorker: async () => {
        if (workerCreateFailure) throw new Error("offline_worker_create_failure");
        return {
          recognize,
          terminate: async () => {}
        };
      }
    },
    normalizeText: value => String(value || ""),
    extractLocalOcrLayoutLines: () => Object.freeze([]),
    normalizeLocalOcrStructuredEvidence: () => ({ status: "INCOMPLETE", layoutLines: [], text: "", axisOrderEvidence: null }),
    extractTrustedLocalOcrDecimalCoordinateEvidence: () => null,
    LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS: { COMPLETE: "COMPLETE" },
    shouldUseProjectedTableOcrAcquisition: () => false
  });
  vm.runInContext(extractFunctionDeclaration(serverSource, "runLocalOcrFamilyClassification"), runtime);
  return vm.runInContext("runLocalOcrFamilyClassification", runtime);
}

function createActualRouteBlockRuntime({ admission, budget }) {
  const start = serverSource.indexOf("    const requestedStructuredProductMode = String(");
  const end = serverSource.indexOf("    const buildIndonesiaStructuredFailureResponse = ({", start);
  assert.ok(start >= 0 && end > start, "actual route block missing");
  const block = serverSource.slice(start, end);
  const runtime = vm.createContext({
    Buffer,
    req: {
      body: { coordinateProductMode: INDONESIA_STRUCTURED_PRODUCT_MODE },
      get(name) {
        return String(name).toLowerCase() === "x-coordinate-product-mode"
          ? INDONESIA_STRUCTURED_PRODUCT_MODE
          : "";
      }
    },
    recognitionBudget: budget,
    coordinateThreeCaseRcAdmission: admission,
    indonesiaStructuredBRcAdmission: { config: { ready: false } },
    INDONESIA_STRUCTURED_B_RC: { mode: INDONESIA_STRUCTURED_PRODUCT_MODE },
    INDONESIA_STRUCTURED_PRODUCT_MODE,
    indonesiaStructuredProductEnabled: true,
    indonesiaStructuredRcOriginalImageBuffer: Buffer.from("offline-image"),
    oneShotLocalOcrSourceContextText: "",
    oneShotLocalOcrAttempted: true,
    oneShotLocalOcrDiagnostics: Object.freeze({
      schemaVersion: "local_ocr_diagnostics_v1",
      phase: "CONTEXT_RETURNED",
      outcome: "SUCCESS_EMPTY",
      preprocessingMode: "SOURCE_IMAGE",
      attempted: true,
      wrapperEntered: true,
      workerCreateStarted: true,
      workerReady: true,
      recognizeStarted: true,
      resultReceived: true,
      postprocessStarted: true,
      postprocessCompleted: true,
      contextReturned: true,
      rawTextPresent: false
    }),
    selectIndonesiaStructuredProductRoute,
    buildIndonesiaStructuredPreflightDiagnostics
  });
  vm.runInContext(`async function executeActualRouteBlock() {${block}
    return {
      route: indonesiaStructuredRoute,
      diagnostics: indonesiaStructuredPreflightDiagnostics,
      modelCrsAuthorized: controlledRcModelCrsAcquisitionAuthorized,
      hintAuthorized: controlledRcProjectedColumnsHintAuthorized
    };
  }`, runtime);
  return vm.runInContext("executeActualRouteBlock", runtime);
}

function createActualProviderFunction({ admission, budget, calls }) {
  const runtime = vm.createContext({
    AbortController,
    Date,
    JSON,
    Math,
    Number,
    Object,
    String,
    setTimeout,
    clearTimeout,
    process: { env: {} },
    console: { log() {}, error() {} },
    aliyunApiKey: "offline-placeholder",
    aliyunBaseURL: "http://127.0.0.1/provider-stub",
    latestProviderModelObservation: null,
    isFamilyRetryAllowed: () => true,
    prepareCoordinateProviderRequest,
    validateCoordinateProviderResponse,
    getRecognitionBudget: () => budget,
    getRecognitionDeadlineSignal: () => null,
    getAliyunChatCompletionsUrl: () => "http://127.0.0.1/provider-stub",
    composeAbortSignals: signals => ({ signal: signals[0], cleanup() {} }),
    coordinateThreeCaseRcAdmission: admission,
    RECOGNITION_BUDGET_CODE: "RECOGNITION_BUDGET_EXHAUSTED",
    RECOGNITION_DEADLINE_CODE: "RECOGNITION_DEADLINE_EXCEEDED",
    fetch: async () => {
      stubbedProviderSendBoundaries += 1;
      calls.push("provider_fetch_stub");
      return {
        ok: true,
        status: 200,
        async json() {
          return {
            model: COORDINATE_THREE_CASE_RC.model,
            choices: [{ finish_reason: "stop", message: { content: "offline structured response" } }],
            usage: { prompt_tokens: 2, completion_tokens: 3, total_tokens: 5 }
          };
        }
      };
    }
  });
  vm.runInContext(extractFunctionDeclaration(serverSource, "callAliyunVision"), runtime);
  return vm.runInContext("callAliyunVision", runtime);
}

function createIntegratedActualRecognitionHandler({ admission, providerReceipts }) {
  const users = {};
  const featureFlags = {
    aiOcrEnabled: true,
    xyConvertEnabled: true,
    kmlExportEnabled: true,
    manualSupportEnabled: true,
    aiJudgeEnabled: true,
    goldCalculatorEnabled: true
  };
  const localOcrDiagnostics = Object.freeze({
    schemaVersion: "local_ocr_diagnostics_v1",
    phase: "CONTEXT_RETURNED",
    outcome: "SUCCESS_EMPTY",
    preprocessingMode: "SOURCE_IMAGE",
    attempted: true,
    wrapperEntered: true,
    workerCreateStarted: true,
    workerReady: true,
    recognizeStarted: true,
    resultReceived: true,
    postprocessStarted: true,
    postprocessCompleted: true,
    contextReturned: true,
    rawTextPresent: false
  });
  const emptyRoute = Object.freeze({
    family: "none",
    matched: false,
    reason: "offline_empty",
    evidence: Object.freeze({
      coordinateRowCount: 0,
      dmsRowCount: 0,
      repeatedHeaderCount: 0,
      groupHeadingCount: 0,
      layoutRegionCount: 0,
      ambiguousMultiPairLineCount: 0
    })
  });
  const runtime = vm.createContext({
    AbortController,
    Blob,
    Buffer,
    Date,
    FormData,
    JSON,
    Math,
    Number,
    Object,
    String,
    console: { log() {}, error() {}, warn() {} },
    process: { env: {} },
    queueMicrotask,
    setTimeout,
    clearTimeout,
    coordinateThreeCaseRcAdmission: admission,
    indonesiaStructuredBRcAdmission: { config: { ready: false } },
    INDONESIA_STRUCTURED_B_RC: { mode: INDONESIA_STRUCTURED_PRODUCT_MODE },
    INDONESIA_STRUCTURED_PRODUCT_MODE,
    INDONESIA_STRUCTURED_PRODUCT_STATUS,
    INDONESIA_STRUCTURED_ROUTE_STATUS,
    INDONESIA_STRUCTURED_PRODUCT_CAPABILITY,
    INDONESIA_STRUCTURED_PRODUCT_MODEL,
    INDONESIA_STRUCTURED_PROVIDER_STATUS,
    indonesiaStructuredProductEnabled: true,
    createRecognitionDiagnosticSession() { return { enabled: false, event() {} }; },
    activateRecognitionDeadlineContext,
    getRecognitionBudget,
    getRecognitionDeadlineSignal() { return null; },
    getRegressionTestMode() { return { active: true }; },
    getCoordinateUsageSessionBinding() { throw new Error("unexpected_usage_session_binding"); },
    createRecognitionAcquisitionEvidenceStore() { return { getOrBuild() { return null; } }; },
    classifyOneShotStructuredFamily() { return emptyRoute; },
    createOneShotAcquisitionContract({ route }) { return { route }; },
    createDmsGroupedRetryOrchestrator() { return {}; },
    createCoordinateUsageCommitController,
    COORDINATE_USAGE_ERROR_CODE,
    coordinateUsageAtomicity: null,
    isRecognitionStopError(error) {
      return ["RECOGNITION_DEADLINE_EXCEEDED", "RECOGNITION_BUDGET_EXHAUSTED"].includes(error?.code);
    },
    buildCoordinateVerificationResponseWithoutRecognitionBudget(payload) { return payload; },
    buildCoordinateVerificationResponse(payload) { return payload; },
    recordCoordinateRecognitionIssue() { throw new Error("unexpected_issue_write"); },
    storeRegressionRecognitionTrace() { return true; },
    coordinateCaseStore: null,
    canonicalizeCoordinateImageUpload(file) { return { valid: true, status: "VALID", file }; },
    validateCoordinateImageUpload() { return { valid: true }; },
    createCoordinateImageIdentity() {
      return Object.freeze({ image_sha256: "offline-redacted", width: 1, height: 1, page: 1 });
    },
    executeCoordinateThreeCaseRcPermissionBoundary,
    readAdminData: async () => ({ users, featureFlags }),
    createCoordinateThreeCaseRcExistingUserLookup: () => async visitorId => (
      String(visitorId || "").trim() === "offline-controlled-visitor"
        ? { user_id: "offline-controlled-visitor" }
        : null
    ),
    supabase: null,
    ensureUser(data, visitorId) {
      const key = String(visitorId || "").trim();
      if (!key) return null;
      data.users[key] ||= { visitorId: key, status: "active", permissions: { aiOcrEnabled: true } };
      return data.users[key];
    },
    getEffectivePermissions(user, flags) {
      return { aiOcrEnabled: Boolean(user?.status !== "disabled"
        && flags?.aiOcrEnabled === true && user?.permissions?.aiOcrEnabled === true) };
    },
    settleCoordinateThreeCaseRcPermissionFailure,
    buildRegressionTextCoordinateResult(rawText) { return { rawText, coordinates: rawText }; },
    buildCoordinateEngineV2ShadowResult() { return null; },
    runBudgetedStage(_stageName, action) { return action(); },
    createRecognitionImageVariants: async () => ({
      version: "offline",
      width: 1,
      height: 1,
      asyncRecommended: false,
      images: Object.freeze([{ role: "overview", dataUrl: "data:image/png;base64,b2ZmbGluZQ==" }])
    }),
    hasExplicitHandwrittenDmsUploadContext() { return false; },
    runLocalOcrFamilyClassification: async () => ({
      attempted: true,
      layoutLines: Object.freeze([]),
      sourceText: "",
      axisEvidenceText: "",
      axisOrderEvidence: null,
      sourceContextText: "",
      sourceContextProvenance: null,
      projectedTableOcrAcquisition: false,
      route: emptyRoute,
      contract: { route: emptyRoute },
      localOcrDiagnostics
    }),
    getUploadedFileDisplayName(file) { return file?.originalname || "coordinate-image"; },
    selectIndonesiaStructuredProductRoute,
    buildIndonesiaStructuredPreflightDiagnostics,
    classifyIndonesiaStructuredProviderFailure,
    buildCoordinateStructuredCall,
    classifyCoordinateStructuredProviderResult,
    buildIndonesiaStructuredProductBridge,
    extractProviderMessageText(response) {
      return String(response?.choices?.[0]?.message?.content || "");
    },
    prepareCoordinateProviderRequest,
    validateCoordinateProviderResponse,
    getAliyunChatCompletionsUrl() { return "http://127.0.0.1/provider-stub"; },
    composeAbortSignals(signals) { return { signal: signals.find(Boolean) || null, cleanup() {} }; },
    isFamilyRetryAllowed() { return true; },
    latestProviderModelObservation: null,
    aliyunApiKey: "offline-placeholder",
    aliyunBaseURL: "http://127.0.0.1/provider-stub",
    aliyunVisionModel: COORDINATE_THREE_CASE_RC.model,
    aliyunOcrModel: "offline-not-called",
    fetch: async (_url, options) => {
      stubbedProviderSendBoundaries += 1;
      providerReceipts.push(Object.freeze({
        sent: true,
        bodyPresent: Boolean(options?.body),
        methodPost: options?.method === "POST"
      }));
      return {
        ok: true,
        status: 200,
        async json() {
          return {
            model: COORDINATE_THREE_CASE_RC.model,
            choices: [{ finish_reason: "stop", message: { content: "offline invalid structured response" } }],
            usage: { prompt_tokens: 2, completion_tokens: 3, total_tokens: 5 }
          };
        }
      };
    },
    RECOGNITION_DEADLINE_CODE: "RECOGNITION_DEADLINE_EXCEEDED",
    RECOGNITION_BUDGET_CODE: "RECOGNITION_BUDGET_EXHAUSTED",
    noCoordinatesText: "NO_COORDINATES"
  });
  vm.runInContext(extractFunctionDeclaration(serverSource, "callAliyunVision"), runtime);
  vm.runInContext(extractFunctionDeclaration(serverSource, "recognizeCoordinatesHandler"), runtime);
  return vm.runInContext("recognizeCoordinatesHandler", runtime);
}

async function runInternalBinding({ admission, headers, imageBuffer, body, onBound, attachBudget = true }) {
  const normalizedHeaders = Object.fromEntries(
    Object.entries(headers).map(([key, value]) => [String(key).toLowerCase(), value])
  );
  const req = {
    body,
    file: { buffer: imageBuffer, size: imageBuffer.length, mimetype: "image/png", originalname: "offline.png" },
    get(name) { return normalizedHeaders[String(name).toLowerCase()] || ""; }
  };
  const res = createRouteResponse();
  const binding = createCoordinateThreeCaseRcInternalBindingMiddleware({
    admission,
    activateDeadlineContext: activateRecognitionDeadlineContext
  });
  const invoke = async () => binding(req, res, async () => {
    req.headers ||= {};
    req.headers["x-regression-test"] = "1";
    const originalGet = req.get.bind(req);
    req.get = name => String(name).toLowerCase() === "x-regression-test" ? "1" : originalGet(name);
    await onBound(req, res);
  });
  if (attachBudget) {
    await new Promise((resolve, reject) => {
      recognitionDeadlineMiddleware({
        profile: "async",
        deadlineMs: 180_000,
        preflightDeadlineMs: 30_000,
        executionDeadlineMs: 150_000,
        responseReserveMs: 5_000,
        lowValueFallbackCutoffMs: 145_000
      })(req, res, () => Promise.resolve(invoke()).then(resolve, reject));
    });
  } else {
    await invoke();
  }
  res.finish();
  return { req, res };
}

async function runActualIndonesiaJobRoute({ admission, runtime, requestId, visitorId, authorization }) {
  const headers = {
    authorization,
    "x-recognition-request-id": requestId,
    "x-coordinate-rc-case-id": "indonesia",
    "x-coordinate-product-mode": INDONESIA_STRUCTURED_PRODUCT_MODE,
    "x-visitor-id": visitorId
  };
  const req = {
    body: { visitorId, coordinateProductMode: INDONESIA_STRUCTURED_PRODUCT_MODE },
    file: {
      buffer: Buffer.from("offline-image"),
      size: Buffer.byteLength("offline-image"),
      mimetype: "image/png",
      originalname: "offline.png"
    },
    get(name) { return headers[String(name).toLowerCase()] || ""; }
  };
  const res = createRouteResponse();
  const ingress = createCoordinateThreeCaseRcJobIngressMiddleware({ admission });
  const preflight = createCoordinateThreeCaseRcJobPreflightMiddleware({
    admission,
    isRecognitionRequestId: value => /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u.test(value),
    randomRequestId() { throw new Error("unexpected_random_request_id"); }
  });
  const routeRuntime = vm.createContext({
    Buffer,
    crypto: { randomUUID() { throw new Error("unexpected_random_request_id"); } },
    coordinateThreeCaseRcAdmission: admission,
    indonesiaStructuredBRcAdmission: { config: { ready: false } },
    INDONESIA_STRUCTURED_B_RC: { mode: INDONESIA_STRUCTURED_PRODUCT_MODE },
    IndonesiaStructuredBRcAdmissionError: class extends Error {},
    COORDINATE_IMAGE_INVALID_CODE: "COORDINATE_IMAGE_INVALID",
    canonicalizeCoordinateImageUpload(file) { return { valid: true, status: "VALID", file }; },
    validateCoordinateImageUpload() { return { valid: true }; },
    isRecognitionRequestId(value) {
      return /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u.test(value);
    },
    getUploadedFileDisplayName(file) { return file?.originalname || "coordinate-image"; },
    getRegressionTestMode() { return { active: true }; },
    recognitionAcquisitionJobRuntime: runtime
  });
  const callback = vm.runInContext(`(${extractAsyncRouteCallback(serverSource, '"/api/recognize-coordinates/jobs"')})`, routeRuntime);
  await new Promise((resolve, reject) => {
    ingress(req, res, () => {
      preflight(req, res, () => Promise.resolve(callback(req, res)).then(resolve, reject));
      if (res.headersSent) resolve();
    });
    if (res.headersSent) resolve();
  });
  return { req, res, accepted: res.payload };
}

await test("actual local OCR function distinguishes post-result processing failure", async () => {
  const run = createLocalOcrRuntime({
    recognize: async () => ({ data: { text: "offline text" } }),
    postprocessFailure: true
  });
  const result = await run({
    imageBuffer: Buffer.from("offline"),
    imageIdentity: { image_sha256: "a".repeat(64) },
    stageCapMs: 100,
    minRequiredMs: 1
  });
  assert.equal(result.attempted, true);
  assert.equal(result.localOcrDiagnostics.resultReceived, true);
  assert.equal(result.localOcrDiagnostics.outcome, "POSTPROCESS_ERROR_AFTER_RESULT");
  assert.equal(result.localOcrDiagnostics.postprocessCompleted, false);
  assert.equal(result.localOcrDiagnostics.contextReturned, false);
});

await test("actual local OCR function preserves deadline phase evidence", async () => {
  const run = createLocalOcrRuntime({
    recognize: async () => {
      const error = new Error("offline deadline");
      error.code = "RECOGNITION_DEADLINE_EXCEEDED";
      throw error;
    }
  });
  await assert.rejects(() => run({
    imageBuffer: Buffer.from("offline"),
    imageIdentity: { image_sha256: "b".repeat(64) },
    stageCapMs: 100,
    minRequiredMs: 1
  }), error => {
    assert.equal(error.code, "RECOGNITION_DEADLINE_EXCEEDED");
    assert.equal(error.localOcrDiagnostics.outcome, "DEADLINE_ABORTED");
    assert.equal(error.localOcrDiagnostics.recognizeStarted, true);
    assert.equal(error.localOcrDiagnostics.contextReturned, false);
    return true;
  });
});

await test("real deadline response and async job receipt retain only the current OCR terminal meaning", async () => {
  const runDeadlineCase = async outcome => {
    const req = { get() { return "44444444-4444-4444-8444-444444444444"; } };
    const res = createRouteResponse();
    recognitionDeadlineMiddleware({
      deadlineMs: 20,
      preflightDeadlineMs: 10,
      executionDeadlineMs: 10,
      responseReserveMs: 1,
      lowValueFallbackCutoffMs: 10
    })(req, res, () => {
      req.recognitionDeadlineContext.budget.setLocalOcrDiagnostics({
        schemaVersion: "local_ocr_diagnostics_v1",
        phase: outcome === "IN_PROGRESS" ? "RECOGNIZE_STARTED" : "CONTEXT_RETURNED",
        outcome,
        preprocessingMode: "SOURCE_IMAGE",
        attempted: true,
        wrapperEntered: true,
        workerCreateStarted: true,
        workerReady: true,
        recognizeStarted: true,
        resultReceived: outcome !== "IN_PROGRESS",
        postprocessStarted: outcome !== "IN_PROGRESS",
        postprocessCompleted: outcome !== "IN_PROGRESS",
        contextReturned: outcome !== "IN_PROGRESS",
        rawTextPresent: false
      });
    });
    await new Promise(resolve => setTimeout(resolve, 35));
    res.finish();
    return res;
  };
  const inProgress = await runDeadlineCase("IN_PROGRESS");
  assert.equal(inProgress.statusCode, 504);
  assert.equal(inProgress.payload?.localOcrDiagnostics?.outcome, "DEADLINE_ABORTED");
  const completed = await runDeadlineCase("SUCCESS_EMPTY");
  assert.equal(completed.statusCode, 504);
  assert.equal(completed.payload?.localOcrDiagnostics?.outcome, "SUCCESS_EMPTY");

  const runtime = createRecognitionAcquisitionJobRuntime({
    execute: async () => ({ httpStatus: inProgress.statusCode, result: inProgress.payload })
  });
  const queued = runtime.enqueue({ requestId: "44444444-4444-4444-8444-444444444444" });
  const terminal = await waitForJobTerminal(runtime, queued);
  assert.equal(terminal.status, "FAILED");
  assert.equal(terminal.result?.localOcrDiagnostics?.outcome, "DEADLINE_ABORTED");
});

await test("actual local OCR function emits fixed create recognize timeout and empty outcomes", async () => {
  const input = {
    imageBuffer: Buffer.from("offline"),
    imageIdentity: { image_sha256: "c".repeat(64) },
    stageCapMs: 20,
    minRequiredMs: 1
  };
  const createFailure = await createLocalOcrRuntime({
    workerCreateFailure: true,
    recognize: async () => ({ data: { text: "" } })
  })(input);
  assert.equal(createFailure.localOcrDiagnostics.outcome, "WORKER_CREATE_ERROR");
  const recognizeFailure = await createLocalOcrRuntime({
    recognize: async () => { throw new Error("offline_recognize_failure"); }
  })(input);
  assert.equal(recognizeFailure.localOcrDiagnostics.outcome, "RECOGNIZE_ERROR");
  const timeout = await createLocalOcrRuntime({
    recognize: async () => new Promise(() => {})
  })(input);
  assert.equal(timeout.localOcrDiagnostics.outcome, "TIMEOUT");
  const empty = await createLocalOcrRuntime({
    recognize: async () => ({ data: { text: "" } })
  })(input);
  assert.equal(empty.localOcrDiagnostics.outcome, "SUCCESS_EMPTY");
  assert.equal(empty.localOcrDiagnostics.contextReturned, true);
  const sanitized = sanitizeLocalOcrDiagnostics({ ...empty.localOcrDiagnostics, rawText: "must disappear" });
  assert.equal(Object.hasOwn(sanitized, "rawText"), false);
});

await test("actual handler route block accepts missing local CRS only from bound controlled admission", async () => {
  const { admission, budget } = await createBoundAdmission();
  const execute = createActualRouteBlockRuntime({ admission, budget });
  const result = await execute();
  assert.equal(result.modelCrsAuthorized, true);
  assert.equal(result.hintAuthorized, true);
  assert.equal(result.route.status, INDONESIA_STRUCTURED_ROUTE_STATUS.SELECTED);
  assert.equal(result.route.evidence.explicitUtm50s, false);
  assert.equal(result.route.evidence.controlledRcModelCrsAcquisitionApplied, true);
  assert.equal(result.diagnostics.localOcrDiagnostics.outcome, "SUCCESS_EMPTY");
});

await test("ordinary requests and explicit CRS conflicts remain fail closed", () => {
  const ordinary = selectIndonesiaStructuredProductRoute({
    enabled: true,
    requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
    sourceContextText: "",
    controlledRcProjectedColumnsHintAuthorized: true
  });
  assert.equal(ordinary.status, INDONESIA_STRUCTURED_ROUTE_STATUS.BLOCKED);
  for (const sourceContextText of [
    "UTM ED50 ZONE 50S X Y",
    "UTM WGS 1972 ZONE 50S X Y",
    "UTM WGS 1984 ZONE 50N X Y",
    "UTM WGS 1984 ZONE 51S X Y"
  ]) {
    const route = selectIndonesiaStructuredProductRoute({
      enabled: true,
      requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
      sourceContextText,
      controlledRcProjectedColumnsHintAuthorized: true,
      controlledRcModelCrsAcquisitionAuthorized: true
    });
    assert.equal(route.status, INDONESIA_STRUCTURED_ROUTE_STATUS.BLOCKED);
    assert.equal(route.reasonCode, "STRUCTURED_PRODUCT_SOURCE_CRS_CONFLICT");
  }
  for (const sourceContextText of ["UTM", "UTM ZONE 50S X Y"]) {
    const unresolved = selectIndonesiaStructuredProductRoute({
      enabled: true,
      requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
      sourceContextText,
      controlledRcProjectedColumnsHintAuthorized: true,
      controlledRcModelCrsAcquisitionAuthorized: true
    });
    assert.equal(unresolved.status, INDONESIA_STRUCTURED_ROUTE_STATUS.SELECTED);
    assert.equal(unresolved.evidence.explicitUtm50s, false);
    assert.equal(unresolved.evidence.controlledRcModelCrsAcquisitionApplied, true);
    if (sourceContextText.includes("ZONE")) assert.equal(unresolved.evidence.sourceCrsUnresolved, true);
  }
});

await test("post-model bridge still requires explicit UTM50S evidence", () => {
  for (const coordinateSystemExplicit of ["", "EPSG:32650"]) {
    const bridge = buildIndonesiaStructuredProductBridge({
      source: {
        coordinateSystemExplicit,
        pointOrder: ["A", "B", "C"],
        points: [
          { id: "A", order: 1, xRaw: "500000", yRaw: "9000000" },
          { id: "B", order: 2, xRaw: "500100", yRaw: "9000100" },
          { id: "C", order: 3, xRaw: "500200", yRaw: "9000000" }
        ]
      },
      objects: [{ geometryType: "Polygon", outerRing: ["A", "B", "C"], innerRings: [] }],
      unresolvedItems: []
    });
    assert.equal(bridge.status, INDONESIA_STRUCTURED_PRODUCT_STATUS.INVALID_REVIEW_REQUIRED);
    assert.equal(bridge.reasonCode, "EXPLICIT_UTM50S_CRS_REQUIRED");
  }
});

await test("actual job callback forwarder handler route and provider function share one controlled budget", async () => {
  const { admission, calls, claim, requestId, visitorId } = await createClaimedAdmission();
  const providerReceipts = [];
  const handler = createIntegratedActualRecognitionHandler({ admission, providerReceipts });
  const transport = async (url, options) => {
    assert.equal(url, "http://127.0.0.1:3000/api/internal/recognize-coordinates-long");
    const image = options.body.get("image");
    const imageBuffer = Buffer.from(await image.arrayBuffer());
    const body = {};
    for (const [key, value] of options.body.entries()) {
      if (typeof value === "string") body[key] = value;
    }
    const routed = await runInternalBinding({
      admission,
      headers: options.headers,
      imageBuffer,
      body,
      onBound: handler
    });
    return {
      ok: routed.res.statusCode >= 200 && routed.res.statusCode < 300,
      status: routed.res.statusCode,
      async json() { return routed.res.payload; }
    };
  };
  const forward = createCoordinateThreeCaseRcJobForwarder({
    fetchImpl: transport,
    getPort: () => 3000,
    internalToken: "offline-internal-token",
    coordinateThreeCaseRcAdmission: admission,
    indonesiaStructuredBRcAdmission: { config: { ready: false } },
    indonesiaProductMode: INDONESIA_STRUCTURED_PRODUCT_MODE
  });
  const runtime = createRecognitionAcquisitionJobRuntime({ execute: forward, maxJobs: 1 });
  const routed = await runActualIndonesiaJobRoute({
    admission,
    runtime,
    requestId,
    visitorId,
    authorization: `Bearer ${claim.claimToken}`
  });
  assert.equal(routed.res.statusCode, 202);
  const terminal = await waitForJobTerminal(runtime, routed.accepted);
  assert.equal(terminal.status, "FAILED");
  assert.equal(terminal.httpStatus, 422);
  assert.equal(providerReceipts.length, 1);
  assert.equal(providerReceipts[0].sent, true);
  assert.equal(calls.filter(name => name === "dispatch_coordinate_three_case_rc").length, 1);
  assert.equal(
    terminal.result?.indonesiaStructuredPreflightDiagnostics?.controlledRcModelCrsAcquisitionAuthorized,
    true
  );
  assert.equal(
    terminal.result?.indonesiaStructuredPreflightDiagnostics?.controlledRcModelCrsAcquisitionApplied,
    true
  );
});

await test("actual forwarder and internal binding reject a missing request budget before provider", async () => {
  const { admission, claim, requestId, visitorId } = await createClaimedAdmission();
  const providerReceipts = [];
  const handler = createIntegratedActualRecognitionHandler({ admission, providerReceipts });
  const authorization = `Bearer ${claim.claimToken}`;
  const forward = createCoordinateThreeCaseRcJobForwarder({
    fetchImpl: async (_url, options) => {
      const image = options.body.get("image");
      const imageBuffer = Buffer.from(await image.arrayBuffer());
      const body = {};
      for (const [key, value] of options.body.entries()) {
        if (typeof value === "string") body[key] = value;
      }
      const routed = await runInternalBinding({
        admission,
        headers: options.headers,
        imageBuffer,
        body,
        attachBudget: false,
        onBound: handler
      });
      return {
        ok: routed.res.statusCode >= 200 && routed.res.statusCode < 300,
        status: routed.res.statusCode,
        async json() { return routed.res.payload; }
      };
    },
    getPort: () => 3000,
    internalToken: "offline-internal-token",
    coordinateThreeCaseRcAdmission: admission,
    indonesiaStructuredBRcAdmission: { config: { ready: false } },
    indonesiaProductMode: INDONESIA_STRUCTURED_PRODUCT_MODE
  });
  const output = await forward({
    requestId,
    file: { buffer: Buffer.from("offline-image"), mimetype: "image/png", originalname: "offline.png" },
    body: { visitorId, coordinateProductMode: INDONESIA_STRUCTURED_PRODUCT_MODE },
    forwardHeaders: {
      authorization,
      "x-recognition-request-id": requestId,
      "x-coordinate-rc-case-id": "indonesia",
      "x-visitor-id": visitorId
    }
  }, { jobId: "offline-job" });
  assert.equal(output.httpStatus, 503);
  assert.equal(output.result.reason, "COORDINATE_THREE_CASE_RC_BUDGET_REQUIRED");
  assert.equal(providerReceipts.length, 0);
});

await test("forged async authorization flag cannot make the actual handler borrow controlled scope", async () => {
  const { admission, claim, requestId, visitorId } = await createClaimedAdmission();
  const providerReceipts = [];
  const handler = createIntegratedActualRecognitionHandler({ admission, providerReceipts });
  const headers = {
    authorization: `Bearer ${claim.claimToken}`,
    "x-recognition-request-id": requestId,
    "x-coordinate-rc-case-id": "indonesia",
    "x-coordinate-product-mode": INDONESIA_STRUCTURED_PRODUCT_MODE,
    "x-visitor-id": visitorId,
    "x-regression-test": "1"
  };
  const req = {
    body: { visitorId, coordinateProductMode: INDONESIA_STRUCTURED_PRODUCT_MODE },
    file: {
      buffer: Buffer.from("offline-image"), size: Buffer.byteLength("offline-image"),
      mimetype: "image/png", originalname: "offline.png"
    },
    coordinateThreeCaseRcAsyncAuthorized: true,
    get(name) { return headers[String(name).toLowerCase()] || ""; }
  };
  const res = createRouteResponse();
  await new Promise((resolve, reject) => {
    recognitionDeadlineMiddleware({
      profile: "async", deadlineMs: 180_000, preflightDeadlineMs: 30_000,
      executionDeadlineMs: 150_000, responseReserveMs: 5_000, lowValueFallbackCutoffMs: 145_000
    })(req, res, () => Promise.resolve(handler(req, res)).then(resolve, reject));
  });
  res.finish();
  assert.ok(res.statusCode >= 400);
  assert.equal(providerReceipts.length, 0);
});

await test("actual job ingress and preflight reject a non-active run before queue or provider", async () => {
  const invalidEnv = { ...readyEnvironment(), COORDINATE_THREE_CASE_RC_RUN_ID: "inactive-run" };
  const { admission, requestId, visitorId } = await createClaimedAdmission({ env: invalidEnv, activate: false });
  const providerReceipts = [];
  const runtime = createRecognitionAcquisitionJobRuntime({
    execute: async () => {
      providerReceipts.push({ sent: true });
      return { httpStatus: 500, result: { success: false } };
    },
    maxJobs: 1
  });
  const routed = await runActualIndonesiaJobRoute({
    admission,
    runtime,
    requestId,
    visitorId,
    authorization: `Bearer ${"A".repeat(43)}`
  });
  assert.notEqual(routed.res.statusCode, 202);
  assert.equal(providerReceipts.length, 0);
});

await test("actual provider function dispatches bound admission before stubbed send", async () => {
  const { admission, budget, calls } = await createBoundAdmission();
  const callProvider = createActualProviderFunction({ admission, budget, calls });
  await callProvider({
    modelName: COORDINATE_THREE_CASE_RC.model,
    prompt: "offline",
    imageItems: [],
    maxTokens: 8000,
    enableThinking: false,
    timeoutMs: 100,
    stageName: "indonesia_structured_b"
  });
  const dispatchIndex = calls.indexOf("dispatch_coordinate_three_case_rc");
  const fetchIndex = calls.indexOf("provider_fetch_stub");
  assert.ok(dispatchIndex >= 0 && fetchIndex > dispatchIndex);
  assert.equal(budget.providerAttemptCount, 1);
});

await test("job and uncharged usage transports preserve only sanitized diagnostics", async () => {
  const tracker = createLocalOcrDiagnosticTracker();
  tracker.markPhase("WRAPPER_ENTERED");
  tracker.markPhase("WORKER_CREATE_STARTED");
  tracker.fail("WORKER_CREATE_ERROR");
  const route = selectIndonesiaStructuredProductRoute({
    enabled: true,
    requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
    sourceContextText: ""
  });
  const diagnostics = buildIndonesiaStructuredPreflightDiagnostics({
    route,
    localOcrAttempted: true,
    localOcrDiagnostics: { ...tracker.snapshot(), rawText: "must disappear" }
  });
  const sanitized = sanitizeIndonesiaStructuredPreflightDiagnostics(diagnostics);
  assert.equal(sanitized.localOcrDiagnostics.outcome, "WORKER_CREATE_ERROR");
  assert.equal(Object.hasOwn(sanitized.localOcrDiagnostics, "rawText"), false);
  const response = buildUnchargedCoordinateFailureResponse({
    body: {
      success: false,
      reason: "indonesia_structured_product_review_required",
      code: "STRUCTURED_PRODUCT_SOURCE_EVIDENCE_REQUIRED",
      providerCallCount: 0,
      providerCompletionState: "NOT_STARTED",
      indonesiaStructuredPreflightDiagnostics: diagnostics
    }
  });
  assert.equal(response.indonesiaStructuredPreflightDiagnostics.localOcrDiagnostics.outcome, "WORKER_CREATE_ERROR");
  const runtime = createRecognitionAcquisitionJobRuntime({
    execute: async () => ({
      httpStatus: 422,
      result: {
        success: false,
        code: "STRUCTURED_PRODUCT_SOURCE_EVIDENCE_REQUIRED",
        providerCallCount: 0,
        providerCompletionState: "NOT_STARTED",
        indonesiaStructuredPreflightDiagnostics: diagnostics
      }
    })
  });
  const queued = runtime.enqueue({ requestId: "22222222-2222-4222-8222-222222222222" });
  let snapshot = null;
  for (let index = 0; index < 20; index += 1) {
    await new Promise(resolve => setImmediate(resolve));
    snapshot = runtime.get(queued.jobId, queued.jobAccessToken);
    if (snapshot?.status === "FAILED") break;
  }
  assert.equal(snapshot?.result?.indonesiaStructuredPreflightDiagnostics?.localOcrDiagnostics?.outcome,
    "WORKER_CREATE_ERROR");
});

const failed = results.filter(result => result.status !== "PASS");
console.log(JSON.stringify({
  suite: "COORDINATE_THREE_CASE_RC_SOURCE_EVIDENCE_RELAXATION",
  evidenceClass: "FOCUSED_OFFLINE_ACTUAL_JOB_HANDLER_ROUTE_WITH_STUBBED_PROVIDER_BOUNDARY",
  passed: results.length - failed.length,
  failed: failed.length,
  total: results.length,
  realProviderCalls: 0,
  httpRequests: 0,
  stubbedProviderSendBoundaries,
  results
}, null, 2));
if (failed.length > 0) process.exitCode = 1;
