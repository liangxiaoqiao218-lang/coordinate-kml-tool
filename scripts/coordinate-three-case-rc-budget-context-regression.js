import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import {
  activateRecognitionDeadlineContext,
  getRecognitionBudget,
  recognitionDeadlineMiddleware
} from "../server/coordinate-finalizer/recognition-deadline.js";
import {
  COORDINATE_THREE_CASE_RC,
  createCoordinateThreeCaseRcAdmission
} from "../server/recognition/coordinate-three-case-rc-admission.js";
import { createRecognitionAcquisitionJobRuntime } from "../server/recognition/recognition-acquisition-job-runtime.js";
import { createCoordinateUsageCommitController } from "../server/coordinate-usage-atomicity.js";
import {
  createCoordinateThreeCaseRcExistingUserLookup,
  createCoordinateThreeCaseRcJobIngressMiddleware,
  createCoordinateThreeCaseRcJobPreflightMiddleware,
  createCoordinateThreeCaseRcInternalBindingMiddleware,
  createCoordinateThreeCaseRcJobForwarder,
  executeCoordinateThreeCaseRcPermissionBoundary,
  settleCoordinateThreeCaseRcPermissionFailure
} from "../server/recognition/coordinate-three-case-rc-runtime.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const serverSource = fs.readFileSync(path.join(root, "server.js"), "utf8");
const clientSource = fs.readFileSync(path.join(root, "scripts/coordinate-three-case-rc-client.ps1"), "utf8");
const mgrsImage = fs.readFileSync(path.join(root, "regression-samples/fixtures/缅甸坐标.jpg"));
const requestIdA = "11111111-1111-4111-8111-111111111111";
const requestIdB = "22222222-2222-4222-8222-222222222222";
const bearer = `Bearer ${"t".repeat(48)}`;
const controlledVisitorId = "controlled-rc-visitor-offline";

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

const readyEnv = Object.freeze({
  COORDINATE_THREE_CASE_RC_ENABLED: "true",
  COORDINATE_THREE_CASE_RC_RUN_ID: COORDINATE_THREE_CASE_RC.runId,
  COORDINATE_THREE_CASE_RC_WINDOW_SECONDS: String(COORDINATE_THREE_CASE_RC.runWindowSeconds),
  COORDINATE_THREE_CASE_RC_CLAIM_TTL_SECONDS: String(COORDINATE_THREE_CASE_RC.claimTtlSeconds),
  SUPABASE_URL: `https://${COORDINATE_THREE_CASE_RC.rcProjectRef}.supabase.co`,
  RC_SUPABASE_PROJECT_REF: COORDINATE_THREE_CASE_RC.rcProjectRef,
  RENDER_SERVICE_NAME: COORDINATE_THREE_CASE_RC.rcServiceName,
  ALIYUN_VISION_MODEL: COORDINATE_THREE_CASE_RC.model,
  ALIYUN_BASE_URL: `https://${COORDINATE_THREE_CASE_RC.providerHostname}/compatible-mode/v1`
});

function captureDeadlineRequest(requestId) {
  const finishListeners = [];
  const req = {
    get(name) {
      return String(name).toLowerCase() === "x-recognition-request-id" ? requestId : "";
    }
  };
  const res = {
    statusCode: 200,
    headersSent: false,
    setHeader() {},
    json(body) {
      return body;
    },
    once(name, listener) {
      if (name === "finish" || name === "close") finishListeners.push(listener);
    }
  };
  let nextCalled = false;
  recognitionDeadlineMiddleware({
    profile: "async",
    deadlineMs: 180_000,
    preflightDeadlineMs: 30_000,
    executionDeadlineMs: 150_000,
    responseReserveMs: 5_000,
    lowValueFallbackCutoffMs: 145_000
  })(req, res, () => {
    nextCalled = true;
  });
  assert.equal(nextCalled, true);
  assert.ok(req.recognitionDeadlineContext?.budget);
  return {
    req,
    budget: req.recognitionDeadlineContext.budget,
    cleanup() {
      for (const listener of finishListeners) listener();
    }
  };
}

function createFakeSupabase({ users = {} } = {}) {
  const calls = [];
  const userReads = [];
  return {
    calls,
    userReads,
    client: {
      async rpc(name, args) {
        calls.push({ name, args });
        if (name === "activate_coordinate_three_case_rc_run") {
          return { data: { accepted: true, action: "ACTIVATED" }, error: null };
        }
        if (name === "claim_coordinate_three_case_rc") {
          return { data: { accepted: true, action: "CLAIMED", expires_at: "2099-01-01T00:00:00Z" }, error: null };
        }
        if (name === "dispatch_coordinate_three_case_rc") {
          return { data: { accepted: true, action: "DISPATCHED" }, error: null };
        }
        if (name === "settle_coordinate_three_case_rc") {
          return { data: { accepted: true, action: "SETTLED" }, error: null };
        }
        throw new Error(`UNEXPECTED_RPC_${name}`);
      },
      from(table) {
        assert.equal(table, "users");
        let visitorId = "";
        const builder = {
          select(fields) {
            assert.equal(fields, "user_id");
            return builder;
          },
          eq(field, value) {
            assert.equal(field, "user_id");
            visitorId = String(value || "");
            return builder;
          },
          async maybeSingle() {
            userReads.push(visitorId);
            return { data: users[visitorId] || null, error: null };
          }
        };
        return builder;
      }
    }
  };
}

async function createReadyAdmission(env = readyEnv, recognitionRequestId = requestIdA, fake = createFakeSupabase()) {
  const admission = createCoordinateThreeCaseRcAdmission({
    supabase: fake.client,
    adminPassword: "test-only-admin",
    env,
    randomToken: () => "t".repeat(48)
  });
  await admission.activateOnStartup();
  await admission.claim({
    adminHeader: "test-only-admin",
    runId: COORDINATE_THREE_CASE_RC.runId,
    batchId: COORDINATE_THREE_CASE_RC.batchId,
    caseId: "mgrs",
    imageSha256: COORDINATE_THREE_CASE_RC.cases.mgrs.imageSha256,
    recognitionRequestId,
    visitorId: controlledVisitorId
  });
  return { admission, calls: fake.calls, fakeSupabase: fake };
}

async function runActualPermissionBoundary({
  admission,
  fakeSupabase,
  visitorId = controlledVisitorId,
  adminUsers = {
    [controlledVisitorId]: {
      visitorId: controlledVisitorId,
      status: "active",
      permissions: { aiOcrEnabled: true }
    }
  },
  onAuthorized
}) {
  return executeCoordinateThreeCaseRcPermissionBoundary({
    guardRequired: admission.config.guardRequired,
    visitorId,
    readAdminData: async () => ({
      users: adminUsers,
      featureFlags: {
        aiOcrEnabled: true,
        xyConvertEnabled: true,
        kmlExportEnabled: true,
        manualSupportEnabled: true,
        aiJudgeEnabled: true,
        goldCalculatorEnabled: true
      }
    }),
    findExistingUser: createCoordinateThreeCaseRcExistingUserLookup({ supabase: fakeSupabase.client }),
    ensureUser(data, currentVisitorId) {
      const key = String(currentVisitorId || "").trim();
      if (!key) return null;
      data.users[key] ||= {
        visitorId: key,
        status: "active",
        permissions: { aiOcrEnabled: true }
      };
      return data.users[key];
    },
    getEffectivePermissions(user, featureFlags) {
      return {
        aiOcrEnabled: Boolean(user?.status !== "disabled"
          && featureFlags?.aiOcrEnabled === true
          && user?.permissions?.aiOcrEnabled === true)
      };
    },
    onAuthorized
  });
}

function validBindInput(recognitionRequestId) {
  return {
    caseId: "mgrs",
    productMode: "",
    recognitionRequestId,
    imageBuffer: mgrsImage,
    authorization: bearer,
    visitorId: controlledVisitorId
  };
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
    status(value) {
      this.statusCode = Number(value);
      return this;
    },
    json(body) {
      this.payload = body;
      this.headersSent = true;
      return this;
    },
    finish() {
      for (const listener of listeners) listener();
    }
  };
}

function runJobIngress({ admission, headerVisitorId = "", bodyVisitorId = "", authorization = bearer }) {
  const req = {
    body: { visitorId: bodyVisitorId },
    get(name) {
      const key = String(name).toLowerCase();
      if (key === "x-visitor-id") return headerVisitorId;
      if (key === "authorization") return authorization;
      return "";
    }
  };
  const res = createRouteResponse();
  let nextCalled = false;
  createCoordinateThreeCaseRcJobIngressMiddleware({ admission })(req, res, () => {
    nextCalled = true;
  });
  return { req, res, nextCalled };
}

async function runActualJobRoutePrefix({
  admission,
  runtime,
  recognitionRequestId = requestIdA,
  headerVisitorId = controlledVisitorId,
  bodyVisitorId = controlledVisitorId,
  authorization = bearer
}) {
  const headers = {
    authorization,
    "x-recognition-request-id": recognitionRequestId,
    "x-coordinate-rc-case-id": "mgrs",
    "x-visitor-id": headerVisitorId
  };
  const req = {
    body: { visitorId: bodyVisitorId },
    file: {
      buffer: Buffer.from(mgrsImage),
      mimetype: "image/jpeg",
      originalname: "frozen-fixture.jpg"
    },
    get(name) { return headers[String(name).toLowerCase()] || ""; }
  };
  const res = createRouteResponse();
  let accepted = null;
  const ingress = createCoordinateThreeCaseRcJobIngressMiddleware({ admission });
  const preflight = createCoordinateThreeCaseRcJobPreflightMiddleware({
    admission,
    isRecognitionRequestId: value => /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u.test(value),
    randomRequestId: () => { throw new Error("UNEXPECTED_RANDOM_REQUEST_ID"); }
  });
  const routeRuntime = vm.createContext({
    Buffer,
    crypto: {
      randomUUID() { throw new Error("UNEXPECTED_RANDOM_REQUEST_ID"); }
    },
    coordinateThreeCaseRcAdmission: admission,
    indonesiaStructuredBRcAdmission: { config: { ready: false } },
    INDONESIA_STRUCTURED_B_RC: { mode: "indonesia_utm50s_structured_b" },
    IndonesiaStructuredBRcAdmissionError: class extends Error {},
    COORDINATE_IMAGE_INVALID_CODE: "COORDINATE_IMAGE_INVALID",
    canonicalizeCoordinateImageUpload(file) {
      return { valid: true, status: "VALID", file };
    },
    validateCoordinateImageUpload() { return { valid: true }; },
    isRecognitionRequestId(value) {
      return /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u.test(value);
    },
    getUploadedFileDisplayName(file) { return file?.originalname || "coordinate-image"; },
    getRegressionTestMode() { return { active: true }; },
    recognitionAcquisitionJobRuntime: runtime
  });
  const routeCallbackSource = extractAsyncRouteCallback(
    serverSource,
    '"/api/recognize-coordinates/jobs"'
  );
  const routeCallback = vm.runInContext(`(${routeCallbackSource})`, routeRuntime);
  await new Promise((resolve, reject) => {
    let routeInvoked = false;
    ingress(req, res, () => {
      try {
        preflight(req, res, () => {
          routeInvoked = true;
          Promise.resolve(routeCallback(req, res)).then(() => {
            accepted = res.payload;
            resolve();
          }, reject);
        });
        if (!routeInvoked && res.headersSent) resolve();
      } catch (error) { reject(error); }
    });
    if (!routeInvoked && res.headersSent) resolve();
  });
  return { req, res, accepted };
}

async function runInternalBinding({
  admission,
  headers,
  imageBuffer = mgrsImage,
  body = {},
  attachDeadlineContext = true,
  mutateHeadersAfterContext = null,
  onBound = async () => ({ success: true })
}) {
  const normalizedHeaders = Object.fromEntries(
    Object.entries(headers).map(([key, value]) => [key.toLowerCase(), value])
  );
  const req = {
    body,
    file: {
      buffer: imageBuffer,
      mimetype: "image/jpeg",
      originalname: "frozen-fixture.jpg"
    },
    get(name) {
      return normalizedHeaders[String(name).toLowerCase()] || "";
    }
  };
  const res = createRouteResponse();
  const bindingMiddleware = createCoordinateThreeCaseRcInternalBindingMiddleware({
    admission,
    activateDeadlineContext: activateRecognitionDeadlineContext
  });
  let nextCalled = false;
  const invokeBinding = async () => {
    if (typeof mutateHeadersAfterContext === "function") mutateHeadersAfterContext(normalizedHeaders);
    await bindingMiddleware(req, res, async () => {
      nextCalled = true;
      const bodyResult = await onBound(req, res);
      if (bodyResult !== undefined && !res.headersSent) res.status(200).json(bodyResult);
    });
  };
  if (attachDeadlineContext) {
    await new Promise((resolve, reject) => {
      recognitionDeadlineMiddleware({
        profile: "async",
        deadlineMs: 180_000,
        preflightDeadlineMs: 30_000,
        executionDeadlineMs: 150_000,
        responseReserveMs: 5_000,
        lowValueFallbackCutoffMs: 145_000
      })(req, res, () => {
        Promise.resolve(invokeBinding()).then(resolve, reject);
      });
    });
  } else {
    await invokeBinding();
  }
  res.finish();
  return { req, res, nextCalled };
}

async function waitForJobTerminal(runtime, accepted) {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    const snapshot = runtime.get(accepted.jobId, accepted.jobAccessToken);
    if (["SUCCEEDED", "FAILED"].includes(snapshot?.status)) return snapshot;
    await new Promise(resolve => setImmediate(resolve));
  }
  throw new Error("OFFLINE_JOB_DID_NOT_SETTLE");
}

function createActualRecognitionHandler({ admission, fakeSupabase }) {
  const adminData = {
    users: {},
    featureFlags: {
      aiOcrEnabled: true,
      xyConvertEnabled: true,
      kmlExportEnabled: true,
      manualSupportEnabled: true,
      aiJudgeEnabled: true,
      goldCalculatorEnabled: true
    }
  };
  const runtime = vm.createContext({
    Buffer,
    console: { log() {}, error() {}, warn() {} },
    process: { env: {} },
    queueMicrotask,
    setTimeout,
    clearTimeout,
    coordinateThreeCaseRcAdmission: admission,
    indonesiaStructuredBRcAdmission: { config: { ready: false } },
    INDONESIA_STRUCTURED_B_RC: { mode: "indonesia_utm50s_structured_b" },
    createRecognitionDiagnosticSession() { return { event() {} }; },
    activateRecognitionDeadlineContext,
    getRecognitionBudget,
    getRecognitionDeadlineSignal() { return null; },
    getRegressionTestMode() { return { active: true }; },
    getCoordinateUsageSessionBinding() { throw new Error("UNEXPECTED_USAGE_SESSION_BINDING"); },
    createRecognitionAcquisitionEvidenceStore() {
      return { getOrBuild() { return null; } };
    },
    classifyOneShotStructuredFamily() {
      return { family: "none", matched: false, reason: "offline", evidence: {} };
    },
    createOneShotAcquisitionContract({ route }) { return { route }; },
    createDmsGroupedRetryOrchestrator() { return {}; },
    createCoordinateUsageCommitController,
    coordinateUsageAtomicity: null,
    isRecognitionStopError() { return false; },
    buildCoordinateVerificationResponseWithoutRecognitionBudget(payload) { return payload; },
    recordCoordinateRecognitionIssue() { throw new Error("UNEXPECTED_ISSUE_WRITE"); },
    storeRegressionRecognitionTrace() { return true; },
    coordinateCaseStore: null,
    canonicalizeCoordinateImageUpload(file) { return { valid: true, status: "VALID", file }; },
    validateCoordinateImageUpload() { return { valid: true }; },
    createCoordinateImageIdentity() {
      return Object.freeze({ image_sha256: "offline-redacted", width: 1, height: 1, page: 1 });
    },
    executeCoordinateThreeCaseRcPermissionBoundary,
    readAdminData: async () => adminData,
    createCoordinateThreeCaseRcExistingUserLookup: () => async visitorId => (
      fakeSupabase.client.from("users").select("user_id").eq("user_id", visitorId).maybeSingle()
        .then(result => result.data || null)
    ),
    supabase: fakeSupabase.client,
    ensureUser(data, visitorId) {
      const key = String(visitorId || "").trim();
      if (!key) return null;
      data.users[key] ||= {
        visitorId: key,
        status: "active",
        permissions: { aiOcrEnabled: true }
      };
      return data.users[key];
    },
    getEffectivePermissions(user, featureFlags) {
      return {
        aiOcrEnabled: Boolean(user?.status !== "disabled"
          && featureFlags?.aiOcrEnabled === true
          && user?.permissions?.aiOcrEnabled === true)
      };
    },
    settleCoordinateThreeCaseRcPermissionFailure,
    buildRegressionTextCoordinateResult(rawText) { return { rawText, coordinates: rawText }; },
    buildCoordinateEngineV2ShadowResult() { return null; },
    aliyunBaseURL: "http://127.0.0.1:1/v1",
    aliyunVisionModel: COORDINATE_THREE_CASE_RC.model,
    aliyunOcrModel: "offline-not-called",
    RECOGNITION_DEADLINE_CODE: "RECOGNITION_DEADLINE_EXCEEDED",
    RECOGNITION_BUDGET_CODE: "RECOGNITION_BUDGET_EXHAUSTED",
    noCoordinatesText: "NO_COORDINATES"
  });
  const handlerSource = extractFunctionDeclaration(serverSource, "recognizeCoordinatesHandler");
  vm.runInContext(handlerSource, runtime);
  return vm.runInContext("recognizeCoordinatesHandler", runtime);
}

const results = [];
async function test(name, action) {
  try {
    await action();
    results.push({ name, status: "PASS" });
  } catch (error) {
    results.push({ name, status: "FAIL", code: error?.message || String(error) });
  }
}

await test("client claim and job request remain ordered before internal forwarding", async () => {
  const claimIndex = clientSource.indexOf("/api/admin/coordinate-products/three-case-rc/claims");
  const jobIndex = clientSource.indexOf("/api/recognize-coordinates/jobs");
  const forwardIndex = serverSource.indexOf("/api/internal/recognize-coordinates-long");
  assert.ok(claimIndex >= 0 && jobIndex > claimIndex && forwardIndex >= 0);
});

await test("production route installs the shared request-owned binding middleware", async () => {
  const routeIndex = serverSource.indexOf('"/api/internal/recognize-coordinates-long"');
  const bindingIndex = serverSource.indexOf("createCoordinateThreeCaseRcInternalBindingMiddleware({", routeIndex);
  const handlerIndex = serverSource.indexOf("recognizeCoordinatesHandler", bindingIndex);
  assert.ok(routeIndex >= 0 && bindingIndex > routeIndex && handlerIndex > bindingIndex);
  assert.match(serverSource.slice(bindingIndex, handlerIndex), /activateDeadlineContext:\s*activateRecognitionDeadlineContext/u);
});

await test("actual jobs callback and recognition handler complete a provider-free request chain", async () => {
  const fake = createFakeSupabase({
    users: {
      [controlledVisitorId]: { user_id: controlledVisitorId, status: "active" }
    }
  });
  const { admission, calls, fakeSupabase } = await createReadyAdmission(readyEnv, requestIdA, fake);
  const recognizeCoordinatesHandler = createActualRecognitionHandler({ admission, fakeSupabase });
  let capturedInternalError = null;
  const internalTransport = async (url, options) => {
    assert.equal(url, "http://127.0.0.1:3000/api/internal/recognize-coordinates-long");
    assert.equal(options.method, "POST");
    const image = options.body.get("image");
    const imageBuffer = Buffer.from(await image.arrayBuffer());
    const body = {};
    for (const [key, value] of options.body.entries()) {
      if (typeof value === "string") body[key] = value;
    }
    let routed;
    try {
      routed = await runInternalBinding({
        admission,
        headers: options.headers,
        imageBuffer,
        body,
        onBound: async (boundReq, boundRes) => {
          boundReq.headers ||= {};
          boundReq.headers["x-regression-test"] = "1";
          const originalGet = boundReq.get.bind(boundReq);
          boundReq.get = name => {
            const key = String(name).toLowerCase();
            if (key === "x-sr08b-deadline-scenario") return "fast_success";
            if (key === "x-regression-test") return "1";
            return originalGet(name);
          };
          return recognizeCoordinatesHandler(boundReq, boundRes);
        }
      });
    } catch (error) {
      capturedInternalError = error;
      throw error;
    }
    return {
      ok: routed.res.statusCode >= 200 && routed.res.statusCode < 300,
      status: routed.res.statusCode,
      async json() {
        return routed.res.payload;
      }
    };
  };
  const forward = createCoordinateThreeCaseRcJobForwarder({
    fetchImpl: internalTransport,
    getPort: () => 3000,
    internalToken: "offline-internal-token",
    coordinateThreeCaseRcAdmission: admission,
    indonesiaStructuredBRcAdmission: { config: { ready: false } },
    indonesiaProductMode: "indonesia_utm50s_structured_b"
  });
  const runtime = createRecognitionAcquisitionJobRuntime({ execute: forward, maxJobs: 1 });
  const routed = await runActualJobRoutePrefix({
    admission,
    runtime
  });
  assert.ok(routed.accepted, `route_status_${routed.res.statusCode}_payload_${Boolean(routed.res.payload)}_keys_${Object.keys(routed.res.payload || {}).join("_")}`);
  const accepted = routed.accepted;
  const terminal = await waitForJobTerminal(runtime, accepted);
  assert.equal(
    terminal.status,
    "SUCCEEDED",
    `terminal_${capturedInternalError?.message || terminal.error?.code || terminal.error || "unknown"}`
  );
  assert.equal(terminal.result?.success, true);
  assert.match(String(terminal.result?.rawText || ""), /11°00'00\.00"N/u);
  assert.deepEqual(fakeSupabase.userReads, [controlledVisitorId]);
  assert.equal(calls.filter(call => call.name === "dispatch_coordinate_three_case_rc").length, 0);
});

await test("missing request context cannot borrow an ambient request budget", async () => {
  const ambient = captureDeadlineRequest(requestIdA);
  activateRecognitionDeadlineContext(ambient.req);
  assert.equal(getRecognitionBudget(), ambient.budget);
  const missingReq = { get: () => requestIdB };
  const ownedContext = activateRecognitionDeadlineContext(missingReq);
  const ownedBudget = ownedContext?.budget || null;
  assert.equal(ownedContext, null);
  assert.equal(ownedBudget, null);
  assert.equal(getRecognitionBudget(), ambient.budget);
  ambient.cleanup();
});

await test("actual job ingress accepts only the claim-bound controlled visitor envelope", async () => {
  const { admission } = await createReadyAdmission();
  const routed = runJobIngress({
    admission,
    headerVisitorId: controlledVisitorId,
    bodyVisitorId: controlledVisitorId
  });
  assert.equal(routed.nextCalled, true);
  assert.equal(routed.req.coordinateThreeCaseRcVisitorId, controlledVisitorId);
});

await test("actual job ingress rejects a missing visitor before queue or provider", async () => {
  const { admission, calls } = await createReadyAdmission();
  const routed = runJobIngress({ admission, headerVisitorId: "", bodyVisitorId: "" });
  assert.equal(routed.nextCalled, false);
  assert.equal(routed.res.statusCode, 400);
  assert.equal(routed.res.payload?.success, false);
  assert.equal(routed.res.payload?.code, "COORDINATE_THREE_CASE_RC_VISITOR_REQUIRED");
  assert.equal(calls.some(call => call.name === "dispatch_coordinate_three_case_rc"), false);
});

await test("actual job ingress rejects a mismatched visitor before queue or provider", async () => {
  const { admission, calls } = await createReadyAdmission();
  const routed = runJobIngress({
    admission,
    headerVisitorId: controlledVisitorId,
    bodyVisitorId: "different-controlled-visitor"
  });
  assert.equal(routed.nextCalled, false);
  assert.equal(routed.res.statusCode, 422);
  assert.equal(routed.res.payload?.success, false);
  assert.equal(routed.res.payload?.code, "COORDINATE_THREE_CASE_RC_VISITOR_MISMATCH");
  assert.equal(calls.some(call => call.name === "dispatch_coordinate_three_case_rc"), false);
});

await test("production permission helper preserves unknown and denied reasons through uncharged settlement", async () => {
  for (const scenario of [
    {
      sourceUser: null,
      reason: "rc_visitor_not_authorized",
      code: "COORDINATE_THREE_CASE_RC_VISITOR_NOT_AUTHORIZED"
    },
    {
      sourceUser: { user_id: controlledVisitorId },
      adminUsers: {
        [controlledVisitorId]: {
          visitorId: controlledVisitorId,
          status: "disabled",
          permissions: { aiOcrEnabled: true }
        }
      },
      reason: "ai_ocr_permission_denied",
      code: "COORDINATE_AI_OCR_PERMISSION_DENIED"
    },
    {
      sourceUser: { user_id: controlledVisitorId, status: "active" },
      adminUsers: {
        [controlledVisitorId]: {
          visitorId: controlledVisitorId,
          status: "active",
          permissions: { aiOcrEnabled: false }
        }
      },
      reason: "ai_ocr_permission_denied",
      code: "COORDINATE_AI_OCR_PERMISSION_DENIED"
    }
  ]) {
    const fake = createFakeSupabase({
      users: scenario.sourceUser ? { [controlledVisitorId]: scenario.sourceUser } : {}
    });
    const { admission, calls, fakeSupabase } = await createReadyAdmission(readyEnv, requestIdA, fake);
    const captured = captureDeadlineRequest(requestIdA);
    const budget = captured.budget;
    admission.bindBudget({ budget, ...validBindInput(requestIdA) });
    let authorizedCalled = false;
    const boundary = await runActualPermissionBoundary({
      admission,
      fakeSupabase,
      adminUsers: scenario.adminUsers || {},
      onAuthorized: async () => {
        authorizedCalled = true;
        return null;
      }
    });
    assert.equal(boundary.allowed, false);
    assert.equal(authorizedCalled, false);
    const usageController = createCoordinateUsageCommitController({
      regressionTestMode: true,
      budget,
      atomicityService: null,
      recognitionRequestId: requestIdA,
      userId: controlledVisitorId,
      sessionBindingSha256: null,
      providerCostState: () => "NOT_INCURRED"
    });
    const settled = await settleCoordinateThreeCaseRcPermissionFailure({
      admission,
      budget,
      usageCommitController: usageController,
      httpStatus: boundary.httpStatus,
      body: boundary.body
    });
    assert.deepEqual(
      { success: settled.success, reason: settled.reason, code: settled.code, usageConsumed: settled.usageConsumed },
      { success: false, reason: scenario.reason, code: scenario.code, usageConsumed: false }
    );
    assert.equal(calls.some(call => call.name === "dispatch_coordinate_three_case_rc"), false);
    assert.equal(calls.filter(call => call.name === "settle_coordinate_three_case_rc").length, 1);
    captured.cleanup();
  }
});

await test("valid request context binds through admission and reaches pre-provider dispatch", async () => {
  const captured = captureDeadlineRequest(requestIdA);
  const context = activateRecognitionDeadlineContext(captured.req);
  const budget = context?.budget || null;
  const { admission, calls } = await createReadyAdmission();
  const bound = admission.bindBudget({ budget, ...validBindInput(requestIdA) });
  assert.equal(bound.bound, true);
  assert.equal(admission.getBoundCaseId(budget), "mgrs");
  const dispatched = await admission.dispatchForBudget({
    budget,
    modelName: COORDINATE_THREE_CASE_RC.model,
    maxTokens: 8_000,
    enableThinking: false
  });
  assert.equal(dispatched.dispatched, true);
  assert.equal(calls.filter(call => call.name === "dispatch_coordinate_three_case_rc").length, 1);
  captured.cleanup();
});

await test("actual internal binding rejects request identity mismatch before provider dispatch", async () => {
  const { admission, calls } = await createReadyAdmission();
  const routed = await runInternalBinding({
    admission,
    headers: {
      authorization: bearer,
      "x-recognition-request-id": requestIdA,
      "x-coordinate-rc-case-id": "mgrs",
      "x-visitor-id": controlledVisitorId
    },
    body: { visitorId: controlledVisitorId },
    mutateHeadersAfterContext: headers => {
      headers["x-recognition-request-id"] = requestIdB;
    }
  });
  assert.equal(routed.nextCalled, false);
  assert.equal(routed.res.statusCode, 503);
  assert.equal(routed.res.payload?.reason, "COORDINATE_THREE_CASE_RC_BUDGET_CONTEXT_MISMATCH");
  assert.equal(calls.some(call => call.name === "dispatch_coordinate_three_case_rc"), false);
});

await test("actual internal binding cannot borrow ambient context when request context is missing", async () => {
  const ambient = captureDeadlineRequest(requestIdA);
  activateRecognitionDeadlineContext(ambient.req);
  const { admission, calls } = await createReadyAdmission();
  const routed = await runInternalBinding({
    admission,
    headers: {
      authorization: bearer,
      "x-recognition-request-id": requestIdB,
      "x-coordinate-rc-case-id": "mgrs",
      "x-visitor-id": controlledVisitorId
    },
    body: { visitorId: controlledVisitorId },
    attachDeadlineContext: false
  });
  assert.equal(routed.nextCalled, false);
  assert.equal(routed.res.statusCode, 503);
  assert.equal(routed.res.payload?.reason, "COORDINATE_THREE_CASE_RC_BUDGET_REQUIRED");
  assert.equal(calls.some(call => call.name === "dispatch_coordinate_three_case_rc"), false);
  ambient.cleanup();
});

await test("actual internal binding rejects a sample mismatch before provider dispatch", async () => {
  const { admission, calls } = await createReadyAdmission();
  const routed = await runInternalBinding({
    admission,
    headers: {
      authorization: bearer,
      "x-recognition-request-id": requestIdA,
      "x-coordinate-rc-case-id": "mgrs",
      "x-visitor-id": controlledVisitorId
    },
    body: { visitorId: controlledVisitorId },
    imageBuffer: Buffer.from("not-the-frozen-image", "utf8")
  });
  assert.equal(routed.nextCalled, false);
  assert.equal(routed.res.statusCode, 422);
  assert.equal(routed.res.payload?.reason, "COORDINATE_THREE_CASE_RC_JOB_PREFLIGHT_REJECTED");
  assert.equal(calls.some(call => call.name === "dispatch_coordinate_three_case_rc"), false);
});

await test("actual internal binding rejects a window mismatch before provider dispatch", async () => {
  const fake = createFakeSupabase();
  const admission = createCoordinateThreeCaseRcAdmission({
    supabase: fake.client,
    adminPassword: "test-only-admin",
    env: { ...readyEnv, COORDINATE_THREE_CASE_RC_RUN_ID: "wrong-window" }
  });
  const routed = await runInternalBinding({
    admission,
    headers: {
      authorization: bearer,
      "x-recognition-request-id": requestIdA,
      "x-coordinate-rc-case-id": "mgrs",
      "x-visitor-id": controlledVisitorId
    },
    body: { visitorId: controlledVisitorId }
  });
  assert.equal(routed.nextCalled, false);
  assert.equal(routed.res.statusCode, 503);
  assert.equal(routed.res.payload?.reason, "COORDINATE_THREE_CASE_RC_ADMISSION_UNAVAILABLE");
  assert.equal(fake.calls.some(call => call.name === "dispatch_coordinate_three_case_rc"), false);
});

const failed = results.filter(result => result.status !== "PASS");
console.log(JSON.stringify({
  suite: "COORDINATE_THREE_CASE_RC_BUDGET_CONTEXT",
  passed: results.length - failed.length,
  failed: failed.length,
  total: results.length,
  providerCalls: 0,
  httpRequests: 0,
  stubbedExternalBoundaries: ["database_rpc", "internal_http_transport", "provider_transport"],
  cases: results
}, null, 2));
if (failed.length > 0) process.exitCode = 1;
