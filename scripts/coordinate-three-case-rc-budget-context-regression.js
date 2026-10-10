import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
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
import {
  createCoordinateThreeCaseRcInternalBindingMiddleware,
  createCoordinateThreeCaseRcJobForwarder
} from "../server/recognition/coordinate-three-case-rc-runtime.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const serverSource = fs.readFileSync(path.join(root, "server.js"), "utf8");
const clientSource = fs.readFileSync(path.join(root, "scripts/coordinate-three-case-rc-client.ps1"), "utf8");
const mgrsImage = fs.readFileSync(path.join(root, "regression-samples/fixtures/缅甸坐标.jpg"));
const requestIdA = "11111111-1111-4111-8111-111111111111";
const requestIdB = "22222222-2222-4222-8222-222222222222";
const bearer = `Bearer ${"t".repeat(48)}`;

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

function createFakeSupabase() {
  const calls = [];
  return {
    calls,
    client: {
      async rpc(name, args) {
        calls.push({ name, args });
        if (name === "activate_coordinate_three_case_rc_run") {
          return { data: { accepted: true, action: "ACTIVATED" }, error: null };
        }
        if (name === "dispatch_coordinate_three_case_rc") {
          return { data: { accepted: true, action: "DISPATCHED" }, error: null };
        }
        throw new Error(`UNEXPECTED_RPC_${name}`);
      }
    }
  };
}

async function createReadyAdmission(env = readyEnv) {
  const fake = createFakeSupabase();
  const admission = createCoordinateThreeCaseRcAdmission({
    supabase: fake.client,
    adminPassword: "test-only-admin",
    env
  });
  await admission.activateOnStartup();
  return { admission, calls: fake.calls };
}

function validBindInput(recognitionRequestId) {
  return {
    caseId: "mgrs",
    productMode: "",
    recognitionRequestId,
    imageBuffer: mgrsImage,
    authorization: bearer
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
      const bodyResult = await onBound(req);
      res.status(200).json(bodyResult);
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

await test("actual job runtime and internal binding reach the budget gate before the send stub", async () => {
  const { admission, calls } = await createReadyAdmission();
  const order = [];
  const internalTransport = async (url, options) => {
    assert.equal(url, "http://127.0.0.1:3000/api/internal/recognize-coordinates-long");
    assert.equal(options.method, "POST");
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
      onBound: async () => {
        order.push("budget_gate");
        const budget = getRecognitionBudget();
        await admission.dispatchForBudget({
          budget,
          modelName: COORDINATE_THREE_CASE_RC.model,
          maxTokens: 8_000,
          enableThinking: false
        });
        order.push("provider_send_stub");
        return {
          success: true,
          model: COORDINATE_THREE_CASE_RC.model,
          providerCallCount: 1,
          providerCompletionState: "SUCCEEDED"
        };
      }
    });
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
  const accepted = runtime.enqueue({
    requestId: requestIdA,
    forwardHeaders: {
      authorization: bearer,
      "x-recognition-request-id": requestIdA,
      "x-coordinate-rc-case-id": "mgrs"
    },
    body: {},
    file: {
      buffer: mgrsImage,
      mimetype: "image/jpeg",
      originalname: "frozen-fixture.jpg"
    }
  });
  const terminal = await waitForJobTerminal(runtime, accepted);
  assert.equal(terminal.status, "SUCCEEDED");
  assert.deepEqual(order, ["budget_gate", "provider_send_stub"]);
  assert.equal(calls.filter(call => call.name === "dispatch_coordinate_three_case_rc").length, 1);
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
      "x-coordinate-rc-case-id": "mgrs"
    },
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
      "x-coordinate-rc-case-id": "mgrs"
    },
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
      "x-coordinate-rc-case-id": "mgrs"
    },
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
      "x-coordinate-rc-case-id": "mgrs"
    }
  });
  assert.equal(routed.nextCalled, false);
  assert.equal(routed.res.statusCode, 503);
  assert.equal(routed.res.payload?.reason, "COORDINATE_THREE_CASE_RC_ADMISSION_UNAVAILABLE");
  assert.equal(fake.calls.some(call => call.name === "dispatch_coordinate_three_case_rc"), false);
});

await test("production provider send remains strictly after budget dispatch", async () => {
  const functionIndex = serverSource.indexOf("async function callAliyunVision");
  const dispatchIndex = serverSource.indexOf("await coordinateThreeCaseRcAdmission.dispatchForBudget({", functionIndex);
  const providerFetchIndex = serverSource.indexOf("response = await fetch(getAliyunChatCompletionsUrl()", dispatchIndex);
  assert.ok(functionIndex >= 0 && dispatchIndex > functionIndex && providerFetchIndex > dispatchIndex);
});

const failed = results.filter(result => result.status !== "PASS");
console.log(JSON.stringify({
  suite: "COORDINATE_THREE_CASE_RC_BUDGET_CONTEXT",
  passed: results.length - failed.length,
  failed: failed.length,
  total: results.length,
  providerCalls: 0,
  httpRequests: 0,
  cases: results
}, null, 2));
if (failed.length > 0) process.exitCode = 1;
