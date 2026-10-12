import assert from "node:assert/strict";
import crypto from "node:crypto";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import { CoordinateConfirmationRuntime } from "../server/coordinate-finalizer/confirmation-runtime.js";
import { createGeometryHash, FINALIZED_COORDINATE_SCHEMA_VERSION, FINALIZED_COORDINATE_CRS } from "../server/coordinate-finalizer/index.js";
import { MapPreviewAdapter } from "../server/spatial/adapters/map-preview-adapter.js";
import { calculateSpatialFacts } from "../server/spatial/spatial-facts.js";
import { evaluateRecognitionOutputCapability } from "../server/recognition/recognition-output-capability.js";
import { createCoordinateThreeCaseRcBrowserHandoff } from "../server/recognition/coordinate-three-case-rc-browser-handoff.js";
import { createRecognitionAcquisitionJobRuntime } from "../server/recognition/recognition-acquisition-job-runtime.js";
import { COORDINATE_THREE_CASE_RC, createCoordinateThreeCaseRcAdmission, CoordinateThreeCaseRcAdmissionError } from "../server/recognition/coordinate-three-case-rc-admission.js";
import { canonicalizeCoordinateImageUpload, validateCoordinateImageUpload } from "../server/recognition/coordinate-image-safety.js";
import { createCoordinateThreeCaseRcJobIngressMiddleware, createCoordinateThreeCaseRcJobPreflightMiddleware,
  createCoordinateThreeCaseRcJobForwarder } from "../server/recognition/coordinate-three-case-rc-runtime.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const cases = [];
const r8Incremental = process.argv.includes("--r8-incremental");
const r8Filter = process.argv.find(value => value.startsWith("--case="))?.slice(7);
const test = async (name, fn) => {
  if (r8Incremental && !name.startsWith("R8 ")) return;
  if (r8Filter && !new RegExp(r8Filter, "u").test(name)) return;
  try {
    await fn();
    cases.push({ name, pass: true });
  } catch (error) {
    cases.push({ name, pass: false, error: error?.stack || error?.message || String(error) });
  }
};

const identity = { resultId: "result-r7", resultRevision: 2, geometryHash: "hash-r7" };
const terminalJob = Object.freeze({
  status: "SUCCEEDED",
  completedAt: Date.parse("2026-10-11T08:00:00Z"),
  expiresAt: Date.parse("2026-10-11T08:30:00Z"),
  rcBinding: {
    runId: "coordinate-three-case-rc-20261011-r7",
    batchId: "coordinate-three-case-rc-20261011-v7",
    caseId: "indonesia",
    recognitionRequestId: "11111111-1111-4111-8111-111111111111",
    visitorId: "rc-b-release-20261010-4f125a9",
    imageSha256: "41f2b2117667fb92f6a4eb703822b1893e29c985be2e14f7b20fbda103b66cf2",
    productMode: "indonesia_utm50s_structured_b"
  },
  result: {
    success: true,
    requestId: "11111111-1111-4111-8111-111111111111",
    recognitionUsageConsumed: true,
    finalizedCoordinateResult: {
      ...identity,
      technicalKmlReady: true,
      crs: { id: "EPSG:4326", axisOrder: "longitude_latitude" },
      geometry: { type: "Polygon", coordinates: [[[1, 2], [3, 2], [3, 4], [1, 2]]] }
    }
  }
});

function runtime(overrides = {}) {
  let clock = Date.parse("2026-10-11T08:00:00Z");
  let counter = 0;
  const instance = createCoordinateThreeCaseRcBrowserHandoff({
    runId: "coordinate-three-case-rc-20261011-r7",
    batchId: "coordinate-three-case-rc-20261011-v7",
    expectedOrigin: "https://rc.example",
    caseDefinitions: { indonesia: { imageSha256: terminalJob.rcBinding.imageSha256, productMode: terminalJob.rcBinding.productMode } },
    now: () => clock,
    randomToken: () => `token_${++counter}_${"x".repeat(32)}`,
    readRunStatus: async () => ({ runStatus: "ACTIVE", runClosed: false, deadlineAt: "2026-10-11T08:20:00Z" }),
    readExistingUserAccess: async () => ({
      existingUser: true,
      permissions: { aiOcrEnabled: true, kmlExportEnabled: true },
      convertRemaining: 1
    }),
    readJob: () => terminalJob,
    validateFinalizedIdentity: () => ({
      ok: true,
      result: terminalJob.result.finalizedCoordinateResult,
      expiresAt: Date.parse("2026-10-11T08:15:00Z")
    }),
    selectedMapProvider: () => "OPENFREEMAP",
    ...overrides
  });
  return { instance, advance: milliseconds => { clock += milliseconds; } };
}

async function actualJobBackedRuntime({ bindingOverrides = {}, clearRegistry = false } = {}) {
  const clock = Date.parse("2026-10-11T08:00:00Z");
  const confirmationRuntime = new CoordinateConfirmationRuntime({ now: () => clock });
  confirmationRuntime.register(terminalJob.result.finalizedCoordinateResult);
  const jobRuntime = createRecognitionAcquisitionJobRuntime({
    now: () => clock,
    execute: async () => ({ httpStatus: 200, result: structuredClone(terminalJob.result) })
  });
  const rcBinding = { ...terminalJob.rcBinding, ...bindingOverrides };
  const queued = jobRuntime.enqueue({ requestId: terminalJob.result.requestId, rcBinding });
  let terminal = null;
  for (let attempt = 0; attempt < 20; attempt += 1) {
    await new Promise(resolve => setImmediate(resolve));
    terminal = jobRuntime.getForHandoff(queued.jobId, queued.jobAccessToken);
    if (["SUCCEEDED", "FAILED"].includes(terminal?.status)) break;
  }
  assert.equal(terminal?.status, "SUCCEEDED");
  if (clearRegistry) confirmationRuntime.clear();
  const { instance } = runtime({
    readJob: (jobId, accessToken) => jobRuntime.getForHandoff(jobId, accessToken),
    validateFinalizedIdentity: candidate => {
      const validated = confirmationRuntime.validateIdentity(candidate);
      const record = confirmationRuntime.records.get(String(candidate?.resultId || ""));
      return Object.freeze({ ...validated, expiresAt: Number(record?.expiresAt) });
    }
  });
  return { instance, queued, terminal, confirmationRuntime };
}

async function expectCode(promise, code) {
  await assert.rejects(promise, error => error?.code === code);
}

function extractFunction(source, name) {
  const match = new RegExp(`(?:async\\s+)?function\\s+${name}\\s*\\(`, "u").exec(source);
  if (!match) throw new Error(`FUNCTION_NOT_FOUND:${name}`);
  const tail = source.slice(match.index);
  for (const end of tail.matchAll(/^\s*\}/gmu)) {
    const candidate = tail.slice(0, end.index + end[0].length);
    try { new vm.Script(candidate); return candidate; } catch {}
  }
  throw new Error(`FUNCTION_UNBALANCED:${name}`);
}

function r8Route(source, route, context) {
  const start = source.indexOf(`\"${route}\"`);
  assert.ok(start >= 0, route);
  const tail = source.slice(start);
  const match = /(?:async )?\(req, res\) => \{/u.exec(tail);
  assert.ok(match, route);
  const callback = tail.slice(match.index);
  for (const end of callback.matchAll(/^\s*\}/gmu)) {
    const code = `(${callback.slice(0, end.index + end[0].length)})`;
    try { new vm.Script(code); } catch { continue; }
    return vm.runInContext(code, context);
  }
  throw new Error("ROUTE_UNBALANCED:" + route);
}

await test("rejects non-controlled visitor", async () => {
  const { instance } = runtime();
  await expectCode(instance.createSession({ visitorId: "other", origin: "https://rc.example" }), "COORDINATE_THREE_CASE_RC_BROWSER_VISITOR_REJECTED");
});

await test("requires existing user and permissions", async () => {
  const { instance } = runtime({ readExistingUserAccess: async () => ({ existingUser: false }) });
  await expectCode(instance.createSession({ visitorId: instance.exactVisitorId, origin: "https://rc.example" }), "COORDINATE_THREE_CASE_RC_BROWSER_PERMISSION_REJECTED");
});

await test("requires positive convert quota", async () => {
  const { instance } = runtime({ readExistingUserAccess: async () => ({ existingUser: true, permissions: { aiOcrEnabled: true, kmlExportEnabled: true }, convertRemaining: 0 }) });
  await expectCode(instance.createSession({ visitorId: instance.exactVisitorId, origin: "https://rc.example" }), "COORDINATE_THREE_CASE_RC_BROWSER_PERMISSION_REJECTED");
});

await test("rejects AMap provider", async () => {
  const { instance } = runtime({ selectedMapProvider: () => "AMAP" });
  await expectCode(instance.createSession({ visitorId: instance.exactVisitorId, origin: "https://rc.example" }), "COORDINATE_THREE_CASE_RC_BROWSER_MAP_PROVIDER_REJECTED");
});

await test("requires active run and deadline", async () => {
  const { instance } = runtime({ readRunStatus: async () => ({ runStatus: "ARMED", runClosed: false }) });
  await expectCode(instance.createSession({ visitorId: instance.exactVisitorId, origin: "https://rc.example" }), "COORDINATE_THREE_CASE_RC_BROWSER_RUN_NOT_ACTIVE");
});

await test("session uses HttpOnly-cookie material and CSRF binding", async () => {
  const { instance } = runtime();
  assert.equal(instance.hasReadySession(), false);
  const session = await instance.createSession({ visitorId: instance.exactVisitorId, origin: "https://rc.example" });
  assert.equal(instance.hasReadySession(), true);
  const auth = { cookieHeader: `${session.cookieName}=${session.cookieValue}`, csrfToken: session.csrfToken, origin: "https://rc.example" };
  assert.equal(instance.status(auth).ready, false);
  assert.throws(() => instance.status({ ...auth, csrfToken: "wrong" }), error => error?.code === "COORDINATE_THREE_CASE_RC_BROWSER_CSRF_REJECTED");
});

await test("register rejects failed or non-technical job", async () => {
  const { instance } = runtime({ readJob: () => ({ status: "FAILED", result: { success: false } }) });
  await expectCode(instance.register({ caseId: "indonesia", jobId: "job", jobAccessToken: "token", recognitionRequestId: "11111111-1111-4111-8111-111111111111" }), "COORDINATE_THREE_CASE_RC_BROWSER_JOB_NOT_CONSUMABLE");
});

await test("register binds terminal job request and identity", async () => {
  const { instance } = runtime();
  const receipt = await instance.register({ caseId: "indonesia", jobId: "job", jobAccessToken: "token", recognitionRequestId: terminalJob.result.requestId });
  assert.equal(receipt.registered, true);
  assert.equal(receipt.caseId, "indonesia");
});

await test("registration cannot be repeated", async () => {
  const { instance } = runtime();
  const input = { caseId: "indonesia", jobId: "job", jobAccessToken: "token", recognitionRequestId: terminalJob.result.requestId };
  await instance.register(input);
  await expectCode(instance.register(input), "COORDINATE_THREE_CASE_RC_BROWSER_HANDOFF_ALREADY_REGISTERED");
});

await test("browser receipt is one time and identity bound", async () => {
  const { instance } = runtime();
  const session = await instance.createSession({ visitorId: instance.exactVisitorId, origin: "https://rc.example" });
  const auth = { cookieHeader: `${session.cookieName}=${session.cookieValue}`, csrfToken: session.csrfToken, origin: "https://rc.example" };
  await instance.register({ caseId: "indonesia", jobId: "job", jobAccessToken: "token", recognitionRequestId: terminalJob.result.requestId });
  assert.equal(instance.status(auth).ready, true);
  const received = instance.receive(auth);
  assert.deepEqual(received.resultIdentity, identity);
  assert.equal(received.result.finalizedCoordinateResult.geometry.type, "Polygon");
  assert.throws(() => instance.receive(auth), error => error?.code === "COORDINATE_THREE_CASE_RC_BROWSER_HANDOFF_UNAVAILABLE");
});

await test("actual job runtime and confirmation registry reach the handoff consumer chain", async () => {
  const { instance, queued, terminal } = await actualJobBackedRuntime();
  assert.equal(terminal.rcBinding.runId, terminalJob.rcBinding.runId);
  assert.equal(Number.isFinite(terminal.expiresAt), true);
  const session = await instance.createSession({ visitorId: instance.exactVisitorId, origin: "https://rc.example" });
  const auth = { cookieHeader: `${session.cookieName}=${session.cookieValue}`, csrfToken: session.csrfToken, origin: "https://rc.example" };
  const registration = await instance.register({
    caseId: "indonesia",
    jobId: queued.jobId,
    jobAccessToken: queued.jobAccessToken,
    recognitionRequestId: terminalJob.result.requestId
  });
  assert.equal(registration.registered, true);
  const received = instance.receive(auth);
  assert.deepEqual(received.resultIdentity, identity);
  const bindingDigest = crypto.createHash("sha256").update([
    received.runId,
    received.caseId,
    received.recognitionRequestId,
    received.resultIdentity.resultId,
    String(received.resultIdentity.resultRevision),
    received.resultIdentity.geometryHash
  ].join("\n")).digest("hex");
  assert.equal(instance.recordConsumerEvidence(auth, {
    schemaVersion: "coordinate_three_case_rc_r7_browser_evidence_v1",
    runId: received.runId,
    caseId: received.caseId,
    bindingDigest,
    resultInstalled: true,
    resultIdentityMatch: true,
    mapState: "READY",
    mapProvider: "OPENFREEMAP",
    mapIdentityMatch: true,
    authorityMutationCount: 0,
    kmlDownloadTriggered: true,
    kmlIdentityMatch: true,
    downloadFileName: "coordinates.kml"
  }).accepted, true);
  assert.equal(instance.readConsumerEvidence().ready, true);
});

await test("actual job runtime rejects a wrong handoff token", async () => {
  const { instance, queued } = await actualJobBackedRuntime();
  await expectCode(instance.register({
    caseId: "indonesia",
    jobId: queued.jobId,
    jobAccessToken: "wrong-token",
    recognitionRequestId: terminalJob.result.requestId
  }), "COORDINATE_THREE_CASE_RC_BROWSER_JOB_NOT_CONSUMABLE");
});

await test("actual job runtime rejects mismatched rcBinding", async () => {
  const { instance, queued } = await actualJobBackedRuntime({ bindingOverrides: { visitorId: "wrong-visitor" } });
  await expectCode(instance.register({
    caseId: "indonesia",
    jobId: queued.jobId,
    jobAccessToken: queued.jobAccessToken,
    recognitionRequestId: terminalJob.result.requestId
  }), "COORDINATE_THREE_CASE_RC_BROWSER_JOB_BINDING_MISMATCH");
});

await test("actual confirmation registry invalidation rejects the handoff", async () => {
  const { instance, queued } = await actualJobBackedRuntime({ clearRegistry: true });
  await expectCode(instance.register({
    caseId: "indonesia",
    jobId: queued.jobId,
    jobAccessToken: queued.jobAccessToken,
    recognitionRequestId: terminalJob.result.requestId
  }), "COORDINATE_THREE_CASE_RC_BROWSER_FINALIZED_IDENTITY_STALE");
});

await test("same-origin browser evidence is recorded only after receive and exact identity digest", async () => {
  const { instance } = runtime();
  const session = await instance.createSession({ visitorId: instance.exactVisitorId, origin: "https://rc.example" });
  const auth = { cookieHeader: `${session.cookieName}=${session.cookieValue}`, csrfToken: session.csrfToken, origin: "https://rc.example" };
  const input = { caseId: "indonesia", jobId: "job", jobAccessToken: "token", recognitionRequestId: terminalJob.result.requestId };
  await instance.register(input);
  const bindingDigest = crypto.createHash("sha256").update([
    terminalJob.rcBinding.runId,
    terminalJob.rcBinding.caseId,
    terminalJob.rcBinding.recognitionRequestId,
    identity.resultId,
    String(identity.resultRevision),
    identity.geometryHash
  ].join("\n")).digest("hex");
  const evidence = {
    schemaVersion: "coordinate_three_case_rc_r7_browser_evidence_v1",
    runId: terminalJob.rcBinding.runId,
    caseId: "indonesia",
    bindingDigest,
    resultInstalled: true,
    resultIdentityMatch: true,
    mapState: "READY",
    mapProvider: "OPENFREEMAP",
    mapIdentityMatch: true,
    authorityMutationCount: 0,
    kmlDownloadTriggered: true,
    kmlIdentityMatch: true,
    downloadFileName: "coordinates.kml"
  };
  assert.throws(() => instance.recordConsumerEvidence(auth, evidence), error => error?.code === "COORDINATE_THREE_CASE_RC_BROWSER_HANDOFF_UNAVAILABLE");
  instance.receive(auth);
  assert.throws(() => instance.recordConsumerEvidence(auth, { ...evidence, bindingDigest: "0".repeat(64) }), error => error?.code === "COORDINATE_THREE_CASE_RC_BROWSER_CONSUMER_EVIDENCE_REJECTED");
  assert.equal(instance.recordConsumerEvidence(auth, evidence).accepted, true);
  const stored = instance.readConsumerEvidence();
  assert.equal(stored.ready, true);
  assert.deepEqual(stored.evidence, evidence);
  assert.throws(() => instance.recordConsumerEvidence(auth, evidence), error => error?.code === "COORDINATE_THREE_CASE_RC_BROWSER_CONSUMER_EVIDENCE_ALREADY_RECORDED");
});

await test("expired handoff fails closed", async () => {
  const { instance, advance } = runtime();
  const session = await instance.createSession({ visitorId: instance.exactVisitorId, origin: "https://rc.example" });
  const auth = { cookieHeader: `${session.cookieName}=${session.cookieValue}`, csrfToken: session.csrfToken, origin: "https://rc.example" };
  await instance.register({ caseId: "indonesia", jobId: "job", jobAccessToken: "token", recognitionRequestId: terminalJob.result.requestId });
  advance((8 * 60 * 1000) + 1);
  assert.throws(() => instance.receive(auth), error => error?.code === "COORDINATE_THREE_CASE_RC_BROWSER_HANDOFF_UNAVAILABLE");
});

await test("finalizer registry deadline is an actual earliest handoff deadline", async () => {
  const registryDeadline = Date.parse("2026-10-11T08:01:00Z");
  const { instance, advance } = runtime({
    validateFinalizedIdentity: () => ({
      ok: true,
      result: terminalJob.result.finalizedCoordinateResult,
      expiresAt: registryDeadline
    })
  });
  const session = await instance.createSession({ visitorId: instance.exactVisitorId, origin: "https://rc.example" });
  const auth = { cookieHeader: `${session.cookieName}=${session.cookieValue}`, csrfToken: session.csrfToken, origin: "https://rc.example" };
  const receipt = await instance.register({ caseId: "indonesia", jobId: "job", jobAccessToken: "token", recognitionRequestId: terminalJob.result.requestId });
  assert.equal(Date.parse(receipt.expiresAt), registryDeadline);
  advance((60 * 1000) + 1);
  assert.throws(() => instance.receive(auth), error => error?.code === "COORDINATE_THREE_CASE_RC_BROWSER_HANDOFF_UNAVAILABLE");
});

await test("missing finalizer registry deadline fails before handoff registration", async () => {
  const { instance } = runtime({
    validateFinalizedIdentity: () => ({ ok: true, result: terminalJob.result.finalizedCoordinateResult })
  });
  await expectCode(
    instance.register({ caseId: "indonesia", jobId: "job", jobAccessToken: "token", recognitionRequestId: terminalJob.result.requestId }),
    "COORDINATE_THREE_CASE_RC_BROWSER_FINALIZED_IDENTITY_DEADLINE_UNAVAILABLE"
  );
});

await test("revoke invalidates session and handoff", async () => {
  const { instance } = runtime();
  const session = await instance.createSession({ visitorId: instance.exactVisitorId, origin: "https://rc.example" });
  const auth = { cookieHeader: `${session.cookieName}=${session.cookieValue}`, csrfToken: session.csrfToken, origin: "https://rc.example" };
  instance.revoke();
  assert.equal(instance.hasReadySession(), false);
  assert.throws(() => instance.status(auth), error => error?.code === "COORDINATE_THREE_CASE_RC_BROWSER_SESSION_REQUIRED");
});

await test("browser result is an explicit whitelist", async () => {
  const secretBearingJob = structuredClone(terminalJob);
  secretBearingJob.result.rawProviderResponse = "must-not-cross";
  secretBearingJob.result.claimToken = "must-not-cross";
  secretBearingJob.result.finalizedCoordinateResult.coordinateRaw = "must-not-cross";
  const { instance } = runtime({ readJob: () => secretBearingJob });
  const session = await instance.createSession({ visitorId: instance.exactVisitorId, origin: "https://rc.example" });
  const auth = { cookieHeader: `${session.cookieName}=${session.cookieValue}`, csrfToken: session.csrfToken, origin: "https://rc.example" };
  await instance.register({ caseId: "indonesia", jobId: "job", jobAccessToken: "token", recognitionRequestId: terminalJob.result.requestId });
  const serialized = JSON.stringify(instance.receive(auth));
  assert.doesNotMatch(serialized, /must-not-cross/u);
  assert.doesNotMatch(serialized, /rawProviderResponse|claimToken|coordinateRaw/u);
});

const index = fs.readFileSync(path.join(root, "index.html"), "utf8");
const server = fs.readFileSync(path.join(root, "server.js"), "utf8");

async function r8UploadedFixture({ beforeRelease = false, wrongImage = false, failClaim = false } = {}) {
  const counters = { claims: 0, dispatches: 0, settlements: 0, uploads: 0, usageSessions: 0, close: 0 };
  const C = COORDINATE_THREE_CASE_RC;
  const env = { COORDINATE_THREE_CASE_RC_ENABLED: "true", COORDINATE_THREE_CASE_RC_RUN_ID: C.runId,
    COORDINATE_THREE_CASE_RC_WINDOW_SECONDS: "900", COORDINATE_THREE_CASE_RC_CLAIM_TTL_SECONDS: "600",
    RC_SUPABASE_PROJECT_REF: C.rcProjectRef, RENDER_SERVICE_NAME: C.rcServiceName,
    SUPABASE_URL: "https://" + C.rcProjectRef + ".supabase.co", ALIYUN_VISION_MODEL: C.model };
  const deadline = new Date(Date.now() + 14 * 60_000).toISOString();
  const ledger = { rpc: async (name) => {
    const actions = { activate_coordinate_three_case_rc_run: "ACTIVATED", claim_coordinate_three_case_rc: "CLAIMED",
      dispatch_coordinate_three_case_rc: "DISPATCHED", settle_coordinate_three_case_rc: "SETTLED", close_coordinate_three_case_rc_run: "CLOSED" };
    if (name === "claim_coordinate_three_case_rc") { counters.claims++; if (failClaim) return { data: { accepted: false, code: "OFFLINE_CLAIM_DENIED" } }; }
    if (name === "dispatch_coordinate_three_case_rc") counters.dispatches++;
    if (name === "settle_coordinate_three_case_rc") counters.settlements++;
    if (name === "close_coordinate_three_case_rc_run") counters.close++;
    if (name === "read_coordinate_three_case_rc_status") return { data: { run_status: "ACTIVE", run_closed: false, deadline_at: deadline } };
    assert.ok(actions[name], "unexpected RPC boundary");
    return { data: { accepted: true, action: actions[name] } };
  } };
  const admission = createCoordinateThreeCaseRcAdmission({ supabase: ledger, adminPassword: "offline-only", env });
  await admission.activateOnStartup();
  const registry = new CoordinateConfirmationRuntime();
  const result = structuredClone(terminalJob.result);
  result.providerModel = C.model; result.model = C.model; result.providerCallCount = 1;
  result.finalizedCoordinateResult.schemaVersion = FINALIZED_COORDINATE_SCHEMA_VERSION;
  result.finalizedCoordinateResult.crs = FINALIZED_COORDINATE_CRS;
  result.finalizedCoordinateResult.geometryHash = createGeometryHash(result.finalizedCoordinateResult.geometry);
  result.finalizedCoordinateResult.mapReady = true;
  result.finalizedCoordinateResult.requiresReview = true;
  result.finalizedCoordinateResult.confirmationStatus = "pending";
  result.finalizedCoordinateResult.resultId = "result-r8-offline";
  result.outputCapabilities = { ...result.finalizedCoordinateResult, technicallyGeneratable: true, mapReady: true, kmlReady: true };
  let submitted = null;
  const forwarder = createCoordinateThreeCaseRcJobForwarder({
    getPort: () => 0, internalToken: "offline-internal", coordinateThreeCaseRcAdmission: admission,
    indonesiaStructuredBRcAdmission: { config: { ready: false } },
    fetchImpl: async (url, options) => {
      assert.equal(url, "http://127.0.0.1:0/api/internal/recognize-coordinates-long");
      const buffer = Buffer.from(await options.body.get("image").arrayBuffer());
      const requestId = options.headers["x-recognition-request-id"];
      const budget = { requestId, providerAttemptCount: 0 };
      admission.bindBudget({ budget, caseId: "indonesia", productMode: C.cases.indonesia.productMode,
        recognitionRequestId: requestId, imageBuffer: buffer, authorization: options.headers.authorization,
        visitorId: options.headers["x-visitor-id"] });
      assert.equal(admission.getControlledModelCrsAcquisitionAuthorization({ budget, caseId: "indonesia",
        productMode: C.cases.indonesia.productMode }).authorized, true);
      await admission.dispatchForBudget({ budget, modelName: C.model, maxTokens: 8000, enableThinking: false });
      budget.providerAttemptCount = 1;
      result.requestId = requestId;
      registry.register(result.finalizedCoordinateResult);
      admission.recordProviderResultForBudget({ budget, completionState: "SUCCEEDED", usage: null });
      await admission.settleForBudget({ budget, httpStatus: 200, body: result });
      return { ok: true, status: 200, json: async () => result };
    }
  });
  const jobs = createRecognitionAcquisitionJobRuntime({ execute: forwarder });
  const instance = createCoordinateThreeCaseRcBrowserHandoff({
    runId: C.runId, batchId: C.batchId, expectedOrigin: "https://rc.example", caseDefinitions: C.cases,
    readRunStatus: () => admission.status({ adminHeader: "offline-only" }),
    readExistingUserAccess: async () => ({ existingUser: true, permissions: { aiOcrEnabled: true, kmlExportEnabled: true }, convertRemaining: 1 }),
    selectedMapProvider: () => "OPENFREEMAP",
    claimUpload: input => admission.claim({ ...input, adminHeader: "offline-only", runId: C.runId, batchId: C.batchId }),
    readJob: (id, access) => jobs.getForHandoff(id, access),
    validateFinalizedIdentity: candidate => ({ ...registry.validateIdentity(candidate),
      expiresAt: registry.records.get(candidate.resultId)?.expiresAt })
  });
  const session = await instance.createSession({ visitorId: instance.exactVisitorId, origin: "https://rc.example" });
  const auth = { cookieHeader: session.cookieName + "=" + session.cookieValue, csrfToken: session.csrfToken, origin: "https://rc.example" };
  if (!beforeRelease) await instance.releaseUpload({ expiresAt: deadline });
  const handlers = [];
  const ctx = vm.createContext({ Buffer, crypto, coordinateThreeCaseRcAdmission: admission,
    coordinateThreeCaseRcBrowserHandoff: instance, recognitionAcquisitionJobRuntime: jobs,
    indonesiaStructuredBRcAdmission: { config: { ready: false } }, INDONESIA_STRUCTURED_B_RC: { mode: C.cases.indonesia.productMode },
    canonicalizeCoordinateImageUpload, validateCoordinateImageUpload,
    getUploadedFileDisplayName: file => file.originalname, getRegressionTestMode: () => ({ active: false }),
    isRecognitionRequestId: value => /^[a-f0-9-]{36}$/u.test(value),
    createCoordinateThreeCaseRcJobIngressMiddleware, createCoordinateThreeCaseRcJobPreflightMiddleware,
    upload: { single: () => (req, res, next) => next() },
    app: { post: (route, ...middleware) => handlers.push(...middleware) }
  });
  for (const name of ["coordinateThreeCaseRcBrowserAuth", "coordinateThreeCaseRcBrowserError", "admitCoordinateRcBrowserUpload"]) {
    vm.runInContext(extractFunction(server, name), ctx);
  }
  const normalizedServer = server.replaceAll("\r\n", "\n");
  const start = normalizedServer.indexOf('app.post(\n  "/api/recognize-coordinates/jobs",');
  const end = normalizedServer.indexOf('\napp.get("/api/recognize-coordinates/jobs/:jobId"', start);
  assert.ok(start > 0 && end > start);
  vm.runInContext(normalizedServer.slice(start, end), ctx);
  const imagePath = "C:/Users/Mir-1/Documents/Codex/2026-04-26/index-html-1-2-kml-3/fixtures/utm/indonesia_utm50s_real_003.jpg";
  const image = fs.readFileSync(imagePath);
  const file = new Blob([wrongImage ? Buffer.from("wrong") : image], { type: "image/jpeg" });
  const ui = vm.createContext({ crypto: crypto.webcrypto, FormData, AbortSignal, Blob, console,
    imageInput: { files: [file], disabled: false }, coordinateThreeCaseRcBrowserStatus: { textContent: "" },
    coordinateThreeCaseRcCsrfToken: session.csrfToken, coordinateThreeCaseRcBrowserMode: true,
    coordinateConsumerReplayBrowserMode: false, COORDINATE_THREE_CASE_RC_R7_VISITOR_ID: instance.exactVisitorId,
    coordinateR8UploadAllowed: true, coordinateR8UploadAttempted: false, coordinateR8Stopped: false,
    coordinateR8RequestId: "", createRecognitionRequestId: () => crypto.randomUUID(),
    prepareCoordinateUsageSession: async visitor => { assert.equal(visitor, instance.exactVisitorId); counters.usageSessions++; },
    failCoordinateR8: async () => { ui.coordinateR8Stopped = true; },
    fetch: async (url, options) => {
      assert.equal(url, "/api/recognize-coordinates/jobs");
      counters.uploads++;
      const headers = { ...options.headers, cookie: auth.cookieHeader, origin: auth.origin };
      const req = { headers, query: {}, body: { visitorId: options.body.get("visitorId") },
        file: { buffer: Buffer.from(await options.body.get("image").arrayBuffer()), mimetype: "image/jpeg", originalname: "image.jpg", size: image.length },
        get(name) { return this.headers[name.toLowerCase()] || ""; } };
      const res = { statusCode: 200, status(code) { this.statusCode=code; return this; },
        setHeader() {}, json(value) { this.payload=value; return value; } };
      let i=0;
      const next = async () => { if (i < handlers.length) return handlers[i++](req,res,next); };
      await next(); submitted = res;
      return { status: res.statusCode, ok: res.statusCode<300, json: async () => res.payload };
    }
  });
  vm.runInContext(extractFunction(index, "controlledConsumerCsrfHeaders") + "\n" + extractFunction(index, "uploadCoordinateR8Image"), ui);
  await ui.uploadCoordinateR8Image();
  if (beforeRelease || wrongImage || failClaim) return { instance, auth, counters, ui, submitted };
  assert.equal(submitted?.statusCode, 202, JSON.stringify(submitted?.payload));
  assert.equal(JSON.stringify(submitted.payload).includes("Token"), false);
  for (let i=0; i<30; i++) {
    await new Promise(resolve => setImmediate(resolve));
    const snapshot=await instance.readFreshResult();
    if (snapshot.ready) break;
    assert.notEqual(snapshot.stage, "FAILED", snapshot.code);
  }
  assert.equal((await instance.status(auth)).ready, true);
  await ui.uploadCoordinateR8Image();
  assert.equal(counters.uploads, 1);
  return { instance, auth, counters, ui, admission, registry, expectedIdentity: result.finalizedCoordinateResult };
}

await test(r8Incremental ? "R8 browser upload real jobs ingress forwarder admission registry product consumers" : "one received registry result dynamically reaches the real install Map and KML consumer functions", async () => {
  const fixture = r8Incremental ? await r8UploadedFixture() : runtime();
  const { instance } = fixture;
  const session = r8Incremental ? null : await instance.createSession({ visitorId: instance.exactVisitorId, origin: "https://rc.example" });
  const auth = r8Incremental ? fixture.auth : { cookieHeader: `${session.cookieName}=${session.cookieValue}`, csrfToken: session.csrfToken, origin: "https://rc.example" };
  if (!r8Incremental) await instance.register({ caseId: "indonesia", jobId: "job", jobAccessToken: "token", recognitionRequestId: terminalJob.result.requestId });
  const received = instance.receive(auth);

  const state = {
    mapOpened: false,
    anchorClicked: false,
    blobText: "",
    mapFetchIdentityMatch: false,
    consumerEvidencePosted: false
  };
  class CapturedBlob {
    constructor(parts) { this.parts = parts; state.blobText = parts.join(""); }
  }
  const context = vm.createContext({
    console,
    structuredClone,
    crypto: crypto.webcrypto,
    TextEncoder,
    Blob: CapturedBlob,
    URL: { createObjectURL: () => "blob:r7", revokeObjectURL: () => {} },
    document: {
      body: { appendChild: () => {}, removeChild: () => {} },
      createElement: () => ({ href: "", download: "", click: () => { state.anchorClicked = true; } })
    },
    recordConsumerEvidence: evidence => instance.recordConsumerEvidence(auth, evidence),
    window: { matchMedia: () => ({ matches: false }) },
    setTimeout,
    clearTimeout
  });
  if (r8Incremental) {
    const mapContext = vm.createContext({
      coordinateConsumerReplay: { inspectIdentity: () => ({ matched: false }) },
      mapPreviewAdapter: new MapPreviewAdapter(), calculateSpatialFacts, evaluateRecognitionOutputCapability,
      validateFinalizedCoordinateIdentity: identity => fixture.registry.validateIdentity(identity)
    });
    const mapRoute = r8Route(server, "/api/map-preview", mapContext);
    context.actualMapRoute = async body => {
      const res = { statusCode: 200, status(code) { this.statusCode=code; return this; }, json(payload) {this.payload=payload;} };
      await mapRoute({body}, res);
      assert.equal(res.statusCode, 200, JSON.stringify(res.payload));
      assert.equal(res.payload.mapPreviewObject.geometry.type, "Polygon");
      return {ok: true, json: async () => res.payload};
    };
  }
  const actualFunctions = [
    "hasFiniteFinalizedGeometry", "getFinalizedCoordinateIdentity", "cloneIdentityBoundValue",
    "deepFreezeIdentityBoundValue", "canonicalIdentityBoundSerialization", "identityBoundValuesEqual",
    "finalizedResultMatchesIdentityBoundSnapshot", "createIdentityBoundSnapshot",
    "rememberLastValidIdentityBoundSnapshot", "hasCompleteUnifiedRecognitionEvidence",
    "createRecognitionAuthorizationState", "installCoordinateRecognitionWorkspaceResult",
    "receiveCoordinateThreeCaseRcBrowserResult", "publishCoordinateThreeCaseRcBrowserConsumerEvidence",
    "controlledConsumerPath", "controlledConsumerCsrfHeaders", "recordCoordinateConsumerReplayStage",
    "isActiveCoordinateConsumerReplayIdentity", "isActiveControlledCoordinateConsumerIdentity",
    "isActiveCoordinateThreeCaseRcR7Identity", "activeCoordinateThreeCaseRcR7HasUnsupportedInnerRings",
    "getFinalizedGeometryCoordinateSource", "getAuthorizedFinalizedGeometryKmlSource",
    "coordinatesAreSame", "getClosedPolygonPairs", "escapeXml", "formatKmlCoordinates",
    "buildPointKml", "buildLineStringKml", "buildPolygonKml", "openSpatialResult", "downloadKmlInternal"
  ].map(name => extractFunction(index, name)).join("\n");
  const setup = `
    const COORDINATE_THREE_CASE_RC_R7_RUN_ID = ${JSON.stringify(received.runId)};
    const COORDINATE_THREE_CASE_RC_R7_VISITOR_ID = "rc-b-release-20261010-4f125a9";
    let coordinateThreeCaseRcBrowserMode = true;
    let coordinateConsumerReplayBrowserMode = false;
    let controlledCoordinateConsumerBrowserMode = true;
    let coordinateR8Stopped = false;
    let coordinateR8RequestId = ${JSON.stringify(received.recognitionRequestId)};
    const failCoordinateR8 = async () => { coordinateR8Stopped = true; };
    let coordinateThreeCaseRcCsrfToken = "csrf-r7";
    let activeFinalizedCoordinateResult = null;
    let activeCoordinateEngineV2 = null;
    let activeCoordinatePrecisionMode = "";
    let activeCoordinateFieldConflictCount = 0;
    let activeRecognitionAcquisitionResult = null;
    let activePointGeometryIntentReview = null;
    let activeCoordinateThreeCaseRcHandoff = null;
    let lastValidIdentityBoundSnapshot = null;
    let finalizedCoordinateTextSnapshot = "";
    let finalizedCoordinateDirty = false;
    let activeMapPreviewResponse = null;
    let activeMapPreviewCacheKey = "";
    let activeProjectedBoundaryBlocked = false;
    let agenticCoordinateController = null;
    let spatialResultEnabled = true;
    const input = { value: "" };
    const coordinateOrder = { value: "auto" };
    const coordinateUploadPanel = { classList: { add() {} } };
    const coordinateThreeCaseRcBrowserPanel = { dataset: {} };
    const coordinateThreeCaseRcReceive = { disabled: false };
    const coordinateThreeCaseRcBrowserStatus = { textContent: "" };
    const mapPreviewAction = { disabled: false };
    const pages = { spatialResult: { dataset: {}, classList: { contains: () => false } } };
    const spatialMapFailure = { hidden: true };
    const spatialResultDiagnostic = { hidden: true, textContent: "" };
    const invalidateSpatialPreview = () => { activeMapPreviewResponse = null; activeMapPreviewCacheKey = ""; };
    const rememberState = () => {};
    const compactRecognizedCoordinateDisplayText = value => String(value || "");
    const getCanonicalCoordinateDisplayText = () => "redacted-fixed-test-input";
    const clearInternalKmlSource = () => {};
    const syncCoordinateReviewConfirmationState = () => {};
    const renderProjectedCrsReviewPanel = () => {};
    const updateCadastralGridPanel = () => {};
    const refreshMapPreviewAction = () => {};
    const stopBlockedCoordinateAction = () => false;
    const shouldUseProvisionalCoordinateResult = () => false;
    const projectedCrsSelectionNeedsConfirmation = () => false;
    const ensureManualInputFinalized = async () => {};
    const getSpatialPreviewCacheKey = (identity = getFinalizedCoordinateIdentity()) => identity
      ? identity.resultId + ":" + identity.resultRevision + ":" + identity.geometryHash : "";
    const fetch = async (url, options) => {
      if (url.endsWith("/browser-handoff/status")) {
        return { ok: true, json: async () => ({ ready: true, caseId: "indonesia" }) };
      }
      if (url.endsWith("/browser-handoff/receive")) {
        return { ok: true, json: async () => structuredClone(state.received) };
      }
      if (url.endsWith("/browser-handoff/consumer-evidence")) {
        const evidence = JSON.parse(options.body);
        const receipt = recordConsumerEvidence(evidence);
        state.consumerEvidencePosted = receipt.accepted === true;
        return { ok: state.consumerEvidencePosted, json: async () => receipt };
      }
      if (url !== "/api/map-preview" || options?.method !== "POST") throw new Error("UNEXPECTED_MAP_BOUNDARY");
      const body = JSON.parse(options.body);
      const identity = getFinalizedCoordinateIdentity();
      state.mapFetchIdentityMatch = body.resultId === identity.resultId
        && body.resultRevision === identity.resultRevision && body.geometryHash === identity.geometryHash;
      if (typeof actualMapRoute === "function") return actualMapRoute(body);
      return { ok: true, json: async () => ({ mapPreviewObject: {
        previewEligibility: { allowed: true }, sourceResultId: identity.resultId,
        sourceRevision: identity.resultRevision, sourceGeometryHash: identity.geometryHash
      } }) };
    };
    const getSourceHeaders = value => value;
    const showPage = () => {};
    const renderSpatialResult = () => {};
    const setSpatialSheetExpanded = () => {};
    const waitForSpatialLayout = async () => {};
    const syncSpatialMapViewport = async () => {};
    const showMessage = () => {};
    const setRecognitionStatus = () => {};
    globalThis.GeoKitSatelliteMap = { open: async () => {
      const identity = getFinalizedCoordinateIdentity(); state.mapOpened = true;
      return { state: "READY", authorityMutationCount: 0, renderReceipt: {
        provider: "OPENFREEMAP", sourceResultId: identity.resultId,
        sourceRevision: identity.resultRevision, sourceGeometryHash: identity.geometryHash,
        authorityMutationCount: 0
      } };
    } };
    const loadUserConfig = async () => {};
    const canUse = () => true;
    const showKmlPermissionFailure = () => {};
    const focusProjectedCrsReviewPanel = () => {};
    const showKmlFailure = () => {};
    const shouldBlockFinalizedCoordinateKml = () => false;
    const showRecognitionProgress = () => {};
    const consumeUsage = async () => { throw new Error("UNEXPECTED_SECOND_USAGE"); };
    const trackEvent = () => {};
    const showKmlCompletion = () => {};
    const hasCadastralGridRows = () => false;
    const hasMgrsRows = () => false;
    const getKmlCoordinateGroups = () => [];
    const downloadProvisionalCoordinateKml = () => { throw new Error("UNEXPECTED_PROVISIONAL_KML"); };
    const downloadAgenticCoordinateKml = () => { throw new Error("UNEXPECTED_AGENTIC_KML"); };
    ${actualFunctions}
    globalThis.__r7Product = {
      receive: receiveCoordinateThreeCaseRcBrowserResult,
      openMap: openSpatialResult,
      downloadKml: downloadKmlInternal,
      evidence: () => ({
        active: isActiveCoordinateThreeCaseRcR7Identity(),
        snapshotPresent: Boolean(lastValidIdentityBoundSnapshot),
        mapReady: activeCoordinateThreeCaseRcHandoff?.mapReady === true,
        kmlDownloadTriggered: activeCoordinateThreeCaseRcHandoff?.kmlDownloadTriggered === true,
        identity: getFinalizedCoordinateIdentity(),
        statusText: coordinateThreeCaseRcBrowserStatus.textContent,
        receiveDisabled: coordinateThreeCaseRcReceive.disabled
      })
    };
  `;
  context.state = state;
  state.received = structuredClone(received);
  new vm.Script(setup).runInContext(context);
  await context.__r7Product.receive();
  await context.__r7Product.openMap();
  assert.equal(context.__r7Product.evidence().active, true, JSON.stringify(context.__r7Product.evidence()));
  await context.__r7Product.downloadKml();
  const evidence = context.__r7Product.evidence();
  assert.equal(evidence.active, true);
  assert.equal(evidence.snapshotPresent, true);
  assert.equal(state.mapFetchIdentityMatch, true);
  assert.equal(state.mapOpened, true);
  assert.equal(evidence.mapReady, true);
  assert.equal(state.anchorClicked, true);
  assert.equal(evidence.kmlDownloadTriggered, true);
  assert.equal(state.consumerEvidencePosted, true);
  assert.match(state.blobText, /REVIEW_REQUIRED \/ UNVERIFIED/u);
  assert.match(state.blobText, /<Polygon>/u);
  assert.equal(JSON.stringify(evidence.identity), JSON.stringify(received.resultIdentity));
  const storedConsumerEvidence = instance.readConsumerEvidence();
  assert.equal(storedConsumerEvidence.ready, true);
  assert.match(storedConsumerEvidence.evidence.bindingDigest, /^[a-f0-9]{64}$/u);

  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "r7-dynamic-consumer-"));
  try {
    const serverEvidencePath = path.join(tempRoot, "server-evidence.json");
    const evidencePath = path.join(tempRoot, "browser.json");
    const resultPath = path.join(tempRoot, "validated.json");
    const downloadPath = path.join(tempRoot, "coordinates.kml");
    fs.writeFileSync(serverEvidencePath, JSON.stringify(storedConsumerEvidence.evidence));
    fs.writeFileSync(resultPath, JSON.stringify({ cases: [{
      caseId: "indonesia",
      recognitionRequestId: received.recognitionRequestId,
      finalizedResult: received.result.finalizedCoordinateResult
    }] }));
    const psQuote = value => `'${String(value).replaceAll("'", "''")}'`;
    const preflightPath = path.join(root, "scripts", "coordinate-three-case-rc-client-preflight.ps1");
    const writerCommand = [
      `. ${psQuote(preflightPath)}`,
      `$e = Get-Content -LiteralPath ${psQuote(serverEvidencePath)} -Raw | ConvertFrom-Json`,
      `Write-CoordinateThreeCaseBrowserConsumerEvidenceArtifact -Evidence $e -Path ${psQuote(evidencePath)} -ExpectedRunId ${psQuote(received.runId)} -ExpectedCaseId 'indonesia' | ConvertTo-Json -Compress`
    ].join("; ");
    const writer = spawn("pwsh", ["-NoProfile", "-Command", writerCommand], { stdio: ["ignore", "pipe", "pipe"] });
    let writerStdout = "";
    let writerStderr = "";
    writer.stdout.on("data", value => { writerStdout += value; });
    writer.stderr.on("data", value => { writerStderr += value; });
    const writerExitCode = await new Promise((resolve, reject) => {
      writer.once("error", reject);
      writer.once("close", resolve);
    });
    assert.equal(writerStderr, "");
    assert.equal(writerExitCode, 0, writerStdout);
    assert.equal(fs.existsSync(evidencePath), true);
    assert.equal(
      JSON.parse(fs.readFileSync(evidencePath, "utf8")).bindingDigest,
      storedConsumerEvidence.evidence.bindingDigest
    );
    fs.writeFileSync(downloadPath, state.blobText);
    const gatePath = path.join(root, "scripts", "coordinate-three-case-rc-r7-browser-consumer-gate.ps1");
    const gate = spawn("pwsh", ["-NoProfile", "-File", gatePath,
      "-BrowserEvidencePath", evidencePath,
      "-ValidatedResultPath", resultPath,
      "-KmlDownloadPath", downloadPath,
      "-ExpectedRunId", received.runId,
      "-ExpectedCaseId", "indonesia",
      "-HandoffExpiresAt", new Date(Date.now() + 30_000).toISOString(),
      "-TimeoutSeconds", "10"], { stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    gate.stdout.on("data", value => { stdout += value; });
    gate.stderr.on("data", value => { stderr += value; });
    const exitCode = await new Promise((resolve, reject) => {
      gate.once("error", reject);
      gate.once("close", resolve);
    });
    assert.equal(stderr, "");
    assert.equal(exitCode, 0, stdout);
    const gateReceipt = JSON.parse(stdout.trim().split(/\r?\n/u).at(-1));
    assert.equal(gateReceipt.success, true);
    assert.equal(gateReceipt.kmlGeometryType, "Polygon");
    assert.equal(gateReceipt.kmlGeometryMatch, true);
    if (r8Incremental) {
      assert.deepEqual(fixture.counters, { claims: 1, dispatches: 1, settlements: 1, uploads: 1, usageSessions: 1, close: 0 });
      await fixture.admission.close({ adminHeader: "offline-only" });
      instance.revoke();
      assert.equal(instance.hasReadySession(), false);
      assert.equal(fixture.counters.close, 1);
    }
  } finally {
    fs.rmSync(tempRoot, { recursive: true, force: true });
  }
});
await test("product binding uses shared install path", async () => {
  assert.match(index, /function installCoordinateRecognitionWorkspaceResult/u);
  assert.equal((index.match(/installCoordinateRecognitionWorkspaceResult\(\{/gu) || []).length >= 2, true);
});

await test("review result bypass is exact-R7 identity only", async () => {
  assert.match(index, /if \(isActiveCoordinateThreeCaseRcR7Identity\(\)\) return false;/u);
  assert.match(index, /technicalKmlReady === true/u);
});

await test("R7 map requires OpenFreeMap READY and exact receipt", async () => {
  assert.match(index, /mapResult\.state !== "READY"/u);
  assert.match(index, /mapResult\.renderReceipt\.provider === "OPENFREEMAP"/u);
  assert.match(index, /mapResult\.renderReceipt\.authorityMutationCount === 0/u);
  assert.match(server, /function getConfiguredSpatialMapProvider\(\)/u);
  assert.match(server, /return webJsKey && securityProxyReady \? "AMAP" : "OPENFREEMAP";/u);
  assert.match(server, /selectedMapProvider: getConfiguredSpatialMapProvider/u);
  assert.doesNotMatch(server, /controlledR7OpenFreeMap|coordinate-three-case-rc"\) === "r7"/u);
});

await test("inner rings fail closed before KML usage", async () => {
  const ringGate = index.indexOf("activeCoordinateThreeCaseRcR7HasUnsupportedInnerRings()", index.indexOf("async function downloadKmlInternal"));
  const consume = index.indexOf('consumeUsage("convert")', ringGate);
  assert.ok(ringGate > 0 && consume > ringGate);
  assert.match(index, /KML_INNER_RING_PRODUCT_PATH_UNSUPPORTED/u);
});

await test("client must prove an authenticated browser session before any model case", async () => {
  const client = fs.readFileSync(path.join(root, "scripts", "coordinate-three-case-rc-client.ps1"), "utf8");
  const readiness = client.indexOf("/browser-readiness");
  const invocation = client.indexOf("return Invoke-RecognitionJob");
  assert.ok(readiness > 0 && invocation > readiness);
  assert.match(client, /R7_BROWSER_SESSION_NOT_READY/u);
  assert.match(client, /-HandoffExpiresAt \$handoffExpiresAt/u);
  assert.match(server, /browserSessionReady: coordinateThreeCaseRcBrowserHandoff\.hasReadySession\(\) === true/u);
});

await test("R7 usage and unverified KML rules stay exact to the R7 identity", async () => {
  assert.match(index, /recognitionUsageConsumed === null/u);
  assert.match(index, /recognitionUsageConsumed === false/u);
  assert.match(index, /REVIEW_REQUIRED \/ UNVERIFIED/u);
  assert.match(index, /requireKmlReady: !isActiveCoordinateThreeCaseRcR7Identity\(\)/u);
});

await test("R8 no release means no claim dispatch or model", async () => {
  const f = await r8UploadedFixture({ beforeRelease: true });
  assert.equal(f.submitted.statusCode, 409);
  assert.equal(f.counters.claims, 0); assert.equal(f.counters.dispatches, 0);
});
await test("R8 wrong image stops before usage session and upload", async () => {
  const f = await r8UploadedFixture({ wrongImage: true });
  assert.equal(f.counters.usageSessions, 0); assert.equal(f.counters.uploads, 0);
});
await test("R8 failed admission remains consumed and cannot retry", async () => {
  const f = await r8UploadedFixture({ failClaim: true });
  assert.equal(f.counters.claims, 1); assert.equal(f.counters.dispatches, 0);
  await expectCode(f.instance.status(f.auth), "RC_R8_UPLOAD_ADMISSION_FAILED");
  await f.ui.uploadCoordinateR8Image(); assert.equal(f.counters.claims, 1);
});
await test("R8 manifest and atomic visitor grant keep historical pool intact", () => {
  const bytes=fs.readFileSync(path.join(root,"docs/coordinate-three-case-rc-manifest.json"));
  const manifest=JSON.parse(bytes);
  assert.equal(crypto.createHash("sha256").update(bytes).digest("hex"), COORDINATE_THREE_CASE_RC.manifestSha256);
  assert.deepEqual(Object.keys(COORDINATE_THREE_CASE_RC.cases), ["indonesia"]);
  assert.equal(manifest.runWindowSeconds, 900);
  assert.equal(manifest.initializationFreeConvertGrant, 1);
  const sql=fs.readFileSync(path.join(root,"scripts/coordinate-three-case-rc-r8-init.sql"),"utf8");
  assert.match(sql,/begin;/u); assert.match(sql,/commit;/u);
  assert.match(sql,/free_convert_count=1/u); assert.match(sql,/free_convert_count=0 and paid_convert_count=0/u);
  assert.match(sql,/dispatched_attempts is distinct from 4/u); assert.match(sql,/reserved_cost_cny is distinct from 3\.264000/u);
  assert.doesNotMatch(sql,/update private\.indonesia_structured_b_batches|insert into private\.indonesia_structured_b_batches/iu);
});
await test("R8 no local OCR is selected only by bound model authorization", () => {
  const selected=server.slice(server.indexOf("const r8WholeImageOnly ="),server.indexOf("oneShotLocalOcrAttempted = oneShotFamilyClassification"));
  assert.match(selected,/getControlledModelCrsAcquisitionAuthorization/u);
  assert.match(selected,/attempted: false/u);
  assert.match(selected,/: await runLocalOcrFamilyClassification/u);
  assert.match(server,/structuredImageItems = indonesiaStructuredAnyRcSelected/u);
  assert.doesNotMatch(fs.readFileSync(path.join(root,"scripts/coordinate-three-case-rc-client.ps1"),"utf8"),/HistoricalArtifactPath|Invoke-RecognitionJob/u);
});
await test("R8 async authentication release and registration cannot revive a stopped run", async () => {
  const deferred = () => { let resolve; const promise = new Promise(r => {resolve=r;}); return {promise, resolve}; };
  const access = deferred();
  const f = runtime({readExistingUserAccess: () => access.promise});
  const first = f.instance.createSession({visitorId:f.instance.exactVisitorId,origin:"https://rc.example"});
  await expectCode(f.instance.createSession({visitorId:f.instance.exactVisitorId,origin:"https://rc.example"}), "RC_R8_BROWSER_AUTH_ALREADY_USED");
  f.instance.revoke();
  access.resolve({existingUser:true,permissions:{aiOcrEnabled:true,kmlExportEnabled:true},convertRemaining:1});
  await expectCode(first,"RC_R8_CLOSED_OR_EXPIRED");
  assert.equal(f.instance.hasReadySession(),false);

  let deferredRun = null;
  const run = {runStatus:"ACTIVE",runClosed:false,deadlineAt:"2026-10-11T08:15:00Z"};
  const g = runtime({readRunStatus:()=>deferredRun ? deferredRun.promise : run});
  await g.instance.createSession({visitorId:g.instance.exactVisitorId,origin:"https://rc.example"});
  deferredRun=deferred();
  const release=g.instance.releaseUpload({expiresAt:run.deadlineAt});
  await expectCode(g.instance.releaseUpload({expiresAt:run.deadlineAt}),"RC_R8_RELEASE_REJECTED");
  g.instance.revoke(); deferredRun.resolve(run);
  await expectCode(release,"RC_R8_CLOSED_OR_EXPIRED");

  const wait=deferred();
  const h=runtime({readRunStatus:()=>wait.promise});
  const registration=h.instance.register({caseId:"indonesia",jobId:"job",jobAccessToken:"token",recognitionRequestId:terminalJob.result.requestId});
  h.instance.revoke(); wait.resolve(run);
  await expectCode(registration,"RC_R8_CLOSED_OR_EXPIRED");
});
await test("R8 actual release result evidence and close routes enforce authorization and terminal failure", async () => {
  const f=await r8UploadedFixture();
  const ctx=vm.createContext({coordinateThreeCaseRcAdmission:f.admission,coordinateThreeCaseRcBrowserHandoff:f.instance,CoordinateThreeCaseRcAdmissionError});
  for(const name of ["coordinateThreeCaseRcBrowserAuth","coordinateThreeCaseRcBrowserError"]) vm.runInContext(extractFunction(server,name),ctx);
  const request=async(route,admin=true)=>{
    const res={statusCode:200,setHeader(){},status(n){this.statusCode=n;return this;},json(value){this.payload=value;}};
    await r8Route(server,route,ctx)({body:{},query:{},get:name=>name==="x-admin-password"&&admin?"offline-only":""},res);
    return res;
  };
  const base="/api/admin/coordinate-products/three-case-rc";
  assert.equal((await request(base+"/release-upload",false)).statusCode,401);
  const fresh=await request(base+"/fresh-result");
  assert.equal(fresh.payload.ready,true);
  assert.equal(fresh.payload.snapshot.result.providerCallCount,1);
  assert.equal(JSON.stringify(fresh.payload).includes("jobAccessToken"),false);
  f.instance.reportFailure(f.auth);
  assert.equal((await request(base+"/browser-evidence")).payload.code,"RC_R8_BROWSER_CONSUMER_FAILED");
  assert.equal((await request(base+"/close",false)).statusCode,401);
  assert.equal(f.counters.close,0);
  const closed=await request(base+"/close");
  assert.equal(closed.payload.closed,true);assert.equal(f.counters.close,1);
  assert.equal(f.instance.hasReadySession(),false);
});
const failed = cases.filter(item => !item.pass);
console.log(JSON.stringify({ suite: "Coordinate Three-Case RC R7 Browser Handoff", passed: cases.length - failed.length, total: cases.length, cases }, null, 2));
if (failed.length) process.exitCode = 1;
