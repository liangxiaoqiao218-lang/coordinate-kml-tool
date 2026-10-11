import assert from "node:assert/strict";
import crypto from "node:crypto";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import { CoordinateConfirmationRuntime } from "../server/coordinate-finalizer/confirmation-runtime.js";
import { createCoordinateThreeCaseRcBrowserHandoff } from "../server/recognition/coordinate-three-case-rc-browser-handoff.js";
import { createRecognitionAcquisitionJobRuntime } from "../server/recognition/recognition-acquisition-job-runtime.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const cases = [];
const test = async (name, fn) => {
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
  const start = match.index;
  const remainder = source.slice(start + match[0].length);
  const next = /\n    (?:async\s+)?function\s+[A-Za-z_$][\w$]*\s*\(/u.exec(remainder);
  const end = next ? start + match[0].length + next.index : source.length;
  return source.slice(start, end).trimEnd();
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

await test("one received registry result dynamically reaches the real install Map and KML consumer functions", async () => {
  const { instance } = runtime();
  const session = await instance.createSession({ visitorId: instance.exactVisitorId, origin: "https://rc.example" });
  const auth = { cookieHeader: `${session.cookieName}=${session.cookieValue}`, csrfToken: session.csrfToken, origin: "https://rc.example" };
  await instance.register({ caseId: "indonesia", jobId: "job", jobAccessToken: "token", recognitionRequestId: terminalJob.result.requestId });
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
  const actualFunctions = [
    "hasFiniteFinalizedGeometry", "getFinalizedCoordinateIdentity", "cloneIdentityBoundValue",
    "deepFreezeIdentityBoundValue", "canonicalIdentityBoundSerialization", "identityBoundValuesEqual",
    "finalizedResultMatchesIdentityBoundSnapshot", "createIdentityBoundSnapshot",
    "rememberLastValidIdentityBoundSnapshot", "hasCompleteUnifiedRecognitionEvidence",
    "createRecognitionAuthorizationState", "installCoordinateRecognitionWorkspaceResult",
    "receiveCoordinateThreeCaseRcBrowserResult", "publishCoordinateThreeCaseRcBrowserConsumerEvidence",
    "isActiveCoordinateThreeCaseRcR7Identity", "activeCoordinateThreeCaseRcR7HasUnsupportedInnerRings",
    "getFinalizedGeometryCoordinateSource", "getAuthorizedFinalizedGeometryKmlSource",
    "coordinatesAreSame", "getClosedPolygonPairs", "escapeXml", "formatKmlCoordinates",
    "buildPointKml", "buildLineStringKml", "buildPolygonKml", "openSpatialResult", "downloadKmlInternal"
  ].map(name => extractFunction(index, name)).join("\n");
  const setup = `
    const COORDINATE_THREE_CASE_RC_R7_RUN_ID = "coordinate-three-case-rc-20261011-r7";
    const COORDINATE_THREE_CASE_RC_R7_VISITOR_ID = "rc-b-release-20261010-4f125a9";
    let coordinateThreeCaseRcBrowserMode = true;
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
  assert.equal(JSON.stringify(evidence.identity), JSON.stringify(identity));
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

const failed = cases.filter(item => !item.pass);
console.log(JSON.stringify({ suite: "Coordinate Three-Case RC R7 Browser Handoff", passed: cases.length - failed.length, total: cases.length, cases }, null, 2));
if (failed.length) process.exitCode = 1;
