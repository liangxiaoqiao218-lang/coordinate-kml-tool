import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import vm from "node:vm";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { CoordinateConfirmationRuntime } from "../server/coordinate-finalizer/confirmation-runtime.js";
import { evaluateRecognitionOutputCapability } from "../server/recognition/recognition-output-capability.js";
import { MapPreviewAdapter } from "../server/spatial/adapters/map-preview-adapter.js";
import { calculateSpatialFacts } from "../server/spatial/spatial-facts.js";
import {
  COORDINATE_CONSUMER_REPLAY,
  createCoordinateConsumerReplay
} from "../server/recognition/coordinate-consumer-replay.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const artifactPath = process.env.CONSUMER_REPLAY_ARTIFACT_PATH;
if (!artifactPath || !path.isAbsolute(artifactPath) || !fs.existsSync(artifactPath)) {
  throw new Error("CONSUMER_REPLAY_ARTIFACT_PATH_REQUIRED");
}
const artifactBytes = fs.readFileSync(artifactPath);
const artifact = JSON.parse(artifactBytes.toString("utf8").replace(/^\uFEFF/u, ""));
const sourceIdentity = artifact.cases[0].finalizedResult;
const cases = [];
const r2Incremental = process.argv.includes("--r2-incremental");
const caseFilter = process.env.CONSUMER_REPLAY_CASE_FILTER ? new RegExp(process.env.CONSUMER_REPLAY_CASE_FILTER, "u") : null;
const observedSideEffects = {
  providerCalls: 0,
  modelCalls: 0,
  recognitionClaims: 0,
  recognitionDispatches: 0,
  databaseWrites: 0,
  businessQuotaCalls: 0
};
async function test(name, fn) {
  if (r2Incremental && !name.startsWith("R2 ")) return;
  if (caseFilter && !caseFilter.test(name)) return;
  try { await fn(); cases.push({ name, pass: true }); }
  catch (error) {
    if (process.env.CONSUMER_REPLAY_REGRESSION_DEBUG === "true") console.error(error?.stack || error);
    cases.push({ name, pass: false, code: error?.message || error?.code });
  }
}

function extractFunction(source, name) {
  const match = new RegExp(`(?:async\\s+)?function\\s+${name}\\s*\\(`, "u").exec(source);
  if (!match) throw new Error(`FUNCTION_NOT_FOUND:${name}`);
  const tail = source.slice(match.index);
  for (const end of tail.matchAll(/^\s*\}/gmu)) {
    const candidate = tail.slice(0, end.index + end[0].length);
    try {
      new vm.Script(candidate);
      return candidate;
    } catch {
      // Continue until the complete production declaration is balanced.
    }
  }
  throw new Error(`FUNCTION_UNBALANCED:${name}`);
}

function extractRouteCallback(source, routeLiteral) {
  const routeStart = source.indexOf(routeLiteral);
  if (routeStart < 0) throw new Error(`ROUTE_NOT_FOUND:${routeLiteral}`);
  const asyncStart = source.indexOf("async (req, res) => {", routeStart);
  const syncStart = source.indexOf("(req, res) => {", routeStart);
  const syncBelongsToAsync = asyncStart >= routeStart && syncStart === asyncStart + "async ".length;
  const callbackStart = syncBelongsToAsync
    ? asyncStart
    : (syncStart >= routeStart && (asyncStart < routeStart || syncStart < asyncStart) ? syncStart : asyncStart);
  if (callbackStart < routeStart) throw new Error(`ROUTE_CALLBACK_NOT_FOUND:${routeLiteral}`);
  const tail = source.slice(callbackStart);
  for (const end of tail.matchAll(/^\s*\}/gmu)) {
    const candidate = tail.slice(0, end.index + end[0].length);
    try {
      new vm.Script(`(${candidate})`);
      return candidate;
    } catch {
      // Continue until the callback body is balanced.
    }
  }
  throw new Error(`ROUTE_CALLBACK_UNBALANCED:${routeLiteral}`);
}

function routeResponse() {
  return {
    statusCode: 200,
    headers: {},
    payload: null,
    setHeader(name, value) { this.headers[String(name).toLowerCase()] = value; return this; },
    status(value) { this.statusCode = value; return this; },
    json(value) { this.payload = value; return value; }
  };
}

function instantiateRouteCallback(context, source, label, routeLiteral) {
  try {
    return vm.runInContext(`(${extractRouteCallback(source, routeLiteral)})`, context);
  } catch (error) {
    throw new Error(`ROUTE_CALLBACK_COMPILE_FAILED:${label}:${error?.message || "UNKNOWN"}`);
  }
}

function fixture({ permission = true, mapProvider = "OPENFREEMAP", clock = { value: Date.now() } } = {}) {
  const registry = new Map();
  const replay = createCoordinateConsumerReplay({
    enabled: true,
    expectedOrigin: "https://coordinate-kml-tool-rc.onrender.com",
    readExistingUserAccess: async () => ({ existingUser: true, permissions: { kmlExportEnabled: permission } }),
    selectedMapProvider: () => mapProvider,
    now: () => clock.value,
    token: (() => { let index = 0; return () => `token-${++index}`; })(),
    uuid: () => "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    registerFinalizedResult(result) { registry.set(result.resultId, result); return result; },
    revokeFinalizedResult(identity) { return registry.delete(identity.resultId); }
  });
  return { replay, registry, clock };
}

function productMapFixture({ clock = { value: Date.now() } } = {}) {
  const confirmationRuntime = new CoordinateConfirmationRuntime({ now: () => clock.value });
  const replay = createCoordinateConsumerReplay({
    enabled: true,
    expectedOrigin: "https://coordinate-kml-tool-rc.onrender.com",
    readExistingUserAccess: async () => ({ existingUser: true, permissions: { kmlExportEnabled: true } }),
    selectedMapProvider: () => "OPENFREEMAP",
    now: () => clock.value,
    token: (() => { let index = 0; return () => `product-token-${++index}`; })(),
    uuid: () => "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    registerFinalizedResult: result => confirmationRuntime.register(result),
    revokeFinalizedResult: identity => confirmationRuntime.records.delete(String(identity?.resultId || ""))
  });
  const serverSource = fs.readFileSync(path.join(root, "server.js"), "utf8");
  const context = vm.createContext({
    coordinateConsumerReplay: replay,
    mapPreviewAdapter: new MapPreviewAdapter(),
    calculateSpatialFacts,
    evaluateRecognitionOutputCapability,
    validateFinalizedCoordinateIdentity: identity => confirmationRuntime.validateIdentity(identity)
  });
  const handler = instantiateRouteCallback(context, serverSource, "map", '"/api/map-preview"');
  return {
    replay,
    clock,
    confirmationRuntime,
    async map(identity) {
      const req = { body: structuredClone(identity), get() { return ""; } };
      const res = routeResponse();
      await handler(req, res);
      return res;
    }
  };
}
async function session(replay) {
  const created = await replay.createSession({
    visitorId: COORDINATE_CONSUMER_REPLAY.visitorId,
    origin: "https://coordinate-kml-tool-rc.onrender.com"
  });
  return {
    created,
    auth: {
      origin: "https://coordinate-kml-tool-rc.onrender.com",
      cookieHeader: `${created.cookieName}=${created.cookieValue}`,
      csrfToken: created.csrfToken
    }
  };
}
function registration(replay, bytes = artifactBytes, sha = COORDINATE_CONSUMER_REPLAY.sourceArtifactSha256) {
  return replay.register({ artifactBase64: bytes.toString("base64"), artifactSha256: sha });
}

await test("exact artifact creates new replay identity", async () => {
  const { replay, registry } = fixture();
  const { auth } = await session(replay);
  const registered = registration(replay);
  assert.equal(registered.registered, true);
  assert.equal(registered.runId, COORDINATE_CONSUMER_REPLAY.runId);
  assert.equal(replay.status(auth).ready, true);
  const received = replay.receive(auth);
  assert.equal(received.replayOnly, true);
  assert.equal(received.businessQuotaBypassed, true);
  assert.notEqual(received.resultIdentity.resultId, sourceIdentity.resultId);
  assert.equal(received.result.finalizedCoordinateResult.geometry.type, sourceIdentity.geometry.type);
  assert.equal(registry.size, 1);
});

await test("wrong artifact hash fails closed", async () => {
  const { replay } = fixture();
  assert.throws(() => registration(replay, artifactBytes, "0".repeat(64)), error =>
    error?.code === "COORDINATE_CONSUMER_REPLAY_ARTIFACT_HASH_MISMATCH");
});

await test("ordinary permission cannot enter replay", async () => {
  const { replay } = fixture({ permission: false });
  await assert.rejects(session(replay), error => error?.code === "COORDINATE_CONSUMER_REPLAY_PERMISSION_REJECTED");
});

await test("non OpenFreeMap provider fails before registration", async () => {
  const { replay } = fixture({ mapProvider: "AMAP" });
  await assert.rejects(session(replay), error => error?.code === "COORDINATE_CONSUMER_REPLAY_MAP_PROVIDER_REJECTED");
});

await test("handoff is one-use and duplicate registration is rejected", async () => {
  const { replay } = fixture();
  const { auth } = await session(replay);
  registration(replay);
  assert.throws(() => registration(replay), error => error?.code === "COORDINATE_CONSUMER_REPLAY_ALREADY_REGISTERED");
  replay.receive(auth);
  assert.throws(() => replay.receive(auth), error => error?.code === "COORDINATE_CONSUMER_REPLAY_UNAVAILABLE");
});

await test("expired handoff cannot be received", async () => {
  const state = fixture();
  const { auth } = await session(state.replay);
  registration(state.replay);
  state.clock.value += COORDINATE_CONSUMER_REPLAY.handoffTtlMs + 1;
  assert.throws(() => state.replay.receive(auth), error => error?.code === "COORDINATE_CONSUMER_REPLAY_UNAVAILABLE");
});

await test("stage ledger rejects stale or unbound updates", async () => {
  const { replay } = fixture();
  const { auth } = await session(replay);
  registration(replay);
  const received = replay.receive(auth);
  assert.throws(() => replay.recordStage(auth, { bindingDigest: "0".repeat(64), stage: "RESULT_INSTALLED" }),
    error => error?.code === "COORDINATE_CONSUMER_REPLAY_BINDING_MISMATCH");
  assert.equal(replay.recordStage(auth, { bindingDigest: received.bindingDigest, stage: "RESULT_INSTALLED" }).accepted, true);
  assert.equal(replay.recordStage(auth, { bindingDigest: received.bindingDigest, stage: "MAP_READY" }).accepted, true);
});

await test("first failure is terminal and later success cannot clear it", async () => {
  const { replay } = fixture();
  const { auth } = await session(replay);
  registration(replay);
  const received = replay.receive(auth);
  assert.equal(replay.recordStage(auth, {
    bindingDigest: received.bindingDigest,
    stage: "RESULT_INSTALL_FAILED",
    failureCode: "RESULT_INSTALL_FAILED"
  }).terminal, true);
  assert.throws(() => replay.recordStage(auth, {
    bindingDigest: received.bindingDigest,
    stage: "MAP_READY"
  }), error => error?.code === "COORDINATE_CONSUMER_REPLAY_UNAVAILABLE");
  assert.equal(replay.readConsumerEvidence().ready, false);
  assert.equal(replay.readConsumerEvidence().lastStage, "RESULT_INSTALL_FAILED");
});

await test("pre-binding receive failure freezes registration", async () => {
  const { replay } = fixture();
  const { auth } = await session(replay);
  assert.equal(replay.recordStage(auth, { stage: "RECEIVE_FAILED", failureCode: "RECEIVE_FAILED" }).terminal, true);
  assert.throws(() => registration(replay), error => error?.code === "COORDINATE_CONSUMER_REPLAY_TERMINAL_FAILURE");
});

await test("replay identity expires for product map consumer", async () => {
  const state = fixture();
  const { auth } = await session(state.replay);
  registration(state.replay);
  const received = state.replay.receive(auth);
  assert.equal(state.replay.inspectIdentity(received.resultIdentity).ok, true);
  state.clock.value += COORDINATE_CONSUMER_REPLAY.handoffTtlMs + 1;
  const expired = state.replay.inspectIdentity(received.resultIdentity);
  assert.equal(expired.matched, true);
  assert.equal(expired.ok, false);
  assert.equal(expired.code, "COORDINATE_CONSUMER_REPLAY_EXPIRED");
});

await test("actual map preview route rejects an expired replay identity", async () => {
  const state = productMapFixture();
  const { auth } = await session(state.replay);
  registration(state.replay);
  const received = state.replay.receive(auth);
  assert.equal((await state.map(received.resultIdentity)).statusCode, 200);
  state.clock.value += COORDINATE_CONSUMER_REPLAY.handoffTtlMs + 1;
  const expired = await state.map(received.resultIdentity);
  assert.equal(expired.statusCode, 410);
  assert.equal(expired.payload?.code, "COORDINATE_CONSUMER_REPLAY_EXPIRED");
});

await test("actual map preview route rejects a terminally failed replay identity", async () => {
  const state = productMapFixture();
  const { auth } = await session(state.replay);
  registration(state.replay);
  const received = state.replay.receive(auth);
  state.replay.recordStage(auth, {
    bindingDigest: received.bindingDigest,
    stage: "MAP_FAILED",
    failureCode: "MAP_PRODUCT_PATH_FAILED"
  });
  const failed = await state.map(received.resultIdentity);
  assert.equal(failed.statusCode, 409);
  assert.equal(failed.payload?.code, "COORDINATE_CONSUMER_REPLAY_TERMINAL_FAILURE");
});

await test("consumer evidence requires map KML identity and replay boundary", async () => {
  const { replay } = fixture();
  const { auth } = await session(replay);
  registration(replay);
  const received = replay.receive(auth);
  replay.recordStage(auth, { bindingDigest: received.bindingDigest, stage: "RESULT_INSTALLED" });
  replay.recordStage(auth, { bindingDigest: received.bindingDigest, stage: "MAP_READY" });
  replay.recordStage(auth, { bindingDigest: received.bindingDigest, stage: "KML_TRIGGERED" });
  const evidence = {
    schemaVersion: "coordinate_consumer_replay_browser_evidence_v1",
    runId: COORDINATE_CONSUMER_REPLAY.runId,
    batchId: COORDINATE_CONSUMER_REPLAY.batchId,
    caseId: "indonesia",
    bindingDigest: received.bindingDigest,
    replayOnly: true,
    businessQuotaBypassed: true,
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
  assert.throws(() => replay.recordConsumerEvidence(auth, { ...evidence, businessQuotaBypassed: false }),
    error => error?.code === "COORDINATE_CONSUMER_REPLAY_EVIDENCE_REJECTED");
  assert.equal(replay.recordConsumerEvidence(auth, evidence).accepted, true);
  assert.equal(replay.readConsumerEvidence().ready, true);
});

await test("close revokes replay identity and session", async () => {
  const { replay, registry } = fixture();
  const { auth } = await session(replay);
  registration(replay);
  const received = replay.receive(auth);
  assert.equal(registry.size, 1);
  assert.equal(replay.close().closed, true);
  assert.equal(registry.size, 0);
  assert.equal(replay.readConsumerEvidence().ready, false);
  assert.equal(replay.readConsumerEvidence().lastStage, "CLOSED");
  assert.equal(replay.inspectIdentity(received.resultIdentity).ok, false);
  assert.throws(() => replay.status(auth), error => error?.code === "COORDINATE_CONSUMER_REPLAY_DISABLED");
});

await test("R2 actual handlers registry install Map KML blob physical gate and close preserve one identity", async () => {
  const index = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const serverSource = fs.readFileSync(path.join(root, "server.js"), "utf8");
  const confirmationRuntime = new CoordinateConfirmationRuntime();
  let existingUserReads = 0;
  const replay = createCoordinateConsumerReplay({
    enabled: true,
    expectedOrigin: "https://coordinate-kml-tool-rc.onrender.com",
    readExistingUserAccess: async () => {
      existingUserReads += 1;
      return { existingUser: true, permissions: { kmlExportEnabled: true } };
    },
    selectedMapProvider: () => "OPENFREEMAP",
    registerFinalizedResult: result => confirmationRuntime.register(result),
    revokeFinalizedResult: identity => confirmationRuntime.records.delete(String(identity?.resultId || ""))
  });
  const serverRuntime = vm.createContext({
    Buffer,
    crypto,
    adminPassword: "offline-admin-password",
    COORDINATE_CONSUMER_REPLAY,
    coordinateConsumerReplay: replay,
    mapPreviewAdapter: new MapPreviewAdapter(),
    calculateSpatialFacts,
    evaluateRecognitionOutputCapability,
    validateFinalizedCoordinateIdentity: identity => confirmationRuntime.validateIdentity(identity)
  });
  vm.runInContext(extractFunction(serverSource, "authorizeCoordinateConsumerReplayAdmin"), serverRuntime);
  vm.runInContext(extractFunction(serverSource, "coordinateThreeCaseRcBrowserError"), serverRuntime);
  vm.runInContext(extractFunction(serverSource, "coordinateConsumerReplayBrowserAuth"), serverRuntime);
  const routeHandlers = {
    session: instantiateRouteCallback(serverRuntime, serverSource, "session", '"/api/admin/coordinate-consumer-replay/browser-session"'),
    register: instantiateRouteCallback(serverRuntime, serverSource, "register", '"/api/admin/coordinate-consumer-replay/register"'),
    status: instantiateRouteCallback(serverRuntime, serverSource, "status", '"/api/coordinate-consumer-replay/status"'),
    receive: instantiateRouteCallback(serverRuntime, serverSource, "receive", '"/api/coordinate-consumer-replay/receive"'),
    stage: instantiateRouteCallback(serverRuntime, serverSource, "stage", '"/api/coordinate-consumer-replay/stage"'),
    evidence: instantiateRouteCallback(serverRuntime, serverSource, "evidence", '"/api/coordinate-consumer-replay/consumer-evidence"'),
    close: instantiateRouteCallback(serverRuntime, serverSource, "close", '"/api/admin/coordinate-consumer-replay/close"'),
    map: instantiateRouteCallback(serverRuntime, serverSource, "map", '"/api/map-preview"')
  };
  const invokeRoute = async (handler, { body = null, headers = {}, query = {} } = {}) => {
    const normalized = Object.fromEntries(Object.entries(headers).map(([key, value]) => [key.toLowerCase(), value]));
    const req = { body, query, get(name) { return normalized[String(name).toLowerCase()] || ""; } };
    const res = routeResponse();
    await handler(req, res);
    return res;
  };
  const origin = "https://coordinate-kml-tool-rc.onrender.com";
  const sessionResponse = await invokeRoute(routeHandlers.session, {
    body: { visitorId: COORDINATE_CONSUMER_REPLAY.visitorId },
    headers: { origin, "x-admin-password": "offline-admin-password" }
  });
  assert.equal(sessionResponse.statusCode, 201);
  const cookieHeader = String(sessionResponse.headers["set-cookie"] || "").split(";", 1)[0];
  const csrfToken = sessionResponse.payload.csrfToken;
  const registrationResponse = await invokeRoute(routeHandlers.register, {
    body: { artifactSha256: COORDINATE_CONSUMER_REPLAY.sourceArtifactSha256,
      artifactBase64: artifactBytes.toString("base64") },
    headers: { "x-admin-password": "offline-admin-password" }
  });
  assert.equal(registrationResponse.statusCode, 201);
  const registered = registrationResponse.payload;
  assert.equal(registered.runId, "coordinate-indonesia-consumer-replay-20261012-r2");
  assert.equal(registered.batchId, "coordinate-indonesia-consumer-replay-20261012-v2");
  assert.ok(Date.parse(registered.expiresAt) <= Date.parse(sessionResponse.payload.expiresAt));
  const browserHeaders = { origin, cookie: cookieHeader, "x-coordinate-replay-csrf": csrfToken };
  const browserState = { anchorClicked: false, blobText: "", mapOpened: false, mapIdentityMatch: false,
    mapStatus: null, mapCode: null, mapResultIdMatch: null, mapRevisionMatch: null, mapGeometryHashMatch: null,
    runtimeRecordPresent: null, runtimeRecordHashMatch: null,
    routeError: null,
    providerCalls: 0, databaseWrites: 0, quotaCalls: 0, fetchPaths: [], statusReady: null,
    serverRouteCalls: 0 };
  let receivedIdentity = null;
  class CapturedBlob {
    constructor(parts) { browserState.blobText = parts.join(""); }
  }
  const routeByPath = new Map([
    ["/api/coordinate-consumer-replay/status", routeHandlers.status],
    ["/api/coordinate-consumer-replay/receive", routeHandlers.receive],
    ["/api/coordinate-consumer-replay/stage", routeHandlers.stage],
    ["/api/coordinate-consumer-replay/consumer-evidence", routeHandlers.evidence],
    ["/api/map-preview", routeHandlers.map]
  ]);
  const routeDispatcher = async (url, options = {}) => {
    try {
      const handler = routeByPath.get(url);
      if (!handler) throw new Error("UNEXPECTED_OR_EXTERNAL_FETCH");
      browserState.serverRouteCalls += 1;
      const suppliedHeaders = options.headers && typeof options.headers === "object" ? options.headers : {};
      const response = await invokeRoute(handler, {
        body: options.body ? JSON.parse(options.body) : null,
        headers: { ...browserHeaders, ...suppliedHeaders }
      });
      if (url === "/api/map-preview") {
        const requestBody = options.body ? JSON.parse(options.body) : {};
        const runtimeRecord = confirmationRuntime.records.get(String(requestBody.resultId || ""))?.result || null;
        browserState.mapResultIdMatch = requestBody.resultId === receivedIdentity?.resultId;
        browserState.mapRevisionMatch = requestBody.resultRevision === receivedIdentity?.resultRevision;
        browserState.mapGeometryHashMatch = requestBody.geometryHash === receivedIdentity?.geometryHash;
        browserState.runtimeRecordPresent = Boolean(runtimeRecord);
        browserState.runtimeRecordHashMatch = runtimeRecord?.geometryHash === requestBody.geometryHash;
        browserState.mapIdentityMatch = response.statusCode >= 200 && response.statusCode < 300;
        browserState.mapStatus = response.statusCode;
        browserState.mapCode = response.payload?.code || null;
      }
      return { ok: response.statusCode >= 200 && response.statusCode < 300, status: response.statusCode,
        json: async () => structuredClone(response.payload) };
    } catch (error) {
      browserState.routeError = error?.message || "UNKNOWN_ROUTE_ERROR";
      throw error;
    }
  };
  const functions = [
    "hasFiniteFinalizedGeometry", "getFinalizedCoordinateIdentity", "cloneIdentityBoundValue",
    "deepFreezeIdentityBoundValue", "canonicalIdentityBoundSerialization", "identityBoundValuesEqual",
    "finalizedResultMatchesIdentityBoundSnapshot", "createIdentityBoundSnapshot",
    "rememberLastValidIdentityBoundSnapshot", "hasCompleteUnifiedRecognitionEvidence",
    "createRecognitionAuthorizationState", "installCoordinateRecognitionWorkspaceResult",
    "controlledConsumerPath", "controlledConsumerCsrfHeaders", "recordCoordinateConsumerReplayStage",
    "markCoordinateConsumerReplayTerminalFailure", "failCoordinateConsumerReplay",
    "receiveCoordinateThreeCaseRcBrowserResult", "publishCoordinateThreeCaseRcBrowserConsumerEvidence",
    "isActiveCoordinateThreeCaseRcR7Identity", "isActiveCoordinateConsumerReplayIdentity",
    "isActiveControlledCoordinateConsumerIdentity", "activeCoordinateThreeCaseRcR7HasUnsupportedInnerRings",
    "getFinalizedGeometryCoordinateSource", "getAuthorizedFinalizedGeometryKmlSource",
    "getConvertibleCoordinateGroups", "hasConvertibleCoordinateResult", "coordinateResultNeedsReview",
    "recognitionActionEnabled", "coordinateRecognitionActionBlocked", "shouldUseProvisionalCoordinateResult",
    "recognitionAuthorizationReasonMessage", "stopBlockedCoordinateAction", "ensureManualInputFinalized",
    "shouldBlockFinalizedCoordinateKml", "loadUserConfig", "canUse", "setKmlGenerationInProgress",
    "clearKmlGenerationProgress", "sanitizeUserFacingTechMessage",
    "coordinatesAreSame", "getClosedPolygonPairs", "escapeXml", "formatKmlCoordinates",
    "buildPointKml", "buildLineStringKml", "buildPolygonKml", "openSpatialResult", "downloadKml",
    "downloadKmlInternal"
  ].map(name => extractFunction(index, name)).join("\n");
  const context = vm.createContext({
    console,
    structuredClone,
    crypto: crypto.webcrypto,
    TextEncoder,
    Blob: CapturedBlob,
    Date,
    URL: { createObjectURL: () => "blob:consumer-replay", revokeObjectURL: () => {} },
    document: {
      body: { appendChild: () => {}, removeChild: () => {} },
      createElement: () => ({ href: "", download: "", click: () => { browserState.anchorClicked = true; } })
    },
    window: { matchMedia: () => ({ matches: false }) },
    setTimeout,
    clearTimeout
  });
  context.browserState = browserState;
  context.routeDispatcher = routeDispatcher;
  context.browserCsrfToken = csrfToken;
  context.captureReceivedIdentity = identity => { receivedIdentity = structuredClone(identity); };
  const setup = `
    const COORDINATE_THREE_CASE_RC_R7_RUN_ID = "coordinate-three-case-rc-20261011-r7";
    const COORDINATE_THREE_CASE_RC_R7_VISITOR_ID = "rc-b-release-20261010-4f125a9";
    const COORDINATE_CONSUMER_REPLAY_RUN_ID = "${COORDINATE_CONSUMER_REPLAY.runId}";
    const COORDINATE_CONSUMER_REPLAY_BATCH_ID = "${COORDINATE_CONSUMER_REPLAY.batchId}";
    let coordinateThreeCaseRcBrowserMode = false;
    let coordinateConsumerReplayBrowserMode = true;
    let controlledCoordinateConsumerBrowserMode = true;
    let coordinateThreeCaseRcCsrfToken = browserCsrfToken;
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
    let appConfig = { permissions: { kmlExportEnabled: true } };
    let kmlGenerationInProgress = false;
    const input = { value: "" };
    const coordinateOrder = { value: "auto" };
    const coordinateUploadPanel = { classList: { add() {} } };
    const coordinateThreeCaseRcBrowserPanel = { dataset: {} };
    const coordinateThreeCaseRcReceive = { disabled: false };
    const coordinateThreeCaseRcBrowserStatus = { textContent: "" };
    const mapPreviewAction = { disabled: false, dataset: {}, setAttribute() {}, hidden: false, textContent: "", title: "" };
    const pages = { spatialResult: { dataset: {}, classList: { contains: () => false } } };
    const spatialMapFailure = { hidden: true };
    const spatialResultDiagnostic = { hidden: true, textContent: "" };
    const recognitionProgress = { textContent: "", classList: { contains: () => false } };
    const invalidateSpatialPreview = () => { activeMapPreviewResponse = null; activeMapPreviewCacheKey = ""; };
    const rememberState = () => {};
    const compactRecognizedCoordinateDisplayText = value => String(value || "");
    const getCanonicalCoordinateDisplayText = () => "redacted-fixed-provider-free-input";
    const clearInternalKmlSource = () => {};
    const syncCoordinateReviewConfirmationState = () => {};
    const renderProjectedCrsReviewPanel = () => {};
    const updateCadastralGridPanel = () => {};
    const refreshMapPreviewAction = () => {};
    const syncKmlActionVisualState = () => {};
    const applyFeatureAvailability = () => {};
    const projectedCrsSelectionNeedsConfirmation = () => false;
    const getSpatialPreviewCacheKey = (identity = getFinalizedCoordinateIdentity()) => identity
      ? identity.resultId + ":" + identity.resultRevision + ":" + identity.geometryHash : "";
    const fetch = async (url, options = {}) => {
      browserState.fetchPaths.push(url);
      const response = await routeDispatcher(url, options);
      if (url === "/api/coordinate-consumer-replay/status") {
        const payload = await response.json(); browserState.statusReady = payload.ready;
        return { ...response, json: async () => payload };
      }
      if (url === "/api/coordinate-consumer-replay/receive") {
        const payload = await response.json();
        captureReceivedIdentity(payload.resultIdentity);
        browserState.receiveContract = {
          accepted: payload.accepted === true,
          runMatch: payload.runId === COORDINATE_CONSUMER_REPLAY_RUN_ID,
          batchMatch: payload.batchId === COORDINATE_CONSUMER_REPLAY_BATCH_ID,
          caseMatch: payload.caseId === "indonesia",
          visitorPresent: Boolean(payload.visitorId),
          bindingPresent: /^[0-9a-f]{64}$/u.test(String(payload.bindingDigest || "")),
          identityPresent: Boolean(payload.resultIdentity),
          finalizedPresent: Boolean(payload.result?.finalizedCoordinateResult)
        };
        return { ...response, json: async () => payload };
      }
      return response;
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
      const identity = getFinalizedCoordinateIdentity();
      browserState.mapOpened = true;
      return { state: "READY", authorityMutationCount: 0, renderReceipt: {
        provider: "OPENFREEMAP", sourceResultId: identity.resultId,
        sourceRevision: identity.resultRevision, sourceGeometryHash: identity.geometryHash,
        authorityMutationCount: 0
      } };
    } };
    const showKmlPermissionFailure = () => {};
    const focusProjectedCrsReviewPanel = () => {};
    const showKmlFailure = () => {};
    const showRecognitionProgress = () => {};
    const consumeUsage = async () => { browserState.quotaCalls += 1; throw new Error("UNEXPECTED_BUSINESS_QUOTA_CALL"); };
    const trackEvent = () => {};
    const showKmlCompletion = () => {};
    const hasCadastralGridRows = () => false;
    const hasMgrsRows = () => false;
    const getKmlCoordinateGroups = () => [];
    const downloadProvisionalCoordinateKml = () => { throw new Error("UNEXPECTED_PROVISIONAL_KML"); };
    const downloadAgenticCoordinateKml = () => { throw new Error("UNEXPECTED_AGENTIC_KML"); };
    ${functions}
    globalThis.__consumerReplayProduct = {
      receive: receiveCoordinateThreeCaseRcBrowserResult,
      openMap: openSpatialResult,
      downloadKml,
      evidence: () => ({
        active: isActiveCoordinateConsumerReplayIdentity(),
        snapshotPresent: Boolean(lastValidIdentityBoundSnapshot),
        mapReady: activeCoordinateThreeCaseRcHandoff?.mapReady === true,
        kmlDownloadTriggered: activeCoordinateThreeCaseRcHandoff?.kmlDownloadTriggered === true,
        handoffPresent: Boolean(activeCoordinateThreeCaseRcHandoff),
        failed: activeCoordinateThreeCaseRcHandoff?.failed === true,
        failureStage: activeCoordinateThreeCaseRcHandoff?.failureStage || null,
        replayOnly: activeCoordinateThreeCaseRcHandoff?.replayOnly === true,
        sourceAuthorityMatch: activeFinalizedCoordinateResult?.sourceAuthority === "historical_consumer_replay",
        expiryFuture: Date.parse(String(activeCoordinateThreeCaseRcHandoff?.expiresAt || "")) > Date.now(),
        identity: getFinalizedCoordinateIdentity()
      })
    };
  `;
  new vm.Script(setup, { filename: "coordinate-consumer-replay-product-vm.js" }).runInContext(context);
  await context.__consumerReplayProduct.receive();
  assert.equal(context.__consumerReplayProduct.evidence().active, true,
    `REPLAY_NOT_ACTIVE_AFTER_INSTALL:${JSON.stringify({ evidence: context.__consumerReplayProduct.evidence(), browserState })}`);
  await context.__consumerReplayProduct.openMap();
  assert.equal(context.__consumerReplayProduct.evidence().mapReady, true,
    `REPLAY_MAP_NOT_READY:${JSON.stringify(browserState)}`);
  await context.__consumerReplayProduct.downloadKml();
  const product = context.__consumerReplayProduct.evidence();
  assert.equal(product.active, true);
  assert.equal(product.snapshotPresent, true);
  assert.equal(product.mapReady, true);
  assert.equal(product.kmlDownloadTriggered, true);
  assert.equal(browserState.mapIdentityMatch, true);
  assert.equal(browserState.mapOpened, true);
  assert.equal(browserState.anchorClicked, true);
  assert.equal(browserState.quotaCalls, 0);
  assert.equal(browserState.providerCalls, 0);
  assert.equal(browserState.databaseWrites, 0);
  assert.deepEqual([...new Set(browserState.fetchPaths)].sort(), [
    "/api/coordinate-consumer-replay/consumer-evidence",
    "/api/coordinate-consumer-replay/receive",
    "/api/coordinate-consumer-replay/stage",
    "/api/coordinate-consumer-replay/status",
    "/api/map-preview"
  ]);
  observedSideEffects.providerCalls += browserState.providerCalls;
  observedSideEffects.databaseWrites += browserState.databaseWrites;
  observedSideEffects.businessQuotaCalls += browserState.quotaCalls;
  assert.match(browserState.blobText, /Historical Consumer Replay Geometry/u);
  assert.match(browserState.blobText, /CONSUMER-ONLY \/ UNVERIFIED \/ NO RECOGNITION/u);
  assert.match(browserState.blobText, /<Polygon>/u);
  const consumerEvidence = replay.readConsumerEvidence();
  assert.equal(consumerEvidence.ready, true);
  assert.equal(consumerEvidence.evidence.bindingDigest, registered.bindingDigest);

  const gateDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "geokit-consumer-replay-gate-"));
  const resolvedTempRoot = fs.realpathSync(os.tmpdir());
  const resolvedGateDirectory = fs.realpathSync(gateDirectory);
  assert.equal(path.dirname(resolvedGateDirectory), resolvedTempRoot);
  assert.match(path.basename(resolvedGateDirectory), /^geokit-consumer-replay-gate-/u);
  try {
    const evidencePath = path.join(resolvedGateDirectory, "browser-evidence.json");
    const kmlPath = path.join(resolvedGateDirectory, "coordinates.kml");
    fs.writeFileSync(evidencePath, `${JSON.stringify(consumerEvidence.evidence, null, 2)}\n`, "utf8");
    fs.writeFileSync(kmlPath, browserState.blobText, "utf8");
    const pwsh = "C:\\Users\\Mir-1\\.cache\\codex-runtimes\\codex-primary-runtime\\dependencies\\native\\powershell\\pwsh.exe";
    assert.equal(fs.existsSync(pwsh), true, "BUNDLED_PWSH_NOT_FOUND");
    const gate = spawnSync(pwsh, [
      "-NoProfile", "-File", path.join(root, "scripts", "coordinate-consumer-replay-gate.ps1"),
      "-BrowserEvidencePath", evidencePath,
      "-HistoricalArtifactPath", artifactPath,
      "-KmlDownloadPath", kmlPath,
      "-ExpectedRunId", COORDINATE_CONSUMER_REPLAY.runId,
      "-ExpectedBatchId", COORDINATE_CONSUMER_REPLAY.batchId,
      "-ExpectedArtifactSha256", COORDINATE_CONSUMER_REPLAY.sourceArtifactSha256,
      "-ExpectedBindingDigest", registered.bindingDigest,
      "-HandoffExpiresAt", consumerEvidence.expiresAt,
      "-TimeoutSeconds", "10"
    ], { encoding: "utf8", timeout: 20_000, windowsHide: true });
    assert.equal(gate.error, undefined, `POWERSHELL_GATE_PROCESS_ERROR:${gate.error?.code || "UNKNOWN"}`);
    assert.equal(gate.status, 0, `POWERSHELL_GATE_FAILED:${String(gate.stdout || "").trim()}`);
    const gateReceipt = JSON.parse(String(gate.stdout || "").trim().split(/\r?\n/u).filter(Boolean).at(-1));
    assert.equal(gateReceipt.success, true);
    assert.equal(gateReceipt.kmlGeometryMatch, true);
    assert.equal(gateReceipt.providerCalls, 0);
    assert.equal(gateReceipt.databaseWrites, 0);
    browserState.powerShellGatePassed = true;
  } finally {
    assert.equal(path.dirname(resolvedGateDirectory), resolvedTempRoot);
    assert.match(path.basename(resolvedGateDirectory), /^geokit-consumer-replay-gate-/u);
    fs.rmSync(resolvedGateDirectory, { recursive: true, force: true });
  }
  const closeResponse = await invokeRoute(routeHandlers.close, {
    headers: { "x-admin-password": "offline-admin-password" }
  });
  assert.equal(closeResponse.statusCode, 200);
  assert.equal(closeResponse.payload?.closed, true);
  const closedMapResponse = await invokeRoute(routeHandlers.map, { body: product.identity });
  assert.equal(closedMapResponse.statusCode, 410);
  assert.equal(closedMapResponse.payload?.code, "COORDINATE_CONSUMER_REPLAY_CLOSED");
  assert.equal(replay.readConsumerEvidence().ready, false);
  assert.equal(replay.readConsumerEvidence().lastStage, "CLOSED");
  assert.equal(confirmationRuntime.records.size, 0);
});

await test("source wiring uses actual product consumers and excludes recognition APIs", async () => {
  const index = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const server = fs.readFileSync(path.join(root, "server.js"), "utf8");
  const client = fs.readFileSync(path.join(root, "scripts", "coordinate-consumer-replay-client.ps1"), "utf8");
  assert.match(index, /openSpatialResult\(\)/u);
  assert.match(index, /GeoKitSatelliteMap\?\.open\(payload\)/u);
  assert.match(index, /downloadKml\(\)/u);
  assert.match(index, /isActiveCoordinateConsumerReplayIdentity/u);
  assert.match(server, /coordinateConsumerReplay\.register/u);
  assert.match(server, /coordinateConsumerReplay\.close/u);
  assert.doesNotMatch(client, /recognize-coordinates|three-case-rc\/claims|dispatch/u);
  assert.doesNotMatch(server.slice(server.indexOf("/api/admin/coordinate-consumer-replay/browser-session")),
    /activate_coordinate_three_case_rc_run/u);
});

await test("R2 actual frontend query binding accepts R2 only and matches runtime V2", () => {
  const index = fs.readFileSync(path.join(root, "index.html"), "utf8");
  const names = ["COORDINATE_CONSUMER_REPLAY_RUN_ID", "COORDINATE_CONSUMER_REPLAY_BATCH_ID", "coordinateConsumerReplayBrowserMode"];
  const declarations = names.map(name => {
    const declaration = index.match(new RegExp(`const ${name} = [^;]+;`, "u"));
    assert.ok(declaration); return declaration[0];
  }).join("\n");
  for (const version of ["r1", "r2", "r7"]) {
    const actual = vm.runInNewContext(`${declarations}\n({ runId: COORDINATE_CONSUMER_REPLAY_RUN_ID,
      batchId: COORDINATE_CONSUMER_REPLAY_BATCH_ID, enabled: coordinateConsumerReplayBrowserMode })`,
    { URLSearchParams, window: { location: { search: `?coordinate-consumer-replay=${version}` } } });
    assert.equal(actual.enabled, version === "r2");
    assert.equal(actual.runId, COORDINATE_CONSUMER_REPLAY.runId);
    assert.equal(actual.batchId, COORDINATE_CONSUMER_REPLAY.batchId);
  }
});

await test("R2 registration cannot outlive existing browser session and actual map handler rejects expiry", async () => {
  const state = productMapFixture();
  const { auth, created } = await session(state.replay);
  state.clock.value = Date.parse(created.expiresAt) - 1000;
  const registered = registration(state.replay);
  assert.equal(registered.expiresAt, created.expiresAt);
  const received = state.replay.receive(auth);
  assert.equal((await state.map(received.resultIdentity)).statusCode, 200);
  state.clock.value += 1000;
  assert.equal((await state.map(received.resultIdentity)).statusCode, 410);
  assert.throws(() => state.replay.status(auth), error => error.code === "COORDINATE_CONSUMER_REPLAY_BROWSER_SESSION_REQUIRED");
  assert.equal(state.replay.close().closed, true);
  assert.equal(state.confirmationRuntime.records.size, 0);
});

await test("R2 missing or expired authentication cannot register historical geometry", async () => {
  for (const expired of [false, true]) {
    const state = fixture();
    if (expired) {
      const { created } = await session(state.replay);
      state.clock.value = Date.parse(created.expiresAt);
    }
    assert.throws(() => registration(state.replay), error => error.code === "COORDINATE_CONSUMER_REPLAY_BROWSER_SESSION_REQUIRED");
    assert.equal(state.registry.size, 0);
  }
});

const passed = cases.filter(item => item.pass).length;
const result = { suite: "COORDINATE_CONSUMER_REPLAY", passed, total: cases.length, ...observedSideEffects, cases };
console.log(JSON.stringify(result));
if (passed !== cases.length) process.exitCode = 1;
