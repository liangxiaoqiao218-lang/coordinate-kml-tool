import crypto from "node:crypto";
import { createGeometryHash } from "../coordinate-finalizer/geometry-hash.js";

export const COORDINATE_CONSUMER_REPLAY = Object.freeze({
  runId: "coordinate-indonesia-consumer-replay-20261012-r1",
  batchId: "coordinate-indonesia-consumer-replay-20261012-v1",
  sourceRunId: "coordinate-three-case-rc-20261011-r7",
  sourceBatchId: "coordinate-three-case-rc-20261011-v7",
  sourceCommit: "279f7bee577152a83efe6a53a5cdb44d6e18cebc",
  sourceArtifactSha256: "0de3ed5542962d8203e15e654bc2e908dc659745b72b55c5c1518075f94f3522",
  sourceImageSha256: "41f2b2117667fb92f6a4eb703822b1893e29c985be2e14f7b20fbda103b66cf2",
  caseId: "indonesia",
  visitorId: "rc-b-release-20261010-4f125a9",
  handoffTtlMs: 8 * 60 * 1000,
  sessionTtlMs: 30 * 60 * 1000
});

const SESSION_COOKIE = "geokit_consumer_replay";
const STAGES = Object.freeze({
  REGISTERED: 1,
  RESULT_RECEIVED: 2,
  RESULT_INSTALLED: 3,
  MAP_READY: 4,
  KML_TRIGGERED: 5,
  EVIDENCE_ACCEPTED: 6
});
const FAILURE_STAGES = new Set(["RECEIVE_FAILED", "RESULT_INSTALL_FAILED", "MAP_FAILED", "KML_FAILED", "EVIDENCE_POST_FAILED"]);

function fail(code, httpStatus = 422) {
  const error = new Error(code);
  error.code = code;
  error.httpStatus = httpStatus;
  throw error;
}

function safeText(value, max = 256) {
  const text = String(value || "").trim();
  return text && text.length <= max && !/[\u0000-\u001f\u007f]/u.test(text) ? text : "";
}

function safeCode(value) {
  const code = safeText(value, 160);
  return /^[A-Z0-9_:-]+$/u.test(code) ? code : "";
}

function sha256(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function randomToken() {
  return crypto.randomBytes(32).toString("base64url");
}

function parseCookies(value) {
  return String(value || "").split(";").reduce((result, entry) => {
    const index = entry.indexOf("=");
    if (index > 0) result[entry.slice(0, index).trim()] = entry.slice(index + 1).trim();
    return result;
  }, {});
}

function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonical(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function finitePosition(value) {
  return Array.isArray(value) && value.length >= 2
    && Number.isFinite(Number(value[0])) && Number(value[0]) >= -180 && Number(value[0]) <= 180
    && Number.isFinite(Number(value[1])) && Number(value[1]) >= -90 && Number(value[1]) <= 90;
}

function validGeometry(geometry) {
  const type = safeText(geometry?.type, 40);
  const coordinates = geometry?.coordinates;
  if (!Array.isArray(coordinates)) return false;
  if (type === "Point") return finitePosition(coordinates);
  if (type === "MultiPoint" || type === "LineString") return coordinates.length > 0 && coordinates.every(finitePosition);
  if (type === "Polygon") {
    return coordinates.length === 1 && Array.isArray(coordinates[0])
      && coordinates[0].length >= 3 && coordinates[0].every(finitePosition);
  }
  if (type === "MultiPolygon") {
    return coordinates.length > 0 && coordinates.every(polygon => Array.isArray(polygon)
      && polygon.length === 1 && Array.isArray(polygon[0])
      && polygon[0].length >= 3 && polygon[0].every(finitePosition));
  }
  return false;
}

function normalizeIdentity(value) {
  const resultId = safeText(value?.resultId, 160);
  const resultRevision = Number(value?.resultRevision);
  const geometryHash = safeText(value?.geometryHash, 160);
  return resultId && Number.isSafeInteger(resultRevision) && resultRevision > 0 && geometryHash
    ? Object.freeze({ resultId, resultRevision, geometryHash })
    : null;
}

function safeCodeList(value) {
  return Object.freeze(Array.isArray(value)
    ? [...new Set(value.map(safeCode).filter(Boolean))]
    : []);
}

function validateSourceArtifact(buffer, suppliedSha256) {
  const digest = sha256(buffer);
  if (safeText(suppliedSha256, 64).toLowerCase() !== COORDINATE_CONSUMER_REPLAY.sourceArtifactSha256
    || digest !== COORDINATE_CONSUMER_REPLAY.sourceArtifactSha256) {
    fail("COORDINATE_CONSUMER_REPLAY_ARTIFACT_HASH_MISMATCH", 409);
  }
  let artifact;
  try {
    artifact = JSON.parse(buffer.toString("utf8").replace(/^\uFEFF/u, ""));
  } catch {
    fail("COORDINATE_CONSUMER_REPLAY_ARTIFACT_JSON_INVALID");
  }
  const cases = Array.isArray(artifact?.cases) ? artifact.cases : [];
  const record = cases.length === 1 ? cases[0] : null;
  const finalized = record?.finalizedResult;
  const sourceIdentity = normalizeIdentity(finalized);
  if (artifact?.schemaVersion !== "coordinate_three_case_validated_results_v1"
    || artifact?.runId !== COORDINATE_CONSUMER_REPLAY.sourceRunId
    || artifact?.batchId !== COORDINATE_CONSUMER_REPLAY.sourceBatchId
    || artifact?.expectedCommit !== COORDINATE_CONSUMER_REPLAY.sourceCommit
    || record?.schemaVersion !== "coordinate_three_case_validated_result_v1"
    || record?.caseId !== COORDINATE_CONSUMER_REPLAY.caseId
    || record?.imageSha256 !== COORDINATE_CONSUMER_REPLAY.sourceImageSha256
    || record?.terminalStatus !== "SUCCEEDED" || record?.success !== true
    || finalized?.identityPresent !== true || finalized?.currentRevisionMatch !== true
    || !sourceIdentity || finalized?.crs?.id !== "EPSG:4326"
    || finalized?.crs?.axisOrder !== "longitude_latitude"
    || finalized?.technicalKmlReady !== true || !validGeometry(finalized?.geometry)) {
    fail("COORDINATE_CONSUMER_REPLAY_ARTIFACT_CONTRACT_MISMATCH", 409);
  }
  return Object.freeze({ artifact, record, finalized, sourceIdentity, digest });
}

function replayFinalizedResult(source, identity) {
  return Object.freeze({
    schemaVersion: safeText(source?.schemaVersion, 160) || "finalized_coordinate_result_v1",
    ...identity,
    sourceAuthority: "historical_consumer_replay",
    coordinateType: safeText(source?.coordinateType, 160),
    precisionMode: safeText(source?.precisionMode, 160) || null,
    family: safeText(source?.family, 160),
    crs: structuredClone(source.crs),
    sourceCrs: source?.sourceCrs ? structuredClone(source.sourceCrs) : null,
    geometry: structuredClone(source.geometry),
    confirmationStatus: "pending",
    qualityGateStatus: "review_required",
    decisionState: "REVIEW_REQUIRED",
    requiresReview: true,
    technicalKmlReady: true,
    finalizedKmlReady: true,
    mapReady: true,
    kmlReady: true,
    reasonCodes: Object.freeze(["HISTORICAL_RESULT_CONSUMER_REPLAY_ONLY"]),
    blockingReasons: Object.freeze([]),
    warnings: Object.freeze(["UNVERIFIED_HISTORICAL_RESULT"]),
    limitations: safeCodeList(source?.limitations)
  });
}

function replayPayload(finalized) {
  const identity = normalizeIdentity(finalized);
  return Object.freeze({
    success: true,
    finalizedCoordinateResult: finalized,
    outputCapabilities: Object.freeze({
      technicallyGeneratable: true,
      mapReady: true,
      kmlReady: true,
      ...identity,
      blockReasons: Object.freeze([]),
      warningReasons: Object.freeze(["HISTORICAL_RESULT_CONSUMER_REPLAY_ONLY"])
    }),
    acquisitionStatus: "HISTORICAL_REPLAY",
    authorizationStatus: "REVIEW_REQUIRED",
    resultStatus: "needs_review",
    requiresReview: true,
    boundaryBlocked: true,
    mapReady: true,
    kmlReady: true,
    recognitionUsageConsumed: null,
    mapStatus: "ENABLED",
    kmlStatus: "ENABLED",
    reviewReasons: Object.freeze(["HISTORICAL_RESULT_CONSUMER_REPLAY_ONLY"]),
    contractReasons: Object.freeze([])
  });
}

export function createCoordinateConsumerReplay({
  enabled = false,
  expectedOrigin,
  exactVisitorId = COORDINATE_CONSUMER_REPLAY.visitorId,
  readExistingUserAccess,
  selectedMapProvider,
  registerFinalizedResult,
  revokeFinalizedResult,
  now = () => Date.now(),
  token = randomToken,
  uuid = () => crypto.randomUUID()
} = {}) {
  const sessions = new Map();
  let entry = null;
  let closed = false;
  let terminalFailure = null;

  function requireEnabled() {
    if (enabled !== true || closed) fail("COORDINATE_CONSUMER_REPLAY_DISABLED", 404);
  }

  function prune() {
    const current = now();
    for (const [key, value] of sessions) if (value.expiresAt <= current) sessions.delete(key);
  }

  async function createSession({ visitorId, origin } = {}) {
    requireEnabled();
    prune();
    if (safeText(origin, 300) !== expectedOrigin) fail("COORDINATE_CONSUMER_REPLAY_ORIGIN_REJECTED", 403);
    if (safeText(visitorId, 160) !== exactVisitorId) fail("COORDINATE_CONSUMER_REPLAY_VISITOR_REJECTED", 403);
    if (String(selectedMapProvider?.() || "").toUpperCase() !== "OPENFREEMAP") {
      fail("COORDINATE_CONSUMER_REPLAY_MAP_PROVIDER_REJECTED", 409);
    }
    const access = await readExistingUserAccess?.(exactVisitorId);
    if (access?.existingUser !== true || access?.permissions?.kmlExportEnabled !== true) {
      fail("COORDINATE_CONSUMER_REPLAY_PERMISSION_REJECTED", 403);
    }
    const cookieValue = token();
    const csrfToken = token();
    const expiresAt = now() + COORDINATE_CONSUMER_REPLAY.sessionTtlMs;
    sessions.set(cookieValue, Object.freeze({ visitorId: exactVisitorId, csrfToken, expiresAt }));
    return Object.freeze({ cookieName: SESSION_COOKIE, cookieValue, csrfToken, visitorId: exactVisitorId,
      expiresAt: new Date(expiresAt).toISOString(), mapProvider: "OPENFREEMAP" });
  }

  function authenticate(auth = {}) {
    requireEnabled();
    prune();
    if (safeText(auth.origin, 300) !== expectedOrigin) fail("COORDINATE_CONSUMER_REPLAY_ORIGIN_REJECTED", 403);
    const cookie = parseCookies(auth.cookieHeader)[SESSION_COOKIE];
    const session = cookie ? sessions.get(cookie) : null;
    if (!session || session.expiresAt <= now()) fail("COORDINATE_CONSUMER_REPLAY_BROWSER_SESSION_REQUIRED", 401);
    if (!safeText(auth.csrfToken, 256) || auth.csrfToken !== session.csrfToken) {
      fail("COORDINATE_CONSUMER_REPLAY_CSRF_REJECTED", 403);
    }
    return session;
  }

  function hasReadySession() {
    if (enabled !== true || closed) return false;
    prune();
    return [...sessions.values()].some(session => session.visitorId === exactVisitorId && session.expiresAt > now());
  }

  function register({ artifactBase64, artifactSha256 } = {}) {
    requireEnabled();
    if (terminalFailure) fail("COORDINATE_CONSUMER_REPLAY_TERMINAL_FAILURE", 409);
    if (entry) fail("COORDINATE_CONSUMER_REPLAY_ALREADY_REGISTERED", 409);
    const encoded = safeText(artifactBase64, 96 * 1024);
    if (!encoded || !/^[A-Za-z0-9+/]+={0,2}$/u.test(encoded)) fail("COORDINATE_CONSUMER_REPLAY_ARTIFACT_ENCODING_INVALID");
    const buffer = Buffer.from(encoded, "base64");
    if (!buffer.length || buffer.length > 64 * 1024 || buffer.toString("base64") !== encoded) {
      fail("COORDINATE_CONSUMER_REPLAY_ARTIFACT_ENCODING_INVALID");
    }
    const source = validateSourceArtifact(buffer, artifactSha256);
    const resultId = uuid();
    const resultRevision = 1;
    const geometryHash = createGeometryHash(source.finalized.geometry);
    const identity = Object.freeze({ resultId, resultRevision, geometryHash });
    const finalized = replayFinalizedResult(source.finalized, identity);
    const registeredFinalized = registerFinalizedResult?.(finalized);
    const registeredIdentity = normalizeIdentity(registeredFinalized);
    if (!registeredIdentity || registeredIdentity.resultId !== identity.resultId
      || registeredIdentity.resultRevision !== identity.resultRevision
      || registeredIdentity.geometryHash !== identity.geometryHash) {
      fail("COORDINATE_CONSUMER_REPLAY_FINALIZED_REGISTRATION_FAILED", 503);
    }
    const bindingDigest = sha256(Buffer.from(canonical({
      runId: COORDINATE_CONSUMER_REPLAY.runId,
      batchId: COORDINATE_CONSUMER_REPLAY.batchId,
      caseId: COORDINATE_CONSUMER_REPLAY.caseId,
      sourceArtifactSha256: source.digest,
      identity
    }), "utf8"));
    const current = now();
    const expiresAt = current + COORDINATE_CONSUMER_REPLAY.handoffTtlMs;
    entry = {
      identity,
      payload: replayPayload(registeredFinalized),
      bindingDigest,
      sourceArtifactSha256: source.digest,
      expiresAt,
      consumed: false,
      lastStage: "REGISTERED",
      lastStageAt: current,
      failed: false,
      failureCode: null,
      consumerEvidence: null
    };
    return Object.freeze({ registered: true, runId: COORDINATE_CONSUMER_REPLAY.runId,
      batchId: COORDINATE_CONSUMER_REPLAY.batchId, caseId: COORDINATE_CONSUMER_REPLAY.caseId,
      expiresAt: new Date(expiresAt).toISOString(), artifactSha256: source.digest,
      bindingDigest });
  }

  function status(auth) {
    authenticate(auth);
    const ready = Boolean(entry && !terminalFailure && !entry.failed && !entry.consumed && entry.expiresAt > now());
    return Object.freeze({ ready, caseId: entry ? COORDINATE_CONSUMER_REPLAY.caseId : null,
      expiresAt: entry ? new Date(entry.expiresAt).toISOString() : null,
      lastStage: terminalFailure?.stage || entry?.lastStage || "WAITING_FOR_REGISTRATION",
      failureCode: terminalFailure?.failureCode || entry?.failureCode || null });
  }

  function receive(auth) {
    const session = authenticate(auth);
    if (!entry || terminalFailure || entry.failed || entry.consumed || entry.expiresAt <= now()) {
      fail("COORDINATE_CONSUMER_REPLAY_UNAVAILABLE", 410);
    }
    entry.consumed = true;
    entry.lastStage = "RESULT_RECEIVED";
    entry.lastStageAt = now();
    return Object.freeze({ accepted: true, runId: COORDINATE_CONSUMER_REPLAY.runId,
      batchId: COORDINATE_CONSUMER_REPLAY.batchId, caseId: COORDINATE_CONSUMER_REPLAY.caseId,
      visitorId: session.visitorId, bindingDigest: entry.bindingDigest,
      resultIdentity: structuredClone(entry.identity), result: structuredClone(entry.payload),
      expiresAt: new Date(entry.expiresAt).toISOString(), replayOnly: true, businessQuotaBypassed: true });
  }

  function recordStage(auth, value = {}) {
    authenticate(auth);
    const stage = safeCode(value.stage);
    if (FAILURE_STAGES.has(stage)) {
      if (terminalFailure) fail("COORDINATE_CONSUMER_REPLAY_TERMINAL_FAILURE", 409);
      if (!entry) {
        if (stage !== "RECEIVE_FAILED" || safeText(value.bindingDigest, 64)) {
          fail("COORDINATE_CONSUMER_REPLAY_UNAVAILABLE", 410);
        }
      } else {
        if (entry.expiresAt <= now()) fail("COORDINATE_CONSUMER_REPLAY_UNAVAILABLE", 410);
        const suppliedBinding = safeText(value.bindingDigest, 64);
        if (suppliedBinding && suppliedBinding !== entry.bindingDigest) {
          fail("COORDINATE_CONSUMER_REPLAY_BINDING_MISMATCH", 409);
        }
        if (!suppliedBinding && stage !== "RECEIVE_FAILED") {
          fail("COORDINATE_CONSUMER_REPLAY_BINDING_MISMATCH", 409);
        }
      }
      const failureCode = safeCode(value.failureCode) || stage;
      terminalFailure = Object.freeze({ stage, failureCode, at: now() });
      if (entry) {
        entry.failed = true;
        entry.lastStage = stage;
        entry.failureCode = failureCode;
        entry.lastStageAt = terminalFailure.at;
      }
      return Object.freeze({ accepted: true, stage, terminal: true });
    }
    if (terminalFailure || !entry || entry.failed || !entry.consumed || entry.expiresAt <= now()) {
      fail("COORDINATE_CONSUMER_REPLAY_UNAVAILABLE", 410);
    }
    if (safeText(value.bindingDigest, 64) !== entry.bindingDigest) fail("COORDINATE_CONSUMER_REPLAY_BINDING_MISMATCH", 409);
    if (STAGES[stage] && STAGES[stage] >= (STAGES[entry.lastStage] || 0)) {
      entry.lastStage = stage;
    } else {
      fail("COORDINATE_CONSUMER_REPLAY_STAGE_REJECTED", 409);
    }
    entry.lastStageAt = now();
    return Object.freeze({ accepted: true, stage: entry.lastStage });
  }

  function recordConsumerEvidence(auth, value = {}) {
    authenticate(auth);
    if (!entry || terminalFailure || entry.failed || !entry.consumed || entry.expiresAt <= now()) {
      fail("COORDINATE_CONSUMER_REPLAY_UNAVAILABLE", 410);
    }
    if (value?.schemaVersion !== "coordinate_consumer_replay_browser_evidence_v1"
      || value?.runId !== COORDINATE_CONSUMER_REPLAY.runId
      || value?.batchId !== COORDINATE_CONSUMER_REPLAY.batchId
      || value?.caseId !== COORDINATE_CONSUMER_REPLAY.caseId
      || safeText(value?.bindingDigest, 64) !== entry.bindingDigest
      || value?.replayOnly !== true || value?.businessQuotaBypassed !== true
      || value?.resultInstalled !== true || value?.resultIdentityMatch !== true
      || value?.mapState !== "READY" || value?.mapProvider !== "OPENFREEMAP"
      || value?.mapIdentityMatch !== true || Number(value?.authorityMutationCount) !== 0
      || value?.kmlDownloadTriggered !== true || value?.kmlIdentityMatch !== true
      || value?.downloadFileName !== "coordinates.kml") {
      fail("COORDINATE_CONSUMER_REPLAY_EVIDENCE_REJECTED", 422);
    }
    if (entry.consumerEvidence) fail("COORDINATE_CONSUMER_REPLAY_EVIDENCE_ALREADY_RECORDED", 409);
    entry.consumerEvidence = Object.freeze({
      schemaVersion: "coordinate_consumer_replay_browser_evidence_v1",
      runId: COORDINATE_CONSUMER_REPLAY.runId,
      batchId: COORDINATE_CONSUMER_REPLAY.batchId,
      caseId: COORDINATE_CONSUMER_REPLAY.caseId,
      bindingDigest: entry.bindingDigest,
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
    });
    entry.lastStage = "EVIDENCE_ACCEPTED";
    entry.lastStageAt = now();
    entry.failureCode = null;
    return Object.freeze({ accepted: true });
  }

  function readConsumerEvidence() {
    if (closed) return Object.freeze({ ready: false, lastStage: "CLOSED", failureCode: terminalFailure?.failureCode || entry?.failureCode || null, evidence: null });
    if (terminalFailure) return Object.freeze({ ready: false, lastStage: terminalFailure.stage,
      failureCode: terminalFailure.failureCode, evidence: null });
    if (!entry) return Object.freeze({ ready: false, lastStage: "WAITING_FOR_REGISTRATION", failureCode: null, evidence: null });
    return Object.freeze({
      ready: Boolean(!entry.failed && entry.expiresAt > now() && entry.consumerEvidence),
      expiresAt: new Date(entry.expiresAt).toISOString(),
      lastStage: entry.lastStage,
      failureCode: entry.failureCode,
      evidence: entry.consumerEvidence ? structuredClone(entry.consumerEvidence) : null
    });
  }

  function close() {
    requireEnabled();
    closed = true;
    sessions.clear();
    if (entry) {
      revokeFinalizedResult?.(entry.identity);
      entry.lastStage = "CLOSED";
      entry.lastStageAt = now();
    }
    return Object.freeze({ closed: true, runId: COORDINATE_CONSUMER_REPLAY.runId });
  }

  function inspectIdentity(value) {
    const candidate = normalizeIdentity(value);
    const matched = Boolean(entry && candidate
      && candidate.resultId === entry.identity.resultId
      && candidate.resultRevision === entry.identity.resultRevision
      && candidate.geometryHash === entry.identity.geometryHash);
    if (!matched) return Object.freeze({ matched: false, ok: false, code: null, httpStatus: 404 });
    if (closed) return Object.freeze({ matched: true, ok: false, code: "COORDINATE_CONSUMER_REPLAY_CLOSED", httpStatus: 410 });
    if (terminalFailure || entry.failed) {
      return Object.freeze({ matched: true, ok: false, code: "COORDINATE_CONSUMER_REPLAY_TERMINAL_FAILURE", httpStatus: 409 });
    }
    if (entry.expiresAt <= now()) {
      return Object.freeze({ matched: true, ok: false, code: "COORDINATE_CONSUMER_REPLAY_EXPIRED", httpStatus: 410 });
    }
    if (!entry.consumed) {
      return Object.freeze({ matched: true, ok: false, code: "COORDINATE_CONSUMER_REPLAY_NOT_CONSUMED", httpStatus: 409 });
    }
    return Object.freeze({ matched: true, ok: true, code: null, httpStatus: 200 });
  }

  return Object.freeze({
    runId: COORDINATE_CONSUMER_REPLAY.runId,
    batchId: COORDINATE_CONSUMER_REPLAY.batchId,
    exactVisitorId,
    createSession,
    hasReadySession,
    register,
    status,
    receive,
    recordStage,
    recordConsumerEvidence,
    readConsumerEvidence,
    inspectIdentity,
    close
  });
}
