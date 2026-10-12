import crypto from "node:crypto";

const SESSION_COOKIE = "geokit_rc_r8";
const HANDOFF_TTL_MS = 8 * 60 * 1000;
const SESSION_TTL_MS = 30 * 60 * 1000;
const RESULT_MAX_AGE_MS = 8 * 60 * 1000;
const EXACT_VISITOR_ID = "rc-b-release-20261010-4f125a9";

function fail(code, httpStatus = 422) {
  const error = new Error(code);
  error.code = code;
  error.httpStatus = httpStatus;
  throw error;
}
function token() {
  return crypto.randomBytes(32).toString("base64url");
}

function sha256(value) {
  return crypto.createHash("sha256").update(String(value || ""), "utf8").digest("hex");
}

function safeText(value, max = 256) {
  const text = String(value || "").trim();
  return text && text.length <= max && !/[\u0000-\u001f\u007f]/u.test(text) ? text : "";
}

function safeUuid(value) {
  const text = safeText(value, 64).toLowerCase();
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u.test(text) ? text : "";
}

function parseCookies(value) {
  return String(value || "").split(";").reduce((result, entry) => {
    const index = entry.indexOf("=");
    if (index <= 0) return result;
    const name = entry.slice(0, index).trim();
    const cookieValue = entry.slice(index + 1).trim();
    if (name) result[name] = cookieValue;
    return result;
  }, {});
}

function normalizeIdentity(value) {
  const resultId = safeText(value?.resultId || value?.sourceResultId, 160);
  const resultRevision = Number(value?.resultRevision ?? value?.sourceRevision);
  const geometryHash = safeText(value?.geometryHash || value?.sourceGeometryHash, 160);
  return resultId && Number.isInteger(resultRevision) && resultRevision > 0 && geometryHash
    ? Object.freeze({ resultId, resultRevision, geometryHash })
    : null;
}

function identityMatches(left, right) {
  return Boolean(left && right
    && left.resultId === right.resultId
    && left.resultRevision === right.resultRevision
    && left.geometryHash === right.geometryHash);
}

function terminalJobResult(job) {
  if (!job || job.status !== "SUCCEEDED" || job.result?.success !== true) return null;
  const response = job.result && typeof job.result === "object" ? job.result : null;
  const finalized = response.finalizedCoordinateResult;
  const identity = normalizeIdentity(finalized?.resultIdentity || finalized);
  if (!finalized || !identity || finalized.technicalKmlReady !== true) return null;
  return { response, finalized, identity, binding: job.rcBinding, completedAt: job.completedAt, expiresAt: job.expiresAt };
}

function safeCodeList(value) {
  return Object.freeze(Array.isArray(value)
    ? [...new Set(value.map(item => safeText(item, 160)).filter(item => /^[A-Za-z0-9_.:-]+$/u.test(item)))]
    : []);
}

function safeFinalizedResult(value) {
  return Object.freeze({
    schemaVersion: safeText(value?.schemaVersion, 160),
    resultId: safeText(value?.resultId, 160),
    resultRevision: Number(value?.resultRevision),
    geometryHash: safeText(value?.geometryHash, 160),
    sourceAuthority: safeText(value?.sourceAuthority, 160) || null,
    coordinateType: safeText(value?.coordinateType, 160) || null,
    precisionMode: safeText(value?.precisionMode, 160) || null,
    family: safeText(value?.family, 160) || null,
    crs: value?.crs ? structuredClone(value.crs) : null,
    geometry: value?.geometry ? structuredClone(value.geometry) : null,
    confirmationStatus: safeText(value?.confirmationStatus, 80) || null,
    qualityGateStatus: safeText(value?.qualityGateStatus, 80) || null,
    decisionState: safeText(value?.decisionState, 80) || null,
    technicalKmlReady: value?.technicalKmlReady === true,
    mapReady: value?.mapReady !== false,
    requiresReview: value?.requiresReview === true,
    kmlReady: value?.kmlReady === true,
    reasonCodes: safeCodeList(value?.reasonCodes),
    blockingReasons: safeCodeList(value?.blockingReasons),
    warnings: safeCodeList(value?.warnings),
    limitations: safeCodeList(value?.limitations)
  });
}

function safeOutputCapabilities(value) {
  if (!value || typeof value !== "object") return null;
  return Object.freeze({
    technicallyGeneratable: value.technicallyGeneratable === true,
    mapReady: value.mapReady === true,
    kmlReady: value.kmlReady === true,
    resultId: safeText(value.resultId, 160),
    resultRevision: Number(value.resultRevision),
    geometryHash: safeText(value.geometryHash, 160),
    blockReasons: safeCodeList(value.blockReasons),
    warningReasons: safeCodeList(value.warningReasons)
  });
}

function browserResultPayload(response, finalized) {
  return Object.freeze({
    success: true,
    requestId: safeUuid(response?.requestId),
    finalizedCoordinateResult: safeFinalizedResult(finalized),
    outputCapabilities: safeOutputCapabilities(response?.outputCapabilities),
    acquisitionStatus: safeText(response?.acquisitionStatus, 80),
    authorizationStatus: safeText(response?.authorizationStatus, 80),
    resultStatus: safeText(response?.resultStatus, 80),
    requiresReview: response?.requiresReview === true,
    boundaryBlocked: response?.boundaryBlocked === true,
    mapReady: response?.mapReady === true,
    kmlReady: response?.kmlReady === true,
    recognitionUsageConsumed: typeof response?.recognitionUsageConsumed === "boolean"
      ? response.recognitionUsageConsumed
      : typeof response?.usageConsumed === "boolean" ? response.usageConsumed : null,
    mapStatus: safeText(response?.mapStatus, 80),
    kmlStatus: safeText(response?.kmlStatus, 80),
    reviewReasons: safeCodeList(response?.reviewReasons),
    contractReasons: safeCodeList(response?.contractReasons)
  });
}

function safeDeadline(value, now) {
  const parsed = Date.parse(String(value || ""));
  return Number.isFinite(parsed) && parsed > now ? parsed : null;
}

export function createCoordinateThreeCaseRcBrowserHandoff({
  runId,
  batchId,
  exactVisitorId = EXACT_VISITOR_ID,
  expectedOrigin = "",
  caseDefinitions = {},
  now = () => Date.now(),
  randomToken = token,
  readRunStatus = async () => ({}),
  readExistingUserAccess = async () => null,
  readJob = () => null,
  validateFinalizedIdentity = () => ({ ok: false }),
  selectedMapProvider = () => "UNKNOWN",
  claimUpload = async () => fail("RC_R8_CLAIM_UNAVAILABLE", 503)
} = {}) {
  const sessions = new Map();
  const handoffs = new Map();
  let sessionCreated = false;
  let releaseAttempted = false;
  let clientDeadline = null;
  let upload = null;
  let refreshPromise = null;
  let stopped = false;

  function assertOpen() {
    if (stopped || (clientDeadline !== null && now() >= clientDeadline)) fail("RC_R8_CLOSED_OR_EXPIRED", 410);
    if (upload?.stage === "FAILED") fail(upload.failureCode || "RC_R8_FAILED", 409);
  }

  async function releaseUpload({ expiresAt } = {}) {
    assertOpen();
    const deadline = safeDeadline(expiresAt, now());
    if (releaseAttempted || !hasReadySession() || !deadline || deadline > now() + 15 * 60 * 1000) {
      fail("RC_R8_RELEASE_REJECTED", 409);
    }
    releaseAttempted = true;
    const run = await readRunStatus();
    assertOpen();
    if (run?.runStatus !== "ACTIVE" || run?.runClosed || !safeDeadline(run?.deadlineAt, now())) fail("RC_R8_RUN_NOT_ACTIVE", 409);
    clientDeadline = Math.min(deadline, Date.parse(run.deadlineAt));
    return Object.freeze({ released: true, runId, expiresAt: new Date(clientDeadline).toISOString() });
  }

  // The browser never receives the RC claim capability. The existing jobs route
  // consumes the single multipart upload and forwards that capability internally.
  async function beginUpload(auth, { imageBuffer, recognitionRequestId, visitorId } = {}) {
    const session = authenticate(auth);
    const requestId = safeUuid(recognitionRequestId);
    if (!clientDeadline || upload || !Buffer.isBuffer(imageBuffer) || !requestId
      || visitorId !== exactVisitorId || session.visitorId !== exactVisitorId
      || crypto.createHash("sha256").update(imageBuffer).digest("hex") !== caseDefinitions?.indonesia?.imageSha256) {
      fail("RC_R8_UPLOAD_REJECTED", 409);
    }
    upload = { requestId, stage: "CLAIMING", job: null, failureCode: null };
    try {
      const claim = await claimUpload({ caseId: "indonesia", imageSha256: caseDefinitions.indonesia.imageSha256,
        recognitionRequestId: requestId, visitorId: exactVisitorId });
      assertOpen();
      if (!claim?.claimToken) fail("RC_R8_CLAIM_REJECTED", 409);
      upload.stage = "UPLOADED";
      return { authorization: `Bearer ${claim.claimToken}`, caseId: "indonesia", productMode: caseDefinitions.indonesia.productMode };
    } catch (error) {
      upload.stage = "FAILED";
      upload.failureCode = "RC_R8_UPLOAD_ADMISSION_FAILED";
      throw error;
    }
  }

  function attachJob(job, requestId) {
    assertOpen();
    if (!upload || upload.stage !== "UPLOADED" || upload.requestId !== requestId
      || !job?.jobId || !job?.jobAccessToken) fail("RC_R8_JOB_BINDING_REJECTED", 409);
    upload.job = { jobId: job.jobId, jobAccessToken: job.jobAccessToken };
    upload.stage = "RUNNING";
  }

  async function refreshUpload() {
    assertOpen();
    if (!upload?.job || upload.stage !== "RUNNING") return;
    if (refreshPromise) return refreshPromise;
    refreshPromise = (async () => {
      const job = readJob(upload.job.jobId, upload.job.jobAccessToken);
      if (!job) fail("RC_R8_JOB_UNAVAILABLE", 410);
      if (["QUEUED", "RUNNING"].includes(job.status)) return;
      if (job.status !== "SUCCEEDED") fail("RC_R8_RECOGNITION_FAILED", 422);
      await register({ caseId: "indonesia", ...upload.job, recognitionRequestId: upload.requestId });
      assertOpen();
      upload.stage = "RESULT_READY";
    })().catch(error => {
      upload.stage = "FAILED";
      upload.failureCode = /^[A-Z0-9_]+$/u.test(error?.code || "") ? error.code : "RC_R8_RESULT_FAILED";
    }).finally(() => { refreshPromise = null; });
    return refreshPromise;
  }

  async function readFreshResult() {
    await refreshUpload();
    const stage = upload?.stage || (clientDeadline ? "WAITING_UPLOAD" : "WAITING_RELEASE");
    if (stage !== "RESULT_READY") return { ready: false, stage, code: upload?.failureCode || null };
    const job = readJob(upload.job.jobId, upload.job.jobAccessToken);
    const terminal = terminalJobResult(job);
    if (!terminal) fail("RC_R8_RESULT_UNAVAILABLE", 410);
    return { ready: true, stage, expiresAt: new Date(handoffs.get(`${runId}:indonesia`).expiresAt).toISOString(),
      snapshot: { status: job.status, httpStatus: job.httpStatus, requestId: upload.requestId,
        result: { ...browserResultPayload(terminal.response, terminal.finalized),
          providerModel: safeText(terminal.response.providerModel || terminal.response.model, 80),
          model: safeText(terminal.response.model || terminal.response.providerModel, 80),
          providerCallCount: terminal.response.providerCallCount,
          providerCompletionState: safeText(terminal.response.providerCompletionState, 80) } } };
  }

  function prune() {
    const current = now();
    for (const [key, value] of sessions) if (value.expiresAt <= current) sessions.delete(key);
  }

  async function createSession({ visitorId, origin } = {}) {
    assertOpen();
    if (sessionCreated) fail("RC_R8_BROWSER_AUTH_ALREADY_USED", 409);
    prune();
    if (safeText(visitorId) !== exactVisitorId) fail("COORDINATE_THREE_CASE_RC_BROWSER_VISITOR_REJECTED", 403);
    if (!expectedOrigin || safeText(origin, 512) !== expectedOrigin) {
      fail("COORDINATE_THREE_CASE_RC_BROWSER_ORIGIN_REJECTED", 403);
    }
    if (String(selectedMapProvider() || "").toUpperCase() !== "OPENFREEMAP") {
      fail("COORDINATE_THREE_CASE_RC_BROWSER_MAP_PROVIDER_REJECTED", 503);
    }
    sessionCreated = true;
    const access = await readExistingUserAccess(exactVisitorId);
    if (!access?.existingUser || access?.permissions?.aiOcrEnabled !== true
      || access?.permissions?.kmlExportEnabled !== true
      || !Number.isInteger(access?.convertRemaining) || access.convertRemaining < 1) {
      fail("COORDINATE_THREE_CASE_RC_BROWSER_PERMISSION_REJECTED", 403);
    }
    const status = await readRunStatus();
    if (status?.runStatus !== "ACTIVE" || status?.runClosed === true) {
      fail("COORDINATE_THREE_CASE_RC_BROWSER_RUN_NOT_ACTIVE", 409);
    }
    assertOpen();
    const current = now();
    const runDeadline = safeDeadline(status?.deadlineAt, current);
    if (!runDeadline) fail("COORDINATE_THREE_CASE_RC_BROWSER_RUN_DEADLINE_UNAVAILABLE", 503);
    const expiresAt = Math.min(current + SESSION_TTL_MS, runDeadline);
    const sessionToken = randomToken();
    const csrfToken = randomToken();
    if (!sessionToken || !csrfToken) fail("COORDINATE_THREE_CASE_RC_BROWSER_TOKEN_FAILED", 500);
    sessions.set(sha256(sessionToken), Object.freeze({
      visitorId: exactVisitorId,
      origin: safeText(origin, 512),
      csrfHash: sha256(csrfToken),
      expiresAt
    }));
    return Object.freeze({
      cookieName: SESSION_COOKIE,
      cookieValue: sessionToken,
      csrfToken,
      expiresAt: new Date(expiresAt).toISOString(),
      visitorId: exactVisitorId,
      mapProvider: "OPENFREEMAP"
    });
  }

  function authenticate({ cookieHeader, csrfToken, origin } = {}) {
    assertOpen();
    prune();
    const cookieToken = parseCookies(cookieHeader)[SESSION_COOKIE] || "";
    const session = sessions.get(sha256(cookieToken));
    if (!session || session.expiresAt <= now()) fail("COORDINATE_THREE_CASE_RC_BROWSER_SESSION_REQUIRED", 401);
    if (!session.origin || safeText(origin, 512) !== session.origin || session.origin !== expectedOrigin) {
      fail("COORDINATE_THREE_CASE_RC_BROWSER_ORIGIN_REJECTED", 403);
    }
    if (sha256(csrfToken) !== session.csrfHash) fail("COORDINATE_THREE_CASE_RC_BROWSER_CSRF_REJECTED", 403);
    return session;
  }

  function hasReadySession() {
    if (stopped || (clientDeadline !== null && now() >= clientDeadline)) return false;
    prune();
    const current = now();
    return [...sessions.values()].some(session => session.visitorId === exactVisitorId
      && session.origin === expectedOrigin
      && session.expiresAt > current);
  }

  async function register({ caseId, jobId, jobAccessToken, recognitionRequestId } = {}) {
    assertOpen();
    prune();
    if (safeText(caseId) !== "indonesia" || !safeText(jobId, 160) || !safeText(jobAccessToken, 256)) {
      fail("COORDINATE_THREE_CASE_RC_BROWSER_HANDOFF_BINDING_INVALID");
    }
    const terminal = terminalJobResult(readJob(jobId, jobAccessToken));
    if (!terminal) fail("COORDINATE_THREE_CASE_RC_BROWSER_JOB_NOT_CONSUMABLE", 409);
    const definition = caseDefinitions?.indonesia;
    const binding = terminal.binding;
    if (!definition || !binding
      || binding.runId !== runId || binding.batchId !== batchId
      || binding.caseId !== "indonesia" || binding.visitorId !== exactVisitorId
      || binding.imageSha256 !== definition.imageSha256
      || binding.productMode !== definition.productMode
      || binding.recognitionRequestId !== safeUuid(recognitionRequestId)) {
      fail("COORDINATE_THREE_CASE_RC_BROWSER_JOB_BINDING_MISMATCH", 409);
    }
    const responseRequestId = safeUuid(terminal.response?.recognitionRequestId || terminal.response?.requestId);
    if (!responseRequestId || responseRequestId !== safeUuid(recognitionRequestId)) {
      fail("COORDINATE_THREE_CASE_RC_BROWSER_REQUEST_IDENTITY_MISMATCH", 409);
    }
    const status = await readRunStatus();
    assertOpen();
    const current = now();
    if (status?.runStatus !== "ACTIVE" || status?.runClosed === true) {
      fail("COORDINATE_THREE_CASE_RC_BROWSER_RUN_NOT_ACTIVE", 409);
    }
    const runDeadline = safeDeadline(status?.deadlineAt, current);
    const jobDeadline = Number.isFinite(terminal.expiresAt) && terminal.expiresAt > current ? terminal.expiresAt : null;
    const resultDeadline = Number.isFinite(terminal.completedAt) && terminal.completedAt > 0
      ? terminal.completedAt + RESULT_MAX_AGE_MS
      : null;
    if (!runDeadline || !jobDeadline || !resultDeadline || resultDeadline <= current) {
      fail("COORDINATE_THREE_CASE_RC_BROWSER_HANDOFF_DEADLINE_UNAVAILABLE", 503);
    }
    const currentFinalized = validateFinalizedIdentity(terminal.identity);
    if (currentFinalized?.ok !== true || !identityMatches(terminal.identity, normalizeIdentity(currentFinalized.result))) {
      fail("COORDINATE_THREE_CASE_RC_BROWSER_FINALIZED_IDENTITY_STALE", 409);
    }
    const registryDeadline = Number(currentFinalized.expiresAt);
    if (!Number.isFinite(registryDeadline) || registryDeadline <= current) {
      fail("COORDINATE_THREE_CASE_RC_BROWSER_FINALIZED_IDENTITY_DEADLINE_UNAVAILABLE", 503);
    }
    const expiresAt = Math.min(current + HANDOFF_TTL_MS, runDeadline, jobDeadline, resultDeadline, registryDeadline,
      clientDeadline ?? Infinity);
    if (!Number.isFinite(expiresAt) || expiresAt <= current) fail("COORDINATE_THREE_CASE_RC_BROWSER_HANDOFF_DEADLINE_UNAVAILABLE", 503);
    const key = `${runId}:indonesia`;
    if (handoffs.has(key)) fail("COORDINATE_THREE_CASE_RC_BROWSER_HANDOFF_ALREADY_REGISTERED", 409);
    handoffs.set(key, {
      caseId: "indonesia",
      recognitionRequestId: responseRequestId,
      identity: terminal.identity,
      payload: browserResultPayload(terminal.response, currentFinalized.result),
      expiresAt,
      consumed: false,
      consumerEvidence: null
    });
    return Object.freeze({ registered: true, caseId: "indonesia", expiresAt: new Date(expiresAt).toISOString() });
  }

  async function status(auth) {
    authenticate(auth);
    await refreshUpload();
    const entry = handoffs.get(`${runId}:indonesia`);
    return Object.freeze({
      ready: Boolean(entry && !entry.consumed && entry.expiresAt > now()),
      caseId: entry ? "indonesia" : null,
      stage: upload?.stage || (clientDeadline ? "WAITING_UPLOAD" : "WAITING_RELEASE"),
      code: upload?.failureCode || null,
      expiresAt: new Date(clientDeadline || authenticate(auth).expiresAt).toISOString()
    });
  }

  function receive(auth) {
    const session = authenticate(auth);
    const key = `${runId}:indonesia`;
    const entry = handoffs.get(key);
    if (!entry || entry.consumed || entry.expiresAt <= now()) fail("COORDINATE_THREE_CASE_RC_BROWSER_HANDOFF_UNAVAILABLE", 410);
    const currentFinalized = validateFinalizedIdentity(entry.identity);
    const currentIdentity = normalizeIdentity(currentFinalized?.result);
    if (currentFinalized?.ok !== true || !identityMatches(entry.identity, currentIdentity)) {
      fail("COORDINATE_THREE_CASE_RC_BROWSER_HANDOFF_STALE_IDENTITY", 409);
    }
    entry.consumed = true;
    return Object.freeze({
      accepted: true,
      runId,
      batchId,
      caseId: entry.caseId,
      visitorId: session.visitorId,
      recognitionRequestId: entry.recognitionRequestId,
      resultIdentity: structuredClone(entry.identity),
      expiresAt: new Date(entry.expiresAt).toISOString(),
      result: browserResultPayload(entry.payload, currentFinalized.result)
    });
  }

  function recordConsumerEvidence(auth, value = {}) {
    authenticate(auth);
    const entry = handoffs.get(`${runId}:indonesia`);
    if (!entry || !entry.consumed || entry.expiresAt <= now()) {
      fail("COORDINATE_THREE_CASE_RC_BROWSER_HANDOFF_UNAVAILABLE", 410);
    }
    const currentFinalized = validateFinalizedIdentity(entry.identity);
    if (currentFinalized?.ok !== true || !identityMatches(entry.identity, normalizeIdentity(currentFinalized.result))) {
      fail("COORDINATE_THREE_CASE_RC_BROWSER_HANDOFF_STALE_IDENTITY", 409);
    }
    const expectedDigest = sha256([
      runId,
      entry.caseId,
      entry.recognitionRequestId,
      entry.identity.resultId,
      String(entry.identity.resultRevision),
      entry.identity.geometryHash
    ].join("\n"));
    if (value?.schemaVersion !== "coordinate_three_case_rc_r7_browser_evidence_v1"
      || value?.runId !== runId || value?.caseId !== entry.caseId
      || safeText(value?.bindingDigest, 64) !== expectedDigest
      || value?.resultInstalled !== true || value?.resultIdentityMatch !== true
      || value?.mapState !== "READY" || value?.mapProvider !== "OPENFREEMAP"
      || value?.mapIdentityMatch !== true || Number(value?.authorityMutationCount) !== 0
      || value?.kmlDownloadTriggered !== true || value?.kmlIdentityMatch !== true
      || value?.downloadFileName !== "coordinates.kml") {
      fail("COORDINATE_THREE_CASE_RC_BROWSER_CONSUMER_EVIDENCE_REJECTED", 422);
    }
    if (entry.consumerEvidence) fail("COORDINATE_THREE_CASE_RC_BROWSER_CONSUMER_EVIDENCE_ALREADY_RECORDED", 409);
    entry.consumerEvidence = Object.freeze({
      schemaVersion: "coordinate_three_case_rc_r7_browser_evidence_v1",
      runId,
      caseId: entry.caseId,
      bindingDigest: expectedDigest,
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
    return Object.freeze({ accepted: true });
  }

  function readConsumerEvidence() {
    assertOpen();
    const entry = handoffs.get(`${runId}:indonesia`);
    if (!entry || entry.expiresAt <= now() || !entry.consumerEvidence) {
      return Object.freeze({ ready: false });
    }
    return Object.freeze({ ready: true, evidence: structuredClone(entry.consumerEvidence) });
  }

  function revoke() {
    stopped = true;
    sessions.clear();
    handoffs.clear();
  }

  function reportFailure(auth) {
    authenticate(auth);
    if (!upload) upload = { requestId: "", job: null };
    upload.stage = "FAILED";
    upload.failureCode = "RC_R8_BROWSER_CONSUMER_FAILED";
    handoffs.clear();
    return { accepted: true };
  }

  return Object.freeze({
    cookieName: SESSION_COOKIE,
    exactVisitorId,
    createSession,
    releaseUpload,
    beginUpload,
    attachJob,
    readFreshResult,
    reportFailure,
    hasReadySession,
    authenticate,
    register,
    status,
    receive,
    recordConsumerEvidence,
    readConsumerEvidence,
    revoke,
    identityMatches
  });
}
