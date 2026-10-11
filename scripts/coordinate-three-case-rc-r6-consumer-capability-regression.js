import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import vm from "node:vm";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import { finalizeCoordinateResult } from "../server/coordinate-finalizer/finalized-coordinate-result-v1.js";
import { evaluateRecognitionOutputCapability } from "../server/recognition/recognition-output-capability.js";
import { createRecognitionAcquisitionJobRuntime } from "../server/recognition/recognition-acquisition-job-runtime.js";
import { buildUnchargedCoordinateFailureResponse } from "../server/coordinate-usage-atomicity.js";
import { buildIndonesiaStructuredPreflightDiagnostics, classifyIndonesiaStructuredProviderFailure } from "../server/recognition/indonesia-structured-diagnostics.js";
import {
  INDONESIA_STRUCTURED_PRODUCT_MODE,
  INDONESIA_STRUCTURED_PRODUCT_STATUS,
  INDONESIA_STRUCTURED_ROUTE_STATUS,
  buildIndonesiaStructuredProductBridge,
  selectIndonesiaStructuredProductRoute
} from "../server/recognition/indonesia-structured-product-bridge.js";
import {
  CAPABILITY as INDONESIA_STRUCTURED_PRODUCT_CAPABILITY,
  MODEL as INDONESIA_STRUCTURED_PRODUCT_MODEL,
  STRUCTURED_RESULT_STATUS as INDONESIA_STRUCTURED_PROVIDER_STATUS,
  buildCoordinateStructuredCall,
  classifyCoordinateStructuredProviderResult
} from "../server/recognition/qwen38-coordinate-structured-adapter.mjs";
import { prepareCoordinateProviderRequest, validateCoordinateProviderResponse } from "../server/recognition/model-provider-compatibility.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const serverSource = fs.readFileSync(path.join(root, "server.js"), "utf8");
const actualStructured = JSON.parse(fs.readFileSync(
  path.join(root, "regression-samples", "indonesia-structured-product", "actual-b-structured.json"),
  "utf8"
));
const results = [];

async function test(name, action, evidenceClass = "DYNAMIC") {
  try {
    await action();
    results.push({ name, status: "PASS", evidenceClass });
  } catch (error) {
    results.push({ name, status: "FAIL", evidenceClass, code: error?.message || String(error) });
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

function createResponse() {
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

function createBudget({ providerCountKnown = true } = {}) {
  return {
    requestId: "66666666-6666-4666-8666-666666666666",
    providerAttemptCount: providerCountKnown ? 0 : undefined,
    providerAttempted: false,
    providerCompletionState: "NOT_STARTED",
    providerCostState: "NOT_INCURRED",
    setIngressMetadata() {}, completeIngressUpload() {}, beginExecutionPhase() {}, markHandlerCompleted() {},
    toSanitizedTrace() { return { providerAttemptCount: this.providerAttemptCount }; },
    assertCanContinue() {}, assertCanStartProvider() {}, setAcquisitionRouteReason() {}, recordSkippedStage() {},
    effectiveProviderTimeout() { return 1_000; },
    stageStarted() { return {}; }, stageCompleted() {},
    markProviderAttempted() {
      if (Number.isSafeInteger(this.providerAttemptCount)) this.providerAttemptCount += 1;
      this.providerAttempted = true;
      this.providerCompletionState = "UNKNOWN";
      this.providerCostState = "INCURRED";
    },
    markProviderCompleted() { this.providerCompletionState = "SUCCEEDED"; }
  };
}

function buildReviewFinalizedResult() {
  return finalizeCoordinateResult({
    resultId: "offline-r6-structured-result",
    resultRevision: 1,
    currentRevision: 1,
    sourceAuthority: "coordinate_engine_v2",
    coordinateType: "indonesia_utm50_projected",
    precisionMode: "indonesia-utm50s-structured-projected",
    family: "indonesia_utm50_projected",
    crs: { id: "EPSG:4326", axisOrder: "longitude_latitude" },
    geometry: { type: "Polygon", coordinates: [[[110, -7], [110.1, -7], [110.1, -7.1], [110, -7]]] },
    confirmationStatus: "pending",
    qualityGateStatus: "review_required",
    decisionState: "REVIEW_REQUIRED",
    requiresReview: true,
    technicalKmlReady: true,
    kmlReady: false
  });
}

function createActualStructuredHandler({ finalizedVariant = "valid", providerCountKnown = true } = {}) {
  const budget = createBudget({ providerCountKnown });
  const providerReceipts = [];
  const emptyRoute = Object.freeze({ family: "none", matched: false, reason: "offline", evidence: {} });
  const localOcrDiagnostics = Object.freeze({
    schemaVersion: "local_ocr_diagnostics_v1", phase: "CONTEXT_RETURNED", outcome: "SUCCESS_EMPTY",
    preprocessingMode: "SOURCE_IMAGE", attempted: true, wrapperEntered: true, workerCreateStarted: true,
    workerReady: true, recognizeStarted: true, resultReceived: true, postprocessStarted: true,
    postprocessCompleted: true, contextReturned: true, rawTextPresent: false
  });
  const admission = {
    config: { guardRequired: true },
    validateVisitorEnvelope({ headerVisitorId }) { return { visitorId: headerVisitorId }; },
    getBoundCaseId(candidateBudget) { return candidateBudget === budget ? "indonesia" : null; },
    getControlledModelCrsAcquisitionAuthorization() { return { authorized: true, reasonCode: "NONE" }; },
    async dispatchForBudget() {}, async settleForBudget() {}, recordProviderResultForBudget() {}
  };
  const runtime = vm.createContext({
    AbortController, Blob, Buffer, Date, FormData, JSON, Math, Number, Object, String,
    console: { log() {}, error() {}, warn() {} }, process: { env: {} }, queueMicrotask, setTimeout, clearTimeout,
    coordinateThreeCaseRcAdmission: admission,
    indonesiaStructuredBRcAdmission: { config: { guardRequired: false, ready: false } },
    INDONESIA_STRUCTURED_B_RC: { mode: INDONESIA_STRUCTURED_PRODUCT_MODE },
    INDONESIA_STRUCTURED_PRODUCT_MODE, INDONESIA_STRUCTURED_PRODUCT_STATUS, INDONESIA_STRUCTURED_ROUTE_STATUS,
    INDONESIA_STRUCTURED_PRODUCT_CAPABILITY, INDONESIA_STRUCTURED_PRODUCT_MODEL, INDONESIA_STRUCTURED_PROVIDER_STATUS,
    indonesiaStructuredProductEnabled: true,
    createRecognitionDiagnosticSession() { return { enabled: false, event() {}, operation() {} }; },
    activateRecognitionDeadlineContext() {}, getRecognitionBudget() { return budget; }, getRecognitionDeadlineSignal() { return null; },
    getRegressionTestMode() { return { active: true, rejectReason: null }; },
    createRecognitionAcquisitionEvidenceStore() { return { getOrBuild() { return null; } }; },
    classifyOneShotStructuredFamily() { return emptyRoute; }, createOneShotAcquisitionContract({ route }) { return { route }; },
    createDmsGroupedRetryOrchestrator() { return { claim() { return false; }, snapshot() { return { activeFamilyOwner: null }; } }; },
    createCoordinateUsageCommitController() {
      return { async schedule() { return { success: true, quota: null }; }, async settle() { return { kind: "REGRESSION_TEST" }; } };
    },
    COORDINATE_USAGE_ERROR_CODE: { COMMIT_OUTCOME_UNKNOWN: "USAGE_COMMIT_OUTCOME_UNKNOWN", ATOMICITY_UNAVAILABLE: "USAGE_ATOMICITY_UNAVAILABLE" },
    COORDINATE_USAGE_COMMIT_RESULT: { QUOTA_EXHAUSTED: "QUOTA_EXHAUSTED" }, coordinateUsageAtomicity: null,
    isRecognitionStopError(error) { return ["RECOGNITION_DEADLINE_EXCEEDED", "RECOGNITION_BUDGET_EXHAUSTED"].includes(error?.code); },
    buildCoordinateVerificationResponseWithoutRecognitionBudget(payload) {
      const finalized = buildReviewFinalizedResult();
      const finalizedCoordinateResult = finalizedVariant === "missing_identity"
        ? { ...finalized, resultId: null }
        : finalizedVariant === "stale_identity"
          ? { ...finalized, currentRevision: finalized.resultRevision + 1 }
          : finalizedVariant === "invalid_geometry"
            ? { ...finalized, geometry: { type: "Polygon", coordinates: [] } }
            : finalized;
      return { ...payload, finalizedCoordinateResult };
    },
    buildCoordinateEngineV2ShadowResult() { return null; }, evaluateRecognitionOutputCapability,
    recordCoordinateRecognitionIssue() { throw new Error("unexpected_issue_write"); }, storeRegressionRecognitionTrace() { return true; },
    coordinateCaseStore: null,
    canonicalizeCoordinateImageUpload(file) { return { valid: true, status: "VALID", file }; },
    validateCoordinateImageUpload() { return { valid: true }; },
    createCoordinateImageIdentity() { return Object.freeze({ image_sha256: "offline-redacted", width: 1, height: 1, page: 1 }); },
    async executeCoordinateThreeCaseRcPermissionBoundary({ onAuthorized }) {
      return { allowed: true, httpStatus: 200, body: null, ...(await onAuthorized({
        adminData: { featureFlags: {} }, user: { user_id: "offline-controlled-visitor" }, permissions: { aiOcrEnabled: true }
      })) };
    },
    readAdminData: async () => ({ featureFlags: {} }),
    createCoordinateThreeCaseRcExistingUserLookup: () => async () => ({ user_id: "offline-controlled-visitor" }),
    ensureUser: () => ({ user_id: "offline-controlled-visitor" }), getEffectivePermissions: () => ({ aiOcrEnabled: true }),
    settleCoordinateThreeCaseRcPermissionFailure: async () => ({}), supabase: null,
    createRecognitionImageVariants: async () => ({ version: "offline", width: 1, height: 1, asyncRecommended: false,
      images: Object.freeze([{ role: "overview", dataUrl: "data:image/png;base64,b2ZmbGluZQ==" }]) }),
    hasExplicitHandwrittenDmsUploadContext() { return false; },
    runLocalOcrFamilyClassification: async () => ({ attempted: true, layoutLines: Object.freeze([]), sourceText: "", axisEvidenceText: "",
      axisOrderEvidence: null, sourceContextText: "", sourceContextProvenance: null, projectedTableOcrAcquisition: false,
      route: emptyRoute, contract: { route: emptyRoute }, localOcrDiagnostics }),
    getUploadedFileDisplayName(file) { return file?.originalname || "offline.png"; },
    selectIndonesiaStructuredProductRoute, buildIndonesiaStructuredPreflightDiagnostics, classifyIndonesiaStructuredProviderFailure,
    buildCoordinateStructuredCall, classifyCoordinateStructuredProviderResult, buildIndonesiaStructuredProductBridge,
    extractProviderMessageText(response) { return String(response?.choices?.[0]?.message?.content || ""); },
    prepareCoordinateProviderRequest, validateCoordinateProviderResponse,
    getAliyunChatCompletionsUrl() { return "http://127.0.0.1/provider-stub"; },
    composeAbortSignals(signals) { return { signal: signals.find(Boolean) || null, cleanup() {} }; }, isFamilyRetryAllowed() { return true; },
    latestProviderModelObservation: null, aliyunApiKey: "offline-placeholder", aliyunBaseURL: "http://127.0.0.1/provider-stub",
    aliyunVisionModel: INDONESIA_STRUCTURED_PRODUCT_MODEL, aliyunOcrModel: "offline-not-called", noCoordinatesText: "NO_COORDINATES",
    RECOGNITION_DEADLINE_CODE: "RECOGNITION_DEADLINE_EXCEEDED", RECOGNITION_BUDGET_CODE: "RECOGNITION_BUDGET_EXHAUSTED",
    COORDINATE_IMAGE_INVALID_CODE: "COORDINATE_IMAGE_INVALID",
    fetch: async (_url, options) => {
      providerReceipts.push(Object.freeze({ methodPost: options?.method === "POST", bodyPresent: Boolean(options?.body) }));
      return { ok: true, status: 200, async json() { return {
        model: INDONESIA_STRUCTURED_PRODUCT_MODEL,
        choices: [{ finish_reason: "stop", message: { content: JSON.stringify(actualStructured) } }],
        usage: { prompt_tokens: 2, completion_tokens: 3, total_tokens: 5 }
      }; } };
    }
  });
  vm.runInContext(extractFunctionDeclaration(serverSource, "buildRecognitionTerminalDiagnostics"), runtime);
  vm.runInContext(extractFunctionDeclaration(serverSource, "buildNoTrustedCandidateTerminalDiagnostics"), runtime);
  vm.runInContext(extractFunctionDeclaration(serverSource, "buildUnifiedAcquisitionTerminalDiagnostics"), runtime);
  vm.runInContext(extractFunctionDeclaration(serverSource, "buildGenericFallbackTerminalDiagnostics"), runtime);
  vm.runInContext(extractFunctionDeclaration(serverSource, "callAliyunVision"), runtime);
  vm.runInContext(extractFunctionDeclaration(serverSource, "recognizeCoordinatesHandler"), runtime);
  return { budget, providerReceipts, handler: vm.runInContext("recognizeCoordinatesHandler", runtime) };
}

async function executeActualStructuredHandler(options = {}) {
  const { handler, budget, providerReceipts } = createActualStructuredHandler(options);
  const headers = { "x-visitor-id": "offline-controlled-visitor", "x-coordinate-product-mode": INDONESIA_STRUCTURED_PRODUCT_MODE,
    "x-regression-test": "1", authorization: "Bearer offline-placeholder" };
  const req = { body: { visitorId: "offline-controlled-visitor", coordinateProductMode: INDONESIA_STRUCTURED_PRODUCT_MODE }, query: {},
    file: { buffer: Buffer.from("offline-image"), size: Buffer.byteLength("offline-image"), mimetype: "image/png", originalname: "offline.png" },
    get(name) { return headers[String(name).toLowerCase()] || ""; } };
  const res = createResponse();
  await handler(req, res);
  res.finish();
  return { res, budget, providerReceipts };
}

async function waitForJob(runtime, accepted) {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    const snapshot = runtime.get(accepted.jobId, accepted.jobAccessToken);
    if (["SUCCEEDED", "FAILED"].includes(snapshot?.status)) return snapshot;
    await new Promise(resolve => setImmediate(resolve));
  }
  throw new Error("offline_job_did_not_settle");
}

function persistSnapshotThroughActualPowerShellHelper(snapshot) {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "r6-dynamic-retention-"));
  try {
    const snapshotPath = path.join(tempRoot, "snapshot.json");
    const artifactPath = path.join(tempRoot, "validated-results.json");
    fs.writeFileSync(snapshotPath, JSON.stringify(snapshot), "utf8");
    const helperPath = path.join(root, "scripts", "coordinate-three-case-rc-result-artifact.ps1");
    const command = [
      ". '" + helperPath.replaceAll("'", "''") + "'",
      "$snapshot=Get-Content -LiteralPath '" + snapshotPath.replaceAll("'", "''") + "' -Raw|ConvertFrom-Json",
      "$definition=[ordered]@{caseId='offline';imageSha256=('b'*64)}",
      "$record=New-CoordinateThreeCaseValidatedResultRecord -Definition $definition -Snapshot $snapshot -ExpectedModel 'qwen3.8-flash'",
      "$state=New-CoordinateThreeCaseValidatedResultState -RunId 'offline-r6' -BatchId 'offline-r6' -ExpectedCommit ('c'*40) -Cases @($record) -RuntimeCommitMatch $true",
      "$null=Write-CoordinateThreeCaseValidatedResultArtifact -Path '" + artifactPath.replaceAll("'", "''") + "' -State $state",
      "$record|ConvertTo-Json -Depth 20 -Compress"
    ].join(";");
    const bundledPwsh = "C:\\Users\\Mir-1\\.cache\\codex-runtimes\\codex-primary-runtime\\dependencies\\native\\powershell\\pwsh.exe";
    const powerShell = fs.existsSync(bundledPwsh) ? bundledPwsh : "powershell.exe";
    const execution = spawnSync(powerShell, ["-NoProfile", "-Command", command], { cwd: root, encoding: "utf8", windowsHide: true });
    assert.equal(execution.status, 0, execution.stderr || execution.stdout);
    const outputLine = execution.stdout.trim().split(/\r?\n/u).at(-1);
    return { record: JSON.parse(outputLine), persisted: JSON.parse(fs.readFileSync(artifactPath, "utf8")) };
  } finally {
    fs.rmSync(tempRoot, { recursive: true, force: true });
  }
}

function createTerminalEmitterRuntime() {
  const runtime = vm.createContext({ Object, String, Number, Array, Set });
  for (const name of [
    "buildRecognitionTerminalDiagnostics",
    "buildNoTrustedCandidateTerminalDiagnostics",
    "buildUnifiedAcquisitionTerminalDiagnostics",
    "buildGenericFallbackTerminalDiagnostics"
  ]) {
    vm.runInContext(extractFunctionDeclaration(serverSource, name), runtime);
  }
  return {
    noTrusted: vm.runInContext("buildNoTrustedCandidateTerminalDiagnostics", runtime),
    unified: vm.runInContext("buildUnifiedAcquisitionTerminalDiagnostics", runtime),
    fallback: vm.runInContext("buildGenericFallbackTerminalDiagnostics", runtime)
  };
}

async function persistTerminalDiagnostic(diagnostic, { success = false } = {}) {
  const result = success
    ? {
        success: true,
        model: INDONESIA_STRUCTURED_PRODUCT_MODEL,
        providerModel: INDONESIA_STRUCTURED_PRODUCT_MODEL,
        providerCallCount: 1,
        providerCompletionState: "SUCCEEDED",
        recognitionTerminalDiagnostics: diagnostic
      }
    : buildUnchargedCoordinateFailureResponse({
        body: {
          success: false,
          reason: "recognition_failed_closed",
          code: "COORDINATE_RECOGNITION_FAILED_CLOSED",
          providerCompletionState: "SUCCEEDED",
          providerCallCount: 1,
          recognitionTerminalDiagnostics: diagnostic
        },
        recognitionRequestId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        authorityReason: "REGRESSION_TEST"
      });
  const runtime = createRecognitionAcquisitionJobRuntime({
    execute: async () => ({ httpStatus: success ? 200 : 422, result })
  });
  const snapshot = await waitForJob(
    runtime,
    runtime.enqueue({ requestId: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" })
  );
  return persistSnapshotThroughActualPowerShellHelper(snapshot);
}

const reviewResult = buildReviewFinalizedResult();
await test("real output evaluator authorizes unverified consumer capability without formal promotion", () => {
  const capability = evaluateRecognitionOutputCapability(reviewResult, { formalAuthorized: false });
  assert.equal(capability.technicallyGeneratable, true); assert.equal(capability.mapReady, true); assert.equal(capability.kmlReady, true);
  assert.equal(capability.unverified, true); assert.equal(reviewResult.requiresReview, true); assert.equal(reviewResult.confirmationStatus, "pending");
  assert.notEqual(reviewResult.decisionState, "AUTO_EXPORT");
});

await test("real output evaluator rejects stale identity and invalid geometry", () => {
  const staleCapability = evaluateRecognitionOutputCapability({ ...reviewResult, currentRevision: 2 }, { formalAuthorized: false });
  assert.equal(staleCapability.technicallyGeneratable, false); assert.ok(staleCapability.blockReasons.includes("RESULT_REVISION_STALE"));
  const invalidCapability = evaluateRecognitionOutputCapability({ ...reviewResult, geometry: { type: "Polygon", coordinates: [] } }, { formalAuthorized: false });
  assert.equal(invalidCapability.technicallyGeneratable, false); assert.equal(invalidCapability.mapReady, false); assert.equal(invalidCapability.kmlReady, false);
});

await test("actual terminal emitters survive the real wrappers job runtime and artifact whitelist", async () => {
  const emitters = createTerminalEmitterRuntime();
  const scenarios = [
    {
      diagnostic: emitters.noTrusted({ contractReasons: ["NO_TRUSTED_CANDIDATES"], candidateCounts: { pointCount: 0, groupCount: 0 },
        downstreamFamilyParserReached: false, deterministicParserOutcome: "NOT_REACHED", detectedFamilyCue: "UNKNOWN" }),
      stage: "UNIFIED_RESPONSE_ATTACHMENT", branch: "NO_TRUSTED_CANDIDATES", success: false
    },
    {
      diagnostic: emitters.unified({ decision: { shouldReturnFailure: true }, evidence: { candidateCoordinates: [], candidateCoordinateGroups: [] },
        contractReasons: ["NO_COORDINATE_EVIDENCE"], downstreamFamilyParserReached: false,
        deterministicParserOutcome: "NOT_REACHED", detectedFamilyCue: "UNKNOWN" }),
      stage: "UNIFIED_ACQUISITION_TERMINAL", branch: "DECISION_SHOULD_RETURN_FAILURE", success: false
    },
    {
      diagnostic: emitters.unified({ decision: { shouldReturnFailure: false }, evidence: { candidateCoordinates: [{}, {}], candidateCoordinateGroups: [{}] },
        contractReasons: ["COORDINATE_FORMAT_REQUIRES_VALIDATION"], downstreamFamilyParserReached: false,
        deterministicParserOutcome: "NOT_REACHED", detectedFamilyCue: "UNKNOWN" }),
      stage: "UNIFIED_ACQUISITION_TERMINAL", branch: "DECISION_REVIEW_REQUIRED", success: true
    },
    {
      diagnostic: emitters.fallback({ downstreamFamilyParserReached: false, deterministicParserOutcome: "NOT_REACHED", detectedFamilyCue: "UNKNOWN" }),
      stage: "GENERIC_FALLBACK", branch: "FALLBACK_ERROR", success: false
    }
  ];
  for (const scenario of scenarios) {
    const retained = await persistTerminalDiagnostic(scenario.diagnostic, { success: scenario.success });
    const persisted = retained.persisted.cases[0].recognitionTerminalDiagnostics;
    assert.equal(persisted.failureStage, scenario.stage);
    assert.equal(persisted.failureBranch, scenario.branch);
    assert.equal(persisted.downstreamFamilyParserReached, false);
    assert.equal(persisted.detectedFamilyCue, "UNKNOWN");
  }
});

await test("all server diagnostic callsites use the fixed builder and fallback uses the observed cue", () => {
  const start = serverSource.indexOf("async function recognizeCoordinatesHandler");
  const handlerSource = serverSource.slice(start);
  assert.equal((handlerSource.match(/buildNoTrustedCandidateTerminalDiagnostics\(\{/gu) || []).length, 1);
  assert.equal((handlerSource.match(/buildUnifiedAcquisitionTerminalDiagnostics\(\{/gu) || []).length, 1);
  assert.equal((handlerSource.match(/buildGenericFallbackTerminalDiagnostics\(\{/gu) || []).length, 1);
  const fallbackStart = handlerSource.indexOf("buildGenericFallbackTerminalDiagnostics({");
  const fallbackBlock = handlerSource.slice(fallbackStart, fallbackStart + 700);
  assert.match(fallbackBlock, /detectedFamilyCue:\s*observedFamilyCue/u);
  assert.doesNotMatch(fallbackBlock, /getMgrsInfo\(rawText\)|hasBftmContext\(rawText\)/u);
}, "STATIC_SOURCE_ASSERTION");

let actualStructuredOutput;
await test("actual VM-loaded structured handler returns identity-bound unverified capabilities", async () => {
  const execution = await executeActualStructuredHandler();
  actualStructuredOutput = execution.res.payload;
  assert.equal(execution.res.statusCode, 200); assert.equal(execution.providerReceipts.length, 1); assert.equal(execution.budget.providerAttemptCount, 1);
  assert.equal(actualStructuredOutput.success, true); assert.equal(actualStructuredOutput.model, INDONESIA_STRUCTURED_PRODUCT_MODEL);
  assert.equal(actualStructuredOutput.providerModel, INDONESIA_STRUCTURED_PRODUCT_MODEL);
  assert.equal(actualStructuredOutput.adapterLabel, "structured-product-bridge"); assert.equal(actualStructuredOutput.providerCallCount, 1);
  assert.equal(actualStructuredOutput.providerCompletionState, "SUCCEEDED");
  assert.equal(actualStructuredOutput.finalizedCoordinateResult.confirmationStatus, "pending");
  assert.equal(actualStructuredOutput.finalizedCoordinateResult.requiresReview, true);
  assert.equal(actualStructuredOutput.outputCapabilities.unverified, true);
  assert.equal(actualStructuredOutput.outputCapabilities.resultId, actualStructuredOutput.finalizedCoordinateResult.resultId);
  assert.equal(actualStructuredOutput.outputCapabilities.geometryHash, actualStructuredOutput.finalizedCoordinateResult.geometryHash);
});

await test("actual VM-loaded structured handler fails consumer capability closed for injected finalizer variants", async () => {
  for (const finalizedVariant of ["missing_identity", "stale_identity", "invalid_geometry"]) {
    const execution = await executeActualStructuredHandler({ finalizedVariant });
    assert.equal(execution.res.statusCode, 200);
    assert.equal(execution.res.payload.success, true);
    assert.equal(execution.res.payload.outputCapabilities.technicallyGeneratable, false);
    assert.equal(execution.res.payload.outputCapabilities.mapReady, false);
    assert.equal(execution.res.payload.outputCapabilities.kmlReady, false);
  }
});

await test("actual VM-loaded structured handler leaves missing provider call count absent", async () => {
  const execution = await executeActualStructuredHandler({ providerCountKnown: false });
  assert.equal(execution.res.statusCode, 200);
  assert.equal(Object.hasOwn(execution.res.payload, "providerCallCount"), false);
  assert.equal(execution.res.payload.providerCompletionState, "SUCCEEDED");
});

await test("actual success job snapshot reaches the real artifact whitelist", async () => {
  assert.ok(actualStructuredOutput);
  const runtime = createRecognitionAcquisitionJobRuntime({ execute: async () => ({ httpStatus: 200, result: actualStructuredOutput }) });
  const snapshot = await waitForJob(runtime, runtime.enqueue({ requestId: "77777777-7777-4777-8777-777777777777" }));
  assert.equal(snapshot.status, "SUCCEEDED");
  const retained = persistSnapshotThroughActualPowerShellHelper(snapshot);
  assert.equal(retained.record.finalizedResult.outputCapabilityState, "PRESENT_MATCH");
  assert.equal(retained.record.finalizedResult.consumerIdentityMatch, true);
  assert.equal(retained.record.finalizedResult.formalAuthorizationState, "UNKNOWN");
  assert.equal(retained.persisted.cases[0].providerCallCount, 1);
});

await test("fixed failure diagnostics survive usage wrapper job sanitizer and artifact whitelist", async () => {
  const runtimeContext = vm.createContext({ Object, String, Number });
  vm.runInContext(extractFunctionDeclaration(serverSource, "buildRecognitionTerminalDiagnostics"), runtimeContext);
  const build = vm.runInContext("buildRecognitionTerminalDiagnostics", runtimeContext);
  const diagnostic = build({ failureStage: "GENERIC_FALLBACK", failureBranch: "FALLBACK_ERROR",
    unifiedTerminalReason: "FALLBACK_ERROR", detectedFamilyCue: "UNKNOWN" });
  const wrapped = buildUnchargedCoordinateFailureResponse({ body: { success: false, reason: "recognition_failed_closed",
    code: "COORDINATE_RECOGNITION_FAILED_CLOSED", providerCompletionState: "SUCCEEDED", providerCallCount: 1,
    recognitionTerminalDiagnostics: { ...diagnostic, rawProviderResponse: "must-not-survive" } },
    recognitionRequestId: "88888888-8888-4888-8888-888888888888", authorityReason: "REGRESSION_TEST" });
  assert.equal(wrapped.recognitionTerminalDiagnostics.failureBranch, "FALLBACK_ERROR");
  assert.equal(wrapped.recognitionTerminalDiagnostics.schemaVersion, "recognition_terminal_diagnostics_v1");
  assert.equal(Object.hasOwn(wrapped.recognitionTerminalDiagnostics, "rawProviderResponse"), false);
  const runtime = createRecognitionAcquisitionJobRuntime({ execute: async () => ({ httpStatus: 422, result: wrapped }) });
  const snapshot = await waitForJob(runtime, runtime.enqueue({ requestId: "88888888-8888-4888-8888-888888888888" }));
  assert.equal(snapshot.status, "FAILED"); assert.equal(snapshot.result.recognitionTerminalDiagnostics.failureBranch, "FALLBACK_ERROR");
  const retained = persistSnapshotThroughActualPowerShellHelper(snapshot);
  assert.equal(retained.record.recognitionTerminalDiagnostics.failureBranch, "FALLBACK_ERROR");
  assert.equal(retained.persisted.cases[0].recognitionTerminalDiagnostics.detectedFamilyCue, "UNKNOWN");
  assert.equal(retained.persisted.cases[0].recognitionTerminalDiagnostics.schemaVersion, "recognition_terminal_diagnostics_v1");
});

await test("unknown diagnostic observations stay unknown through both wrappers and artifact retention", async () => {
  const source = createTerminalEmitterRuntime().fallback({
    deterministicParserOutcome: "UNKNOWN",
    detectedFamilyCue: "UNKNOWN"
  });
  assert.equal(source.candidatePointCount, null);
  assert.equal(source.candidateGroupCount, null);
  assert.equal(source.downstreamFamilyParserReached, null);
  const wrapped = buildUnchargedCoordinateFailureResponse({
    body: { success: false, reason: "recognition_failed_closed", code: "COORDINATE_RECOGNITION_FAILED_CLOSED",
      providerCompletionState: "UNKNOWN", recognitionTerminalDiagnostics: source },
    authorityReason: "REGRESSION_TEST"
  });
  assert.equal(wrapped.recognitionTerminalDiagnostics.candidatePointCount, null);
  assert.equal(wrapped.recognitionTerminalDiagnostics.downstreamFamilyParserReached, null);
  const runtime = createRecognitionAcquisitionJobRuntime({ execute: async () => ({ httpStatus: 422, result: wrapped }) });
  const snapshot = await waitForJob(runtime, runtime.enqueue({ requestId: "99999999-9999-4999-8999-999999999999" }));
  assert.equal(snapshot.result.recognitionTerminalDiagnostics.candidatePointCount, null);
  assert.equal(snapshot.result.recognitionTerminalDiagnostics.downstreamFamilyParserReached, null);
  const retained = persistSnapshotThroughActualPowerShellHelper(snapshot);
  assert.equal(retained.persisted.cases[0].recognitionTerminalDiagnostics.candidatePointCount, null);
  assert.equal(retained.persisted.cases[0].recognitionTerminalDiagnostics.downstreamFamilyParserReached, null);
});

const failed = results.filter(result => result.status !== "PASS");
const dynamicCases = results.filter(result => result.evidenceClass === "DYNAMIC");
const staticCases = results.filter(result => result.evidenceClass === "STATIC_SOURCE_ASSERTION");
console.log(JSON.stringify({ suite: "COORDINATE_THREE_CASE_RC_R6_DYNAMIC_HANDLER_AND_RETENTION",
  passed: results.length - failed.length, failed: failed.length, total: results.length,
  dynamicPassed: dynamicCases.filter(result => result.status === "PASS").length,
  dynamicTotal: dynamicCases.length,
  staticPassed: staticCases.filter(result => result.status === "PASS").length,
  staticTotal: staticCases.length,
  evidenceClass: "DYNAMIC_VM_REAL_SOURCE_WITH_LOCAL_STUBBED_BOUNDARIES_PLUS_STATIC_CALLSITE_CHECK", providerCalls: 0, externalHttpRequests: 0,
  listenerStarted: false, cases: results }, null, 2));
if (failed.length) process.exitCode = 1;
