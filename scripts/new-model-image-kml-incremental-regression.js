import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import path from "node:path";
import {
  COORDINATE_PROVIDER_MODELS,
  PROVIDER_COMPATIBILITY_ERROR_CODE,
  prepareCoordinateProviderRequest,
  validateCoordinateProviderResponse
} from "../server/recognition/model-provider-compatibility.js";
import {
  buildIndonesiaStructuredPreflightDiagnostics,
  classifyIndonesiaStructuredProviderFailure,
  sanitizeIndonesiaStructuredPreflightDiagnostics
} from "../server/recognition/indonesia-structured-diagnostics.js";
import {
  INDONESIA_STRUCTURED_PRODUCT_MODE,
  INDONESIA_STRUCTURED_ROUTE_STATUS,
  selectIndonesiaStructuredProductRoute
} from "../server/recognition/indonesia-structured-product-bridge.js";
import { buildUnchargedCoordinateFailureResponse } from "../server/coordinate-usage-atomicity.js";
import {
  RECOGNITION_ACQUISITION_JOB_STATUS,
  createRecognitionAcquisitionJobRuntime
} from "../server/recognition/recognition-acquisition-job-runtime.js";
import { buildCoordinateVerificationResponse } from "../server/verification/index.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const serverSource = await readFile(path.join(root, "server.js"), "utf8");
const indexSource = await readFile(path.join(root, "index.html"), "utf8");
const agenticSource = await readFile(path.join(root, "server/agentic-coordinate-recognition/service.js"), "utf8");
const agenticFinalizationSource = await readFile(path.join(root, "server/agentic-coordinate-finalization/pipeline.js"), "utf8");
const compatibilitySource = await readFile(path.join(root, "server/recognition/model-provider-compatibility.js"), "utf8");

const dynamicCases = [];
const staticAssertions = [];

async function dynamic(name, fn) {
  await fn();
  dynamicCases.push(name);
}

function sourceAssertion(name, condition) {
  assert.equal(Boolean(condition), true, name);
  staticAssertions.push(name);
}

function validResponse(model, content = "final coordinate output") {
  return {
    model,
    choices: [{ finish_reason: "stop", message: { content } }],
    usage: { prompt_tokens: 2, completion_tokens: 3, total_tokens: 5 }
  };
}

await dynamic("vision adapter defaults thinking off and preserves Agentic request shape", () => {
  const policy = prepareCoordinateProviderRequest({
    modelName: COORDINATE_PROVIDER_MODELS.VISION,
    maxTokens: 12000,
    responseFormat: { type: "json_object" },
    highResolutionImages: true
  });
  assert.deepEqual(policy, {
    applied: true,
    role: "VISION",
    stream: false,
    enableThinking: false,
    includeHighResolutionImages: true,
    includeResponseFormat: true
  });
});

await dynamic("OCR adapter accepts the frozen 8000 token cap", () => {
  const policy = prepareCoordinateProviderRequest({ modelName: COORDINATE_PROVIDER_MODELS.OCR, maxTokens: 8000 });
  assert.equal(policy.role, "OCR");
  assert.equal(policy.stream, false);
  assert.equal(policy.enableThinking, undefined);
});

await dynamic("OCR adapter rejects unsupported request parameters before dispatch", () => {
  assert.throws(
    () => prepareCoordinateProviderRequest({ modelName: COORDINATE_PROVIDER_MODELS.OCR, enableThinking: false }),
    error => error?.code === PROVIDER_COMPATIBILITY_ERROR_CODE.OCR_PARAMETER_UNSUPPORTED
  );
});

await dynamic("OCR adapter rejects token count above its candidate contract", () => {
  assert.throws(
    () => prepareCoordinateProviderRequest({ modelName: COORDINATE_PROVIDER_MODELS.OCR, maxTokens: 8001 }),
    error => error?.code === PROVIDER_COMPATIBILITY_ERROR_CODE.TOKEN_LIMIT_EXCEEDED
  );
});

await dynamic("unmanaged override remains outside the candidate adapter", () => {
  assert.equal(prepareCoordinateProviderRequest({ modelName: "configured-external-model" }).applied, false);
});

await dynamic("strict response accepts consumable string content", () => {
  assert.equal(validateCoordinateProviderResponse({
    modelName: COORDINATE_PROVIDER_MODELS.VISION,
    payload: validResponse(COORDINATE_PROVIDER_MODELS.VISION)
  }).responseContractValid, true);
});

await dynamic("strict response accepts consumable array text content", () => {
  assert.equal(validateCoordinateProviderResponse({
    modelName: COORDINATE_PROVIDER_MODELS.OCR,
    payload: validResponse(COORDINATE_PROVIDER_MODELS.OCR, [{ text: "rows" }])
  }).responseContractValid, true);
});

await dynamic("strict response rejects reasoning-only object content", () => {
  assert.throws(
    () => validateCoordinateProviderResponse({
      modelName: COORDINATE_PROVIDER_MODELS.VISION,
      payload: validResponse(COORDINATE_PROVIDER_MODELS.VISION, { reasoning: "not final" })
    }),
    error => error?.code === PROVIDER_COMPATIBILITY_ERROR_CODE.RESPONSE_CONTENT_EMPTY
  );
});

await dynamic("strict response rejects model mismatch", () => {
  assert.throws(
    () => validateCoordinateProviderResponse({
      modelName: COORDINATE_PROVIDER_MODELS.OCR,
      payload: validResponse(COORDINATE_PROVIDER_MODELS.VISION)
    }),
    error => error?.code === PROVIDER_COMPATIBILITY_ERROR_CODE.RESPONSE_MODEL_MISMATCH
  );
});

await dynamic("strict response rejects inconsistent usage", () => {
  const payload = validResponse(COORDINATE_PROVIDER_MODELS.VISION);
  payload.usage.total_tokens = 9;
  assert.throws(
    () => validateCoordinateProviderResponse({ modelName: COORDINATE_PROVIDER_MODELS.VISION, payload }),
    error => error?.code === PROVIDER_COMPATIBILITY_ERROR_CODE.RESPONSE_USAGE_INVALID
  );
});

await dynamic("strict response rejects null or coerced usage evidence", () => {
  const nullUsage = validResponse(COORDINATE_PROVIDER_MODELS.VISION);
  nullUsage.usage = { prompt_tokens: null, completion_tokens: null, total_tokens: null };
  assert.throws(
    () => validateCoordinateProviderResponse({ modelName: COORDINATE_PROVIDER_MODELS.VISION, payload: nullUsage }),
    error => error?.code === PROVIDER_COMPATIBILITY_ERROR_CODE.RESPONSE_USAGE_INVALID
  );
  const booleanUsage = validResponse(COORDINATE_PROVIDER_MODELS.VISION);
  booleanUsage.usage = { prompt_tokens: false, completion_tokens: false, total_tokens: false };
  assert.throws(
    () => validateCoordinateProviderResponse({ modelName: COORDINATE_PROVIDER_MODELS.VISION, payload: booleanUsage }),
    error => error?.code === PROVIDER_COMPATIBILITY_ERROR_CODE.RESPONSE_USAGE_INVALID
  );
});

await dynamic("provider failure accounting distinguishes pre-dispatch and dispatched failures", () => {
  assert.deepEqual(classifyIndonesiaStructuredProviderFailure({ providerAttemptCount: 0, errorCode: "MODEL_OCR_PARAMETER_UNSUPPORTED" }), {
    providerCallCount: 0,
    providerCompletionState: "NOT_STARTED",
    settlementOutcome: "FAILED_PRE_PROVIDER"
  });
  assert.deepEqual(classifyIndonesiaStructuredProviderFailure({ providerAttemptCount: 1, errorCode: "ALIYUN_TIMEOUT" }), {
    providerCallCount: 1,
    providerCompletionState: "TIMED_OUT",
    settlementOutcome: "OUTCOME_UNKNOWN"
  });
});

await dynamic("missing X/Y without a controlled hint reports exact projected-column gate", () => {
  const route = selectIndonesiaStructuredProductRoute({
    enabled: true,
    requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
    sourceContextText: "UTM WGS 1984 ZONA 50S"
  });
  const diagnostics = buildIndonesiaStructuredPreflightDiagnostics({
    route,
    localOcrAttempted: true,
    sourceContextPresent: true,
    preflightChecked: true,
    preflightPassed: false,
    preflightFailureCode: "INDONESIA_STRUCTURED_B_RC_JOB_PREFLIGHT_REJECTED"
  });
  assert.equal(route.status, INDONESIA_STRUCTURED_ROUTE_STATUS.BLOCKED);
  assert.equal(diagnostics.localOcrAttempted, true);
  assert.equal(diagnostics.explicitUtm50s, true);
  assert.equal(diagnostics.projectedColumns, false);
  assert.equal(diagnostics.routeFailureCode, "STRUCTURED_PRODUCT_PROJECTED_COLUMNS_EVIDENCE_REQUIRED");
  assert.equal(diagnostics.preflightFailureCode, "INDONESIA_STRUCTURED_B_RC_JOB_PREFLIGHT_REJECTED");
});

await dynamic("controlled RC hint satisfies only X/Y evidence while preserving UTM50S prerequisite", () => {
  const selected = selectIndonesiaStructuredProductRoute({
    enabled: true,
    requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
    sourceContextText: "UTM WGS 1984 ZONA 50S",
    controlledRcProjectedColumnsHintAuthorized: true
  });
  const blocked = selectIndonesiaStructuredProductRoute({
    enabled: true,
    requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
    sourceContextText: "X Y",
    controlledRcProjectedColumnsHintAuthorized: true
  });
  assert.equal(selected.status, INDONESIA_STRUCTURED_ROUTE_STATUS.SELECTED);
  assert.equal(selected.evidence.projectedColumnsHintOnly, true);
  assert.equal(buildIndonesiaStructuredPreflightDiagnostics({ route: blocked }).routeFailureCode,
    "STRUCTURED_PRODUCT_UTM50S_EVIDENCE_REQUIRED");
});

await dynamic("feature-disabled diagnostics still report observed source predicates", () => {
  const route = selectIndonesiaStructuredProductRoute({
    enabled: false,
    requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
    sourceContextText: "EPSG:32750 X Y"
  });
  const diagnostics = buildIndonesiaStructuredPreflightDiagnostics({ route, sourceContextPresent: true });
  assert.equal(diagnostics.explicitUtm50s, true);
  assert.equal(diagnostics.projectedColumns, true);
  assert.equal(diagnostics.routeFailureCode, "STRUCTURED_PRODUCT_FEATURE_DISABLED");
});

await dynamic("diagnostic sanitizer drops non-contract fields", () => {
  const route = selectIndonesiaStructuredProductRoute({
    enabled: true,
    requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
    sourceContextText: "EPSG:32750 X Y"
  });
  const diagnostics = buildIndonesiaStructuredPreflightDiagnostics({
    route,
    sourceContextPresent: true,
    preflightChecked: true,
    preflightPassed: true,
    preflightFailureCode: "NONE",
    controlledRcProjectedColumnsHintAuthorized: true
  });
  const sanitized = sanitizeIndonesiaStructuredPreflightDiagnostics({ ...diagnostics, rawText: "must disappear" });
  assert.equal(Object.hasOwn(sanitized, "rawText"), false);
  assert.equal(sanitized.routeSelected, true);
});

await dynamic("uncharged product response preserves sanitized failure diagnostics", () => {
  const diagnostics = buildIndonesiaStructuredPreflightDiagnostics({
    route: selectIndonesiaStructuredProductRoute({
      enabled: true,
      requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
      sourceContextText: "UTM WGS 1984 ZONA 50S"
    }),
    sourceContextPresent: true,
    preflightChecked: true,
    preflightPassed: false,
    preflightFailureCode: "INDONESIA_STRUCTURED_B_RC_JOB_PREFLIGHT_REJECTED"
  });
  const response = buildUnchargedCoordinateFailureResponse({
    body: {
      success: false,
      reason: "indonesia_structured_product_review_required",
      code: "STRUCTURED_PRODUCT_SOURCE_EVIDENCE_REQUIRED",
      providerCallCount: 0,
      providerCompletionState: "NOT_STARTED",
      indonesiaStructuredPreflightDiagnostics: { ...diagnostics, rawProviderResponse: "must disappear" }
    }
  });
  assert.equal(response.providerCallCount, 0);
  assert.equal(response.providerCompletionState, "NOT_STARTED");
  assert.equal(response.indonesiaStructuredPreflightDiagnostics.projectedColumns, false);
  assert.equal(Object.hasOwn(response.indonesiaStructuredPreflightDiagnostics, "rawProviderResponse"), false);
});

await dynamic("verification response preserves diagnostics before uncharged settlement", () => {
  const diagnostics = buildIndonesiaStructuredPreflightDiagnostics({
    route: selectIndonesiaStructuredProductRoute({
      enabled: true,
      requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
      sourceContextText: "UTM WGS 1984 ZONA 50S"
    }),
    sourceContextPresent: true,
    preflightChecked: true,
    preflightPassed: false,
    preflightFailureCode: "INDONESIA_STRUCTURED_B_RC_JOB_PREFLIGHT_REJECTED"
  });
  const response = buildCoordinateVerificationResponse({
    success: false,
    reason: "indonesia_structured_product_review_required",
    code: "STRUCTURED_PRODUCT_SOURCE_EVIDENCE_REQUIRED",
    rawText: "",
    coordinates: "",
    precisionMode: "indonesia-utm50s-structured-projected",
    requiresReview: true,
    indonesiaStructuredPreflightDiagnostics: diagnostics
  }, { groups: [], requires_review: true });
  assert.equal(response.indonesiaStructuredPreflightDiagnostics.routeFailureCode,
    "STRUCTURED_PRODUCT_PROJECTED_COLUMNS_EVIDENCE_REQUIRED");
  assert.equal(response.finalizedCoordinateResult?.kmlReady, false);
});

await dynamic("async job failure snapshot preserves sanitized diagnostics", async () => {
  const diagnostics = buildIndonesiaStructuredPreflightDiagnostics({
    route: selectIndonesiaStructuredProductRoute({
      enabled: true,
      requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
      sourceContextText: "UTM WGS 1984 ZONA 50S"
    }),
    sourceContextPresent: true,
    preflightChecked: true,
    preflightPassed: false,
    preflightFailureCode: "INDONESIA_STRUCTURED_B_RC_JOB_PREFLIGHT_REJECTED"
  });
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
  const queued = runtime.enqueue({ requestId: "11111111-1111-4111-8111-111111111111" });
  let snapshot;
  for (let index = 0; index < 20; index += 1) {
    await new Promise(resolve => setImmediate(resolve));
    snapshot = runtime.get(queued.jobId, queued.jobAccessToken);
    if (snapshot?.status === RECOGNITION_ACQUISITION_JOB_STATUS.FAILED) break;
  }
  assert.equal(snapshot?.status, RECOGNITION_ACQUISITION_JOB_STATUS.FAILED);
  assert.equal(snapshot.result.indonesiaStructuredPreflightDiagnostics.preflightPassed, false);
  assert.equal(snapshot.result.indonesiaStructuredPreflightDiagnostics.routeFailureCode,
    "STRUCTURED_PRODUCT_PROJECTED_COLUMNS_EVIDENCE_REQUIRED");
});

sourceAssertion("ordinary vision default is qwen3.8-flash", /COORDINATE_PROVIDER_MODELS\.VISION/u.test(serverSource));
sourceAssertion("OCR default is qwen3.5-ocr", /COORDINATE_PROVIDER_MODELS\.OCR/u.test(serverSource));
sourceAssertion("shared provider request adapter is invoked", /prepareCoordinateProviderRequest\(\{/u.test(serverSource));
sourceAssertion("shared provider response adapter is invoked", /validateCoordinateProviderResponse\(\{ modelName, payload: data \}\)/u.test(serverSource));
sourceAssertion("request contract is checked before budget or provider dispatch", serverSource.indexOf("const providerCompatibility = prepareCoordinateProviderRequest({")
  < serverSource.indexOf("const budget = getRecognitionBudget();", serverSource.indexOf("async function callAliyunVision"))
  && serverSource.indexOf("const providerCompatibility = prepareCoordinateProviderRequest({")
    < serverSource.indexOf("budget?.markProviderAttempted();", serverSource.indexOf("async function callAliyunVision")));
sourceAssertion("Mozambique late transcription remains on OCR model", /promptName:\s*"late-transcription"[\s\S]{0,160}modelName:\s*aliyunOcrModel/u.test(serverSource));
sourceAssertion("MGRS OCR compatibility retains 1600 token contract", /modelName:\s*aliyunOcrModel,[\s\S]{0,150}prompt:\s*mgrsDirectPrompt[\s\S]{0,150}maxTokens:\s*1600/u.test(serverSource));
sourceAssertion("BFTM OCR compatibility uses 8000 token contract", /modelName:\s*aliyunOcrModel,[\s\S]{0,150}prompt:\s*bftmRetryPrompt[\s\S]{0,150}maxTokens:\s*8000/u.test(serverSource));
sourceAssertion("Agentic one-shot keeps a single call and zero retry/fallback", /maxTokens:\s*12000/u.test(agenticSource)
  && /providerCallCount,[\s\S]{0,100}retryCount:\s*0,[\s\S]{0,60}fallbackCount:\s*0/u.test(agenticSource));
sourceAssertion("mining quick judgment remains on the shared vision adapter", /\/api\/analyze-mining-image/u.test(serverSource)
  && /callAliyunVision\(\{[\s\S]{0,120}modelName:\s*aliyunVisionModel,[\s\S]{0,160}maxTokens:\s*360/u.test(serverSource));
sourceAssertion("edit finalization adds no new call and preserves existing changed-text call boundary", /canReuseRecognition[\s\S]{0,180}providerCallCount:\s*0/u.test(agenticFinalizationSource)
  && /:\s*await runAgenticCoordinateFinalization\(/u.test(agenticFinalizationSource));
sourceAssertion("candidate adapter introduces no fetch/retry/fallback implementation", !/\bfetch\s*\(|\bretry\b|\bfallback\b/iu.test(compatibilitySource));
sourceAssertion("frontend map preview binds resultId revision and geometryHash", /outputCapabilities\.resultId === finalized\.resultId[\s\S]{0,120}outputCapabilities\.resultRevision === finalized\.resultRevision[\s\S]{0,120}outputCapabilities\.geometryHash === finalized\.geometryHash/u.test(indexSource));
sourceAssertion("frontend KML request uses current finalized identity", /resultId:\s*snapshot\.resultIdentity\.resultId,[\s\S]{0,100}resultRevision:\s*snapshot\.resultIdentity\.resultRevision,[\s\S]{0,100}geometryHash:\s*snapshot\.resultIdentity\.geometryHash/u.test(indexSource));

const dynamicPassed = dynamicCases.length;
const staticPassed = staticAssertions.length;
const totalPassed = dynamicPassed + staticPassed;
console.log(JSON.stringify({
  suite: "New Model Image-to-KML Incremental Regression",
  evidenceClass: "OFFLINE_DYNAMIC_AND_STATIC_NO_PROVIDER",
  providerCalls: 0,
  dynamic: { passed: dynamicPassed, total: dynamicCases.length },
  static: { passed: staticPassed, total: staticAssertions.length },
  combined: { passed: totalPassed, total: dynamicCases.length + staticAssertions.length },
  status: "PASS"
}, null, 2));
