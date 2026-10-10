import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  buildIndonesiaStructuredProductBridge,
  INDONESIA_STRUCTURED_PRODUCT_MODE,
  INDONESIA_STRUCTURED_PRODUCT_STATUS,
  INDONESIA_STRUCTURED_ROUTE_STATUS,
  selectIndonesiaStructuredProductRoute
} from "../server/recognition/indonesia-structured-product-bridge.js";
import {
  buildCoordinateStructuredCall,
  CAPABILITY,
  classifyCoordinateStructuredProviderResult,
  MODEL,
  STRUCTURED_RESULT_STATUS
} from "../server/recognition/qwen38-coordinate-structured-adapter.mjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const fixturePath = path.join(root, "regression-samples", "indonesia-structured-product", "actual-b-structured.json");
const actualB = JSON.parse(await readFile(fixturePath, "utf8"));
const clone = value => structuredClone(value);

const cases = [];
function test(name, action) {
  action();
  cases.push(name);
}

test("actual B becomes review-required deterministic product payload", () => {
  const result = buildIndonesiaStructuredProductBridge(actualB);
  assert.equal(result.status, INDONESIA_STRUCTURED_PRODUCT_STATUS.ACCEPTED_REVIEW_REQUIRED);
  assert.equal(result.payload.indonesiaUtm50.transformStatus, "SUCCESS");
  assert.equal(result.payload.indonesiaUtm50.rowCount, 16);
  assert.equal(result.payload.coordinates.split("\n").length, 16);
  assert.equal(result.payload.requiresReview, true);
  assert.equal(result.payload.structuredProductEvidence.modelOutputIsAuthority, false);
  assert.equal(result.payload.structuredProductEvidence.modelKmlAccepted, false);
  assert.deepEqual(result.payload.structuredProductEvidence.pointOrder, actualB.source.pointOrder);
  assert.deepEqual(result.payload.structuredProductEvidence.points, actualB.source.points);
  assert.ok(result.payload.structuredProductEvidence.reviewReasons.includes("MODEL_SYMBOL_NORMALIZATION_REVIEW_REQUIRED"));
});

test("missing explicit CRS fails closed", () => {
  const candidate = clone(actualB);
  candidate.source.coordinateSystemExplicit = "";
  const result = buildIndonesiaStructuredProductBridge(candidate);
  assert.equal(result.status, INDONESIA_STRUCTURED_PRODUCT_STATUS.INVALID_REVIEW_REQUIRED);
  assert.equal(result.reasonCode, "EXPLICIT_UTM50S_CRS_REQUIRED");
  assert.equal(result.payload, null);
});

test("symbol-only unresolved provenance preserves finite review geometry", () => {
  const candidate = clone(actualB);
  candidate.unresolvedItems = ["SOURCE_SYMBOL_AMBIGUOUS"];
  const result = buildIndonesiaStructuredProductBridge(candidate);
  assert.equal(result.status, INDONESIA_STRUCTURED_PRODUCT_STATUS.ACCEPTED_REVIEW_REQUIRED);
  assert.equal(result.payload.indonesiaUtm50.rowCount, 16);
  assert.deepEqual(result.payload.structuredProductEvidence.unresolvedItems, ["SOURCE_SYMBOL_AMBIGUOUS"]);
  assert.ok(result.payload.structuredProductEvidence.reviewReasons.includes("STRUCTURED_UNRESOLVED_ITEM_REVIEW_REQUIRED"));
});

test("coordinate-critical unresolved provenance fails closed", () => {
  const candidate = clone(actualB);
  candidate.unresolvedItems = ["CRS zone is unresolved"];
  const result = buildIndonesiaStructuredProductBridge(candidate);
  assert.equal(result.status, INDONESIA_STRUCTURED_PRODUCT_STATUS.INVALID_REVIEW_REQUIRED);
  assert.equal(result.reasonCode, "STRUCTURED_CRITICAL_UNRESOLVED_ITEMS_PRESENT");
  assert.equal(result.payload, null);
});

test("explicit datum conflict fails before deterministic projection", () => {
  const candidate = clone(actualB);
  candidate.source.datumExplicit = "Tete";
  const result = buildIndonesiaStructuredProductBridge(candidate);
  assert.equal(result.status, INDONESIA_STRUCTURED_PRODUCT_STATUS.INVALID_REVIEW_REQUIRED);
  assert.equal(result.reasonCode, "EXPLICIT_DATUM_CONFLICT");
  assert.equal(result.payload, null);
});

test("legacy OCR text remains outside the structured bridge", () => {
  const result = buildIndonesiaStructuredProductBridge(null);
  assert.equal(result.status, INDONESIA_STRUCTURED_PRODUCT_STATUS.NOT_APPLICABLE);
  assert.equal(result.payload, null);
});

test("empty or truncated structured result cannot create geometry", () => {
  for (const value of [{}, { source: actualB.source }, { source: actualB.source, objects: [] }]) {
    const result = buildIndonesiaStructuredProductBridge(value);
    assert.equal(result.status, INDONESIA_STRUCTURED_PRODUCT_STATUS.INVALID_REVIEW_REQUIRED);
    assert.equal(result.payload, null);
  }
});

test("point order and polygon topology must bind to the same observations", () => {
  const reordered = clone(actualB);
  [reordered.source.pointOrder[0], reordered.source.pointOrder[1]] = [
    reordered.source.pointOrder[1], reordered.source.pointOrder[0]
  ];
  assert.equal(buildIndonesiaStructuredProductBridge(reordered).reasonCode, "STRUCTURED_POINT_ORDER_MISMATCH");

  const mismatchedRing = clone(actualB);
  [mismatchedRing.objects[0].outerRing[0], mismatchedRing.objects[0].outerRing[1]] = [
    mismatchedRing.objects[0].outerRing[1], mismatchedRing.objects[0].outerRing[0]
  ];
  assert.equal(buildIndonesiaStructuredProductBridge(mismatchedRing).reasonCode, "STRUCTURED_OUTER_RING_ORDER_MISMATCH");
});

test("unsupported inner rings cannot be flattened or silently discarded", () => {
  const candidate = clone(actualB);
  candidate.objects[0].innerRings = [["1", "2", "3"]];
  const result = buildIndonesiaStructuredProductBridge(candidate);
  assert.equal(result.reasonCode, "STRUCTURED_INNER_RINGS_NOT_SUPPORTED");
  assert.equal(result.payload, null);
});

test("deterministic transform failure produces no product payload", () => {
  const result = buildIndonesiaStructuredProductBridge(actualB, { transform: () => null });
  assert.equal(result.status, INDONESIA_STRUCTURED_PRODUCT_STATUS.INVALID_REVIEW_REQUIRED);
  assert.equal(result.reasonCode, "INDONESIA_UTM50_TRANSFORM_FAILED");
  assert.equal(result.payload, null);
});

test("model-provided KML is never consumed as authority", () => {
  const candidate = clone(actualB);
  candidate.kml = "<kml>model output must not be consumed</kml>";
  const result = buildIndonesiaStructuredProductBridge(candidate);
  assert.equal(result.status, INDONESIA_STRUCTURED_PRODUCT_STATUS.ACCEPTED_REVIEW_REQUIRED);
  assert.equal(result.payload.structuredProductEvidence.modelKmlAccepted, false);
  assert.equal(Object.hasOwn(result.payload, "kml"), false);
});

test("B route requires explicit mode, feature flag and visible UTM50S X/Y evidence", () => {
  assert.equal(selectIndonesiaStructuredProductRoute({
    enabled: true,
    requestedMode: "",
    sourceContextText: "UTM WGS 1984 ZONA 50S X Y"
  }).status, INDONESIA_STRUCTURED_ROUTE_STATUS.NOT_SELECTED);
  assert.equal(selectIndonesiaStructuredProductRoute({
    enabled: false,
    requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
    sourceContextText: "UTM WGS 1984 ZONA 50S X Y"
  }).reasonCode, "STRUCTURED_PRODUCT_FEATURE_DISABLED");
  assert.equal(selectIndonesiaStructuredProductRoute({
    enabled: true,
    requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
    sourceContextText: "ordinary DMS table"
  }).reasonCode, "STRUCTURED_PRODUCT_SOURCE_EVIDENCE_REQUIRED");
  assert.equal(selectIndonesiaStructuredProductRoute({
    enabled: true,
    requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
    sourceContextText: "SISTEM KOORDINAT UTM WGS 1984 ZONA 50S\nPoint X Y"
  }).status, INDONESIA_STRUCTURED_ROUTE_STATUS.SELECTED);
});

test("frozen V-C request and response contract feeds the bridge without rewriting", () => {
  const imageItems = [{ type: "image_url", image_url: { url: "data:image/png;base64,AA==" } }];
  const request = buildCoordinateStructuredCall({ imageItems, stageName: "indonesia_structured_b" });
  assert.equal(request.modelName, MODEL);
  assert.equal(request.temperature, 0);
  assert.equal(request.enableThinking, false);
  const extractedText = JSON.stringify(actualB);
  const response = {
    model: MODEL,
    choices: [{ finish_reason: "stop", message: { content: extractedText } }],
    usage: { prompt_tokens: 10, completion_tokens: 20, total_tokens: 30 }
  };
  const classified = classifyCoordinateStructuredProviderResult({
    selectedCapability: CAPABILITY,
    response,
    extractedText
  });
  assert.equal(classified.status, STRUCTURED_RESULT_STATUS.VALID);
  assert.deepEqual(classified.structured, actualB);
  assert.equal(buildIndonesiaStructuredProductBridge(classified.structured).payload.indonesiaUtm50.rowCount, 16);
});

test("selected B malformed response is terminal and never becomes a bridge payload", () => {
  const classified = classifyCoordinateStructuredProviderResult({
    selectedCapability: CAPABILITY,
    response: { model: MODEL, choices: [] },
    extractedText: ""
  });
  assert.equal(classified.status, STRUCTURED_RESULT_STATUS.INVALID);
  assert.equal(typeof classified.reasonCode, "string");
});

const pureSummary = {
  suite: "indonesia-structured-product-integration-regression",
  status: "PASS",
  passed: cases.length,
  total: cases.length,
  cases,
  evidence: {
    actualBReused: true,
    actualBPointCount: 16,
    existingDeterministicProjectionReused: true,
    modelOutputAuthorityGranted: false,
    glyphReviewBoundaryPreserved: true,
    realProviderCalls: 0,
    networkCalls: 0,
    databaseWrites: 0
  }
};

if (process.argv[2] === "--http-child") {
  const { default: http } = await import("node:http");
  const nativeListen = http.Server.prototype.listen;
  let providerCalls = 0;
  let providerRequest = null;
  http.Server.prototype.listen = function patchedListen(_port, callback) {
    return nativeListen.call(this, 0, "127.0.0.1", () => {
      process.send?.({ port: this.address().port });
      callback?.();
    });
  };
  globalThis.fetch = async (_url, options = {}) => {
    providerCalls += 1;
    if (providerCalls > 1) throw new Error("UNEXPECTED_SECOND_PROVIDER_CALL");
    providerRequest = JSON.parse(String(options.body || "{}"));
    const content = process.env.TEST_PROVIDER_SCENARIO === "invalid"
      ? "not-json"
      : JSON.stringify(actualB);
    return new Response(JSON.stringify({
      id: "mock-indonesia-structured-product",
      model: MODEL,
      choices: [{ finish_reason: "stop", message: { content } }],
      usage: { prompt_tokens: 100, completion_tokens: 200, total_tokens: 300 }
    }), { status: 200, headers: { "content-type": "application/json" } });
  };
  process.on("message", message => {
    if (message === "stats") process.send?.({ providerCalls, providerRequest });
  });
  await import("../server.js");
  await new Promise(() => {});
}

async function runHttpScenario(scenario, { sourceContext, requestedMode = INDONESIA_STRUCTURED_PRODUCT_MODE } = {}) {
  const injectedClassification = `    const route = classifyOneShotStructuredFamily();
    const oneShotFamilyClassification = {
      attempted: true,
      route,
      sourceText: "",
      layoutLines: Object.freeze([]),
      axisEvidenceText: "X | Y",
      axisOrderEvidence: null,
      projectedTableOcrAcquisition: true,
      sourceContextText: String(process.env.TEST_LOCAL_SOURCE_CONTEXT || ""),
      sourceContextProvenance: Object.freeze({
        schema_version: "projected_source_context_v1",
        image_sha256: coordinateImageIdentity.image_sha256,
        same_request: true,
        mode: "overview_footer_composite"
      }),
      contract: createOneShotAcquisitionContract({ route })
    };
`;
  const preload = `import { registerHooks } from 'node:module';
registerHooks({load(url, context, nextLoad) {
  const result = nextLoad(url, context);
  if (!url.endsWith('/server.js')) return result;
  let source = String(result.source);
  const callStart = source.indexOf('    const oneShotFamilyClassification = await runLocalOcrFamilyClassification({');
  const callEnd = source.indexOf('    });', callStart) + 7;
  if (callStart < 0 || callEnd < 7) throw new Error('INDONESIA_STRUCTURED_HTTP_INJECTION_NOT_INSTALLED');
  return {...result, source: source.slice(0, callStart) + ${JSON.stringify(injectedClassification)} + source.slice(callEnd)};
}});`;
  const child = spawn(process.execPath, [
    "--import", `data:text/javascript,${encodeURIComponent(preload)}`,
    fileURLToPath(import.meta.url), "--http-child"
  ], {
    cwd: root,
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe", "ipc"],
    env: {
      SystemRoot: process.env.SystemRoot,
      PATH: process.env.PATH,
      NODE_ENV: "test",
      PORT: "0",
      ENABLE_REGRESSION_TEST_MODE: "true",
      INDONESIA_STRUCTURED_B_PRODUCT_ENABLED: "true",
      ALIYUN_API_KEY: "local-mock-only",
      ALIYUN_BASE_URL: "http://127.0.0.1:1/v1",
      SUPABASE_URL: "",
      SUPABASE_SERVICE_ROLE_KEY: "",
      SPATIAL_RESULT_ENABLED: "true",
      TEST_PROVIDER_SCENARIO: scenario,
      TEST_LOCAL_SOURCE_CONTEXT: sourceContext,
      DOTENV_CONFIG_PATH: path.join(root, "__no_test_env__")
    }
  });
  let stderr = "";
  child.stdout.resume();
  child.stderr.on("data", chunk => { stderr += String(chunk); });
  const signal = AbortSignal.timeout(60_000);
  try {
    const [{ port }] = await Promise.race([
      once(child, "message", { signal }),
      once(child, "exit").then(([code]) => { throw new Error(`HTTP_CHILD_EXITED_BEFORE_READY:${code}`); })
    ]);
    const fixture = await readFile(path.join(root, "regression-samples", "production-recognition-recovery-p0", "indonesia-utm50s-real-002.jpg"));
    const form = new FormData();
    form.set("visitorId", `indonesia-structured-${scenario}`);
    form.set("coordinateProductMode", requestedMode);
    form.set("image", new Blob([fixture], { type: "image/jpeg" }), "indonesia-structured.jpg");
    const response = await fetch(`http://127.0.0.1:${port}/api/recognize-coordinates`, {
      method: "POST",
      headers: { "x-regression-test": "1", "x-regression-case-id": `indonesia-structured-${scenario}` },
      body: form,
      signal
    });
    const payload = await response.json();
    const statsPromise = once(child, "message", { signal });
    child.send("stats");
    const [stats] = await statsPromise;
    return { status: response.status, payload, stats };
  } catch (error) {
    if (stderr) process.stderr.write(stderr.slice(-8000));
    throw error;
  } finally {
    child.kill();
    await once(child, "exit").catch(() => {});
  }
}

if (process.argv[2] !== "--http-child") {
  const sourceContext = "No. X Y LATITUDE LONGITUDE\nSISTEM KOORDINAT UTM WGS 1984 ZONA 50S";
  const valid = await runHttpScenario("valid", { sourceContext });
  assert.equal(valid.status, 200, JSON.stringify(valid.payload));
  assert.equal(valid.stats.providerCalls, 1);
  assert.equal(valid.stats.providerRequest?.model, MODEL);
  assert.equal(valid.stats.providerRequest?.temperature, 0);
  assert.equal(valid.stats.providerRequest?.max_tokens, 8000);
  assert.equal(valid.payload.success, true);
  assert.equal(valid.payload.structuredProductMode, INDONESIA_STRUCTURED_PRODUCT_MODE);
  assert.equal(valid.payload.finalizedCoordinateResult?.schemaVersion, "finalized_coordinate_result_v1");
  assert.equal(valid.payload.finalizedCoordinateResult?.decisionState, "REVIEW_REQUIRED");
  assert.equal(valid.payload.finalizedCoordinateResult?.confirmationStatus, "pending");
  assert.equal(valid.payload.coordinateEngineV2?.groups?.[0]?.points?.length, 16);
  assert.equal(valid.payload.finalizedCoordinateResult?.technicalKmlReady, true);
  assert.equal(valid.payload.finalizedCoordinateResult?.kmlReady, true);
  assert.equal(valid.payload.finalizedCoordinateResult?.geometry?.type, "Polygon");
  assert.equal(valid.payload.finalizedCoordinateResult?.geometry?.coordinates?.[0]?.length, 17);
  assert.equal(valid.payload.structuredProductEvidence?.modelOutputIsAuthority, false);

  const invalid = await runHttpScenario("invalid", { sourceContext });
  assert.equal(invalid.status, 422, JSON.stringify(invalid.payload));
  assert.equal(invalid.stats.providerCalls, 1);
  assert.equal(invalid.payload.success, false);
  assert.equal(invalid.payload.code, "E_FINAL_NON_JSON");
  assert.notEqual(invalid.payload.finalizedCoordinateResult?.technicalKmlReady, true);
  assert.notEqual(invalid.payload.finalizedCoordinateResult?.kmlReady, true);

  const blocked = await runHttpScenario("blocked", { sourceContext: "ordinary DMS table" });
  assert.equal(blocked.status, 422, JSON.stringify(blocked.payload));
  assert.equal(blocked.stats.providerCalls, 0);
  assert.equal(blocked.payload.code, "STRUCTURED_PRODUCT_SOURCE_EVIDENCE_REQUIRED");

  process.stdout.write(`${JSON.stringify({
    ...pureSummary,
    httpHandler: {
      status: "PASS",
      scenarios: 3,
      mockProviderCalls: valid.stats.providerCalls + invalid.stats.providerCalls + blocked.stats.providerCalls,
      realProviderCalls: 0,
      validFinalizedPoints: valid.payload.coordinateEngineV2.groups[0].points.length,
      validDecisionState: valid.payload.finalizedCoordinateResult.decisionState,
      validMapConsumableGeometry: valid.payload.finalizedCoordinateResult.geometry?.type === "Polygon",
      validTechnicalKmlReady: valid.payload.finalizedCoordinateResult.technicalKmlReady,
      invalidFailClosed: invalid.status === 422
        && invalid.payload.finalizedCoordinateResult?.technicalKmlReady !== true
        && invalid.payload.finalizedCoordinateResult?.kmlReady !== true,
      sourceEvidenceGateFailClosed: blocked.status === 422 && blocked.stats.providerCalls === 0,
      automaticRetryCount: 0,
      databaseWrites: 0
    }
  }, null, 2)}\n`);
}
