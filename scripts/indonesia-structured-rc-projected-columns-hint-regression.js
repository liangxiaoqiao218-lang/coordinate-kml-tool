import assert from "node:assert/strict";
import { createHash } from "node:crypto";
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
  createIndonesiaStructuredBRcAdmission,
  INDONESIA_STRUCTURED_B_RC
} from "../server/recognition/indonesia-structured-b-rc-admission.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const imagePath = String(process.env.INDONESIA_STRUCTURED_B_TEST_IMAGE || "").trim();
assert.ok(imagePath, "INDONESIA_STRUCTURED_B_TEST_IMAGE is required");
const imageBuffer = await readFile(imagePath);
assert.equal(createHash("sha256").update(imageBuffer).digest("hex"), INDONESIA_STRUCTURED_B_RC.imageSha256);
assert.equal(INDONESIA_STRUCTURED_B_RC.runId, "indonesia-b-rc-enablement-20261010-04");

const crsOnlyContext = "SISTEM KOORDINAT UTM WGS 1984 ZONA 50S";
const completeContext = `${crsOnlyContext}\nNo. X Y Latitude Longitude`;

const controlledHintRoute = selectIndonesiaStructuredProductRoute({
  enabled: true,
  requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
  sourceContextText: crsOnlyContext,
  controlledRcProjectedColumnsHintAuthorized: true
});
assert.equal(controlledHintRoute.status, INDONESIA_STRUCTURED_ROUTE_STATUS.SELECTED);
assert.equal(controlledHintRoute.evidence.explicitUtm50s, true);
assert.equal(controlledHintRoute.evidence.projectedColumns, false);
assert.equal(controlledHintRoute.evidence.projectedColumnsHintOnly, true);

for (const controlledRcProjectedColumnsHintAuthorized of [false, "true", 1, null, undefined]) {
  const ordinaryRoute = selectIndonesiaStructuredProductRoute({
    enabled: true,
    requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
    sourceContextText: crsOnlyContext,
    controlledRcProjectedColumnsHintAuthorized
  });
  assert.equal(ordinaryRoute.status, INDONESIA_STRUCTURED_ROUTE_STATUS.BLOCKED);
  assert.equal(ordinaryRoute.evidence.projectedColumns, false);
  assert.equal(ordinaryRoute.evidence.projectedColumnsHintOnly, false);
}

const missingCrsRoute = selectIndonesiaStructuredProductRoute({
  enabled: true,
  requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
  sourceContextText: "No. X Y Latitude Longitude",
  controlledRcProjectedColumnsHintAuthorized: true
});
assert.equal(missingCrsRoute.status, INDONESIA_STRUCTURED_ROUTE_STATUS.BLOCKED);
assert.equal(missingCrsRoute.evidence.explicitUtm50s, false);

const unchangedCompleteRoute = selectIndonesiaStructuredProductRoute({
  enabled: true,
  requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
  sourceContextText: completeContext
});
assert.equal(unchangedCompleteRoute.status, INDONESIA_STRUCTURED_ROUTE_STATUS.SELECTED);
assert.equal(unchangedCompleteRoute.evidence.projectedColumns, true);
assert.equal(unchangedCompleteRoute.evidence.projectedColumnsHintOnly, false);

assert.equal(selectIndonesiaStructuredProductRoute({
  enabled: true,
  requestedMode: "ordinary_recognition",
  sourceContextText: crsOnlyContext,
  controlledRcProjectedColumnsHintAuthorized: true
}).status, INDONESIA_STRUCTURED_ROUTE_STATUS.NOT_SELECTED);

const missingPostModelCrs = buildIndonesiaStructuredProductBridge({
  source: { coordinateSystemExplicit: "", datumExplicit: "", points: [], pointOrder: [] },
  objects: [],
  unresolvedItems: []
});
assert.equal(missingPostModelCrs.status, INDONESIA_STRUCTURED_PRODUCT_STATUS.INVALID_REVIEW_REQUIRED);
assert.equal(missingPostModelCrs.reasonCode, "EXPLICIT_UTM50S_CRS_REQUIRED");

const conflictingPostModelDatum = buildIndonesiaStructuredProductBridge({
  source: { coordinateSystemExplicit: "UTM WGS 1984 ZONA 50S", datumExplicit: "NAD83", points: [], pointOrder: [] },
  objects: [],
  unresolvedItems: []
});
assert.equal(conflictingPostModelDatum.status, INDONESIA_STRUCTURED_PRODUCT_STATUS.INVALID_REVIEW_REQUIRED);
assert.equal(conflictingPostModelDatum.reasonCode, "EXPLICIT_DATUM_CONFLICT");

const configEnv = {
  INDONESIA_STRUCTURED_B_PRODUCT_ENABLED: "true",
  INDONESIA_STRUCTURED_B_RC_SINGLE_RUN_ENABLED: "true",
  INDONESIA_STRUCTURED_B_RC_SINGLE_RUN_ID: INDONESIA_STRUCTURED_B_RC.runId,
  INDONESIA_STRUCTURED_B_RC_ALLOWED_IMAGE_SHA256: INDONESIA_STRUCTURED_B_RC.imageSha256,
  INDONESIA_STRUCTURED_B_RC_SINGLE_RUN_WINDOW_SECONDS: String(INDONESIA_STRUCTURED_B_RC.runWindowSeconds),
  INDONESIA_STRUCTURED_B_RC_CLAIM_TTL_SECONDS: String(INDONESIA_STRUCTURED_B_RC.claimTtlSeconds),
  SUPABASE_URL: `https://${INDONESIA_STRUCTURED_B_RC.rcProjectRef}.supabase.co`,
  RC_SUPABASE_PROJECT_REF: INDONESIA_STRUCTURED_B_RC.rcProjectRef,
  RENDER_SERVICE_NAME: INDONESIA_STRUCTURED_B_RC.rcServiceName,
  ALIYUN_VISION_MODEL: INDONESIA_STRUCTURED_B_RC.model,
  ALIYUN_BASE_URL: `https://${INDONESIA_STRUCTURED_B_RC.providerHostname}/compatible-mode/v1`
};
const rpcCalls = [];
const admission = createIndonesiaStructuredBRcAdmission({
  env: configEnv,
  adminPassword: "unused-in-preflight",
  supabase: { rpc: async (name, args) => {
    rpcCalls.push({ name, args });
    return { data: { accepted: true, action: "ACTIVATED" }, error: null };
  } }
});
await admission.activateOnStartup();
const requestId = "11111111-1111-4111-8111-111111111111";
const authorization = `Bearer ${"a".repeat(32)}`;
assert.deepEqual(admission.preflightJob({
  mode: INDONESIA_STRUCTURED_B_RC.mode,
  recognitionRequestId: requestId,
  imageBuffer,
  authorization
}), { requestId, imageSha256: INDONESIA_STRUCTURED_B_RC.imageSha256 });
assert.throws(() => admission.preflightJob({
  mode: INDONESIA_STRUCTURED_B_RC.mode,
  recognitionRequestId: requestId,
  imageBuffer: Buffer.from("wrong-image"),
  authorization
}), error => error?.code === "INDONESIA_STRUCTURED_B_RC_JOB_PREFLIGHT_REJECTED");
assert.equal(rpcCalls.length, 1, "local preflight must not consume dispatch or provider budget");

async function assertDispatchRejectionPreventsProvider(rejectionCode) {
  let providerCallCount = 0;
  const dispatchRpcCalls = [];
  const rejectingAdmission = createIndonesiaStructuredBRcAdmission({
    env: configEnv,
    adminPassword: "unused-in-preflight",
    supabase: { rpc: async (name, args) => {
      dispatchRpcCalls.push({ name, args });
      if (name === "claim_indonesia_structured_b_single_run") {
        return { data: { accepted: true, action: "ACTIVATED" }, error: null };
      }
      if (name === "dispatch_indonesia_structured_b_single_run") {
        return { data: { accepted: false, action: "REJECTED", code: rejectionCode }, error: null };
      }
      throw new Error("unexpected_rpc");
    } }
  });
  await rejectingAdmission.activateOnStartup();
  rejectingAdmission.preflightJob({
    mode: INDONESIA_STRUCTURED_B_RC.mode,
    recognitionRequestId: requestId,
    imageBuffer,
    authorization
  });
  const route = selectIndonesiaStructuredProductRoute({
    enabled: true,
    requestedMode: INDONESIA_STRUCTURED_PRODUCT_MODE,
    sourceContextText: crsOnlyContext,
    controlledRcProjectedColumnsHintAuthorized: true
  });
  assert.equal(route.status, INDONESIA_STRUCTURED_ROUTE_STATUS.SELECTED);
  await assert.rejects(async () => {
    await rejectingAdmission.dispatch({
      mode: INDONESIA_STRUCTURED_B_RC.mode,
      recognitionRequestId: requestId,
      imageBuffer,
      authorization,
      asyncAuthorized: true
    });
    providerCallCount += 1;
  }, error => error?.code === rejectionCode);
  assert.equal(providerCallCount, 0, `${rejectionCode} must fail before provider invocation`);
  assert.deepEqual(dispatchRpcCalls.map(call => call.name), [
    "claim_indonesia_structured_b_single_run",
    "dispatch_indonesia_structured_b_single_run"
  ]);
}

for (const rejectionCode of [
  "INDONESIA_STRUCTURED_B_RC_TOKEN_INVALID",
  "INDONESIA_STRUCTURED_B_RC_WINDOW_CLOSED",
  "INDONESIA_STRUCTURED_B_RC_BUDGET_EXHAUSTED"
]) {
  await assertDispatchRejectionPreventsProvider(rejectionCode);
}

const serverSource = await readFile(path.join(root, "server.js"), "utf8");
const preflightIndex = serverSource.indexOf("indonesiaStructuredBRcAdmission.preflightJob({");
const routeIndex = serverSource.indexOf("const indonesiaStructuredRoute = selectIndonesiaStructuredProductRoute({", preflightIndex);
const dispatchIndex = serverSource.indexOf("await indonesiaStructuredBRcAdmission.dispatch({", routeIndex);
const providerIndex = serverSource.indexOf("structuredProviderResponse = await callAliyunVision(structuredProviderRequest);", dispatchIndex);
assert.ok(preflightIndex >= 0 && preflightIndex < routeIndex);
assert.ok(routeIndex < dispatchIndex && dispatchIndex < providerIndex,
  "authoritative dispatch budget gate must remain before the one provider call");
assert.match(serverSource,
  /indonesiaStructuredRcSelected\s*&&\s*req\.indonesiaStructuredRcAsyncAuthorized\s*===\s*true/u);
assert.match(serverSource,
  /sourceContextText:\s*oneShotLocalOcrSourceContextText,\s*controlledRcProjectedColumnsHintAuthorized/u);

console.log("Indonesia structured RC projected-columns hint regression: PASS (Provider calls: 0)");
