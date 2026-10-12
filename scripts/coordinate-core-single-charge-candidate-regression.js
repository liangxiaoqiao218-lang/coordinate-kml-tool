import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath, pathToFileURL } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const indexSource = fs.readFileSync(path.join(root, "index.html"), "utf8");
const serverSource = fs.readFileSync(path.join(root, "server.js"), "utf8");
const bridgeSource = fs.readFileSync(
  path.join(root, "server", "recognition", "indonesia-structured-product-bridge.js"),
  "utf8"
);
const adapterSource = fs.readFileSync(
  path.join(root, "server", "recognition", "qwen38-coordinate-structured-adapter.mjs"),
  "utf8"
);
const bridgeModule = await import(pathToFileURL(
  path.join(root, "server", "recognition", "indonesia-structured-product-bridge.js")
).href);

function sliceBetween(source, start, end) {
  const startIndex = source.indexOf(start);
  const endIndex = source.indexOf(end, startIndex);
  assert.ok(startIndex >= 0, `missing start marker: ${start}`);
  assert.ok(endIndex > startIndex, `missing end marker: ${end}`);
  return source.slice(startIndex, endIndex);
}

const dynamicCases = [];
const staticAssertions = [];
const record = (collection, name, action) => {
  action();
  collection.push(name);
};

const bindingSource = sliceBetween(
  indexSource,
  "const RECOGNITION_USAGE_BINDING_STATUS",
  "function cloneIdentityBoundValue"
);
const context = {
  Object,
  String,
  Boolean,
  isRecognitionRequestId: value => /^[0-9a-f-]{36}$/u.test(String(value || "")),
  ensureVisitorId: () => "visitor",
  recovery: null
};
vm.runInNewContext(`
  let activeFinalizedCoordinateResult = null;
  let activeRecognitionUsageBinding = null;
  let activeRecognitionUsageIdentity = null;
  let finalizedCoordinateDirty = false;
  let finalizedCoordinateTextSnapshot = "recognized";
  let coordinateInputEditRevision = 1;
  const input = { value: "recognized" };
  function getFinalizedCoordinateIdentity(result = activeFinalizedCoordinateResult) {
    if (!result?.resultId || !Number.isSafeInteger(result?.resultRevision)
      || !result?.geometryHash || !result?.geometry) return null;
    return { resultId: result.resultId, resultRevision: result.resultRevision, geometryHash: result.geometryHash };
  }
  async function recoverCommittedCoordinateResult() { return globalThis.recovery(); }
  ${bindingSource}
  globalThis.bindingApi = {
    statuses: RECOGNITION_USAGE_BINDING_STATUS,
    remember: rememberCommittedRecognitionUsageBinding,
    verify: verifyCommittedRecognitionUsageBindingForCurrentResult,
    stillCurrent: recognitionUsageVerificationStillCurrent,
    restore: restoreCommittedRecognitionUsageBindingFromSnapshot,
    setCurrent(result) { activeFinalizedCoordinateResult = result; },
    setDraft({ dirty = false, snapshot = "recognized", text = "recognized", revision = 1 } = {}) {
      finalizedCoordinateDirty = dirty;
      finalizedCoordinateTextSnapshot = snapshot;
      input.value = text;
      coordinateInputEditRevision = revision;
    }
  };
`, context);

const api = context.bindingApi;
const requestId = "11111111-1111-4111-8111-111111111111";
const original = Object.freeze({
  resultId: "result-1",
  resultRevision: 1,
  geometryHash: "a".repeat(64),
  geometry: { type: "Point", coordinates: [1, 2] }
});
const edited = Object.freeze({ ...original, resultRevision: 2, geometryHash: "b".repeat(64) });

record(dynamicCases, "no recognition binding is not applicable", async () => {});
api.setCurrent(original);
assert.equal(await api.verify(), api.statuses.NOT_APPLICABLE);

record(dynamicCases, "exact server-recovered identity verifies", () => {});
assert.equal(api.remember({ requestId, result: original }), true);
context.recovery = async () => ({
  response: { ok: true },
  data: { success: true, recovered: true, usageConsumed: true, requestId, finalizedCoordinateResult: original }
});
assert.equal(await api.verify(), api.statuses.VERIFIED);
assert.equal(await api.verify(), api.statuses.VERIFIED);

record(dynamicCases, "known recognized identity without recoverable request fails closed", () => {});
assert.equal(api.remember({ requestId: "invalid", result: original }), false);
assert.equal(await api.verify(), api.statuses.UNVERIFIED);
assert.equal(api.remember({ requestId, result: original }), true);

record(dynamicCases, "usage verification rejects a concurrent draft change", () => {});
api.setDraft();
const verificationSnapshot = { identity: original, inputRevision: 1, sourceText: "recognized" };
assert.equal(api.stillCurrent(verificationSnapshot), true);
api.setDraft({ dirty: true, snapshot: "recognized", text: "edited", revision: 2 });
assert.equal(api.stillCurrent(verificationSnapshot), false);
api.setDraft();

record(dynamicCases, "identity-bound history snapshot restores only a server-verifiable recognition binding", () => {});
assert.equal(api.restore({
  recognitionUsage: { identity: original, binding: { requestId, identity: original } }
}, original), true);
assert.equal(await api.verify(), api.statuses.VERIFIED);

record(dynamicCases, "conflicting CRS evidence blocks the structured route", () => {});
const selectedRoute = bridgeModule.selectIndonesiaStructuredProductRoute({
  enabled: true,
  requestedMode: bridgeModule.INDONESIA_STRUCTURED_PRODUCT_MODE,
  sourceContextText: "UTM WGS 1984 ZONA 50 S X Y"
});
assert.equal(selectedRoute.status, bridgeModule.INDONESIA_STRUCTURED_ROUTE_STATUS.SELECTED);
for (const conflictingContext of [
  "UTM WGS 1984 ZONA 50 S EPSG:3440 X Y",
  "EPSG:32750 ITRF 2008 EASTING NORTHING",
  "UTM WGS 1984 ZONA 50 S UTM ZONE 49 S X Y",
  "EPSG:32750 BFTM X Y"
]) {
  const blocked = bridgeModule.selectIndonesiaStructuredProductRoute({
    enabled: true,
    requestedMode: bridgeModule.INDONESIA_STRUCTURED_PRODUCT_MODE,
    sourceContextText: conflictingContext
  });
  assert.equal(blocked.status, bridgeModule.INDONESIA_STRUCTURED_ROUTE_STATUS.BLOCKED);
  assert.equal(blocked.reasonCode, "STRUCTURED_PRODUCT_SOURCE_CRS_CONFLICT");
  assert.equal(blocked.evidence.sourceCrsConflict, true);
}

record(dynamicCases, "changed identity is not covered by old charge", () => {});
api.setCurrent(edited);
assert.equal(await api.verify(), api.statuses.NOT_APPLICABLE);

record(dynamicCases, "mismatched recovered identity fails closed", () => {});
api.setCurrent(original);
context.recovery = async () => ({
  response: { ok: true },
  data: { success: true, recovered: true, usageConsumed: true, requestId, finalizedCoordinateResult: edited }
});
assert.equal(await api.verify(), api.statuses.UNVERIFIED);

record(dynamicCases, "recovery failure fails closed", () => {});
context.recovery = async () => { throw new Error("offline simulated recovery failure"); };
assert.equal(await api.verify(), api.statuses.UNVERIFIED);

record(staticAssertions, "ordinary structured entry is product-flagged", () => {
  assert.match(serverSource, /const indonesiaStructuredProductUiAvailable = indonesiaStructuredProductEnabled;/u);
  assert.doesNotMatch(serverSource, /indonesiaStructuredProductUiAvailable = process\.env\.NODE_ENV !== "production"/u);
  assert.match(indexSource, /appendIndonesiaStructuredProductMode\(formData\)/u);
  assert.match(indexSource, /UTM 50S 坐标表（仅在原图明确显示坐标系与 X\/Y 列时使用）/u);
});

record(staticAssertions, "structured output remains deterministic and review-required", () => {
  const modelEvidenceStatusIndex = serverSource.indexOf(
    "INDONESIA_STRUCTURED_ROUTE_STATUS.MODEL_SOURCE_EVIDENCE_REQUIRED"
  );
  const structuredRouteStart = serverSource.lastIndexOf("if ([", modelEvidenceStatusIndex);
  const structuredRouteEnd = serverSource.indexOf("const selectedProviderPrompt", modelEvidenceStatusIndex);
  assert.ok(modelEvidenceStatusIndex >= 0 && structuredRouteStart >= 0 && structuredRouteEnd > structuredRouteStart);
  const structuredRouteBlock = serverSource.slice(structuredRouteStart, structuredRouteEnd);
  assert.match(bridgeSource, /EXPLICIT_UTM50S_CRS_REQUIRED/u);
  assert.match(bridgeSource, /STRUCTURED_POINT_ORDER_MISMATCH/u);
  assert.match(bridgeSource, /ACCEPTED_REVIEW_REQUIRED/u);
  assert.match(adapterSource, /MODEL = "qwen3\.8-flash"/u);
  assert.match(adapterSource, /INVALID_STRUCTURED/u);
  assert.match(adapterSource, /response\.model === expectedModel/u);
  assert.equal(Array.from(structuredRouteBlock.matchAll(/callAliyunVision\(/gu)).length, 1);
  assert.match(structuredRouteBlock, /本路径不重试，也不调用旧 OCR 或其他模型/u);
});

record(staticAssertions, "missing reset marker preserves current balances", () => {
  const normalizeBlock = sliceBetween(serverSource, "async function normalizeSupabaseDailyFreeQuota", "async function updateSupabaseUserVisitMeta");
  assert.match(normalizeBlock, /if \(!resetField\) return user;/u);
  assert.ok(normalizeBlock.indexOf("if (!resetField) return user;") < normalizeBlock.indexOf("getTodayFreeUsageCounts"));
});

record(staticAssertions, "KML verifies committed identity before skipping consumption", () => {
  const kmlBlock = sliceBetween(indexSource, "async function downloadKmlInternal", "function isRecognitionRequestId");
  assert.match(kmlBlock, /verifyCommittedRecognitionUsageBindingForCurrentResult/u);
  assert.match(kmlBlock, /recognitionUsageVerificationStillCurrent\(usageVerificationSnapshot\)/u);
  assert.match(kmlBlock, /KML_RESULT_CHANGED_DURING_USAGE_VERIFICATION/u);
  assert.ok(kmlBlock.indexOf("recognitionUsageVerificationStillCurrent(usageVerificationSnapshot)") < kmlBlock.indexOf("const groups = finalizedGeometrySource.groups"));
  assert.match(kmlBlock, /KML_RECOGNITION_USAGE_BINDING_UNVERIFIED/u);
  assert.match(kmlBlock, /recognitionUsageBindingStatus === RECOGNITION_USAGE_BINDING_STATUS\.NOT_APPLICABLE[\s\S]*consumeUsage\("convert"\)/u);
  assert.doesNotMatch(kmlBlock, /usageConsumed\s*===\s*true[\s\S]*consumeUsage\("convert"\)/u);
});

record(staticAssertions, "ordinary recognition stores request and finalized identity", () => {
  assert.match(indexSource, /rememberCommittedRecognitionUsageBinding\(\{[\s\S]*requestId: data\.requestId \|\| recognitionRequestId,[\s\S]*result: activeFinalizedCoordinateResult/u);
  assert.match(bindingSource, /recovered\.data\?\.recovered === true/u);
  assert.match(bindingSource, /finalizedCoordinateIdentityEquals\(currentIdentity, recoveredIdentity\)/u);
  assert.match(indexSource, /recognitionUsage: finalizedCoordinateIdentityEquals\(activeRecognitionUsageIdentity, identity\)/u);
  assert.match(indexSource, /restoreCommittedRecognitionUsageBindingFromSnapshot\(snapshot, serverResult\)/u);
});

const total = dynamicCases.length + staticAssertions.length;
console.log(JSON.stringify({
  status: "PASS",
  evidenceClass: "OFFLINE_NO_NETWORK",
  dynamicPassed: dynamicCases.length,
  staticPassed: staticAssertions.length,
  totalPassed: total,
  providerCalls: 0,
  databaseWrites: 0
}));
