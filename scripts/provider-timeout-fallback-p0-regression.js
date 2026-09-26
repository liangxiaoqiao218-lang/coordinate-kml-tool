import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  PROVIDER_TIMEOUT_AFTER_LOCAL_OCR_CODE,
  planProviderTimeoutLocalOcrRecovery
} from "../server/recognition/provider-timeout-recovery.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const serverSource = await readFile(path.join(root, "server.js"), "utf8");

if (process.argv[2] === "--timeout-http-child") {
  const { default: http } = await import("node:http");
  let providerCalls = 0;
  const nativeListen = http.Server.prototype.listen;
  http.Server.prototype.listen = function patchedListen(_port, callback) {
    return nativeListen.call(this, 0, "127.0.0.1", () => {
      process.send?.({ port: this.address().port });
      callback?.();
    });
  };
  globalThis.fetch = async (_url, init = {}) => {
    providerCalls += 1;
    if (providerCalls > 1) throw new Error("TEST_UNEXPECTED_SECOND_PROVIDER_CALL");
    return await new Promise((resolve, reject) => {
      const abort = () => {
        const error = new Error("mock provider timed out");
        error.name = "AbortError";
        reject(error);
      };
      if (init.signal?.aborted) return abort();
      init.signal?.addEventListener("abort", abort, { once: true });
      setTimeout(abort, 25);
    });
  };
  process.on("message", message => {
    if (message === "stats") process.send?.({ providerCalls });
  });
  await import("../server.js");
  await new Promise(() => {});
}

async function runTimeoutHttpScenario(sourceText) {
  const preload = `import { registerHooks } from 'node:module';
registerHooks({load(url, context, nextLoad) {
  const result = nextLoad(url, context);
  if (!url.endsWith('/server.js')) return result;
  let source = String(result.source);
  const callStart = source.indexOf('    const oneShotFamilyClassification = await runLocalOcrFamilyClassification({');
  const callEnd = source.indexOf('    });', callStart) + 7;
  if (callStart < 0 || callEnd < 7) throw new Error('TIMEOUT_RECOVERY_INJECTION_NOT_INSTALLED');
  const injected = ${JSON.stringify(`    recognitionBudget?.assertCanStartLocalOcr({ stageName: "local_ocr", minRequiredMs: 2500, lowValue: false });
    const testSourceText = String(process.env.TEST_LOCAL_OCR_SOURCE_TEXT || "");
    const layoutLines = Object.freeze(testSourceText.split(/\\r?\\n/u).filter(Boolean).map((text, index) => ({
    text, bbox: [10, 10 + (index * 30), 900, 30 + (index * 30)], local_line_index: index,
    page: coordinateImageIdentity.page, resultRevision: 1, image_sha256: coordinateImageIdentity.image_sha256
    })));
    const route = classifyOneShotStructuredFamily({ text: testSourceText, layoutLines });
    const oneShotFamilyClassification = { attempted: true, route, sourceText: testSourceText, layoutLines, axisOrderEvidence: null,
      projectedTableOcrAcquisition: shouldUseProjectedTableOcrAcquisition(testSourceText),
      contract: createOneShotAcquisitionContract({ route, sourceText: testSourceText, layoutLines, imageIdentity: coordinateImageIdentity, resultRevision: 1 }) };
`)};
  return {...result, source: source.slice(0, callStart) + injected + source.slice(callEnd)};
}});`;
  const child = spawn(process.execPath, [
    "--import", `data:text/javascript,${encodeURIComponent(preload)}`,
    fileURLToPath(import.meta.url), "--timeout-http-child"
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
      ALIYUN_API_KEY: "local-mock-only",
      ALIYUN_BASE_URL: "http://127.0.0.1:1/v1",
      P0_QUALIFICATION_ACQUISITION_ENABLED: "true",
      SPATIAL_RESULT_ENABLED: "true",
      TEST_LOCAL_OCR_SOURCE_TEXT: sourceText,
      DOTENV_CONFIG_PATH: path.join(root, "__no_test_env__")
    }
  });
  child.stdout.resume();
  child.stderr.resume();
  const signal = AbortSignal.timeout(15_000);
  try {
    const [{ port }] = await once(child, "message", { signal });
    const fixture = await readFile(path.join(root, "regression-samples", "production-recognition-recovery-p0", "indonesia-utm50s-real-002.jpg"));
    const form = new FormData();
    form.set("visitorId", "provider-timeout-fallback-p0");
    form.set("image", new Blob([fixture], { type: "image/jpeg" }), "projected-table.jpg");
    const response = await fetch(`http://127.0.0.1:${port}/api/recognize-coordinates`, {
      method: "POST",
      headers: { "x-regression-test": "1", "x-regression-case-id": "indonesia-utm50s-real-002" },
      body: form,
      signal
    });
    const payload = await response.json();
    const statsPromise = once(child, "message", { signal });
    child.send("stats");
    const [stats] = await statsPromise;
    return { status: response.status, payload, providerCalls: stats.providerCalls };
  } finally {
    child.kill();
    await once(child, "exit").catch(() => {});
  }
}

const reused = planProviderTimeoutLocalOcrRecovery({
  localOcrAttempted: true,
  sourceText: "NC | XV | YV\n1 | 510000 | 9700100\n2 | 510100 | 9700050\n3 | 510000 | 9699900",
  layoutLines: [{ text: "NC | XV | YV" }],
  axisOrderEvidence: { status: "REVIEW_REQUIRED" },
  projectedTableOcrAcquisition: true
});
assert.equal(reused.allowNewLocalOcr, false);
assert.equal(reused.hasReusableEvidence, true);
assert.equal(reused.evidence.projectedTableOcrAcquisition, true);
assert.equal(reused.evidence.layoutLines.length, 1);

const emptyAfterAttempt = planProviderTimeoutLocalOcrRecovery({ localOcrAttempted: true });
assert.equal(emptyAfterAttempt.allowNewLocalOcr, false);
assert.equal(emptyAfterAttempt.hasReusableEvidence, false);
assert.equal(emptyAfterAttempt.shouldFailClosedWithoutNewOcr, true);

const unused = planProviderTimeoutLocalOcrRecovery({ localOcrAttempted: false, sourceText: "must-not-reuse" });
assert.equal(unused.allowNewLocalOcr, true);
assert.equal(unused.hasReusableEvidence, false);
assert.equal(unused.evidence.sourceText, "");

const timeoutCatchStart = serverSource.indexOf("const timeoutLocalOcrRecovery = planProviderTimeoutLocalOcrRecovery");
const fallbackStart = serverSource.indexOf("const fallback = timeoutRoutingOcrFallback", timeoutCatchStart);
assert.ok(timeoutCatchStart >= 0 && fallbackStart > timeoutCatchStart);
const timeoutRoutingSource = serverSource.slice(timeoutCatchStart, fallbackStart);
assert.match(timeoutRoutingSource, /localOcrAttempted:\s*oneShotLocalOcrAttempted/);
assert.match(timeoutRoutingSource, /timeoutLocalOcrRecovery\.allowNewLocalOcr/);
assert.doesNotMatch(timeoutRoutingSource, /localOcrAttempted:\s*false/);
assert.equal((timeoutRoutingSource.match(/runLocalOcrFallback\(/gu) || []).length, 2);
assert.match(serverSource, new RegExp(PROVIDER_TIMEOUT_AFTER_LOCAL_OCR_CODE));
assert.match(serverSource, /fallback\.reusedLocalOcrEvidence\s*\?\s*keepReusedLocalOcrEvidenceAsReview/u);
const reviewGuardStart = serverSource.indexOf("function keepReusedLocalOcrEvidenceAsReview");
const reviewGuardEnd = serverSource.indexOf("function ", reviewGuardStart + 10);
const reviewGuardSource = serverSource.slice(reviewGuardStart, reviewGuardEnd);
for (const required of [
  /requiresReview:\s*true/u,
  /mapReady:\s*false/u,
  /kmlReady:\s*false/u,
  /mapStatus:\s*"CLOSED"/u,
  /kmlStatus:\s*"CLOSED"/u,
  /previewEligibility:\s*\{\s*allowed:\s*false\s*\}/u
]) assert.match(reviewGuardSource, required);

for (const phase of ["request_dispatched", "response_headers_received", "response_body_complete"]) {
  assert.match(serverSource, new RegExp(`phase: \\"${phase}\\"`));
  const phaseIndex = serverSource.indexOf(`phase: "${phase}"`);
  const logStart = serverSource.lastIndexOf('console.log("[Aliyun] Provider phase"', phaseIndex);
  const logEnd = serverSource.indexOf("});", phaseIndex) + 3;
  const phaseLogSource = serverSource.slice(logStart, logEnd);
  assert.doesNotMatch(phaseLogSource, /requestBody|responseBody|rawText|authorization|cookie|apiKey|secret/iu);
}

const insufficient = await runTimeoutHttpScenario("");
assert.equal(insufficient.status, 503, JSON.stringify(insufficient.payload));
assert.equal(insufficient.providerCalls, 1);
assert.equal(insufficient.payload.code, PROVIDER_TIMEOUT_AFTER_LOCAL_OCR_CODE);
assert.equal(insufficient.payload.usageConsumed, false);
assert.equal(insufficient.payload.retryAllowed, true);
assert.equal(insufficient.payload.mapReady, false);
assert.equal(insufficient.payload.kmlReady, false);
assert.equal(insufficient.payload.localOcrCallCount, 1);

console.log("provider-timeout-fallback-p0-regression: PASS");
