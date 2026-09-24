import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomInt, randomUUID } from "node:crypto";
import { once } from "node:events";
import net from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";
import sharp from "sharp";
import { readFile } from "node:fs/promises";
import { evaluateUnifiedRecognitionFinalAuthorization } from "../server/recognition/recognition-first-acquisition.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const requestId = "66666666-6666-4666-8666-666666666666";
const syntheticXOrigin = randomInt(500_000, 700_000);
const syntheticYOrigin = randomInt(1_000_000, 2_000_000);
const providerText = [
  "CONTEXT | ITRF 2008 / Projection BFTM",
  "HEADING | Boundary",
  "Point X Y",
  ...Array.from({ length: 20 }, (_, index) => (
    `${index + 1} ${syntheticXOrigin - (index * 500)} ${syntheticYOrigin + (index * 1200)}`
  ))
].join("\n");

const completeEvidence = {
  candidateCoordinates: [{}],
  candidateCoordinateGroups: [{}],
  visibleCrsEvidence: [],
  imageEvidence: { imageCount: 2 }
};
const forgedLegacyAuthorization = evaluateUnifiedRecognitionFinalAuthorization({
  body: {
    requiresReview: false,
    authorizationStatus: "AUTHORIZED",
    resultStatus: "authorized",
    mapReady: true,
    kmlReady: true,
    finalizedCoordinateResult: {
      decisionState: "AUTO_EXPORT",
      qualityGateStatus: "passed",
      requiresReview: false,
      kmlReady: true,
      geometry: { type: "Polygon", coordinates: [] },
      crs: { id: "EPSG:4326" }
    }
  },
  evidence: completeEvidence,
  decision: { authorizationStatus: "VALIDATION_PENDING", resultStatus: "validation_pending" },
  conformance: { status: "REVIEW_REQUIRED" }
});
assert.equal(forgedLegacyAuthorization.authorized, false);
assert.equal(forgedLegacyAuthorization.contractRequiresReview, true);

const missingUnifiedEvidence = evaluateUnifiedRecognitionFinalAuthorization({
  body: {
    requiresReview: false,
    mapReady: true,
    kmlReady: true,
    finalizedCoordinateResult: {
      decisionState: "AUTO_EXPORT",
      qualityGateStatus: "passed",
      requiresReview: false,
      kmlReady: true,
      geometry: { type: "Polygon", coordinates: [] },
      crs: { id: "EPSG:4326" }
    }
  },
  evidence: null,
  decision: { authorizationStatus: "VALIDATION_PENDING", resultStatus: "validation_pending" },
  conformance: { status: "CONFORMANT" }
});
assert.equal(missingUnifiedEvidence.authorized, false);
assert.equal(missingUnifiedEvidence.hasUnifiedEvidence, false);

if (process.argv.includes("--server")) {
  const { default: http } = await import("node:http");
  const nativeFetch = globalThis.fetch;
  let providerCalls = 0;
  globalThis.fetch = async (url, init) => {
    if (String(url) !== "http://127.0.0.1:1/v1/chat/completions") {
      return nativeFetch(url, init);
    }
    providerCalls += 1;
    assert.equal(providerCalls, 1);
    return new Response(JSON.stringify({
      id: "offline-provider-v6",
      choices: [{ message: { content: providerText } }],
      usage: { total_tokens: 1 }
    }), { status: 200, headers: { "content-type": "application/json" } });
  };
  const nativeListen = http.Server.prototype.listen;
  http.Server.prototype.listen = function listen(port, callback) {
    this.once("listening", () => process.send({ port: this.address().port }));
    return nativeListen.call(this, port, "127.0.0.1", callback);
  };
  process.on("message", message => {
    if (message === "stats") process.send({ providerCalls });
  });
  await import("../server.js");
  await new Promise(() => {});
}

const portProbe = net.createServer();
portProbe.listen(0, "127.0.0.1");
await once(portProbe, "listening");
const reservedPort = portProbe.address().port;
await new Promise((resolve, reject) => portProbe.close(error => (error ? reject(error) : resolve())));

const child = spawn(process.execPath, [fileURLToPath(import.meta.url), "--server"], {
  cwd: root,
  windowsHide: true,
  stdio: ["ignore", "pipe", "pipe", "ipc"],
  env: {
    SystemRoot: process.env.SystemRoot,
    PATH: process.env.PATH,
    NODE_ENV: "test",
    PORT: String(reservedPort),
    ENABLE_REGRESSION_TEST_MODE: "true",
    ALIYUN_API_KEY: "local-mock-only",
    ALIYUN_BASE_URL: "http://127.0.0.1:1/v1",
    DOTENV_CONFIG_PATH: path.join(root, "__no_test_env__")
  }
});
let stdout = "";
let stderr = "";
child.stdout.on("data", chunk => { stdout += chunk.toString(); });
child.stderr.on("data", chunk => { stderr += chunk.toString(); });
const signal = AbortSignal.timeout(45_000);

try {
  const [{ port }] = await once(child, "message", { signal });
  const syntheticLongImage = await sharp({
    create: {
      width: 800,
      height: 3200,
      channels: 3,
      background: "white"
    }
  }).png().toBuffer();
  const form = new FormData();
  form.set("visitorId", "async-unified-v6-regression");
  form.set("image", new Blob([syntheticLongImage], { type: "image/png" }), `${randomUUID()}.png`);
  const enqueueResponse = await fetch(`http://127.0.0.1:${port}/api/recognize-coordinates/jobs`, {
    method: "POST",
    headers: {
      "x-regression-test": "1",
      "x-visitor-id": "async-unified-v6-regression",
      "x-recognition-request-id": requestId
    },
    body: form,
    signal
  });
  assert.equal(enqueueResponse.status, 202);
  const enqueued = await enqueueResponse.json();
  assert.equal(enqueued.requestId, requestId);
  assert.match(enqueued.jobId, /^[0-9a-f-]{36}$/u);
  assert.match(enqueued.jobAccessToken, /^[A-Za-z0-9_-]{32,128}$/u);

  let snapshot;
  for (let attempt = 0; attempt < 500; attempt += 1) {
    const response = await fetch(`http://127.0.0.1:${port}/api/recognize-coordinates/jobs/${enqueued.jobId}`, {
      headers: { "x-recognition-job-token": enqueued.jobAccessToken },
      signal
    });
    snapshot = await response.json();
    if (["SUCCEEDED", "FAILED"].includes(snapshot.status)) break;
    await new Promise(resolve => setTimeout(resolve, 10));
  }

  const result = snapshot?.result || {};
  const diagnostic = JSON.stringify({ snapshot: { ...snapshot, result }, stdout, stderr });
  assert.equal(snapshot?.status, "SUCCEEDED", diagnostic);
  assert.equal(snapshot?.httpStatus, 200, diagnostic);
  assert.equal(snapshot?.requestId, requestId);
  assert.equal(result.requestId, requestId);
  assert.equal(result.success, true);
  assert.equal(result.providerCompletionState, "SUCCEEDED");
  assert.equal(result.providerCallCount, 1);
  assert.equal(result.acquisitionStatus, "COMPLETED");
  assert.equal(result.authorizationStatus, "REVIEW_REQUIRED");
  assert.equal(result.resultStatus, "needs_review");
  assert.equal(result.requiresReview, true);
  assert.equal(result.boundaryBlocked, true);
  assert.equal(result.mapReady, false);
  assert.equal(result.kmlReady, false);
  assert.equal(result.mapStatus, "CLOSED");
  assert.equal(result.kmlStatus, "CLOSED");
  assert.equal(result.usageConsumed, false);
  assert.equal(result.userUsageConsumed, false);
  assert.equal(result.recoveryRequired, false);
  assert.equal(result.candidateCoordinates.length, 20);
  assert.equal(result.candidateCoordinateGroups.length, 1);
  assert.equal(result.recognitionAcquisition.diagnostics.boundRowCount, 20);
  assert.equal(result.recognitionAcquisition.diagnostics.unboundRowCount, 0);
  assert.ok(result.visibleCrsEvidence.some(item => /BFTM/iu.test(item.text)));
  assert.ok(result.imageAcquisitionEvidence.imageCount > 1);
  assert.ok(result.imageAcquisitionEvidence.detailTileCount > 0);
  assert.ok(result.contractReasons.includes("COORDINATE_FORMAT_REQUIRES_VALIDATION"));
  assert.ok(result.reviewReasons.includes("COORDINATE_FORMAT_REQUIRES_VALIDATION"));
  assert.notEqual(result.finalizedCoordinateResult?.decisionState, "AUTO_EXPORT");
  assert.equal(result.finalizedCoordinateResult?.requiresReview, true);
  assert.equal(result.finalizedCoordinateResult?.kmlReady, false);

  const statsPromise = once(child, "message", { signal });
  child.send("stats");
  const [stats] = await statsPromise;
  assert.equal(stats.providerCalls, 1);
  assert.equal((stdout.match(/Recognition acquisition evidence:/gu) || []).length, 1);
  assert.equal((stdout.match(/Recognition acquisition final state:/gu) || []).length, 1);
  assert.equal(stdout.includes(String(syntheticXOrigin)), false);
  assert.doesNotMatch(stderr, /(?:Error|ERR_|Unhandled|AssertionError)/u);

  const indexSource = await readFile(path.join(root, "index.html"), "utf8");
  assert.match(
    indexSource,
    /markPendingCoordinateRecognitionJobTerminalApplied\(\s*terminalRecognitionJobSnapshot,\s*terminalRecognitionJobSnapshot,\s*\{ applied: true \}/u
  );
  assert.doesNotMatch(indexSource, /if \(status === "SUCCEEDED" \|\| status === "FAILED"\) \{\s*clearPendingCoordinateRecognitionJob\(\)/u);
  assert.match(indexSource, /activeRecognitionAcquisitionResult\?\.mapStatus === "CLOSED"[\s\S]*agenticCoordinateController\?\.enabled/u);
  assert.match(indexSource, /activeRecognitionAcquisitionResult\?\.kmlStatus === "CLOSED"[\s\S]*agenticCoordinateController\?\.enabled/u);
  console.log("recognition async unified evidence v6: PASS");
} finally {
  const ended = once(child, "exit");
  child.kill();
  await ended;
}
