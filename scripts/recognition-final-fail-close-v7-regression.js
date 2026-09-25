import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomInt, randomUUID } from "node:crypto";
import { once } from "node:events";
import { readFile } from "node:fs/promises";
import net from "node:net";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import sharp from "sharp";
import { evaluateUnifiedRecognitionFinalAuthorization } from "../server/recognition/recognition-first-acquisition.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const requestId = "77777777-7777-4777-8777-777777777777";
const syntheticXOrigin = Number(process.env.V7_SYNTHETIC_X_ORIGIN) || randomInt(520_000, 700_000);
const syntheticYOrigin = Number(process.env.V7_SYNTHETIC_Y_ORIGIN) || randomInt(1_220_000, 1_520_000);
const projectedBoundaryOffsets = [
  [0, 0], [400, 0], [800, 0], [1200, 0], [1600, 0],
  [1600, 400], [1600, 800], [1600, 1200], [1600, 1600], [1600, 2000],
  [1200, 2000], [800, 2000], [400, 2000], [0, 2000], [-400, 2000],
  [-400, 1600], [-400, 1200], [-400, 800], [-400, 400], [-200, 200]
];
const providerText = [
  "CONTEXT | ITRF 2008 / Projection BFTM",
  "HEADING | Projected boundary table",
  "Vertex | X (m) | Y (m)",
  ...projectedBoundaryOffsets.map(([xOffset, yOffset], index) => (
    `${index + 1} | ${syntheticXOrigin + xOffset} | ${syntheticYOrigin + yOffset}`
  ))
].join("\n");

const incompleteAuthorization = evaluateUnifiedRecognitionFinalAuthorization({
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
      mapReady: true,
      kmlReady: true,
      geometry: { type: "Polygon", coordinates: [] },
      crs: { id: "EPSG:4326" }
    }
  },
  evidence: {
    acquisitionStatus: "NO_COORDINATE_EVIDENCE",
    candidateCoordinates: [{}],
    candidateCoordinateGroups: [{}],
    visibleCrsEvidence: [],
    imageEvidence: { imageCount: 2 }
  },
  decision: {
    acquisitionStatus: "NO_COORDINATE_EVIDENCE",
    authorizationStatus: "NOT_ESTABLISHED",
    resultStatus: "failed"
  },
  conformance: { status: "CONFORMANT" }
});
assert.equal(incompleteAuthorization.authorized, false);
assert.equal(incompleteAuthorization.acquisitionIncomplete, true);
assert.equal(incompleteAuthorization.hasUnifiedEvidence, false);

if (process.argv.includes("--server")) {
  const { default: http } = await import("node:http");
  const nativeFetch = globalThis.fetch;
  let providerCalls = 0;
  globalThis.fetch = async (url, init) => {
    if (String(url) !== "http://127.0.0.1:1/v1/chat/completions") return nativeFetch(url, init);
    providerCalls += 1;
    assert.equal(providerCalls, 1);
    return new Response(JSON.stringify({
      id: "offline-provider-v7",
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

function sliceBetween(source, start, end) {
  const startIndex = source.indexOf(start);
  const endIndex = source.indexOf(end, startIndex + start.length);
  assert.notEqual(startIndex, -1, `Missing source marker: ${start}`);
  assert.notEqual(endIndex, -1, `Missing source marker: ${end}`);
  return source.slice(startIndex, endIndex);
}

const indexSource = await readFile(path.join(root, "index.html"), "utf8");
const browserGuardSource = [
  sliceBetween(indexSource, "function hasCompleteUnifiedRecognitionEvidence", "function refreshMapPreviewAction"),
  sliceBetween(indexSource, "function refreshMapPreviewAction", "function coordinateKmlVisualState"),
  sliceBetween(indexSource, "function coordinateKmlVisualState", "function syncKmlActionVisualState"),
  sliceBetween(indexSource, "function createAgenticSpatialPayload", "async function openAgenticSpatialResult"),
  sliceBetween(indexSource, "async function openAgenticSpatialResult", "async function openSpatialResult"),
  sliceBetween(indexSource, "async function downloadAgenticCoordinateKml", "async function downloadKml")
].join("\n");
let finalizeCalls = 0;
const browserContext = vm.createContext({
  activeRecognitionAcquisitionResult: null,
  activeFinalizedCoordinateResult: null,
  agenticCoordinateController: {
    enabled: true,
    finalize: async () => {
      finalizeCalls += 1;
      throw new Error("finalize must not be called for missing unified authorization");
    }
  },
  input: { value: "review candidate" },
  mapPreviewAction: {
    hidden: true,
    disabled: false,
    dataset: {},
    setAttribute() {},
    textContent: "",
    title: ""
  },
  syncKmlActionVisualState() {},
  refreshSpatialShareActions() {},
  showMessage() {},
  Blob,
  URL,
  document: { createElement() { return {}; }, body: { append() {} } }
});
vm.runInContext(browserGuardSource, browserContext);
const missingEvidenceState = browserContext.createRecognitionAuthorizationState({
  success: true,
  acquisitionStatus: "COMPLETED",
  authorizationStatus: "AUTHORIZED",
  resultStatus: "authorized",
  mapStatus: "ENABLED",
  kmlStatus: "ENABLED",
  kmlReady: true
});
assert.equal(missingEvidenceState.evidenceComplete, false);
assert.equal(missingEvidenceState.authorizationStatus, "REVIEW_REQUIRED");
assert.equal(missingEvidenceState.resultStatus, "needs_review");
assert.equal(missingEvidenceState.mapStatus, "CLOSED");
assert.equal(missingEvidenceState.kmlStatus, "CLOSED");
assert.equal(missingEvidenceState.kmlReady, false);
browserContext.refreshMapPreviewAction();
assert.equal(browserContext.mapPreviewAction.disabled, true);
assert.equal(browserContext.mapPreviewAction.dataset.state, "blocked");
assert.equal(browserContext.coordinateKmlVisualState(), "blocked");
await browserContext.openAgenticSpatialResult();
await browserContext.downloadAgenticCoordinateKml();
assert.equal(finalizeCalls, 0);
const reviewSpatialPayload = browserContext.createAgenticSpatialPayload({
  documentRevision: 1,
  result: { resultStatus: "needs_review" },
  map: {
    geometryHash: "synthetic-map-hash",
    feature: { geometry: { type: "Polygon", coordinates: [] } }
  },
  kml: { geometryHash: "synthetic-kml-hash" }
});
assert.equal(reviewSpatialPayload.mapPreviewObject.previewEligibility.allowed, false);
assert.equal(reviewSpatialPayload.kmlEligibility.allowed, false);
assert.equal(reviewSpatialPayload.kmlEligibility.kmlReady, false);

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
    V7_SYNTHETIC_X_ORIGIN: String(syntheticXOrigin),
    V7_SYNTHETIC_Y_ORIGIN: String(syntheticYOrigin),
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
  form.set("visitorId", "final-fail-close-v7-regression");
  form.set("image", new Blob([syntheticLongImage], { type: "image/png" }), `${randomUUID()}.png`);
  const enqueueResponse = await fetch(`http://127.0.0.1:${port}/api/recognize-coordinates/jobs`, {
    method: "POST",
    headers: {
      "x-regression-test": "1",
      "x-visitor-id": "final-fail-close-v7-regression",
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
  assert.equal(result.requestId, requestId);
  assert.equal(result.providerCompletionState, "SUCCEEDED");
  assert.equal(result.providerCallCount, 1);
  assert.equal(result.acquisitionStatus, "COMPLETED");
  assert.equal(result.authorizationStatus, "AUTHORIZED");
  assert.equal(result.resultStatus, "authorized");
  assert.equal(result.requiresReview, false);
  assert.equal(result.boundaryBlocked, false);
  assert.equal(result.mapReady, true);
  assert.equal(result.kmlReady, true);
  assert.equal(result.mapStatus, "ENABLED");
  assert.equal(result.kmlStatus, "ENABLED");
  assert.equal(result.previewEligibility?.allowed, true);
  assert.equal(result.kmlEligibility?.allowed, true);
  assert.equal(result.usageConsumed, false);
  assert.equal(result.userUsageConsumed, false);
  assert.equal(result.recoveryRequired, false);
  assert.equal(String(result.coordinates || "").split(/\r?\n/u).filter(Boolean).length, 20);
  assert.equal(result.candidatePointCount, 20);
  assert.equal(result.candidateGroupCount, 1);
  assert.ok(result.visibleCrsEvidence.some(item => /BFTM/iu.test(item.text)));
  assert.ok(result.imageAcquisitionEvidence.imageCount > 1);
  assert.ok(result.imageAcquisitionEvidence.detailTileCount > 0);
  assert.equal(result.contractReasons.includes("COORDINATE_FORMAT_REQUIRES_VALIDATION"), false);
  assert.equal(result.finalizedCoordinateResult?.decisionState, "AUTO_EXPORT");
  assert.equal(result.finalizedCoordinateResult?.requiresReview, false);
  assert.notEqual(result.finalizedCoordinateResult?.mapReady, false);
  assert.equal(result.finalizedCoordinateResult?.kmlReady, true);

  const statsPromise = once(child, "message", { signal });
  child.send("stats");
  const [stats] = await statsPromise;
  assert.equal(stats.providerCalls, 1);
  assert.equal((stdout.match(/Recognition acquisition evidence:/gu) || []).length, 1);
  assert.equal((stdout.match(/Recognition acquisition final state:/gu) || []).length, 1);
  assert.equal(stdout.includes(String(syntheticXOrigin)), false);
  assert.doesNotMatch(stderr, /(?:Error|ERR_|Unhandled|AssertionError)/u);
  console.log("recognition final fail-close v7: PASS");
} finally {
  const ended = once(child, "exit");
  child.kill();
  await ended;
}
