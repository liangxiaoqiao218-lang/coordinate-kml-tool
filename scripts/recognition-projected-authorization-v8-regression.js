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
import {
  buildRecognitionAcquisitionEvidence,
  evaluateProjectedCoordinateAuthorizationEvidence,
  evaluateUnifiedRecognitionAcquisition,
  evaluateUnifiedRecognitionFinalAuthorization
} from "../server/recognition/recognition-first-acquisition.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const requestId = "88888888-8888-4888-8888-888888888888";
const syntheticXOrigin = Number(process.env.V8_SYNTHETIC_X_ORIGIN) || randomInt(420_000, 580_000);
const syntheticYOrigin = Number(process.env.V8_SYNTHETIC_Y_ORIGIN) || randomInt(900_000, 1_800_000);
const projectedBoundaryOffsets = [
  [0, 0], [500, 0], [1000, 0], [1500, 0],
  [1500, 500], [1500, 1000], [1500, 1500],
  [1000, 1500], [500, 1500], [0, 1500],
  [-500, 1500], [-500, 1000], [-500, 500], [-250, 250]
];

function buildProjectedProviderText({ includeCrs = true } = {}) {
  return [
    ...(includeCrs ? ["CONTEXT | Projected coordinate system UTM Zone 31N"] : []),
    "HEADING | Projected boundary",
    "Sommets | X (m) | Y (m)",
    ...projectedBoundaryOffsets.map(([xOffset, yOffset], index) => (
      `${index + 1} | ${syntheticXOrigin + xOffset} | ${syntheticYOrigin + yOffset}`
    ))
  ].join("\n");
}

const providerText = buildProjectedProviderText();
const imageAcquisition = {
  width: 900,
  height: 3600,
  bytes: 2_100_000,
  images: [
    { role: "overview" },
    { role: "detail" },
    { role: "detail" }
  ]
};

function buildFinalizedBody() {
  return {
    success: true,
    requiresReview: false,
    boundaryBlocked: false,
    mapReady: true,
    kmlReady: true,
    coordinateEngineV2: {
      source_crs: {
        id: "EPSG:32631",
        projection: "utm",
        zone: 31,
        hemisphere: "N",
        axisOrder: "easting_northing"
      }
    },
    finalizedCoordinateResult: {
      decisionState: "AUTO_EXPORT",
      qualityGateStatus: "passed",
      requiresReview: false,
      kmlReady: true,
      geometry: { type: "Polygon", coordinates: [[[2, 8], [3, 8], [3, 9], [2, 8]]] },
      crs: { id: "EPSG:4326" }
    }
  };
}

const completeEvidence = buildRecognitionAcquisitionEvidence({
  rawText: providerText,
  acquisition: imageAcquisition,
  providerResponseId: "offline-provider-v8"
});
assert.equal(completeEvidence.acquisitionStatus, "COMPLETED");
assert.equal(completeEvidence.normalizationStatus, "COMPLETED");
assert.equal(completeEvidence.candidateCoordinates.length, projectedBoundaryOffsets.length);
assert.equal(completeEvidence.candidateCoordinateGroups.length, 1);
assert.equal(completeEvidence.diagnostics.boundRowCount, projectedBoundaryOffsets.length);
assert.equal(completeEvidence.diagnostics.unboundRowCount, 0);
assert.equal(completeEvidence.candidateCoordinates.every(candidate => candidate.format === "PROJECTED_XY"), true);
assert.equal(completeEvidence.candidateCoordinates.every(candidate => candidate.axisOrder === "x_y"), true);

const projectedEvidenceAuthorization = evaluateProjectedCoordinateAuthorizationEvidence(completeEvidence);
assert.equal(projectedEvidenceAuthorization.applicable, true);
assert.equal(projectedEvidenceAuthorization.eligible, true);
assert.deepEqual(projectedEvidenceAuthorization.reasons, []);

const extendedGridEvidence = buildRecognitionAcquisitionEvidence({
  rawText: [
    "HEADING | Generic projected grid",
    "NC | XV | YV | DESCRIPTION | NUMBER",
    "1 | 412345.25 | 7654321.5 | Alpha | 280",
    "2 | 412845.25 | 7654321.5 | Beta | 281",
    "3 | 412845.25 | 7654821.5 | Gamma | 282"
  ].join("\n"),
  acquisition: imageAcquisition,
  providerResponseId: "offline-provider-v8-extended-grid"
});
assert.equal(extendedGridEvidence.candidateCoordinates.length, 3);
assert.equal(extendedGridEvidence.candidateCoordinateGroups.length, 1);
assert.equal(extendedGridEvidence.diagnostics.boundRowCount, 3);
assert.equal(extendedGridEvidence.diagnostics.unboundRowCount, 0);
assert.equal(extendedGridEvidence.diagnostics.rejectedRowCount, 0);
assert.equal(extendedGridEvidence.projectedCoordinateEvidence.rowCount, 3);
assert.equal(extendedGridEvidence.projectedCoordinateEvidence.status, "COMPLETE");
assert.equal(extendedGridEvidence.candidateCoordinates.every(candidate => candidate.axisOrder === "x_y"), true);
const extendedGridAuthorization = evaluateProjectedCoordinateAuthorizationEvidence(extendedGridEvidence);
assert.equal(extendedGridAuthorization.eligible, false);
assert.ok(extendedGridAuthorization.reasons.includes("PROJECTED_CRS_UNRESOLVED"));

const utmZonaEvidence = buildRecognitionAcquisitionEvidence({
  rawText: [
    "CONTEXT | UTM WGS 1984 ZONA 50S",
    "No | X | Y",
    "1 | 778000 | 9721000",
    "2 | 778100 | 9721100",
    "3 | 778200 | 9721200"
  ].join("\n"),
  acquisition: imageAcquisition,
  providerResponseId: "offline-provider-v8-utm-zona"
});
assert.equal(utmZonaEvidence.visibleCrsEvidence.length, 1);
assert.equal(utmZonaEvidence.visibleCrsEvidence[0].text, "UTM WGS 1984 ZONA 50S");
assert.deepEqual(utmZonaEvidence.projectedCoordinateEvidence.crsEvidence, {
  status: "EXPLICIT", projection: "utm", zone: 50, hemisphere: "S"
});
assert.equal(evaluateProjectedCoordinateAuthorizationEvidence(utmZonaEvidence).eligible, true);

for (const unsafeGridText of [
  "This prose mentions NC and XV and YV without a table",
  "NC | XV | YV | DESCRIPTION | NUMBER\n1 | 412345.25 | 7654321.5 | Alpha",
  "NC | XV | YV | DESCRIPTION | NUMBER\n1 | 412345.25 | 7654321.5 | Alpha | 280 | extra",
  "NC | XV | YV | DESCRIPTION | NUMBER\n1 | not-a-coordinate | 7654321.5 | Alpha | 280"
]) {
  const unsafeGridEvidence = buildRecognitionAcquisitionEvidence({
    rawText: unsafeGridText,
    acquisition: imageAcquisition,
    providerResponseId: "offline-provider-v8-unsafe-grid"
  });
  assert.equal(unsafeGridEvidence.candidateCoordinates.length, 0);
}

const projectedDecision = evaluateUnifiedRecognitionAcquisition({
  evidence: completeEvidence,
  contractStatus: "REVIEW_REQUIRED",
  contractReason: "GENERIC_REVIEW_ONLY"
});
assert.equal(projectedDecision.projectedAuthorizationEligible, true);
assert.equal(projectedDecision.authorizationStatus, "VALIDATION_PENDING");
assert.equal(projectedDecision.mayProceedToGeometryValidation, true);
assert.equal(projectedDecision.contractReasons.includes("COORDINATE_FORMAT_REQUIRES_VALIDATION"), false);

const finalAuthorization = evaluateUnifiedRecognitionFinalAuthorization({
  body: buildFinalizedBody(),
  evidence: completeEvidence,
  decision: projectedDecision,
  conformance: { status: "REVIEW_REQUIRED", reason: "GENERIC_REVIEW_ONLY" },
  providerCallCount: 1
});
assert.equal(finalAuthorization.authorized, true);
assert.equal(finalAuthorization.finalRequiresReview, false);
assert.equal(finalAuthorization.projectedFinalRequirementsFailed, false);

const missingCrsEvidence = buildRecognitionAcquisitionEvidence({
  rawText: buildProjectedProviderText({ includeCrs: false }),
  acquisition: imageAcquisition,
  providerResponseId: "offline-provider-v8-missing-crs"
});
const missingCrsDecision = evaluateUnifiedRecognitionAcquisition({
  evidence: missingCrsEvidence,
  contractStatus: "REVIEW_REQUIRED",
  contractReason: "GENERIC_REVIEW_ONLY"
});
assert.equal(missingCrsDecision.projectedAuthorizationEligible, false);
assert.equal(missingCrsDecision.authorizationStatus, "REVIEW_REQUIRED");
assert.ok(missingCrsDecision.contractReasons.includes("PROJECTED_CRS_UNRESOLVED"));
assert.equal(evaluateUnifiedRecognitionFinalAuthorization({
  body: buildFinalizedBody(),
  evidence: missingCrsEvidence,
  decision: missingCrsDecision,
  conformance: { status: "REVIEW_REQUIRED", reason: "GENERIC_REVIEW_ONLY" },
  providerCallCount: 1
}).authorized, false);

const unboundEvidence = {
  ...completeEvidence,
  diagnostics: {
    ...completeEvidence.diagnostics,
    boundRowCount: completeEvidence.candidateCoordinates.length - 1,
    unboundRowCount: 1
  }
};
assert.equal(evaluateProjectedCoordinateAuthorizationEvidence(unboundEvidence).eligible, false);
assert.ok(evaluateProjectedCoordinateAuthorizationEvidence(unboundEvidence).reasons.includes("PROJECTED_ROWS_NOT_FULLY_BOUND"));

const unresolvedAxisEvidence = {
  ...completeEvidence,
  candidateCoordinates: completeEvidence.candidateCoordinates.map(candidate => ({
    ...candidate,
    axisOrder: null
  }))
};
assert.equal(evaluateProjectedCoordinateAuthorizationEvidence(unresolvedAxisEvidence).eligible, false);
assert.ok(evaluateProjectedCoordinateAuthorizationEvidence(unresolvedAxisEvidence).reasons.includes("PROJECTED_AXIS_ORDER_UNRESOLVED"));

const incompleteImageEvidence = {
  ...completeEvidence,
  imageEvidence: { ...completeEvidence.imageEvidence, sourceBytes: 0 }
};
assert.equal(evaluateProjectedCoordinateAuthorizationEvidence(incompleteImageEvidence).eligible, false);
assert.ok(evaluateProjectedCoordinateAuthorizationEvidence(incompleteImageEvidence).reasons.includes("PROJECTED_IMAGE_EVIDENCE_INCOMPLETE"));

const multipleGroupEvidence = {
  ...completeEvidence,
  candidateCoordinateGroups: [
    completeEvidence.candidateCoordinateGroups[0],
    { ...completeEvidence.candidateCoordinateGroups[0], groupId: "candidate_group_2" }
  ],
  diagnostics: { ...completeEvidence.diagnostics, candidateGroupCount: 2 }
};
assert.equal(evaluateProjectedCoordinateAuthorizationEvidence(multipleGroupEvidence).eligible, false);
assert.ok(evaluateProjectedCoordinateAuthorizationEvidence(multipleGroupEvidence).reasons.includes("PROJECTED_GROUP_BOUNDARY_NOT_UNIQUE"));

const missingEvidenceAuthorization = evaluateUnifiedRecognitionFinalAuthorization({
  body: buildFinalizedBody(),
  evidence: null,
  decision: { acquisitionStatus: "COMPLETED", authorizationStatus: "VALIDATION_PENDING" },
  conformance: { status: "CONFORMANT" },
  providerCallCount: 1
});
assert.equal(missingEvidenceAuthorization.authorized, false);
assert.equal(missingEvidenceAuthorization.hasUnifiedEvidence, false);

const repeatedProviderAuthorization = evaluateUnifiedRecognitionFinalAuthorization({
  body: buildFinalizedBody(),
  evidence: completeEvidence,
  decision: projectedDecision,
  conformance: { status: "REVIEW_REQUIRED", reason: "GENERIC_REVIEW_ONLY" },
  providerCallCount: 2
});
assert.equal(repeatedProviderAuthorization.authorized, false);
assert.ok(repeatedProviderAuthorization.finalAuthorizationReasons.includes("PROJECTED_PROVIDER_CALL_COUNT_INVALID"));

const finalizerConflictAuthorization = evaluateUnifiedRecognitionFinalAuthorization({
  body: {
    ...buildFinalizedBody(),
    finalizedCoordinateResult: {
      ...buildFinalizedBody().finalizedCoordinateResult,
      qualityGateStatus: "review_required",
      requiresReview: true,
      kmlReady: false
    }
  },
  evidence: completeEvidence,
  decision: projectedDecision,
  conformance: { status: "REVIEW_REQUIRED", reason: "GENERIC_REVIEW_ONLY" },
  providerCallCount: 1
});
assert.equal(finalizerConflictAuthorization.authorized, false);
assert.ok(finalizerConflictAuthorization.finalAuthorizationReasons.includes("PROJECTED_FINALIZER_GEOMETRY_CRS_VALIDATION_FAILED"));

if (process.argv.includes("--server")) {
  const { default: http } = await import("node:http");
  const nativeFetch = globalThis.fetch;
  let providerCalls = 0;
  globalThis.fetch = async (url, init) => {
    if (String(url) !== "http://127.0.0.1:1/v1/chat/completions") return nativeFetch(url, init);
    providerCalls += 1;
    assert.equal(providerCalls, 1);
    await new Promise(resolve => setTimeout(resolve, 120));
    return new Response(JSON.stringify({
      id: "offline-provider-v8",
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
  sliceBetween(indexSource, "function hasCompleteUnifiedRecognitionEvidence", "function coordinateKmlVisualState"),
  sliceBetween(indexSource, "function coordinateKmlVisualState", "function syncKmlActionVisualState"),
  sliceBetween(indexSource, "function syncKmlActionVisualState", "function setKmlGenerationInProgress"),
  sliceBetween(indexSource, "function createAgenticSpatialPayload", "async function openAgenticSpatialResult"),
  sliceBetween(indexSource, "async function openAgenticSpatialResult", "async function openSpatialResult"),
  sliceBetween(indexSource, "async function downloadAgenticCoordinateKml", "async function downloadKml")
].join("\n");
let finalizeCalls = 0;
let linkClicks = 0;
const finalizedOutcome = {
  documentRevision: 1,
  result: { resultStatus: "authorized" },
  map: {
    geometryHash: "projected-authorized-map",
    feature: { geometry: { type: "Polygon", coordinates: [[[2, 8], [3, 8], [3, 9], [2, 8]]] } }
  },
  kml: { geometryHash: "projected-authorized-kml", content: "<kml/>" }
};
const browserContext = vm.createContext({
  activeRecognitionAcquisitionResult: null,
  activeFinalizedCoordinateResult: null,
  activeMapPreviewResponse: null,
  activeMapPreviewCacheKey: "",
  agenticCoordinateController: {
    enabled: true,
    finalize: async () => {
      finalizeCalls += 1;
      return finalizedOutcome;
    }
  },
  input: { value: "projected coordinates" },
  mapPreviewAction: {
    hidden: true,
    disabled: true,
    dataset: {},
    setAttribute() {},
    textContent: "",
    title: ""
  },
  coordinateKmlAction: { disabled: true, dataset: {}, setAttribute() {}, textContent: "" },
  spatialKmlAction: { disabled: true, dataset: {}, setAttribute() {}, textContent: "" },
  kmlGenerationInProgress: false,
  pages: { spatialResult: { dataset: {} } },
  showMessage() {},
  showPage() {},
  renderSpatialResult() {},
  setSpatialSheetExpanded() {},
  waitForSpatialLayout: async () => {},
  syncSpatialMapViewport: async () => {},
  refreshSpatialShareActions() {},
  window: { matchMedia: () => ({ matches: false }) },
  GeoKitSatelliteMap: { open: async () => ({ state: "READY" }) },
  Blob,
  URL: {
    createObjectURL: () => "blob:offline-v8",
    revokeObjectURL() {}
  },
  document: {
    createElement() {
      return {
        click() { linkClicks += 1; },
        remove() {}
      };
    },
    body: { append() {} }
  }
});
vm.runInContext(browserGuardSource, browserContext);
const authorizedPayload = {
  ...buildFinalizedBody(),
  recognitionAcquisition: completeEvidence,
  acquisitionStatus: "COMPLETED",
  authorizationStatus: "AUTHORIZED",
  resultStatus: "authorized",
  requiresReview: false,
  boundaryBlocked: false,
  mapStatus: "ENABLED",
  kmlStatus: "ENABLED",
  kmlReady: true,
  candidateCoordinates: completeEvidence.candidateCoordinates,
  candidateCoordinateGroups: completeEvidence.candidateCoordinateGroups,
  visibleCrsEvidence: completeEvidence.visibleCrsEvidence,
  imageAcquisitionEvidence: completeEvidence.imageEvidence,
  reviewReasons: [],
  contractReasons: []
};
browserContext.activeRecognitionAcquisitionResult = browserContext.createRecognitionAuthorizationState(authorizedPayload);
browserContext.activeFinalizedCoordinateResult = authorizedPayload.finalizedCoordinateResult;
browserContext.refreshMapPreviewAction();
assert.equal(browserContext.mapPreviewAction.disabled, false);
assert.equal(browserContext.coordinateKmlVisualState(), "enabled");
const spatialPayload = browserContext.createAgenticSpatialPayload(finalizedOutcome);
assert.equal(spatialPayload.mapPreviewObject.previewEligibility.allowed, true);
assert.equal(spatialPayload.kmlEligibility.allowed, true);
await browserContext.openAgenticSpatialResult();
await browserContext.downloadAgenticCoordinateKml();
assert.equal(finalizeCalls, 2);
assert.equal(linkClicks, 1);

browserContext.activeRecognitionAcquisitionResult = browserContext.createRecognitionAuthorizationState({
  ...authorizedPayload,
  authorizationStatus: "REVIEW_REQUIRED",
  resultStatus: "needs_review",
  requiresReview: true,
  boundaryBlocked: true,
  mapStatus: "CLOSED",
  kmlStatus: "CLOSED",
  kmlReady: false,
  reviewReasons: ["PROJECTED_CRS_UNRESOLVED"]
});
browserContext.refreshMapPreviewAction();
assert.equal(browserContext.mapPreviewAction.disabled, true);
assert.match(browserContext.mapPreviewAction.title, /投影坐标系|轴顺序/u);
await browserContext.openAgenticSpatialResult();
await browserContext.downloadAgenticCoordinateKml();
assert.equal(finalizeCalls, 2);
assert.equal(linkClicks, 1);

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
    V8_SYNTHETIC_X_ORIGIN: String(syntheticXOrigin),
    V8_SYNTHETIC_Y_ORIGIN: String(syntheticYOrigin),
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
    create: { width: 900, height: 3600, channels: 3, background: "white" }
  }).png().toBuffer();
  let jobCreateCount = 0;
  let recoverCallCount = 0;
  const form = new FormData();
  form.set("visitorId", "projected-authorization-v8-regression");
  form.set("image", new Blob([syntheticLongImage], { type: "image/png" }), `${randomUUID()}.png`);
  jobCreateCount += 1;
  const enqueueResponse = await fetch(`http://127.0.0.1:${port}/api/recognize-coordinates/jobs`, {
    method: "POST",
    headers: {
      "x-regression-test": "1",
      "x-visitor-id": "projected-authorization-v8-regression",
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
  let refreshSnapshot = null;
  for (let attempt = 0; attempt < 500; attempt += 1) {
    const response = await fetch(`http://127.0.0.1:${port}/api/recognize-coordinates/jobs/${enqueued.jobId}`, {
      headers: { "x-recognition-job-token": enqueued.jobAccessToken },
      signal
    });
    snapshot = await response.json();
    if (snapshot.status === "RUNNING" && !refreshSnapshot) {
      const refreshed = await fetch(`http://127.0.0.1:${port}/api/recognize-coordinates/jobs/${enqueued.jobId}`, {
        headers: { "x-recognition-job-token": enqueued.jobAccessToken },
        signal
      });
      refreshSnapshot = await refreshed.json();
      assert.equal(refreshSnapshot.jobId, enqueued.jobId);
      assert.equal(refreshSnapshot.requestId, requestId);
    }
    if (["SUCCEEDED", "FAILED"].includes(snapshot.status)) break;
    await new Promise(resolve => setTimeout(resolve, 10));
  }

  const result = snapshot?.result || {};
  const diagnostic = JSON.stringify({ snapshot: { ...snapshot, result }, stdout, stderr });
  assert.ok(refreshSnapshot, diagnostic);
  assert.equal(snapshot?.status, "SUCCEEDED", diagnostic);
  assert.equal(snapshot?.httpStatus, 200, diagnostic);
  assert.equal(snapshot?.jobId, enqueued.jobId);
  assert.equal(snapshot?.requestId, requestId);
  assert.equal(result.providerCompletionState, "SUCCEEDED");
  assert.equal(result.providerCallCount, 1);
  assert.equal(result.acquisitionStatus, "COMPLETED");
  assert.equal(result.authorizationStatus, "AUTHORIZED");
  assert.equal(result.resultStatus, "authorized");
  assert.equal(result.requiresReview, false);
  assert.equal(result.boundaryBlocked, false);
  assert.equal(result.mapStatus, "ENABLED");
  assert.equal(result.kmlStatus, "ENABLED");
  assert.equal(result.kmlReady, true);
  assert.equal(result.previewEligibility?.allowed, true);
  assert.equal(result.kmlEligibility?.allowed, true);
  assert.equal(result.candidatePointCount, projectedBoundaryOffsets.length);
  assert.equal(result.candidateGroupCount, 1);
  assert.equal(result.recognitionAcquisition?.diagnostics?.boundRowCount, projectedBoundaryOffsets.length);
  assert.equal(result.recognitionAcquisition?.diagnostics?.unboundRowCount, 0);
  assert.equal(result.contractReasons.includes("COORDINATE_FORMAT_REQUIRES_VALIDATION"), false);
  assert.equal(result.finalizedCoordinateResult?.decisionState, "AUTO_EXPORT");
  assert.equal(result.recoveryRequired, false);
  assert.equal(jobCreateCount, 1);
  assert.equal(recoverCallCount, 0);

  const statsPromise = once(child, "message", { signal });
  child.send("stats");
  const [stats] = await statsPromise;
  assert.equal(stats.providerCalls, 1);
  assert.equal((stdout.match(/Recognition acquisition evidence:/gu) || []).length, 1);
  assert.equal((stdout.match(/Recognition acquisition final state:/gu) || []).length, 1);
  assert.equal(stdout.includes(String(syntheticXOrigin)), false);
  assert.doesNotMatch(stderr, /(?:Error|ERR_|Unhandled|AssertionError)/u);

  assert.match(indexSource, /markPendingCoordinateRecognitionJobTerminalApplied\([\s\S]*?\{ applied: true \}/u);
  assert.match(indexSource, /pendingCoordinateRecognitionJob[\s\S]*?jobAccessToken/u);
  assert.doesNotMatch(indexSource, /if \(status === "SUCCEEDED" \|\| status === "FAILED"\) \{\s*clearPendingCoordinateRecognitionJob\(\)/u);
  console.log("recognition projected authorization v8: PASS");
} finally {
  const ended = once(child, "exit");
  child.kill();
  await ended;
}
