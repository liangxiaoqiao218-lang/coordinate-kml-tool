import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  RECOGNITION_ACQUISITION_JOB_STATUS,
  createRecognitionAcquisitionJobRuntime,
  getRecognitionAcquisitionJobHttpStatus
} from "../server/recognition/recognition-acquisition-job-runtime.js";
import { buildUnchargedCoordinateFailureResponse } from "../server/coordinate-usage-atomicity.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const requestId = "22222222-2222-4222-8222-222222222222";

async function waitForTerminal(runtime, job) {
  let snapshot;
  for (let attempt = 0; attempt < 100; attempt += 1) {
    snapshot = runtime.get(job.jobId, job.jobAccessToken);
    if ([RECOGNITION_ACQUISITION_JOB_STATUS.SUCCEEDED, RECOGNITION_ACQUISITION_JOB_STATUS.FAILED].includes(snapshot?.status)) {
      return snapshot;
    }
    await new Promise(resolve => setImmediate(resolve));
  }
  throw new Error("job_did_not_finish");
}

const reviewRuntime = createRecognitionAcquisitionJobRuntime({
  execute: async () => ({
    httpStatus: 200,
    result: {
      success: true,
      acquisitionStatus: "COMPLETED",
      authorizationStatus: "REVIEW_REQUIRED",
      resultStatus: "needs_review",
      mapStatus: "CLOSED",
      kmlStatus: "CLOSED",
      providerCallCount: 1,
      usageConsumed: true
    }
  })
});
const reviewJob = reviewRuntime.enqueue({ requestId });
const reviewSnapshot = await waitForTerminal(reviewRuntime, reviewJob);
assert.equal(reviewSnapshot.status, RECOGNITION_ACQUISITION_JOB_STATUS.SUCCEEDED);
assert.equal(reviewSnapshot.requestId, requestId);
assert.equal(reviewSnapshot.result.authorizationStatus, "REVIEW_REQUIRED");
assert.equal(reviewSnapshot.result.resultStatus, "needs_review");
assert.equal(reviewSnapshot.result.mapStatus, "CLOSED");
assert.equal(reviewSnapshot.result.kmlStatus, "CLOSED");
assert.equal(getRecognitionAcquisitionJobHttpStatus(reviewSnapshot), 200);

const timeoutFailure = buildUnchargedCoordinateFailureResponse({
  recognitionRequestId: requestId,
  body: {
    success: false,
    reason: "recognition_failed_closed",
    code: "COORDINATE_RECOGNITION_FAILED_CLOSED",
    providerCompletionState: "TIMED_OUT",
    providerCallCount: 1,
    rawText: "must-not-leak",
    coordinates: "must-not-leak"
  }
});
assert.equal(timeoutFailure.providerCompletionState, "TIMED_OUT");
assert.equal(timeoutFailure.providerCallCount, 1);
assert.equal(timeoutFailure.usageConsumed, false);
assert.equal(timeoutFailure.userUsageConsumed, false);
assert.equal(timeoutFailure.recoveryRequired, false);
assert.equal(timeoutFailure.rawText, "");
assert.equal(timeoutFailure.coordinates, "");

const failureRuntime = createRecognitionAcquisitionJobRuntime({
  execute: async () => ({ httpStatus: 422, result: timeoutFailure })
});
const failureJob = failureRuntime.enqueue({ requestId });
const failureSnapshot = await waitForTerminal(failureRuntime, failureJob);
assert.equal(failureSnapshot.status, RECOGNITION_ACQUISITION_JOB_STATUS.FAILED);
assert.equal(failureSnapshot.httpStatus, 422);
assert.equal(failureSnapshot.requestId, requestId);
assert.equal(failureSnapshot.result.providerCompletionState, "TIMED_OUT");
assert.equal(failureSnapshot.result.providerCallCount, 1);
assert.equal(failureSnapshot.result.usageConsumed, false);
assert.equal(failureSnapshot.result.recoveryRequired, false);
assert.equal(JSON.stringify(failureSnapshot).includes("must-not-leak"), false);

const indexSource = await readFile(path.join(root, "index.html"), "utf8");
const serverSource = await readFile(path.join(root, "server.js"), "utf8");
const deadlineSource = await readFile(path.join(root, "server/coordinate-finalizer/recognition-deadline.js"), "utf8");

const inlineScripts = [...indexSource.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/gi)]
  .filter(match => !/application\/ld\+json/i.test(match[0]));
for (const script of inlineScripts) {
  assert.doesNotThrow(() => new Function(script[1]));
}

const helperStart = indexSource.indexOf("const PENDING_COORDINATE_COMMIT_REQUEST_KEY");
const helperEnd = indexSource.indexOf("function createRecognitionRequestId", helperStart);
assert.ok(helperStart > 0 && helperEnd > helperStart);
const helperSource = indexSource.slice(helperStart, helperEnd);
const storageValues = new Map();
const sessionStorage = {
  getItem: key => storageValues.get(key) ?? null,
  setItem: (key, value) => storageValues.set(key, String(value)),
  removeItem: key => storageValues.delete(key)
};
const debugEvents = [];
let pollSnapshot = null;
let observedPollToken = "";
const makeResponse = ({ ok, status, payload }) => ({
  ok,
  status,
  headers: { get: () => "" },
  json: async () => payload
});
const clientHelpers = new Function(
  "sessionStorage",
  "fetch",
  "getSourceHeaders",
  "appendDebug",
  "loadImageForCompression",
  `${helperSource}\nreturn { getPendingCoordinateCommitRequestId, rememberPendingCoordinateCommitRequestId, clearPendingCoordinateCommitRequestId, getPendingCoordinateRecognitionJob, rememberPendingCoordinateRecognitionJob, clearPendingCoordinateRecognitionJob, markPendingCoordinateRecognitionJobTerminalApplied, createCoordinateRecognitionTerminalSnapshot, shouldRecoverCoordinateUsageOutcome, shouldUseAsyncCoordinateRecognition, pollCoordinateRecognitionJob };`
)(
  sessionStorage,
  async (_url, options) => {
    observedPollToken = options?.headers?.["x-recognition-job-token"] || "";
    return makeResponse({
      ok: pollSnapshot.status === "SUCCEEDED",
      status: pollSnapshot.httpStatus,
      payload: pollSnapshot
    });
  },
  headers => headers,
  message => debugEvents.push(message),
  async () => ({ naturalWidth: 1000, naturalHeight: 3000 })
);

const storedJob = {
  jobId: "33333333-3333-4333-8333-333333333333",
  jobAccessToken: "abcdefghijklmnopqrstuvwxyzABCDEFGH123456",
  requestId,
  createdAt: "2026-09-25T00:00:00.000Z"
};
assert.equal(clientHelpers.rememberPendingCoordinateRecognitionJob(storedJob), true);
assert.deepEqual(clientHelpers.getPendingCoordinateRecognitionJob(), storedJob);
assert.equal(storageValues.has("geokit_pending_coordinate_commit_request_id"), false);
assert.equal(await clientHelpers.shouldUseAsyncCoordinateRecognition({ size: 1_500_000 }), true);
assert.equal(await clientHelpers.shouldUseAsyncCoordinateRecognition({ size: 1000 }), true);

pollSnapshot = {
  success: false,
  async: true,
  jobId: storedJob.jobId,
  requestId,
  status: "FAILED",
  httpStatus: 422,
  result: timeoutFailure,
  error: { code: timeoutFailure.code, message: "Recognition acquisition job failed" }
};
const polledFailure = await clientHelpers.pollCoordinateRecognitionJob(storedJob);
assert.equal(observedPollToken, storedJob.jobAccessToken);
assert.equal(polledFailure.response.status, 422);
assert.equal(polledFailure.data.jobStatus, "FAILED");
assert.equal(polledFailure.data.providerCallCount, 1);
assert.equal(polledFailure.data.usageConsumed, false);
assert.deepEqual(clientHelpers.getPendingCoordinateRecognitionJob(), storedJob);
assert.equal(clientHelpers.markPendingCoordinateRecognitionJobTerminalApplied(storedJob, polledFailure.data, { applied: true }), true);
assert.equal(clientHelpers.getPendingCoordinateRecognitionJob(), null);
assert.ok(debugEvents.some(message => message.includes(storedJob.jobId) && message.includes(requestId)));

clientHelpers.rememberPendingCoordinateRecognitionJob(storedJob);
pollSnapshot = {
  success: true,
  async: true,
  jobId: storedJob.jobId,
  requestId,
  status: "SUCCEEDED",
  httpStatus: 200,
  result: reviewSnapshot.result,
  error: null
};
const refreshedJob = clientHelpers.getPendingCoordinateRecognitionJob();
const polledReview = await clientHelpers.pollCoordinateRecognitionJob(refreshedJob);
assert.equal(polledReview.data.jobStatus, "SUCCEEDED");
assert.equal(polledReview.data.authorizationStatus, "REVIEW_REQUIRED");
assert.equal(polledReview.data.mapStatus, "CLOSED");
assert.deepEqual(clientHelpers.getPendingCoordinateRecognitionJob(), storedJob);
assert.equal(clientHelpers.markPendingCoordinateRecognitionJobTerminalApplied(storedJob, polledReview.data), false);
assert.deepEqual(clientHelpers.getPendingCoordinateRecognitionJob(), storedJob);
assert.equal(clientHelpers.markPendingCoordinateRecognitionJobTerminalApplied(storedJob, polledReview.data, { applied: true }), true);
assert.equal(clientHelpers.getPendingCoordinateRecognitionJob(), null);

const usageUnknownTerminal = {
  ...polledReview.data,
  success: false,
  code: "USAGE_COMMIT_OUTCOME_UNKNOWN",
  usageConsumed: null,
  recoveryRequired: true
};
const terminalSnapshot = clientHelpers.createCoordinateRecognitionTerminalSnapshot(storedJob, usageUnknownTerminal);
assert.deepEqual(terminalSnapshot, {
  jobId: storedJob.jobId,
  requestId,
  jobStatus: "SUCCEEDED",
  httpStatus: 200,
  providerCompletionState: "",
  providerCallCount: 1,
  usageConsumed: null,
  recoveryRequired: true,
  code: "USAGE_COMMIT_OUTCOME_UNKNOWN"
});
assert.equal("jobAccessToken" in terminalSnapshot, false);
assert.equal(clientHelpers.shouldRecoverCoordinateUsageOutcome({
  result: usageUnknownTerminal,
  responseWasRecovery: false,
  recoveryOnly: false,
  terminalJobSnapshot: terminalSnapshot
}), true);
assert.equal(clientHelpers.shouldRecoverCoordinateUsageOutcome({
  result: usageUnknownTerminal,
  responseWasRecovery: false,
  recoveryOnly: true,
  terminalJobSnapshot: terminalSnapshot
}), true);
assert.equal(clientHelpers.shouldRecoverCoordinateUsageOutcome({
  result: polledReview.data,
  responseWasRecovery: false,
  recoveryOnly: true,
  terminalJobSnapshot: terminalSnapshot
}), false);
let ordinaryRefreshRecoverCallCount = 0;
if (clientHelpers.shouldRecoverCoordinateUsageOutcome({
  result: polledReview.data,
  responseWasRecovery: false,
  recoveryOnly: true,
  terminalJobSnapshot: terminalSnapshot
})) ordinaryRefreshRecoverCallCount += 1;
assert.equal(ordinaryRefreshRecoverCallCount, 0);
assert.equal(clientHelpers.shouldRecoverCoordinateUsageOutcome({
  result: usageUnknownTerminal,
  responseWasRecovery: true,
  recoveryOnly: true,
  terminalJobSnapshot: terminalSnapshot
}), false);

async function simulateTerminalUsageRecovery({ fromRefresh = false, recoveredResult, applyRecoveredResult = true }) {
  storageValues.clear();
  let jobCreateCount = 1;
  let providerCallCount = 1;
  let recoverCallCount = 0;
  let loading = true;
  clientHelpers.rememberPendingCoordinateRecognitionJob(storedJob);
  clientHelpers.rememberPendingCoordinateCommitRequestId(requestId);
  const snapshot = clientHelpers.createCoordinateRecognitionTerminalSnapshot(storedJob, usageUnknownTerminal);
  const shouldRecover = clientHelpers.shouldRecoverCoordinateUsageOutcome({
    result: usageUnknownTerminal,
    responseWasRecovery: false,
    recoveryOnly: fromRefresh,
    terminalJobSnapshot: snapshot
  });
  if (shouldRecover) recoverCallCount += 1;
  assert.equal(clientHelpers.markPendingCoordinateRecognitionJobTerminalApplied(
    snapshot,
    snapshot,
    { applied: false }
  ), false);
  if (recoveredResult.code === "USAGE_COMMIT_OUTCOME_UNKNOWN") {
    loading = false;
  } else if (recoveredResult.success === true && applyRecoveredResult) {
    clientHelpers.clearPendingCoordinateCommitRequestId();
    clientHelpers.markPendingCoordinateRecognitionJobTerminalApplied(snapshot, snapshot, { applied: true });
    loading = false;
  } else if (recoveredResult.recoveryRequired === false) {
    clientHelpers.clearPendingCoordinateCommitRequestId();
    clientHelpers.markPendingCoordinateRecognitionJobTerminalApplied(snapshot, snapshot, { applied: true });
    loading = false;
  }
  return {
    fromRefresh,
    jobCreateCount,
    providerCallCount,
    recoverCallCount,
    loading,
    pendingJob: clientHelpers.getPendingCoordinateRecognitionJob(),
    pendingCommitRequestId: clientHelpers.getPendingCoordinateCommitRequestId()
  };
}

const recoveredSuccess = { success: true, usageConsumed: true, requestId };
const recoveredBeforeApplication = await simulateTerminalUsageRecovery({
  fromRefresh: true,
  recoveredResult: recoveredSuccess,
  applyRecoveredResult: false
});
assert.deepEqual(recoveredBeforeApplication.pendingJob, storedJob);
assert.equal(recoveredBeforeApplication.pendingCommitRequestId, requestId);
for (const fromRefresh of [false, true]) {
  const recovered = await simulateTerminalUsageRecovery({ fromRefresh, recoveredResult: recoveredSuccess });
  assert.equal(recovered.jobCreateCount, 1);
  assert.equal(recovered.providerCallCount, 1);
  assert.equal(recovered.recoverCallCount, 1);
  assert.equal(recovered.loading, false);
  assert.equal(recovered.pendingJob, null);
  assert.equal(recovered.pendingCommitRequestId, "");
}

storageValues.clear();
clientHelpers.rememberPendingCoordinateRecognitionJob(storedJob);
clientHelpers.rememberPendingCoordinateCommitRequestId(requestId);
assert.equal(clientHelpers.markPendingCoordinateRecognitionJobTerminalApplied(
  terminalSnapshot,
  terminalSnapshot,
  { applied: false }
), false);
assert.deepEqual(clientHelpers.getPendingCoordinateRecognitionJob(), storedJob);
assert.equal(clientHelpers.getPendingCoordinateCommitRequestId(), requestId);

const recoveryStillUnknown = await simulateTerminalUsageRecovery({
  fromRefresh: true,
  recoveredResult: usageUnknownTerminal
});
assert.equal(recoveryStillUnknown.recoverCallCount, 1);
assert.equal(recoveryStillUnknown.jobCreateCount, 1);
assert.equal(recoveryStillUnknown.providerCallCount, 1);
assert.equal(recoveryStillUnknown.loading, false);
assert.deepEqual(recoveryStillUnknown.pendingJob, storedJob);
assert.equal(recoveryStillUnknown.pendingCommitRequestId, requestId);

const terminalNoCharge = await simulateTerminalUsageRecovery({
  fromRefresh: true,
  recoveredResult: {
    success: false,
    code: "COORDINATE_USAGE_RECOVERY_UNAVAILABLE",
    usageConsumed: false,
    recoveryTerminal: true,
    recoveryRequired: false
  }
});
assert.equal(terminalNoCharge.recoverCallCount, 1);
assert.equal(terminalNoCharge.loading, false);
assert.equal(terminalNoCharge.pendingJob, null);
assert.equal(terminalNoCharge.pendingCommitRequestId, "");

assert.match(indexSource, /PENDING_COORDINATE_RECOGNITION_JOB_KEY/);
assert.match(indexSource, /sessionStorage\.setItem\(PENDING_COORDINATE_RECOGNITION_JOB_KEY/);
assert.match(indexSource, /x-recognition-job-token/);
assert.match(indexSource, /pollCoordinateRecognitionJob\(pendingCoordinateRecognitionJob\)/);
assert.match(indexSource, /useAsyncJob \? "\/api\/recognize-coordinates\/jobs" : "\/api\/recognize-coordinates"/);
assert.match(indexSource, /Number\(file\.size\) >= 1_500_000/);
assert.match(indexSource, /aspectRatio >= 2\.4 \|\| \(width \* height\) >= 6_000_000/);
assert.match(indexSource, /if \(await shouldUseAsyncCoordinateRecognition\(selectedFile\)\) \{\s*await recognizeImage\(\);\s*return;/);
assert.match(
  indexSource,
  /await agenticCoordinateInitializationPromise;\s*if \(getPendingCoordinateRecognitionJob\(\)\) \{\s*await resumePendingCoordinateWorkOnPageShow\(\);\s*return;\s*\}\s*if \(getPendingCoordinateCommitRequestId\(\)\) \{\s*await resumePendingCoordinateWorkOnPageShow\(\);\s*return;\s*\}\s*if \(agenticCoordinateController\?\.enabled\)/,
  "refresh must independently prioritize async-job and usage-commit recovery before any alternate recognition controller"
);
assert.match(indexSource, /RECOGNITION_ASYNC_JOB_CLIENT_WAIT_EXCEEDED/);
assert.match(indexSource, /terminalRecognitionJobSnapshot = createCoordinateRecognitionTerminalSnapshot/);
assert.match(indexSource, /shouldRecoverCoordinateUsageOutcome\(\{[\s\S]*terminalJobSnapshot: terminalRecognitionJobSnapshot/);
assert.match(indexSource, /clearPendingCommitAfterResultApplied = true/);
assert.match(indexSource, /if \(clearPendingCommitAfterResultApplied\) clearPendingCoordinateCommitRequestId\(\);/);
assert.match(indexSource, /data\?\.usageConsumed === false\s*&& data\?\.recoveryRequired !== true\) clearPendingCoordinateCommitRequestId\(\);/);
assert.match(indexSource, /markPendingCoordinateRecognitionJobTerminalApplied\(\s*terminalRecognitionJobSnapshot,\s*terminalRecognitionJobSnapshot/);
assert.match(indexSource, /const asyncTerminalMustStop = Boolean\(terminalRecognitionJobSnapshot\)/);
assert.ok(
  indexSource.indexOf("if (asyncTerminalMustStop)") < indexSource.indexOf('showRecognitionProgress("正在解析坐标...", "loading")'),
  "terminal FAILED or uncharged jobs must stop before coordinate parsing"
);
assert.match(indexSource, /setDebugRunning\(false\)[\s\S]*imageInput\.disabled = false/);
assert.match(indexSource, /pendingCoordinateRecognitionJob\s*\?\s*""\s*:\s*getPendingCoordinateCommitRequestId\(\)/);
assert.doesNotMatch(indexSource, /recoverCommittedCoordinateResult\([^)]*jobId/);

assert.match(serverSource, /forwardHeaders\["x-recognition-request-id"\] = recognitionRequestId/);
assert.match(serverSource, /requestId: recognitionRequestId,[\s\S]*file:/);
assert.match(serverSource, /providerCompletionState: recognitionBudget\?\.providerCompletionState/);
assert.match(serverSource, /providerCallCount: recognitionBudget\?\.providerAttemptCount/);
assert.match(deadlineSource, /providerCompletionState: budget\.providerCompletionState/);
assert.match(deadlineSource, /recoveryRequired: false/);

console.log(JSON.stringify({
  status: "PASS",
  checks: {
    asyncReviewTerminal: reviewSnapshot.status,
    failedHttpStatus: failureSnapshot.httpStatus,
    providerCallCount: failureSnapshot.result.providerCallCount,
    usageConsumed: failureSnapshot.result.usageConsumed,
    refreshJobStateSeparated: true,
    refreshUsageCommitBeforeAgentic: true,
    terminalUsageUnknownRecovery: true,
    refreshedTerminalUsageUnknownRecovery: true,
    recoverCallCount: 1,
    jobCreateCount: 1,
    boundedClientPolling: true,
    productionCalls: 0
  }
}, null, 2));
