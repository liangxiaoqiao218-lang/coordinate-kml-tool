import assert from "node:assert/strict";
import crypto from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildRecognitionAcquisitionEvidence,
  buildRecognitionFirstPromptPrefix,
  createRecognitionImageVariants,
  planRecognitionImageVariants
} from "../server/recognition/recognition-first-acquisition.js";
import {
  RECOGNITION_ACQUISITION_JOB_STATUS,
  createRecognitionAcquisitionJobRuntime,
  getRecognitionAcquisitionJobHttpStatus
} from "../server/recognition/recognition-acquisition-job-runtime.js";
import { recognitionDeadlineMiddleware } from "../server/coordinate-finalizer/recognition-deadline.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const baselinePath = path.join(root, "regression-samples", "recognition-first-acquisition-v1", "blind-baseline.json");
const baseline = JSON.parse(await readFile(baselinePath, "utf8"));
const requiredCategories = new Set(["DMS", "WGS84_DECIMAL", "BFTM", "UTM", "GK", "MGRS", "LONG_TABLE", "MULTISITE"]);

assert.equal(baseline.providerCalls, 0);
assert.equal(baseline.productionRecognitionCalls, 0);
assert.deepEqual(new Set(baseline.records.map(record => record.category)), requiredCategories);
assert.equal(baseline.records.length, 8);

let sourceBytes = 0;
let submittedImageBytes = 0;
let submittedImageCount = 0;
let detailedRecords = 0;
let asyncRecommendedRecords = 0;
let acquisitionRenderMs = 0;

for (const record of baseline.records) {
  const fixturePath = path.resolve(path.dirname(baselinePath), record.fixture);
  const buffer = await readFile(fixturePath);
  assert.equal(crypto.createHash("sha256").update(buffer).digest("hex"), record.sha256, record.id);
  assert.ok(Number.isInteger(record.expectedPointCount) && record.expectedPointCount > 0, record.id);
  assert.ok(record.expectedContinuity && record.expectedCrsEvidence && record.expectedGeometry && record.expectedKmlState, record.id);
  const renderStartedAt = performance.now();
  const acquisition = await createRecognitionImageVariants({ buffer, bytes: buffer.length });
  acquisitionRenderMs += performance.now() - renderStartedAt;
  assert.equal(acquisition.images[0].role, "overview", record.id);
  assert.equal(acquisition.images.filter(image => image.role === "overview").length, 1, record.id);
  assert.ok(acquisition.images.length >= 1 && acquisition.images.length <= 6, record.id);
  assert.ok(acquisition.images.every(image => image.mimeType === "image/jpeg" && image.dataUrl.startsWith("data:image/jpeg;base64,")), record.id);
  if (acquisition.needsDetail) {
    detailedRecords += 1;
    assert.ok(acquisition.images.length >= 3, `${record.id}: detail coverage`);
  }
  if (acquisition.asyncRecommended) asyncRecommendedRecords += 1;
  sourceBytes += buffer.length;
  submittedImageBytes += acquisition.images.reduce((sum, image) => sum + image.bytes, 0);
  submittedImageCount += acquisition.images.length;
}

const tallPlan = planRecognitionImageVariants({ width: 1200, height: 5000, bytes: 2_000_000 });
assert.equal(tallPlan.regions[0].role, "overview");
assert.ok(tallPlan.regions.length >= 4);
assert.equal(tallPlan.asyncRecommended, true);
for (let index = 2; index < tallPlan.regions.length; index += 1) {
  assert.ok(tallPlan.regions[index].top > tallPlan.regions[index - 1].top);
  assert.ok(tallPlan.regions[index].top < tallPlan.regions[index - 1].top + tallPlan.regions[index - 1].height);
}

const prompt = buildRecognitionFirstPromptPrefix({ images: [{ role: "overview" }, { role: "detail" }] });
assert.match(prompt, /one source page/i);
assert.match(prompt, /overlapping high-resolution detail tiles/i);
assert.match(prompt, /Return all visible coordinate candidates/i);
assert.match(prompt, /downstream validation decides authority, Map, and KML/i);

const rawProviderText = [
  "CONTEXT | WGS 84 / UTM ZONE 50S",
  "No. | X | Y | Latitude | Longitude",
  "1 | 778984.492 | 9721476.737 | 2°31'2.794\"S | 119°30'31.553\"E",
  "2 | 779099.680 | 9721476.848 | 2°31'2.783\"S | 119°30'35.279\"E"
].join("\n");
const evidence = buildRecognitionAcquisitionEvidence({
  rawText: rawProviderText,
  acquisition: { width: 1600, height: 1132, bytes: 281000, images: [{ role: "overview" }, { role: "detail" }] },
  providerResponseId: "provider-test-id"
});
assert.equal(evidence.rawProviderText, rawProviderText);
assert.equal(evidence.candidateCoordinateLines.length, 2);
assert.ok(evidence.visibleCrsEvidence.some(item => /UTM ZONE 50S/i.test(item.text)));
assert.equal(evidence.authority, "EVIDENCE_ONLY");

let releaseFirstJob;
const firstJobGate = new Promise(resolve => { releaseFirstJob = resolve; });
const executionOrder = [];
const runtime = createRecognitionAcquisitionJobRuntime({
  execute: async input => {
    executionOrder.push(input.id);
    if (input.id === 1) await firstJobGate;
    return { httpStatus: 200, result: { id: input.id } };
  }
});
const firstJob = runtime.enqueue({ id: 1 });
const secondJob = runtime.enqueue({ id: 2 });
await new Promise(resolve => setImmediate(resolve));
assert.equal(runtime.get(firstJob.jobId, firstJob.jobAccessToken).status, RECOGNITION_ACQUISITION_JOB_STATUS.RUNNING);
assert.equal(runtime.get(secondJob.jobId, secondJob.jobAccessToken).status, RECOGNITION_ACQUISITION_JOB_STATUS.QUEUED);
assert.equal(runtime.get(firstJob.jobId), null);
assert.equal(runtime.get(firstJob.jobId, secondJob.jobAccessToken), null);
releaseFirstJob();
while (runtime.get(secondJob.jobId, secondJob.jobAccessToken).status !== RECOGNITION_ACQUISITION_JOB_STATUS.SUCCEEDED) {
  await new Promise(resolve => setImmediate(resolve));
}
assert.deepEqual(executionOrder, [1, 2]);
assert.deepEqual(runtime.get(secondJob.jobId, secondJob.jobAccessToken).result, { id: 2 });

const non2xxRuntime = createRecognitionAcquisitionJobRuntime({
  execute: async () => ({
    httpStatus: 422,
    result: {
      success: false,
      reason: "COORDINATE_RECOGNITION_FAILED_CLOSED",
      userUsageConsumed: false,
      rawText: "must-not-leak",
      coordinates: "must-not-leak"
    }
  })
});
const non2xxJob = non2xxRuntime.enqueue({});
let non2xxSnapshot;
while (non2xxSnapshot?.completedAt == null) {
  await new Promise(resolve => setImmediate(resolve));
  non2xxSnapshot = non2xxRuntime.get(non2xxJob.jobId, non2xxJob.jobAccessToken);
}
assert.equal(non2xxSnapshot.status, RECOGNITION_ACQUISITION_JOB_STATUS.FAILED);
assert.equal(non2xxSnapshot.httpStatus, 422);
assert.equal(getRecognitionAcquisitionJobHttpStatus(non2xxSnapshot), 422);
assert.deepEqual(non2xxSnapshot.result, {
  success: false,
  reason: "COORDINATE_RECOGNITION_FAILED_CLOSED",
  userUsageConsumed: false
});
assert.equal(non2xxSnapshot.error.code, "COORDINATE_RECOGNITION_FAILED_CLOSED");

const completedCapacityRuntime = createRecognitionAcquisitionJobRuntime({
  maxJobs: 1,
  execute: async input => ({ httpStatus: 200, result: { id: input.id } })
});
const completedCapacityFirst = completedCapacityRuntime.enqueue({ id: 1 });
while (completedCapacityRuntime.get(
  completedCapacityFirst.jobId,
  completedCapacityFirst.jobAccessToken
)?.completedAt == null) {
  await new Promise(resolve => setImmediate(resolve));
}
const completedCapacitySecond = completedCapacityRuntime.enqueue({ id: 2 });
assert.equal(completedCapacityRuntime.get(
  completedCapacityFirst.jobId,
  completedCapacityFirst.jobAccessToken
), null);
assert.equal(completedCapacityRuntime.get(
  completedCapacitySecond.jobId,
  completedCapacitySecond.jobAccessToken
).status, RECOGNITION_ACQUISITION_JOB_STATUS.QUEUED);

let releaseCapacityJob;
const capacityGate = new Promise(resolve => { releaseCapacityJob = resolve; });
const runningCapacityRuntime = createRecognitionAcquisitionJobRuntime({
  maxJobs: 1,
  execute: async () => {
    await capacityGate;
    return { httpStatus: 200, result: { success: true } };
  }
});
const runningCapacityJob = runningCapacityRuntime.enqueue({});
await new Promise(resolve => setImmediate(resolve));
assert.equal(runningCapacityRuntime.get(
  runningCapacityJob.jobId,
  runningCapacityJob.jobAccessToken
).status, RECOGNITION_ACQUISITION_JOB_STATUS.RUNNING);
assert.throws(
  () => runningCapacityRuntime.enqueue({}),
  error => error?.code === "RECOGNITION_JOB_CAPACITY_REACHED"
);
releaseCapacityJob();

assert.doesNotThrow(() => recognitionDeadlineMiddleware({
  profile: "async",
  deadlineMs: 180_000,
  preflightDeadlineMs: 30_000,
  executionDeadlineMs: 150_000
}));
assert.throws(() => recognitionDeadlineMiddleware({ deadlineMs: 60_000 }), /below_60000ms/);

const serverSource = await readFile(path.join(root, "server.js"), "utf8");
assert.match(serverSource, /createRecognitionImageVariants\(\{/);
assert.match(serverSource, /recognitionImageAcquisition\.images\.map/);
assert.match(serverSource, /ONE_SHOT_STRUCTURED_FAMILY\.GENERIC_REVIEW/);
assert.match(serverSource, /recognition_first_acquisition_deferred/);
assert.match(serverSource, /rawText:\s*wgs84PrimaryRawText/);
assert.match(serverSource, /rawText,\s*\n\s*coordinates:\s*buildRecognitionAcquisitionEvidence/);
assert.match(serverSource, /\/api\/recognize-coordinates\/jobs/);
assert.match(serverSource, /x-recognition-job-token/);
assert.match(serverSource, /getRecognitionAcquisitionJobHttpStatus\(job\)/);
assert.match(serverSource, /profile:\s*"async"/);
const availabilityBlock = serverSource.slice(serverSource.indexOf("if (enforcedAvailability)"), serverSource.indexOf("let mozambiqueTypeLock"));
assert.doesNotMatch(availabilityBlock, /return res\./);

const metrics = {
  fixtures: baseline.records.length,
  acquisitionRenderPass: baseline.records.length,
  acquisitionRenderAccuracy: 1,
  providerTranscriptionAccuracy: null,
  sourceBytes,
  submittedImageBytes,
  payloadExpansionRatio: Number((submittedImageBytes / sourceBytes).toFixed(3)),
  submittedImageCount,
  meanImagesPerRequest: Number((submittedImageCount / baseline.records.length).toFixed(3)),
  acquisitionRenderTotalMs: Number(acquisitionRenderMs.toFixed(1)),
  acquisitionRenderMeanMs: Number((acquisitionRenderMs / baseline.records.length).toFixed(1)),
  detailedRecords,
  asyncRecommendedRecords,
  providerCalls: 0
};

console.log(JSON.stringify({ status: "PASS", metrics }, null, 2));
