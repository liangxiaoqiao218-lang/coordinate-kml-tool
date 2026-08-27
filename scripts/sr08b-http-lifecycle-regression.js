import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";

const baseUrl = String(process.env.SR08B_BASE_URL || "http://127.0.0.1:32109").replace(/\/$/, "");
const visitorId = `sr08b-${Date.now()}`;

async function jsonRequest(path, options = {}) {
  const response = await fetch(`${baseUrl}${path}`, options);
  const payload = await response.json();
  return { response, payload };
}

const originalLines = [
  `1. 11°00'00.00"N, 08°00'00.00"W`,
  `2. 11°00'30.00"N, 08°00'00.00"W`,
  `3. 11°00'30.00"N, 08°00'30.00"W`,
  `4. 11°00'00.00"N, 08°00'30.00"W`
];
const editedLines = [...originalLines];
editedLines[3] = `4. 11°00'00.00"N, 08°00'45.00"W`;

const initial = await jsonRequest("/api/regression/parse-coordinate-text", {
  method: "POST",
  headers: { "content-type": "application/json", "x-regression-test": "true" },
  body: JSON.stringify({ text: originalLines.join("\n") })
});
assert.equal(initial.response.status, 200);
const revision1 = initial.payload.finalizedCoordinateResult;
assert.equal(revision1.decisionState, "AUTO_EXPORT");

const revised = await jsonRequest("/api/coordinate-revision", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({
    resultId: revision1.resultId,
    resultRevision: revision1.resultRevision,
    geometryHash: revision1.geometryHash,
    coordinateText: editedLines.join("\n")
  })
});
assert.equal(revised.response.status, 200);
const revision2 = revised.payload.finalizedCoordinateResult;
assert.equal(revision2.resultRevision, revision1.resultRevision + 1);
assert.notEqual(revision2.geometryHash, revision1.geometryHash);
assert.equal(revision2.decisionState, "AUTO_EXPORT", "manual server-side reparse does not require handwritten confirmation");

const stale = await jsonRequest("/api/coordinate-confirmation", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ resultId: revision1.resultId, resultRevision: revision1.resultRevision, geometryHash: revision1.geometryHash, action: "accept" })
});
assert.equal(stale.response.status, 409);
assert.equal(stale.payload.code, "STALE_CONFIRMATION_REVISION");

const mismatch = await jsonRequest("/api/coordinate-confirmation", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ resultId: revision2.resultId, resultRevision: revision2.resultRevision, geometryHash: "sha256:mismatch", action: "accept" })
});
assert.equal(mismatch.response.status, 409);
assert.equal(mismatch.payload.code, "GEOMETRY_HASH_MISMATCH");

const confirmed = await jsonRequest("/api/coordinate-confirmation", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ resultId: revision2.resultId, resultRevision: revision2.resultRevision, geometryHash: revision2.geometryHash, action: "accept" })
});
assert.equal(confirmed.response.status, 200);
assert.equal(confirmed.payload.finalizedCoordinateResult.decisionState, "AUTO_EXPORT");

const duplicate = await jsonRequest("/api/coordinate-confirmation", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ resultId: revision2.resultId, resultRevision: revision2.resultRevision, geometryHash: revision2.geometryHash, action: "accept" })
});
assert.equal(duplicate.payload.idempotent, true);

const scenarios = ["fast_success", "slow_provider", "provider_hang", "ocr_hang", "multiple_fallback"];
const deadlineEvidence = [];
for (const scenario of scenarios) {
  const form = new FormData();
  form.append("image", new Blob(["sr08b-fixture"], { type: "image/png" }), "fixture.png");
  form.append("visitorId", visitorId);
  const startedAt = performance.now();
  const outcome = await jsonRequest("/api/recognize-coordinates", {
    method: "POST",
    headers: {
      "x-regression-test": "true",
      "x-visitor-id": visitorId,
      "x-sr08b-deadline-scenario": scenario
    },
    body: form
  });
  const elapsedMs = Math.round(performance.now() - startedAt);
  assert.ok(elapsedMs < 60_000, `${scenario} must complete below 60 seconds`);
  if (["fast_success", "slow_provider"].includes(scenario)) {
    assert.equal(outcome.response.status, 200);
  } else {
    assert.equal(outcome.response.status, 504);
    assert.equal(outcome.payload.code, "RECOGNITION_DEADLINE_EXCEEDED");
    assert.equal(outcome.payload.providerCancellationState, "aborted");
  }
  deadlineEvidence.push({
    scenario,
    elapsedMs,
    httpStatus: outcome.response.status,
    responseCode: outcome.payload.code || "SUCCESS",
    terminationReason: outcome.payload.terminationReason || "completed",
    providerCancellationState: outcome.payload.providerCancellationState || "not_required"
  });
}

const version = await jsonRequest("/api/version");
assert.equal(version.payload.runtimeIdentity.finalizerSchemaVersion, "finalized_coordinate_result_v1");
assert.equal(version.payload.runtimeIdentity.spatialResultEnabled, false);
assert.ok(version.payload.runtimeIdentity.recognitionHardDeadlineMs < 60_000);

console.log(JSON.stringify({
  suite: "sr08b-http-lifecycle-regression",
  passed: 12,
  deadlineEvidence,
  runtimeIdentity: version.payload.runtimeIdentity
}, null, 2));
