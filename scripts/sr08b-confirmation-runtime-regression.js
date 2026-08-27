import assert from "node:assert/strict";
import fs from "node:fs";
import {
  COORDINATE_CONFIRMATION_STATUS,
  COORDINATE_DECISION_STATE,
  COORDINATE_GATE_REASON,
  COORDINATE_QUALITY_GATE_STATUS,
  CoordinateConfirmationRuntime,
  FINALIZED_COORDINATE_CRS,
  finalizeCoordinateResult
} from "../server/coordinate-finalizer/index.js";

const clock = () => "2026-08-26T00:00:00.000Z";
function candidate(overrides = {}) {
  return {
    resultId: "confirmation-result",
    resultRevision: 1,
    currentRevision: 1,
    sourceAuthority: "legacy",
    coordinateType: "handwritten_dms_experimental",
    precisionMode: "handwritten-dms-coordinates",
    crs: FINALIZED_COORDINATE_CRS,
    geometry: { type: "Point", coordinates: [-8.67, 11.47] },
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.PENDING,
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.PASSED,
    requiresReview: false,
    kmlReady: true,
    groups: [{ groupId: "group_1", requiresReview: false, kmlReady: true }],
    ...overrides
  };
}

let now = 1_000;
const runtime = new CoordinateConfirmationRuntime({ ttlMs: 1_000, maxResults: 10, now: () => now });

const notRequired = finalizeCoordinateResult(candidate({ confirmationStatus: "not_required" }), { clock });
assert.equal(notRequired.decisionState, "AUTO_EXPORT", "C01 not_required -> AUTO_EXPORT");

const pending = finalizeCoordinateResult(candidate(), { clock });
runtime.register(pending);
assert.equal(pending.decisionState, "REVIEW_REQUIRED", "C02 pending -> REVIEW_REQUIRED");

const accepted = runtime.confirm({
  resultId: pending.resultId,
  resultRevision: pending.resultRevision,
  geometryHash: pending.geometryHash,
  action: "accept"
});
assert.equal(accepted.ok, true, "C03 current revision is accepted");
assert.equal(accepted.finalizedCoordinateResult.decisionState, "AUTO_EXPORT");
assert.equal(accepted.finalizedCoordinateResult.geometryHash, pending.geometryHash);

const stale = runtime.confirm({
  resultId: pending.resultId,
  resultRevision: 0,
  geometryHash: pending.geometryHash,
  action: "accept"
});
assert.equal(stale.code, COORDINATE_GATE_REASON.STALE_CONFIRMATION_REVISION, "C04 stale revision rejected");

const edited = finalizeCoordinateResult(candidate({
  resultRevision: 2,
  currentRevision: 2,
  geometry: { type: "Point", coordinates: [-8.68, 11.48] }
}), { clock });
runtime.register(edited);
assert.notEqual(edited.geometryHash, pending.geometryHash, "C05 edit changes geometry hash");
assert.equal(edited.decisionState, "REVIEW_REQUIRED");

const reconfirmed = runtime.confirm({
  resultId: edited.resultId,
  resultRevision: edited.resultRevision,
  geometryHash: edited.geometryHash,
  action: "accept"
});
assert.equal(reconfirmed.finalizedCoordinateResult.decisionState, "AUTO_EXPORT", "C06 edited geometry reconfirmed");

const qualityFailed = finalizeCoordinateResult(candidate({ resultId: "quality-failed-result", qualityGateStatus: "failed" }), { clock });
runtime.register(qualityFailed);
const qualityConfirmed = runtime.confirm({
  resultId: qualityFailed.resultId,
  resultRevision: qualityFailed.resultRevision,
  geometryHash: qualityFailed.geometryHash,
  action: "accept"
});
assert.equal(qualityConfirmed.finalizedCoordinateResult.decisionState, "BLOCKED", "C07 confirmation cannot override quality failure");

const invalidCrs = finalizeCoordinateResult(candidate({ resultId: "invalid-crs-result", crs: { id: "EPSG:3857", axisOrder: "longitude_latitude" } }), { clock });
runtime.register(invalidCrs);
const crsConfirmed = runtime.confirm({
  resultId: invalidCrs.resultId,
  resultRevision: invalidCrs.resultRevision,
  geometryHash: invalidCrs.geometryHash,
  action: "accept"
});
assert.equal(crsConfirmed.finalizedCoordinateResult.decisionState, "BLOCKED", "C08 confirmation cannot override CRS failure");

const mismatch = runtime.confirm({
  resultId: edited.resultId,
  resultRevision: edited.resultRevision,
  geometryHash: "sha256:not-current",
  action: "accept"
});
assert.equal(mismatch.code, COORDINATE_GATE_REASON.GEOMETRY_HASH_MISMATCH, "C09 hash mismatch rejected");

runtime.register(reconfirmed.finalizedCoordinateResult);
const duplicate = runtime.confirm({
  resultId: reconfirmed.finalizedCoordinateResult.resultId,
  resultRevision: reconfirmed.finalizedCoordinateResult.resultRevision,
  geometryHash: reconfirmed.finalizedCoordinateResult.geometryHash,
  action: "accept"
});
assert.equal(duplicate.ok, true, "C10 duplicate accepted request succeeds");
assert.equal(duplicate.idempotent, true, "C10 duplicate is idempotent");

now += 2_000;
const expired = runtime.confirm({
  resultId: duplicate.finalizedCoordinateResult.resultId,
  resultRevision: duplicate.finalizedCoordinateResult.resultRevision,
  geometryHash: duplicate.finalizedCoordinateResult.geometryHash,
  action: "accept"
});
assert.equal(expired.code, COORDINATE_GATE_REASON.CONFIRMATION_RESULT_EXPIRED, "expired runtime record fails closed");

const html = fs.readFileSync("index.html", "utf8");
assert.match(html, /fetch\("\/api\/coordinate-confirmation"/);
assert.match(html, /fetch\("\/api\/coordinate-revision"/);
assert.match(html, /finalizedCoordinateDirty/);
assert.match(html, /shouldBlockFinalizedCoordinateKml\(\)/);
assert.match(html, /activeFinalizedCoordinateResult\.kmlReady !== true/);
assert.match(html, /if \(activeFinalizedCoordinateResult\) finalizedCoordinateDirty = true/);

console.log(JSON.stringify({
  suite: "sr08b-confirmation-runtime-regression",
  passed: 13,
  cases: ["C01", "C02", "C03", "C04", "C05", "C06", "C07", "C08", "C09", "C10", "TTL", "UI_BINDING", "KML_GATE"]
}, null, 2));
