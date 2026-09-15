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
import {
  buildDmsGroupedPartialMultisiteRecoveryCandidate,
  evaluateDmsGroupedAcquisitionExpansion,
  evaluateDmsWeakPartialMultisiteRecovery,
  parseDmsSourceCoordinateRow
} from "../server/recognition/dms-source-structure.js";

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
const runtime = new CoordinateConfirmationRuntime({ ttlMs: 1_000, maxResults: 20, now: () => now });

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

const recoveryIdentity = {
  candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
  sourceCandidateSeparate: true,
  directCanonicalPromotion: false,
  partialMultisiteRecoveryProvenance: {
    schemaVersion: "dms_grouped_partial_multisite_recovery_v1",
    recoveryMode: "STAGE1_PARTIAL_MULTISITE_8_TO_16",
    recoverySource: "dms_grouped"
  },
  sourceCandidates: {
    stage1: { rawText: "stage1", coordinates: "stage1-normalized" },
    structuredReread: { rawText: "reread", coordinates: "reread-normalized" }
  }
};
const recoveryPending = finalizeCoordinateResult(candidate({
  resultId: "partial-recovery-lifecycle",
  ...recoveryIdentity
}), { clock });
runtime.register(recoveryPending);
const recoveryConfirmed = runtime.confirm({
  resultId: recoveryPending.resultId,
  resultRevision: recoveryPending.resultRevision,
  geometryHash: recoveryPending.geometryHash,
  action: "accept"
}).finalizedCoordinateResult;
assert.equal(recoveryConfirmed.candidateRole, "NONAUTHORITATIVE_REVIEW_CANDIDATE", "C08A recovery role survives confirmation");
assert.equal(recoveryConfirmed.sourceCandidateSeparate, true);
assert.equal(recoveryConfirmed.directCanonicalPromotion, false);
assert.equal(recoveryConfirmed.partialMultisiteRecoveryProvenance.recoveryMode, "STAGE1_PARTIAL_MULTISITE_8_TO_16");
assert.equal(recoveryConfirmed.sourceCandidates.stage1.rawText, "stage1");
assert.equal(recoveryConfirmed.sourceCandidates.structuredReread.rawText, "reread");

const weakRows = Array.from({ length: 16 }, (_, index) => {
  const label = index < 8 ? index + 1 : index < 12 ? index - 7 : index - 11;
  return `${label}. 10°00'${String(index + 1).padStart(2, "0")}.0"N, 20°00'${String(index + 1).padStart(2, "0")}.0"E`;
});
const weakBaselineRows = [...weakRows.slice(0, 4), ...weakRows.slice(8, 10), ...weakRows.slice(12, 15)];
const weakStage1RawText = [
  "SITES1", "POINT | LATITUDE | LONGITUDE", ...weakRows.slice(0, 4), `5. 10°00'30.0"N, 20°00'30.0"N`, "",
  "SITES2", "POINT | LATITUDE | LONGITUDE", ...weakRows.slice(8, 10), "",
  "SITES3", "POINT | LATITUDE | LONGITUDE", ...weakRows.slice(12, 15)
].join("\n");
const weakRetryRawText = [
  "SITES1", "POINT | LATITUDE | LONGITUDE", ...weakRows.slice(0, 8), "",
  "SITES2", "POINT | LATITUDE | LONGITUDE", ...weakRows.slice(8, 12), "",
  "SITES3", "POINT | LATITUDE | LONGITUDE", ...weakRows.slice(12)
].join("\n");
const weakImageBuffer = Buffer.from("synthetic-weak-partial-image-v1");
const weakEvidence = evaluateDmsWeakPartialMultisiteRecovery({
  isImageInput: true,
  candidateSignal: true,
  structureText: weakStage1RawText,
  imageInputBuffer: weakImageBuffer
});
assert.equal(weakEvidence.accepted, true);
const weakExpansion = evaluateDmsGroupedAcquisitionExpansion({
  baselineText: weakStage1RawText,
  retryText: weakRetryRawText,
  allowWeakPartialMultisiteRecovery: true,
  isImageInput: true,
  projectedTableSignal: false,
  explicitHandwrittenSignal: false,
  weakPartialCandidateSignal: true,
  weakPartialInputEvidence: weakEvidence.inputEvidence,
  imageInputBuffer: weakImageBuffer
});
assert.equal(weakExpansion.accepted, true);
const weakStage1Coordinates = weakBaselineRows.map(row => {
  const point = parseDmsSourceCoordinateRow(row);
  return `${point.longitude},${point.latitude}`;
}).join("\n");
const weakBuilt = buildDmsGroupedPartialMultisiteRecoveryCandidate({
  stage1RawText: weakStage1RawText,
  stage1Coordinates: weakStage1Coordinates,
  retryRawText: weakRetryRawText,
  retryCoordinates: weakExpansion.normalizedCoordinates,
  expansion: weakExpansion,
  ownerFamily: "dms_grouped",
  imageInputBuffer: weakImageBuffer,
  allowUnsanitizedWeakPartialStage1: true
});
assert.equal(weakBuilt.accepted, true);
const weakLifecycleRebuilt = buildDmsGroupedPartialMultisiteRecoveryCandidate({
  stage1RawText: weakBuilt.stage1Candidate.rawText,
  stage1Coordinates: weakBuilt.stage1Candidate.coordinates,
  retryRawText: weakBuilt.rawText,
  retryCoordinates: weakBuilt.normalizedCoordinates,
  expansion: {
    accepted: true,
    recoveryMode: weakBuilt.provenance.recoveryMode,
    baselineRowCount: weakBuilt.provenance.baselineRowCount,
    retryRowCount: weakBuilt.provenance.retryRowCount,
    addedRowCount: weakBuilt.provenance.addedRowCount,
    baselineRowsPreserved: weakBuilt.provenance.baselineRowsPreserved,
    groupLocalBaselineRowsPreserved: weakBuilt.provenance.groupLocalBaselineRowsPreserved,
    baselineGroupCount: weakBuilt.provenance.baselineGroupCount,
    baselineGroupSizes: weakBuilt.provenance.baselineGroupSizes,
    baselineGroupIdentities: weakBuilt.provenance.baselineGroupIdentities,
    groupCount: weakBuilt.provenance.retryGroupCount,
    groupSizes: weakBuilt.provenance.retryGroupSizes,
    groupIdentities: weakBuilt.provenance.retryGroupIdentities,
    weakPartialQualification: weakBuilt.provenance.weakPartialQualification,
    stage1RejectedEvidence: weakBuilt.provenance.stage1RejectedEvidence,
    weakPartialInputEvidence: weakBuilt.weakPartialInputEvidence
  },
  ownerFamily: weakBuilt.provenance.ownerFamily,
  weakPartialInputEvidence: weakBuilt.weakPartialInputEvidence
});
assert.equal(weakLifecycleRebuilt.accepted, true, weakLifecycleRebuilt.reason);
const weakGeometryGroups = [weakRows.slice(0, 8), weakRows.slice(8, 12), weakRows.slice(12)].map(groupRows => {
  const positions = groupRows.map(row => {
    const point = parseDmsSourceCoordinateRow(row);
    return [point.longitude, point.latitude];
  });
  return [[...positions, [...positions[0]]]];
});
const weakCanonicalGeometry = { type: "MultiPolygon", coordinates: weakGeometryGroups };
const weakFinalizerGroups = [1, 2, 3]
  .map(index => ({ groupId: `group_${index}`, requiresReview: false, kmlReady: true }));
const weakRecoveryIdentity = {
  ...recoveryIdentity,
  partialMultisiteRecoveryProvenance: weakBuilt.provenance,
  partialMultisiteRecoveryInputEvidence: weakBuilt.weakPartialInputEvidence,
  sourceCandidates: {
    stage1: {
      ...weakBuilt.stage1Candidate,
      rowCount: weakBuilt.provenance.baselineRowCount,
      candidateRole: "STAGE1_ACQUISITION_CANDIDATE",
      candidateSha256: weakBuilt.provenance.stage1CandidateSha256
    },
    structuredReread: {
      rawText: weakBuilt.rawText,
      coordinates: weakBuilt.normalizedCoordinates,
      rowCount: weakBuilt.provenance.retryRowCount,
      candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
      candidateSha256: weakBuilt.provenance.retryCandidateSha256
    }
  }
};
const weakRecoveryPending = finalizeCoordinateResult(candidate({
  resultId: "weak-partial-recovery-lifecycle",
  ...weakRecoveryIdentity,
  geometry: weakCanonicalGeometry,
  groups: weakFinalizerGroups
}), { clock });
assert.equal(weakRecoveryPending.decisionState, "REVIEW_REQUIRED", "C08C valid weak recovery remains review-only");
assert.notEqual(weakRecoveryPending.geometry, null, "C08C valid reread geometry remains available only for review");
runtime.register(weakRecoveryPending);
const weakRecoveryConfirmation = runtime.confirm({
  resultId: weakRecoveryPending.resultId,
  resultRevision: weakRecoveryPending.resultRevision,
  geometryHash: weakRecoveryPending.geometryHash,
  action: "accept"
});
assert.equal(weakRecoveryConfirmation.ok, true, "C08C valid weak recovery can enter the existing confirmation flow");
const weakRecoveryConfirmed = weakRecoveryConfirmation.finalizedCoordinateResult;
assert.equal(weakRecoveryConfirmed.candidateRole, "NONAUTHORITATIVE_REVIEW_CANDIDATE", "C08C weak recovery role survives confirmation");
assert.equal(weakRecoveryConfirmed.sourceCandidateSeparate, true);
assert.equal(weakRecoveryConfirmed.directCanonicalPromotion, false);
assert.equal(weakRecoveryConfirmed.partialMultisiteRecoveryProvenance.recoveryMode, "STAGE1_WEAK_PARTIAL_MULTISITE_TO_16");
assert.equal(weakRecoveryConfirmed.partialMultisiteRecoveryProvenance.stage1RejectedEvidence.rejectedLineCount, 1);
assert.deepEqual(weakRecoveryConfirmed.partialMultisiteRecoveryInputEvidence, weakBuilt.weakPartialInputEvidence);
assert.equal(weakRecoveryConfirmed.sourceCandidates.stage1.rawText, weakBuilt.stage1Candidate.rawText);
assert.equal(weakRecoveryConfirmed.sourceCandidates.structuredReread.rawText, weakRetryRawText);

for (const [invalidIndex, invalidWeakIdentity] of [
  {
    ...weakRecoveryIdentity,
    partialMultisiteRecoveryInputEvidence: null
  },
  {
    ...weakRecoveryIdentity,
    partialMultisiteRecoveryInputEvidence: "IMAGE"
  },
  {
    ...weakRecoveryIdentity,
    partialMultisiteRecoveryInputEvidence: {
      ...weakBuilt.weakPartialInputEvidence,
      projectedTableSignal: true
    }
  },
  {
    ...weakRecoveryIdentity,
    partialMultisiteRecoveryInputEvidence: {
      ...weakBuilt.weakPartialInputEvidence,
      runtimeAttestationId: "f".repeat(48)
    }
  },
  {
    ...weakRecoveryIdentity,
    sourceCandidates: {
      ...weakRecoveryIdentity.sourceCandidates,
      structuredReread: {
        ...weakRecoveryIdentity.sourceCandidates.structuredReread,
        rawText: "NOT_16_ROWS",
        coordinates: "UNBOUND"
      }
    }
  },
  {
    ...weakRecoveryIdentity,
    sourceCandidates: {
      ...weakRecoveryIdentity.sourceCandidates,
      structuredReread: {
        ...weakRecoveryIdentity.sourceCandidates.structuredReread,
        candidateSha256: "0".repeat(64)
      }
    }
  }
].entries()) {
  const invalidPending = finalizeCoordinateResult(candidate({
    resultId: `weak-partial-invalid-${invalidIndex + 1}`,
    ...invalidWeakIdentity,
    geometry: weakCanonicalGeometry,
    groups: weakFinalizerGroups
  }), { clock });
  assert.equal(invalidPending.decisionState, "BLOCKED", "C08D invalid weak recovery binding fails closed");
  assert.equal(invalidPending.geometry, null);
  assert.equal(invalidPending.kmlReady, false);
  assert.equal(invalidPending.partialMultisiteRecoveryProvenance, null);
  runtime.register(invalidPending);
  const invalidConfirmation = runtime.confirm({
    resultId: invalidPending.resultId,
    resultRevision: invalidPending.resultRevision,
    geometryHash: invalidPending.geometryHash,
    action: "accept"
  });
  assert.notEqual(invalidConfirmation.ok, true, "C08D confirmation cannot restore invalid weak recovery");
}
const forgedWeakGeometry = finalizeCoordinateResult(candidate({
  resultId: "weak-partial-invalid-geometry",
  ...weakRecoveryIdentity,
  geometry: { type: "Point", coordinates: [20, 10] },
  groups: weakFinalizerGroups
}), { clock });
assert.equal(forgedWeakGeometry.decisionState, "BLOCKED", "C08D reread cannot authorize unrelated geometry");
assert.equal(forgedWeakGeometry.geometry, null);
assert.equal(forgedWeakGeometry.kmlReady, false);

const weakRecoveryQualityBlocked = finalizeCoordinateResult(candidate({
  resultId: "weak-partial-recovery-quality-blocked",
  ...weakRecoveryIdentity,
  geometry: weakCanonicalGeometry,
  groups: weakFinalizerGroups,
  qualityGateStatus: "failed"
}), { clock });
runtime.register(weakRecoveryQualityBlocked);
const weakBlockedConfirmation = runtime.confirm({
  resultId: weakRecoveryQualityBlocked.resultId,
  resultRevision: weakRecoveryQualityBlocked.resultRevision,
  geometryHash: weakRecoveryQualityBlocked.geometryHash,
  action: "accept"
});
assert.equal(weakBlockedConfirmation.finalizedCoordinateResult.decisionState, "BLOCKED");
assert.equal(weakBlockedConfirmation.finalizedCoordinateResult.kmlReady, false);
assert.equal(weakBlockedConfirmation.finalizedCoordinateResult.directCanonicalPromotion, false);

const independentBlockers = [
  { resultId: "recovery-source-blocked", explicitAuthorityRejected: true },
  { resultId: "recovery-quality-blocked", qualityGateStatus: "failed" },
  { resultId: "recovery-crs-blocked", crs: { id: "EPSG:3857", axisOrder: "longitude_latitude" } },
  { resultId: "recovery-geometry-blocked", geometry: null },
  { resultId: "recovery-kml-blocked", kmlAuthorityBlocked: true },
  { resultId: "recovery-availability-blocked", availabilityStatus: "TEMPORARILY_UNAVAILABLE" }
];
for (const blocker of independentBlockers) {
  const blockedPending = finalizeCoordinateResult(candidate({ ...recoveryIdentity, ...blocker }), { clock });
  runtime.register(blockedPending);
  const blockedConfirmed = runtime.confirm({
    resultId: blockedPending.resultId,
    resultRevision: blockedPending.resultRevision,
    geometryHash: blockedPending.geometryHash,
    action: "accept"
  });
  if (blockedConfirmed.ok) {
    assert.equal(blockedConfirmed.finalizedCoordinateResult.decisionState, "BLOCKED", `C08B confirmation cannot override ${blocker.resultId}`);
    assert.equal(blockedConfirmed.finalizedCoordinateResult.kmlReady, false);
    assert.equal(blockedConfirmed.finalizedCoordinateResult.candidateRole, "NONAUTHORITATIVE_REVIEW_CANDIDATE");
    assert.equal(blockedConfirmed.finalizedCoordinateResult.directCanonicalPromotion, false);
  } else {
    assert.equal(blockedPending.decisionState, "BLOCKED", `C08B unconfirmable ${blocker.resultId} remains blocked`);
    assert.equal(blockedPending.kmlReady, false);
    assert.equal(blockedConfirmed.code, COORDINATE_GATE_REASON.GEOMETRY_HASH_MISMATCH);
  }
}

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
  passed: 17,
  cases: ["C01", "C02", "C03", "C04", "C05", "C06", "C07", "C08", "C08A", "C08B", "C08C", "C08D", "C09", "C10", "TTL", "UI_BINDING", "KML_GATE"]
}, null, 2));
