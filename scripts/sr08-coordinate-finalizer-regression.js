import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import {
  COORDINATE_CONFIRMATION_STATUS,
  COORDINATE_DECISION_STATE,
  COORDINATE_GATE_REASON,
  COORDINATE_QUALITY_GATE_STATUS,
  CoordinateConfirmationRuntime,
  FINALIZED_COORDINATE_CRS,
  acceptCoordinateRevision,
  composeAbortSignals,
  consumeFinalizedGeometry,
  createCoordinateRevision,
  createGeometryHash,
  createLegacyFinalizerInput,
  createSpatialExecutionBoundary,
  createSpatialResponseIdentity,
  createV3FinalizerInput,
  finalizeCoordinateResult,
  registerFinalizedCoordinateResult,
  getRecognitionDeadlineContext,
  getRecognitionHardDeadlineMs,
  incrementCoordinateRevision,
  recognitionDeadlineMiddleware,
  spatialResponseMatchesCurrent
} from "../server/coordinate-finalizer/index.js";
import { FinalizedResultSpatialGeometryAdapter } from "../server/spatial/adapters/finalized-result-adapter.js";
import {
  buildDmsGroupedPartialMultisiteRecoveryCandidate,
  evaluateDmsGroupedAcquisitionExpansion,
  evaluateDmsWeakPartialMultisiteRecovery,
  parseDmsSourceCoordinateRow
} from "../server/recognition/dms-source-structure.js";

const FIXED_TIME = "2026-08-26T00:00:00.000Z";
const clock = () => FIXED_TIME;
const pointGeometry = Object.freeze({ type: "Point", coordinates: Object.freeze([103.1, 16.5]) });

function candidate(overrides = {}) {
  return {
    resultId: "result-1",
    resultRevision: 1,
    currentRevision: 1,
    sourceAuthority: "legacy",
    coordinateType: "wgs84_decimal",
    precisionMode: "wgs84-table-coordinates",
    crs: FINALIZED_COORDINATE_CRS,
    geometry: pointGeometry,
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.NOT_REQUIRED,
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.PASSED,
    requiresReview: false,
    kmlReady: true,
    groups: [{ groupId: "group_1", requiresReview: false, kmlReady: true }],
    warnings: [],
    ...overrides
  };
}

const cases = [];
function test(id, name, fn) {
  cases.push({ id, name, fn });
}

test("F01", "valid result is AUTO_EXPORT", () => {
  const result = finalizeCoordinateResult(candidate(), { clock });
  assert.equal(result.schemaVersion, "finalized_coordinate_result_v1");
  assert.equal(result.decisionState, COORDINATE_DECISION_STATE.AUTO_EXPORT);
  assert.equal(result.reasonCodes.length, 0);
  assert.equal(result.crs.axisOrder, "longitude_latitude");
  assert.ok(result.geometryHash.startsWith("sha256:"));
  assert.equal("rawText" in result, false);
  assert.equal("coordinates" in result, false);
});

test("F02", "pending confirmation requires review", () => {
  const result = finalizeCoordinateResult(candidate({ confirmationStatus: "pending" }), { clock });
  assert.equal(result.decisionState, COORDINATE_DECISION_STATE.REVIEW_REQUIRED);
  assert.ok(result.reasonCodes.includes(COORDINATE_GATE_REASON.CONFIRMATION_REQUIRED));
});

test("F03", "quality failure blocks", () => {
  const result = finalizeCoordinateResult(candidate({ qualityGateStatus: "failed" }), { clock });
  assert.equal(result.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
  assert.ok(result.reasonCodes.includes(COORDINATE_GATE_REASON.QUALITY_GATE_FAILED));
});

test("F04", "requires review cannot auto export", () => {
  const result = finalizeCoordinateResult(candidate({ requiresReview: true }), { clock });
  assert.equal(result.decisionState, COORDINATE_DECISION_STATE.REVIEW_REQUIRED);
});

test("F05", "invalid CRS blocks", () => {
  const result = finalizeCoordinateResult(candidate({ crs: { id: "EPSG:3857", axisOrder: "longitude_latitude" } }), { clock });
  assert.ok(result.reasonCodes.includes(COORDINATE_GATE_REASON.CRS_NOT_FINALIZED));
});

test("F06", "invalid geometry blocks", () => {
  const result = finalizeCoordinateResult(candidate({ geometry: { type: "Point", coordinates: [200, 16.5] } }), { clock });
  assert.ok(result.reasonCodes.includes(COORDINATE_GATE_REASON.GEOMETRY_INVALID));
});

test("F07", "KML not ready blocks", () => {
  const result = finalizeCoordinateResult(candidate({ kmlReady: false }), { clock });
  assert.ok(result.reasonCodes.includes(COORDINATE_GATE_REASON.KML_NOT_READY));
});

test("F08", "unknown gate state fails closed", () => {
  const result = finalizeCoordinateResult(candidate({ qualityGateStatus: "unknown" }), { clock });
  assert.equal(result.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
  assert.ok(result.reasonCodes.includes(COORDINATE_GATE_REASON.QUALITY_GATE_UNKNOWN));
});

test("R01-R05", "confirmation and edit revision lifecycle", () => {
  const initial = createCoordinateRevision({ resultId: "revision-result", confirmationRequired: true });
  let finalized = finalizeCoordinateResult(candidate({
    resultId: initial.resultId,
    resultRevision: initial.resultRevision,
    currentRevision: initial.resultRevision,
    confirmationStatus: initial.confirmationStatus,
    confirmedRevision: initial.confirmedRevision
  }), { clock });
  assert.equal(finalized.decisionState, "REVIEW_REQUIRED");

  const confirmed = acceptCoordinateRevision(initial);
  finalized = finalizeCoordinateResult(candidate({
    resultId: confirmed.resultId,
    resultRevision: confirmed.resultRevision,
    currentRevision: confirmed.resultRevision,
    confirmationStatus: confirmed.confirmationStatus,
    confirmedRevision: confirmed.confirmedRevision
  }), { clock });
  assert.equal(finalized.decisionState, "AUTO_EXPORT");

  const edited = incrementCoordinateRevision(confirmed);
  assert.equal(edited.resultRevision, 2);
  assert.equal(edited.confirmedRevision, null);
  finalized = finalizeCoordinateResult(candidate({
    resultId: edited.resultId,
    resultRevision: edited.resultRevision,
    currentRevision: edited.resultRevision,
    confirmationStatus: edited.confirmationStatus,
    confirmedRevision: edited.confirmedRevision
  }), { clock });
  assert.equal(finalized.decisionState, "REVIEW_REQUIRED");

  const reconfirmed = acceptCoordinateRevision(edited);
  finalized = finalizeCoordinateResult(candidate({
    resultId: reconfirmed.resultId,
    resultRevision: reconfirmed.resultRevision,
    currentRevision: reconfirmed.resultRevision,
    confirmationStatus: reconfirmed.confirmationStatus,
    confirmedRevision: reconfirmed.confirmedRevision
  }), { clock });
  assert.equal(finalized.decisionState, "AUTO_EXPORT");
});

test("R06", "geometry hash is canonical and ignores presentation fields", () => {
  const first = createGeometryHash({ type: "Point", coordinates: [103.1, 16.5] });
  const second = createGeometryHash({ coordinates: [103.1, 16.5], type: "Point" });
  assert.equal(first, second);
});

test("I01", "legacy structured input produces geometry without raw text", () => {
  const input = createLegacyFinalizerInput({
    recognitionResult: { precisionMode: "wgs84-table-coordinates", rawText: "must not be consumed" },
    coordinateEngineV2: {
      coordinate_type: "wgs84_decimal",
      precision_mode: "wgs84-table-coordinates",
      requires_review: false,
      groups: [{ group_id: "group_1", geometry: "point", requires_review: false, kml_ready: true, points: [{ lon: 103.1, lat: 16.5 }] }]
    },
    verification: { status: "PASS", warnings: [] },
    revision: { resultId: "legacy-1", resultRevision: 1 }
  });
  const result = finalizeCoordinateResult(input, { clock });
  assert.deepEqual(result.geometry, pointGeometry);
  assert.equal(result.decisionState, "AUTO_EXPORT");
});

test("I01A", "13-to-16 dms_grouped acquisition delta requires exact confirmation before release", () => {
  const groupSizes = [8, 4, 4];
  const groupIdentities = ["SITE:1", "SITE:2", "SITE:3"];
  const acquisitionGroups = groupSizes.map((size, groupIndex) => ({
    group_id: `group_${groupIndex + 1}`,
    group_name: `SITES${groupIndex + 1}`,
    geometry: "polygon",
    requires_review: true,
    kml_ready: false,
    points: Array.from({ length: size }, (_, pointIndex) => {
      const angle = (Math.PI * 2 * pointIndex) / size;
      return { label: String(pointIndex + 1), lon: 103 + groupIndex + Math.cos(angle) * 0.1, lat: 16 + groupIndex + Math.sin(angle) * 0.1 };
    })
  }));
  const provenance = {
    schemaVersion: "dms_grouped_acquisition_delta_v1",
    ownerFamily: "dms_grouped",
    candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
    baselineRowCount: 13,
    retryRowCount: 16,
    addedRowCount: 3,
    baselineRowsPreserved: true,
    groupLocalBaselineRowsPreserved: true,
    baselineGroupCount: 3,
    baselineGroupSizes: [6, 4, 3],
    baselineGroupIdentities: groupIdentities,
    retryGroupCount: 3,
    retryGroupSizes: groupSizes,
    retryGroupIdentities: groupIdentities,
    strongBoundariesProven: true,
    labelsContinuous: true,
    sourceCandidateSeparate: true,
    directCanonicalPromotion: false,
    stage1CandidateSha256: "a".repeat(64),
    retryCandidateSha256: "b".repeat(64)
  };
  const input = createLegacyFinalizerInput({
    recognitionResult: { precisionMode: "dms-coordinates" },
    coordinateEngineV2: {
      coordinate_type: "standard_dms_table",
      precision_mode: "dms-coordinates",
      requires_review: true,
      acquisition_delta_provenance: provenance,
      groups: acquisitionGroups
    },
    verification: { status: "REVIEW", warnings: [] },
    revision: { resultId: "delta-1", resultRevision: 1 }
  });
  const pending = finalizeCoordinateResult(input, { clock });
  assert.equal(input.currentAuthorizedGeometryExportable, false);
  assert.equal(pending.decisionState, COORDINATE_DECISION_STATE.REVIEW_REQUIRED);
  assert.equal(pending.kmlReady, false);
  assert.equal(pending.familySafetyPolicy.acquisitionDeltaProvenance.stage1CandidateSha256, "a".repeat(64));
  assert.equal(consumeFinalizedGeometry(pending, geometry => geometry.type).consumed, false);

  const runtime = new CoordinateConfirmationRuntime({ now: () => 1_000 });
  runtime.register(pending);
  const confirmed = runtime.confirm({
    resultId: pending.resultId,
    resultRevision: pending.resultRevision,
    geometryHash: pending.geometryHash,
    action: "accept"
  }).finalizedCoordinateResult;
  assert.equal(confirmed.decisionState, COORDINATE_DECISION_STATE.AUTO_EXPORT);
  assert.equal(confirmed.kmlReady, true);
  assert.equal(consumeFinalizedGeometry(confirmed, geometry => geometry.type).consumed, true);

  const invalidInput = createLegacyFinalizerInput({
    recognitionResult: {},
    coordinateEngineV2: {
      coordinate_type: "standard_dms_table",
      precision_mode: "dms-coordinates",
      requires_review: true,
      acquisition_delta_provenance: { ...provenance, retryCandidateSha256: provenance.stage1CandidateSha256 },
      groups: acquisitionGroups
    },
    verification: { status: "REVIEW" },
    revision: { resultId: "delta-invalid", resultRevision: 1 }
  });
  const invalid = finalizeCoordinateResult(invalidInput, { clock });
  assert.equal(invalid.geometry, null);
  assert.equal(invalid.qualityGateStatus, COORDINATE_QUALITY_GATE_STATUS.FAILED);
  assert.equal(invalid.decisionState, COORDINATE_DECISION_STATE.BLOCKED);

  const malformedTopology = finalizeCoordinateResult(createLegacyFinalizerInput({
    recognitionResult: {},
    coordinateEngineV2: {
      coordinate_type: "standard_dms_table",
      precision_mode: "dms-coordinates",
      requires_review: true,
      acquisition_delta_provenance: provenance,
      groups: acquisitionGroups.slice(0, 2)
    },
    verification: { status: "REVIEW" },
    revision: { resultId: "delta-malformed-topology", resultRevision: 1 }
  }), { clock });
  assert.equal(malformedTopology.geometry, null);
  assert.equal(malformedTopology.qualityGateStatus, COORDINATE_QUALITY_GATE_STATUS.FAILED);
  assert.equal(malformedTopology.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
});

test("I01B", "8-to-16 recovery identity survives Finalizer and Confirmation and forged content fails closed", () => {
  const rows = Array.from({ length: 16 }, (_, index) => {
    const label = index < 8 ? index + 1 : index < 12 ? index - 7 : index - 11;
    return `${label}. 10°00'${String(index + 1).padStart(2, "0")}.0\"N, 20°00'${String(index + 1).padStart(2, "0")}.0\"E`;
  });
  const baselineText = rows.slice(0, 8).join("\n");
  const retryText = [
    "SITES1", "POINT | LATITUDE | LONGITUDE", ...rows.slice(0, 8), "",
    "SITES2", "POINT | LATITUDE | LONGITUDE", ...rows.slice(8, 12), "",
    "SITES3", "POINT | LATITUDE | LONGITUDE", ...rows.slice(12)
  ].join("\n");
  const expansion = evaluateDmsGroupedAcquisitionExpansion({
    baselineText,
    retryText,
    allowPartialMultisiteRecovery: true
  });
  const normalizedRows = expansion.normalizedCoordinates.split(/\r?\n/).filter(Boolean);
  const built = buildDmsGroupedPartialMultisiteRecoveryCandidate({
    stage1RawText: baselineText,
    stage1Coordinates: normalizedRows.slice(0, 8).join("\n"),
    retryRawText: retryText,
    retryCoordinates: expansion.normalizedCoordinates,
    expansion,
    ownerFamily: "dms_grouped"
  });
  assert.equal(built.accepted, true);
  const groups = [rows.slice(0, 8), rows.slice(8, 12), rows.slice(12)].map((groupRows, index) => ({
    group_id: `group_${index + 1}`,
    group_name: `SITES${index + 1}`,
    geometry: "polygon",
    requires_review: true,
    kml_ready: false,
    points: groupRows.map(row => {
      const point = parseDmsSourceCoordinateRow(row);
      return { label: point.label, lon: point.longitude, lat: point.latitude };
    })
  }));
  const recognitionResult = {
    stage1Candidate: built.stage1Candidate,
    candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
    sourceCandidateSeparate: true,
    directCanonicalPromotion: false,
    partialMultisiteRecoveryCandidate: {
      rawText: built.rawText,
      coordinates: built.normalizedCoordinates,
      candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
      provenance: built.provenance
    },
    partialMultisiteRecoveryProvenance: built.provenance,
    sourceCandidates: {
      stage1: {
        ...built.stage1Candidate,
        rowCount: built.provenance.baselineRowCount,
        candidateRole: "STAGE1_ACQUISITION_CANDIDATE",
        candidateSha256: built.provenance.stage1CandidateSha256
      },
      structuredReread: {
        rawText: built.rawText,
        coordinates: built.normalizedCoordinates,
        rowCount: built.provenance.retryRowCount,
        candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
        candidateSha256: built.provenance.retryCandidateSha256
      }
    }
  };
  const coordinateEngineV2 = {
    coordinate_type: "standard_dms_table",
    precision_mode: "dms-coordinates",
    requires_review: true,
    partial_multisite_recovery_provenance: built.provenance,
    groups
  };
  const input = createLegacyFinalizerInput({
    recognitionResult,
    coordinateEngineV2,
    verification: { status: "REVIEW", warnings: [] },
    revision: { resultId: "partial-recovery-1", resultRevision: 1 }
  });
  assert.equal(input.candidateRole, "NONAUTHORITATIVE_REVIEW_CANDIDATE");
  assert.equal(input.sourceCandidateSeparate, true);
  assert.equal(input.directCanonicalPromotion, false);
  const pending = finalizeCoordinateResult(input, { clock });
  assert.equal(pending.partialMultisiteRecoveryProvenance.recoverySource, "dms_grouped");
  assert.equal(pending.sourceCandidates.stage1.rawText, baselineText);
  assert.equal(pending.sourceCandidates.structuredReread.rawText, retryText);
  assert.equal(pending.sourceCandidates.stage1.candidateRole, "STAGE1_ACQUISITION_CANDIDATE");
  assert.equal(pending.sourceCandidates.structuredReread.candidateRole, "NONAUTHORITATIVE_REVIEW_CANDIDATE");
  assert.equal(pending.sourceCandidates.stage1.candidateSha256, built.provenance.stage1CandidateSha256);
  assert.equal(pending.sourceCandidates.structuredReread.candidateSha256, built.provenance.retryCandidateSha256);
  assert.equal(pending.candidateRole, "NONAUTHORITATIVE_REVIEW_CANDIDATE");
  assert.equal(pending.directCanonicalPromotion, false);

  const runtime = new CoordinateConfirmationRuntime({ now: () => 1_000 });
  runtime.register(pending);
  const confirmed = runtime.confirm({
    resultId: pending.resultId,
    resultRevision: pending.resultRevision,
    geometryHash: pending.geometryHash,
    action: "accept"
  }).finalizedCoordinateResult;
  assert.equal(confirmed.candidateRole, "NONAUTHORITATIVE_REVIEW_CANDIDATE");
  assert.equal(confirmed.sourceCandidateSeparate, true);
  assert.equal(confirmed.directCanonicalPromotion, false);
  assert.equal(confirmed.partialMultisiteRecoveryProvenance.recoveryMode, "STAGE1_PARTIAL_MULTISITE_8_TO_16");
  assert.equal(confirmed.sourceCandidates.stage1.rawText, baselineText);
  assert.equal(confirmed.sourceCandidates.structuredReread.rawText, retryText);
  assert.equal(confirmed.sourceCandidates.stage1.candidateSha256, built.provenance.stage1CandidateSha256);
  assert.equal(confirmed.sourceCandidates.structuredReread.candidateSha256, built.provenance.retryCandidateSha256);

  const forgedInput = createLegacyFinalizerInput({
    recognitionResult: {
      ...recognitionResult,
      partialMultisiteRecoveryCandidate: {
        ...recognitionResult.partialMultisiteRecoveryCandidate,
        coordinates: recognitionResult.partialMultisiteRecoveryCandidate.coordinates.replace(normalizedRows[15], "99,12")
      }
    },
    coordinateEngineV2,
    verification: { status: "REVIEW", warnings: [] },
    revision: { resultId: "partial-recovery-forged", resultRevision: 1 }
  });
  const forged = finalizeCoordinateResult(forgedInput, { clock });
  assert.equal(forged.geometry, null);
  assert.equal(forged.kmlAuthorityBlocked, true);
  assert.equal(forged.qualityGateStatus, COORDINATE_QUALITY_GATE_STATUS.FAILED);
  assert.equal(forged.decisionState, COORDINATE_DECISION_STATE.BLOCKED);

  for (const [name, tamperedRecognition, tamperedEngine] of [
    [
      "structured-boundary",
      recognitionResult,
      {
        ...coordinateEngineV2,
        partial_multisite_recovery_provenance: { ...built.provenance, strongBoundariesProven: false }
      }
    ],
    [
      "recognition-label",
      { ...recognitionResult, partialMultisiteRecoveryProvenance: { ...built.provenance, labelsContinuous: false } },
      coordinateEngineV2
    ],
    [
      "source-binding",
      {
        ...recognitionResult,
        sourceCandidates: {
          ...recognitionResult.sourceCandidates,
          structuredReread: {
            ...recognitionResult.sourceCandidates.structuredReread,
            candidateSha256: "forged-reread-binding"
          }
        }
      },
      coordinateEngineV2
    ]
  ]) {
    const tampered = finalizeCoordinateResult(createLegacyFinalizerInput({
      recognitionResult: tamperedRecognition,
      coordinateEngineV2: tamperedEngine,
      verification: { status: "REVIEW", warnings: [] },
      revision: { resultId: `partial-recovery-${name}`, resultRevision: 1 }
    }), { clock });
    assert.equal(tampered.geometry, null);
    assert.equal(tampered.kmlAuthorityBlocked, true);
    assert.equal(tampered.qualityGateStatus, COORDINATE_QUALITY_GATE_STATUS.FAILED);
    assert.equal(tampered.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
    assert.equal(tampered.partialMultisiteRecoveryProvenance, null);
    assert.equal(tampered.sourceCandidates, null);
  }

  const {
    partialMultisiteRecoveryCandidate: _droppedCandidate,
    partialMultisiteRecoveryProvenance: _droppedProvenance,
    ...identityWithoutProof
  } = recognitionResult;
  const identityLoss = finalizeCoordinateResult(createLegacyFinalizerInput({
    recognitionResult: identityWithoutProof,
    coordinateEngineV2: {
      ...coordinateEngineV2,
      partial_multisite_recovery_provenance: undefined,
      requires_review: false,
      groups: coordinateEngineV2.groups.map(group => ({
        ...group,
        requires_review: false,
        kml_ready: true
      }))
    },
    verification: { status: "PASS", warnings: [] },
    revision: { resultId: "partial-recovery-identity-loss", resultRevision: 1 }
  }), { clock });
  assert.equal(identityLoss.geometry, null);
  assert.equal(identityLoss.kmlAuthorityBlocked, true);
  assert.equal(identityLoss.qualityGateStatus, COORDINATE_QUALITY_GATE_STATUS.FAILED);
  assert.equal(identityLoss.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
  assert.equal(identityLoss.kmlReady, false);

  const driftedRows = finalizeCoordinateResult(createLegacyFinalizerInput({
    recognitionResult: {
      ...recognitionResult,
      sourceCandidates: {
        stage1: { ...recognitionResult.sourceCandidates.stage1, rowCount: 999 },
        structuredReread: { ...recognitionResult.sourceCandidates.structuredReread, rowCount: 1 }
      }
    },
    coordinateEngineV2,
    verification: { status: "PASS", warnings: [] },
    revision: { resultId: "partial-recovery-row-drift", resultRevision: 1 }
  }), { clock });
  assert.equal(driftedRows.geometry, null);
  assert.equal(driftedRows.kmlAuthorityBlocked, true);
  assert.equal(driftedRows.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
});

test("I01C", "weak partial recovery provenance survives Finalizer and Confirmation without becoming Direct-16", () => {
  const rows = Array.from({ length: 16 }, (_, index) => {
    const label = index < 8 ? index + 1 : index < 12 ? index - 7 : index - 11;
    return `${label}. 10°00'${String(index + 1).padStart(2, "0")}.0"N, 20°00'${String(index + 1).padStart(2, "0")}.0"E`;
  });
  const baselineRows = [...rows.slice(0, 4), ...rows.slice(8, 12), ...rows.slice(12, 15)];
  const baselineText = [
    "SITES1", "POINT | LATITUDE | LONGITUDE", ...rows.slice(0, 4), `5. 10°00'30.0"N, 20°00'30.0"N`, "",
    "SITES2", "POINT | LATITUDE | LONGITUDE", ...rows.slice(8, 12), `5. 10°00'31.0"N, 20°00'31.0"N`, "",
    "SITES3", "POINT | LATITUDE | LONGITUDE", ...rows.slice(12, 15), `4. 10°00'32.0"N, 20°00'32.0"N`
  ].join("\n");
  const retryText = [
    "SITES1", "POINT | LATITUDE | LONGITUDE", ...rows.slice(0, 8), "",
    "SITES2", "POINT | LATITUDE | LONGITUDE", ...rows.slice(8, 12), "",
    "SITES3", "POINT | LATITUDE | LONGITUDE", ...rows.slice(12)
  ].join("\n");
  const weakImageBuffer = Buffer.from("synthetic-weak-partial-image-v1");
  const weakEvidence = evaluateDmsWeakPartialMultisiteRecovery({
    isImageInput: true,
    candidateSignal: true,
    structureText: baselineText,
    imageInputBuffer: weakImageBuffer
  });
  assert.equal(weakEvidence.accepted, true);
  const expansion = evaluateDmsGroupedAcquisitionExpansion({
    baselineText,
    retryText,
    allowWeakPartialMultisiteRecovery: true,
    isImageInput: true,
    projectedTableSignal: false,
    explicitHandwrittenSignal: false,
    weakPartialCandidateSignal: true,
    weakPartialInputEvidence: weakEvidence.inputEvidence,
    imageInputBuffer: weakImageBuffer
  });
  assert.equal(expansion.accepted, true);
  const baselineCoordinates = baselineRows.map(row => {
    const point = parseDmsSourceCoordinateRow(row);
    return `${point.longitude},${point.latitude}`;
  }).join("\n");
  const built = buildDmsGroupedPartialMultisiteRecoveryCandidate({
    stage1RawText: baselineText,
    stage1Coordinates: baselineCoordinates,
    retryRawText: retryText,
    retryCoordinates: expansion.normalizedCoordinates,
    expansion,
    ownerFamily: "dms_grouped",
    imageInputBuffer: weakImageBuffer,
    allowUnsanitizedWeakPartialStage1: true
  });
  assert.equal(built.accepted, true);
  assert.equal(built.provenance.recoveryMode, "STAGE1_WEAK_PARTIAL_MULTISITE_TO_16");
  assert.equal(built.provenance.stage1RejectedEvidence.rejectedLineCount, 3);
  const groups = [rows.slice(0, 8), rows.slice(8, 12), rows.slice(12)].map((groupRows, index) => ({
    group_id: `group_${index + 1}`,
    group_name: `SITES${index + 1}`,
    geometry: "polygon",
    requires_review: true,
    kml_ready: false,
    points: groupRows.map(row => {
      const point = parseDmsSourceCoordinateRow(row);
      return { label: point.label, lon: point.longitude, lat: point.latitude };
    })
  }));
  const recognitionResult = {
    stage1Candidate: built.stage1Candidate,
    candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
    sourceCandidateSeparate: true,
    directCanonicalPromotion: false,
    partialMultisiteRecoveryCandidate: {
      rawText: built.rawText,
      coordinates: built.normalizedCoordinates,
      candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
      provenance: built.provenance
    },
    partialMultisiteRecoveryProvenance: built.provenance,
    partialMultisiteRecoveryInputEvidence: built.weakPartialInputEvidence,
    sourceCandidates: {
      stage1: {
        ...built.stage1Candidate,
        rowCount: built.provenance.baselineRowCount,
        candidateRole: "STAGE1_ACQUISITION_CANDIDATE",
        candidateSha256: built.provenance.stage1CandidateSha256
      },
      structuredReread: {
        rawText: built.rawText,
        coordinates: built.normalizedCoordinates,
        rowCount: built.provenance.retryRowCount,
        candidateRole: "NONAUTHORITATIVE_REVIEW_CANDIDATE",
        candidateSha256: built.provenance.retryCandidateSha256
      }
    }
  };
  const coordinateEngineV2 = {
    coordinate_type: "standard_dms_table",
    precision_mode: "dms-coordinates",
    requires_review: true,
    partial_multisite_recovery_provenance: built.provenance,
    groups
  };
  const pending = finalizeCoordinateResult(createLegacyFinalizerInput({
    recognitionResult,
    coordinateEngineV2,
    verification: { status: "REVIEW", warnings: [] },
    revision: { resultId: "weak-partial-recovery-1", resultRevision: 1 }
  }), { clock });
  assert.equal(pending.candidateRole, "NONAUTHORITATIVE_REVIEW_CANDIDATE");
  assert.equal(pending.directCanonicalPromotion, false);
  assert.equal(pending.partialMultisiteRecoveryProvenance.recoveryMode, "STAGE1_WEAK_PARTIAL_MULTISITE_TO_16");
  assert.equal(pending.partialMultisiteRecoveryProvenance.stage1RejectedEvidence.rejectedLineCount, 3);
  assert.deepEqual(pending.partialMultisiteRecoveryInputEvidence, built.weakPartialInputEvidence);
  const runtime = new CoordinateConfirmationRuntime({ now: () => 1_000 });
  runtime.register(pending);
  const confirmed = runtime.confirm({
    resultId: pending.resultId,
    resultRevision: pending.resultRevision,
    geometryHash: pending.geometryHash,
    action: "accept"
  }).finalizedCoordinateResult;
  assert.equal(confirmed.candidateRole, "NONAUTHORITATIVE_REVIEW_CANDIDATE");
  assert.equal(confirmed.sourceCandidateSeparate, true);
  assert.equal(confirmed.directCanonicalPromotion, false);
  assert.equal(confirmed.partialMultisiteRecoveryProvenance.recoveryMode, "STAGE1_WEAK_PARTIAL_MULTISITE_TO_16");
  assert.deepEqual(confirmed.partialMultisiteRecoveryInputEvidence, built.weakPartialInputEvidence);
  assert.equal(confirmed.sourceCandidates.stage1.rawText, built.stage1Candidate.rawText);
  assert.equal(confirmed.sourceCandidates.stage1.rawText.includes(`10°00'30.0"N`), false);
  assert.equal(confirmed.sourceCandidates.stage1.rawText.includes(`10°00'31.0"N`), false);
  assert.equal(confirmed.sourceCandidates.stage1.rawText.includes(`10°00'32.0"N`), false);
  assert.equal(confirmed.sourceCandidates.structuredReread.rawText, retryText);

  const forged = finalizeCoordinateResult(createLegacyFinalizerInput({
    recognitionResult: {
      ...recognitionResult,
      partialMultisiteRecoveryProvenance: {
        ...built.provenance,
        stage1RejectedEvidence: {
          ...built.provenance.stage1RejectedEvidence,
          rejectedLineCount: 0
        }
      }
    },
    coordinateEngineV2,
    verification: { status: "PASS", warnings: [] },
    revision: { resultId: "weak-partial-recovery-forged", resultRevision: 1 }
  }), { clock });
  assert.equal(forged.geometry, null);
  assert.equal(forged.kmlAuthorityBlocked, true);
  assert.equal(forged.decisionState, COORDINATE_DECISION_STATE.BLOCKED);

  const unattested = finalizeCoordinateResult(createLegacyFinalizerInput({
    recognitionResult: {
      ...recognitionResult,
      partialMultisiteRecoveryInputEvidence: {
        ...built.weakPartialInputEvidence,
        runtimeAttestationId: "f".repeat(48)
      }
    },
    coordinateEngineV2,
    verification: { status: "PASS", warnings: [] },
    revision: { resultId: "weak-partial-recovery-unattested", resultRevision: 1 }
  }), { clock });
  assert.equal(unattested.geometry, null);
  assert.equal(unattested.kmlAuthorityBlocked, true);
  assert.equal(unattested.decisionState, COORDINATE_DECISION_STATE.BLOCKED);

  for (const forgedQualification of [
    { ...built.provenance.weakPartialQualification, inputModality: "MANUAL" },
    { ...built.provenance.weakPartialQualification, projectedTableSignal: true },
    { ...built.provenance.weakPartialQualification, independentHandwrittenSignal: true },
    { ...built.provenance.weakPartialQualification, candidateSignal: false },
    null,
    "IMAGE"
  ]) {
    const forgedProvenance = {
      ...built.provenance,
      weakPartialQualification: forgedQualification
    };
    const blocked = finalizeCoordinateResult(createLegacyFinalizerInput({
      recognitionResult: {
        ...recognitionResult,
        partialMultisiteRecoveryCandidate: {
          ...recognitionResult.partialMultisiteRecoveryCandidate,
          provenance: forgedProvenance
        },
        partialMultisiteRecoveryProvenance: forgedProvenance
      },
      coordinateEngineV2: {
        ...coordinateEngineV2,
        partial_multisite_recovery_provenance: forgedProvenance
      },
      verification: { status: "PASS", warnings: [] },
      revision: { resultId: `weak-partial-qualification-${String(forgedQualification?.inputModality || forgedQualification)}`, resultRevision: 1 }
    }), { clock });
    assert.equal(blocked.geometry, null);
    assert.equal(blocked.kmlAuthorityBlocked, true);
    assert.equal(blocked.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
    assert.equal(blocked.partialMultisiteRecoveryProvenance, null);
  }

  const rejectedRawLeak = `${built.stage1Candidate.rawText}\n5. 10°00'30.0"N, 20°00'30.0"N`;
  const leakedRecognition = {
    ...recognitionResult,
    stage1Candidate: {
      ...recognitionResult.stage1Candidate,
      rawText: rejectedRawLeak
    },
    sourceCandidates: {
      ...recognitionResult.sourceCandidates,
      stage1: {
        ...recognitionResult.sourceCandidates.stage1,
        rawText: rejectedRawLeak
      }
    }
  };
  const leaked = finalizeCoordinateResult(createLegacyFinalizerInput({
    recognitionResult: leakedRecognition,
    coordinateEngineV2,
    verification: { status: "PASS", warnings: [] },
    revision: { resultId: "weak-partial-rejected-raw-leak", resultRevision: 1 }
  }), { clock });
  assert.equal(leaked.geometry, null);
  assert.equal(leaked.kmlAuthorityBlocked, true);
  assert.equal(leaked.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
  assert.equal(leaked.sourceCandidates, null);

  const defensiveLeak = finalizeCoordinateResult({
    ...pending,
    resultId: "weak-partial-defensive-rejected-raw-leak",
    resultRevision: 1,
    sourceCandidates: {
      ...pending.sourceCandidates,
      stage1: {
        ...pending.sourceCandidates.stage1,
        rawText: rejectedRawLeak
      }
    }
  }, { clock });
  assert.equal(defensiveLeak.geometry, null);
  assert.equal(defensiveLeak.kmlAuthorityBlocked, true);
  assert.equal(defensiveLeak.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
  assert.equal(defensiveLeak.sourceCandidates, null);
});

test("I02", "V3 remains fail closed without authority decision", () => {
  const input = createV3FinalizerInput({
    coordinateEngineV3: {
      coordinate_type: "wgs84_decimal",
      precision_mode: "decimal",
      requires_review: false,
      groups: [{ group_id: "group_1", geometry: "point", requires_review: false, kml_ready: true, points: [{ lon: 103.1, lat: 16.5 }] }]
    },
    verification: { status: "PASS" },
    productionAuthority: false,
    revision: { resultId: "v3-1", resultRevision: 1 }
  });
  assert.equal(finalizeCoordinateResult(input, { clock }).decisionState, "BLOCKED");
});

test("S01", "fake consumer only receives AUTO_EXPORT geometry", () => {
  let count = 0;
  const accepted = consumeFinalizedGeometry(finalizeCoordinateResult(candidate(), { clock }), geometry => {
    count += 1;
    return geometry.type;
  });
  const blocked = consumeFinalizedGeometry(finalizeCoordinateResult(candidate({ qualityGateStatus: "failed" }), { clock }), () => {
    count += 1;
  });
  assert.equal(accepted.consumed, true);
  assert.equal(accepted.value, "Point");
  assert.equal(blocked.consumed, false);
  assert.equal(count, 1);
});

test("S02", "Spatial adapter consumes finalized result without reparsing", () => {
  const finalized = finalizeCoordinateResult(candidate(), { clock });
  registerFinalizedCoordinateResult(finalized);
  const adapted = new FinalizedResultSpatialGeometryAdapter().adapt(finalized);
  assert.equal(adapted.ok, true);
  assert.equal(adapted.geometry.schemaVersion, "normalized_geometry_v1");
  assert.deepEqual(adapted.geometry.geometry, pointGeometry);
  assert.equal(adapted.geometry.source.geometryHash, finalized.geometryHash);
});

test("S03", "Spatial adapter preserves review while allowing current authorized geometry", () => {
  const finalized = finalizeCoordinateResult(candidate({ confirmationStatus: "pending", currentAuthorizedGeometryExportable: true }), { clock });
  registerFinalizedCoordinateResult(finalized);
  const adapted = new FinalizedResultSpatialGeometryAdapter().adapt(finalized);
  assert.equal(adapted.ok, true);
  assert.equal(adapted.geometry.gate.decisionState, "REVIEW_REQUIRED");
  assert.ok(adapted.geometry.warnings.includes("CONFIRMATION_REQUIRED"));
});

test("A01", "async response identity rejects stale revision and hash", () => {
  const current = createSpatialResponseIdentity(finalizeCoordinateResult(candidate(), { clock }));
  assert.equal(spatialResponseMatchesCurrent(current, current), true);
  assert.equal(spatialResponseMatchesCurrent({ ...current, resultRevision: 2 }, current), false);
  assert.equal(spatialResponseMatchesCurrent({ ...current, geometryHash: "sha256:stale" }, current), false);
});

test("K01", "master flag defaults off and performs zero Spatial work", () => {
  let initialized = 0;
  let requested = 0;
  const boundary = createSpatialExecutionBoundary({
    env: {},
    initializeSpatial() { initialized += 1; },
    requestProvider() { requested += 1; }
  });
  const result = boundary.run({});
  assert.equal(result.status, "disabled");
  assert.equal(initialized, 0);
  assert.equal(requested, 0);
});

test("K02", "master flag can enable an explicitly guarded boundary", () => {
  let initialized = 0;
  const boundary = createSpatialExecutionBoundary({ env: { SPATIAL_RESULT_ENABLED: "true" }, initializeSpatial() { initialized += 1; } });
  assert.equal(boundary.run({}).status, "enabled");
  assert.equal(initialized, 1);
});

test("ND-F01", "forged near-duplicate authority cannot create Finalizer geometry", () => {
  const finalized = finalizeCoordinateResult(candidate({
    resultId: "near-duplicate-forged",
    nearDuplicateDecision: {
      schema_version: "near_duplicate_decision_v1",
      decision: "SAME_LOCATION_CONFIRMED",
      reason_codes: [],
      binding: { authority_revision: 1, observation_ids: ["obs-a", "obs-b"] },
      decision_sha256: "0".repeat(64)
    },
    geometryIntentAuthorityGate: {
      schema_version: "geometry_intent_authority_gate_v1",
      decision: "AUTHORIZED",
      geometry_type: "Point",
      authority_revision: 1,
      observation_ids: ["obs-a", "obs-b"],
      near_duplicate_decision_sha256: "0".repeat(64),
      gate_sha256: "0".repeat(64)
    }
  }), { clock });
  assert.equal(finalized.geometry, null);
  assert.equal(finalized.kmlReady, false);
  assert.equal(finalized.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
  assert.equal(finalized.nearDuplicateDecision, null);
  assert.equal(finalized.geometryIntentAuthorityGate, null);
});

test("ND-F02", "legacy candidates without near-duplicate declarations preserve existing authority", () => {
  const finalized = finalizeCoordinateResult(candidate({ resultId: "near-duplicate-not-declared" }), { clock });
  assert.deepEqual(finalized.geometry, pointGeometry);
  assert.equal(finalized.nearDuplicateDecision, null);
  assert.equal(finalized.geometryIntentAuthorityGate, null);
});

test("D01", "deadline configuration is always below 60 seconds", () => {
  assert.equal(getRecognitionHardDeadlineMs({}), 55_000);
  assert.equal(getRecognitionHardDeadlineMs({ RECOGNITION_HARD_DEADLINE_MS: "80000" }), 59_000);
});

test("D02", "request deadline aborts and returns controlled 504", async () => {
  const emitter = new EventEmitter();
  const response = Object.assign(emitter, {
    headersSent: false,
    statusCode: 200,
    body: null,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.headersSent = true; this.body = body; this.emit("finish"); return this; }
  });
  let context;
  recognitionDeadlineMiddleware({ deadlineMs: 15 })({}, response, () => { context = getRecognitionDeadlineContext(); });
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(response.statusCode, 504);
  assert.equal(response.body.code, "RECOGNITION_DEADLINE_EXCEEDED");
  assert.equal(context.signal.aborted, true);
});

test("D03", "request signal cancels composed provider signal", () => {
  const request = new AbortController();
  const provider = new AbortController();
  const combined = composeAbortSignals([request.signal, provider.signal]);
  request.abort();
  assert.equal(combined.signal.aborted, true);
  combined.cleanup();
});

let passed = 0;
for (const entry of cases) {
  try {
    await entry.fn();
    passed += 1;
    console.log(`PASS ${entry.id} ${entry.name}`);
  } catch (error) {
    console.error(`FAIL ${entry.id} ${entry.name}`);
    throw error;
  }
}
console.log(`SR-08 regression: ${passed}/${cases.length} PASS`);
