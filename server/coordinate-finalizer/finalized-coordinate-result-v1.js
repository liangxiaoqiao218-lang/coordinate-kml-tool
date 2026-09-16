import { randomUUID } from "node:crypto";
import { createGeometryHash } from "./geometry-hash.js";
import { evaluateCoordinateReleaseGate } from "./unified-gate.js";
import {
  COORDINATE_DECISION_STATE,
  COORDINATE_QUALITY_GATE_STATUS,
  FINALIZED_COORDINATE_SCHEMA_VERSION
} from "./reason-codes.js";
import {
  FAMILY_AVAILABILITY_STATUS,
  isFamilyAvailabilityBlocked
} from "./family-availability-policy.js";
import {
  buildDmsGroupedPartialMultisiteRecoveryCandidate,
  partialMultisiteRecoveryProvenanceMatches,
  validateWeakPartialMultisiteLifecycleEvidence
} from "../recognition/dms-source-structure.js";
import { validateWgs84NearDuplicateAuthority } from "../recognition/wgs84-near-duplicate-consolidation.js";

function uniqueStrings(values) {
  return Object.freeze([...new Set((Array.isArray(values) ? values : []).map(value => String(value || "").trim()).filter(Boolean))]);
}

function deepFreeze(value) {
  if (!value || typeof value !== "object" || Object.isFrozen(value)) return value;
  Object.values(value).forEach(deepFreeze);
  return Object.freeze(value);
}

function bindNearDuplicateAuthorityToGeometry(geometry, decision, geometryIntentGate) {
  if (!geometry || !decision || !geometryIntentGate) return geometry;
  return {
    ...geometry,
    coordinateAuthorityBinding: {
      schemaVersion: "near_duplicate_geometry_authority_binding_v1",
      decisionSha256: decision.decision_sha256,
      provenanceSha256: decision.binding?.provenance_sha256 || null,
      canonicalCoordinateSha256: decision.binding?.canonical_coordinate_sha256 || null,
      geometryIntentGateSha256: geometryIntentGate.gate_sha256
    }
  };
}

function normalizedGroups(text = "") {
  return String(text || "").trim().split(/\n\s*\n/).map(group => group.split(/\r?\n/)
    .map(line => line.trim()).filter(Boolean).map(line => {
      const match = line.match(/^([-+]?\d+(?:\.\d+)?)\s*,\s*([-+]?\d+(?:\.\d+)?)$/);
      if (!match) return null;
      const longitude = Number(match[1]);
      const latitude = Number(match[2]);
      return Number.isFinite(longitude) && Number.isFinite(latitude)
        && Math.abs(longitude) <= 180 && Math.abs(latitude) <= 90
        ? [longitude, latitude]
        : null;
    }));
}

function expectedWeakPartialGeometry(coordinates = "") {
  const groups = normalizedGroups(coordinates);
  if (groups.length !== 3
    || ![8, 4, 4].every((size, index) => groups[index]?.length === size)
    || groups.some(group => group.some(point => !point))) return null;
  return {
    type: "MultiPolygon",
    coordinates: groups.map(group => [[...group, [...group[0]]]])
  };
}

function rebuildWeakPartialLifecycleEvidence(candidate = {}) {
  const provenance = candidate?.partialMultisiteRecoveryProvenance;
  if (provenance?.recoveryMode !== "STAGE1_WEAK_PARTIAL_MULTISITE_TO_16") {
    return Object.freeze({ valid: true, provenance, sourceCandidates: candidate?.sourceCandidates || null, inputEvidence: null });
  }
  const stage1 = candidate?.sourceCandidates?.stage1;
  const reread = candidate?.sourceCandidates?.structuredReread;
  if (!stage1 || typeof stage1 !== "object" || !reread || typeof reread !== "object"
    || stage1.candidateRole !== "STAGE1_ACQUISITION_CANDIDATE"
    || reread.candidateRole !== "NONAUTHORITATIVE_REVIEW_CANDIDATE"
    || candidate.candidateRole !== "NONAUTHORITATIVE_REVIEW_CANDIDATE"
    || candidate.sourceCandidateSeparate !== true
    || candidate.directCanonicalPromotion !== false) return Object.freeze({ valid: false });
  const inputEvidence = candidate?.partialMultisiteRecoveryInputEvidence;
  if (validateWeakPartialMultisiteLifecycleEvidence({
    stage1RawText: stage1.rawText,
    qualification: provenance.weakPartialQualification,
    rejectedEvidence: provenance.stage1RejectedEvidence,
    inputEvidence
  }).valid !== true) return Object.freeze({ valid: false });
  const rebuilt = buildDmsGroupedPartialMultisiteRecoveryCandidate({
    stage1RawText: String(stage1.rawText || ""),
    stage1Coordinates: String(stage1.coordinates || ""),
    retryRawText: String(reread.rawText || ""),
    retryCoordinates: String(reread.coordinates || ""),
    expansion: {
      accepted: true,
      recoveryMode: provenance.recoveryMode,
      baselineRowCount: provenance.baselineRowCount,
      retryRowCount: provenance.retryRowCount,
      addedRowCount: provenance.addedRowCount,
      baselineRowsPreserved: provenance.baselineRowsPreserved,
      groupLocalBaselineRowsPreserved: provenance.groupLocalBaselineRowsPreserved,
      baselineGroupCount: provenance.baselineGroupCount,
      baselineGroupSizes: provenance.baselineGroupSizes,
      baselineGroupIdentities: provenance.baselineGroupIdentities,
      groupCount: provenance.retryGroupCount,
      groupSizes: provenance.retryGroupSizes,
      groupIdentities: provenance.retryGroupIdentities,
      weakPartialQualification: provenance.weakPartialQualification,
      stage1RejectedEvidence: provenance.stage1RejectedEvidence,
      weakPartialInputEvidence: inputEvidence
    },
    ownerFamily: provenance.ownerFamily,
    weakPartialInputEvidence: inputEvidence
  });
  const sourcesMatch = rebuilt.accepted === true
    && stage1.rawText === rebuilt.stage1Candidate.rawText
    && stage1.coordinates === rebuilt.stage1Candidate.coordinates
    && stage1.rowCount === rebuilt.provenance.baselineRowCount
    && stage1.candidateSha256 === rebuilt.provenance.stage1CandidateSha256
    && reread.rawText === rebuilt.rawText
    && reread.coordinates === rebuilt.normalizedCoordinates
    && reread.rowCount === rebuilt.provenance.retryRowCount
    && reread.candidateSha256 === rebuilt.provenance.retryCandidateSha256;
  const expectedGeometry = sourcesMatch
    ? expectedWeakPartialGeometry(rebuilt.normalizedCoordinates)
    : null;
  const geometryMatches = expectedGeometry !== null
    && JSON.stringify(candidate.geometry) === JSON.stringify(expectedGeometry)
    && Array.isArray(candidate.groups)
    && candidate.groups.length === 3;
  if (!sourcesMatch || !geometryMatches
    || !partialMultisiteRecoveryProvenanceMatches(provenance, rebuilt.provenance)
    || JSON.stringify(inputEvidence) !== JSON.stringify(rebuilt.weakPartialInputEvidence)) {
    return Object.freeze({ valid: false });
  }
  return Object.freeze({
    valid: true,
    provenance: rebuilt.provenance,
    sourceCandidates: Object.freeze({
      stage1: Object.freeze({ ...stage1 }),
      structuredReread: Object.freeze({ ...reread })
    }),
    inputEvidence: Object.freeze({ ...rebuilt.weakPartialInputEvidence })
  });
}

export function finalizeCoordinateResult(candidate = {}, { clock = () => new Date().toISOString() } = {}) {
  const now = clock();
  const resultId = candidate.resultId || randomUUID();
  const resultRevision = candidate.resultRevision ?? 1;
  const availabilityStatus = candidate.availabilityStatus || FAMILY_AVAILABILITY_STATUS.AVAILABLE;
  const availabilityBlocked = isFamilyAvailabilityBlocked({ status: availabilityStatus });
  const weakPartialLifecycle = rebuildWeakPartialLifecycleEvidence(candidate);
  const weakPartialEvidenceValid = weakPartialLifecycle.valid === true;
  const nearDuplicateAuthority = validateWgs84NearDuplicateAuthority({
    decision: candidate.nearDuplicateDecision,
    geometryIntentGate: candidate.geometryIntentAuthorityGate,
    resultRevision
  });
  const nearDuplicateEvidenceValid = nearDuplicateAuthority.valid === true;
  const nearDuplicateAuthorityBlocked = nearDuplicateAuthority.blocked === true;
  const effectiveCandidate = {
    ...candidate,
    explicitAuthorityRejected: candidate.explicitAuthorityRejected === true
      || !weakPartialEvidenceValid || !nearDuplicateEvidenceValid,
    qualityGateStatus: weakPartialEvidenceValid && nearDuplicateEvidenceValid
      ? candidate.qualityGateStatus
      : COORDINATE_QUALITY_GATE_STATUS.FAILED,
    availabilityStatus,
    technicalKmlReady: availabilityBlocked || !weakPartialEvidenceValid || nearDuplicateAuthorityBlocked
      ? false
      : (candidate.technicalKmlReady === true || candidate.kmlReady === true),
    requiresReview: availabilityBlocked ? false : (!weakPartialEvidenceValid || nearDuplicateAuthorityBlocked || candidate.requiresReview),
    kmlReady: availabilityBlocked || !weakPartialEvidenceValid || nearDuplicateAuthorityBlocked ? false : candidate.kmlReady,
    kmlAuthorityBlocked: candidate.kmlAuthorityBlocked === true || !weakPartialEvidenceValid || nearDuplicateAuthorityBlocked,
    resultId,
    resultRevision
  };
  const gate = evaluateCoordinateReleaseGate(effectiveCandidate);
  const baseGeometry = weakPartialEvidenceValid && !nearDuplicateAuthorityBlocked && candidate.geometry
    ? structuredClone(candidate.geometry)
    : null;
  const geometry = bindNearDuplicateAuthorityToGeometry(
    baseGeometry,
    nearDuplicateAuthority.declared && nearDuplicateEvidenceValid ? candidate.nearDuplicateDecision : null,
    nearDuplicateAuthority.declared && nearDuplicateEvidenceValid ? candidate.geometryIntentAuthorityGate : null
  );
  const geometryHash = geometry ? createGeometryHash(geometry) : null;
  const result = {
    schemaVersion: FINALIZED_COORDINATE_SCHEMA_VERSION,
    resultId,
    resultRevision,
    geometryHash,
    sourceAuthority: candidate.sourceAuthority || null,
    explicitAuthorityRejected: effectiveCandidate.explicitAuthorityRejected === true,
    coordinateType: candidate.coordinateType || null,
    precisionMode: candidate.precisionMode || null,
    family: candidate.family || candidate.coordinateType || null,
    availabilityStatus,
    availabilityReasonCode: candidate.availabilityReasonCode || null,
    familyAvailabilityPolicy: candidate.familyAvailabilityPolicy
      ? deepFreeze(structuredClone(candidate.familyAvailabilityPolicy))
      : null,
    crs: candidate.crs ? Object.freeze({ ...candidate.crs }) : null,
    geometry: geometry ? Object.freeze(geometry) : null,
    confirmationStatus: candidate.confirmationStatus || null,
    qualityGateStatus: effectiveCandidate.qualityGateStatus || null,
    decisionState: gate.decisionState,
    technicalKmlReady: effectiveCandidate.technicalKmlReady === true,
    ...(effectiveCandidate.kmlAuthorityBlocked === true ? { kmlAuthorityBlocked: true } : {}),
    requiresReview: effectiveCandidate.requiresReview !== false,
    kmlReady: gate.kmlReady === true,
    reasonCodes: gate.reasonCodes,
    blockingReasons: gate.blockingReasons,
    warnings: uniqueStrings(candidate.warnings),
    limitations: uniqueStrings(candidate.limitations),
    groups: Object.freeze((Array.isArray(candidate.groups) ? candidate.groups : []).map(group => Object.freeze({ ...group }))),
    familySafetyPolicy: candidate.familySafetyPolicy ? deepFreeze(structuredClone(candidate.familySafetyPolicy)) : null,
    candidateRole: candidate.candidateRole === "NONAUTHORITATIVE_REVIEW_CANDIDATE"
      ? candidate.candidateRole
      : null,
    sourceCandidateSeparate: candidate.sourceCandidateSeparate === true,
    directCanonicalPromotion: candidate.directCanonicalPromotion === false ? false : null,
    partialMultisiteRecoveryProvenance: weakPartialEvidenceValid && weakPartialLifecycle.provenance
      ? deepFreeze(structuredClone(weakPartialLifecycle.provenance))
      : null,
    partialMultisiteRecoveryInputEvidence: weakPartialEvidenceValid && weakPartialLifecycle.inputEvidence
      ? deepFreeze(structuredClone(weakPartialLifecycle.inputEvidence))
      : null,
    sourceCandidates: weakPartialEvidenceValid && weakPartialLifecycle.sourceCandidates
      ? deepFreeze(structuredClone(weakPartialLifecycle.sourceCandidates))
      : null,
    nearDuplicateDecision: nearDuplicateAuthority.declared && nearDuplicateEvidenceValid
      ? deepFreeze(structuredClone(candidate.nearDuplicateDecision))
      : null,
    geometryIntentAuthorityGate: nearDuplicateAuthority.declared && nearDuplicateEvidenceValid
      ? deepFreeze(structuredClone(candidate.geometryIntentAuthorityGate))
      : null,
    createdAt: candidate.createdAt || now,
    finalizedAt: now
  };
  result.gate = Object.freeze({
    decisionState: result.decisionState,
    qualityGateStatus: result.qualityGateStatus,
    confirmationStatus: result.confirmationStatus,
    availabilityStatus: result.availabilityStatus,
    availabilityReasonCode: result.availabilityReasonCode
  });
  return Object.freeze(result);
}

export function consumeFinalizedGeometry(finalizedResult, consumer) {
  if (finalizedResult?.schemaVersion !== FINALIZED_COORDINATE_SCHEMA_VERSION
    || finalizedResult?.decisionState !== COORDINATE_DECISION_STATE.AUTO_EXPORT) {
    return Object.freeze({ consumed: false, reasonCodes: finalizedResult?.reasonCodes || [] });
  }
  return Object.freeze({ consumed: true, value: consumer(finalizedResult.geometry, finalizedResult) });
}
