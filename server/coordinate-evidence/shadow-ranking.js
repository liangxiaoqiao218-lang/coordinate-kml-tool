import {
  COORDINATE_EVIDENCE_RECOMMENDED_STATE,
  COORDINATE_EVIDENCE_SHADOW_DECISION_SCHEMA_VERSION,
  createCoordinateEvidenceCandidate,
  isCoordinateEvidenceCandidate
} from "./schema.js";

const CONFIDENCE_SCORE = Object.freeze({
  high: 3,
  medium: 2,
  low: 1,
  unknown: 0
});

function normalizeCandidate(candidate = {}) {
  return isCoordinateEvidenceCandidate(candidate)
    ? candidate
    : createCoordinateEvidenceCandidate(candidate);
}

function getConfidenceScore(candidate = {}) {
  return CONFIDENCE_SCORE[String(candidate.confidence?.level || "unknown").toLowerCase()] || 0;
}

function getSafetyScore(candidate = {}) {
  const attributes = candidate.attributes || {};
  let score = 0;
  if (attributes.transformVerified) score += 3;
  if (attributes.hasStructuredTable) score += 2;
  if (attributes.hasExplicitHemisphere) score += 1;
  if (attributes.hasExplicitCoordinateOrder) score += 1;
  if (attributes.geometryValid === false) score -= 4;
  if (attributes.hemisphereAmbiguous) score -= 3;
  if (attributes.coordinateOrderAmbiguous) score -= 2;
  return score;
}

function getStateScore(candidate = {}) {
  if (candidate.recommendedState === COORDINATE_EVIDENCE_RECOMMENDED_STATE.AUTO_EXPORT && candidate.canGenerateKml) {
    return 2;
  }
  if (candidate.recommendedState === COORDINATE_EVIDENCE_RECOMMENDED_STATE.CONFIRM_REQUIRED) {
    return 1;
  }
  return 0;
}

function compareCandidates(left = {}, right = {}) {
  const authorityDelta = Number(right.authority?.level || 0) - Number(left.authority?.level || 0);
  if (authorityDelta !== 0) return authorityDelta;

  const confidenceDelta = getConfidenceScore(right) - getConfidenceScore(left);
  if (confidenceDelta !== 0) return confidenceDelta;

  const safetyDelta = getSafetyScore(right) - getSafetyScore(left);
  if (safetyDelta !== 0) return safetyDelta;

  const stateDelta = getStateScore(right) - getStateScore(left);
  if (stateDelta !== 0) return stateDelta;

  return String(left.evidenceId || "").localeCompare(String(right.evidenceId || ""));
}

function inferDecisionReason(winner = {}, candidates = []) {
  const runnerUp = candidates.find(candidate => candidate.evidenceId !== winner.evidenceId);
  if (!runnerUp) return "single_candidate";
  if (Number(winner.authority?.level || 0) > Number(runnerUp.authority?.level || 0)) {
    return "higher_authority_evidence";
  }
  if (getConfidenceScore(winner) > getConfidenceScore(runnerUp)) {
    return "higher_confidence_evidence";
  }
  if (getSafetyScore(winner) > getSafetyScore(runnerUp)) {
    return "safer_evidence";
  }
  return "stable_shadow_ranking";
}

function currentWinnerType(currentWinner = {}) {
  return String(
    currentWinner.currentWinnerType
    || currentWinner.coordinateType
    || currentWinner.evidenceType
    || ""
  ).trim();
}

function currentWinnerPrecision(currentWinner = {}) {
  return String(
    currentWinner.currentWinnerPrecision
    || currentWinner.precisionMode
    || ""
  ).trim();
}

export function rankCoordinateEvidenceCandidates(candidates = [], currentWinner = {}) {
  const normalizedCandidates = (Array.isArray(candidates) ? candidates : [])
    .map(normalizeCandidate)
    .filter(isCoordinateEvidenceCandidate);

  const sortedCandidates = [...normalizedCandidates].sort(compareCandidates);
  const winner = sortedCandidates[0] || null;
  const currentType = currentWinnerType(currentWinner);
  const currentPrecision = currentWinnerPrecision(currentWinner);
  const differenceFromCurrentWinner = Boolean(
    winner
    && currentType
    && winner.evidenceType !== currentType
    && winner.coordinateSource !== currentType
  );

  return Object.freeze({
    schemaVersion: COORDINATE_EVIDENCE_SHADOW_DECISION_SCHEMA_VERSION,
    winnerEvidenceId: winner?.evidenceId || null,
    winnerEvidenceType: winner?.evidenceType || null,
    winnerAuthority: winner?.authority || null,
    currentWinnerType: currentType || null,
    currentWinnerPrecision: currentPrecision || null,
    differenceFromCurrentWinner,
    reason: winner ? inferDecisionReason(winner, sortedCandidates) : "no_candidates",
    blockedByShadowOnly: Boolean(
      winner
      && (
        winner.recommendedState === COORDINATE_EVIDENCE_RECOMMENDED_STATE.BLOCKED_REVIEW
        || winner.canGenerateKml === false
      )
    ),
    affectsLegacyWinner: false,
    affectsCoordinateResult: false,
    affectsKml: false
  });
}

export function sortCoordinateEvidenceCandidates(candidates = []) {
  return (Array.isArray(candidates) ? candidates : [])
    .map(normalizeCandidate)
    .filter(isCoordinateEvidenceCandidate)
    .sort(compareCandidates);
}
