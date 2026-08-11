export const COORDINATE_EVIDENCE_SHADOW_OBSERVATION_SCHEMA_VERSION =
  "coordinate_evidence_shadow_observation_v1";

export const SHADOW_OBSERVATION_CLASSIFICATION = Object.freeze({
  PASS: "PASS",
  FAIL_CANDIDATE_MISSING: "FAIL_CANDIDATE_MISSING",
  FAIL_RANKING_MISMATCH: "FAIL_RANKING_MISMATCH",
  FAIL_ISOLATION: "FAIL_ISOLATION",
  PENDING: "PENDING"
});

export const SHADOW_OBSERVATION_CATEGORY = Object.freeze({
  STRUCTURED_LEGAL_COORDINATE: "structured_legal_coordinate",
  EXPLICIT_GEOGRAPHIC_SEMANTIC: "explicit_geographic_semantic",
  VERIFIED_TRANSFORMATION: "verified_transformation",
  PENDING_FIXTURE: "pending_fixture"
});

const SECRET_VALUE_PATTERN =
  /(sk-[a-z0-9_-]{8,}|dashscope[_-]?[a-z0-9_-]*|supabase[_-]?[a-z0-9_-]*|bearer\s+[a-z0-9._-]+|api[_-]?key\s*[:=]|secret\s*[:=]|token\s*[:=]|password\s*[:=]|authorization\s*[:=])/ig;

function cleanString(value, fallback = "") {
  return String(value ?? fallback)
    .replace(SECRET_VALUE_PATTERN, "[REDACTED]")
    .trim();
}

function nullableString(value) {
  const cleaned = cleanString(value);
  return cleaned || null;
}

function numberOrNull(value) {
  const numberValue = Number(value);
  return Number.isFinite(numberValue) ? numberValue : null;
}

function booleanOrNull(value) {
  if (value === true) return true;
  if (value === false) return false;
  return null;
}

export const SHADOW_OBSERVATION_POLICIES = Object.freeze({
  [SHADOW_OBSERVATION_CATEGORY.STRUCTURED_LEGAL_COORDINATE]: Object.freeze({
    category: SHADOW_OBSERVATION_CATEGORY.STRUCTURED_LEGAL_COORDINATE,
    comparisonRequired: true,
    expectedMinimumCandidates: 2,
    requiredWinnerEvidenceType: "structured_cadastral_table",
    requiredWinnerAuthority: 5,
    requiredLoserEvidenceTypes: Object.freeze(["utm_crs_text"]),
    optionalLoserEvidenceTypes: Object.freeze([])
  }),
  [SHADOW_OBSERVATION_CATEGORY.EXPLICIT_GEOGRAPHIC_SEMANTIC]: Object.freeze({
    category: SHADOW_OBSERVATION_CATEGORY.EXPLICIT_GEOGRAPHIC_SEMANTIC,
    comparisonRequired: false,
    expectedMinimumCandidates: 1,
    requiredWinnerEvidenceType: "explicit_geographic_dms",
    requiredWinnerAuthority: 5,
    requiredWinnerConfidence: "high",
    requiredLoserEvidenceTypes: Object.freeze([]),
    optionalLoserEvidenceTypes: Object.freeze(["generic_decimal"]),
    candidateAbsencePolicy: "optional_loser_absence_allowed"
  }),
  [SHADOW_OBSERVATION_CATEGORY.VERIFIED_TRANSFORMATION]: Object.freeze({
    category: SHADOW_OBSERVATION_CATEGORY.VERIFIED_TRANSFORMATION,
    comparisonRequired: true,
    expectedMinimumCandidates: 2,
    requiredWinnerEvidenceType: "verified_utm_transformation",
    requiredWinnerAuthority: 4,
    requiredTransformVerified: true,
    requiredLoserEvidenceTypes: Object.freeze(["utm_crs_text"]),
    optionalLoserEvidenceTypes: Object.freeze([])
  }),
  [SHADOW_OBSERVATION_CATEGORY.PENDING_FIXTURE]: Object.freeze({
    category: SHADOW_OBSERVATION_CATEGORY.PENDING_FIXTURE,
    fixtureStatus: "pending_real_fixture",
    historicalRegressionEnabled: false,
    expectedMinimumCandidates: 0,
    requiredWinnerEvidenceType: null,
    requiredLoserEvidenceTypes: Object.freeze([]),
    optionalLoserEvidenceTypes: Object.freeze([])
  })
});

export function getShadowObservationPolicy(categoryOrPolicy = SHADOW_OBSERVATION_CATEGORY.PENDING_FIXTURE) {
  if (categoryOrPolicy && typeof categoryOrPolicy === "object") {
    return Object.freeze({
      requiredLoserEvidenceTypes: [],
      optionalLoserEvidenceTypes: [],
      ...categoryOrPolicy
    });
  }
  return SHADOW_OBSERVATION_POLICIES[categoryOrPolicy]
    || SHADOW_OBSERVATION_POLICIES[SHADOW_OBSERVATION_CATEGORY.PENDING_FIXTURE];
}

export function createLegacySnapshot(source = {}) {
  const coordinateResult = source.coordinateResult || {};
  return Object.freeze({
    coordinateType: nullableString(source.coordinateType),
    precisionMode: nullableString(source.precisionMode),
    confirmationStatus: nullableString(source.confirmationStatus),
    qualityGateStatus: nullableString(source.qualityGateStatus),
    coordinateResultState: nullableString(source.coordinateResultState || coordinateResult.state),
    kmlReady: source.kmlReady === true || source.kml_ready === true
  });
}

export function summarizeObservationCandidate(candidate = {}) {
  const authority = candidate.authority && typeof candidate.authority === "object"
    ? candidate.authority.level
    : candidate.authority ?? candidate.authorityLevel;
  const confidence = candidate.confidence && typeof candidate.confidence === "object"
    ? candidate.confidence.level
    : candidate.confidence ?? candidate.confidenceLevel;

  return Object.freeze({
    evidenceType: nullableString(candidate.evidenceType || candidate.type),
    authority: numberOrNull(authority),
    confidence: nullableString(confidence),
    sourceParser: nullableString(candidate.sourceParser || candidate.source?.parser),
    coordinateSource: nullableString(candidate.coordinateSource || candidate.source?.coordinateSource),
    reason: nullableString(candidate.reason || candidate.attributes?.reason),
    transformVerified: candidate.attributes?.transformVerified === true
      || candidate.transformVerified === true
      || candidate.verification?.transformVerified === true
  });
}

export function summarizeShadowDecision(decision = {}) {
  const authority = decision.winnerAuthority && typeof decision.winnerAuthority === "object"
    ? decision.winnerAuthority.level
    : decision.winnerAuthority || decision.authority;
  return Object.freeze({
    winner: nullableString(
      decision.winnerEvidenceType
      || decision.winner?.evidenceType
      || decision.winner
    ),
    authority: numberOrNull(authority),
    reason: nullableString(decision.reason),
    differenceFromCurrentWinner: decision.differenceFromCurrentWinner === true
  });
}

export function summarizeIsolation(decision = {}, isolation = {}) {
  return Object.freeze({
    affectsLegacyWinner: booleanOrNull(isolation.affectsLegacyWinner ?? decision.affectsLegacyWinner),
    affectsCoordinateResult: booleanOrNull(isolation.affectsCoordinateResult ?? decision.affectsCoordinateResult),
    affectsKml: booleanOrNull(isolation.affectsKml ?? decision.affectsKml)
  });
}

function findCandidate(candidates = [], evidenceType) {
  return candidates.find(candidate => candidate.evidenceType === evidenceType);
}

function isolationFailed(isolation = {}) {
  return isolation.affectsLegacyWinner !== false
    || isolation.affectsCoordinateResult !== false
    || isolation.affectsKml !== false;
}

function requiredCandidatesMissing(candidates = [], policy = {}) {
  if ((candidates || []).length < Number(policy.expectedMinimumCandidates || 0)) return true;
  if (policy.requiredWinnerEvidenceType && !findCandidate(candidates, policy.requiredWinnerEvidenceType)) {
    return true;
  }
  return (policy.requiredLoserEvidenceTypes || [])
    .some(evidenceType => !findCandidate(candidates, evidenceType));
}

function winnerMismatch(candidates = [], decision = {}, policy = {}) {
  if (!policy.requiredWinnerEvidenceType) return false;
  const winner = findCandidate(candidates, policy.requiredWinnerEvidenceType);
  if (!winner) return true;
  if (decision.winner !== policy.requiredWinnerEvidenceType) return true;
  if (
    policy.requiredWinnerAuthority !== undefined
    && Number(decision.authority || winner.authority || 0) !== Number(policy.requiredWinnerAuthority)
  ) {
    return true;
  }
  if (
    policy.requiredWinnerConfidence
    && String(winner.confidence || "").toLowerCase() !== String(policy.requiredWinnerConfidence).toLowerCase()
  ) {
    return true;
  }
  if (policy.requiredTransformVerified === true && winner.transformVerified !== true) {
    return true;
  }
  if (decision.differenceFromCurrentWinner === true && !decision.reason) {
    return true;
  }
  return false;
}

export function classifyShadowObservation(observation = {}) {
  const policy = getShadowObservationPolicy(observation.policy || observation.category);
  if (policy.historicalRegressionEnabled === false || policy.category === SHADOW_OBSERVATION_CATEGORY.PENDING_FIXTURE) {
    return SHADOW_OBSERVATION_CLASSIFICATION.PENDING;
  }
  if (isolationFailed(observation.isolation)) {
    return SHADOW_OBSERVATION_CLASSIFICATION.FAIL_ISOLATION;
  }
  if (requiredCandidatesMissing(observation.candidates, policy)) {
    return SHADOW_OBSERVATION_CLASSIFICATION.FAIL_CANDIDATE_MISSING;
  }
  if (winnerMismatch(observation.candidates, observation.shadowDecision, policy)) {
    return SHADOW_OBSERVATION_CLASSIFICATION.FAIL_RANKING_MISMATCH;
  }
  return SHADOW_OBSERVATION_CLASSIFICATION.PASS;
}

export function buildCoordinateEvidenceShadowObservation(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const policy = getShadowObservationPolicy(input.policy || input.category || source.category);
  const rawCandidates = input.candidates || source.coordinateEvidenceCandidates || source.candidates || [];
  const rawShadowDecision = input.shadowDecision || source.shadowEvidenceDecision || source.shadowDecision || {};
  const candidates = Object.freeze((Array.isArray(rawCandidates) ? rawCandidates : [])
    .map(summarizeObservationCandidate)
    .filter(candidate => candidate.evidenceType));
  const shadowDecision = summarizeShadowDecision(rawShadowDecision);
  const isolation = summarizeIsolation(rawShadowDecision, input.isolation || source.isolation);

  const observation = {
    schemaVersion: COORDINATE_EVIDENCE_SHADOW_OBSERVATION_SCHEMA_VERSION,
    sampleId: nullableString(input.sampleId || source.sampleId),
    timestamp: nullableString(input.timestamp || source.timestamp) || new Date().toISOString(),
    commit: nullableString(input.commit || source.commit),
    branch: nullableString(input.branch || source.branch),
    category: policy.category,
    fixture: Object.freeze({
      fileName: nullableString(input.fixture?.fileName || source.fixture?.fileName || input.fileName || source.fileName),
      fixtureStatus: nullableString(input.fixture?.fixtureStatus || source.fixture?.fixtureStatus || policy.fixtureStatus) || "real_current_capture",
      fixtureHash: nullableString(input.fixture?.fixtureHash || source.fixture?.fixtureHash || input.fixtureHash || source.fixtureHash)
    }),
    policy: Object.freeze({
      category: policy.category,
      fixtureStatus: policy.fixtureStatus || null,
      historicalRegressionEnabled: policy.historicalRegressionEnabled ?? null,
      comparisonRequired: policy.comparisonRequired === true,
      expectedMinimumCandidates: Number(policy.expectedMinimumCandidates || 0),
      requiredWinnerEvidenceType: policy.requiredWinnerEvidenceType || null,
      requiredWinnerAuthority: policy.requiredWinnerAuthority ?? null,
      requiredWinnerConfidence: policy.requiredWinnerConfidence || null,
      requiredTransformVerified: policy.requiredTransformVerified === true,
      requiredLoserEvidenceTypes: Object.freeze([...(policy.requiredLoserEvidenceTypes || [])]),
      optionalLoserEvidenceTypes: Object.freeze([...(policy.optionalLoserEvidenceTypes || [])]),
      candidateAbsencePolicy: policy.candidateAbsencePolicy || null
    }),
    legacySnapshot: createLegacySnapshot(input.legacySnapshot || source.legacySnapshot || source),
    candidates,
    shadowDecision,
    isolation
  };

  return Object.freeze({
    ...observation,
    classification: classifyShadowObservation(observation)
  });
}
