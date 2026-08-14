import {
  createLegacySnapshot,
  summarizeObservationCandidate,
  summarizeShadowDecision
} from "./shadow-observation.js";

export const EVIDENCE_ARBITRATION_PROPOSAL_SCHEMA_VERSION =
  "evidence_arbitration_proposal_v1";

export const EVIDENCE_ARBITRATION_PROPOSAL_MODE = Object.freeze({
  DRY_RUN: "dry_run",
  REVIEW_ONLY: "review_only",
  DISABLED: "disabled"
});

export const EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION = Object.freeze({
  AGREEMENT: "AGREEMENT",
  TYPE_AGREEMENT_WITH_COORDINATE_DISAGREEMENT: "TYPE_AGREEMENT_WITH_COORDINATE_DISAGREEMENT",
  REVIEW_REQUIRED: "REVIEW_REQUIRED",
  NO_PROPOSAL: "NO_PROPOSAL",
  BLOCKED_KML_GATE: "BLOCKED_KML_GATE",
  BLOCKED_PENDING_POLICY: "BLOCKED_PENDING_POLICY"
});

const SECRET_VALUE_PATTERN =
  /(sk-[a-z0-9_-]{8,}|dashscope[_-]?[a-z0-9_-]*|supabase[_-]?[a-z0-9_-]*|bearer\s+[a-z0-9._-]+|api[_-]?key\s*[:=]|secret\s*[:=]|token\s*[:=]|password\s*[:=]|authorization\s*[:=])/ig;

const PENDING_CATEGORY_PATTERN = /pending|indonesia.*dms.*utm|dms.*utm.*indonesia/i;

const EVIDENCE_INTERPRETATION = Object.freeze({
  structured_cadastral_table: Object.freeze({
    coordinateType: "madagascar_cadastral_grid",
    precisionMode: "cadastral-grid-num-xv-yv",
    recommendedAction: "manual_review"
  }),
  explicit_geographic_dms: Object.freeze({
    coordinateType: "cote_divoire_geographic_dms_table",
    precisionMode: "cote-divoire-geographic-dms-table",
    recommendedAction: "preserve_or_review"
  }),
  verified_utm_transformation: Object.freeze({
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y",
    recommendedAction: "preserve_or_review"
  }),
  utm_crs_text: Object.freeze({
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y-review",
    recommendedAction: "manual_review"
  })
});

function cleanString(value, fallback = "") {
  const cleaned = String(value ?? fallback)
    .replace(SECRET_VALUE_PATTERN, "[REDACTED]")
    .trim();
  return cleaned || fallback;
}

function nullableString(value) {
  const cleaned = cleanString(value);
  return cleaned || null;
}

function booleanValue(value, fallback = false) {
  return value === true || value === false ? value : fallback;
}

function normalizeFlags(input = {}) {
  return Object.freeze({
    dryRun: booleanValue(input.dryRun, true),
    reviewOnly: booleanValue(input.reviewOnly, true),
    migration: booleanValue(input.migration, false),
    kmlGate: booleanValue(input.kmlGate, false)
  });
}

function proposalMode(flags = {}) {
  if (flags.migration === true) return EVIDENCE_ARBITRATION_PROPOSAL_MODE.REVIEW_ONLY;
  if (flags.dryRun === true) return EVIDENCE_ARBITRATION_PROPOSAL_MODE.DRY_RUN;
  return EVIDENCE_ARBITRATION_PROPOSAL_MODE.DISABLED;
}

function normalizeCandidateList(candidates = []) {
  return Object.freeze((Array.isArray(candidates) ? candidates : [])
    .map(candidate => Object.freeze({
      ...summarizeObservationCandidate(candidate),
      authorityCategory: nullableString(candidate.authority?.category || candidate.authorityCategory),
      coordinateInterpretation: candidate.coordinateInterpretation && typeof candidate.coordinateInterpretation === "object"
        ? candidate.coordinateInterpretation
        : null,
      canGenerateKml: candidate.canGenerateKml === true
        ? true
        : candidate.canGenerateKml === false
          ? false
          : null
    }))
    .filter(candidate => candidate.evidenceType));
}

function normalizeCoordinatePoint(value = {}, index = 0) {
  const lat = Number(value.lat);
  const lon = Number(value.lon);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  return Object.freeze({
    point: cleanString(value.point || value.label || value.id || String(index + 1)),
    lat: Number(lat.toFixed(9)),
    lon: Number(lon.toFixed(9)),
    source: nullableString(value.source) || "unknown"
  });
}

function parseCoordinateText(value = "") {
  return String(value || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .map((line, index) => {
      const numbers = [...line.matchAll(/[-+]?\d+(?:\.\d+)?/g)].map(match => Number(match[0]));
      if (line.includes("|") && numbers.length >= 3) {
        return normalizeCoordinatePoint({
          point: String(numbers[0]),
          lat: numbers[1],
          lon: numbers[2],
          source: "legacy_coordinates_text"
        }, index);
      }
      if (numbers.length >= 2) {
        return normalizeCoordinatePoint({
          point: String(index + 1),
          lat: numbers[0],
          lon: numbers[1],
          source: "legacy_coordinates_text"
        }, index);
      }
      return null;
    })
    .filter(Boolean);
}

function engineCoordinates(source = {}) {
  const groups = Array.isArray(source.coordinateEngineV2?.groups)
    ? source.coordinateEngineV2.groups
    : [];
  return groups.flatMap(group => Array.isArray(group.points) ? group.points : [])
    .map((point, index) => normalizeCoordinatePoint({
      point: point.point || point.id || point.label || String(index + 1),
      lat: point.lat ?? point.latitude,
      lon: point.lon ?? point.lng ?? point.longitude,
      source: "legacy_coordinate_engine_v2"
    }, index))
    .filter(Boolean);
}

function legacyCoordinateInterpretation(source = {}) {
  if (source.coordinateInterpretation?.normalizedCoordinates) {
    return source.coordinateInterpretation;
  }
  const publicCoordinates = parseCoordinateText(source.coordinates || source.rawCoordinates || "");
  const normalizedCoordinates = publicCoordinates.length
    ? publicCoordinates
    : engineCoordinates(source);
  if (!normalizedCoordinates.length) return null;
  return Object.freeze({
    schemaVersion: "legacy_coordinate_interpretation_v1",
    interpretationStatus: "COMPLETE",
    deterministicConversion: false,
    hemisphereResolved: false,
    pointCount: normalizedCoordinates.length,
    normalizedCoordinates: Object.freeze(normalizedCoordinates),
    affectsLegacyWinner: false,
    affectsCoordinateResult: false,
    affectsKml: false
  });
}

function normalizeInterpretation(value = {}) {
  if (!value || typeof value !== "object" || !Array.isArray(value.normalizedCoordinates)) return null;
  const normalizedCoordinates = value.normalizedCoordinates
    .map(normalizeCoordinatePoint)
    .filter(Boolean);
  if (!normalizedCoordinates.length) return null;
  return Object.freeze({
    schemaVersion: nullableString(value.schemaVersion) || "coordinate_interpretation_v1",
    interpretationStatus: nullableString(value.interpretationStatus || value.status) || "COMPLETE",
    deterministicConversion: value.deterministicConversion === true,
    hemisphereResolved: value.hemisphereResolved === true,
    pointCount: normalizedCoordinates.length,
    normalizedCoordinates: Object.freeze(normalizedCoordinates),
    affectsLegacyWinner: false,
    affectsCoordinateResult: false,
    affectsKml: false
  });
}

function compareCoordinateInterpretations(legacy = null, evidence = null, tolerance = 0.000001) {
  const legacyInterpretation = normalizeInterpretation(legacy);
  const evidenceInterpretation = normalizeInterpretation(evidence);
  if (!legacyInterpretation || !evidenceInterpretation) {
    return Object.freeze({
      available: false,
      comparisonTolerance: tolerance,
      wouldChangeCoordinateValues: false,
      numericDisagreement: false,
      hemisphereDisagreement: false,
      maxCoordinateDelta: null,
      pointMismatchCount: 0,
      pointLevelDiff: Object.freeze([])
    });
  }

  const count = Math.min(
    legacyInterpretation.normalizedCoordinates.length,
    evidenceInterpretation.normalizedCoordinates.length
  );
  const pointLevelDiff = [];
  let maxCoordinateDelta = 0;
  let pointMismatchCount = 0;
  let hemisphereDisagreement = false;

  for (let index = 0; index < count; index += 1) {
    const legacyPoint = legacyInterpretation.normalizedCoordinates[index];
    const evidencePoint = evidenceInterpretation.normalizedCoordinates[index];
    const latDelta = Number(Math.abs(legacyPoint.lat - evidencePoint.lat).toFixed(9));
    const lonDelta = Number(Math.abs(legacyPoint.lon - evidencePoint.lon).toFixed(9));
    const pointHemisphereDisagreement = Math.sign(legacyPoint.lat) !== Math.sign(evidencePoint.lat)
      || Math.sign(legacyPoint.lon) !== Math.sign(evidencePoint.lon);
    const numericMismatch = latDelta > tolerance || lonDelta > tolerance;
    if (numericMismatch) pointMismatchCount += 1;
    if (pointHemisphereDisagreement) hemisphereDisagreement = true;
    maxCoordinateDelta = Math.max(maxCoordinateDelta, latDelta, lonDelta);
    pointLevelDiff.push(Object.freeze({
      point: evidencePoint.point || legacyPoint.point || String(index + 1),
      legacy: Object.freeze({
        lat: legacyPoint.lat,
        lon: legacyPoint.lon
      }),
      evidence: Object.freeze({
        lat: evidencePoint.lat,
        lon: evidencePoint.lon
      }),
      latDelta,
      lonDelta,
      hemisphereDisagreement: pointHemisphereDisagreement,
      numericMismatch
    }));
  }

  if (legacyInterpretation.normalizedCoordinates.length !== evidenceInterpretation.normalizedCoordinates.length) {
    pointMismatchCount += Math.abs(
      legacyInterpretation.normalizedCoordinates.length - evidenceInterpretation.normalizedCoordinates.length
    );
  }

  const numericDisagreement = pointMismatchCount > 0;

  return Object.freeze({
    available: true,
    comparisonTolerance: tolerance,
    wouldChangeCoordinateValues: numericDisagreement || hemisphereDisagreement,
    numericDisagreement,
    hemisphereDisagreement,
    maxCoordinateDelta: Number(maxCoordinateDelta.toFixed(9)),
    pointMismatchCount,
    pointLevelDiff: Object.freeze(pointLevelDiff)
  });
}

function findWinnerCandidate(candidates = [], winnerEvidenceType = "") {
  return candidates.find(candidate => candidate.evidenceType === winnerEvidenceType) || null;
}

function inferInterpretation(winnerEvidenceType = "", winnerCandidate = {}, legacySnapshot = {}) {
  const mapped = EVIDENCE_INTERPRETATION[winnerEvidenceType];
  if (mapped) return mapped;
  return Object.freeze({
    coordinateType: nullableString(winnerCandidate.proposedCoordinateType)
      || nullableString(legacySnapshot.coordinateType)
      || winnerEvidenceType
      || null,
    precisionMode: nullableString(winnerCandidate.proposedPrecisionMode)
      || nullableString(legacySnapshot.precisionMode),
    recommendedAction: "manual_review"
  });
}

function sameLegacyInterpretation(legacySnapshot = {}, proposed = {}) {
  return Boolean(
    proposed.proposedCoordinateType
    && legacySnapshot.coordinateType
    && proposed.proposedCoordinateType === legacySnapshot.coordinateType
    && (
      !proposed.proposedPrecisionMode
      || !legacySnapshot.precisionMode
      || proposed.proposedPrecisionMode === legacySnapshot.precisionMode
    )
  );
}

function isPendingPolicy(input = {}) {
  const category = cleanString(input.category || input.policy?.category || input.fixture?.fixtureStatus);
  const fixtureStatus = cleanString(input.fixture?.fixtureStatus || input.fixtureStatus);
  return input.pendingPolicy === true
    || input.indonesiaPending === true
    || PENDING_CATEGORY_PATTERN.test(category)
    || /pending/i.test(fixtureStatus);
}

function buildShadowWinnerSummary(shadowDecision = {}, winnerCandidate = {}) {
  return Object.freeze({
    evidenceType: nullableString(shadowDecision.winner || winnerCandidate.evidenceType),
    authorityLevel: shadowDecision.authority ?? winnerCandidate.authority ?? null,
    authorityCategory: nullableString(winnerCandidate.authorityCategory),
    confidence: nullableString(winnerCandidate.confidence),
    reason: nullableString(shadowDecision.reason || winnerCandidate.reason)
  });
}

function classifyProposal({
  pendingPolicy,
  shadowDecision,
  winnerCandidate,
  legacySnapshot,
  proposal,
  flags
}) {
  if (pendingPolicy) {
    return EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.BLOCKED_PENDING_POLICY;
  }
  if (!shadowDecision.winner || !winnerCandidate) {
    return EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.NO_PROPOSAL;
  }
  if (
    sameLegacyInterpretation(legacySnapshot, proposal)
    && proposal.coordinateComparison?.wouldChangeCoordinateValues === true
  ) {
    return EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.TYPE_AGREEMENT_WITH_COORDINATE_DISAGREEMENT;
  }
  if (sameLegacyInterpretation(legacySnapshot, proposal)) {
    return EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.AGREEMENT;
  }
  if (winnerCandidate.canGenerateKml === false && flags.kmlGate === true) {
    return EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.BLOCKED_KML_GATE;
  }
  return EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.REVIEW_REQUIRED;
}

function buildBlockReasons({
  classification,
  flags,
  proposal,
  winnerCandidate,
  shadowDecision
}) {
  const reasons = [];
  if (classification === EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.NO_PROPOSAL) {
    reasons.push("shadow_candidate_unavailable");
  }
  if (classification === EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.BLOCKED_PENDING_POLICY) {
    reasons.push("pending_fixture_policy");
  }
  if (classification === EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.REVIEW_REQUIRED) {
    reasons.push("manual_review_required");
  }
  if (classification === EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.TYPE_AGREEMENT_WITH_COORDINATE_DISAGREEMENT) {
    reasons.push("coordinate_value_disagreement");
    if (proposal.coordinateComparison?.hemisphereDisagreement === true) {
      reasons.push("hemisphere_disagreement");
    }
  }
  if (classification === EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.BLOCKED_KML_GATE) {
    reasons.push("kml_safety_gate_blocked");
  }
  if (flags.migration !== true) {
    reasons.push("migration_flag_disabled");
  }
  if (flags.kmlGate !== true) {
    reasons.push("kml_gate_disabled");
  }
  if (proposal.wouldChangeLegacy === true && !shadowDecision.reason) {
    reasons.push("difference_reason_missing");
  }
  if (winnerCandidate?.canGenerateKml === false) {
    reasons.push("winner_cannot_generate_kml");
  }
  return Object.freeze([...new Set(reasons)]);
}

export function createEvidenceArbitrationFlags(input = {}) {
  return normalizeFlags(input);
}

export function buildEvidenceArbitrationProposal(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const flags = normalizeFlags(input.flags || source.flags || {});
  const mode = proposalMode(flags);
  const legacySnapshot = createLegacySnapshot(
    input.legacySnapshot
    || source.legacySnapshot
    || source
  );
  const legacyInterpretation = legacyCoordinateInterpretation(
    input.legacySnapshot
    || source.legacySnapshot
    || source
  );
  const candidates = normalizeCandidateList(
    input.candidates
    || source.coordinateEvidenceCandidates
    || source.candidates
    || []
  );
  const shadowDecision = summarizeShadowDecision(
    input.shadowDecision
    || source.shadowEvidenceDecision
    || source.shadowDecision
    || {}
  );
  const winnerCandidate = findWinnerCandidate(candidates, shadowDecision.winner);
  const interpretation = inferInterpretation(shadowDecision.winner, winnerCandidate || {}, legacySnapshot);
  const pendingPolicy = isPendingPolicy({
    ...source,
    ...input,
    fixture: input.fixture || source.fixture || {}
  });
  const proposalCore = {
    proposedCoordinateType: nullableString(input.proposal?.proposedCoordinateType)
      || interpretation.coordinateType
      || null,
    proposedPrecisionMode: nullableString(input.proposal?.proposedPrecisionMode)
      || interpretation.precisionMode
      || null,
    recommendedAction: nullableString(input.proposal?.recommendedAction)
      || interpretation.recommendedAction
      || "manual_review"
  };
  const proposalWithDiff = {
    ...proposalCore,
    coordinateComparison: compareCoordinateInterpretations(
      legacyInterpretation,
      winnerCandidate?.coordinateInterpretation,
      Number(input.comparisonTolerance || source.comparisonTolerance || 0.000001)
    ),
    wouldChangeLegacy: !pendingPolicy && Boolean(
      shadowDecision.winner
      && (
        proposalCore.proposedCoordinateType !== legacySnapshot.coordinateType
        || (
          proposalCore.proposedPrecisionMode
          && legacySnapshot.precisionMode
          && proposalCore.proposedPrecisionMode !== legacySnapshot.precisionMode
        )
      )
    )
  };
  const classification = classifyProposal({
    pendingPolicy,
    shadowDecision,
    winnerCandidate,
    legacySnapshot,
    proposal: proposalWithDiff,
    flags
  });
  const blockReasons = buildBlockReasons({
    classification,
    flags,
    proposal: proposalWithDiff,
    winnerCandidate,
    shadowDecision
  });

  return Object.freeze({
    schemaVersion: EVIDENCE_ARBITRATION_PROPOSAL_SCHEMA_VERSION,
    enabled: flags.dryRun === true || flags.reviewOnly === true || flags.migration === true,
    mode,
    flags,
    shadowWinner: buildShadowWinnerSummary(shadowDecision, winnerCandidate || {}),
    legacySnapshot,
    legacyCoordinateInterpretation: legacyInterpretation,
    proposal: Object.freeze({
      classification,
      wouldChangeLegacy: proposalWithDiff.wouldChangeLegacy,
      wouldChangeCoordinateValues: proposalWithDiff.coordinateComparison.wouldChangeCoordinateValues,
      numericDisagreement: proposalWithDiff.coordinateComparison.numericDisagreement,
      hemisphereDisagreement: proposalWithDiff.coordinateComparison.hemisphereDisagreement,
      maxCoordinateDelta: proposalWithDiff.coordinateComparison.maxCoordinateDelta,
      pointMismatchCount: proposalWithDiff.coordinateComparison.pointMismatchCount,
      comparisonTolerance: proposalWithDiff.coordinateComparison.comparisonTolerance,
      coordinateComparison: proposalWithDiff.coordinateComparison,
      proposedCoordinateType: proposalWithDiff.proposedCoordinateType,
      proposedPrecisionMode: proposalWithDiff.proposedPrecisionMode,
      recommendedAction: proposalWithDiff.recommendedAction,
      blockReasons
    }),
    safety: Object.freeze({
      rollbackSafe: true,
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false
    })
  });
}
