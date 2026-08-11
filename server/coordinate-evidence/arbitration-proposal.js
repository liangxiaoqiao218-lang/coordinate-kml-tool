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
      canGenerateKml: candidate.canGenerateKml === true
        ? true
        : candidate.canGenerateKml === false
          ? false
          : null
    }))
    .filter(candidate => candidate.evidenceType));
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
    proposal: Object.freeze({
      classification,
      wouldChangeLegacy: proposalWithDiff.wouldChangeLegacy,
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
