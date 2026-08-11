import {
  EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION,
  buildEvidenceArbitrationProposal
} from "./arbitration-proposal.js";
import { createLegacySnapshot } from "./shadow-observation.js";

export const EVIDENCE_ARBITRATION_DRY_RUN_DIFF_SCHEMA_VERSION =
  "evidence_arbitration_dry_run_diff_v1";

export const EVIDENCE_ARBITRATION_DRY_RUN_CLASSIFICATION = Object.freeze({
  AGREEMENT: "AGREEMENT",
  REVIEW_REQUIRED: "REVIEW_REQUIRED",
  BLOCKED: "BLOCKED",
  NO_CHANGE: "NO_CHANGE",
  NO_PROPOSAL: "NO_PROPOSAL"
});

const SECRET_VALUE_PATTERN =
  /(sk-[a-z0-9_-]{8,}|dashscope[_-]?[a-z0-9_-]*|supabase[_-]?[a-z0-9_-]*|bearer\s+[a-z0-9._-]+|api[_-]?key\s*[:=]|secret\s*[:=]|token\s*[:=]|password\s*[:=]|authorization\s*[:=])/ig;

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

function normalizeBoolean(value, fallback = false) {
  return value === true || value === false ? value : fallback;
}

function normalizeLegacy(source = {}) {
  return createLegacySnapshot(source);
}

function normalizeProposal(input = {}) {
  if (input?.schemaVersion === "evidence_arbitration_proposal_v1") {
    return input;
  }
  return buildEvidenceArbitrationProposal(input);
}

function summarizeProposal(proposal = {}) {
  return Object.freeze({
    classification: nullableString(proposal.proposal?.classification),
    winnerEvidenceType: nullableString(proposal.shadowWinner?.evidenceType),
    winnerAuthority: proposal.shadowWinner?.authorityLevel ?? null,
    proposedCoordinateType: nullableString(proposal.proposal?.proposedCoordinateType),
    proposedPrecisionMode: nullableString(proposal.proposal?.proposedPrecisionMode),
    recommendedAction: nullableString(proposal.proposal?.recommendedAction),
    blockReasons: Object.freeze(
      Array.isArray(proposal.proposal?.blockReasons)
        ? proposal.proposal.blockReasons.map(reason => cleanString(reason)).filter(Boolean)
        : []
    )
  });
}

function wouldChangeCoordinateType(legacy = {}, proposalSummary = {}) {
  return Boolean(
    proposalSummary.proposedCoordinateType
    && legacy.coordinateType
    && proposalSummary.proposedCoordinateType !== legacy.coordinateType
  );
}

function wouldChangePrecisionMode(legacy = {}, proposalSummary = {}) {
  return Boolean(
    proposalSummary.proposedPrecisionMode
    && legacy.precisionMode
    && proposalSummary.proposedPrecisionMode !== legacy.precisionMode
  );
}

function buildChangeSummary(diff = {}, proposalSummary = {}) {
  if (proposalSummary.classification === EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.NO_PROPOSAL) {
    return "no proposal available";
  }
  if (proposalSummary.classification === EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.BLOCKED_PENDING_POLICY) {
    return "proposal blocked by pending fixture policy";
  }
  if (proposalSummary.classification === EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.BLOCKED_KML_GATE) {
    return "proposal blocked by KML safety gate";
  }
  const changes = [];
  if (diff.wouldChangeCoordinateType) changes.push("coordinateType");
  if (diff.wouldChangePrecisionMode) changes.push("precisionMode");
  if (diff.wouldChangeCoordinateResultState) changes.push("coordinateResult.state");
  if (diff.wouldChangeKml) changes.push("kml_ready");
  return changes.length
    ? `would change ${changes.join(", ")}`
    : "no production behavior change";
}

function classifyDryRun({ proposalSummary = {}, diff = {} }) {
  if (proposalSummary.classification === EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.NO_PROPOSAL) {
    return EVIDENCE_ARBITRATION_DRY_RUN_CLASSIFICATION.NO_PROPOSAL;
  }
  if (
    proposalSummary.classification === EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.BLOCKED_PENDING_POLICY
    || proposalSummary.classification === EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.BLOCKED_KML_GATE
  ) {
    return EVIDENCE_ARBITRATION_DRY_RUN_CLASSIFICATION.BLOCKED;
  }
  if (proposalSummary.classification === EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.AGREEMENT) {
    return EVIDENCE_ARBITRATION_DRY_RUN_CLASSIFICATION.AGREEMENT;
  }
  if (proposalSummary.classification === EVIDENCE_ARBITRATION_PROPOSAL_CLASSIFICATION.REVIEW_REQUIRED) {
    return EVIDENCE_ARBITRATION_DRY_RUN_CLASSIFICATION.REVIEW_REQUIRED;
  }
  return diff.wouldChangeLegacy
    ? EVIDENCE_ARBITRATION_DRY_RUN_CLASSIFICATION.REVIEW_REQUIRED
    : EVIDENCE_ARBITRATION_DRY_RUN_CLASSIFICATION.NO_CHANGE;
}

export function buildEvidenceArbitrationDryRunDiff(input = {}) {
  const source = input.response && typeof input.response === "object" ? input.response : input;
  const proposal = normalizeProposal(
    input.proposal
    || source.evidenceArbitrationProposal
    || source.proposal
    || source
  );
  const legacy = normalizeLegacy(
    input.legacySnapshot
    || proposal.legacySnapshot
    || source.legacySnapshot
    || source
  );
  const proposalSummary = summarizeProposal(proposal);
  const diffCore = {
    wouldChangeLegacy: normalizeBoolean(proposal.proposal?.wouldChangeLegacy, false),
    wouldChangeCoordinateType: wouldChangeCoordinateType(legacy, proposalSummary),
    wouldChangePrecisionMode: wouldChangePrecisionMode(legacy, proposalSummary),
    wouldChangeCoordinateResultState: false,
    wouldChangeKml: false
  };
  const diff = Object.freeze({
    ...diffCore,
    changeSummary: buildChangeSummary(diffCore, proposalSummary)
  });
  const classification = classifyDryRun({ proposalSummary, diff });

  return Object.freeze({
    schemaVersion: EVIDENCE_ARBITRATION_DRY_RUN_DIFF_SCHEMA_VERSION,
    enabled: proposal.enabled === true,
    mode: "dry_run",
    legacy,
    proposal: proposalSummary,
    diff,
    classification,
    safety: Object.freeze({
      migrationEnabled: false,
      rollbackSafe: true,
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false
    })
  });
}
