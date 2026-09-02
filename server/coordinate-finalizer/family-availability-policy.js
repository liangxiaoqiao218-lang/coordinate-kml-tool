import { COORDINATE_GATE_REASON } from "./reason-codes.js";

export const FAMILY_AVAILABILITY_POLICY_ID = "coordinate_family_availability_policy";
export const FAMILY_AVAILABILITY_POLICY_VERSION = "1";

export const FAMILY_AVAILABILITY_STATUS = Object.freeze({
  AVAILABLE: "AVAILABLE",
  BLOCKED_BY_PROVIDER: "BLOCKED_BY_PROVIDER",
  TEMPORARILY_UNAVAILABLE: "TEMPORARILY_UNAVAILABLE"
});

const POLICY_EFFECTIVE_FROM = "2026-08-26";
const POLICY_EVIDENCE_REFERENCE = "SR-08G.1_PROVIDER_RELIABILITY_ADJUDICATION";

const KYRGYZ_REMOVAL_CONDITIONS = Object.freeze([
  "REPLACEMENT_OR_PROVIDER_CANDIDATE_IDENTIFIED",
  "REAL_FIXTURE_RELIABILITY_VALIDATED",
  "ROUTING_STABLE",
  "LATENCY_WITHIN_APPROVED_BUDGET",
  "TRUTH_AND_GATE_REGRESSION_PASSED",
  "RELEASE_AUTHORITY_APPROVED"
]);

const HANDWRITTEN_REMOVAL_CONDITIONS = Object.freeze([
  "USABLE_RECOGNITION_RESTORED",
  "PROVIDER_VARIANCE_ACCEPTABLE_OR_REPLACEMENT_PROVEN",
  "REVIEW_SEMANTICS_VALIDATED",
  "REAL_IMAGE_REGRESSION_PASSED",
  "RELEASE_AUTHORITY_APPROVED"
]);

const POLICY_ENTRIES = Object.freeze({
  kyrgyz_gk: Object.freeze({
    family: "kyrgyz_gk",
    status: FAMILY_AVAILABILITY_STATUS.BLOCKED_BY_PROVIDER,
    reasonCode: COORDINATE_GATE_REASON.FAMILY_BLOCKED_BY_PROVIDER,
    effectiveFrom: POLICY_EFFECTIVE_FROM,
    evidenceReference: "SR-08G_KYRGYZ_0_OF_5_TIMEOUT_DOMINANT",
    removalConditions: KYRGYZ_REMOVAL_CONDITIONS
  }),
  handwritten_dms_experimental: Object.freeze({
    family: "handwritten_dms_experimental",
    status: FAMILY_AVAILABILITY_STATUS.TEMPORARILY_UNAVAILABLE,
    reasonCode: COORDINATE_GATE_REASON.FAMILY_TEMPORARILY_UNAVAILABLE,
    effectiveFrom: POLICY_EFFECTIVE_FROM,
    evidenceReference: "SR-08G_HANDWRITTEN_0_OF_5_TIMEOUT_DOMINANT",
    removalConditions: HANDWRITTEN_REMOVAL_CONDITIONS
  })
});

const FAMILY_ALIASES = Object.freeze({
  kyrgyzstan_gk: "kyrgyz_gk",
  "kyrgyz-gk": "kyrgyz_gk",
  "kyrgyz-gk-point-x-y": "kyrgyz_gk",
  "cadastral-grid-num-xv-yv": "madagascar_cadastral_grid",
  handwritten_dms: "handwritten_dms_experimental",
  "handwritten-dms-coordinates": "handwritten_dms_experimental"
});

export const FAMILY_AVAILABILITY_POLICY = Object.freeze({
  policyId: FAMILY_AVAILABILITY_POLICY_ID,
  policyVersion: FAMILY_AVAILABILITY_POLICY_VERSION,
  effectiveFrom: POLICY_EFFECTIVE_FROM,
  evidenceReference: POLICY_EVIDENCE_REFERENCE,
  entries: POLICY_ENTRIES
});

export function normalizeAvailabilityFamily(family = "") {
  const normalized = String(family || "").trim().toLowerCase();
  return FAMILY_ALIASES[normalized] || normalized;
}

export function getFamilyAvailability(family = "") {
  const normalizedFamily = normalizeAvailabilityFamily(family);
  const entry = POLICY_ENTRIES[normalizedFamily];
  if (!entry) {
    return Object.freeze({
      policyId: FAMILY_AVAILABILITY_POLICY_ID,
      policyVersion: FAMILY_AVAILABILITY_POLICY_VERSION,
      family: normalizedFamily,
      status: FAMILY_AVAILABILITY_STATUS.AVAILABLE,
      reasonCode: null,
      effectiveFrom: POLICY_EFFECTIVE_FROM,
      evidenceReference: POLICY_EVIDENCE_REFERENCE,
      removalConditions: Object.freeze([]),
      providerCallAllowed: true,
      recognitionAvailable: true
    });
  }
  return Object.freeze({
    policyId: FAMILY_AVAILABILITY_POLICY_ID,
    policyVersion: FAMILY_AVAILABILITY_POLICY_VERSION,
    ...entry,
    providerCallAllowed: false,
    recognitionAvailable: false
  });
}

export function evaluateFamilyAvailability({ family = "", authoritativeEvidence = false } = {}) {
  const availability = getFamilyAvailability(family);
  if (authoritativeEvidence !== true || availability.status === FAMILY_AVAILABILITY_STATUS.AVAILABLE) {
    return Object.freeze({
      ...availability,
      enforced: false,
      providerCallAllowed: true
    });
  }
  return Object.freeze({
    ...availability,
    enforced: true,
    providerCallAllowed: false
  });
}

export function isFamilyAvailabilityBlocked(value = {}) {
  return value?.status === FAMILY_AVAILABILITY_STATUS.BLOCKED_BY_PROVIDER
    || value?.status === FAMILY_AVAILABILITY_STATUS.TEMPORARILY_UNAVAILABLE;
}

export function buildFamilyAvailabilityBlockedEngine({
  availability = {},
  coordinateType = "",
  precisionMode = ""
} = {}) {
  const family = String(availability.family || coordinateType || "");
  const warning = "This coordinate family is currently unavailable for authoritative recognition.";
  return {
    schema_version: "coordinate_engine_v2",
    coordinate_type: coordinateType || family,
    precision_mode: precisionMode,
    confidence: 0,
    requires_review: false,
    source: {
      image_count: 1,
      ocr_engine: "availability_policy",
      fallback_used: false
    },
    groups: [],
    warnings: [warning],
    debug: {
      matched_detectors: [family, "family_availability_policy"],
      blocked_fallbacks: [
        "provider_call",
        "generic_provider",
        "unrelated_family_retry",
        "legacy_multi_call_chain",
        "kml_generator"
      ],
      supplemental_fallbacks: []
    }
  };
}
