export const RETRY_OWNER_FAMILY = Object.freeze({
  HANDWRITTEN_DMS: "handwritten_dms",
  POINT_AZ_DMS_TABLE: "point_az_dms_table"
});

export function authorizeFamilyRetryDispatch({ activeFamilyOwner, targetOwner } = {}) {
  const activeOwner = String(activeFamilyOwner || "").trim();
  const requestedOwner = String(targetOwner || "").trim();
  if (!requestedOwner) return Object.freeze({ allowed: false, reason: "target_owner_required" });
  if (!activeOwner) return Object.freeze({ allowed: true, reason: "family_owner_not_locked" });
  if (activeOwner === requestedOwner) return Object.freeze({ allowed: true, reason: "same_family_owner" });
  return Object.freeze({ allowed: false, reason: "cross_family_transition_not_authorized" });
}

export function hasPointAzHeadingEvidence(text) {
  const normalized = String(text || "").normalize("NFKD").replace(/[\u0300-\u036f]/g, "");
  return /\bpoint\b/i.test(normalized)
    && /\b(?:nord|latitude)\b/i.test(normalized)
    && /\b(?:est|ouest|longitude)\b/i.test(normalized);
}

export function hasPointAzSourceOrderContinuity(labels) {
  if (!Array.isArray(labels) || labels.length < 3) return false;
  const ordinals = labels.map(label => {
    const normalized = String(label || "").trim().toUpperCase();
    return /^[A-Z]$/.test(normalized) ? normalized.charCodeAt(0) - 64 : NaN;
  });
  if (ordinals.some(value => !Number.isInteger(value))) return false;
  if (new Set(ordinals).size !== ordinals.length) return false;
  return ordinals.every((value, index) => index === 0 || value === ordinals[index - 1] + 1);
}

export function canAuthorizePointAzRetry({ ownerFamily, typedPointAzEvidence } = {}) {
  return ownerFamily === RETRY_OWNER_FAMILY.POINT_AZ_DMS_TABLE
    && typedPointAzEvidence?.established === true
    && typedPointAzEvidence?.type === "POINT_AZ_LABELED_TABLE_EVIDENCE"
    && typedPointAzEvidence?.headingSemantics === true
    && typedPointAzEvidence?.orderedLabelContinuity === true;
}

export const FAMILY_RETRY_STAGE_NAMES = Object.freeze(new Set([
  "family_retry",
  "wgs84_primary",
  "handwritten_retry",
  "wgs84_retry",
  "mgrs_retry",
  "kyrgyz_retry",
  "cadastral_retry",
  "cadastral_layout",
  "cadastral_grid",
  "cote_divoire_retry"
]));

export function isFamilyRetryAllowed({ stageName, familyEvidence = false } = {}) {
  if (!FAMILY_RETRY_STAGE_NAMES.has(stageName)) return true;
  return familyEvidence === true;
}
