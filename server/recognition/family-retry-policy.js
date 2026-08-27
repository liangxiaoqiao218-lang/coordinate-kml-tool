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
