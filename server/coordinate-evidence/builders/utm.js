import {
  AUTHORITY_CATEGORY,
  COORDINATE_EVIDENCE_RECOMMENDED_STATE,
  createCoordinateEvidenceCandidate
} from "../schema.js";

function getShadowIntent(value = {}) {
  return value.utm?.crsEvidenceShadow?.shadowIntent
    || value.utm?.crsEvidence?.shadowIntent
    || value.crsEvidenceShadow?.shadowIntent
    || value.crsEvidence?.shadowIntent
    || value.shadowIntent
    || {};
}

function isValidUtmIntent(intent = {}) {
  return Boolean(
    intent
    && intent.projection === "utm"
    && intent.datum === "WGS84"
    && Number.isInteger(intent.zone)
    && (intent.hemisphere === "north" || intent.hemisphere === "south")
  );
}

function hasCrsConflict(intent = {}) {
  return Array.isArray(intent.conflicts) && intent.conflicts.length > 0;
}

function getTransformationVerification(value = {}) {
  return value.utm?.structuredUtmTable?.transformationVerification
    || value.utm?.structuredUtmTable
    || value.utm?.structuredUtmPriority?.transformationVerification
    || value.structuredUtmPriority?.transformationVerification
    || value.structuredUtmTable?.transformationVerification
    || value.structuredUtmTable
    || {};
}

function getStructuredRowCount(value = {}) {
  return Number(
    value.utm?.structuredUtmPriority?.table?.rows?.length
    || value.utm?.structuredUtmTable?.rowCount
    || value.utm?.structuredUtmTable?.table?.rows?.length
    || value.structuredUtmPriority?.table?.rows?.length
    || value.structuredUtmTable?.rowCount
    || value.rowCount
    || 0
  );
}

function isTransformPass(verification = {}) {
  return /match|passed|pass|verified/i.test(String(verification.status || verification.transformationStatus || ""));
}

export function buildUtmCrsTextEvidenceCandidate(value = {}) {
  const intent = getShadowIntent(value);
  if (!isValidUtmIntent(intent)) return null;

  return createCoordinateEvidenceCandidate({
    evidenceType: "utm_crs_text",
    sourceParser: "crs_evidence",
    coordinateSource: "map_frame_crs_label",
    authority: {
      level: 3,
      category: AUTHORITY_CATEGORY.CRS_CONTEXT,
      reason: "utm_crs_text"
    },
    confidence: {
      level: intent.confidence === "confirmed" ? "high" : "medium",
      reason: intent.confidence === "confirmed" ? "clear_utm_wgs84_zone" : "utm_crs_hint"
    },
    attributes: {
      hasExplicitHemisphere: Boolean(intent.hemisphere),
      hasExplicitCoordinateOrder: false,
      hasStructuredTable: false,
      crsEvidence: true,
      transformVerified: false,
      geometryValid: !hasCrsConflict(intent),
      hemisphereAmbiguous: false,
      coordinateOrderAmbiguous: false
    },
    coordinateSummary: {
      pointCount: 0,
      geometryType: "crs_context",
      groupCount: 0
    },
    conflicts: Array.isArray(intent.conflicts) ? intent.conflicts : [],
    recommendedState: hasCrsConflict(intent)
      ? COORDINATE_EVIDENCE_RECOMMENDED_STATE.BLOCKED_REVIEW
      : COORDINATE_EVIDENCE_RECOMMENDED_STATE.CONFIRM_REQUIRED,
    canGenerateKml: false,
    reason: "utm_crs_text"
  });
}

export function buildVerifiedUtmTransformationEvidenceCandidate(value = {}) {
  const intent = getShadowIntent(value);
  const verification = getTransformationVerification(value);
  const rowCount = getStructuredRowCount(value);
  const accepted = Boolean(
    value.utm?.structuredUtmPriority?.accepted
    || value.utm?.structuredUtmTable?.accepted
    || value.structuredUtmPriority?.accepted
    || value.structuredUtmTable?.accepted
  );

  if (!isValidUtmIntent(intent) || !accepted || rowCount <= 0 || !isTransformPass(verification)) {
    return null;
  }

  return createCoordinateEvidenceCandidate({
    evidenceType: "verified_utm_transformation",
    sourceParser: "structured_utm_table",
    coordinateSource: "structured_projected_rows",
    authority: {
      level: 4,
      category: AUTHORITY_CATEGORY.VERIFIED_TRANSFORMATION,
      reason: "verified_utm_transformation"
    },
    confidence: {
      level: "high",
      reason: "structured_rows_and_transform_match"
    },
    attributes: {
      hasExplicitHemisphere: Boolean(intent.hemisphere),
      hasExplicitCoordinateOrder: true,
      hasStructuredTable: true,
      crsEvidence: true,
      transformVerified: true,
      geometryValid: true,
      hemisphereAmbiguous: false,
      coordinateOrderAmbiguous: false
    },
    coordinateSummary: {
      pointCount: rowCount,
      geometryType: rowCount > 2 ? "polygon" : "projected_rows",
      groupCount: rowCount > 0 ? 1 : 0
    },
    conflicts: [],
    recommendedState: COORDINATE_EVIDENCE_RECOMMENDED_STATE.AUTO_EXPORT,
    canGenerateKml: true,
    reason: "verified_utm_transformation"
  });
}

export function buildUtmEvidenceCandidates(value = {}) {
  return [
    buildVerifiedUtmTransformationEvidenceCandidate(value),
    buildUtmCrsTextEvidenceCandidate(value)
  ].filter(Boolean);
}
