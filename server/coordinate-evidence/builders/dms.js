import {
  AUTHORITY_CATEGORY,
  COORDINATE_EVIDENCE_RECOMMENDED_STATE,
  createCoordinateEvidenceCandidate
} from "../schema.js";

function countEnginePoints(coordinateEngineV2 = {}) {
  return (Array.isArray(coordinateEngineV2.groups) ? coordinateEngineV2.groups : [])
    .reduce((total, group) => total + (Array.isArray(group.points) ? group.points.length : 0), 0);
}

function countGroups(coordinateEngineV2 = {}) {
  return Array.isArray(coordinateEngineV2.groups) ? coordinateEngineV2.groups.length : 0;
}

function getGeometryType(coordinateEngineV2 = {}, fallback = "unknown") {
  const groups = Array.isArray(coordinateEngineV2.groups) ? coordinateEngineV2.groups : [];
  return String(groups[0]?.geometry || fallback || "unknown");
}

function hasExplicitDmsHemisphereEvidence(value = {}) {
  return Boolean(
    value.hasExplicitHemisphere
    || value.hasExplicitCoordinateOrder
    || value.dmsAccepted
    || value.dmsGroupedAccepted
    || value.pointAzDmsTableAccepted
    || value.frenchPerimeterDms?.isFrenchPerimeterDms
    || value.coordinateEngineV2?.coordinate_type === "cote_divoire_geographic_dms_table"
    || /dms|latitude|longitude|nord|ouest|south|north|east|west|longitude_e|latitude_s/i.test(String(value.reason || value.sourceHint || ""))
  );
}

function buildConfidence({ handwritten = false, explicit = false, requiresReview = false } = {}) {
  if (handwritten || requiresReview) {
    return {
      level: "medium",
      reason: handwritten ? "handwritten_dms_requires_user_confirmation" : "dms_requires_review"
    };
  }
  return {
    level: explicit ? "high" : "medium",
    reason: explicit ? "explicit_hemisphere_and_order" : "dms_detected_without_full_context"
  };
}

function buildCoordinateSummary(value = {}) {
  const coordinateEngineV2 = value.coordinateEngineV2 || {};
  const pointCount = Number(value.pointCount || countEnginePoints(coordinateEngineV2) || value.handwrittenDms?.pointRows || 0);
  return {
    pointCount: Number.isFinite(pointCount) ? pointCount : 0,
    geometryType: value.geometryType || getGeometryType(coordinateEngineV2, pointCount > 2 ? "polygon" : "unknown"),
    groupCount: Number(value.groupCount || countGroups(coordinateEngineV2) || 0)
  };
}

export function buildDmsGeographicEvidenceCandidate(value = {}) {
  const coordinateEngineV2 = value.coordinateEngineV2 || {};
  const isCoteDIvoire = coordinateEngineV2.coordinate_type === "cote_divoire_geographic_dms_table";
  const isHandwritten = Boolean(value.handwrittenDms?.isHandwrittenDms || value.precisionMode === "handwritten-dms-coordinates");
  const hasDms = Boolean(
    value.dmsAccepted
    || value.dmsGroupedAccepted
    || value.pointAzDmsTableAccepted
    || value.frenchPerimeterDms?.isFrenchPerimeterDms
    || isHandwritten
    || isCoteDIvoire
    || /dms/i.test(String(value.precisionMode || coordinateEngineV2.precision_mode || ""))
  );

  if (!hasDms) return null;

  const explicit = hasExplicitDmsHemisphereEvidence(value);
  const requiresReview = Boolean(value.requires_review || coordinateEngineV2.requires_review || isHandwritten);
  const evidenceType = isCoteDIvoire ? "explicit_geographic_dms" : "dms_geographic";
  const sourceParser = isHandwritten
    ? "handwritten_dms_parser"
    : isCoteDIvoire
      ? "cote_divoire_dms_parser"
      : "dms_parser";

  return createCoordinateEvidenceCandidate({
    evidenceType,
    sourceParser,
    coordinateSource: explicit ? "explicit_lat_lon" : "dms_rows",
    authority: {
      level: 5,
      category: AUTHORITY_CATEGORY.EXPLICIT_LEGAL_COORDINATE,
      reason: explicit ? "explicit_dms_with_hemisphere" : "dms_coordinate_source"
    },
    confidence: buildConfidence({ handwritten: isHandwritten, explicit, requiresReview }),
    attributes: {
      hasExplicitHemisphere: explicit,
      hasExplicitCoordinateOrder: explicit,
      hasStructuredTable: Boolean(value.dmsGroupedAccepted || value.pointAzDmsTableAccepted || isCoteDIvoire),
      crsEvidence: false,
      transformVerified: false,
      geometryValid: value.geometryValid === false ? false : true,
      hemisphereAmbiguous: false,
      coordinateOrderAmbiguous: false
    },
    coordinateSummary: buildCoordinateSummary(value),
    conflicts: Array.isArray(value.conflicts) ? value.conflicts : [],
    recommendedState: requiresReview
      ? COORDINATE_EVIDENCE_RECOMMENDED_STATE.CONFIRM_REQUIRED
      : COORDINATE_EVIDENCE_RECOMMENDED_STATE.AUTO_EXPORT,
    canGenerateKml: !requiresReview && value.geometryValid !== false,
    reason: isHandwritten
      ? "handwritten_dms_coordinate_source"
      : explicit
        ? "explicit_dms_with_hemisphere"
        : "dms_coordinate_source"
  });
}
