import {
  AUTHORITY_CATEGORY,
  COORDINATE_EVIDENCE_RECOMMENDED_STATE,
  createCoordinateEvidenceCandidate
} from "../schema.js";
import { buildDeterministicDmsInterpretation } from "../dms-interpreter.js";

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

function getDmsContext(value = {}) {
  return value.dms && typeof value.dms === "object" ? value.dms : {};
}

function hasExplicitDmsHemisphereEvidence(value = {}) {
  const dmsContext = getDmsContext(value);
  return Boolean(
    value.hasExplicitHemisphere
    || dmsContext.hasExplicitHemisphere
    || value.hasExplicitCoordinateOrder
    || dmsContext.hasExplicitCoordinateOrder
    || value.dmsAccepted
    || dmsContext.dmsAccepted
    || value.dmsGroupedAccepted
    || dmsContext.dmsGroupedAccepted
    || value.pointAzDmsTableAccepted
    || dmsContext.pointAzDmsTableAccepted
    || value.frenchPerimeterDms?.isFrenchPerimeterDms
    || dmsContext.frenchPerimeterDms?.isFrenchPerimeterDms
    || value.coordinateEngineV2?.coordinate_type === "cote_divoire_geographic_dms_table"
    || /dms|latitude|longitude|nord|ouest|south|north|east|west|longitude_e|latitude_s/i.test(String(value.reason || value.sourceHint || dmsContext.sourceHint || ""))
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
  const pointCount = Number(
    value.coordinateInterpretation?.pointCount
    || value.pointCount
    || countEnginePoints(coordinateEngineV2)
    || value.handwrittenDms?.pointRows
    || 0
  );
  return {
    pointCount: Number.isFinite(pointCount) ? pointCount : 0,
    geometryType: value.geometryType || getGeometryType(coordinateEngineV2, pointCount > 2 ? "polygon" : "unknown"),
    groupCount: Number(value.groupCount || countGroups(coordinateEngineV2) || 0)
  };
}

function getStructuredDmsSource(value = {}, dmsContext = {}) {
  const source = value.structuredDmsInterpretation
    || value.dmsCoordinateInterpretation
    || value.coordinateInterpretationSource
    || dmsContext.structuredDmsInterpretation
    || dmsContext.dmsCoordinateInterpretation
    || dmsContext.coordinateInterpretationSource
    || null;
  if (source && typeof source === "object") return source;
  const rows = value.dmsRows || value.structuredDmsRows || dmsContext.dmsRows || dmsContext.structuredDmsRows;
  if (Array.isArray(rows)) {
    return {
      rows,
      headerSemantics: value.headerSemantics || dmsContext.headerSemantics || {}
    };
  }
  return null;
}

function buildCoordinateInterpretation(value = {}, dmsContext = {}) {
  const existing = value.coordinateInterpretation || dmsContext.coordinateInterpretation;
  if (existing && typeof existing === "object" && existing.schemaVersion) return existing;
  const source = getStructuredDmsSource(value, dmsContext);
  return source ? buildDeterministicDmsInterpretation(source) : null;
}

export function buildDmsGeographicEvidenceCandidate(value = {}) {
  const dmsContext = getDmsContext(value);
  const coordinateEngineV2 = value.coordinateEngineV2 || {};
  const isCoteDIvoire = coordinateEngineV2.coordinate_type === "cote_divoire_geographic_dms_table";
  const handwrittenDms = value.handwrittenDms || dmsContext.handwrittenDms || {};
  const frenchPerimeterDms = value.frenchPerimeterDms || dmsContext.frenchPerimeterDms || {};
  const dmsAccepted = Boolean(value.dmsAccepted || dmsContext.dmsAccepted);
  const dmsGroupedAccepted = Boolean(value.dmsGroupedAccepted || dmsContext.dmsGroupedAccepted);
  const pointAzDmsTableAccepted = Boolean(value.pointAzDmsTableAccepted || dmsContext.pointAzDmsTableAccepted);
  const isHandwritten = Boolean(handwrittenDms.isHandwrittenDms || value.precisionMode === "handwritten-dms-coordinates");
  const coordinateInterpretation = buildCoordinateInterpretation(value, dmsContext);
  const hasDms = Boolean(
    dmsAccepted
    || dmsGroupedAccepted
    || pointAzDmsTableAccepted
    || frenchPerimeterDms.isFrenchPerimeterDms
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
      hasStructuredTable: Boolean(dmsGroupedAccepted || pointAzDmsTableAccepted || isCoteDIvoire),
      crsEvidence: false,
      transformVerified: false,
      geometryValid: value.geometryValid === false ? false : true,
      hemisphereAmbiguous: false,
      coordinateOrderAmbiguous: false
    },
    coordinateInterpretation,
    coordinateSummary: buildCoordinateSummary({ ...dmsContext, ...value, handwrittenDms, coordinateInterpretation }),
    conflicts: Array.isArray(value.conflicts) ? value.conflicts : (Array.isArray(dmsContext.conflicts) ? dmsContext.conflicts : []),
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
