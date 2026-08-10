import {
  AUTHORITY_CATEGORY,
  COORDINATE_EVIDENCE_RECOMMENDED_STATE,
  createCoordinateEvidenceCandidate
} from "../schema.js";

function getRows(value = {}) {
  return Array.isArray(value.cadastralGrid?.rows) ? value.cadastralGrid.rows : [];
}

export function buildStructuredCadastralEvidenceCandidate(value = {}) {
  const rows = getRows(value);
  const isCadastral = Boolean(
    value.cadastralGrid?.isCadastralGrid
    || value.precisionMode === "cadastral-grid-num-xv-yv"
  );

  if (!isCadastral) return null;

  const pointCount = Number(value.cadastralGrid?.rowCount || rows.length || value.pointCount || 0);

  return createCoordinateEvidenceCandidate({
    evidenceType: "structured_cadastral_table",
    sourceParser: "cadastral_grid_parser",
    coordinateSource: "num_xv_yv_table",
    authority: {
      level: 5,
      category: AUTHORITY_CATEGORY.EXPLICIT_LEGAL_COORDINATE,
      reason: "structured_cadastral_table"
    },
    confidence: {
      level: pointCount >= 4 ? "high" : "medium",
      reason: pointCount >= 4 ? "valid_num_xv_yv_rows" : "cadastral_table_detected"
    },
    attributes: {
      hasExplicitHemisphere: false,
      hasExplicitCoordinateOrder: true,
      hasStructuredTable: true,
      crsEvidence: false,
      transformVerified: false,
      geometryValid: pointCount >= 3,
      hemisphereAmbiguous: false,
      coordinateOrderAmbiguous: false
    },
    coordinateSummary: {
      pointCount,
      geometryType: pointCount > 2 ? "polygon" : "cadastral_table",
      groupCount: pointCount > 0 ? 1 : 0
    },
    conflicts: Array.isArray(value.conflicts) ? value.conflicts : [],
    recommendedState: pointCount >= 3
      ? COORDINATE_EVIDENCE_RECOMMENDED_STATE.CONFIRM_REQUIRED
      : COORDINATE_EVIDENCE_RECOMMENDED_STATE.BLOCKED_REVIEW,
    canGenerateKml: false,
    reason: "structured_cadastral_table"
  });
}
