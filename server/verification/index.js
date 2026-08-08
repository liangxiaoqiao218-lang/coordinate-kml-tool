import { CONFLICT_DETECTOR_SUPPORTED_SCOPE, detectCoordinateConflicts } from "./conflict-detector.js";
import { calculateCoordinateVerificationScore } from "./coordinate-confidence.js";
import { validateCoordinateGeometry } from "./geo-validator.js";
import {
  attachEvidenceToVerificationGroups,
  buildRecognitionEvidence
} from "../evidence/recognition-evidence-adapter.js";
import { buildEvidenceAcquisition } from "../evidence-acquisition/index.js";
import { finalizeCoordinateResponse } from "../coordinate-type/response-finalizer.js";

function uniqueWarnings(values) {
  return Array.from(new Set(values.map(value => String(value || "").trim()).filter(Boolean)));
}

function hasNumericValue(value) {
  return value !== null && value !== undefined && value !== "" && Number.isFinite(Number(value));
}

function hasWgs84Point(point = {}) {
  return hasNumericValue(point.lat)
    && hasNumericValue(point.lon)
    && Number(point.lat) >= -90
    && Number(point.lat) <= 90
    && Number(point.lon) >= -180
    && Number(point.lon) <= 180;
}

function getProjectedValidationState(recognitionResult = {}, coordinateEngineV2 = {}) {
  const groups = Array.isArray(coordinateEngineV2.groups) ? coordinateEngineV2.groups : [];
  const points = groups.flatMap(group => Array.isArray(group.points) ? group.points : []);
  const typeEvidence = [
    coordinateEngineV2.coordinate_type,
    coordinateEngineV2.precision_mode,
    recognitionResult.precisionMode,
    recognitionResult.projection
  ].filter(Boolean).join(" ").toLowerCase();
  const projectedByType = /utm|bftm|kyrgyz|gauss|projected|cadastral|\bx[\s/_-]*y\b/.test(typeEvidence);
  const projectedPoints = points.filter(point => (
    hasNumericValue(point.x)
    || hasNumericValue(point.y)
    || Boolean(point.projection)
    || Boolean(point.grid_cell)
  ));
  const hasProjectedData = projectedByType || projectedPoints.length > 0;

  if (!hasProjectedData) {
    return {
      validation_scope: "coordinate_and_geometry",
      geometry_validation: "EVALUATED",
      projected_validation_complete: true
    };
  }

  const crsHint = String(recognitionResult.projection || "").trim();
  const crsConfirmed = projectedPoints.length > 0
    && projectedPoints.every(point => Boolean(String(point.projection || crsHint).trim()));
  const wgs84Converted = points.length > 0 && points.every(hasWgs84Point);
  const geometryValidated = groups.length > 0 && groups.every(group => group.validation?.status === "scored");
  const projectedValidationComplete = crsConfirmed && wgs84Converted && geometryValidated;

  return projectedValidationComplete
    ? {
        validation_scope: "coordinate_and_geometry",
        geometry_validation: "EVALUATED",
        projected_validation_complete: true
      }
    : {
        validation_scope: "format_only",
        geometry_validation: "NOT_EVALUATED",
        projected_validation_complete: false
      };
}

export function buildCoordinateVerification({ recognitionResult = {}, coordinateEngineV2 = {}, evidence = null } = {}) {
  const evidenceLayer = evidence || buildRecognitionEvidence({ recognitionResult, coordinateEngineV2 });
  const conflicts = detectCoordinateConflicts({ recognitionResult, coordinateEngineV2, evidence: evidenceLayer });
  const geometryWarnings = validateCoordinateGeometry({ recognitionResult, coordinateEngineV2 });
  const scoreResult = calculateCoordinateVerificationScore({
    recognitionResult,
    coordinateEngineV2,
    conflicts,
    geometryWarnings
  });
  const groups = Array.isArray(coordinateEngineV2.groups) ? coordinateEngineV2.groups : [];
  const missingResult = groups.length === 0 || groups.every(group => !Array.isArray(group.points) || group.points.length === 0);
  const highConflict = conflicts.some(conflict => conflict.severity === "high");
  const highGeometry = geometryWarnings.some(warning => warning.severity === "high");
  const mediumEvidence = conflicts.length > 0 || geometryWarnings.length > 0;
  const engineReview = Boolean(coordinateEngineV2.requires_review || groups.some(group => group.requires_review));
  const validationState = getProjectedValidationState(recognitionResult, coordinateEngineV2);
  let status = "PASS";

  if (missingResult || highConflict || highGeometry) {
    status = "BLOCK";
  } else if (!validationState.projected_validation_complete || mediumEvidence || engineReview || scoreResult.verification_score < 0.85) {
    status = "REVIEW";
  }

  const warnings = uniqueWarnings([
    ...(Array.isArray(coordinateEngineV2.warnings) ? coordinateEngineV2.warnings : []),
    ...conflicts.map(conflict => `${conflict.point_id || "point"} ${conflict.field} 存在候选冲突。`),
    ...geometryWarnings.map(warning => warning.message),
    ...(!validationState.projected_validation_complete ? ["Projected coordinates have format-only verification; CRS, WGS84 conversion, and geometry validation are not complete."] : [])
  ]);

  return {
    schema_version: "coordinate_verification_v1",
    status,
    verification_score: scoreResult.verification_score,
    score_method: "rule_based_v1",
    score_calibrated: false,
    validation_scope: validationState.validation_scope,
    geometry_validation: validationState.geometry_validation,
    supported_scope: [...CONFLICT_DETECTOR_SUPPORTED_SCOPE],
    warnings,
    conflicts,
    geometryWarnings,
    groups: attachEvidenceToVerificationGroups(scoreResult.groups, evidenceLayer),
    evidence_schema_version: evidenceLayer.schema_version,
    shadow_only: true,
    affects_coordinates: false,
    affects_kml: false
  };
}

export function buildCoordinateVerificationResponse(payload = {}, coordinateEngineV2 = null) {
  const engine = coordinateEngineV2 || payload.coordinateEngineV2 || {};
  const evidenceAcquisition = buildEvidenceAcquisition({
    recognitionResult: payload,
    coordinateEngineV2: engine
  });
  const evidence = buildRecognitionEvidence({
    recognitionResult: payload,
    coordinateEngineV2: engine,
    context: { evidenceAcquisition }
  });
  return {
    ...payload,
    coordinateEngineV2: engine,
    evidenceAcquisition,
    evidence,
    verification: buildCoordinateVerification({
      recognitionResult: payload,
      coordinateEngineV2: engine,
      evidence
    })
  };
}

export function buildFinalizedCoordinateVerificationResponse(payload = {}, coordinateEngineV2 = null) {
  const engine = coordinateEngineV2 || payload.coordinateEngineV2 || {};
  const finalizedPayload = finalizeCoordinateResponse(payload, { coordinateEngineV2: engine });
  return buildCoordinateVerificationResponse(finalizedPayload, engine);
}
