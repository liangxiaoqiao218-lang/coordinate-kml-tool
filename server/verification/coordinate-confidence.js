import { parseDmsRows } from "./dms-utils.js";

function clamp(value) {
  return Math.max(0, Math.min(1, Number(value) || 0));
}

function weightedScore(factors) {
  const totalWeight = factors.reduce((sum, factor) => sum + factor.weight, 0);
  if (!totalWeight) return 0;
  return clamp(factors.reduce((sum, factor) => sum + (clamp(factor.score) * factor.weight), 0) / totalWeight);
}

function hasNumericValue(value) {
  return value !== null && value !== undefined && value !== "" && Number.isFinite(Number(value));
}

function pointHasMachineReadableValue(point = {}) {
  const hasWgs84 = hasNumericValue(point.lat) && hasNumericValue(point.lon);
  const hasProjected = hasNumericValue(point.x) && hasNumericValue(point.y);
  const hasGrid = Boolean(point.grid_cell);
  const hasDms = parseDmsRows(point.raw || "").some(row => row.fields.latitude && row.fields.longitude);
  return hasWgs84 || hasProjected || hasGrid || hasDms;
}

function getPointRangeScore(point = {}) {
  if (hasNumericValue(point.lat) && hasNumericValue(point.lon)) {
    return Number(point.lat) >= -90 && Number(point.lat) <= 90 && Number(point.lon) >= -180 && Number(point.lon) <= 180 ? 1 : 0;
  }
  const dmsRows = parseDmsRows(point.raw || "");
  if (dmsRows.length) return dmsRows.every(row => row.tokens.every(token => token.valid)) ? 1 : 0;
  if (hasNumericValue(point.x) && hasNumericValue(point.y)) return 0.85;
  if (point.grid_cell) return 0.85;
  return 0;
}

function getLabelCompleteness(points) {
  const labels = points.map(point => Number.parseInt(point.label, 10));
  if (!labels.length || labels.some(label => !Number.isInteger(label))) return points.length > 0 ? 0.9 : 0;
  const sorted = [...new Set(labels)].sort((a, b) => a - b);
  if (sorted.length !== labels.length) return 0.4;
  return sorted.every((label, index) => index === 0 || label === sorted[index - 1] + 1) ? 1 : 0.6;
}

function buildCharacterVerificationScores(value, fieldHasConflict) {
  return Array.from(String(value || "")).map((character, index) => ({
    index,
    character,
    verification_score: fieldHasConflict ? 0.55 : 0.98
  }));
}

export function calculateCoordinateVerificationScore({ coordinateEngineV2 = {}, conflicts = [], geometryWarnings = [] } = {}) {
  const groups = Array.isArray(coordinateEngineV2.groups) ? coordinateEngineV2.groups : [];
  const highGeometry = geometryWarnings.some(warning => warning.severity === "high");
  const mediumGeometry = geometryWarnings.some(warning => warning.severity === "medium");
  let globalPointIndex = 0;
  const groupResults = groups.map((group, groupIndex) => {
    const points = Array.isArray(group.points) ? group.points : [];
    const parserScore = points.length > 0
      ? points.filter(pointHasMachineReadableValue).length / points.length
      : 0;
    const pointCountScore = getLabelCompleteness(points);
    const pointResults = points.map((point, pointIndex) => {
      const pointId = String(point.label || pointIndex + 1);
      const currentGlobalPointIndex = globalPointIndex;
      globalPointIndex += 1;
      const pointConflicts = conflicts.filter(conflict => Number.isInteger(conflict.point_index)
        ? conflict.point_index === currentGlobalPointIndex
        : String(conflict.point_id) === pointId);
      const dmsRow = parseDmsRows(point.raw || "")[0];
      const rangeScore = getPointRangeScore(point);
      const formatScore = dmsRow
        ? (dmsRow.fields.latitude && dmsRow.fields.longitude && dmsRow.tokens.every(token => token.valid) ? 1 : 0.35)
        : (pointHasMachineReadableValue(point) ? 1 : 0);
      const conflictScore = pointConflicts.some(conflict => conflict.severity === "high")
        ? 0.15
        : pointConflicts.length > 0 ? 0.55 : 1;
      const verificationScore = weightedScore([
        { score: pointHasMachineReadableValue(point) ? 1 : 0, weight: 0.3 },
        { score: formatScore, weight: 0.25 },
        { score: rangeScore, weight: 0.2 },
        { score: conflictScore, weight: 0.25 }
      ]);
      const fields = {};
      if (dmsRow) {
        ["latitude", "longitude"].forEach(field => {
          const token = dmsRow.fields[field];
          if (!token) return;
          const fieldConflicts = pointConflicts.filter(conflict => conflict.field.startsWith(`${field}.`));
          const fieldVerificationScore = fieldConflicts.some(conflict => conflict.severity === "high")
            ? 0.35
            : fieldConflicts.length > 0 ? 0.65 : Math.min(0.99, verificationScore + 0.05);
          fields[field] = {
            value: token.raw,
            verification_score: Number(fieldVerificationScore.toFixed(2)),
            character_scores: buildCharacterVerificationScores(token.raw, fieldConflicts.length > 0)
          };
        });
      }

      return {
        point_id: pointId,
        verification_score: Number(verificationScore.toFixed(2)),
        fields,
        factors: {
          parser_success: pointHasMachineReadableValue(point) ? 1 : 0,
          format_valid: formatScore,
          coordinate_range: rangeScore,
          conflict_free: conflictScore
        }
      };
    });
    const averagePointScore = pointResults.length
      ? pointResults.reduce((sum, point) => sum + point.verification_score, 0) / pointResults.length
      : 0;
    const geometryScore = highGeometry ? 0 : mediumGeometry ? 0.55 : 1;
    const conflictScore = conflicts.some(conflict => conflict.severity === "high")
      ? 0.15
      : conflicts.length > 0 ? 0.6 : 1;
    const verificationScore = weightedScore([
      { score: parserScore, weight: 0.25 },
      { score: averagePointScore, weight: 0.25 },
      { score: pointCountScore, weight: 0.15 },
      { score: geometryScore, weight: 0.2 },
      { score: conflictScore, weight: 0.15 }
    ]);

    return {
      group_id: group.group_id || `group_${groupIndex + 1}`,
      verification_score: Number(verificationScore.toFixed(2)),
      points: pointResults,
      factors: {
        parser_success: Number(parserScore.toFixed(2)),
        point_count_complete: Number(pointCountScore.toFixed(2)),
        geometry_reasonable: geometryScore,
        conflict_free: conflictScore
      }
    };
  });

  const verificationScore = groupResults.length
    ? groupResults.reduce((sum, group) => sum + group.verification_score, 0) / groupResults.length
    : 0;

  return {
    verification_score: Number(verificationScore.toFixed(2)),
    groups: groupResults
  };
}
