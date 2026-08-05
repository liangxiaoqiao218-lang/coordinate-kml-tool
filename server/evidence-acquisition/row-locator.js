import { parseDmsTokens } from "../verification/dms-utils.js";

export const ROW_LOCATION_MATCH_THRESHOLD = 0.68;

function clamp(value) {
  return Math.max(0, Math.min(1, Number(value) || 0));
}

function extractPointId(text = "") {
  return String(text).match(/^\s*(?:point\s*)?([A-Z]|\d{1,3})\s*(?:[.):-]|\||\s)/i)?.[1] || null;
}

function normalizeDigits(text = "") {
  return String(text).replace(/[^0-9]/g, "");
}

function editDistance(left = "", right = "") {
  const a = String(left);
  const b = String(right);
  const previous = Array.from({ length: b.length + 1 }, (_, index) => index);
  for (let i = 1; i <= a.length; i += 1) {
    let diagonal = previous[0];
    previous[0] = i;
    for (let j = 1; j <= b.length; j += 1) {
      const old = previous[j];
      previous[j] = Math.min(
        previous[j] + 1,
        previous[j - 1] + 1,
        diagonal + (a[i - 1] === b[j - 1] ? 0 : 1)
      );
      diagonal = old;
    }
  }
  return previous[b.length];
}

function textSimilarity(left = "", right = "") {
  const a = normalizeDigits(left);
  const b = normalizeDigits(right);
  if (!a || !b) return 0;
  return clamp(1 - (editDistance(a, b) / Math.max(a.length, b.length)));
}

function directionSet(text = "") {
  const values = new Set();
  parseDmsTokens(text).forEach(token => values.add(token.direction));
  if (values.size === 0) {
    (String(text).toUpperCase().match(/[NSEWO]/g) || []).forEach(value => values.add(value === "O" ? "W" : value));
  }
  return values;
}

function directionSimilarity(left = "", right = "") {
  const a = directionSet(left);
  const b = directionSet(right);
  if (a.size === 0 && b.size === 0) return 0.5;
  const union = new Set([...a, ...b]);
  const intersection = [...a].filter(value => b.has(value));
  return union.size ? intersection.length / union.size : 0;
}

function getObservationY(observation = {}) {
  return Array.isArray(observation.bbox) ? Number(observation.bbox[1]) : null;
}

function getOrderScore(pointIndex, pointCount, observationRank, observationCount) {
  if (pointCount <= 1 || observationCount <= 1) return 1;
  const expected = pointIndex / (pointCount - 1);
  const actual = observationRank / (observationCount - 1);
  return clamp(1 - Math.abs(expected - actual));
}

function getYContinuityScore(previousBinding, observation) {
  if (!previousBinding?.bbox || !Array.isArray(observation.bbox)) return 0.5;
  return Number(observation.bbox[1]) >= Number(previousBinding.bbox[1]) ? 1 : 0;
}

function scoreCandidate({ point, pointIndex, pointCount, groupId, observation, observationRank, observationCount, previousBinding }) {
  const pointId = String(point?.label || pointIndex + 1);
  const observationPointId = String(observation.point_id || extractPointId(observation.text) || "");
  const labelsAvailable = Boolean(pointId && observationPointId);
  const labelScore = labelsAvailable && pointId.toUpperCase() === observationPointId.toUpperCase() ? 1 : 0;
  const labelMismatchPenalty = labelsAvailable && labelScore === 0 ? 0.25 : 0;
  const tokenScore = textSimilarity(point?.raw || "", observation.text);
  const directions = directionSimilarity(point?.raw || "", observation.text);
  const groupScore = observation.group_id ? (String(observation.group_id) === String(groupId) ? 1 : 0) : 0.7;
  const orderScore = getOrderScore(pointIndex, pointCount, observationRank, observationCount);
  const yScore = getYContinuityScore(previousBinding, observation);
  const score = clamp(
    (labelScore * 0.45)
    + (tokenScore * 0.3)
    + (directions * 0.1)
    + (groupScore * 0.05)
    + (orderScore * 0.05)
    + (yScore * 0.05)
    - labelMismatchPenalty
  );
  return {
    score,
    factors: {
      point_label: labelScore,
      dms_token_similarity: Number(tokenScore.toFixed(3)),
      direction_match: Number(directions.toFixed(3)),
      group_order: Number(((groupScore + orderScore) / 2).toFixed(3)),
      y_continuity: yScore
    }
  };
}

export function locateCoordinateRows({ coordinateEngineV2 = {}, observations = [], threshold = ROW_LOCATION_MATCH_THRESHOLD } = {}) {
  const usableObservations = (Array.isArray(observations) ? observations : [])
    .map((observation, index) => ({ observation, originalIndex: index }))
    .sort((left, right) => {
      const pageDifference = Number(left.observation.page || 1) - Number(right.observation.page || 1);
      if (pageDifference) return pageDifference;
      const leftY = getObservationY(left.observation);
      const rightY = getObservationY(right.observation);
      if (leftY !== null && rightY !== null) return leftY - rightY;
      return left.originalIndex - right.originalIndex;
    });
  const usedObservationIds = new Set();
  const bindings = [];

  (Array.isArray(coordinateEngineV2.groups) ? coordinateEngineV2.groups : []).forEach((group, groupIndex) => {
    const groupId = String(group.group_id || `group_${groupIndex + 1}`);
    const points = Array.isArray(group.points) ? group.points : [];
    let previousBinding = null;
    points.forEach((point, pointIndex) => {
      const candidates = usableObservations
        .filter(item => !usedObservationIds.has(item.observation.observation_id))
        .map((item, observationRank) => ({
          ...item,
          ...scoreCandidate({
            point,
            pointIndex,
            pointCount: points.length,
            groupId,
            observation: item.observation,
            observationRank,
            observationCount: usableObservations.length,
            previousBinding
          })
        }))
        .sort((left, right) => right.score - left.score);
      const selected = candidates[0] || null;
      const matched = Boolean(selected && selected.score >= threshold);
      if (matched) usedObservationIds.add(selected.observation.observation_id);
      const hasPixelBbox = matched && selected.observation.location_status === "PIXEL_BBOX";
      const binding = {
        point_id: String(point?.label || pointIndex + 1),
        group_id: groupId,
        row_id: `g${groupIndex + 1}-r${pointIndex + 1}`,
        observation_id: matched ? selected.observation.observation_id : null,
        bbox: hasPixelBbox ? selected.observation.bbox : null,
        page: matched ? selected.observation.page : 1,
        source: matched ? selected.observation.source : null,
        location_status: hasPixelBbox ? "PIXEL_BBOX" : "LOGICAL_ROW_ONLY",
        match_score: Number((selected?.score || 0).toFixed(3)),
        score_method: "rule_based_row_match_v1",
        score_calibrated: false,
        match_factors: selected?.factors || {},
        threshold
      };
      bindings.push(binding);
      previousBinding = binding;
    });
  });

  return bindings;
}
