export const CANDIDATE_SELECTION_DECISION = Object.freeze({
  REPLACE_CURRENT: "REPLACE_CURRENT",
  MERGE_RETRY_FIELDS: "MERGE_RETRY_FIELDS",
  KEEP_CURRENT: "KEEP_CURRENT",
  KEEP_CURRENT_AND_REQUIRE_REVIEW: "KEEP_CURRENT_AND_REQUIRE_REVIEW",
  REJECT_CANDIDATE: "REJECT_CANDIDATE"
});

function count(value, field) {
  if (!Number.isInteger(value) || value < 0) throw new TypeError(`${field}_invalid`);
  return value;
}
function score(value, field) {
  if (!Number.isFinite(value) || value < 0 || value > 1) throw new TypeError(`${field}_invalid`);
  return value;
}
function text(value, field) {
  if (typeof value !== "string" || !value.trim()) throw new TypeError(`${field}_invalid`);
  return value.trim();
}
function records(value, field) {
  if (!Array.isArray(value)) throw new TypeError(`${field}_invalid`);
  return Object.freeze(value.map(item => Object.freeze({ ...item })));
}

export function createCandidateEvidence(input = {}) {
  return Object.freeze({
    candidateId: text(input.candidateId, "candidate_id"),
    ownerFamily: text(input.ownerFamily, "owner_family"),
    sourceStage: text(input.sourceStage, "source_stage"),
    coordinateCount: count(input.coordinateCount, "coordinate_count"),
    parseCompleteness: score(input.parseCompleteness, "parse_completeness"),
    validRangeCount: count(input.validRangeCount, "valid_range_count"),
    invalidRangeCount: count(input.invalidRangeCount, "invalid_range_count"),
    pointLabels: Object.freeze((input.pointLabels || []).map(label => text(label, "point_label"))),
    labelContinuity: score(input.labelContinuity, "label_continuity"),
    rowAlignment: score(input.rowAlignment, "row_alignment"),
    duplicateCount: count(input.duplicateCount, "duplicate_count"),
    geometryConsistency: score(input.geometryConsistency, "geometry_consistency"),
    unresolvedIssues: records(input.unresolvedIssues, "unresolved_issues"),
    fieldEvidence: records(input.fieldEvidence, "field_evidence")
  });
}

function key(item) { return `${String(item.pointLabel || "").trim()}\u0000${String(item.field || "").trim()}`; }
const COMPONENT_FIELDS = Object.freeze([
  "latitude.degree", "latitude.minute", "latitude.second", "latitude.hemisphere",
  "longitude.degree", "longitude.minute", "longitude.second", "longitude.hemisphere"
]);

function selectedSource(currentCount, retryCount) {
  if (retryCount === 0) return "STAGE_1";
  if (currentCount === 0) return "STAGE_2";
  return "MIXED_RUNTIME_EVIDENCE";
}

export function compareCandidateEvidence(currentInput, retryInput) {
  const current = createCandidateEvidence(currentInput);
  const retry = createCandidateEvidence(retryInput);
  if (current.ownerFamily !== retry.ownerFamily) {
    return Object.freeze({
      decision: CANDIDATE_SELECTION_DECISION.REJECT_CANDIDATE,
      reviewRequired: false,
      conflicts: Object.freeze([]),
      selectedFieldEvidence: current.fieldEvidence,
      selectedSource: "STAGE_1",
      pointLabels: current.pointLabels,
      unresolvedIssues: current.unresolvedIssues,
      reason: "owner_family_mismatch"
    });
  }

  const currentFields = new Map(current.fieldEvidence.map(item => [key(item), item]));
  const retryFields = new Map(retry.fieldEvidence.map(item => [key(item), item]));
  const pointLabels = Object.freeze([...new Set([...current.pointLabels, ...retry.pointLabels])]);
  const selected = [];
  const conflicts = [];
  const unresolvedIssues = [];
  let currentSelections = 0;
  let retrySelections = 0;

  for (const pointLabel of pointLabels) {
    for (const field of COMPONENT_FIELDS) {
      const fieldKey = `${pointLabel}\u0000${field}`;
      const before = currentFields.get(fieldKey);
      const after = retryFields.get(fieldKey);
      if (before && after && before.value !== after.value) {
        selected.push(before);
        currentSelections += 1;
        conflicts.push(Object.freeze({
          pointId: pointLabel,
          pointLabel,
          axis: before.axis,
          component: before.component,
          field,
          currentValue: before.value,
          retryValue: after.value,
          currentEvidenceRef: before.evidenceRef || null,
          retryEvidenceRef: after.evidenceRef || null
        }));
      } else if (before) {
        selected.push(before);
        currentSelections += 1;
      } else if (after) {
        selected.push(after);
        retrySelections += 1;
      } else {
        unresolvedIssues.push(Object.freeze({ pointId: pointLabel, pointLabel, field, reason: "component_missing_from_all_candidates" }));
      }
    }
  }

  const reviewRequired = conflicts.length > 0 || unresolvedIssues.length > 0;
  let decision = CANDIDATE_SELECTION_DECISION.KEEP_CURRENT;
  let reason = "no_strict_component_improvement";
  if (conflicts.length > 0) {
    decision = CANDIDATE_SELECTION_DECISION.KEEP_CURRENT_AND_REQUIRE_REVIEW;
    reason = "valid_component_conflict_without_independent_proof";
  } else if (retrySelections > 0 && currentSelections > 0) {
    decision = CANDIDATE_SELECTION_DECISION.MERGE_RETRY_FIELDS;
    reason = "retry_fills_missing_or_invalid_components_without_regression";
  } else if (retrySelections > 0) {
    decision = CANDIDATE_SELECTION_DECISION.REPLACE_CURRENT;
    reason = "retry_supplies_all_selected_components";
  }

  return Object.freeze({
    decision,
    reviewRequired,
    conflicts: Object.freeze(conflicts),
    selectedFieldEvidence: Object.freeze(selected),
    selectedSource: selectedSource(currentSelections, retrySelections),
    pointLabels,
    unresolvedIssues: Object.freeze(unresolvedIssues),
    currentSelectionCount: currentSelections,
    retrySelectionCount: retrySelections,
    reason
  });
}
