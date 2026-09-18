export const RECOGNITION_COMPLETENESS_DECISION = Object.freeze({
  COMPLETE_CANDIDATE: "COMPLETE_CANDIDATE",
  POINT_REVIEW_REQUIRED: "POINT_REVIEW_REQUIRED",
  REVIEW_REQUIRED: "REVIEW_REQUIRED",
  FAMILY_RETRY_ONLY: "FAMILY_RETRY_ONLY",
  LOCAL_OCR_EVIDENCE_ONLY: "LOCAL_OCR_EVIDENCE_ONLY",
  FAILED_CLOSED: "FAILED_CLOSED",
  NON_COORDINATE_REJECTED: "NON_COORDINATE_REJECTED"
});

export const RECOGNITION_COMPLETENESS_NEXT_ACTION = Object.freeze({
  ACCEPT_EVIDENCE: "ACCEPT_EVIDENCE",
  POINT_REVIEW: "POINT_REVIEW",
  HUMAN_REVIEW: "HUMAN_REVIEW",
  FAMILY_RETRY: "FAMILY_RETRY",
  LOCAL_OCR_EVIDENCE: "LOCAL_OCR_EVIDENCE",
  STOP: "STOP"
});

const FAMILY_RETRY_ALLOWLIST = new Set([
  "bftm",
  "cadastral_grid",
  "cote_divoire",
  "dms_grouped",
  "french_perimeter_dms",
  "handwritten_dms",
  "kyrgyz_gk",
  "mgrs",
  "mozambique_geographic",
  "point_az_dms_table",
  "wgs84_table"
]);

const MAP_SOURCE_ROLES = new Set(["MAP_SEARCH_BOX", "MAP_PLACE_DETAILS", "MAP_PLUS_CODE"]);
const NON_COORDINATE_KINDS = new Set(["MATH_EXPRESSION", "ORDINARY_REPORT", "PSEUDO_COORDINATE", "SPLIT_NUMERIC_COLUMNS"]);
const PROVIDER_TERMINAL_STATES = new Set(["FAILED", "TIMEOUT", "UNCERTAIN"]);

function normalizeToken(value) {
  return String(value || "").trim().toUpperCase();
}

function toCount(value, fallback = 0) {
  const number = Number(value);
  return Number.isInteger(number) && number >= 0 ? number : fallback;
}

function nonEmptyLines(value) {
  return String(value || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .filter(line => !/^未识别到有效坐标。?$/u.test(line));
}

function detectTableHeader(text) {
  return /(?:经度|纬度|longitude|latitude|\blon\b|\blat\b|\bpoint\b|\bnord\b|\best\b|\beasting\b|\bnorthing\b|\bx\b\s*[|,;:/-]\s*\by\b)/iu.test(String(text || ""));
}

function detectCoordinateFormat(text) {
  const value = String(text || "");
  if (/[°º]\s*\d{1,2}\s*['’′]\s*\d{1,2}(?:\.\d+)?\s*(?:["”″]|''?)?\s*[NSEW]/iu.test(value)) return "DMS";
  if (/\b(?:UTM|EPSG|EASTING|NORTHING|ZONE|ZONA|FUSO)\b/iu.test(value)) return "PROJECTED";
  if (/[+-]?\d{1,3}\.\d+\s*[,|;]\s*[+-]?\d{1,3}\.\d+/u.test(value)) return "WGS84_DECIMAL";
  return "UNKNOWN";
}

function extractLeadingLabels(text) {
  const labels = [];
  for (const line of nonEmptyLines(text)) {
    const match = line.match(/^(?:point\s*)?([A-Z]|\d{1,3})(?:[.)\]:-]|\s+(?=[+-]?\d|\d{1,3}\s*[°º]))/iu);
    if (match) labels.push(normalizeToken(match[1]));
  }
  return labels;
}

function analyzeLabelContinuity(labels) {
  const unique = Array.from(new Set(labels));
  if (unique.length < 2 || unique.length !== labels.length) {
    return { present: unique.length > 0, continuous: unique.length < 2, duplicate: unique.length !== labels.length };
  }
  const numeric = unique.every(label => /^\d+$/.test(label));
  const alpha = unique.every(label => /^[A-Z]$/.test(label));
  if (!numeric && !alpha) return { present: true, continuous: false, duplicate: false };
  const values = numeric ? unique.map(Number) : unique.map(label => label.charCodeAt(0));
  const continuous = values.every((value, index) => index === 0 || value === values[index - 1] + 1);
  return { present: true, continuous, duplicate: false };
}

function analyzeGroups(layoutGroups) {
  if (!Array.isArray(layoutGroups) || layoutGroups.length === 0) {
    return { present: false, complete: true, groupCount: 0, rowCount: 0 };
  }
  let complete = true;
  let rowCount = 0;
  for (const group of layoutGroups) {
    const rows = toCount(group?.rowCount ?? group?.rows?.length, 0);
    const expected = Number.isInteger(Number(group?.expectedRowCount)) ? Number(group.expectedRowCount) : null;
    rowCount += rows;
    if (group?.complete === false || (expected !== null && rows !== expected) || rows === 0) complete = false;
  }
  return { present: true, complete, groupCount: layoutGroups.length, rowCount };
}

function hasCompleteProjectedCrs(crsEvidence) {
  return Boolean(
    crsEvidence
    && String(crsEvidence.datum || "").trim()
    && String(crsEvidence.zone || "").trim()
    && String(crsEvidence.hemisphere || "").trim()
    && String(crsEvidence.axisOrder || "").trim()
  );
}

function sanitizedResult({
  decision,
  nextAction,
  reasons,
  coordinateRowCount,
  coordinateFormat,
  tableHeaderPresent,
  labelContinuity,
  groupAnalysis,
  allowLocalOcrEvidence = false,
  retryFamily = null,
  preserveSourceRepresentation = true,
  preserveGroupBoundaries = false,
  forcePointReview = false
}) {
  return Object.freeze({
    schemaVersion: "recognition_completeness_v1",
    decision,
    nextAction,
    reasons: Object.freeze(Array.from(new Set(reasons.filter(Boolean)))),
    evidence: Object.freeze({
      coordinateRowCount,
      coordinateFormat,
      tableHeaderPresent,
      rowLabelsPresent: labelContinuity.present,
      rowLabelsContinuous: labelContinuity.continuous,
      rowLabelsDuplicated: labelContinuity.duplicate,
      layoutGroupsPresent: groupAnalysis.present,
      layoutGroupsComplete: groupAnalysis.complete,
      layoutGroupCount: groupAnalysis.groupCount
    }),
    allowGenericProviderRetry: false,
    allowFamilyProviderRetry: decision === RECOGNITION_COMPLETENESS_DECISION.FAMILY_RETRY_ONLY,
    allowLocalOcrEvidence,
    retryFamily,
    preserveSourceRepresentation,
    preserveGroupBoundaries,
    forcePointReview,
    geometryInferenceAllowed: false,
    pointCreationAllowed: false,
    mapCreationAllowed: false,
    kmlCreationAllowed: false
  });
}

export function assessRecognitionCompleteness(input = {}) {
  const rawText = String(input.rawText || "");
  const coordinates = String(input.coordinates || "");
  const combinedText = `${rawText}\n${coordinates}`;
  const coordinateRowCount = Number.isInteger(Number(input.coordinateRowCount))
    ? toCount(input.coordinateRowCount)
    : nonEmptyLines(coordinates).length;
  const coordinateFormat = normalizeToken(input.coordinateFormat) || detectCoordinateFormat(combinedText);
  const sourceKind = normalizeToken(input.sourceKind);
  const providerStatus = normalizeToken(input.providerStatus || "SUCCESS");
  const family = String(input.family || "").trim().toLowerCase();
  const sourceRoles = Array.isArray(input.sourceRoles) ? input.sourceRoles.map(normalizeToken) : [];
  const mapRoleCount = new Set(sourceRoles.filter(role => MAP_SOURCE_ROLES.has(role))).size;
  const tableHeaderPresent = input.structure?.tableHeaderPresent === true || detectTableHeader(rawText);
  const rawLabels = extractLeadingLabels(rawText);
  const labels = rawLabels.length > 0 ? rawLabels : extractLeadingLabels(coordinates);
  const labelContinuity = analyzeLabelContinuity(labels);
  const groupAnalysis = analyzeGroups(input.layoutGroups);
  const familyEvidencePresent = input.structure?.familyEvidencePresent === true;
  const expectedRowCount = Number.isInteger(Number(input.structure?.expectedRowCount))
    ? Number(input.structure.expectedRowCount)
    : null;
  const explicitMissingRows = toCount(input.structure?.missingRowCount, 0);
  const brokenRows = toCount(input.structure?.brokenRowCount, 0);
  const incompleteByCount = expectedRowCount !== null && coordinateRowCount < expectedRowCount;
  const structurallyIncomplete = Boolean(
    input.structure?.incomplete === true
    || explicitMissingRows > 0
    || brokenRows > 0
    || incompleteByCount
    || (labelContinuity.present && !labelContinuity.continuous)
    || (groupAnalysis.present && !groupAnalysis.complete)
  );

  if (NON_COORDINATE_KINDS.has(sourceKind)) {
    return sanitizedResult({
      decision: RECOGNITION_COMPLETENESS_DECISION.NON_COORDINATE_REJECTED,
      nextAction: RECOGNITION_COMPLETENESS_NEXT_ACTION.STOP,
      reasons: ["NON_COORDINATE_CONTENT"],
      coordinateRowCount,
      coordinateFormat,
      tableHeaderPresent,
      labelContinuity,
      groupAnalysis
    });
  }

  if (PROVIDER_TERMINAL_STATES.has(providerStatus)) {
    const allowLocalOcrEvidence = input.localOcrAttempted !== true;
    return sanitizedResult({
      decision: allowLocalOcrEvidence
        ? RECOGNITION_COMPLETENESS_DECISION.LOCAL_OCR_EVIDENCE_ONLY
        : RECOGNITION_COMPLETENESS_DECISION.FAILED_CLOSED,
      nextAction: allowLocalOcrEvidence
        ? RECOGNITION_COMPLETENESS_NEXT_ACTION.LOCAL_OCR_EVIDENCE
        : RECOGNITION_COMPLETENESS_NEXT_ACTION.STOP,
      reasons: ["PROVIDER_TERMINAL_STATE", allowLocalOcrEvidence ? "ONE_LOCAL_OCR_ALLOWED" : "LOCAL_OCR_ALREADY_ATTEMPTED"],
      coordinateRowCount,
      coordinateFormat,
      tableHeaderPresent,
      labelContinuity,
      groupAnalysis,
      allowLocalOcrEvidence
    });
  }

  if (coordinateRowCount === 0) {
    const allowLocalOcrEvidence = input.localOcrAttempted !== true;
    return sanitizedResult({
      decision: allowLocalOcrEvidence
        ? RECOGNITION_COMPLETENESS_DECISION.LOCAL_OCR_EVIDENCE_ONLY
        : RECOGNITION_COMPLETENESS_DECISION.FAILED_CLOSED,
      nextAction: allowLocalOcrEvidence
        ? RECOGNITION_COMPLETENESS_NEXT_ACTION.LOCAL_OCR_EVIDENCE
        : RECOGNITION_COMPLETENESS_NEXT_ACTION.STOP,
      reasons: ["EMPTY_PARSED_RESULT", allowLocalOcrEvidence ? "ONE_LOCAL_OCR_ALLOWED" : "LOCAL_OCR_ALREADY_ATTEMPTED"],
      coordinateRowCount,
      coordinateFormat,
      tableHeaderPresent,
      labelContinuity,
      groupAnalysis,
      allowLocalOcrEvidence
    });
  }

  if (coordinateFormat === "PROJECTED" && !hasCompleteProjectedCrs(input.crsEvidence)) {
    return sanitizedResult({
      decision: RECOGNITION_COMPLETENESS_DECISION.REVIEW_REQUIRED,
      nextAction: RECOGNITION_COMPLETENESS_NEXT_ACTION.HUMAN_REVIEW,
      reasons: ["PROJECTED_CRS_EVIDENCE_INCOMPLETE"],
      coordinateRowCount,
      coordinateFormat,
      tableHeaderPresent,
      labelContinuity,
      groupAnalysis,
      preserveGroupBoundaries: groupAnalysis.present
    });
  }

  if (input.handwrittenConflict === true) {
    return sanitizedResult({
      decision: RECOGNITION_COMPLETENESS_DECISION.REVIEW_REQUIRED,
      nextAction: RECOGNITION_COMPLETENESS_NEXT_ACTION.HUMAN_REVIEW,
      reasons: ["HANDWRITTEN_CANDIDATE_CONFLICT"],
      coordinateRowCount,
      coordinateFormat,
      tableHeaderPresent,
      labelContinuity,
      groupAnalysis,
      preserveGroupBoundaries: groupAnalysis.present
    });
  }

  if (mapRoleCount >= 2) {
    const samePlaceNearDuplicate = input.samePlaceNearDuplicate === true;
    return sanitizedResult({
      decision: samePlaceNearDuplicate
        ? RECOGNITION_COMPLETENESS_DECISION.POINT_REVIEW_REQUIRED
        : RECOGNITION_COMPLETENESS_DECISION.REVIEW_REQUIRED,
      nextAction: samePlaceNearDuplicate
        ? RECOGNITION_COMPLETENESS_NEXT_ACTION.POINT_REVIEW
        : RECOGNITION_COMPLETENESS_NEXT_ACTION.HUMAN_REVIEW,
      reasons: [samePlaceNearDuplicate ? "MAP_SAME_PLACE_PRECISION_VARIANTS" : "MAP_DISTINCT_OR_UNBOUND_LOCATIONS"],
      coordinateRowCount,
      coordinateFormat,
      tableHeaderPresent,
      labelContinuity,
      groupAnalysis,
      forcePointReview: samePlaceNearDuplicate
    });
  }

  if (structurallyIncomplete) {
    const retryFamily = FAMILY_RETRY_ALLOWLIST.has(family)
      && (tableHeaderPresent || groupAnalysis.present || familyEvidencePresent)
      ? family
      : null;
    return sanitizedResult({
      decision: retryFamily
        ? RECOGNITION_COMPLETENESS_DECISION.FAMILY_RETRY_ONLY
        : RECOGNITION_COMPLETENESS_DECISION.REVIEW_REQUIRED,
      nextAction: retryFamily
        ? RECOGNITION_COMPLETENESS_NEXT_ACTION.FAMILY_RETRY
        : RECOGNITION_COMPLETENESS_NEXT_ACTION.HUMAN_REVIEW,
      reasons: [retryFamily ? "STRUCTURE_PROVES_FAMILY_INCOMPLETE" : "INCOMPLETE_STRUCTURE_WITHOUT_TRUSTED_FAMILY"],
      coordinateRowCount,
      coordinateFormat,
      tableHeaderPresent,
      labelContinuity,
      groupAnalysis,
      retryFamily,
      preserveGroupBoundaries: groupAnalysis.present
    });
  }

  if (coordinateRowCount === 1) {
    return sanitizedResult({
      decision: RECOGNITION_COMPLETENESS_DECISION.COMPLETE_CANDIDATE,
      nextAction: RECOGNITION_COMPLETENESS_NEXT_ACTION.ACCEPT_EVIDENCE,
      reasons: ["VALID_SINGLE_COORDINATE_CANDIDATE"],
      coordinateRowCount,
      coordinateFormat,
      tableHeaderPresent,
      labelContinuity,
      groupAnalysis
    });
  }

  if (tableHeaderPresent && (!labelContinuity.present || labelContinuity.continuous) && groupAnalysis.complete) {
    return sanitizedResult({
      decision: groupAnalysis.groupCount > 1
        ? RECOGNITION_COMPLETENESS_DECISION.REVIEW_REQUIRED
        : RECOGNITION_COMPLETENESS_DECISION.COMPLETE_CANDIDATE,
      nextAction: groupAnalysis.groupCount > 1
        ? RECOGNITION_COMPLETENESS_NEXT_ACTION.HUMAN_REVIEW
        : RECOGNITION_COMPLETENESS_NEXT_ACTION.ACCEPT_EVIDENCE,
      reasons: [groupAnalysis.groupCount > 1 ? "COMPLETE_MULTI_GROUP_REVIEW" : "STRUCTURED_ROWS_COMPLETE"],
      coordinateRowCount,
      coordinateFormat,
      tableHeaderPresent,
      labelContinuity,
      groupAnalysis,
      preserveGroupBoundaries: groupAnalysis.present
    });
  }

  return sanitizedResult({
    decision: RECOGNITION_COMPLETENESS_DECISION.REVIEW_REQUIRED,
    nextAction: RECOGNITION_COMPLETENESS_NEXT_ACTION.HUMAN_REVIEW,
    reasons: ["MULTI_COORDINATE_SOURCE_BINDING_UNPROVEN"],
    coordinateRowCount,
    coordinateFormat,
    tableHeaderPresent,
    labelContinuity,
    groupAnalysis,
    preserveGroupBoundaries: groupAnalysis.present
  });
}
