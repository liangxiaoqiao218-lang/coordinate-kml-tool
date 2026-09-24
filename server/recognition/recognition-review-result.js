import { extractVisibleCrsEvidence } from "./recognition-first-acquisition.js";

export const RECOGNITION_REVIEW_RESULT_VERSION = "recognition_review_result_v2";

export const ACQUISITION_REVIEW_STATUS = Object.freeze({
  NO_COORDINATE_EVIDENCE: "NO_COORDINATE_EVIDENCE",
  REVIEW_REQUIRED: "ACQUISITION_COMPLETED_REVIEW_REQUIRED",
  AUTHORIZATION_CANDIDATE: "ACQUISITION_COMPLETED_AUTHORIZATION_CANDIDATE"
});

const DMS_COMPONENT_PATTERN = /[-+]?\d{1,3}\s*[°º˚]\s*\d{1,2}\s*(?:['′’]\s*)?\d{1,2}(?:[.,]\d+)?\s*(?:["″”]\s*)?(?:N|S|E|W|O|NORTH|SOUTH|EAST|WEST|NORD|SUD|EST|OUEST)\b/giu;
const CANDIDATE_DMS_SIGNAL = /[°º˚]|\b(?:N|S|E|W|O|NORTH|SOUTH|EAST|WEST|NORD|SUD|EST|OUEST)\b/iu;
const GROUP_MARKER = /^\s*(?:GROUP|HEADING|SECTION|TITLE)\s*\|\s*(\S[\s\S]*?)\s*$/iu;
const META_MARKER = /^\s*(?:CONTEXT|UNCLASSIFIED\s+STRUCTURED\s+COORDINATE\s+EVIDENCE)\b/iu;
const HEADER_AXIS_PATTERN = /\b(?:LAT(?:ITUDE)?|PARALL[EÈ]LE)\b[\s\S]*\b(?:LON(?:GITUDE)?|M[EÉ]RIDIEN)\b|\b(?:LON(?:GITUDE)?|M[EÉ]RIDIEN)\b[\s\S]*\b(?:LAT(?:ITUDE)?|PARALL[EÈ]LE)\b/iu;

function normalizeHemisphere(value) {
  const text = String(value || "").toUpperCase();
  if (["N", "NORTH", "NORD"].includes(text)) return "N";
  if (["S", "SOUTH", "SUD"].includes(text)) return "S";
  if (["E", "EAST", "EST"].includes(text)) return "E";
  if (["W", "O", "WEST", "OUEST"].includes(text)) return "W";
  return "";
}

function parseDmsComponent(source) {
  const text = String(source || "").trim();
  const match = text.match(/^([-+]?\d{1,3})\s*[°º˚]\s*(\d{1,2})\s*(?:['′’]\s*)?(\d{1,2}(?:[.,]\d+)?)\s*(?:["″”]\s*)?(N|S|E|W|O|NORTH|SOUTH|EAST|WEST|NORD|SUD|EST|OUEST)$/iu);
  if (!match) return null;
  const degrees = Number(match[1]);
  const minutes = Number(match[2]);
  const seconds = Number(match[3].replace(",", "."));
  const hemisphere = normalizeHemisphere(match[4]);
  const axis = ["N", "S"].includes(hemisphere) ? "latitude" : "longitude";
  const maximumDegrees = axis === "latitude" ? 90 : 180;
  if (![degrees, minutes, seconds].every(Number.isFinite)
    || minutes < 0 || minutes >= 60 || seconds < 0 || seconds >= 60
    || Math.abs(degrees) > maximumDegrees
    || (Math.abs(degrees) === maximumDegrees && (minutes !== 0 || seconds !== 0))) return null;
  const sign = ["S", "W"].includes(hemisphere) ? -1 : 1;
  if (degrees < 0 && sign > 0) return null;
  return Object.freeze({
    sourceText: text,
    axis,
    hemisphere,
    value: sign * (Math.abs(degrees) + (minutes / 60) + (seconds / 3600))
  });
}

function classifyRowFailure(line, tokenCount) {
  const numericCount = (String(line || "").match(/[-+]?\d+(?:[.,]\d+)?/gu) || []).length;
  if (tokenCount === 2 && numericCount > 7) return "DMS_EXTRA_CONTENT";
  if (tokenCount !== 2) return "DMS_ROW_MALFORMED";
  return "DMS_DIRECTION_CONFLICT";
}

function parseProviderDmsRow(line, lineNumber) {
  const sourceText = String(line || "").trim();
  const tokenMatches = [...sourceText.matchAll(DMS_COMPONENT_PATTERN)];
  if (tokenMatches.length !== 2) {
    return Object.freeze({ accepted: false, reason: classifyRowFailure(sourceText, tokenMatches.length) });
  }
  const components = tokenMatches.map(match => parseDmsComponent(match[0]));
  if (components.some(component => !component)) {
    return Object.freeze({ accepted: false, reason: "DMS_DIRECTION_CONFLICT" });
  }
  const latitude = components.find(component => component.axis === "latitude");
  const longitude = components.find(component => component.axis === "longitude");
  if (!latitude || !longitude) {
    return Object.freeze({ accepted: false, reason: "DMS_DIRECTION_CONFLICT" });
  }

  let remainder = sourceText;
  for (const match of [...tokenMatches].reverse()) {
    remainder = `${remainder.slice(0, match.index)} ${remainder.slice(match.index + match[0].length)}`;
  }
  remainder = remainder.replace(/^\s*(?:ROW|COORDINATE)\s*\|\s*/iu, "").trim();
  let sourceLabel = null;
  const labelled = remainder.match(/^\s*(?:(?:POINT|PT|VERTEX)\s*)?([1-9]\d{0,2})(?=\s|[|:;,.#)\-])/iu);
  if (labelled) {
    sourceLabel = labelled[1];
    remainder = remainder.slice(labelled[0].length);
  }
  const unconsumed = remainder.replace(/[\s|:;,()\[\]{}\-]+/gu, "");
  if (unconsumed) {
    return Object.freeze({ accepted: false, reason: /\d/u.test(unconsumed) ? "DMS_EXTRA_CONTENT" : "DMS_ROW_MALFORMED" });
  }
  return Object.freeze({
    accepted: true,
    row: Object.freeze({
      sourceLineNumber: lineNumber,
      sourceText,
      sourceLabel,
      latitude: latitude.value,
      longitude: longitude.value,
      latitudeSource: latitude.sourceText,
      longitudeSource: longitude.sourceText,
      axisOrder: components[0].axis === "latitude" ? "latitude_longitude" : "longitude_latitude"
    })
  });
}

function isPlainStructuralHeading(line) {
  const text = String(line || "").trim();
  if (!text || META_MARKER.test(text) || HEADER_AXIS_PATTERN.test(text) || CANDIDATE_DMS_SIGNAL.test(text)) return false;
  const numericTokens = text.match(/\d+(?:[.,]\d+)?/gu) || [];
  return /\p{L}/u.test(text) && numericTokens.length <= 1 && text.length <= 180;
}

function labelAssessment(rows) {
  const labels = rows.map(row => row.sourceLabel);
  const present = labels.filter(Boolean);
  if (present.length === 0) return Object.freeze({ state: "MISSING", continuous: false });
  if (present.length !== labels.length) return Object.freeze({ state: "MIXED", continuous: false });
  const numbers = labels.map(Number);
  if (new Set(numbers).size !== numbers.length) return Object.freeze({ state: "DUPLICATE", continuous: false });
  const continuous = numbers.every((value, index) => value === index + 1);
  return Object.freeze({ state: continuous ? "CONTINUOUS" : "NONCONTIGUOUS", continuous });
}

function freezeGroup(group, index) {
  const rows = Object.freeze(group.rows.map((row, rowIndex) => Object.freeze({
    ...row,
    candidateOrder: rowIndex + 1
  })));
  const labels = labelAssessment(rows);
  return Object.freeze({
    groupId: `candidate_group_${index + 1}`,
    titlePath: Object.freeze([...group.titlePath]),
    visibleTitle: group.titlePath.at(-1) || null,
    boundaryEvidence: group.boundaryEvidence,
    sourceStartLine: rows[0]?.sourceLineNumber || null,
    sourceEndLine: rows.at(-1)?.sourceLineNumber || null,
    orderingEvidence: labels.continuous ? "EXPLICIT_CONTINUOUS_SOURCE_LABELS" : "VISIBLE_SOURCE_ROW_ORDER_ONLY",
    sourceLabelsContinuous: labels.continuous,
    sourceLabelState: labels.state,
    rows
  });
}

function reviewReasonForLabelState(state) {
  if (state === "MISSING") return "SOURCE_LABELS_MISSING";
  if (state === "MIXED") return "SOURCE_LABELS_MIXED";
  if (state === "DUPLICATE") return "SOURCE_LABELS_DUPLICATE";
  if (state === "NONCONTIGUOUS") return "SOURCE_LABELS_NONCONTIGUOUS";
  return null;
}

function normalizedTitlePathKey(titlePath) {
  return titlePath
    .map(value => String(value || "").trim().replace(/\s+/gu, " ").toLocaleUpperCase())
    .filter(Boolean)
    .join("\u001f");
}

export function normalizeProviderDmsReviewResult(rawText = "") {
  const source = String(rawText || "");
  const visibleCrsEvidence = extractVisibleCrsEvidence(source);
  const geographicCrsExplicit = visibleCrsEvidence.some(evidence => (
    /\bWGS\s*[- ]?84\b|\bEPSG\s*:?\s*4326\b/iu.test(String(evidence?.text || ""))
  ));
  const provisionalGroups = [];
  const rejectedRows = [];
  let pendingHeadings = [];
  let currentGroup = null;
  let pendingBoundaryEvidence = "document_start";

  const closeGroup = () => {
    if (currentGroup?.rows.length) provisionalGroups.push(currentGroup);
    currentGroup = null;
  };

  source.split(/\r?\n/u).forEach((sourceLine, index) => {
    const lineNumber = index + 1;
    const line = sourceLine.trim();
    if (!line) return;
    const tokenCount = [...line.matchAll(DMS_COMPONENT_PATTERN)].length;
    const candidateSignal = tokenCount > 0 || ((line.match(/[°º˚]/gu) || []).length > 0 && CANDIDATE_DMS_SIGNAL.test(line));
    if (candidateSignal) {
      const parsed = parseProviderDmsRow(line, lineNumber);
      if (!parsed.accepted) {
        rejectedRows.push(Object.freeze({ lineNumber, text: line, reason: parsed.reason }));
        return;
      }
      if (!currentGroup) {
        currentGroup = {
          titlePath: pendingHeadings.slice(-4),
          boundaryEvidence: pendingBoundaryEvidence,
          rows: []
        };
        pendingHeadings = [];
        pendingBoundaryEvidence = "row_sequence";
      }
      currentGroup.rows.push(parsed.row);
      return;
    }

    if (HEADER_AXIS_PATTERN.test(line)) {
      if (currentGroup?.rows.length) {
        closeGroup();
        pendingBoundaryEvidence = "repeated_visible_header";
      }
      return;
    }

    const markedHeading = line.match(GROUP_MARKER)?.[1]?.trim() || "";
    if (markedHeading || isPlainStructuralHeading(line)) {
      if (currentGroup?.rows.length) {
        closeGroup();
        pendingBoundaryEvidence = markedHeading ? "visible_group_marker" : "visible_heading_transition";
      }
      const heading = markedHeading || line;
      if (pendingHeadings.at(-1) !== heading) pendingHeadings.push(heading);
    }
  });
  closeGroup();

  const allCandidates = provisionalGroups.flatMap(group => group.rows);
  if (allCandidates.length === 0) {
    return Object.freeze({
      version: RECOGNITION_REVIEW_RESULT_VERSION,
      status: ACQUISITION_REVIEW_STATUS.NO_COORDINATE_EVIDENCE,
      candidatePointCount: 0,
      candidateGroupCount: 0,
      boundRowCount: 0,
      unboundRowCount: 0,
      candidateGroups: Object.freeze([]),
      unboundCandidates: Object.freeze([]),
      rejectedRows: Object.freeze(rejectedRows),
      visibleCrsEvidence,
      reviewReasons: Object.freeze([]),
      authorizationCandidate: false
    });
  }

  const groups = provisionalGroups.map(freezeGroup);
  const titlePathKeys = groups.map(group => normalizedTitlePathKey(group.titlePath));
  const nonEmptyTitlePaths = titlePathKeys.every(Boolean);
  const titlePathsUnique = nonEmptyTitlePaths && new Set(titlePathKeys).size === titlePathKeys.length;
  const repeatedHeaderContinuationUnresolved = groups.some(group => (
    group.boundaryEvidence === "repeated_visible_header" && group.titlePath.length === 0
  ));
  const boundariesUnique = titlePathsUnique
    && (groups.length === 1 || groups.slice(1).every(group => [
      "visible_group_marker",
      "visible_heading_transition"
    ].includes(group.boundaryEvidence)));
  const candidateGroups = boundariesUnique ? groups : [];
  const unboundCandidates = boundariesUnique ? [] : allCandidates.map((row, index) => Object.freeze({
    ...row,
    candidateOrder: index + 1
  }));
  const reasons = new Set();
  if (!boundariesUnique) reasons.add("GROUP_BOUNDARY_UNRESOLVED");
  if (nonEmptyTitlePaths && !titlePathsUnique) reasons.add("GROUP_TITLE_PATH_DUPLICATE");
  if (repeatedHeaderContinuationUnresolved) reasons.add("REPEATED_HEADER_CONTINUATION_UNRESOLVED");
  for (const rejected of rejectedRows) reasons.add(rejected.reason);
  for (const group of groups) {
    const reason = reviewReasonForLabelState(group.sourceLabelState);
    if (reason) reasons.add(reason);
  }
  if (!geographicCrsExplicit) reasons.add("CRS_EVIDENCE_MISSING");
  if (unboundCandidates.length > 0) reasons.add("COORDINATE_ROW_UNBOUND");
  const authorizationCandidate = boundariesUnique
    && rejectedRows.length === 0
    && groups.every(group => group.sourceLabelsContinuous)
    && geographicCrsExplicit;
  return Object.freeze({
    version: RECOGNITION_REVIEW_RESULT_VERSION,
    status: authorizationCandidate
      ? ACQUISITION_REVIEW_STATUS.AUTHORIZATION_CANDIDATE
      : ACQUISITION_REVIEW_STATUS.REVIEW_REQUIRED,
    candidatePointCount: allCandidates.length,
    candidateGroupCount: candidateGroups.length,
    boundRowCount: candidateGroups.reduce((total, group) => total + group.rows.length, 0),
    unboundRowCount: unboundCandidates.length,
    candidateGroups: Object.freeze(candidateGroups),
    unboundCandidates: Object.freeze(unboundCandidates),
    rejectedRows: Object.freeze(rejectedRows),
    visibleCrsEvidence,
    geographicCrsExplicit,
    reviewReasons: Object.freeze([...reasons]),
    authorizationCandidate
  });
}

export function formatProviderDmsReviewCoordinates(reviewResult = {}) {
  const groups = Array.isArray(reviewResult?.candidateGroups) && reviewResult.candidateGroups.length > 0
    ? reviewResult.candidateGroups
    : [{ rows: Array.isArray(reviewResult?.unboundCandidates) ? reviewResult.unboundCandidates : [] }];
  return groups.map(group => group.rows.map(row => `${row.longitude},${row.latitude}`).join("\n"))
    .filter(Boolean)
    .join("\n\n");
}
