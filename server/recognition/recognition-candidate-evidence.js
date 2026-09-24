export const RECOGNITION_CANDIDATE_EVIDENCE_VERSION = "recognition_candidate_evidence_v3";

const NUMBER_SOURCE = String.raw`[+-]?\d+(?:[.,]\d+)?`;
const NUMBER_PATTERN = new RegExp(`^${NUMBER_SOURCE}$`, "u");
const DMS_SOURCE = String.raw`[-+]?\d{1,3}\s*[°º˚掳潞藲]\s*\d{1,2}\s*(?:['′鈥测瞈]*)?\s*\d{1,2}(?:[.,]\d+)?\s*(?:["″鈥斥砛]*)?\s*(?:N|S|E|W|O|NORTH|SOUTH|EAST|WEST|NORD|SUD|EST|OUEST)\b`;
const DMS_PATTERN = new RegExp(DMS_SOURCE, "giu");
const DMS_SIGNAL_PATTERN = new RegExp(DMS_SOURCE, "iu");
const DIRECTION_PATTERN = /(?:N|S|E|W|O|NORTH|SOUTH|EAST|WEST|NORD|SUD|EST|OUEST)\s*$/iu;
const MGRS_PATTERN = /(?:[1-9]|[1-5]\d|60)\s*[C-HJ-NP-X]\s*[A-HJ-NP-Z]{2}(?:\s*\d{2,10}){1,2}/giu;
const HEADING_MARKER_PATTERN = /^\s*(?:GROUP|HEADING|SECTION|TITLE)\s*\|\s*(\S[\s\S]*?)\s*$/iu;
const META_MARKER_PATTERN = /^\s*(?:CONTEXT|CRS|DATUM|PROJECTION|ZONE|AXIS|SOURCE)\s*\|/iu;

function normalizedHeaderToken(token) {
  return String(token || "").trim().toUpperCase().replace(/[._-]+/gu, "");
}

function analyzeCoordinateHeader(line) {
  const text = String(line || "").trim();
  if (!text || DMS_SIGNAL_PATTERN.test(text)) return null;
  const tokens = text
    .replace(/[|\t,;:/#()[\]{}°º˚掳潞藲-]+/gu, " ")
    .trim()
    .split(/\s+/u)
    .filter(Boolean)
    .map(normalizedHeaderToken);
  const allowed = /^(?:NO|N|NUMBER|NUM|POINT|PT|VERTEX|SOMMET|ID|COORDINATE|COORDINATES|COORDONNEE|COORDONNEES|X\d*|Y\d*|EASTING\d*|NORTHING\d*|LAT\d*|LATITUDE\d*|LON\d*|LONG\d*|LONGITUDE\d*|PARALLELE\d*|MERIDIEN\d*|经度|緯度|纬度|横坐标|縱坐標|纵坐标)$/iu;
  if (tokens.length < 2 || !tokens.every(token => allowed.test(token))) return null;
  const latitudeTokens = tokens.filter(token => /^(?:LAT|LATITUDE|PARALLELE|纬度|緯度)\d*$/iu.test(token));
  const longitudeTokens = tokens.filter(token => /^(?:LON|LONG|LONGITUDE|MERIDIEN|经度)\d*$/iu.test(token));
  const xTokens = tokens.filter(token => /^(?:X|EASTING|横坐标)\d*$/iu.test(token));
  const yTokens = tokens.filter(token => /^(?:Y|NORTHING|纵坐标|縱坐標)\d*$/iu.test(token));
  const geographicPairs = Math.min(latitudeTokens.length, longitudeTokens.length);
  const projectedPairs = Math.min(xTokens.length, yTokens.length);
  if (geographicPairs + projectedPairs === 0) return null;
  const firstLatitude = tokens.findIndex(token => /^(?:LAT|LATITUDE|PARALLELE|纬度|緯度)\d*$/iu.test(token));
  const firstLongitude = tokens.findIndex(token => /^(?:LON|LONG|LONGITUDE|MERIDIEN|经度)\d*$/iu.test(token));
  const firstX = tokens.findIndex(token => /^(?:X|EASTING|横坐标)\d*$/iu.test(token));
  const firstY = tokens.findIndex(token => /^(?:Y|NORTHING|纵坐标|縱坐標)\d*$/iu.test(token));
  return Object.freeze({
    family: geographicPairs > 0 && projectedPairs > 0
      ? "MIXED"
      : geographicPairs > 0 ? "GEOGRAPHIC" : "PROJECTED",
    pairCount: Math.max(geographicPairs, projectedPairs),
    geographicPairs,
    projectedPairs,
    geographicAxisOrder: firstLatitude >= 0 && firstLongitude >= 0 && firstLatitude < firstLongitude
      ? "latitude_longitude"
      : "longitude_latitude",
    projectedAxisOrder: firstX >= 0 && firstY >= 0 && firstX < firstY ? "x_y" : "y_x",
    text
  });
}

function normalizeCoordinateNumber(value) {
  const text = String(value || "").trim();
  if (!NUMBER_PATTERN.test(text)) return null;
  const number = Number(text.replace(",", "."));
  return Number.isFinite(number) ? Object.freeze({ source: text, value: number }) : null;
}

function sourceLabelFromField(value) {
  const text = String(value || "").trim();
  return /^[1-9]\d{0,5}$/u.test(text) || /^[\p{L}][\p{L}\d._-]{0,31}$/u.test(text) ? text : null;
}

function directionAxis(value) {
  const direction = String(value || "").match(DIRECTION_PATTERN)?.[0]?.toUpperCase() || "";
  if (/^(?:N|S|NORTH|SOUTH|NORD|SUD)$/u.test(direction)) return "latitude";
  if (/^(?:E|W|O|EAST|WEST|EST|OUEST)$/u.test(direction)) return "longitude";
  return null;
}

function parseKeyedCoordinateRow(text, lineNumber) {
  const source = String(text || "").trim();
  const labelMatch = source.match(/^\s*(?:(?:POINT|PT|VERTEX|SOMMET)\s*)?([1-9]\d{0,5}|[\p{L}][\p{L}\d._-]{0,31})(?=\s+(?:X|Y|EASTING|NORTHING|LAT|LATITUDE|LON|LONG|LONGITUDE)\b)/iu);
  const sourceLabel = labelMatch?.[1] || null;
  const body = labelMatch ? source.slice(labelMatch[0].length).trim() : source;
  const componentPattern = new RegExp(`(?:^|\\s)(X|Y|EASTING|NORTHING|LAT|LATITUDE|LON|LONG|LONGITUDE)(\\d*)\\s*[:=]\\s*(${NUMBER_SOURCE})(?=\\s|$)`, "giu");
  const components = [...body.matchAll(componentPattern)];
  if (components.length < 2 || components.length % 2 !== 0) return null;
  let remainder = body;
  for (const match of [...components].reverse()) {
    remainder = `${remainder.slice(0, match.index)} ${remainder.slice(match.index + match[0].length)}`;
  }
  if (remainder.replace(/[\s|:;,()[\]{}-]+/gu, "")) return null;
  const bySuffix = new Map();
  for (const component of components) {
    const axis = component[1].toUpperCase();
    const suffix = component[2] || "0";
    const numeric = normalizeCoordinateNumber(component[3]);
    if (!numeric) return null;
    if (!bySuffix.has(suffix)) bySuffix.set(suffix, []);
    bySuffix.get(suffix).push({ axis, numeric });
  }
  const candidates = [];
  for (const pair of bySuffix.values()) {
    const latitude = pair.find(item => /^(?:LAT|LATITUDE)$/u.test(item.axis));
    const longitude = pair.find(item => /^(?:LON|LONG|LONGITUDE)$/u.test(item.axis));
    const x = pair.find(item => /^(?:X|EASTING)$/u.test(item.axis));
    const y = pair.find(item => /^(?:Y|NORTHING)$/u.test(item.axis));
    if (latitude && longitude && pair.length === 2) {
      candidates.push({ format: "WGS84_DECIMAL", latitude: latitude.numeric.value, longitude: longitude.numeric.value });
    } else if (x && y && pair.length === 2) {
      candidates.push({ format: "PROJECTED_XY", x: x.numeric.value, y: y.numeric.value });
    } else return null;
  }
  return candidates.map((candidate, candidateIndex) => Object.freeze({
    ...candidate,
    sourceLineNumber: lineNumber,
    sourceText: source,
    sourceLabel,
    sourceLabelInferred: false,
    sourceRowCandidateIndex: candidateIndex + 1
  }));
}

function parseDmsCoordinateRow(line, lineNumber, { header = null } = {}) {
  const text = String(line || "").trim();
  const matches = [...text.matchAll(DMS_PATTERN)];
  if (matches.length < 2 || matches.length % 2 !== 0) return null;
  let remainder = text;
  for (const match of [...matches].reverse()) {
    remainder = `${remainder.slice(0, match.index)} ${remainder.slice(match.index + match[0].length)}`;
  }
  remainder = remainder.replace(/^\s*(?:ROW|COORDINATE)\s*\|\s*/iu, "").trim();
  let sourceLabel = null;
  const labelMatch = remainder.match(/^\s*(?:(?:POINT|PT|VERTEX|SOMMET)\s*)?([1-9]\d{0,5}|[\p{L}][\p{L}\d._-]{0,31})(?=\s|[|:;,.#)\-])/iu);
  if (labelMatch) {
    sourceLabel = labelMatch[1];
    remainder = remainder.slice(labelMatch[0].length).trim();
  }
  const auxiliary = remainder.split(/[|\t;\s]+/u).filter(Boolean).map(normalizeCoordinateNumber);
  const structuralRemainder = remainder.replace(/[\s|:;,()\[\]{}-]+/gu, "");
  const auxiliaryAllowed = header?.family === "MIXED" && auxiliary.length === 2 && auxiliary.every(Boolean);
  if (structuralRemainder && !auxiliaryAllowed) return null;
  const candidates = [];
  for (let index = 0; index < matches.length; index += 2) {
    const pair = matches.slice(index, index + 2);
    const axes = pair.map(match => directionAxis(match[0]));
    if (axes.filter(axis => axis === "latitude").length !== 1 || axes.filter(axis => axis === "longitude").length !== 1) return null;
    const latitudeMatch = pair[axes.indexOf("latitude")];
    const longitudeMatch = pair[axes.indexOf("longitude")];
    candidates.push(Object.freeze({
      format: "DMS",
      sourceLineNumber: lineNumber,
      sourceText: text,
      sourceLabel,
      sourceLabelInferred: false,
      sourceRowCandidateIndex: (index / 2) + 1,
      latitudeSource: latitudeMatch[0],
      longitudeSource: longitudeMatch[0],
      axisOrder: pair[0] === latitudeMatch ? "latitude_longitude" : "longitude_latitude"
    }));
  }
  return candidates;
}

function parseMgrsCoordinateRow(line, lineNumber) {
  const text = String(line || "").trim();
  const matches = [...text.matchAll(MGRS_PATTERN)];
  MGRS_PATTERN.lastIndex = 0;
  if (matches.length !== 1) return null;
  let remainder = `${text.slice(0, matches[0].index)} ${text.slice(matches[0].index + matches[0][0].length)}`;
  remainder = remainder.replace(/^\s*(?:ROW|COORDINATE|POINT)\s*\|\s*/iu, "").trim();
  let sourceLabel = null;
  const label = remainder.replace(/[\s|:;,()\[\]{}-]+/gu, "");
  if (label) {
    sourceLabel = sourceLabelFromField(label);
    if (!sourceLabel) return null;
  }
  return [Object.freeze({
    format: "MGRS",
    sourceLineNumber: lineNumber,
    sourceText: text,
    sourceLabel,
    sourceLabelInferred: false,
    sourceRowCandidateIndex: 1,
    mgrs: matches[0][0].replace(/\s+/gu, " ").trim()
  })];
}

function parseNumericCoordinateRow(line, lineNumber, { header = null, crsEvidencePresent = false } = {}) {
  const original = String(line || "").trim();
  if (!original) return null;
  const keyed = parseKeyedCoordinateRow(original, lineNumber);
  if (keyed) return keyed;
  const explicitRowPrefix = /^\s*(?:ROW|COORDINATE|POINT)\s*\|/iu.test(original);
  const text = original.replace(/^\s*(?:ROW|COORDINATE|POINT)\s*\|\s*/iu, "").trim();
  const commaPair = text.match(/^([+-]?\d+\.\d{4,})\s*,\s*([+-]?\d+\.\d{4,})$/u);
  if (!header && commaPair) {
    const first = Number(commaPair[1]);
    const second = Number(commaPair[2]);
    if (Math.abs(first) <= 180 && Math.abs(second) <= 90) {
      return [Object.freeze({
        format: "WGS84_DECIMAL",
        sourceLineNumber: lineNumber,
        sourceText: original,
        sourceLabel: null,
        sourceLabelInferred: false,
        sourceRowCandidateIndex: 1,
        longitude: first,
        latitude: second,
        axisOrder: "longitude_latitude"
      })];
    }
  }
  const delimited = /[|\t;]/u.test(text);
  const fields = (delimited ? text.split(/[|\t;]/u) : text.split(/\s+/u)).map(value => value.trim()).filter(Boolean);
  const expectedPairCount = Math.max(1, Number(header?.pairCount || 1));
  let sourceLabel = null;
  let coordinateFields = fields;
  if (fields.length === (expectedPairCount * 2) + 1) {
    sourceLabel = sourceLabelFromField(fields[0]);
    if (!sourceLabel) return null;
    coordinateFields = fields.slice(1);
  }
  if (coordinateFields.length !== expectedPairCount * 2 || !coordinateFields.every(field => normalizeCoordinateNumber(field))) return null;
  if (!header && !(explicitRowPrefix && crsEvidencePresent && sourceLabel)) return null;
  const candidates = [];
  for (let index = 0; index < coordinateFields.length; index += 2) {
    const first = normalizeCoordinateNumber(coordinateFields[index]);
    const second = normalizeCoordinateNumber(coordinateFields[index + 1]);
    const geographic = header?.family === "GEOGRAPHIC";
    const axisOrder = geographic ? header.geographicAxisOrder : header?.projectedAxisOrder || "x_y";
    const candidate = geographic
      ? (axisOrder === "latitude_longitude"
        ? { format: "WGS84_DECIMAL", latitude: first.value, longitude: second.value, axisOrder }
        : { format: "WGS84_DECIMAL", longitude: first.value, latitude: second.value, axisOrder })
      : (axisOrder === "y_x"
        ? { format: "PROJECTED_XY", y: first.value, x: second.value, axisOrder }
        : { format: "PROJECTED_XY", x: first.value, y: second.value, axisOrder });
    if (candidate.format === "WGS84_DECIMAL"
      && (Math.abs(candidate.latitude) > 90 || Math.abs(candidate.longitude) > 180)) return null;
    candidates.push(Object.freeze({
      ...candidate,
      sourceLineNumber: lineNumber,
      sourceText: original,
      sourceLabel,
      sourceLabelInferred: false,
      sourceRowCandidateIndex: (index / 2) + 1
    }));
  }
  return candidates;
}

function parseCoordinateCandidates(line, lineNumber, options = {}) {
  return parseDmsCoordinateRow(line, lineNumber, options)
    || parseMgrsCoordinateRow(line, lineNumber)
    || parseNumericCoordinateRow(line, lineNumber, options);
}

function isPlainStructuralHeading(line) {
  const text = String(line || "").trim();
  if (!text || META_MARKER_PATTERN.test(text) || analyzeCoordinateHeader(text) || DMS_SIGNAL_PATTERN.test(text)) return false;
  const numericTokens = text.match(/\d+(?:[.,]\d+)?/gu) || [];
  return /\p{L}/u.test(text) && numericTokens.length <= 1 && text.length <= 180;
}

function assessSourceLabels(rows) {
  const labels = rows.map(row => row.sourceLabel);
  const present = labels.filter(Boolean);
  if (present.length === 0) return Object.freeze({ state: "MISSING", continuous: false });
  if (present.length !== labels.length) return Object.freeze({ state: "MIXED", continuous: false });
  const numeric = labels.map(value => (/^[1-9]\d*$/u.test(value) ? Number(value) : null));
  if (numeric.every(Number.isSafeInteger)) {
    if (new Set(numeric).size !== numeric.length) return Object.freeze({ state: "DUPLICATE", continuous: false });
    const continuous = numeric.every((value, index) => value === index + 1);
    return Object.freeze({ state: continuous ? "CONTINUOUS" : "NONCONTIGUOUS", continuous });
  }
  if (new Set(labels.map(value => value.toLocaleUpperCase())).size !== labels.length) {
    return Object.freeze({ state: "DUPLICATE", continuous: false });
  }
  return Object.freeze({ state: "VISIBLE_NON_NUMERIC", continuous: false });
}

function titlePathKey(titlePath) {
  return titlePath.map(value => String(value || "").trim().replace(/\s+/gu, " ").toLocaleUpperCase()).filter(Boolean).join("\u001f");
}

function freezeCandidateGroup(group, index) {
  const rows = Object.freeze(group.rows.map((row, rowIndex) => Object.freeze({ ...row, candidateOrder: rowIndex + 1 })));
  const labels = assessSourceLabels(rows);
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

export function extractRecognitionCandidateEvidence({ rawText = "", visibleCrsEvidence = [] } = {}) {
  const source = String(rawText || "");
  const candidateLines = [];
  const allCandidates = [];
  const rejectedRows = [];
  const provisionalGroups = [];
  let activeHeader = null;
  let repeatedHeaderCount = 0;
  let pendingHeadings = [];
  let currentGroup = null;
  let nextBoundaryEvidence = "document_start";
  const closeGroup = () => {
    if (currentGroup?.rows.length) provisionalGroups.push(currentGroup);
    currentGroup = null;
  };

  source.split(/\r?\n/u).forEach((sourceLine, index) => {
    const lineNumber = index + 1;
    const text = String(sourceLine || "").trim();
    if (!text) return;
    const markedHeading = text.match(HEADING_MARKER_PATTERN)?.[1]?.trim() || "";
    if (markedHeading || isPlainStructuralHeading(text)) {
      if (currentGroup?.rows.length) {
        closeGroup();
        pendingHeadings = [];
        nextBoundaryEvidence = markedHeading ? "visible_group_marker" : "visible_heading_transition";
      }
      const heading = markedHeading || text;
      if (pendingHeadings.at(-1) !== heading) pendingHeadings.push(heading);
      activeHeader = null;
      return;
    }
    const header = analyzeCoordinateHeader(text);
    if (header) {
      if (activeHeader) repeatedHeaderCount += 1;
      activeHeader = header;
      return;
    }
    const candidates = parseCoordinateCandidates(text, lineNumber, {
      header: activeHeader,
      crsEvidencePresent: visibleCrsEvidence.length > 0
    });
    if (candidates?.length) {
      if (!currentGroup) {
        currentGroup = {
          titlePath: pendingHeadings.slice(-4),
          boundaryEvidence: nextBoundaryEvidence,
          rows: []
        };
        pendingHeadings = [];
        nextBoundaryEvidence = "row_sequence";
      }
      candidateLines.push(Object.freeze({ lineNumber, text: sourceLine }));
      currentGroup.rows.push(...candidates);
      allCandidates.push(...candidates);
      return;
    }
    const numericCount = (text.match(/[+-]?\d+(?:[.,]\d+)?/gu) || []).length;
    const mgrsSignal = MGRS_PATTERN.test(text);
    MGRS_PATTERN.lastIndex = 0;
    const headerBoundNumericSignal = activeHeader
      && numericCount >= 2
      && /^\s*(?:(?:ROW|COORDINATE|POINT|PT|VERTEX|SOMMET)\b[\s|:]*)?[+-]?\d/iu.test(text);
    const coordinateSignal = DMS_SIGNAL_PATTERN.test(text)
      || mgrsSignal
      || /^\s*(?:ROW|COORDINATE|POINT)\s*\|/iu.test(text)
      || headerBoundNumericSignal;
    if (coordinateSignal) {
      rejectedRows.push(Object.freeze({ lineNumber, text: sourceLine, reason: "COORDINATE_ROW_NOT_FULLY_CONSUMED" }));
      return;
    }
    if (!META_MARKER_PATTERN.test(text)) activeHeader = null;
  });
  closeGroup();

  const frozenGroups = provisionalGroups.map(freezeCandidateGroup);
  const titleKeys = frozenGroups.map(group => titlePathKey(group.titlePath));
  const titlePathsUnique = frozenGroups.length <= 1
    || (titleKeys.every(Boolean) && new Set(titleKeys).size === titleKeys.length);
  const candidateGroups = titlePathsUnique ? frozenGroups : [];
  const unboundCandidates = titlePathsUnique ? [] : allCandidates.map((candidate, index) => Object.freeze({
    ...candidate,
    candidateOrder: index + 1
  }));
  const reviewReasons = new Set();
  if (!titlePathsUnique) reviewReasons.add("GROUP_BOUNDARY_AMBIGUOUS");
  if (rejectedRows.length > 0) reviewReasons.add("CANDIDATE_NORMALIZATION_PARTIAL");
  if (visibleCrsEvidence.length === 0) reviewReasons.add("CRS_EVIDENCE_MISSING");
  if (unboundCandidates.length > 0) reviewReasons.add("COORDINATE_ROW_UNBOUND");
  for (const group of frozenGroups) {
    if (group.sourceLabelState === "MISSING") reviewReasons.add("SOURCE_LABELS_MISSING");
    if (group.sourceLabelState === "MIXED") reviewReasons.add("SOURCE_LABELS_MIXED");
    if (group.sourceLabelState === "DUPLICATE") reviewReasons.add("SOURCE_LABELS_DUPLICATE");
    if (group.sourceLabelState === "NONCONTIGUOUS") reviewReasons.add("SOURCE_LABELS_NONCONTIGUOUS");
  }
  if (allCandidates.some(candidate => candidate.sourceRowCandidateIndex > 1)) {
    reviewReasons.add("MULTIPLE_COORDINATES_PER_SOURCE_ROW");
  }
  return Object.freeze({
    version: RECOGNITION_CANDIDATE_EVIDENCE_VERSION,
    candidateCoordinateLines: Object.freeze(candidateLines),
    candidateCoordinates: Object.freeze(allCandidates),
    candidateCoordinateGroups: Object.freeze(candidateGroups),
    unboundCandidates: Object.freeze(unboundCandidates),
    rejectedRows: Object.freeze(rejectedRows),
    reviewReasons: Object.freeze([...reviewReasons]),
    diagnostics: Object.freeze({
      candidatePointCount: allCandidates.length,
      candidateLineCount: candidateLines.length,
      candidateGroupCount: candidateGroups.length,
      boundRowCount: candidateGroups.reduce((total, group) => total + group.rows.length, 0),
      unboundRowCount: unboundCandidates.length,
      rejectedRowCount: rejectedRows.length,
      repeatedHeaderCount
    })
  });
}

export function buildRecognitionAcquisitionLogSummary({
  evidence,
  providerCallCount = 0,
  contractReason = null,
  finalState = null
} = {}) {
  return Object.freeze({
    providerCompletionState: String(evidence?.providerCompletionState || "UNKNOWN"),
    providerCallCount: Math.max(0, Number(providerCallCount) || 0),
    imageCount: Math.max(0, Number(evidence?.imageEvidence?.imageCount) || 0),
    detailTileCount: Math.max(0, Number(evidence?.imageEvidence?.detailTileCount) || 0),
    providerOutputLength: Math.max(0, Number(evidence?.diagnostics?.providerOutputLength) || 0),
    candidatePointCount: Math.max(0, Number(evidence?.diagnostics?.candidatePointCount) || 0),
    candidateGroupCount: Math.max(0, Number(evidence?.diagnostics?.candidateGroupCount) || 0),
    boundRowCount: Math.max(0, Number(evidence?.diagnostics?.boundRowCount) || 0),
    unboundRowCount: Math.max(0, Number(evidence?.diagnostics?.unboundRowCount) || 0),
    contractReason: String(contractReason || "NONE").slice(0, 160),
    finalState: String(finalState || evidence?.authorizationStatus || "UNKNOWN").slice(0, 80)
  });
}
