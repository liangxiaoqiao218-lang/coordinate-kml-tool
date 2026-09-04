import { createCandidateEvidence } from "./candidate-selection.js";
import { DMS_PARSE_STATUS, DMS_SOURCE_NOTATION, tokenizeHandwrittenDmsRow } from "./dms-evidence-parser.js";

const OWNER_FAMILY = "handwritten_dms";
const normalizeRow = row => String(row || "").trim().replace(/\s+/g, " ").toUpperCase();

function coordinateRows(input) {
  const lines = Array.isArray(input) ? input.map(String) : String(input || "").split(/\r?\n/);
  const entries = [];
  let groupIndex = 0;
  let pendingGroupBoundary = false;
  for (const line of lines) {
    const row = String(line || "").trim();
    if (!row) {
      if (entries.length) pendingGroupBoundary = true;
      continue;
    }
    if (pendingGroupBoundary) {
      groupIndex += 1;
      pendingGroupBoundary = false;
    }
    entries.push(Object.freeze({ row, groupIndex }));
  }
  return entries;
}

function splitRow(row) {
  const source = String(row || "");
  const labelMatch = source.match(/^\s*(?:(?:POINT|PT)\s*)?([A-Z]|\d{1,3})\s*[).:\-|]\s*/i);
  const withoutLabel = labelMatch ? source.slice(labelMatch[0].length) : source;
  const parts = withoutLabel.split(/\s*[,;|]\s*/, 2);
  return { source, sourceLabel: labelMatch ? labelMatch[1].toUpperCase() : null, parts: parts.length === 2 ? parts : [withoutLabel] };
}

function canonicalPointLabel(sourceLabel, index) {
  if (!sourceLabel) return `P${String(index + 1).padStart(2, "0")}`;
  if (/^\d+$/.test(sourceLabel)) return `P${String(Number(sourceLabel)).padStart(2, "0")}`;
  return sourceLabel;
}

function componentRecords(parsed, pointLabel, axis, sourceStage, groupIndex) {
  const values = {
    degree: String(parsed.degrees),
    minute: String(parsed.minutes),
    second: String(parsed.exactSeconds ?? parsed.seconds),
    hemisphere: parsed.hemisphere
  };
  return Object.entries(values).map(([component, value]) => Object.freeze({
    pointId: pointLabel,
    pointLabel,
    axis,
    component,
    field: `${axis}.${component}`,
    value,
    groupIndex,
    sourceStage,
    sourceNotation: parsed.sourceNotation,
    sourceText: parsed.sourceText,
    sourceSpan: parsed.sourceSpan || null,
    parseStatus: parsed.parseStatus,
    partialFieldStatus: "VALID",
    evidenceRef: `${sourceStage}:${pointLabel}:${axis}.${component}`
  }));
}

function unresolvedRecord(parsed, pointLabel, sourceStage, field = null, groupIndex = 0) {
  return Object.freeze({
    pointId: pointLabel,
    pointLabel,
    axis: parsed.axis || null,
    field,
    groupIndex,
    sourceText: parsed.sourceText,
    sourceNotation: parsed.sourceNotation || DMS_SOURCE_NOTATION.UNRESOLVED,
    parseStatus: parsed.parseStatus,
    reason: parsed.reason,
    evidenceRef: `${sourceStage}:${pointLabel}:${field || "unresolved"}`
  });
}

export function buildHandwrittenCandidateEvidence(coordinates, { candidateId, sourceStage, ownerFamily = OWNER_FAMILY } = {}) {
  if (ownerFamily !== OWNER_FAMILY) throw new TypeError("handwritten_candidate_owner_invalid");
  const rowEntries = coordinateRows(coordinates);
  const rows = rowEntries.map(entry => entry.row);
  const parsedRows = rowEntries.map(({ row, groupIndex }, index) => {
    const split = splitRow(row);
    const pointLabel = canonicalPointLabel(split.sourceLabel, index);
    const tokenized = tokenizeHandwrittenDmsRow(split.parts.join(","), { ownerFamily });
    const parsedByAxis = new Map();
    const unresolved = tokenized.unresolved.map(parsed => unresolvedRecord(parsed, pointLabel, sourceStage, parsed.axis, groupIndex));
    for (const parsed of tokenized.fields) {
      parsedByAxis.set(parsed.axis, parsed);
    }
    for (const field of ["latitude", "longitude"]) {
      if (!parsedByAxis.has(field) && !unresolved.some(item => item.axis === field)) {
        unresolved.push(unresolvedRecord({ sourceText: split.source, sourceNotation: DMS_SOURCE_NOTATION.UNRESOLVED, parseStatus: DMS_PARSE_STATUS.MISSING, reason: `${field}_missing`, axis: field }, pointLabel, sourceStage, field, groupIndex));
      }
    }
    const fieldEvidence = [...parsedByAxis.entries()].flatMap(([axis, parsed]) => componentRecords(parsed, pointLabel, axis, sourceStage, groupIndex));
    return { row, groupIndex, pointLabel, sourceLabel: split.sourceLabel, fieldEvidence, unresolved };
  });
  const pointLabels = parsedRows.map(row => row.pointLabel);
  const duplicateLabelCount = pointLabels.length - new Set(pointLabels).size;
  const duplicateRowCount = rows.length - new Set(rows.map(normalizeRow)).size;
  const validFieldCount = parsedRows.reduce((sum, row) => sum + row.fieldEvidence.length, 0);
  const totalFieldCount = rows.length * 8;
  const completeRowCount = parsedRows.filter(row => row.fieldEvidence.length === 8 && row.unresolved.length === 0).length;
  const ordinals = pointLabels.map(label => /^P\d+$/.test(label) ? Number(label.slice(1)) : (/^[A-Z]$/.test(label) ? label.charCodeAt(0) - 64 : NaN));
  const labelContinuity = duplicateLabelCount === 0 && ordinals.every((value, index) => Number.isFinite(value) && (index === 0 || value === ordinals[index - 1] + 1)) ? 1 : 0;
  const unresolvedIssues = parsedRows.flatMap(row => row.unresolved);
  return createCandidateEvidence({
    candidateId,
    ownerFamily,
    sourceStage,
    coordinateCount: rows.length,
    parseCompleteness: totalFieldCount ? validFieldCount / totalFieldCount : 0,
    validRangeCount: validFieldCount,
    invalidRangeCount: totalFieldCount - validFieldCount,
    pointLabels,
    labelContinuity,
    rowAlignment: rows.length ? completeRowCount / rows.length : 0,
    duplicateCount: duplicateLabelCount + duplicateRowCount,
    geometryConsistency: validFieldCount > 0 ? 1 : 0,
    unresolvedIssues,
    fieldEvidence: parsedRows.flatMap(row => row.fieldEvidence)
  });
}

function componentMap(fieldEvidence) {
  return new Map((fieldEvidence || []).map(item => [`${item.pointLabel}\u0000${item.field}`, item]));
}

export function materializeHandwrittenDmsRows(fieldEvidence, pointLabels) {
  const fields = componentMap(fieldEvidence);
  const rows = [];
  let previousGroupIndex = null;
  for (const pointLabel of pointLabels || []) {
    const components = ["degree", "minute", "second", "hemisphere"];
    const records = axis => components.map(component => fields.get(`${pointLabel}\u0000${axis}.${component}`));
    const latitudeRecords = records("latitude");
    const longitudeRecords = records("longitude");
    const groupIndex = [...latitudeRecords, ...longitudeRecords]
      .map(record => record?.groupIndex)
      .find(value => Number.isInteger(value));
    const latitude = latitudeRecords.map(record => record?.value);
    const longitude = longitudeRecords.map(record => record?.value);
    if ([...latitude, ...longitude].some(value => value == null || value === "")) return null;
    const renderAxis = (recordsForAxis, values) => {
      const sourceTexts = [...new Set(recordsForAxis.map(record => String(record?.sourceText || "").trim()).filter(Boolean))];
      return sourceTexts.length === 1
        ? sourceTexts[0]
        : `${values[0]}°${values[1]}'${values[2]}\"${values[3]}`;
    };
    if (rows.length && groupIndex != null && previousGroupIndex != null && groupIndex !== previousGroupIndex) rows.push("");
    rows.push(`${renderAxis(latitudeRecords, latitude)},${renderAxis(longitudeRecords, longitude)}`);
    if (groupIndex != null) previousGroupIndex = groupIndex;
  }
  return Object.freeze(rows);
}
