import { LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS } from "../evidence-acquisition/local-ocr-map-layout-classifier.js";
import { extractProjectedSourceContext } from "./projected-source-evidence.js";

export const MULTI_REPRESENTATION_SOURCE_EVIDENCE_VERSION = "multi_representation_source_evidence_v1";

const DMS_COMPONENT = /([-+]?\d{1,3})\s*[°º˚]\s*(\d{1,2})\s*['′’]?\s*(\d{1,2}(?:[.,]\d+)?)\s*(?:["″”]|'{2})?\s*(N|S|E|W|O)\b/giu;

function normalizeHemisphere(value) {
  const direction = String(value || "").toUpperCase();
  return direction === "O" ? "W" : direction;
}

function parseDmsComponent(match) {
  const degrees = Number(match[1]);
  const minutes = Number(match[2]);
  const seconds = Number(String(match[3] || "").replace(",", "."));
  const hemisphere = normalizeHemisphere(match[4]);
  const latitude = ["N", "S"].includes(hemisphere);
  const maximum = latitude ? 90 : 180;
  if (![degrees, minutes, seconds].every(Number.isFinite)
    || minutes < 0 || minutes >= 60 || seconds < 0 || seconds >= 60
    || Math.abs(degrees) > maximum
    || (Math.abs(degrees) === maximum && (minutes !== 0 || seconds !== 0))) return null;
  const sign = ["S", "W"].includes(hemisphere) ? -1 : 1;
  return Object.freeze({
    axis: latitude ? "latitude" : "longitude",
    hemisphere,
    value: sign * (Math.abs(degrees) + minutes / 60 + seconds / 3600),
    sourceText: String(match[0] || "").trim()
  });
}

function parseDmsPair(line) {
  const matches = [...String(line || "").matchAll(DMS_COMPONENT)];
  if (matches.length !== 2) return null;
  const components = matches.map(parseDmsComponent);
  if (components.some(component => !component)) return null;
  const latitude = components.find(component => component.axis === "latitude");
  const longitude = components.find(component => component.axis === "longitude");
  if (!latitude || !longitude) return null;
  return Object.freeze({
    latitude: latitude.value,
    longitude: longitude.value,
    latitudeSource: latitude.sourceText,
    longitudeSource: longitude.sourceText,
    firstIndex: matches[0].index,
    endIndex: matches[1].index + matches[1][0].length
  });
}

function decimalToken(value) {
  const source = String(value || "").trim().replace(/[−–—]/gu, "-");
  if (!/^[+-]?\d{4,10}(?:[.,]\d+)?$/u.test(source)) return null;
  const normalized = source.replace(",", ".");
  const number = Number(normalized);
  if (!Number.isFinite(number)) return null;
  const fractionDigits = /[.,](\d+)$/u.exec(source)?.[1]?.length || 0;
  return { source, normalized, number, fractionDigits };
}

function normalizeProjectedColumn(tokens, { axis, context } = {}) {
  const explicitPrecisions = tokens.map(token => token?.fractionDigits || 0).filter(value => value > 0);
  const uniquePrecisions = [...new Set(explicitPrecisions)];
  const establishedPrecision = uniquePrecisions.length === 1 && explicitPrecisions.length >= 2
    ? uniquePrecisions[0]
    : 0;
  return tokens.map(token => {
    if (!token) return null;
    let numeric = token.number;
    let normalized = token.normalized;
    const utm = context?.crsEvidence?.projection === "utm";
    const withinRange = axis === "x"
      ? numeric >= 100_000 && numeric <= 1_000_000
      : numeric >= 0 && numeric <= 10_000_000;
    if (!withinRange && utm && token.fractionDigits === 0 && establishedPrecision > 0) {
      const unsigned = token.normalized.replace(/^[+-]/u, "");
      const sign = token.normalized.startsWith("-") ? "-" : "";
      if (unsigned.length > establishedPrecision) {
        normalized = `${sign}${unsigned.slice(0, -establishedPrecision)}.${unsigned.slice(-establishedPrecision)}`;
        numeric = Number(normalized);
      }
    }
    const valid = axis === "x"
      ? numeric >= 100_000 && numeric <= 1_000_000
      : numeric >= 0 && numeric <= 10_000_000;
    return valid && Number.isFinite(numeric)
      ? Object.freeze({ value: numeric, text: normalized })
      : null;
  });
}

function labelsAreContinuous(labels) {
  if (labels.length === 0 || new Set(labels).size !== labels.length) return false;
  if (labels.every(label => /^\d{1,6}$/u.test(label))) {
    return labels.every((label, index) => Number(label) === index + 1);
  }
  if (labels.every(label => /^[A-Z]$/u.test(label))) {
    return labels.every((label, index) => label.charCodeAt(0) === 65 + index);
  }
  return false;
}

function parseCandidateRows(sourceText) {
  const rows = [];
  for (const [lineIndex, sourceLine] of String(sourceText || "").split(/\r?\n/u).entries()) {
    const dms = parseDmsPair(sourceLine);
    if (!dms) continue;
    const prefix = sourceLine.slice(0, dms.firstIndex);
    const numericTokens = prefix.match(/[+-]?\d{1,10}(?:[.,]\d+)?/gu) || [];
    if (numericTokens.length < 3) continue;
    const [labelSource, xSource, ySource] = numericTokens.slice(-3);
    const xToken = decimalToken(xSource);
    const yToken = decimalToken(ySource);
    const label = String(labelSource || "").replace(/^\+/u, "").toUpperCase();
    if (!xToken || !yToken) continue;
    if (!/^(?:\d{1,6}|[A-Z])$/u.test(label)) continue;
    const suffixNumbers = sourceLine.slice(dms.endIndex).match(/\d{4,}/gu) || [];
    if (suffixNumbers.length > 0) continue;
    rows.push({
      label,
      xToken,
      yToken,
      latitude: dms.latitude,
      longitude: dms.longitude,
      latitudeSource: dms.latitudeSource,
      longitudeSource: dms.longitudeSource,
      sourceText: sourceLine.trim(),
      sourceLineNumber: lineIndex + 1
    });
  }
  return rows;
}

export function extractMultiRepresentationSourceEvidence({
  sourceContextText = "",
  sourceContextProvenance = null,
  imageIdentity = null
} = {}) {
  const context = extractProjectedSourceContext(sourceContextText);
  const imageSha256 = String(imageIdentity?.image_sha256 || "");
  const provenanceSha256 = String(sourceContextProvenance?.image_sha256 || "");
  const sourceBound = /^[0-9a-f]{64}$/u.test(imageSha256)
    && provenanceSha256 === imageSha256
    && sourceContextProvenance?.same_request === true;
  const candidates = parseCandidateRows(sourceContextText);
  if (candidates.length === 0) {
    return Object.freeze({
      version: MULTI_REPRESENTATION_SOURCE_EVIDENCE_VERSION,
      status: "NOT_AVAILABLE",
      reason: "MULTI_REPRESENTATION_ROWS_NOT_FOUND",
      rows: Object.freeze([]),
      sourceContextBinding: Object.freeze({ bound: false, image_sha256: null })
    });
  }
  if (!sourceBound || context.status !== "COMPLETE") {
    return Object.freeze({
      version: MULTI_REPRESENTATION_SOURCE_EVIDENCE_VERSION,
      status: "INCOMPLETE",
      reason: !sourceBound ? "IMAGE_IDENTITY_NOT_BOUND" : "PROJECTED_CONTEXT_INCOMPLETE",
      rows: Object.freeze([]),
      sourceContextBinding: Object.freeze({ bound: false, image_sha256: null })
    });
  }
  const byLabel = new Map();
  let conflictingDuplicate = false;
  for (const candidate of candidates) {
    const identity = JSON.stringify([
      candidate.xToken.normalized,
      candidate.yToken.normalized,
      candidate.latitude,
      candidate.longitude
    ]);
    const prior = byLabel.get(candidate.label);
    if (prior && prior.identity !== identity) conflictingDuplicate = true;
    if (!prior) byLabel.set(candidate.label, { identity, candidate });
  }
  const selected = [...byLabel.values()].map(value => value.candidate);
  const labels = selected.map(row => row.label);
  const xValues = normalizeProjectedColumn(selected.map(row => row.xToken), { axis: "x", context });
  const yValues = normalizeProjectedColumn(selected.map(row => row.yToken), { axis: "y", context });
  if (conflictingDuplicate || !labelsAreContinuous(labels) || xValues.some(value => !value) || yValues.some(value => !value)) {
    return Object.freeze({
      version: MULTI_REPRESENTATION_SOURCE_EVIDENCE_VERSION,
      status: "CONFLICT",
      reason: conflictingDuplicate ? "SOURCE_ROW_CONFLICT" : "SOURCE_LABEL_OR_PROJECTED_VALUE_CONFLICT",
      rows: Object.freeze([]),
      sourceContextBinding: Object.freeze({ bound: true, image_sha256: imageSha256 })
    });
  }
  const rows = Object.freeze(selected.map((row, index) => Object.freeze({
    label: row.label,
    x: xValues[index].value,
    y: yValues[index].value,
    xSource: xValues[index].text,
    ySource: yValues[index].text,
    latitude: row.latitude,
    longitude: row.longitude,
    latitudeSource: row.latitudeSource,
    longitudeSource: row.longitudeSource,
    sourceLineNumber: row.sourceLineNumber
  })));
  return Object.freeze({
    version: MULTI_REPRESENTATION_SOURCE_EVIDENCE_VERSION,
    status: "COMPLETE",
    reason: "SAME_IMAGE_MULTI_REPRESENTATION_ROWS_BOUND",
    rows,
    labels: Object.freeze(labels),
    axisOrder: context.axisOrder,
    crsEvidence: context.crsEvidence,
    sourceContextBinding: Object.freeze({
      bound: true,
      image_sha256: imageSha256,
      mode: String(sourceContextProvenance?.mode || "")
    })
  });
}

function closeEnough(left, right, tolerance) {
  return Number.isFinite(Number(left)) && Number.isFinite(Number(right))
    && Math.abs(Number(left) - Number(right)) <= tolerance;
}

function providerDmsRows(reviewEvidence) {
  const grouped = Array.isArray(reviewEvidence?.candidateGroups)
    ? reviewEvidence.candidateGroups.flatMap(group => Array.isArray(group?.rows) ? group.rows : [])
    : [];
  return grouped.length > 0
    ? grouped
    : Array.isArray(reviewEvidence?.unboundCandidates) ? reviewEvidence.unboundCandidates : [];
}

export function bindProviderRepresentationsToSource({
  providerDmsReviewEvidence = null,
  providerProjectedEvidence = null,
  sourceContextText = "",
  sourceContextProvenance = null,
  imageIdentity = null
} = {}) {
  const earlyDmsRows = providerDmsRows(providerDmsReviewEvidence);
  const earlyProjectedRows = providerProjectedEvidence?.status === LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS.COMPLETE
    && Array.isArray(providerProjectedEvidence?.rows)
    ? providerProjectedEvidence.rows
    : [];
  const earlyProviderMode = earlyDmsRows.length > 0 && earlyProjectedRows.length > 0
    ? "BOTH"
    : earlyDmsRows.length > 0 ? "DMS_ONLY" : earlyProjectedRows.length > 0 ? "PROJECTED_ONLY" : "NONE";
  const sourceEvidence = extractMultiRepresentationSourceEvidence({
    sourceContextText,
    sourceContextProvenance,
    imageIdentity
  });
  if (sourceEvidence.status !== "COMPLETE") {
    return Object.freeze({
      version: MULTI_REPRESENTATION_SOURCE_EVIDENCE_VERSION,
      status: sourceEvidence.status,
      reason: sourceEvidence.reason,
      sourceEvidence,
      providerMode: earlyProviderMode,
      rows: Object.freeze([]),
      labels: Object.freeze([])
    });
  }
  let dmsRows = earlyDmsRows;
  const projectedRows = earlyProjectedRows;
  if (dmsRows.length === 0 && projectedRows.length > 0
    && projectedRows.every(row => row?.referenceDms
      && Number.isFinite(Number(row.referenceDms.latitudeDecimal))
      && Number.isFinite(Number(row.referenceDms.longitudeDecimal)))) {
    dmsRows = projectedRows.map(row => Object.freeze({
      sourceLabel: String(row.label || ""),
      latitude: Number(row.referenceDms.latitudeDecimal),
      longitude: Number(row.referenceDms.longitudeDecimal),
      latitudeSource: String(row.referenceDms.latitude || ""),
      longitudeSource: String(row.referenceDms.longitude || ""),
      sourceText: String(row.sourceText || "")
    }));
  }
  const hasDms = dmsRows.length > 0;
  const hasProjected = projectedRows.length > 0;
  if (!hasDms && !hasProjected) {
    return Object.freeze({
      version: MULTI_REPRESENTATION_SOURCE_EVIDENCE_VERSION,
      status: "NOT_APPLICABLE",
      reason: "PROVIDER_REPRESENTATION_NOT_AVAILABLE",
      sourceEvidence,
      providerMode: "NONE",
      rows: Object.freeze([]),
      labels: Object.freeze([])
    });
  }
  const countMatches = (!hasDms || dmsRows.length === sourceEvidence.rows.length)
    && (!hasProjected || projectedRows.length === sourceEvidence.rows.length);
  const dmsMatches = !hasDms || dmsRows.every((row, index) => {
    const sourceRow = sourceEvidence.rows[index];
    return closeEnough(row.latitude, sourceRow.latitude, 0.000002)
      && closeEnough(row.longitude, sourceRow.longitude, 0.000002)
      && (!row.sourceLabel || String(row.sourceLabel).toUpperCase() === sourceRow.label);
  });
  const projectedMatches = !hasProjected || projectedRows.every((row, index) => {
    const sourceRow = sourceEvidence.rows[index];
    return closeEnough(row.x, sourceRow.x, 0.01)
      && closeEnough(row.y, sourceRow.y, 0.01)
      && (!row.label || String(row.label).toUpperCase() === sourceRow.label);
  });
  if (!countMatches || !dmsMatches || !projectedMatches) {
    return Object.freeze({
      version: MULTI_REPRESENTATION_SOURCE_EVIDENCE_VERSION,
      status: "CONFLICT",
      reason: !countMatches ? "PROVIDER_SOURCE_ROW_COUNT_CONFLICT"
        : !dmsMatches ? "PROVIDER_SOURCE_DMS_OR_ORDER_CONFLICT"
          : "PROVIDER_SOURCE_PROJECTED_OR_ORDER_CONFLICT",
      sourceEvidence,
      providerMode: hasDms && hasProjected ? "BOTH" : hasDms ? "DMS_ONLY" : "PROJECTED_ONLY",
      rows: Object.freeze([]),
      labels: Object.freeze([])
    });
  }
  const rows = Object.freeze(sourceEvidence.rows.map((sourceRow, index) => Object.freeze({
    ...sourceRow,
    providerDmsSourceText: hasDms ? String(dmsRows[index]?.sourceText || "") : "",
    providerProjectedSourceText: hasProjected ? String(projectedRows[index]?.sourceText || "") : ""
  })));
  return Object.freeze({
    version: MULTI_REPRESENTATION_SOURCE_EVIDENCE_VERSION,
    status: "COMPLETE",
    reason: "PROVIDER_REPRESENTATION_BOUND_TO_SAME_IMAGE_ROWS",
    sourceEvidence,
    providerMode: hasDms && hasProjected ? "BOTH" : hasDms ? "DMS_ONLY" : "PROJECTED_ONLY",
    rows,
    labels: sourceEvidence.labels,
    candidateDmsText: hasDms
      ? rows.map((row, index) => `${row.label} | ${dmsRows[index].latitudeSource} | ${dmsRows[index].longitudeSource}`).join("\n")
      : ""
  });
}
