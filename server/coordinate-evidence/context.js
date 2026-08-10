export const PRE_DECISION_EVIDENCE_CONTEXT_SCHEMA_VERSION = "pre_decision_evidence_context_v1";

const SECRET_KEY_PATTERN = /api[_-]?key|secret|token|password|authorization|credential|env|raw[_-]?ocr|rawtext|prompt|modelresponse|fullresponse|image|buffer|base64/i;
const SECRET_VALUE_PATTERN = /(sk-[a-z0-9_-]{8,}|dashscope[_-]?[a-z0-9_-]*|supabase[_-]?[a-z0-9_-]*|bearer\s+[a-z0-9._-]+|api[_-]?key\s*[:=]|secret\s*[:=]|token\s*[:=]|password\s*[:=]|authorization\s*[:=])/ig;

function cleanString(value = "", fallback = "") {
  const text = String(value ?? "")
    .replace(SECRET_VALUE_PATTERN, "[REDACTED]")
    .trim();
  return text || fallback;
}

function normalizeBoolean(value, fallback = false) {
  return value === true || value === false ? value : fallback;
}

function normalizeCount(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? Math.max(0, Math.trunc(number)) : fallback;
}

function sanitizeArray(value = []) {
  return Array.isArray(value)
    ? value.map(item => cleanString(item)).filter(Boolean)
    : [];
}

function sanitizePlainValue(value) {
  if (value === null || value === undefined) return value;
  if (typeof value === "string") return cleanString(value);
  if (typeof value === "number" || typeof value === "boolean") return value;
  if (Array.isArray(value)) return value.map(sanitizePlainValue);
  if (typeof value === "object") {
    return Object.freeze(Object.fromEntries(Object.entries(value)
      .filter(([key]) => !SECRET_KEY_PATTERN.test(key))
      .map(([key, nestedValue]) => [key, sanitizePlainValue(nestedValue)])));
  }
  return null;
}

function normalizeHandwrittenDms(value = {}) {
  return Object.freeze({
    isHandwrittenDms: normalizeBoolean(value.isHandwrittenDms),
    pointRows: normalizeCount(value.pointRows || value.rowCount || value.pointCount)
  });
}

function normalizeFrenchPerimeterDms(value = {}) {
  const points = Array.isArray(value.points) ? value.points : [];
  return Object.freeze({
    isFrenchPerimeterDms: normalizeBoolean(value.isFrenchPerimeterDms),
    pointCount: normalizeCount(value.pointCount || points.length)
  });
}

function normalizeDmsContext(value = {}) {
  return Object.freeze({
    dmsAccepted: normalizeBoolean(value.dmsAccepted),
    dmsGroupedAccepted: normalizeBoolean(value.dmsGroupedAccepted),
    pointAzDmsTableAccepted: normalizeBoolean(value.pointAzDmsTableAccepted),
    handwrittenDms: normalizeHandwrittenDms(value.handwrittenDms || {}),
    frenchPerimeterDms: normalizeFrenchPerimeterDms(value.frenchPerimeterDms || {}),
    hasExplicitHemisphere: normalizeBoolean(value.hasExplicitHemisphere),
    hasExplicitCoordinateOrder: normalizeBoolean(value.hasExplicitCoordinateOrder),
    sourceHint: cleanString(value.sourceHint).slice(0, 160),
    pointCount: normalizeCount(value.pointCount),
    groupCount: normalizeCount(value.groupCount),
    geometryType: cleanString(value.geometryType, "unknown")
  });
}

function getCadastralRows(value = {}) {
  return Array.isArray(value.rows)
    ? value.rows
    : Array.isArray(value.cadastralGrid?.rows)
      ? value.cadastralGrid.rows
      : [];
}

function normalizeCadastralContext(value = {}) {
  const rows = getCadastralRows(value);
  return Object.freeze({
    isCadastralGrid: normalizeBoolean(value.isCadastralGrid || value.cadastralGrid?.isCadastralGrid),
    rowCount: normalizeCount(value.rowCount || value.cadastralGrid?.rowCount || rows.length),
    hasNumXvYvHeader: normalizeBoolean(value.hasNumXvYvHeader),
    geometryType: cleanString(value.geometryType, "cadastral_table")
  });
}

function normalizeCrsIntent(value = {}) {
  const intent = value.shadowIntent || value;
  return Object.freeze({
    projection: cleanString(intent.projection),
    datum: cleanString(intent.datum),
    zone: Number.isInteger(Number(intent.zone)) ? Number(intent.zone) : null,
    hemisphere: cleanString(intent.hemisphere),
    confidence: cleanString(intent.confidence),
    conflicts: sanitizePlainValue(Array.isArray(intent.conflicts) ? intent.conflicts : [])
  });
}

function normalizeStructuredUtmTable(value = {}) {
  const table = value.structuredUtmTable || value.structuredUtmPriority || value;
  return Object.freeze({
    accepted: normalizeBoolean(table.accepted),
    reason: cleanString(table.reason),
    rowCount: normalizeCount(table.rowCount || table.table?.rows?.length),
    transformationStatus: cleanString(
      table.transformationVerification?.status
      || table.transformationStatus
      || ""
    )
  });
}

function normalizeUtmContext(value = {}) {
  return Object.freeze({
    crsEvidenceShadow: value.crsEvidenceShadow || value.crsEvidence
      ? Object.freeze({
          shadowIntent: normalizeCrsIntent(value.crsEvidenceShadow?.shadowIntent || value.crsEvidence?.shadowIntent || value.crsEvidenceShadow || value.crsEvidence)
        })
      : null,
    structuredUtmTable: normalizeStructuredUtmTable(value.structuredUtmTable || value.structuredUtmPriority || {}),
    explicitUtmEvidenceLock: normalizeBoolean(value.explicitUtmEvidenceLock)
  });
}

function normalizeSuppressionContext(value = {}) {
  return Object.freeze({
    utmEvidenceLockApplied: normalizeBoolean(value.utmEvidenceLockApplied || value.explicitUtmEvidenceLock),
    suppressedFallbacks: sanitizeArray(value.suppressedFallbacks),
    reason: cleanString(value.reason).slice(0, 160)
  });
}

export function createPreDecisionEvidenceContext(value = {}) {
  return Object.freeze({
    schemaVersion: PRE_DECISION_EVIDENCE_CONTEXT_SCHEMA_VERSION,
    dms: normalizeDmsContext(value.dms || value),
    cadastral: normalizeCadastralContext(value.cadastral || value),
    utm: normalizeUtmContext(value.utm || value),
    suppression: normalizeSuppressionContext(value.suppression || value)
  });
}

export function snapshotPreSuppressionCandidates(candidates = {}, suppression = {}) {
  return createPreDecisionEvidenceContext({
    dms: {
      dmsAccepted: candidates.dmsAccepted,
      dmsGroupedAccepted: candidates.dmsGroupedAccepted,
      pointAzDmsTableAccepted: candidates.pointAzDmsTableAccepted,
      handwrittenDms: candidates.handwrittenDms,
      frenchPerimeterDms: candidates.frenchPerimeterDms,
      hasExplicitHemisphere: candidates.hasExplicitHemisphere,
      hasExplicitCoordinateOrder: candidates.hasExplicitCoordinateOrder,
      sourceHint: candidates.sourceHint,
      pointCount: candidates.pointCount,
      groupCount: candidates.groupCount,
      geometryType: candidates.geometryType
    },
    cadastral: candidates.cadastralGrid || candidates.cadastral || {},
    utm: {
      crsEvidenceShadow: candidates.crsEvidenceShadow || candidates.crsEvidence,
      structuredUtmTable: candidates.structuredUtmTable || candidates.structuredUtmPriority,
      explicitUtmEvidenceLock: candidates.explicitUtmEvidenceLock
    },
    suppression
  });
}

export function sanitizePreDecisionEvidenceContext(context = {}) {
  return createPreDecisionEvidenceContext(sanitizePlainValue(context) || {});
}
