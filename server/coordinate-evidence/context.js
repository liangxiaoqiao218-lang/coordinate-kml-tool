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

function normalizeNumber(value, fallback = null) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
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

function normalizeDmsCoordinatePoint(value = {}, index = 0) {
  const lat = Number(value.lat);
  const lon = Number(value.lon);
  return Object.freeze({
    point: cleanString(value.point || value.label || value.id, String(index + 1)),
    lat: Number.isFinite(lat) ? Number(lat.toFixed(9)) : null,
    lon: Number.isFinite(lon) ? Number(lon.toFixed(9)) : null,
    source: cleanString(value.source || "dms_deterministic")
  });
}

function normalizeDmsToken(value = {}) {
  return Object.freeze({
    role: cleanString(value.role),
    degrees: Number.isFinite(Number(value.degrees)) ? Number(value.degrees) : null,
    minutes: Number.isFinite(Number(value.minutes)) ? Number(value.minutes) : null,
    seconds: Number.isFinite(Number(value.seconds)) ? Number(value.seconds) : null,
    hemisphere: cleanString(value.hemisphere)
  });
}

function normalizeDmsSourceRow(value = {}, index = 0) {
  return Object.freeze({
    point: cleanString(value.point, String(index + 1)),
    latitude: normalizeDmsToken(value.latitude || {}),
    longitude: normalizeDmsToken(value.longitude || {})
  });
}

function normalizeDmsCoordinateInterpretation(value = {}) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const normalizedCoordinates = Array.isArray(value.normalizedCoordinates)
    ? value.normalizedCoordinates.map(normalizeDmsCoordinatePoint)
      .filter(point => point.lat !== null && point.lon !== null)
    : [];
  const sourceRows = Array.isArray(value.sourceRows)
    ? value.sourceRows.map(normalizeDmsSourceRow)
    : [];
  const status = cleanString(value.interpretationStatus || value.status, "INCOMPLETE").toUpperCase();
  const normalizedStatus = ["COMPLETE", "INCOMPLETE", "INVALID"].includes(status) ? status : "INCOMPLETE";
  return Object.freeze({
    schemaVersion: cleanString(value.schemaVersion || "dms_coordinate_interpretation_v1"),
    interpretationStatus: normalizedStatus,
    deterministicConversion: normalizeBoolean(value.deterministicConversion),
    hemisphereResolved: normalizeBoolean(value.hemisphereResolved),
    pointCount: normalizeCount(value.pointCount || normalizedCoordinates.length),
    normalizedCoordinates: Object.freeze(normalizedCoordinates),
    sourceRows: Object.freeze(sourceRows),
    errors: Object.freeze(sanitizeArray(value.errors)),
    affectsLegacyWinner: false,
    affectsCoordinateResult: false,
    affectsKml: false
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
    geometryType: cleanString(value.geometryType, "unknown"),
    coordinateInterpretation: normalizeDmsCoordinateInterpretation(
      value.coordinateInterpretation || value.structuredDmsInterpretation || {}
    )
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

function normalizeVerificationPoint(value = {}, index = 0) {
  const projected = value.projected || {};
  const transformed = value.transformed || {};
  const reference = value.reference || {};
  return Object.freeze({
    point: cleanString(value.point, String(index + 1)),
    projected: Object.freeze({
      x: normalizeNumber(projected.x ?? value.projectedX ?? value.easting),
      y: normalizeNumber(projected.y ?? value.projectedY ?? value.northing)
    }),
    transformed: Object.freeze({
      latitude: normalizeNumber(transformed.latitude ?? value.transformedLatitude),
      longitude: normalizeNumber(transformed.longitude ?? value.transformedLongitude)
    }),
    reference: Object.freeze({
      latitude: normalizeNumber(reference.latitude ?? value.referenceLatitude),
      longitude: normalizeNumber(reference.longitude ?? value.referenceLongitude)
    }),
    latitudeDifference: normalizeNumber(value.latitudeDifference),
    longitudeDifference: normalizeNumber(value.longitudeDifference),
    maximumDifference: normalizeNumber(value.maximumDifference),
    status: cleanString(value.status, "not_available"),
    referenceSource: cleanString(value.referenceSource, "other"),
    referenceMergeMode: cleanString(value.referenceMergeMode)
  });
}

function normalizeTransformationVerification(value = {}) {
  const rows = Array.isArray(value.pointLevelVerification)
    ? value.pointLevelVerification
    : Array.isArray(value.rows)
      ? value.rows
      : [];
  const mismatchedPointLabels = Array.isArray(value.mismatchedPointLabels)
    ? value.mismatchedPointLabels
    : rows.filter(row => row?.status === "mismatch").map(row => row?.point);
  const pointLevelVerification = rows.map(normalizeVerificationPoint);
  return Object.freeze({
    status: cleanString(value.status),
    tolerance: normalizeNumber(value.tolerance),
    comparedRows: normalizeCount(value.comparedRows || pointLevelVerification.filter(row => row.status !== "not_available").length),
    matchedRows: normalizeCount(value.matchedRows || pointLevelVerification.filter(row => row.status === "match").length),
    mismatchedRows: normalizeCount(value.mismatchedRows || pointLevelVerification.filter(row => row.status === "mismatch").length),
    mismatchedPointLabels: Object.freeze(sanitizeArray(mismatchedPointLabels)),
    maximumDifference: normalizeNumber(value.maximumDifference),
    pointLevelVerification: Object.freeze(pointLevelVerification)
  });
}

function normalizeStructuredUtmTable(value = {}) {
  const table = value.structuredUtmTable || value.structuredUtmPriority || value;
  const verification = table.transformationVerification || {};
  return Object.freeze({
    accepted: normalizeBoolean(table.accepted),
    reason: cleanString(table.reason),
    rowCount: normalizeCount(table.rowCount || table.table?.rows?.length),
    transformationStatus: cleanString(
      verification?.status
      || table.transformationStatus
      || ""
    ),
    transformationVerification: normalizeTransformationVerification(verification)
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

function normalizeGeographicHeaderVisionContext(value = {}) {
  const semantic = value.semantic && typeof value.semantic === "object" ? value.semantic : {};
  const confidence = semantic.confidence && typeof semantic.confidence === "object"
    ? semantic.confidence.level
    : semantic.confidence;
  return Object.freeze({
    schemaVersion: cleanString(value.schemaVersion || "geographic_header_vision_v1"),
    status: cleanString(value.status || "not_run"),
    observationCount: normalizeCount(value.observationCount || (Array.isArray(value.observations) ? value.observations.length : 0)),
    semantic: Object.freeze({
      evidenceType: cleanString(semantic.evidenceType || "geographic_header_semantic"),
      detected: normalizeBoolean(semantic.detected),
      hasLatitudeHeader: normalizeBoolean(semantic.hasLatitudeHeader),
      hasLongitudeHeader: normalizeBoolean(semantic.hasLongitudeHeader),
      hasHemisphereIndicator: normalizeBoolean(semantic.hasHemisphereIndicator),
      latitudeIndicators: sanitizeArray(semantic.latitudeIndicators),
      longitudeIndicators: sanitizeArray(semantic.longitudeIndicators),
      coordinateOrder: cleanString(semantic.coordinateOrder || "unknown"),
      confidence: cleanString(confidence || "low"),
      reason: cleanString(semantic.reason || semantic.confidenceReason || "").slice(0, 160)
    }),
    affectsLegacyWinner: false,
    affectsCoordinateResult: false,
    affectsKml: false
  });
}

function normalizeCadastralSemanticVisionContext(value = {}) {
  return Object.freeze({
    schemaVersion: cleanString(value.schemaVersion || "cadastral_semantic_vision_v1"),
    status: cleanString(value.status || "not_run"),
    detected: normalizeBoolean(value.detected),
    tableType: cleanString(value.tableType || "unknown"),
    indicators: sanitizeArray(value.indicators),
    layoutHints: Object.freeze({
      hasListeCarres: normalizeBoolean(value.layoutHints?.hasListeCarres),
      hasCadastralGrid: normalizeBoolean(value.layoutHints?.hasCadastralGrid),
      hasTableStructure: normalizeBoolean(value.layoutHints?.hasTableStructure)
    }),
    confidence: cleanString(value.confidence || "low"),
    reason: cleanString(value.reason || "").slice(0, 160),
    affectsLegacyWinner: false,
    affectsCoordinateResult: false,
    affectsKml: false
  });
}

export function createPreDecisionEvidenceContext(value = {}) {
  return Object.freeze({
    schemaVersion: PRE_DECISION_EVIDENCE_CONTEXT_SCHEMA_VERSION,
    dms: normalizeDmsContext(value.dms || value),
    cadastral: normalizeCadastralContext(value.cadastral || value),
    utm: normalizeUtmContext(value.utm || value),
    geographicHeaderVision: normalizeGeographicHeaderVisionContext(value.geographicHeaderVision || {}),
    cadastralSemanticVision: normalizeCadastralSemanticVisionContext(value.cadastralSemanticVision || {}),
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
      geometryType: candidates.geometryType,
      coordinateInterpretation: candidates.coordinateInterpretation || candidates.structuredDmsInterpretation
    },
    cadastral: candidates.cadastralGrid || candidates.cadastral || {},
    utm: {
      crsEvidenceShadow: candidates.crsEvidenceShadow || candidates.crsEvidence,
      structuredUtmTable: candidates.structuredUtmTable || candidates.structuredUtmPriority,
      explicitUtmEvidenceLock: candidates.explicitUtmEvidenceLock
    },
    geographicHeaderVision: candidates.geographicHeaderVision,
    cadastralSemanticVision: candidates.cadastralSemanticVision,
    suppression
  });
}

export function sanitizePreDecisionEvidenceContext(context = {}) {
  return createPreDecisionEvidenceContext(sanitizePlainValue(context) || {});
}
