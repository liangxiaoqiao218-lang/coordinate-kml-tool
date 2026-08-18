export const ACQUISITION_SCHEMA_VERSION = "coordinate_engine_v3_acquisition_v1";
export const ACQUISITION_CANDIDATE_SCHEMA_VERSION = "coordinate_engine_v3_acquisition_candidate_v1";

export const ACQUISITION_STATUS = Object.freeze({
  SUCCESS: "SUCCESS",
  PARTIAL: "PARTIAL",
  FAILED: "FAILED",
  DEADLINE_EXCEEDED: "DEADLINE_EXCEEDED",
});

export const ACQUISITION_SOURCE_TYPE = Object.freeze({
  WHOLE_IMAGE: "whole_image",
  TABLE: "table",
  TEXT_BLOCK: "text_block",
  COORDINATE_BLOCK: "coordinate_block",
  TARGETED_REGION: "targeted_region",
});

export const ACQUISITION_PROVENANCE = Object.freeze({
  PRIMARY: "primary",
  TARGETED: "targeted",
  LOCAL_FALLBACK: "local_fallback",
});

export const ACQUISITION_AUTHORITY_FIELDS = Object.freeze([
  "recognizerId",
  "coordinateType",
  "winner",
  "owner",
  "confirmationStatus",
  "qualityGateStatus",
  "kmlReady",
  "kmlPermission",
  "shadowWinner",
  "arbitrationProposal",
  "migrationStatus",
]);

export const ACQUISITION_SENSITIVE_FIELDS = Object.freeze([
  "apiKey",
  "credentials",
  "rawPrompt",
  "rawProviderResponse",
  "base64",
  "imageBase64",
  "imageBytes",
  "filesystemPath",
  "filePath",
  "localPath",
  "prompt",
  "providerResponse",
]);

const SOURCE_TYPES = new Set(Object.values(ACQUISITION_SOURCE_TYPE));
const PROVENANCE_VALUES = new Set(Object.values(ACQUISITION_PROVENANCE));
const STATUS_VALUES = new Set(Object.values(ACQUISITION_STATUS));
const AUTHORITY_FIELD_SET = new Set(ACQUISITION_AUTHORITY_FIELDS);
const SENSITIVE_FIELD_SET = new Set(ACQUISITION_SENSITIVE_FIELDS);

function cleanString(value, fallback = "") {
  const text = String(value ?? "")
    .replace(/[\r\n\t]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return text || fallback;
}

function cleanLongText(value, fallback = "") {
  const text = String(value ?? "")
    .replace(/\u0000/g, "")
    .replace(/[ \t]+/g, " ")
    .trim();
  return text || fallback;
}

function cleanStringArray(value) {
  if (!Array.isArray(value)) return Object.freeze([]);
  return Object.freeze(value
    .map((item) => cleanString(item))
    .filter(Boolean));
}

function clampConfidence(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return null;
  return Math.max(0, Math.min(1, number));
}

function cleanDuration(value) {
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? number : 0;
}

function hasOwn(object, key) {
  return Object.prototype.hasOwnProperty.call(Object(object), key);
}

function listPresentFields(value, fieldSet) {
  if (!value || typeof value !== "object") return [];
  return Object.keys(value).filter((key) => fieldSet.has(key));
}

function sanitizePrimitive(value) {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "boolean") return value;
  if (typeof value === "string") return cleanString(value);
  if (value === null || value === undefined) return null;
  return cleanString(value);
}

function sanitizePlainObject(value, depth = 0) {
  if (!value || typeof value !== "object" || depth > 2) return sanitizePrimitive(value);
  if (Array.isArray(value)) {
    return Object.freeze(value.map((item) => sanitizePlainObject(item, depth + 1)));
  }

  const output = {};
  for (const [key, raw] of Object.entries(value)) {
    if (SENSITIVE_FIELD_SET.has(key)) continue;
    if (AUTHORITY_FIELD_SET.has(key)) continue;
    output[cleanString(key)] = sanitizePlainObject(raw, depth + 1);
  }
  return Object.freeze(output);
}

function sanitizeRows(value) {
  if (!Array.isArray(value)) return Object.freeze([]);
  return Object.freeze(value
    .filter((row) => row && typeof row === "object")
    .map((row) => sanitizePlainObject(row)));
}

function sanitizeTiming(value = {}) {
  const timing = value && typeof value === "object" ? value : {};
  return Object.freeze({
    durationMs: cleanDuration(timing.durationMs),
  });
}

function sanitizeAcquisitionTiming(value = {}) {
  const timing = value && typeof value === "object" ? value : {};
  return Object.freeze({
    totalDurationMs: cleanDuration(timing.totalDurationMs),
    primaryDurationMs: cleanDuration(timing.primaryDurationMs),
    targetedDurationMs: cleanDuration(timing.targetedDurationMs),
  });
}

function sanitizeCropRegion(value = {}) {
  if (!value || typeof value !== "object") return null;
  const cropRegion = {
    x: Number(value.x),
    y: Number(value.y),
    width: Number(value.width),
    height: Number(value.height),
    coordinateSpace: cleanString(value.coordinateSpace, "image"),
  };
  if (![cropRegion.x, cropRegion.y, cropRegion.width, cropRegion.height].every(Number.isFinite)) {
    return null;
  }
  return Object.freeze({
    x: Math.max(0, cropRegion.x),
    y: Math.max(0, cropRegion.y),
    width: Math.max(0, cropRegion.width),
    height: Math.max(0, cropRegion.height),
    coordinateSpace: cropRegion.coordinateSpace,
  });
}

function sanitizeCompleteness(value = {}, fallback = {}) {
  const source = value && typeof value === "object" ? value : {};
  const expectedRowCount = Number(source.expectedRowCount ?? fallback.expectedRowCount);
  const structuredRowCount = Number(source.structuredRowCount ?? fallback.structuredRowCount);
  return Object.freeze({
    expectedRowCount: Number.isFinite(expectedRowCount) && expectedRowCount > 0 ? expectedRowCount : null,
    structuredRowCount: Number.isFinite(structuredRowCount) && structuredRowCount >= 0 ? structuredRowCount : null,
    truncated: source.truncated === true || fallback.truncated === true,
  });
}

function createStableId(value = {}) {
  const basis = JSON.stringify({
    text: cleanLongText(value.text),
    structuredRows: sanitizeRows(value.structuredRows),
    headers: cleanStringArray(value.headers),
    sourceType: value.sourceType,
    provenance: value.provenance,
  });
  let hash = 0;
  for (let index = 0; index < basis.length; index += 1) {
    hash = ((hash << 5) - hash) + basis.charCodeAt(index);
    hash |= 0;
  }
  return `candidate_${Math.abs(hash).toString(36)}`;
}

export function validateAcquisitionCandidate(value = {}) {
  const errors = [];
  const authorityFields = listPresentFields(value, AUTHORITY_FIELD_SET);
  if (authorityFields.length) errors.push("candidate_contains_authority_fields");
  if (!SOURCE_TYPES.has(value.sourceType)) errors.push("invalid_source_type");
  if (!PROVENANCE_VALUES.has(value.provenance)) errors.push("invalid_provenance");
  const confidence = clampConfidence(value.confidence);
  if (value.confidence !== undefined && confidence === null) errors.push("invalid_confidence");
  return Object.freeze({
    valid: errors.length === 0,
    errors: Object.freeze(errors),
    authorityFields: Object.freeze(authorityFields),
  });
}

export function createAcquisitionCandidate(value = {}) {
  const validation = validateAcquisitionCandidate(value);
  if (!validation.valid) {
    throw new Error(`Invalid acquisition candidate: ${validation.errors.join(",")}`);
  }
  return Object.freeze({
    schemaVersion: ACQUISITION_CANDIDATE_SCHEMA_VERSION,
    id: cleanString(value.id, createStableId(value)),
    text: cleanLongText(value.text),
    structuredRows: sanitizeRows(value.structuredRows),
    headers: cleanStringArray(value.headers),
    documentCues: cleanStringArray(value.documentCues),
    sourceType: value.sourceType,
    provenance: value.provenance,
    confidence: clampConfidence(value.confidence),
    timing: sanitizeTiming(value.timing),
    cropRegion: sanitizeCropRegion(value.cropRegion),
    completeness: sanitizeCompleteness(value.completeness, {
      expectedRowCount: value.expectedRowCount,
      structuredRowCount: Array.isArray(value.structuredRows) ? value.structuredRows.length : null,
      truncated: value.truncated,
    }),
  });
}

export function validateAcquisitionResult(value = {}) {
  const errors = [];
  if (!STATUS_VALUES.has(value.status)) errors.push("invalid_acquisition_status");
  if (!Array.isArray(value.candidates)) errors.push("candidates_not_array");
  const providerCalls = Number(value.providerCalls);
  if (!Number.isFinite(providerCalls) || providerCalls < 0) errors.push("invalid_provider_calls");
  if (providerCalls > 2) errors.push("provider_call_limit_exceeded");
  return Object.freeze({
    valid: errors.length === 0,
    errors: Object.freeze(errors),
  });
}

export function createAcquisitionResult(value = {}) {
  const candidateInputs = Array.isArray(value.candidates) ? value.candidates : [];
  const candidates = candidateInputs.map((candidate) => (
    candidate?.schemaVersion === ACQUISITION_CANDIDATE_SCHEMA_VERSION
      ? candidate
      : createAcquisitionCandidate(candidate)
  ));
  const result = {
    schemaVersion: ACQUISITION_SCHEMA_VERSION,
    status: value.status,
    candidates: Object.freeze(candidates),
    timing: sanitizeAcquisitionTiming(value.timing),
    providerCalls: Math.max(0, Number(value.providerCalls) || 0),
    warnings: cleanStringArray(value.warnings),
  };
  const validation = validateAcquisitionResult(result);
  if (!validation.valid) {
    throw new Error(`Invalid acquisition result: ${validation.errors.join(",")}`);
  }
  return Object.freeze(result);
}

export function acquisitionCandidateToRunnerInput(candidate = {}) {
  return Object.freeze({
    text: cleanLongText(candidate.text),
    rawText: cleanLongText(candidate.text),
    structuredRows: Array.isArray(candidate.structuredRows) ? candidate.structuredRows : Object.freeze([]),
    tableRows: Array.isArray(candidate.structuredRows) ? candidate.structuredRows : Object.freeze([]),
    headers: Array.isArray(candidate.headers) ? candidate.headers : Object.freeze([]),
    documentCues: Array.isArray(candidate.documentCues) ? candidate.documentCues : Object.freeze([]),
    acquisitionCandidateId: cleanString(candidate.id),
    acquisitionSourceType: cleanString(candidate.sourceType),
    acquisitionProvenance: cleanString(candidate.provenance),
  });
}

export function getCandidateDedupeKey(candidate = {}) {
  return JSON.stringify({
    text: cleanLongText(candidate.text).replace(/\s+/g, " "),
    structuredRows: Array.isArray(candidate.structuredRows) ? candidate.structuredRows : [],
    headers: Array.isArray(candidate.headers) ? candidate.headers : [],
  });
}

export function dedupeAcquisitionCandidates(candidates = []) {
  const seen = new Set();
  const deduped = [];
  const duplicateCandidateIds = [];
  for (const candidate of candidates) {
    const key = getCandidateDedupeKey(candidate);
    if (seen.has(key)) {
      duplicateCandidateIds.push(candidate.id);
      continue;
    }
    seen.add(key);
    deduped.push(candidate);
  }
  return Object.freeze({
    candidates: Object.freeze(deduped),
    duplicateCandidateIds: Object.freeze(duplicateCandidateIds),
  });
}

export function calculateCandidateCompleteness(candidate = {}) {
  const structuredRows = Array.isArray(candidate.structuredRows) ? candidate.structuredRows : [];
  const headers = Array.isArray(candidate.headers) ? candidate.headers : [];
  const expectedRowCount = Number(candidate.expectedRowCount ?? candidate.completeness?.expectedRowCount);
  const truncated = candidate.truncated === true || candidate.completeness?.truncated === true;
  const structuredRowCount = structuredRows.length;
  return Object.freeze({
    candidateId: cleanString(candidate.id),
    structuredRowCount,
    hasText: Boolean(cleanLongText(candidate.text)),
    hasHeaders: headers.length > 0,
    expectedRowCount: Number.isFinite(expectedRowCount) && expectedRowCount > 0 ? expectedRowCount : null,
    truncated,
    incompleteStructuredCandidate: Boolean(
      headers.length > 0
      && (
        truncated
        || (Number.isFinite(expectedRowCount) && expectedRowCount > structuredRowCount)
      ),
    ),
  });
}

export function stripSensitiveAcquisitionMetadata(value = {}) {
  return sanitizePlainObject(value);
}
