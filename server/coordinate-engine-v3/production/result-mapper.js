import {
  V3_PRODUCTION_REASON_CODE,
  V3_PRODUCTION_REASON_PRIORITY,
  V3_PRODUCTION_RESULT_SCHEMA_VERSION,
  V3_PRODUCTION_STATUS,
  V3_TECHNICAL_KML_HARD_BLOCK_REASONS,
} from "./contracts.js";
import {
  getV3ProductionScopeStatus,
  V3_PRODUCTION_SCOPE_STATUS,
  V3_PRODUCTION_SUPPORTED_SCOPE_V1,
} from "./supported-scope.js";

const RUNNER_STATUS = Object.freeze({
  MATCHED: "MATCHED",
  NO_MATCH: "NO_MATCH",
  AMBIGUOUS: "AMBIGUOUS",
  DEADLINE_EXCEEDED: "DEADLINE_EXCEEDED",
  RECOGNIZER_ERROR: "RECOGNIZER_ERROR",
});

const ADAPTER_STATUS = Object.freeze({
  MATCHED_RESULT: "MATCHED_RESULT",
  NO_RECOGNIZER_MATCH: "NO_RECOGNIZER_MATCH",
  AMBIGUOUS_RECOGNIZER_MATCH: "AMBIGUOUS_RECOGNIZER_MATCH",
  MULTIPLE_CANDIDATE_CONFLICT: "MULTIPLE_CANDIDATE_CONFLICT",
});

const ACQUISITION_STATUS = Object.freeze({
  SUCCESS: "SUCCESS",
  PARTIAL: "PARTIAL",
  FAILED: "FAILED",
  DEADLINE_EXCEEDED: "DEADLINE_EXCEEDED",
});

const EXPERIMENTAL_STRATEGY_KEYS = new Set([
  "table_context_composite",
  "full_image_ocr",
  "structural_router",
  "complex_structured_document",
  "indonesia_utm_complex_table_experimental",
  "qwen-vl-ocr-latest",
]);

function cleanString(value, fallback = "") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function cleanArray(value) {
  return Object.freeze((Array.isArray(value) ? value : []).filter((item) => item !== undefined && item !== null));
}

function hasArrayItems(value) {
  return Array.isArray(value) && value.length > 0;
}

function getNormalized(input = {}) {
  return input.normalized || input.adapterResult?.normalized || input.runnerResult?.normalized || null;
}

function getRunnerResult(input = {}) {
  return input.runnerResult || null;
}

function getAdapterResult(input = {}) {
  return input.adapterResult || null;
}

function getAcquisitionResult(input = {}) {
  return input.acquisitionResult || null;
}

function getOwner({ normalized, runnerResult, adapterResult } = {}) {
  return cleanString(
    adapterResult?.recognizerId
      || runnerResult?.recognizerId
      || normalized?.recognizerId,
  ) || null;
}

function getCoordinateType({ normalized, runnerResult, adapterResult } = {}) {
  return cleanString(
    adapterResult?.coordinateType
      || runnerResult?.coordinateType
      || normalized?.coordinateType,
  ) || null;
}

function getTechnicalBlockReason(normalized = {}) {
  return cleanString(normalized?.technicalKmlBlockReason) || null;
}

function coordinatesAreStructurallyValid(coordinates = []) {
  if (!Array.isArray(coordinates) || coordinates.length === 0) return false;
  return coordinates.every((point) => (
    point
    && point.numeric !== false
    && point.latitude !== null
    && point.longitude !== null
    && Number.isFinite(Number(point.latitude))
    && Number.isFinite(Number(point.longitude))
  ));
}

function hasProviderTimeout(acquisitionResult = {}, metadata = {}) {
  const warnings = Array.isArray(acquisitionResult?.warnings) ? acquisitionResult.warnings : [];
  const diagnostics = acquisitionResult?.diagnostics || {};
  return metadata.providerTimeout === true
    || warnings.includes("PROVIDER_TIMEOUT")
    || diagnostics.providerErrorCode === "PROVIDER_TIMEOUT";
}

function candidateCount(acquisitionResult = {}) {
  if (!acquisitionResult || typeof acquisitionResult !== "object") return 0;
  return Array.isArray(acquisitionResult.candidates) ? acquisitionResult.candidates.length : 0;
}

function hasMeaningfulAvailableData(input = {}, normalized = null) {
  const acquisitionResult = getAcquisitionResult(input);
  const adapterResult = getAdapterResult(input);
  const metadata = input.productionMetadata || {};
  return Boolean(
    normalized
    || candidateCount(acquisitionResult) > 0
    || hasArrayItems(adapterResult?.candidateResults)
    || metadata.meaningfulEvidence === true
    || Number(metadata.availableRows) > 0
    || Number(metadata.safeRecoveredRows) > 0
    || Number(metadata.partialRows) > 0,
  );
}

function candidateHasIncompleteRows(candidate = {}) {
  const completeness = candidate?.completeness || {};
  const expected = Number(completeness.expectedRowCount ?? candidate.expectedRowCount);
  const structured = Number(completeness.structuredRowCount ?? candidate.structuredRows?.length);
  return Boolean(
    completeness.truncated === true
    || (Number.isFinite(expected) && Number.isFinite(structured) && expected > structured),
  );
}

function hasIncompleteExtraction(acquisitionResult = {}, metadata = {}) {
  if (metadata.incompleteExtraction === true) return true;
  if (acquisitionResult?.status === ACQUISITION_STATUS.PARTIAL) return true;
  const candidates = Array.isArray(acquisitionResult?.candidates) ? acquisitionResult.candidates : [];
  return candidates.some(candidateHasIncompleteRows);
}

function hasPartialRows(metadata = {}) {
  const expected = Number(metadata.expectedRows);
  const recovered = Number(metadata.availableRows ?? metadata.safeRecoveredRows ?? metadata.partialRows);
  return Number.isFinite(expected)
    && Number.isFinite(recovered)
    && recovered > 0
    && recovered < expected;
}

function hasExperimentalPath(input = {}, scopeStatus) {
  const metadata = input.productionMetadata || {};
  if (metadata.experimental === true || scopeStatus === V3_PRODUCTION_SCOPE_STATUS.EXPERIMENTAL) return true;
  const keys = [
    metadata.strategyId,
    metadata.inputMode,
    metadata.model,
    metadata.path,
    input.acquisitionStrategy?.strategyId,
    input.acquisitionStrategy?.inputMode,
    input.acquisitionStrategy?.model,
  ].map(cleanString).filter(Boolean);
  return keys.some((key) => EXPERIMENTAL_STRATEGY_KEYS.has(key));
}

function collectReasonCodes(input = {}, normalized = null, scopeStatus = V3_PRODUCTION_SCOPE_STATUS.UNSUPPORTED) {
  const runnerResult = getRunnerResult(input);
  const adapterResult = getAdapterResult(input);
  const acquisitionResult = getAcquisitionResult(input);
  const metadata = input.productionMetadata || {};
  const reasons = [];
  const coordinates = Array.isArray(normalized?.coordinates) ? normalized.coordinates : [];
  const meaningfulData = hasMeaningfulAvailableData(input, normalized);

  if (!meaningfulData && !normalized) reasons.push(V3_PRODUCTION_REASON_CODE.NO_USABLE_CANDIDATE);
  if (normalized && !coordinatesAreStructurallyValid(coordinates)) {
    reasons.push(coordinates.length ? V3_PRODUCTION_REASON_CODE.INVALID_COORDINATE_STRUCTURE : V3_PRODUCTION_REASON_CODE.NO_NORMALIZED_COORDINATES);
  }
  if (!normalized && meaningfulData && metadata.recognizerNotAvailable !== true) {
    reasons.push(V3_PRODUCTION_REASON_CODE.NO_NORMALIZED_COORDINATES);
  }
  if (normalized?.technicalKmlReady === false
    && getTechnicalBlockReason(normalized)
    && getTechnicalBlockReason(normalized) !== V3_TECHNICAL_KML_HARD_BLOCK_REASONS.NO_COORDINATES) {
    reasons.push(V3_PRODUCTION_REASON_CODE.INVALID_GEOMETRY);
  }
  if (hasIncompleteExtraction(acquisitionResult, metadata)) reasons.push(V3_PRODUCTION_REASON_CODE.INCOMPLETE_EXTRACTION);
  if (hasPartialRows(metadata)) reasons.push(V3_PRODUCTION_REASON_CODE.PARTIAL_ROWS_RECOVERED);
  if (adapterResult?.status === ADAPTER_STATUS.MULTIPLE_CANDIDATE_CONFLICT) reasons.push(V3_PRODUCTION_REASON_CODE.CANDIDATE_CONFLICT);
  if (adapterResult?.status === ADAPTER_STATUS.AMBIGUOUS_RECOGNIZER_MATCH || runnerResult?.status === RUNNER_STATUS.AMBIGUOUS) {
    reasons.push(V3_PRODUCTION_REASON_CODE.AMBIGUOUS_RECOGNIZER);
  }
  if (metadata.ambiguousCoordinateSystem === true) reasons.push(V3_PRODUCTION_REASON_CODE.AMBIGUOUS_COORDINATE_SYSTEM);
  if (metadata.recognizerNotAvailable === true) reasons.push(V3_PRODUCTION_REASON_CODE.RECOGNIZER_NOT_AVAILABLE);
  if (hasProviderTimeout(acquisitionResult, metadata)) reasons.push(V3_PRODUCTION_REASON_CODE.PROVIDER_TIMEOUT);
  if (hasExperimentalPath(input, scopeStatus)) reasons.push(V3_PRODUCTION_REASON_CODE.EXPERIMENTAL_PATH_REQUIRED);
  if (normalized && scopeStatus === V3_PRODUCTION_SCOPE_STATUS.REVIEW_ONLY) reasons.push(V3_PRODUCTION_REASON_CODE.UNVERIFIED_PRODUCTION_SCOPE);
  if (normalized && scopeStatus === V3_PRODUCTION_SCOPE_STATUS.UNSUPPORTED) reasons.push(V3_PRODUCTION_REASON_CODE.UNSUPPORTED_PRODUCTION_SCOPE);
  if (metadata.lowConfidence === true) reasons.push(V3_PRODUCTION_REASON_CODE.LOW_CONFIDENCE);
  if (runnerResult?.status === RUNNER_STATUS.NO_MATCH && meaningfulData && metadata.recognizerNotAvailable !== true) {
    reasons.push(V3_PRODUCTION_REASON_CODE.RECOGNIZER_NOT_AVAILABLE);
  }
  if (runnerResult?.status === RUNNER_STATUS.RECOGNIZER_ERROR) {
    reasons.push(V3_PRODUCTION_REASON_CODE.UNSUPPORTED_COORDINATE_TYPE);
  }

  return Object.freeze([...new Set(reasons)]);
}

export function chooseV3ProductionReason(reasonCodes = []) {
  const unique = [...new Set(reasonCodes.filter(Boolean))];
  for (const reason of V3_PRODUCTION_REASON_PRIORITY) {
    if (unique.includes(reason)) return reason;
  }
  return unique[0] || null;
}

function determineStatus({ normalized, reasonCode, scopeStatus, experimentalPath }) {
  if (normalized
    && scopeStatus === V3_PRODUCTION_SCOPE_STATUS.SUPPORTED
    && normalized.technicalKmlReady === true
    && !reasonCode
    && experimentalPath !== true) {
    return V3_PRODUCTION_STATUS.SUCCESS;
  }

  if (normalized && normalized.technicalKmlReady === true) {
    return V3_PRODUCTION_STATUS.REVIEW_REQUIRED;
  }

  if ([
    V3_PRODUCTION_REASON_CODE.INCOMPLETE_EXTRACTION,
    V3_PRODUCTION_REASON_CODE.PARTIAL_ROWS_RECOVERED,
    V3_PRODUCTION_REASON_CODE.CANDIDATE_CONFLICT,
    V3_PRODUCTION_REASON_CODE.AMBIGUOUS_RECOGNIZER,
    V3_PRODUCTION_REASON_CODE.AMBIGUOUS_COORDINATE_SYSTEM,
    V3_PRODUCTION_REASON_CODE.RECOGNIZER_NOT_AVAILABLE,
    V3_PRODUCTION_REASON_CODE.PROVIDER_TIMEOUT,
    V3_PRODUCTION_REASON_CODE.EXPERIMENTAL_PATH_REQUIRED,
    V3_PRODUCTION_REASON_CODE.LOW_CONFIDENCE,
    V3_PRODUCTION_REASON_CODE.UNVERIFIED_PRODUCTION_SCOPE,
  ].includes(reasonCode)) {
    return V3_PRODUCTION_STATUS.REVIEW_REQUIRED;
  }

  return V3_PRODUCTION_STATUS.UNSUPPORTED;
}

function buildAvailableData(input = {}, normalized = null) {
  const acquisitionResult = getAcquisitionResult(input);
  const adapterResult = getAdapterResult(input);
  const runnerResult = getRunnerResult(input);
  const metadata = input.productionMetadata || {};
  return Object.freeze({
    acquisitionStatus: acquisitionResult?.status || null,
    adapterStatus: adapterResult?.status || null,
    runnerStatus: runnerResult?.status || null,
    candidateCount: candidateCount(acquisitionResult),
    coordinateCount: Array.isArray(normalized?.coordinates) ? normalized.coordinates.length : 0,
    expectedRows: Number.isFinite(Number(metadata.expectedRows)) ? Number(metadata.expectedRows) : null,
    availableRows: Number.isFinite(Number(metadata.availableRows ?? metadata.safeRecoveredRows ?? metadata.partialRows))
      ? Number(metadata.availableRows ?? metadata.safeRecoveredRows ?? metadata.partialRows)
      : null,
    evidenceLabel: cleanString(metadata.evidenceLabel) || null,
  });
}

function buildMissingRequirement(reasonCode = null) {
  const map = {
    [V3_PRODUCTION_REASON_CODE.INCOMPLETE_EXTRACTION]: "complete_coordinate_extraction",
    [V3_PRODUCTION_REASON_CODE.PARTIAL_ROWS_RECOVERED]: "all_expected_rows",
    [V3_PRODUCTION_REASON_CODE.RECOGNIZER_NOT_AVAILABLE]: "implemented_supported_recognizer",
    [V3_PRODUCTION_REASON_CODE.EXPERIMENTAL_PATH_REQUIRED]: "production_authorized_acquisition_path",
    [V3_PRODUCTION_REASON_CODE.UNVERIFIED_PRODUCTION_SCOPE]: "production_supported_scope_validation",
    [V3_PRODUCTION_REASON_CODE.NO_USABLE_CANDIDATE]: "usable_coordinate_candidate",
    [V3_PRODUCTION_REASON_CODE.NO_NORMALIZED_COORDINATES]: "normalized_coordinates",
    [V3_PRODUCTION_REASON_CODE.INVALID_GEOMETRY]: "valid_geometry",
    [V3_PRODUCTION_REASON_CODE.INVALID_COORDINATE_STRUCTURE]: "valid_coordinate_structure",
  };
  return map[reasonCode] || null;
}

function getKmlState(status, normalized = null) {
  if (status === V3_PRODUCTION_STATUS.UNSUPPORTED) {
    return {
      technicalKmlReady: false,
      technicalKmlBlockReason: getTechnicalBlockReason(normalized) || V3_TECHNICAL_KML_HARD_BLOCK_REASONS.NO_COORDINATES,
    };
  }
  return {
    technicalKmlReady: normalized?.technicalKmlReady === true,
    technicalKmlBlockReason: getTechnicalBlockReason(normalized),
  };
}

export function mapV3ProductionResult(input = {}, {
  scope = V3_PRODUCTION_SUPPORTED_SCOPE_V1,
} = {}) {
  const normalized = getNormalized(input);
  const runnerResult = getRunnerResult(input);
  const adapterResult = getAdapterResult(input);
  const owner = getOwner({ normalized, runnerResult, adapterResult });
  const coordinateType = getCoordinateType({ normalized, runnerResult, adapterResult });
  const scopeStatus = getV3ProductionScopeStatus({ recognizerId: owner, coordinateType, scope });
  const reasonCodes = collectReasonCodes(input, normalized, scopeStatus);
  const experimentalPath = hasExperimentalPath(input, scopeStatus);
  const reasonCode = chooseV3ProductionReason(reasonCodes);
  const status = determineStatus({ normalized, reasonCode, scopeStatus, experimentalPath });
  const kml = getKmlState(status, normalized);
  const coordinates = cleanArray(normalized?.coordinates);

  return Object.freeze({
    schemaVersion: V3_PRODUCTION_RESULT_SCHEMA_VERSION,
    status,
    reasonCode: status === V3_PRODUCTION_STATUS.SUCCESS ? null : reasonCode,
    owner,
    recognizerId: owner,
    coordinateType,
    coordinates,
    geometry: normalized?.geometryType || null,
    crs: normalized?.crs || null,
    precisionMode: normalized?.precisionMode || null,
    technicalKmlReady: kml.technicalKmlReady,
    technicalKmlBlockReason: kml.technicalKmlBlockReason,
    warnings: cleanArray(normalized?.warnings),
    suspectedPoints: cleanArray(normalized?.suspectedPoints),
    availableData: buildAvailableData(input, normalized),
    missingRequirement: status === V3_PRODUCTION_STATUS.SUCCESS ? null : buildMissingRequirement(reasonCode),
    productionSupported: status === V3_PRODUCTION_STATUS.SUCCESS,
    productionScopeStatus: scopeStatus,
    reasonCodes,
  });
}
