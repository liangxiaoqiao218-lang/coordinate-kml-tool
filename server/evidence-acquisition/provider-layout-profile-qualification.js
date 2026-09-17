import { createHash } from "node:crypto";
import { isCanonicalCoordinateImageIdentity } from "../recognition/coordinate-image-safety.js";

export const PROVIDER_LAYOUT_PROFILE_QUALIFICATION_SCHEMA_VERSION = "provider_layout_profile_qualification_v1";
export const PROVIDER_LAYOUT_PROFILE_QUALIFICATION_COLLECTOR_VERSION = "P0E_E1_V1";
export const PROVIDER_LAYOUT_PROFILE_QUALIFICATION_TTL_MS = 5 * 60 * 1000;
export const PROVIDER_LAYOUT_PROFILE_QUALIFICATION_STATUS = Object.freeze({
  NO_STRUCTURED_LAYOUT: "NO_STRUCTURED_LAYOUT",
  RESPONSE_CONTRACT_UNSUPPORTED: "RESPONSE_CONTRACT_UNSUPPORTED",
  COORDINATE_SPACE_UNPROVEN: "COORDINATE_SPACE_UNPROVEN",
  QUALIFICATION_CANDIDATE: "QUALIFICATION_CANDIDATE",
  QUALIFICATION_CONFLICT: "QUALIFICATION_CONFLICT"
});

export const PROVIDER_LAYOUT_RESPONSE_CONTRACT = Object.freeze({
  DASHSCOPE_STRUCTURED_LAYOUT_V1: "DASHSCOPE_STRUCTURED_LAYOUT_V1"
});

const SERVER_PROVIDER_LAYOUT_QUALIFICATION_CAPABILITY = Symbol("SERVER_PROVIDER_LAYOUT_QUALIFICATION_CAPABILITY");
const SERVER_QUALIFICATION_IMAGE_SHA256_BINDING = Symbol("SERVER_QUALIFICATION_IMAGE_SHA256_BINDING");
const SUPPORTED_PROVIDER = "ALIYUN_DASHSCOPE";
const SUPPORTED_MODEL_FAMILY = "QWEN_VL";
const SUPPORTED_CONTRACTS = new Set(Object.values(PROVIDER_LAYOUT_RESPONSE_CONTRACT));
const ORIGINAL_IMAGE_PIXELS = "ORIGINAL_IMAGE_PIXELS";
const CONTAINERS = Object.freeze([
  ["ocr_result.words_info", response => response?.ocr_result?.words_info],
  ["ocrResult.words_info", response => response?.ocrResult?.words_info],
  ["output.ocr_result.words_info", response => response?.output?.ocr_result?.words_info],
  ["output.layout", response => response?.output?.layout]
]);
const FIXED_STATUSES = new Set(Object.values(PROVIDER_LAYOUT_PROFILE_QUALIFICATION_STATUS));

function text(value) {
  return String(value ?? "").trim();
}

function sha256(value) {
  return createHash("sha256").update(String(value)).digest("hex");
}

function deepFreeze(value) {
  if (!value || typeof value !== "object" || Object.isFrozen(value)) return value;
  Object.values(value).forEach(deepFreeze);
  return Object.freeze(value);
}

function stableValue(value) {
  if (Array.isArray(value)) return value.map(stableValue);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.keys(value).sort().map(key => [key, stableValue(value[key])]));
}

function digest(value) {
  return sha256(JSON.stringify(stableValue(value)));
}

function typeOf(value) {
  if (Array.isArray(value)) return "array";
  if (value === null) return "null";
  return typeof value;
}

function modelFamily(modelName) {
  return /^qwen-vl(?:-|$)/i.test(text(modelName)) ? SUPPORTED_MODEL_FAMILY : "UNSUPPORTED";
}

function imageBinding(imageIdentity) {
  if (!isCanonicalCoordinateImageIdentity(imageIdentity)) return null;
  return {
    schema_version: imageIdentity.schema_version,
    image_sha256: imageIdentity.image_sha256,
    byte_length: imageIdentity.byte_length,
    mime_type: imageIdentity.mime_type,
    width: imageIdentity.width,
    height: imageIdentity.height,
    page: imageIdentity.page,
    image_id: imageIdentity.image_id,
    request_asset_id: imageIdentity.request_asset_id
  };
}

function candidateCoordinateSpace(candidate) {
  return text(candidate?.bbox_coordinate_space ?? candidate?.coordinate_space ?? candidate?.coordinateSpace).toUpperCase();
}

function candidatePage(candidate) {
  return Number(candidate?.page ?? candidate?.page_number ?? candidate?.pageNumber);
}

function candidateWidth(candidate) {
  return Number(candidate?.image_width ?? candidate?.imageWidth ?? candidate?.source_width ?? candidate?.sourceWidth);
}

function candidateHeight(candidate) {
  return Number(candidate?.image_height ?? candidate?.imageHeight ?? candidate?.source_height ?? candidate?.sourceHeight);
}

function candidateBbox(candidate) {
  const bbox = Array.isArray(candidate?.bbox) ? candidate.bbox : candidate?.bbox_2d;
  return Array.isArray(bbox) && bbox.length === 4 ? bbox.map(Number) : null;
}

function candidateRef(candidate) {
  return text(candidate?.source_ref);
}

function candidateLine(candidate) {
  return text(candidate?.source_line_id);
}

function isLoopbackAddress(value) {
  const address = text(value).split(",")[0].trim().replace(/^::ffff:/, "").replace(/^\[|\]$/g, "");
  return address === "127.0.0.1" || address === "::1" || address.toLowerCase() === "localhost";
}

export function isProviderLayoutQualificationReadAllowed({
  regressionTestHeader = "",
  regressionTestModeEnabled = "",
  nodeEnv = "",
  remoteAddresses = []
} = {}) {
  const requested = /^(1|true|yes)$/i.test(text(regressionTestHeader));
  const enabled = /^(1|true|yes)$/i.test(text(regressionTestModeEnabled));
  const production = text(nodeEnv).toLowerCase() === "production";
  const local = Array.isArray(remoteAddresses) && remoteAddresses.some(isLoopbackAddress);
  return requested && enabled && !production && local;
}

function hasUnknownStructuredLayoutContainer(response) {
  return Array.isArray(response?.layout)
    || Array.isArray(response?.structured_layout)
    || Array.isArray(response?.output?.layout_v2)
    || Array.isArray(response?.output?.ocr_result?.words_v2);
}

function hasValidBbox(candidate, imageIdentity) {
  const bbox = candidateBbox(candidate);
  if (!bbox || !bbox.every(Number.isFinite)) return false;
  const [x1, y1, x2, y2] = bbox;
  return x1 >= 0 && y1 >= 0 && x2 > x1 && y2 > y1
    && x2 <= imageIdentity.width && y2 <= imageIdentity.height;
}

function structuralCandidate(candidate, index) {
  const bbox = Array.isArray(candidate?.bbox) ? candidate.bbox : candidate?.bbox_2d;
  const sourceRef = candidateRef(candidate);
  const sourceLineId = candidateLine(candidate);
  return {
    ordinal: index + 1,
    field_types: Object.fromEntries(Object.keys(candidate || {}).sort().map(key => [key, typeOf(candidate[key])])),
    text_present: Boolean(text(candidate?.text ?? candidate?.word ?? candidate?.value)),
    bbox_field: Array.isArray(candidate?.bbox) ? "bbox" : Array.isArray(candidate?.bbox_2d) ? "bbox_2d" : "missing",
    bbox_arity: Array.isArray(bbox) ? bbox.length : 0,
    coordinate_space: candidateCoordinateSpace(candidate) || "MISSING",
    page_declared: Number.isSafeInteger(candidatePage(candidate)) && candidatePage(candidate) > 0,
    image_width_declared: Number.isSafeInteger(candidateWidth(candidate)) && candidateWidth(candidate) > 0,
    image_height_declared: Number.isSafeInteger(candidateHeight(candidate)) && candidateHeight(candidate) > 0,
    source_ref_present: Boolean(sourceRef),
    source_line_id_present: Boolean(sourceLineId),
    source_ref_sha256: sourceRef ? sha256(sourceRef) : null,
    source_line_id_sha256: sourceLineId ? sha256(sourceLineId) : null
  };
}

function evidenceIdentity(evidence) {
  return {
    schema_version: evidence.schema_version,
    status: evidence.status,
    provider_id: evidence.provider_id,
    model_family: evidence.model_family,
    response_contract_id: evidence.response_contract_id,
    provider_response_id_sha256: evidence.provider_response_id_sha256,
    canonical_image_identity_sha256: evidence.canonical_image_identity_sha256,
    structured_layout_container_path: evidence.structured_layout_container_path,
    structured_layout_field_types_sha256: evidence.structured_layout_field_types_sha256,
    ordered_candidate_structure_sha256: evidence.ordered_candidate_structure_sha256,
    bbox_coordinate_space: evidence.bbox_coordinate_space,
    page_and_source_dimensions_sha256: evidence.page_and_source_dimensions_sha256,
    source_ref_complete: evidence.source_ref_complete,
    source_line_id_complete: evidence.source_line_id_complete,
    result_revision: evidence.result_revision,
    qualification_collector_version: evidence.qualification_collector_version
  };
}

function qualifiedEvidence(fields) {
  const evidence = {
    schema_version: PROVIDER_LAYOUT_PROFILE_QUALIFICATION_SCHEMA_VERSION,
    ...fields,
    qualification_collector_version: PROVIDER_LAYOUT_PROFILE_QUALIFICATION_COLLECTOR_VERSION
  };
  evidence.qualification_sha256 = digest(evidenceIdentity(evidence));
  Object.defineProperty(evidence, SERVER_PROVIDER_LAYOUT_QUALIFICATION_CAPABILITY, {
    value: true,
    enumerable: false
  });
  return deepFreeze(evidence);
}

export function collectProviderLayoutProfileQualification({
  response,
  providerId = "",
  modelName = "",
  responseContractId = "",
  providerResponseId = "",
  imageIdentity,
  resultRevision = 1
} = {}) {
  const image = imageBinding(imageIdentity);
  const provider = text(providerId).toUpperCase();
  const family = modelFamily(modelName);
  const contract = text(responseContractId);
  const suppliedResponseId = text(providerResponseId);
  const responseId = text(response?.id ?? response?.request_id);
  const revisionValid = Number.isSafeInteger(resultRevision) && resultRevision > 0;
  const common = {
    provider_id: provider || "UNSUPPORTED",
    model_family: family,
    response_contract_id: contract || "UNSUPPORTED",
    provider_response_id_sha256: responseId ? sha256(responseId) : null,
    canonical_image_identity_sha256: image ? digest(image) : null,
    structured_layout_container_path: null,
    structured_layout_field_types_sha256: null,
    ordered_candidate_structure_sha256: null,
    bbox_coordinate_space: null,
    page_and_source_dimensions_sha256: null,
    source_ref_complete: false,
    source_line_id_complete: false,
    result_revision: revisionValid ? resultRevision : null
  };
  Object.defineProperty(common, SERVER_QUALIFICATION_IMAGE_SHA256_BINDING, {
    value: image?.image_sha256 || null,
    enumerable: true
  });

  if (!image || !revisionValid
    || provider !== SUPPORTED_PROVIDER
    || family !== SUPPORTED_MODEL_FAMILY
    || !SUPPORTED_CONTRACTS.has(contract)
    || !response || typeof response !== "object" || Array.isArray(response)) {
    return qualifiedEvidence({ ...common, status: PROVIDER_LAYOUT_PROFILE_QUALIFICATION_STATUS.RESPONSE_CONTRACT_UNSUPPORTED });
  }
  if (!responseId || !suppliedResponseId || responseId !== suppliedResponseId) {
    return qualifiedEvidence({ ...common, status: PROVIDER_LAYOUT_PROFILE_QUALIFICATION_STATUS.QUALIFICATION_CONFLICT });
  }

  const present = CONTAINERS
    .map(([path, read]) => [path, read(response)])
    .filter(([, value]) => Array.isArray(value));
  if (present.length === 0) {
    if (hasUnknownStructuredLayoutContainer(response)) {
      return qualifiedEvidence({ ...common, status: PROVIDER_LAYOUT_PROFILE_QUALIFICATION_STATUS.RESPONSE_CONTRACT_UNSUPPORTED });
    }
    return qualifiedEvidence({ ...common, status: PROVIDER_LAYOUT_PROFILE_QUALIFICATION_STATUS.NO_STRUCTURED_LAYOUT });
  }
  if (present.length !== 1) {
    return qualifiedEvidence({ ...common, status: PROVIDER_LAYOUT_PROFILE_QUALIFICATION_STATUS.QUALIFICATION_CONFLICT });
  }

  const [containerPath, candidates] = present[0];
  if (candidates.some(candidate => (
    text(candidate?.candidate_schema_version) !== "provider_layout_candidate_v1"
  ))) {
    return qualifiedEvidence({
      ...common,
      structured_layout_container_path: containerPath,
      status: PROVIDER_LAYOUT_PROFILE_QUALIFICATION_STATUS.RESPONSE_CONTRACT_UNSUPPORTED
    });
  }
  const structures = candidates.map(structuralCandidate);
  const candidateRefs = candidates.map(candidateRef);
  const sourceLines = candidates.map(candidateLine);
  const spaces = candidates.map(candidateCoordinateSpace);
  const pages = candidates.map(candidatePage);
  const widths = candidates.map(candidateWidth);
  const heights = candidates.map(candidateHeight);
  const populated = {
    ...common,
    structured_layout_container_path: containerPath,
    structured_layout_field_types_sha256: digest(structures.map(item => item.field_types)),
    ordered_candidate_structure_sha256: digest(structures),
    bbox_coordinate_space: spaces.length > 0 && spaces.every(value => value === ORIGINAL_IMAGE_PIXELS)
      ? ORIGINAL_IMAGE_PIXELS
      : null,
    page_and_source_dimensions_sha256: pages.length > 0
      && pages.every(Number.isSafeInteger)
      && widths.every(Number.isSafeInteger)
      && heights.every(Number.isSafeInteger)
      ? digest(pages.map((page, index) => ({ page, width: widths[index], height: heights[index] })))
      : null,
    source_ref_complete: candidateRefs.length > 0 && candidateRefs.every(Boolean),
    source_line_id_complete: sourceLines.length > 0 && sourceLines.every(Boolean)
  };

  if (candidates.length === 0) {
    return qualifiedEvidence({ ...populated, status: PROVIDER_LAYOUT_PROFILE_QUALIFICATION_STATUS.NO_STRUCTURED_LAYOUT });
  }
  const coordinateSpaceProven = spaces.every(value => value === ORIGINAL_IMAGE_PIXELS);
  const pageAndDimensionsProven = pages.every((page, index) => (
    Number.isSafeInteger(page) && page === imageIdentity.page
    && Number.isSafeInteger(widths[index]) && widths[index] === imageIdentity.width
    && Number.isSafeInteger(heights[index]) && heights[index] === imageIdentity.height
  ));
  if (!coordinateSpaceProven || !pageAndDimensionsProven) {
    return qualifiedEvidence({ ...populated, status: PROVIDER_LAYOUT_PROFILE_QUALIFICATION_STATUS.COORDINATE_SPACE_UNPROVEN });
  }

  const refsValid = candidateRefs.every(Boolean)
    && sourceLines.every(Boolean)
    && new Set(candidateRefs).size === candidates.length
    && new Set(sourceLines).size === candidates.length;
  const candidatesValid = candidates.every(candidate => (
    hasValidBbox(candidate, imageIdentity)
    && Boolean(text(candidate?.text ?? candidate?.word ?? candidate?.value))
  ));
  if (!refsValid || !candidatesValid) {
    return qualifiedEvidence({ ...populated, status: PROVIDER_LAYOUT_PROFILE_QUALIFICATION_STATUS.QUALIFICATION_CONFLICT });
  }
  return qualifiedEvidence({ ...populated, status: PROVIDER_LAYOUT_PROFILE_QUALIFICATION_STATUS.QUALIFICATION_CANDIDATE });
}

export function validateProviderLayoutProfileQualification({
  evidence,
  providerResponseId = "",
  imageIdentity,
  resultRevision = 1
} = {}) {
  const invalid = reason => Object.freeze({ valid: false, reason, evidence: null });
  if (!evidence || evidence[SERVER_PROVIDER_LAYOUT_QUALIFICATION_CAPABILITY] !== true) {
    return invalid("QUALIFICATION_CAPABILITY_MISSING");
  }
  const image = imageBinding(imageIdentity);
  const responseId = text(providerResponseId);
  if (!image || !responseId || !Number.isSafeInteger(resultRevision) || resultRevision <= 0) {
    return invalid("QUALIFICATION_BINDING_INVALID");
  }
  if (evidence.schema_version !== PROVIDER_LAYOUT_PROFILE_QUALIFICATION_SCHEMA_VERSION
    || !FIXED_STATUSES.has(evidence.status)
    || evidence.provider_response_id_sha256 !== sha256(responseId)
    || evidence.canonical_image_identity_sha256 !== digest(image)
    || evidence.result_revision !== resultRevision
    || evidence.qualification_collector_version !== PROVIDER_LAYOUT_PROFILE_QUALIFICATION_COLLECTOR_VERSION
    || evidence.qualification_sha256 !== digest(evidenceIdentity(evidence))) {
    return invalid("QUALIFICATION_BINDING_MISMATCH");
  }
  return Object.freeze({ valid: true, reason: null, evidence });
}

export function hasProviderLayoutProfileQualificationCapability(value) {
  return value?.[SERVER_PROVIDER_LAYOUT_QUALIFICATION_CAPABILITY] === true;
}

export class ProviderLayoutProfileQualificationRuntime {
  constructor({ ttlMs = PROVIDER_LAYOUT_PROFILE_QUALIFICATION_TTL_MS, now = () => Date.now() } = {}) {
    this.ttlMs = ttlMs;
    this.now = now;
    this.used = false;
    this.record = null;
    this.consumedBinding = null;
  }

  capture({ evidence, requestId = "", imageSha256 = "" } = {}) {
    const normalizedRequestId = text(requestId).toLowerCase();
    const normalizedImageSha256 = text(imageSha256).toLowerCase();
    if (this.used) return Object.freeze({ ok: false, reason: "QUALIFICATION_CAPTURE_ALREADY_USED" });
    if (!hasProviderLayoutProfileQualificationCapability(evidence)
      || !/^[0-9a-f-]{16,128}$/.test(normalizedRequestId)
      || !/^[0-9a-f]{64}$/.test(normalizedImageSha256)
      || evidence.canonical_image_identity_sha256 == null
      || evidence[SERVER_QUALIFICATION_IMAGE_SHA256_BINDING] !== normalizedImageSha256) {
      return Object.freeze({ ok: false, reason: "QUALIFICATION_CAPTURE_BINDING_INVALID" });
    }
    this.used = true;
    this.record = Object.freeze({
      request_id_sha256: sha256(normalizedRequestId),
      image_sha256: normalizedImageSha256,
      evidence,
      expires_at: this.now() + this.ttlMs
    });
    return Object.freeze({ ok: true, reason: null });
  }

  consume({ requestId = "", imageSha256 = "" } = {}) {
    const normalizedRequestId = text(requestId).toLowerCase();
    const normalizedImageSha256 = text(imageSha256).toLowerCase();
    const requestIdSha256 = sha256(normalizedRequestId);
    if (this.consumedBinding
      && this.consumedBinding.request_id_sha256 === requestIdSha256
      && this.consumedBinding.image_sha256 === normalizedImageSha256) {
      return Object.freeze({ ok: false, reason: "QUALIFICATION_REPLAY_REJECTED", evidence: null });
    }
    if (!this.record) return Object.freeze({ ok: false, reason: "QUALIFICATION_NOT_FOUND", evidence: null });
    if (this.record.expires_at <= this.now()) {
      this.record = null;
      return Object.freeze({ ok: false, reason: "QUALIFICATION_EXPIRED", evidence: null });
    }
    if (this.record.request_id_sha256 !== requestIdSha256
      || this.record.image_sha256 !== normalizedImageSha256) {
      return Object.freeze({ ok: false, reason: "QUALIFICATION_READ_BINDING_MISMATCH", evidence: null });
    }
    const evidence = this.record.evidence;
    this.consumedBinding = Object.freeze({ request_id_sha256: requestIdSha256, image_sha256: normalizedImageSha256 });
    this.record = null;
    return Object.freeze({ ok: true, reason: null, evidence });
  }
}
