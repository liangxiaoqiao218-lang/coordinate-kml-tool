import { createHash } from "node:crypto";
import { isCanonicalCoordinateImageIdentity } from "../recognition/coordinate-image-safety.js";
import {
  ORIGINAL_IMAGE_PIXEL_SPACE,
  SERVER_PROVENANCE_ATTESTATION,
  createImageTextObservation
} from "./observation-schema.js";
import {
  STRUCTURED_PROVIDER_LAYOUT_EXTRACTION_CAPABILITY,
  validateProviderLayoutRoleClassification
} from "./provider-layout-role-classifier.js";

export const TRUSTED_LAYOUT_ATTESTATION_SCHEMA_VERSION = "trusted_layout_attestation_v1";
export const TRUSTED_ROW_BINDING_SCHEMA_VERSION = "trusted_row_binding_v1";
export const TRUSTED_LAYOUT_ATTESTATION_CAPABILITY = Symbol("TRUSTED_LAYOUT_ATTESTATION_CAPABILITY");

const TRUSTED_ATTESTOR = "SERVER_LAYOUT_CLASSIFIER_V2";
const SERVER_LAYOUT_CLASSIFICATION_CAPABILITY = Symbol("SERVER_LAYOUT_CLASSIFICATION_CAPABILITY");
const SOURCE_TYPE_ALLOWLIST = new Set(["PROVIDER_STRUCTURED_LAYOUT_V1", "LOCAL_OCR_STRUCTURED_LAYOUT_V1", "SYNTHETIC_REGRESSION_V1"]);
const ROLE_REGION = Object.freeze({
  MAP_SEARCH_BOX: "MAP_SEARCH_BOX_REGION",
  MAP_PLACE_DETAILS: "MAP_PLACE_DETAILS_REGION"
});

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

function finiteBbox(value, imageIdentity) {
  if (!Array.isArray(value) || value.length !== 4) return null;
  const bbox = value.map(Number);
  if (!bbox.every(Number.isFinite)) return null;
  const [x1, y1, x2, y2] = bbox;
  if (x1 < 0 || y1 < 0 || x2 <= x1 || y2 <= y1
    || x2 > imageIdentity.width || y2 > imageIdentity.height) return null;
  return bbox;
}

function bboxesDoNotOverlap(left, right) {
  return left[2] <= right[0] || right[2] <= left[0]
    || left[3] <= right[1] || right[3] <= left[1];
}

function flattenPoints(coordinateEngineV2 = {}) {
  return (Array.isArray(coordinateEngineV2.groups) ? coordinateEngineV2.groups : []).flatMap((group, groupIndex) => {
    const groupId = text(group.group_id || `group_${groupIndex + 1}`);
    return (Array.isArray(group.points) ? group.points : []).map((point, pointIndex) => ({
      group_id: groupId,
      point_id: text(point.label || pointIndex + 1),
      raw: text(point.raw)
    }));
  });
}

function candidateForObservation(points, raw = {}) {
  const groupId = text(raw.group_id);
  const pointId = text(raw.point_id);
  const matches = groupId || pointId
    ? points.filter(point => point.group_id === groupId && point.point_id === pointId)
    : points.filter(point => point.raw === text(raw.text));
  if (matches.length !== 1 || !matches[0].raw || matches[0].raw !== text(raw.text)) return null;
  return matches[0];
}

function observationIdentity(observation) {
  return {
    observation_id: observation.observation_id,
    image_sha256: observation.image_sha256,
    image_byte_length: observation.image_byte_length,
    image_mime_type: observation.image_mime_type,
    image_width: observation.image_width,
    image_height: observation.image_height,
    page: observation.page,
    request_asset_id: observation.request_asset_id,
    source: observation.source,
    source_ref: observation.source_ref,
    source_line_id: observation.source_line_id,
    source_role: observation.source_role,
    source_region_id: observation.source_region_id,
    bbox: observation.bbox,
    coordinate_space: observation.coordinate_space,
    text_sha256: observation.text_sha256,
    candidate_provenance_sha256: observation.candidate_provenance_sha256,
    provider_response_id_sha256: observation.provider_response_id_sha256,
    attestation_revision: observation.attestation_revision,
    provenance_attestor: observation.provenance_attestor
  };
}

function bindingIdentity(binding) {
  return {
    schema_version: binding.schema_version,
    group_id: binding.group_id,
    point_id: binding.point_id,
    observation_id: binding.observation_id,
    candidate_provenance_sha256: binding.candidate_provenance_sha256,
    layout_attestation_sha256: binding.layout_attestation_sha256,
    result_revision: binding.result_revision
  };
}

function attestationIdentity(attestation) {
  return {
    schema_version: attestation.schema_version,
    image_sha256: attestation.image_sha256,
    image_byte_length: attestation.image_byte_length,
    image_mime_type: attestation.image_mime_type,
    image_width: attestation.image_width,
    image_height: attestation.image_height,
    page: attestation.page,
    image_id: attestation.image_id,
    request_asset_id: attestation.request_asset_id,
    result_revision: attestation.result_revision,
    provenance_attestor: attestation.provenance_attestor,
    observation_attestations: attestation.observations.map(item => item.layout_attestation_sha256),
    row_bindings: attestation.row_bindings.map(item => item.binding_sha256)
  };
}

export function extractProviderLayoutCandidates(response = {}) {
  const containers = [
    response?.ocr_result?.words_info,
    response?.ocrResult?.words_info,
    response?.output?.ocr_result?.words_info,
    response?.output?.layout
  ];
  const values = containers.find(Array.isArray) || [];
  return values.map((value, index) => {
    const explicitSourceRef = text(value?.source_ref ?? value?.id);
    const explicitSourceLineId = text(value?.source_line_id ?? value?.line_id ?? value?.id);
    const candidate = {
      text: text(value?.text ?? value?.word ?? value?.value),
      bbox: Array.isArray(value?.bbox) ? value.bbox : value?.bbox_2d,
      polygon: Array.isArray(value?.polygon) ? value.polygon : value?.location,
      source: "providerStructuredLayout",
      source_type: "PROVIDER_STRUCTURED_LAYOUT_V1",
      source_ref: explicitSourceRef || `provider_layout_${index + 1}`,
      source_line_id: explicitSourceLineId || `provider_layout_${index + 1}`,
      source_ref_explicit: Boolean(explicitSourceRef),
      source_line_id_explicit: Boolean(explicitSourceLineId),
      page: value?.page ?? null,
      image_width: value?.image_width ?? value?.imageWidth ?? null,
      image_height: value?.image_height ?? value?.imageHeight ?? null,
      source_role: null,
      source_region_id: null,
      provenance_trust: "UNTRUSTED"
    };
    Object.defineProperty(candidate, STRUCTURED_PROVIDER_LAYOUT_EXTRACTION_CAPABILITY, { value: true, enumerable: false });
    return candidate;
  }).filter(value => value.text && (Array.isArray(value.bbox) || Array.isArray(value.polygon)));
}

export function createServerClassifiedLayoutRows({
  candidates = [],
  classification = null,
  imageIdentity,
  providerResponseId = "",
  resultRevision = 1
} = {}) {
  const validation = validateProviderLayoutRoleClassification({
    classification,
    imageIdentity,
    candidates,
    providerResponseId,
    resultRevision
  });
  if (!validation.valid) return null;
  const assignments = new Map(validation.classification.assignments.map(item => [item.source_ref, item.source_role]));
  const classified = [];
  for (const candidate of candidates) {
    const sourceRef = text(candidate?.source_ref);
    const role = assignments.get(sourceRef);
    if (candidate?.[STRUCTURED_PROVIDER_LAYOUT_EXTRACTION_CAPABILITY] !== true
      || candidate.source_type !== "PROVIDER_STRUCTURED_LAYOUT_V1"
      || !role) return null;
    const row = {
      ...candidate,
      source_role: role,
      source_region_id: ROLE_REGION[role],
      provenance_trust: "SERVER_ATTESTED",
      provenance_attestor: TRUSTED_ATTESTOR
    };
    Object.defineProperty(row, SERVER_PROVENANCE_ATTESTATION, { value: true, enumerable: false });
    Object.defineProperty(row, SERVER_LAYOUT_CLASSIFICATION_CAPABILITY, { value: true, enumerable: false });
    Object.defineProperty(row, STRUCTURED_PROVIDER_LAYOUT_EXTRACTION_CAPABILITY, { value: true, enumerable: false });
    classified.push(Object.freeze(row));
  }
  return Object.freeze(classified);
}

export function createTrustedLayoutAttestation({
  imageIdentity,
  observations = [],
  coordinateEngineV2 = {},
  resultRevision = 1,
  providerResponseId = "",
  providerLayoutClassification = null,
  provenanceAttestor = TRUSTED_ATTESTOR
} = {}) {
  if (!isCanonicalCoordinateImageIdentity(imageIdentity)
    || provenanceAttestor !== TRUSTED_ATTESTOR
    || !Number.isSafeInteger(resultRevision)
    || resultRevision <= 0) return null;
  const points = flattenPoints(coordinateEngineV2);
  if (points.length !== 2 || !Array.isArray(observations) || observations.length !== 2) return null;
  const hasProviderStructuredLayout = observations.some(item => item?.source_type === "PROVIDER_STRUCTURED_LAYOUT_V1");
  const providerClassificationValidation = hasProviderStructuredLayout
    ? validateProviderLayoutRoleClassification({
      classification: providerLayoutClassification,
      imageIdentity,
      candidates: observations,
      providerResponseId,
      resultRevision
    })
    : null;
  if (hasProviderStructuredLayout && !providerClassificationValidation.valid) return null;
  const providerResponseIdSha256 = providerResponseId ? sha256(providerResponseId) : null;
  const observationIds = new Set();
  const trustedObservations = [];
  const rowBindings = [];

  for (let index = 0; index < observations.length; index += 1) {
    const raw = observations[index] || {};
    const sourceType = text(raw.source_type);
    const role = text(raw.source_role);
    const region = text(raw.source_region_id);
    const candidate = candidateForObservation(points, raw);
    const bbox = finiteBbox(raw.bbox, imageIdentity);
    const isSyntheticServerEvidence = sourceType === "SYNTHETIC_REGRESSION_V1"
      && raw[SERVER_PROVENANCE_ATTESTATION] === true
      && raw.provenance_trust === "SERVER_ATTESTED";
    const isServerClassifiedLayout = raw[SERVER_LAYOUT_CLASSIFICATION_CAPABILITY] === true
      && raw[SERVER_PROVENANCE_ATTESTATION] === true
      && providerClassificationValidation?.valid === true
      && raw.provenance_trust === "SERVER_ATTESTED"
      && raw.provenance_attestor === TRUSTED_ATTESTOR
      && /^[0-9a-f]{64}$/.test(providerResponseIdSha256 || "");
    if ((!isSyntheticServerEvidence && !isServerClassifiedLayout)
      || !SOURCE_TYPE_ALLOWLIST.has(sourceType)
      || !candidate
      || !bbox
      || ROLE_REGION[role] !== region
      || text(raw.coordinate_space || ORIGINAL_IMAGE_PIXEL_SPACE) !== ORIGINAL_IMAGE_PIXEL_SPACE
      || (raw.image_sha256 && raw.image_sha256 !== imageIdentity.image_sha256)
      || (raw.request_asset_id && raw.request_asset_id !== imageIdentity.request_asset_id)
      || (raw.image_id && raw.image_id !== imageIdentity.image_id)
      || (raw.page && Number(raw.page) !== imageIdentity.page)
      || (raw.image_width && Number(raw.image_width) !== imageIdentity.width)
      || (raw.image_height && Number(raw.image_height) !== imageIdentity.height)) return null;
    const sourceLineId = text(raw.source_line_id);
    const sourceRef = text(raw.source_ref);
    if (!sourceLineId || !sourceRef) return null;
    const candidateProvenanceSha256 = sha256(JSON.stringify({
      group_id: candidate.group_id,
      point_id: candidate.point_id,
      semantic_label: raw.semantic_label,
      measurement_semantics: raw.measurement_semantics,
      boundary_point: raw.boundary_point,
      table_row: raw.table_row,
      contradictory_evidence: raw.contradictory_evidence,
      source_type: sourceType,
      source_ref: sourceRef,
      source_line_id: sourceLineId,
      provider_response_id_sha256: providerResponseIdSha256,
      candidate_raw_sha256: sha256(candidate.raw),
      observation_text_sha256: sha256(text(raw.text))
    }));
    const observationId = `obs_${sha256(JSON.stringify({
      image_sha256: imageIdentity.image_sha256,
      page: imageIdentity.page,
      source_line_id: sourceLineId,
      bbox,
      text_sha256: sha256(text(raw.text))
    })).slice(0, 24)}`;
    if (observationIds.has(observationId)) return null;
    observationIds.add(observationId);
    const input = {
      observation_id: observationId,
      image_id: imageIdentity.image_id,
      page: imageIdentity.page,
      text: text(raw.text),
      bbox,
      coordinate_space: ORIGINAL_IMAGE_PIXEL_SPACE,
      source: text(raw.source) || sourceType,
      source_type: sourceType,
      source_ref: sourceRef,
      request_asset_id: imageIdentity.request_asset_id,
      source_line_id: sourceLineId,
      source_role: role,
      source_region_id: region,
      provenance_trust: "SERVER_ATTESTED",
      provenance_attestor: TRUSTED_ATTESTOR,
      group_id: candidate.group_id,
      point_id: candidate.point_id,
      image: imageIdentity,
      image_sha256: imageIdentity.image_sha256,
      image_byte_length: imageIdentity.byte_length,
      image_mime_type: imageIdentity.mime_type,
      text_sha256: sha256(text(raw.text)),
      candidate_provenance_sha256: candidateProvenanceSha256,
      provider_response_id_sha256: providerResponseIdSha256,
      attestation_revision: resultRevision,
      semantic_label: raw.semantic_label,
      measurement_semantics: raw.measurement_semantics,
      boundary_point: raw.boundary_point,
      table_row: raw.table_row,
      contradictory_evidence: raw.contradictory_evidence
    };
    Object.defineProperty(input, SERVER_PROVENANCE_ATTESTATION, { value: true });
    const observation = createImageTextObservation(input);
    const layoutAttestationSha256 = sha256(JSON.stringify(observationIdentity(observation)));
    observation.layout_attestation_sha256 = layoutAttestationSha256;
    const binding = {
      schema_version: TRUSTED_ROW_BINDING_SCHEMA_VERSION,
      point_id: candidate.point_id,
      group_id: candidate.group_id,
      row_id: `trusted-${candidate.group_id}-${candidate.point_id}`,
      observation_id: observation.observation_id,
      bbox: observation.bbox,
      page: observation.page,
      source: observation.source,
      location_status: "PIXEL_BBOX",
      match_score: 1,
      score_method: "trusted_layout_exact_binding_v1",
      score_calibrated: true,
      match_factors: Object.freeze({ exact_candidate_text: 1, original_image_binding: 1, trusted_layout: 1 }),
      candidate_provenance_sha256: candidateProvenanceSha256,
      layout_attestation_sha256: layoutAttestationSha256,
      result_revision: resultRevision
    };
    binding.binding_sha256 = sha256(JSON.stringify(bindingIdentity(binding)));
    trustedObservations.push(observation);
    rowBindings.push(binding);
  }

  if (new Set(trustedObservations.map(item => item.source_role)).size !== 2
    || !trustedObservations.some(item => item.source_role === "MAP_SEARCH_BOX")
    || !trustedObservations.some(item => item.source_role === "MAP_PLACE_DETAILS")
    || trustedObservations[0].source_line_id === trustedObservations[1].source_line_id
    || !bboxesDoNotOverlap(trustedObservations[0].bbox, trustedObservations[1].bbox)) return null;

  const attestation = {
    schema_version: TRUSTED_LAYOUT_ATTESTATION_SCHEMA_VERSION,
    image_sha256: imageIdentity.image_sha256,
    image_byte_length: imageIdentity.byte_length,
    image_mime_type: imageIdentity.mime_type,
    image_width: imageIdentity.width,
    image_height: imageIdentity.height,
    page: imageIdentity.page,
    image_id: imageIdentity.image_id,
    request_asset_id: imageIdentity.request_asset_id,
    result_revision: resultRevision,
    provenance_attestor: TRUSTED_ATTESTOR,
    observations: trustedObservations,
    row_bindings: rowBindings
  };
  attestation.attestation_sha256 = sha256(JSON.stringify(attestationIdentity(attestation)));
  Object.defineProperty(attestation, TRUSTED_LAYOUT_ATTESTATION_CAPABILITY, { value: true, enumerable: false });
  return deepFreeze(attestation);
}

export function validateTrustedLayoutAttestation({ attestation, imageIdentity, coordinateEngineV2 = {}, resultRevision = 1 } = {}) {
  const invalid = reason => Object.freeze({ valid: false, reason, attestation: null });
  if (!attestation || attestation[TRUSTED_LAYOUT_ATTESTATION_CAPABILITY] !== true) return invalid("TRUSTED_LAYOUT_CAPABILITY_MISSING");
  if (!isCanonicalCoordinateImageIdentity(imageIdentity)) return invalid("CANONICAL_IMAGE_IDENTITY_INVALID");
  if (attestation.schema_version !== TRUSTED_LAYOUT_ATTESTATION_SCHEMA_VERSION
    || attestation.result_revision !== resultRevision
    || attestation.image_sha256 !== imageIdentity.image_sha256
    || attestation.image_byte_length !== imageIdentity.byte_length
    || attestation.image_mime_type !== imageIdentity.mime_type
    || attestation.image_width !== imageIdentity.width
    || attestation.image_height !== imageIdentity.height
    || attestation.page !== imageIdentity.page
    || attestation.image_id !== imageIdentity.image_id
    || attestation.request_asset_id !== imageIdentity.request_asset_id) return invalid("TRUSTED_LAYOUT_IMAGE_OR_REVISION_MISMATCH");
  const points = flattenPoints(coordinateEngineV2);
  if (attestation.observations.length !== points.length || attestation.row_bindings.length !== points.length) {
    return invalid("TRUSTED_LAYOUT_BINDING_CARDINALITY_INVALID");
  }
  if (points.length !== 2
    || new Set(attestation.observations.map(item => item.source_role)).size !== 2
    || !attestation.observations.some(item => item.source_role === "MAP_SEARCH_BOX")
    || !attestation.observations.some(item => item.source_role === "MAP_PLACE_DETAILS")
    || attestation.observations[0].source_line_id === attestation.observations[1].source_line_id
    || !bboxesDoNotOverlap(attestation.observations[0].bbox, attestation.observations[1].bbox)) {
    return invalid("TRUSTED_LAYOUT_SOURCE_REGION_PROOF_INVALID");
  }
  for (const observation of attestation.observations) {
    const candidate = candidateForObservation(points, observation);
    const providerResponseBound = observation.source_type !== "PROVIDER_STRUCTURED_LAYOUT_V1"
      || /^[0-9a-f]{64}$/.test(String(observation.provider_response_id_sha256 || ""));
    if (!candidate
      || !finiteBbox(observation.bbox, imageIdentity)
      || !providerResponseBound
      || observation.provenance_trust !== "SERVER_ATTESTED"
      || observation.provenance_attestor !== TRUSTED_ATTESTOR
      || observation.layout_attestation_sha256 !== sha256(JSON.stringify(observationIdentity(observation)))) {
      return invalid("TRUSTED_LAYOUT_OBSERVATION_INVALID");
    }
  }
  for (const binding of attestation.row_bindings) {
    const observation = attestation.observations.find(item => item.observation_id === binding.observation_id);
    if (!observation
      || binding.schema_version !== TRUSTED_ROW_BINDING_SCHEMA_VERSION
      || binding.result_revision !== resultRevision
      || binding.candidate_provenance_sha256 !== observation.candidate_provenance_sha256
      || binding.layout_attestation_sha256 !== observation.layout_attestation_sha256
      || binding.binding_sha256 !== sha256(JSON.stringify(bindingIdentity(binding)))) {
      return invalid("TRUSTED_LAYOUT_ROW_BINDING_INVALID");
    }
  }
  if (attestation.attestation_sha256 !== sha256(JSON.stringify(attestationIdentity(attestation)))) {
    return invalid("TRUSTED_LAYOUT_DIGEST_INVALID");
  }
  return Object.freeze({ valid: true, reason: null, attestation });
}
