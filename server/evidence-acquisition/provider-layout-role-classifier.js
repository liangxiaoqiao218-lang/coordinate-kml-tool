import { createHash } from "node:crypto";
import { isCanonicalCoordinateImageIdentity } from "../recognition/coordinate-image-safety.js";

export const PROVIDER_LAYOUT_ROLE_CLASSIFICATION_SCHEMA_VERSION = "provider_layout_role_classification_v1";
export const SERVER_LAYOUT_ROLE_CLASSIFICATION_CAPABILITY = Symbol("SERVER_LAYOUT_ROLE_CLASSIFICATION_CAPABILITY");
export const STRUCTURED_PROVIDER_LAYOUT_EXTRACTION_CAPABILITY = Symbol("STRUCTURED_PROVIDER_LAYOUT_EXTRACTION_CAPABILITY");

export const PROVIDER_LAYOUT_CLASSIFICATION_REASON = Object.freeze({
  EVIDENCE_MISSING: "TRUSTED_LAYOUT_CLASSIFIER_EVIDENCE_MISSING",
  INPUT_INVALID: "TRUSTED_LAYOUT_CLASSIFIER_INPUT_INVALID",
  CANDIDATE_SET_INVALID: "TRUSTED_LAYOUT_CLASSIFIER_CANDIDATE_SET_INVALID",
  OUTPUT_INVALID: "TRUSTED_LAYOUT_CLASSIFIER_OUTPUT_INVALID",
  BINDING_MISMATCH: "TRUSTED_LAYOUT_CLASSIFIER_BINDING_MISMATCH"
});

const SERVER_CLASSIFIER_PROFILE_CAPABILITY = Symbol("SERVER_CLASSIFIER_PROFILE_CAPABILITY");
const SERVER_CLASSIFICATION_OUTCOME_CAPABILITY = Symbol("SERVER_CLASSIFICATION_OUTCOME_CAPABILITY");
const ALLOWED_ROLES = new Set(["MAP_SEARCH_BOX", "MAP_PLACE_DETAILS"]);
const SUPPORTED_CLASSIFIER_PROFILES = new Map([
  ["SYNTHETIC_LAYOUT_ROLE_PROFILE_V1", "P0D_SYNTHETIC_V1"]
]);

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

function imageIdentityBinding(imageIdentity) {
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

function normalizeBbox(candidate, imageIdentity) {
  let values = Array.isArray(candidate?.bbox) ? candidate.bbox : null;
  if (!values && Array.isArray(candidate?.polygon)) {
    const points = Array.isArray(candidate.polygon[0])
      ? candidate.polygon
      : candidate.polygon.reduce((rows, value, index, source) => {
        if (index % 2 === 0) rows.push([value, source[index + 1]]);
        return rows;
      }, []);
    if (points.length >= 2 && points.every(point => Array.isArray(point) && point.length >= 2)) {
      const xs = points.map(point => Number(point[0]));
      const ys = points.map(point => Number(point[1]));
      if (xs.every(Number.isFinite) && ys.every(Number.isFinite)) {
        values = [Math.min(...xs), Math.min(...ys), Math.max(...xs), Math.max(...ys)];
      }
    }
  }
  if (!Array.isArray(values) || values.length !== 4) return null;
  const bbox = values.map(Number);
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

function candidateBindings(candidates, imageIdentity) {
  if (!Array.isArray(candidates) || candidates.length !== 2) return null;
  const sourceRefs = new Set();
  const sourceLines = new Set();
  const bindings = [];
  for (const candidate of candidates) {
    const sourceRef = text(candidate?.source_ref);
    const sourceLineId = text(candidate?.source_line_id);
    const candidateText = text(candidate?.text);
    const bbox = normalizeBbox(candidate, imageIdentity);
    if (candidate?.[STRUCTURED_PROVIDER_LAYOUT_EXTRACTION_CAPABILITY] !== true
      || candidate?.source_type !== "PROVIDER_STRUCTURED_LAYOUT_V1"
      || candidate?.source_ref_explicit !== true
      || candidate?.source_line_id_explicit !== true
      || !sourceRef
      || !sourceLineId
      || !candidateText
      || !bbox
      || (candidate.page != null && Number(candidate.page) !== imageIdentity.page)
      || (candidate.image_width != null && Number(candidate.image_width) !== imageIdentity.width)
      || (candidate.image_height != null && Number(candidate.image_height) !== imageIdentity.height)
      || sourceRefs.has(sourceRef)
      || sourceLines.has(sourceLineId)) return null;
    sourceRefs.add(sourceRef);
    sourceLines.add(sourceLineId);
    bindings.push({
      source_type: candidate.source_type,
      source_ref: sourceRef,
      source_line_id: sourceLineId,
      text_sha256: sha256(candidateText),
      bbox,
      page: imageIdentity.page,
      image_width: imageIdentity.width,
      image_height: imageIdentity.height
    });
  }
  if (!bboxesDoNotOverlap(bindings[0].bbox, bindings[1].bbox)) return null;
  return bindings;
}

function classificationIdentity(classification) {
  return {
    schema_version: classification.schema_version,
    classifier_profile_id: classification.classifier_profile_id,
    classifier_version: classification.classifier_version,
    canonical_image_identity_sha256: classification.canonical_image_identity_sha256,
    provider_response_id_sha256: classification.provider_response_id_sha256,
    structured_layout_candidate_set_sha256: classification.structured_layout_candidate_set_sha256,
    result_revision: classification.result_revision,
    assignments: classification.assignments
  };
}

function createOutcome({ classification = null, reason = null } = {}) {
  const outcome = { ok: Boolean(classification), classification, reason };
  Object.defineProperty(outcome, SERVER_CLASSIFICATION_OUTCOME_CAPABILITY, { value: true, enumerable: false });
  return Object.freeze(outcome);
}

export function createServerOwnedLayoutClassifierProfile({ profileId, classifierVersion, classify } = {}) {
  const normalizedProfileId = text(profileId);
  const normalizedVersion = text(classifierVersion);
  if (!normalizedProfileId
    || SUPPORTED_CLASSIFIER_PROFILES.get(normalizedProfileId) !== normalizedVersion
    || typeof classify !== "function") return null;
  const profile = {
    profile_id: normalizedProfileId,
    classifier_version: normalizedVersion
  };
  Object.defineProperty(profile, "classify", { value: classify, enumerable: false });
  Object.defineProperty(profile, SERVER_CLASSIFIER_PROFILE_CAPABILITY, { value: true, enumerable: false });
  return Object.freeze(profile);
}

export function getProductionProviderLayoutClassifierProfile() {
  // P0E qualification evidence is observational only. Enabling a Production
  // classifier profile requires a separately reviewed, calibrated profile.
  return null;
}

export function classifyProviderLayoutRoles({
  profile,
  imageIdentity,
  candidates = [],
  providerResponseId = "",
  resultRevision = 1
} = {}) {
  if (!profile
    || profile[SERVER_CLASSIFIER_PROFILE_CAPABILITY] !== true
    || typeof profile.classify !== "function"
    || !text(profile.profile_id)
    || !text(profile.classifier_version)) {
    return createOutcome({ reason: PROVIDER_LAYOUT_CLASSIFICATION_REASON.EVIDENCE_MISSING });
  }
  const imageBinding = imageIdentityBinding(imageIdentity);
  const normalizedProviderResponseId = text(providerResponseId);
  if (!imageBinding
    || !normalizedProviderResponseId
    || !Number.isSafeInteger(resultRevision)
    || resultRevision <= 0) {
    return createOutcome({ reason: PROVIDER_LAYOUT_CLASSIFICATION_REASON.INPUT_INVALID });
  }
  const bindings = candidateBindings(candidates, imageIdentity);
  if (!bindings) return createOutcome({ reason: PROVIDER_LAYOUT_CLASSIFICATION_REASON.CANDIDATE_SET_INVALID });

  let output;
  try {
    output = profile.classify(deepFreeze(bindings.map(binding => ({ ...binding }))));
  } catch {
    return createOutcome({ reason: PROVIDER_LAYOUT_CLASSIFICATION_REASON.OUTPUT_INVALID });
  }
  if (!Array.isArray(output) || output.length !== bindings.length) {
    return createOutcome({ reason: PROVIDER_LAYOUT_CLASSIFICATION_REASON.OUTPUT_INVALID });
  }
  const assignmentsByRef = new Map();
  for (const item of output) {
    const sourceRef = text(item?.source_ref);
    const role = text(item?.source_role);
    if (!sourceRef || !ALLOWED_ROLES.has(role) || assignmentsByRef.has(sourceRef)) {
      return createOutcome({ reason: PROVIDER_LAYOUT_CLASSIFICATION_REASON.OUTPUT_INVALID });
    }
    assignmentsByRef.set(sourceRef, role);
  }
  if (assignmentsByRef.size !== bindings.length
    || new Set(assignmentsByRef.values()).size !== 2
    || !assignmentsByRef.has(bindings[0].source_ref)
    || !assignmentsByRef.has(bindings[1].source_ref)) {
    return createOutcome({ reason: PROVIDER_LAYOUT_CLASSIFICATION_REASON.OUTPUT_INVALID });
  }

  const assignments = bindings.map(binding => ({
    ...binding,
    source_role: assignmentsByRef.get(binding.source_ref)
  }));
  const classification = {
    schema_version: PROVIDER_LAYOUT_ROLE_CLASSIFICATION_SCHEMA_VERSION,
    classifier_profile_id: profile.profile_id,
    classifier_version: profile.classifier_version,
    canonical_image_identity_sha256: sha256(JSON.stringify(imageBinding)),
    provider_response_id_sha256: sha256(normalizedProviderResponseId),
    structured_layout_candidate_set_sha256: sha256(JSON.stringify(bindings)),
    result_revision: resultRevision,
    assignments
  };
  classification.classification_sha256 = sha256(JSON.stringify(classificationIdentity(classification)));
  Object.defineProperty(classification, SERVER_LAYOUT_ROLE_CLASSIFICATION_CAPABILITY, {
    value: true,
    enumerable: false
  });
  return createOutcome({ classification: deepFreeze(classification) });
}

export function validateProviderLayoutRoleClassification({
  classification,
  imageIdentity,
  candidates = [],
  providerResponseId = "",
  resultRevision = 1
} = {}) {
  const invalid = reason => Object.freeze({ valid: false, reason, classification: null });
  if (!classification || classification[SERVER_LAYOUT_ROLE_CLASSIFICATION_CAPABILITY] !== true) {
    return invalid(PROVIDER_LAYOUT_CLASSIFICATION_REASON.EVIDENCE_MISSING);
  }
  const imageBinding = imageIdentityBinding(imageIdentity);
  const bindings = imageBinding ? candidateBindings(candidates, imageIdentity) : null;
  const normalizedProviderResponseId = text(providerResponseId);
  if (!imageBinding || !bindings || !normalizedProviderResponseId
    || !Number.isSafeInteger(resultRevision) || resultRevision <= 0) {
    return invalid(PROVIDER_LAYOUT_CLASSIFICATION_REASON.INPUT_INVALID);
  }
  if (classification.schema_version !== PROVIDER_LAYOUT_ROLE_CLASSIFICATION_SCHEMA_VERSION
    || SUPPORTED_CLASSIFIER_PROFILES.get(classification.classifier_profile_id) !== classification.classifier_version
    || classification.canonical_image_identity_sha256 !== sha256(JSON.stringify(imageBinding))
    || classification.provider_response_id_sha256 !== sha256(normalizedProviderResponseId)
    || classification.structured_layout_candidate_set_sha256 !== sha256(JSON.stringify(bindings))
    || classification.result_revision !== resultRevision
    || classification.classification_sha256 !== sha256(JSON.stringify(classificationIdentity(classification)))) {
    return invalid(PROVIDER_LAYOUT_CLASSIFICATION_REASON.BINDING_MISMATCH);
  }
  if (!Array.isArray(classification.assignments)
    || classification.assignments.length !== bindings.length
    || classification.assignments.some((assignment, index) => (
      JSON.stringify({ ...assignment, source_role: undefined }) !== JSON.stringify({ ...bindings[index], source_role: undefined })
      || !ALLOWED_ROLES.has(assignment.source_role)
    ))
    || new Set(classification.assignments.map(item => item.source_role)).size !== 2) {
    return invalid(PROVIDER_LAYOUT_CLASSIFICATION_REASON.BINDING_MISMATCH);
  }
  return Object.freeze({ valid: true, reason: null, classification });
}

export function getProviderLayoutRoleClassificationFailureReason(outcome) {
  return outcome?.[SERVER_CLASSIFICATION_OUTCOME_CAPABILITY] === true
    && outcome.ok === false
    && Object.values(PROVIDER_LAYOUT_CLASSIFICATION_REASON).includes(outcome.reason)
    ? outcome.reason
    : null;
}
