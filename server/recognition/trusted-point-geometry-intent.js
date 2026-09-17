import { createHash, randomUUID } from "node:crypto";
import { isCanonicalCoordinateImageIdentity } from "./coordinate-image-safety.js";
import { TRUSTED_LAYOUT_ATTESTATION_CAPABILITY } from "../evidence-acquisition/trusted-layout-attestation.js";

export const TRUSTED_POINT_GEOMETRY_INTENT_SCHEMA_VERSION = "trusted_point_geometry_intent_v1";
export const POINT_GEOMETRY_INTENT_REVIEW_SCHEMA_VERSION = "point_geometry_intent_review_v1";
export const POINT_GEOMETRY_CONFIRMATION_SCHEMA_VERSION = "point_geometry_intent_confirmation_v1";
export const TRUSTED_POINT_GEOMETRY_INTENT_CAPABILITY = Symbol("TRUSTED_POINT_GEOMETRY_INTENT_CAPABILITY");
export const POINT_GEOMETRY_CONFIRMATION_CAPABILITY = Symbol("POINT_GEOMETRY_CONFIRMATION_CAPABILITY");

const POINT_SEMANTIC_SOURCE = "SERVER_BOUND_EXPLICIT_POINT_REVIEW_V1";
const POINT_INTENT_ATTESTOR = "SERVER_POINT_GEOMETRY_INTENT_ATTESTOR_V1";
const DEFAULT_REVIEW_TTL_MS = 15 * 60 * 1000;
const DEFAULT_MAX_REVIEWS = 500;

function text(value) {
  return String(value ?? "").trim();
}

function canonicalize(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalize).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonicalize(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function digest(value) {
  return createHash("sha256").update(canonicalize(value)).digest("hex");
}

function deepFreeze(value) {
  if (!value || typeof value !== "object" || Object.isFrozen(value)) return value;
  Object.values(value).forEach(deepFreeze);
  return Object.freeze(value);
}

function finitePoint(point = {}) {
  const latitude = Number(point.lat);
  const longitude = Number(point.lon);
  return Number.isFinite(latitude) && latitude >= -90 && latitude <= 90
    && Number.isFinite(longitude) && longitude >= -180 && longitude <= 180;
}

function imageIdentityPayload(imageIdentity = {}) {
  if (!isCanonicalCoordinateImageIdentity(imageIdentity)) return null;
  return {
    image_sha256: imageIdentity.image_sha256,
    image_byte_length: imageIdentity.byte_length,
    image_mime_type: imageIdentity.mime_type,
    image_width: imageIdentity.width,
    image_height: imageIdentity.height,
    page: imageIdentity.page,
    image_id: imageIdentity.image_id,
    request_asset_id: imageIdentity.request_asset_id
  };
}

function canonicalCandidateSetPayload(canonicalPoints = [], coordinateEngineV2 = {}) {
  if (!Array.isArray(canonicalPoints) || canonicalPoints.length !== 1 || !finitePoint(canonicalPoints[0])) return null;
  const point = canonicalPoints[0];
  const sourceCrs = coordinateEngineV2.source_crs || null;
  return {
    points: [{
      group_id: text(point.group_id || "group_1"),
      point_id: text(point.label || "1"),
      latitude: Number(point.lat),
      longitude: Number(point.lon),
      raw_sha256: digest(text(point.raw))
    }],
    crs: sourceCrs?.id || sourceCrs || "EPSG:4326",
    axis_order: sourceCrs?.axisOrder || sourceCrs?.axis_order || "latitude_longitude"
  };
}

function proposedPointGeometryPayload(canonicalPoints = []) {
  if (!Array.isArray(canonicalPoints) || canonicalPoints.length !== 1 || !finitePoint(canonicalPoints[0])) return null;
  return {
    type: "Point",
    coordinates: [Number(canonicalPoints[0].lon), Number(canonicalPoints[0].lat)]
  };
}

function reviewIdentity(review = {}) {
  return {
    schema_version: review.schema_version,
    review_id: review.review_id,
    result_id: review.result_id,
    result_revision: review.result_revision,
    geometry_type: review.geometry_type,
    semantic_evidence_source: review.semantic_evidence_source,
    canonical_image_identity_sha256: review.canonical_image_identity_sha256,
    trusted_layout_attestation_sha256: review.trusted_layout_attestation_sha256,
    near_duplicate_decision_sha256: review.near_duplicate_decision_sha256,
    canonical_candidate_set_sha256: review.canonical_candidate_set_sha256,
    proposed_point_geometry_sha256: review.proposed_point_geometry_sha256,
    attestor_version: review.attestor_version
  };
}

function intentIdentity(intent = {}) {
  return {
    schema_version: intent.schema_version,
    geometry_type: intent.geometry_type,
    canonical_image_identity_sha256: intent.canonical_image_identity_sha256,
    trusted_layout_attestation_sha256: intent.trusted_layout_attestation_sha256,
    near_duplicate_decision_sha256: intent.near_duplicate_decision_sha256,
    canonical_candidate_set_sha256: intent.canonical_candidate_set_sha256,
    result_id: intent.result_id,
    result_revision: intent.result_revision,
    point_semantic_evidence_source: intent.point_semantic_evidence_source,
    attestor_version: intent.attestor_version,
    review_id: intent.review_id,
    review_binding_sha256: intent.review_binding_sha256,
    proposed_point_geometry_sha256: intent.proposed_point_geometry_sha256,
    confirmation_action: intent.confirmation_action
  };
}

function confirmationIdentity(binding = {}) {
  return {
    schema_version: binding.schema_version,
    trusted_point_geometry_intent_sha256: binding.trusted_point_geometry_intent_sha256,
    result_id: binding.result_id,
    result_revision: binding.result_revision,
    geometry_hash: binding.geometry_hash,
    action: binding.action
  };
}

function runtimeFailure(code, httpStatus) {
  return Object.freeze({ ok: false, code, httpStatus });
}

export class PointGeometryIntentReviewRuntime {
  constructor({ ttlMs = DEFAULT_REVIEW_TTL_MS, maxReviews = DEFAULT_MAX_REVIEWS, now = () => Date.now() } = {}) {
    if (!Number.isSafeInteger(ttlMs) || ttlMs <= 0) throw new TypeError("point_intent_review_ttl_invalid");
    if (!Number.isSafeInteger(maxReviews) || maxReviews <= 0) throw new TypeError("point_intent_review_max_invalid");
    this.ttlMs = ttlMs;
    this.maxReviews = maxReviews;
    this.now = now;
    this.records = new Map();
  }

  cleanup() {
    const now = this.now();
    for (const [reviewId, record] of this.records) {
      if (record.expiresAt <= now) this.records.delete(reviewId);
    }
    while (this.records.size > this.maxReviews) this.records.delete(this.records.keys().next().value);
  }

  issue({ imageIdentity, trustedLayoutAttestation, nearDuplicateDecision, canonicalPoints,
    coordinateEngineV2, resultId, resultRevision, context = null } = {}) {
    this.cleanup();
    const imagePayload = imageIdentityPayload(imageIdentity);
    const candidateSet = canonicalCandidateSetPayload(canonicalPoints, coordinateEngineV2);
    const proposedGeometry = proposedPointGeometryPayload(canonicalPoints);
    if (!imagePayload || !candidateSet || !proposedGeometry
      || trustedLayoutAttestation?.[TRUSTED_LAYOUT_ATTESTATION_CAPABILITY] !== true
      || !/^[0-9a-f]{64}$/.test(text(trustedLayoutAttestation.attestation_sha256))
      || nearDuplicateDecision?.schema_version !== "near_duplicate_decision_v1"
      || nearDuplicateDecision?.decision !== "SAME_LOCATION_CONFIRMED"
      || !/^[0-9a-f]{64}$/.test(text(nearDuplicateDecision.decision_sha256))
      || !text(resultId) || !Number.isSafeInteger(resultRevision) || resultRevision < 1) return null;
    if (trustedLayoutAttestation.image_sha256 !== imageIdentity.image_sha256
      || trustedLayoutAttestation.result_revision !== resultRevision) return null;

    const review = {
      schema_version: POINT_GEOMETRY_INTENT_REVIEW_SCHEMA_VERSION,
      review_id: randomUUID(),
      result_id: text(resultId),
      result_revision: resultRevision,
      geometry_type: "Point",
      semantic_evidence_source: POINT_SEMANTIC_SOURCE,
      canonical_image_identity_sha256: digest(imagePayload),
      trusted_layout_attestation_sha256: trustedLayoutAttestation.attestation_sha256,
      near_duplicate_decision_sha256: nearDuplicateDecision.decision_sha256,
      canonical_candidate_set_sha256: digest(candidateSet),
      proposed_point_geometry_sha256: digest(proposedGeometry),
      attestor_version: POINT_INTENT_ATTESTOR
    };
    review.review_binding_sha256 = digest(reviewIdentity(review));
    const publicReview = deepFreeze({ ...review });
    this.records.set(review.review_id, {
      review: publicReview,
      imageIdentity,
      trustedLayoutAttestation,
      nearDuplicateDecision,
      canonicalPoints: structuredClone(canonicalPoints),
      coordinateEngineV2: structuredClone(coordinateEngineV2),
      context,
      expiresAt: this.now() + this.ttlMs,
      consumed: false
    });
    this.cleanup();
    return publicReview;
  }

  accept(value = {}) {
    const reviewId = text(value.reviewId || value.review_id);
    const record = this.records.get(reviewId);
    if (!record) return runtimeFailure("POINT_GEOMETRY_INTENT_REVIEW_NOT_FOUND", 404);
    if (record.expiresAt <= this.now()) {
      this.records.delete(reviewId);
      return runtimeFailure("POINT_GEOMETRY_INTENT_REVIEW_EXPIRED", 410);
    }
    if (record.consumed === true) return runtimeFailure("POINT_GEOMETRY_INTENT_REVIEW_REPLAYED", 409);
    const review = record.review;
    if (text(value.action) !== "accept_point") return runtimeFailure("POINT_GEOMETRY_INTENT_ACTION_INVALID", 400);
    if (text(value.reviewBindingSha256 || value.review_binding_sha256) !== review.review_binding_sha256
      || text(value.resultId || value.result_id) !== review.result_id
      || Number(value.resultRevision ?? value.result_revision) !== review.result_revision
      || text(value.geometryType || value.geometry_type) !== "Point") {
      return runtimeFailure("POINT_GEOMETRY_INTENT_REVIEW_IDENTITY_MISMATCH", 409);
    }
    const intent = {
      schema_version: TRUSTED_POINT_GEOMETRY_INTENT_SCHEMA_VERSION,
      geometry_type: "Point",
      canonical_image_identity_sha256: review.canonical_image_identity_sha256,
      trusted_layout_attestation_sha256: review.trusted_layout_attestation_sha256,
      near_duplicate_decision_sha256: review.near_duplicate_decision_sha256,
      canonical_candidate_set_sha256: review.canonical_candidate_set_sha256,
      result_id: review.result_id,
      result_revision: review.result_revision,
      point_semantic_evidence_source: POINT_SEMANTIC_SOURCE,
      attestor_version: POINT_INTENT_ATTESTOR,
      review_id: review.review_id,
      review_binding_sha256: review.review_binding_sha256,
      proposed_point_geometry_sha256: review.proposed_point_geometry_sha256,
      confirmation_action: "accept_point"
    };
    intent.intent_sha256 = digest(intentIdentity(intent));
    Object.defineProperty(intent, TRUSTED_POINT_GEOMETRY_INTENT_CAPABILITY, { value: true, enumerable: false });
    record.consumed = true;
    return Object.freeze({ ok: true, intent: deepFreeze(intent), context: record.context });
  }

  clear() {
    this.records.clear();
  }
}

export function validateTrustedPointGeometryIntent({ intent, imageIdentity, trustedLayoutAttestation,
  nearDuplicateDecision, canonicalPoints, coordinateEngineV2, resultRevision } = {}) {
  const invalid = reason => Object.freeze({ valid: false, reason, intentSha256: null });
  if (!intent || intent[TRUSTED_POINT_GEOMETRY_INTENT_CAPABILITY] !== true) return invalid("POINT_GEOMETRY_INTENT_CAPABILITY_MISSING");
  const imagePayload = imageIdentityPayload(imageIdentity);
  const candidateSet = canonicalCandidateSetPayload(canonicalPoints, coordinateEngineV2);
  const proposedGeometry = proposedPointGeometryPayload(canonicalPoints);
  if (!imagePayload || !candidateSet || !proposedGeometry
    || trustedLayoutAttestation?.[TRUSTED_LAYOUT_ATTESTATION_CAPABILITY] !== true) {
    return invalid("POINT_GEOMETRY_INTENT_EVIDENCE_INVALID");
  }
  const valid = intent.schema_version === TRUSTED_POINT_GEOMETRY_INTENT_SCHEMA_VERSION
    && intent.geometry_type === "Point"
    && intent.point_semantic_evidence_source === POINT_SEMANTIC_SOURCE
    && intent.attestor_version === POINT_INTENT_ATTESTOR
    && intent.confirmation_action === "accept_point"
    && intent.result_revision === resultRevision
    && intent.canonical_image_identity_sha256 === digest(imagePayload)
    && intent.trusted_layout_attestation_sha256 === trustedLayoutAttestation.attestation_sha256
    && intent.near_duplicate_decision_sha256 === nearDuplicateDecision?.decision_sha256
    && intent.canonical_candidate_set_sha256 === digest(candidateSet)
    && intent.proposed_point_geometry_sha256 === digest(proposedGeometry)
    && /^[0-9a-f]{64}$/.test(text(intent.review_binding_sha256))
    && intent.intent_sha256 === digest(intentIdentity(intent));
  return valid
    ? Object.freeze({ valid: true, reason: null, intentSha256: intent.intent_sha256 })
    : invalid("POINT_GEOMETRY_INTENT_BINDING_INVALID");
}

export function createPointGeometryConfirmationBinding({ intentSha256, resultId, resultRevision, geometryHash } = {}) {
  if (!/^[0-9a-f]{64}$/.test(text(intentSha256)) || !text(resultId)
    || !Number.isSafeInteger(resultRevision) || resultRevision < 1
    || !/^sha256:[0-9a-f]{64}$/.test(text(geometryHash))) return null;
  const binding = {
    schema_version: POINT_GEOMETRY_CONFIRMATION_SCHEMA_VERSION,
    trusted_point_geometry_intent_sha256: intentSha256,
    result_id: text(resultId),
    result_revision: resultRevision,
    geometry_hash: text(geometryHash),
    action: "accept"
  };
  binding.confirmation_sha256 = digest(confirmationIdentity(binding));
  Object.defineProperty(binding, POINT_GEOMETRY_CONFIRMATION_CAPABILITY, { value: true, enumerable: false });
  return deepFreeze(binding);
}

export function validatePointGeometryConfirmationBinding({ binding, intentSha256, resultId, resultRevision, geometryHash } = {}) {
  return Boolean(binding
    && binding[POINT_GEOMETRY_CONFIRMATION_CAPABILITY] === true
    && binding.schema_version === POINT_GEOMETRY_CONFIRMATION_SCHEMA_VERSION
    && binding.trusted_point_geometry_intent_sha256 === intentSha256
    && binding.result_id === resultId
    && binding.result_revision === resultRevision
    && binding.geometry_hash === geometryHash
    && binding.action === "accept"
    && binding.confirmation_sha256 === digest(confirmationIdentity(binding)));
}

export const pointGeometryIntentReviewRuntime = new PointGeometryIntentReviewRuntime();
