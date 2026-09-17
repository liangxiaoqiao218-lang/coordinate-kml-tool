import { createHash } from "node:crypto";
import {
  IMAGE_OBSERVATION_SCHEMA_VERSION,
  ORIGINAL_IMAGE_OBSERVATION_ATTESTATION
} from "../evidence-acquisition/observation-schema.js";
import {
  TRUSTED_LAYOUT_ATTESTATION_CAPABILITY,
  TRUSTED_ROW_BINDING_SCHEMA_VERSION
} from "../evidence-acquisition/trusted-layout-attestation.js";
import { validateTrustedPointGeometryIntent } from "./trusted-point-geometry-intent.js";

export const NEAR_DUPLICATE_DECISION_SCHEMA_VERSION = "near_duplicate_decision_v1";
export const GEOMETRY_INTENT_GATE_SCHEMA_VERSION = "geometry_intent_authority_gate_v1";

export const NEAR_DUPLICATE_DECISION = Object.freeze({
  SAME_LOCATION_CONFIRMED: "SAME_LOCATION_CONFIRMED",
  DISTINCT_POINTS: "DISTINCT_POINTS",
  REVIEW_REQUIRED: "REVIEW_REQUIRED",
  PROVENANCE_INSUFFICIENT: "PROVENANCE_INSUFFICIENT"
});

const TRUSTED_ROLE_PAIR = Object.freeze(["MAP_PLACE_DETAILS", "MAP_SEARCH_BOX"]);
const TRUSTED_ROLE_REGION = Object.freeze({
  MAP_SEARCH_BOX: "MAP_SEARCH_BOX_REGION",
  MAP_PLACE_DETAILS: "MAP_PLACE_DETAILS_REGION"
});
const TRUSTED_ATTESTORS = new Set(["SERVER_LAYOUT_CLASSIFIER_V1", "SERVER_LAYOUT_CLASSIFIER_V2", "SYNTHETIC_REGRESSION_V1"]);

function deepFreeze(value) {
  if (!value || typeof value !== "object" || Object.isFrozen(value)) return value;
  Object.values(value).forEach(deepFreeze);
  return Object.freeze(value);
}

function digest(value) {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

function text(value) {
  return String(value ?? "").trim();
}

function decimalToken(value) {
  const match = text(value).match(/^([+-]?)(\d+)(?:\.(\d+))?$/);
  if (!match) return null;
  const fraction = match[3] || "";
  const sign = match[1] === "-" ? -1n : 1n;
  return Object.freeze({
    source: text(value),
    scale: fraction.length,
    units: sign * BigInt(`${match[2]}${fraction}`)
  });
}

function pow10(count) {
  return 10n ** BigInt(count);
}

function roundToScale(value, targetScale) {
  if (!value || targetScale > value.scale) return null;
  if (targetScale === value.scale) return value.units;
  const divisor = pow10(value.scale - targetScale);
  const absolute = value.units < 0n ? -value.units : value.units;
  const quotient = absolute / divisor;
  const remainder = absolute % divisor;
  const rounded = quotient + (remainder * 2n >= divisor ? 1n : 0n);
  return value.units < 0n ? -rounded : rounded;
}

function strictRoundingContains(lower, higher) {
  return Boolean(lower && higher
    && higher.scale > lower.scale
    && roundToScale(higher, lower.scale) === lower.units);
}

function extractDecimalPair(value = "") {
  const source = text(value);
  const pipeParts = source.split("|").map(part => part.trim());
  const pairSource = pipeParts.find(part => /^[+-]?\d+(?:\.\d+)?\s*,\s*[+-]?\d+(?:\.\d+)?$/.test(part))
    || source.replace(/^\s*(?:point|pt|点|点号)?\s*(?:[A-Za-z]|\d{1,3})\s*(?:[.\)、):：，、\]\?\-]\s*|\s+)/i, "");
  const match = pairSource.match(/^\s*([+-]?\d+(?:\.\d+)?)\s*,\s*([+-]?\d+(?:\.\d+)?)\s*$/);
  if (!match) return null;
  return Object.freeze({ latitude: decimalToken(match[1]), longitude: decimalToken(match[2]) });
}

function exactPoint(point = {}, observation = {}) {
  const pair = extractDecimalPair(observation.text || point.raw);
  if (!pair) return null;
  const latitude = Number(pair.latitude.source);
  const longitude = Number(pair.longitude.source);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)
    || latitude !== Number(point.lat) || longitude !== Number(point.lon)) return null;
  return Object.freeze({
    point,
    observation,
    latitude: pair.latitude,
    longitude: pair.longitude
  });
}

function validBbox(observation = {}) {
  return observation.location_status === "PIXEL_BBOX"
    && observation.coordinate_space === "ORIGINAL_IMAGE_PIXELS"
    && Number.isFinite(observation.image_width)
    && Number.isFinite(observation.image_height)
    && observation.image_width > 0
    && observation.image_height > 0
    && Array.isArray(observation.bbox)
    && observation.bbox.length === 4
    && observation.bbox.every(Number.isFinite)
    && observation.bbox[0] >= 0
    && observation.bbox[1] >= 0
    && observation.bbox[2] > observation.bbox[0]
    && observation.bbox[3] > observation.bbox[1]
    && observation.bbox[2] <= observation.image_width
    && observation.bbox[3] <= observation.image_height;
}

function sourceRegionsAreSpatiallyDistinct(left = {}, right = {}) {
  if (!validBbox(left) || !validBbox(right)) return false;
  const [leftX1, leftY1, leftX2, leftY2] = left.bbox;
  const [rightX1, rightY1, rightX2, rightY2] = right.bbox;
  return leftX2 <= rightX1 || rightX2 <= leftX1
    || leftY2 <= rightY1 || rightY2 <= leftY1;
}

function hasDistinctMeasurementMeaning(observation = {}) {
  if (observation.measurement_semantics === "DISTINCT_MEASUREMENT_POINT") return true;
  if (observation.boundary_point === true || observation.table_row === true) return true;
  const label = text(observation.semantic_label || observation.point_label);
  return Boolean(label && !/^(?:location|coordinate|位置|坐标)$/i.test(label));
}

function trustedObservation(observation = {}, revision = 1) {
  return observation.schema_version === IMAGE_OBSERVATION_SCHEMA_VERSION
    && observation[ORIGINAL_IMAGE_OBSERVATION_ATTESTATION] === true
    && validBbox(observation)
    && observation.provenance_trust === "SERVER_ATTESTED"
    && TRUSTED_ATTESTORS.has(observation.provenance_attestor)
    && TRUSTED_ROLE_PAIR.includes(observation.source_role)
    && TRUSTED_ROLE_REGION[observation.source_role] === observation.source_region_id
    && observation.page_attested === true
    && Boolean(text(observation.request_asset_id))
    && Boolean(text(observation.image_id))
    && Boolean(text(observation.source))
    && Boolean(text(observation.source_ref))
    && Boolean(text(observation.source_line_id))
    && /^[0-9a-f]{64}$/.test(text(observation.image_sha256))
    && Number.isSafeInteger(observation.image_byte_length)
    && observation.image_byte_length > 0
    && Boolean(text(observation.image_mime_type))
    && /^[0-9a-f]{64}$/.test(text(observation.text_sha256))
    && /^[0-9a-f]{64}$/.test(text(observation.candidate_provenance_sha256))
    && /^[0-9a-f]{64}$/.test(text(observation.layout_attestation_sha256))
    && observation.attestation_revision === revision;
}

function trustedBinding(binding = {}, observation = {}, revision = 1) {
  const identity = {
    schema_version: binding.schema_version,
    group_id: binding.group_id,
    point_id: binding.point_id,
    observation_id: binding.observation_id,
    candidate_provenance_sha256: binding.candidate_provenance_sha256,
    layout_attestation_sha256: binding.layout_attestation_sha256,
    result_revision: binding.result_revision
  };
  return binding.schema_version === TRUSTED_ROW_BINDING_SCHEMA_VERSION
    && binding.binding_authority === "SERVER_ATTESTED"
    && binding.score_method === "trusted_layout_exact_binding_v1"
    && binding.score_calibrated === true
    && binding.result_revision === revision
    && binding.observation_id === observation.observation_id
    && binding.candidate_provenance_sha256 === observation.candidate_provenance_sha256
    && binding.layout_attestation_sha256 === observation.layout_attestation_sha256
    && binding.binding_sha256 === digest(identity);
}

function bindingForPoint(evidence = {}, point = {}, pointIndex = 0) {
  const groupId = text(point.group_id || "group_1");
  const pointId = text(point.label || pointIndex + 1);
  const matches = (Array.isArray(evidence.rowBindings) ? evidence.rowBindings : []).filter(binding => (
    text(binding.group_id) === groupId && text(binding.point_id) === pointId
  ));
  if (matches.length !== 1 || !matches[0].observation_id) return null;
  return matches[0];
}

function observationForBinding(evidence = {}, binding = null) {
  if (!binding) return null;
  const matches = (Array.isArray(evidence.observations) ? evidence.observations : [])
    .filter(observation => observation.observation_id === binding.observation_id);
  return matches.length === 1 ? matches[0] : null;
}

function decisionPayload({ state, reasonCodes, observationIds = [], canonicalObservationId = null, canonicalPoint = null,
  revision = 1, provenanceDigest = null } = {}) {
  const binding = {
    observation_ids: [...observationIds].sort(),
    provenance_sha256: provenanceDigest,
    authority_revision: revision,
    canonical_observation_id: canonicalObservationId,
    canonical_coordinate_sha256: canonicalPoint ? digest({ lat: canonicalPoint.lat, lon: canonicalPoint.lon }) : null
  };
  const identity = {
    schema_version: NEAR_DUPLICATE_DECISION_SCHEMA_VERSION,
    decision: state,
    reason_codes: [...reasonCodes],
    binding
  };
  return deepFreeze({ ...identity, decision_sha256: digest(identity) });
}

function geometryGatePayload({ decision, intent = null, revision = 1, imageIdentity = null,
  trustedLayoutAttestation = null, canonicalPoints = [], coordinateEngineV2 = {} } = {}) {
  const observationIds = decision?.binding?.observation_ids || [];
  const intentValidation = validateTrustedPointGeometryIntent({
    intent,
    imageIdentity,
    trustedLayoutAttestation,
    nearDuplicateDecision: decision,
    canonicalPoints,
    coordinateEngineV2,
    resultRevision: revision
  });
  const authorizedGeometry = intentValidation.valid === true
    && decision?.decision === NEAR_DUPLICATE_DECISION.SAME_LOCATION_CONFIRMED;
  const identity = {
    schema_version: GEOMETRY_INTENT_GATE_SCHEMA_VERSION,
    decision: authorizedGeometry ? "AUTHORIZED" : "BLOCKED",
    geometry_type: authorizedGeometry ? "Point" : null,
    reason_code: authorizedGeometry ? "EXPLICIT_BOUND_POINT_INTENT" : intentValidation.reason,
    near_duplicate_decision_sha256: decision?.decision_sha256 || null,
    authority_revision: revision,
    result_id: authorizedGeometry ? intent.result_id : null,
    trusted_point_geometry_intent_sha256: intentValidation.intentSha256,
    observation_ids: [...observationIds].sort()
  };
  return deepFreeze({ ...identity, gate_sha256: digest(identity) });
}

function blockedResult({ state, reasons, points, evidence, revision, intent, coordinateEngineV2 = {}, imageIdentity = null } = {}) {
  const boundObservationIds = (Array.isArray(evidence?.rowBindings) ? evidence.rowBindings : [])
    .map(item => item.observation_id).filter(Boolean);
  const observationIds = boundObservationIds.length > 0
    ? boundObservationIds
    : (Array.isArray(evidence?.observations) ? evidence.observations : [])
      .map(item => item.observation_id).filter(Boolean);
  const provenanceDigest = digest({
    observations: evidence?.observations || [],
    rowBindings: evidence?.rowBindings || []
  });
  const decision = decisionPayload({
    state,
    reasonCodes: reasons,
    observationIds,
    revision,
    provenanceDigest
  });
  const geometryIntentGate = geometryGatePayload({
    decision,
    intent,
    revision,
    imageIdentity,
    trustedLayoutAttestation: evidence?.trustedLayoutAttestation,
    canonicalPoints: points,
    coordinateEngineV2
  });
  return deepFreeze({
    applies: true,
    canonicalPoints: points,
    decision,
    geometryIntentGate,
    authorityBlocked: geometryIntentGate.decision !== "AUTHORIZED"
  });
}

export function evaluateWgs84NearDuplicateConsolidation({ coordinateEngineV2 = {}, evidenceAcquisition = {},
  recognitionResult = {}, revision = 1 } = {}) {
  const groups = Array.isArray(coordinateEngineV2.groups) ? coordinateEngineV2.groups : [];
  const points = groups.flatMap(group => (Array.isArray(group.points) ? group.points.map(point => ({ ...point, group_id: group.group_id })) : []));
  const wgs84Chat = coordinateEngineV2.coordinate_type === "wgs84_chat_coordinates"
    || coordinateEngineV2.precision_mode === "wgs84-chat-coordinates";
  if (!wgs84Chat || points.length < 2) return deepFreeze({ applies: false, canonicalPoints: points });

  if (groups.length !== 1) {
    return blockedResult({
      state: NEAR_DUPLICATE_DECISION.REVIEW_REQUIRED,
      reasons: ["CROSS_GROUP_NEAR_DUPLICATE_AUTHORITY_UNPROVEN"],
      points,
      evidence: evidenceAcquisition,
      revision,
      intent: recognitionResult.trustedPointGeometryIntent,
      coordinateEngineV2,
      imageIdentity: recognitionResult.imageMetadata
    });
  }

  if (points.length > 2) {
    const decimalPairs = points.map(point => extractDecimalPair(point.raw)).filter(Boolean);
    const hasRoundingCluster = decimalPairs.some((left, leftIndex) => decimalPairs.some((right, rightIndex) => (
      leftIndex !== rightIndex
      && ((strictRoundingContains(left.latitude, right.latitude) && strictRoundingContains(left.longitude, right.longitude))
        || (strictRoundingContains(right.latitude, left.latitude) && strictRoundingContains(right.longitude, left.longitude)))
    )));
    if (!hasRoundingCluster) return deepFreeze({ applies: false, canonicalPoints: points });
    return blockedResult({
      state: NEAR_DUPLICATE_DECISION.REVIEW_REQUIRED,
      reasons: ["UNPROVEN_THREE_OR_MORE_OBSERVATION_CLUSTER"],
      points,
      evidence: evidenceAcquisition,
      revision,
      intent: recognitionResult.trustedPointGeometryIntent,
      coordinateEngineV2,
      imageIdentity: recognitionResult.imageMetadata
    });
  }

  const bindings = points.map((point, index) => bindingForPoint(evidenceAcquisition, point, index));
  const observations = bindings.map(binding => observationForBinding(evidenceAcquisition, binding));
  const exact = points.map((point, index) => exactPoint(point, observations[index] || {}));
  const lowerFirst = exact[0] && exact[1]
    && strictRoundingContains(exact[0].latitude, exact[1].latitude)
    && strictRoundingContains(exact[0].longitude, exact[1].longitude);
  const lowerSecond = exact[0] && exact[1]
    && strictRoundingContains(exact[1].latitude, exact[0].latitude)
    && strictRoundingContains(exact[1].longitude, exact[0].longitude);
  const roundingRelated = Boolean(lowerFirst || lowerSecond);

  const declaredCrs = coordinateEngineV2.source_crs;
  const crsCompatible = !declaredCrs || declaredCrs === "EPSG:4326"
    || (declaredCrs.id === "EPSG:4326"
      && [undefined, null, "latitude_longitude"].includes(declaredCrs.axisOrder || declaredCrs.axis_order));

  const commonReasons = [];
  const trustedLayoutCapability = evidenceAcquisition[TRUSTED_LAYOUT_ATTESTATION_CAPABILITY] === true
    && evidenceAcquisition.trusted_layout_status === "ATTESTED";
  if (!crsCompatible) commonReasons.push("CRS_OR_AXIS_CONFLICT");
  if (evidenceAcquisition?.shadow_only !== true
    || evidenceAcquisition?.affects_coordinates !== false
    || evidenceAcquisition?.affects_kml !== false) commonReasons.push("ORIGINAL_EVIDENCE_AUTHORITY_INVALID");
  if (bindings.some(binding => !binding) || observations.some(observation => !observation)) commonReasons.push("UNBOUND_OBSERVATION");
  if (!trustedLayoutCapability) commonReasons.push("TRUSTED_LAYOUT_ATTESTATION_MISSING");
  if (new Set(observations.filter(Boolean).map(item => item.observation_id)).size !== 2) commonReasons.push("OBSERVATION_IDENTITY_INVALID");
  if (observations.some(observation => !trustedObservation(observation, revision))) commonReasons.push("PROVENANCE_MISSING_OR_MALFORMED");
  if (bindings.some((binding, index) => !binding || !observations[index]
    || !trustedBinding(binding, observations[index], revision))) commonReasons.push("TRUSTED_ROW_BINDING_INVALID");
  if (observations.length === 2 && observations.every(Boolean)) {
    if (observations[0].request_asset_id !== observations[1].request_asset_id
      || observations[0].image_id !== observations[1].image_id
      || observations[0].page !== observations[1].page) commonReasons.push("ASSET_IMAGE_OR_PAGE_CONFLICT");
    if (observations[0].source_line_id === observations[1].source_line_id) commonReasons.push("SAME_OCR_LINE_MULTI_ENGINE");
    if (observations[0].source_region_id === observations[1].source_region_id) commonReasons.push("SOURCE_REGION_NOT_DISTINCT");
    if (!sourceRegionsAreSpatiallyDistinct(observations[0], observations[1])) {
      commonReasons.push("SOURCE_REGIONS_SPATIALLY_OVERLAP");
    }
    if (observations.some(item => item.contradictory_evidence === true)) commonReasons.push("CONTRADICTORY_EVIDENCE");
  }
  if (exact.some(item => !item)) commonReasons.push("OBSERVATION_VALUE_BINDING_INVALID");

  if (commonReasons.length > 0) {
    return blockedResult({
      state: NEAR_DUPLICATE_DECISION.PROVENANCE_INSUFFICIENT,
      reasons: commonReasons,
      points,
      evidence: evidenceAcquisition,
      revision,
      intent: recognitionResult.trustedPointGeometryIntent,
      coordinateEngineV2,
      imageIdentity: recognitionResult.imageMetadata
    });
  }

  if (!roundingRelated) {
    return blockedResult({
      state: NEAR_DUPLICATE_DECISION.DISTINCT_POINTS,
      reasons: ["STRICT_ROUNDING_RELATION_NOT_PROVEN"],
      points,
      evidence: evidenceAcquisition,
      revision,
      intent: recognitionResult.trustedPointGeometryIntent,
      coordinateEngineV2,
      imageIdentity: recognitionResult.imageMetadata
    });
  }

  const reasons = [];
  if (exact.every(Boolean) && (Math.sign(Number(exact[0].latitude.source)) !== Math.sign(Number(exact[1].latitude.source))
    || Math.sign(Number(exact[0].longitude.source)) !== Math.sign(Number(exact[1].longitude.source)))) {
    reasons.push("SIGN_OR_HEMISPHERE_CONFLICT");
  }
  if (observations.length === 2 && observations.every(Boolean)) {
    if (JSON.stringify([...observations.map(item => item.source_role)].sort()) !== JSON.stringify([...TRUSTED_ROLE_PAIR])) {
      reasons.push("UI_SOURCE_ROLE_PAIR_NOT_WHITELISTED");
    }
    if (observations.some(hasDistinctMeasurementMeaning)) reasons.push("DISTINCT_MEASUREMENT_SEMANTICS");
  }

  if (reasons.length > 0) {
    return blockedResult({
      state: NEAR_DUPLICATE_DECISION.PROVENANCE_INSUFFICIENT,
      reasons,
      points,
      evidence: evidenceAcquisition,
      revision,
      intent: recognitionResult.trustedPointGeometryIntent,
      coordinateEngineV2,
      imageIdentity: recognitionResult.imageMetadata
    });
  }

  const highIndex = lowerFirst ? 1 : 0;
  const canonicalPoint = points[highIndex];
  const observationIds = observations.map(item => item.observation_id);
  const provenanceDigest = digest({ observations, bindings });
  const decision = decisionPayload({
    state: NEAR_DUPLICATE_DECISION.SAME_LOCATION_CONFIRMED,
    reasonCodes: ["STRICT_FIXED_DECIMAL_ROUNDING_AND_PROVENANCE_MATCH"],
    observationIds,
    canonicalObservationId: observations[highIndex].observation_id,
    canonicalPoint,
    revision,
    provenanceDigest
  });
  const geometryIntentGate = geometryGatePayload({
    decision,
    intent: recognitionResult.trustedPointGeometryIntent,
    revision,
    imageIdentity: recognitionResult.imageMetadata,
    trustedLayoutAttestation: evidenceAcquisition.trustedLayoutAttestation,
    canonicalPoints: [canonicalPoint],
    coordinateEngineV2
  });
  return deepFreeze({
    applies: true,
    canonicalPoints: [canonicalPoint],
    decision,
    geometryIntentGate,
    authorityBlocked: geometryIntentGate.decision !== "AUTHORIZED"
  });
}

export function validateWgs84NearDuplicateAuthority({ decision, geometryIntentGate, resultId = null, resultRevision } = {}) {
  if (!decision && !geometryIntentGate) return Object.freeze({ declared: false, valid: true, blocked: false });
  if (!decision || !geometryIntentGate
    || decision.schema_version !== NEAR_DUPLICATE_DECISION_SCHEMA_VERSION
    || geometryIntentGate.schema_version !== GEOMETRY_INTENT_GATE_SCHEMA_VERSION
    || decision.binding?.authority_revision !== resultRevision
    || geometryIntentGate.authority_revision !== resultRevision
    || (geometryIntentGate.decision === "AUTHORIZED" && geometryIntentGate.result_id !== resultId)) {
    return Object.freeze({ declared: true, valid: false, blocked: true, reason: "NEAR_DUPLICATE_AUTHORITY_BINDING_INVALID" });
  }
  const decisionIdentity = {
    schema_version: decision.schema_version,
    decision: decision.decision,
    reason_codes: decision.reason_codes,
    binding: decision.binding
  };
  const gateIdentity = {
    schema_version: geometryIntentGate.schema_version,
    decision: geometryIntentGate.decision,
    geometry_type: geometryIntentGate.geometry_type,
    reason_code: geometryIntentGate.reason_code,
    near_duplicate_decision_sha256: geometryIntentGate.near_duplicate_decision_sha256,
    authority_revision: geometryIntentGate.authority_revision,
    result_id: geometryIntentGate.result_id,
    trusted_point_geometry_intent_sha256: geometryIntentGate.trusted_point_geometry_intent_sha256,
    observation_ids: geometryIntentGate.observation_ids
  };
  const valid = decision.decision_sha256 === digest(decisionIdentity)
    && geometryIntentGate.gate_sha256 === digest(gateIdentity)
    && geometryIntentGate.near_duplicate_decision_sha256 === decision.decision_sha256;
  const authorized = valid && geometryIntentGate.decision === "AUTHORIZED" && (
    (decision.decision === NEAR_DUPLICATE_DECISION.SAME_LOCATION_CONFIRMED
      && geometryIntentGate.geometry_type === "Point")
  );
  return Object.freeze({
    declared: true,
    valid,
    blocked: !authorized,
    trustedPointIntent: authorized,
    trustedPointGeometryIntentSha256: authorized ? geometryIntentGate.trusted_point_geometry_intent_sha256 : null,
    reason: valid ? (authorized ? null : geometryIntentGate.reason_code) : "NEAR_DUPLICATE_AUTHORITY_DIGEST_INVALID"
  });
}

export function applyWgs84NearDuplicateAuthority({ recognitionResult = {}, coordinateEngineV2 = {}, evidenceAcquisition = {},
  revision = 1 } = {}) {
  const evaluated = evaluateWgs84NearDuplicateConsolidation({
    recognitionResult,
    coordinateEngineV2,
    evidenceAcquisition,
    revision
  });
  if (!evaluated.applies) return Object.freeze({ recognitionResult, coordinateEngineV2, evaluation: evaluated });
  const canonicalized = evaluated.decision.decision === NEAR_DUPLICATE_DECISION.SAME_LOCATION_CONFIRMED;
  const groups = (coordinateEngineV2.groups || []).map((group, index) => ({
    ...group,
    points: canonicalized && index === 0 ? evaluated.canonicalPoints.map(point => ({ ...point })) : group.points,
    geometry: canonicalized && index === 0 ? "point" : group.geometry,
    requires_review: evaluated.authorityBlocked ? true : group.requires_review,
    kml_ready: evaluated.authorityBlocked ? false : group.kml_ready,
    warnings: Array.from(new Set([
      ...(group.warnings || []),
      ...(evaluated.authorityBlocked ? ["Coordinate near-duplicate or geometry intent authority requires review."] : [])
    ]))
  }));
  return deepFreeze({
    recognitionResult: {
      ...recognitionResult,
      nearDuplicateDecision: evaluated.decision,
      geometryIntentAuthorityGate: evaluated.geometryIntentGate
    },
    coordinateEngineV2: {
      ...coordinateEngineV2,
      groups,
      requires_review: evaluated.authorityBlocked ? true : coordinateEngineV2.requires_review,
      near_duplicate_decision_v1: evaluated.decision,
      geometry_intent_authority_gate_v1: evaluated.geometryIntentGate,
      near_duplicate_authority_blocked: evaluated.authorityBlocked
    },
    evaluation: evaluated
  });
}
