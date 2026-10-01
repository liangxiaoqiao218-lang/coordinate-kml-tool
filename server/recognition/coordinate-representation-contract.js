import {
  COORDINATE_QUALITY_GATE_STATUS,
  FINALIZED_COORDINATE_CRS,
  finalizeCoordinateResult
} from "../coordinate-finalizer/index.js";
import { evaluateRecognitionOutputCapability } from "./recognition-output-capability.js";
import { createHash } from "node:crypto";

export const COORDINATE_REPRESENTATION_CONTRACT_VERSION = "coordinate_representation_contract_v1";

export const COORDINATE_REPRESENTATION_ROLE = Object.freeze({
  BOUNDARY: "BOUNDARY",
  REVIEW_POINTS: "REVIEW_POINTS",
  REVIEW_LINE: "REVIEW_LINE"
});

function finitePosition(position) {
  return Array.isArray(position) && position.length === 2
    && Number.isFinite(position[0]) && position[0] >= -180 && position[0] <= 180
    && Number.isFinite(position[1]) && position[1] >= -90 && position[1] <= 90;
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

function normalizeSourceReference(value = {}) {
  const status = String(value.status || "").trim().toUpperCase();
  const crsId = String(value.crsId || "").trim().toUpperCase();
  const axisOrder = String(value.axisOrder || "").trim().toLowerCase();
  const evidenceRefs = [...new Set((Array.isArray(value.evidenceRefs) ? value.evidenceRefs : [])
    .map(item => String(item || "").trim()).filter(Boolean))];
  if (!["EXPLICIT", "CONFIRMED", "ASSUMED", "UNKNOWN"].includes(status)
    || !crsId || !axisOrder || evidenceRefs.length === 0) {
    throw new TypeError("REPRESENTATION_SOURCE_REFERENCE_INVALID");
  }
  return Object.freeze({ status, crsId, axisOrder, evidenceRefs: Object.freeze(evidenceRefs) });
}

function sourceBindingPayload({ result, role, sourceReference }) {
  return {
    resultId: result.resultId,
    resultRevision: result.resultRevision,
    geometryHash: result.geometryHash,
    role,
    sourceReference
  };
}

function representationFor({ role, result, sourceReference, explicitLineIntent = false }) {
  const normalizedSourceReference = normalizeSourceReference(sourceReference);
  const sourceReferenceBinding = Object.freeze({
    resultId: result.resultId,
    resultRevision: result.resultRevision,
    geometryHash: result.geometryHash,
    sourceReference: normalizedSourceReference,
    bindingSha256: digest(sourceBindingPayload({ result, role, sourceReference: normalizedSourceReference }))
  });
  return Object.freeze({
    role,
    boundarySemantics: role === COORDINATE_REPRESENTATION_ROLE.BOUNDARY,
    explicitLineIntent: role === COORDINATE_REPRESENTATION_ROLE.REVIEW_LINE && explicitLineIntent === true,
    result,
    sourceReferenceBinding,
    outputCapabilities: evaluateRecognitionOutputCapability(result, { formalAuthorized: false })
  });
}

export function bindCoordinateRepresentationResult({ role, result, sourceReference, explicitLineIntent = false } = {}) {
  if (!result?.resultId || !Number.isSafeInteger(result?.resultRevision)) {
    throw new TypeError("REPRESENTATION_RESULT_IDENTITY_INVALID");
  }
  if (role === COORDINATE_REPRESENTATION_ROLE.REVIEW_LINE
    && (explicitLineIntent !== true || result.geometry?.type !== "LineString")) {
    throw new TypeError("EXPLICIT_LINE_INTENT_REQUIRED");
  }
  if (role === COORDINATE_REPRESENTATION_ROLE.REVIEW_POINTS
    && !["Point", "MultiPoint"].includes(result.geometry?.type)) {
    throw new TypeError("REPRESENTATION_POINT_GEOMETRY_INVALID");
  }
  if (role === COORDINATE_REPRESENTATION_ROLE.BOUNDARY
    && !["Polygon", "MultiPolygon"].includes(result.geometry?.type)) {
    throw new TypeError("REPRESENTATION_BOUNDARY_GEOMETRY_INVALID");
  }
  return representationFor({ role, result, sourceReference, explicitLineIntent });
}

export function validateRepresentationSourceBinding(representation = {}) {
  const binding = representation.sourceReferenceBinding;
  if (!binding || !representation.result) return false;
  const payload = sourceBindingPayload({
    result: representation.result,
    role: representation.role,
    sourceReference: binding.sourceReference
  });
  return binding.resultId === representation.result.resultId
    && binding.resultRevision === representation.result.resultRevision
    && binding.geometryHash === representation.result.geometryHash
    && binding.bindingSha256 === digest(payload);
}

function geometryFor({ role, positions, explicitLineIntent }) {
  if (!Array.isArray(positions) || positions.length === 0 || positions.some(position => !finitePosition(position))) {
    throw new TypeError("REPRESENTATION_POSITIONS_INVALID");
  }
  if (role === COORDINATE_REPRESENTATION_ROLE.REVIEW_POINTS) {
    return positions.length === 1
      ? { type: "Point", coordinates: positions[0] }
      : { type: "MultiPoint", coordinates: positions };
  }
  if (role === COORDINATE_REPRESENTATION_ROLE.REVIEW_LINE) {
    if (explicitLineIntent !== true || positions.length < 2) throw new TypeError("EXPLICIT_LINE_INTENT_REQUIRED");
    return { type: "LineString", coordinates: positions };
  }
  if (role === COORDINATE_REPRESENTATION_ROLE.BOUNDARY) {
    if (positions.length < 3) throw new TypeError("BOUNDARY_POSITIONS_INSUFFICIENT");
    return { type: "Polygon", coordinates: [[...positions, [...positions[0]]]] };
  }
  throw new TypeError("REPRESENTATION_ROLE_INVALID");
}

export function createCoordinateRepresentation({
  role,
  positions,
  sourceAuthority = "manual_input",
  coordinateType = "projected_xy",
  precisionMode = null,
  explicitLineIntent = false,
  boundaryBlocked = false,
  sourceReference,
  warnings = [],
  limitations = []
} = {}) {
  normalizeSourceReference(sourceReference);
  const geometry = geometryFor({ role, positions, explicitLineIntent });
  const blockedBoundary = role === COORDINATE_REPRESENTATION_ROLE.BOUNDARY && boundaryBlocked === true;
  const result = finalizeCoordinateResult({
    resultRevision: 1,
    currentRevision: 1,
    confirmedRevision: null,
    sourceAuthority,
    coordinateType,
    precisionMode,
    family: coordinateType,
    crs: FINALIZED_COORDINATE_CRS,
    geometry,
    confirmationStatus: "pending",
    qualityGateStatus: blockedBoundary
      ? COORDINATE_QUALITY_GATE_STATUS.FAILED
      : COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED,
    technicalKmlReady: !blockedBoundary,
    currentAuthorizedGeometryExportable: !blockedBoundary,
    requiresReview: true,
    kmlReady: !blockedBoundary,
    kmlAuthorityBlocked: blockedBoundary,
    groups: [{ groupId: "group_1", requiresReview: true, kmlReady: !blockedBoundary }],
    warnings,
    limitations
  });
  return representationFor({ role, result, sourceReference, explicitLineIntent });
}

export function buildCoordinateRepresentationContract(representations = []) {
  const values = representations.filter(Boolean);
  const identities = values.map(item => item?.result?.resultId).filter(Boolean);
  if (identities.length !== values.length || new Set(identities).size !== identities.length) {
    throw new TypeError("REPRESENTATION_IDENTITIES_NOT_DISTINCT");
  }
  if (values.some(value => !validateRepresentationSourceBinding(value))) {
    throw new TypeError("REPRESENTATION_SOURCE_BINDING_INVALID");
  }
  return Object.freeze({
    schemaVersion: COORDINATE_REPRESENTATION_CONTRACT_VERSION,
    representations: Object.freeze(values)
  });
}
