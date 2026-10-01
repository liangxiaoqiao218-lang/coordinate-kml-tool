import {
  COORDINATE_QUALITY_GATE_STATUS,
  FINALIZED_COORDINATE_CRS,
  finalizeCoordinateResult
} from "../coordinate-finalizer/index.js";
import { evaluateRecognitionOutputCapability } from "./recognition-output-capability.js";

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
  warnings = [],
  limitations = []
} = {}) {
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
  return Object.freeze({
    role,
    boundarySemantics: role === COORDINATE_REPRESENTATION_ROLE.BOUNDARY,
    explicitLineIntent: role === COORDINATE_REPRESENTATION_ROLE.REVIEW_LINE,
    result,
    outputCapabilities: evaluateRecognitionOutputCapability(result, { formalAuthorized: false })
  });
}

export function buildCoordinateRepresentationContract(representations = []) {
  const values = representations.filter(Boolean);
  const identities = values.map(item => item?.result?.resultId).filter(Boolean);
  if (identities.length !== values.length || new Set(identities).size !== identities.length) {
    throw new TypeError("REPRESENTATION_IDENTITIES_NOT_DISTINCT");
  }
  return Object.freeze({
    schemaVersion: COORDINATE_REPRESENTATION_CONTRACT_VERSION,
    representations: Object.freeze(values)
  });
}
