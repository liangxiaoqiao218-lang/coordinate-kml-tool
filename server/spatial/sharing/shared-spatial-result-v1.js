import crypto from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import {
  FINALIZED_COORDINATE_CRS,
  FINALIZED_COORDINATE_SCHEMA_VERSION,
  createGeometryHash,
  validateFinalizedCrs,
  validateFinalizedGeometry
} from "../../coordinate-finalizer/index.js";
import { calculateSpatialFacts } from "../spatial-facts.js";

export const SHARED_SPATIAL_RESULT_SCHEMA_VERSION = "shared_spatial_result_v1";
export const SHARE_ID_BYTES = 24;
export const SHARE_MANAGER_CAPABILITY_BYTES = 32;
export const SHARE_RECIPIENT_CAPABILITY_BYTES = 32;
export const SHARE_MAX_VERTICES = 5000;
export const SHARE_MAX_SNAPSHOT_BYTES = 512 * 1024;
export const SHARED_SPATIAL_SNAPSHOT_HASH_ALGORITHM = "sha256";
export const SHARED_SPATIAL_SNAPSHOT_HASH_FIELD_ORDER = Object.freeze([
  "schemaVersion",
  "createdAt",
  "expiresAt",
  "accessScope",
  "usagePermission",
  "source",
  "geometry",
  "crs",
  "axisOrder",
  "spatialFacts",
  "coordinateDisplay",
  "reviewState",
  "confirmationState",
  "capabilities",
  "vertexCount"
]);
export const SHARE_ACCESS_SCOPE = Object.freeze({
  RECIPIENT_ONLY: "RECIPIENT_ONLY",
  ANYONE_WITH_LINK: "ANYONE_WITH_LINK"
});
export const SHARE_USAGE_PERMISSION = Object.freeze({
  VIEW_ONLY: "VIEW_ONLY",
  ALLOW_EDIT: "ALLOW_EDIT"
});

function deepFreeze(value) {
  if (!value || typeof value !== "object" || Object.isFrozen(value)) return value;
  Object.values(value).forEach(deepFreeze);
  return Object.freeze(value);
}

function clone(value) {
  return structuredClone(value);
}

function canonicalJson(value) {
  if (value === undefined) return "null";
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(",")}}`;
}

export function canonicalSharedSpatialSnapshotContent(snapshot) {
  const entries = SHARED_SPATIAL_SNAPSHOT_HASH_FIELD_ORDER.map(field => [field, snapshot?.[field]]);
  return `{${entries.map(([field, value]) => `${JSON.stringify(field)}:${canonicalJson(value)}`).join(",")}}`;
}

export function computeSharedSpatialSnapshotHash(snapshot) {
  return crypto
    .createHash(SHARED_SPATIAL_SNAPSHOT_HASH_ALGORITHM)
    .update(canonicalSharedSpatialSnapshotContent(snapshot), "utf8")
    .digest("hex");
}

function countVertices(geometry) {
  if (geometry.type === "Point") return 1;
  if (geometry.type === "LineString") return geometry.coordinates.length;
  if (geometry.type === "Polygon") return geometry.coordinates.reduce((sum, ring) => sum + ring.length, 0);
  return geometry.coordinates.reduce(
    (sum, polygon) => sum + polygon.reduce((polygonSum, ring) => polygonSum + ring.length, 0),
    0
  );
}

function assertControlledSharingValue(value, allowed, code) {
  if (!Object.values(allowed).includes(value)) {
    throw Object.assign(new TypeError(code), { code });
  }
  return value;
}

function reviewReasonFromFinalizedResult(result, authoritativeReviewReason) {
  if (result.requiresReview !== true) return null;
  const reason = authoritativeReviewReason ?? result.reviewReason ?? null;
  return reason && typeof reason === "object" ? clone(reason) : null;
}

export function createShareId(randomBytes = crypto.randomBytes) {
  return randomBytes(SHARE_ID_BYTES).toString("base64url");
}

export function isValidShareId(value) {
  return /^[A-Za-z0-9_-]{32}$/.test(String(value || ""));
}

export function createManagerCapability(randomBytes = crypto.randomBytes) {
  return randomBytes(SHARE_MANAGER_CAPABILITY_BYTES).toString("base64url");
}

export function createRecipientCapability(randomBytes = crypto.randomBytes) {
  return randomBytes(SHARE_RECIPIENT_CAPABILITY_BYTES).toString("base64url");
}

export function hashManagerCapability(value) {
  const capability = String(value || "");
  if (!capability) return "";
  return crypto.createHash("sha256").update(capability, "utf8").digest("hex");
}

export function managerCapabilityMatches(value, expectedHash) {
  const actualHash = hashManagerCapability(value);
  const expected = String(expectedHash || "");
  if (!/^[a-f0-9]{64}$/.test(actualHash) || !/^[a-f0-9]{64}$/.test(expected)) return false;
  return crypto.timingSafeEqual(Buffer.from(actualHash, "hex"), Buffer.from(expected, "hex"));
}

export const hashRecipientCapability = hashManagerCapability;
export const recipientCapabilityMatches = managerCapabilityMatches;

export function buildSharedSpatialSnapshot({
  finalizedResult,
  reviewReason = null,
  shareId = createShareId(),
  accessScope = SHARE_ACCESS_SCOPE.RECIPIENT_ONLY,
  usagePermission = SHARE_USAGE_PERMISSION.VIEW_ONLY,
  createdAt = new Date().toISOString()
} = {}) {
  if (!isValidShareId(shareId)) throw Object.assign(new TypeError("SHARE_ID_INVALID"), { code: "SHARE_ID_INVALID" });
  if (!finalizedResult || finalizedResult.schemaVersion !== FINALIZED_COORDINATE_SCHEMA_VERSION) {
    throw Object.assign(new TypeError("SHARE_SOURCE_INVALID"), { code: "SHARE_SOURCE_INVALID" });
  }
  const crs = validateFinalizedCrs(finalizedResult.crs);
  if (!crs.ok || finalizedResult.crs?.id !== "EPSG:4326" || finalizedResult.crs?.axisOrder !== "longitude_latitude") {
    throw Object.assign(new TypeError("SHARE_CRS_INVALID"), { code: "SHARE_CRS_INVALID" });
  }
  const geometryResult = validateFinalizedGeometry(finalizedResult.geometry);
  if (!geometryResult.ok) throw Object.assign(new TypeError("SHARE_GEOMETRY_INVALID"), { code: "SHARE_GEOMETRY_INVALID" });
  const geometry = geometryResult.geometry;
  const geometryHash = createGeometryHash(geometry);
  if (geometryHash !== finalizedResult.geometryHash || !isDeepStrictEqual(geometry, finalizedResult.geometry)) {
    throw Object.assign(new TypeError("SHARE_GEOMETRY_IDENTITY_MISMATCH"), { code: "SHARE_GEOMETRY_IDENTITY_MISMATCH" });
  }
  const vertexCount = countVertices(geometry);
  if (vertexCount > SHARE_MAX_VERTICES) {
    throw Object.assign(new RangeError("SHARE_VERTEX_LIMIT_EXCEEDED"), { code: "SHARE_VERTEX_LIMIT_EXCEEDED" });
  }
  const snapshot = {
    schemaVersion: SHARED_SPATIAL_RESULT_SCHEMA_VERSION,
    shareId,
    accessScope: assertControlledSharingValue(accessScope, SHARE_ACCESS_SCOPE, "SHARE_ACCESS_SCOPE_INVALID"),
    usagePermission: assertControlledSharingValue(usagePermission, SHARE_USAGE_PERMISSION, "SHARE_USAGE_PERMISSION_INVALID"),
    source: {
      resultId: finalizedResult.resultId,
      resultRevision: finalizedResult.resultRevision,
      sourceGeometryHash: finalizedResult.geometryHash,
      authority: finalizedResult.sourceAuthority || null,
      coordinateType: finalizedResult.coordinateType || null
    },
    geometry: clone(geometry),
    crs: { ...FINALIZED_COORDINATE_CRS },
    axisOrder: FINALIZED_COORDINATE_CRS.axisOrder,
    spatialFacts: clone(calculateSpatialFacts(geometry)),
    reviewState: {
      requiresReview: finalizedResult.requiresReview === true,
      reviewReason: reviewReasonFromFinalizedResult(finalizedResult, reviewReason)
    },
    confirmationState: {
      status: finalizedResult.confirmationStatus || null,
      resultRevision: finalizedResult.resultRevision
    },
    capabilities: {
      editable: usagePermission === SHARE_USAGE_PERMISSION.ALLOW_EDIT,
      recognition: false,
      kmlDownload: false
    },
    createdAt,
    expiresAt: null,
    snapshotHash: "",
    snapshotBytes: 0,
    vertexCount
  };
  snapshot.snapshotHash = computeSharedSpatialSnapshotHash(snapshot);
  let bytes = Buffer.byteLength(JSON.stringify(snapshot), "utf8");
  while (snapshot.snapshotBytes !== bytes) {
    snapshot.snapshotBytes = bytes;
    bytes = Buffer.byteLength(JSON.stringify(snapshot), "utf8");
  }
  if (snapshot.snapshotBytes > SHARE_MAX_SNAPSHOT_BYTES) {
    throw Object.assign(new RangeError("SHARE_SNAPSHOT_TOO_LARGE"), { code: "SHARE_SNAPSHOT_TOO_LARGE" });
  }
  return deepFreeze(snapshot);
}

export function publicSharedSpatialResult(record, { canRevoke = false } = {}) {
  const snapshot = clone(record.snapshot);
  return deepFreeze({
    ...snapshot,
    canRevoke: canRevoke === true
  });
}
