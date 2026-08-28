import { FINALIZED_SCHEMA_VERSION, MAP_PREVIEW_GATE } from "./constants.js";

const SUPPORTED_TYPES = new Set(["Point", "LineString", "Polygon", "MultiPolygon"]);

function canonicalize(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalize).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonicalize(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

async function sha256(value) {
  const bytes = new TextEncoder().encode(value);
  const digest = await globalThis.crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, "0")).join("");
}

export async function createCanonicalGeometryHash(geometry) {
  const payload = canonicalize({ schemaVersion: FINALIZED_SCHEMA_VERSION, geometry });
  return `sha256:${await sha256(payload)}`;
}

function positionIsValid(position) {
  return Array.isArray(position)
    && position.length === 2
    && Number.isFinite(position[0]) && position[0] >= -180 && position[0] <= 180
    && Number.isFinite(position[1]) && position[1] >= -90 && position[1] <= 90;
}

function positionsEqual(left, right) {
  return Array.isArray(left) && Array.isArray(right)
    && left[0] === right[0] && left[1] === right[1];
}

function lineIsValid(line, minimum) {
  return Array.isArray(line) && line.length >= minimum && line.every(positionIsValid);
}

function ringIsValid(ring) {
  return lineIsValid(ring, 4)
    && positionsEqual(ring[0], ring[ring.length - 1])
    && new Set(ring.slice(0, -1).map(position => `${position[0]}:${position[1]}`)).size >= 3;
}

export function geometryIsDrawable(geometry) {
  if (!geometry || typeof geometry !== "object" || !SUPPORTED_TYPES.has(geometry.type)) return false;
  if (geometry.type === "Point") return positionIsValid(geometry.coordinates);
  if (geometry.type === "LineString") return lineIsValid(geometry.coordinates, 2);
  if (geometry.type === "Polygon") {
    return Array.isArray(geometry.coordinates)
      && geometry.coordinates.length > 0
      && geometry.coordinates.every(ringIsValid);
  }
  return Array.isArray(geometry.coordinates)
    && geometry.coordinates.length > 0
    && geometry.coordinates.every(polygon => Array.isArray(polygon)
      && polygon.length > 0
      && polygon.every(ringIsValid));
}

function blocked(reasonCode) {
  return Object.freeze({
    ok: false,
    generalMapGate: MAP_PREVIEW_GATE,
    previewEligibility: Object.freeze({ allowed: false }),
    reasonCodes: Object.freeze([reasonCode]),
    preview: null
  });
}

function freezeDeep(value) {
  if (!value || typeof value !== "object" || Object.isFrozen(value)) return value;
  Object.values(value).forEach(freezeDeep);
  return Object.freeze(value);
}

export async function adaptFinalizedCoordinateResult(input) {
  if (!input || typeof input !== "object" || Array.isArray(input)
    || input.schemaVersion !== FINALIZED_SCHEMA_VERSION) {
    return blocked("FINALIZED_COORDINATE_RESULT_V1_REQUIRED");
  }
  if (!input.resultId || !Number.isSafeInteger(input.resultRevision) || !input.geometryHash) {
    return blocked("CANONICAL_IDENTITY_INVALID");
  }
  if (input.crs?.id !== "EPSG:4326" || input.crs?.axisOrder !== "longitude_latitude") {
    return blocked("CANONICAL_WGS84_CRS_REQUIRED");
  }
  if (!geometryIsDrawable(input.geometry)) return blocked("GEOMETRY_NOT_DRAWABLE");

  const computedHash = await createCanonicalGeometryHash(input.geometry);
  if (computedHash !== input.geometryHash) return blocked("GEOMETRY_HASH_MISMATCH");

  const warnings = [...new Set([
    ...(Array.isArray(input.warnings) ? input.warnings : []),
    ...(input.requiresReview ? ["REVIEW_REQUIRED"] : []),
    ...(input.confirmationStatus === "pending" ? ["CONFIRMATION_PENDING"] : []),
    ...(input.kmlReady === false ? ["KML_BLOCKED"] : [])
  ])];

  return freezeDeep({
    ok: true,
    generalMapGate: MAP_PREVIEW_GATE,
    previewEligibility: { allowed: true, warning: warnings.length > 0 },
    reasonCodes: [],
    preview: {
      sourceResultId: input.resultId,
      sourceRevision: input.resultRevision,
      sourceGeometryHash: input.geometryHash,
      crs: { id: "EPSG:4326", axisOrder: "longitude_latitude" },
      geometry: structuredClone(input.geometry),
      warnings
    }
  });
}
