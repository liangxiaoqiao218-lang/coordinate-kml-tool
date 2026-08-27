import { createHash } from "node:crypto";
import { FINALIZED_COORDINATE_SCHEMA_VERSION } from "./reason-codes.js";

function canonicalize(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalize).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonicalize(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

export function createGeometryHash(geometry, schemaVersion = FINALIZED_COORDINATE_SCHEMA_VERSION) {
  return `sha256:${createHash("sha256")
    .update(canonicalize({ schemaVersion, geometry }))
    .digest("hex")}`;
}

export function createSpatialResponseIdentity(result) {
  return Object.freeze({
    resultId: result?.resultId ?? null,
    resultRevision: result?.resultRevision ?? null,
    geometryHash: result?.geometryHash ?? null
  });
}

export function spatialResponseMatchesCurrent(responseIdentity, currentIdentity) {
  return Boolean(responseIdentity && currentIdentity
    && responseIdentity.resultId === currentIdentity.resultId
    && responseIdentity.resultRevision === currentIdentity.resultRevision
    && responseIdentity.geometryHash === currentIdentity.geometryHash);
}
