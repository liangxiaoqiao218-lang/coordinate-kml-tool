import { compareLegacyAndTypedUtm } from "./legacy-compare.js";

export const UTM_MIGRATION_STATUSES = Object.freeze([
  "MATCH",
  "V2_ONLY",
  "LEGACY_ONLY",
  "CRS_CONFLICT",
  "TRANSFORMATION_MISMATCH"
]);

const LEGACY_UTM_PRECISION_MODES = new Set(["utm30n-projected-x-y"]);

function summarizeLegacy(legacyResult) {
  if (!legacyResult || !LEGACY_UTM_PRECISION_MODES.has(legacyResult.precisionMode)) return null;
  return {
    type: legacyResult.precisionMode,
    pointCount: Array.isArray(legacyResult.transformedWgs84) ? legacyResult.transformedWgs84.length : 0
  };
}

function summarizeV2(typedResult) {
  const intent = typedResult?.typedUtmIntent;
  if (!intent) return null;
  return {
    type: intent.coordinateType,
    projection: intent.projection,
    datum: intent.datum,
    zone: intent.zone,
    hemisphere: intent.hemisphere,
    epsg: intent.epsg,
    pointCount: Array.isArray(typedResult.transformedWgs84) ? typedResult.transformedWgs84.length : 0
  };
}

function hasCrsConflict(shadowIntent) {
  return Array.isArray(shadowIntent?.conflicts) && shadowIntent.conflicts.length > 0;
}

function isUtmBlocked(shadowIntent) {
  return Array.isArray(shadowIntent?.blockedFallbacks)
    && shadowIntent.blockedFallbacks.includes("utm_projected_xy");
}

export function observeUtmMigration({
  sample,
  legacyResult = null,
  typedResult = null,
  shadowIntent = null,
  tolerance = 1e-8
} = {}) {
  const legacy = summarizeLegacy(legacyResult);
  const v2 = summarizeV2(typedResult);
  const base = {
    sample: String(sample || "unknown"),
    legacy,
    v2,
    comparison: null,
    migrationStatus: null,
    disposition: "NOT_UTM"
  };

  if (hasCrsConflict(shadowIntent)) {
    return {
      ...base,
      migrationStatus: "CRS_CONFLICT",
      disposition: "OBSERVE",
      comparison: { status: "CRS_CONFLICT", conflicts: shadowIntent.conflicts }
    };
  }

  if (legacy && v2) {
    const comparison = compareLegacyAndTypedUtm({ legacyResult, typedResult, tolerance });
    if (comparison.status === "match") {
      return {
        ...base,
        migrationStatus: "MATCH",
        disposition: "OBSERVE",
        comparison: {
          status: "MATCH",
          tolerance: comparison.tolerance,
          maxDifference: comparison.maximumDifference,
          pointCount: comparison.pointCount
        }
      };
    }
    if (comparison.reason === "crs_mismatch") {
      return {
        ...base,
        migrationStatus: "CRS_CONFLICT",
        disposition: "OBSERVE",
        comparison: {
          status: "CRS_CONFLICT",
          tolerance: comparison.tolerance,
          differences: comparison.crsDifferences
        }
      };
    }
    return {
      ...base,
      migrationStatus: "TRANSFORMATION_MISMATCH",
      disposition: "OBSERVE",
      comparison: {
        status: "TRANSFORMATION_MISMATCH",
        reason: comparison.reason,
        tolerance: comparison.tolerance,
        maxDifference: comparison.maximumDifference ?? null
      }
    };
  }

  if (v2) {
    return {
      ...base,
      migrationStatus: "V2_ONLY",
      disposition: "OBSERVE",
      comparison: { status: "V2_ONLY", reason: "legacy_utm_result_unavailable" }
    };
  }

  if (legacy) {
    return {
      ...base,
      migrationStatus: "LEGACY_ONLY",
      disposition: "OBSERVE",
      comparison: { status: "LEGACY_ONLY", reason: "v2_confirmed_typed_result_unavailable" }
    };
  }

  if (isUtmBlocked(shadowIntent)) {
    return {
      ...base,
      disposition: "BLOCKED",
      comparison: { status: "BLOCKED", reason: "utm_projected_xy_blocked" }
    };
  }

  return base;
}

export function createUtmMigrationObservationReport(observations = []) {
  if (!Array.isArray(observations)) throw new TypeError("observations must be an array");
  const statusCounts = Object.fromEntries(UTM_MIGRATION_STATUSES.map(status => [status, 0]));
  const dispositionCounts = { OBSERVE: 0, NOT_UTM: 0, BLOCKED: 0 };
  for (const observation of observations) {
    if (observation?.migrationStatus in statusCounts) statusCounts[observation.migrationStatus] += 1;
    if (observation?.disposition in dispositionCounts) dispositionCounts[observation.disposition] += 1;
  }
  return {
    schemaVersion: "utm_migration_observation_v1",
    shadowOnly: true,
    sampleCount: observations.length,
    statusCounts,
    dispositionCounts,
    observations
  };
}
