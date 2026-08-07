import { transformUtmWgs84Point } from "./utm-wgs84-transform.js";

export const UTM_MIGRATION_DECISIONS = Object.freeze([
  "V2_ALLOWED",
  "LEGACY_ONLY",
  "BLOCKED"
]);

function decision(migrationDecision, reason, { legacyAvailable, v2Available }) {
  return Object.freeze({
    migrationDecision,
    reason: Object.freeze(Array.from(new Set(reason))),
    legacyAvailable: Boolean(legacyAvailable),
    v2Available: Boolean(v2Available)
  });
}

function expectedWgs84UtmEpsg(zone, hemisphere) {
  const base = hemisphere === "north" ? 32600 : hemisphere === "south" ? 32700 : null;
  return base && Number.isInteger(zone) && zone >= 1 && zone <= 60 ? `EPSG:${base + zone}` : null;
}

function hasConfirmedCrs(shadowIntent) {
  if (!shadowIntent || shadowIntent.confidence !== "confirmed" || shadowIntent.projection !== "utm") return false;
  if (shadowIntent.datum !== "WGS84") return false;
  if (Array.isArray(shadowIntent.conflicts) && shadowIntent.conflicts.length > 0) return false;
  const expectedEpsg = expectedWgs84UtmEpsg(shadowIntent.zone, shadowIntent.hemisphere);
  return Boolean(expectedEpsg && shadowIntent.epsg === expectedEpsg);
}

function hasCrsConflict(shadowIntent, migrationObservation) {
  return (Array.isArray(shadowIntent?.conflicts) && shadowIntent.conflicts.length > 0)
    || migrationObservation?.migrationStatus === "CRS_CONFLICT";
}

const TRANSFORMATION_PROVENANCE_TOLERANCE = 1e-8;
const HARD_TYPED_RESULT_FAILURES = new Set([
  "INVALID_PROJECTED_COORDINATES",
  "POINT_COUNT_MISMATCH",
  "STALE_TRANSFORMED_RESULT"
]);

function typedResultValidation(valid, reason = null) {
  return Object.freeze({ valid, reason });
}

function validateTypedResult(typedResult, shadowIntent) {
  const intent = typedResult?.typedUtmIntent;
  const projected = Array.isArray(typedResult?.projectedCoordinates) ? typedResult.projectedCoordinates : [];
  const transformed = Array.isArray(typedResult?.transformedWgs84) ? typedResult.transformedWgs84 : [];
  if (!intent || !hasConfirmedCrs(shadowIntent)) return typedResultValidation(false, "TYPED_RESULT_UNAVAILABLE");
  if (intent.coordinateType !== "utm_projected_xy" || intent.projection !== "utm" || intent.datum !== "WGS84") {
    return typedResultValidation(false, "TYPED_RESULT_UNAVAILABLE");
  }
  if (intent.zone !== shadowIntent.zone || intent.hemisphere !== shadowIntent.hemisphere || intent.epsg !== shadowIntent.epsg) {
    return typedResultValidation(false, "TYPED_RESULT_UNAVAILABLE");
  }
  if (projected.length !== transformed.length) return typedResultValidation(false, "POINT_COUNT_MISMATCH");
  if (projected.length === 0) return typedResultValidation(false, "TYPED_RESULT_UNAVAILABLE");

  const projectedValid = projected.every(point => (
    Number.isFinite(point?.easting)
    && Number.isFinite(point?.northing)
    && point.easting >= 100000
    && point.easting <= 900000
    && point.northing >= 0
    && point.northing <= 10000000
  ));
  if (!projectedValid) return typedResultValidation(false, "INVALID_PROJECTED_COORDINATES");

  const transformedValid = transformed.every(point => (
    Number.isFinite(point?.latitude)
    && Number.isFinite(point?.longitude)
    && point.latitude >= -90
    && point.latitude <= 90
    && point.longitude >= -180
    && point.longitude <= 180
  ));
  if (!transformedValid) return typedResultValidation(false, "STALE_TRANSFORMED_RESULT");

  try {
    const provenanceMatches = projected.every((point, index) => {
      const generated = transformUtmWgs84Point({
        easting: point.easting,
        northing: point.northing,
        zone: intent.zone,
        hemisphere: intent.hemisphere
      });
      const provided = transformed[index];
      return Math.abs(generated.latitude - provided.latitude) <= TRANSFORMATION_PROVENANCE_TOLERANCE
        && Math.abs(generated.longitude - provided.longitude) <= TRANSFORMATION_PROVENANCE_TOLERANCE;
    });
    return provenanceMatches
      ? typedResultValidation(true)
      : typedResultValidation(false, "STALE_TRANSFORMED_RESULT");
  } catch {
    return typedResultValidation(false, "INVALID_PROJECTED_COORDINATES");
  }
}

function isQualityGatePass(qualityGate) {
  if (qualityGate === true || qualityGate === "PASS") return true;
  return String(qualityGate?.status || "").toUpperCase() === "PASS";
}

function isUserConfirmed(userConfirmation) {
  if (userConfirmation === true || userConfirmation === "CONFIRMED") return true;
  return userConfirmation?.confirmed === true
    || String(userConfirmation?.status || "").toUpperCase() === "CONFIRMED";
}

function hasLegacyResult(legacyResult, migrationObservation) {
  if (migrationObservation?.legacy) return true;
  if (migrationObservation?.migrationStatus === "LEGACY_ONLY") return true;
  if (migrationObservation?.disposition === "NOT_UTM" || migrationObservation?.disposition === "BLOCKED") {
    return Boolean(legacyResult);
  }
  return false;
}

function fallbackDecision(reason, availability) {
  return availability.legacyAvailable
    ? decision("LEGACY_ONLY", reason, availability)
    : decision("BLOCKED", reason, availability);
}

export function evaluateShadowMigrationGate({
  legacyResult = null,
  shadowIntent = null,
  typedResult = null,
  migrationObservation = null,
  qualityGate = null,
  userConfirmation = null
} = {}) {
  const legacyAvailable = hasLegacyResult(legacyResult, migrationObservation);
  const crsConfirmed = hasConfirmedCrs(shadowIntent);
  const typedValidation = validateTypedResult(typedResult, shadowIntent);
  const v2Available = typedValidation.valid;
  const availability = { legacyAvailable, v2Available };

  if (hasCrsConflict(shadowIntent, migrationObservation)) {
    return decision("BLOCKED", ["CRS_CONFLICT"], availability);
  }

  if (migrationObservation?.migrationStatus === "TRANSFORMATION_MISMATCH") {
    return decision("BLOCKED", ["TRANSFORMATION_MISMATCH"], availability);
  }

  if (migrationObservation?.disposition === "BLOCKED") {
    return decision("BLOCKED", ["UTM_PROJECTED_XY_BLOCKED"], availability);
  }

  if (migrationObservation?.disposition === "NOT_UTM") {
    return legacyAvailable
      ? decision("LEGACY_ONLY", ["NOT_UTM"], availability)
      : decision("BLOCKED", ["UNKNOWN_PROJECTED_XY"], availability);
  }

  if (HARD_TYPED_RESULT_FAILURES.has(typedValidation.reason)) {
    return decision("BLOCKED", [typedValidation.reason], availability);
  }

  if (!crsConfirmed) {
    return fallbackDecision(["CRS_NOT_CONFIRMED"], availability);
  }

  if (!v2Available) {
    return fallbackDecision([typedValidation.reason || "TYPED_RESULT_UNAVAILABLE"], availability);
  }

  const migrationStatus = migrationObservation?.migrationStatus;
  if (migrationStatus !== "MATCH" && migrationStatus !== "V2_ONLY") {
    return fallbackDecision(["MIGRATION_STATUS_NOT_ALLOWED"], availability);
  }

  if (!isQualityGatePass(qualityGate)) {
    return fallbackDecision(["QUALITY_GATE_NOT_PASS"], availability);
  }

  if (!isUserConfirmed(userConfirmation)) {
    return fallbackDecision(["USER_CONFIRMATION_REQUIRED"], availability);
  }

  return decision("V2_ALLOWED", [
    "CRS_CONFIRMED",
    "TYPED_RESULT_VALID",
    "QUALITY_GATE_PASS",
    "USER_CONFIRMED",
    migrationStatus === "MATCH" ? "LEGACY_V2_MATCH" : "V2_ONLY_NEW_CAPABILITY"
  ], availability);
}
