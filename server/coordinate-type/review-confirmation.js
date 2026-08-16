import { buildFinalizedCoordinateVerificationResponse } from "../verification/index.js";
import { buildStructuredUtmPriority } from "../utm-intent/structured-projected-priority.js";

const REVIEW_SUMMARY_SCHEMA_VERSION = "coordinate_review_summary_v1";
const REVERIFICATION_CONTEXT_SCHEMA_VERSION = "coordinate_reverification_context_v1";

function finiteNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function cleanString(value, fallback = "") {
  return String(value ?? fallback).trim();
}

function normalizePointLabel(value, fallback) {
  return cleanString(value, fallback) || String(fallback || "");
}

function normalizeHemisphere(value) {
  const normalized = cleanString(value).toLowerCase();
  if (normalized === "north" || normalized === "n") return "north";
  if (normalized === "south" || normalized === "s") return "south";
  return "";
}

function normalizeCrs(value = {}) {
  const zone = Number(value.zone);
  const hemisphere = normalizeHemisphere(value.hemisphere);
  if (!Number.isInteger(zone) || zone < 1 || zone > 60 || !hemisphere) return null;
  return Object.freeze({
    coordinateType: "utm_projected_xy",
    projection: "utm",
    datum: "WGS84",
    zone,
    hemisphere,
    epsg: `EPSG:${hemisphere === "north" ? 32600 + zone : 32700 + zone}`,
    confidence: "confirmed",
    source: cleanString(value.source, "user_review")
  });
}

function getPriorityCrs(priority = {}) {
  const intent = priority.typedUtmIntent || {};
  return normalizeCrs({
    zone: intent.zone,
    hemisphere: intent.hemisphere,
    source: intent.source || "structured_utm_priority"
  });
}

function normalizeProjectedRow(value = {}, index = 0) {
  const easting = finiteNumber(value.easting ?? value.x ?? value.X);
  const northing = finiteNumber(value.northing ?? value.y ?? value.Y);
  if (!Number.isFinite(easting) || !Number.isFinite(northing)) return null;
  return Object.freeze({
    index,
    point: normalizePointLabel(value.point ?? value.label, index + 1),
    easting,
    northing,
    xText: cleanString(value.xText ?? value.x ?? value.easting, easting),
    yText: cleanString(value.yText ?? value.y ?? value.northing, northing)
  });
}

function normalizeReferenceRow(value = {}, index = 0) {
  const referenceLatitude = finiteNumber(value.referenceLatitude ?? value.latitude);
  const referenceLongitude = finiteNumber(value.referenceLongitude ?? value.longitude);
  if (!Number.isFinite(referenceLatitude) || !Number.isFinite(referenceLongitude)) return null;
  return Object.freeze({
    index,
    point: normalizePointLabel(value.point ?? value.label, index + 1),
    referenceLatitude,
    referenceLongitude
  });
}

function normalizeVerificationRow(row = {}) {
  const point = cleanString(row.point);
  if (!point) return null;
  return Object.freeze({
    point,
    suspectedField: cleanString(row.suspectedField),
    observed: Object.freeze({
      x: finiteNumber(row.projected?.x ?? row.observed?.x),
      y: finiteNumber(row.projected?.y ?? row.observed?.y)
    }),
    reference: Object.freeze({
      latitude: finiteNumber(row.reference?.latitude),
      longitude: finiteNumber(row.reference?.longitude)
    }),
    latitudeDifference: finiteNumber(row.latitudeDifference),
    longitudeDifference: finiteNumber(row.longitudeDifference),
    maximumDifference: finiteNumber(row.maximumDifference),
    referenceSource: cleanString(row.referenceSource),
    referenceMergeMode: cleanString(row.referenceMergeMode)
  });
}

function buildReviewPoint(row = {}) {
  const normalized = normalizeVerificationRow(row);
  if (!normalized) return null;
  const latitudeDifference = Number(normalized.latitudeDifference || 0);
  const longitudeDifference = Number(normalized.longitudeDifference || 0);
  const suspectedField = normalized.suspectedField
    || (longitudeDifference > latitudeDifference
      ? "X"
      : latitudeDifference > longitudeDifference
        ? "Y"
        : "BOTH");
  return Object.freeze({
    ...normalized,
    suspectedField,
    reason: "coordinate_verification_mismatch"
  });
}

export function buildCoordinateReverificationContext(priority = {}) {
  if (!priority || typeof priority !== "object") return null;
  const crs = getPriorityCrs(priority);
  const rows = Array.isArray(priority.table?.rows) ? priority.table.rows : [];
  const projectedRows = rows.map(normalizeProjectedRow).filter(Boolean);
  const referenceRows = rows.map(normalizeReferenceRow).filter(Boolean);
  if (!crs || projectedRows.length === 0 || referenceRows.length === 0) return null;
  return Object.freeze({
    schemaVersion: REVERIFICATION_CONTEXT_SCHEMA_VERSION,
    crs,
    projectedRows: Object.freeze(projectedRows),
    referenceRows: Object.freeze(referenceRows),
    tolerance: finiteNumber(priority.transformationVerification?.tolerance) ?? 1e-6
  });
}

export function buildCoordinateReviewSummary(priority = {}) {
  if (!priority || priority.reason !== "transformation_verification_failed") return null;
  const verification = priority.transformationVerification || {};
  const rows = Array.isArray(verification.pointLevelVerification)
    ? verification.pointLevelVerification
    : Array.isArray(verification.rows)
      ? verification.rows
      : [];
  const points = rows
    .filter(row => row?.status === "mismatch")
    .map(buildReviewPoint)
    .filter(Boolean);
  if (points.length === 0) return null;
  return Object.freeze({
    schemaVersion: REVIEW_SUMMARY_SCHEMA_VERSION,
    status: "review_required",
    reason: "coordinate_verification_mismatch",
    mismatchedPointLabels: Object.freeze(points.map(point => point.point)),
    points: Object.freeze(points),
    referenceRows: Object.freeze((buildCoordinateReverificationContext(priority)?.referenceRows || [])),
    crs: buildCoordinateReverificationContext(priority)?.crs || null
  });
}

export function attachReviewConfirmationContracts(payload = {}, priority = {}) {
  const reviewSummary = buildCoordinateReviewSummary(priority);
  const reverificationContext = buildCoordinateReverificationContext(priority);
  return {
    ...payload,
    ...(reviewSummary ? { reviewSummary } : {}),
    ...(reverificationContext ? { reverificationContext } : {})
  };
}

export function buildUtmTableFromReviewInput({ coordinates = "", projectedRows = [], referenceRows = [] } = {}) {
  const providedProjectedRows = Array.isArray(projectedRows) ? projectedRows : [];
  const parsedCoordinateRows = cleanString(coordinates)
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .map((line, index) => {
      const parts = line.match(/[+-]?\d+(?:[.,]\d+)?/g) || [];
      if (parts.length < 2) return null;
      return {
        point: String(index + 1),
        x: parts[0].replace(/,/g, "."),
        y: parts[1].replace(/,/g, ".")
      };
    })
    .filter(Boolean);
  const sourceRows = providedProjectedRows.length > 0 ? providedProjectedRows : parsedCoordinateRows;
  const references = new Map((Array.isArray(referenceRows) ? referenceRows : [])
    .map((row, index) => normalizeReferenceRow(row, index))
    .filter(Boolean)
    .map(row => [row.point, row]));
  const rows = sourceRows
    .map((row, index) => {
      const projected = normalizeProjectedRow(row, index);
      if (!projected) return null;
      const reference = references.get(projected.point) || references.get(String(index + 1));
      if (!reference) return null;
      return {
        index,
        point: projected.point,
        easting: projected.easting,
        northing: projected.northing,
        xText: projected.xText,
        yText: projected.yText,
        referenceLatitude: reference.referenceLatitude,
        referenceLongitude: reference.referenceLongitude,
        latitudeText: String(reference.referenceLatitude),
        longitudeText: String(reference.referenceLongitude)
      };
    })
    .filter(Boolean);
  return {
    status: rows.length >= 3 ? "observed" : "none",
    rows,
    rawModelText: ""
  };
}

export function buildReverifiedCoordinateResponse({ coordinates = "", projectedRows = [], crs = {}, referenceRows = [] } = {}) {
  const shadowIntent = normalizeCrs(crs);
  if (!shadowIntent) {
    return {
      success: false,
      error: "INVALID_CRS",
      reviewSummary: {
        schemaVersion: REVIEW_SUMMARY_SCHEMA_VERSION,
        status: "review_required",
        reason: "invalid_crs",
        mismatchedPointLabels: [],
        points: []
      }
    };
  }
  const table = buildUtmTableFromReviewInput({ coordinates, projectedRows, referenceRows });
  const priority = buildStructuredUtmPriority({ shadowIntent, table });
  if (!priority) {
    return {
      success: false,
      error: "INVALID_COORDINATES",
      reviewSummary: {
        schemaVersion: REVIEW_SUMMARY_SCHEMA_VERSION,
        status: "review_required",
        reason: "invalid_coordinates",
        mismatchedPointLabels: [],
        points: []
      }
    };
  }
  const payload = attachReviewConfirmationContracts({
    success: true,
    rawText: priority.coordinates,
    coordinates: priority.coordinates,
    coordinateType: "utm_projected_xy",
    precisionMode: priority.accepted ? "utm-projected-x-y" : "utm-projected-x-y-review",
    projection: "utm",
    typedUtmIntent: priority.typedUtmIntent,
    structuredUtmTable: {
      accepted: Boolean(priority.accepted),
      reason: priority.reason,
      rowCount: priority.table?.rows?.length || 0,
      transformationVerification: priority.transformationVerification
    }
  }, priority);
  return buildFinalizedCoordinateVerificationResponse(payload);
}

export function buildConfirmedCoordinateResponse({ coordinates = "", projectedRows = [], crs = {}, referenceRows = [] } = {}) {
  const response = buildReverifiedCoordinateResponse({ coordinates, projectedRows, crs, referenceRows });
  const verified = response?.qualityGateStatus === "passed"
    && response?.confirmationStatus === "awaiting_confirmation"
    && response?.requires_review === false
    && response?.coordinateType === "utm_projected_xy"
    && response?.precisionMode === "utm-projected-x-y";
  if (!verified) {
    return {
      ...response,
      success: false,
      confirmationStatus: response?.confirmationStatus || "blocked",
      kml_ready: false
    };
  }
  const acceptedPayload = {
    ...response,
    coordinateArbitration: {
      ...response.coordinateArbitration,
      confirmationStatus: "accepted",
      kml_ready: true
    },
    confirmationStatus: "accepted",
    kml_ready: true
  };
  return buildFinalizedCoordinateVerificationResponse(acceptedPayload);
}
