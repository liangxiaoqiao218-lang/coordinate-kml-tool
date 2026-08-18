import { parseIndonesiaUtmTable } from "./parser.js";
import { transformIndonesiaUtmPoint } from "./transform.js";

export const INDONESIA_UTM_VERIFICATION_TOLERANCE = 1e-6;

function numericOrNull(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function buildPointVerification(row = {}, index = 0, crs = {}, tolerance = INDONESIA_UTM_VERIFICATION_TOLERANCE) {
  let transformed = null;
  try {
    if (crs?.status === "resolved") {
      transformed = transformIndonesiaUtmPoint({
        easting: row.easting,
        northing: row.northing,
        zone: crs.zone,
        hemisphere: crs.hemisphere,
      });
    }
  } catch {
    transformed = null;
  }
  const hasReference = Number.isFinite(row.referenceLatitude) && Number.isFinite(row.referenceLongitude);
  if (!transformed || !hasReference) {
    return Object.freeze({
      point: String(row.point || index + 1),
      transformed: Object.freeze({
        latitude: numericOrNull(transformed?.latitude),
        longitude: numericOrNull(transformed?.longitude),
      }),
      reference: Object.freeze({
        latitude: numericOrNull(row.referenceLatitude),
        longitude: numericOrNull(row.referenceLongitude),
      }),
      latitudeDifference: null,
      longitudeDifference: null,
      maximumDifference: null,
      status: "not_available",
    });
  }
  const latitudeDifference = Math.abs(transformed.latitude - row.referenceLatitude);
  const longitudeDifference = Math.abs(transformed.longitude - row.referenceLongitude);
  const maximumDifference = Math.max(latitudeDifference, longitudeDifference);
  return Object.freeze({
    point: String(row.point || index + 1),
    transformed: Object.freeze({
      latitude: transformed.latitude,
      longitude: transformed.longitude,
    }),
    reference: Object.freeze({
      latitude: row.referenceLatitude,
      longitude: row.referenceLongitude,
    }),
    latitudeDifference,
    longitudeDifference,
    maximumDifference,
    status: maximumDifference <= tolerance ? "match" : "mismatch",
  });
}

export function buildIndonesiaUtmVerification({
  rows = [],
  crs = {},
  tolerance = INDONESIA_UTM_VERIFICATION_TOLERANCE,
} = {}) {
  const pointLevelVerification = Array.isArray(rows)
    ? rows.map((row, index) => buildPointVerification(row, index, crs, tolerance))
    : [];
  const compared = pointLevelVerification.filter((row) => row.status !== "not_available");
  const mismatches = compared.filter((row) => row.status === "mismatch");
  const status = compared.length === 0
    ? "unavailable"
    : mismatches.length > 0
      ? "mismatch"
      : compared.length < pointLevelVerification.length
        ? "partial"
        : "match";
  return Object.freeze({
    status,
    tolerance,
    comparedRows: compared.length,
    matchedRows: compared.filter((row) => row.status === "match").length,
    mismatchedRows: mismatches.length,
    mismatchedPointLabels: Object.freeze(mismatches.map((row) => String(row.point)).filter(Boolean)),
    maximumDifference: compared.reduce((maximum, row) => Math.max(maximum, Number(row.maximumDifference || 0)), 0),
    pointLevelVerification: Object.freeze(pointLevelVerification),
    providerCalls: 0,
    visionCalls: 0,
    ocrCalls: 0,
  });
}

export async function verifyIndonesiaUtm(normalized = {}, context = {}) {
  const parsed = parseIndonesiaUtmTable(context.input || {});
  const verification = buildIndonesiaUtmVerification({
    rows: parsed.rows || [],
    crs: parsed.crs || {},
  });
  return Object.freeze({
    verified: parsed.crs?.status === "resolved"
      && Array.isArray(normalized.coordinates)
      && normalized.coordinates.length > 0
      && verification.status !== "mismatch",
    ...verification,
  });
}
