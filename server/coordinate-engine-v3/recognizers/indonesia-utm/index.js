import { createNormalizedCoordinateResult, createWarningMetadata, RECOGNIZER_PORT_STATUS } from "../../contracts.js";
import { createRecognizerContract } from "../../recognizer-contract.js";
import {
  INDONESIA_UTM_OUTPUT_CRS,
  INDONESIA_UTM_PRECISION_MODE,
  INDONESIA_UTM_SOURCE_CRS,
} from "./crs.js";
import { canHandleIndonesiaUtm, parseIndonesiaUtmTable } from "./parser.js";
import { transformIndonesiaUtmPoint, transformIndonesiaUtmRows } from "./transform.js";
import { buildIndonesiaUtmVerification, INDONESIA_UTM_VERIFICATION_TOLERANCE, verifyIndonesiaUtm } from "./verify.js";

export const INDONESIA_UTM_RECOGNIZER_ID = "indonesia_utm";

function inferGeometryType(rows = []) {
  if (rows.length === 1) return "point";
  if (rows.length === 2) return "line";
  if (rows.length >= 3) return "polygon";
  return "unknown";
}

function makeWarning({ code, message, point = "", suspectedField = "", currentValue = null, reason = "" } = {}) {
  return createWarningMetadata({
    code,
    severity: "warning",
    message,
    point,
    suspectedField,
    currentValue,
    reason,
  });
}

function buildWarnings(result = {}, verification = {}) {
  const warnings = [];
  if (result.crs?.status !== "resolved") {
    warnings.push(makeWarning({
      code: "CRS_UNRESOLVED",
      message: "Indonesia UTM table was detected, but zone or hemisphere could not be resolved.",
      suspectedField: "crs",
      reason: result.crs?.reason || "zone_or_hemisphere_unresolved",
    }));
  }
  if (verification.status === "mismatch") {
    warnings.push(makeWarning({
      code: "UTM_REFERENCE_MISMATCH",
      message: "Projected UTM coordinates disagree with one or more optional DMS reference rows.",
      suspectedField: "coordinate",
      currentValue: verification.mismatchedPointLabels.join(", "),
      reason: "optional_dms_reference_mismatch",
    }));
  }
  return warnings;
}

function buildSuspectedPoints(rows = [], verification = {}) {
  if (verification.status !== "mismatch") return [];
  const rowByPoint = new Map(rows.map((row) => [String(row.point), row]));
  return verification.mismatchedPointLabels.map((point) => {
    const row = rowByPoint.get(String(point));
    return {
      point: String(point),
      suspectedField: "coordinate",
      currentValue: row ? `${row.easting},${row.northing}` : null,
      reason: "utm_reference_mismatch",
    };
  });
}

export function toIndonesiaUtmKmlCoordinate(point = {}) {
  return `${Number(point.longitude)},${Number(point.latitude)},${Number.isFinite(Number(point.altitude)) ? Number(point.altitude) : 0}`;
}

export async function recognizeIndonesiaUtm(input = {}, context = {}) {
  const latencyBudget = context.latencyBudget;
  if (latencyBudget?.deadlineExceeded?.() === true) {
    return Object.freeze({
      handled: false,
      status: "deadline_exceeded",
      rows: Object.freeze([]),
      warnings: Object.freeze([makeWarning({
        code: "RECOGNITION_DEADLINE_EXCEEDED",
        message: "Recognizer deadline exceeded before deterministic Indonesia UTM parsing.",
      })]),
      providerCalls: 0,
      visionCalls: 0,
      ocrCalls: 0,
    });
  }
  const parsed = parseIndonesiaUtmTable(input);
  return Object.freeze({
    handled: parsed.rows.length > 0,
    status: parsed.rows.length > 0 ? (parsed.crs?.status === "resolved" ? "accepted" : "crs_unresolved") : "not_handled",
    rows: Object.freeze(parsed.rows.map(Object.freeze)),
    crs: parsed.crs || null,
    mapping: parsed.mapping || null,
    reason: parsed.reason,
    warnings: Object.freeze([]),
    providerCalls: 0,
    visionCalls: 0,
    ocrCalls: 0,
  });
}

export function normalizeIndonesiaUtm(result = {}) {
  const rows = Array.isArray(result.rows) ? result.rows : [];
  const crs = result.crs || {};
  const verification = buildIndonesiaUtmVerification({ rows, crs });
  const warnings = buildWarnings(result, verification);
  const suspectedPoints = buildSuspectedPoints(rows, verification);
  const coordinates = crs.status === "resolved"
    ? rows.map((row) => {
      const transformed = transformIndonesiaUtmPoint({
        easting: row.easting,
        northing: row.northing,
        zone: crs.zone,
        hemisphere: crs.hemisphere,
      });
      return {
        label: String(row.point),
        latitude: transformed.latitude,
        longitude: transformed.longitude,
        altitude: 0,
        sourceValue: row.sourceValue,
        sourceProjected: {
          x: row.easting,
          y: row.northing,
          axisSemantics: "x_easting_y_northing",
          sourceCrs: crs.epsg || INDONESIA_UTM_SOURCE_CRS,
        },
      };
    })
    : rows.map((row) => ({
      label: String(row.point),
      sourceValue: row.sourceValue,
      sourceProjected: {
        x: row.easting,
        y: row.northing,
        axisSemantics: "x_easting_y_northing",
        sourceCrs: "unresolved_wgs84_utm",
      },
    }));
  return createNormalizedCoordinateResult({
    coordinateType: "indonesia_utm",
    recognizerId: INDONESIA_UTM_RECOGNIZER_ID,
    coordinates,
    geometryType: inferGeometryType(rows),
    crs: crs.epsg || "unresolved_wgs84_utm",
    precisionMode: INDONESIA_UTM_PRECISION_MODE,
    warnings,
    suspectedPoints,
    sourceTrace: [
      "indonesia_utm:deterministic_structured_table",
      crs.epsg || "crs_unresolved",
      INDONESIA_UTM_OUTPUT_CRS,
    ],
  });
}

export const indonesiaUtmRecognizer = createRecognizerContract({
  recognizerId: INDONESIA_UTM_RECOGNIZER_ID,
  coordinateType: "indonesia_utm",
  portStatus: RECOGNIZER_PORT_STATUS.IMPLEMENTED,
  canHandle: canHandleIndonesiaUtm,
  recognize: recognizeIndonesiaUtm,
  normalize: normalizeIndonesiaUtm,
  verify: verifyIndonesiaUtm,
});

export {
  canHandleIndonesiaUtm,
  parseIndonesiaUtmTable,
  transformIndonesiaUtmPoint,
  transformIndonesiaUtmRows,
  buildIndonesiaUtmVerification,
  INDONESIA_UTM_SOURCE_CRS,
  INDONESIA_UTM_OUTPUT_CRS,
  INDONESIA_UTM_PRECISION_MODE,
  INDONESIA_UTM_VERIFICATION_TOLERANCE,
  verifyIndonesiaUtm,
};

export default indonesiaUtmRecognizer;
