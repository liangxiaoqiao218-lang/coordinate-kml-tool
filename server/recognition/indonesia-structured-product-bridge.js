import {
  formatIndonesiaUtm50Rows,
  getIndonesiaUtm50Info
} from "./family-primary-routing.js";
import { utmToWgs84 } from "../projection/utm.js";

export const INDONESIA_STRUCTURED_PRODUCT_STATUS = Object.freeze({
  NOT_APPLICABLE: "NOT_APPLICABLE",
  ACCEPTED_REVIEW_REQUIRED: "ACCEPTED_REVIEW_REQUIRED",
  INVALID_REVIEW_REQUIRED: "INVALID_REVIEW_REQUIRED"
});

export const INDONESIA_STRUCTURED_PRODUCT_MODE = "indonesia_utm50s_structured_b";
export const INDONESIA_STRUCTURED_ROUTE_STATUS = Object.freeze({
  NOT_SELECTED: "NOT_SELECTED",
  SELECTED: "SELECTED",
  BLOCKED: "BLOCKED"
});

const ALLOWED_EXPLICIT_CRS = new Set(["UTM WGS 1984 ZONA 50S", "EPSG:32750"]);
const ALLOWED_EXPLICIT_DATUM = new Set(["WGS 1984", "WGS 84", "WGS84", "WORLD GEODETIC SYSTEM 1984"]);
const MAX_POINTS = 200;
const CRITICAL_UNRESOLVED_PATTERN = /(?:\bcrs\b|coordinate\s*system|datum|reference\s*frame|\bzone\b|hemisphere|easting|northing|x\s*\/\s*y|xraw|yraw|point\s*order|outer\s*ring|inner\s*ring|missing\s*(?:point|row|coordinate)|坐标系|基准|分区|半球|东坐标|北坐标|点序|缺失.*(?:点|行|坐标))/iu;

function invalid(reasonCode, details = {}) {
  return Object.freeze({
    status: INDONESIA_STRUCTURED_PRODUCT_STATUS.INVALID_REVIEW_REQUIRED,
    reasonCode,
    details: Object.freeze({ ...details }),
    payload: null
  });
}

function text(value) {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeCrs(value) {
  return text(value).replace(/\s+/gu, " ").toUpperCase();
}

export function selectIndonesiaStructuredProductRoute({
  enabled = false,
  requestedMode = "",
  sourceContextText = "",
  controlledRcProjectedColumnsHintAuthorized = false
} = {}) {
  const requested = text(requestedMode);
  if (requested !== INDONESIA_STRUCTURED_PRODUCT_MODE) {
    return Object.freeze({ status: INDONESIA_STRUCTURED_ROUTE_STATUS.NOT_SELECTED, reasonCode: null });
  }
  if (enabled !== true) {
    return Object.freeze({ status: INDONESIA_STRUCTURED_ROUTE_STATUS.BLOCKED, reasonCode: "STRUCTURED_PRODUCT_FEATURE_DISABLED" });
  }
  const context = normalizeCrs(sourceContextText);
  const explicitUtm50s = /(?:UTM\s+WGS\s*1984[^\r\n]{0,80}(?:ZONA|ZONE)\s*50\s*S|EPSG\s*:?\s*32750)/u.test(context);
  const projectedColumns = /(?:\bX\b[^\r\n]{0,60}\bY\b|\bEASTING\b[^\r\n]{0,60}\bNORTHING\b)/u.test(context);
  const projectedColumnsHintOnly = !projectedColumns && controlledRcProjectedColumnsHintAuthorized === true;
  if (!explicitUtm50s || (!projectedColumns && !projectedColumnsHintOnly)) {
    return Object.freeze({
      status: INDONESIA_STRUCTURED_ROUTE_STATUS.BLOCKED,
      reasonCode: "STRUCTURED_PRODUCT_SOURCE_EVIDENCE_REQUIRED",
      evidence: Object.freeze({ explicitUtm50s, projectedColumns, projectedColumnsHintOnly: false })
    });
  }
  return Object.freeze({
    status: INDONESIA_STRUCTURED_ROUTE_STATUS.SELECTED,
    reasonCode: null,
    evidence: Object.freeze({
      explicitUtm50s: true,
      projectedColumns,
      projectedColumnsHintOnly
    })
  });
}

function classifyUnresolvedItems(items) {
  const preserved = items.map(item => text(item));
  return Object.freeze({
    preserved: Object.freeze(preserved),
    critical: Object.freeze(preserved.filter(item => CRITICAL_UNRESOLVED_PATTERN.test(item))),
    reviewOnly: Object.freeze(preserved.filter(item => !CRITICAL_UNRESOLVED_PATTERN.test(item)))
  });
}

function rawPoint(point, index) {
  if (!point || typeof point !== "object" || Array.isArray(point)) return null;
  const id = text(point.id);
  const xRaw = text(point.xRaw);
  const yRaw = text(point.yRaw);
  if (!id || !/^[-+]?\d{5,7}(?:[.,]\d+)?$/u.test(xRaw)
    || !/^[-+]?\d{6,8}(?:[.,]\d+)?$/u.test(yRaw)
    || !Number.isSafeInteger(point.order) || point.order !== index + 1) return null;
  return Object.freeze({
    id,
    order: point.order,
    xRaw,
    yRaw,
    latitudeRaw: text(point.latitudeRaw),
    longitudeRaw: text(point.longitudeRaw)
  });
}

function buildDeterministicSourceText(source, points) {
  const rows = points.map(point => [
    point.id,
    point.xRaw,
    point.yRaw,
    point.latitudeRaw,
    point.longitudeRaw
  ].join(" | "));
  return [
    text(source.title) || "STRUCTURED COORDINATE TABLE",
    text(source.coordinateSystemExplicit),
    "Point | X | Y | LATITUDE | LONGITUDE",
    ...rows
  ].join("\n");
}

export function buildIndonesiaStructuredProductBridge(structuredOutput, {
  transform = utmToWgs84
} = {}) {
  if (structuredOutput == null) {
    return Object.freeze({
      status: INDONESIA_STRUCTURED_PRODUCT_STATUS.NOT_APPLICABLE,
      reasonCode: null,
      details: Object.freeze({}),
      payload: null
    });
  }
  if (!structuredOutput || typeof structuredOutput !== "object" || Array.isArray(structuredOutput)) {
    return invalid("STRUCTURED_OUTPUT_OBJECT_REQUIRED");
  }

  const source = structuredOutput.source;
  const objects = structuredOutput.objects;
  const unresolvedItems = structuredOutput.unresolvedItems;
  if (!source || typeof source !== "object" || Array.isArray(source)
    || !Array.isArray(objects) || !Array.isArray(unresolvedItems)) {
    return invalid("STRUCTURED_OUTPUT_CONTRACT_INCOMPLETE");
  }

  const sourceCrs = normalizeCrs(source.coordinateSystemExplicit);
  if (!ALLOWED_EXPLICIT_CRS.has(sourceCrs)) {
    return invalid("EXPLICIT_UTM50S_CRS_REQUIRED", { sourceCrsPresent: Boolean(sourceCrs) });
  }
  const sourceDatum = normalizeCrs(source.datumExplicit);
  if (sourceDatum && !ALLOWED_EXPLICIT_DATUM.has(sourceDatum)) {
    return invalid("EXPLICIT_DATUM_CONFLICT", { sourceDatumPresent: true });
  }
  if (!Array.isArray(source.points) || source.points.length < 3 || source.points.length > MAX_POINTS) {
    return invalid("STRUCTURED_POINT_COUNT_INVALID", {
      pointCount: Array.isArray(source.points) ? source.points.length : 0
    });
  }

  const points = source.points.map(rawPoint);
  if (points.some(point => point === null)) return invalid("STRUCTURED_POINT_ROW_INVALID");
  const pointIds = points.map(point => point.id);
  if (new Set(pointIds).size !== pointIds.length) return invalid("STRUCTURED_POINT_ID_DUPLICATE");
  if (!Array.isArray(source.pointOrder)
    || JSON.stringify(source.pointOrder.map(String)) !== JSON.stringify(pointIds)) {
    return invalid("STRUCTURED_POINT_ORDER_MISMATCH");
  }
  if (objects.length !== 1) return invalid("SINGLE_POLYGON_OBJECT_REQUIRED", { objectCount: objects.length });
  const object = objects[0];
  if (!object || object.geometryType !== "Polygon"
    || !Array.isArray(object.outerRing) || !Array.isArray(object.innerRings)) {
    return invalid("STRUCTURED_POLYGON_OBJECT_INVALID");
  }
  if (JSON.stringify(object.outerRing.map(String)) !== JSON.stringify(pointIds)) {
    return invalid("STRUCTURED_OUTER_RING_ORDER_MISMATCH");
  }
  if (object.innerRings.length !== 0) {
    return invalid("STRUCTURED_INNER_RINGS_NOT_SUPPORTED", { innerRingCount: object.innerRings.length });
  }
  const unresolved = classifyUnresolvedItems(unresolvedItems);
  if (unresolved.critical.length !== 0) {
    return invalid("STRUCTURED_CRITICAL_UNRESOLVED_ITEMS_PRESENT", {
      unresolvedItemCount: unresolvedItems.length,
      criticalUnresolvedItemCount: unresolved.critical.length
    });
  }

  const deterministicSourceText = buildDeterministicSourceText(source, points);
  const indonesiaUtm50 = getIndonesiaUtm50Info(deterministicSourceText, { transform });
  if (!indonesiaUtm50.structureConfirmed || indonesiaUtm50.transformStatus !== "SUCCESS"
    || indonesiaUtm50.rowCount !== points.length) {
    return invalid(indonesiaUtm50.failureCode || "STRUCTURED_UTM50_TRANSFORM_FAILED", {
      transformStatus: indonesiaUtm50.transformStatus || "UNKNOWN",
      sourcePointCount: points.length,
      transformedPointCount: indonesiaUtm50.rowCount || 0
    });
  }
  if (JSON.stringify(indonesiaUtm50.rows.map(row => String(row.label))) !== JSON.stringify(pointIds)) {
    return invalid("STRUCTURED_TRANSFORMED_LABEL_ORDER_MISMATCH");
  }

  const consistencyStatus = indonesiaUtm50.projectedDmsCrosscheck === "FAIL"
    ? "CONFLICT"
    : indonesiaUtm50.projectedDmsCrosscheck === "PASS" ? "CONSISTENT" : "INCOMPLETE";
  const reviewReasons = [
    "STRUCTURED_MODEL_OUTPUT_REQUIRES_HUMAN_REVIEW",
    "MODEL_SYMBOL_NORMALIZATION_REVIEW_REQUIRED"
  ];
  if (unresolved.reviewOnly.length > 0) reviewReasons.push("STRUCTURED_UNRESOLVED_ITEM_REVIEW_REQUIRED");
  if (consistencyStatus !== "CONSISTENT") reviewReasons.push(`DMS_REFERENCE_${consistencyStatus}`);

  return Object.freeze({
    status: INDONESIA_STRUCTURED_PRODUCT_STATUS.ACCEPTED_REVIEW_REQUIRED,
    reasonCode: null,
    details: Object.freeze({
      sourceCrs: "EPSG:32750",
      pointCount: points.length,
      objectCount: objects.length,
      unresolvedItemCount: unresolvedItems.length,
      modelOutputIsAuthority: false,
      modelKmlAccepted: false
    }),
    payload: Object.freeze({
      rawText: deterministicSourceText,
      coordinates: formatIndonesiaUtm50Rows(indonesiaUtm50),
      precisionMode: "indonesia-utm50s-structured-projected",
      warning: "已按原始结构化 X/Y 和明确 UTM 50S 上下文执行确定性转换；模型输出不是地图或 KML 权威，请核对原图，尤其检查可能被规范化的度分秒符号。",
      indonesiaUtm50,
      coordinateEvidenceConsistencyStatus: consistencyStatus,
      requiresReview: true,
      structuredProductEvidence: Object.freeze({
        schemaVersion: "indonesia_structured_product_evidence_v1",
        sourceCrs: "EPSG:32750",
        sourceCoordinateSystemExplicit: text(source.coordinateSystemExplicit),
        sourceDatumExplicit: source.datumExplicit == null ? null : text(source.datumExplicit),
        pointOrder: Object.freeze([...pointIds]),
        points: Object.freeze(points),
        object: Object.freeze({
          objectId: text(object.objectId),
          displayName: text(object.displayName),
          geometryType: "Polygon",
          outerRing: Object.freeze(object.outerRing.map(String)),
          innerRings: Object.freeze([])
        }),
        unresolvedItems: unresolved.preserved,
        unresolvedItemClassification: Object.freeze({
          criticalCount: unresolved.critical.length,
          reviewOnlyCount: unresolved.reviewOnly.length
        }),
        reviewReasons: Object.freeze(reviewReasons),
        rawFieldsPreserved: true,
        modelOutputIsAuthority: false,
        modelKmlAccepted: false
      }),
      parserTrace: Object.freeze([
        "INDONESIA_STRUCTURED_PRODUCT:strict_contract_accepted",
        "INDONESIA_STRUCTURED_PRODUCT:explicit_epsg_32750",
        "INDONESIA_STRUCTURED_PRODUCT:existing_deterministic_utm_transform",
        `INDONESIA_STRUCTURED_PRODUCT:dms_crosscheck_${indonesiaUtm50.projectedDmsCrosscheck}`,
        "INDONESIA_STRUCTURED_PRODUCT:review_required",
        "INDONESIA_STRUCTURED_PRODUCT:model_output_not_authority"
      ])
    })
  });
}
