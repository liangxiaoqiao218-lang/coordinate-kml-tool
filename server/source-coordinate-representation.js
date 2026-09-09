import {
  extractDmsSourceStructure,
  normalizeDmsBoundaryIdentity,
  parseDmsSourceCoordinateRow
} from "./recognition/dms-source-structure.js";

const SOURCE_COORDINATE_REPRESENTATION_SCHEMA = "source_coordinate_representation_v1";

function sourceLines(text) {
  return String(text || "").replace(/\r\n/g, "\n").split("\n");
}

function sourceGroups(text) {
  const groups = [];
  let current = [];
  for (const line of sourceLines(text)) {
    if (!line.trim()) {
      if (current.length) groups.push(current);
      current = [];
      continue;
    }
    current.push(line);
  }
  if (current.length) groups.push(current);
  return groups;
}

function enginePointLabels(engine = {}) {
  return (Array.isArray(engine.groups) ? engine.groups : [])
    .flatMap(group => Array.isArray(group?.points) ? group.points : [])
    .map(point => String(point?.label || point?.name || point?.id || "").trim())
    .filter(Boolean);
}

function normalizeAxisOrder(value) {
  const normalized = String(value || "").trim().toLowerCase().replace(/[\s,\-/]+/g, "_");
  if (["latitude_longitude", "lat_lon", "lat_lng"].includes(normalized)) return "latitude_longitude";
  if (["longitude_latitude", "lon_lat", "lng_lat"].includes(normalized)) return "longitude_latitude";
  if (["easting_northing", "x_y"].includes(normalized)) return "easting_northing";
  return null;
}

function sourceAxisOrder(engine = {}, family = "", format = "") {
  const explicit = String(engine?.source_crs?.axisOrder || engine?.source_crs?.axis_order || "").trim();
  if (explicit) return normalizeAxisOrder(explicit);
  const identity = `${family} ${format}`.toLowerCase();
  if (/projected|utm|bftm|kyrgyz|gauss|cadastral|mgrs|x[-_ ]?y/.test(identity)) return "easting_northing";
  if (/wgs84[-_ ]?table|longitude[-_ ]?latitude/.test(identity)) return "longitude_latitude";
  if (/dms|chat|latitude[-_ ]?longitude/.test(identity)) return "latitude_longitude";
  return null;
}

function sourceHemispheres(text) {
  const values = [];
  const pattern = /\d\s*["'\u2032\u2033]?\s*([NSEWO])(?=\s*(?:[,|;]|\s|$))/gi;
  let match;
  while ((match = pattern.exec(String(text || ""))) !== null) values.push(match[1].toUpperCase() === "O" ? "W" : match[1].toUpperCase());
  return [...new Set(values)];
}

function sourceCrsEvidence(payload = {}, engine = {}) {
  return engine?.source_crs
    || payload?.indonesiaUtm50?.sourceCrs
    || payload?.indonesiaUtm50?.source_crs
    || payload?.projection
    || null;
}

function pointCoordinate(point = {}) {
  const latitudeValue = point.latitude ?? point.lat ?? point.y_latitude ?? point.y;
  const longitudeValue = point.longitude ?? point.lng ?? point.lon ?? point.x_longitude ?? point.x;
  if (latitudeValue === null || latitudeValue === undefined || String(latitudeValue).trim() === ""
    || longitudeValue === null || longitudeValue === undefined || String(longitudeValue).trim() === "") return null;
  const latitude = Number(latitudeValue);
  const longitude = Number(longitudeValue);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return null;
  if (Math.abs(latitude) > 90 || Math.abs(longitude) > 180) return null;
  return Object.freeze({
    label: String(point.label || point.name || point.id || "").trim(),
    latitude,
    longitude
  });
}

function parseDecimalCoordinateLine(line, axisOrder) {
  const match = String(line || "").match(/^\s*(?:(\d{1,4}|[A-Z]{1,4}\d{0,3})\s*[\).:\-]?\s+)?(-?\d+(?:\.\d+)?)\s*[,|\s]\s*(-?\d+(?:\.\d+)?)/i);
  if (!match) return null;
  const first = Number(match[2]);
  const second = Number(match[3]);
  if (!Number.isFinite(first) || !Number.isFinite(second)) return null;

  const label = match[1] ? String(match[1]).trim() : "";
  const latitude = axisOrder === "longitude_latitude" ? second : first;
  const longitude = axisOrder === "longitude_latitude" ? first : second;
  if (Math.abs(latitude) > 90 || Math.abs(longitude) > 180) return null;
  return Object.freeze({ label, latitude, longitude });
}

function groupsFromEngine(engine = {}, coordinateDisplayText = "", axisOrder = "latitude_longitude") {
  const engineGroups = (Array.isArray(engine.groups) ? engine.groups : [])
    .map(group => {
      const points = (Array.isArray(group?.points) ? group.points : [])
        .map(pointCoordinate)
        .filter(Boolean);
      const identity = normalizeDmsBoundaryIdentity(
        group?.group_name || group?.groupName || group?.name || group?.title || ""
      );
      return points.length ? Object.freeze({ points, identity }) : null;
    })
    .filter(Boolean);

  if (engineGroups.length) return engineGroups;

  return sourceGroups(coordinateDisplayText)
    .map(group => {
      const points = group.map(line => parseDecimalCoordinateLine(line, axisOrder)).filter(Boolean);
      return points.length ? Object.freeze({ points, identity: "" }) : null;
    })
    .filter(Boolean);
}

function labelMatches(sourceLabel, engineLabel) {
  if (!sourceLabel || !engineLabel) return !sourceLabel && !engineLabel;
  return String(sourceLabel).trim().toUpperCase() === String(engineLabel).trim().toUpperCase();
}

function matchesCoordinate(sourcePoint, enginePoint) {
  return Math.abs(sourcePoint.latitude - enginePoint.latitude) <= sourcePoint.latitudeTolerance
    && Math.abs(sourcePoint.longitude - enginePoint.longitude) <= sourcePoint.longitudeTolerance;
}

function expectedHemisphere(value, positive, negative) {
  if (!Number.isFinite(value) || Object.is(value, -0) || value === 0) return "";
  return value > 0 ? positive : negative;
}

function rawDmsSemanticallyMatchesResult(rawDmsStructure, engineGroups, expectedAxisOrder) {
  if (!rawDmsStructure?.rowCount || !engineGroups.length) {
    return Object.freeze({ verified: false, reason: "missing_rows_or_engine_points" });
  }
  if (!["latitude_longitude", "longitude_latitude"].includes(expectedAxisOrder)) {
    return Object.freeze({ verified: false, reason: "axis_order_unresolved" });
  }
  const comparisonSourceGroups = rawDmsStructure.groupCount !== engineGroups.length
    && rawDmsStructure.reason === "blank_line"
    && engineGroups.length === 1
    && rawDmsStructure.groups.every(group => !normalizeDmsBoundaryIdentity(group.name))
    ? [{ name: null, rows: rawDmsStructure.rows }]
    : rawDmsStructure.groups;
  if (comparisonSourceGroups.length !== engineGroups.length) {
    return Object.freeze({ verified: false, reason: "group_count_mismatch" });
  }

  for (let groupIndex = 0; groupIndex < comparisonSourceGroups.length; groupIndex += 1) {
    const sourceGroup = comparisonSourceGroups[groupIndex];
    const engineGroup = engineGroups[groupIndex];
    if (!engineGroup || sourceGroup.rows.length !== engineGroup.points.length) {
      return Object.freeze({ verified: false, reason: "group_size_mismatch" });
    }
    const sourceIdentity = normalizeDmsBoundaryIdentity(sourceGroup.name);
    if (sourceIdentity !== String(engineGroup.identity || "")) {
      return Object.freeze({ verified: false, reason: "group_identity_mismatch" });
    }

    for (let pointIndex = 0; pointIndex < sourceGroup.rows.length; pointIndex += 1) {
      const sourcePoint = parseDmsSourceCoordinateRow(sourceGroup.rows[pointIndex]);
      const enginePoint = engineGroup.points[pointIndex];
      if (!sourcePoint || !enginePoint) {
        return Object.freeze({ verified: false, reason: "unparseable_point" });
      }
      if (sourcePoint.axisOrder !== expectedAxisOrder) {
        return Object.freeze({ verified: false, reason: "axis_order_mismatch" });
      }
      const expectedLatitudeHemisphere = expectedHemisphere(enginePoint.latitude, "N", "S");
      const expectedLongitudeHemisphere = expectedHemisphere(enginePoint.longitude, "E", "W");
      if (!expectedLatitudeHemisphere || !expectedLongitudeHemisphere
        || sourcePoint.latitudeHemisphere !== expectedLatitudeHemisphere
        || sourcePoint.longitudeHemisphere !== expectedLongitudeHemisphere) {
        return Object.freeze({ verified: false, reason: "hemisphere_mismatch" });
      }
      if (!labelMatches(sourcePoint.label, enginePoint.label)) {
        return Object.freeze({ verified: false, reason: "label_order_mismatch" });
      }
      if (!matchesCoordinate(sourcePoint, enginePoint)) {
        return Object.freeze({ verified: false, reason: "point_value_mismatch" });
      }
    }
  }

  return Object.freeze({ verified: true, reason: "pointwise_dms_semantic_match" });
}

function renderCanonicalEngineGroups(engineGroups, axisOrder) {
  return engineGroups.map(group => group.points.map(point => {
    const first = axisOrder === "longitude_latitude" ? point.longitude : point.latitude;
    const second = axisOrder === "longitude_latitude" ? point.latitude : point.longitude;
    return `${first},${second}`;
  }).join("\n")).join("\n\n");
}

export function buildSourceCoordinateRepresentation(recognitionResult = {}, coordinateEngineV2 = {}) {
  const family = String(coordinateEngineV2?.coordinate_type || recognitionResult?.coordinateType || "").trim() || null;
  const format = String(coordinateEngineV2?.precision_mode || recognitionResult?.precisionMode || "").trim() || null;
  const sourceText = typeof recognitionResult?.coordinates === "string" ? recognitionResult.coordinates : "";
  const coordinateDisplayText = sourceText.trim() ? sourceText.replace(/\r\n/g, "\n") : "";
  const rawDmsStructure = extractDmsSourceStructure(recognitionResult?.rawText || coordinateDisplayText);
  const axisOrder = sourceAxisOrder(coordinateEngineV2, family, format);
  const coordinateAlreadyPreservesDms = extractDmsSourceStructure(coordinateDisplayText).rowCount > 0;
  const engineGroups = groupsFromEngine(coordinateEngineV2, coordinateDisplayText, axisOrder || "latitude_longitude");
  const rawDmsEquivalence = rawDmsSemanticallyMatchesResult(rawDmsStructure, engineGroups, axisOrder);
  const useRawDms = rawDmsEquivalence.verified === true;
  const canonicalEngineDisplay = renderCanonicalEngineGroups(engineGroups, axisOrder);
  const displayText = rawDmsEquivalence.verified === true
    ? rawDmsStructure.displayText
    : (coordinateAlreadyPreservesDms && canonicalEngineDisplay ? canonicalEngineDisplay : coordinateDisplayText);
  const groups = useRawDms
    ? rawDmsStructure.groups.map(group => [...group.rows])
    : sourceGroups(displayText);

  return Object.freeze({
    schema_version: SOURCE_COORDINATE_REPRESENTATION_SCHEMA,
    family,
    format,
    rawText: String(recognitionResult?.rawText || ""),
    rows: useRawDms ? [...rawDmsStructure.rows] : sourceLines(displayText).filter(line => line.trim()),
    groups,
    groupNames: useRawDms ? rawDmsStructure.groups.map(group => group.name) : groups.map(() => null),
    pointLabels: enginePointLabels(coordinateEngineV2),
    axisOrder,
    hemisphere: sourceHemispheres(displayText),
    precision: format,
    sourceCrsEvidence: sourceCrsEvidence(recognitionResult, coordinateEngineV2),
    sourceEquivalence: rawDmsEquivalence.reason,
    displayText,
    editable: Boolean(displayText)
  });
}
