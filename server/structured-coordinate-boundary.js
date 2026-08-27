import { finiteNumberOrNull } from "./coordinate-values.js";
import { UTM30N_CRS, utmToWgs84 } from "./projection/utm.js";
import { getDmsPointCoordinates, parseDmsRows } from "./verification/dms-utils.js";

function pointLabel(text, fallback) {
  return String(text || "").match(/^\s*(?:point\s*)?([A-Z]|\d{1,3})\s*(?:[.|):\-]|\||\s)/i)?.[1]?.toUpperCase() || fallback;
}

const specializedBoundaryDetectors = Object.freeze([
  Object.freeze({ coordinateType: "mozambique_geographic_table", matches: payload => payload.mozambiqueGeographicTable?.isMozambiqueGeographicTable === true }),
  Object.freeze({ coordinateType: "kyrgyzstan_gk", matches: payload => payload.kyrgyzGk?.isKyrgyzGk === true }),
  Object.freeze({ coordinateType: "madagascar_cadastral_grid", matches: payload => payload.cadastralGrid?.isCadastralGrid === true }),
  Object.freeze({ coordinateType: "mgrs_utm_grid_reference", matches: payload => payload.mgrs?.isMgrs === true }),
  Object.freeze({ coordinateType: "bftm_xy", matches: payload => payload.bftmLongTable?.isBftmLongTable === true }),
  Object.freeze({ coordinateType: "decimal_latlon", matches: payload => payload.wgs84TableCoordinates?.isWgs84TableCoordinates === true }),
  Object.freeze({ coordinateType: "decimal_latlon", matches: payload => payload.chatCoordinates?.isChatCoordinates === true })
]);

export function inferStructuredBoundaryType(payload = {}) {
  const specializedTypes = Array.from(new Set(
    specializedBoundaryDetectors
      .filter(detector => detector.matches(payload))
      .map(detector => detector.coordinateType)
  ));
  if (specializedTypes.length > 1) {
    return "ambiguous_specialized_family";
  }
  if (specializedTypes.length === 1) {
    return specializedTypes[0];
  }

  const precisionMode = String(payload.precisionMode || "");
  return precisionMode === "utm30n-projected-x-y" || payload.projection === "utm30n"
    ? "projected_xy"
    : "";
}

export function getStructuredCoordinateBlocks(payload = {}, coordinateType = "") {
  const coordinateLines = String(payload.coordinates || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean);

  if (String(payload.precisionMode || "") !== "point-az-dms-table") {
    return String(payload.coordinates || "")
      .split(/\n\s*\n/g)
      .map(block => block.split(/\r?\n/).map(line => ({ line: line.trim(), label: "" })).filter(entry => entry.line));
  }

  const auditedRows = parseDmsRows(payload.rawText || "");
  const coordinateRows = parseDmsRows(coordinateLines.join("\n"));
  return [coordinateLines.map((line, index) => ({
    line,
    label: auditedRows.length === coordinateLines.length
      ? String(auditedRows[index]?.label || "")
      : String(coordinateRows[index]?.label || "")
  }))];
}

export function parseStructuredBoundaryPoint(raw, coordinateType, index = 0, explicitLabel = "") {
  const fallbackLabel = String(index + 1);
  if (coordinateType === "standard_dms_table") {
    const coordinate = getDmsPointCoordinates(raw);
    if (!coordinate) return null;
    return {
      label: explicitLabel || pointLabel(raw, fallbackLabel),
      lat: coordinate.lat,
      lon: coordinate.lon,
      confidence: 0.85
    };
  }

  if (coordinateType !== "projected_xy") return null;
  const pipeParts = String(raw || "").split("|").map(part => part.trim());
  const numericText = pipeParts.length >= 3 ? `${pipeParts[1]},${pipeParts[2]}` : String(raw || "");
  const match = numericText.match(/^\s*([-+]?\d+(?:\.\d+)?)\s*,\s*([-+]?\d+(?:\.\d+)?)\s*$/);
  if (!match) return null;
  const x = finiteNumberOrNull(match[1]);
  const y = finiteNumberOrNull(match[2]);
  if (x === null || y === null) return null;
  const converted = utmToWgs84(UTM30N_CRS.zone, x, y, true);
  if (!converted) return null;
  return {
    label: explicitLabel || (pipeParts.length >= 3 ? pipeParts[0] : fallbackLabel),
    x,
    y,
    lat: converted.lat,
    lon: converted.lon,
    projection: "utm30n",
    source_crs: { ...UTM30N_CRS },
    confidence: 0.9
  };
}
