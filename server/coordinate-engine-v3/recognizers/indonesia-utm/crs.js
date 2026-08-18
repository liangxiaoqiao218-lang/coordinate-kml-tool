export const INDONESIA_UTM_SOURCE_CRS = "EPSG:32750";
export const INDONESIA_UTM_OUTPUT_CRS = "EPSG:4326";
export const INDONESIA_UTM_PRECISION_MODE = "utm-projected-x-y";

function normalizeText(value = "") {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

export function hasExplicitUtmWgs84Context(value = "") {
  const text = normalizeText(value);
  return /\bUTM\b|Universal\s+Transverse\s+Mercator/i.test(text)
    && /WGS\s*(?:19)?84|WGS\s*1984/i.test(text);
}

export function parseIndonesiaUtmCrs(value = "") {
  const text = normalizeText(value);
  const epsgMatch = text.match(/\bEPSG\s*[: ]\s*(326|327)(\d{2})\b/i);
  const zoneMatch = text.match(/\b(?:zona|zone)\s*[:#-]?\s*(\d{1,2})\s*([NS])?\b/i)
    || text.match(/\b(\d{1,2})\s*([NS])\b/i);
  const south = /\b(?:south|southern|selatan)\b/i.test(text);
  const north = /\b(?:north|northern|utara)\b/i.test(text);
  const hasUtmWgs84 = hasExplicitUtmWgs84Context(text);

  let zone = zoneMatch ? Number(zoneMatch[1]) : null;
  let hemisphere = zoneMatch?.[2]
    ? (zoneMatch[2].toUpperCase() === "S" ? "south" : "north")
    : south
      ? "south"
      : north
        ? "north"
        : null;

  if (epsgMatch) {
    zone = Number(epsgMatch[2]);
    hemisphere = epsgMatch[1] === "327" ? "south" : "north";
  }

  const validZone = Number.isInteger(zone) && zone >= 1 && zone <= 60;
  const datum = /WGS\s*(?:19)?84|WGS\s*1984/i.test(text) ? "WGS84" : null;
  const epsg = validZone && hemisphere
    ? `EPSG:${hemisphere === "south" ? 327 : 326}${String(zone).padStart(2, "0")}`
    : null;

  return Object.freeze({
    status: hasUtmWgs84 && validZone && Boolean(hemisphere) ? "resolved" : "unresolved",
    hasUtmWgs84,
    datum,
    zone: validZone ? zone : null,
    hemisphere,
    epsg,
    reason: hasUtmWgs84
      ? validZone && hemisphere
        ? "explicit_wgs84_utm_zone_hemisphere"
        : "zone_or_hemisphere_unresolved"
      : "missing_explicit_wgs84_utm_context",
  });
}
