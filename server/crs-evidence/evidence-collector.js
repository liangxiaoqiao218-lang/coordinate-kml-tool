function unique(values) {
  return [...new Set(values.filter(value => value !== null && value !== undefined && value !== ""))];
}

function normalizeHemisphere(value = "") {
  const token = String(value || "").toUpperCase();
  if (/^(?:N|NORTH|NORTHERN|NORTE|NORD)$/.test(token)) return "north";
  if (/^(?:S|SOUTH|SOUTHERN|SUL|SELATAN|SUD)$/.test(token)) return "south";
  return null;
}

function findUtmZoneClaims(text = "") {
  const claims = [];
  const pattern = /\b(?:ZONE|ZONA|FUSO|FUSEAU)\s*(?:UTM\s*)?(60|[1-5]?\d)(?:\s*([NS]|NORTH|NORTHERN|NORTE|NORD|SOUTH|SOUTHERN|SUL|SELATAN|SUD))?\b/gi;
  for (const match of String(text || "").matchAll(pattern)) {
    const zone = Number(match[1]);
    if (!Number.isInteger(zone) || zone < 1 || zone > 60) continue;
    claims.push({
      zone,
      hemisphere: normalizeHemisphere(match[2]),
      rawText: match[0]
    });
  }
  return claims;
}

function findExplicitEpsgClaims(text = "") {
  return unique(Array.from(String(text || "").matchAll(/\bEPSG\s*:?\s*(\d{4,6})\b/gi), match => `EPSG:${match[1]}`));
}

function hasMgrsStructure(text = "") {
  return /\bMGRS\b|UTM\s+Grid\s+Reference|Map\s+Ref/i.test(text)
    || /\b(?:[1-9]|[1-5]\d|60)[C-HJ-NP-X][A-HJ-NP-Z]{2}\b/i.test(text)
    || /\b(?:[1-9]|[1-5]\d|60)[C-HJ-NP-X]\s*[A-HJ-NP-Z]{2}\s*\d{1,10}\s+\d{1,10}\b/i.test(text);
}

function hasExplicitUtmCrs(text = "") {
  const withoutGridReference = String(text || "").replace(/UTM\s+Grid\s+Reference/gi, " ");
  return /\bUTM\b|Universal\s+Transverse\s+Mercator/i.test(withoutGridReference);
}

function evidenceFor(observations, predicate) {
  return observations.filter(observation => predicate(observation.rawText));
}

export function collectCrsEvidence(acquisition = {}) {
  const observations = Array.isArray(acquisition.observations) ? acquisition.observations : [];
  const text = observations.map(observation => observation.rawText).join("\n");
  const mgrs = hasMgrsStructure(text);
  const utm = hasExplicitUtmCrs(text);
  const bftm = /\bBFTM\b|Projection\s+BFTM/i.test(text);
  const kyrgyzGk = /Gauss[\s-]*Kr[uü]ger|Гаусс|Крюгер|EPSG\s*:?\s*28413/i.test(text);
  const datumClaims = unique([
    /\bWGS\s*(?:19)?84\b|World\s+Geodetic\s+System\s+1984/i.test(text) ? "WGS84" : null
  ]);
  const zoneClaims = findUtmZoneClaims(text);
  const zones = unique(zoneClaims.map(claim => claim.zone));
  const hemispheres = unique(zoneClaims.map(claim => claim.hemisphere));
  const epsgClaims = findExplicitEpsgClaims(text);
  const conflicts = [];

  if (utm && mgrs) conflicts.push({ type: "crs_conflict", sources: ["utm", "mgrs"] });
  if (utm && bftm) conflicts.push({ type: "crs_conflict", sources: ["utm", "bftm"] });
  if (zones.length > 1) conflicts.push({ type: "crs_conflict", sources: zones.map(zone => `zone:${zone}`) });
  if (hemispheres.length > 1) conflicts.push({ type: "crs_conflict", sources: hemispheres.map(value => `hemisphere:${value}`) });
  if (epsgClaims.length > 1) conflicts.push({ type: "crs_conflict", sources: epsgClaims });

  const usableUtm = utm && !mgrs && !bftm && !kyrgyzGk && conflicts.length === 0;
  return {
    projection: usableUtm ? "utm" : null,
    datum: usableUtm && datumClaims.length === 1 ? datumClaims[0] : null,
    zone: usableUtm && zones.length === 1 ? zones[0] : null,
    hemisphere: usableUtm && hemispheres.length === 1 ? hemispheres[0] : null,
    epsg: usableUtm && epsgClaims.length === 1 ? epsgClaims[0] : null,
    evidence: observations,
    conflicts,
    exclusions: [
      ...(bftm ? ["bftm"] : []),
      ...(mgrs ? ["mgrs"] : []),
      ...(kyrgyzGk ? ["kyrgyzstan_gk"] : [])
    ],
    evidenceByField: {
      projection: evidenceFor(observations, rawText => hasExplicitUtmCrs(rawText)),
      datum: evidenceFor(observations, rawText => /\bWGS\s*(?:19)?84\b|World\s+Geodetic\s+System\s+1984/i.test(rawText)),
      zone_hemisphere: evidenceFor(observations, rawText => findUtmZoneClaims(rawText).length > 0),
      epsg: evidenceFor(observations, rawText => findExplicitEpsgClaims(rawText).length > 0)
    }
  };
}
