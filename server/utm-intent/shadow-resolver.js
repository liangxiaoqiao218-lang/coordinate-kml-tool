const UTM_BLOCKED_FALLBACKS = Object.freeze([
  "bftm_xy",
  "generic_projected_xy",
  "wgs84_chat_coordinates"
]);

const MGRS_BLOCKED_FALLBACKS = Object.freeze(["utm_projected_xy"]);

function createUnknownShadowIntent({ evidence = [], conflicts = [], blockedFallbacks = [] } = {}) {
  return {
    shadowIntent: {
      projection: null,
      datum: null,
      zone: null,
      hemisphere: null,
      epsg: null,
      confidence: "unknown",
      evidence,
      conflicts,
      blockedFallbacks: [...blockedFallbacks]
    }
  };
}

function createCrsConflict(sources) {
  return {
    type: "crs_conflict",
    sources: [...sources]
  };
}

function normalizeSearchText(value = "") {
  return String(value || "")
    .normalize("NFKC")
    .replace(/[‐‑‒–—]/g, "-")
    .replace(/\s+/g, " ")
    .trim();
}

function collectContextText(rawText = "", coordinateContext = {}) {
  const context = coordinateContext && typeof coordinateContext === "object"
    ? coordinateContext
    : { text: coordinateContext };
  const values = [
    rawText,
    context.text,
    context.coordinates,
    context.hint,
    context.caption,
    context.projectionLabel
  ];

  return normalizeSearchText(values.filter(Boolean).join("\n"));
}

function hasExplicitBftmEvidence(text = "") {
  return /\bBFTM\b|Projection\s+BFTM|Coordonn[eé]es?\s+(?:en\s+)?BFTM/i.test(text);
}

function hasMgrsEvidence(text = "") {
  const explicitLabel = /\bMGRS\b|UTM\s+Grid\s+Reference/i.test(text);
  const token = /\b(?:[1-9]|[1-5]\d|60)[C-HJ-NP-X]\s*[A-HJ-NP-Z]{2}\s*\d{1,10}\s+\d{1,10}\b/i.test(text)
    || /\b(?:[1-9]|[1-5]\d|60)[C-HJ-NP-X][A-HJ-NP-Z]{2}\s*\d{2,20}\b/i.test(text);
  return explicitLabel || token;
}

function hasExplicitKyrgyzGkEvidence(text = "") {
  return /Gauss[\s-]*Kr[uü]ger|Гаусс|Крюгер|EPSG\s*:?\s*28413/i.test(text);
}

function findWgs84Evidence(text = "") {
  const match = text.match(/\bWGS\s*(?:19)?84\b|World\s+Geodetic\s+System\s+1984/i);
  return match ? match[0] : "";
}

function findUtmEvidence(text = "") {
  const withoutMgrsLabel = text.replace(/UTM\s+Grid\s+Reference/gi, " ");
  const match = withoutMgrsLabel.match(/\bUTM\b|Universal\s+Transverse\s+Mercator/i);
  return match ? match[0] : "";
}

function findZoneEvidence(text = "") {
  const patterns = [
    /\b(?:ZONE|ZONA|FUSO)\s*(?:UTM\s*)?(60|[1-5]?\d)\s*([NS])\b/i,
    /\bUTM\s*(?:ZONE|ZONA|FUSO)?\s*(60|[1-5]?\d)\s*([NS])\b/i,
    /\b(?:ZONE|ZONA|FUSO)\s*(60|[1-5]?\d)\s*(NORTH|NORTHERN|NORTE|NORTHEN|SOUTH|SOUTHERN|SUL|SELATAN)\b/i
  ];

  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (!match) {
      continue;
    }

    const zone = Number(match[1]);
    if (!Number.isInteger(zone) || zone < 1 || zone > 60) {
      return null;
    }

    const direction = String(match[2] || "").toUpperCase();
    const hemisphere = /^(N|NORTH|NORTHERN|NORTE|NORTHEN)$/.test(direction)
      ? "north"
      : /^(S|SOUTH|SOUTHERN|SUL|SELATAN)$/.test(direction)
        ? "south"
        : null;

    if (!hemisphere) {
      return null;
    }

    return {
      zone,
      hemisphere,
      text: match[0]
    };
  }

  const zoneOnlyMatch = text.match(/\b(?:ZONE|ZONA|FUSO)\s*(?:UTM\s*)?(60|[1-5]?\d)\b/i);
  if (zoneOnlyMatch) {
    const zone = Number(zoneOnlyMatch[1]);
    if (Number.isInteger(zone) && zone >= 1 && zone <= 60) {
      return {
        zone,
        hemisphere: null,
        text: zoneOnlyMatch[0]
      };
    }
  }

  return null;
}

function findExplicitEpsg(text = "") {
  const match = text.match(/\bEPSG\s*:?\s*(326|327)(0[1-9]|[1-5]\d|60)\b/i);
  if (!match) {
    return null;
  }

  return {
    epsg: `EPSG:${match[1]}${match[2]}`,
    zone: Number(match[2]),
    hemisphere: match[1] === "326" ? "north" : "south",
    text: match[0]
  };
}

function buildWgs84UtmEpsg(zone, hemisphere) {
  if (!Number.isInteger(zone) || zone < 1 || zone > 60) {
    return null;
  }
  if (hemisphere !== "north" && hemisphere !== "south") {
    return null;
  }

  const code = (hemisphere === "north" ? 32600 : 32700) + zone;
  return `EPSG:${code}`;
}

export function resolveShadowUtmIntent({ rawText = "", coordinateContext = {} } = {}) {
  const text = collectContextText(rawText, coordinateContext);

  if (!text) {
    return createUnknownShadowIntent();
  }

  const utmEvidence = findUtmEvidence(text);
  const mgrsEvidence = hasMgrsEvidence(text);
  const bftmEvidence = hasExplicitBftmEvidence(text);
  const kyrgyzGkEvidence = hasExplicitKyrgyzGkEvidence(text);

  if (utmEvidence && mgrsEvidence) {
    return createUnknownShadowIntent({
      evidence: [{
        source: "ocr_or_coordinate_context",
        field: "projection",
        value: "utm",
        text: utmEvidence
      }, {
        source: "ocr_or_coordinate_context",
        field: "projection",
        value: "mgrs",
        text: "MGRS token or label"
      }],
      conflicts: [createCrsConflict(["utm", "mgrs"])],
      blockedFallbacks: MGRS_BLOCKED_FALLBACKS
    });
  }

  if (utmEvidence && bftmEvidence) {
    return createUnknownShadowIntent({
      evidence: [{
        source: "ocr_or_coordinate_context",
        field: "projection",
        value: "utm",
        text: utmEvidence
      }, {
        source: "ocr_or_coordinate_context",
        field: "projection",
        value: "bftm",
        text: "Explicit BFTM evidence"
      }],
      conflicts: [createCrsConflict(["utm", "bftm"])],
      blockedFallbacks: UTM_BLOCKED_FALLBACKS
    });
  }

  if (mgrsEvidence) {
    return createUnknownShadowIntent({
      evidence: [{
      source: "shadow_exclusion",
      field: "projection",
      value: "mgrs",
      text: "MGRS structure blocks numeric UTM XY shadow intent."
      }],
      blockedFallbacks: MGRS_BLOCKED_FALLBACKS
    });
  }

  if (bftmEvidence) {
    return createUnknownShadowIntent({
      evidence: [{
        source: "shadow_exclusion",
        field: "projection",
        value: "bftm",
        text: "Explicit BFTM evidence blocks UTM shadow intent."
      }]
    });
  }

  if (kyrgyzGkEvidence) {
    return createUnknownShadowIntent({
      evidence: [{
        source: "shadow_exclusion",
        field: "projection",
        value: "kyrgyzstan_gk",
        text: "Explicit Kyrgyzstan Gauss-Kruger evidence blocks UTM shadow intent."
      }]
    });
  }

  if (!utmEvidence) {
    return createUnknownShadowIntent();
  }

  const datumEvidence = findWgs84Evidence(text);
  const zoneEvidence = findZoneEvidence(text);
  const explicitEpsg = findExplicitEpsg(text);
  const evidence = [{
    source: "ocr_or_coordinate_context",
    field: "projection",
    value: "utm",
    text: utmEvidence
  }];

  if (datumEvidence) {
    evidence.push({
      source: "ocr_or_coordinate_context",
      field: "datum",
      value: "WGS84",
      text: datumEvidence
    });
  }

  if (zoneEvidence) {
    evidence.push({
      source: "ocr_or_coordinate_context",
      field: zoneEvidence.hemisphere ? "zone_hemisphere" : "zone",
      value: zoneEvidence.hemisphere
        ? `${zoneEvidence.zone}${zoneEvidence.hemisphere === "north" ? "N" : "S"}`
        : String(zoneEvidence.zone),
      text: zoneEvidence.text
    });
  }

  if (explicitEpsg) {
    evidence.push({
      source: "ocr_or_coordinate_context",
      field: "epsg",
      value: explicitEpsg.epsg,
      text: explicitEpsg.text
    });
  }

  const zone = zoneEvidence?.zone ?? explicitEpsg?.zone ?? null;
  const hemisphere = zoneEvidence?.hemisphere ?? explicitEpsg?.hemisphere ?? null;
  const derivedEpsg = datumEvidence ? buildWgs84UtmEpsg(zone, hemisphere) : null;
  const epsgConsistent = !explicitEpsg || !derivedEpsg || explicitEpsg.epsg === derivedEpsg;
  if (!epsgConsistent) {
    evidence.push({
      source: "shadow_conflict",
      field: "epsg",
      value: explicitEpsg.epsg,
      text: `Explicit ${explicitEpsg.epsg} conflicts with derived ${derivedEpsg}.`
    });
    return createUnknownShadowIntent({
      evidence,
      conflicts: [createCrsConflict(["utm_zone_hemisphere", "epsg"])],
      blockedFallbacks: UTM_BLOCKED_FALLBACKS
    });
  }

  const confirmed = Boolean(datumEvidence && zoneEvidence && derivedEpsg && epsgConsistent);
  const candidate = !confirmed && Boolean(datumEvidence || zoneEvidence || explicitEpsg);

  return {
    shadowIntent: {
      projection: "utm",
      datum: datumEvidence ? "WGS84" : null,
      zone,
      hemisphere,
      epsg: confirmed ? derivedEpsg : (explicitEpsg?.epsg || null),
      confidence: confirmed ? "confirmed" : candidate ? "candidate" : "unknown",
      evidence,
      conflicts: [],
      blockedFallbacks: [...UTM_BLOCKED_FALLBACKS]
    }
  };
}
