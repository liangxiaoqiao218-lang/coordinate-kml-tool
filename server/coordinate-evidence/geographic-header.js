export const GEOGRAPHIC_HEADER_SEMANTIC_SCHEMA_VERSION = "geographic_header_semantic_v1";

const LATITUDE_HEADER_PATTERN = /\b(?:lat(?:itude)?|纬度)\b|北纬|南纬/i;
const LONGITUDE_HEADER_PATTERN = /\b(?:lon(?:gitude)?|long|经度)\b|东经|西经/i;
const POINT_HEADER_PATTERN = /\b(?:points?|pts?|point\s*id|label|no\.?|num|编号|点号)\b/i;

const LATITUDE_FORWARD_PATTERN = /\b(?:lat(?:itude)?|纬度)\b\s*[:：|/-]?\s*(n|s|north|south|nord|sud|南|北)\b|北纬|南纬/i;
const LATITUDE_REVERSE_PATTERN = /\b(n|s|north|south|nord|sud|南|北)\b\s*[:：|/-]?\s*\b(?:lat(?:itude)?|纬度)\b/i;
const LONGITUDE_FORWARD_PATTERN = /\b(?:lon(?:gitude)?|long|经度)\b\s*[:：|/-]?\s*(e|w|o|east|west|est|ouest|东|西)\b|东经|西经/i;
const LONGITUDE_REVERSE_PATTERN = /\b(e|w|o|east|west|est|ouest|东|西)\b\s*[:：|/-]?\s*\b(?:lon(?:gitude)?|long|经度)\b/i;

const COORDINATE_LIKE_ROW_PATTERN = /(?:\d{1,3}\s*[°º]\s*\d{1,2}|\d{1,3}[.,]\d{3,})/;
const SECRET_VALUE_PATTERN = /(sk-[a-z0-9_-]{8,}|dashscope[_-]?[a-z0-9_-]*|supabase[_-]?[a-z0-9_-]*|bearer\s+[a-z0-9._-]+|api[_-]?key\s*[:=]|secret\s*[:=]|token\s*[:=]|password\s*[:=]|authorization\s*[:=])/ig;

function cleanText(value = "") {
  return String(value ?? "")
    .replace(SECRET_VALUE_PATTERN, "[REDACTED]")
    .normalize("NFKC")
    .replace(/[’‘]/g, "'")
    .trim();
}

function stripDiacritics(value = "") {
  return String(value || "")
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "");
}

function splitSegments(value = "") {
  return cleanText(value)
    .split(/\r?\n|[|\t;]/)
    .map(segment => segment.trim())
    .filter(Boolean);
}

function normalizeLatitudeIndicator(value = "") {
  const folded = stripDiacritics(value).toLowerCase();
  if (/^(n|north|nord|北)$/.test(folded)) return "N";
  if (/^(s|south|sud|南)$/.test(folded)) return "S";
  if (/北纬/.test(value)) return "N";
  if (/南纬/.test(value)) return "S";
  return "";
}

function normalizeLongitudeIndicator(value = "") {
  const folded = stripDiacritics(value).toLowerCase();
  if (/^(e|east|est|东)$/.test(folded)) return "E";
  if (/^(w|o|west|ouest|西)$/.test(folded)) return "W";
  if (/东经/.test(value)) return "E";
  if (/西经/.test(value)) return "W";
  return "";
}

function unique(values = []) {
  return [...new Set(values.filter(Boolean))];
}

function matchAssociatedIndicators(segments = [], forwardPattern, reversePattern, normalizeIndicator) {
  const indicators = [];

  for (const segment of segments) {
    const folded = stripDiacritics(segment);
    const forward = folded.match(forwardPattern) || segment.match(forwardPattern);
    const reverse = folded.match(reversePattern) || segment.match(reversePattern);
    const directChinese = segment.match(/北纬|南纬|东经|西经/);

    if (forward?.[1]) indicators.push(normalizeIndicator(forward[1]));
    if (reverse?.[1]) indicators.push(normalizeIndicator(reverse[1]));
    if (directChinese?.[0]) indicators.push(normalizeIndicator(directChinese[0]));
  }

  return unique(indicators);
}

function inferCoordinateOrder(value = "") {
  const text = stripDiacritics(cleanText(value)).toLowerCase();
  const latitudeIndex = text.search(LATITUDE_HEADER_PATTERN);
  const longitudeIndex = text.search(LONGITUDE_HEADER_PATTERN);

  if (latitudeIndex < 0 || longitudeIndex < 0) return "unknown";
  return latitudeIndex < longitudeIndex ? "latitude_longitude" : "longitude_latitude";
}

function buildConfidence({ hasLatitudeHeader, hasLongitudeHeader, hasHemisphereIndicator, hasCoordinateLikeRows, hasPointHeader } = {}) {
  if (hasLatitudeHeader && hasLongitudeHeader && hasHemisphereIndicator && (hasCoordinateLikeRows || hasPointHeader)) {
    return Object.freeze({
      level: "high",
      reason: "latitude_longitude_header_with_hemisphere"
    });
  }
  if (hasLatitudeHeader && hasLongitudeHeader && hasHemisphereIndicator) {
    return Object.freeze({
      level: "medium",
      reason: "latitude_longitude_header_with_hemisphere_no_rows"
    });
  }
  return Object.freeze({
    level: "low",
    reason: "insufficient_geographic_header_semantic"
  });
}

export function detectGeographicHeaderSemanticEvidence(input = {}) {
  const text = typeof input === "string"
    ? input
    : [
        input.text,
        input.ocrText,
        input.semanticHint,
        input.headerText
      ].filter(Boolean).join("\n");
  const sanitizedText = cleanText(text);
  const foldedText = stripDiacritics(sanitizedText);
  const segments = splitSegments(sanitizedText);

  const hasLatitudeHeader = LATITUDE_HEADER_PATTERN.test(foldedText) || LATITUDE_HEADER_PATTERN.test(sanitizedText);
  const hasLongitudeHeader = LONGITUDE_HEADER_PATTERN.test(foldedText) || LONGITUDE_HEADER_PATTERN.test(sanitizedText);
  const latitudeIndicators = matchAssociatedIndicators(
    segments,
    LATITUDE_FORWARD_PATTERN,
    LATITUDE_REVERSE_PATTERN,
    normalizeLatitudeIndicator
  );
  const longitudeIndicators = matchAssociatedIndicators(
    segments,
    LONGITUDE_FORWARD_PATTERN,
    LONGITUDE_REVERSE_PATTERN,
    normalizeLongitudeIndicator
  );
  const hasPointHeader = POINT_HEADER_PATTERN.test(foldedText) || POINT_HEADER_PATTERN.test(sanitizedText);
  const hasCoordinateLikeRows = sanitizedText
    .split(/\r?\n/)
    .some(line => COORDINATE_LIKE_ROW_PATTERN.test(line) && !LATITUDE_HEADER_PATTERN.test(line) && !LONGITUDE_HEADER_PATTERN.test(line));
  const hasHemisphereIndicator = latitudeIndicators.length > 0 || longitudeIndicators.length > 0;
  const detected = Boolean(hasLatitudeHeader && hasLongitudeHeader && hasHemisphereIndicator);
  const confidence = buildConfidence({
    hasLatitudeHeader,
    hasLongitudeHeader,
    hasHemisphereIndicator,
    hasCoordinateLikeRows,
    hasPointHeader
  });

  return Object.freeze({
    schemaVersion: GEOGRAPHIC_HEADER_SEMANTIC_SCHEMA_VERSION,
    evidenceType: "geographic_header_semantic",
    detected,
    hasLatitudeHeader,
    hasLongitudeHeader,
    hasHemisphereIndicator,
    latitudeIndicators: Object.freeze(latitudeIndicators),
    longitudeIndicators: Object.freeze(longitudeIndicators),
    coordinateOrder: inferCoordinateOrder(sanitizedText),
    hasPointHeader,
    hasCoordinateLikeRows,
    countryIndependent: true,
    confidence,
    reason: detected ? confidence.reason : "missing_latitude_longitude_hemisphere_header"
  });
}

function normalizeReasons(values = []) {
  return unique(values.map(value => cleanText(value)).filter(Boolean));
}

function inferRoutingSource(reasons = []) {
  if (reasons.includes("country_filename_cue")) return "filename";
  if (reasons.includes("geographic_header_semantic")) return "semantic";
  if (reasons.includes("raw_text_geographic_header_semantic")) return "raw_text";
  if (reasons.includes("hint_geographic_header_semantic")) return "hint";
  return "";
}

export function shouldRunGeographicHeaderSupplementalProducer(input = {}) {
  const countryCueDetected = input.countryCueDetected === true;
  const geographicHeaderSemantic = input.geographicHeaderSemantic
    || detectGeographicHeaderSemanticEvidence({
      text: [
        input.rawText,
        input.rawHint,
        input.hint,
        input.semanticHint
      ].filter(Boolean).join("\n")
    });
  const reasons = [];

  if (countryCueDetected) {
    reasons.push(input.countryCueSource === "filename" ? "country_filename_cue" : "country_context_cue");
  }
  if (geographicHeaderSemantic?.detected === true) {
    reasons.push("geographic_header_semantic");
    if (detectGeographicHeaderSemanticEvidence(input.rawText || "").detected) {
      reasons.push("raw_text_geographic_header_semantic");
    }
    if (detectGeographicHeaderSemanticEvidence([
      input.rawHint,
      input.hint,
      input.semanticHint
    ].filter(Boolean).join("\n")).detected) {
      reasons.push("hint_geographic_header_semantic");
    }
  }

  const normalizedReasons = normalizeReasons(reasons);

  return Object.freeze({
    shouldRun: normalizedReasons.length > 0,
    reasons: Object.freeze(normalizedReasons),
    source: inferRoutingSource(normalizedReasons),
    geographicHeaderSemantic,
    affectsLegacyWinner: false,
    affectsCoordinateResult: false,
    affectsKml: false
  });
}
