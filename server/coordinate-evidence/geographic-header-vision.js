import {
  GEOGRAPHIC_HEADER_SEMANTIC_SCHEMA_VERSION,
  detectGeographicHeaderSemanticEvidence
} from "./geographic-header.js";

export const GEOGRAPHIC_HEADER_VISION_SCHEMA_VERSION = "geographic_header_vision_v1";

export const GEOGRAPHIC_HEADER_VISION_PROMPT = `You are performing literal geographic table-header transcription from a mining or cadastral document image.

Read ONLY visible geographic coordinate table headers and hemisphere indicators.

Target visible header text includes:
- Latitude, Lat, Longitude, Lon, Long;
- North, South, East, West;
- N, S, E, W;
- Nord, Sud, Est, Ouest.

Literal evidence rules:
- Do not read, copy, or return coordinate point rows.
- Do not output numeric coordinates.
- Do not infer hemisphere from country, filename, map location, coordinate values, or CRS text.
- Do not decide whether the coordinates are valid.
- If geographic headers are not clearly visible, return status none with an empty observations array.

Return strict JSON only, with no markdown:
{
  "status": "observed" | "none",
  "observations": [
    {
      "field": "latitude_header" | "longitude_header",
      "indicator": "N" | "S" | "E" | "W" | "North" | "South" | "East" | "West" | "Nord" | "Sud" | "Est" | "Ouest",
      "source": "geographic_header_vision",
      "region": "table_header" | "table_caption" | "document_body" | "unknown"
    }
  ]
}`;

const SECRET_KEY_PATTERN = /api[_-]?key|secret|token|password|authorization|credential|env|raw[_-]?ocr|prompt|model[_-]?response|full[_-]?response|image|buffer|base64/i;
const SECRET_VALUE_PATTERN = /(sk-[a-z0-9_-]{8,}|dashscope[_-]?[a-z0-9_-]*|supabase[_-]?[a-z0-9_-]*|bearer\s+[a-z0-9._-]+|api[_-]?key\s*[:=]|secret\s*[:=]|token\s*[:=]|password\s*[:=]|authorization\s*[:=])/ig;
const COORDINATE_LIKE_TEXT_PATTERN = /(?:\d{1,3}\s*[°º˚]\s*\d{1,2}|\d{1,3}[.,]\d{3,})/;

function cleanText(value = "") {
  return String(value ?? "")
    .replace(SECRET_VALUE_PATTERN, "[REDACTED]")
    .normalize("NFKC")
    .trim();
}

function stripDiacritics(value = "") {
  return cleanText(value)
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "");
}

function parseJsonObject(value = "") {
  const text = cleanText(value)
    .replace(/^```(?:json)?\s*/i, "")
    .replace(/\s*```$/i, "")
    .trim();

  if (!text) return null;

  try {
    return JSON.parse(text);
  } catch {
    const start = text.indexOf("{");
    const end = text.lastIndexOf("}");
    if (start < 0 || end <= start) return null;
    try {
      return JSON.parse(text.slice(start, end + 1));
    } catch {
      return null;
    }
  }
}

function normalizeLatitudeIndicator(value = "") {
  const folded = stripDiacritics(value).toLowerCase();
  if (/^(n|north|nord|north latitude|latitude north)$/.test(folded)) return "N";
  if (/^(s|south|sud|south latitude|latitude south)$/.test(folded)) return "S";
  return "";
}

function normalizeLongitudeIndicator(value = "") {
  const folded = stripDiacritics(value).toLowerCase();
  if (/^(e|east|est|east longitude|longitude east)$/.test(folded)) return "E";
  if (/^(w|o|west|ouest|west longitude|longitude west)$/.test(folded)) return "W";
  return "";
}

function normalizeField(value = "") {
  const field = stripDiacritics(value).toLowerCase().replace(/[\s-]+/g, "_");
  if (/latitude|lat/.test(field)) return "latitude_header";
  if (/longitude|long|lon/.test(field)) return "longitude_header";
  return "";
}

function normalizeRegion(value = "") {
  const region = stripDiacritics(value).toLowerCase().replace(/[\s-]+/g, "_");
  return region && !SECRET_KEY_PATTERN.test(region) ? region.slice(0, 48) : "table_header";
}

function unique(values = []) {
  return [...new Set(values.filter(Boolean))];
}

function safeEntries(value = {}) {
  return Object.entries(value)
    .filter(([key]) => !SECRET_KEY_PATTERN.test(key));
}

function collectTextFromObject(value = {}) {
  const pieces = [];

  for (const [key, entryValue] of safeEntries(value)) {
    if (key === "observations" || key === "semantic") continue;
    if (entryValue === null || entryValue === undefined) continue;
    if (typeof entryValue === "string" || typeof entryValue === "number" || typeof entryValue === "boolean") {
      const field = normalizeField(key);
      if (field === "latitude_header") {
        pieces.push(`Latitude ${entryValue}`);
      } else if (field === "longitude_header") {
        pieces.push(`Longitude ${entryValue}`);
      } else {
        pieces.push(`${key} ${entryValue}`);
      }
    }
  }

  if (Array.isArray(value.observations)) {
    for (const observation of value.observations) {
      if (!observation || typeof observation !== "object") continue;
      const field = normalizeField(observation.field || "");
      if (!field) continue;
      const headerName = field === "latitude_header" ? "Latitude" : "Longitude";
      pieces.push([
        headerName,
        observation.indicator,
        observation.rawText,
        observation.raw_text,
        observation.text,
        observation.value
      ].filter(Boolean).join(" "));
    }
  }

  if (value.semantic && typeof value.semantic === "object") {
    pieces.push([
      "Latitude",
      ...(Array.isArray(value.semantic.latitudeIndicators) ? value.semantic.latitudeIndicators : []),
      "Longitude",
      ...(Array.isArray(value.semantic.longitudeIndicators) ? value.semantic.longitudeIndicators : [])
    ].join(" "));
  }

  return pieces.join("\n");
}

function normalizeObservation(field, indicator, region = "table_header") {
  if (!field || !indicator) return null;
  return Object.freeze({
    field,
    indicator,
    region: normalizeRegion(region)
  });
}

function observationsFromSemantic(semantic = {}) {
  const observations = [];
  for (const indicator of Array.isArray(semantic.latitudeIndicators) ? semantic.latitudeIndicators : []) {
    observations.push(normalizeObservation("latitude_header", indicator));
  }
  for (const indicator of Array.isArray(semantic.longitudeIndicators) ? semantic.longitudeIndicators : []) {
    observations.push(normalizeObservation("longitude_header", indicator));
  }
  return observations.filter(Boolean);
}

function normalizeExplicitObservations(value = {}) {
  if (!Array.isArray(value.observations)) return [];

  return value.observations
    .map(observation => {
      if (!observation || typeof observation !== "object") return null;
      const field = normalizeField(observation.field || "");
      const indicator = field === "latitude_header"
        ? normalizeLatitudeIndicator(observation.indicator || observation.rawText || observation.raw_text || observation.text || observation.value || "")
        : field === "longitude_header"
          ? normalizeLongitudeIndicator(observation.indicator || observation.rawText || observation.raw_text || observation.text || observation.value || "")
          : "";
      return normalizeObservation(field, indicator, observation.region || "table_header");
    })
    .filter(Boolean);
}

function buildSemanticSummary(semantic = {}) {
  return Object.freeze({
    schemaVersion: GEOGRAPHIC_HEADER_SEMANTIC_SCHEMA_VERSION,
    evidenceType: "geographic_header_semantic",
    detected: semantic.detected === true,
    hasLatitudeHeader: semantic.hasLatitudeHeader === true,
    hasLongitudeHeader: semantic.hasLongitudeHeader === true,
    hasHemisphereIndicator: semantic.hasHemisphereIndicator === true,
    latitudeIndicators: Object.freeze(Array.isArray(semantic.latitudeIndicators) ? [...semantic.latitudeIndicators] : []),
    longitudeIndicators: Object.freeze(Array.isArray(semantic.longitudeIndicators) ? [...semantic.longitudeIndicators] : []),
    coordinateOrder: String(semantic.coordinateOrder || "unknown"),
    confidence: String(semantic.confidence?.level || "low"),
    confidenceReason: String(semantic.confidence?.reason || ""),
    reason: String(semantic.reason || "")
  });
}

function buildVisionResult({ status, observations, semantic }) {
  return Object.freeze({
    schemaVersion: GEOGRAPHIC_HEADER_VISION_SCHEMA_VERSION,
    status,
    observations: Object.freeze(observations),
    semantic: buildSemanticSummary(semantic),
    affectsLegacyWinner: false,
    affectsCoordinateResult: false,
    affectsKml: false
  });
}

function hasCoordinateLikeEvidence(input = {}) {
  if (input.coordinateLikeEvidence === true) return true;
  if (Number(input.coordinateRowCount) > 0) return true;
  const text = [
    input.rawText,
    input.coordinates,
    input.semanticHint
  ].filter(Boolean).join("\n");
  return COORDINATE_LIKE_TEXT_PATTERN.test(cleanText(text));
}

function hasProtectedHighAuthorityEvidence(input = {}) {
  return input.explicitGeographicDms === true
    || input.structuredCadastralTable === true
    || input.verifiedUtmTransformation === true
    || input.cadastralGrid?.isCadastralGrid === true
    || input.structuredUtmPriority?.accepted === true
    || input.structuredUtmTable?.accepted === true
    || input.coordinateEngineV2?.coordinate_type === "cote_divoire_geographic_dms_table";
}

export function shouldRunGeographicHeaderVisionPass(input = {}) {
  const imageAvailable = input.imageAvailable === true
    || (Array.isArray(input.imageItems) && input.imageItems.length > 0);
  const existingSemantic = input.geographicHeaderSemantic
    || detectGeographicHeaderSemanticEvidence({
      text: [
        input.rawText,
        input.rawHint,
        input.hint,
        input.semanticHint
      ].filter(Boolean).join("\n")
    });
  const semanticAlreadyDetected = existingSemantic?.detected === true;
  const protectedHighAuthorityEvidence = hasProtectedHighAuthorityEvidence(input);
  const coordinateLikeEvidence = hasCoordinateLikeEvidence(input);
  const shouldRun = Boolean(
    imageAvailable
    && !semanticAlreadyDetected
    && coordinateLikeEvidence
    && !protectedHighAuthorityEvidence
  );
  const reason = !imageAvailable
    ? "image_unavailable"
    : semanticAlreadyDetected
      ? "geographic_header_semantic_already_detected"
      : protectedHighAuthorityEvidence
        ? "high_authority_evidence_already_present"
        : coordinateLikeEvidence
          ? "coordinate_like_rows_without_header_semantic"
          : "no_coordinate_like_evidence";

  return Object.freeze({
    shouldRun,
    reason,
    imageAvailable,
    semanticAlreadyDetected,
    coordinateLikeEvidence,
    protectedHighAuthorityEvidence,
    affectsLegacyWinner: false,
    affectsCoordinateResult: false,
    affectsKml: false
  });
}

export function parseGeographicHeaderVisionOutput(input = {}) {
  const parsed = typeof input === "string" ? parseJsonObject(input) : null;
  const source = parsed || input;
  const sourceText = typeof input === "string" && !parsed
    ? input
    : source && typeof source === "object"
      ? collectTextFromObject(source)
      : "";

  if (source === null || source === undefined || (typeof source !== "object" && typeof input !== "string")) {
    const semantic = detectGeographicHeaderSemanticEvidence("");
    return buildVisionResult({ status: "invalid", observations: [], semantic });
  }

  const semantic = detectGeographicHeaderSemanticEvidence(sourceText);
  const explicitObservations = source && typeof source === "object" ? normalizeExplicitObservations(source) : [];
  const observations = explicitObservations.length > 0
    ? explicitObservations
    : observationsFromSemantic(semantic);

  return buildVisionResult({
    status: semantic.detected ? "observed" : "not_detected",
    observations,
    semantic
  });
}

export async function runGeographicHeaderVisionPass({ imageItems = [], invokeVision } = {}) {
  if (typeof invokeVision !== "function") {
    throw new TypeError("runGeographicHeaderVisionPass requires an invokeVision function");
  }
  if (!Array.isArray(imageItems) || imageItems.length === 0) {
    throw new TypeError("runGeographicHeaderVisionPass requires at least one image item");
  }
  const modelText = await invokeVision({
    prompt: GEOGRAPHIC_HEADER_VISION_PROMPT,
    imageItems
  });
  return parseGeographicHeaderVisionOutput(modelText);
}
