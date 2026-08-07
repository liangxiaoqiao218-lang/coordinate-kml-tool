export const CRS_VISION_SCHEMA_VERSION = "crs_vision_observations_v1";

const ALLOWED_FIELDS = new Set([
  "crs_label",
  "projection_label",
  "datum_label",
  "zone_label",
  "hemisphere_label",
  "epsg_label",
  "grid_reference_label"
]);

export const CRS_VISION_PROMPT = `You are performing literal CRS metadata transcription from a mining or cadastral document image.

Read ONLY visible coordinate reference system metadata. Do not read or return coordinate point rows.

Inspect the whole image, especially:
- the bottom footer and map frame;
- title blocks and legends;
- text immediately above or below coordinate tables;
- projection, datum, coordinate-system, and grid-reference labels.

Target visible text includes:
- UTM or Universal Transverse Mercator;
- WGS84, WGS 1984, or World Geodetic System 1984;
- Zone, Zona, Fuso, Fuseau, zone number, and an explicitly printed hemisphere;
- EPSG identifiers;
- Projection BFTM, ITRF, Gauss-Kruger, MGRS, or Map Ref labels when visibly present.

Literal evidence rules:
- Copy only text that is visibly present in the image.
- Preserve a complete label together, for example: UTM WGS 1984 ZONA 50S.
- Do not infer a CRS from country, filename, map location, language, coordinate values, easting, or northing.
- Do not infer a zone or hemisphere from coordinate ranges.
- Do not derive or invent an EPSG identifier.
- Do not convert, normalize, correct, or complete the visible CRS text.
- Do not output X/Y rows, latitude/longitude rows, point labels, map ticks, scale values, areas, dates, or page numbers.
- If only the word UTM is visible, return only that literal evidence.
- If no CRS or grid-reference metadata is visible, return status none with an empty observations array.

Return strict JSON only, with no markdown:
{
  "status": "observed" | "none",
  "observations": [
    {
      "field": "crs_label" | "projection_label" | "datum_label" | "zone_label" | "hemisphere_label" | "epsg_label" | "grid_reference_label",
      "rawText": "literal visible text",
      "source": "crs_vision",
      "region": "bottom_footer" | "title" | "legend" | "table_caption" | "map_frame" | "document_body" | "unknown"
    }
  ]
}`;

function stripMarkdownFence(value = "") {
  return String(value || "")
    .trim()
    .replace(/^```(?:json)?\s*/i, "")
    .replace(/\s*```$/, "")
    .trim();
}

function parseJsonObject(value = "") {
  const clean = stripMarkdownFence(value);
  try {
    return JSON.parse(clean);
  } catch {
    const start = clean.indexOf("{");
    const end = clean.lastIndexOf("}");
    if (start < 0 || end <= start) return null;
    try {
      return JSON.parse(clean.slice(start, end + 1));
    } catch {
      return null;
    }
  }
}

function looksLikeCoordinatePointRow(rawText = "") {
  const text = String(rawText || "").trim();
  const hasCrsToken = /\b(?:UTM|WGS|EPSG|BFTM|ITRF|MGRS)\b|Gauss[\s-]*Kr[uü]ger|Map\s+Ref|Sistem\s+Koordinat|Syst[eè]me\s+de\s+R[eé]f[eé]rence/i.test(text);
  if (hasCrsToken) return false;
  const projectedPair = /^\s*(?:[A-Z]|\d{1,3})?\s*[|:;,-]?\s*\d{5,}(?:[.,]\d+)?\s*[,|;\s]+\s*\d{5,}(?:[.,]\d+)?\s*$/i.test(text);
  const dmsPair = /\d{1,3}\s*[°º].*[NSEW]\s*[,|;\s]+.*\d{1,3}\s*[°º].*[NSEW]/i.test(text);
  return projectedPair || dmsPair;
}

function normalizeObservation(value = {}) {
  const field = String(value.field || "").trim();
  const rawText = String(value.rawText ?? value.raw_text ?? value.text ?? "").trim();
  if (!ALLOWED_FIELDS.has(field) || !rawText || looksLikeCoordinatePointRow(rawText)) {
    return null;
  }
  return {
    field,
    rawText,
    source: "crs_vision",
    region: String(value.region || "unknown").trim() || "unknown"
  };
}

export function normalizeCrsVisionOutput(value = {}) {
  const observations = (Array.isArray(value.observations) ? value.observations : [])
    .map(normalizeObservation)
    .filter(Boolean);
  return {
    status: observations.length > 0 ? "observed" : "none",
    observations
  };
}

export function parseCrsVisionModelText(modelText = "") {
  const parsed = parseJsonObject(modelText);
  return parsed
    ? normalizeCrsVisionOutput(parsed)
    : { status: "invalid", observations: [] };
}

export async function runCrsVisionPass({ imageItems = [], invokeVision } = {}) {
  if (typeof invokeVision !== "function") {
    throw new TypeError("runCrsVisionPass requires an invokeVision function");
  }
  if (!Array.isArray(imageItems) || imageItems.length === 0) {
    throw new TypeError("runCrsVisionPass requires at least one image item");
  }
  const modelText = await invokeVision({
    prompt: CRS_VISION_PROMPT,
    imageItems
  });
  return parseCrsVisionModelText(modelText);
}
