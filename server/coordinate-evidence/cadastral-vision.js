export const CADASTRAL_SEMANTIC_VISION_SCHEMA_VERSION = "cadastral_semantic_vision_v1";

export const CADASTRAL_SEMANTIC_VISION_PROMPT = `You are performing literal cadastral table-header observation from a mining or cadastral document image.

Read ONLY visible cadastral table headers and table-caption semantics.

Target visible text includes:
- num, No., Number, N°, №;
- XV, X V;
- YV, Y V;
- Liste_Carrés, Liste Carres, Liste Carrés;
- carreau, carreaux, carrés miniers;
- cadastral grid, grille cadastrale, mineral cadastral grid.

Literal evidence rules:
- Do not read, copy, or return coordinate rows.
- Do not return XV or YV numeric values.
- Do not output latitude, longitude, X/Y coordinate values, or KML.
- Do not infer from country, filename, map position, CRS text, or coordinate values.
- If cadastral headers are not clearly visible, return status none with an empty indicators array.

Return strict JSON only, with no markdown:
{
  "status": "observed" | "none",
  "tableType": "num_xv_yv" | "unknown",
  "indicators": ["num", "XV", "YV"],
  "layoutHints": {
    "hasListeCarres": true,
    "hasCadastralGrid": true,
    "hasTableStructure": true
  }
}`;

const SECRET_KEY_PATTERN = /api[_-]?key|secret|token|password|authorization|credential|env|raw[_-]?ocr|prompt|model[_-]?response|full[_-]?response|image|buffer|base64/i;
const SECRET_VALUE_PATTERN = /(sk-[a-z0-9_-]{8,}|dashscope[_-]?[a-z0-9_-]*|supabase[_-]?[a-z0-9_-]*|bearer\s+[a-z0-9._-]+|api[_-]?key\s*[:=]|secret\s*[:=]|token\s*[:=]|password\s*[:=]|authorization\s*[:=])/ig;

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

function safeEntries(value = {}) {
  return Object.entries(value)
    .filter(([key]) => !SECRET_KEY_PATTERN.test(key));
}

function collectTextFromValue(value, depth = 0) {
  if (value === null || value === undefined || depth > 4) return "";
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return cleanText(value);
  }
  if (Array.isArray(value)) {
    return value.map(item => collectTextFromValue(item, depth + 1)).filter(Boolean).join("\n");
  }
  if (typeof value !== "object") return "";

  const pieces = [];
  for (const [key, entryValue] of safeEntries(value)) {
    pieces.push(key);
    pieces.push(collectTextFromValue(entryValue, depth + 1));
  }
  return pieces.filter(Boolean).join("\n");
}

function collectText(input) {
  const parsed = typeof input === "string" ? parseJsonObject(input) : null;
  if (parsed) return collectTextFromValue(parsed);
  if (typeof input === "string") return cleanText(input);
  if (input && typeof input === "object") return collectTextFromValue(input);
  return "";
}

function hasNumIndicator(text = "") {
  const folded = stripDiacritics(text);
  return /(?:^|[^\p{L}\p{N}])(?:num|n[°ºo]?|no\.?|number|numero|numero|№|#)(?:$|[^\p{L}\p{N}])/iu.test(folded);
}

function hasXvIndicator(text = "") {
  const folded = stripDiacritics(text);
  return /(?:^|[^\p{L}\p{N}])x\s*v(?:$|[^\p{L}\p{N}])/iu.test(folded);
}

function hasYvIndicator(text = "") {
  const folded = stripDiacritics(text);
  return /(?:^|[^\p{L}\p{N}])y\s*v(?:$|[^\p{L}\p{N}])/iu.test(folded);
}

function hasListeCarresIndicator(text = "") {
  const folded = stripDiacritics(text).toLowerCase();
  return /liste[_\s-]*carres?/.test(folded)
    || /carres?\s+miniers?/.test(folded)
    || /carreaux?/.test(folded);
}

function hasCadastralGridIndicator(text = "") {
  const folded = stripDiacritics(text).toLowerCase();
  return /cadastral|cadastre|grille\s+cadastrale|mineral\s+cadastral|矿权|网格/.test(folded);
}

const PROJECTED_COORDINATE_PATTERN = /\b\d{5,}(?:[.,]\d+)?\s*[,;|\s]\s*\d{5,}(?:[.,]\d+)?\b/;

function hasProjectedCoordinateAmbiguity(input = {}) {
  if (input.projectedCoordinateAmbiguity === true) return true;
  if (input.utmAccepted === true || input.utm30Accepted === true) return true;
  if (input.utmProjected === true || input.typedUtmAccepted === true) return true;
  const text = [
    input.rawText,
    input.coordinates,
    input.semanticHint
  ].filter(Boolean).join("\n");
  return PROJECTED_COORDINATE_PATTERN.test(cleanText(text));
}

function hasPossibleCadastralLayout(input = {}) {
  if (input.possibleCadastralLayout === true) return true;
  const text = [
    input.rawText,
    input.rawHint,
    input.hint,
    input.semanticHint
  ].filter(Boolean).join("\n");
  return hasListeCarresIndicator(text)
    || hasCadastralGridIndicator(text)
    || (hasXvIndicator(text) && hasYvIndicator(text));
}

function hasProtectedHighAuthorityEvidence(input = {}) {
  return input.explicitGeographicDms === true
    || input.structuredCadastralTable === true
    || input.verifiedUtmTransformation === true
    || input.cadastralGrid?.isCadastralGrid === true
    || input.cadastralSemanticVision?.detected === true
    || input.structuredUtmPriority?.accepted === true
    || input.structuredUtmTable?.accepted === true;
}

function buildIndicators({ hasNum, hasXv, hasYv }) {
  return Object.freeze([
    hasNum ? "num" : "",
    hasXv ? "XV" : "",
    hasYv ? "YV" : ""
  ].filter(Boolean));
}

function buildResult({
  status,
  detected,
  tableType = "unknown",
  indicators = [],
  layoutHints = {},
  confidence = "low",
  reason = ""
}) {
  return Object.freeze({
    schemaVersion: CADASTRAL_SEMANTIC_VISION_SCHEMA_VERSION,
    status,
    detected: detected === true,
    tableType,
    indicators: Object.freeze([...indicators]),
    layoutHints: Object.freeze({
      hasListeCarres: layoutHints.hasListeCarres === true,
      hasCadastralGrid: layoutHints.hasCadastralGrid === true,
      hasTableStructure: layoutHints.hasTableStructure === true
    }),
    confidence,
    reason,
    affectsLegacyWinner: false,
    affectsCoordinateResult: false,
    affectsKml: false
  });
}

export function parseCadastralSemanticVisionOutput(input = {}) {
  if (input === null || input === undefined || (typeof input !== "object" && typeof input !== "string")) {
    return buildResult({
      status: "invalid",
      detected: false,
      reason: "invalid_input"
    });
  }

  const text = collectText(input);
  if (!text) {
    return buildResult({
      status: "invalid",
      detected: false,
      reason: "empty_input"
    });
  }

  const hasNum = hasNumIndicator(text);
  const hasXv = hasXvIndicator(text);
  const hasYv = hasYvIndicator(text);
  const hasListeCarres = hasListeCarresIndicator(text);
  const hasCadastralGrid = hasCadastralGridIndicator(text);
  const hasTableStructure = hasXv && hasYv && (hasNum || hasListeCarres || hasCadastralGrid);
  const detected = hasTableStructure;
  const indicators = buildIndicators({ hasNum, hasXv, hasYv });

  return buildResult({
    status: detected ? "observed" : "not_detected",
    detected,
    tableType: detected ? "num_xv_yv" : "unknown",
    indicators,
    layoutHints: {
      hasListeCarres,
      hasCadastralGrid,
      hasTableStructure
    },
    confidence: detected && indicators.length >= 3 ? "high" : detected ? "medium" : "low",
    reason: detected
      ? "num_xv_yv_cadastral_table_visible"
      : "cadastral_semantic_not_detected"
  });
}

export function shouldRunCadastralSemanticVisionPass(input = {}) {
  const imageAvailable = input.imageAvailable === true
    || (Array.isArray(input.imageItems) && input.imageItems.length > 0);
  const existingSemantic = input.cadastralSemanticVision
    || parseCadastralSemanticVisionOutput([
      input.rawText,
      input.rawHint,
      input.hint,
      input.semanticHint
    ].filter(Boolean).join("\n"));
  const semanticAlreadyDetected = existingSemantic?.detected === true;
  const protectedHighAuthorityEvidence = hasProtectedHighAuthorityEvidence(input);
  const projectedCoordinateAmbiguity = hasProjectedCoordinateAmbiguity(input);
  const possibleCadastralLayout = hasPossibleCadastralLayout(input);
  const shouldRun = Boolean(
    imageAvailable
    && !semanticAlreadyDetected
    && !protectedHighAuthorityEvidence
    && (projectedCoordinateAmbiguity || possibleCadastralLayout)
  );
  const reasons = [];
  if (!imageAvailable) reasons.push("image_unavailable");
  if (semanticAlreadyDetected) reasons.push("cadastral_semantic_already_detected");
  if (protectedHighAuthorityEvidence) reasons.push("high_authority_evidence_already_present");
  if (projectedCoordinateAmbiguity) reasons.push("projected_coordinate_ambiguity");
  if (possibleCadastralLayout) reasons.push("possible_cadastral_layout");
  if (shouldRun && !semanticAlreadyDetected) reasons.unshift("cadastral_candidate_missing");
  if (reasons.length === 0) reasons.push("no_cadastral_semantic_trigger");

  return Object.freeze({
    shouldRun,
    reasons: Object.freeze([...new Set(reasons)]),
    imageAvailable,
    semanticAlreadyDetected,
    protectedHighAuthorityEvidence,
    projectedCoordinateAmbiguity,
    possibleCadastralLayout,
    affectsLegacyWinner: false,
    affectsCoordinateResult: false,
    affectsKml: false
  });
}

export async function runCadastralSemanticVisionPass({ imageItems = [], invokeVision } = {}) {
  if (typeof invokeVision !== "function") {
    throw new TypeError("runCadastralSemanticVisionPass requires an invokeVision function");
  }
  if (!Array.isArray(imageItems) || imageItems.length === 0) {
    throw new TypeError("runCadastralSemanticVisionPass requires at least one image item");
  }
  const modelText = await invokeVision({
    prompt: CADASTRAL_SEMANTIC_VISION_PROMPT,
    imageItems
  });
  return parseCadastralSemanticVisionOutput(modelText);
}
