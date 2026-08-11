export const CADASTRAL_SEMANTIC_VISION_SCHEMA_VERSION = "cadastral_semantic_vision_v1";

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
