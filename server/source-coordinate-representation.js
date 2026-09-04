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
    .map(point => String(point?.label || "").trim())
    .filter(Boolean);
}

function sourceAxisOrder(engine = {}, family = "", format = "") {
  const explicit = String(engine?.source_crs?.axisOrder || engine?.source_crs?.axis_order || "").trim();
  if (explicit) return explicit;
  const identity = `${family} ${format}`.toLowerCase();
  if (/projected|utm|bftm|kyrgyz|gauss|cadastral|mgrs|x[-_ ]?y/.test(identity)) return "easting_northing";
  if (/wgs84[-_ ]?table|longitude[-_ ]?latitude/.test(identity)) return "longitude_latitude";
  if (/dms|chat|latitude[-_ ]?longitude/.test(identity)) return "latitude_longitude";
  return null;
}

function sourceHemispheres(text) {
  const values = [];
  const pattern = /\d\s*["'\u2032\u2033]?\s*([NSEWO])(?=\s*(?:[,|;]|$))/gi;
  let match;
  while ((match = pattern.exec(String(text || ""))) !== null) values.push(match[1].toUpperCase());
  return [...new Set(values)];
}

function sourceCrsEvidence(payload = {}, engine = {}) {
  return engine?.source_crs
    || payload?.indonesiaUtm50?.sourceCrs
    || payload?.indonesiaUtm50?.source_crs
    || payload?.projection
    || null;
}

export function buildSourceCoordinateRepresentation(recognitionResult = {}, coordinateEngineV2 = {}) {
  const family = String(coordinateEngineV2?.coordinate_type || recognitionResult?.coordinateType || "").trim() || null;
  const format = String(coordinateEngineV2?.precision_mode || recognitionResult?.precisionMode || "").trim() || null;
  const sourceText = typeof recognitionResult?.coordinates === "string" ? recognitionResult.coordinates : "";
  const displayText = sourceText.trim() ? sourceText.replace(/\r\n/g, "\n") : "";
  const groups = sourceGroups(displayText);

  return Object.freeze({
    schema_version: SOURCE_COORDINATE_REPRESENTATION_SCHEMA,
    family,
    format,
    rawText: String(recognitionResult?.rawText || ""),
    rows: sourceLines(displayText).filter(line => line.trim()),
    groups,
    pointLabels: enginePointLabels(coordinateEngineV2),
    axisOrder: sourceAxisOrder(coordinateEngineV2, family, format),
    hemisphere: sourceHemispheres(displayText),
    precision: format,
    sourceCrsEvidence: sourceCrsEvidence(recognitionResult, coordinateEngineV2),
    displayText,
    editable: Boolean(displayText)
  });
}
