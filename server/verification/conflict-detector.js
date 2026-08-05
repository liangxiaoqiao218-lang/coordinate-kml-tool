import { parseDmsRows } from "./dms-utils.js";

export const CONFLICT_DETECTOR_SUPPORTED_SCOPE = Object.freeze([
  "handwritten_dms",
  "standard_dms"
]);

function addTextSource(sources, id, text) {
  const value = String(text || "").trim();
  if (!value || sources.some(source => source.text === value)) {
    return;
  }
  sources.push({ id, text: value });
}

function collectRecognitionSources(recognitionResult = {}) {
  const sources = [];
  const routing = recognitionResult.handwrittenVisionRouting || {};

  addTextSource(sources, "vision_general", routing.generalVisionRawText);
  addTextSource(sources, "vision_verification", routing.handwrittenVisionRawText);
  addTextSource(sources, "vision_final", routing.finalRawText);
  addTextSource(sources, "raw_text", recognitionResult.rawText);
  addTextSource(sources, "parser_result", recognitionResult.coordinates);

  const candidateCollections = [
    recognitionResult.candidates,
    recognitionResult.coordinateCandidates,
    recognitionResult.verificationCandidates
  ];
  candidateCollections.forEach((collection, collectionIndex) => {
    (Array.isArray(collection) ? collection : []).forEach((candidate, candidateIndex) => {
      addTextSource(
        sources,
        `candidate_${collectionIndex + 1}_${candidateIndex + 1}`,
        typeof candidate === "string" ? candidate : candidate?.text || candidate?.value || candidate?.coordinates
      );
    });
  });

  return sources;
}

function getComponentSeverity(component) {
  if (component === "direction" || component === "degrees") return "high";
  return "medium";
}

export function detectCoordinateConflicts({ recognitionResult = {} } = {}) {
  const sources = collectRecognitionSources(recognitionResult)
    .map(source => ({ ...source, rows: parseDmsRows(source.text) }))
    .filter(source => source.rows.length > 0);

  if (sources.length < 2) {
    return [];
  }

  const observations = new Map();
  sources.forEach(source => {
    source.rows.forEach((row, rowIndex) => {
      const pointId = row.label || String(rowIndex + 1);
      ["latitude", "longitude"].forEach(field => {
        const token = row.fields[field];
        if (!token) return;
        ["degrees", "minutes", "seconds", "direction"].forEach(component => {
          const key = `${rowIndex}|${pointId}|${field}|${component}`;
          if (!observations.has(key)) observations.set(key, []);
          observations.get(key).push({
            source: source.id,
            value: String(token[component]),
            raw: token.raw
          });
        });
      });
    });
  });

  const conflicts = [];
  observations.forEach((items, key) => {
    const candidates = Array.from(new Set(items.map(item => item.value)));
    if (candidates.length < 2) return;

    const [pointIndex, pointId, coordinateField, component] = key.split("|");
    conflicts.push({
      point_index: Number(pointIndex),
      point_id: pointId,
      field: `${coordinateField}.${component}`,
      candidates,
      severity: getComponentSeverity(component),
      sources: items,
      auto_correct: false
    });
  });

  return conflicts;
}
