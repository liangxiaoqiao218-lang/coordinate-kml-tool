import { parseDmsRows } from "../verification/dms-utils.js";
import { EVIDENCE_SCHEMA_VERSION, createImageEvidence } from "./evidence-schema.js";

const SOURCE_TYPES = Object.freeze({
  vision_general: "generalVision",
  vision_verification: "handwrittenVision",
  vision_final: "finalVision",
  raw_text: "rawText",
  parser_result: "parserResult",
  coordinate_engine_v2: "coordinateEngineV2"
});

const PRIMARY_SOURCE_PRIORITY = Object.freeze([
  "handwrittenVision",
  "generalVision",
  "finalVision",
  "parserResult",
  "rawText",
  "coordinateEngineV2"
]);

function getImageId(recognitionResult = {}, context = {}) {
  return context.image_id
    || context.imageId
    || recognitionResult.image_id
    || recognitionResult.imageId
    || null;
}

function collectSourceTexts(recognitionResult = {}, coordinateEngineV2 = {}) {
  const routing = recognitionResult.handwrittenVisionRouting || {};
  const sources = [
    { id: "vision_general", text: routing.generalVisionRawText },
    { id: "vision_verification", text: routing.handwrittenVisionRawText },
    { id: "vision_final", text: routing.finalRawText },
    { id: "parser_result", text: recognitionResult.coordinates },
    { id: "raw_text", text: recognitionResult.rawText }
  ];

  const engineText = (Array.isArray(coordinateEngineV2.groups) ? coordinateEngineV2.groups : [])
    .flatMap(group => Array.isArray(group.points) ? group.points : [])
    .map(point => String(point?.raw || "").trim())
    .filter(Boolean)
    .join("\n");
  sources.push({ id: "coordinate_engine_v2", text: engineText });

  const seenTexts = new Set();
  return sources
    .map(source => ({
      ...source,
      type: SOURCE_TYPES[source.id],
      text: String(source.text || "").trim()
    }))
    .filter(source => {
      if (!source.text) return false;
      const isVisionSource = source.id.startsWith("vision_");
      const isRequiredEngineFallback = source.id === "coordinate_engine_v2"
        && parseDmsRows(source.text).length === 0;
      if (!isVisionSource && !isRequiredEngineFallback && seenTexts.has(source.text)) return false;
      seenTexts.add(source.text);
      return true;
    });
}

function flattenEnginePoints(coordinateEngineV2 = {}) {
  const points = [];
  (Array.isArray(coordinateEngineV2.groups) ? coordinateEngineV2.groups : []).forEach((group, groupIndex) => {
    (Array.isArray(group.points) ? group.points : []).forEach((point, pointIndex) => {
      points.push({
        group_id: String(group.group_id || `group_${groupIndex + 1}`),
        point_id: String(point?.label || pointIndex + 1),
        point_index: points.length
      });
    });
  });
  return points;
}

function getProvidedBbox(recognitionResult = {}, context = {}, sourceType, rowIndex, field, pointMeta = {}) {
  const sourceRows = recognitionResult.imageEvidence?.sources?.[sourceType]?.rows;
  const row = Array.isArray(sourceRows) ? sourceRows[rowIndex] : null;
  const legacyBbox = row?.fields?.[field]?.bbox || row?.bbox || null;
  if (legacyBbox) return legacyBbox;
  if (field !== "coordinate") return null;
  const bindings = Array.isArray(context.evidenceAcquisition?.rowBindings)
    ? context.evidenceAcquisition.rowBindings
    : [];
  const binding = bindings.find(item => (
    String(item.group_id) === String(pointMeta.group_id)
    && String(item.point_id) === String(pointMeta.point_id)
    && item.location_status === "PIXEL_BBOX"
  ));
  return binding?.bbox || null;
}

function parseEvidenceRows(source = {}) {
  const dmsRows = parseDmsRows(source.text);
  if (dmsRows.length > 0) return dmsRows;
  if (source.id !== "coordinate_engine_v2") return [];
  return String(source.text || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .map((line, index) => ({
      label: String(index + 1),
      line,
      fields: {},
      tokens: []
    }));
}

function createRowEvidence({ recognitionResult, context, imageId, source, row, rowIndex, pointMeta, field, rawText }) {
  return createImageEvidence({
    image_id: imageId,
    page: recognitionResult.imageEvidence?.page || 1,
    region_id: recognitionResult.imageEvidence?.region_id || "coordinate_region_1",
    group_id: pointMeta?.group_id || "group_1",
    row_id: `row_${rowIndex + 1}`,
    point_id: pointMeta?.point_id || row.label || String(rowIndex + 1),
    field,
    bbox: getProvidedBbox(recognitionResult, context, source.type, rowIndex, field, pointMeta),
    source: source.type,
    raw_text: rawText
  });
}

function buildEvidenceItems(recognitionResult = {}, coordinateEngineV2 = {}, context = {}) {
  const imageId = getImageId(recognitionResult, context);
  const pointMeta = flattenEnginePoints(coordinateEngineV2);
  const items = [];

  collectSourceTexts(recognitionResult, coordinateEngineV2).forEach(source => {
    parseEvidenceRows(source).forEach((row, rowIndex) => {
      const meta = pointMeta[rowIndex];
      items.push(createRowEvidence({
        recognitionResult,
        context,
        imageId,
        source,
        row,
        rowIndex,
        pointMeta: meta,
        field: "coordinate",
        rawText: row.line
      }));
      ["latitude", "longitude"].forEach(field => {
        const token = row.fields[field];
        if (!token) return;
        items.push(createRowEvidence({
          recognitionResult,
          context,
          imageId,
          source,
          row,
          rowIndex,
          pointMeta: meta,
          field,
          rawText: token.raw
        }));
      });
    });
  });

  return items;
}

function sortBySourcePriority(items) {
  return [...items].sort((left, right) => (
    PRIMARY_SOURCE_PRIORITY.indexOf(left.source) - PRIMARY_SOURCE_PRIORITY.indexOf(right.source)
  ));
}

function buildPointEvidenceGroups(coordinateEngineV2 = {}, items = []) {
  return (Array.isArray(coordinateEngineV2.groups) ? coordinateEngineV2.groups : []).map((group, groupIndex) => ({
    group_id: String(group.group_id || `group_${groupIndex + 1}`),
    points: (Array.isArray(group.points) ? group.points : []).map((point, pointIndex) => {
      const pointId = String(point?.label || pointIndex + 1);
      const matches = sortBySourcePriority(items.filter(item => (
        item.group_id === String(group.group_id || `group_${groupIndex + 1}`)
        && item.point_id === pointId
      )));
      const rowEvidence = matches.filter(item => item.field === "coordinate");
      const fieldEvidence = field => matches.filter(item => item.field === field);
      return {
        point_id: pointId,
        evidence: rowEvidence[0] || null,
        evidence_ids: rowEvidence.map(item => item.evidence_id),
        fields: {
          latitude: fieldEvidence("latitude").map(item => item.evidence_id),
          longitude: fieldEvidence("longitude").map(item => item.evidence_id)
        }
      };
    })
  }));
}

export function buildRecognitionEvidence({ recognitionResult = {}, coordinateEngineV2 = {}, context = {} } = {}) {
  const items = buildEvidenceItems(recognitionResult, coordinateEngineV2, context);
  const hasPixelBbox = items.some(item => Array.isArray(item.bbox));
  return {
    schema_version: EVIDENCE_SCHEMA_VERSION,
    image_id: getImageId(recognitionResult, context),
    location_scope: hasPixelBbox ? "pixel_bbox_and_logical_row" : "logical_row_only",
    pixel_bbox_available: hasPixelBbox,
    items,
    groups: buildPointEvidenceGroups(coordinateEngineV2, items),
    shadow_only: true,
    affects_coordinates: false,
    affects_kml: false
  };
}

export function findEvidenceForObservation(evidenceLayer = {}, { source, rowIndex, field } = {}) {
  const sourceType = SOURCE_TYPES[source] || source;
  const targetRowId = `row_${Number(rowIndex) + 1}`;
  const items = Array.isArray(evidenceLayer.items) ? evidenceLayer.items : [];
  const fieldEvidence = items.find(item => (
    item.source === sourceType
    && item.row_id === targetRowId
    && item.field === field
  ));
  const rowEvidence = items.find(item => (
    item.source === sourceType
    && item.row_id === targetRowId
    && item.field === "coordinate"
  ));
  if (fieldEvidence?.bbox) return fieldEvidence;
  if (rowEvidence?.bbox) return rowEvidence;
  return fieldEvidence || rowEvidence || null;
}

export function attachEvidenceToVerificationGroups(groups = [], evidenceLayer = {}) {
  const evidenceGroups = new Map((Array.isArray(evidenceLayer.groups) ? evidenceLayer.groups : [])
    .map(group => [String(group.group_id), group]));
  return (Array.isArray(groups) ? groups : []).map((group, groupIndex) => {
    const evidenceGroup = evidenceGroups.get(String(group.group_id || `group_${groupIndex + 1}`));
    const evidencePoints = new Map((Array.isArray(evidenceGroup?.points) ? evidenceGroup.points : [])
      .map(point => [String(point.point_id), point]));
    return {
      ...group,
      points: (Array.isArray(group.points) ? group.points : []).map(point => {
        const pointEvidence = evidencePoints.get(String(point.point_id));
        const fields = Object.fromEntries(Object.entries(point.fields || {}).map(([field, value]) => [field, {
          ...value,
          evidence_ids: pointEvidence?.fields?.[field] || []
        }]));
        return {
          ...point,
          fields,
          evidence: pointEvidence?.evidence || null,
          evidence_ids: pointEvidence?.evidence_ids || []
        };
      })
    };
  });
}
