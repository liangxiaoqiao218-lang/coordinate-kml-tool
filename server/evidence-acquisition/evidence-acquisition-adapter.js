import {
  ORIGINAL_IMAGE_PIXEL_SPACE,
  createImageTextObservation,
  polygonToObservationBbox
} from "./observation-schema.js";

const SOURCE_ALIASES = Object.freeze({
  vision_general: "generalVision",
  vision_verification: "handwrittenVision",
  vision_final: "finalVision",
  qwen_ocr: "qwenOcr",
  raw_text: "rawText"
});

function getImageMeta(recognitionResult = {}) {
  const image = recognitionResult.imageMetadata || recognitionResult.image || {};
  return {
    image_id: recognitionResult.image_id || recognitionResult.imageId || image.image_id || image.id || null,
    width: Number(image.width) || null,
    height: Number(image.height) || null,
    page: Number.parseInt(image.page || recognitionResult.page, 10) || 1
  };
}

function extractPointId(text = "") {
  return String(text).match(/^\s*(?:point\s*)?([A-Z]|\d{1,3})\s*(?:[.):-]|\||\s)/i)?.[1] || null;
}

function normalizeSource(value, fallback = "unknown") {
  const source = String(value || fallback).trim();
  return SOURCE_ALIASES[source] || source;
}

function getRawObservationCollections(recognitionResult = {}) {
  const ocrResult = recognitionResult.ocrResult || recognitionResult.ocr_result || {};
  return [
    { source: "qwenOcr", values: recognitionResult.ocrLineLocations },
    { source: "qwenOcr", values: recognitionResult.ocrObservations },
    { source: "qwenOcr", values: ocrResult.words_info },
    { source: "visionObservation", values: recognitionResult.visionObservations },
    { source: "imageObservation", values: recognitionResult.imageObservations }
  ];
}

function normalizeRawObservation(value = {}, index, fallbackSource, imageMeta) {
  const polygon = value.polygon || value.location || null;
  const bbox = value.bbox || value.bbox_2d || polygonToObservationBbox(polygon, imageMeta);
  const source = normalizeSource(value.source, fallbackSource);
  const text = String(value.text ?? value.raw_text ?? value.value ?? "");
  const trustedAbsolutePixelSource = source === "qwenOcr" || value.trusted_pixel_bbox === true;
  return createImageTextObservation({
    observation_id: value.observation_id,
    image_id: value.image_id || imageMeta.image_id,
    page: value.page || imageMeta.page,
    text,
    bbox,
    polygon,
    coordinate_space: value.coordinate_space || (trustedAbsolutePixelSource ? ORIGINAL_IMAGE_PIXEL_SPACE : null),
    source,
    source_ref: value.source_ref || value.id || `${source}_${index + 1}`,
    group_id: value.group_id,
    point_id: value.point_id || extractPointId(text),
    image: imageMeta
  });
}

function collectLegacyImageEvidence(recognitionResult = {}, imageMeta = {}) {
  const imageEvidence = recognitionResult.imageEvidence || {};
  const observations = [];
  Object.entries(imageEvidence.sources || {}).forEach(([source, container]) => {
    (Array.isArray(container?.rows) ? container.rows : []).forEach((row, rowIndex) => {
      observations.push(normalizeRawObservation({
        ...row,
        text: row.text || row.raw_text || "",
        source,
        source_ref: row.source_ref || `${source}_row_${rowIndex + 1}`,
        point_id: row.point_id || String(rowIndex + 1),
        page: row.page || imageEvidence.page,
        image_id: row.image_id || imageEvidence.image_id,
        coordinate_space: ORIGINAL_IMAGE_PIXEL_SPACE
      }, rowIndex, source, imageMeta));
    });
  });
  return observations;
}

function buildLogicalRawTextObservations(recognitionResult = {}, imageMeta = {}) {
  return String(recognitionResult.rawText || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .map((text, index) => createImageTextObservation({
      image_id: imageMeta.image_id,
      page: imageMeta.page,
      text,
      coordinate_space: null,
      source: "rawText",
      source_ref: `raw_text_${index + 1}`,
      point_id: extractPointId(text),
      image: imageMeta
    }));
}

export function buildImageTextObservations({ recognitionResult = {} } = {}) {
  const imageMeta = getImageMeta(recognitionResult);
  const observations = [];
  getRawObservationCollections(recognitionResult).forEach(collection => {
    (Array.isArray(collection.values) ? collection.values : []).forEach((value, index) => {
      observations.push(normalizeRawObservation(value, index, collection.source, imageMeta));
    });
  });
  observations.push(...collectLegacyImageEvidence(recognitionResult, imageMeta));

  const hasPixelObservations = observations.some(observation => observation.location_status === "PIXEL_BBOX");
  if (!hasPixelObservations) {
    observations.push(...buildLogicalRawTextObservations(recognitionResult, imageMeta));
  }

  const seen = new Set();
  return observations.filter(observation => {
    if (seen.has(observation.observation_id)) return false;
    seen.add(observation.observation_id);
    return true;
  });
}
