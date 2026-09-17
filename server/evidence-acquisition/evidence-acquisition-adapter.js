import {
  ORIGINAL_IMAGE_PIXEL_SPACE,
  SERVER_PROVENANCE_ATTESTATION,
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
    request_asset_id: recognitionResult.request_asset_id || recognitionResult.requestAssetId || image.request_asset_id || null,
    image_sha256: recognitionResult.image_sha256 || image.image_sha256 || null,
    byte_length: Number(recognitionResult.image_byte_length || image.byte_length) || null,
    mime_type: recognitionResult.image_mime_type || image.mime_type || null,
    width: Number(image.width) || null,
    height: Number(image.height) || null,
    page: Number.parseInt(image.page || recognitionResult.page, 10) || null
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
    { source: "providerStructuredLayout", values: recognitionResult.providerLayoutCandidates },
    { source: "visionObservation", values: recognitionResult.visionObservations },
    { source: "imageObservation", values: recognitionResult.imageObservations }
  ];
}

function normalizeRawObservation(value = {}, index, fallbackSource, imageMeta) {
  const polygon = value.polygon || value.location || null;
  const bbox = value.bbox || value.bbox_2d || polygonToObservationBbox(polygon, imageMeta);
  const source = normalizeSource(value.source, fallbackSource);
  const text = String(value.text ?? value.raw_text ?? value.value ?? "");
  const trustedAbsolutePixelSource = ["qwenOcr", "providerStructuredLayout", "localOcrStructuredLayout"].includes(source);
  const observationInput = {
    observation_id: value.observation_id,
    image_id: value.image_id || imageMeta.image_id,
    image_sha256: value.image_sha256 || imageMeta.image_sha256,
    image_byte_length: value.image_byte_length || imageMeta.byte_length,
    image_mime_type: value.image_mime_type || imageMeta.mime_type,
    page: value.page || imageMeta.page,
    text,
    bbox,
    polygon,
    coordinate_space: value.coordinate_space || (trustedAbsolutePixelSource ? ORIGINAL_IMAGE_PIXEL_SPACE : null),
    source,
    source_type: value.source_type,
    source_ref: value.source_ref || value.id || `${source}_${index + 1}`,
    request_asset_id: value.request_asset_id || imageMeta.request_asset_id,
    source_line_id: value.source_line_id || value.id || `${source}_${index + 1}`,
    source_role: value.source_role,
    source_region_id: value.source_region_id,
    provenance_trust: value.provenance_trust,
    provenance_attestor: value.provenance_attestor,
    text_sha256: value.text_sha256,
    candidate_provenance_sha256: value.candidate_provenance_sha256,
    provider_response_id_sha256: value.provider_response_id_sha256,
    attestation_revision: value.attestation_revision,
    semantic_label: value.semantic_label,
    measurement_semantics: value.measurement_semantics,
    boundary_point: value.boundary_point,
    table_row: value.table_row,
    contradictory_evidence: value.contradictory_evidence,
    group_id: value.group_id,
    point_id: value.point_id || extractPointId(text),
    image: imageMeta
  };
  if (value[SERVER_PROVENANCE_ATTESTATION] === true) {
    Object.defineProperty(observationInput, SERVER_PROVENANCE_ATTESTATION, { value: true });
  }
  return createImageTextObservation(observationInput);
}

function attestProvenanceValue(value = {}) {
  const attested = { ...value };
  Object.defineProperty(attested, SERVER_PROVENANCE_ATTESTATION, { value: true, enumerable: true });
  return attested;
}

function discardUnattestedProvenance(value = {}) {
  return {
    ...value,
    source_role: null,
    source_region_id: null,
    provenance_trust: "UNTRUSTED",
    provenance_attestor: null
  };
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
      image_sha256: imageMeta.image_sha256,
      image_byte_length: imageMeta.byte_length,
      image_mime_type: imageMeta.mime_type,
      request_asset_id: imageMeta.request_asset_id,
      page: imageMeta.page,
      text,
      coordinate_space: null,
      source: "rawText",
      source_ref: `raw_text_${index + 1}`,
      source_line_id: `raw_text_${index + 1}`,
      point_id: extractPointId(text),
      image: imageMeta
    }));
}

export function buildImageTextObservations({ recognitionResult = {} } = {}) {
  const imageMeta = getImageMeta(recognitionResult);
  const provenanceAttestedByServer = recognitionResult[SERVER_PROVENANCE_ATTESTATION] === true;
  const observations = [];
  getRawObservationCollections(recognitionResult).forEach(collection => {
    (Array.isArray(collection.values) ? collection.values : []).forEach((value, index) => {
      observations.push(normalizeRawObservation(
        provenanceAttestedByServer ? attestProvenanceValue(value) : discardUnattestedProvenance(value),
        index,
        collection.source,
        imageMeta
      ));
    });
  });
  const legacyRecognitionResult = provenanceAttestedByServer ? {
    ...recognitionResult,
    imageEvidence: {
      ...(recognitionResult.imageEvidence || {}),
      sources: Object.fromEntries(Object.entries(recognitionResult.imageEvidence?.sources || {}).map(([source, container]) => [
        source,
        { ...container, rows: (container?.rows || []).map(attestProvenanceValue) }
      ]))
    }
  } : {
    ...recognitionResult,
    imageEvidence: {
      ...(recognitionResult.imageEvidence || {}),
      sources: Object.fromEntries(Object.entries(recognitionResult.imageEvidence?.sources || {}).map(([source, container]) => [
        source,
        { ...container, rows: (container?.rows || []).map(discardUnattestedProvenance) }
      ]))
    }
  };
  observations.push(...collectLegacyImageEvidence(legacyRecognitionResult, imageMeta));

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
