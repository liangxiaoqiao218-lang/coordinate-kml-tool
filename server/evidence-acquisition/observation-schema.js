import crypto from "node:crypto";

export const IMAGE_OBSERVATION_SCHEMA_VERSION = "image_observation_v1";
export const ORIGINAL_IMAGE_PIXEL_SPACE = "ORIGINAL_IMAGE_PIXELS";

function optionalText(value) {
  const text = String(value ?? "").trim();
  return text || null;
}

function normalizePage(value) {
  const page = Number.parseInt(value, 10);
  return Number.isInteger(page) && page > 0 ? page : 1;
}

export function normalizeObservationBbox(value, image = {}) {
  if (!Array.isArray(value) || value.length !== 4) return null;
  const bbox = value.map(Number);
  if (!bbox.every(Number.isFinite)) return null;
  const [x1, y1, x2, y2] = bbox;
  if (x1 < 0 || y1 < 0 || x2 < x1 || y2 < y1) return null;
  const width = Number(image.width);
  const height = Number(image.height);
  if (Number.isFinite(width) && width > 0 && x2 > width) return null;
  if (Number.isFinite(height) && height > 0 && y2 > height) return null;
  return bbox;
}

export function normalizeObservationPolygon(value, image = {}) {
  if (!Array.isArray(value) || value.length !== 8) return null;
  const polygon = value.map(Number);
  if (!polygon.every(Number.isFinite)) return null;
  const width = Number(image.width);
  const height = Number(image.height);
  for (let index = 0; index < polygon.length; index += 2) {
    const x = polygon[index];
    const y = polygon[index + 1];
    if (x < 0 || y < 0) return null;
    if (Number.isFinite(width) && width > 0 && x > width) return null;
    if (Number.isFinite(height) && height > 0 && y > height) return null;
  }
  return polygon;
}

export function polygonToObservationBbox(polygon, image = {}) {
  const normalized = normalizeObservationPolygon(polygon, image);
  if (!normalized) return null;
  const xs = normalized.filter((_, index) => index % 2 === 0);
  const ys = normalized.filter((_, index) => index % 2 === 1);
  return normalizeObservationBbox([
    Math.min(...xs),
    Math.min(...ys),
    Math.max(...xs),
    Math.max(...ys)
  ], image);
}

function buildObservationId(value = {}) {
  const signature = [
    value.image_id,
    value.page,
    value.source,
    value.source_ref,
    value.group_id,
    value.point_id,
    value.text
  ].map(item => String(item ?? "")).join("|");
  return `obs_${crypto.createHash("sha256").update(signature).digest("hex").slice(0, 16)}`;
}

export function createImageTextObservation(value = {}) {
  const image = value.image || {};
  const coordinateSpace = optionalText(value.coordinate_space);
  const acceptsPixelLocation = coordinateSpace === ORIGINAL_IMAGE_PIXEL_SPACE;
  const polygon = acceptsPixelLocation ? normalizeObservationPolygon(value.polygon, image) : null;
  const bbox = acceptsPixelLocation
    ? (normalizeObservationBbox(value.bbox, image) || polygonToObservationBbox(polygon, image))
    : null;
  const observation = {
    schema_version: IMAGE_OBSERVATION_SCHEMA_VERSION,
    observation_id: optionalText(value.observation_id),
    image_id: optionalText(value.image_id),
    page: normalizePage(value.page),
    text: String(value.text ?? ""),
    bbox,
    polygon,
    coordinate_space: bbox ? ORIGINAL_IMAGE_PIXEL_SPACE : null,
    source: optionalText(value.source),
    source_ref: optionalText(value.source_ref),
    group_id: optionalText(value.group_id),
    point_id: optionalText(value.point_id),
    location_status: bbox ? "PIXEL_BBOX" : "LOGICAL_ROW_ONLY"
  };
  observation.observation_id = observation.observation_id || buildObservationId(observation);
  return observation;
}
