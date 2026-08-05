import crypto from "node:crypto";

export const EVIDENCE_SCHEMA_VERSION = "coordinate_evidence_v1";

function optionalText(value) {
  const text = String(value ?? "").trim();
  return text || null;
}

function normalizePage(value) {
  const page = Number.parseInt(value, 10);
  return Number.isInteger(page) && page > 0 ? page : 1;
}

export function normalizeEvidenceBbox(value) {
  if (!Array.isArray(value) || value.length !== 4) return null;
  const bbox = value.map(Number);
  if (!bbox.every(Number.isFinite)) return null;
  const [x1, y1, x2, y2] = bbox;
  if (x2 < x1 || y2 < y1) return null;
  return bbox;
}

function buildEvidenceId(value = {}) {
  const signature = [
    value.image_id,
    value.page,
    value.region_id,
    value.group_id,
    value.row_id,
    value.point_id,
    value.field,
    value.source,
    value.raw_text
  ].map(item => String(item ?? "")).join("|");
  return `ev_${crypto.createHash("sha256").update(signature).digest("hex").slice(0, 16)}`;
}

export function createImageEvidence(value = {}) {
  const bbox = normalizeEvidenceBbox(value.bbox);
  const evidence = {
    evidence_id: optionalText(value.evidence_id),
    image_id: optionalText(value.image_id),
    page: normalizePage(value.page),
    region_id: optionalText(value.region_id),
    group_id: optionalText(value.group_id),
    row_id: optionalText(value.row_id),
    point_id: optionalText(value.point_id),
    field: optionalText(value.field),
    bbox,
    source: optionalText(value.source),
    raw_text: String(value.raw_text ?? ""),
    location_status: bbox ? "PIXEL_BBOX" : "LOGICAL_ROW_ONLY"
  };
  evidence.evidence_id = evidence.evidence_id || buildEvidenceId(evidence);
  return evidence;
}

export function isImageEvidence(value = {}) {
  return Boolean(
    value
    && typeof value === "object"
    && typeof value.evidence_id === "string"
    && typeof value.source === "string"
    && typeof value.raw_text === "string"
    && (value.bbox === null || normalizeEvidenceBbox(value.bbox))
  );
}
