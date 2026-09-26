import { EVIDENCE_KIND } from './constants.js';

const EVIDENCE_KINDS = new Set(Object.values(EVIDENCE_KIND));

function freezeBox(value) {
  if (!Array.isArray(value) || value.length !== 4) return null;
  const box = value.map(Number);
  if (box.some(number => !Number.isFinite(number) || number < 0 || number > 1)) {
    throw new Error('Evidence bbox values must be normalized numbers between 0 and 1');
  }
  if (box[0] > box[2] || box[1] > box[3]) throw new Error('Evidence bbox is invalid');
  return Object.freeze(box);
}

export class CoordinateEvidenceBoard {
  #regions = new Map();
  #uncertainties = [];
  #reviewItems = [];
  #toolResults = [];

  addRegion(region) {
    const id = String(region?.id || '').trim();
    if (!id) throw new Error('Evidence region id is required');
    if (this.#regions.has(id)) throw new Error(`Duplicate evidence region id: ${id}`);
    const kind = String(region?.kind || EVIDENCE_KIND.OTHER);
    if (!EVIDENCE_KINDS.has(kind)) throw new Error(`Unsupported evidence kind: ${kind}`);
    const confidence = Number(region?.confidence ?? 0);
    if (!Number.isFinite(confidence) || confidence < 0 || confidence > 1) {
      throw new Error('Evidence confidence must be between 0 and 1');
    }
    const normalized = Object.freeze({
      id,
      kind,
      bbox: freezeBox(region?.bbox),
      sourceText: String(region?.sourceText || ''),
      semanticRole: String(region?.semanticRole || ''),
      confidence,
      provenance: String(region?.provenance || 'agent_observation'),
    });
    this.#regions.set(id, normalized);
    return normalized;
  }

  addUncertainty(item) {
    const normalized = Object.freeze({
      code: String(item?.code || 'UNSPECIFIED_UNCERTAINTY'),
      message: String(item?.message || 'Evidence remains uncertain'),
      evidenceRegionIds: Object.freeze((item?.evidenceRegionIds || []).map(String)),
      blocking: item?.blocking !== false,
    });
    this.#uncertainties.push(normalized);
    return normalized;
  }

  addReviewItem(item) {
    const normalized = Object.freeze({
      fieldPath: String(item?.fieldPath || 'document'),
      question: String(item?.question || 'Please verify the source image'),
      evidenceRegionIds: Object.freeze((item?.evidenceRegionIds || []).map(String)),
      candidates: Object.freeze((item?.candidates || []).map(String)),
    });
    this.#reviewItems.push(normalized);
    return normalized;
  }

  addToolResult(result) {
    const normalized = Object.freeze({
      actionId: String(result?.actionId || ''),
      toolName: String(result?.toolName || ''),
      ok: result?.ok === true,
      output: result?.output ?? null,
      error: result?.error ? String(result.error) : null,
    });
    this.#toolResults.push(normalized);
    return normalized;
  }

  hasBlockingUncertainty() {
    return this.#uncertainties.some(item => item.blocking);
  }

  snapshot() {
    return Object.freeze({
      regions: Object.freeze([...this.#regions.values()]),
      uncertainties: Object.freeze([...this.#uncertainties]),
      reviewItems: Object.freeze([...this.#reviewItems]),
      toolResults: Object.freeze([...this.#toolResults]),
    });
  }
}
