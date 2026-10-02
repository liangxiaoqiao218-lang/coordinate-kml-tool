export const CONTEXT_EVIDENCE_IR_VERSION = 'context-bound-coordinate-evidence/v1';

export const EVIDENCE_ASSERTION_STATE = Object.freeze({
  EXPLICIT: 'explicit',
  INFERRED: 'inferred',
  HUMAN_CONFIRMED: 'human_confirmed',
  UNKNOWN: 'unknown',
});

const ASSERTION_STATES = new Set(Object.values(EVIDENCE_ASSERTION_STATE));
const SOURCE_KINDS = new Set(['document', 'page', 'region', 'table', 'row', 'field']);
const GEOMETRY_TYPES = new Set(['Point', 'MultiPoint', 'LineString', 'Polygon', 'MultiPolygon', 'Grid', 'Unknown']);

function text(value, path, { nullable = false } = {}) {
  if (value === null && nullable) return null;
  const normalized = String(value ?? '').trim();
  if (!normalized && !nullable) throw new Error(`${path} is required`);
  return normalized || null;
}

function finiteOrNull(value, path) {
  if (value === null || value === undefined || value === '') return null;
  const normalized = Number(value);
  if (!Number.isFinite(normalized)) throw new Error(`${path} must be finite or null`);
  return normalized;
}

function deepFreeze(value) {
  if (!value || typeof value !== 'object' || Object.isFrozen(value)) return value;
  Object.freeze(value);
  for (const child of Object.values(value)) deepFreeze(child);
  return value;
}

function normalizeSource(source, index) {
  const kind = text(source?.kind, `sources[${index}].kind`);
  if (!SOURCE_KINDS.has(kind)) throw new Error(`sources[${index}].kind is unsupported`);
  const pageIndex = finiteOrNull(source?.pageIndex, `sources[${index}].pageIndex`);
  if (pageIndex !== null && (!Number.isInteger(pageIndex) || pageIndex < 0)) {
    throw new Error(`sources[${index}].pageIndex must be a non-negative integer`);
  }
  const bbox = source?.bbox ?? null;
  if (bbox !== null) {
    if (!Array.isArray(bbox) || bbox.length !== 4 || bbox.some(value => !Number.isFinite(Number(value)))) {
      throw new Error(`sources[${index}].bbox must contain four finite values`);
    }
    if (bbox.some(value => Number(value) < 0 || Number(value) > 1)
      || Number(bbox[0]) > Number(bbox[2]) || Number(bbox[1]) > Number(bbox[3])) {
      throw new Error(`sources[${index}].bbox is invalid`);
    }
  }
  return {
    id: text(source?.id, `sources[${index}].id`),
    kind,
    parentId: text(source?.parentId, `sources[${index}].parentId`, { nullable: true }),
    pageIndex,
    bbox: bbox === null ? null : bbox.map(Number),
    semanticRole: text(source?.semanticRole, `sources[${index}].semanticRole`, { nullable: true }),
    sourceText: String(source?.sourceText ?? ''),
  };
}

function normalizeAssertion(assertion, index, sourceIds) {
  const state = text(assertion?.state, `assertions[${index}].state`);
  if (!ASSERTION_STATES.has(state)) throw new Error(`assertions[${index}].state is unsupported`);
  const evidenceSourceIds = (assertion?.evidenceSourceIds || []).map(String);
  for (const sourceId of evidenceSourceIds) {
    if (!sourceIds.has(sourceId)) throw new Error(`assertions[${index}] references unknown source ${sourceId}`);
  }
  if (state !== EVIDENCE_ASSERTION_STATE.UNKNOWN && evidenceSourceIds.length === 0) {
    throw new Error(`assertions[${index}] requires source evidence`);
  }
  if (state === EVIDENCE_ASSERTION_STATE.INFERRED && !String(assertion?.rationale || '').trim()) {
    throw new Error(`assertions[${index}] requires an inference rationale`);
  }
  return {
    id: text(assertion?.id, `assertions[${index}].id`),
    subject: text(assertion?.subject, `assertions[${index}].subject`),
    value: assertion?.value ?? null,
    state,
    evidenceSourceIds,
    rationale: text(assertion?.rationale, `assertions[${index}].rationale`, { nullable: true }),
  };
}

function normalizeField(field, rowIndex, fieldIndex, sourceIds) {
  const sourceId = text(field?.sourceId, `rows[${rowIndex}].fields[${fieldIndex}].sourceId`);
  if (!sourceIds.has(sourceId)) throw new Error(`rows[${rowIndex}].fields[${fieldIndex}] references unknown source ${sourceId}`);
  const raw = String(field?.raw ?? '');
  if (!raw) throw new Error(`rows[${rowIndex}].fields[${fieldIndex}].raw is required`);
  return {
    name: text(field?.name, `rows[${rowIndex}].fields[${fieldIndex}].name`),
    representation: text(field?.representation, `rows[${rowIndex}].fields[${fieldIndex}].representation`),
    raw,
    value: finiteOrNull(field?.value, `rows[${rowIndex}].fields[${fieldIndex}].value`),
    sourceId,
  };
}

function normalizeRow(row, index, sourceIds, groupIds) {
  const sourceId = text(row?.sourceId, `rows[${index}].sourceId`);
  const groupId = text(row?.groupId, `rows[${index}].groupId`);
  if (!sourceIds.has(sourceId)) throw new Error(`rows[${index}] references unknown source ${sourceId}`);
  if (!groupIds.has(groupId)) throw new Error(`rows[${index}] references unknown group ${groupId}`);
  const order = Number(row?.order);
  if (!Number.isInteger(order) || order < 0) throw new Error(`rows[${index}].order must be a non-negative integer`);
  if (!Array.isArray(row?.fields) || row.fields.length === 0) throw new Error(`rows[${index}].fields is required`);
  const fields = row.fields.map((field, fieldIndex) => normalizeField(field, index, fieldIndex, sourceIds));
  if (new Set(fields.map(field => `${field.representation}:${field.name}`)).size !== fields.length) {
    throw new Error(`rows[${index}] contains duplicate semantic fields`);
  }
  return {
    id: text(row?.id, `rows[${index}].id`),
    groupId,
    order,
    label: text(row?.label, `rows[${index}].label`, { nullable: true }),
    sourceId,
    sourceText: text(row?.sourceText, `rows[${index}].sourceText`),
    fields,
    needsReview: row?.needsReview === true,
  };
}

export function normalizeContextBoundCoordinateEvidence(input) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) throw new Error('context evidence input must be an object');
  const sha256 = text(input?.document?.sha256, 'document.sha256').toLowerCase();
  if (!/^[a-f0-9]{64}$/.test(sha256)) throw new Error('document.sha256 must be a lowercase SHA-256');
  const sources = (input.sources || []).map(normalizeSource);
  if (sources.length === 0) throw new Error('sources must not be empty');
  const sourceIds = new Set(sources.map(source => source.id));
  if (sourceIds.size !== sources.length) throw new Error('source ids must be unique');
  for (const source of sources) {
    if (source.parentId && !sourceIds.has(source.parentId)) throw new Error(`source ${source.id} references unknown parent ${source.parentId}`);
  }

  const groups = (input.groups || []).map((group, index) => ({
    id: text(group?.id, `groups[${index}].id`),
    name: text(group?.name, `groups[${index}].name`, { nullable: true }),
  }));
  const groupIds = new Set(groups.map(group => group.id));
  if (groups.length === 0 || groupIds.size !== groups.length) throw new Error('groups must contain unique ids');

  const rows = (input.rows || []).map((row, index) => normalizeRow(row, index, sourceIds, groupIds));
  if (new Set(rows.map(row => row.id)).size !== rows.length) throw new Error('row ids must be unique');
  for (const group of groups) {
    const orders = rows.filter(row => row.groupId === group.id).map(row => row.order);
    if (orders.length === 0) throw new Error(`group ${group.id} has no rows`);
    if (new Set(orders).size !== orders.length) throw new Error(`group ${group.id} contains duplicate row order`);
  }

  const assertions = (input.assertions || []).map((assertion, index) => normalizeAssertion(assertion, index, sourceIds));
  if (new Set(assertions.map(assertion => assertion.id)).size !== assertions.length) throw new Error('assertion ids must be unique');
  const assertionIds = new Set(assertions.map(assertion => assertion.id));
  const requireAssertion = (id, path) => {
    const normalized = text(id, path);
    if (!assertionIds.has(normalized)) throw new Error(`${path} references unknown assertion ${normalized}`);
    return normalized;
  };

  const geometryType = text(input?.geometry?.type, 'geometry.type');
  if (!GEOMETRY_TYPES.has(geometryType)) throw new Error('geometry.type is unsupported');
  const normalized = {
    schemaVersion: CONTEXT_EVIDENCE_IR_VERSION,
    mode: 'SHADOW_EVIDENCE_ONLY',
    document: { sha256, pageCount: Number(input?.document?.pageCount || 1) },
    sources,
    assertions,
    groups,
    rows,
    coordinateSystem: {
      kind: text(input?.coordinateSystem?.kind, 'coordinateSystem.kind'),
      name: text(input?.coordinateSystem?.name, 'coordinateSystem.name', { nullable: true }),
      epsg: text(input?.coordinateSystem?.epsg, 'coordinateSystem.epsg', { nullable: true }),
      datum: text(input?.coordinateSystem?.datum, 'coordinateSystem.datum', { nullable: true }),
      zone: text(input?.coordinateSystem?.zone, 'coordinateSystem.zone', { nullable: true }),
      hemisphere: text(input?.coordinateSystem?.hemisphere, 'coordinateSystem.hemisphere', { nullable: true }),
      unit: text(input?.coordinateSystem?.unit, 'coordinateSystem.unit', { nullable: true }),
      axisOrder: text(input?.coordinateSystem?.axisOrder, 'coordinateSystem.axisOrder', { nullable: true }),
      assertionId: requireAssertion(input?.coordinateSystem?.assertionId, 'coordinateSystem.assertionId'),
    },
    geometry: {
      type: geometryType,
      assertionId: requireAssertion(input?.geometry?.assertionId, 'geometry.assertionId'),
    },
    review: {
      required: input?.review?.required === true,
      reasons: (input?.review?.reasons || []).map(String),
    },
    truth: {
      independentlyConfirmed: input?.truth?.independentlyConfirmed === true,
      authorityRefs: (input?.truth?.authorityRefs || []).map(String),
    },
  };
  if (!Number.isInteger(normalized.document.pageCount) || normalized.document.pageCount < 1) {
    throw new Error('document.pageCount must be a positive integer');
  }
  return deepFreeze(normalized);
}

export function assertionById(ir, id) {
  return ir.assertions.find(assertion => assertion.id === id) || null;
}
