import {
  AGENTIC_COORDINATE_KIND,
  AGENTIC_CRS_STATUS,
  AGENTIC_RESULT_STATUS,
  normalizeAgenticCoordinateResult,
} from '../agentic-coordinate-recognition/contract.js';
import {
  EVIDENCE_ASSERTION_STATE,
  assertionById,
  normalizeContextBoundCoordinateEvidence,
} from './context-bound-coordinate-evidence.js';

export const SHADOW_CANONICAL_ADAPTER_VERSION = 'context-evidence-shadow-canonical-adapter/v1';

function fieldValue(row, representation, name) {
  return row.fields.find(field => field.representation === representation && field.name === name)?.value ?? null;
}

function pointFromRow(row) {
  return {
    label: row.label,
    sourceText: row.sourceText,
    x: fieldValue(row, 'projected', 'x'),
    y: fieldValue(row, 'projected', 'y'),
    latitude: fieldValue(row, 'geographic', 'latitude'),
    longitude: fieldValue(row, 'geographic', 'longitude'),
    needsReview: row.needsReview,
  };
}

function crsStatus(ir, assertion) {
  if (assertion?.state === EVIDENCE_ASSERTION_STATE.HUMAN_CONFIRMED
    || assertion?.state === EVIDENCE_ASSERTION_STATE.EXPLICIT) return AGENTIC_CRS_STATUS.IDENTIFIED;
  if (assertion?.state === EVIDENCE_ASSERTION_STATE.INFERRED) return AGENTIC_CRS_STATUS.NEEDS_CONFIRMATION;
  return AGENTIC_CRS_STATUS.UNKNOWN;
}

function coordinateKind(kind) {
  if (kind === AGENTIC_COORDINATE_KIND.GEOGRAPHIC || kind === AGENTIC_COORDINATE_KIND.PROJECTED) return kind;
  return AGENTIC_COORDINATE_KIND.UNKNOWN;
}

export function adaptContextEvidenceToShadowCanonical(input) {
  const ir = normalizeContextBoundCoordinateEvidence(input);
  const crsAssertion = assertionById(ir, ir.coordinateSystem.assertionId);
  const geometryAssertion = assertionById(ir, ir.geometry.assertionId);
  const inferred = [crsAssertion, geometryAssertion].some(assertion => assertion?.state === EVIDENCE_ASSERTION_STATE.INFERRED);
  const unknown = [crsAssertion, geometryAssertion].some(assertion => assertion?.state === EVIDENCE_ASSERTION_STATE.UNKNOWN);
  const rowReview = ir.rows.some(row => row.needsReview);
  const needsReview = ir.review.required || inferred || unknown || rowReview;
  const groups = ir.groups.map(group => ({
    name: group.name,
    points: ir.rows.filter(row => row.groupId === group.id)
      .sort((left, right) => left.order - right.order)
      .map(pointFromRow),
  }));
  const warnings = [
    ...ir.review.reasons,
    ...(inferred ? ['Context-derived assumptions require verification'] : []),
    ...(unknown ? ['Required context remains unknown'] : []),
  ];
  const candidate = normalizeAgenticCoordinateResult({
    success: true,
    resultStatus: needsReview ? AGENTIC_RESULT_STATUS.NEEDS_REVIEW : AGENTIC_RESULT_STATUS.USABLE,
    displayText: groups.flatMap(group => group.points.map(point => point.sourceText)).join('\n'),
    coordinateSystem: {
      kind: coordinateKind(ir.coordinateSystem.kind),
      name: ir.coordinateSystem.name,
      epsg: ir.coordinateSystem.epsg,
      status: crsStatus(ir, crsAssertion),
    },
    geometryType: ir.geometry.type,
    groups,
    warnings,
  });
  return Object.freeze({
    schemaVersion: SHADOW_CANONICAL_ADAPTER_VERSION,
    mode: 'SHADOW_ONLY',
    affectsProductionAuthority: false,
    documentSha256: ir.document.sha256,
    candidate,
    evidenceSummary: Object.freeze({
      sourceCount: ir.sources.length,
      rowCount: ir.rows.length,
      groupCount: ir.groups.length,
      independentlyConfirmed: ir.truth.independentlyConfirmed,
      assertionStates: Object.freeze(Object.fromEntries(ir.assertions.map(assertion => [assertion.subject, assertion.state]))),
    }),
    finalizerPreview: Object.freeze({
      existingFinalizerContract: 'finalized_coordinate_result_v1',
      shadowOnly: true,
      authorityGranted: false,
      mapAuthorityGranted: false,
      kmlAuthorityGranted: false,
      result: candidate,
    }),
  });
}

export function compareShadowCanonical(existingCandidate, shadow) {
  const normalizedExisting = normalizeAgenticCoordinateResult(existingCandidate);
  const differences = [];
  const compare = (field, left, right) => {
    if (JSON.stringify(left) !== JSON.stringify(right)) differences.push({ field, existing: left, shadow: right });
  };
  compare('coordinateSystem', normalizedExisting.coordinateSystem, shadow.candidate.coordinateSystem);
  compare('geometryType', normalizedExisting.geometryType, shadow.candidate.geometryType);
  compare('groups', normalizedExisting.groups, shadow.candidate.groups);
  return Object.freeze({
    status: differences.length ? 'EXPLAINED_OR_REVIEW_REQUIRED' : 'MATCHED',
    differences: Object.freeze(differences),
    authorityChanged: false,
  });
}
