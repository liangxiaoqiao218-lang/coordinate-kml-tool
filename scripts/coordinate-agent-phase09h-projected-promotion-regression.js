import assert from 'node:assert/strict';

import {
  CoordinateAgentToolRegistry,
  CoordinateIntelligenceAgentKernel,
  MockCoordinateAgentProviderAdapter,
  registerCoordinateMathTools,
} from '../server/coordinate-agent/index.js';
import { transformAndVerifyProjectedPoints } from '../server/projection/projected-crs-registry.js';

const projectedPairs = [
  { x: 600000, y: 0 },
  { x: 600100, y: 100 },
];

function projectedTurn({ geometryType = 'MultiPoint', uncertainties = [], reviewItems = [] } = {}) {
  return {
    observation: {
      summary: 'Whole image observed with explicit projected coordinate evidence.',
      orientationDegrees: 0,
      regions: [{
        id: 'whole', kind: 'document', bbox: [0, 0, 1, 1], sourceText: '',
        semanticRole: 'whole image', confidence: 1, provenance: 'offline_mock',
      }],
    },
    plan: { rationale: 'Use the deterministic safety layer.', actions: [] },
    candidate: {
      success: true,
      resultStatus: 'needs_review',
      displayText: 'source-faithful projected rows',
      coordinateSystem: { kind: 'projected', name: 'BFTM', epsg: null, status: 'identified' },
      geometryType,
      groups: [{
        name: 'Visible group',
        points: projectedPairs.map((pair, index) => ({
          label: `P${index + 1}`,
          sourceText: `P${index + 1} projected row`,
          x: pair.x,
          y: pair.y,
          latitude: null,
          longitude: null,
          needsReview: true,
        })),
      }],
      warnings: ['Deterministic projected conversion pending'],
    },
    uncertainties,
    reviewItems,
  };
}

async function runPromotionCase(options) {
  const registry = new CoordinateAgentToolRegistry();
  registerCoordinateMathTools(registry);
  return new CoordinateIntelligenceAgentKernel({
    providerAdapter: new MockCoordinateAgentProviderAdapter([projectedTurn(options)]),
    toolRegistry: registry,
    maxProviderCalls: 1,
    maxIterations: 1,
  }).run({ imageRef: 'offline-image', requestId: 'offline:phase09h' });
}

function promotionGate(result) {
  return result.evidence.toolResults.find(item => item.actionId === 'safety-projection-transform')
    ?.output?.promotionGate;
}

const eligible = await runPromotionCase({ geometryType: 'MultiPoint' });
assert.equal(eligible.terminalState, 'CONFIRMED');
assert.equal(promotionGate(eligible).eligible, true);
assert.equal(promotionGate(eligible).geometryStatus, 'identified');
assert.equal(promotionGate(eligible).blockingUncertaintyCount, 0);
assert.equal(promotionGate(eligible).reviewItemCount, 0);
assert.equal(eligible.coordinateResult.groups[0].points.every(point => (
  Number.isFinite(point.latitude) && Number.isFinite(point.longitude) && point.needsReview === false
)), true);
assert.equal(eligible.evidence.toolResults.filter(item => item.actionId.startsWith('safety-math-')).length, 2);

const unknownGeometry = await runPromotionCase({ geometryType: 'Unknown' });
assert.equal(unknownGeometry.terminalState, 'REVIEW_REQUIRED');
assert.equal(promotionGate(unknownGeometry).geometryStatus, 'unknown');
assert.equal(promotionGate(unknownGeometry).eligible, false);
assert.deepEqual(unknownGeometry.authorization, { mapAllowed: false, kmlAllowed: false });

const blockedEvidence = await runPromotionCase({
  uncertainties: [{
    code: 'SOURCE_EVIDENCE_UNRESOLVED',
    message: 'Source evidence remains unresolved',
    evidenceRegionIds: ['whole'],
    blocking: true,
  }],
});
assert.equal(blockedEvidence.terminalState, 'REVIEW_REQUIRED');
assert.equal(promotionGate(blockedEvidence).blockingUncertaintyCount, 1);
assert.equal(promotionGate(blockedEvidence).eligible, false);

const pendingReview = await runPromotionCase({
  reviewItems: [{
    fieldPath: 'geometryType',
    question: 'Confirm geometry authority',
    evidenceRegionIds: ['whole'],
    candidates: ['MultiPoint', 'Unknown'],
  }],
});
assert.equal(pendingReview.terminalState, 'REVIEW_REQUIRED');
assert.equal(promotionGate(pendingReview).reviewItemCount, 1);
assert.equal(promotionGate(pendingReview).eligible, false);

const partialTransform = transformAndVerifyProjectedPoints({
  coordinateSystem: { name: 'BFTM', epsg: null },
  axisOrder: 'easting_northing',
  points: [projectedPairs[0], { x: null, y: projectedPairs[1].y }],
});
assert.equal(partialTransform.valid, false);
assert.equal(partialTransform.transformedPointCount, 1);
assert.equal(partialTransform.forwardStatus, 'failed');

const roundTripFailure = transformAndVerifyProjectedPoints({
  coordinateSystem: { name: 'BFTM', epsg: null },
  axisOrder: 'easting_northing',
  points: projectedPairs,
  toleranceMeters: 1e-15,
});
assert.equal(roundTripFailure.valid, false);
assert.ok(roundTripFailure.roundTripVerifiedPointCount < projectedPairs.length);
assert.equal(roundTripFailure.inverseStatus, 'failed');

const axisConflict = transformAndVerifyProjectedPoints({
  coordinateSystem: { name: 'BFTM', epsg: null },
  axisOrder: 'northing_easting',
  points: projectedPairs,
});
assert.equal(axisConflict.valid, false);
assert.equal(axisConflict.axisStatus, 'conflict_or_unknown');

console.log(JSON.stringify({
  suite: 'coordinate-agent-phase09h-projected-promotion-regression',
  status: 'PASS',
  eligibleGate: promotionGate(eligible),
  unknownGeometryGate: promotionGate(unknownGeometry),
  blockedEvidenceCount: promotionGate(blockedEvidence).blockingUncertaintyCount,
  pendingReviewCount: promotionGate(pendingReview).reviewItemCount,
  partialTransformStatus: partialTransform.forwardStatus,
  roundTripFailureStatus: roundTripFailure.inverseStatus,
  axisConflictStatus: axisConflict.axisStatus,
  finalAuthorityVerifiedPointCount: 2,
  realProviderCalls: 0,
  automaticRetries: 0,
  mapAllowedOnFailure: false,
  kmlAllowedOnFailure: false,
}, null, 2));
