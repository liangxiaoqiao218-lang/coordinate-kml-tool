import assert from 'node:assert/strict';

import {
  CoordinateAgentToolRegistry,
  CoordinateIntelligenceAgentKernel,
  MockCoordinateAgentProviderAdapter,
  registerCoordinateMathTools,
} from '../server/coordinate-agent/index.js';
import {
  PROJECTED_ROUND_TRIP_TOLERANCE_METERS,
  resolveProjectedCrs,
  transformAndVerifyProjectedPoints,
} from '../server/projection/projected-crs-registry.js';

function projectedTurn({ name, epsg = null, points, geometryType = 'MultiPoint', uncertainties = [] }) {
  return {
    observation: {
      summary: 'Whole image observed with explicit projected CRS and complete X/Y evidence.',
      orientationDegrees: 0,
      regions: [{
        id: 'whole', kind: 'document', bbox: [0, 0, 1, 1], sourceText: '',
        semanticRole: 'whole image', confidence: 1, provenance: 'offline_mock',
      }],
    },
    plan: { rationale: 'Leave deterministic conversion to the local safety layer.', actions: [] },
    candidate: {
      success: true,
      resultStatus: 'needs_review',
      displayText: 'source-faithful projected rows',
      coordinateSystem: { kind: 'projected', name, epsg, status: 'identified' },
      geometryType,
      groups: [{
        name: 'Visible group',
        points: points.map((pair, index) => ({
          label: `P${index + 1}`,
          sourceText: `P${index + 1} visible projected row`,
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
    reviewItems: [],
  };
}

const registryIdentity = resolveProjectedCrs({ name: 'BFTM ITRF2008', epsg: null });
assert.equal(registryIdentity.status, 'identified');
assert.equal(registryIdentity.definition.id, 'BFTM:ITRF2008');
assert.equal(resolveProjectedCrs({ name: null, epsg: 'EPSG:32630' }).status, 'identified');
assert.equal(resolveProjectedCrs({ name: 'WGS 84 / UTM zone 30N', epsg: null }).definition.id, 'EPSG:32630');
assert.equal(resolveProjectedCrs({ name: 'UTM zone 30N', epsg: null }).status, 'unsupported');
assert.equal(resolveProjectedCrs({ name: null, epsg: 'EPSG:28413' }).status, 'unsupported');
assert.equal(resolveProjectedCrs({ name: 'BFTM', epsg: 'EPSG:32630' }).status, 'conflict');
assert.equal(resolveProjectedCrs({ name: 'BFTM', epsg: 'EPSG:28413' }).status, 'unsupported');

const bftmPairs = [
  { x: 600000, y: 0 },
  { x: 600100, y: 100 },
];
const direct = transformAndVerifyProjectedPoints({
  coordinateSystem: { name: 'BFTM', epsg: null },
  axisOrder: 'easting_northing',
  points: bftmPairs,
});
assert.equal(direct.valid, true);
assert.equal(direct.transformedPointCount, 2);
assert.equal(direct.roundTripVerifiedPointCount, 2);
assert.ok(direct.maximumRoundTripErrorMeters <= PROJECTED_ROUND_TRIP_TOLERANCE_METERS);

const utmNorth = transformAndVerifyProjectedPoints({
  coordinateSystem: { name: null, epsg: 'EPSG:32630' },
  axisOrder: 'easting_northing',
  points: [{ x: 500000, y: 0 }],
});
assert.equal(utmNorth.valid, true);
assert.equal(utmNorth.crsId, 'EPSG:32630');

const utmSouth = transformAndVerifyProjectedPoints({
  coordinateSystem: { name: null, epsg: 'EPSG:32730' },
  axisOrder: 'easting_northing',
  points: [{ x: 500000, y: 10000000 }],
});
assert.equal(utmSouth.valid, true);
assert.equal(utmSouth.crsId, 'EPSG:32730');

const axisConflict = transformAndVerifyProjectedPoints({
  coordinateSystem: { name: 'BFTM', epsg: null },
  axisOrder: 'northing_easting',
  points: bftmPairs,
});
assert.equal(axisConflict.valid, false);
assert.equal(axisConflict.axisStatus, 'conflict_or_unknown');

const unknownCrs = transformAndVerifyProjectedPoints({
  coordinateSystem: { name: 'Unregistered projected CRS', epsg: null },
  axisOrder: 'easting_northing',
  points: bftmPairs,
});
assert.equal(unknownCrs.valid, false);
assert.equal(unknownCrs.crsStatus, 'unsupported');

const registry = new CoordinateAgentToolRegistry();
registerCoordinateMathTools(registry);
const confirmed = await new CoordinateIntelligenceAgentKernel({
  providerAdapter: new MockCoordinateAgentProviderAdapter([
    projectedTurn({ name: 'BFTM', points: bftmPairs }),
  ]),
  toolRegistry: registry,
  maxProviderCalls: 1,
  maxIterations: 1,
}).run({ imageRef: 'offline-image', requestId: 'offline:phase09e:confirmed' });
assert.equal(confirmed.terminalState, 'CONFIRMED');
assert.deepEqual(confirmed.authorization, { mapAllowed: true, kmlAllowed: true });
assert.equal(confirmed.coordinateResult.groups[0].points.every(point => (
  Number.isFinite(point.latitude) && Number.isFinite(point.longitude) && point.needsReview === false
)), true);
const projectionEvidence = confirmed.evidence.toolResults.find(item => item.actionId === 'safety-projection-transform');
assert.equal(projectionEvidence.output.valid, true);

const unsupportedRegistry = new CoordinateAgentToolRegistry();
registerCoordinateMathTools(unsupportedRegistry);
const unsupported = await new CoordinateIntelligenceAgentKernel({
  providerAdapter: new MockCoordinateAgentProviderAdapter([
    projectedTurn({ name: null, epsg: 'EPSG:28413', points: bftmPairs }),
  ]),
  toolRegistry: unsupportedRegistry,
  maxProviderCalls: 1,
  maxIterations: 1,
}).run({ imageRef: 'offline-image', requestId: 'offline:phase09e:unsupported' });
assert.equal(unsupported.terminalState, 'REVIEW_REQUIRED');
assert.deepEqual(unsupported.authorization, { mapAllowed: false, kmlAllowed: false });

const uncertainRegistry = new CoordinateAgentToolRegistry();
registerCoordinateMathTools(uncertainRegistry);
const uncertain = await new CoordinateIntelligenceAgentKernel({
  providerAdapter: new MockCoordinateAgentProviderAdapter([
    projectedTurn({
      name: 'BFTM',
      points: bftmPairs,
      uncertainties: [{
        code: 'SOURCE_DIGIT_UNRESOLVED',
        message: 'One source digit remains unresolved',
        evidenceRegionIds: ['whole'],
        blocking: true,
      }],
    }),
  ]),
  toolRegistry: uncertainRegistry,
  maxProviderCalls: 1,
  maxIterations: 1,
}).run({ imageRef: 'offline-image', requestId: 'offline:phase09e:blocked-evidence' });
assert.equal(uncertain.terminalState, 'REVIEW_REQUIRED');
assert.deepEqual(uncertain.authorization, { mapAllowed: false, kmlAllowed: false });

console.log(JSON.stringify({
  suite: 'coordinate-agent-phase09e-projection-regression',
  status: 'PASS',
  supportedCrsFamilies: ['BFTM:ITRF2008', 'EPSG:32601-32660', 'EPSG:32701-32760'],
  toleranceMeters: PROJECTED_ROUND_TRIP_TOLERANCE_METERS,
  maximumAggregateRoundTripErrorMeters: direct.maximumRoundTripErrorMeters,
  transformedPointCount: direct.transformedPointCount,
  verifiedPointCount: direct.roundTripVerifiedPointCount,
  confirmedTerminalState: confirmed.terminalState,
  unsupportedTerminalState: unsupported.terminalState,
  unresolvedEvidenceTerminalState: uncertain.terminalState,
  realProviderCalls: 0,
  mapAllowedOnFailure: unsupported.authorization.mapAllowed,
  kmlAllowedOnFailure: unsupported.authorization.kmlAllowed,
}, null, 2));
