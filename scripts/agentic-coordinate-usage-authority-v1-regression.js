import assert from 'node:assert/strict';

import {
  buildAgenticCoordinateUsageAuthority,
  evaluateAgenticCoordinateUsageAuthority,
} from '../server/agentic-coordinate-usage-authority.js';
import {
  CoordinateUsageAtomicityService,
  createCoordinateUsageCommitController,
  evaluateCoordinateUsageAuthority,
} from '../server/coordinate-usage-atomicity.js';

const requestId = '11111111-1111-4111-8111-111111111111';
const result = {
  success: true,
  resultStatus: 'usable',
  displayText: 'A 10,20\nB 10.1,20.1',
  coordinateSystem: { kind: 'geographic', name: 'WGS84', epsg: 'EPSG:4326', status: 'identified' },
  geometryType: 'LineString',
  groups: [{ name: null, points: [
    { label: 'A', sourceText: 'A 10,20', x: null, y: null, latitude: 10, longitude: 20, needsReview: false },
    { label: 'B', sourceText: 'B 10.1,20.1', x: null, y: null, latitude: 10.1, longitude: 20.1, needsReview: false },
  ] }],
  warnings: [],
};

const authority = buildAgenticCoordinateUsageAuthority({ recognitionRequestId: requestId, result });
const body = { success: true, requestId, result, agenticCoordinateAuthority: authority };

const evaluated = evaluateAgenticCoordinateUsageAuthority({ httpStatus: 200, body });
assert.equal(evaluated.eligible, true);
assert.equal(evaluated.identity.resultId, `agentic:${requestId}`);
assert.equal(evaluateCoordinateUsageAuthority({ httpStatus: 200, body }).eligible, true);

const tampered = structuredClone(body);
tampered.result.groups[0].points[0].latitude = 12;
assert.equal(evaluateCoordinateUsageAuthority({ httpStatus: 200, body: tampered }).eligible, false);

const failed = structuredClone(result);
failed.success = false;
failed.resultStatus = 'failed';
failed.displayText = '';
failed.groups = [];
assert.throws(
  () => buildAgenticCoordinateUsageAuthority({ recognitionRequestId: requestId, result: failed }),
  /failed recognition/
);

const needsReview = structuredClone(result);
needsReview.resultStatus = 'needs_review';
needsReview.groups[0].points[0].needsReview = true;
const reviewAuthority = buildAgenticCoordinateUsageAuthority({ recognitionRequestId: requestId, result: needsReview });
assert.equal(reviewAuthority.decisionState, 'REVIEW_REQUIRED');

const atomicState = { prepared: null, commitCalls: 0 };
const atomicityService = new CoordinateUsageAtomicityService({
  sealKey: Buffer.alloc(32, 9),
  supabase: {
    async rpc(name, args) {
      if (name === 'prepare_coordinate_recognition_result') {
        atomicState.prepared = args;
        return { data: [{ result: 'PREPARED', state: 'PREPARED' }], error: null };
      }
      if (name === 'commit_coordinate_recognition_usage') {
        atomicState.commitCalls += 1;
        return { data: [{ result: 'COMMITTED', state: 'COMMITTED', consume_type: 'free' }], error: null };
      }
      if (name === 'get_coordinate_recognition_commit_state') {
        return { data: [{
          result: 'ALREADY_COMMITTED',
          state: 'COMMITTED',
          consume_type: 'free',
          sealed_result: atomicState.prepared.p_sealed_result,
          sealed_result_sha256: atomicState.prepared.p_sealed_result_sha256,
        }], error: null };
      }
      throw new Error(`unexpected RPC ${name}`);
    },
  },
});
const usageController = createCoordinateUsageCommitController({
  atomicityService,
  recognitionRequestId: requestId,
  userId: 'agentic-test-user',
  sessionBindingSha256: 'a'.repeat(64),
  providerCostState: () => 'USAGE_REPORTED',
});
usageController.schedule({ recognitionMode: 'agentic_coordinate_v1' });
const settlement = await usageController.settle({ httpStatus: 200, body });
assert.equal(settlement.kind, 'USAGE_COMMITTED');
assert.equal(atomicState.commitCalls, 1);
const recovered = await atomicityService.recover({
  recognitionRequestId: requestId,
  userId: 'agentic-test-user',
  sessionBindingSha256: 'a'.repeat(64),
});
assert.equal(recovered.responsePayload.result.displayText, result.displayText);
assert.equal(atomicState.commitCalls, 1);

console.log('agentic coordinate usage authority v1 regression: PASS');
