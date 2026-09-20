import assert from 'node:assert/strict';

import { AgenticCoordinateController } from '../assets/agentic-coordinate/agentic-coordinate-controller.js';

function response(body, ok = true) {
  return {
    ok,
    json: async () => body,
  };
}

const recognitionResult = {
  contractVersion: 'agentic-coordinate-recognition/v1',
  success: true,
  resultStatus: 'usable',
  displayText: 'A 10,20\nB 10.1,20\nC 10.1,20.1',
  coordinateSystem: { kind: 'geographic', name: 'WGS84', epsg: 'EPSG:4326', status: 'identified' },
  geometryType: 'Polygon',
  groups: [{ name: null, points: [
    { label: 'A', sourceText: 'A 10,20', x: null, y: null, latitude: 10, longitude: 20, needsReview: false },
    { label: 'B', sourceText: 'B 10.1,20', x: null, y: null, latitude: 10.1, longitude: 20, needsReview: false },
    { label: 'C', sourceText: 'C 10.1,20.1', x: null, y: null, latitude: 10.1, longitude: 20.1, needsReview: false },
  ] }],
  warnings: [],
  summary: { groupCount: 1, pointCount: 3 },
};

const changes = [];
let recognizeResolve;
const requests = [];
const usageEvents = [];
const controller = new AgenticCoordinateController({
  onChange: workspace => changes.push(workspace),
  usage: {
    getVisitorId: () => 'visitor-1',
    createRequestId: () => '11111111-1111-4111-8111-111111111111',
    prepareSession: async visitorId => { usageEvents.push(`prepare:${visitorId}`); },
    rememberPendingRequest: requestId => { usageEvents.push(`remember:${requestId}`); },
    clearPendingRequest: () => { usageEvents.push('clear'); },
    headers: headers => ({ ...headers, 'x-test-source': 'agentic' }),
  },
  fetchImpl: async (url, options = {}) => {
    requests.push({ url, options });
    if (url.endsWith('/status')) return response({ enabled: true });
    if (url.endsWith('/recognize')) {
      return new Promise(resolve => { recognizeResolve = () => resolve(response({ success: true, usageConsumed: true, result: recognitionResult })); });
    }
    if (url.endsWith('/finalize')) {
      const request = JSON.parse(options.body);
      return response({
        success: true,
        documentRevision: request.documentRevision,
        map: { documentRevision: request.documentRevision, geometryHash: 'hash', feature: { geometry: { type: 'Polygon', coordinates: [] } } },
        kml: { documentRevision: request.documentRevision, geometryHash: 'hash', content: '<kml />' },
      });
    }
    throw new Error(`Unexpected URL ${url}`);
  },
});

assert.equal(await controller.initialize(), true);
const firstFile = new Blob(['first'], { type: 'image/png' });
firstFile.name = 'first.png';
const firstRecognition = controller.recognize(firstFile);
assert.equal(controller.workspace.phase, 'recognizing');
assert.equal(controller.workspace.currentText, '');
await Promise.resolve();
await Promise.resolve();
assert.equal(typeof recognizeResolve, 'function');
recognizeResolve();
await firstRecognition;
assert.equal(controller.workspace.phase, 'usable');
assert.equal(controller.workspace.currentText, recognitionResult.displayText);
assert.equal(controller.hasCommittedRecognitionUsage(), true);
const recognitionRequest = requests.find(item => item.url.endsWith('/recognize'));
assert.equal(recognitionRequest.options.headers['x-visitor-id'], 'visitor-1');
assert.equal(recognitionRequest.options.headers['x-recognition-request-id'], '11111111-1111-4111-8111-111111111111');
assert.deepEqual(usageEvents, [
  'prepare:visitor-1',
  'remember:11111111-1111-4111-8111-111111111111',
  'clear',
]);

const originalRevision = controller.workspace.documentRevision;
controller.edit(recognitionResult.displayText.replace('A 10,20', 'A 10.05,20'));
assert.equal(controller.workspace.documentRevision, originalRevision + 1);
assert.deepEqual(controller.workspace.derived, { map: null, kml: null });

const finalized = await controller.finalize({ name: 'Edited' });
assert.equal(finalized.map.geometryHash, finalized.kml.geometryHash);
assert.equal(controller.workspace.derived.map.documentRevision, controller.workspace.documentRevision);

const staleFinalize = controller.finalize();
controller.edit(`${controller.workspace.currentText}\nD 10,20.1`);
await assert.rejects(staleFinalize, error => error.code === 'STALE_AGENTIC_COORDINATE_REVISION');
assert.deepEqual(controller.workspace.derived, { map: null, kml: null });

assert.ok(changes.length >= 5);
assert.equal(requests.filter(item => item.url.endsWith('/recognize')).length, 1);

let pendingRequestId = '22222222-2222-4222-8222-222222222222';
let recoveryCalls = 0;
const recoveryController = new AgenticCoordinateController({
  fetchImpl: async url => response({ enabled: url.endsWith('/status') }),
  usage: {
    getVisitorId: () => 'visitor-2',
    getPendingRequestId: () => pendingRequestId,
    clearPendingRequest: () => { pendingRequestId = ''; },
    recover: async ({ requestId }) => {
      recoveryCalls += 1;
      return { ok: true, payload: { success: true, usageConsumed: true, requestId, result: recognitionResult, recovered: true } };
    },
  },
});
assert.equal(await recoveryController.initialize(), true);
const recovered = await recoveryController.recoverPending();
assert.equal(recovered.recovered, true);
assert.equal(recoveryCalls, 1);
assert.equal(pendingRequestId, '');
assert.equal(recoveryController.workspace.currentText, recognitionResult.displayText);
assert.equal(recoveryController.hasCommittedRecognitionUsage(), true);

console.log('agentic coordinate controller v1 regression: PASS');
