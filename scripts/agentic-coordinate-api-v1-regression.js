import assert from 'node:assert/strict';

import {
  createAgenticCoordinateApi,
  getAgenticCoordinateApiReadiness,
  requireAgenticCoordinateApiEnabled,
} from '../server/agentic-coordinate-api.js';

function providerResponse(payload) {
  return {
    choices: [{ message: { content: JSON.stringify(payload) } }],
  };
}

function responseRecorder() {
  return {
    statusCode: 200,
    headers: {},
    body: null,
    status(value) { this.statusCode = value; return this; },
    setHeader(name, value) { this.headers[name] = value; },
    json(value) { this.body = value; return value; },
  };
}

const result = {
  success: true,
  resultStatus: 'usable',
  displayText: 'A 10.0,20.0\nB 10.1,20.0\nC 10.1,20.1',
  coordinateSystem: {
    kind: 'geographic',
    name: 'WGS84',
    epsg: 'EPSG:4326',
    status: 'identified',
  },
  geometryType: 'Polygon',
  groups: [{
    name: null,
    points: [
      { label: 'A', sourceText: 'A 10.0,20.0', x: null, y: null, latitude: 10, longitude: 20, needsReview: false },
      { label: 'B', sourceText: 'B 10.1,20.0', x: null, y: null, latitude: 10.1, longitude: 20, needsReview: false },
      { label: 'C', sourceText: 'C 10.1,20.1', x: null, y: null, latitude: 10.1, longitude: 20.1, needsReview: false },
    ],
  }],
  warnings: [],
};

let providerCalls = 0;
const api = createAgenticCoordinateApi({
  modelName: 'fake-model',
  providerCall: async () => {
    providerCalls += 1;
    return providerResponse(result);
  },
});

const recognitionResponse = responseRecorder();
await api.recognize({
  file: {
    mimetype: 'image/png',
    buffer: Buffer.from('fake-image'),
  },
}, recognitionResponse);
assert.equal(recognitionResponse.statusCode, 200);
assert.equal(recognitionResponse.body.success, true);
assert.equal(recognitionResponse.body.result.summary.pointCount, 3);
assert.equal(providerCalls, 1);

const finalizeResponse = responseRecorder();
await api.finalize({
  body: {
    documentRevision: 2,
    currentText: result.displayText,
    sourceText: result.displayText,
    recognitionResult: result,
    name: 'Current polygon',
  },
}, finalizeResponse);
assert.equal(finalizeResponse.statusCode, 200);
assert.equal(finalizeResponse.body.map.documentRevision, 2);
assert.equal(finalizeResponse.body.map.geometryHash, finalizeResponse.body.kml.geometryHash);
assert.equal(providerCalls, 1);

const editedResult = structuredClone(result);
editedResult.displayText = result.displayText.replace('10.0', '10.05');
editedResult.groups[0].points[0].sourceText = 'A 10.05,20.0';
editedResult.groups[0].points[0].latitude = 10.05;
const editedApi = createAgenticCoordinateApi({
  modelName: 'fake-model',
  providerCall: async () => {
    providerCalls += 1;
    return providerResponse(editedResult);
  },
});
const editedResponse = responseRecorder();
await editedApi.finalize({
  body: {
    documentRevision: 3,
    currentText: editedResult.displayText,
    sourceText: result.displayText,
    recognitionResult: result,
  },
}, editedResponse);
assert.equal(editedResponse.statusCode, 200);
assert.equal(editedResponse.body.map.feature.geometry.coordinates[0][0][1], 10.05);
assert.equal(providerCalls, 2);

const missingImageResponse = responseRecorder();
await api.recognize({}, missingImageResponse);
assert.equal(missingImageResponse.statusCode, 400);
assert.equal(missingImageResponse.body.success, false);

const previousEnabled = process.env.AGENTIC_COORDINATE_V1_ENABLED;
const previousAtomicReady = process.env.AGENTIC_COORDINATE_ATOMIC_USAGE_READY;
delete process.env.AGENTIC_COORDINATE_V1_ENABLED;
delete process.env.AGENTIC_COORDINATE_ATOMIC_USAGE_READY;
const disabledResponse = responseRecorder();
let nextCalled = false;
requireAgenticCoordinateApiEnabled({}, disabledResponse, () => { nextCalled = true; });
assert.equal(disabledResponse.statusCode, 404);
assert.equal(disabledResponse.body.code, 'AGENTIC_COORDINATE_V1_DISABLED');
assert.equal(nextCalled, false);

process.env.AGENTIC_COORDINATE_V1_ENABLED = 'true';
assert.deepEqual(getAgenticCoordinateApiReadiness(), {
  enabled: false,
  featureEnabled: true,
  atomicUsageReady: false,
});
nextCalled = false;
requireAgenticCoordinateApiEnabled({}, responseRecorder(), () => { nextCalled = true; });
assert.equal(nextCalled, false);

process.env.AGENTIC_COORDINATE_ATOMIC_USAGE_READY = 'true';
assert.deepEqual(getAgenticCoordinateApiReadiness(), {
  enabled: true,
  featureEnabled: true,
  atomicUsageReady: true,
});
requireAgenticCoordinateApiEnabled({}, responseRecorder(), () => { nextCalled = true; });
assert.equal(nextCalled, true);
if (previousEnabled === undefined) delete process.env.AGENTIC_COORDINATE_V1_ENABLED;
else process.env.AGENTIC_COORDINATE_V1_ENABLED = previousEnabled;
if (previousAtomicReady === undefined) delete process.env.AGENTIC_COORDINATE_ATOMIC_USAGE_READY;
else process.env.AGENTIC_COORDINATE_ATOMIC_USAGE_READY = previousAtomicReady;

console.log('agentic coordinate api v1 regression: PASS');
