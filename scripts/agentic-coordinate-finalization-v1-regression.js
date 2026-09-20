import assert from 'node:assert/strict';

import {
  buildAgenticKml,
  createAgenticGeometryArtifact,
  finalizeAgenticCoordinateDocument,
  runAgenticCoordinateFinalization,
  transformAgenticCoordinateResult,
} from '../server/agentic-coordinate-finalization/index.js';

function providerPayload(payload) {
  return {
    choices: [{
      message: {
        content: JSON.stringify(payload),
      },
    }],
  };
}

const editedPolygonPayload = {
  success: true,
  resultStatus: 'usable',
  displayText: 'A 11°28\'31.26"N, 08°40\'42.13"W\nB 11°28\'31.60"N, 08°40\'32.90"W\nC 11°28\'18.01"N, 08°40\'31.01"W',
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
      { label: 'A', sourceText: 'A 11°28\'31.26"N, 08°40\'42.13"W', x: null, y: null, latitude: 11.47535, longitude: -8.6783694444, needsReview: false },
      { label: 'B', sourceText: 'B 11°28\'31.60"N, 08°40\'32.90"W', x: null, y: null, latitude: 11.4754444444, longitude: -8.6758055556, needsReview: false },
      { label: 'C', sourceText: 'C 11°28\'18.01"N, 08°40\'31.01"W', x: null, y: null, latitude: 11.4716694444, longitude: -8.6752805556, needsReview: false },
    ],
  }],
  warnings: [],
};

let callCount = 0;
const finalized = await runAgenticCoordinateFinalization({
  currentText: editedPolygonPayload.displayText,
  recognitionContext: {
    displayText: editedPolygonPayload.displayText.replace('31.26', '37.26'),
    geometryType: 'Polygon',
  },
  documentRevision: 7,
  modelName: 'fake-model',
  providerCall: async ({ prompt, imageItems, stageName }) => {
    callCount += 1;
    assert.match(prompt, /CURRENT TEXT \(authoritative\)/);
    assert.match(prompt, /31\.26/);
    assert.deepEqual(imageItems, []);
    assert.equal(stageName, 'agentic_text_finalize');
    return providerPayload(editedPolygonPayload);
  },
});

assert.equal(callCount, 1);
assert.equal(finalized.documentRevision, 7);
assert.equal(finalized.execution.providerCallCount, 1);
assert.equal(finalized.execution.retryCount, 0);
assert.equal(finalized.result.groups[0].points[0].sourceText.includes('31.26'), true);

const polygonArtifact = createAgenticGeometryArtifact({
  documentRevision: finalized.documentRevision,
  result: finalized.result,
});
assert.equal(polygonArtifact.geometry.type, 'Polygon');
assert.equal(polygonArtifact.geometry.coordinates[0].length, 4);
assert.deepEqual(
  polygonArtifact.geometry.coordinates[0][0],
  polygonArtifact.geometry.coordinates[0][3],
);

const kml = buildAgenticKml({ geometryArtifact: polygonArtifact, name: 'Edited & current' });
assert.match(kml, /Edited &amp; current/);
assert.match(kml, /-8\.6783694444,11\.47535,0/);

const observations = {
  ...finalized.result,
  geometryType: 'MultiPoint',
};
const observationsArtifact = createAgenticGeometryArtifact({
  documentRevision: 8,
  result: observations,
});
assert.equal(observationsArtifact.geometry.type, 'MultiPoint');
assert.equal(observationsArtifact.geometry.coordinates.length, 3);

const siteTwo = {
  ...finalized.result.groups[0],
  name: 'SITE2',
  points: finalized.result.groups[0].points.map(point => ({
    ...point,
    longitude: point.longitude + 0.05,
  })),
};
const multiPolygon = {
  ...finalized.result,
  geometryType: 'MultiPolygon',
  groups: [
    { ...finalized.result.groups[0], name: 'SITE1' },
    siteTwo,
  ],
};
const multiPolygonArtifact = createAgenticGeometryArtifact({
  documentRevision: 9,
  result: multiPolygon,
});
assert.equal(multiPolygonArtifact.geometry.type, 'MultiPolygon');
assert.equal(multiPolygonArtifact.geometry.coordinates.length, 2);

const projected = {
  ...finalized.result,
  coordinateSystem: {
    kind: 'projected',
    name: 'UTM WGS 1984 ZONA 50S',
    epsg: 'EPSG:32750',
    status: 'identified',
  },
  groups: [{
    name: null,
    points: [{
      label: '1',
      sourceText: '1 | 778807.293 | 9721476.737',
      x: 778807.293,
      y: 9721476.737,
      latitude: null,
      longitude: null,
      needsReview: false,
    }],
  }],
};
assert.throws(
  () => createAgenticGeometryArtifact({ documentRevision: 10, result: projected }),
  (error) => error.code === 'PROJECTED_CRS_TRANSFORM_REQUIRED',
);

const utm50s = transformAgenticCoordinateResult(projected);
assert.equal(utm50s.transformation.sourceCrs, 'EPSG:32750');
assert.equal(utm50s.transformation.targetCrs, 'EPSG:4326');
assert.ok(utm50s.groups[0].points[0].latitude < 0);
assert.ok(utm50s.groups[0].points[0].longitude > 110);
const utmArtifact = createAgenticGeometryArtifact({
  documentRevision: 10,
  result: { ...utm50s, geometryType: 'MultiPoint' },
});
assert.equal(utmArtifact.geometry.type, 'MultiPoint');

const utm28n = {
  ...projected,
  coordinateSystem: {
    kind: 'projected',
    name: '28N',
    epsg: null,
    status: 'identified',
  },
  geometryType: 'MultiPoint',
  groups: [{
    name: null,
    points: [{
      ...projected.groups[0].points[0],
      sourceText: '28N | 469000 | 2229000',
      x: 469000,
      y: 2229000,
    }],
  }],
};
const utm28nTransformed = transformAgenticCoordinateResult(utm28n);
assert.equal(utm28nTransformed.transformation.sourceCrs, 'EPSG:32628');
assert.ok(utm28nTransformed.groups[0].points[0].latitude > 0);
assert.ok(utm28nTransformed.groups[0].points[0].longitude < 0);

const kyrgyz = {
  ...projected,
  coordinateSystem: {
    kind: 'projected',
    name: 'Kyrgyzstan GK',
    epsg: 'EPSG:28413',
    status: 'identified',
  },
  geometryType: 'MultiPoint',
  groups: [{
    name: null,
    points: [{
      ...projected.groups[0].points[0],
      sourceText: '1 | 13261341 | 4607777',
      x: 13261341,
      y: 4607777,
    }],
  }],
};
const kyrgyzTransformed = transformAgenticCoordinateResult(kyrgyz);
assert.equal(kyrgyzTransformed.transformation.sourceCrs, 'EPSG:28413');
assert.ok(kyrgyzTransformed.groups[0].points[0].latitude > 39);
assert.ok(kyrgyzTransformed.groups[0].points[0].longitude > 69);

const bftmResult = {
  ...projected,
  displayText: '1 | 658800 | 1364200\n2 | 651600 | 1364200\n3 | 651600 | 1364000',
  coordinateSystem: {
    kind: 'projected',
    name: 'ITRF 2008 / Projection BFTM',
    epsg: null,
    status: 'identified',
  },
  geometryType: 'Polygon',
  groups: [{ name: null, points: [
    { label: '1', sourceText: '658800,1364200', x: 658800, y: 1364200, latitude: null, longitude: null, needsReview: false },
    { label: '2', sourceText: '651600,1364200', x: 651600, y: 1364200, latitude: null, longitude: null, needsReview: false },
    { label: '3', sourceText: '651600,1364000', x: 651600, y: 1364000, latitude: null, longitude: null, needsReview: false },
  ] }],
};
const bftmTransformed = transformAgenticCoordinateResult(bftmResult);
assert.equal(bftmTransformed.transformation.sourceCrs, 'BFTM:ITRF2008');
assert.equal(bftmTransformed.transformation.method, 'bftm');
assert.ok(bftmTransformed.groups[0].points.every(point => (
  point.latitude > 12 && point.latitude < 13 && point.longitude > -2 && point.longitude < 0
)));

const bftmFinal = await finalizeAgenticCoordinateDocument({
  documentRevision: 5,
  currentText: bftmResult.displayText,
  sourceText: bftmResult.displayText,
  recognitionResult: bftmResult,
  providerCall: async () => {
    throw new Error('Provider must not be called for unchanged BFTM text');
  },
});
assert.equal(bftmFinal.map.feature.geometry.type, 'Polygon');
assert.equal(bftmFinal.map.geometryHash, bftmFinal.kml.geometryHash);

const reused = await finalizeAgenticCoordinateDocument({
  documentRevision: 11,
  currentText: finalized.result.displayText,
  sourceText: finalized.result.displayText,
  recognitionResult: finalized.result,
  name: 'No edit',
  providerCall: async () => {
    throw new Error('Provider must not be called for unchanged recognition text');
  },
});
assert.equal(reused.execution.providerCallCount, 0);
assert.equal(reused.execution.source, 'initial_recognition');
assert.equal(reused.map.documentRevision, 11);
assert.equal(reused.kml.documentRevision, 11);
assert.equal(reused.map.geometryHash, reused.kml.geometryHash);

let editedPipelineCalls = 0;
const editedPipeline = await finalizeAgenticCoordinateDocument({
  documentRevision: 12,
  currentText: editedPolygonPayload.displayText,
  sourceText: editedPolygonPayload.displayText.replace('31.26', '37.26'),
  recognitionResult: finalized.result,
  name: 'Edited once',
  providerCall: async () => {
    editedPipelineCalls += 1;
    return providerPayload(editedPolygonPayload);
  },
});
assert.equal(editedPipelineCalls, 1);
assert.equal(editedPipeline.execution.providerCallCount, 1);
assert.equal(editedPipeline.map.geometryHash, editedPipeline.kml.geometryHash);
assert.match(editedPipeline.kml.content, /-8\.6783694444,11\.47535,0/);

console.log('agentic coordinate finalization v1 regression: PASS');
