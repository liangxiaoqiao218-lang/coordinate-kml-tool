import assert from 'node:assert/strict';

import {
  createAgenticGeometryArtifact,
  finalizeAgenticCoordinateDocument,
} from '../server/agentic-coordinate-finalization/index.js';
import { normalizeAgenticCoordinateResult } from '../server/agentic-coordinate-recognition/index.js';

function point(label, latitude, longitude) {
  return {
    label,
    sourceText: `${label} | ${latitude} | ${longitude}`,
    x: null,
    y: null,
    latitude,
    longitude,
    needsReview: false,
  };
}

function geographicResult({ displayText, geometryType, groups }) {
  return normalizeAgenticCoordinateResult({
    success: true,
    resultStatus: 'usable',
    displayText,
    coordinateSystem: { kind: 'geographic', name: 'WGS84', epsg: 'EPSG:4326', status: 'identified' },
    geometryType,
    groups,
    warnings: [],
  });
}

const ungroupedCoordinates = [
  [10.8708333333, -8.2666666667], [10.8, -8.2666666667], [10.8, -8.2975],
  [10.8555555556, -8.2975], [10.8555555556, -8.2947222222], [10.8583333333, -8.2947222222],
  [10.8583333333, -8.2916666667], [10.8611111111, -8.2916666667], [10.8611111111, -8.2888888889],
  [10.8638888889, -8.2888888889], [10.8638888889, -8.2852777778], [10.8672222222, -8.2852777778],
  [10.8672222222, -8.2816666667], [10.8694444444, -8.2816666667], [10.8694444444, -8.2777777778],
  [10.8719444444, -8.2777777778], [10.8719444444, -8.2741666667], [10.875, -8.2741666667],
  [10.875, -8.2711111111], [10.8777777778, -8.2711111111], [10.8777777778, -8.2683333333],
  [10.8802777778, -8.2683333333], [10.8802777778, -8.2641666667], [10.8833333333, -8.2641666667],
  [10.8833333333, -8.2544444444], [10.8708333333, -8.2544444444],
];
const ungrouped = geographicResult({
  displayText: ungroupedCoordinates.map(([lat, lon], index) => `POINT ${index + 1} | ${lat} | ${lon}`).join('\n'),
  geometryType: 'Polygon',
  groups: [{ name: null, points: ungroupedCoordinates.map(([lat, lon], index) => point(`POINT ${index + 1}`, lat, lon)) }],
});
assert.equal(ungrouped.summary.groupCount, 1);
assert.equal(ungrouped.summary.pointCount, 26);
const ungroupedGeometry = createAgenticGeometryArtifact({ documentRevision: 1, result: ungrouped });
assert.equal(ungroupedGeometry.geometry.type, 'Polygon');
assert.equal(ungroupedGeometry.geometry.coordinates[0].length, 27);

const siteCounts = [8, 4, 4];
const siteStarts = [
  [12.01025, -9.1613333333],
  [11.9963055556, -9.1241666667],
  [11.994, -9.0995],
];
const grouped = geographicResult({
  displayText: 'SITES1\n8 points\n\nSITES2\n4 points\n\nSITES3\n4 points',
  geometryType: 'MultiPolygon',
  groups: siteCounts.map((count, groupIndex) => ({
    name: `SITES${groupIndex + 1}`,
    points: Array.from({ length: count }, (_, pointIndex) => point(
      `${pointIndex + 1}`,
      siteStarts[groupIndex][0] + pointIndex * 0.0005,
      siteStarts[groupIndex][1] + (pointIndex % 2) * 0.0007 + Math.floor(pointIndex / 2) * 0.0003,
    )),
  })),
});
assert.deepEqual(grouped.groups.map(group => group.points.length), siteCounts);
assert.ok(grouped.groups.flatMap(group => group.points).every(item => item.latitude !== item.longitude));
const groupedGeometry = createAgenticGeometryArtifact({ documentRevision: 2, result: grouped });
assert.equal(groupedGeometry.geometry.type, 'MultiPolygon');
assert.equal(groupedGeometry.geometry.coordinates.length, 3);

const observations = geographicResult({
  displayText: 'Bull QV_1\nBull QV_2\nBull QV_3\nBull QV_4\nBull QV_5',
  geometryType: 'MultiPoint',
  groups: [{ name: 'Observed bull quartz veins', points: [
    point('Bull QV_1', 20.12, -2.79),
    point('Bull QV_2', 20.123, -2.792),
    point('Bull QV_3', 20.119, -2.796),
    point('Bull QV_4', 20.122, -2.806),
    point('Bull QV_5', 20.118, -2.788),
  ] }],
});
const observationGeometry = createAgenticGeometryArtifact({ documentRevision: 3, result: observations });
assert.equal(observationGeometry.geometry.type, 'MultiPoint');
assert.equal(observationGeometry.geometry.coordinates.length, 5);

for (const [documentRevision, result] of [[4, ungrouped], [5, grouped], [6, observations]]) {
  const finalized = await finalizeAgenticCoordinateDocument({
    documentRevision,
    currentText: result.displayText,
    sourceText: result.displayText,
    recognitionResult: result,
    providerCall: async () => { throw new Error('Unchanged accepted samples must not call Provider'); },
  });
  assert.equal(finalized.execution.providerCallCount, 0);
  assert.equal(finalized.map.geometryHash, finalized.kml.geometryHash);
  assert.match(finalized.kml.content, /<kml /);
}

console.log('agentic coordinate user sample acceptance v1 regression: PASS');
