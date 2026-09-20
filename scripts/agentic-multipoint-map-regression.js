import assert from 'node:assert/strict';

import {
  createGeometryRenderPlan,
  geometryBounds,
  geometryIsDrawable,
} from '../assets/spatial-map/geometry-render-plan.js';
import { calculateSpatialFacts } from '../server/spatial/spatial-facts.js';

const observations = {
  type: 'MultiPoint',
  coordinates: [
    [-8.6783694444, 11.47535],
    [-8.6758055556, 11.4754444444],
    [-8.6752805556, 11.4716694444],
  ],
};

assert.equal(geometryIsDrawable(observations), true);
assert.deepEqual(geometryBounds(observations), {
  west: -8.6783694444,
  south: 11.4716694444,
  east: -8.6752805556,
  north: 11.4754444444,
});

const plan = createGeometryRenderPlan(observations);
assert.equal(plan.source.data.geometry.type, 'MultiPoint');
assert.equal(plan.layers.length, 1);
assert.equal(plan.layers[0].type, 'circle');

const facts = calculateSpatialFacts(observations);
assert.equal(facts.geometryType, 'MultiPoint');
assert.equal(facts.pointCount, 3);
assert.equal(facts.areaMeters2, null);
assert.equal(facts.perimeterMeters, null);
assert.ok(facts.centroid[0] < -8.67);
assert.ok(facts.centroid[1] > 11.47);

assert.equal(geometryIsDrawable({ type: 'MultiPoint', coordinates: [] }), false);
assert.equal(geometryIsDrawable({ type: 'MultiPoint', coordinates: [[200, 20]] }), false);

console.log('agentic multipoint map regression: PASS');

