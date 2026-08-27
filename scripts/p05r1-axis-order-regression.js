import assert from "node:assert/strict";
import { parseManualLongitudeLatitudeText } from "../server/manual-coordinate-input.js";

const cases = [
  ["116.391245,39.907654", 116.391245, 39.907654],
  ["-122.4194,37.7749", -122.4194, 37.7749],
  ["2.3522,48.8566", 2.3522, 48.8566],
  ["103.8198,1.3521", 103.8198, 1.3521]
];

for (const [input, longitude, latitude] of cases) {
  const points = parseManualLongitudeLatitudeText(input);
  assert.equal(points?.length, 1, `${input} should produce one point`);
  assert.equal(points[0].lon, longitude);
  assert.equal(points[0].lat, latitude);
  assert.equal(points[0].kmlCoordinate, `${longitude},${latitude},0`);
  console.log(`PASS manual lon,lat ${input}`);
}

const polygon = parseManualLongitudeLatitudeText([
  "116.391245,39.907654",
  "116.401245,39.907654",
  "116.401245,39.917654"
].join("\n"));
assert.deepEqual(polygon.map(point => [point.lon, point.lat]), [
  [116.391245, 39.907654],
  [116.401245, 39.907654],
  [116.401245, 39.917654]
]);
assert.equal(parseManualLongitudeLatitudeText("200,95"), null);
assert.equal(parseManualLongitudeLatitudeText("39.907654,116.391245"), null);
console.log("P-05R1 axis-order regression: 7/7 PASS");
