import assert from "node:assert/strict";
import fs from "node:fs";
import { parseManualLongitudeLatitudeText } from "../server/manual-coordinate-input.js";

function extractFunctionSource(source, functionName) {
  const marker = `function ${functionName}(`;
  const start = source.indexOf(marker);
  assert.notEqual(start, -1, `${functionName} must exist`);
  const openBrace = source.indexOf("{", start);
  let depth = 0;
  for (let index = openBrace; index < source.length; index += 1) {
    if (source[index] === "{") depth += 1;
    if (source[index] === "}") {
      depth -= 1;
      if (depth === 0) return source.slice(start, index + 1);
    }
  }
  throw new Error(`${functionName} body is not closed`);
}

const indexHtml = fs.readFileSync("index.html", "utf8");
const stripLeadingCoordinateLabel = Function(`
  ${extractFunctionSource(indexHtml, "stripLeadingCoordinateLabel")}
  return stripLeadingCoordinateLabel;
`)();
const normalizeManualCoordinateTextForFinalizer = Function(`
  ${extractFunctionSource(indexHtml, "stripLeadingCoordinateLabel")}
  ${extractFunctionSource(indexHtml, "normalizeManualCoordinateTextForFinalizer")}
  return normalizeManualCoordinateTextForFinalizer;
`)();

const exactCoordinate = "116.391245,39.907654";
const clientLabelCases = [
  [exactCoordinate, exactCoordinate],
  [`1. ${exactCoordinate}`, exactCoordinate],
  [`1) ${exactCoordinate}`, exactCoordinate],
  [`1: ${exactCoordinate}`, exactCoordinate],
  [`1 - ${exactCoordinate}`, exactCoordinate],
  [`Point 1: ${exactCoordinate}`, exactCoordinate],
  ["-11.178174,12.319572", "-11.178174,12.319572"],
  ["0.123456,10.123456", "0.123456,10.123456"],
  ["-0.123456,10.123456", "-0.123456,10.123456"]
];

for (const [input, expected] of clientLabelCases) {
  assert.equal(stripLeadingCoordinateLabel(input), expected, `${input} must preserve coordinate semantics`);
  const points = parseManualLongitudeLatitudeText(expected);
  assert.equal(points?.length, 1, `${input} should produce one normalized point`);
}

const numberedPolygon = [
  "1. 116.391245,39.907654",
  "2. 116.392245,39.907654",
  "3. 116.392245,39.908654",
  "4. 116.391245,39.908654"
].join("\n");
const normalizedPolygon = [
  "116.391245,39.907654",
  "116.392245,39.907654",
  "116.392245,39.908654",
  "116.391245,39.908654"
].join("\n");
const boundaryEqualityCases = [
  [exactCoordinate, exactCoordinate],
  [`1. ${exactCoordinate}`, exactCoordinate],
  [`1) ${exactCoordinate}`, exactCoordinate],
  [`1: ${exactCoordinate}`, exactCoordinate],
  [numberedPolygon, normalizedPolygon]
];

for (const [rawText, expected] of boundaryEqualityCases) {
  const clientKmlNormalizedText = normalizeManualCoordinateTextForFinalizer(rawText);
  const manualFinalizerRequestCoordinateText = normalizeManualCoordinateTextForFinalizer(rawText);
  assert.equal(clientKmlNormalizedText, expected);
  assert.equal(manualFinalizerRequestCoordinateText, expected);
  assert.equal(clientKmlNormalizedText, manualFinalizerRequestCoordinateText);
}

assert.match(indexHtml, /const normalizedText = normalizeManualCoordinateTextForFinalizer\(text\);/);
assert.equal((indexHtml.match(/coordinateText: normalizedCoordinateText/g) || []).length, 2);

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
  "116.392245,39.907654",
  "116.392245,39.908654",
  "116.391245,39.908654"
].join("\n"));
assert.deepEqual(polygon.map(point => [point.lon, point.lat]), [
  [116.391245, 39.907654],
  [116.392245, 39.907654],
  [116.392245, 39.908654],
  [116.391245, 39.908654]
]);
assert.equal(parseManualLongitudeLatitudeText("200,95"), null);
assert.equal(parseManualLongitudeLatitudeText("39.907654,116.391245"), null);
console.log("P-05R1 axis-order and client/manual-finalizer boundary regression: 23/23 PASS");
