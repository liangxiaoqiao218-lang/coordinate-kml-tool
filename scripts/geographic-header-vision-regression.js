import assert from "node:assert/strict";
import test from "node:test";
import {
  GEOGRAPHIC_HEADER_VISION_SCHEMA_VERSION,
  parseGeographicHeaderVisionOutput
} from "../server/coordinate-evidence/index.js";

test("North and West header vision output normalizes to N/W semantic evidence", () => {
  const result = parseGeographicHeaderVisionOutput({
    "Latitude header detected": "North",
    "Longitude header detected": "West"
  });

  assert.equal(result.schemaVersion, GEOGRAPHIC_HEADER_VISION_SCHEMA_VERSION);
  assert.equal(result.status, "observed");
  assert.equal(result.semantic.detected, true);
  assert.deepEqual(result.semantic.latitudeIndicators, ["N"]);
  assert.deepEqual(result.semantic.longitudeIndicators, ["W"]);
  assert.equal(result.semantic.confidence, "medium");
});

test("French Nord and Ouest header vision output is detected", () => {
  const result = parseGeographicHeaderVisionOutput(`
{
  "observations": [
    { "field": "latitude_header", "indicator": "Nord", "region": "table_header" },
    { "field": "longitude_header", "indicator": "Ouest", "region": "table_header" }
  ]
}
`);

  assert.equal(result.status, "observed");
  assert.equal(result.observations.length, 2);
  assert.deepEqual(result.semantic.latitudeIndicators, ["N"]);
  assert.deepEqual(result.semantic.longitudeIndicators, ["W"]);
});

test("South and East header vision output normalizes to S/E", () => {
  const result = parseGeographicHeaderVisionOutput("Latitude South\nLongitude East");

  assert.equal(result.status, "observed");
  assert.deepEqual(result.semantic.latitudeIndicators, ["S"]);
  assert.deepEqual(result.semantic.longitudeIndicators, ["E"]);
});

test("random text is not detected as a geographic header", () => {
  const result = parseGeographicHeaderVisionOutput("random text");

  assert.equal(result.status, "not_detected");
  assert.equal(result.semantic.detected, false);
  assert.deepEqual(result.observations, []);
});

test("coordinate rows only are not parsed as header semantic", () => {
  const result = parseGeographicHeaderVisionOutput("5.591667,-2.790556\n5.577222,-2.773889");

  assert.equal(result.status, "not_detected");
  assert.equal(result.semantic.detected, false);
  assert.deepEqual(result.observations, []);
});

test("parser does not expose raw OCR, prompts, model responses, credentials, or image data", () => {
  const result = parseGeographicHeaderVisionOutput({
    "Latitude header detected": "North",
    "Longitude header detected": "West",
    rawOcr: "POINTS Latitude N Longitude W",
    prompt: "read this image",
    modelResponse: "full response text",
    token: "abc",
    Authorization: "Bearer abc.def",
    image: "base64data"
  });
  const serialized = JSON.stringify(result);

  assert.equal(result.status, "observed");
  assert.doesNotMatch(serialized, /POINTS Latitude N Longitude W|read this image|full response text|Bearer abc\.def|base64data/i);
  assert.doesNotMatch(serialized, /rawOcr|prompt|modelResponse|Authorization|token|image/i);
});

test("vision parser output remains shadow-only and does not include legacy state fields", () => {
  const result = parseGeographicHeaderVisionOutput("Latitude North\nLongitude West");

  assert.equal(result.affectsLegacyWinner, false);
  assert.equal(result.affectsCoordinateResult, false);
  assert.equal(result.affectsKml, false);
  assert.equal("coordinateType" in result, false);
  assert.equal("precisionMode" in result, false);
  assert.equal("coordinateResult" in result, false);
  assert.equal("kml_ready" in result, false);
});
