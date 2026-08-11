import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import {
  GEOGRAPHIC_HEADER_SEMANTIC_SCHEMA_VERSION,
  detectGeographicHeaderSemanticEvidence
} from "../server/coordinate-evidence/index.js";

test("Latitude N and Longitude W header emits geographic header semantic evidence", () => {
  const evidence = detectGeographicHeaderSemanticEvidence(`
POINTS
Latitude N
Longitude W

1 5.591638 2.790556
2 5.577222 2.773889
`);

  assert.equal(evidence.schemaVersion, GEOGRAPHIC_HEADER_SEMANTIC_SCHEMA_VERSION);
  assert.equal(evidence.evidenceType, "geographic_header_semantic");
  assert.equal(evidence.detected, true);
  assert.equal(evidence.hasLatitudeHeader, true);
  assert.equal(evidence.hasLongitudeHeader, true);
  assert.equal(evidence.hasHemisphereIndicator, true);
  assert.deepEqual(evidence.latitudeIndicators, ["N"]);
  assert.deepEqual(evidence.longitudeIndicators, ["W"]);
  assert.equal(evidence.confidence.level, "high");
});

test("Latitude South and Longitude East are country independent", () => {
  const evidence = detectGeographicHeaderSemanticEvidence(`
POINT
Latitude South
Longitude East
`);

  assert.equal(evidence.detected, true);
  assert.deepEqual(evidence.latitudeIndicators, ["S"]);
  assert.deepEqual(evidence.longitudeIndicators, ["E"]);
  assert.equal(evidence.countryIndependent, true);
  assert.equal(evidence.reason, "latitude_longitude_header_with_hemisphere");
});

test("French Nord and Ouest headers are detected without country name", () => {
  const evidence = detectGeographicHeaderSemanticEvidence(`
POINTS | Latitude Nord | Longitude Ouest
1 | 05°35'29,00"N | 2°47'26,00"W
`);

  assert.equal(evidence.detected, true);
  assert.deepEqual(evidence.latitudeIndicators, ["N"]);
  assert.deepEqual(evidence.longitudeIndicators, ["W"]);
  assert.equal(evidence.coordinateOrder, "latitude_longitude");
});

test("decimal pairs without semantic headers do not emit trigger evidence", () => {
  const evidence = detectGeographicHeaderSemanticEvidence(`
5.591638,-2.790556
5.577222,-2.773889
`);

  assert.equal(evidence.detected, false);
  assert.equal(evidence.hasLatitudeHeader, false);
  assert.equal(evidence.hasLongitudeHeader, false);
  assert.equal(evidence.hasHemisphereIndicator, false);
});

test("generic Longitude W then Latitude N order is represented without country dependency", () => {
  const evidence = detectGeographicHeaderSemanticEvidence(`
No | Longitude W | Latitude N
A | 2°47'26"W | 05°35'29"N
`);

  assert.equal(evidence.detected, true);
  assert.deepEqual(evidence.latitudeIndicators, ["N"]);
  assert.deepEqual(evidence.longitudeIndicators, ["W"]);
  assert.equal(evidence.coordinateOrder, "longitude_latitude");
});

test("country filename alone is not treated as geographic header semantic evidence", () => {
  const evidence = detectGeographicHeaderSemanticEvidence("科特迪瓦03.png");

  assert.equal(evidence.detected, false);
  assert.equal(evidence.hasLatitudeHeader, false);
  assert.equal(evidence.hasLongitudeHeader, false);
});

test("existing Cote d'Ivoire trigger source remains present and untouched", () => {
  const source = readFileSync(new URL("../server.js", import.meta.url), "utf8");

  assert.match(source, /function\s+hasCoteDIvoireGeographicDmsCue/);
  assert.match(source, /科特迪瓦/);
  assert.match(source, /latitude\\s\*\(\?:n\|nord\)/);
  assert.match(source, /longitude\\s\*\(\?:w\|ouest\)/);
});

test("helper output does not expose raw OCR, prompts, model responses, or credentials", () => {
  const evidence = detectGeographicHeaderSemanticEvidence({
    text: `
POINTS | Latitude N | Longitude W
token:=abc
Authorization: Bearer abc.def
secret:=hidden
prompt:=read this image
modelResponse:=full raw answer
`
  });
  const serialized = JSON.stringify(evidence);

  assert.equal(evidence.detected, true);
  assert.doesNotMatch(serialized, /abc\.def|hidden|read this image|full raw answer/i);
  assert.doesNotMatch(serialized, /raw OCR|prompt|modelResponse|Authorization/i);
});
