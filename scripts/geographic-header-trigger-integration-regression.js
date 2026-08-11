import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import {
  detectGeographicHeaderSemanticEvidence,
  shouldRunGeographicHeaderSupplementalProducer
} from "../server/coordinate-evidence/index.js";

test("generic Latitude N and Longitude W header allows supplemental producer routing", () => {
  const routing = shouldRunGeographicHeaderSupplementalProducer({
    rawText: `
POINTS
Latitude N
Longitude W
5.591638,2.790556
`
  });

  assert.equal(routing.shouldRun, true);
  assert.ok(routing.reasons.includes("geographic_header_semantic"));
  assert.ok(routing.reasons.includes("raw_text_geographic_header_semantic"));
  assert.equal(routing.source, "semantic");
  assert.equal(routing.affectsLegacyWinner, false);
  assert.equal(routing.affectsCoordinateResult, false);
  assert.equal(routing.affectsKml, false);
});

test("pure decimal rows do not allow supplemental producer routing", () => {
  const routing = shouldRunGeographicHeaderSupplementalProducer({
    rawText: `
5.591638,-2.790556
5.577222,-2.773889
`
  });

  assert.equal(routing.shouldRun, false);
  assert.deepEqual(routing.reasons, []);
  assert.equal(routing.source, "");
});

test("existing Cote d'Ivoire country filename cue remains a routing source", () => {
  const routing = shouldRunGeographicHeaderSupplementalProducer({
    countryCueDetected: true,
    countryCueSource: "filename",
    rawText: "5.591638,-2.790556"
  });

  assert.equal(routing.shouldRun, true);
  assert.deepEqual(routing.reasons, ["country_filename_cue"]);
  assert.equal(routing.source, "filename");
});

test("Latitude South and Longitude East routes independently of country name", () => {
  const routing = shouldRunGeographicHeaderSupplementalProducer({
    rawText: `
POINT
Latitude South
Longitude East
`
  });

  assert.equal(routing.shouldRun, true);
  assert.ok(routing.reasons.includes("geographic_header_semantic"));
  assert.equal(routing.geographicHeaderSemantic.countryIndependent, true);
  assert.deepEqual(routing.geographicHeaderSemantic.latitudeIndicators, ["S"]);
  assert.deepEqual(routing.geographicHeaderSemantic.longitudeIndicators, ["E"]);
});

test("precomputed geographic header semantic evidence can route without raw text", () => {
  const semantic = detectGeographicHeaderSemanticEvidence(`
POINTS | Latitude Nord | Longitude Ouest
`);
  const routing = shouldRunGeographicHeaderSupplementalProducer({
    geographicHeaderSemantic: semantic
  });

  assert.equal(routing.shouldRun, true);
  assert.deepEqual(routing.reasons, ["geographic_header_semantic"]);
});

test("routing helper does not expose raw OCR, prompt, model response, or credential markers", () => {
  const routing = shouldRunGeographicHeaderSupplementalProducer({
    rawText: `
POINTS | Latitude N | Longitude W
token:=abc
Authorization: Bearer abc.def
secret:=hidden
prompt:=read this image
modelResponse:=full answer
`
  });
  const serialized = JSON.stringify(routing);

  assert.equal(routing.shouldRun, true);
  assert.doesNotMatch(serialized, /abc\.def|hidden|read this image|full answer/i);
  assert.doesNotMatch(serialized, /Authorization|prompt|modelResponse/i);
});

test("server integration uses geographic header routing without touching final decision modules", () => {
  const source = readFileSync(new URL("../server.js", import.meta.url), "utf8");

  assert.match(source, /shouldRunGeographicHeaderSupplementalProducer/);
  assert.match(source, /geographicHeaderProducerRouting\.shouldRun/);
  assert.doesNotMatch(source, /geographicHeaderProducerRouting[\s\S]{0,400}arbitrateCoordinateType/);
  assert.doesNotMatch(source, /geographicHeaderProducerRouting[\s\S]{0,400}coordinateResult/);
  assert.doesNotMatch(source, /geographicHeaderProducerRouting[\s\S]{0,400}kml_ready/);
});

test("legacy state fields are not part of routing output", () => {
  const routing = shouldRunGeographicHeaderSupplementalProducer({
    rawText: "POINTS | Latitude N | Longitude W"
  });

  assert.equal("coordinateType" in routing, false);
  assert.equal("precisionMode" in routing, false);
  assert.equal("coordinateResult" in routing, false);
  assert.equal("kml_ready" in routing, false);
});
