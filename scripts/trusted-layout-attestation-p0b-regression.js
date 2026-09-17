import assert from "node:assert/strict";
import {
  createCoordinateImageIdentity,
  isCanonicalCoordinateImageIdentity
} from "../server/recognition/coordinate-image-safety.js";
import {
  SERVER_PROVENANCE_ATTESTATION,
  buildEvidenceAcquisition,
  createServerClassifiedLayoutRows,
  createTrustedLayoutAttestation,
  extractProviderLayoutCandidates,
  validateTrustedLayoutAttestation
} from "../server/evidence-acquisition/index.js";
import { evaluateWgs84NearDuplicateConsolidation } from "../server/recognition/wgs84-near-duplicate-consolidation.js";

const low = "35.447819,83.178991";
const high = "35.4478191,83.1789913";
const tests = [];

function test(id, name, run) {
  tests.push({ id, name, run });
}

function makeBmp(width = 100, height = 100) {
  const rowBytes = Math.floor(((24 * width) + 31) / 32) * 4;
  const pixelBytes = rowBytes * height;
  const buffer = Buffer.alloc(54 + pixelBytes);
  buffer.write("BM", 0, "ascii");
  buffer.writeUInt32LE(buffer.length, 2);
  buffer.writeUInt32LE(54, 10);
  buffer.writeUInt32LE(40, 14);
  buffer.writeInt32LE(width, 18);
  buffer.writeInt32LE(height, 22);
  buffer.writeUInt16LE(1, 26);
  buffer.writeUInt16LE(24, 28);
  buffer.writeUInt32LE(0, 30);
  buffer.writeUInt32LE(pixelBytes, 34);
  return buffer;
}

const file = { buffer: makeBmp(), mimetype: "image/bmp", size: 30054 };
const imageIdentity = createCoordinateImageIdentity(file, { requestId: "synthetic-layout-request", page: 1 });
const engine = Object.freeze({
  coordinate_type: "wgs84_chat_coordinates",
  precision_mode: "wgs84-chat-coordinates",
  groups: [{
    group_id: "group_1",
    points: [
      { label: "1", raw: low, lat: 35.447819, lon: 83.178991 },
      { label: "2", raw: high, lat: 35.4478191, lon: 83.1789913 }
    ]
  }]
});

function serverRow(value) {
  const row = {
    coordinate_space: "ORIGINAL_IMAGE_PIXELS",
    source_type: "SYNTHETIC_REGRESSION_V1",
    provenance_trust: "SERVER_ATTESTED",
    ...value
  };
  Object.defineProperty(row, SERVER_PROVENANCE_ATTESTATION, { value: true });
  return row;
}

function rows(overrides = {}) {
  const values = [
    serverRow({
      text: low,
      bbox: [5, 5, 95, 25],
      source: "syntheticLayout",
      source_ref: "search-box-coordinate",
      source_line_id: "line-search",
      source_role: "MAP_SEARCH_BOX",
      source_region_id: "MAP_SEARCH_BOX_REGION",
      group_id: "group_1",
      point_id: "1"
    }),
    serverRow({
      text: high,
      bbox: [5, 55, 95, 75],
      source: "syntheticLayout",
      source_ref: "place-details-coordinate",
      source_line_id: "line-details",
      source_role: "MAP_PLACE_DETAILS",
      source_region_id: "MAP_PLACE_DETAILS_REGION",
      group_id: "group_1",
      point_id: "2"
    })
  ];
  for (const [index, patch] of Object.entries(overrides)) {
    values[Number(index)] = serverRow({ ...values[Number(index)], ...patch });
  }
  return values;
}

function create(overrides = {}) {
  return createTrustedLayoutAttestation({
    imageIdentity: overrides.imageIdentity || imageIdentity,
    observations: overrides.observations || rows(),
    coordinateEngineV2: overrides.coordinateEngineV2 || engine,
    resultRevision: overrides.resultRevision || 1,
    providerResponseId: "synthetic-provider-response"
  });
}

test("TLA-01", "canonical image identity binds bytes, dimensions and request asset", () => {
  assert.equal(isCanonicalCoordinateImageIdentity(imageIdentity), true);
  assert.equal(imageIdentity.width, 100);
  assert.equal(imageIdentity.height, 100);
  assert.match(imageIdentity.image_sha256, /^[0-9a-f]{64}$/);
  assert.match(imageIdentity.request_asset_id, /^asset_[0-9a-f]{64}$/);
  assert.equal("buffer" in imageIdentity, false);
});

test("TLA-02", "trusted two-region layout creates exact attestation and bindings", () => {
  const attestation = create();
  assert.ok(attestation);
  const result = validateTrustedLayoutAttestation({ attestation, imageIdentity, coordinateEngineV2: engine, resultRevision: 1 });
  assert.equal(result.valid, true);
  assert.equal(attestation.observations.length, 2);
  assert.equal(attestation.row_bindings.every(binding => binding.score_calibrated === true), true);
});

test("TLA-03", "ordinary JSON cannot mint trusted layout capability", () => {
  const forged = JSON.parse(JSON.stringify(create()));
  const result = validateTrustedLayoutAttestation({ attestation: forged, imageIdentity, coordinateEngineV2: engine, resultRevision: 1 });
  assert.equal(result.valid, false);
  assert.equal(result.reason, "TRUSTED_LAYOUT_CAPABILITY_MISSING");
});

test("TLA-04", "provider self-asserted provenance without in-process capability is rejected", () => {
  const untrusted = rows();
  const plain = { ...untrusted[0], SERVER_PROVENANCE_ATTESTATION: true };
  assert.equal(create({ observations: [plain, untrusted[1]] }), null);
});

test("TLA-05", "canonical image mismatch invalidates attestation", () => {
  const otherIdentity = createCoordinateImageIdentity(
    { buffer: makeBmp(101, 100), mimetype: "image/bmp" },
    { requestId: "other-request", page: 1 }
  );
  const result = validateTrustedLayoutAttestation({
    attestation: create(),
    imageIdentity: otherIdentity,
    coordinateEngineV2: engine,
    resultRevision: 1
  });
  assert.equal(result.valid, false);
  assert.equal(result.reason, "TRUSTED_LAYOUT_IMAGE_OR_REVISION_MISMATCH");
});

test("TLA-06", "out-of-bounds bbox fails closed", () => {
  assert.equal(create({ observations: rows({ 1: { bbox: [5, 55, 105, 75] } }) }), null);
});

test("TLA-07", "overlapping source regions fail closed", () => {
  assert.equal(create({ observations: rows({ 1: { bbox: [5, 20, 95, 60] } }) }), null);
});

test("TLA-08", "same source line fails closed", () => {
  assert.equal(create({ observations: rows({ 1: { source_line_id: "line-search" } }) }), null);
});

test("TLA-09", "duplicate source role fails closed", () => {
  assert.equal(create({ observations: rows({
    1: { source_role: "MAP_SEARCH_BOX", source_region_id: "MAP_SEARCH_BOX_REGION" }
  }) }), null);
});

test("TLA-10", "candidate text or point binding ambiguity fails closed", () => {
  assert.equal(create({ observations: rows({ 1: { text: low } }) }), null);
});

test("TLA-11", "stale result revision fails closed", () => {
  const result = validateTrustedLayoutAttestation({
    attestation: create(),
    imageIdentity,
    coordinateEngineV2: engine,
    resultRevision: 2
  });
  assert.equal(result.valid, false);
  assert.equal(result.reason, "TRUSTED_LAYOUT_IMAGE_OR_REVISION_MISMATCH");
});

test("TLA-12", "provider self-described roles stay untrusted until independently server-classified", () => {
  const candidates = extractProviderLayoutCandidates({
    id: "provider-response-1",
    output: { layout: [
      {
        text: low,
        bbox: [5, 5, 95, 25],
        id: "provider-search",
        line_id: "provider-line-search",
        source_role: "MAP_SEARCH_BOX"
      },
      {
        text: high,
        bbox: [5, 55, 95, 75],
        id: "provider-details",
        line_id: "provider-line-details",
        source_role: "MAP_PLACE_DETAILS"
      }
    ] }
  });
  assert.equal(candidates.length, 2);
  assert.equal(candidates.every(candidate => candidate.provenance_trust === "UNTRUSTED"), true);
  assert.equal(candidates.every(candidate => candidate.source_role === null && candidate.source_region_id === null), true,
    "provider role assertions must be discarded during extraction");
  assert.equal(createTrustedLayoutAttestation({
    imageIdentity,
    observations: candidates,
    coordinateEngineV2: engine,
    resultRevision: 1,
    providerResponseId: "provider-response-1"
  }), null, "response identity cannot upgrade provider self-described roles");
  const serverClassified = createServerClassifiedLayoutRows({
    candidates,
    classifications: [
      { source_ref: "provider-search", source_role: "MAP_SEARCH_BOX" },
      { source_ref: "provider-details", source_role: "MAP_PLACE_DETAILS" }
    ]
  });
  assert.ok(serverClassified, "independent server classification produces an in-process capability");
  assert.equal(createTrustedLayoutAttestation({
    imageIdentity,
    observations: serverClassified,
    coordinateEngineV2: engine,
    resultRevision: 1,
    providerResponseId: ""
  }), null, "server-classified Provider rows still require a bound Provider response identity");
  assert.ok(create({ observations: serverClassified }), "server-classified rows can be attested after all bindings validate");
  const plainClassified = structuredClone(serverClassified);
  assert.equal(create({ observations: plainClassified }), null,
    "ordinary JSON cannot preserve the server layout classification capability");
});

test("TLA-13", "trusted layout replaces heuristic bindings without changing shadow-only contract", () => {
  const attestation = create();
  const evidence = buildEvidenceAcquisition({
    recognitionResult: { imageMetadata: imageIdentity, trustedLayoutAttestation: attestation, rawText: `${low}\n${high}` },
    coordinateEngineV2: engine,
    resultRevision: 1
  });
  assert.equal(evidence.trusted_layout_status, "ATTESTED");
  assert.equal(evidence.rowBindings.every(binding => binding.binding_authority === "SERVER_ATTESTED"), true);
  assert.equal(evidence.shadow_only, true);
  assert.equal(evidence.affects_coordinates, false);
  assert.equal(evidence.affects_kml, false);
});

test("TLA-14", "layout proof alone cannot authorize geometry intent, Map or KML", () => {
  const attestation = create();
  const recognitionResult = { imageMetadata: imageIdentity, trustedLayoutAttestation: attestation, rawText: `${low}\n${high}` };
  const evidenceAcquisition = buildEvidenceAcquisition({ recognitionResult, coordinateEngineV2: engine, resultRevision: 1 });
  const result = evaluateWgs84NearDuplicateConsolidation({
    coordinateEngineV2: engine,
    evidenceAcquisition,
    recognitionResult,
    revision: 1
  });
  assert.equal(result.decision.decision, "SAME_LOCATION_CONFIRMED");
  assert.equal(result.geometryIntentGate.decision, "BLOCKED");
  assert.equal(result.authorityBlocked, true);
});

test("TLA-15", "three-candidate clusters cannot receive a two-region attestation", () => {
  const expanded = {
    ...engine,
    groups: [{
      ...engine.groups[0],
      points: [...engine.groups[0].points, { label: "3", raw: "35.44781912,83.17899131", lat: 35.44781912, lon: 83.17899131 }]
    }]
  };
  assert.equal(create({ coordinateEngineV2: expanded }), null);
});

test("TLA-16", "swapped or modified candidate coordinates cannot reuse the attestation", () => {
  const swapped = {
    ...engine,
    groups: [{
      ...engine.groups[0],
      points: engine.groups[0].points.map(point => ({ ...point, lat: point.lon, lon: point.lat }))
    }]
  };
  const recognitionResult = { imageMetadata: imageIdentity, trustedLayoutAttestation: create(), rawText: `${low}\n${high}` };
  const evidenceAcquisition = buildEvidenceAcquisition({
    recognitionResult,
    coordinateEngineV2: swapped,
    resultRevision: 1
  });
  const result = evaluateWgs84NearDuplicateConsolidation({
    coordinateEngineV2: swapped,
    evidenceAcquisition,
    recognitionResult,
    revision: 1
  });
  assert.equal(result.decision.decision, "PROVENANCE_INSUFFICIENT");
  assert.equal(result.decision.reason_codes.includes("OBSERVATION_VALUE_BINDING_INVALID"), true);
});

let passed = 0;
for (const item of tests) {
  try {
    item.run();
    passed += 1;
    console.log(`PASS ${item.id} ${item.name}`);
  } catch (error) {
    console.error(`FAIL ${item.id} ${item.name}`);
    throw error;
  }
}

console.log(JSON.stringify({
  suite: "trusted-layout-attestation-p0b-regression",
  incident: "GJ-COORD-MAP-SCREENSHOT-DUPLICATE-001",
  passed,
  total: tests.length,
  providerCalls: 0,
  externalNetworkCalls: 0,
  databaseOrUsageWrites: 0,
  goldenStatus: "GOLDEN_CANDIDATE / BLOCKED_BY_SOURCE_IMAGE_ATTESTATION"
}, null, 2));
