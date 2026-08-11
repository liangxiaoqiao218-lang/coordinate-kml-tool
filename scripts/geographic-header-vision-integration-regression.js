import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";

import {
  parseGeographicHeaderVisionOutput,
  runGeographicHeaderVisionPass,
  shouldRunGeographicHeaderSupplementalProducer,
  shouldRunGeographicHeaderVisionPass,
  snapshotPreSuppressionCandidates
} from "../server/coordinate-evidence/index.js";
import { buildFinalizedCoordinateVerificationResponse } from "../server/verification/index.js";

function makeReviewPayload(context) {
  return {
    coordinateType: "wgs84_chat_coordinates",
    precisionMode: "wgs84-chat-review",
    confirmationStatus: "blocked",
    qualityGateStatus: "blocked",
    kml_ready: false,
    coordinateResult: {
      state: "BLOCKED_REVIEW"
    },
    _coordinateEvidenceContext: context
  };
}

function pickLegacyFields(response = {}) {
  return {
    coordinateType: response.coordinateType,
    precisionMode: response.precisionMode,
    confirmationStatus: response.confirmationStatus,
    qualityGateStatus: response.qualityGateStatus,
    kml_ready: response.kml_ready,
    coordinateResultState: response.coordinateResult?.state
  };
}

test("generic decimal rows without raw header are eligible for header vision pass", () => {
  const routing = shouldRunGeographicHeaderVisionPass({
    imageAvailable: true,
    rawText: "5.591667,-2.790556\n5.577222,-2.773889",
    coordinateRowCount: 2
  });

  assert.equal(routing.shouldRun, true);
  assert.equal(routing.reason, "coordinate_like_rows_without_header_semantic");
  assert.equal(routing.affectsLegacyWinner, false);
  assert.equal(routing.affectsCoordinateResult, false);
  assert.equal(routing.affectsKml, false);
});

test("raw header semantic suppresses duplicate header vision pass", () => {
  const routing = shouldRunGeographicHeaderVisionPass({
    imageAvailable: true,
    rawText: "POINTS\nLatitude N\nLongitude W\n5.591667,2.790556",
    coordinateRowCount: 1
  });

  assert.equal(routing.shouldRun, false);
  assert.equal(routing.reason, "geographic_header_semantic_already_detected");
});

test("high-authority evidence suppresses extra header vision pass", () => {
  const cadastral = shouldRunGeographicHeaderVisionPass({
    imageAvailable: true,
    rawText: "5.591667,-2.790556",
    coordinateRowCount: 1,
    cadastralGrid: {
      isCadastralGrid: true,
      rows: Array.from({ length: 32 }, (_, index) => ({ num: index + 1 }))
    }
  });
  const verifiedUtm = shouldRunGeographicHeaderVisionPass({
    imageAvailable: true,
    rawText: "540625 316",
    coordinateRowCount: 1,
    structuredUtmPriority: {
      accepted: true
    }
  });

  assert.equal(cadastral.shouldRun, false);
  assert.equal(cadastral.reason, "high_authority_evidence_already_present");
  assert.equal(verifiedUtm.shouldRun, false);
  assert.equal(verifiedUtm.reason, "high_authority_evidence_already_present");
});

test("header vision pass output can feed supplemental producer routing", async () => {
  const vision = await runGeographicHeaderVisionPass({
    imageItems: [{ type: "image_url", image_url: { url: "data:image/png;base64,stub", detail: "high" } }],
    invokeVision: async () => JSON.stringify({
      status: "observed",
      observations: [
        { field: "latitude_header", indicator: "N", source: "geographic_header_vision", region: "table_header" },
        { field: "longitude_header", indicator: "W", source: "geographic_header_vision", region: "table_header" }
      ]
    })
  });
  const routing = shouldRunGeographicHeaderSupplementalProducer({
    geographicHeaderSemantic: vision.semantic
  });

  assert.equal(vision.schemaVersion, "geographic_header_vision_v1");
  assert.equal(vision.status, "observed");
  assert.equal(vision.semantic.detected, true);
  assert.equal(routing.shouldRun, true);
  assert.ok(routing.reasons.includes("geographic_header_semantic"));
});

test("pre-decision context preserves sanitized header vision observations for debug", () => {
  const vision = parseGeographicHeaderVisionOutput({
    observations: [
      { field: "latitude_header", indicator: "N", region: "table_header" },
      { field: "longitude_header", indicator: "W", region: "table_header" }
    ]
  });
  const context = snapshotPreSuppressionCandidates({
    geographicHeaderVision: vision
  }, {
    reason: "main_route_pre_decision_snapshot"
  });
  const response = buildFinalizedCoordinateVerificationResponse(makeReviewPayload(context), null, {
    includeCoordinateEvidenceDebug: true
  });

  assert.equal(response.debugEvidenceContext.geographicHeaderVision.schemaVersion, "geographic_header_vision_v1");
  assert.equal(response.debugEvidenceContext.geographicHeaderVision.status, "observed");
  assert.equal(response.debugEvidenceContext.geographicHeaderVision.semantic.detected, true);
  assert.deepEqual(response.debugEvidenceContext.geographicHeaderVision.semantic.latitudeIndicators, ["N"]);
  assert.deepEqual(response.debugEvidenceContext.geographicHeaderVision.semantic.longitudeIndicators, ["W"]);
});

test("default response does not expose debug header vision context", () => {
  const context = snapshotPreSuppressionCandidates({
    geographicHeaderVision: parseGeographicHeaderVisionOutput("Latitude N\nLongitude W")
  });
  const response = buildFinalizedCoordinateVerificationResponse(makeReviewPayload(context));

  assert.equal("debugEvidenceContext" in response, false);
  assert.equal("_coordinateEvidenceContext" in response, false);
  assert.ok(response.coordinateEvidenceSummary);
});

test("debug exposure with header vision preserves legacy state fields", () => {
  const context = snapshotPreSuppressionCandidates({
    geographicHeaderVision: parseGeographicHeaderVisionOutput("Latitude South\nLongitude East")
  });
  const plain = buildFinalizedCoordinateVerificationResponse(makeReviewPayload(context));
  const debug = buildFinalizedCoordinateVerificationResponse(makeReviewPayload(context), null, {
    includeCoordinateEvidenceDebug: true
  });

  assert.deepEqual(pickLegacyFields(debug), pickLegacyFields(plain));
});

test("header vision integration keeps security and decision boundaries", () => {
  const malicious = parseGeographicHeaderVisionOutput({
    observations: [
      {
        field: "latitude_header",
        indicator: "N",
        rawText: "token:=abc Authorization: Bearer abc.def prompt:=read image modelResponse:=full answer",
        region: "table_header"
      },
      { field: "longitude_header", indicator: "W", region: "table_header" }
    ],
    prompt: "secret prompt",
    image: "base64payload",
    token: "hidden"
  });
  const serialized = JSON.stringify(malicious);
  const source = readFileSync(new URL("../server.js", import.meta.url), "utf8");

  assert.doesNotMatch(serialized, /abc\.def|secret prompt|base64payload|hidden|full answer|read image/i);
  assert.match(source, /runGeographicHeaderVisionPass/);
  assert.match(source, /shouldRunGeographicHeaderVisionPass/);
  assert.match(source, /geographicHeaderVisionRouting\.shouldRun/);
  assert.doesNotMatch(source, /geographicHeaderVisionRouting[\s\S]{0,500}arbitrateCoordinateType/);
  assert.doesNotMatch(source, /geographicHeaderVisionRouting[\s\S]{0,500}coordinateResult/);
  assert.doesNotMatch(source, /geographicHeaderVisionRouting[\s\S]{0,500}kml_ready/);
});
