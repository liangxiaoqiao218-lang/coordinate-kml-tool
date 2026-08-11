import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";
import {
  createPreDecisionEvidenceContext,
  runCadastralSemanticVisionPass,
  shouldRunCadastralSemanticVisionPass,
  snapshotPreSuppressionCandidates
} from "../server/coordinate-evidence/index.js";

const serverSource = fs.readFileSync(new URL("../server.js", import.meta.url), "utf8");

test("projected coordinate ambiguity routes to cadastral semantic vision when cadastral candidate is missing", () => {
  const routing = shouldRunCadastralSemanticVisionPass({
    imageItems: [{ type: "image_url", image_url: { url: "data:image/png;base64,abc" } }],
    rawText: "540625,316\n540625,315",
    coordinates: "540625,316\n540625,315",
    projectedCoordinateAmbiguity: true,
    cadastralGrid: { isCadastralGrid: false }
  });

  assert.equal(routing.shouldRun, true);
  assert.equal(routing.mode, "standard");
  assert.ok(routing.reasons.includes("cadastral_candidate_missing"));
  assert.ok(routing.reasons.includes("projected_coordinate_ambiguity"));
  assert.equal(routing.affectsLegacyWinner, false);
  assert.equal(routing.affectsCoordinateResult, false);
  assert.equal(routing.affectsKml, false);
});

test("existing cadastral grid skips extra semantic vision pass", () => {
  const routing = shouldRunCadastralSemanticVisionPass({
    imageItems: [{}],
    rawText: "num | XV | YV\n280 | 292812.5 | 360937.5",
    cadastralGrid: { isCadastralGrid: true, rowCount: 32 }
  });

  assert.equal(routing.shouldRun, false);
  assert.ok(routing.reasons.includes("cadastral_semantic_already_detected"));
  assert.ok(routing.reasons.includes("high_authority_evidence_already_present"));
});

test("verified UTM only skips cadastral semantic pass", () => {
  const routing = shouldRunCadastralSemanticVisionPass({
    imageItems: [{}],
    rawText: "654321,9876543",
    projectedCoordinateAmbiguity: true,
    structuredUtmPriority: { accepted: true }
  });

  assert.equal(routing.shouldRun, false);
  assert.equal(routing.mode, "standard");
  assert.equal(routing.acceptedUtmEvidence, true);
  assert.equal(routing.utmAcceptedCadastralAmbiguity, false);
  assert.ok(routing.reasons.includes("accepted_utm_evidence_present"));
});

test("UTM accepted with cadastral ambiguity allows shadow-only semantic observation", () => {
  const routing = shouldRunCadastralSemanticVisionPass({
    imageItems: [{}],
    rawText: "Liste Carres\n654321,9876543",
    coordinates: "654321,9876543",
    projectedCoordinateAmbiguity: true,
    structuredUtmPriority: { accepted: true },
    cadastralGrid: { isCadastralGrid: false }
  });

  assert.equal(routing.shouldRun, true);
  assert.equal(routing.mode, "shadow_observation_only");
  assert.equal(routing.acceptedUtmEvidence, true);
  assert.equal(routing.utmAcceptedCadastralAmbiguity, true);
  assert.ok(routing.reasons.includes("utm_accepted_but_cadastral_observation_allowed"));
  assert.ok(routing.reasons.includes("possible_cadastral_layout"));
  assert.equal(routing.affectsLegacyWinner, false);
  assert.equal(routing.affectsCoordinateResult, false);
  assert.equal(routing.affectsKml, false);
});

test("ordinary UTM CRS text without projected ambiguity does not route", () => {
  const routing = shouldRunCadastralSemanticVisionPass({
    imageItems: [{}],
    rawText: "UTM\nWGS84\nZone 50S",
    coordinates: ""
  });

  assert.equal(routing.shouldRun, false);
  assert.ok(routing.reasons.includes("no_cadastral_semantic_trigger"));
});

test("semantic vision pass uses injected vision invocation and returns normalized schema", async () => {
  const result = await runCadastralSemanticVisionPass({
    imageItems: [{}],
    invokeVision: async ({ prompt, imageItems }) => {
      assert.match(prompt, /Do not read, copy, or return coordinate rows/i);
      assert.equal(imageItems.length, 1);
      return JSON.stringify({
        status: "observed",
        tableType: "num_xv_yv",
        indicators: ["num", "XV", "YV"],
        layoutHints: {
          hasListeCarres: true,
          hasCadastralGrid: true,
          hasTableStructure: true
        }
      });
    }
  });

  assert.equal(result.schemaVersion, "cadastral_semantic_vision_v1");
  assert.equal(result.status, "observed");
  assert.equal(result.detected, true);
  assert.deepEqual(result.indicators, ["num", "XV", "YV"]);
});

test("pre-decision evidence context preserves sanitized cadastral semantic vision summary", () => {
  const context = snapshotPreSuppressionCandidates({
    cadastralGrid: { isCadastralGrid: false, rowCount: 0 },
    cadastralSemanticVision: {
      schemaVersion: "cadastral_semantic_vision_v1",
      status: "observed",
      detected: true,
      tableType: "num_xv_yv",
      indicators: ["num", "XV", "YV"],
      layoutHints: {
        hasListeCarres: true,
        hasCadastralGrid: true,
        hasTableStructure: true
      },
      confidence: "high",
      reason: "num_xv_yv_cadastral_table_visible",
      rawOcr: "num | XV | YV | 280 | 292812.5 | 360937.5",
      prompt: "read this image",
      token: "abc"
    }
  }, {
    reason: "regression"
  });
  const serialized = JSON.stringify(context);

  assert.equal(context.cadastralSemanticVision.detected, true);
  assert.deepEqual(context.cadastralSemanticVision.indicators, ["num", "XV", "YV"]);
  assert.doesNotMatch(serialized, /292812\.5|360937\.5|read this image|token|rawOcr|prompt/i);
});

test("context defaults remain safe when cadastral semantic vision is absent", () => {
  const context = createPreDecisionEvidenceContext();

  assert.equal(context.cadastralSemanticVision.schemaVersion, "cadastral_semantic_vision_v1");
  assert.equal(context.cadastralSemanticVision.status, "not_run");
  assert.equal(context.cadastralSemanticVision.detected, false);
  assert.equal(context.cadastralSemanticVision.affectsLegacyWinner, false);
  assert.equal(context.cadastralSemanticVision.affectsCoordinateResult, false);
  assert.equal(context.cadastralSemanticVision.affectsKml, false);
});

test("server integration stays in recognition acquisition layer and does not touch migration targets", () => {
  assert.match(serverSource, /stage:\s*"cadastral_semantic_vision"/);
  assert.match(serverSource, /stage:\s*"cadastral_semantic_table_read"/);
  assert.match(serverSource, /runCadastralSemanticVisionPass/);
  assert.match(serverSource, /CADASTRAL_SEMANTIC_VISION:semantic_observed/);
  assert.match(serverSource, /cadastralSemanticVision/);

  assert.doesNotMatch(serverSource, /cadastralSemanticVision[\s\S]{0,120}kml_ready\s*=\s*true/);
  assert.doesNotMatch(serverSource, /cadastralSemanticVision[\s\S]{0,120}coordinateType\s*=/);
});

test("server integration records shadow observation only mode without decision migration", () => {
  assert.match(serverSource, /CADASTRAL_SEMANTIC_VISION:shadow_observation_only/);
  assert.match(serverSource, /verifiedUtmTransformation:\s*Boolean\(\s*structuredUtmPriority\?\.accepted\s*&&\s*structuredUtmPriority\?\.transformationVerification\?\.status\s*===\s*"match"\s*\)/);
  assert.doesNotMatch(serverSource, /shadow_observation_only[\s\S]{0,200}kml_ready\s*=\s*true/);
  assert.doesNotMatch(serverSource, /shadow_observation_only[\s\S]{0,200}coordinateType\s*=/);
});
