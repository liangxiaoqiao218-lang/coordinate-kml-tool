import assert from "node:assert/strict";

import {
  createPreDecisionEvidenceContext,
  sanitizePreDecisionEvidenceContext,
  snapshotPreSuppressionCandidates
} from "../server/coordinate-evidence/index.js";
import { buildFinalizedCoordinateVerificationResponse } from "../server/verification/index.js";

function test(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    console.error(error);
    process.exitCode = 1;
  }
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

test("context schema starts with safe normalized defaults", () => {
  const context = createPreDecisionEvidenceContext();
  assert.equal(context.schemaVersion, "pre_decision_evidence_context_v1");
  assert.equal(context.dms.dmsAccepted, false);
  assert.equal(context.cadastral.isCadastralGrid, false);
  assert.equal(context.utm.crsEvidenceShadow, null);
  assert.deepEqual(context.suppression.suppressedFallbacks, []);
});

test("UTM suppression preserves pre-suppression DMS context", () => {
  const context = snapshotPreSuppressionCandidates({
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true,
    sourceHint: "Longitude E Latitude S",
    pointCount: 4,
    crsEvidenceShadow: {
      shadowIntent: {
        projection: "utm",
        datum: "WGS84",
        zone: 50,
        hemisphere: "south",
        confidence: "confirmed",
        conflicts: []
      }
    },
    explicitUtmEvidenceLock: true
  }, {
    utmEvidenceLockApplied: true,
    suppressedFallbacks: ["DMS", "WGS84_CHAT"],
    reason: "explicit_utm_evidence_lock"
  });

  assert.equal(context.dms.dmsAccepted, true);
  assert.equal(context.dms.hasExplicitHemisphere, true);
  assert.equal(context.dms.hasExplicitCoordinateOrder, true);
  assert.equal(context.utm.crsEvidenceShadow.shadowIntent.projection, "utm");
  assert.equal(context.utm.crsEvidenceShadow.shadowIntent.zone, 50);
  assert.equal(context.suppression.utmEvidenceLockApplied, true);
  assert.deepEqual(context.suppression.suppressedFallbacks, ["DMS", "WGS84_CHAT"]);
});

test("cadastral context preserves row summary without full coordinate rows", () => {
  const context = createPreDecisionEvidenceContext({
    cadastral: {
      isCadastralGrid: true,
      rowCount: 32,
      hasNumXvYvHeader: true,
      rows: [{ num: "280", xv: "292812.5", yv: "360937.5" }]
    }
  });

  assert.equal(context.cadastral.isCadastralGrid, true);
  assert.equal(context.cadastral.rowCount, 32);
  assert.equal(context.cadastral.hasNumXvYvHeader, true);
  assert.equal("rows" in context.cadastral, false);
});

test("Cote d'Ivoire header semantics are represented as short DMS source hint", () => {
  const context = createPreDecisionEvidenceContext({
    dms: {
      dmsAccepted: true,
      hasExplicitHemisphere: true,
      hasExplicitCoordinateOrder: true,
      sourceHint: "Latitude Nord / Longitude Ouest",
      pointCount: 4,
      geometryType: "polygon"
    }
  });

  assert.equal(context.dms.dmsAccepted, true);
  assert.equal(context.dms.sourceHint, "Latitude Nord / Longitude Ouest");
  assert.equal(context.dms.pointCount, 4);
  assert.equal(context.dms.geometryType, "polygon");
});

test("context creation does not affect finalized legacy response", () => {
  const payload = {
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y-review",
    coordinateArbitration: {
      coordinateType: "utm_projected_xy",
      precisionMode: "utm-projected-x-y-review",
      authority: "explicit_crs_evidence",
      requires_review: true,
      arbitrationEligible: false,
      confirmationStatus: "blocked",
      qualityGateStatus: "blocked",
      kml_allowed: false,
      kml_ready: false,
      lat_lon_role: "verification_only",
      reason: "explicit_utm_crs_without_validated_structured_xy",
      blockedFallbacks: ["dms"]
    }
  };
  const before = buildFinalizedCoordinateVerificationResponse(payload);
  const context = snapshotPreSuppressionCandidates({
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true
  }, {
    utmEvidenceLockApplied: true,
    suppressedFallbacks: ["DMS"]
  });
  const after = buildFinalizedCoordinateVerificationResponse({
    ...payload,
    _coordinateEvidenceContext: context
  });

  assert.deepEqual(pickLegacyFields(after), pickLegacyFields(before));
  assert.equal("_coordinateEvidenceContext" in after, false);
});

test("sanitization strips raw OCR, prompt, model response, and credential-like values", () => {
  const context = sanitizePreDecisionEvidenceContext({
    dms: {
      dmsAccepted: true,
      sourceHint: "Authorization: Bearer abc.def token:=abc123",
      rawText: "raw OCR should disappear",
      prompt: "secret prompt should disappear",
      modelResponse: "full response should disappear"
    },
    apiKey: "DASHSCOPE_SECRET",
    imageBuffer: "base64-image-data"
  });
  const serialized = JSON.stringify(context);

  assert.equal(serialized.includes("Bearer abc.def"), false);
  assert.equal(serialized.includes("raw OCR should disappear"), false);
  assert.equal(serialized.includes("secret prompt should disappear"), false);
  assert.equal(serialized.includes("full response should disappear"), false);
  assert.equal(serialized.includes("DASHSCOPE_SECRET"), false);
  assert.equal(serialized.includes("base64-image-data"), false);
});

test("structured UTM context keeps only sanitized CRS and transformation status", () => {
  const context = createPreDecisionEvidenceContext({
    utm: {
      crsEvidenceShadow: {
        shadowIntent: {
          projection: "utm",
          datum: "WGS84",
          zone: 50,
          hemisphere: "south",
          confidence: "confirmed",
          conflicts: [{ field: "zone", message: "none" }]
        }
      },
      structuredUtmTable: {
        accepted: true,
        rowCount: 4,
        transformationVerification: {
          status: "match",
          rows: [{ x: 540625, y: 316 }]
        }
      },
      explicitUtmEvidenceLock: true
    }
  });

  assert.equal(context.utm.crsEvidenceShadow.shadowIntent.projection, "utm");
  assert.equal(context.utm.structuredUtmTable.accepted, true);
  assert.equal(context.utm.structuredUtmTable.rowCount, 4);
  assert.equal(context.utm.structuredUtmTable.transformationStatus, "match");
  assert.equal("rows" in context.utm.structuredUtmTable, false);
});

if (process.exitCode !== 1) {
  console.log("Coordinate Evidence Context Regression: 7/7 PASS");
}
