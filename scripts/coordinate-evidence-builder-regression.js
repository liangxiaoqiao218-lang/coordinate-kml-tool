import assert from "node:assert/strict";
import {
  buildCoordinateEvidenceCandidates,
  buildCoordinateEvidenceShadowModel,
  buildDmsGeographicEvidenceCandidate,
  buildStructuredCadastralEvidenceCandidate,
  buildUtmEvidenceCandidates,
  buildVerifiedUtmTransformationEvidenceCandidate,
  sanitizeCandidateForResponse
} from "../server/coordinate-evidence/index.js";

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function findCandidate(candidates, evidenceType) {
  return candidates.find(candidate => candidate.evidenceType === evidenceType);
}

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("DMS explicit hemisphere candidate uses authority level 5", () => {
  const candidate = buildDmsGeographicEvidenceCandidate({
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true,
    pointCount: 4,
    geometryType: "polygon",
    reason: "Longitude E Latitude S"
  });
  assert.equal(candidate.evidenceType, "dms_geographic");
  assert.equal(candidate.authority.level, 5);
  assert.equal(candidate.confidence.level, "high");
  assert.equal(candidate.attributes.hasExplicitHemisphere, true);
  assert.equal(candidate.recommendedState, "AUTO_EXPORT");
});

test("Handwritten DMS keeps high authority but medium confidence", () => {
  const candidate = buildDmsGeographicEvidenceCandidate({
    handwrittenDms: {
      isHandwrittenDms: true,
      pointRows: 4
    },
    precisionMode: "handwritten-dms-coordinates",
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true
  });
  assert.equal(candidate.evidenceType, "dms_geographic");
  assert.equal(candidate.sourceParser, "handwritten_dms_parser");
  assert.equal(candidate.authority.level, 5);
  assert.equal(candidate.confidence.level, "medium");
  assert.equal(candidate.recommendedState, "CONFIRM_REQUIRED");
});

test("UTM CRS text creates level 3 CRS context candidate", () => {
  const candidates = buildUtmEvidenceCandidates({
    crsEvidenceShadow: {
      shadowIntent: {
        projection: "utm",
        datum: "WGS84",
        zone: 50,
        hemisphere: "south",
        confidence: "confirmed",
        conflicts: []
      }
    }
  });
  const crs = findCandidate(candidates, "utm_crs_text");
  assert.ok(crs);
  assert.equal(crs.authority.level, 3);
  assert.equal(crs.authority.category, "crs_context");
  assert.equal(crs.confidence.level, "high");
  assert.equal(findCandidate(candidates, "verified_utm_transformation"), undefined);
});

test("Verified UTM requires accepted rows and transform match", () => {
  const valid = buildVerifiedUtmTransformationEvidenceCandidate({
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
    structuredUtmPriority: {
      accepted: true,
      table: {
        rows: [{}, {}, {}, {}]
      },
      transformationVerification: {
        status: "match"
      }
    }
  });
  assert.equal(valid.evidenceType, "verified_utm_transformation");
  assert.equal(valid.authority.level, 4);
  assert.equal(valid.attributes.transformVerified, true);

  const invalid = buildVerifiedUtmTransformationEvidenceCandidate({
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
    structuredUtmPriority: {
      accepted: true,
      table: {
        rows: [{}, {}, {}, {}]
      },
      transformationVerification: {
        status: "not_available"
      }
    }
  });
  assert.equal(invalid, null);
});

test("Structured cadastral candidate uses authority level 5", () => {
  const candidate = buildStructuredCadastralEvidenceCandidate({
    cadastralGrid: {
      isCadastralGrid: true,
      rows: [{}, {}, {}, {}],
      rowCount: 4
    }
  });
  assert.equal(candidate.evidenceType, "structured_cadastral_table");
  assert.equal(candidate.authority.level, 5);
  assert.equal(candidate.confidence.level, "high");
  assert.equal(candidate.canGenerateKml, false);
});

test("Madagascar builder shadow ranks cadastral above UTM CRS text", () => {
  const { coordinateEvidenceCandidates, shadowEvidenceDecision } = buildCoordinateEvidenceShadowModel({
    cadastralGrid: {
      isCadastralGrid: true,
      rows: [{}, {}, {}, {}],
      rowCount: 4
    },
    crsEvidenceShadow: {
      shadowIntent: {
        projection: "utm",
        datum: "WGS84",
        zone: 50,
        hemisphere: "south",
        confidence: "confirmed",
        conflicts: []
      }
    }
  }, {
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y-review"
  });
  assert.ok(findCandidate(coordinateEvidenceCandidates, "structured_cadastral_table"));
  assert.ok(findCandidate(coordinateEvidenceCandidates, "utm_crs_text"));
  assert.equal(shadowEvidenceDecision.winnerEvidenceType, "structured_cadastral_table");
});

test("Cote d'Ivoire builder emits explicit geographic DMS candidate", () => {
  const candidates = buildCoordinateEvidenceCandidates({
    coordinateEngineV2: {
      coordinate_type: "cote_divoire_geographic_dms_table",
      precision_mode: "cote-divoire-geographic-dms-table",
      requires_review: false,
      groups: [{
        geometry: "polygon",
        points: [{}, {}, {}, {}]
      }]
    },
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true,
    sourceHint: "Longitude Ouest Latitude Nord"
  });
  const dms = findCandidate(candidates, "explicit_geographic_dms");
  assert.ok(dms);
  assert.equal(dms.authority.level, 5);
  assert.equal(dms.confidence.level, "high");
});

test("Indonesia03 builder shadow ranks DMS above UTM CRS hint", () => {
  const { coordinateEvidenceCandidates, shadowEvidenceDecision } = buildCoordinateEvidenceShadowModel({
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true,
    pointCount: 4,
    reason: "Longitude E Latitude S",
    crsEvidenceShadow: {
      shadowIntent: {
        projection: "utm",
        datum: "WGS84",
        zone: 50,
        hemisphere: "south",
        confidence: "confirmed",
        conflicts: []
      }
    }
  }, {
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y-review"
  });
  assert.ok(findCandidate(coordinateEvidenceCandidates, "dms_geographic"));
  assert.ok(findCandidate(coordinateEvidenceCandidates, "utm_crs_text"));
  assert.equal(shadowEvidenceDecision.winnerEvidenceType, "dms_geographic");
  assert.equal(shadowEvidenceDecision.differenceFromCurrentWinner, true);
});

test("Builder aggregation does not mutate input payload", () => {
  const payload = {
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true,
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
    }
  };
  const before = clone(payload);
  buildCoordinateEvidenceShadowModel(payload, { coordinateType: "utm_projected_xy" });
  assert.deepEqual(payload, before);
});

test("Sanitized builder output excludes sensitive raw/debug fields", () => {
  const dms = buildDmsGeographicEvidenceCandidate({
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    hasExplicitCoordinateOrder: true,
    reason: "token:=abc123 Authorization: Bearer abc.def",
    rawText: "raw OCR should not survive",
    prompt: "secret prompt"
  });
  const exposed = sanitizeCandidateForResponse({
    ...dms,
    rawText: "raw OCR should not survive",
    prompt: "secret prompt",
    apiKey: "DASHSCOPE_SECRET"
  });
  const serialized = JSON.stringify(exposed);
  assert.equal(serialized.includes("Bearer abc.def"), false);
  assert.equal(serialized.includes("raw OCR should not survive"), false);
  assert.equal(serialized.includes("secret prompt"), false);
  assert.equal(serialized.includes("DASHSCOPE_SECRET"), false);
});

let passed = 0;
for (const { name, fn } of tests) {
  try {
    fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    console.error(error);
    process.exitCode = 1;
    break;
  }
}

if (process.exitCode !== 1) {
  console.log(`Coordinate Evidence Builder Regression: ${passed}/${tests.length} PASS`);
}
