import assert from "node:assert/strict";
import {
  AUTHORITY_CATEGORY,
  COORDINATE_EVIDENCE_CANDIDATE_SCHEMA_VERSION,
  COORDINATE_EVIDENCE_RECOMMENDED_STATE,
  createCoordinateEvidenceCandidate,
  rankCoordinateEvidenceCandidates,
  sanitizeCandidateForResponse,
  sanitizeShadowDecisionForResponse
} from "../server/coordinate-evidence/index.js";

function candidate(overrides = {}) {
  return createCoordinateEvidenceCandidate({
    evidenceType: "unknown_evidence",
    sourceParser: "regression",
    coordinateSource: "unknown_source",
    authority: {
      level: 0,
      category: AUTHORITY_CATEGORY.UNKNOWN,
      reason: "regression_default"
    },
    confidence: {
      level: "unknown",
      reason: "regression_default"
    },
    attributes: {
      geometryValid: true
    },
    coordinateSummary: {
      pointCount: 4,
      geometryType: "polygon",
      groupCount: 1
    },
    recommendedState: COORDINATE_EVIDENCE_RECOMMENDED_STATE.AUTO_EXPORT,
    canGenerateKml: true,
    reason: "regression_default",
    ...overrides
  });
}

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("schema creation keeps required fields", () => {
  const item = candidate({
    evidenceType: "dms_geographic",
    authority: {
      level: 5,
      category: AUTHORITY_CATEGORY.EXPLICIT_LEGAL_COORDINATE,
      reason: "explicit_dms_with_hemisphere"
    }
  });
  assert.equal(item.schemaVersion, COORDINATE_EVIDENCE_CANDIDATE_SCHEMA_VERSION);
  assert.match(item.evidenceId, /^ev_/);
  assert.equal(item.evidenceType, "dms_geographic");
  assert.equal(item.authority.level, 5);
  assert.equal(item.coordinateSummary.pointCount, 4);
});

test("authority and confidence remain separate", () => {
  const item = candidate({
    evidenceType: "utm_crs_text",
    authority: {
      level: 3,
      category: AUTHORITY_CATEGORY.CRS_CONTEXT,
      reason: "utm_crs_text"
    },
    confidence: {
      level: "high",
      reason: "clear_crs_label"
    }
  });
  assert.equal(item.authority.level, 3);
  assert.equal(item.confidence.level, "high");
  assert.notEqual(String(item.authority.level), item.confidence.level);
});

test("Madagascar shadow ranking prefers structured cadastral over UTM CRS text", () => {
  const structured = candidate({
    evidenceType: "structured_cadastral_table",
    sourceParser: "madagascar_cadastral_grid",
    coordinateSource: "num_xv_yv_table",
    authority: {
      level: 5,
      category: AUTHORITY_CATEGORY.EXPLICIT_LEGAL_COORDINATE,
      reason: "structured_cadastral_table"
    },
    confidence: {
      level: "high",
      reason: "valid_num_xv_yv_rows"
    },
    attributes: {
      hasStructuredTable: true,
      geometryValid: true
    },
    reason: "valid_num_xv_yv"
  });
  const crs = candidate({
    evidenceType: "utm_crs_text",
    sourceParser: "crs_evidence",
    coordinateSource: "map_frame_crs_label",
    authority: {
      level: 3,
      category: AUTHORITY_CATEGORY.CRS_CONTEXT,
      reason: "utm_crs_text"
    },
    confidence: {
      level: "high",
      reason: "clear_utm_wgs84_zone"
    }
  });
  const decision = rankCoordinateEvidenceCandidates([crs, structured], { coordinateType: "utm_projected_xy" });
  assert.equal(decision.winnerEvidenceType, "structured_cadastral_table");
  assert.equal(decision.reason, "higher_authority_evidence");
});

test("Cote d'Ivoire shadow ranking prefers explicit geographic over generic decimal", () => {
  const explicit = candidate({
    evidenceType: "explicit_geographic_dms",
    sourceParser: "cote_divoire_dms_parser",
    coordinateSource: "latitude_nord_longitude_ouest",
    authority: {
      level: 5,
      category: AUTHORITY_CATEGORY.EXPLICIT_LEGAL_COORDINATE,
      reason: "explicit_hemisphere_header"
    },
    confidence: {
      level: "high",
      reason: "ouest_nord_present"
    },
    attributes: {
      hasExplicitHemisphere: true,
      hasExplicitCoordinateOrder: true,
      geometryValid: true
    }
  });
  const decimal = candidate({
    evidenceType: "generic_decimal",
    sourceParser: "wgs84_chat_parser",
    coordinateSource: "naked_decimal_pair",
    authority: {
      level: 1,
      category: AUTHORITY_CATEGORY.WEAK_NUMERIC,
      reason: "generic_decimal"
    },
    confidence: {
      level: "medium",
      reason: "numbers_parse"
    }
  });
  const decision = rankCoordinateEvidenceCandidates([decimal, explicit], { coordinateType: "wgs84_chat_coordinates" });
  assert.equal(decision.winnerEvidenceType, "explicit_geographic_dms");
});

test("Indonesia03 shadow ranking prefers DMS geographic over UTM CRS hint", () => {
  const dms = candidate({
    evidenceType: "dms_geographic",
    sourceParser: "dms_parser",
    coordinateSource: "longitude_e_latitude_s",
    authority: {
      level: 5,
      category: AUTHORITY_CATEGORY.EXPLICIT_LEGAL_COORDINATE,
      reason: "explicit_dms_geographic"
    },
    confidence: {
      level: "high",
      reason: "explicit_e_s_hemisphere"
    },
    attributes: {
      hasExplicitHemisphere: true,
      hasExplicitCoordinateOrder: true,
      geometryValid: true
    }
  });
  const utmHint = candidate({
    evidenceType: "utm_crs_text",
    sourceParser: "crs_evidence",
    coordinateSource: "map_frame_crs_label",
    authority: {
      level: 3,
      category: AUTHORITY_CATEGORY.CRS_CONTEXT,
      reason: "utm_crs_hint"
    },
    confidence: {
      level: "high",
      reason: "clear_crs_label"
    }
  });
  const decision = rankCoordinateEvidenceCandidates([utmHint, dms], {
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y-review"
  });
  assert.equal(decision.winnerEvidenceType, "dms_geographic");
  assert.equal(decision.differenceFromCurrentWinner, true);
});

test("verified UTM ranks above UTM CRS text", () => {
  const verifiedUtm = candidate({
    evidenceType: "verified_utm_transformation",
    sourceParser: "structured_utm_table",
    coordinateSource: "structured_projected_rows",
    authority: {
      level: 4,
      category: AUTHORITY_CATEGORY.VERIFIED_TRANSFORMATION,
      reason: "verified_utm_transformation"
    },
    confidence: {
      level: "high",
      reason: "transform_match"
    },
    attributes: {
      crsEvidence: true,
      transformVerified: true,
      geometryValid: true
    }
  });
  const crs = candidate({
    evidenceType: "utm_crs_text",
    sourceParser: "crs_evidence",
    coordinateSource: "map_frame_crs_label",
    authority: {
      level: 3,
      category: AUTHORITY_CATEGORY.CRS_CONTEXT,
      reason: "utm_crs_text"
    },
    confidence: {
      level: "high",
      reason: "clear_crs_label"
    }
  });
  const decision = rankCoordinateEvidenceCandidates([crs, verifiedUtm], { coordinateType: "utm_projected_xy" });
  assert.equal(decision.winnerEvidenceType, "verified_utm_transformation");
});

test("sanitization removes secret markers and raw OCR/debug fields", () => {
  const dirty = candidate({
    evidenceType: "dms_geographic",
    reason: "api_key:=DASHSCOPE_SECRET token:=abc123",
    conflicts: [
      {
        reason: "Authorization: Bearer abc.def",
        rawText: "full raw OCR should be dropped",
        prompt: "prompt should be dropped"
      }
    ]
  });
  const exposed = sanitizeCandidateForResponse({
    ...dirty,
    rawText: "SECRET RAW OCR",
    prompt: "SECRET PROMPT",
    apiKey: "DASHSCOPE_API_KEY"
  });
  const serialized = JSON.stringify(exposed);
  assert.equal(serialized.includes("DASHSCOPE_SECRET"), false);
  assert.equal(serialized.includes("Bearer abc.def"), false);
  assert.equal(serialized.includes("raw OCR should be dropped"), false);
  assert.equal(serialized.includes("SECRET PROMPT"), false);
  assert.equal(serialized.includes("apiKey"), false);
});

test("shadow decision is explicitly non-mutating", () => {
  const item = candidate({
    evidenceType: "dms_geographic",
    authority: {
      level: 5,
      category: AUTHORITY_CATEGORY.EXPLICIT_LEGAL_COORDINATE,
      reason: "explicit_dms"
    }
  });
  const decision = rankCoordinateEvidenceCandidates([item], { coordinateType: "utm_projected_xy" });
  const exposed = sanitizeShadowDecisionForResponse(decision);
  assert.equal(exposed.affectsLegacyWinner, false);
  assert.equal(exposed.affectsCoordinateResult, false);
  assert.equal(exposed.affectsKml, false);
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
  console.log(`Coordinate Evidence Candidate Regression: ${passed}/${tests.length} PASS`);
}
