import assert from "node:assert/strict";
import {
  AUTHORITY_CATEGORY,
  COORDINATE_EVIDENCE_RECOMMENDED_STATE,
  SHADOW_OBSERVATION_CATEGORY,
  SHADOW_OBSERVATION_CLASSIFICATION,
  buildCoordinateEvidenceShadowObservation,
  createCoordinateEvidenceCandidate,
  rankCoordinateEvidenceCandidates
} from "../server/coordinate-evidence/index.js";

function baseCandidate(overrides = {}) {
  return createCoordinateEvidenceCandidate({
    evidenceType: "unknown_evidence",
    sourceParser: "shadow_observation_regression",
    coordinateSource: "unknown_source",
    authority: {
      level: 0,
      category: AUTHORITY_CATEGORY.UNKNOWN,
      reason: "shadow_observation_regression_default"
    },
    confidence: {
      level: "unknown",
      reason: "shadow_observation_regression_default"
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
    reason: "shadow_observation_regression_default",
    ...overrides
  });
}

function structuredCadastralCandidate() {
  return baseCandidate({
    evidenceType: "structured_cadastral_table",
    sourceParser: "cadastral_grid_parser",
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
    recommendedState: COORDINATE_EVIDENCE_RECOMMENDED_STATE.BLOCKED_REVIEW,
    canGenerateKml: false,
    reason: "structured_cadastral_table"
  });
}

function explicitGeographicDmsCandidate() {
  return baseCandidate({
    evidenceType: "explicit_geographic_dms",
    sourceParser: "cote_divoire_dms_parser",
    coordinateSource: "explicit_lat_lon",
    authority: {
      level: 5,
      category: AUTHORITY_CATEGORY.EXPLICIT_LEGAL_COORDINATE,
      reason: "explicit_semantic_evidence"
    },
    confidence: {
      level: "high",
      reason: "explicit_hemisphere_and_order"
    },
    attributes: {
      hasExplicitHemisphere: true,
      hasExplicitCoordinateOrder: true,
      geometryValid: true
    },
    reason: "explicit_dms_with_hemisphere"
  });
}

function verifiedUtmCandidate() {
  return baseCandidate({
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
      reason: "projection_transform_matches"
    },
    attributes: {
      transformVerified: true,
      geometryValid: true
    },
    reason: "verified_utm_transformation"
  });
}

function utmCrsTextCandidate() {
  return baseCandidate({
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
    },
    attributes: {
      crsEvidence: true,
      geometryValid: true
    },
    recommendedState: COORDINATE_EVIDENCE_RECOMMENDED_STATE.BLOCKED_REVIEW,
    canGenerateKml: false,
    reason: "utm_crs_text"
  });
}

function legacySnapshot(overrides = {}) {
  return {
    coordinateType: "utm_projected_xy",
    precisionMode: "utm-projected-x-y-review",
    confirmationStatus: "blocked",
    qualityGateStatus: "blocked",
    coordinateResult: {
      state: "BLOCKED_REVIEW"
    },
    kml_ready: false,
    ...overrides
  };
}

function observationFor({ sampleId, category, candidates, legacy = legacySnapshot(), shadowDecision }) {
  const decision = shadowDecision || rankCoordinateEvidenceCandidates(candidates, legacy);
  return buildCoordinateEvidenceShadowObservation({
    sampleId,
    timestamp: "2026-08-11T00:00:00.000Z",
    commit: "test-commit",
    branch: "test-branch",
    category,
    fixture: {
      fileName: `${sampleId}.png`,
      fixtureStatus: "real_current_capture",
      fixtureHash: "sha256:test"
    },
    legacySnapshot: legacy,
    candidates,
    shadowDecision: decision
  });
}

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("Madagascar observation passes with structured cadastral over UTM CRS", () => {
  const observation = observationFor({
    sampleId: "madagascar_cadastral_candidate_001",
    category: SHADOW_OBSERVATION_CATEGORY.STRUCTURED_LEGAL_COORDINATE,
    candidates: [
      structuredCadastralCandidate(),
      utmCrsTextCandidate()
    ]
  });

  assert.equal(observation.schemaVersion, "coordinate_evidence_shadow_observation_v1");
  assert.equal(observation.classification, SHADOW_OBSERVATION_CLASSIFICATION.PASS);
  assert.equal(observation.shadowDecision.winner, "structured_cadastral_table");
  assert.equal(observation.shadowDecision.authority, 5);
  assert.equal(observation.isolation.affectsLegacyWinner, false);
  assert.equal(observation.isolation.affectsCoordinateResult, false);
  assert.equal(observation.isolation.affectsKml, false);
});

test("Cote d'Ivoire observation passes with explicit geographic DMS only", () => {
  const observation = observationFor({
    sampleId: "cote_divoire_single_03",
    category: SHADOW_OBSERVATION_CATEGORY.EXPLICIT_GEOGRAPHIC_SEMANTIC,
    candidates: [
      explicitGeographicDmsCandidate()
    ],
    legacy: legacySnapshot({
      coordinateType: "cote_divoire_geographic_dms_table",
      precisionMode: "cote-divoire-geographic-dms-table",
      confirmationStatus: "not_required",
      qualityGateStatus: "passed",
      coordinateResult: {
        state: "AUTO_EXPORT"
      },
      kml_ready: true
    })
  });

  assert.equal(observation.classification, SHADOW_OBSERVATION_CLASSIFICATION.PASS);
  assert.equal(observation.policy.comparisonRequired, false);
  assert.deepEqual(observation.policy.optionalLoserEvidenceTypes, ["generic_decimal"]);
  assert.equal(observation.shadowDecision.winner, "explicit_geographic_dms");
  assert.equal(observation.candidates.length, 1);
});

test("verified transformation observation passes over UTM CRS text", () => {
  const observation = observationFor({
    sampleId: "indonesia_utm50s_verified_transformation",
    category: SHADOW_OBSERVATION_CATEGORY.VERIFIED_TRANSFORMATION,
    candidates: [
      verifiedUtmCandidate(),
      utmCrsTextCandidate()
    ],
    legacy: legacySnapshot({
      precisionMode: "utm-projected-x-y",
      confirmationStatus: "awaiting_confirmation",
      qualityGateStatus: "passed",
      coordinateResult: {
        state: "AUTO_EXPORT"
      }
    })
  });

  const winner = observation.candidates.find(candidate => candidate.evidenceType === "verified_utm_transformation");
  assert.equal(observation.classification, SHADOW_OBSERVATION_CLASSIFICATION.PASS);
  assert.equal(winner.transformVerified, true);
  assert.equal(observation.shadowDecision.winner, "verified_utm_transformation");
  assert.equal(observation.shadowDecision.authority, 4);
});

test("Indonesia DMS-vs-UTM historical case remains pending without fixture", () => {
  const observation = buildCoordinateEvidenceShadowObservation({
    sampleId: "indonesia_dms_vs_utm",
    timestamp: "2026-08-11T00:00:00.000Z",
    commit: "test-commit",
    branch: "test-branch",
    category: SHADOW_OBSERVATION_CATEGORY.PENDING_FIXTURE,
    fixture: {
      fileName: null,
      fixtureStatus: "pending_real_fixture",
      fixtureHash: null
    },
    candidates: [],
    shadowDecision: {},
    legacySnapshot: {}
  });

  assert.equal(observation.classification, SHADOW_OBSERVATION_CLASSIFICATION.PENDING);
  assert.equal(observation.policy.requiredWinnerEvidenceType, null);
  assert.equal(observation.candidates.length, 0);
});

test("legacy isolation violation is classified separately", () => {
  const candidates = [
    structuredCadastralCandidate(),
    utmCrsTextCandidate()
  ];
  const legacy = legacySnapshot();
  const decision = {
    ...rankCoordinateEvidenceCandidates(candidates, legacy),
    affectsLegacyWinner: true
  };
  const observation = observationFor({
    sampleId: "madagascar_isolation_failure",
    category: SHADOW_OBSERVATION_CATEGORY.STRUCTURED_LEGAL_COORDINATE,
    candidates,
    legacy,
    shadowDecision: decision
  });

  assert.equal(observation.classification, SHADOW_OBSERVATION_CLASSIFICATION.FAIL_ISOLATION);
  assert.equal(observation.isolation.affectsLegacyWinner, true);
});

test("observation output omits raw OCR, prompts, model responses, credentials, and coordinate rows", () => {
  const observation = buildCoordinateEvidenceShadowObservation({
    sampleId: "security_marker_case",
    timestamp: "2026-08-11T00:00:00.000Z",
    commit: "test-commit",
    branch: "test-branch",
    category: SHADOW_OBSERVATION_CATEGORY.EXPLICIT_GEOGRAPHIC_SEMANTIC,
    rawOcr: "raw OCR should not be copied",
    prompt: "prompt should not be copied",
    modelResponse: "model response should not be copied",
    token: "token=secret",
    coordinateRows: [[292812.5, 360937.5]],
    candidates: [
      {
        ...explicitGeographicDmsCandidate(),
        rawOcr: "raw OCR should not be copied",
        prompt: "prompt should not be copied",
        modelResponse: "model response should not be copied",
        token: "token=secret",
        coordinateRows: [[292812.5, 360937.5]]
      }
    ],
    shadowDecision: {
      winnerEvidenceType: "explicit_geographic_dms",
      winnerAuthority: { level: 5 },
      reason: "single_candidate",
      affectsLegacyWinner: false,
      affectsCoordinateResult: false,
      affectsKml: false
    },
    legacySnapshot: legacySnapshot()
  });
  const serialized = JSON.stringify(observation);

  assert.equal(observation.classification, SHADOW_OBSERVATION_CLASSIFICATION.PASS);
  assert.equal(serialized.includes("raw OCR should not be copied"), false);
  assert.equal(serialized.includes("prompt should not be copied"), false);
  assert.equal(serialized.includes("model response should not be copied"), false);
  assert.equal(serialized.includes("token=secret"), false);
  assert.equal(serialized.includes("292812.5"), false);
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
  console.log(`Coordinate Evidence Shadow Observation Regression: ${passed}/${tests.length} PASS`);
}
