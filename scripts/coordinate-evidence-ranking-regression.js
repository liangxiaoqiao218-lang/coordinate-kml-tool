import assert from "node:assert/strict";
import {
  AUTHORITY_CATEGORY,
  COORDINATE_EVIDENCE_RECOMMENDED_STATE,
  createCoordinateEvidenceCandidate,
  rankCoordinateEvidenceCandidates
} from "../server/coordinate-evidence/index.js";

const REGRESSION_SCHEMA_VERSION = "coordinate_evidence_ranking_regression_v1";

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function baseCandidate(overrides = {}) {
  return createCoordinateEvidenceCandidate({
    evidenceType: "unknown_evidence",
    sourceParser: "ranking_regression",
    coordinateSource: "unknown_source",
    authority: {
      level: 0,
      category: AUTHORITY_CATEGORY.UNKNOWN,
      reason: "ranking_regression_default"
    },
    confidence: {
      level: "unknown",
      reason: "ranking_regression_default"
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
    reason: "ranking_regression_default",
    ...overrides
  });
}

function structuredCadastralCandidate() {
  return baseCandidate({
    evidenceId: "ev_madagascar_structured_cadastral",
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
    recommendedState: COORDINATE_EVIDENCE_RECOMMENDED_STATE.BLOCKED_REVIEW,
    canGenerateKml: false,
    reason: "structured_cadastral_table"
  });
}

function utmCrsTextCandidate() {
  return baseCandidate({
    evidenceId: "ev_utm_crs_text",
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

function explicitGeographicDmsCandidate() {
  return baseCandidate({
    evidenceId: "ev_cote_divoire_explicit_geographic_dms",
    evidenceType: "explicit_geographic_dms",
    sourceParser: "geographic_dms_semantic_producer",
    coordinateSource: "latitude_longitude_header_with_hemisphere",
    authority: {
      level: 5,
      category: AUTHORITY_CATEGORY.EXPLICIT_LEGAL_COORDINATE,
      reason: "explicit_semantic_evidence"
    },
    confidence: {
      level: "high",
      reason: "latitude_longitude_hemisphere_order_present"
    },
    attributes: {
      hasExplicitHemisphere: true,
      hasExplicitCoordinateOrder: true,
      geometryValid: true
    },
    reason: "explicit_semantic_evidence"
  });
}

function genericDecimalCandidate() {
  return baseCandidate({
    evidenceId: "ev_generic_decimal",
    evidenceType: "generic_decimal",
    sourceParser: "generic_decimal_parser",
    coordinateSource: "naked_decimal_pair",
    authority: {
      level: 1,
      category: AUTHORITY_CATEGORY.WEAK_NUMERIC,
      reason: "generic_decimal_without_semantic_header"
    },
    confidence: {
      level: "medium",
      reason: "numbers_parse_without_header"
    },
    reason: "generic_decimal_without_semantic_header"
  });
}

const regressionCases = Object.freeze([
  Object.freeze({
    schemaVersion: REGRESSION_SCHEMA_VERSION,
    caseId: "madagascar_cadastral_vs_utm",
    description: "Structured cadastral table evidence outranks UTM CRS text.",
    fixtureStatus: "real_rc_closed",
    inputCandidates: Object.freeze([
      structuredCadastralCandidate(),
      utmCrsTextCandidate()
    ]),
    expectedShadowDecision: Object.freeze({
      winnerEvidenceType: "structured_cadastral_table",
      winnerAuthorityLevel: 5,
      loserEvidenceTypes: Object.freeze(["utm_crs_text"]),
      reason: "higher_authority_evidence",
      caseRationale: "structured_cadastral_table_authority"
    }),
    legacySnapshot: Object.freeze({
      coordinateType: "utm_projected_xy",
      precisionMode: "utm-projected-x-y-review",
      confirmationStatus: "blocked",
      qualityGateStatus: "blocked",
      coordinateResult: Object.freeze({
        state: "BLOCKED_REVIEW"
      }),
      kml_ready: false
    }),
    legacyIsolation: Object.freeze({
      mustRemainUnchanged: true
    })
  }),
  Object.freeze({
    schemaVersion: REGRESSION_SCHEMA_VERSION,
    caseId: "cote_divoire_explicit_dms_vs_generic_decimal",
    description: "Explicit geographic DMS semantic evidence outranks generic decimal evidence.",
    fixtureStatus: "real_rc_closed",
    inputCandidates: Object.freeze([
      explicitGeographicDmsCandidate(),
      genericDecimalCandidate()
    ]),
    expectedShadowDecision: Object.freeze({
      winnerEvidenceType: "explicit_geographic_dms",
      winnerAuthorityLevel: 5,
      loserEvidenceTypes: Object.freeze(["generic_decimal"]),
      reason: "higher_authority_evidence",
      caseRationale: "explicit_semantic_evidence"
    }),
    legacySnapshot: Object.freeze({
      coordinateType: "wgs84_chat_coordinates",
      precisionMode: "wgs84-chat-coordinates",
      confirmationStatus: "confirmed",
      qualityGateStatus: "passed",
      coordinateResult: Object.freeze({
        state: "AUTO_EXPORT"
      }),
      kml_ready: true
    }),
    legacyIsolation: Object.freeze({
      mustRemainUnchanged: true
    })
  }),
  Object.freeze({
    schemaVersion: REGRESSION_SCHEMA_VERSION,
    caseId: "indonesia_dms_vs_utm_pending",
    description: "Pending placeholder only; no real historical fixture is available.",
    fixtureStatus: "pending_real_fixture",
    historicalRegressionEnabled: false,
    reason: "fixture_unavailable",
    inputCandidates: Object.freeze([]),
    expectedShadowDecision: null,
    legacySnapshot: null,
    legacyIsolation: Object.freeze({
      mustRemainUnchanged: true
    })
  })
]);

function assertSchemaShape(regressionCase) {
  assert.equal(regressionCase.schemaVersion, REGRESSION_SCHEMA_VERSION);
  assert.equal(typeof regressionCase.caseId, "string");
  assert.equal(typeof regressionCase.description, "string");
  assert.equal(typeof regressionCase.fixtureStatus, "string");
  assert.ok(Array.isArray(regressionCase.inputCandidates));
  assert.ok(Object.hasOwn(regressionCase, "expectedShadowDecision"));
  assert.ok(Object.hasOwn(regressionCase, "legacySnapshot"));
  assert.ok(regressionCase.legacyIsolation);
}

function assertCandidateShape(candidate) {
  assert.equal(typeof candidate.evidenceType, "string");
  assert.equal(typeof candidate.sourceParser, "string");
  assert.equal(typeof candidate.coordinateSource, "string");
  assert.equal(Number.isInteger(candidate.authority.level), true);
  assert.equal(typeof candidate.authority.category, "string");
  assert.equal(typeof candidate.authority.reason, "string");
  assert.equal(typeof candidate.confidence.level, "string");
  assert.equal(typeof candidate.confidence.reason, "string");
  assert.ok(candidate.attributes);
  assert.ok(Array.isArray(candidate.conflicts));
  assert.equal(typeof candidate.reason, "string");
}

function assertLegacyIsolation(before, after) {
  assert.deepEqual(after, before);
}

function assertShadowDecisionIsObservationOnly(decision) {
  assert.equal(decision.affectsLegacyWinner, false);
  assert.equal(decision.affectsCoordinateResult, false);
  assert.equal(decision.affectsKml, false);
}

function findCandidate(candidates, evidenceType) {
  return candidates.find(candidate => candidate.evidenceType === evidenceType);
}

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("ranking regression schema contains required fields", () => {
  for (const regressionCase of regressionCases) {
    assertSchemaShape(regressionCase);
    for (const candidate of regressionCase.inputCandidates) {
      assertCandidateShape(candidate);
    }
  }
});

test("Madagascar ranking baseline prefers structured cadastral over UTM CRS text", () => {
  const regressionCase = regressionCases.find(item => item.caseId === "madagascar_cadastral_vs_utm");
  const legacyBefore = clone(regressionCase.legacySnapshot);
  const decision = rankCoordinateEvidenceCandidates(regressionCase.inputCandidates, regressionCase.legacySnapshot);
  const legacyAfter = clone(regressionCase.legacySnapshot);

  assert.ok(findCandidate(regressionCase.inputCandidates, "structured_cadastral_table"));
  assert.ok(findCandidate(regressionCase.inputCandidates, "utm_crs_text"));
  assert.equal(decision.winnerEvidenceType, regressionCase.expectedShadowDecision.winnerEvidenceType);
  assert.equal(decision.winnerAuthority.level, regressionCase.expectedShadowDecision.winnerAuthorityLevel);
  assert.equal(decision.reason, regressionCase.expectedShadowDecision.reason);
  assert.equal(regressionCase.expectedShadowDecision.caseRationale, "structured_cadastral_table_authority");
  assert.equal(decision.differenceFromCurrentWinner, true);
  assertShadowDecisionIsObservationOnly(decision);
  assertLegacyIsolation(legacyBefore, legacyAfter);
});

test("Cote d'Ivoire ranking baseline prefers explicit semantic DMS over generic decimal", () => {
  const regressionCase = regressionCases.find(item => item.caseId === "cote_divoire_explicit_dms_vs_generic_decimal");
  const legacyBefore = clone(regressionCase.legacySnapshot);
  const decision = rankCoordinateEvidenceCandidates(regressionCase.inputCandidates, regressionCase.legacySnapshot);
  const legacyAfter = clone(regressionCase.legacySnapshot);
  const explicit = findCandidate(regressionCase.inputCandidates, "explicit_geographic_dms");

  assert.ok(explicit);
  assert.ok(findCandidate(regressionCase.inputCandidates, "generic_decimal"));
  assert.equal(explicit.attributes.hasExplicitHemisphere, true);
  assert.equal(explicit.attributes.hasExplicitCoordinateOrder, true);
  assert.equal(decision.winnerEvidenceType, regressionCase.expectedShadowDecision.winnerEvidenceType);
  assert.equal(decision.winnerAuthority.level, regressionCase.expectedShadowDecision.winnerAuthorityLevel);
  assert.equal(decision.reason, regressionCase.expectedShadowDecision.reason);
  assert.equal(regressionCase.expectedShadowDecision.caseRationale, "explicit_semantic_evidence");
  assertShadowDecisionIsObservationOnly(decision);
  assertLegacyIsolation(legacyBefore, legacyAfter);
});

test("Indonesia DMS-vs-UTM remains pending and is not counted as historical regression", () => {
  const regressionCase = regressionCases.find(item => item.caseId === "indonesia_dms_vs_utm_pending");
  assert.equal(regressionCase.fixtureStatus, "pending_real_fixture");
  assert.equal(regressionCase.historicalRegressionEnabled, false);
  assert.equal(regressionCase.reason, "fixture_unavailable");
  assert.equal(regressionCase.inputCandidates.length, 0);
  assert.equal(regressionCase.expectedShadowDecision, null);
  assert.equal(regressionCase.legacySnapshot, null);
});

test("ranking regression does not mutate candidate or legacy inputs", () => {
  for (const regressionCase of regressionCases.filter(item => item.historicalRegressionEnabled !== false)) {
    const candidatesBefore = clone(regressionCase.inputCandidates);
    const legacyBefore = clone(regressionCase.legacySnapshot);
    rankCoordinateEvidenceCandidates(regressionCase.inputCandidates, regressionCase.legacySnapshot);
    assert.deepEqual(clone(regressionCase.inputCandidates), candidatesBefore);
    assert.deepEqual(clone(regressionCase.legacySnapshot), legacyBefore);
  }
});

test("formal ranking baselines only include real closed fixtures", () => {
  const formalCases = regressionCases.filter(item => item.historicalRegressionEnabled !== false);
  assert.deepEqual(
    formalCases.map(item => item.caseId).sort(),
    [
      "cote_divoire_explicit_dms_vs_generic_decimal",
      "madagascar_cadastral_vs_utm"
    ]
  );
  for (const regressionCase of formalCases) {
    assert.equal(regressionCase.fixtureStatus, "real_rc_closed");
  }
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
  console.log(`Coordinate Evidence Ranking Regression: ${passed}/${tests.length} PASS`);
}
