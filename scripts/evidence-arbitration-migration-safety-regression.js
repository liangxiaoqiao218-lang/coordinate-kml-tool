import assert from "node:assert/strict";
import {
  AUTHORITY_CATEGORY,
  COORDINATE_EVIDENCE_RECOMMENDED_STATE,
  EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION,
  EVIDENCE_ARBITRATION_MIGRATION_SAFETY_SCHEMA_VERSION,
  buildEvidenceArbitrationDryRunDiff,
  buildEvidenceArbitrationMigrationSafety,
  buildEvidenceArbitrationProposal,
  createCoordinateEvidenceCandidate,
  rankCoordinateEvidenceCandidates
} from "../server/coordinate-evidence/index.js";

function baseCandidate(overrides = {}) {
  return createCoordinateEvidenceCandidate({
    evidenceType: "unknown_evidence",
    sourceParser: "migration_safety_regression",
    coordinateSource: "migration_safety_regression",
    authority: {
      level: 0,
      category: AUTHORITY_CATEGORY.UNKNOWN,
      reason: "migration_safety_regression_default"
    },
    confidence: {
      level: "unknown",
      reason: "migration_safety_regression_default"
    },
    attributes: {
      geometryValid: true
    },
    coordinateSummary: {
      pointCount: 4,
      geometryType: "polygon",
      groupCount: 1
    },
    recommendedState: COORDINATE_EVIDENCE_RECOMMENDED_STATE.CONFIRM_REQUIRED,
    canGenerateKml: false,
    reason: "migration_safety_regression_default",
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
    recommendedState: COORDINATE_EVIDENCE_RECOMMENDED_STATE.AUTO_EXPORT,
    canGenerateKml: true,
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
    recommendedState: COORDINATE_EVIDENCE_RECOMMENDED_STATE.AUTO_EXPORT,
    canGenerateKml: true,
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

function dryRunFor({ candidates, legacy = legacySnapshot(), extraProposal = {} }) {
  const decision = rankCoordinateEvidenceCandidates(candidates, legacy);
  const proposal = buildEvidenceArbitrationProposal({
    legacySnapshot: legacy,
    candidates,
    shadowDecision: decision,
    ...extraProposal
  });
  return buildEvidenceArbitrationDryRunDiff({ proposal });
}

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("flag disabled keeps legacy and blocks migration", () => {
  const safety = buildEvidenceArbitrationMigrationSafety({
    dryRun: dryRunFor({
      candidates: [
        structuredCadastralCandidate(),
        utmCrsTextCandidate()
      ]
    })
  });

  assert.equal(safety.schemaVersion, EVIDENCE_ARBITRATION_MIGRATION_SAFETY_SCHEMA_VERSION);
  assert.equal(safety.eligibility.classification, EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.MIGRATION_DISABLED);
  assert.equal(safety.eligibility.migrationEligible, false);
  assert.equal(safety.safety.migrationApplied, false);
  assert.ok(safety.eligibility.blockReasons.includes("migration_flag_disabled"));
});

test("proposal remains visible when migration is disabled", () => {
  const safety = buildEvidenceArbitrationMigrationSafety({
    dryRun: dryRunFor({
      candidates: [
        structuredCadastralCandidate(),
        utmCrsTextCandidate()
      ]
    }),
    flags: {
      migration: false,
      kmlGate: false,
      autoDecision: false
    }
  });

  assert.equal(safety.proposal.winnerEvidenceType, "structured_cadastral_table");
  assert.equal(safety.dryRun.wouldChangeLegacy, true);
  assert.equal(safety.eligibility.reviewOnlyEligible, true);
  assert.equal(safety.safety.affectsLegacyWinner, false);
});

test("rollback metadata restores legacy systems without git revert", () => {
  const safety = buildEvidenceArbitrationMigrationSafety({
    dryRun: dryRunFor({
      candidates: [
        verifiedUtmCandidate(),
        utmCrsTextCandidate()
      ],
      legacy: legacySnapshot({
        coordinateType: "utm_projected_xy",
        precisionMode: "utm-projected-x-y",
        confirmationStatus: "awaiting_confirmation",
        qualityGateStatus: "passed",
        coordinateResult: {
          state: "AUTO_EXPORT"
        },
        kml_ready: false
      })
    })
  });

  assert.equal(safety.rollback.rollbackSafe, true);
  assert.equal(safety.rollback.rollbackFlag, "ENABLE_EVIDENCE_ARBITRATION_MIGRATION");
  assert.equal(safety.rollback.restoresLegacyArbitration, true);
  assert.equal(safety.rollback.restoresCoordinateResult, true);
  assert.equal(safety.rollback.restoresKmlBehavior, true);
  assert.equal(safety.rollback.preservesShadowObservation, true);
});

test("KML gate disabled keeps KML migration unavailable", () => {
  const safety = buildEvidenceArbitrationMigrationSafety({
    dryRun: dryRunFor({
      candidates: [
        verifiedUtmCandidate(),
        utmCrsTextCandidate()
      ],
      legacy: legacySnapshot({
        coordinateType: "utm_projected_xy",
        precisionMode: "utm-projected-x-y",
        confirmationStatus: "awaiting_confirmation",
        qualityGateStatus: "passed",
        coordinateResult: {
          state: "AUTO_EXPORT"
        },
        kml_ready: false
      })
    }),
    flags: {
      migration: true,
      kmlGate: false,
      autoDecision: true
    }
  });

  assert.equal(safety.eligibility.classification, EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.LIMITED_MIGRATION_ELIGIBLE);
  assert.equal(safety.eligibility.kmlGateEligible, false);
  assert.ok(safety.eligibility.blockReasons.includes("kml_gate_disabled"));
  assert.equal(safety.safety.affectsKml, false);
});

test("Indonesia pending policy blocks migration", () => {
  const dryRun = dryRunFor({
    candidates: [
      verifiedUtmCandidate(),
      utmCrsTextCandidate()
    ],
    extraProposal: {
      category: "indonesia_dms_vs_utm",
      fixture: {
        fixtureStatus: "pending_real_fixture"
      }
    }
  });
  const safety = buildEvidenceArbitrationMigrationSafety({
    dryRun,
    category: "indonesia_dms_vs_utm",
    fixture: {
      fixtureStatus: "pending_real_fixture"
    },
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    }
  });

  assert.equal(safety.eligibility.classification, EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.BLOCKED_PENDING_POLICY);
  assert.equal(safety.eligibility.migrationEligible, false);
  assert.ok(safety.eligibility.blockReasons.includes("pending_fixture_policy"));
});

test("Madagascar remains review-only and never auto migrates", () => {
  const safety = buildEvidenceArbitrationMigrationSafety({
    dryRun: dryRunFor({
      candidates: [
        structuredCadastralCandidate(),
        utmCrsTextCandidate()
      ]
    }),
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    }
  });

  assert.equal(safety.category, "structured_legal_coordinate");
  assert.equal(safety.eligibility.classification, EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.MANUAL_REVIEW_REQUIRED);
  assert.equal(safety.eligibility.migrationEligible, false);
  assert.equal(safety.eligibility.autoDecisionEligible, false);
  assert.ok(safety.eligibility.blockReasons.includes("structured_legal_coordinate_review_only"));
});

test("Cote d'Ivoire requires review before migration", () => {
  const safety = buildEvidenceArbitrationMigrationSafety({
    dryRun: dryRunFor({
      candidates: [explicitGeographicDmsCandidate()],
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
    }),
    flags: {
      migration: true,
      kmlGate: false,
      autoDecision: false
    }
  });

  assert.equal(safety.category, "explicit_geographic_semantic");
  assert.equal(safety.eligibility.classification, EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.MANUAL_REVIEW_REQUIRED);
  assert.equal(safety.eligibility.migrationEligible, false);
  assert.ok(safety.eligibility.blockReasons.includes("manual_review_required"));
});

test("Verified transformation can become limited migration eligible behind all flags", () => {
  const safety = buildEvidenceArbitrationMigrationSafety({
    dryRun: dryRunFor({
      candidates: [
        verifiedUtmCandidate(),
        utmCrsTextCandidate()
      ],
      legacy: legacySnapshot({
        coordinateType: "utm_projected_xy",
        precisionMode: "utm-projected-x-y",
        confirmationStatus: "awaiting_confirmation",
        qualityGateStatus: "passed",
        coordinateResult: {
          state: "AUTO_EXPORT"
        },
        kml_ready: false
      })
    }),
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    }
  });

  assert.equal(safety.category, "verified_transformation");
  assert.equal(safety.eligibility.classification, EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.LIMITED_MIGRATION_ELIGIBLE);
  assert.equal(safety.eligibility.migrationEligible, true);
  assert.equal(safety.eligibility.autoDecisionEligible, true);
  assert.equal(safety.eligibility.kmlGateEligible, true);
  assert.equal(safety.safety.migrationApplied, false);
});

test("Missing proposal remains no proposal", () => {
  const proposal = buildEvidenceArbitrationProposal({
    legacySnapshot: legacySnapshot(),
    candidates: [utmCrsTextCandidate()],
    shadowDecision: {}
  });
  const dryRun = buildEvidenceArbitrationDryRunDiff({ proposal });
  const safety = buildEvidenceArbitrationMigrationSafety({
    dryRun,
    flags: {
      migration: true,
      kmlGate: true,
      autoDecision: true
    }
  });

  assert.equal(safety.eligibility.classification, EVIDENCE_ARBITRATION_MIGRATION_SAFETY_CLASSIFICATION.NO_PROPOSAL);
  assert.equal(safety.eligibility.migrationEligible, false);
  assert.ok(safety.eligibility.blockReasons.includes("shadow_candidate_unavailable"));
});

test("Migration safety output sanitizes secret-like strings", () => {
  const safety = buildEvidenceArbitrationMigrationSafety({
    dryRun: {
      schemaVersion: "evidence_arbitration_dry_run_diff_v1",
      classification: "REVIEW_REQUIRED",
      proposal: {
        classification: "REVIEW_REQUIRED",
        winnerEvidenceType: "secret:=value",
        winnerAuthority: 5,
        proposedCoordinateType: "utm_projected_xy",
        proposedPrecisionMode: "utm-projected-x-y",
        blockReasons: ["authorization:=bearer abc"]
      },
      diff: {
        wouldChangeLegacy: true,
        wouldChangeCoordinateType: true,
        wouldChangePrecisionMode: false,
        wouldChangeKml: false
      }
    },
    flags: {
      migration: false
    }
  });
  const serialized = JSON.stringify(safety);

  assert.equal(serialized.includes("secret:=value"), false);
  assert.equal(serialized.includes("authorization:=bearer"), false);
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
  console.log(`Evidence Arbitration Migration Safety Regression: ${passed}/${tests.length} PASS`);
}
