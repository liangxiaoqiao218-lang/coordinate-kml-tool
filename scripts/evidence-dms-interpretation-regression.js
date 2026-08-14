import assert from "node:assert/strict";
import { access } from "node:fs/promises";
import { readFileSync } from "node:fs";
import {
  DMS_INTERPRETATION_STATUS,
  buildDeterministicDmsInterpretation,
  buildDmsGeographicEvidenceCandidate,
  buildEvidenceArbitrationDryRunDiff,
  buildEvidenceArbitrationProposal,
  rankCoordinateEvidenceCandidates
} from "../server/coordinate-evidence/index.js";
import { buildFinalizedCoordinateVerificationResponse } from "../server/verification/index.js";

const fixturePath = "artifacts/fixtures/cote-divoire-dms-real-001.jpeg";
const tolerance = 0.000001;

const coteDIvoireRows = Object.freeze([
  Object.freeze({
    point: "1",
    latitude: { degrees: 11, minutes: 52, seconds: 11.93, hemisphere: "N" },
    longitude: { degrees: 8, minutes: 53, seconds: 32.66, hemisphere: "W" }
  }),
  Object.freeze({
    point: "2",
    latitude: { degrees: 11, minutes: 52, seconds: 17.21, hemisphere: "N" },
    longitude: { degrees: 8, minutes: 53, seconds: 33.18, hemisphere: "W" }
  }),
  Object.freeze({
    point: "3",
    latitude: { degrees: 11, minutes: 52, seconds: 12.57, hemisphere: "N" },
    longitude: { degrees: 8, minutes: 53, seconds: 54.03, hemisphere: "W" }
  }),
  Object.freeze({
    point: "4",
    latitude: { degrees: 11, minutes: 52, seconds: 7.65, hemisphere: "N" },
    longitude: { degrees: 8, minutes: 53, seconds: 53.56, hemisphere: "W" }
  })
]);

const groundTruth = Object.freeze([
  Object.freeze({ point: "1", lat: 11.869980556, lon: -8.892405556 }),
  Object.freeze({ point: "2", lat: 11.871447222, lon: -8.89255 }),
  Object.freeze({ point: "3", lat: 11.870158333, lon: -8.898341667 }),
  Object.freeze({ point: "4", lat: 11.868791667, lon: -8.898211111 })
]);

function assertClose(actual, expected, message) {
  assert.ok(Math.abs(Number(actual) - Number(expected)) <= tolerance, `${message}: actual=${actual} expected=${expected}`);
}

function coteDIvoireInterpretation() {
  return buildDeterministicDmsInterpretation({
    rows: coteDIvoireRows,
    headerSemantics: {
      latitude: "nord",
      longitude: "ouest"
    }
  });
}

function legacyCoteDIvoireSnapshot(overrides = {}) {
  return {
    coordinateType: "cote_divoire_geographic_dms_table",
    precisionMode: "cote-divoire-geographic-dms-table",
    confirmationStatus: "not_required",
    qualityGateStatus: "blocked",
    coordinateResult: {
      state: "BLOCKED_REVIEW"
    },
    kml_ready: false,
    coordinates: [
      "label | WGS84 | KML",
      "1 | 11.870514, 8.89235 | 8.89235,11.870514,0",
      "2 | 11.871447, 8.89255 | 8.89255,11.871447,0",
      "3 | 11.870158, 8.893175 | 8.893175,11.870158,0",
      "4 | 11.871014, 8.893156 | 8.893156,11.871014,0"
    ].join("\n"),
    ...overrides
  };
}

function explicitDmsCandidate(coordinateInterpretation = coteDIvoireInterpretation()) {
  return buildDmsGeographicEvidenceCandidate({
    coordinateEngineV2: {
      coordinate_type: "cote_divoire_geographic_dms_table",
      precision_mode: "cote-divoire-geographic-dms-table",
      requires_review: true,
      groups: [
        {
          geometry: "polygon",
          points: [{}, {}, {}, {}]
        }
      ]
    },
    dmsAccepted: true,
    hasExplicitHemisphere: true,
    coordinateInterpretation
  });
}

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

function serverSource() {
  return readFileSync("server.js", "utf8");
}

test("Cote d'Ivoire real fixture is present as benchmark input", async () => {
  await access(fixturePath);
});

test("Cote d'Ivoire deterministic DMS interpretation matches 4/4 ground truth", () => {
  const interpretation = coteDIvoireInterpretation();
  assert.equal(interpretation.interpretationStatus, DMS_INTERPRETATION_STATUS.COMPLETE);
  assert.equal(interpretation.deterministicConversion, true);
  assert.equal(interpretation.hemisphereResolved, true);
  assert.equal(interpretation.normalizedCoordinates.length, 4);
  interpretation.normalizedCoordinates.forEach((point, index) => {
    assertClose(point.lat, groundTruth[index].lat, `P${index + 1} lat`);
    assertClose(point.lon, groundTruth[index].lon, `P${index + 1} lon`);
    assert.equal(point.lon < 0, true, `P${index + 1} west longitude`);
  });
});

test("French Nord/Ouest header semantics applies negative west longitude", () => {
  const interpretation = buildDeterministicDmsInterpretation({
    headerSemantics: {
      latitude: "Latitude nord",
      longitude: "Longitude ouest"
    },
    rows: [
      {
        point: "A",
        latitude: { degrees: 11, minutes: 52, seconds: 11.93 },
        longitude: { degrees: 8, minutes: 53, seconds: 32.66 }
      }
    ]
  });
  assert.equal(interpretation.interpretationStatus, DMS_INTERPRETATION_STATUS.COMPLETE);
  assertClose(interpretation.normalizedCoordinates[0].lat, 11.869980556, "nord latitude");
  assertClose(interpretation.normalizedCoordinates[0].lon, -8.892405556, "ouest longitude");
});

test("S/E hemisphere applies negative latitude and positive longitude", () => {
  const interpretation = buildDeterministicDmsInterpretation({
    rows: [
      {
        point: "A",
        latitude: { degrees: 14, minutes: 36, seconds: 0, hemisphere: "S" },
        longitude: { degrees: 32, minutes: 57, seconds: 20, hemisphere: "E" }
      }
    ]
  });
  assert.equal(interpretation.interpretationStatus, DMS_INTERPRETATION_STATUS.COMPLETE);
  assertClose(interpretation.normalizedCoordinates[0].lat, -14.6, "south latitude");
  assertClose(interpretation.normalizedCoordinates[0].lon, 32.955555556, "east longitude");
});

test("decimal seconds are preserved", () => {
  const interpretation = buildDeterministicDmsInterpretation({
    rows: [
      {
        point: "A",
        latitude: { degrees: 1, minutes: 2, seconds: 3.45, hemisphere: "N" },
        longitude: { degrees: 4, minutes: 5, seconds: 6.78, hemisphere: "W" }
      }
    ]
  });
  assertClose(interpretation.normalizedCoordinates[0].lat, 1.034291667, "decimal seconds latitude");
  assertClose(interpretation.normalizedCoordinates[0].lon, -4.085216667, "decimal seconds longitude");
});

test("invalid minutes and seconds reject without creating coordinates", () => {
  const invalidMinutes = buildDeterministicDmsInterpretation({
    rows: [
      {
        point: "A",
        latitude: { degrees: 1, minutes: 60, seconds: 0, hemisphere: "N" },
        longitude: { degrees: 1, minutes: 2, seconds: 3, hemisphere: "E" }
      }
    ]
  });
  const invalidSeconds = buildDeterministicDmsInterpretation({
    rows: [
      {
        point: "A",
        latitude: { degrees: 1, minutes: 2, seconds: 60, hemisphere: "N" },
        longitude: { degrees: 1, minutes: 2, seconds: 3, hemisphere: "E" }
      }
    ]
  });
  assert.equal(invalidMinutes.interpretationStatus, DMS_INTERPRETATION_STATUS.INVALID);
  assert.equal(invalidMinutes.normalizedCoordinates.length, 0);
  assert.equal(invalidSeconds.interpretationStatus, DMS_INTERPRETATION_STATUS.INVALID);
  assert.equal(invalidSeconds.normalizedCoordinates.length, 0);
});

test("missing hemisphere fails closed as incomplete", () => {
  const interpretation = buildDeterministicDmsInterpretation({
    rows: [
      {
        point: "A",
        latitude: { degrees: 1, minutes: 2, seconds: 3 },
        longitude: { degrees: 4, minutes: 5, seconds: 6 }
      }
    ]
  });
  assert.equal(interpretation.interpretationStatus, DMS_INTERPRETATION_STATUS.INCOMPLETE);
  assert.equal(interpretation.deterministicConversion, false);
  assert.equal(interpretation.normalizedCoordinates.length, 0);
});

test("candidate carries sanitized coordinate interpretation without production effects", () => {
  const candidate = explicitDmsCandidate();
  assert.equal(candidate.evidenceType, "explicit_geographic_dms");
  assert.equal(candidate.coordinateInterpretation.interpretationStatus, DMS_INTERPRETATION_STATUS.COMPLETE);
  assert.equal(candidate.coordinateInterpretation.normalizedCoordinates.length, 4);
  assert.equal(candidate.coordinateInterpretation.affectsLegacyWinner, false);
  assert.equal(candidate.coordinateInterpretation.affectsCoordinateResult, false);
  assert.equal(candidate.coordinateInterpretation.affectsKml, false);
});

test("proposal detects numeric and hemisphere disagreement when type agrees", () => {
  const candidates = [explicitDmsCandidate()];
  const legacy = legacyCoteDIvoireSnapshot();
  const proposal = buildEvidenceArbitrationProposal({
    legacySnapshot: legacy,
    candidates,
    shadowDecision: rankCoordinateEvidenceCandidates(candidates, legacy)
  });
  assert.equal(proposal.proposal.classification, "TYPE_AGREEMENT_WITH_COORDINATE_DISAGREEMENT");
  assert.equal(proposal.proposal.wouldChangeLegacy, false);
  assert.equal(proposal.proposal.wouldChangeCoordinateValues, true);
  assert.equal(proposal.proposal.numericDisagreement, true);
  assert.equal(proposal.proposal.hemisphereDisagreement, true);
  assert.equal(proposal.proposal.pointMismatchCount, 4);
  assert.ok(proposal.proposal.blockReasons.includes("coordinate_value_disagreement"));
  assert.ok(proposal.proposal.blockReasons.includes("hemisphere_disagreement"));
  assert.equal(proposal.safety.affectsLegacyWinner, false);
});

test("dry-run exposes point-level numeric and hemisphere diff", () => {
  const candidates = [explicitDmsCandidate()];
  const legacy = legacyCoteDIvoireSnapshot();
  const proposal = buildEvidenceArbitrationProposal({
    legacySnapshot: legacy,
    candidates,
    shadowDecision: rankCoordinateEvidenceCandidates(candidates, legacy)
  });
  const diff = buildEvidenceArbitrationDryRunDiff({ proposal });
  assert.equal(diff.classification, "COORDINATE_DISAGREEMENT");
  assert.equal(diff.diff.wouldChangeCoordinateValues, true);
  assert.equal(diff.diff.numericDisagreement, true);
  assert.equal(diff.diff.hemisphereDisagreement, true);
  assert.equal(diff.diff.pointLevelDiff.length, 4);
  assert.equal(diff.diff.pointLevelDiff[0].evidence.lon < 0, true);
  assert.equal(diff.safety.affectsLegacyWinner, false);
  assert.equal(diff.safety.affectsCoordinateResult, false);
  assert.equal(diff.safety.affectsKml, false);
});

test("same coordinates within tolerance remains agreement", () => {
  const candidates = [explicitDmsCandidate()];
  const legacy = legacyCoteDIvoireSnapshot({
    coordinates: [
      "1 | 11.869980557, -8.892405555",
      "2 | 11.871447222, -8.89255",
      "3 | 11.870158333, -8.898341667",
      "4 | 11.868791667, -8.898211111"
    ].join("\n")
  });
  const proposal = buildEvidenceArbitrationProposal({
    legacySnapshot: legacy,
    candidates,
    shadowDecision: rankCoordinateEvidenceCandidates(candidates, legacy)
  });
  assert.equal(proposal.proposal.classification, "AGREEMENT");
  assert.equal(proposal.proposal.wouldChangeCoordinateValues, false);
});

test("security sanitization excludes raw prompt/model/image markers", () => {
  const candidate = explicitDmsCandidate({
    ...coteDIvoireInterpretation(),
    prompt: "do not expose",
    modelResponse: "do not expose",
    rawText: "do not expose",
    imageBase64: "AAAA"
  });
  const serialized = JSON.stringify(candidate);
  assert.equal(serialized.includes("do not expose"), false);
  assert.equal(/prompt|modelResponse|rawText|imageBase64/i.test(serialized), false);
});

test("Cote d'Ivoire parser preserves structured DMS rows before decimal normalization", () => {
  const source = serverSource();
  assert.match(source, /function\s+buildCoteDIvoireStructuredDmsRow/);
  assert.match(source, /structuredDmsRow:\s*buildCoteDIvoireStructuredDmsRow/);
  assert.match(source, /sanitizeCoteDIvoireDmsToken\(latPart,\s*"latitude"/);
  assert.match(source, /sanitizeCoteDIvoireDmsToken\(lonPart,\s*"longitude"/);
  assert.match(source, /normalizeCoteDIvoireDmsHemisphere\(part\.direction,\s*fallbackHemisphere\)/);
});

test("Cote d'Ivoire normalization preserves structured DMS rows for evidence pass-through", () => {
  const source = serverSource();
  assert.match(source, /function\s+normalizeCoteDIvoireGeographicDmsTable/);
  assert.match(source, /structuredDmsRow:\s*point\.structuredDmsRow/);
  assert.match(source, /function\s+collectCoteDIvoireStructuredDmsRows/);
});

test("Cote d'Ivoire supplemental producer wires deterministic DMS interpretation into evidence context only", () => {
  const source = serverSource();
  assert.match(source, /buildDeterministicDmsInterpretation/);
  assert.match(source, /function\s+mergeCoteDIvoireDmsInterpretationIntoEvidenceContext/);
  assert.match(source, /recognitionPayload\._coordinateEvidenceContext\s*=\s*mergeCoteDIvoireDmsInterpretationIntoEvidenceContext/);
  assert.match(source, /coteDIvoireDmsInterpretation\?\.interpretationStatus\s*===\s*"COMPLETE"/);
  assert.doesNotMatch(source, /coordinates\s*=\s*coteDIvoireDmsInterpretation/);
  assert.doesNotMatch(source, /kml_ready\s*=\s*coteDIvoireDmsInterpretation/);
});

test("runtime debug attachment consumes structured DMS context without mutating legacy public result", () => {
  const legacy = legacyCoteDIvoireSnapshot();
  const interpretation = coteDIvoireInterpretation();
  const response = buildFinalizedCoordinateVerificationResponse({
    ...legacy,
    _coordinateEvidenceContext: {
      dms: {
        dmsAccepted: true,
        hasExplicitHemisphere: true,
        hasExplicitCoordinateOrder: true,
        pointCount: 4,
        groupCount: 1,
        geometryType: "polygon",
        coordinateInterpretation: interpretation
      }
    }
  }, {
    coordinate_type: "cote_divoire_geographic_dms_table",
    precision_mode: "cote-divoire-geographic-dms-table",
    requires_review: true,
    groups: [
      {
        geometry: "polygon",
        points: groundTruth.map(point => ({
          label: point.point,
          lat: point.lat,
          lon: point.lon
        }))
      }
    ]
  }, { includeCoordinateEvidenceDebug: true });
  const candidate = response.coordinateEvidenceCandidates.find(item => item.evidenceType === "explicit_geographic_dms");
  assert.equal(response.coordinates, legacy.coordinates);
  assert.equal(response.coordinateType, legacy.coordinateType);
  assert.equal(response.precisionMode, legacy.precisionMode);
  assert.equal(response.kml_ready, legacy.kml_ready);
  assert.ok(candidate);
  assert.equal(candidate.coordinateInterpretation.interpretationStatus, DMS_INTERPRETATION_STATUS.COMPLETE);
  assert.equal(candidate.coordinateInterpretation.normalizedCoordinates.length, 4);
  assert.equal(response.evidenceArbitrationProposal.proposal.numericDisagreement, true);
  assert.equal(response.evidenceArbitrationProposal.proposal.hemisphereDisagreement, true);
  assert.equal(response.evidenceArbitrationDryRun.classification, "COORDINATE_DISAGREEMENT");
  assert.equal(response.evidenceArbitrationDryRun.diff.pointLevelDiff.length, 4);
  assert.equal(response.coordinateEvidenceSummary.affectsLegacyWinner, false);
  assert.equal(response.coordinateEvidenceSummary.affectsCoordinateResult, false);
  assert.equal(response.coordinateEvidenceSummary.affectsKml, false);
});

let passed = 0;
for (const { name, fn } of tests) {
  try {
    await fn();
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
  console.log(`Evidence DMS Interpretation Regression: ${passed}/${tests.length} PASS`);
}
