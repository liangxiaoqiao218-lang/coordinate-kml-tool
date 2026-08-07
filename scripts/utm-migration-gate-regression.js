import assert from "node:assert/strict";

import { evaluateShadowMigrationGate } from "../server/utm-intent/migration-gate.js";
import { observeUtmMigration } from "../server/utm-intent/migration-observer.js";
import { resolveShadowUtmIntent } from "../server/utm-intent/shadow-resolver.js";
import { buildShadowTypedUtmResult } from "../server/utm-intent/typed-result.js";

const UTM30_POINTS = [[727250, 1219700], [728400, 1219700], [728400, 1219500], [728700, 1219500]];
const UTM50_POINTS = [[779271.176, 9720912.526], [779554.165, 9720912.526]];
const QUALITY_PASS = Object.freeze({ status: "PASS" });
const USER_CONFIRMED = Object.freeze({ status: "CONFIRMED" });

function typedFrom(rawText, points) {
  const shadowIntent = resolveShadowUtmIntent({ rawText }).shadowIntent;
  return {
    shadowIntent,
    typedResult: buildShadowTypedUtmResult({ shadowIntent, projectedCoordinates: points })
  };
}

function gate(input) {
  return evaluateShadowMigrationGate({
    qualityGate: QUALITY_PASS,
    userConfirmation: USER_CONFIRMED,
    ...input
  });
}

const utm30 = typedFrom("UTM WGS 1984 ZONE 30N", UTM30_POINTS);
const legacyUtm30 = {
  precisionMode: "utm30n-projected-x-y",
  transformedWgs84: structuredClone(utm30.typedResult.transformedWgs84)
};
const utm30Observation = observeUtmMigration({
  sample: "UTM30_burkina",
  legacyResult: legacyUtm30,
  typedResult: utm30.typedResult,
  shadowIntent: utm30.shadowIntent
});
const utm30Decision = gate({ ...utm30, legacyResult: legacyUtm30, migrationObservation: utm30Observation });
assert.equal(utm30Decision.migrationDecision, "V2_ALLOWED");
assert.equal(utm30Decision.legacyAvailable, true);
assert.equal(utm30Decision.v2Available, true);
assert.ok(utm30Decision.reason.includes("LEGACY_V2_MATCH"));
assert.deepEqual(Object.keys(utm30Decision), ["migrationDecision", "reason", "legacyAvailable", "v2Available"]);
console.log("PASS UTM30 MATCH -> V2_ALLOWED");

const utm50 = typedFrom("UTM WGS 1984 ZONA 50S", UTM50_POINTS);
const utm50Observation = observeUtmMigration({
  sample: "Indonesia_UTM50S",
  legacyResult: { precisionMode: "projected-x-y" },
  typedResult: utm50.typedResult,
  shadowIntent: utm50.shadowIntent
});
const utm50Decision = gate({ ...utm50, migrationObservation: utm50Observation });
assert.equal(utm50Decision.migrationDecision, "V2_ALLOWED");
assert.equal(utm50Decision.legacyAvailable, false);
assert.equal(utm50Decision.v2Available, true);
assert.ok(utm50Decision.reason.includes("V2_ONLY_NEW_CAPABILITY"));
console.log("PASS UTM50S V2_ONLY -> V2_ALLOWED");

const bftmIntent = resolveShadowUtmIntent({ rawText: "Projection BFTM / ITRF 2008" }).shadowIntent;
const legacyBftm = { precisionMode: "bftm-projected-x-y" };
const bftmObservation = observeUtmMigration({ sample: "BFTM", shadowIntent: bftmIntent });
const bftmDecision = gate({ legacyResult: legacyBftm, shadowIntent: bftmIntent, migrationObservation: bftmObservation });
assert.deepEqual(bftmDecision, {
  migrationDecision: "LEGACY_ONLY",
  reason: ["NOT_UTM"],
  legacyAvailable: true,
  v2Available: false
});
console.log("PASS BFTM NOT_UTM -> LEGACY_ONLY");

const mgrsIntent = resolveShadowUtmIntent({ rawText: "MGRS / UTM Grid Reference 47RLH 24469 42832" }).shadowIntent;
const mgrsObservation = observeUtmMigration({ sample: "MGRS", shadowIntent: mgrsIntent });
const mgrsDecision = gate({ legacyResult: { precisionMode: "mgrs" }, shadowIntent: mgrsIntent, migrationObservation: mgrsObservation });
assert.equal(mgrsDecision.migrationDecision, "BLOCKED");
assert.deepEqual(mgrsDecision.reason, ["UTM_PROJECTED_XY_BLOCKED"]);
console.log("PASS MGRS -> BLOCKED");

const kyrgyzIntent = resolveShadowUtmIntent({ rawText: "Gauss-Kruger rectangular coordinate system" }).shadowIntent;
const legacyKyrgyz = { precisionMode: "kyrgyzstan-gauss-kruger" };
const kyrgyzObservation = observeUtmMigration({ sample: "Kyrgyz_GK", shadowIntent: kyrgyzIntent });
const kyrgyzDecision = gate({ legacyResult: legacyKyrgyz, shadowIntent: kyrgyzIntent, migrationObservation: kyrgyzObservation });
assert.equal(kyrgyzDecision.migrationDecision, "LEGACY_ONLY");
assert.deepEqual(kyrgyzDecision.reason, ["NOT_UTM"]);
console.log("PASS Kyrgyz GK NOT_UTM -> LEGACY_ONLY");

const utm29 = typedFrom("UTM WGS 1984 ZONE 29N", UTM30_POINTS);
const conflictObservation = observeUtmMigration({
  sample: "Legacy_30N_vs_V2_29N",
  legacyResult: legacyUtm30,
  typedResult: utm29.typedResult,
  shadowIntent: utm29.shadowIntent
});
const conflictDecision = gate({ ...utm29, legacyResult: legacyUtm30, migrationObservation: conflictObservation });
assert.equal(conflictDecision.migrationDecision, "BLOCKED");
assert.deepEqual(conflictDecision.reason, ["CRS_CONFLICT"]);
console.log("PASS CRS conflict -> BLOCKED");

const shiftedLegacy = structuredClone(legacyUtm30);
shiftedLegacy.transformedWgs84[0].longitude += 2e-6;
const mismatchObservation = observeUtmMigration({
  sample: "UTM30_shifted_legacy",
  legacyResult: shiftedLegacy,
  typedResult: utm30.typedResult,
  shadowIntent: utm30.shadowIntent,
  tolerance: 1e-6
});
const mismatchDecision = gate({ ...utm30, legacyResult: shiftedLegacy, migrationObservation: mismatchObservation });
assert.equal(mismatchDecision.migrationDecision, "BLOCKED");
assert.deepEqual(mismatchDecision.reason, ["TRANSFORMATION_MISMATCH"]);
console.log("PASS transformation mismatch -> BLOCKED");

const legacyOnlyIntent = resolveShadowUtmIntent({ rawText: "UTM coordinates" }).shadowIntent;
const legacyOnlyObservation = observeUtmMigration({
  sample: "Legacy_without_confirmed_CRS",
  legacyResult: legacyUtm30,
  shadowIntent: legacyOnlyIntent
});
const legacyOnlyDecision = gate({
  legacyResult: legacyUtm30,
  shadowIntent: legacyOnlyIntent,
  migrationObservation: legacyOnlyObservation
});
assert.equal(legacyOnlyDecision.migrationDecision, "LEGACY_ONLY");
assert.deepEqual(legacyOnlyDecision.reason, ["CRS_NOT_CONFIRMED"]);
console.log("PASS incomplete CRS with legacy -> LEGACY_ONLY");

const unknownIntent = resolveShadowUtmIntent({ rawText: "779271.176, 9720912.526" }).shadowIntent;
const unknownObservation = observeUtmMigration({ sample: "Unknown_projected_XY", shadowIntent: unknownIntent });
const unknownDecision = gate({ shadowIntent: unknownIntent, migrationObservation: unknownObservation });
assert.equal(unknownDecision.migrationDecision, "BLOCKED");
assert.deepEqual(unknownDecision.reason, ["UNKNOWN_PROJECTED_XY"]);
console.log("PASS unknown projected XY -> BLOCKED");

const unconfirmedDecision = evaluateShadowMigrationGate({
  ...utm50,
  migrationObservation: utm50Observation,
  qualityGate: QUALITY_PASS,
  userConfirmation: { status: "PENDING" }
});
assert.equal(unconfirmedDecision.migrationDecision, "BLOCKED");
assert.deepEqual(unconfirmedDecision.reason, ["USER_CONFIRMATION_REQUIRED"]);
console.log("PASS V2_ONLY without user confirmation -> BLOCKED");

const legacyQualityReviewDecision = evaluateShadowMigrationGate({
  ...utm30,
  legacyResult: legacyUtm30,
  migrationObservation: utm30Observation,
  qualityGate: { status: "REVIEW" },
  userConfirmation: USER_CONFIRMED
});
assert.equal(legacyQualityReviewDecision.migrationDecision, "LEGACY_ONLY");
assert.deepEqual(legacyQualityReviewDecision.reason, ["QUALITY_GATE_NOT_PASS"]);
console.log("PASS UTM30 Quality Gate REVIEW -> LEGACY_ONLY");

const v2OnlyQualityReviewDecision = evaluateShadowMigrationGate({
  ...utm50,
  migrationObservation: utm50Observation,
  qualityGate: { status: "REVIEW" },
  userConfirmation: USER_CONFIRMED
});
assert.equal(v2OnlyQualityReviewDecision.migrationDecision, "BLOCKED");
assert.deepEqual(v2OnlyQualityReviewDecision.reason, ["QUALITY_GATE_NOT_PASS"]);
console.log("PASS UTM50S Quality Gate REVIEW -> BLOCKED");

function withProjectedMutation(typedResult, mutate) {
  const copy = structuredClone(typedResult);
  mutate(copy);
  return copy;
}

const outOfRangeProjectedDecision = gate({
  shadowIntent: utm50.shadowIntent,
  typedResult: withProjectedMutation(utm50.typedResult, result => { result.projectedCoordinates[0].easting = 99999; }),
  migrationObservation: utm50Observation
});
assert.equal(outOfRangeProjectedDecision.migrationDecision, "BLOCKED");
assert.deepEqual(outOfRangeProjectedDecision.reason, ["INVALID_PROJECTED_COORDINATES"]);
console.log("PASS out-of-range projected coordinate -> BLOCKED");

const nanProjected = structuredClone(utm50.typedResult);
nanProjected.projectedCoordinates[0].easting = Number.NaN;
const nanProjectedDecision = gate({ shadowIntent: utm50.shadowIntent, typedResult: nanProjected, migrationObservation: utm50Observation });
assert.equal(nanProjectedDecision.migrationDecision, "BLOCKED");
assert.deepEqual(nanProjectedDecision.reason, ["INVALID_PROJECTED_COORDINATES"]);
console.log("PASS NaN projected coordinate -> BLOCKED");

const infiniteProjected = structuredClone(utm50.typedResult);
infiniteProjected.projectedCoordinates[0].northing = Number.POSITIVE_INFINITY;
const infiniteProjectedDecision = gate({ shadowIntent: utm50.shadowIntent, typedResult: infiniteProjected, migrationObservation: utm50Observation });
assert.equal(infiniteProjectedDecision.migrationDecision, "BLOCKED");
assert.deepEqual(infiniteProjectedDecision.reason, ["INVALID_PROJECTED_COORDINATES"]);
console.log("PASS infinite projected coordinate -> BLOCKED");

const staleTransformed = withProjectedMutation(utm50.typedResult, result => { result.projectedCoordinates[0].easting += 10; });
const staleTransformedDecision = gate({ shadowIntent: utm50.shadowIntent, typedResult: staleTransformed, migrationObservation: utm50Observation });
assert.equal(staleTransformedDecision.migrationDecision, "BLOCKED");
assert.deepEqual(staleTransformedDecision.reason, ["STALE_TRANSFORMED_RESULT"]);
console.log("PASS changed X/Y with stale transformed WGS84 -> BLOCKED");

const mismatchedLength = withProjectedMutation(utm50.typedResult, result => { result.transformedWgs84.pop(); });
const mismatchedLengthDecision = gate({ shadowIntent: utm50.shadowIntent, typedResult: mismatchedLength, migrationObservation: utm50Observation });
assert.equal(mismatchedLengthDecision.migrationDecision, "BLOCKED");
assert.deepEqual(mismatchedLengthDecision.reason, ["POINT_COUNT_MISMATCH"]);
console.log("PASS projected/transformed point-count mismatch -> BLOCKED");

console.log("\nMigration Gate Schema:");
console.log(JSON.stringify(utm30Decision, null, 2));
console.log("\nUTM Shadow Migration Gate Regression: 17/17 PASS");
