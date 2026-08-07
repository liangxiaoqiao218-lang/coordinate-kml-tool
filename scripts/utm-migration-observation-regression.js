import assert from "node:assert/strict";

import { resolveShadowUtmIntent } from "../server/utm-intent/shadow-resolver.js";
import { buildShadowTypedUtmResult } from "../server/utm-intent/typed-result.js";
import { observeUtmMigration, createUtmMigrationObservationReport } from "../server/utm-intent/migration-observer.js";

const UTM30_POINTS = [[727250, 1219700], [728400, 1219700], [728400, 1219500], [728700, 1219500]];
const UTM50_POINTS = [[779271.176, 9720912.526], [779554.165, 9720912.526]];

function intentFrom(rawText) {
  return resolveShadowUtmIntent({ rawText }).shadowIntent;
}

function typedFrom(rawText, points) {
  const shadowIntent = intentFrom(rawText);
  return { shadowIntent, typedResult: buildShadowTypedUtmResult({ shadowIntent, projectedCoordinates: points }) };
}

const utm30 = typedFrom("UTM WGS 1984 ZONE 30N", UTM30_POINTS);
const legacyUtm30 = {
  precisionMode: "utm30n-projected-x-y",
  transformedWgs84: structuredClone(utm30.typedResult.transformedWgs84)
};
const observations = [];

const match = observeUtmMigration({
  sample: "UTM30_burkina",
  legacyResult: legacyUtm30,
  typedResult: utm30.typedResult,
  shadowIntent: utm30.shadowIntent
});
assert.equal(match.migrationStatus, "MATCH");
assert.equal(match.comparison.maxDifference, 0);
observations.push(match);
console.log("PASS UTM30 MATCH");

const utm50 = typedFrom("UTM WGS 1984 ZONA 50S", UTM50_POINTS);
const v2Only = observeUtmMigration({
  sample: "Indonesia_UTM50S",
  legacyResult: { precisionMode: "projected-x-y" },
  typedResult: utm50.typedResult,
  shadowIntent: utm50.shadowIntent
});
assert.equal(v2Only.migrationStatus, "V2_ONLY");
assert.equal(v2Only.v2.epsg, "EPSG:32750");
observations.push(v2Only);
console.log("PASS UTM50S V2_ONLY");

const legacyOnlyIntent = intentFrom("UTM coordinates");
const legacyOnly = observeUtmMigration({
  sample: "Legacy_without_confirmed_CRS",
  legacyResult: legacyUtm30,
  typedResult: null,
  shadowIntent: legacyOnlyIntent
});
assert.equal(legacyOnly.migrationStatus, "LEGACY_ONLY");
observations.push(legacyOnly);
console.log("PASS incomplete CRS evidence LEGACY_ONLY");

const utm29 = typedFrom("UTM WGS 1984 ZONE 29N", UTM30_POINTS);
const crsConflict = observeUtmMigration({
  sample: "Legacy_30N_vs_V2_29N",
  legacyResult: legacyUtm30,
  typedResult: utm29.typedResult,
  shadowIntent: utm29.shadowIntent
});
assert.equal(crsConflict.migrationStatus, "CRS_CONFLICT");
assert.ok(crsConflict.comparison.differences.some(item => item.field === "zone"));
observations.push(crsConflict);
console.log("PASS CRS disagreement CRS_CONFLICT");

const shiftedLegacy = structuredClone(legacyUtm30);
shiftedLegacy.transformedWgs84[0].longitude += 2e-6;
const transformationMismatch = observeUtmMigration({
  sample: "UTM30_shifted_legacy",
  legacyResult: shiftedLegacy,
  typedResult: utm30.typedResult,
  shadowIntent: utm30.shadowIntent,
  tolerance: 1e-6
});
assert.equal(transformationMismatch.migrationStatus, "TRANSFORMATION_MISMATCH");
observations.push(transformationMismatch);
console.log("PASS coordinate difference TRANSFORMATION_MISMATCH");

for (const testCase of [
  { sample: "BFTM", rawText: "Projection BFTM / ITRF 2008", disposition: "NOT_UTM" },
  { sample: "MGRS", rawText: "MGRS / UTM Grid Reference 47RLH 24469 42832", disposition: "BLOCKED" },
  { sample: "Kyrgyz_GK", rawText: "Gauss-Kruger rectangular coordinate system", disposition: "NOT_UTM" }
]) {
  const shadowIntent = intentFrom(testCase.rawText);
  const typedResult = buildShadowTypedUtmResult({ shadowIntent, projectedCoordinates: UTM30_POINTS });
  const observation = observeUtmMigration({ sample: testCase.sample, shadowIntent, typedResult });
  assert.equal(typedResult, null);
  assert.equal(observation.migrationStatus, null);
  assert.equal(observation.disposition, testCase.disposition);
  observations.push(observation);
  console.log(`PASS ${testCase.sample} ${testCase.disposition}`);
}

const report = createUtmMigrationObservationReport(observations);
assert.deepEqual(report.statusCounts, {
  MATCH: 1,
  V2_ONLY: 1,
  LEGACY_ONLY: 1,
  CRS_CONFLICT: 1,
  TRANSFORMATION_MISMATCH: 1
});
assert.deepEqual(report.dispositionCounts, { OBSERVE: 5, NOT_UTM: 2, BLOCKED: 1 });
assert.equal(report.shadowOnly, true);

console.log("\nMigration Summary:");
console.log(JSON.stringify({
  schemaVersion: report.schemaVersion,
  shadowOnly: report.shadowOnly,
  sampleCount: report.sampleCount,
  statusCounts: report.statusCounts,
  dispositionCounts: report.dispositionCounts,
  productionPathsChanged: false
}, null, 2));
console.log("\nUTM Migration Observation Regression: 8/8 PASS");
