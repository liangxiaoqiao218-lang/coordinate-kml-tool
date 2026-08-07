import assert from "node:assert/strict";

import { compareLegacyAndCanonicalUtmKml } from "../server/utm-intent/export-compare.js";
import { createUtmMigrationControl } from "../server/utm-intent/migration-control.js";
import { evaluateShadowMigrationGate } from "../server/utm-intent/migration-gate.js";
import {
  createUtmMigrationObservationReport,
  observeUtmMigration
} from "../server/utm-intent/migration-observer.js";
import { resolveShadowUtmIntent } from "../server/utm-intent/shadow-resolver.js";
import { buildShadowTypedUtmResult } from "../server/utm-intent/typed-result.js";

const UTM30_POINTS = [
  [727250, 1219700],
  [728400, 1219700],
  [728400, 1219500],
  [728700, 1219500]
];
const UTM50_POINTS = [
  [779271.176, 9720912.526],
  [779554.165, 9720912.526],
  [779554.165, 9720734.464],
  [779271.176, 9720734.464]
];
const DOCUMENT_NAME = "UTM30 Migration Fixture";
const PLACEMARK_NAME = "UTM30 Boundary";

function typedFrom(rawText, points) {
  const shadowIntent = resolveShadowUtmIntent({ rawText }).shadowIntent;
  return {
    shadowIntent,
    typedResult: buildShadowTypedUtmResult({ shadowIntent, projectedCoordinates: points })
  };
}

function legacyPolygonKml(points, { geometry = "Polygon", shiftFirstLongitude = 0 } = {}) {
  const normalized = points.map((point, index) => ({
    longitude: Number(point.longitude) + (index === 0 ? shiftFirstLongitude : 0),
    latitude: Number(point.latitude)
  }));
  const coordinates = geometry === "Polygon" ? [...normalized, normalized[0]] : normalized;
  return `<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <name>${DOCUMENT_NAME}</name>
    <Placemark>
      <name>${PLACEMARK_NAME}</name>
      <${geometry}><coordinates>${coordinates.map(point => `${point.longitude},${point.latitude},0`).join(" ")}</coordinates></${geometry}>
    </Placemark>
  </Document>
</kml>`;
}

const utm30 = typedFrom("UTM WGS 1984 ZONE 30N", UTM30_POINTS);
const legacyUtm30 = {
  precisionMode: "utm30n-projected-x-y",
  transformedWgs84: structuredClone(utm30.typedResult.transformedWgs84)
};
const matchingLegacyKml = legacyPolygonKml(legacyUtm30.transformedWgs84);
const exportMatch = compareLegacyAndCanonicalUtmKml({
  legacyKml: matchingLegacyKml,
  typedResult: utm30.typedResult,
  geometry: "Polygon",
  documentName: DOCUMENT_NAME,
  placemarkName: PLACEMARK_NAME
});
assert.equal(exportMatch.status, "MATCH");
assert.equal(exportMatch.maximumDifference, 0);
assert.equal(exportMatch.geometryTypes[0], "Polygon");
console.log("PASS legacy vs canonical UTM30 KML semantic MATCH");

const exportCoordinateMismatch = compareLegacyAndCanonicalUtmKml({
  legacyKml: legacyPolygonKml(legacyUtm30.transformedWgs84, { shiftFirstLongitude: 2e-6 }),
  typedResult: utm30.typedResult,
  geometry: "Polygon",
  documentName: DOCUMENT_NAME,
  placemarkName: PLACEMARK_NAME,
  tolerance: 1e-6
});
assert.equal(exportCoordinateMismatch.status, "EXPORT_COMPARE_FAILED");
assert.equal(exportCoordinateMismatch.reason, "COORDINATE_MISMATCH");
console.log("PASS KML coordinate mismatch is blocked");

const exportGeometryMismatch = compareLegacyAndCanonicalUtmKml({
  legacyKml: legacyPolygonKml(legacyUtm30.transformedWgs84, { geometry: "LineString" }),
  typedResult: utm30.typedResult,
  geometry: "Polygon",
  documentName: DOCUMENT_NAME,
  placemarkName: PLACEMARK_NAME
});
assert.equal(exportGeometryMismatch.status, "EXPORT_COMPARE_FAILED");
assert.equal(exportGeometryMismatch.reason, "GEOMETRY_MISMATCH");
console.log("PASS KML geometry mismatch is blocked");

const inconsistentTypedResult = structuredClone(utm30.typedResult);
inconsistentTypedResult.typedUtmIntent.epsg = "EPSG:32730";
const invalidTypedExport = compareLegacyAndCanonicalUtmKml({
  legacyKml: matchingLegacyKml,
  typedResult: inconsistentTypedResult,
  geometry: "Polygon",
  documentName: DOCUMENT_NAME,
  placemarkName: PLACEMARK_NAME
});
assert.equal(invalidTypedExport.status, "EXPORT_COMPARE_FAILED");
assert.equal(invalidTypedExport.reason, "INVALID_EXPORT_INPUT");
console.log("PASS inconsistent Typed CRS cannot enter canonical KML compare");

const matchObservation = observeUtmMigration({
  sample: "UTM30_MATCH",
  legacyResult: legacyUtm30,
  typedResult: utm30.typedResult,
  shadowIntent: utm30.shadowIntent
});
const gateAllowed = evaluateShadowMigrationGate({
  legacyResult: legacyUtm30,
  shadowIntent: utm30.shadowIntent,
  typedResult: utm30.typedResult,
  migrationObservation: matchObservation,
  qualityGate: { status: "PASS" },
  userConfirmation: { status: "CONFIRMED" }
});
assert.equal(gateAllowed.migrationDecision, "V2_ALLOWED");

const migrationControl = createUtmMigrationControl();
assert.equal(migrationControl.getState().mode, "legacy");
assert.equal(migrationControl.resolveAuthority({ migrationGate: gateAllowed, exportComparison: exportMatch }).authority, "legacy");
console.log("PASS migration control defaults to legacy authority");

assert.throws(
  () => createUtmMigrationControl({ initialMode: "controlled" }),
  /cannot be used as an initial migration mode/
);
console.log("PASS migration control cannot initialize in controlled mode");

migrationControl.transition("shadow", { reason: "phase_5_1_shadow_observation" });
const shadowAuthority = migrationControl.resolveAuthority({ migrationGate: gateAllowed, exportComparison: exportMatch });
assert.equal(shadowAuthority.authority, "legacy");
assert.equal(shadowAuthority.canonicalObserved, true);
console.log("PASS shadow mode observes canonical while legacy remains authoritative");

assert.throws(
  () => migrationControl.transition("controlled", { reason: "hold_must_not_enable", readinessDecision: "HOLD_LEGACY" }),
  /READY_FOR_CONTROLLED_MIGRATION/
);
assert.equal(migrationControl.getState().mode, "shadow");
console.log("PASS HOLD_LEGACY cannot enter controlled mode");

migrationControl.transition("controlled", {
  reason: "rollback_drill_controlled_entry",
  readinessDecision: "READY_FOR_CONTROLLED_MIGRATION"
});
const controlledAuthority = migrationControl.resolveAuthority({ migrationGate: gateAllowed, exportComparison: exportMatch });
assert.equal(controlledAuthority.authority, "canonical");
console.log("PASS drill-only controlled mode requires Gate and Export MATCH");

const blockedControlledAuthority = migrationControl.resolveAuthority({
  migrationGate: gateAllowed,
  exportComparison: exportCoordinateMismatch
});
assert.equal(blockedControlledAuthority.authority, "blocked");
assert.equal(blockedControlledAuthority.reason, "EXPORT_COMPARE_REQUIRED");
console.log("PASS controlled mode blocks canonical authority on Export mismatch");

migrationControl.transition("legacy", { reason: "rollback_drill_restore_legacy" });
const rollbackAuthority = migrationControl.resolveAuthority({ migrationGate: gateAllowed, exportComparison: exportMatch });
assert.equal(rollbackAuthority.authority, "legacy");
assert.equal(migrationControl.getState().audit.at(-1).from, "controlled");
assert.equal(migrationControl.getState().audit.at(-1).to, "legacy");
console.log("PASS Rollback Drill controlled -> legacy restores legacy authority");

const utm50 = typedFrom("UTM WGS 1984 ZONA 50S", UTM50_POINTS);
const v2OnlyObservation = observeUtmMigration({
  sample: "UTM50S_V2_ONLY",
  typedResult: utm50.typedResult,
  shadowIntent: utm50.shadowIntent
});
const incompleteIntent = resolveShadowUtmIntent({ rawText: "UTM coordinates" }).shadowIntent;
const legacyOnlyObservation = observeUtmMigration({
  sample: "UTM30_LEGACY_ONLY",
  legacyResult: legacyUtm30,
  shadowIntent: incompleteIntent
});
const utm29 = typedFrom("UTM WGS 1984 ZONE 29N", UTM30_POINTS);
const conflictObservation = observeUtmMigration({
  sample: "UTM30_CRS_CONFLICT",
  legacyResult: legacyUtm30,
  typedResult: utm29.typedResult,
  shadowIntent: utm29.shadowIntent
});
const shiftedLegacy = structuredClone(legacyUtm30);
shiftedLegacy.transformedWgs84[0].longitude += 2e-6;
const mismatchObservation = observeUtmMigration({
  sample: "UTM30_TRANSFORMATION_MISMATCH",
  legacyResult: shiftedLegacy,
  typedResult: utm30.typedResult,
  shadowIntent: utm30.shadowIntent,
  tolerance: 1e-6
});
const observationReport = createUtmMigrationObservationReport([
  matchObservation,
  v2OnlyObservation,
  legacyOnlyObservation,
  conflictObservation,
  mismatchObservation
]);
assert.deepEqual(observationReport.statusCounts, {
  MATCH: 1,
  V2_ONLY: 1,
  LEGACY_ONLY: 1,
  CRS_CONFLICT: 1,
  TRANSFORMATION_MISMATCH: 1
});
assert.equal(observationReport.shadowOnly, true);
console.log("PASS Shadow Observation Report records all five migration statuses");

console.log("\nExport Compare: 4/4 PASS");
console.log("Migration Kill Switch: 6/6 PASS");
console.log("Rollback Drill: PASS");
console.log("Shadow Observation Report:");
console.log(JSON.stringify({
  schemaVersion: observationReport.schemaVersion,
  shadowOnly: observationReport.shadowOnly,
  sampleCount: observationReport.sampleCount,
  statusCounts: observationReport.statusCounts
}, null, 2));
console.log("\nMigration Infrastructure Regression: 12/12 PASS");
