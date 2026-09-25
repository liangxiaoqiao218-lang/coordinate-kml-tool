import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  applyGoldenGovernance,
  createConfirmedTruthHash,
  validateGoldenGovernance
} from "../runner-semantics.js";

const testDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(testDir, "..", "..");
const expectedFixtureHash = "ada8e2c6827fff89f5aebdd4c0dfe5ba7ee79b34eacd070ae8801babfed9f0b2";
const approvedTruthHashes = {
  utm30_burkina_003: "cfa2a035eec483bc435430d237c359b0ae57d948615b6e1cbc36b4961bf96775",
  point_az_dms_table_001: "075d6f9108cb26ee3686cc978e9d47b384a22af2501486e32d5421db0bc93950",
  handwritten_dms_001: "112dc85cb721e66f171eb5e9a43d73f87127dff2579989bab5f9b06f77a58a70",
  "madagascar-cadastral-real-001": "5c52ae6afac0589dc9f628c83fd30043b559544a7e35aeda2c659b1b7f8cd655",
  "indonesia-dms-real-001": "615d5acb906c4ff07cd767983336a3c1a4d99fcd85e34c327eb674655cdc7044",
  "indonesia-projected-real-002": "1cc1b22b83a13f634a32f0f37772a7d1c3695f5c4394166a1d9f8819ff75cd03"
};

const readJson = async relativePath => JSON.parse(await readFile(path.join(repoRoot, relativePath), "utf8"));
const baseline = await readJson("COORDINATE_RECOGNITION_GOLDEN_BASELINE.json");
const governance = await readJson("release-governance/sr08d5-golden-policy.json");
const madagascarMigration = await readJson("release-governance/madagascar-golden-migration-v1.json");
const recoveryGolden = await readJson("regression-samples/production-recognition-recovery-p0/golden-records.json");
assert.deepEqual(validateGoldenGovernance(governance), []);

const baselineById = new Map(baseline.samples.map(sample => [sample.sample_id, sample]));
const governed = sampleId => applyGoldenGovernance(baselineById.get(sampleId), governance);
const passed = [];
const check = (id, fn) => { fn(); passed.push(id); };

check("G01", () => {
  const sample = governed("utm30_burkina_003");
  assert.equal(sample.expected_requires_review, true);
  assert.equal(sample.expected_kml_ready, false);
  assert.deepEqual(sample.expected_decision_states, ["BLOCKED"]);
  assert.equal(sample.golden_governance.releasePolicy.geometryBlockerMustRemainActive, true);
});
check("G02", () => assert.equal(createConfirmedTruthHash(baselineById.get("utm30_burkina_003")), approvedTruthHashes.utm30_burkina_003));
check("G03", () => {
  const sample = governed("point_az_dms_table_001");
  assert.equal(sample.golden_governance.releasePolicy.policy, "REVIEW_REQUIRED_UNTIL_CONFIRMATION");
  assert.equal(sample.expected_requires_review, true);
  assert.equal(sample.expected_kml_ready, false);
  assert.equal(sample.expected_confirmation_status, "pending");
  assert.equal(sample.expected_family_policy_id, "POINT_AZ_TEMPORARY_REVIEW_POLICY");
  assert.equal(sample.expected_family_policy_version, "1");
});
check("G04", () => assert.equal(createConfirmedTruthHash(baselineById.get("point_az_dms_table_001")), approvedTruthHashes.point_az_dms_table_001));
check("G05", () => assert.equal(createConfirmedTruthHash(baselineById.get("handwritten_dms_001")), approvedTruthHashes.handwritten_dms_001));
check("G06", () => assert.equal(governance.cases.handwritten_dms_001.providerVarianceMaturity, "PROVIDER_VARIANCE_TRACKED"));
check("G07", () => assert.deepEqual(governed("low_clarity_blurry_dms_001").expected_review_group_indexes, [1]));
check("G08", () => {
  const rule = governance.cases.low_clarity_blurry_dms_001;
  assert.equal(rule.truthMaturity, "MISSING_TRUTH");
  assert.equal(rule.policyMaturity, "CONFIRMED_POLICY");
  assert.equal(Object.hasOwn(rule, "approvedTruthSha256"), false);
  assert.equal(Object.hasOwn(rule.releasePolicy, "expectedCoordinateType"), false);
  assert.equal(Object.hasOwn(rule.releasePolicy, "expectedPointCount"), false);
  assert.equal(Object.hasOwn(rule.releasePolicy, "expectedGeometry"), false);
});
check("G09", () => assert.deepEqual(governed("oblique_dms_001").expected_review_group_indexes, [1, 2]));
check("G10", () => {
  const rule = governance.cases.oblique_dms_001;
  assert.equal(rule.truthMaturity, "MISSING_TRUTH");
  assert.equal(rule.policyMaturity, "CONFIRMED_POLICY");
  assert.equal(Object.hasOwn(rule, "approvedTruthSha256"), false);
});

check("G11", () => {
  for (const sampleId of [
    "madagascar-cadastral-real-001",
    "indonesia-dms-real-001",
    "indonesia-projected-real-002"
  ]) {
    const sample = governed(sampleId);
    const rule = governance.cases[sampleId];
    assert.equal(rule.supersession.status, "APPROVED");
    assert.equal(rule.supersession.supersedesPolicy, "AVAILABLE_WITH_WARNING");
    assert.equal(rule.supersession.authority, "V8_STRICT_FINALIZER");
    assert.equal(rule.supersession.authorityCommit, "3c0f52cb49158c239b360acc60b5f9a326a92beb");
    assert.equal(rule.supersession.productionBaseline, "40abc5e08e0589189994604697a2b232e1c9dfcf");
    assert.equal(rule.supersession.coordinateTruthChanged, false);
    assert.equal(rule.releasePolicy.policy, "REVIEW_REQUIRED");
    assert.equal(sample.expected_requires_review, true);
    assert.equal(sample.expected_kml_ready, false);
    assert.equal(sample.expected_confirmation_status, "pending");
    assert.equal(sample.expected_quality_gate_status, "review_required");
    assert.deepEqual(sample.expected_decision_states, ["REVIEW_REQUIRED"]);
    assert.deepEqual(sample.expected_review_group_indexes,
      sampleId === "madagascar-cadastral-real-001"
        ? Array.from({ length: 32 }, (_, index) => index + 1)
        : [1]);
  }
});

check("G12", () => {
  const metadata = governance.cases["madagascar-cadastral-real-001"].evidenceMetadata;
  assert.equal(metadata.sourceCrs, "EPSG:29702");
  assert.equal(metadata.axisOrder, "easting_northing");
  assert.equal(metadata.sourceCrsAuthorityScope, "GOLDEN_REPLAY_TRANSFORM_METADATA_ONLY");
  assert.equal(metadata.authoritativeAcquisitionCrsEvidence, false);
  assert.equal(madagascarMigration.newExpectation.kmlPolicy, "REVIEW_REQUIRED_KML_CLOSED");
  assert.equal(madagascarMigration.newExpectation.authoritativeAcquisitionCrsEvidence, false);
});

check("G13", () => {
  for (const sampleId of ["indonesia-dms-real-001", "indonesia-projected-real-002"]) {
    const metadata = governance.cases[sampleId].evidenceMetadata;
    assert.equal(metadata.visibleProjectedCrsEvidence, "UTM WGS 1984 ZONA 50S");
    assert.equal(metadata.visibleProjectedCrsAuthority, "APPROVED_REPLAY_ACQUISITION_EVIDENCE");
  }
});

check("G14", () => {
  const records = new Map(recoveryGolden.records.map(record => [record.id, record]));
  assert.equal(records.get("madagascar-cadastral-real-001").kmlBehavior,
    "review_required_kml_closed_missing_authoritative_acquisition_crs");
  assert.equal(records.get("indonesia-dms-real-001").kmlBehavior, "review_required_kml_closed");
  assert.equal(records.get("indonesia-projected-real-002").kmlBehavior, "review_required_kml_closed");
});

check("G16", () => {
  assert.equal(governance.fixtureSetHash, expectedFixtureHash);
});

check("G17", () => {
  for (const [sampleId, approvedHash] of Object.entries(approvedTruthHashes)) {
    assert.equal(createConfirmedTruthHash(baselineById.get(sampleId)), approvedHash);
    assert.equal(governance.cases[sampleId].approvedTruthSha256, approvedHash);
  }
});

console.log(`GOVERNANCE_REGRESSION=PASS (${passed.length}/${passed.length})`);
console.log(`CASES=${passed.join(",")}`);
console.log(`FIXTURE_SET_HASH=${expectedFixtureHash}`);
console.log("CONFIRMED_TRUTH_DIFF=0");
