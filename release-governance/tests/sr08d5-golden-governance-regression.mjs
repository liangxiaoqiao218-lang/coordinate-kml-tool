import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile, readdir } from "node:fs/promises";
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
  handwritten_dms_001: "112dc85cb721e66f171eb5e9a43d73f87127dff2579989bab5f9b06f77a58a70"
};

const readJson = async relativePath => JSON.parse(await readFile(path.join(repoRoot, relativePath), "utf8"));
const baseline = await readJson("COORDINATE_RECOGNITION_GOLDEN_BASELINE.json");
const governance = await readJson("release-governance/sr08d5-golden-policy.json");
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

async function listFixtureFiles() {
  const files = ["COORDINATE_RECOGNITION_GOLDEN_BASELINE.json", "COORDINATE_ERROR_LIBRARY.json"];
  const allowed = new Set([".json", ".yaml", ".yml", ".jpg", ".jpeg", ".png", ".webp"]);
  async function walk(relativeDirectory) {
    const entries = await readdir(path.join(repoRoot, relativeDirectory), { withFileTypes: true });
    for (const entry of entries) {
      const relativePath = path.posix.join(relativeDirectory.replaceAll("\\", "/"), entry.name);
      if (entry.isDirectory()) await walk(relativePath);
      else if (allowed.has(path.extname(entry.name).toLowerCase())) files.push(relativePath);
    }
  }
  await walk("regression-samples");
  return files.sort((left, right) => left.toLowerCase().localeCompare(right.toLowerCase()));
}

async function hashFileSet(files) {
  const lines = [];
  for (const relativePath of files) {
    const bytes = await readFile(path.join(repoRoot, relativePath));
    lines.push(`${relativePath}:${createHash("sha256").update(bytes).digest("hex")}`);
  }
  return createHash("sha256").update(lines.join("\n")).digest("hex");
}

check("G16", () => {});
const fixtureFiles = await listFixtureFiles();
assert.equal(fixtureFiles.length, 52);
assert.equal(await hashFileSet(fixtureFiles), expectedFixtureHash);

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
