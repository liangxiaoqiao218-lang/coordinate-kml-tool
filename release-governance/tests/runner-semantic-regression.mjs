import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, rm, unlink, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import {
  computeCanonicalGitCommitFingerprints,
  computeCanonicalGitReleaseFingerprints,
  computeFixtureSetFingerprint,
  computeProductionSourceFingerprint,
  computeReleaseGovernanceFingerprint,
  validateReleaseEvidenceBinding
} from "../evidence-binding.js";
import { classifyGoldenRun } from "../runner-semantics.js";

const execFileAsync = promisify(execFile);
const testDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(testDir, "..", "..");

const rule = {
  truthMaturity: "CONFIRMED_TRUTH",
  policyMaturity: "CONFIRMED_POLICY",
  releasePolicy: { policy: "REVIEW_REQUIRED" },
  evidenceMetadata: { expectedReviewGroupIndexes: [1] }
};
const sample = { golden_governance: rule };
const truthDiff = { severity: "BLOCKER", field: "pointCount", expected: 26, actual: 25 };
const passed = [];
const check = (id, fn) => { fn(); passed.push(id); };

check("G11", () => {
  const result = classifyGoldenRun({ sample, actual: { decisionState: "BLOCKED", finalizerEvaluated: true }, diffs: [] });
  assert.equal(result.gateSafetyStatus, "CONSERVATIVE_REVIEW");
  assert.equal(result.classifications.includes("UNSAFE_GATE_FAILURE"), false);
});
check("G12", () => {
  const result = classifyGoldenRun({ sample, actual: { decisionState: "BLOCKED", finalizerEvaluated: true }, diffs: [truthDiff] });
  assert.equal(result.truthStatus, "MISMATCH");
  assert.equal(result.gateSafetyStatus, "TRUTH_FAILURE_SAFE_FAIL_CLOSED");
  assert.equal(result.classifications.includes("UNSAFE_GATE_FAILURE"), false);
});
check("G13", () => {
  const result = classifyGoldenRun({ sample, actual: { decisionState: "AUTO_EXPORT", finalizerEvaluated: true }, diffs: [truthDiff] });
  assert.equal(result.gateSafetyStatus, "UNSAFE_GATE_FAILURE");
  assert.equal(result.releaseSeverity, "P0");
});
check("G14", () => {
  const result = classifyGoldenRun({ sample, actual: { decisionState: "BLOCKED", finalizerEvaluated: true }, diffs: [{ field: "reviewGroupIndexes" }] });
  assert.equal(result.truthStatus, "MATCH");
  assert.equal(result.metadataStatus, "MISMATCH");
  assert.equal(result.classifications.includes("METADATA_MISMATCH"), true);
});
check("G15", () => {
  const varianceSample = {
    golden_governance: { ...rule, providerVarianceMaturity: "PROVIDER_VARIANCE_TRACKED" }
  };
  const result = classifyGoldenRun({ sample: varianceSample, actual: { decisionState: "BLOCKED", finalizerEvaluated: true }, diffs: [truthDiff] });
  assert.equal(result.providerVarianceStatus, "DETECTED");
  assert.equal(result.classifications.includes("PROVIDER_VARIANCE"), true);
  assert.equal(result.gateSafetyStatus, "TRUTH_FAILURE_SAFE_FAIL_CLOSED");
});

const git = async (repoRoot, args) => (await execFileAsync("git", args, {
  cwd: repoRoot,
  encoding: "utf8",
  windowsHide: true
})).stdout.trim();

const canonicalRepo = await mkdtemp(path.join(os.tmpdir(), "geokit-canonical-regression-"));
try {
  await git(canonicalRepo, ["init"]);
  await git(canonicalRepo, ["config", "user.name", "GeoKit Regression"]);
  await git(canonicalRepo, ["config", "user.email", "regression@geokit.invalid"]);
  await git(canonicalRepo, ["config", "core.autocrlf", "true"]);
  await mkdir(path.join(canonicalRepo, "server"), { recursive: true });
  const demoPath = path.join(canonicalRepo, "server", "demo.js");
  const addedPath = path.join(canonicalRepo, "server", "added.js");
  const ignoredPath = path.join(canonicalRepo, "server", "unapproved.js");
  await writeFile(demoPath, "export const value = 'alpha';\n", "utf8");
  await git(canonicalRepo, ["add", "--", "server/demo.js"]);
  await git(canonicalRepo, ["commit", "-m", "base"]);
  const baseCommit = await git(canonicalRepo, ["rev-parse", "HEAD"]);
  const realIndexBefore = await git(canonicalRepo, ["write-tree"]);

  await writeFile(demoPath, "export const value = 'beta';\r\n", "utf8");
  const commitFromCrlfCheckout = await computeCanonicalGitCommitFingerprints({
    repoRoot: canonicalRepo,
    commit: baseCommit
  });
  const crlf = await computeCanonicalGitReleaseFingerprints({
    repoRoot: canonicalRepo,
    baseCommit,
    approvedPaths: ["server/demo.js"]
  });
  passed.push("G16_LF_CRLF_EQUIVALENCE");

  await writeFile(demoPath, "export const value = 'beta';\n", "utf8");
  const commitFromLfCheckout = await computeCanonicalGitCommitFingerprints({
    repoRoot: canonicalRepo,
    commit: baseCommit
  });
  assert.equal(commitFromCrlfCheckout.source.hash, commitFromLfCheckout.source.hash);
  assert.equal(commitFromCrlfCheckout.authority, "GIT_CANONICAL_RELEASE_TREE");
  passed.push("G17A_COMMIT_TREE_LINE_ENDING_INVARIANCE");
  const lf = await computeCanonicalGitReleaseFingerprints({
    repoRoot: canonicalRepo,
    baseCommit,
    approvedPaths: ["server/demo.js"]
  });
  assert.equal(crlf.source.hash, lf.source.hash);
  passed.push("G17_CANONICAL_HASH_EQUAL");

  const repeated = await computeCanonicalGitReleaseFingerprints({
    repoRoot: canonicalRepo,
    baseCommit,
    approvedPaths: ["server/demo.js"]
  });
  assert.equal(repeated.source.hash, lf.source.hash);
  passed.push("G18_REPEATABLE");

  await writeFile(demoPath, "export const value = 'gamma';\n", "utf8");
  const semanticChange = await computeCanonicalGitReleaseFingerprints({
    repoRoot: canonicalRepo,
    baseCommit,
    approvedPaths: ["server/demo.js"]
  });
  assert.notEqual(semanticChange.source.hash, lf.source.hash);
  passed.push("G19_SEMANTIC_CHANGE");

  await writeFile(demoPath, "export const value = 'beta';\n", "utf8");
  await writeFile(addedPath, "export const added = true;\r\n", "utf8");
  const addedForward = await computeCanonicalGitReleaseFingerprints({
    repoRoot: canonicalRepo,
    baseCommit,
    approvedPaths: ["server/demo.js", "server/added.js"]
  });
  const addedReverse = await computeCanonicalGitReleaseFingerprints({
    repoRoot: canonicalRepo,
    baseCommit,
    approvedPaths: ["server/added.js", "server/demo.js"]
  });
  assert.notEqual(addedForward.source.hash, lf.source.hash);
  assert.equal(addedForward.source.hash, addedReverse.source.hash);
  passed.push("G20_ADDED_FILE", "G21_PATH_ORDERING");

  await unlink(demoPath);
  const deleted = await computeCanonicalGitReleaseFingerprints({
    repoRoot: canonicalRepo,
    baseCommit,
    approvedPaths: ["server/demo.js"]
  });
  assert.notEqual(deleted.source.hash, lf.source.hash);
  passed.push("G22_DELETED_FILE");

  await writeFile(demoPath, "export const value = 'beta';\n", "utf8");
  await writeFile(ignoredPath, "export const ignored = true;\n", "utf8");
  const withUnapprovedWorktreeFile = await computeCanonicalGitReleaseFingerprints({
    repoRoot: canonicalRepo,
    baseCommit,
    approvedPaths: ["server/demo.js"]
  });
  assert.equal(withUnapprovedWorktreeFile.source.hash, lf.source.hash);
  passed.push("G23_UNAPPROVED_EXCLUSION");

  const realIndexAfter = await git(canonicalRepo, ["write-tree"]);
  assert.equal(realIndexAfter, realIndexBefore);
  assert.equal(await git(canonicalRepo, ["diff", "--cached", "--name-only"]), "");
  passed.push("G24_TEMPORARY_INDEX_ISOLATION");
} finally {
  await rm(canonicalRepo, { recursive: true, force: true });
}

const currentCanonical = await computeCanonicalGitCommitFingerprints({ repoRoot, commit: "HEAD" });
assert.equal(currentCanonical.authority, "GIT_CANONICAL_RELEASE_TREE");
assert.equal(currentCanonical.source.files.includes("scripts/coordinate-regression-runner.js"), true);
assert.equal(currentCanonical.source.files.some(file => file.startsWith("docs/")), false);
assert.equal(currentCanonical.source.files.some(file => file.startsWith("release-governance/")), false);
assert.equal(currentCanonical.source.files.some(file => file.startsWith("regression-samples/")), false);
assert.equal(currentCanonical.governance.files.includes("scripts/coordinate-regression-runner.js"), true);
assert.equal(currentCanonical.governance.files.some(file => file.startsWith("release-governance/")), true);
assert.equal(currentCanonical.governance.files.some(file => file.startsWith("docs/")), false);
assert.equal(currentCanonical.fixture.files.some(file => file.startsWith("regression-samples/")), true);
assert.equal(currentCanonical.fixture.files.some(file => file.startsWith("docs/")), false);
passed.push("G25_RELEASE_SCOPE_AUTHORITY");

const canonicalBinding = await validateReleaseEvidenceBinding({
  repoRoot,
  canonicalCommit: "HEAD",
  runtimeIdentity: { runtimeSourceSha256: currentCanonical.source.hash },
  frozenIdentity: {
    productionSourceHash: currentCanonical.source.hash,
    releaseGovernanceHash: currentCanonical.governance.hash,
    fixtureSetHash: currentCanonical.fixture.hash
  }
});
assert.equal(canonicalBinding.status, "BOUND");
assert.equal(canonicalBinding.sourceIdentityAuthority, "GIT_CANONICAL_RELEASE_TREE");
assert.equal(canonicalBinding.releaseIdentityAuthority, true);
passed.push("G26_CANONICAL_BINDING_AUTHORITY");

await assert.rejects(validateReleaseEvidenceBinding({
  repoRoot,
  runtimeIdentity: { runtimeSourceSha256: "0".repeat(64) },
  frozenIdentity: {
    productionSourceHash: "0".repeat(64),
    releaseGovernanceHash: "0".repeat(64),
    fixtureSetHash: "0".repeat(64)
  }
}), { code: "CANONICAL_RELEASE_IDENTITY_REQUIRED" });
passed.push("G27_LEGACY_AUTO_FALLBACK_BLOCKED");

const [legacySource, legacyGovernance, legacyFixture] = await Promise.all([
  computeProductionSourceFingerprint(repoRoot),
  computeReleaseGovernanceFingerprint(repoRoot),
  computeFixtureSetFingerprint(repoRoot)
]);
const legacyDiagnostic = await validateReleaseEvidenceBinding({
  repoRoot,
  legacyDiagnostic: true,
  runtimeIdentity: { runtimeSourceSha256: legacySource.hash },
  frozenIdentity: {
    productionSourceHash: legacySource.hash,
    releaseGovernanceHash: legacyGovernance.hash,
    fixtureSetHash: legacyFixture.hash
  }
});
assert.equal(legacyDiagnostic.status, "DIAGNOSTIC_ONLY");
assert.equal(legacyDiagnostic.sourceIdentityAuthority, "WORKING_TREE_BYTES_LEGACY_DIAGNOSTIC_NON_AUTHORITY");
assert.equal(legacyDiagnostic.releaseIdentityAuthority, false);
passed.push("G28_LEGACY_DIAGNOSTIC_NON_AUTHORITY");

console.log(`RUNNER_SEMANTIC_REGRESSION=PASS (${passed.length}/${passed.length})`);
console.log(`CASES=${passed.join(",")}`);
console.log("CROSS_PLATFORM_LINE_ENDING_INVARIANCE=PASS");
console.log("CANONICAL_IDENTITY_ONLY=PASS");
