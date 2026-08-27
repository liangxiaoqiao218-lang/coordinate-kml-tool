import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, rm, unlink, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { computeCanonicalGitReleaseFingerprints } from "../evidence-binding.js";
import { classifyGoldenRun } from "../runner-semantics.js";

const execFileAsync = promisify(execFile);

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
  const crlf = await computeCanonicalGitReleaseFingerprints({
    repoRoot: canonicalRepo,
    baseCommit,
    approvedPaths: ["server/demo.js"]
  });
  passed.push("G16_LF_CRLF_EQUIVALENCE");

  await writeFile(demoPath, "export const value = 'beta';\n", "utf8");
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

console.log(`RUNNER_SEMANTIC_REGRESSION=PASS (${passed.length}/${passed.length})`);
console.log(`CASES=${passed.join(",")}`);
