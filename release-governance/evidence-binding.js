import { createHash } from "node:crypto";
import { execFile } from "node:child_process";
import { mkdtemp, readFile, readdir, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";

const HASH_PATTERN = /^[a-f0-9]{64}$/i;
const PRODUCTION_ROOT_EXTENSIONS = new Set([".js", ".html", ".json", ".jpg", ".jpeg", ".png", ".svg"]);
const FIXTURE_EXTENSIONS = new Set([".json", ".yaml", ".yml", ".jpg", ".jpeg", ".png", ".webp"]);
const execFileAsync = promisify(execFile);

function normalizeRelativePath(value) {
  return String(value || "").replaceAll("\\", "/");
}

function compareCanonicalPaths(left, right) {
  const normalizedLeft = normalizeRelativePath(left).normalize("NFC");
  const normalizedRight = normalizeRelativePath(right).normalize("NFC");
  const foldedLeft = normalizedLeft.toLowerCase();
  const foldedRight = normalizedRight.toLowerCase();
  if (foldedLeft < foldedRight) return -1;
  if (foldedLeft > foldedRight) return 1;
  if (normalizedLeft < normalizedRight) return -1;
  if (normalizedLeft > normalizedRight) return 1;
  return 0;
}

function uniqueCanonicalPaths(relativePaths) {
  return [...new Set(relativePaths.map(value => normalizeRelativePath(value).normalize("NFC")))]
    .sort(compareCanonicalPaths);
}

async function walkFiles(root, relativeDirectory) {
  const absoluteDirectory = path.join(root, relativeDirectory);
  const entries = await readdir(absoluteDirectory, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const relativePath = path.posix.join(normalizeRelativePath(relativeDirectory), entry.name);
    if (entry.isDirectory()) files.push(...await walkFiles(root, relativePath));
    else if (entry.isFile()) files.push(relativePath);
  }
  return files;
}

async function hashFileSet(repoRoot, relativePaths) {
  // Historical working-byte fingerprints retain their original ordering for
  // evidence continuity. New release identities use uniqueCanonicalPaths()
  // against Git blobs instead.
  const paths = [...new Set(relativePaths.map(normalizeRelativePath))]
    .sort((left, right) => left.toLowerCase().localeCompare(right.toLowerCase()));
  const lines = [];
  for (const relativePath of paths) {
    const bytes = await readFile(path.join(repoRoot, relativePath));
    const fileHash = createHash("sha256").update(bytes).digest("hex");
    lines.push(`${relativePath}:${fileHash}`);
  }
  return {
    hash: createHash("sha256").update(lines.join("\n")).digest("hex"),
    fileCount: paths.length,
    files: paths
  };
}

function splitNullTerminated(value) {
  return String(value || "").split("\0").filter(Boolean).map(normalizeRelativePath);
}

async function runGit(repoRoot, args, { env = {}, encoding = "utf8" } = {}) {
  const result = await execFileAsync("git", args, {
    cwd: repoRoot,
    env: { ...process.env, ...env },
    encoding,
    maxBuffer: 128 * 1024 * 1024,
    windowsHide: true
  });
  return result.stdout;
}

function productionPathsFromGitTree(relativePaths) {
  const rootRuntimeFiles = relativePaths
    .filter(relativePath => !relativePath.includes("/"))
    .filter(relativePath => PRODUCTION_ROOT_EXTENSIONS.has(path.posix.extname(relativePath).toLowerCase()))
    .filter(relativePath => !relativePath.startsWith("COORDINATE_"));
  const serverFiles = relativePaths.filter(relativePath => relativePath.startsWith("server/"));
  const dataFiles = relativePaths.filter(relativePath => relativePath.startsWith("data/"));
  const scriptFiles = relativePaths.filter(relativePath => (
    relativePath.startsWith("scripts/") && path.posix.extname(relativePath).toLowerCase() === ".js"
  ));
  return uniqueCanonicalPaths([...rootRuntimeFiles, ...serverFiles, ...dataFiles, ...scriptFiles]);
}

function governancePathsFromGitTree(relativePaths) {
  return uniqueCanonicalPaths([
    "scripts/coordinate-regression-runner.js",
    ...relativePaths.filter(relativePath => (
      relativePath.startsWith("release-governance/")
      && !/(^|\/)transient(?:\/|$)/i.test(relativePath)
    ))
  ]).filter(relativePath => relativePaths.includes(relativePath));
}

function fixturePathsFromGitTree(relativePaths) {
  const fixtureFiles = relativePaths.filter(relativePath => (
    relativePath.startsWith("regression-samples/")
    && FIXTURE_EXTENSIONS.has(path.posix.extname(relativePath).toLowerCase())
  ));
  return uniqueCanonicalPaths([
    "COORDINATE_RECOGNITION_GOLDEN_BASELINE.json",
    "COORDINATE_ERROR_LIBRARY.json",
    ...fixtureFiles
  ]).filter(relativePath => relativePaths.includes(relativePath));
}

async function hashGitIndexFileSet(repoRoot, indexEnv, relativePaths) {
  const paths = uniqueCanonicalPaths(relativePaths);
  const lines = [];
  for (const relativePath of paths) {
    const bytes = await runGit(repoRoot, ["show", `:${relativePath}`], {
      env: indexEnv,
      encoding: "buffer"
    });
    const fileHash = createHash("sha256").update(bytes).digest("hex");
    lines.push(`${relativePath}:${fileHash}`);
  }
  return {
    hash: createHash("sha256").update(lines.join("\n")).digest("hex"),
    fileCount: paths.length,
    files: paths
  };
}

async function indexEntryModes(repoRoot, indexEnv) {
  const output = await runGit(repoRoot, ["ls-files", "--stage", "-z"], { env: indexEnv });
  return splitNullTerminated(output).map(entry => {
    const separator = entry.indexOf("\t");
    const metadata = entry.slice(0, separator).split(/\s+/);
    return { mode: metadata[0], path: normalizeRelativePath(entry.slice(separator + 1)) };
  });
}

/**
 * Build the proposed release tree in an isolated temporary Git index and hash
 * Git's canonical blob bytes. The real index and OS-specific checkout bytes are
 * never used as release identity authority.
 */
export async function computeCanonicalGitReleaseFingerprints({ repoRoot, baseCommit, approvedPaths }) {
  const approved = uniqueCanonicalPaths(approvedPaths || []);
  if (!approved.length) throw new TypeError("APPROVED_RELEASE_PATHS_REQUIRED");
  const temporaryDirectory = await mkdtemp(path.join(os.tmpdir(), "geokit-canonical-index-"));
  const temporaryIndex = path.join(temporaryDirectory, "index");
  const indexEnv = { GIT_INDEX_FILE: temporaryIndex };
  try {
    await runGit(repoRoot, ["read-tree", String(baseCommit || "HEAD")], { env: indexEnv });
    await runGit(repoRoot, ["add", "-A", "--", ...approved], { env: indexEnv });

    const changedPaths = uniqueCanonicalPaths(splitNullTerminated(await runGit(
      repoRoot,
      ["diff", "--cached", "--name-only", "-z", String(baseCommit || "HEAD"), "--"],
      { env: indexEnv }
    )));
    const unapprovedPaths = changedPaths.filter(relativePath => !approved.includes(relativePath));
    const missingApprovedPaths = approved.filter(relativePath => !changedPaths.includes(relativePath));
    if (unapprovedPaths.length || missingApprovedPaths.length) {
      const error = new Error("CANONICAL_RELEASE_SCOPE_MISMATCH");
      error.code = "CANONICAL_RELEASE_SCOPE_MISMATCH";
      error.unapprovedPaths = unapprovedPaths;
      error.missingApprovedPaths = missingApprovedPaths;
      throw error;
    }

    const entries = await indexEntryModes(repoRoot, indexEnv);
    const submodules = entries.filter(entry => entry.mode === "160000");
    if (submodules.length) {
      const error = new Error("CANONICAL_RELEASE_SUBMODULE_UNSUPPORTED");
      error.code = "CANONICAL_RELEASE_SUBMODULE_UNSUPPORTED";
      error.paths = submodules.map(entry => entry.path);
      throw error;
    }
    const treePaths = entries.map(entry => entry.path);
    const [source, governance, fixture] = await Promise.all([
      hashGitIndexFileSet(repoRoot, indexEnv, productionPathsFromGitTree(treePaths)),
      hashGitIndexFileSet(repoRoot, indexEnv, governancePathsFromGitTree(treePaths)),
      hashGitIndexFileSet(repoRoot, indexEnv, fixturePathsFromGitTree(treePaths))
    ]);
    return Object.freeze({
      authority: "GIT_CANONICAL_RELEASE_TREE",
      baseCommit: String(baseCommit || "HEAD"),
      approvedFileCount: approved.length,
      changedFileCount: changedPaths.length,
      unapprovedPaths: Object.freeze(unapprovedPaths),
      missingApprovedPaths: Object.freeze(missingApprovedPaths),
      symlinkCount: entries.filter(entry => entry.mode === "120000").length,
      source,
      governance,
      fixture
    });
  } finally {
    await rm(temporaryDirectory, { recursive: true, force: true });
  }
}

export async function computeProductionSourceFingerprint(repoRoot) {
  const rootEntries = await readdir(repoRoot, { withFileTypes: true });
  const rootRuntimeFiles = rootEntries
    .filter(entry => entry.isFile())
    .map(entry => entry.name)
    .filter(name => PRODUCTION_ROOT_EXTENSIONS.has(path.extname(name).toLowerCase()))
    .filter(name => !name.startsWith("COORDINATE_"));
  const serverFiles = await walkFiles(repoRoot, "server");
  const dataFiles = await walkFiles(repoRoot, "data");
  const scriptFiles = (await walkFiles(repoRoot, "scripts"))
    .filter(relativePath => path.extname(relativePath).toLowerCase() === ".js");
  return hashFileSet(repoRoot, [...rootRuntimeFiles, ...serverFiles, ...dataFiles, ...scriptFiles]);
}

export async function computeReleaseGovernanceFingerprint(repoRoot) {
  const governanceFiles = (await walkFiles(repoRoot, "release-governance"))
    .filter(relativePath => !/(^|\/)transient(?:\/|$)/i.test(relativePath));
  return hashFileSet(repoRoot, ["scripts/coordinate-regression-runner.js", ...governanceFiles]);
}

export async function computeFixtureSetFingerprint(repoRoot) {
  const fixtureFiles = (await walkFiles(repoRoot, "regression-samples"))
    .filter(relativePath => FIXTURE_EXTENSIONS.has(path.extname(relativePath).toLowerCase()));
  return hashFileSet(repoRoot, [
    "COORDINATE_RECOGNITION_GOLDEN_BASELINE.json",
    "COORDINATE_ERROR_LIBRARY.json",
    ...fixtureFiles
  ]);
}

function requireHash(value, name) {
  const normalized = String(value || "").trim().toLowerCase();
  if (!HASH_PATTERN.test(normalized)) {
    const error = new Error(`${name} must be a 64-character SHA-256 value.`);
    error.code = "EVIDENCE_BINDING_MISMATCH";
    error.field = name;
    throw error;
  }
  return normalized;
}

export async function validateReleaseEvidenceBinding({
  repoRoot,
  runtimeIdentity,
  frozenIdentity,
  canonicalRelease = null
}) {
  const frozen = {
    productionSourceHash: requireHash(frozenIdentity?.productionSourceHash, "FROZEN_PRODUCTION_SOURCE_HASH"),
    releaseGovernanceHash: requireHash(frozenIdentity?.releaseGovernanceHash, "FROZEN_RELEASE_GOVERNANCE_HASH"),
    fixtureSetHash: requireHash(frozenIdentity?.fixtureSetHash, "FROZEN_FIXTURE_SET_HASH")
  };
  const canonicalFingerprints = canonicalRelease
    ? await computeCanonicalGitReleaseFingerprints({ repoRoot, ...canonicalRelease })
    : null;
  const [source, governance, fixture] = await Promise.all([
    canonicalFingerprints?.source || computeProductionSourceFingerprint(repoRoot),
    canonicalFingerprints?.governance || computeReleaseGovernanceFingerprint(repoRoot),
    computeFixtureSetFingerprint(repoRoot)
  ]);
  const runtimeSourceHash = requireHash(runtimeIdentity?.runtimeSourceSha256, "RUNTIME_SOURCE_SHA256");
  const actual = {
    productionSourceHash: source.hash,
    runtimeSourceHash,
    releaseGovernanceHash: governance.hash,
    fixtureSetHash: fixture.hash
  };
  const mismatches = [];
  if (actual.productionSourceHash !== frozen.productionSourceHash) mismatches.push("production_source_vs_frozen");
  if (actual.runtimeSourceHash !== frozen.productionSourceHash) mismatches.push("runtime_source_vs_frozen");
  if (actual.releaseGovernanceHash !== frozen.releaseGovernanceHash) mismatches.push("governance_vs_frozen");
  if (actual.fixtureSetHash !== frozen.fixtureSetHash) mismatches.push("fixture_vs_frozen");
  if (mismatches.length) {
    const error = new Error(`EVIDENCE_BINDING_MISMATCH: ${mismatches.join(",")}`);
    error.code = "EVIDENCE_BINDING_MISMATCH";
    error.mismatches = mismatches;
    error.actual = actual;
    error.frozen = frozen;
    throw error;
  }
  return Object.freeze({
    status: "BOUND",
    sourceIdentityAuthority: canonicalFingerprints?.authority || "WORKING_TREE_BYTES_LEGACY",
    ...frozen,
    runtimeSourceHash,
    sourceFileCount: source.fileCount,
    governanceFileCount: governance.fileCount,
    fixtureFileCount: fixture.fileCount
  });
}
