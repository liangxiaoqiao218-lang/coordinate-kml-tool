import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const DEFAULT_RELEASE_VERSION = "coordinate-engine-v2-shadow-rc";
const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(SCRIPT_DIR, "..");
const DEFAULT_OUTPUT_PATH = path.join(REPO_ROOT, "release-manifest.json");
const REQUIRED_ROOT_FILES = [
  "index.html",
  "admin.html",
  "server.js",
  "package.json",
  "package-lock.json",
  "pricing-config.js",
  "wechat-qr.jpg",
  "whatsapp-qr.jpg"
];
const REQUIRED_DIRECTORIES = [
  "server",
  "assets"
];
const SHARE_ASSET_PATTERN = /^share-.+\.(?:jpg|jpeg|png|svg)$/iu;

function normalizePath(value) {
  return value.split(path.sep).join("/");
}

function runGit(args) {
  return execFileSync("git", args, {
    cwd: REPO_ROOT,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"]
  }).trim();
}

function runGitBuffer(args) {
  return execFileSync("git", args, {
    cwd: REPO_ROOT,
    encoding: "buffer",
    maxBuffer: 200 * 1024 * 1024,
    stdio: ["ignore", "pipe", "pipe"]
  });
}

function readBoundedEnv(value, maxLength = 200) {
  const normalized = typeof value === "string" ? value.trim() : "";
  if (!normalized || normalized.length > maxLength || /[\u0000-\u001f\u007f]/u.test(normalized)) return "";
  return normalized;
}

function getReleaseVersion() {
  const releaseVersion = readBoundedEnv(process.env.RELEASE_IDENTITY_RELEASE_VERSION, 100) || DEFAULT_RELEASE_VERSION;
  if (!/^[A-Za-z0-9][A-Za-z0-9._+-]*$/u.test(releaseVersion)) {
    throw new Error("Unable to resolve a safe release version identity.");
  }
  return releaseVersion;
}

function getGitCommit() {
  const commit = (readBoundedEnv(process.env.RENDER_GIT_COMMIT, 40) || runGit(["rev-parse", "HEAD"])).toLowerCase();
  if (!/^[0-9a-f]{40}$/u.test(commit)) {
    throw new Error("Unable to resolve a full 40-character Git commit.");
  }
  return commit;
}

function getGitBranch() {
  const branch = readBoundedEnv(process.env.RENDER_GIT_BRANCH, 200)
    || readBoundedEnv(process.env.RELEASE_IDENTITY_BRANCH, 200)
    || runGit(["rev-parse", "--abbrev-ref", "HEAD"]);
  if (branch === "HEAD") {
    throw new Error("Unable to resolve branch identity from trusted metadata; git is detached at HEAD.");
  }
  if (!/^[A-Za-z0-9][A-Za-z0-9._+/-]*$/u.test(branch)) {
    throw new Error("Unable to resolve a safe Git branch identity.");
  }
  return branch;
}

function assertSafeRelativePath(relativePath) {
  const normalized = normalizePath(relativePath);
  if (!normalized || normalized.startsWith("../") || path.isAbsolute(normalized)) {
    throw new Error(`Unsafe artifact path: ${relativePath}`);
  }
  if (
    normalized === "release-manifest.json"
    || normalized === ".env"
    || normalized.startsWith(".env.")
    || normalized.startsWith("node_modules/")
    || normalized.startsWith("docs/")
    || normalized.startsWith("scripts/")
    || normalized.startsWith("regression-samples/")
    || normalized.startsWith("logs/")
    || normalized.startsWith("screenshots/")
    || normalized.startsWith("artifacts/")
  ) {
    throw new Error(`Excluded path attempted in artifact payload: ${relativePath}`);
  }
  return normalized;
}

function collectFilesFromDirectory(relativeDirectory) {
  const directoryPath = path.join(REPO_ROOT, relativeDirectory);
  if (!fs.existsSync(directoryPath) || !fs.statSync(directoryPath).isDirectory()) {
    throw new Error(`Missing required artifact directory: ${relativeDirectory}`);
  }

  const files = [];
  const visit = currentDirectory => {
    fs.readdirSync(currentDirectory, { withFileTypes: true }).forEach(entry => {
      const fullPath = path.join(currentDirectory, entry.name);
      if (entry.isDirectory()) {
        visit(fullPath);
        return;
      }
      if (entry.isFile()) {
        files.push(normalizePath(path.relative(REPO_ROOT, fullPath)));
      }
    });
  };
  visit(directoryPath);
  return files;
}

function collectRuntimePayloadFiles() {
  const trackedFiles = runGit(["ls-tree", "-r", "--name-only", "HEAD"])
    .split(/\r?\n/u)
    .filter(Boolean)
    .map(normalizePath);
  const trackedFileSet = new Set(trackedFiles);

  REQUIRED_ROOT_FILES.forEach(relativePath => {
    if (!trackedFileSet.has(relativePath)) {
      throw new Error(`Missing required tracked artifact file: ${relativePath}`);
    }
  });

  REQUIRED_DIRECTORIES.forEach(relativeDirectory => {
    const prefix = `${relativeDirectory}/`;
    if (!trackedFiles.some(file => file.startsWith(prefix))) {
      throw new Error(`Missing required tracked artifact directory: ${relativeDirectory}`);
    }
  });

  return trackedFiles
    .filter(relativePath => {
      if (REQUIRED_ROOT_FILES.includes(relativePath)) return true;
      if (REQUIRED_DIRECTORIES.some(relativeDirectory => relativePath.startsWith(`${relativeDirectory}/`))) return true;
      return SHARE_ASSET_PATTERN.test(relativePath);
    })
    .map(assertSafeRelativePath)
    .sort((left, right) => left.localeCompare(right));
}

function sha256File(relativePath) {
  return crypto.createHash("sha256").update(runGitBuffer(["show", `HEAD:${relativePath}`])).digest("hex");
}

function computeRuntimePayloadHash(files = collectRuntimePayloadFiles()) {
  const manifestLines = files.map(relativePath => `${sha256File(relativePath)}  ${relativePath}`);
  const payloadManifest = `${manifestLines.join("\n")}\n`;
  return `sha256:${crypto.createHash("sha256").update(payloadManifest, "utf8").digest("hex")}`;
}

function utcTimestamp() {
  return new Date().toISOString().replace(/\.\d{3}Z$/u, "Z");
}

function buildManifest() {
  const runtimeFiles = collectRuntimePayloadFiles();
  return {
    manifest: {
      releaseVersion: getReleaseVersion(),
      commit: getGitCommit(),
      branch: getGitBranch(),
      artifactHash: computeRuntimePayloadHash(runtimeFiles),
      buildTime: utcTimestamp()
    },
    runtimeFiles
  };
}

function writeJsonNoBom(filePath, value) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, { encoding: "utf8" });
}

function parseArgs(argv) {
  const options = {
    outputPath: DEFAULT_OUTPUT_PATH,
    quiet: false,
    regression: false
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--output") {
      const next = argv[index + 1];
      if (!next) throw new Error("--output requires a path.");
      options.outputPath = path.resolve(REPO_ROOT, next);
      index += 1;
    } else if (arg === "--quiet") {
      options.quiet = true;
    } else if (arg === "--regression") {
      options.regression = true;
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }

  return options;
}

function assertManifestShape(manifest) {
  assert.deepEqual(Object.keys(manifest), ["releaseVersion", "commit", "branch", "artifactHash", "buildTime"]);
  assert.match(manifest.releaseVersion, /^[A-Za-z0-9][A-Za-z0-9._+-]*$/u);
  assert.match(manifest.commit, /^[0-9a-f]{40}$/u);
  assert.match(manifest.branch, /^[A-Za-z0-9][A-Za-z0-9._+/-]*$/u);
  assert.match(manifest.artifactHash, /^sha256:[0-9a-f]{64}$/u);
  assert.match(manifest.buildTime, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/u);
  assert.equal(Number.isNaN(new Date(manifest.buildTime).getTime()), false);
  assert.equal(/api[_-]?key|secret|token|password|supabase|dashscope|aliyun/iu.test(JSON.stringify(manifest)), false);
}

function runRegression() {
  const tempDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "release-manifest-regression-"));
  const outputPath = path.join(tempDirectory, "release-manifest.json");
  const originalRenderCommit = process.env.RENDER_GIT_COMMIT;
  const originalRenderBranch = process.env.RENDER_GIT_BRANCH;
  const originalReleaseBranch = process.env.RELEASE_IDENTITY_BRANCH;
  const originalReleaseVersion = process.env.RELEASE_IDENTITY_RELEASE_VERSION;
  try {
    process.env.RENDER_GIT_COMMIT = "";
    process.env.RENDER_GIT_BRANCH = "";
    process.env.RELEASE_IDENTITY_BRANCH = "";
    process.env.RELEASE_IDENTITY_RELEASE_VERSION = "";
    const { manifest, runtimeFiles } = buildManifest();
    writeJsonNoBom(outputPath, manifest);
    const loaded = JSON.parse(fs.readFileSync(outputPath, "utf8"));
    assertManifestShape(loaded);
    assert.equal(loaded.commit, getGitCommit());
    assert.equal(loaded.branch, getGitBranch());
    assert.equal(loaded.releaseVersion, DEFAULT_RELEASE_VERSION);
    assert.equal(loaded.artifactHash, computeRuntimePayloadHash(runtimeFiles));
    assert.equal(runtimeFiles.includes("release-manifest.json"), false);
    assert.equal(runtimeFiles.some(file => file === ".env" || file.startsWith(".env.")), false);
    assert.equal(runtimeFiles.some(file => file.startsWith("node_modules/")), false);
    assert.equal(runtimeFiles.some(file => file.startsWith("docs/") || file.startsWith("scripts/")), false);
    assert.equal(runtimeFiles.some(file => file.startsWith("artifacts/")), false);

    process.env.RENDER_GIT_COMMIT = "f".repeat(40);
    process.env.RENDER_GIT_BRANCH = "v2/utm-intent-router";
    process.env.RELEASE_IDENTITY_RELEASE_VERSION = "coordinate-engine-v2-shadow-rc";
    const renderManifest = buildManifest().manifest;
    assert.equal(renderManifest.commit, "f".repeat(40));
    assert.equal(renderManifest.branch, "v2/utm-intent-router");
    assert.equal(renderManifest.releaseVersion, "coordinate-engine-v2-shadow-rc");

    process.env.RENDER_GIT_BRANCH = "";
    process.env.RELEASE_IDENTITY_BRANCH = "v2/utm-intent-router";
    assert.equal(buildManifest().manifest.branch, "v2/utm-intent-router");

    process.env.RENDER_GIT_COMMIT = "short";
    assert.throws(() => buildManifest(), /full 40-character Git commit/u);
    console.log("Release Manifest Generation Regression: PASS");
  } finally {
    if (originalRenderCommit === undefined) delete process.env.RENDER_GIT_COMMIT;
    else process.env.RENDER_GIT_COMMIT = originalRenderCommit;
    if (originalRenderBranch === undefined) delete process.env.RENDER_GIT_BRANCH;
    else process.env.RENDER_GIT_BRANCH = originalRenderBranch;
    if (originalReleaseBranch === undefined) delete process.env.RELEASE_IDENTITY_BRANCH;
    else process.env.RELEASE_IDENTITY_BRANCH = originalReleaseBranch;
    if (originalReleaseVersion === undefined) delete process.env.RELEASE_IDENTITY_RELEASE_VERSION;
    else process.env.RELEASE_IDENTITY_RELEASE_VERSION = originalReleaseVersion;
    fs.rmSync(tempDirectory, { recursive: true, force: true });
  }
}

function main() {
  const options = parseArgs(process.argv.slice(2));
  if (options.regression) {
    runRegression();
    return;
  }

  const { manifest, runtimeFiles } = buildManifest();
  writeJsonNoBom(options.outputPath, manifest);
  if (!options.quiet) {
    console.log(JSON.stringify({
      outputPath: options.outputPath,
      releaseVersion: manifest.releaseVersion,
      commit: manifest.commit,
      branch: manifest.branch,
      artifactHash: manifest.artifactHash,
      buildTime: manifest.buildTime,
      runtimeFileCount: runtimeFiles.length
    }, null, 2));
  }
}

main();
