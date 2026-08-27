import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { mkdir, readFile, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parse as parseYaml } from "yaml";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const evidenceRoot = path.join(repoRoot, "release-evidence");
const coordinateFixtureRoot = path.join(repoRoot, "regression-samples", "fixtures");
const ocrRoot = path.join(repoRoot, "regression-samples", "OCR_GOLDEN");
const coordinateSourceRoot = "D:\\萨赫勒数字科技有限公司\\关于西非的业务\\测试素材";
const ocrRecordSource = "C:\\Users\\Mir-1\\Documents\\Codex\\2026-08-24\\geokitlab-v1.0.3-development\\regression-samples\\OCR_GOLDEN";
const baseline = JSON.parse(await readFile(path.join(repoRoot, "COORDINATE_RECOGNITION_GOLDEN_BASELINE.json"), "utf8"));
const ocr = parseYaml(await readFile(path.join(ocrRoot, "records.yaml"), "utf8"));

const coordinateFileNames = {
  wgs84_table_rc2_congo_001: "刚果，两个坐标在同一张图.jpg",
  wgs84_table_timeout_rescue_001: "微信图片_20260503091216_182_19.jpg",
  bftm_burkina_002: "布基纳法索02.jpg",
  utm30_burkina_003: "布基纳法索03.png",
  mgrs_myanmar_001: "缅甸坐标.jpg",
  kyrgyz_gk_001: "吉尔吉斯斯坦矿地坐标.png",
  madagascar_cadastral_candidate_001: "马达加斯加坐标.png",
  mozambique_tete_001: "莫桑比克矿地.jpg",
  cote_divoire_single_01: "科特迪瓦01.png",
  cote_divoire_single_02: "科特迪瓦02.png",
  cote_divoire_single_03: "科特迪瓦03.png",
  cote_divoire_single_04: "科特迪瓦04.png",
  cote_divoire_multi_001: "科特迪瓦4个矿区坐标.jpg",
  handwritten_dms_001: "手写坐标.jpg",
  dms_grouped_two_areas_001: "两块矿地.jpg",
  point_az_dms_table_001: "微信图片_20260427122118_114_19.jpg",
  low_clarity_blurry_dms_001: "模糊坐标.jpg",
  oblique_dms_001: "斜拍坐标.jpg",
  long_coordinate_table_001: "长坐标.png"
};

function hash(value) {
  return createHash("sha256").update(value).digest("hex");
}

function imageDimensions(buffer) {
  if (buffer.length >= 24 && buffer.subarray(1, 4).toString("ascii") === "PNG") {
    return { width: buffer.readUInt32BE(16), height: buffer.readUInt32BE(20) };
  }
  if (buffer[0] === 0xff && buffer[1] === 0xd8) {
    let offset = 2;
    while (offset + 9 < buffer.length) {
      if (buffer[offset] !== 0xff) { offset += 1; continue; }
      const marker = buffer[offset + 1];
      const length = buffer.readUInt16BE(offset + 2);
      if ([0xc0, 0xc1, 0xc2, 0xc3, 0xc5, 0xc6, 0xc7, 0xc9, 0xca, 0xcb, 0xcd, 0xce, 0xcf].includes(marker)) {
        return { width: buffer.readUInt16BE(offset + 7), height: buffer.readUInt16BE(offset + 5) };
      }
      if (length < 2) break;
      offset += length + 2;
    }
  }
  return { width: null, height: null };
}

async function fileEvidence(filePath) {
  try {
    const bytes = await readFile(filePath);
    return { exists: true, bytes: bytes.length, sha256: hash(bytes), ...imageDimensions(bytes) };
  } catch {
    return { exists: false, bytes: null, sha256: null, width: null, height: null };
  }
}

const fixtureInventory = [];
const copyReport = [];
for (const sample of baseline.samples) {
  const fileName = coordinateFileNames[sample.sample_id];
  if (sample.input_type === "image") {
    const actualLocation = path.join(coordinateFixtureRoot, fileName);
    const sourcePath = path.join(coordinateSourceRoot, fileName);
    const actual = await fileEvidence(actualLocation);
    const source = await fileEvidence(sourcePath);
    fixtureInventory.push({
      fixtureId: sample.sample_id,
      type: sample.input_type,
      required: true,
      baselineStatus: sample.baseline_status,
      expectedPath: sample.local_path,
      actualLocation,
      ...actual,
      sourceWorktree: coordinateSourceRoot,
      trusted: source.exists && source.sha256 === actual.sha256,
      trustBasis: "exact baseline filename in relocated historical evidence root; byte identity verified",
      truthStatus: ["missing", "partial", "unstable", "locked_candidate"].includes(sample.baseline_status)
        ? "not_fully_locked"
        : "locked_or_experimental_locked",
      action: "copied_by_explicit_allowlist"
    });
    copyReport.push({
      sourcePath,
      destinationPath: actualLocation,
      filename: fileName,
      sha256Before: source.sha256,
      sha256After: actual.sha256,
      reason: "restore Coordinate Golden fixture into clean release integration",
      passed: source.exists && actual.exists && source.sha256 === actual.sha256
    });
  } else if (String(sample.fixture || "").startsWith("TEXT:")) {
    const text = String(sample.fixture).slice(5);
    fixtureInventory.push({
      fixtureId: sample.sample_id,
      type: sample.input_type,
      required: true,
      expectedPath: "inline TEXT fixture",
      actualLocation: "inline TEXT fixture",
      exists: true,
      bytes: Buffer.byteLength(text),
      sha256: hash(text),
      sourceWorktree: repoRoot,
      trusted: sample.baseline_status === "locked",
      trustBasis: "locked inline baseline truth",
      action: "none"
    });
  } else {
    const actualLocation = path.join(repoRoot, ...String(sample.fixture || "").split("/"));
    const actual = await fileEvidence(actualLocation);
    fixtureInventory.push({
      fixtureId: sample.sample_id,
      type: sample.input_type,
      required: true,
      expectedPath: sample.fixture,
      actualLocation,
      ...actual,
      sourceWorktree: repoRoot,
      trusted: actual.exists && sample.baseline_status === "locked",
      trustBasis: "existing locked expected truth file",
      action: sample.sample_id === "dms_text_single_point_001" ? "corrected locator to existing expected.json" : "none"
    });
  }
}

const ocrProvenance = [];
for (const record of ocr.records || []) {
  const imageFile = path.join(ocrRoot, "fixtures", path.basename(record.original_image));
  const actual = await fileEvidence(imageFile);
  ocrProvenance.push({
    sampleId: record.sample_id,
    imageFile,
    sha256: actual.sha256,
    expectedSha256: record.image_hash_sha256,
    dimensions: `${actual.width}x${actual.height}`,
    expectedDimensions: record.resolution,
    expectedOcrTruth: record.ocr_expected_text,
    expectedCoordinates: record.expected_wgs84,
    crs: record.crs,
    geometry: record.geometry,
    evidenceLevel: record.evidence_level,
    recordSource: path.join(ocrRecordSource, "records.yaml"),
    createdValidatedBy: "unrecorded_in_source_records",
    fixtureValid: actual.exists
      && actual.sha256 === record.image_hash_sha256
      && actual.bytes === record.image_bytes
      && `${actual.width}x${actual.height}` === record.resolution
  });
  const source = record.original_image;
  copyReport.push({
    sourcePath: source,
    destinationPath: imageFile,
    filename: path.basename(source),
    sha256Before: record.image_hash_sha256,
    sha256After: actual.sha256,
    reason: "restore audited OCR Golden fixture into clean release integration",
    passed: actual.sha256 === record.image_hash_sha256
  });
}

const fixtureSetSha256 = hash(fixtureInventory.map(item => `${item.fixtureId}:${item.sha256}`).sort().join("\n")
  + "\n"
  + ocrProvenance.map(item => `${item.sampleId}:${item.sha256}`).sort().join("\n"));
const statusLines = execFileSync("git", ["status", "--porcelain=v1"], { cwd: repoRoot, encoding: "utf8" }).trim().split(/\r?\n/).filter(Boolean);
const sourceFiles = statusLines
  .map(line => line.slice(3).trim())
  .filter(file => /\.(?:js|html|json|yaml|md)$/i.test(file) && !file.startsWith("release-evidence/"))
  .sort();
const sourceHashes = [];
for (const relativePath of sourceFiles) {
  const filePath = path.join(repoRoot, relativePath);
  try { sourceHashes.push(`${relativePath}:${hash(await readFile(filePath))}`); } catch { /* deleted or directory */ }
}
const runtimeSourceSha256 = hash(sourceHashes.join("\n"));
const runtimeFingerprint = {
  baseCommit: execFileSync("git", ["rev-parse", "HEAD"], { cwd: repoRoot, encoding: "utf8" }).trim(),
  branch: execFileSync("git", ["branch", "--show-current"], { cwd: repoRoot, encoding: "utf8" }).trim(),
  workingTreeDirty: statusLines.length > 0,
  runtimeSourceSha256,
  fixtureSetSha256,
  finalizerSchema: "finalized_coordinate_result_v1",
  confirmationRuntimeVersion: "coordinate_confirmation_runtime_v1",
  spatialFlag: false,
  deadlineDefaultMs: 55000,
  deadlineMaximumMs: 59000,
  sourceFiles
};
const manifest = {
  schemaVersion: "sr08c_release_evidence_manifest_v1",
  generatedAt: new Date().toISOString(),
  overallStatus: "FAIL",
  environment: "clean isolated release-integration worktree; local test server; provider credentials absent",
  ...runtimeFingerprint,
  gates: [
    { gate: "Coordinate Golden", runner: "npm.cmd run regression -- --gate --include-text", fixtureSet: "24 required", result: "FAIL 6/24; 0 missing; 0 skip; provider unavailable and rate limiting" },
    { gate: "OCR Golden", runner: "npm.cmd run ocr-golden", fixtureSet: "6 audited; 3 required positive", result: "READY offline; BLOCKED_BY_PROVIDER live" },
    { gate: "Manual Input Finalizer", runner: "npm.cmd run sr08c-manual-regression", fixtureSet: "deterministic manual WGS84", result: "PASS 5/5" },
    { gate: "Browser Confirmation E2E", runner: "in-app browser", fixtureSet: "audited real OCR image", result: "BLOCKED by provider credentials; file chooser unavailable" },
    { gate: "Real Provider Cancellation", runner: "not executable without provider credential", fixtureSet: "non-sensitive OCR fixture", result: "BLOCKED_BY_PROVIDER" },
    { gate: "SR-08 Finalizer", runner: "npm.cmd run sr08-regression", fixtureSet: "deterministic", result: "PASS 21/21" },
    { gate: "SR-08B Confirmation", runner: "npm.cmd run sr08b-regression", fixtureSet: "deterministic", result: "PASS 13/13" },
    { gate: "SR-08B HTTP Deadline", runner: "npm.cmd run sr08b-http-regression", fixtureSet: "deterministic HTTP lifecycle", result: "PASS 12/12; all requests below 60 seconds" },
    { gate: "KML Regression", runner: "kml-export-permission-regression + handwritten-dms-manual-edit-regression", fixtureSet: "point line polygon permission and DMS edit", result: "PASS 5/5 + PASS" },
    { gate: "Kill Switch", runner: "SR-08 K01/K02 and runtime identity", fixtureSet: "spatial flag OFF", result: "PASS" },
    { gate: "Verification and Evidence", runner: "verification/evidence/evidence-acquisition regressions", fixtureSet: "deterministic", result: "PASS 10/10 + 3/3 + 4/4" },
    { gate: "Runtime Identity", runner: "/api/version", fixtureSet: "runtime and fixture fingerprints", result: "PASS traceable dirty worktree identity" },
    { gate: "Syntax and Diff", runner: "node --check + git diff --check", fixtureSet: "changed JavaScript and worktree diff", result: "PASS" }
  ],
  artifacts: [
    "release-evidence/sr08c-fixture-inventory.json",
    "release-evidence/sr08c-fixture-copy-report.json",
    "release-evidence/sr08c-ocr-provenance-audit.json",
    "release-evidence/sr08c-runtime-fingerprint.json"
  ]
};

await mkdir(evidenceRoot, { recursive: true });
const writeJson = (name, value) => writeFile(path.join(evidenceRoot, name), `${JSON.stringify(value, null, 2)}\n`, "utf8");
await Promise.all([
  writeJson("sr08c-fixture-inventory.json", { schemaVersion: "sr08c_fixture_inventory_v1", count: fixtureInventory.length, fixtureSetSha256, fixtures: fixtureInventory }),
  writeJson("sr08c-fixture-copy-report.json", { schemaVersion: "sr08c_fixture_copy_report_v1", count: copyReport.length, copies: copyReport }),
  writeJson("sr08c-ocr-provenance-audit.json", { schemaVersion: "sr08c_ocr_provenance_audit_v1", recordCount: ocrProvenance.length, records: ocrProvenance }),
  writeJson("sr08c-runtime-fingerprint.json", runtimeFingerprint),
  writeJson("sr08c-release-evidence-manifest.json", manifest)
]);
console.log(JSON.stringify({ generated: 5, fixtureCount: fixtureInventory.length, ocrRecordCount: ocrProvenance.length, runtimeSourceSha256, fixtureSetSha256 }, null, 2));
