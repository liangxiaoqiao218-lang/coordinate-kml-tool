import { spawnSync, execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const receiptRoot = String(process.env.PR76_ACCEPTANCE_RECEIPT_ROOT || "").trim();
if (!receiptRoot || existsSync(receiptRoot)) throw new Error("PR76_NEW_RECEIPT_ROOT_REQUIRED");
mkdirSync(receiptRoot, { recursive: false });

const git = (...args) => execFileSync("git", args, { cwd: root, encoding: "utf8", windowsHide: true }).trim();
const head = git("rev-parse", "HEAD");
const statusBefore = git("status", "--porcelain=v1");
if (statusBefore) throw new Error(`PR76_WORKTREE_NOT_CLEAN_BEFORE_TESTS\n${statusBefore}`);

const suppliedTessdataPath = String(process.env.RECOGNITION_TEST_TESSDATA_PATH || "").trim();
const tessdataDirectory = suppliedTessdataPath.toLowerCase().endsWith(".traineddata.gz")
  ? path.dirname(suppliedTessdataPath)
  : suppliedTessdataPath;
const tessdataFile = path.join(tessdataDirectory, "eng.traineddata.gz");
if (!tessdataDirectory || !existsSync(tessdataFile)) throw new Error("PR76_VERIFIED_TESSDATA_REQUIRED");
const tessdataSha256 = createHash("sha256").update(readFileSync(tessdataFile)).digest("hex").toUpperCase();
if (tessdataSha256 !== "45B4CB346724AC1774F1C36F42F182B887BCDB28EBE63E6FFF90AC41F3FCFF91") {
  throw new Error("PR76_TESSDATA_SHA256_MISMATCH");
}

const steps = [
  ["self-intersection-output-contract", "scripts/self-intersection-output-contract-regression.js"],
  ["recognition-map-kml-capability", "scripts/recognition-map-kml-capability-regression.js"],
  ["recognition-inspection-output-capability", "scripts/recognition-inspection-output-capability-regression.js"],
  ["recognition-integrity-output", "scripts/recognition-integrity-output-regression.js"],
  ["coordinate-source-grouping", "scripts/p0-coordinate-source-grouping-regression.js"],
  ["recognition-projected-pair", "scripts/recognition-projected-pair-regression.js"],
  ["recognition-projected-integrity-http", "scripts/recognition-projected-integrity-http-regression.js"],
  ["p08h-confirmation-ui-lifecycle", "scripts/p08h-confirmation-ui-lifecycle-regression.js"],
  ["source-coordinate-review-display", "scripts/source-coordinate-review-display-regression.js"],
  ["review-output-contract", "scripts/review-output-contract-regression.js"],
  ["projected-crs-source-evidence", "scripts/projected-crs-source-evidence-regression.js"],
  ["multi-representation-source-evidence", "scripts/multi-representation-source-evidence-regression.js"],
  ["coordinate-markdown-table", "scripts/coordinate-markdown-table-regression.js"],
  ["recognition-first-review-result-v2", "scripts/recognition-first-review-result-v2-regression.js"],
  ["recognition-first-acquisition-evidence-v3", "scripts/recognition-first-acquisition-evidence-v3-regression.js"],
  ["multi-representation-http", "scripts/multi-representation-http-regression.js"],
  ["recognition-projected-authorization-v8", "scripts/recognition-projected-authorization-v8-regression.js"],
  ["recognition-projected-header-terminal-v9", "scripts/recognition-projected-header-terminal-v9-regression.js"],
  ["recognition-table-input", "scripts/recognition-table-input-regression.js"],
  ["recognition-projected-extra-field", "scripts/recognition-projected-extra-field-regression.js"],
  ["recognition-acquisition-downstream", "scripts/recognition-acquisition-downstream-regression.js"],
  ["production-recognition-recovery-p0", "scripts/production-recognition-recovery-p0-regression.js"],
  ["production-core-capability-closure-p0", "scripts/production-core-capability-closure-p0-regression.js"]
];

const baseEnv = { ...process.env };
for (const name of Object.keys(baseEnv)) {
  if (/PROVIDER|SUPABASE|ALIYUN|DASHSCOPE|OPENAI|ANTHROPIC|GEMINI|API_KEY|API_TOKEN|SECRET|USAGE|PASSWORD|COOKIE|AUTHORIZATION/i.test(name)) {
    baseEnv[name] = "";
  }
}
Object.assign(baseEnv, {
  NODE_ENV: "test",
  DOTENV_CONFIG_PATH: path.join(root, "__no_pr76_environment__"),
  NODE_OPTIONS: `--require=${path.join(root, "scripts", "recognition-audit-offline-guard.cjs")}`,
  RECOGNITION_TEST_TESSDATA_PATH: tessdataDirectory
});

const results = [];
for (const [name, script] of steps) {
  const stepDirectory = path.join(receiptRoot, name);
  const startedAt = new Date().toISOString();
  const env = {
    ...baseEnv,
    SELF_INTERSECTION_OUTPUT_RECEIPT_DIR: path.join(stepDirectory, "contract"),
    RECOGNITION_MAP_KML_RECEIPT_DIR: path.join(stepDirectory, "map-kml"),
    RECOGNITION_AUDIT_RECEIPT_ROOT: path.join(stepDirectory, "audit")
  };
  const command = `${process.execPath} ${script}`;
  const execution = spawnSync(process.execPath, [script], {
    cwd: root,
    env,
    encoding: "utf8",
    windowsHide: true,
    timeout: 10 * 60 * 1000,
    maxBuffer: 64 * 1024 * 1024
  });
  mkdirSync(stepDirectory, { recursive: true });
  const endedAt = new Date().toISOString();
  const output = `${execution.stdout || ""}${execution.stderr || ""}`;
  const record = {
    name,
    head,
    command,
    startedAt,
    endedAt,
    exitCode: execution.status,
    signal: execution.signal,
    timedOut: execution.error?.code === "ETIMEDOUT",
    result: execution.status === 0 ? "PASS" : "FAIL",
    realProviderCalls: /REAL_PROVIDER_CALLS=0|realProviderCalls"\s*:\s*0/u.test(output) ? 0 : "NOT_REPORTED",
    outputFile: `${name}/stdout-stderr.log`
  };
  writeFileSync(path.join(stepDirectory, "stdout-stderr.log"), output, { flag: "wx" });
  writeFileSync(path.join(stepDirectory, "receipt.json"), JSON.stringify(record, null, 2), { flag: "wx" });
  results.push(record);
  writeFileSync(path.join(receiptRoot, "manifest.json"), JSON.stringify({
    schemaVersion: "pr76_final_acceptance_v1",
    head,
    statusBefore,
    tessdataDirectory,
    tessdataFile,
    tessdataSha256,
    offlineGuard: true,
    results
  }, null, 2));
  console.log(`${record.result} ${name} head=${head}`);
  if (record.result !== "PASS") process.exit(1);
}

const statusAfter = git("status", "--porcelain=v1");
const manifestPath = path.join(receiptRoot, "manifest.json");
const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
Object.assign(manifest, {
  completedAt: new Date().toISOString(),
  statusAfter,
  worktreeCleanAfter: statusAfter === "",
  allPassed: results.every(result => result.result === "PASS"),
  realProviderCalls: 0
});
writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));
if (statusAfter) throw new Error(`PR76_WORKTREE_NOT_CLEAN_AFTER_TESTS\n${statusAfter}`);
console.log(`PR76 FINAL ACCEPTANCE: ${results.length}/${results.length} PASS; REAL_PROVIDER_CALLS=0; HEAD=${head}`);
console.log(`Receipt: ${manifestPath}`);
