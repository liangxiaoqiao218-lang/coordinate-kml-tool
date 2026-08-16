import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  COORDINATE_ENGINE_V3_DISABLED_REASON,
  recognizeWithIsolatedRecognizers,
} from "../server/coordinate-engine-v3/index.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

async function readRepoFile(relativePath) {
  return readFile(path.join(repoRoot, relativePath), "utf8");
}

function assertNoRuntimeImport(source, fileName) {
  assert.equal(
    /coordinate-engine-v3/.test(source),
    false,
    `${fileName} must not import coordinate-engine-v3 during foundation phase`,
  );
}

const serverSource = await readRepoFile("server.js");
const indexSource = await readRepoFile("index.html");

assertNoRuntimeImport(serverSource, "server.js");
assertNoRuntimeImport(indexSource, "index.html");

const disabledResult = await recognizeWithIsolatedRecognizers({ image: "baseline-compatibility" }, { env: {} });
assert.equal(disabledResult.handled, false);
assert.equal(disabledResult.reason, COORDINATE_ENGINE_V3_DISABLED_REASON);

console.log("Coordinate Engine V3 Baseline Compatibility Regression: PASS");

