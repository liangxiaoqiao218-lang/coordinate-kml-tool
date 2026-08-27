import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { computeFixtureSetFingerprint } from "../evidence-binding.js";

const testDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(testDir, "..", "..");
const expectedFixtureHash = "82bfa5090ee2d7a1e2a3d0e911d74f765ecf98daebabc74a3522acbdd61ab1cf";

const first = await computeFixtureSetFingerprint(repoRoot);
const second = await computeFixtureSetFingerprint(repoRoot);

assert.equal(first.hash, expectedFixtureHash);
assert.equal(second.hash, expectedFixtureHash);
assert.equal(first.fileCount, 52);
assert.equal(second.fileCount, first.fileCount);
assert.deepEqual(second.files, first.files);

console.log("CURRENT_RELEASE_FIXTURE_AUTHORITY_REGRESSION=PASS");
console.log(`CURRENT_FIXTURE_SET_HASH=${first.hash}`);
console.log(`FIXTURE_FILE_COUNT=${first.fileCount}`);
console.log("SECOND_FIXTURE_CALCULATION_MATCH=true");
console.log("CURRENT_FIXTURE_TEST_USES_PRODUCTION_AUTHORITY_CALCULATOR=true");
