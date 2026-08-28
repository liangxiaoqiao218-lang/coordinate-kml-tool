import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { computeCanonicalGitCommitFingerprints } from "../evidence-binding.js";

const testDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(testDir, "..", "..");
const expectedFixtureHash = "dfa774517eabb18bfc47981c1511be0cca4c01dd92e2592803820f0fab435697";

const first = await computeCanonicalGitCommitFingerprints({ repoRoot, commit: "HEAD" });
const second = await computeCanonicalGitCommitFingerprints({ repoRoot, commit: "HEAD" });

assert.equal(first.fixture.hash, expectedFixtureHash);
assert.equal(second.fixture.hash, expectedFixtureHash);
assert.equal(first.fixture.fileCount, 52);
assert.equal(second.fixture.fileCount, first.fixture.fileCount);
assert.deepEqual(second.fixture.files, first.fixture.files);
assert.equal(first.authority, "GIT_CANONICAL_RELEASE_TREE");

console.log("CURRENT_RELEASE_FIXTURE_AUTHORITY_REGRESSION=PASS");
console.log(`CURRENT_FIXTURE_SET_HASH=${first.fixture.hash}`);
console.log(`FIXTURE_FILE_COUNT=${first.fixture.fileCount}`);
console.log("SECOND_FIXTURE_CALCULATION_MATCH=true");
console.log("CURRENT_FIXTURE_TEST_USES_GIT_CANONICAL_RELEASE_TREE=true");
