import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { validateReleaseEvidenceBinding } from '../release-governance/evidence-binding.js';
import {
  LOCAL_PATCH_CANDIDATE_SPEC,
  validateLocalPatchCandidateIdentity,
} from './local-patch-candidate-identity.js';
import { createMiningJudgeAcceptanceHarness } from './mining-quick-judge-browser-harness.js';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const spec = LOCAL_PATCH_CANDIDATE_SPEC;
let passed = 0;

async function test(name, fn) {
  await fn();
  passed += 1;
  console.log(`PASS ${name}`);
}

await test('exact local candidate identity binds all three authorities', async () => {
  const result = await validateLocalPatchCandidateIdentity({
    repoRoot,
    baseCommit: spec.baseCommit,
    candidateManifestSha256: spec.candidateManifestSha256,
    trackedPatchSha256: spec.trackedPatchSha256,
  });
  assert.equal(result.status, 'LOCAL_PATCH_CANDIDATE_BOUND');
  assert.equal(result.baseCommitMatch, true);
  assert.equal(result.candidateManifestSha256Match, true);
  assert.equal(result.trackedPatchSha256Match, true);
});

for (const [name, override] of [
  ['wrong base commit fails closed', { baseCommit: '0'.repeat(40) }],
  ['wrong manifest identity fails closed', { candidateManifestSha256: '0'.repeat(64) }],
  ['wrong patch identity fails closed', { trackedPatchSha256: '0'.repeat(64) }],
]) {
  await test(name, async () => {
    await assert.rejects(
      validateLocalPatchCandidateIdentity({
        repoRoot,
        baseCommit: spec.baseCommit,
        candidateManifestSha256: spec.candidateManifestSha256,
        trackedPatchSha256: spec.trackedPatchSha256,
        ...override,
      }),
      error => error?.code === 'EVIDENCE_BINDING_MISMATCH',
    );
  });
}

await test('production binding still rejects blank frozen source identity', async () => {
  await assert.rejects(
    validateReleaseEvidenceBinding({
      repoRoot,
      canonicalCommit: spec.baseCommit,
      runtimeIdentity: { runtimeSourceSha256: '0'.repeat(64) },
      frozenIdentity: {
        productionSourceHash: '',
        releaseGovernanceHash: '0'.repeat(64),
        fixtureSetHash: '0'.repeat(64),
      },
    }),
    error => error?.code === 'EVIDENCE_BINDING_MISMATCH' && error?.field === 'FROZEN_PRODUCTION_SOURCE_HASH',
  );
});

await test('judge harness requires explicit enablement', async () => {
  assert.throws(
    () => createMiningJudgeAcceptanceHarness({ enabled: '', nodeEnv: 'test' }),
    /EXPLICIT_ENABLE_REQUIRED/,
  );
});

await test('judge harness is forbidden in production', async () => {
  assert.throws(
    () => createMiningJudgeAcceptanceHarness({ enabled: '1', nodeEnv: 'production' }),
    /PRODUCTION_FORBIDDEN/,
  );
});

await test('judge harness requires a loopback bind address', async () => {
  assert.throws(
    () => createMiningJudgeAcceptanceHarness({ enabled: '1', nodeEnv: 'test', host: '0.0.0.0' }),
    /LOOPBACK_REQUIRED/,
  );
});

console.log(`Local patch candidate identity regression: ${passed}/${passed} PASS`);
