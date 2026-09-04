import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFile } from 'node:child_process';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import {
  computeCanonicalCandidateFileHashes,
  HISTORICAL_LOCAL_PATCH_CANDIDATE_SPEC_ID,
  LOCAL_PATCH_CANDIDATE_SPEC,
  P1_LOCAL_PATCH_CANDIDATE_SPEC_ID,
  resolveLocalPatchCandidateSpec,
  validateCandidateObservation,
  validateLocalPatchCandidateIdentity,
} from './local-patch-candidate-identity.js';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const execFileAsync = promisify(execFile);
const p1Spec = await resolveLocalPatchCandidateSpec({ repoRoot, candidateSpecId: P1_LOCAL_PATCH_CANDIDATE_SPEC_ID });
let passed = 0;

async function test(name, fn) {
  await fn();
  passed += 1;
  console.log(`PASS ${name}`);
}

function requestedFor(spec, overrides = {}) {
  return {
    candidateSpecId: spec.candidateSpecId,
    qualificationMode: 'LOCAL_PATCH_CANDIDATE',
    baseCommit: spec.baseCommit,
    candidateManifestSha256: spec.candidateManifestSha256,
    trackedPatchSha256: spec.trackedPatchSha256,
    candidateSourceHash: spec.candidateSourceHash,
    frozenReleaseGovernanceHash: spec.frozenReleaseGovernanceHash,
    frozenFixtureSetHash: spec.frozenFixtureSetHash,
    nodeEnv: 'test',
    apiUrl: 'http://127.0.0.1:3000/api/recognize-coordinates',
    p0DeterministicReplay: '1',
    ...overrides,
  };
}

function observationFor(spec, overrides = {}) {
  return {
    actualHead: spec.baseCommit,
    applicationChangedPaths: Object.keys(spec.files),
    fileHashes: { ...spec.files },
    trackedPatchSha256: spec.trackedPatchSha256,
    candidateSourceHash: spec.candidateSourceHash || null,
    baseGovernanceHash: spec.frozenReleaseGovernanceHash || null,
    baseFixtureSetHash: spec.frozenFixtureSetHash || null,
    ...overrides,
  };
}

function expectMismatch(spec, requestedOverrides = {}, observationOverrides = {}, expectedField) {
  assert.throws(
    () => validateCandidateObservation({
      spec,
      requested: requestedFor(spec, requestedOverrides),
      observation: observationFor(spec, observationOverrides),
    }),
    error => error?.code === 'EVIDENCE_BINDING_MISMATCH' && (!expectedField || error?.field === expectedField),
  );
}

await test('historical legacy spec preserves and validates exact original identity', () => {
  assert.equal(LOCAL_PATCH_CANDIDATE_SPEC.candidateSpecId, HISTORICAL_LOCAL_PATCH_CANDIDATE_SPEC_ID);
  assert.equal(LOCAL_PATCH_CANDIDATE_SPEC.baseCommit, 'a80c908f4ba38e45a98eb3aa30c8361a03422db4');
  assert.equal(Object.keys(LOCAL_PATCH_CANDIDATE_SPEC.files).length, 17);
  assert.equal(LOCAL_PATCH_CANDIDATE_SPEC.candidateManifestSha256, '9934438bec9d32070e14bc45298df4e584a5c5fd03e0f0b69f09b2f9fec30957');
  assert.equal(LOCAL_PATCH_CANDIDATE_SPEC.trackedPatchSha256, '5742afa5e648da560e4c1e56959771069b36857dda90ae0ccbd16ec38d8df4e1');
  assert.equal(validateCandidateObservation({
    spec: LOCAL_PATCH_CANDIDATE_SPEC,
    requested: requestedFor(LOCAL_PATCH_CANDIDATE_SPEC),
    observation: observationFor(LOCAL_PATCH_CANDIDATE_SPEC),
  }).status, 'LOCAL_PATCH_CANDIDATE_BOUND');
});

await test('P1 exact repository spec binds pre-commit patch or verifies immutable post-commit blobs', async () => {
  const { stdout } = await execFileAsync('git', ['rev-parse', 'HEAD'], { cwd: repoRoot, encoding: 'utf8', windowsHide: true });
  const actualHead = stdout.trim().toLowerCase();
  if (actualHead === p1Spec.baseCommit) {
    const result = await validateLocalPatchCandidateIdentity({
      repoRoot,
      ...requestedFor(p1Spec),
    });
    assert.equal(result.status, 'LOCAL_PATCH_CANDIDATE_BOUND');
    assert.equal(result.candidateSpecId, P1_LOCAL_PATCH_CANDIDATE_SPEC_ID);
    assert.equal(result.baseCommitMatch, true);
    assert.equal(result.candidateManifestSha256Match, true);
    assert.equal(result.trackedPatchSha256Match, true);
    assert.equal(result.candidateSourceHashMatch, true);
    return;
  }
  const { stdout: parentOutput } = await execFileAsync('git', ['rev-parse', 'HEAD^'], { cwd: repoRoot, encoding: 'utf8', windowsHide: true });
  assert.equal(parentOutput.trim().toLowerCase(), p1Spec.baseCommit);
  const hashes = await computeCanonicalCandidateFileHashes({
    repoRoot,
    baseCommit: p1Spec.baseCommit,
    candidatePaths: Object.keys(p1Spec.files),
    candidateCommit: actualHead,
  });
  assert.deepEqual(hashes, p1Spec.files);
});

await test('canonical candidate identity is invariant to CRLF checkout bytes and survives commit', async () => {
  const temporaryRepository = await mkdtemp(path.join(os.tmpdir(), 'geokit-canonical-file-regression-'));
  try {
    const run = (...args) => execFileAsync('git', args, { cwd: temporaryRepository, encoding: 'utf8', windowsHide: true });
    await run('init');
    await run('config', 'user.name', 'GeoKit Regression');
    await run('config', 'user.email', 'regression@invalid.example');
    await run('config', 'core.autocrlf', 'true');
    await writeFile(path.join(temporaryRepository, 'candidate.txt'), 'alpha\n', 'utf8');
    await run('add', '--', 'candidate.txt');
    await run('commit', '-m', 'base');
    const { stdout: baseOutput } = await run('rev-parse', 'HEAD');
    const baseCommit = baseOutput.trim();
    const crlfBytes = Buffer.from('alpha\r\nbeta\r\n', 'utf8');
    const canonicalBytes = Buffer.from('alpha\nbeta\n', 'utf8');
    await writeFile(path.join(temporaryRepository, 'candidate.txt'), crlfBytes);
    const worktreeHashes = await computeCanonicalCandidateFileHashes({
      repoRoot: temporaryRepository,
      baseCommit,
      candidatePaths: ['candidate.txt'],
    });
    const expectedCanonicalHash = createHash('sha256').update(canonicalBytes).digest('hex');
    const rawWorktreeHash = createHash('sha256').update(crlfBytes).digest('hex');
    assert.equal(worktreeHashes['candidate.txt'], expectedCanonicalHash);
    assert.notEqual(rawWorktreeHash, expectedCanonicalHash);
    await run('add', '--', 'candidate.txt');
    await run('commit', '-m', 'candidate');
    const { stdout: candidateOutput } = await run('rev-parse', 'HEAD');
    const committedHashes = await computeCanonicalCandidateFileHashes({
      repoRoot: temporaryRepository,
      baseCommit,
      candidatePaths: ['candidate.txt'],
      candidateCommit: candidateOutput.trim(),
    });
    assert.deepEqual(committedHashes, worktreeHashes);

    const manifestPath = path.join(temporaryRepository, 'release-governance', 'local-patch-candidate-specs', 'p1-spatial-ui-v1.json');
    const canonicalManifest = await readFile(path.join(repoRoot, 'release-governance', 'local-patch-candidate-specs', 'p1-spatial-ui-v1.json'), 'utf8');
    await mkdir(path.dirname(manifestPath), { recursive: true });
    await writeFile(manifestPath, canonicalManifest.replaceAll('\n', '\r\n'), 'utf8');
    const crlfManifestSpec = await resolveLocalPatchCandidateSpec({
      repoRoot: temporaryRepository,
      candidateSpecId: P1_LOCAL_PATCH_CANDIDATE_SPEC_ID,
    });
    assert.equal(crlfManifestSpec.candidateManifestSha256, p1Spec.candidateManifestSha256);
  } finally {
    await rm(temporaryRepository, { recursive: true, force: true });
  }
});

await test('unknown spec ID blocks', async () => {
  await assert.rejects(resolveLocalPatchCandidateSpec({ repoRoot, candidateSpecId: 'unknown' }), error => error?.field === 'LOCAL_PATCH_CANDIDATE_SPEC_ID');
});
await test('missing spec ID blocks with multiple specs', async () => {
  await assert.rejects(resolveLocalPatchCandidateSpec({ repoRoot, candidateSpecId: '' }), error => error?.field === 'LOCAL_PATCH_CANDIDATE_SPEC_ID');
});
await test('wrong base blocks', () => expectMismatch(p1Spec, { baseCommit: '0'.repeat(40) }, {}, 'BASE_COMMIT'));
await test('wrong HEAD blocks', () => expectMismatch(p1Spec, {}, { actualHead: '0'.repeat(40) }, 'HEAD'));
await test('wrong file count blocks', () => expectMismatch(p1Spec, {}, { applicationChangedPaths: Object.keys(p1Spec.files).slice(1) }, 'fileCount'));
await test('extra file blocks', () => expectMismatch(p1Spec, {}, { applicationChangedPaths: [...Object.keys(p1Spec.files), 'extra.js'] }, 'fileCount'));
await test('missing file blocks', () => expectMismatch(p1Spec, {}, { applicationChangedPaths: Object.keys(p1Spec.files).slice(0, -1) }, 'fileCount'));
await test('wrong file hash blocks', () => {
  const [first] = Object.keys(p1Spec.files);
  expectMismatch(p1Spec, {}, { fileHashes: { ...p1Spec.files, [first]: '0'.repeat(64) } }, 'files');
});
await test('wrong patch hash blocks', () => expectMismatch(p1Spec, { trackedPatchSha256: '0'.repeat(64) }, {}, 'TRACKED_PATCH_SHA256'));
await test('wrong candidate source hash blocks', () => expectMismatch(p1Spec, { candidateSourceHash: '0'.repeat(64) }, {}, 'CANDIDATE_SOURCE_HASH'));
await test('wrong manifest hash blocks', () => expectMismatch(p1Spec, { candidateManifestSha256: '0'.repeat(64) }, {}, 'CANDIDATE_MANIFEST_SHA256'));
await test('wrong governance hash blocks', () => expectMismatch(p1Spec, { frozenReleaseGovernanceHash: '0'.repeat(64) }, {}, 'FROZEN_RELEASE_GOVERNANCE_HASH'));
await test('wrong fixture-set hash blocks', () => expectMismatch(p1Spec, { frozenFixtureSetHash: '0'.repeat(64) }, {}, 'FROZEN_FIXTURE_SET_HASH'));
await test('Production NODE_ENV blocks replay', () => expectMismatch(p1Spec, { nodeEnv: 'production' }, {}, 'NODE_ENV'));
await test('non-loopback API blocks replay', () => expectMismatch(p1Spec, { apiUrl: 'https://geokitlab.com/api/recognize-coordinates' }, {}, 'COORDINATE_REGRESSION_API_URL'));
await test('P0 deterministic replay disabled blocks replay', () => expectMismatch(p1Spec, { p0DeterministicReplay: '0' }, {}, 'P0_DETERMINISTIC_REPLAY'));
await test('binding failure never authorizes Provider fallback', () => {
  let providerCalls = 0;
  assert.throws(() => {
    validateCandidateObservation({ spec: p1Spec, requested: requestedFor(p1Spec, { baseCommit: '0'.repeat(40) }), observation: observationFor(p1Spec) });
    providerCalls += 1;
  }, error => error?.code === 'EVIDENCE_BINDING_MISMATCH');
  assert.equal(providerCalls, 0);
});

console.log(`Local patch candidate identity regression: ${passed}/${passed} PASS`);
console.log('HISTORICAL_SPEC_PRESERVED=true');
console.log('MULTIPLE_EXACT_SPECS_SUPPORTED=true');
console.log('IMPLICIT_SPEC_FALLBACK=false');
console.log('REAL_PROVIDER_FALLBACK_ON_BINDING_FAILURE=false');
