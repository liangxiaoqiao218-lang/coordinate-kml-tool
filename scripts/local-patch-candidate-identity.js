import { createHash } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import os from 'node:os';
import path from 'node:path';
import { promisify } from 'node:util';
import {
  computeCanonicalGitCommitFingerprints,
  computeCanonicalGitReleaseFingerprints,
} from '../release-governance/evidence-binding.js';

const execFileAsync = promisify(execFile);
const HASH_PATTERN = /^[a-f0-9]{64}$/;
const COMMIT_PATTERN = /^[a-f0-9]{40}$/;
const QUALIFICATION_MODE = 'LOCAL_PATCH_CANDIDATE';

export const HISTORICAL_LOCAL_PATCH_CANDIDATE_SPEC_ID = 'historical-legacy-v1';
export const P1_LOCAL_PATCH_CANDIDATE_SPEC_ID = 'p1-spatial-ui-v1';

export const LOCAL_PATCH_CANDIDATE_SPEC = Object.freeze({
  candidateSpecId: HISTORICAL_LOCAL_PATCH_CANDIDATE_SPEC_ID,
  kind: 'legacy-inline-opaque-manifest',
  qualificationMode: QUALIFICATION_MODE,
  baseCommit: 'a80c908f4ba38e45a98eb3aa30c8361a03422db4',
  candidateManifestSha256: '9934438bec9d32070e14bc45298df4e584a5c5fd03e0f0b69f09b2f9fec30957',
  trackedPatchSha256: '5742afa5e648da560e4c1e56959771069b36857dda90ae0ccbd16ec38d8df4e1',
  files: Object.freeze({
    'index.html': '039d5282c7b457f683729ef1cf8a3fda00ed6d8a080fd0315ff3f2b786abb6f1',
    'regression-samples/Madagascar/README.md': 'ef95c1e1be0623728319db618249fd9828eb8228dd82d605082e47e3da1bdc71',
    'regression-samples/Madagascar/expected.json': '81ac497998eaff860e07ef92f5195aca64f319902e7890e8c886bc41cf3f9e79',
    'regression-samples/production-recognition-recovery-p0/golden-records.json': 'fac707411b43afb2615f795aa680615808f62248252e89c22aeea45dbf4c34ce',
    'regression-samples/production-recognition-recovery-p0/indonesia-utm50s-real-002.jpg': '707e971aef6e5a6744cbd860cf701e41218fe6fb9a609b88e8bd121d03348b5a',
    'release-governance/family-availability-policy-v1.json': '5ed9361c58ca5feffe9bb96d3f70d5f429f234ceb34be316605e4d71576069b9',
    'scripts/p08f-review-confirm-kml-regression.js': 'fcfb1d243ad403dd2850b68b7e07f5d9dbe53368cad3cfb11b39b2852138bc26',
    'scripts/p08h-confirmation-ui-lifecycle-regression.js': '1679af00da72b0e9dee339ed6a9cdeb78c5ffe017e556f7e631dab71c01157c6',
    'scripts/production-recognition-recovery-p0-regression.js': '3930d0d277553d465f0abf7efe425f42dcd9c543f0b3b3aa23d7ee59c183921a',
    'scripts/sr08-coordinate-finalizer-regression.js': 'd317af559e6fac8d5b43bae9ecaf707eaa13a096aff33aa4d1f9b4e3626f7e79',
    'scripts/sr08f7-family-critical-path-regression.js': '4cf38aebb35e0b7f498c67d9e0aba00d372a216ed85e793a294cdd149151bf4e',
    'scripts/sr08g2-family-availability-regression.js': '5b7b82cb6294941d12ac9f1e3f22e1cd4b0c2c81bcdd15946afd4fd80274ae64',
    'server.js': 'dbf5551a8c5d2827861e6bdefbc849c7de39af0d56a42f1ba05aeda7e6966610',
    'server/coordinate-finalizer/family-availability-policy.js': 'ba76d2c40482e15d3cdf242d37e580bb40fb4b39dee2ee711de8a4eb1daed81f',
    'server/coordinate-finalizer/finalizer-inputs.js': 'ce4ade49b69c0358f9763479a1336238c4f9269b630f1554f5623d9668fff2c7',
    'server/coordinate-finalizer/unified-gate.js': '5619f7b28002df6c544bf2fa80c3561e15d89df6b477c426c88f929bf4ec6f9d',
    'server/recognition/family-primary-routing.js': 'e9969831951ec5f7c8a798a2275e213de60d147560520003e1c52b1b1e34b5a3',
  }),
});

const P1_MANIFEST_RELATIVE_PATH = 'release-governance/local-patch-candidate-specs/p1-spatial-ui-v1.json';
const P1_MANIFEST_SHA256 = 'ec0e978341b66bd41d89e1ef7a7b736f1822b27d5bc0abb3dfa85100f9bbf4ed';
const GOVERNANCE_TOOLING_PATHS = Object.freeze([
  'scripts/local-patch-candidate-identity.js',
  'scripts/coordinate-regression-runner.js',
  P1_MANIFEST_RELATIVE_PATH,
  'scripts/local-patch-candidate-identity-regression.js',
  'scripts/coordinate-regression-runner-reconciliation-regression.js',
  'scripts/evidence-regression.js',
  'release-governance/tests/current-release-fixture-authority-regression.mjs',
  'scripts/evidence-acquisition-regression.js',
]);
const ALLOWED_UNTRACKED_PATHS = Object.freeze([
  P1_MANIFEST_RELATIVE_PATH,
  'regression-evidence/r8-r1/r8-r1-sanitized-result.json',
]);

function mismatch(message, field) {
  const error = new Error(message);
  error.code = 'EVIDENCE_BINDING_MISMATCH';
  error.field = field;
  return error;
}

function normalizedExact(value, expected, name, pattern) {
  const normalized = String(value || '').trim().toLowerCase();
  if (!pattern.test(normalized)) throw mismatch(`${name} is malformed.`, name);
  if (normalized !== expected) throw mismatch(`${name} does not match the selected frozen candidate.`, name);
  return normalized;
}

function canonicalManifestValue(value) {
  return {
    schemaVersion: value.schemaVersion,
    candidateSpecId: value.candidateSpecId,
    qualificationMode: value.qualificationMode,
    baseCommit: value.baseCommit,
    fileCount: value.fileCount,
    trackedPatchSha256: value.trackedPatchSha256,
    candidateSourceHash: value.candidateSourceHash,
    frozenReleaseGovernanceHash: value.frozenReleaseGovernanceHash,
    frozenFixtureSetHash: value.frozenFixtureSetHash,
    files: value.files.map(entry => ({ path: entry.path, sha256: entry.sha256 })),
  };
}

function validateCanonicalManifestBytes(bytes, manifest) {
  const text = bytes.toString('utf8');
  if (bytes.length >= 3 && bytes[0] === 0xef && bytes[1] === 0xbb && bytes[2] === 0xbf) {
    throw mismatch('Versioned candidate manifest must not contain a UTF-8 BOM.', 'CANDIDATE_MANIFEST_SHA256');
  }
  if (text.includes('\r') || !text.endsWith('\n') || text.endsWith('\n\n')) {
    throw mismatch('Versioned candidate manifest must use LF and exactly one trailing LF.', 'CANDIDATE_MANIFEST_SHA256');
  }
  const paths = manifest.files.map(entry => entry.path);
  if ([...paths].sort().join('\n') !== paths.join('\n')) {
    throw mismatch('Versioned candidate manifest files must be path-sorted.', 'CANDIDATE_MANIFEST_SHA256');
  }
  const canonical = `${JSON.stringify(canonicalManifestValue(manifest), null, 2)}\n`;
  if (text !== canonical) throw mismatch('Versioned candidate manifest bytes are not canonical.', 'CANDIDATE_MANIFEST_SHA256');
}

async function loadP1Spec(repoRoot) {
  const canonicalFiles = await readCanonicalCandidateFileBytes({
    repoRoot,
    baseCommit: 'HEAD',
    candidatePaths: [P1_MANIFEST_RELATIVE_PATH],
  });
  const bytes = canonicalFiles[P1_MANIFEST_RELATIVE_PATH];
  const actualManifestHash = createHash('sha256').update(bytes).digest('hex');
  if (actualManifestHash !== P1_MANIFEST_SHA256) throw mismatch('Repository candidate manifest hash does not match its registered authority.', 'CANDIDATE_MANIFEST_SHA256');
  let manifest;
  try {
    manifest = JSON.parse(bytes.toString('utf8'));
  } catch {
    throw mismatch('Versioned candidate manifest is not valid JSON.', 'CANDIDATE_MANIFEST_SHA256');
  }
  validateCanonicalManifestBytes(bytes, manifest);
  if (manifest.schemaVersion !== 'local_patch_candidate_spec_v1') throw mismatch('Unsupported candidate manifest schema.', 'schemaVersion');
  if (manifest.candidateSpecId !== P1_LOCAL_PATCH_CANDIDATE_SPEC_ID) throw mismatch('Candidate spec ID does not match manifest authority.', 'candidateSpecId');
  if (manifest.qualificationMode !== QUALIFICATION_MODE) throw mismatch('Candidate qualification mode mismatch.', 'qualificationMode');
  if (!Array.isArray(manifest.files) || manifest.files.length !== manifest.fileCount) throw mismatch('Candidate file count mismatch.', 'fileCount');
  for (const entry of manifest.files) {
    if (!entry || typeof entry.path !== 'string' || !HASH_PATTERN.test(String(entry.sha256 || ''))) throw mismatch('Candidate file entry is malformed.', 'files');
  }
  return Object.freeze({
    ...manifest,
    kind: 'versioned-repository-manifest',
    candidateManifestSha256: actualManifestHash,
    manifestPath: P1_MANIFEST_RELATIVE_PATH,
    files: Object.freeze(Object.fromEntries(manifest.files.map(entry => [entry.path, entry.sha256]))),
  });
}

export async function resolveLocalPatchCandidateSpec({ repoRoot, candidateSpecId }) {
  const requested = String(candidateSpecId || '').trim();
  if (!requested) throw mismatch('LOCAL_PATCH_CANDIDATE_SPEC_ID is required.', 'LOCAL_PATCH_CANDIDATE_SPEC_ID');
  if (requested === HISTORICAL_LOCAL_PATCH_CANDIDATE_SPEC_ID) return LOCAL_PATCH_CANDIDATE_SPEC;
  if (requested === P1_LOCAL_PATCH_CANDIDATE_SPEC_ID) return loadP1Spec(repoRoot);
  throw mismatch(`Unknown LOCAL_PATCH_CANDIDATE_SPEC_ID: ${requested}`, 'LOCAL_PATCH_CANDIDATE_SPEC_ID');
}

async function git(repoRoot, args, encoding = 'utf8', env = {}) {
  const { stdout } = await execFileAsync('git', args, {
    cwd: repoRoot,
    env: { ...process.env, ...env },
    encoding,
    maxBuffer: 16 * 1024 * 1024,
    windowsHide: true,
  });
  return stdout;
}

async function readCanonicalCandidateFileBytes({
  repoRoot,
  baseCommit,
  candidatePaths,
  candidateCommit = null,
}) {
  const paths = [...new Set((candidatePaths || []).map(relativePath => String(relativePath).replaceAll('\\', '/')))].sort();
  if (!paths.length) throw mismatch('Candidate file paths are required.', 'files');
  const temporaryDirectory = await mkdtemp(path.join(os.tmpdir(), 'geokit-local-patch-index-'));
  const temporaryIndex = path.join(temporaryDirectory, 'index');
  const indexEnv = { GIT_INDEX_FILE: temporaryIndex };
  try {
    await git(repoRoot, ['read-tree', String(candidateCommit || baseCommit)], 'utf8', indexEnv);
    if (!candidateCommit) await git(repoRoot, ['add', '-A', '--', ...paths], 'utf8', indexEnv);
    const bytesByPath = {};
    for (const relativePath of paths) {
      let bytes;
      try { bytes = await git(repoRoot, ['show', `:${relativePath}`], null, indexEnv); }
      catch { throw mismatch(`Candidate file is missing: ${relativePath}`, 'files'); }
      bytesByPath[relativePath] = Buffer.from(bytes);
    }
    return Object.freeze(bytesByPath);
  } finally {
    await rm(temporaryDirectory, { recursive: true, force: true });
  }
}

export async function computeCanonicalCandidateFileHashes(options) {
  const canonicalFiles = await readCanonicalCandidateFileBytes(options);
  return Object.freeze(Object.fromEntries(Object.entries(canonicalFiles).map(([relativePath, bytes]) => [
    relativePath,
    createHash('sha256').update(bytes).digest('hex'),
  ])));
}

function splitNullTerminated(value) {
  return String(value || '').split('\0').filter(Boolean).map(item => item.replaceAll('\\', '/'));
}

function isLoopbackApi(value) {
  try {
    const hostname = new URL(String(value || '')).hostname.toLowerCase();
    return hostname === 'localhost' || hostname === '127.0.0.1' || hostname === '[::1]';
  } catch {
    return false;
  }
}

export function validateCandidateObservation({ spec, requested, observation }) {
  if (requested.candidateSpecId !== spec.candidateSpecId) throw mismatch('Selected candidate spec ID mismatch.', 'LOCAL_PATCH_CANDIDATE_SPEC_ID');
  if (String(requested.qualificationMode || '').trim().toUpperCase() !== spec.qualificationMode) throw mismatch('QUALIFICATION_MODE mismatch.', 'QUALIFICATION_MODE');
  const requestedBase = normalizedExact(requested.baseCommit, spec.baseCommit, 'BASE_COMMIT', COMMIT_PATTERN);
  const requestedManifest = normalizedExact(requested.candidateManifestSha256, spec.candidateManifestSha256, 'CANDIDATE_MANIFEST_SHA256', HASH_PATTERN);
  const requestedPatch = normalizedExact(requested.trackedPatchSha256, spec.trackedPatchSha256, 'TRACKED_PATCH_SHA256', HASH_PATTERN);
  if (observation.actualHead !== requestedBase) throw mismatch('BASE_COMMIT does not match HEAD.', 'HEAD');

  const expectedPaths = Object.keys(spec.files).sort();
  const observedPaths = [...observation.applicationChangedPaths].sort();
  if (observedPaths.length !== expectedPaths.length) throw mismatch('Candidate changed file count mismatch.', 'fileCount');
  if (observedPaths.join('\n') !== expectedPaths.join('\n')) throw mismatch('Candidate changed file path set mismatch.', 'files');
  for (const relativePath of expectedPaths) {
    if (observation.fileHashes[relativePath] !== spec.files[relativePath]) throw mismatch(`Candidate file hash mismatch: ${relativePath}`, 'files');
  }
  if (observation.trackedPatchSha256 !== requestedPatch) throw mismatch('TRACKED_PATCH_SHA256 does not match the frozen application patch.', 'TRACKED_PATCH_SHA256');

  if (spec.kind === 'versioned-repository-manifest') {
    normalizedExact(requested.candidateSourceHash, spec.candidateSourceHash, 'CANDIDATE_SOURCE_HASH', HASH_PATTERN);
    normalizedExact(requested.frozenReleaseGovernanceHash, spec.frozenReleaseGovernanceHash, 'FROZEN_RELEASE_GOVERNANCE_HASH', HASH_PATTERN);
    normalizedExact(requested.frozenFixtureSetHash, spec.frozenFixtureSetHash, 'FROZEN_FIXTURE_SET_HASH', HASH_PATTERN);
    if (observation.candidateSourceHash !== spec.candidateSourceHash) throw mismatch('Canonical candidate source hash mismatch.', 'CANDIDATE_SOURCE_HASH');
    if (observation.baseGovernanceHash !== spec.frozenReleaseGovernanceHash) throw mismatch('Frozen governance inheritance mismatch.', 'FROZEN_RELEASE_GOVERNANCE_HASH');
    if (observation.baseFixtureSetHash !== spec.frozenFixtureSetHash) throw mismatch('Frozen fixture-set inheritance mismatch.', 'FROZEN_FIXTURE_SET_HASH');
  }
  if (String(requested.nodeEnv || '').trim().toLowerCase() === 'production') throw mismatch('Local patch replay is forbidden in Production.', 'NODE_ENV');
  if (!isLoopbackApi(requested.apiUrl)) throw mismatch('Local patch replay API must be loopback-only.', 'COORDINATE_REGRESSION_API_URL');
  if (String(requested.p0DeterministicReplay || '') !== '1') throw mismatch('P0 deterministic replay must be explicitly enabled.', 'P0_DETERMINISTIC_REPLAY');

  return Object.freeze({
    status: 'LOCAL_PATCH_CANDIDATE_BOUND', candidateSpecId: spec.candidateSpecId, qualificationMode: spec.qualificationMode,
    baseCommit: requestedBase, baseCommitMatch: true,
    candidateManifestSha256: requestedManifest, candidateManifestSha256Match: true,
    trackedPatchSha256: observation.trackedPatchSha256, trackedPatchSha256Match: true,
    candidateSourceHash: observation.candidateSourceHash || null,
    candidateSourceHashMatch: spec.kind === 'versioned-repository-manifest' ? true : null,
    productionSourceHash: null, runtimeSourceHash: null,
    releaseGovernanceHash: observation.baseGovernanceHash || null,
    fixtureSetHash: observation.baseFixtureSetHash || null,
  });
}

export async function validateLocalPatchCandidateIdentity({
  repoRoot, candidateSpecId, qualificationMode, baseCommit, candidateManifestSha256,
  trackedPatchSha256, candidateSourceHash, frozenReleaseGovernanceHash,
  frozenFixtureSetHash, nodeEnv, apiUrl, p0DeterministicReplay,
}) {
  const spec = await resolveLocalPatchCandidateSpec({ repoRoot, candidateSpecId });
  const actualHead = String(await git(repoRoot, ['rev-parse', 'HEAD'])).trim().toLowerCase();
  const allChangedPaths = splitNullTerminated(await git(repoRoot, ['diff', 'HEAD', '--name-only', '-z', '--']));
  const untrackedPaths = splitNullTerminated(await git(repoRoot, ['ls-files', '--others', '--exclude-standard', '-z']));
  const unexpectedUntrackedPaths = untrackedPaths.filter(relativePath => !ALLOWED_UNTRACKED_PATHS.includes(relativePath));
  if (unexpectedUntrackedPaths.length) throw mismatch(`Unexpected untracked candidate files: ${unexpectedUntrackedPaths.join(', ')}`, 'files');
  const governancePaths = new Set(GOVERNANCE_TOOLING_PATHS);
  const applicationChangedPaths = allChangedPaths.filter(relativePath => !governancePaths.has(relativePath));
  const candidatePaths = Object.keys(spec.files);
  const fileHashes = await computeCanonicalCandidateFileHashes({
    repoRoot,
    baseCommit: spec.baseCommit,
    candidatePaths,
  });
  const patchBytes = await git(repoRoot, ['diff', '--binary', '--', ...candidatePaths], null);
  const actualPatch = createHash('sha256').update(patchBytes).digest('hex');

  let canonicalCandidate = null;
  let canonicalBase = null;
  if (spec.kind === 'versioned-repository-manifest') {
    canonicalCandidate = await computeCanonicalGitReleaseFingerprints({ repoRoot, baseCommit: spec.baseCommit, approvedPaths: candidatePaths });
    canonicalBase = await computeCanonicalGitCommitFingerprints({ repoRoot, commit: spec.baseCommit });
  }

  return validateCandidateObservation({
    spec,
    requested: { candidateSpecId, qualificationMode, baseCommit, candidateManifestSha256, trackedPatchSha256, candidateSourceHash, frozenReleaseGovernanceHash, frozenFixtureSetHash, nodeEnv, apiUrl, p0DeterministicReplay },
    observation: {
      actualHead, applicationChangedPaths, fileHashes, trackedPatchSha256: actualPatch,
      candidateSourceHash: canonicalCandidate?.source?.hash || null,
      baseGovernanceHash: canonicalBase?.governance?.hash || null,
      baseFixtureSetHash: canonicalBase?.fixture?.hash || null,
    },
  });
}
