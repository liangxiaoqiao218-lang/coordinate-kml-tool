import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import path from 'node:path';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);
const HASH_PATTERN = /^[a-f0-9]{64}$/;
const COMMIT_PATTERN = /^[a-f0-9]{40}$/;

export const LOCAL_PATCH_CANDIDATE_SPEC = Object.freeze({
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

function mismatch(message, field) {
  const error = new Error(message);
  error.code = 'EVIDENCE_BINDING_MISMATCH';
  error.field = field;
  return error;
}

function requireExact(value, expected, name, pattern) {
  const normalized = String(value || '').trim().toLowerCase();
  if (!pattern.test(normalized)) throw mismatch(`${name} is malformed.`, name);
  if (normalized !== expected) throw mismatch(`${name} does not match the frozen local candidate.`, name);
  return normalized;
}

async function git(repoRoot, args, encoding = 'utf8') {
  const { stdout } = await execFileAsync('git', args, {
    cwd: repoRoot,
    encoding,
    maxBuffer: 16 * 1024 * 1024,
    windowsHide: true,
  });
  return stdout;
}

export async function validateLocalPatchCandidateIdentity({
  repoRoot,
  baseCommit,
  candidateManifestSha256,
  trackedPatchSha256,
}) {
  const spec = LOCAL_PATCH_CANDIDATE_SPEC;
  const requestedBase = requireExact(baseCommit, spec.baseCommit, 'BASE_COMMIT', COMMIT_PATTERN);
  const requestedManifest = requireExact(candidateManifestSha256, spec.candidateManifestSha256, 'CANDIDATE_MANIFEST_SHA256', HASH_PATTERN);
  const requestedPatch = requireExact(trackedPatchSha256, spec.trackedPatchSha256, 'TRACKED_PATCH_SHA256', HASH_PATTERN);
  const actualHead = String(await git(repoRoot, ['rev-parse', 'HEAD'])).trim().toLowerCase();
  if (actualHead !== requestedBase) throw mismatch('BASE_COMMIT does not match HEAD.', 'BASE_COMMIT');

  for (const [relativePath, expectedHash] of Object.entries(spec.files)) {
    let bytes;
    try {
      bytes = await readFile(path.join(repoRoot, relativePath));
    } catch {
      throw mismatch(`Candidate manifest file is missing: ${relativePath}`, 'CANDIDATE_MANIFEST_SHA256');
    }
    const actualHash = createHash('sha256').update(bytes).digest('hex');
    if (actualHash !== expectedHash) {
      throw mismatch(`Candidate manifest content mismatch: ${relativePath}`, 'CANDIDATE_MANIFEST_SHA256');
    }
  }

  const candidatePaths = Object.keys(spec.files);
  const patchBytes = await git(repoRoot, ['diff', '--binary', '--', ...candidatePaths], null);
  const actualPatch = createHash('sha256').update(patchBytes).digest('hex');
  if (actualPatch !== requestedPatch) throw mismatch('TRACKED_PATCH_SHA256 does not match the frozen application patch.', 'TRACKED_PATCH_SHA256');

  return Object.freeze({
    status: 'LOCAL_PATCH_CANDIDATE_BOUND',
    qualificationMode: 'LOCAL_PATCH_CANDIDATE',
    baseCommit: requestedBase,
    baseCommitMatch: true,
    candidateManifestSha256: requestedManifest,
    candidateManifestSha256Match: true,
    trackedPatchSha256: actualPatch,
    trackedPatchSha256Match: true,
    productionSourceHash: null,
    runtimeSourceHash: null,
    releaseGovernanceHash: null,
    fixtureSetHash: null,
  });
}
