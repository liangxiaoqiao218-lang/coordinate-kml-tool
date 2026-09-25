import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  P0_REQUIRED_FIXTURE_SET,
  buildLocalPatchCandidateBindingInput,
  classifyAcquisitionTerminal,
  establishEvidenceBinding,
  evaluateP0ReleaseGate,
  summarizeResults,
} from './coordinate-regression-runner.js';
import {
  HISTORICAL_LOCAL_PATCH_CANDIDATE_SPEC_ID,
  P1_LOCAL_PATCH_CANDIDATE_SPEC_ID,
  resolveLocalPatchCandidateSpec,
  validateCandidateObservation,
} from './local-patch-candidate-identity.js';
import {
  assertP0ReplayRuntimeSafety,
  computeAcquisitionEvidenceSha256,
  createP0ReplayProviderIndex,
  extractP0ReplayProviderInputImages,
  loadP0ReleaseGateGovernance,
  loadP0ReplayManifest,
  providerInputIdentityKey,
  validateP0ReplayFixture,
  validateP0ReplayManifest,
} from './p0-deterministic-replay.js';
import {
  computeCanonicalGitCommitFingerprints,
  validateReleaseEvidenceBinding,
} from '../release-governance/evidence-binding.js';
import { canonicalizeCoordinateImageUpload } from '../server/recognition/coordinate-image-safety.js';
import { createRecognitionImageVariants } from '../server/recognition/recognition-first-acquisition.js';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const baseline = JSON.parse(await readFile(path.join(repoRoot, 'COORDINATE_RECOGNITION_GOLDEN_BASELINE.json'), 'utf8'));
const gateGovernance = await loadP0ReleaseGateGovernance(repoRoot, baseline);
const measuredZero = Object.freeze({
  measurementActive: true,
  observedProviderAcquisitionAttempts: 3,
  authorizedReplayProviderCalls: 3,
  unauthorizedProviderCalls: 0,
});
let passed = 0;
const check = async (name, fn) => {
  await fn();
  passed += 1;
  console.log(`PASS ${name}`);
};

const result = (sampleId, status, inputType = 'image') => ({
  sample: { sample_id: sampleId, input_type: inputType, baseline_status: 'locked' },
  status,
  historicalFindings: [],
  diffs: [],
  warnings: [],
  runs: [],
  semanticSummary: {},
});

await check('HTTP 400 without replay is BLOCKED_NO_REPLAY', () => {
  assert.equal(classifyAcquisitionTerminal({ httpStatus: 400, error: 'bad request' }), 'BLOCKED_NO_REPLAY');
});
await check('HTTP 400 for P0 replay is PRODUCT_FAIL', () => {
  assert.equal(classifyAcquisitionTerminal({ httpStatus: 400, error: 'bad request' }, { p0Critical: true, deterministicReplay: true }), 'PRODUCT_FAIL');
});
await check('HTTP 500 for an executed P0 text fixture is PRODUCT_FAIL', () => {
  assert.equal(classifyAcquisitionTerminal({ httpStatus: 500, error: 'server error' }, { deterministicReplay: true }), 'PRODUCT_FAIL');
});
await check('invalid success envelope cannot PASS', () => {
  assert.equal(classifyAcquisitionTerminal({ httpStatus: 200, finalizerEvaluated: false, normalizedEvidenceAvailable: false }), 'BLOCKED_NO_REPLAY');
});
await check('valid recognition envelope proceeds to comparison', () => {
  assert.equal(classifyAcquisitionTerminal({ httpStatus: 200, finalizerEvaluated: true, normalizedEvidenceAvailable: true }), null);
});

const manifest = await loadP0ReplayManifest(repoRoot);
const providerIndex = createP0ReplayProviderIndex(manifest);
const providerPayloadByCase = new Map();
await check('manifest contains only exact three approved P0 cases', () => {
  assert.deepEqual(manifest.records.map(record => record.caseId).sort(), [...P0_REQUIRED_FIXTURE_SET].sort());
  assert.equal(manifest.networkFallbackAllowed, false);
});
await check('manifest pins exact ordered Provider variant identities', async () => {
  for (const record of manifest.records) {
    const fixtureBytes = await readFile(path.join(repoRoot, record.fixture));
    const mimeType = /\.png$/i.test(record.fixture) ? 'image/png' : 'image/jpeg';
    const canonical = canonicalizeCoordinateImageUpload({
      buffer: fixtureBytes,
      size: fixtureBytes.length,
      mimetype: mimeType,
      originalname: path.basename(record.fixture),
    });
    assert.equal(canonical.valid, true, record.caseId);
    const acquisition = await createRecognitionImageVariants({
      buffer: canonical.file.buffer,
      bytes: canonical.file.buffer.length,
    });
    const actualProviderImages = acquisition.images.map(image => {
      const bytes = Buffer.from(image.dataUrl.split(',')[1], 'base64');
      return {
        role: image.role,
        mimeType: image.mimeType,
        byteLength: bytes.length,
        sha256: createHash('sha256').update(bytes).digest('hex'),
      };
    });
    assert.deepEqual(actualProviderImages, record.providerInputImages, record.caseId);
    const payload = {
      messages: [{
        role: 'user',
        content: [
          { type: 'text', text: 'offline replay' },
          ...acquisition.images.map(image => ({ type: 'image_url', image_url: { url: image.dataUrl } })),
        ],
      }],
    };
    providerPayloadByCase.set(record.caseId, payload);
    const identity = extractP0ReplayProviderInputImages(payload);
    assert.equal(providerIndex.get(providerInputIdentityKey(identity))?.caseId, record.caseId);
  }
});
await check('tampered Provider variant fails closed', () => {
  const payload = structuredClone(providerPayloadByCase.get(P0_REQUIRED_FIXTURE_SET[0]));
  const item = payload.messages[0].content.find(entry => entry.type === 'image_url');
  const [prefix, encoded] = item.image_url.url.split(',');
  item.image_url.url = `${prefix},${Buffer.concat([Buffer.from(encoded, 'base64'), Buffer.from([0])]).toString('base64')}`;
  const identity = extractP0ReplayProviderInputImages(payload);
  assert.equal(providerIndex.get(providerInputIdentityKey(identity)), undefined);
});
await check('missing Provider variant fails closed', () => {
  const payload = structuredClone(providerPayloadByCase.get(P0_REQUIRED_FIXTURE_SET[0]));
  payload.messages[0].content = payload.messages[0].content.filter(entry => entry.type !== 'image_url');
  assert.equal(extractP0ReplayProviderInputImages(payload), null);
});
await check('extra Provider variant fails closed', () => {
  const payload = structuredClone(providerPayloadByCase.get(P0_REQUIRED_FIXTURE_SET[0]));
  const image = payload.messages[0].content.find(entry => entry.type === 'image_url');
  payload.messages[0].content.push(structuredClone(image));
  const identity = extractP0ReplayProviderInputImages(payload);
  assert.equal(providerIndex.get(providerInputIdentityKey(identity)), undefined);
});
await check('missing Provider identity manifest field fails closed', () => {
  const plain = JSON.parse(JSON.stringify(manifest));
  delete plain.records[0].providerInputImages;
  assert.throws(() => validateP0ReplayManifest(plain), /P0_REPLAY_PROVIDER_IMAGE_SET_INVALID/);
});
await check('unknown fixture fails closed without replay', async () => {
  const validation = await validateP0ReplayFixture(repoRoot, { sample_id: 'unknown-image', fixture: 'unknown.jpg' }, manifest);
  assert.equal(validation.status, 'BLOCKED_NO_REPLAY');
});
await check('registered fixture path mismatch is BLOCKED_FIXTURE', async () => {
  const validation = await validateP0ReplayFixture(repoRoot, { sample_id: P0_REQUIRED_FIXTURE_SET[0], fixture: 'wrong.jpg' }, manifest);
  assert.equal(validation.status, 'BLOCKED_FIXTURE');
});
await check('registered fixture content hash mismatch is BLOCKED_FIXTURE', async () => {
  const first = manifest.records[0];
  const mismatched = { ...manifest, records: [{ ...first, sha256: '0'.repeat(64) }, ...manifest.records.slice(1)] };
  const validation = await validateP0ReplayFixture(repoRoot, { sample_id: first.caseId, fixture: first.fixture }, mismatched);
  assert.equal(validation.status, 'BLOCKED_FIXTURE');
  assert.equal(validation.reason, 'fixture_hash_mismatch');
});
await check('all exact P0 fixtures are hash-bound READY', async () => {
  for (const record of manifest.records) {
    const validation = await validateP0ReplayFixture(repoRoot, { sample_id: record.caseId, fixture: record.fixture }, manifest);
    assert.equal(validation.status, 'READY');
    assert.equal(validation.sha256, record.sha256);
  }
});
await check('replay evidence content hash mismatch fails closed', () => {
  const plain = JSON.parse(JSON.stringify(manifest));
  plain.records[2].approvedAcquisitionLines[2] = plain.records[2].approvedAcquisitionLines[2].replace('Ilakaka', 'Andriandampy');
  assert.throws(() => validateP0ReplayManifest(plain), /P0_REPLAY_EVIDENCE_HASH_MISMATCH/);
});
await check('Madagascar replay preserves exact approved 31/1 commune distribution', () => {
  const record = manifest.records.find(entry => entry.caseId === 'madagascar-cadastral-real-001');
  const rows = record.approvedAcquisitionLines.slice(2);
  assert.equal(rows.length, 32);
  assert.equal(rows.filter(row => row.includes(' | Ilakaka | ')).length, 31);
  assert.equal(rows.filter(row => row.includes(' | Andriandampy | ')).length, 1);
  assert.equal(rows[22], '23 | 294062.5 | 361562.5 | Andriandampy | 333');
  assert.equal(computeAcquisitionEvidenceSha256(record.approvedAcquisitionLines), record.acquisitionEvidenceSha256);
});
await check('non-loopback replay API is rejected before request in every supported mode', () => {
  for (const qualificationMode of ['LOCAL_PATCH_CANDIDATE', 'FROZEN_PRODUCTION']) {
    assert.throws(() => assertP0ReplayRuntimeSafety({
      apiUrl: 'https://production.example/api/recognize-coordinates',
      qualificationMode,
      replayEnabled: true,
      nodeEnv: 'test',
    }), /P0_REPLAY_LOOPBACK_API_REQUIRED/);
  }
});
await check('production replay activation is rejected in every supported mode', () => {
  for (const qualificationMode of ['LOCAL_PATCH_CANDIDATE', 'FROZEN_PRODUCTION']) {
    assert.throws(() => assertP0ReplayRuntimeSafety({
      apiUrl: 'http://127.0.0.1:32121/api/recognize-coordinates',
      qualificationMode,
      replayEnabled: true,
      nodeEnv: 'production',
    }), /P0_REPLAY_PRODUCTION_MODE_FORBIDDEN/);
  }
});
await check('committed-head replay requires an explicitly supported strict mode', () => {
  const safety = assertP0ReplayRuntimeSafety({
    apiUrl: 'http://127.0.0.1:32121/api/recognize-coordinates',
    qualificationMode: 'FROZEN_PRODUCTION',
    replayEnabled: true,
    nodeEnv: 'test',
  });
  assert.equal(safety.qualificationMode, 'FROZEN_PRODUCTION');
  assert.throws(() => assertP0ReplayRuntimeSafety({
    apiUrl: 'http://127.0.0.1:32121/api/recognize-coordinates',
    qualificationMode: 'UNBOUND_COMMITTED_HEAD',
    replayEnabled: true,
    nodeEnv: 'test',
  }), /P0_REPLAY_STRICT_QUALIFICATION_REQUIRED/);
});

const passingResults = [
  ...P0_REQUIRED_FIXTURE_SET.map(id => result(id, 'PASS')),
  ...gateGovernance.blockedNoReplayFixtures.map(entry => ({
    ...result(entry.fixtureId, 'BLOCKED_NO_REPLAY'),
    runs: [{ skipped: true, skipReason: entry.reason }],
  })),
  ...Array.from({ length: 5 }, (_, index) => result(`text-case-${index + 1}`, 'PASS', 'text')),
];
await check('exact current P0 release formula state passes', () => {
  const gate = evaluateP0ReleaseGate(passingResults, { status: 'LOCAL_PATCH_CANDIDATE_BOUND' }, gateGovernance, measuredZero);
  assert.equal(gate.status, 'PASS');
  const summary = summarizeResults(passingResults);
  assert.equal(summary.pass, 8);
  assert.equal(summary.productFail, 0);
  assert.equal(summary.blockedNoReplay, 18);
  assert.equal(summary.blockedFixture, 0);
  assert.equal(summary.baselineReviewRequired, 0);
  assert.equal(summary.skipOutOfScope, 0);
  assert.equal(gate.providerCallsMeasured, true);
  assert.equal(gate.allNoReplayExplicitlyEnumerated, true);
  assert.deepEqual(gate.directlyAffectedBlockedWithoutSubstitute, []);
});
await check('strict canonical committed-head binding passes the P0 identity gate', () => {
  const gate = evaluateP0ReleaseGate(passingResults, {
    status: 'BOUND',
    qualificationMode: 'FROZEN_PRODUCTION',
    releaseIdentityAuthority: true,
    sourceIdentityAuthority: 'GIT_CANONICAL_RELEASE_TREE',
  }, gateGovernance, measuredZero);
  assert.equal(gate.identityBindingPass, true);
  assert.equal(gate.status, 'PASS');
});
await check('unattested committed-head status cannot pass the P0 identity gate', () => {
  const gate = evaluateP0ReleaseGate(passingResults, {
    status: 'BOUND',
    qualificationMode: 'FROZEN_PRODUCTION',
    releaseIdentityAuthority: false,
    sourceIdentityAuthority: 'WORKING_TREE_BYTES_LEGACY_DIAGNOSTIC_NON_AUTHORITY',
  }, gateGovernance, measuredZero);
  assert.equal(gate.identityBindingPass, false);
  assert.equal(gate.status, 'FAIL');
});
await check('wrong committed-head canonical hash fails closed', async () => {
  const canonical = await computeCanonicalGitCommitFingerprints({ repoRoot, commit: 'HEAD' });
  await assert.rejects(validateReleaseEvidenceBinding({
    repoRoot,
    canonicalCommit: 'HEAD',
    runtimeIdentity: { runtimeSourceSha256: canonical.source.hash },
    frozenIdentity: {
      productionSourceHash: '0'.repeat(64),
      releaseGovernanceHash: canonical.governance.hash,
      fixtureSetHash: canonical.fixture.hash,
    },
  }), error => error?.code === 'EVIDENCE_BINDING_MISMATCH'
    && error?.mismatches?.includes('production_source_vs_frozen'));
});
await check('P0 gate rejects any product failure', () => {
  const gate = evaluateP0ReleaseGate([...passingResults, result('runtime-regression', 'PRODUCT_FAIL')], { status: 'LOCAL_PATCH_CANDIDATE_BOUND' }, gateGovernance, measuredZero);
  assert.equal(gate.status, 'FAIL');
});
await check('P0 gate rejects blocked required fixture', () => {
  const failed = passingResults.map(entry => entry.sample.sample_id === P0_REQUIRED_FIXTURE_SET[0]
    ? result(P0_REQUIRED_FIXTURE_SET[0], 'BLOCKED_FIXTURE')
    : entry);
  assert.equal(evaluateP0ReleaseGate(failed, { status: 'LOCAL_PATCH_CANDIDATE_BOUND' }, gateGovernance, measuredZero).status, 'FAIL');
});
await check('unexpected BLOCKED_NO_REPLAY fixture fails enumeration', () => {
  const gate = evaluateP0ReleaseGate([...passingResults, {
    ...result('unexpected-image', 'BLOCKED_NO_REPLAY'),
    runs: [{ skipped: true, skipReason: 'no_approved_deterministic_replay' }],
  }], { status: 'LOCAL_PATCH_CANDIDATE_BOUND' }, gateGovernance, measuredZero);
  assert.equal(gate.status, 'FAIL');
  assert.deepEqual(gate.unexpectedBlockedNoReplayFixtures, ['unexpected-image']);
});
await check('directly affected blocked fixture without substitute fails gate', () => {
  const plain = JSON.parse(JSON.stringify(gateGovernance));
  plain.blockedNoReplayFixtures[0].directlyAffectedAreas = ['MADAGASCAR_CADASTRAL_ACQUISITION_CRS_GEOMETRY_AND_KML_POLICY'];
  const gate = evaluateP0ReleaseGate(passingResults, { status: 'LOCAL_PATCH_CANDIDATE_BOUND' }, plain, measuredZero);
  assert.equal(gate.status, 'FAIL');
  assert.deepEqual(gate.directlyAffectedBlockedWithoutSubstitute, [plain.blockedNoReplayFixtures[0].fixtureId]);
});
await check('measured unauthorized Provider call fails gate', () => {
  const measurement = { ...measuredZero, observedProviderAcquisitionAttempts: 4, unauthorizedProviderCalls: 1 };
  const gate = evaluateP0ReleaseGate(passingResults, { status: 'LOCAL_PATCH_CANDIDATE_BOUND' }, gateGovernance, measurement);
  assert.equal(gate.status, 'FAIL');
  assert.equal(gate.unauthorizedProviderCalls, 1);
});
await check('summary emits all six terminal counts and IDs', () => {
  const statuses = ['PASS', 'PRODUCT_FAIL', 'BLOCKED_NO_REPLAY', 'BLOCKED_FIXTURE', 'BASELINE_REVIEW_REQUIRED', 'SKIP_OUT_OF_SCOPE'];
  const summary = summarizeResults(statuses.map((status, index) => result(`case-${index}`, status)));
  assert.equal(summary.pass, 1);
  assert.equal(summary.productFail, 1);
  assert.equal(summary.blockedNoReplay, 1);
  assert.equal(summary.blockedFixture, 1);
  assert.equal(summary.baselineReviewRequired, 1);
  assert.equal(summary.skipOutOfScope, 1);
});

const p1Spec = await resolveLocalPatchCandidateSpec({ repoRoot, candidateSpecId: P1_LOCAL_PATCH_CANDIDATE_SPEC_ID });
const p1Environment = Object.freeze({
  QUALIFICATION_MODE: 'LOCAL_PATCH_CANDIDATE',
  LOCAL_PATCH_CANDIDATE_SPEC_ID: P1_LOCAL_PATCH_CANDIDATE_SPEC_ID,
  BASE_COMMIT: p1Spec.baseCommit,
  CANDIDATE_MANIFEST_SHA256: p1Spec.candidateManifestSha256,
  TRACKED_PATCH_SHA256: p1Spec.trackedPatchSha256,
  CANDIDATE_SOURCE_HASH: p1Spec.candidateSourceHash,
  FROZEN_RELEASE_GOVERNANCE_HASH: p1Spec.frozenReleaseGovernanceHash,
  FROZEN_FIXTURE_SET_HASH: p1Spec.frozenFixtureSetHash,
  P0_DETERMINISTIC_REPLAY: '1',
  NODE_ENV: 'test',
  COORDINATE_REGRESSION_API_URL: 'http://127.0.0.1:3000/api/recognize-coordinates',
});
const p1Observation = Object.freeze({
  actualHead: p1Spec.baseCommit,
  applicationChangedPaths: Object.keys(p1Spec.files),
  fileHashes: { ...p1Spec.files },
  trackedPatchSha256: p1Spec.trackedPatchSha256,
  candidateSourceHash: p1Spec.candidateSourceHash,
  baseGovernanceHash: p1Spec.frozenReleaseGovernanceHash,
  baseFixtureSetHash: p1Spec.frozenFixtureSetHash,
});

await check('runner forwards explicit historical spec selection without implicit replacement', () => {
  const input = buildLocalPatchCandidateBindingInput({
    ...p1Environment,
    LOCAL_PATCH_CANDIDATE_SPEC_ID: HISTORICAL_LOCAL_PATCH_CANDIDATE_SPEC_ID,
  });
  assert.equal(input.candidateSpecId, HISTORICAL_LOCAL_PATCH_CANDIDATE_SPEC_ID);
});
await check('runner forwards every explicit P1 binding input', () => {
  const input = buildLocalPatchCandidateBindingInput(p1Environment);
  assert.equal(input.candidateSpecId, P1_LOCAL_PATCH_CANDIDATE_SPEC_ID);
  assert.equal(input.qualificationMode, 'LOCAL_PATCH_CANDIDATE');
  assert.equal(input.baseCommit, p1Spec.baseCommit);
  assert.equal(input.candidateManifestSha256, p1Spec.candidateManifestSha256);
  assert.equal(input.trackedPatchSha256, p1Spec.trackedPatchSha256);
  assert.equal(input.candidateSourceHash, p1Spec.candidateSourceHash);
  assert.equal(input.frozenReleaseGovernanceHash, p1Spec.frozenReleaseGovernanceHash);
  assert.equal(input.frozenFixtureSetHash, p1Spec.frozenFixtureSetHash);
  assert.equal(input.p0DeterministicReplay, '1');
});
await check('P1 frozen identity contract remains valid after later merge commits', () => {
  const binding = validateCandidateObservation({
    spec: p1Spec,
    requested: buildLocalPatchCandidateBindingInput(p1Environment),
    observation: p1Observation,
  });
  assert.equal(binding.status, 'LOCAL_PATCH_CANDIDATE_BOUND');
  assert.equal(binding.candidateSpecId, P1_LOCAL_PATCH_CANDIDATE_SPEC_ID);
});
await check('missing spec selection stops without implicit fallback or acquisition', async () => {
  let acquisitionCalls = 0;
  await assert.rejects(
    establishEvidenceBinding({ ...p1Environment, LOCAL_PATCH_CANDIDATE_SPEC_ID: '' }).then(() => { acquisitionCalls += 1; }),
    error => error?.code === 'EVIDENCE_BINDING_MISMATCH' && error?.field === 'LOCAL_PATCH_CANDIDATE_SPEC_ID',
  );
  assert.equal(acquisitionCalls, 0);
});
await check('identity mismatch stops before acquisition with zero Provider fallback', async () => {
  let acquisitionCalls = 0;
  assert.throws(
    () => {
      validateCandidateObservation({
        spec: p1Spec,
        requested: buildLocalPatchCandidateBindingInput({ ...p1Environment, TRACKED_PATCH_SHA256: '0'.repeat(64) }),
        observation: p1Observation,
      });
      acquisitionCalls += 1;
    },
    error => error?.code === 'EVIDENCE_BINDING_MISMATCH' && error?.field === 'TRACKED_PATCH_SHA256',
  );
  assert.equal(acquisitionCalls, 0);
});

console.log(`COORDINATE_REGRESSION_RUNNER_RECONCILIATION=PASS (${passed}/${passed})`);
console.log('HTTP_400_CAN_NEVER_PASS=true');
console.log('NETWORK_FALLBACK_ALLOWED=false');
console.log('EXPLICIT_CANDIDATE_SPEC_SELECTION=true');
console.log('REAL_PROVIDER_CALLS=0');
