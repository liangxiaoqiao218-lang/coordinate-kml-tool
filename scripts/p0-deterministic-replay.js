import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';

export const P0_REPLAY_MANIFEST_PATH = 'release-governance/p0-deterministic-replay-manifest.json';
export const P0_RELEASE_GATE_GOVERNANCE_PATH = 'release-governance/p0-release-gate-governance.json';
const REQUIRED_CASE_IDS = Object.freeze([
  'indonesia-dms-real-001',
  'indonesia-projected-real-002',
  'madagascar-cadastral-real-001',
]);

export function computeAcquisitionEvidenceSha256(lines) {
  return createHash('sha256').update((lines || []).join('\n'), 'utf8').digest('hex');
}

export function isLoopbackUrl(value) {
  let url;
  try {
    url = new URL(String(value || ''));
  } catch {
    return false;
  }
  const hostname = url.hostname.toLowerCase().replace(/^\[|\]$/g, '');
  return ['localhost', '127.0.0.1', '::1'].includes(hostname);
}

export function assertP0ReplayRuntimeSafety({ apiUrl, qualificationMode, replayEnabled, nodeEnv }) {
  if (replayEnabled !== true) throw new Error('P0_REPLAY_EXPLICIT_ENABLEMENT_REQUIRED');
  if (String(nodeEnv || '').toLowerCase() === 'production') throw new Error('P0_REPLAY_PRODUCTION_MODE_FORBIDDEN');
  if (String(qualificationMode || '').toUpperCase() !== 'LOCAL_PATCH_CANDIDATE') {
    throw new Error('P0_REPLAY_LOCAL_PATCH_QUALIFICATION_REQUIRED');
  }
  if (!isLoopbackUrl(apiUrl)) throw new Error('P0_REPLAY_LOOPBACK_API_REQUIRED');
  return Object.freeze({ replayLoopbackRequired: true, productionActivationPossible: false });
}

export async function loadP0ReplayManifest(repoRoot) {
  const manifest = JSON.parse(await readFile(path.join(repoRoot, P0_REPLAY_MANIFEST_PATH), 'utf8'));
  return validateP0ReplayManifest(manifest);
}

export function validateP0ReplayManifest(manifest) {
  if (manifest.schemaVersion !== 'production_recognition_p0_replay_manifest_v1') throw new Error('P0_REPLAY_MANIFEST_SCHEMA_INVALID');
  if (manifest.networkFallbackAllowed !== false) throw new Error('P0_REPLAY_NETWORK_FALLBACK_MUST_BE_FALSE');
  if (!Array.isArray(manifest.records) || manifest.records.length !== 3) throw new Error('P0_REPLAY_EXACT_THREE_RECORDS_REQUIRED');
  const caseIds = manifest.records.map(record => record.caseId).sort();
  if (JSON.stringify(caseIds) !== JSON.stringify([...REQUIRED_CASE_IDS].sort())) throw new Error('P0_REPLAY_CASE_SET_INVALID');
  for (const record of manifest.records) {
    if (record.evidenceClass !== 'POSITIVE_APPROVED_REPLAY') throw new Error(`P0_REPLAY_EVIDENCE_CLASS_INVALID:${record.caseId}`);
    if (!/^[a-f0-9]{64}$/.test(record.sha256 || '')) throw new Error(`P0_REPLAY_HASH_INVALID:${record.caseId}`);
    if (!Array.isArray(record.approvedAcquisitionLines) || !record.approvedAcquisitionLines.length) throw new Error(`P0_REPLAY_ACQUISITION_EVIDENCE_REQUIRED:${record.caseId}`);
    if (!/^[a-f0-9]{64}$/.test(record.acquisitionEvidenceSha256 || '')) throw new Error(`P0_REPLAY_EVIDENCE_HASH_INVALID:${record.caseId}`);
    if (computeAcquisitionEvidenceSha256(record.approvedAcquisitionLines) !== record.acquisitionEvidenceSha256) {
      throw new Error(`P0_REPLAY_EVIDENCE_HASH_MISMATCH:${record.caseId}`);
    }
  }
  return Object.freeze({
    ...manifest,
    records: Object.freeze(manifest.records.map(record => Object.freeze({
      ...record,
      approvedAcquisitionLines: Object.freeze([...record.approvedAcquisitionLines]),
    }))),
  });
}

export async function loadP0ReleaseGateGovernance(repoRoot, baseline) {
  const governance = JSON.parse(await readFile(path.join(repoRoot, P0_RELEASE_GATE_GOVERNANCE_PATH), 'utf8'));
  if (governance.schemaVersion !== 'production_recognition_p0_release_gate_governance_v1') {
    throw new Error('P0_RELEASE_GATE_GOVERNANCE_SCHEMA_INVALID');
  }
  const blocked = governance.blockedNoReplayFixtures;
  if (!Array.isArray(blocked) || blocked.length !== 18) throw new Error('P0_BLOCKED_NO_REPLAY_EXACT_18_REQUIRED');
  const blockedIds = blocked.map(entry => entry.fixtureId);
  if (new Set(blockedIds).size !== blockedIds.length) throw new Error('P0_BLOCKED_NO_REPLAY_DUPLICATE_FIXTURE');
  const baselineIds = new Set((baseline?.samples || []).map(sample => sample.sample_id));
  for (const entry of blocked) {
    if (!baselineIds.has(entry.fixtureId)) throw new Error(`P0_BLOCKED_NO_REPLAY_UNKNOWN_BASELINE_FIXTURE:${entry.fixtureId}`);
    if (entry.reason !== governance.blockedNoReplayReason) throw new Error(`P0_BLOCKED_NO_REPLAY_REASON_INVALID:${entry.fixtureId}`);
    if (!entry.fixtureIdentity?.path || !/^[a-f0-9]{64}$/.test(entry.fixtureIdentity?.sha256 || '')) {
      throw new Error(`P0_BLOCKED_NO_REPLAY_IDENTITY_INVALID:${entry.fixtureId}`);
    }
    const bytes = await readFile(path.join(repoRoot, entry.fixtureIdentity.path));
    const actualSha256 = createHash('sha256').update(bytes).digest('hex');
    if (actualSha256 !== entry.fixtureIdentity.sha256) throw new Error(`P0_BLOCKED_NO_REPLAY_IDENTITY_MISMATCH:${entry.fixtureId}`);
    if (!Array.isArray(entry.directlyAffectedAreas) || !Array.isArray(entry.approvedSubstituteCoverage)) {
      throw new Error(`P0_BLOCKED_NO_REPLAY_RELATION_INVALID:${entry.fixtureId}`);
    }
  }
  const relations = governance.p0CriticalRelations || [];
  const relationIds = relations.map(entry => entry.fixtureId).sort();
  if (JSON.stringify(relationIds) !== JSON.stringify([...REQUIRED_CASE_IDS].sort())) {
    throw new Error('P0_CRITICAL_RELATION_SET_INVALID');
  }
  return Object.freeze({
    ...governance,
    blockedNoReplayFixtures: Object.freeze(blocked.map(entry => Object.freeze({
      ...entry,
      fixtureIdentity: Object.freeze({ ...entry.fixtureIdentity }),
      directlyAffectedAreas: Object.freeze([...entry.directlyAffectedAreas]),
      approvedSubstituteCoverage: Object.freeze([...entry.approvedSubstituteCoverage]),
    }))),
  });
}

export async function validateP0ReplayFixture(repoRoot, sample, manifest) {
  const record = manifest.records.find(item => item.caseId === sample.sample_id);
  if (!record) return Object.freeze({ registered: false, status: 'BLOCKED_NO_REPLAY', record: null });
  const expectedPath = path.normalize(record.fixture);
  const samplePath = path.normalize(String(sample.fixture || ''));
  if (samplePath !== expectedPath) return Object.freeze({ registered: true, status: 'BLOCKED_FIXTURE', reason: 'fixture_path_mismatch', record });
  let bytes;
  try {
    bytes = await readFile(path.join(repoRoot, record.fixture));
  } catch {
    return Object.freeze({ registered: true, status: 'BLOCKED_FIXTURE', reason: 'fixture_missing', record });
  }
  const sha256 = createHash('sha256').update(bytes).digest('hex');
  if (sha256 !== record.sha256) return Object.freeze({ registered: true, status: 'BLOCKED_FIXTURE', reason: 'fixture_hash_mismatch', actualSha256: sha256, record });
  return Object.freeze({ registered: true, status: 'READY', sha256, record });
}
