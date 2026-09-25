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
const HASH_PATTERN = /^[a-f0-9]{64}$/;
const PROVIDER_IMAGE_ROLE_PATTERN = /^(?:overview|detail)$/;
const PROVIDER_IMAGE_MIME_PATTERN = /^image\/(?:jpeg|png|webp)$/;
const ALLOWED_QUALIFICATION_MODES = new Set(['LOCAL_PATCH_CANDIDATE', 'FROZEN_PRODUCTION']);

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
  const normalizedQualificationMode = String(qualificationMode || '').trim().toUpperCase();
  if (!ALLOWED_QUALIFICATION_MODES.has(normalizedQualificationMode)) {
    throw new Error('P0_REPLAY_STRICT_QUALIFICATION_REQUIRED');
  }
  if (!isLoopbackUrl(apiUrl)) throw new Error('P0_REPLAY_LOOPBACK_API_REQUIRED');
  return Object.freeze({
    replayLoopbackRequired: true,
    productionActivationPossible: false,
    qualificationMode: normalizedQualificationMode,
  });
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
    if (!HASH_PATTERN.test(record.sha256 || '')) throw new Error(`P0_REPLAY_HASH_INVALID:${record.caseId}`);
    if (!Array.isArray(record.approvedAcquisitionLines) || !record.approvedAcquisitionLines.length) throw new Error(`P0_REPLAY_ACQUISITION_EVIDENCE_REQUIRED:${record.caseId}`);
    if (!HASH_PATTERN.test(record.acquisitionEvidenceSha256 || '')) throw new Error(`P0_REPLAY_EVIDENCE_HASH_INVALID:${record.caseId}`);
    if (computeAcquisitionEvidenceSha256(record.approvedAcquisitionLines) !== record.acquisitionEvidenceSha256) {
      throw new Error(`P0_REPLAY_EVIDENCE_HASH_MISMATCH:${record.caseId}`);
    }
    if (!Array.isArray(record.providerInputImages) || record.providerInputImages.length < 1 || record.providerInputImages.length > 8) {
      throw new Error(`P0_REPLAY_PROVIDER_IMAGE_SET_INVALID:${record.caseId}`);
    }
    for (const [index, image] of record.providerInputImages.entries()) {
      if (!PROVIDER_IMAGE_ROLE_PATTERN.test(String(image?.role || ''))
          || !PROVIDER_IMAGE_MIME_PATTERN.test(String(image?.mimeType || ''))
          || !Number.isSafeInteger(image?.byteLength)
          || image.byteLength < 1
          || !HASH_PATTERN.test(String(image?.sha256 || ''))) {
        throw new Error(`P0_REPLAY_PROVIDER_IMAGE_IDENTITY_INVALID:${record.caseId}:${index}`);
      }
    }
  }
  return Object.freeze({
    ...manifest,
    records: Object.freeze(manifest.records.map(record => Object.freeze({
      ...record,
      approvedAcquisitionLines: Object.freeze([...record.approvedAcquisitionLines]),
      providerInputImages: Object.freeze(record.providerInputImages.map(image => Object.freeze({ ...image }))),
    }))),
  });
}

function parseCanonicalImageDataUrl(value) {
  const match = String(value || '').match(/^data:(image\/(?:jpeg|png|webp));base64,([A-Za-z0-9+/]+={0,2})$/);
  if (!match) return null;
  const bytes = Buffer.from(match[2], 'base64');
  if (!bytes.length || bytes.toString('base64') !== match[2]) return null;
  return Object.freeze({
    mimeType: match[1],
    byteLength: bytes.length,
    sha256: createHash('sha256').update(bytes).digest('hex'),
  });
}

export function extractP0ReplayProviderInputImages(payload) {
  const content = payload?.messages?.[0]?.content;
  if (!Array.isArray(content)) return null;
  const imageItems = content.filter(item => item?.type === 'image_url');
  if (imageItems.length < 1 || imageItems.length > 8) return null;
  const identities = imageItems.map(item => parseCanonicalImageDataUrl(item?.image_url?.url));
  return identities.every(Boolean) ? Object.freeze(identities) : null;
}

export function providerInputIdentityKey(images) {
  if (!Array.isArray(images) || images.length < 1) return null;
  const normalized = images.map(image => ({
    mimeType: String(image?.mimeType || ''),
    byteLength: Number(image?.byteLength),
    sha256: String(image?.sha256 || ''),
  }));
  if (normalized.some(image => !PROVIDER_IMAGE_MIME_PATTERN.test(image.mimeType)
      || !Number.isSafeInteger(image.byteLength)
      || image.byteLength < 1
      || !HASH_PATTERN.test(image.sha256))) return null;
  return JSON.stringify(normalized);
}

export function createP0ReplayProviderIndex(manifest) {
  const validated = validateP0ReplayManifest(manifest);
  const index = new Map();
  for (const record of validated.records) {
    const key = providerInputIdentityKey(record.providerInputImages);
    if (!key || index.has(key)) throw new Error(`P0_REPLAY_PROVIDER_IMAGE_IDENTITY_DUPLICATE:${record.caseId}`);
    index.set(key, record);
  }
  return index;
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
