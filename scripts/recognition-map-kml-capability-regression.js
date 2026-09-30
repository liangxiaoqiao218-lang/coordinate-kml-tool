import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  COORDINATE_QUALITY_GATE_STATUS,
  FINALIZED_COORDINATE_CRS,
  finalizeCoordinateResult
} from '../server/coordinate-finalizer/index.js';
import {
  evaluateUnifiedRecognitionFinalAuthorization,
  getRecognitionAcquisitionIntegrityBlockReasons,
  getRecognitionAcquisitionMapBlockReasons
} from '../server/recognition/recognition-first-acquisition.js';
import { MapPreviewAdapter } from '../server/spatial/adapters/map-preview-adapter.js';

const root = fileURLToPath(new URL('../', import.meta.url));
const receiptDirectory = process.env.RECOGNITION_MAP_KML_RECEIPT_DIR || '';
assert.ok(receiptDirectory, 'RECOGNITION_MAP_KML_RECEIPT_DIR is required');

const multiPoint = { type: 'MultiPoint', coordinates: [[119.1, -2.1], [119.2, -2.2], [119.3, -2.3]] };
const polygon = { type: 'Polygon', coordinates: [[[119.1, -2.1], [119.2, -2.1], [119.2, -2.2], [119.1, -2.1]]] };

function finalized({ geometry = multiPoint, crs = FINALIZED_COORDINATE_CRS, currentRevision = 1,
  technicalKmlReady = false, kmlReady = false, currentAuthorizedGeometryExportable = false } = {}) {
  return finalizeCoordinateResult({
    resultId: 'map-kml-capability-result', resultRevision: 1, currentRevision,
    confirmedRevision: null, sourceAuthority: 'legacy', coordinateType: 'standard_dms_table',
    precisionMode: 'dms-review', family: 'standard_dms_table', crs, geometry,
    confirmationStatus: 'pending', qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED,
    technicalKmlReady, currentAuthorizedGeometryExportable, requiresReview: true, kmlReady,
    kmlAuthorityBlocked: false, groups: [{ groupId: 'group_1', requiresReview: true, kmlReady }]
  });
}

function state(result, reviewReasons = []) {
  const evidence = {
    acquisitionStatus: 'COMPLETED', candidateCoordinates: [{ rowIndex: 1 }],
    candidateCoordinateGroups: [{ groupId: 'group_1' }], visibleCrsEvidence: [{ id: 'EPSG:4326' }],
    imageEvidence: { imageSha256: 'synthetic' }, reviewReasons, rejectedRows: [], unboundCandidates: []
  };
  const decision = { acquisitionStatus: 'COMPLETED', authorizationStatus: 'REVIEW_REQUIRED',
    resultStatus: 'needs_review', contractReasons: reviewReasons };
  const body = { finalizedCoordinateResult: result, requiresReview: true,
    authorizationStatus: 'REVIEW_REQUIRED', resultStatus: 'needs_review', kmlReady: result.kmlReady };
  return { evidence, decision, authorization: evaluateUnifiedRecognitionFinalAuthorization({ body, evidence, decision, providerCallCount: 1 }) };
}

const checks = [];
function check(name, action) { action(); checks.push(name); console.log(`PASS ${name}`); }

check('valid review MultiPoint keeps map open while KML remains closed', () => {
  const result = finalized();
  const current = state(result, ['SOURCE_LABELS_MISSING']);
  assert.deepEqual(getRecognitionAcquisitionIntegrityBlockReasons(current), ['SOURCE_LABELS_MISSING']);
  assert.deepEqual(getRecognitionAcquisitionMapBlockReasons(current), []);
  assert.equal(current.authorization.mapReady, true);
  assert.equal(current.authorization.kmlReady, false);
  assert.equal(new MapPreviewAdapter().adapt(result, { expectedIdentity: result }).previewEligibility.allowed, true);
});

check('valid review Polygon preserves existing map and KML capability', () => {
  const result = finalized({ geometry: polygon, technicalKmlReady: true, kmlReady: true,
    currentAuthorizedGeometryExportable: true });
  const current = state(result);
  assert.equal(current.authorization.mapReady, true);
  assert.equal(current.authorization.kmlReady, true);
});

check('invalid geometry closes map and KML', () => {
  const result = finalized({ geometry: { type: 'MultiPoint', coordinates: [[Infinity, 0], [1, 1]] } });
  const current = state(result);
  assert.equal(current.authorization.mapReady, false);
  assert.equal(current.authorization.kmlReady, false);
});

check('invalid CRS closes map and KML', () => {
  const result = finalized({ crs: { id: 'EPSG:3857', axisOrder: 'easting_northing' } });
  const current = state(result);
  assert.equal(current.authorization.mapReady, false);
  assert.equal(current.authorization.kmlReady, false);
});

check('integrity conflict closes map and KML', () => {
  const result = finalized();
  const current = state(result, ['SOURCE_LABELS_DUPLICATE']);
  assert.deepEqual(getRecognitionAcquisitionMapBlockReasons(current), ['SOURCE_LABELS_DUPLICATE']);
  assert.equal(current.authorization.mapReady, false);
  assert.equal(current.authorization.kmlReady, false);
});

check('stale result revision closes map and KML', () => {
  const result = finalized({ currentRevision: 2 });
  const current = state(result);
  assert.equal(current.authorization.mapReady, false);
  assert.equal(current.authorization.kmlReady, false);
  const preview = new MapPreviewAdapter().adapt(result, { expectedIdentity: { ...result, resultRevision: 2 } });
  assert.equal(preview.previewEligibility.allowed, false);
  assert.equal(preview.previewReasonCodes[0], 'STALE_SOURCE_REVISION');
});

const http = spawnSync(process.execPath,
  ['scripts/production-core-capability-closure-p0-regression.js', '--only-map-kml-coupling'],
  { cwd: root, encoding: 'utf8', windowsHide: true, timeout: 60_000,
    env: { ...process.env, ALIYUN_API_KEY: '', DASHSCOPE_API_KEY: '', OPENAI_API_KEY: '',
      SUPABASE_URL: '', SUPABASE_SERVICE_ROLE_KEY: '', SUPABASE_ANON_KEY: '', USAGE_SERVICE_ROLE_KEY: '' } });
assert.equal(http.error, undefined, http.error?.message);
assert.equal(http.status, 0, `${http.stdout}\n${http.stderr}`);
assert.match(http.stdout, /Production Core Closure P0: 1\/1 PASS; REAL_PROVIDER_CALLS=0/);
checks.push('real local HTTP pending MultiPoint map/KML independence');
console.log('PASS real local HTTP pending MultiPoint map/KML independence');

mkdirSync(receiptDirectory, { recursive: true });
writeFileSync(path.join(receiptDirectory, 'results.json'), JSON.stringify({
  schemaVersion: 'recognition_map_kml_capability_regression_v1', origin: 'SYNTHETIC',
  replayClassification: 'LOCAL_SYNTHETIC_HTTP_SCENARIO_NOT_PRODUCTION_REPLAY',
  checks, httpStdout: http.stdout.trim(), realProviderCalls: 0
}, null, 2), { flag: 'wx' });
console.log(`Recognition Map/KML Capability: ${checks.length}/${checks.length} PASS; REAL_PROVIDER_CALLS=0`);
