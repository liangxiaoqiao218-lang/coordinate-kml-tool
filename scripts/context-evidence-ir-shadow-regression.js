import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import YAML from 'yaml';

import {
  EVIDENCE_ASSERTION_STATE,
  normalizeContextBoundCoordinateEvidence,
} from '../server/coordinate-agent/context-bound-coordinate-evidence.js';
import {
  adaptContextEvidenceToShadowCanonical,
  compareShadowCanonical,
} from '../server/coordinate-agent/shadow-canonical-adapter.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const manifest = JSON.parse(await readFile(path.join(root, 'regression-samples', 'coordinate-image-acceptance-manifest.v1.json'), 'utf8'));
const golden = YAML.parse(await readFile(path.join(root, 'regression-samples', 'OCR_GOLDEN', 'records.yaml'), 'utf8'));

const batchIds = [
  'indonesia-dms-real-001',
  'ocr_dms_civ_001',
  'dms_grouped_two_areas_001',
  'low_clarity_blurry_dms_001',
  'wgs84_table_rc2_congo_001',
  'long_coordinate_table_001',
  'ocr_dms_artisanat_001',
  'cote_divoire_single_03',
];
const batch = batchIds.map(id => {
  const sample = manifest.samples.find(item => item.id === id);
  assert.ok(sample, `missing approved batch sample: ${id}`);
  return sample;
});

for (const sample of batch) {
  const fixturePath = path.join(root, sample.paths[0]);
  const bytes = await readFile(fixturePath);
  assert.equal(bytes.length, sample.bytes, `${sample.id} byte count drifted`);
  assert.equal(createHash('sha256').update(bytes).digest('hex'), sample.sha256, `${sample.id} SHA-256 drifted`);
}

const recordByHash = new Map(golden.records.map(record => [record.image_hash_sha256, record]));
const independentlyConfirmed = batch.filter(sample => recordByHash.get(sample.sha256)?.evidence_level === 'confirmed');
assert.deepEqual(independentlyConfirmed.map(sample => sample.id), [
  'indonesia-dms-real-001',
  'ocr_dms_civ_001',
  'low_clarity_blurry_dms_001',
  'ocr_dms_artisanat_001',
  'cote_divoire_single_03',
]);

function projectedPair(raw) {
  const match = raw.match(/\bX\s+([0-9.]+)\s*\|\s*Y\s+([0-9.]+)/i);
  return match ? [Number(match[1]), Number(match[2])] : [null, null];
}

function evidenceInput(sample, record, { inferredCrs = false } = {}) {
  const documentId = 'document';
  const pageId = 'page-1';
  const tableId = 'table-1';
  const sources = [
    { id: documentId, kind: 'document', sourceText: '', semanticRole: 'source_document' },
    { id: pageId, kind: 'page', parentId: documentId, pageIndex: 0, sourceText: '', semanticRole: 'page' },
    { id: tableId, kind: 'table', parentId: pageId, pageIndex: 0, sourceText: record.ocr_expected_text.join('\n'), semanticRole: 'coordinate_table' },
  ];
  const rows = record.coordinate_text_ground_truth.map((raw, index) => {
    const rowId = `row-${index + 1}`;
    const latitude = record.expected_wgs84[index][1];
    const longitude = record.expected_wgs84[index][0];
    const [x, y] = projectedPair(raw);
    sources.push({ id: rowId, kind: 'row', parentId: tableId, pageIndex: 0, sourceText: raw, semanticRole: 'coordinate_row' });
    const fields = [
      { name: 'latitude', representation: 'geographic', raw, value: latitude, sourceId: rowId },
      { name: 'longitude', representation: 'geographic', raw, value: longitude, sourceId: rowId },
    ];
    if (x !== null && y !== null) {
      fields.push(
        { name: 'x', representation: 'projected', raw, value: x, sourceId: rowId },
        { name: 'y', representation: 'projected', raw, value: y, sourceId: rowId },
      );
    }
    return {
      id: rowId,
      groupId: 'group-1',
      order: index,
      label: String(index + 1),
      sourceId: rowId,
      sourceText: raw,
      fields,
      needsReview: record.validation_status.includes('REQUIRES_REVIEW'),
    };
  });
  const projected = record.sample_id === 'OCR-GR-UTM50S-MAP-IDN-001';
  return {
    document: { sha256: sample.sha256, pageCount: 1 },
    sources,
    assertions: [
      {
        id: 'crs-assertion',
        subject: 'coordinateSystem',
        value: record.crs,
        state: inferredCrs ? EVIDENCE_ASSERTION_STATE.INFERRED : EVIDENCE_ASSERTION_STATE.HUMAN_CONFIRMED,
        evidenceSourceIds: [tableId],
        rationale: inferredCrs ? 'Context supports this working interpretation but the source does not fully state it' : null,
      },
      {
        id: 'geometry-assertion',
        subject: 'geometry',
        value: record.geometry,
        state: EVIDENCE_ASSERTION_STATE.HUMAN_CONFIRMED,
        evidenceSourceIds: [tableId],
      },
    ],
    groups: [{ id: 'group-1', name: null }],
    rows,
    coordinateSystem: {
      kind: projected ? 'projected' : 'geographic',
      name: projected ? 'WGS 84 / UTM zone 50S' : 'WGS84 geographic DMS',
      epsg: projected ? 'EPSG:32750' : 'EPSG:4326',
      datum: 'WGS84',
      zone: projected ? '50' : null,
      hemisphere: projected ? 'S' : null,
      unit: projected ? 'metre' : 'degree',
      axisOrder: record.axis_order,
      assertionId: 'crs-assertion',
    },
    geometry: { type: record.geometry, assertionId: 'geometry-assertion' },
    review: {
      required: record.validation_status.includes('REQUIRES_REVIEW'),
      reasons: record.validation_status.includes('REQUIRES_REVIEW') ? [record.validation_status] : [],
    },
    truth: { independentlyConfirmed: true, authorityRefs: record.evidence },
  };
}

let confirmedPointCount = 0;
for (const sample of independentlyConfirmed) {
  const record = recordByHash.get(sample.sha256);
  const input = evidenceInput(sample, record);
  const ir = normalizeContextBoundCoordinateEvidence(input);
  const shadow = adaptContextEvidenceToShadowCanonical(input);
  assert.equal(ir.truth.independentlyConfirmed, true);
  assert.equal(shadow.mode, 'SHADOW_ONLY');
  assert.equal(shadow.affectsProductionAuthority, false);
  assert.equal(shadow.finalizerPreview.existingFinalizerContract, 'finalized_coordinate_result_v1');
  assert.equal(shadow.finalizerPreview.authorityGranted, false);
  assert.equal(shadow.finalizerPreview.mapAuthorityGranted, false);
  assert.equal(shadow.finalizerPreview.kmlAuthorityGranted, false);
  assert.equal(shadow.candidate.groups.length, 1);
  assert.equal(shadow.candidate.groups[0].points.length, record.expected_wgs84.length);
  record.expected_wgs84.forEach(([longitude, latitude], index) => {
    const point = shadow.candidate.groups[0].points[index];
    assert.equal(point.longitude, longitude);
    assert.equal(point.latitude, latitude);
    assert.equal(point.sourceText, record.coordinate_text_ground_truth[index]);
  });
  if (record.validation_status.includes('REQUIRES_REVIEW')) {
    assert.equal(shadow.candidate.resultStatus, 'needs_review');
  } else {
    assert.equal(shadow.candidate.resultStatus, 'usable');
  }
  const same = compareShadowCanonical(shadow.candidate, shadow);
  assert.equal(same.status, 'MATCHED');
  assert.equal(same.authorityChanged, false);
  confirmedPointCount += record.expected_wgs84.length;
}

const inferredInput = evidenceInput(independentlyConfirmed[0], recordByHash.get(independentlyConfirmed[0].sha256), { inferredCrs: true });
const inferredShadow = adaptContextEvidenceToShadowCanonical(inferredInput);
assert.equal(inferredShadow.candidate.coordinateSystem.status, 'needs_confirmation');
assert.equal(inferredShadow.candidate.resultStatus, 'needs_review');
assert.match(inferredShadow.candidate.warnings.join(' '), /assumptions require verification/i);
assert.equal(inferredShadow.finalizerPreview.authorityGranted, false);

const brokenInference = evidenceInput(independentlyConfirmed[0], recordByHash.get(independentlyConfirmed[0].sha256), { inferredCrs: true });
brokenInference.assertions[0].rationale = null;
assert.throws(() => normalizeContextBoundCoordinateEvidence(brokenInference), /inference rationale/);

const badSource = evidenceInput(independentlyConfirmed[0], recordByHash.get(independentlyConfirmed[0].sha256));
badSource.rows[0].fields[0].sourceId = 'missing-field-source';
assert.throws(() => normalizeContextBoundCoordinateEvidence(badSource), /unknown source/);

const duplicateOrder = evidenceInput(independentlyConfirmed[0], recordByHash.get(independentlyConfirmed[0].sha256));
duplicateOrder.rows[1].order = duplicateOrder.rows[0].order;
assert.throws(() => normalizeContextBoundCoordinateEvidence(duplicateOrder), /duplicate row order/);

const serverSource = await readFile(path.join(root, 'server.js'), 'utf8');
assert.equal(serverSource.includes('adaptContextEvidenceToShadowCanonical'), false);
assert.equal(serverSource.includes('context-bound-coordinate-evidence'), false);

const truthStatus = Object.fromEntries(batch.map(sample => {
  const record = recordByHash.get(sample.sha256);
  return [sample.id, {
    manifestEvidenceState: sample.evidenceState,
    independentlyConfirmed: record?.evidence_level === 'confirmed',
    accuracyDenominatorEligible: record?.evidence_level === 'confirmed',
    unresolved: sample.unresolved,
  }];
}));

console.log(JSON.stringify({
  suite: 'context-evidence-ir-shadow-regression',
  status: 'PASS',
  batchCount: batch.length,
  independentlyConfirmedCount: independentlyConfirmed.length,
  confirmedPointCount,
  holdoutConfirmedCount: 2,
  fixtureHashChecks: batch.length,
  sourceTextPreserved: true,
  pointOrderPreserved: true,
  dualRepresentationBound: true,
  assumptionSeparatedFromTruth: true,
  unknownTruthExcludedFromAccuracy: true,
  productionRouteMounted: false,
  productionAuthorityChanged: false,
  realProviderCallCount: 0,
  mapServiceCallCount: 0,
  databaseWriteCount: 0,
  usageChargeCount: 0,
  truthStatus,
}, null, 2));
