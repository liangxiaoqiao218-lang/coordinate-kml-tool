import assert from 'node:assert/strict';

import {
  resolveProjectedCrs,
  transformAndVerifyProjectedPoints,
} from '../server/projection/projected-crs-registry.js';

const punctuatedBftm = resolveProjectedCrs({ name: ' bftm / ITRF 2008 ', epsg: null });
assert.equal(punctuatedBftm.status, 'identified');
assert.equal(punctuatedBftm.definition.id, 'BFTM:ITRF2008');
assert.deepEqual(punctuatedBftm.identityDiagnostic, {
  namePresent: true,
  epsgPresent: false,
  nameSyntax: 'standard_bftm_name',
  epsgSyntax: 'absent',
  nameCrsId: 'BFTM:ITRF2008',
  epsgCrsId: null,
  normalizedCrsId: 'BFTM:ITRF2008',
  normalizationStatus: 'identified',
});

for (const identifier of [
  'EPSG::32750',
  'urn:ogc:def:crs:EPSG::32750',
  'https://www.opengis.net/def/crs/EPSG/0/32750',
]) {
  const resolution = resolveProjectedCrs({ name: null, epsg: identifier });
  assert.equal(resolution.status, 'identified');
  assert.equal(resolution.definition.id, 'EPSG:32750');
  assert.equal(resolution.identityDiagnostic.epsgCrsId, 'EPSG:32750');
}

const standardName = resolveProjectedCrs({ name: 'WGS 84 / UTM Zone 30 N', epsg: null });
assert.equal(standardName.status, 'identified');
assert.equal(standardName.definition.id, 'EPSG:32630');
assert.equal(standardName.identityDiagnostic.nameSyntax, 'standard_wgs84_utm_name');

const unsupportedStandard = resolveProjectedCrs({ name: null, epsg: 'EPSG:28413' });
assert.equal(unsupportedStandard.status, 'unsupported');
assert.equal(unsupportedStandard.identityDiagnostic.epsgSyntax, 'standard_epsg_syntax');
assert.equal(unsupportedStandard.identityDiagnostic.epsgCrsId, 'EPSG:28413');
assert.equal(unsupportedStandard.identityDiagnostic.normalizedCrsId, null);

const malformed = resolveProjectedCrs({ name: null, epsg: 'EPSG:32630N' });
assert.equal(malformed.status, 'unsupported');
assert.equal(malformed.identityDiagnostic.epsgSyntax, 'invalid_epsg_syntax');
assert.equal(malformed.identityDiagnostic.epsgCrsId, null);

const unapprovedSuffix = resolveProjectedCrs({
  name: 'BFTM vendor-specific descriptive suffix',
  epsg: null,
});
assert.equal(unapprovedSuffix.status, 'unsupported');
assert.equal(unapprovedSuffix.identityDiagnostic.nameSyntax, 'supported_token_with_unapproved_suffix');
assert.equal(unapprovedSuffix.identityDiagnostic.nameCrsId, null);
assert.equal(JSON.stringify(unapprovedSuffix.identityDiagnostic).includes('vendor-specific'), false);

const unknownFreeText = resolveProjectedCrs({
  name: 'arbitrary free text that must not be reported',
  epsg: null,
});
assert.equal(unknownFreeText.status, 'unsupported');
assert.equal(unknownFreeText.identityDiagnostic.nameSyntax, 'unrecognized');
assert.equal(JSON.stringify(unknownFreeText.identityDiagnostic).includes('arbitrary'), false);

const conflict = resolveProjectedCrs({ name: 'BFTM', epsg: 'EPSG:32630' });
assert.equal(conflict.status, 'conflict');
assert.equal(conflict.identityDiagnostic.nameCrsId, 'BFTM:ITRF2008');
assert.equal(conflict.identityDiagnostic.epsgCrsId, 'EPSG:32630');
assert.equal(conflict.identityDiagnostic.normalizedCrsId, null);

const axisConflict = transformAndVerifyProjectedPoints({
  coordinateSystem: { name: 'BFTM / ITRF 2008', epsg: null },
  axisOrder: 'northing_easting',
  points: [{ x: 600000, y: 0 }],
});
assert.equal(axisConflict.valid, false);
assert.equal(axisConflict.axisStatus, 'conflict_or_unknown');
assert.equal(axisConflict.transformedPointCount, 0);

console.log(JSON.stringify({
  suite: 'coordinate-agent-phase09f-crs-identity-regression',
  status: 'PASS',
  normalizedStandardIdentifiers: [
    punctuatedBftm.identityDiagnostic.normalizedCrsId,
    standardName.identityDiagnostic.normalizedCrsId,
    'EPSG:32750',
  ],
  unsupportedStandardStatus: unsupportedStandard.status,
  malformedIdentifierStatus: malformed.status,
  conflictStatus: conflict.status,
  axisConflictStatus: axisConflict.axisStatus,
  rawNamesReported: false,
  realProviderCalls: 0,
  automaticRetries: 0,
  mapAllowed: false,
  kmlAllowed: false,
}, null, 2));
