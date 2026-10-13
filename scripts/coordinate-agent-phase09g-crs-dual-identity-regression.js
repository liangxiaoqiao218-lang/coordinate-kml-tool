import assert from 'node:assert/strict';

import {
  resolveProjectedCrs,
  transformAndVerifyProjectedPoints,
} from '../server/projection/projected-crs-registry.js';

const consistentQualifiedName = resolveProjectedCrs({
  name: 'WGS 84 / UTM zone 30N (EPSG:32630)',
  epsg: 'urn:ogc:def:crs:EPSG::32630',
});
assert.equal(consistentQualifiedName.status, 'identified');
assert.equal(consistentQualifiedName.definition.id, 'EPSG:32630');
assert.deepEqual(consistentQualifiedName.identityDiagnostic, {
  namePresent: true,
  epsgPresent: true,
  nameSyntax: 'formal_name_with_standard_epsg_qualifier',
  epsgSyntax: 'standard_epsg_syntax',
  nameCrsId: 'EPSG:32630',
  nameQualifiedEpsgId: 'EPSG:32630',
  epsgCrsId: 'EPSG:32630',
  normalizedCrsId: 'EPSG:32630',
  normalizationStatus: 'identified',
  identityConsistency: 'consistent',
});

const qualifierExternalConflict = resolveProjectedCrs({
  name: 'WGS 84 / UTM zone 30N (EPSG:32630)',
  epsg: 'EPSG:32631',
});
assert.equal(qualifierExternalConflict.status, 'conflict');
assert.equal(qualifierExternalConflict.identityDiagnostic.identityConsistency, 'conflict');
assert.equal(qualifierExternalConflict.identityDiagnostic.normalizedCrsId, null);

const nameMeaningConflict = resolveProjectedCrs({
  name: 'WGS 84 / UTM zone 30N (EPSG:32631)',
  epsg: 'EPSG:32631',
});
assert.equal(nameMeaningConflict.status, 'conflict');
assert.equal(nameMeaningConflict.identityDiagnostic.identityConsistency, 'conflict');

const nameInternalConflict = resolveProjectedCrs({
  name: 'WGS 84 / UTM zone 30N (EPSG::32631)',
  epsg: null,
});
assert.equal(nameInternalConflict.status, 'conflict');
assert.equal(nameInternalConflict.identityDiagnostic.identityConsistency, 'conflict');

const unsupportedQualifiedIdentity = resolveProjectedCrs({
  name: 'BFTM / ITRF 2008 (EPSG:31500)',
  epsg: 'EPSG:31500',
});
assert.equal(unsupportedQualifiedIdentity.status, 'unsupported');
assert.equal(unsupportedQualifiedIdentity.identityDiagnostic.nameSyntax, 'formal_name_with_standard_epsg_qualifier');
assert.equal(unsupportedQualifiedIdentity.identityDiagnostic.nameCrsId, 'BFTM:ITRF2008');
assert.equal(unsupportedQualifiedIdentity.identityDiagnostic.nameQualifiedEpsgId, 'EPSG:31500');
assert.equal(unsupportedQualifiedIdentity.identityDiagnostic.epsgCrsId, 'EPSG:31500');
assert.equal(unsupportedQualifiedIdentity.identityDiagnostic.identityConsistency, 'unverified');
assert.equal(unsupportedQualifiedIdentity.identityDiagnostic.normalizedCrsId, null);

const unknownModifier = resolveProjectedCrs({
  name: 'WGS 84 / UTM zone 30N vendor profile',
  epsg: 'EPSG:32630',
});
assert.equal(unknownModifier.status, 'unsupported');
assert.equal(unknownModifier.identityDiagnostic.nameSyntax, 'supported_token_with_unapproved_suffix');
assert.equal(unknownModifier.identityDiagnostic.identityConsistency, 'unverified');
assert.equal(JSON.stringify(unknownModifier.identityDiagnostic).includes('vendor'), false);

const unrecognizedNameCannotBeIgnored = resolveProjectedCrs({
  name: 'unregistered projected reference',
  epsg: 'EPSG:32630',
});
assert.equal(unrecognizedNameCannotBeIgnored.status, 'unsupported');
assert.equal(unrecognizedNameCannotBeIgnored.identityDiagnostic.nameSyntax, 'unrecognized');
assert.equal(unrecognizedNameCannotBeIgnored.identityDiagnostic.normalizedCrsId, null);

const axisConflict = transformAndVerifyProjectedPoints({
  coordinateSystem: {
    name: 'WGS 84 / UTM zone 30N (EPSG:32630)',
    epsg: 'EPSG:32630',
  },
  axisOrder: 'northing_easting',
  points: [{ x: 500000, y: 0 }],
});
assert.equal(axisConflict.valid, false);
assert.equal(axisConflict.axisStatus, 'conflict_or_unknown');
assert.equal(axisConflict.transformedPointCount, 0);

console.log(JSON.stringify({
  suite: 'coordinate-agent-phase09g-crs-dual-identity-regression',
  status: 'PASS',
  formalNameSyntax: consistentQualifiedName.identityDiagnostic.nameSyntax,
  consistentIdentityStatus: consistentQualifiedName.identityDiagnostic.identityConsistency,
  qualifierExternalConflictStatus: qualifierExternalConflict.status,
  nameMeaningConflictStatus: nameMeaningConflict.status,
  nameInternalConflictStatus: nameInternalConflict.status,
  unsupportedQualifiedIdentityStatus: unsupportedQualifiedIdentity.status,
  unsupportedQualifiedIdentityConsistency: unsupportedQualifiedIdentity.identityDiagnostic.identityConsistency,
  unknownModifierStatus: unknownModifier.status,
  unrecognizedNameStatus: unrecognizedNameCannotBeIgnored.status,
  axisConflictStatus: axisConflict.axisStatus,
  rawNamesReported: false,
  realProviderCalls: 0,
  automaticRetries: 0,
  mapAllowed: false,
  kmlAllowed: false,
}, null, 2));
