import { bftmToWgs84 } from './bftm.js';
import { utmToWgs84 } from './utm.js';

export const PROJECTED_ROUND_TRIP_TOLERANCE_METERS = 0.5;

const WGS84_A = 6378137;
const WGS84_E = 0.08181919084262149;
const WGS84_E2 = WGS84_E ** 2;
const WGS84_EP2 = WGS84_E2 / (1 - WGS84_E2);

function normalizeIdentity(value) {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/[_-]+/gu, ' ')
    .replace(/[^A-Z0-9: ]+/gu, ' ')
    .replace(/\s+/gu, ' ')
    .replace(/\bITRF\s+(\d{4})\b/gu, 'ITRF$1');
}

function normalizeEpsg(value) {
  const source = String(value || '').trim();
  const patterns = [
    /^(?:EPSG\s*:{1,2}\s*)?(\d{4,6})$/iu,
    /^URN:OGC:DEF:CRS:EPSG:{1,2}(\d{4,6})$/iu,
    /^HTTPS?:\/\/(?:WWW\.)?OPENGIS\.NET\/DEF\/CRS\/EPSG\/0\/(\d{4,6})\/?$/iu,
  ];
  for (const pattern of patterns) {
    const match = source.match(pattern);
    if (match) return `EPSG:${match[1]}`;
  }
  return null;
}

function utmDefinitionFromEpsg(epsg) {
  const code = Number(String(epsg || '').replace(/^EPSG:/u, ''));
  const northern = code >= 32601 && code <= 32660;
  const southern = code >= 32701 && code <= 32760;
  if (!northern && !southern) return null;
  const zone = code % 100;
  return Object.freeze({
    id: `EPSG:${code}`,
    projection: 'utm',
    axisOrder: 'easting_northing',
    zone,
    northernHemisphere: northern,
    centralMeridian: (zone - 1) * 6 - 180 + 3,
    falseEasting: 500000,
    falseNorthing: northern ? 0 : 10000000,
    scaleFactor: 0.9996,
  });
}

const BFTM_ALIASES = new Set(['BFTM', 'BFTM:ITRF2008', 'BFTM ITRF2008']);
const BFTM_DEFINITION = Object.freeze({
  id: 'BFTM:ITRF2008',
  projection: 'transverse-mercator',
  axisOrder: 'easting_northing',
  centralMeridian: -1.5,
  falseEasting: 600000,
  falseNorthing: 0,
  scaleFactor: 0.9996,
});

function utmDefinitionFromExplicitName(value) {
  const normalized = normalizeIdentity(value);
  const match = normalized.match(/^WGS ?84 UTM(?: ZONE)? (\d{1,2})\s*([NS])$/u);
  if (!match) return null;
  const zone = Number(match[1]);
  if (!Number.isInteger(zone) || zone < 1 || zone > 60) return null;
  const code = (match[2] === 'N' ? 32600 : 32700) + zone;
  return utmDefinitionFromEpsg(`EPSG:${code}`);
}

function definitionForExplicitIdentity(value) {
  const epsg = normalizeEpsg(value);
  if (epsg) return utmDefinitionFromEpsg(epsg);
  if (BFTM_ALIASES.has(normalizeIdentity(value))) return BFTM_DEFINITION;
  return utmDefinitionFromExplicitName(value);
}

function classifyNameIdentity(value, definition) {
  if (value === null || String(value).trim() === '') return 'absent';
  if (definition?.id === BFTM_DEFINITION.id) return 'standard_bftm_name';
  if (definition?.projection === 'utm') return 'standard_wgs84_utm_name';
  const normalized = normalizeIdentity(value);
  if (/\b(?:BFTM|UTM|EPSG|ITRF|WGS)\b/u.test(normalized)) {
    return 'supported_token_with_unapproved_suffix';
  }
  return 'unrecognized';
}

function buildIdentityDiagnostic({ name, epsg, byName, byEpsg, explicitEpsg, status }) {
  const nameSyntax = classifyNameIdentity(name, byName);
  const epsgSyntax = epsg === null || String(epsg).trim() === ''
    ? 'absent'
    : explicitEpsg
      ? 'standard_epsg_syntax'
      : 'invalid_epsg_syntax';
  const nameCrsId = byName?.id || null;
  const epsgCrsId = explicitEpsg || null;
  const normalizedCrsId = status === 'identified'
    ? (byEpsg || byName)?.id || null
    : null;
  return Object.freeze({
    namePresent: nameSyntax !== 'absent',
    epsgPresent: epsgSyntax !== 'absent',
    nameSyntax,
    epsgSyntax,
    nameCrsId,
    epsgCrsId,
    normalizedCrsId,
    normalizationStatus: status,
  });
}

export function resolveProjectedCrs({ name = null, epsg = null } = {}) {
  const byEpsg = epsg === null ? null : definitionForExplicitIdentity(epsg);
  const byName = name === null ? null : definitionForExplicitIdentity(name);
  const explicitEpsg = epsg === null ? null : normalizeEpsg(epsg);
  if (epsg !== null && !explicitEpsg) {
    const status = 'unsupported';
    return Object.freeze({
      status,
      definition: null,
      reason: 'CRS_IDENTIFIER_UNSUPPORTED',
      identityDiagnostic: buildIdentityDiagnostic({ name, epsg, byName, byEpsg, explicitEpsg, status }),
    });
  }
  if (epsg !== null && !byEpsg) {
    const status = 'unsupported';
    return Object.freeze({
      status,
      definition: null,
      reason: 'CRS_IDENTIFIER_UNSUPPORTED',
      identityDiagnostic: buildIdentityDiagnostic({ name, epsg, byName, byEpsg, explicitEpsg, status }),
    });
  }
  if (byEpsg && byName && byEpsg.id !== byName.id) {
    const status = 'conflict';
    return Object.freeze({
      status,
      definition: null,
      reason: 'CRS_IDENTIFIER_CONFLICT',
      identityDiagnostic: buildIdentityDiagnostic({ name, epsg, byName, byEpsg, explicitEpsg, status }),
    });
  }
  const definition = byEpsg || byName;
  if (!definition) {
    const status = 'unsupported';
    return Object.freeze({
      status,
      definition: null,
      reason: 'CRS_IDENTIFIER_UNSUPPORTED',
      identityDiagnostic: buildIdentityDiagnostic({ name, epsg, byName, byEpsg, explicitEpsg, status }),
    });
  }
  const status = 'identified';
  return Object.freeze({
    status,
    definition,
    reason: null,
    identityDiagnostic: buildIdentityDiagnostic({ name, epsg, byName, byEpsg, explicitEpsg, status }),
  });
}

function meridionalArc(latitudeRadians) {
  return WGS84_A * (
    (1 - WGS84_E2 / 4 - 3 * WGS84_E2 ** 2 / 64 - 5 * WGS84_E2 ** 3 / 256) * latitudeRadians
    - (3 * WGS84_E2 / 8 + 3 * WGS84_E2 ** 2 / 32 + 45 * WGS84_E2 ** 3 / 1024) * Math.sin(2 * latitudeRadians)
    + (15 * WGS84_E2 ** 2 / 256 + 45 * WGS84_E2 ** 3 / 1024) * Math.sin(4 * latitudeRadians)
    - (35 * WGS84_E2 ** 3 / 3072) * Math.sin(6 * latitudeRadians)
  );
}

function wgs84ToTransverseMercator(latitude, longitude, definition) {
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)
    || latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180) return null;
  const phi = latitude * Math.PI / 180;
  const lambdaDelta = (longitude - definition.centralMeridian) * Math.PI / 180;
  const sinPhi = Math.sin(phi);
  const cosPhi = Math.cos(phi);
  const tanPhi = Math.tan(phi);
  const n = WGS84_A / Math.sqrt(1 - WGS84_E2 * sinPhi ** 2);
  const t = tanPhi ** 2;
  const c = WGS84_EP2 * cosPhi ** 2;
  const a = cosPhi * lambdaDelta;
  const easting = definition.falseEasting + definition.scaleFactor * n * (
    a + (1 - t + c) * a ** 3 / 6
    + (5 - 18 * t + t ** 2 + 72 * c - 58 * WGS84_EP2) * a ** 5 / 120
  );
  const northing = definition.falseNorthing + definition.scaleFactor * (
    meridionalArc(phi) + n * tanPhi * (
      a ** 2 / 2
      + (5 - t + 9 * c + 4 * c ** 2) * a ** 4 / 24
      + (61 - 58 * t + t ** 2 + 600 * c - 330 * WGS84_EP2) * a ** 6 / 720
    )
  );
  return Number.isFinite(easting) && Number.isFinite(northing)
    ? Object.freeze({ easting, northing })
    : null;
}

function projectedToWgs84(definition, x, y) {
  if (definition.projection === 'utm') {
    const result = utmToWgs84(definition.zone, x, y, definition.northernHemisphere);
    return result ? Object.freeze({ latitude: result.lat, longitude: result.lon }) : null;
  }
  if (definition.id === BFTM_DEFINITION.id) {
    const result = bftmToWgs84(x, y);
    return result ? Object.freeze({ latitude: result.latitude, longitude: result.longitude }) : null;
  }
  return null;
}

export function transformAndVerifyProjectedPoints({ coordinateSystem, axisOrder, points, toleranceMeters = PROJECTED_ROUND_TRIP_TOLERANCE_METERS } = {}) {
  const resolution = resolveProjectedCrs(coordinateSystem);
  const normalizedAxis = String(axisOrder || '').trim().toLowerCase();
  const axisValid = normalizedAxis === 'easting_northing';
  const boundedTolerance = Number(toleranceMeters);
  if (resolution.status !== 'identified' || !axisValid
    || !Number.isFinite(boundedTolerance) || boundedTolerance <= 0 || boundedTolerance > 1) {
    return Object.freeze({
      valid: false,
      crsStatus: resolution.status,
      crsId: resolution.definition?.id || null,
      registryMatched: false,
      axisStatus: axisValid ? 'identified' : 'conflict_or_unknown',
      pointCount: Array.isArray(points) ? points.length : 0,
      transformedPointCount: 0,
      roundTripVerifiedPointCount: 0,
      forwardStatus: 'not_completed',
      inverseStatus: 'not_completed',
      spatialStatus: 'not_completed',
      toleranceMeters: boundedTolerance,
      maximumRoundTripErrorMeters: null,
      identityDiagnostic: resolution.identityDiagnostic,
      transformedPoints: Object.freeze([]),
      failureCode: resolution.reason || 'PROJECTED_AXIS_SEMANTICS_UNAVAILABLE',
    });
  }

  const sourcePoints = Array.isArray(points) ? points : [];
  const transformedPoints = [];
  let maximumRoundTripErrorMeters = 0;
  let roundTripVerifiedPointCount = 0;
  for (const point of sourcePoints) {
    if (!Number.isFinite(point?.x) || !Number.isFinite(point?.y)) continue;
    const geographic = projectedToWgs84(resolution.definition, point.x, point.y);
    if (!geographic || geographic.latitude < -90 || geographic.latitude > 90
      || geographic.longitude < -180 || geographic.longitude > 180) continue;
    const roundTrip = wgs84ToTransverseMercator(geographic.latitude, geographic.longitude, resolution.definition);
    if (!roundTrip) continue;
    const roundTripErrorMeters = Math.hypot(roundTrip.easting - point.x, roundTrip.northing - point.y);
    maximumRoundTripErrorMeters = Math.max(maximumRoundTripErrorMeters, roundTripErrorMeters);
    const roundTripValid = roundTripErrorMeters <= boundedTolerance;
    if (roundTripValid) roundTripVerifiedPointCount += 1;
    transformedPoints.push(Object.freeze({
      latitude: geographic.latitude,
      longitude: geographic.longitude,
      roundTripErrorMeters,
      roundTripValid,
    }));
  }
  const complete = sourcePoints.length > 0 && transformedPoints.length === sourcePoints.length;
  const roundTripComplete = complete && roundTripVerifiedPointCount === sourcePoints.length;
  return Object.freeze({
    valid: roundTripComplete,
    crsStatus: 'identified',
    crsId: resolution.definition.id,
    registryMatched: true,
    axisStatus: 'identified',
    pointCount: sourcePoints.length,
    transformedPointCount: transformedPoints.length,
    roundTripVerifiedPointCount,
    forwardStatus: complete ? 'passed' : 'failed',
    inverseStatus: roundTripComplete ? 'passed' : 'failed',
    spatialStatus: roundTripComplete ? 'passed' : 'failed',
    toleranceMeters: boundedTolerance,
    maximumRoundTripErrorMeters: complete ? maximumRoundTripErrorMeters : null,
    identityDiagnostic: resolution.identityDiagnostic,
    transformedPoints: Object.freeze(transformedPoints),
    failureCode: roundTripComplete ? null : 'PROJECTED_ROUND_TRIP_VERIFICATION_FAILED',
  });
}
