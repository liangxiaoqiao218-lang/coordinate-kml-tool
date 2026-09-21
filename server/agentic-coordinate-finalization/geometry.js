import { createHash } from 'node:crypto';

function geometryError(code, message) {
  const error = new Error(message);
  error.code = code;
  return error;
}

function geographicPositions(result) {
  if (result.coordinateSystem?.kind !== 'geographic'
    && result.transformation?.status !== 'success') {
    throw geometryError(
      'PROJECTED_CRS_TRANSFORM_REQUIRED',
      'Map and KML require an identified projected-CRS transform before projected coordinates can be used',
    );
  }

  return result.groups.map((group, groupIndex) => group.points.map((point, pointIndex) => {
    if (!Number.isFinite(point.longitude) || !Number.isFinite(point.latitude)) {
      throw geometryError(
        'GEOGRAPHIC_COORDINATE_REQUIRED',
        `groups[${groupIndex}].points[${pointIndex}] has no geographic coordinate`,
      );
    }
    return [point.longitude, point.latitude];
  }));
}

function closeRing(positions) {
  if (positions.length < 3) {
    throw geometryError('POLYGON_REQUIRES_THREE_POINTS', 'A polygon requires at least three coordinate points');
  }
  const first = positions[0];
  const last = positions[positions.length - 1];
  return first[0] === last[0] && first[1] === last[1]
    ? positions
    : [...positions, [...first]];
}

function flattenGroups(groups) {
  return groups.flatMap(group => group);
}

export function buildAgenticGeoJsonGeometry(result) {
  if (result?.success && result.resultStatus === 'needs_review') {
    throw geometryError(
      'AGENTIC_REVIEW_REQUIRED',
      'Coordinate review is required before map and KML generation',
    );
  }
  if (!result?.success || result.resultStatus !== 'usable') {
    throw geometryError('UNUSABLE_COORDINATE_RESULT', 'Recognition result is not usable for geometry');
  }

  const groups = geographicPositions(result);
  const all = flattenGroups(groups);

  switch (result.geometryType) {
    case 'Point':
      if (all.length !== 1) {
        throw geometryError('POINT_COUNT_MISMATCH', 'Point geometry must contain exactly one coordinate');
      }
      return Object.freeze({ type: 'Point', coordinates: all[0] });
    case 'MultiPoint':
    case 'Grid':
      return Object.freeze({ type: 'MultiPoint', coordinates: all });
    case 'LineString':
      if (groups.length !== 1 || all.length < 2) {
        throw geometryError('LINESTRING_GROUP_MISMATCH', 'LineString requires one group with at least two points');
      }
      return Object.freeze({ type: 'LineString', coordinates: all });
    case 'Polygon':
      if (groups.length !== 1) {
        throw geometryError('POLYGON_GROUP_MISMATCH', 'Polygon requires exactly one explicit group');
      }
      return Object.freeze({ type: 'Polygon', coordinates: [closeRing(groups[0])] });
    case 'MultiPolygon':
      return Object.freeze({
        type: 'MultiPolygon',
        coordinates: groups.map(group => [closeRing(group)]),
      });
    default:
      throw geometryError('UNSUPPORTED_GEOMETRY_TYPE', `Unsupported geometry type: ${result.geometryType}`);
  }
}

export function createAgenticGeometryArtifact({ documentRevision, result }) {
  if (!Number.isInteger(documentRevision) || documentRevision < 1) {
    throw new Error('documentRevision must be a positive integer');
  }

  const geometry = buildAgenticGeoJsonGeometry(result);
  const canonical = JSON.stringify(geometry);
  return Object.freeze({
    documentRevision,
    geometry,
    geometryHash: createHash('sha256').update(canonical).digest('hex'),
    reviewRequired: false,
  });
}
