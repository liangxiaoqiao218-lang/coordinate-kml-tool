import {
  AGENTIC_COORDINATE_KIND,
  AGENTIC_CRS_STATUS,
  AGENTIC_GEOMETRY_TYPE,
  AGENTIC_RESULT_STATUS,
} from './contract.js';

function consistencyError(issues) {
  const error = new Error(`agentic coordinate result is inconsistent: ${issues.join('; ')}`);
  error.code = 'AGENTIC_RESULT_INCONSISTENT';
  error.issues = Object.freeze([...issues]);
  return error;
}

function allPoints(result) {
  return result.groups.flatMap(group => group.points);
}

export function assertAgenticCoordinateConsistency(result) {
  if (!result?.success) return result;

  const issues = [];
  const points = allPoints(result);
  const groupCount = result.groups.length;
  const pointCount = points.length;
  const groupSizes = result.groups.map(group => group.points.length);
  const groupNames = result.groups.map(group => String(group.name || '').trim());

  if (groupCount > 1) {
    if (groupNames.some(name => !name)) {
      issues.push('multiple groups require an explicit visible name for every group');
    }
    const normalizedGroupNames = groupNames.map(name => name.toLocaleLowerCase());
    if (new Set(normalizedGroupNames).size !== normalizedGroupNames.length) {
      issues.push('multiple groups require distinct visible group names');
    }
  }

  switch (result.geometryType) {
    case AGENTIC_GEOMETRY_TYPE.POINT:
      if (groupCount !== 1 || pointCount !== 1) {
        issues.push('Point requires exactly one group with one point');
      }
      break;
    case AGENTIC_GEOMETRY_TYPE.MULTI_POINT:
      if (pointCount < 2) issues.push('MultiPoint requires at least two points');
      break;
    case AGENTIC_GEOMETRY_TYPE.LINE_STRING:
      if (groupCount !== 1 || pointCount < 2) {
        issues.push('LineString requires exactly one group with at least two points');
      }
      break;
    case AGENTIC_GEOMETRY_TYPE.POLYGON:
      if (groupCount !== 1 || pointCount < 3) {
        issues.push('Polygon requires exactly one group with at least three points');
      }
      break;
    case AGENTIC_GEOMETRY_TYPE.MULTI_POLYGON:
      if (groupCount < 2 || groupSizes.some(size => size < 3)) {
        issues.push('MultiPolygon requires at least two groups with at least three points each');
      }
      break;
    case AGENTIC_GEOMETRY_TYPE.GRID:
      break;
    case AGENTIC_GEOMETRY_TYPE.UNKNOWN:
      if (result.resultStatus === AGENTIC_RESULT_STATUS.USABLE) {
        issues.push('Unknown geometry cannot be marked usable');
      }
      break;
    default:
      issues.push(`unsupported geometry type ${result.geometryType}`);
  }

  if (result.coordinateSystem.kind === AGENTIC_COORDINATE_KIND.GEOGRAPHIC
    && points.some(point => point.latitude === null || point.longitude === null)) {
    issues.push('geographic result requires latitude and longitude on every point');
  }
  if (result.coordinateSystem.kind === AGENTIC_COORDINATE_KIND.PROJECTED
    && points.some(point => point.x === null || point.y === null)) {
    issues.push('projected result requires X and Y on every point');
  }
  if (result.coordinateSystem.status === AGENTIC_CRS_STATUS.IDENTIFIED
    && !result.coordinateSystem.name
    && !result.coordinateSystem.epsg) {
    issues.push('identified coordinate system requires a name or EPSG');
  }
  if (result.resultStatus === AGENTIC_RESULT_STATUS.USABLE
    && result.coordinateSystem.status !== AGENTIC_CRS_STATUS.IDENTIFIED) {
    issues.push('usable result requires an identified coordinate system');
  }

  if (issues.length > 0) throw consistencyError(issues);
  return result;
}
