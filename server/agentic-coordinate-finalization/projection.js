import { convertKyrgyzGkToWgs84 } from '../projection/kyrgyz-gk.js';
import { utmToWgs84 } from '../projection/utm.js';

function projectionError(code, message) {
  const error = new Error(message);
  error.code = code;
  return error;
}

function parseEpsg(value) {
  const match = String(value || '').match(/(?:EPSG\s*:\s*)?(\d{4,6})/i);
  return match ? Number(match[1]) : null;
}

function explicitUtmDefinition(coordinateSystem) {
  const epsg = parseEpsg(coordinateSystem?.epsg);
  if (epsg >= 32601 && epsg <= 32660) {
    return { id: `EPSG:${epsg}`, type: 'utm', zone: epsg - 32600, northern: true };
  }
  if (epsg >= 32701 && epsg <= 32760) {
    return { id: `EPSG:${epsg}`, type: 'utm', zone: epsg - 32700, northern: false };
  }

  const name = String(coordinateSystem?.name || '').trim();
  const match = name.match(/(?:\bUTM\b[^\d]{0,24}|^)(\d{1,2})\s*([NS])\b/i);
  if (!match) return null;
  const zone = Number(match[1]);
  if (!Number.isInteger(zone) || zone < 1 || zone > 60) return null;
  const northern = match[2].toUpperCase() === 'N';
  return {
    id: `EPSG:${northern ? 32600 + zone : 32700 + zone}`,
    type: 'utm',
    zone,
    northern,
  };
}

function explicitKyrgyzDefinition(coordinateSystem) {
  const epsg = parseEpsg(coordinateSystem?.epsg);
  if (epsg === 28413) return { id: 'EPSG:28413', type: 'kyrgyz_gk' };
  if (coordinateSystem?.status !== 'identified') return null;
  return /(?:Kyrgyz|Кыргыз|Киргиз).*(?:GK|Gauss|Гаусс)|(?:GK|Gauss|Гаусс).*(?:Kyrgyz|Кыргыз|Киргиз)/iu
    .test(String(coordinateSystem?.name || ''))
    ? { id: 'EPSG:28413', type: 'kyrgyz_gk' }
    : null;
}

export function resolveExplicitAgenticProjection(coordinateSystem) {
  if (coordinateSystem?.kind !== 'projected' || coordinateSystem?.status !== 'identified') {
    return null;
  }
  return explicitUtmDefinition(coordinateSystem)
    || explicitKyrgyzDefinition(coordinateSystem);
}

function transformPoint(point, definition) {
  if (!Number.isFinite(point.x) || !Number.isFinite(point.y)) {
    throw projectionError('PROJECTED_PAIR_REQUIRED', 'Projected transform requires complete X/Y coordinates');
  }

  const converted = definition.type === 'utm'
    ? utmToWgs84(definition.zone, point.x, point.y, definition.northern)
    : convertKyrgyzGkToWgs84(point.x, point.y);

  const latitude = converted?.lat ?? converted?.latitude;
  const longitude = converted?.lon ?? converted?.longitude;
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    throw projectionError('PROJECTED_TRANSFORM_FAILED', `Could not transform ${definition.id} coordinate`);
  }

  return Object.freeze({
    ...point,
    latitude,
    longitude,
  });
}

export function transformAgenticCoordinateResult(result) {
  if (result?.coordinateSystem?.kind === 'geographic') return result;

  const definition = resolveExplicitAgenticProjection(result?.coordinateSystem);
  if (!definition) {
    throw projectionError(
      'PROJECTED_CRS_TRANSFORM_REQUIRED',
      'Projected coordinates require an explicitly identified and supported CRS',
    );
  }

  const groups = result.groups.map(group => Object.freeze({
    ...group,
    points: Object.freeze(group.points.map(point => transformPoint(point, definition))),
  }));

  return Object.freeze({
    ...result,
    groups: Object.freeze(groups),
    transformation: Object.freeze({
      status: 'success',
      sourceCrs: definition.id,
      targetCrs: 'EPSG:4326',
      method: definition.type,
    }),
  });
}

