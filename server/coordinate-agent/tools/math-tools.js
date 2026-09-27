import { GENERIC_TOOL_NAMES } from '../constants.js';

function checkCoordinatePair(args) {
  const latitude = Number(args.latitude);
  const longitude = Number(args.longitude);
  const valid = Number.isFinite(latitude)
    && Number.isFinite(longitude)
    && latitude >= -90
    && latitude <= 90
    && longitude >= -180
    && longitude <= 180;
  return Object.freeze({ valid, latitude, longitude });
}

function spatialConsistency(args) {
  const points = Array.isArray(args.points) ? args.points : [];
  const invalidIndexes = [];
  points.forEach((point, index) => {
    if (!checkCoordinatePair(point).valid) invalidIndexes.push(index);
  });
  return Object.freeze({
    valid: invalidIndexes.length === 0,
    pointCount: points.length,
    invalidIndexes: Object.freeze(invalidIndexes),
  });
}

export function registerCoordinateMathTools(registry) {
  registry.register({
    name: GENERIC_TOOL_NAMES.COORDINATE_MATH_CHECK,
    description: 'Validate an explicit geographic pair without inferring missing values.',
    inputSchema: { type: 'object', additionalProperties: false, required: ['latitude', 'longitude'], properties: { latitude: { type: 'number' }, longitude: { type: 'number' } } },
    execute: checkCoordinatePair,
  });
  registry.register({
    name: GENERIC_TOOL_NAMES.SPATIAL_CONSISTENCY_CHECK,
    description: 'Check explicit geographic pairs for mathematical validity without changing them.',
    inputSchema: { type: 'object', additionalProperties: false, required: ['points'], properties: { points: { type: 'array' } } },
    execute: spatialConsistency,
  });
  return registry;
}
