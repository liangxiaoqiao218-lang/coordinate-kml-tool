const MAX_CONVERSION_BATCH_SIZE = 40;
// This is intentionally not a country-boundary approximation. Conversion is
// limited to conservative inland zones with ample international-border margin.
// Any coordinate outside these zones remains canonical WGS84 for display.
const MAINLAND_INLAND_CONVERSION_ZONES = Object.freeze([
  Object.freeze({ id: "BEIJING_INLAND", west: 115, south: 38, east: 118.5, north: 41.5 }),
  Object.freeze({ id: "SHANGHAI_INLAND", west: 120, south: 30, east: 122, north: 32.5 }),
  Object.freeze({ id: "GUANGZHOU_INLAND", west: 112, south: 22.2, east: 114.8, north: 24 }),
  Object.freeze({ id: "CHENGDU_INLAND", west: 102.5, south: 29, east: 105.5, north: 32 }),
  Object.freeze({ id: "WUHAN_INLAND", west: 112.5, south: 29, east: 116, north: 32.5 })
]);

function isPosition(value) {
  return Array.isArray(value) && value.length === 2
    && Number.isFinite(value[0]) && Number.isFinite(value[1]);
}

export function isMainlandChinaDisplayConversionRequired(longitude, latitude) {
  if (!Number.isFinite(longitude) || !Number.isFinite(latitude)) return false;
  return MAINLAND_INLAND_CONVERSION_ZONES.some(zone => longitude >= zone.west
    && longitude <= zone.east
    && latitude >= zone.south
    && latitude <= zone.north);
}

export function isMainlandCoordinate([longitude, latitude]) {
  return isMainlandChinaDisplayConversionRequired(longitude, latitude);
}

function coordinatePaths(geometry) {
  const paths = [];
  const visit = (value, path) => {
    if (isPosition(value)) {
      paths.push({ path, position: value });
      return;
    }
    if (!Array.isArray(value)) throw Object.assign(new Error("DISPLAY_GEOMETRY_INVALID"), {
      code: "DISPLAY_GEOMETRY_INVALID"
    });
    value.forEach((child, index) => visit(child, [...path, index]));
  };
  visit(geometry.coordinates, []);
  return paths;
}

function assignAtPath(root, path, value) {
  let cursor = root.coordinates;
  for (let index = 0; index < path.length - 1; index += 1) cursor = cursor[path[index]];
  cursor[path[path.length - 1]] = [...value];
}

export class AMapDisplayCoordinateAdapter {
  constructor({ convertBatch, batchSize = MAX_CONVERSION_BATCH_SIZE } = {}) {
    this.convertBatch = convertBatch;
    this.batchSize = Math.min(MAX_CONVERSION_BATCH_SIZE, Math.max(1, Number(batchSize) || MAX_CONVERSION_BATCH_SIZE));
  }

  async convert(canonicalGeometry) {
    const displayGeometry = structuredClone(canonicalGeometry);
    const positions = coordinatePaths(canonicalGeometry);
    const mainland = positions.filter(entry => isMainlandCoordinate(entry.position));
    if (mainland.length === 0) {
      return Object.freeze({ geometry: displayGeometry, status: "OVERSEAS_WGS84_PASSTHROUGH", batchCount: 0 });
    }
    if (typeof this.convertBatch !== "function") {
      throw Object.assign(new Error("AMAP_DISPLAY_CONVERTER_UNAVAILABLE"), {
        code: "AMAP_DISPLAY_CONVERTER_UNAVAILABLE"
      });
    }

    const converted = [];
    for (let offset = 0; offset < mainland.length; offset += this.batchSize) {
      const batch = mainland.slice(offset, offset + this.batchSize).map(entry => [...entry.position]);
      const result = await this.convertBatch(batch);
      if (!Array.isArray(result) || result.length !== batch.length || !result.every(isPosition)) {
        throw Object.assign(new Error("AMAP_DISPLAY_CONVERSION_FAILED"), {
          code: "AMAP_DISPLAY_CONVERSION_FAILED"
        });
      }
      converted.push(...result.map(position => [...position]));
    }
    mainland.forEach((entry, index) => assignAtPath(displayGeometry, entry.path, converted[index]));
    return Object.freeze({
      geometry: displayGeometry,
      status: mainland.length === positions.length
        ? "MAINLAND_WGS84_TO_PROVIDER_DISPLAY"
        : "MIXED_MAINLAND_CONVERTED_OVERSEAS_PASSTHROUGH",
      batchCount: Math.ceil(mainland.length / this.batchSize)
    });
  }
}

export const AMAP_CONVERSION_BATCH_LIMIT = MAX_CONVERSION_BATCH_SIZE;
