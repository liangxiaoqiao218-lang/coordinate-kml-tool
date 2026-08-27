const EARTH_RADIUS_METERS = 6371008.8;

function radians(value) {
  return value * Math.PI / 180;
}

function haversine(left, right) {
  const dLat = radians(right[1] - left[1]);
  const dLon = radians(right[0] - left[0]);
  const a = Math.sin(dLat / 2) ** 2
    + Math.cos(radians(left[1])) * Math.cos(radians(right[1])) * Math.sin(dLon / 2) ** 2;
  return EARTH_RADIUS_METERS * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function flattenPositions(geometry) {
  if (geometry.type === "Point") return [geometry.coordinates];
  if (geometry.type === "LineString") return geometry.coordinates;
  if (geometry.type === "Polygon") return geometry.coordinates.flat();
  if (geometry.type === "MultiPolygon") return geometry.coordinates.flat(2);
  return [];
}

function bbox(positions) {
  const longitudes = positions.map(position => position[0]);
  const latitudes = positions.map(position => position[1]);
  return [Math.min(...longitudes), Math.min(...latitudes), Math.max(...longitudes), Math.max(...latitudes)];
}

function averagePosition(positions) {
  const sum = positions.reduce((result, position) => [result[0] + position[0], result[1] + position[1]], [0, 0]);
  return [sum[0] / positions.length, sum[1] / positions.length];
}

function ringCentroid(ring) {
  let crossSum = 0;
  let longitudeSum = 0;
  let latitudeSum = 0;
  for (let index = 0; index < ring.length - 1; index += 1) {
    const left = ring[index];
    const right = ring[index + 1];
    const cross = left[0] * right[1] - right[0] * left[1];
    crossSum += cross;
    longitudeSum += (left[0] + right[0]) * cross;
    latitudeSum += (left[1] + right[1]) * cross;
  }
  if (Math.abs(crossSum) < Number.EPSILON) {
    return { centroid: averagePosition(ring), weight: 0 };
  }
  return {
    centroid: [longitudeSum / (3 * crossSum), latitudeSum / (3 * crossSum)],
    weight: Math.abs(crossSum / 2)
  };
}

function polygonCentroid(polygon) {
  const [outer = [], ...holes] = polygon;
  const parts = [
    { ...ringCentroid(outer), direction: 1 },
    ...holes.map(ring => ({ ...ringCentroid(ring), direction: -1 }))
  ];
  const weight = parts.reduce((sum, part) => sum + part.weight * part.direction, 0);
  if (weight <= Number.EPSILON) return { centroid: averagePosition(outer), weight: 0 };
  return {
    centroid: [
      parts.reduce((sum, part) => sum + part.centroid[0] * part.weight * part.direction, 0) / weight,
      parts.reduce((sum, part) => sum + part.centroid[1] * part.weight * part.direction, 0) / weight
    ],
    weight
  };
}

function lineCentroid(line) {
  let totalLength = 0;
  let longitude = 0;
  let latitude = 0;
  for (let index = 1; index < line.length; index += 1) {
    const length = haversine(line[index - 1], line[index]);
    totalLength += length;
    longitude += ((line[index - 1][0] + line[index][0]) / 2) * length;
    latitude += ((line[index - 1][1] + line[index][1]) / 2) * length;
  }
  return totalLength > 0 ? [longitude / totalLength, latitude / totalLength] : averagePosition(line);
}

function geometryCentroid(geometry, positions) {
  if (geometry.type === "Point") return geometry.coordinates;
  if (geometry.type === "LineString") return lineCentroid(geometry.coordinates);
  if (geometry.type === "Polygon") return polygonCentroid(geometry.coordinates).centroid;
  if (geometry.type === "MultiPolygon") {
    const values = geometry.coordinates.map(polygonCentroid);
    const weight = values.reduce((sum, value) => sum + value.weight, 0);
    if (weight > Number.EPSILON) {
      return [
        values.reduce((sum, value) => sum + value.centroid[0] * value.weight, 0) / weight,
        values.reduce((sum, value) => sum + value.centroid[1] * value.weight, 0) / weight
      ];
    }
  }
  return averagePosition(positions);
}

function lineLength(line) {
  let length = 0;
  for (let index = 1; index < line.length; index += 1) length += haversine(line[index - 1], line[index]);
  return length;
}

function ringArea(ring) {
  const center = averagePosition(ring);
  const projected = ring.map(([longitude, latitude]) => [
    radians(longitude - center[0]) * EARTH_RADIUS_METERS * Math.cos(radians(center[1])),
    radians(latitude - center[1]) * EARTH_RADIUS_METERS
  ]);
  let twiceArea = 0;
  for (let index = 0; index < projected.length - 1; index += 1) {
    twiceArea += projected[index][0] * projected[index + 1][1]
      - projected[index + 1][0] * projected[index][1];
  }
  return Math.abs(twiceArea) / 2;
}

function polygonFacts(polygon) {
  const [outer = [], ...holes] = polygon;
  return {
    area: Math.max(0, ringArea(outer) - holes.reduce((sum, ring) => sum + ringArea(ring), 0)),
    perimeter: [outer, ...holes].reduce((sum, ring) => sum + lineLength(ring), 0)
  };
}

export function calculateSpatialFacts(geometry) {
  if (!geometry || typeof geometry !== "object") throw new TypeError("SPATIAL_FACTS_GEOMETRY_REQUIRED");
  const positions = flattenPositions(geometry);
  if (positions.length === 0) throw new TypeError("SPATIAL_FACTS_POSITIONS_REQUIRED");

  let areaMeters2 = null;
  let perimeterMeters = null;
  let lengthMeters = null;
  if (geometry.type === "LineString") lengthMeters = lineLength(geometry.coordinates);
  if (geometry.type === "Polygon") {
    const values = polygonFacts(geometry.coordinates);
    areaMeters2 = values.area;
    perimeterMeters = values.perimeter;
  }
  if (geometry.type === "MultiPolygon") {
    const values = geometry.coordinates.map(polygonFacts);
    areaMeters2 = values.reduce((sum, value) => sum + value.area, 0);
    perimeterMeters = values.reduce((sum, value) => sum + value.perimeter, 0);
  }

  const pointCount = geometry.type === "Point"
    ? 1
    : new Set(positions.map(position => `${position[0]}:${position[1]}`)).size;

  return Object.freeze({
    schemaVersion: "spatial_facts_v1",
    geometryType: geometry.type,
    areaMeters2,
    perimeterMeters,
    lengthMeters,
    centroid: Object.freeze(geometryCentroid(geometry, positions)),
    bbox: Object.freeze(bbox(positions)),
    pointCount,
    availability: Object.freeze({
      area: areaMeters2 === null ? "unavailable" : "available",
      perimeter: perimeterMeters === null ? "unavailable" : "available",
      length: lengthMeters === null ? "unavailable" : "available"
    })
  });
}
