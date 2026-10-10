export class IndonesiaKmlAcceptanceError extends Error {
  constructor(code) {
    super(code);
    this.name = "IndonesiaKmlAcceptanceError";
    this.code = code;
  }
}

function reject(code) {
  throw new IndonesiaKmlAcceptanceError(code);
}

export function revisionIsCurrent(expectedRevision, currentRevision) {
  return Number.isSafeInteger(expectedRevision)
    && Number.isSafeInteger(currentRevision)
    && expectedRevision === currentRevision;
}

export function validateIndonesiaFeatureCollection(featureCollection) {
  if (featureCollection?.type !== "FeatureCollection" || !Array.isArray(featureCollection.features)) {
    reject("FEATURE_COLLECTION_REQUIRED");
  }
  const unsupported = featureCollection.features.filter(feature => !["Point", "Polygon"].includes(feature.geometry?.type));
  if (unsupported.length) reject("UNSUPPORTED_EXTRA_GEOMETRY_PRESENT");
  const points = featureCollection.features.filter(feature => feature.geometry?.type === "Point");
  const polygons = featureCollection.features.filter(feature => feature.geometry?.type === "Polygon");
  if (polygons.length !== 1) reject("EXPECTED_ONE_POLYGON");
  const polygon = polygons[0].geometry;
  if (polygon.coordinates.length !== 1 || polygon.coordinates[0].length !== 17) {
    reject("EXPECTED_CLOSED_16_VERTEX_OUTER_RING");
  }
  const first = polygon.coordinates[0][0];
  const last = polygon.coordinates[0].at(-1);
  if (first[0] !== last[0] || first[1] !== last[1]) reject("EXPECTED_CLOSED_RING");
  const distinct = new Set(polygon.coordinates[0].slice(0, -1).map(position => `${position[0]}:${position[1]}`));
  if (distinct.size !== 16) reject("EXPECTED_16_DISTINCT_OUTER_VERTICES");
  if (![0, 16].includes(points.length)) reject("POINT_PLACEMARK_COUNT_MUST_BE_ZERO_OR_16");

  let pointEvidence = "RING_VERTEX_ORDER_ONLY_NO_SOURCE_POINT_IDS";
  let displayLabels = Array.from({ length: 16 }, (_, index) => `环序 ${index + 1}`);
  if (points.length === 16) {
    const expectedLabels = Array.from({ length: 16 }, (_, index) => String(index + 1));
    const actualLabels = points.map(feature => String(feature.properties?.name || ""));
    if (JSON.stringify(actualLabels) !== JSON.stringify(expectedLabels)) reject("POINT_LABEL_ORDER_1_TO_16_REQUIRED");
    points.forEach((feature, index) => {
      const point = feature.geometry.coordinates;
      const ringPoint = polygon.coordinates[0][index];
      if (point[0] !== ringPoint[0] || point[1] !== ringPoint[1]) reject("POINT_PLACEMARK_RING_ORDER_MISMATCH");
    });
    pointEvidence = "POINT_PLACEMARKS_1_TO_16_MATCH_RING_ORDER_REVIEW_PENDING";
    displayLabels = actualLabels;
  }
  return { polygon, pointPlacemarkCount: points.length, pointEvidence, displayLabels };
}

function haversineMeters(left, right) {
  const radians = value => value * Math.PI / 180;
  const dLat = radians(right[1] - left[1]);
  const dLon = radians(right[0] - left[0]);
  const lat1 = radians(left[1]);
  const lat2 = radians(right[1]);
  const value = Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
  return 6371008.8 * 2 * Math.atan2(Math.sqrt(value), Math.sqrt(1 - value));
}

export function maximumRingDifferenceMeters(leftRing, rightRing) {
  if (!Array.isArray(leftRing) || !Array.isArray(rightRing) || leftRing.length !== rightRing.length) {
    reject("COMPARABLE_RING_PAIR_REQUIRED");
  }
  return Math.max(...leftRing.map((position, index) => haversineMeters(position, rightRing[index])));
}
