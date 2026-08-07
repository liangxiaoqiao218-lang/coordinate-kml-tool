const LEGACY_UTM_ALIASES = Object.freeze({
  "utm30n-projected-x-y": Object.freeze({
    projection: "utm",
    datum: "WGS84",
    zone: 30,
    hemisphere: "north",
    epsg: "EPSG:32630"
  })
});

const MIN_TOLERANCE = 1e-8;
const MAX_TOLERANCE = 1e-6;

function normalizeLegacyPoint(point, index) {
  return {
    index,
    longitude: Number(point?.longitude ?? point?.lon),
    latitude: Number(point?.latitude ?? point?.lat)
  };
}

function failure(reason, details = {}) {
  return { status: "migration_compare_failed", reason, ...details };
}

export function compareLegacyAndTypedUtm({ legacyResult, typedResult, tolerance = MIN_TOLERANCE } = {}) {
  const numericTolerance = Number(tolerance);
  if (!Number.isFinite(numericTolerance) || numericTolerance < MIN_TOLERANCE || numericTolerance > MAX_TOLERANCE) {
    throw new RangeError(`tolerance must be between ${MIN_TOLERANCE} and ${MAX_TOLERANCE}`);
  }

  const legacyCrs = LEGACY_UTM_ALIASES[legacyResult?.precisionMode];
  if (!legacyCrs) {
    return { status: "not_comparable", reason: "legacy_utm_alias_unavailable", tolerance: numericTolerance };
  }
  if (!typedResult?.typedUtmIntent) return failure("typed_utm_result_missing", { tolerance: numericTolerance });

  const typedCrs = typedResult.typedUtmIntent;
  const crsFields = ["projection", "datum", "zone", "hemisphere", "epsg"];
  const crsDifferences = crsFields
    .filter(field => legacyCrs[field] !== typedCrs[field])
    .map(field => ({ field, legacy: legacyCrs[field], typed: typedCrs[field] }));
  if (crsDifferences.length > 0) return failure("crs_mismatch", { tolerance: numericTolerance, crsDifferences });

  const legacyPoints = (Array.isArray(legacyResult.transformedWgs84) ? legacyResult.transformedWgs84 : [])
    .map(normalizeLegacyPoint);
  const typedPoints = Array.isArray(typedResult.transformedWgs84) ? typedResult.transformedWgs84 : [];
  if (legacyPoints.length !== typedPoints.length) {
    return failure("point_count_mismatch", {
      tolerance: numericTolerance,
      legacyPointCount: legacyPoints.length,
      typedPointCount: typedPoints.length
    });
  }

  const pointComparisons = legacyPoints.map((legacyPoint, index) => {
    const typedPoint = typedPoints[index] || {};
    const longitudeDifference = Math.abs(legacyPoint.longitude - Number(typedPoint.longitude));
    const latitudeDifference = Math.abs(legacyPoint.latitude - Number(typedPoint.latitude));
    return {
      index,
      longitudeDifference,
      latitudeDifference,
      maximumDifference: Math.max(longitudeDifference, latitudeDifference)
    };
  });
  const maximumDifference = pointComparisons.reduce((maximum, point) => Math.max(maximum, point.maximumDifference), 0);
  if (!Number.isFinite(maximumDifference) || maximumDifference > numericTolerance) {
    return failure("transformation_mismatch", { tolerance: numericTolerance, maximumDifference, pointComparisons });
  }

  return {
    status: "match",
    tolerance: numericTolerance,
    maximumDifference,
    pointCount: typedPoints.length,
    crsDifferences: [],
    pointComparisons
  };
}
