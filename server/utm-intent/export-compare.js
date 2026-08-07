const MIN_TOLERANCE = 1e-8;
const MAX_TOLERANCE = 1e-6;
const SUPPORTED_GEOMETRIES = new Set(["Point", "LineString", "Polygon"]);

function escapeXml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function decodeXml(value) {
  return String(value)
    .replace(/&apos;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/&gt;/g, ">")
    .replace(/&lt;/g, "<")
    .replace(/&amp;/g, "&");
}

function normalizeTolerance(value) {
  const tolerance = Number(value);
  if (!Number.isFinite(tolerance) || tolerance < MIN_TOLERANCE || tolerance > MAX_TOLERANCE) {
    throw new RangeError(`tolerance must be between ${MIN_TOLERANCE} and ${MAX_TOLERANCE}`);
  }
  return tolerance;
}

function normalizeGeometry(value) {
  const geometry = String(value || "Polygon");
  if (!SUPPORTED_GEOMETRIES.has(geometry)) {
    throw new RangeError('geometry must be "Point", "LineString", or "Polygon"');
  }
  return geometry;
}

function expectedWgs84UtmEpsg(zone, hemisphere) {
  const base = hemisphere === "north" ? 32600 : hemisphere === "south" ? 32700 : null;
  return base && Number.isInteger(zone) && zone >= 1 && zone <= 60 ? `EPSG:${base + zone}` : null;
}

function normalizeTypedPoints(typedResult) {
  const intent = typedResult?.typedUtmIntent;
  if (!intent || intent.coordinateType !== "utm_projected_xy" || intent.projection !== "utm") {
    throw new TypeError("canonical export requires a typed UTM result");
  }
  const expectedEpsg = expectedWgs84UtmEpsg(intent.zone, intent.hemisphere);
  if (intent.datum !== "WGS84" || !expectedEpsg || intent.epsg !== expectedEpsg) {
    throw new TypeError("canonical export requires an explicit WGS84 UTM CRS");
  }

  const points = Array.isArray(typedResult.transformedWgs84) ? typedResult.transformedWgs84 : [];
  if (points.length === 0) throw new TypeError("canonical export requires transformed WGS84 points");
  if (!Array.isArray(typedResult.projectedCoordinates)
    || typedResult.projectedCoordinates.length !== points.length) {
    throw new TypeError("canonical export requires matching projected and transformed point counts");
  }
  return points.map((point, index) => {
    const longitude = Number(point?.longitude ?? point?.lon);
    const latitude = Number(point?.latitude ?? point?.lat);
    if (!Number.isFinite(longitude) || !Number.isFinite(latitude)
      || longitude < -180 || longitude > 180 || latitude < -90 || latitude > 90) {
      throw new TypeError(`invalid transformed WGS84 point at index ${index}`);
    }
    return { longitude, latitude, altitude: 0 };
  });
}

function coordinatesText(points) {
  return points.map(point => `${point.longitude},${point.latitude},${point.altitude}`).join(" ");
}

function closePolygon(points) {
  const first = points[0];
  const last = points[points.length - 1];
  if (first.longitude === last.longitude && first.latitude === last.latitude && first.altitude === last.altitude) {
    return points;
  }
  return [...points, { ...first }];
}

function buildPlacemark({ geometry, points, placemarkName }) {
  if (geometry === "Point") {
    return points.map((point, index) => `    <Placemark>
      <name>${escapeXml(points.length === 1 ? placemarkName : `${placemarkName} ${index + 1}`)}</name>
      <Point><coordinates>${coordinatesText([point])}</coordinates></Point>
    </Placemark>`).join("\n");
  }
  if (geometry === "LineString") {
    return `    <Placemark>
      <name>${escapeXml(placemarkName)}</name>
      <LineString><coordinates>${coordinatesText(points)}</coordinates></LineString>
    </Placemark>`;
  }
  return `    <Placemark>
      <name>${escapeXml(placemarkName)}</name>
      <Polygon><outerBoundaryIs><LinearRing><coordinates>${coordinatesText(closePolygon(points))}</coordinates></LinearRing></outerBoundaryIs></Polygon>
    </Placemark>`;
}

export function buildCanonicalTypedUtmKml({
  typedResult,
  geometry = "Polygon",
  documentName = "UTM Migration Comparison",
  placemarkName = "UTM Boundary"
} = {}) {
  const normalizedGeometry = normalizeGeometry(geometry);
  const points = normalizeTypedPoints(typedResult);
  if (normalizedGeometry === "LineString" && points.length < 2) {
    throw new TypeError("LineString export requires at least two points");
  }
  if (normalizedGeometry === "Polygon" && points.length < 3) {
    throw new TypeError("Polygon export requires at least three points");
  }
  const placemarks = buildPlacemark({ geometry: normalizedGeometry, points, placemarkName });
  return `<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <name>${escapeXml(documentName)}</name>
${placemarks}
  </Document>
</kml>`;
}

function extractName(text) {
  const match = String(text || "").match(/<name>([\s\S]*?)<\/name>/i);
  return match ? decodeXml(match[1].trim()) : null;
}

function parseCoordinateText(text) {
  return String(text || "").trim().split(/\s+/).filter(Boolean).map((token, index) => {
    const [longitudeText, latitudeText, altitudeText = "0"] = token.split(",");
    const longitude = Number(longitudeText);
    const latitude = Number(latitudeText);
    const altitude = Number(altitudeText);
    if (!Number.isFinite(longitude) || !Number.isFinite(latitude) || !Number.isFinite(altitude)) {
      throw new TypeError(`invalid KML coordinate at index ${index}`);
    }
    return { longitude, latitude, altitude };
  });
}

export function parseUtmMigrationKml(kml) {
  const source = String(kml || "");
  if (!source.includes("<kml") || !source.includes("</kml>")) {
    throw new TypeError("KML document is missing a kml root element");
  }
  const documentMatch = source.match(/<Document\b[^>]*>([\s\S]*?)<\/Document>/i);
  if (!documentMatch) throw new TypeError("KML document is missing a Document element");

  const geometries = [];
  const placemarkPattern = /<Placemark\b[^>]*>([\s\S]*?)<\/Placemark>/gi;
  let placemarkMatch;
  while ((placemarkMatch = placemarkPattern.exec(documentMatch[1])) !== null) {
    const body = placemarkMatch[1];
    const geometryMatch = body.match(/<(Point|LineString|Polygon)\b[\s\S]*?<coordinates>([\s\S]*?)<\/coordinates>[\s\S]*?<\/\1>/i);
    if (!geometryMatch) throw new TypeError("Placemark is missing a supported geometry and coordinates");
    geometries.push({
      name: extractName(body),
      type: geometryMatch[1],
      coordinates: parseCoordinateText(geometryMatch[2])
    });
  }
  if (geometries.length === 0) throw new TypeError("KML document contains no comparable Placemarks");
  return {
    documentName: extractName(documentMatch[1]),
    geometries
  };
}

function failed(reason, details = {}) {
  return { status: "EXPORT_COMPARE_FAILED", reason, ...details };
}

export function compareLegacyAndCanonicalUtmKml({
  legacyKml,
  typedResult,
  geometry = "Polygon",
  documentName = "UTM Migration Comparison",
  placemarkName = "UTM Boundary",
  tolerance = MIN_TOLERANCE
} = {}) {
  const numericTolerance = normalizeTolerance(tolerance);
  let canonicalKml;
  let legacy;
  let canonical;
  try {
    canonicalKml = buildCanonicalTypedUtmKml({ typedResult, geometry, documentName, placemarkName });
    legacy = parseUtmMigrationKml(legacyKml);
    canonical = parseUtmMigrationKml(canonicalKml);
  } catch (error) {
    return failed("INVALID_EXPORT_INPUT", { tolerance: numericTolerance, message: error.message });
  }

  if (legacy.documentName !== canonical.documentName) {
    return failed("DOCUMENT_NAME_MISMATCH", {
      tolerance: numericTolerance,
      legacyDocumentName: legacy.documentName,
      canonicalDocumentName: canonical.documentName
    });
  }
  if (legacy.geometries.length !== canonical.geometries.length) {
    return failed("PLACEMARK_COUNT_MISMATCH", {
      tolerance: numericTolerance,
      legacyPlacemarkCount: legacy.geometries.length,
      canonicalPlacemarkCount: canonical.geometries.length
    });
  }

  const pointComparisons = [];
  for (let groupIndex = 0; groupIndex < legacy.geometries.length; groupIndex += 1) {
    const legacyGeometry = legacy.geometries[groupIndex];
    const canonicalGeometry = canonical.geometries[groupIndex];
    if (legacyGeometry.name !== canonicalGeometry.name) {
      return failed("PLACEMARK_NAME_MISMATCH", { tolerance: numericTolerance, groupIndex });
    }
    if (legacyGeometry.type !== canonicalGeometry.type) {
      return failed("GEOMETRY_MISMATCH", {
        tolerance: numericTolerance,
        groupIndex,
        legacyGeometry: legacyGeometry.type,
        canonicalGeometry: canonicalGeometry.type
      });
    }
    if (legacyGeometry.coordinates.length !== canonicalGeometry.coordinates.length) {
      return failed("POINT_COUNT_MISMATCH", {
        tolerance: numericTolerance,
        groupIndex,
        legacyPointCount: legacyGeometry.coordinates.length,
        canonicalPointCount: canonicalGeometry.coordinates.length
      });
    }
    legacyGeometry.coordinates.forEach((legacyPoint, pointIndex) => {
      const canonicalPoint = canonicalGeometry.coordinates[pointIndex];
      const longitudeDifference = Math.abs(legacyPoint.longitude - canonicalPoint.longitude);
      const latitudeDifference = Math.abs(legacyPoint.latitude - canonicalPoint.latitude);
      const altitudeDifference = Math.abs(legacyPoint.altitude - canonicalPoint.altitude);
      pointComparisons.push({
        groupIndex,
        pointIndex,
        longitudeDifference,
        latitudeDifference,
        altitudeDifference,
        maximumDifference: Math.max(longitudeDifference, latitudeDifference, altitudeDifference)
      });
    });
  }

  const maximumDifference = pointComparisons.reduce((maximum, point) => Math.max(maximum, point.maximumDifference), 0);
  if (!Number.isFinite(maximumDifference) || maximumDifference > numericTolerance) {
    return failed("COORDINATE_MISMATCH", {
      tolerance: numericTolerance,
      maximumDifference,
      pointComparisons
    });
  }

  return {
    status: "MATCH",
    tolerance: numericTolerance,
    maximumDifference,
    placemarkCount: canonical.geometries.length,
    pointCount: pointComparisons.length,
    geometryTypes: canonical.geometries.map(item => item.type),
    canonicalKml
  };
}
