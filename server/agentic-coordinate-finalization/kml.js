function escapeXml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&apos;');
}

function coordinateText(position) {
  return `${position[0]},${position[1]},0`;
}

function geometryToKml(geometry) {
  switch (geometry.type) {
    case 'Point':
      return `<Point><coordinates>${coordinateText(geometry.coordinates)}</coordinates></Point>`;
    case 'MultiPoint':
      return `<MultiGeometry>${geometry.coordinates.map(position => (
        `<Point><coordinates>${coordinateText(position)}</coordinates></Point>`
      )).join('')}</MultiGeometry>`;
    case 'LineString':
      return `<LineString><coordinates>${geometry.coordinates.map(coordinateText).join(' ')}</coordinates></LineString>`;
    case 'Polygon':
      return `<Polygon><outerBoundaryIs><LinearRing><coordinates>${geometry.coordinates[0].map(coordinateText).join(' ')}</coordinates></LinearRing></outerBoundaryIs></Polygon>`;
    case 'MultiPolygon':
      return `<MultiGeometry>${geometry.coordinates.map(polygon => geometryToKml({
        type: 'Polygon',
        coordinates: polygon,
      })).join('')}</MultiGeometry>`;
    default:
      throw new Error(`Unsupported GeoJSON geometry type: ${geometry.type}`);
  }
}

export function buildAgenticKml({ geometryArtifact, name = 'Coordinate result' }) {
  if (!geometryArtifact?.geometry || !Number.isInteger(geometryArtifact.documentRevision)) {
    throw new Error('geometryArtifact is required');
  }

  return `<?xml version="1.0" encoding="UTF-8"?>\n<kml xmlns="http://www.opengis.net/kml/2.2"><Document><name>${escapeXml(name)}</name><Placemark><name>${escapeXml(name)}</name>${geometryToKml(geometryArtifact.geometry)}</Placemark></Document></kml>`;
}

