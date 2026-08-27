const DECIMAL_COORDINATE_LINE = /^\s*([-+]?\d+(?:\.\d+)?)\s*[,，]\s*([-+]?\d+(?:\.\d+)?)\s*$/;

export function parseManualLongitudeLatitudeText(text) {
  const lines = String(text || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean);
  if (lines.length === 0) return null;

  const points = [];
  for (const line of lines) {
    const match = line.match(DECIMAL_COORDINATE_LINE);
    if (!match) return null;

    const longitude = Number(match[1]);
    const latitude = Number(match[2]);
    if (
      !Number.isFinite(longitude)
      || !Number.isFinite(latitude)
      || Math.abs(longitude) > 180
      || Math.abs(latitude) > 90
    ) {
      return null;
    }

    points.push({
      label: String(points.length + 1),
      lon: longitude,
      lat: latitude,
      raw: line,
      kmlCoordinate: `${longitude},${latitude},0`
    });
  }

  return points;
}
