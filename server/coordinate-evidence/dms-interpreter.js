export const DMS_COORDINATE_INTERPRETATION_SCHEMA_VERSION =
  "dms_coordinate_interpretation_v1";

export const DMS_INTERPRETATION_STATUS = Object.freeze({
  COMPLETE: "COMPLETE",
  INCOMPLETE: "INCOMPLETE",
  INVALID: "INVALID"
});

const ROLE = Object.freeze({
  LATITUDE: "latitude",
  LONGITUDE: "longitude"
});

const HEMISPHERE = Object.freeze({
  NORTH: "N",
  SOUTH: "S",
  EAST: "E",
  WEST: "W"
});

function cleanString(value = "") {
  return String(value ?? "").trim();
}

function numberOrNull(value) {
  const numberValue = Number(value);
  return Number.isFinite(numberValue) ? numberValue : null;
}

function roundCoordinate(value) {
  const numberValue = Number(value);
  return Number.isFinite(numberValue)
    ? Number(numberValue.toFixed(9))
    : null;
}

export function normalizeDmsHemisphere(value = "", role = "") {
  const text = cleanString(value).toLowerCase();
  if (!text) return null;

  if (/^(n|north|nord|norte|北|北纬)$/.test(text)) return HEMISPHERE.NORTH;
  if (/^(s|south|sud|sur|南|南纬)$/.test(text)) return HEMISPHERE.SOUTH;
  if (/^(e|east|est|este|东|东经)$/.test(text)) return HEMISPHERE.EAST;
  if (/^(w|west|ouest|oeste|o|西|西经)$/.test(text)) return HEMISPHERE.WEST;

  if (/nord|north/.test(text)) return HEMISPHERE.NORTH;
  if (/sud|south/.test(text)) return HEMISPHERE.SOUTH;
  if (/ouest|west|\bo\b|\bw\b/.test(text)) return HEMISPHERE.WEST;
  if (/est|east|\be\b/.test(text)) return HEMISPHERE.EAST;

  if (role === ROLE.LATITUDE && /lat/.test(text) && /n/.test(text)) return HEMISPHERE.NORTH;
  if (role === ROLE.LATITUDE && /lat/.test(text) && /s/.test(text)) return HEMISPHERE.SOUTH;
  if (role === ROLE.LONGITUDE && /lon|long/.test(text) && /(w|o|ouest|west)/.test(text)) return HEMISPHERE.WEST;
  if (role === ROLE.LONGITUDE && /lon|long/.test(text) && /(e|est|east)/.test(text)) return HEMISPHERE.EAST;

  return null;
}

function headerHemisphere(headerSemantics = {}, role = "") {
  const direct = headerSemantics[role];
  const alternate = role === ROLE.LATITUDE
    ? headerSemantics.latitudeHeader || headerSemantics.lat
    : headerSemantics.longitudeHeader || headerSemantics.lon || headerSemantics.long;
  return normalizeDmsHemisphere(direct || alternate, role);
}

function normalizeRole(value = "") {
  const text = cleanString(value).toLowerCase();
  if (text === ROLE.LATITUDE || text === "lat") return ROLE.LATITUDE;
  if (text === ROLE.LONGITUDE || text === "lon" || text === "lng" || text === "long") return ROLE.LONGITUDE;
  return "";
}

function normalizeDmsToken(token = {}, headerSemantics = {}) {
  const role = normalizeRole(token.role);
  const degrees = numberOrNull(token.degrees ?? token.degree ?? token.deg);
  const minutes = numberOrNull(token.minutes ?? token.minute ?? token.min);
  const seconds = numberOrNull(token.seconds ?? token.second ?? token.sec);
  const hemisphere = normalizeDmsHemisphere(token.hemisphere ?? token.direction, role)
    || headerHemisphere(headerSemantics, role);

  return Object.freeze({
    role,
    degrees,
    minutes,
    seconds,
    hemisphere
  });
}

function validateToken(token = {}) {
  if (!token.role || ![ROLE.LATITUDE, ROLE.LONGITUDE].includes(token.role)) {
    return "missing_role";
  }
  if (token.degrees === null || token.minutes === null || token.seconds === null) {
    return "missing_dms_token";
  }
  if (token.minutes < 0 || token.minutes >= 60) {
    return "invalid_minutes";
  }
  if (token.seconds < 0 || token.seconds >= 60) {
    return "invalid_seconds";
  }
  if (!token.hemisphere) {
    return "missing_hemisphere";
  }
  if (token.role === ROLE.LATITUDE && ![HEMISPHERE.NORTH, HEMISPHERE.SOUTH].includes(token.hemisphere)) {
    return "invalid_latitude_hemisphere";
  }
  if (token.role === ROLE.LONGITUDE && ![HEMISPHERE.EAST, HEMISPHERE.WEST].includes(token.hemisphere)) {
    return "invalid_longitude_hemisphere";
  }
  return "";
}

export function dmsTokenToDecimal(token = {}) {
  const error = validateToken(token);
  if (error) {
    return Object.freeze({
      status: DMS_INTERPRETATION_STATUS.INVALID,
      error,
      value: null
    });
  }

  const absolute = Math.abs(token.degrees) + token.minutes / 60 + token.seconds / 3600;
  const negative = token.hemisphere === HEMISPHERE.SOUTH || token.hemisphere === HEMISPHERE.WEST;
  return Object.freeze({
    status: DMS_INTERPRETATION_STATUS.COMPLETE,
    error: "",
    value: roundCoordinate(negative ? -absolute : absolute)
  });
}

function normalizePointLabel(value, fallback) {
  const text = cleanString(value);
  return text || String(fallback);
}

function normalizeDmsRows(input = {}) {
  if (Array.isArray(input.rows)) return input.rows;
  if (Array.isArray(input.points)) return input.points;
  if (Array.isArray(input.dmsRows)) return input.dmsRows;
  return [];
}

function pickToken(row = {}, role = "", headerSemantics = {}) {
  const value = role === ROLE.LATITUDE
    ? row.latitude || row.lat || row.northing
    : row.longitude || row.lon || row.lng || row.easting;
  if (value && typeof value === "object") {
    return normalizeDmsToken({ ...value, role: value.role || role }, headerSemantics);
  }
  return normalizeDmsToken({
    role,
    degrees: row[`${role}Degrees`] ?? row[`${role}Deg`] ?? row[`${role}_degrees`] ?? row[`${role}_deg`],
    minutes: row[`${role}Minutes`] ?? row[`${role}Min`] ?? row[`${role}_minutes`] ?? row[`${role}_min`],
    seconds: row[`${role}Seconds`] ?? row[`${role}Sec`] ?? row[`${role}_seconds`] ?? row[`${role}_sec`],
    hemisphere: row[`${role}Hemisphere`] ?? row[`${role}_hemisphere`]
  }, headerSemantics);
}

export function buildDeterministicDmsInterpretation(input = {}) {
  const headerSemantics = input.headerSemantics && typeof input.headerSemantics === "object"
    ? input.headerSemantics
    : {};
  const rows = normalizeDmsRows(input);
  const normalizedCoordinates = [];
  const sourceRows = [];
  const errors = [];

  rows.forEach((row, index) => {
    const latitude = pickToken(row, ROLE.LATITUDE, headerSemantics);
    const longitude = pickToken(row, ROLE.LONGITUDE, headerSemantics);
    const latitudeResult = dmsTokenToDecimal(latitude);
    const longitudeResult = dmsTokenToDecimal(longitude);
    const point = normalizePointLabel(row.point ?? row.label ?? row.id, index + 1);

    sourceRows.push(Object.freeze({
      point,
      latitude,
      longitude
    }));

    if (latitudeResult.status !== DMS_INTERPRETATION_STATUS.COMPLETE) {
      errors.push(`point_${point}_latitude_${latitudeResult.error}`);
    }
    if (longitudeResult.status !== DMS_INTERPRETATION_STATUS.COMPLETE) {
      errors.push(`point_${point}_longitude_${longitudeResult.error}`);
    }

    if (
      latitudeResult.status === DMS_INTERPRETATION_STATUS.COMPLETE
      && longitudeResult.status === DMS_INTERPRETATION_STATUS.COMPLETE
    ) {
      normalizedCoordinates.push(Object.freeze({
        point,
        lat: latitudeResult.value,
        lon: longitudeResult.value,
        source: "dms_deterministic"
      }));
    }
  });

  const status = rows.length === 0 || errors.some(error => /missing/.test(error))
    ? DMS_INTERPRETATION_STATUS.INCOMPLETE
    : errors.length > 0
      ? DMS_INTERPRETATION_STATUS.INVALID
      : DMS_INTERPRETATION_STATUS.COMPLETE;

  const hemisphereResolved = status === DMS_INTERPRETATION_STATUS.COMPLETE
    && sourceRows.every(row => Boolean(row.latitude.hemisphere && row.longitude.hemisphere));

  return Object.freeze({
    schemaVersion: DMS_COORDINATE_INTERPRETATION_SCHEMA_VERSION,
    interpretationStatus: status,
    deterministicConversion: status === DMS_INTERPRETATION_STATUS.COMPLETE,
    hemisphereResolved,
    pointCount: normalizedCoordinates.length,
    normalizedCoordinates: Object.freeze(normalizedCoordinates),
    sourceRows: Object.freeze(sourceRows),
    errors: Object.freeze(errors),
    affectsLegacyWinner: false,
    affectsCoordinateResult: false,
    affectsKml: false
  });
}
