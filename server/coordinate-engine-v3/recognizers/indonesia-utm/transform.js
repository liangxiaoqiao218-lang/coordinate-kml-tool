const WGS84_SEMI_MAJOR_AXIS = 6378137;
const WGS84_ECCENTRICITY = 0.08181919084262149;
const UTM_SCALE_FACTOR = 0.9996;
const UTM_FALSE_EASTING = 500000;
const UTM_SOUTH_FALSE_NORTHING = 10000000;

function assertFiniteNumber(value, field) {
  const number = Number(value);
  if (!Number.isFinite(number)) throw new TypeError(`${field} must be a finite number`);
  return number;
}

function assertUtmZone(value) {
  const zone = Number(value);
  if (!Number.isInteger(zone) || zone < 1 || zone > 60) {
    throw new RangeError("UTM zone must be an integer from 1 through 60");
  }
  return zone;
}

function assertHemisphere(value) {
  if (value !== "north" && value !== "south") {
    throw new RangeError('UTM hemisphere must be "north" or "south"');
  }
  return value;
}

export function transformIndonesiaUtmPoint({ easting, northing, zone, hemisphere } = {}) {
  const x = assertFiniteNumber(easting, "easting") - UTM_FALSE_EASTING;
  let y = assertFiniteNumber(northing, "northing");
  const normalizedZone = assertUtmZone(zone);
  const normalizedHemisphere = assertHemisphere(hemisphere);
  if (normalizedHemisphere === "south") y -= UTM_SOUTH_FALSE_NORTHING;

  const eccentricity = WGS84_ECCENTRICITY;
  const eccentricitySquared = eccentricity * eccentricity;
  const secondEccentricitySquared = eccentricitySquared / (1 - eccentricitySquared);
  const meridionalArc = y / UTM_SCALE_FACTOR;
  const mu = meridionalArc / (WGS84_SEMI_MAJOR_AXIS * (
    1
    - eccentricitySquared / 4
    - 3 * eccentricity ** 4 / 64
    - 5 * eccentricity ** 6 / 256
  ));
  const e1 = (1 - Math.sqrt(1 - eccentricitySquared)) / (1 + Math.sqrt(1 - eccentricitySquared));
  const j1 = 3 * e1 / 2 - 27 * e1 ** 3 / 32;
  const j2 = 21 * e1 ** 2 / 16 - 55 * e1 ** 4 / 32;
  const j3 = 151 * e1 ** 3 / 96;
  const j4 = 1097 * e1 ** 4 / 512;
  const footprintLatitude = mu
    + j1 * Math.sin(2 * mu)
    + j2 * Math.sin(4 * mu)
    + j3 * Math.sin(6 * mu)
    + j4 * Math.sin(8 * mu);
  const sinFootprint = Math.sin(footprintLatitude);
  const cosFootprint = Math.cos(footprintLatitude);
  const tanFootprint = Math.tan(footprintLatitude);
  const c1 = secondEccentricitySquared * cosFootprint ** 2;
  const t1 = tanFootprint ** 2;
  const r1 = WGS84_SEMI_MAJOR_AXIS * (1 - eccentricitySquared)
    / (1 - eccentricitySquared * sinFootprint ** 2) ** 1.5;
  const n1 = WGS84_SEMI_MAJOR_AXIS / Math.sqrt(1 - eccentricitySquared * sinFootprint ** 2);
  const d = x / (n1 * UTM_SCALE_FACTOR);
  const q1 = n1 * tanFootprint / r1;
  const q2 = d ** 2 / 2;
  const q3 = (5 + 3 * t1 + 10 * c1 - 4 * c1 ** 2 - 9 * secondEccentricitySquared) * d ** 4 / 24;
  const q4 = (61 + 90 * t1 + 298 * c1 + 45 * t1 ** 2 - 252 * secondEccentricitySquared - 3 * c1 ** 2) * d ** 6 / 720;
  const latitudeRadians = footprintLatitude - q1 * (q2 - q3 + q4);
  const q5 = d;
  const q6 = (1 + 2 * t1 + c1) * d ** 3 / 6;
  const q7 = (5 - 2 * c1 + 28 * t1 - 3 * c1 ** 2 + 8 * secondEccentricitySquared + 24 * t1 ** 2) * d ** 5 / 120;
  const centralMeridian = (normalizedZone - 1) * 6 - 180 + 3;
  const longitude = centralMeridian + (q5 - q6 + q7) / cosFootprint * 180 / Math.PI;
  const latitude = latitudeRadians * 180 / Math.PI;

  if (!Number.isFinite(longitude) || !Number.isFinite(latitude)) {
    throw new RangeError("UTM transformation produced a non-finite WGS84 point");
  }

  return Object.freeze({ longitude, latitude });
}

export function transformIndonesiaUtmRows(rows = [], crs = {}) {
  if (!Array.isArray(rows)) throw new TypeError("rows must be an array");
  return Object.freeze(rows.map((row, index) => Object.freeze({
    index,
    point: String(row.point || index + 1),
    ...transformIndonesiaUtmPoint({
      easting: row.easting,
      northing: row.northing,
      zone: crs.zone,
      hemisphere: crs.hemisphere,
    }),
  })));
}
