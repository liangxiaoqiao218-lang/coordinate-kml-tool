// Projection-only reuse of the verified isolated Kyrgyz GK implementation.
// Same EPSG:28413 definition as the existing frontend; no CRS inference or recognizer.
export const KYRGYZ_GK_OUTPUT_CRS = "EPSG:4326";
export const KYRGYZ_GK_CRS = Object.freeze({id:"EPSG:28413", projection:"gauss-kruger", axisOrder:"easting_northing"});
const KYRGYZ_GK_PROJ4_DEF = "+proj=tmerc +lat_0=0 +lon_0=75 +k=1 +x_0=13500000 +y_0=0 +ellps=krass +towgs84=25,-141,-78.5,0,-0.35,-0.736,0 +units=m +no_defs +type=crs";
const KRASSOWSKY_A = 6378245;
const KRASSOWSKY_INV_F = 298.3;
const WGS84_A = 6378137;
const WGS84_INV_F = 298.257223563;
const KYRGYZ_BOUNDS = Object.freeze({
  minLongitude: 69,
  maxLongitude: 80,
  minLatitude: 39,
  maxLatitude: 43,
});

function looksLikeKyrgyzGkPair(x, y) {
  const easting = Number(x);
  const northing = Number(y);
  return Number.isFinite(easting)
    && Number.isFinite(northing)
    && easting >= 13000000
    && easting <= 13999999
    && northing >= 3900000
    && northing <= 4800000;
}

function inverseTransverseMercator(easting, northing) {
  const f = 1 / KRASSOWSKY_INV_F;
  const e2 = 2 * f - f ** 2;
  const ep2 = e2 / (1 - e2);
  const e1 = (1 - Math.sqrt(1 - e2)) / (1 + Math.sqrt(1 - e2));
  const k0 = 1;
  const falseEasting = 13500000;
  const falseNorthing = 0;
  const longitudeOrigin = 75 * Math.PI / 180;
  const x = Number(easting) - falseEasting;
  const y = Number(northing) - falseNorthing;
  const m = y / k0;
  const mu = m / (KRASSOWSKY_A * (1 - e2 / 4 - 3 * e2 ** 2 / 64 - 5 * e2 ** 3 / 256));
  const fp = mu
    + (3 * e1 / 2 - 27 * e1 ** 3 / 32) * Math.sin(2 * mu)
    + (21 * e1 ** 2 / 16 - 55 * e1 ** 4 / 32) * Math.sin(4 * mu)
    + (151 * e1 ** 3 / 96) * Math.sin(6 * mu)
    + (1097 * e1 ** 4 / 512) * Math.sin(8 * mu);
  const sinfp = Math.sin(fp);
  const cosfp = Math.cos(fp);
  const tanfp = Math.tan(fp);
  const c1 = ep2 * cosfp ** 2;
  const t1 = tanfp ** 2;
  const n1 = KRASSOWSKY_A / Math.sqrt(1 - e2 * sinfp ** 2);
  const r1 = KRASSOWSKY_A * (1 - e2) / ((1 - e2 * sinfp ** 2) ** 1.5);
  const d = x / (n1 * k0);
  const lat = fp - (n1 * tanfp / r1) * (
    d ** 2 / 2
    - (5 + 3 * t1 + 10 * c1 - 4 * c1 ** 2 - 9 * ep2) * d ** 4 / 24
    + (61 + 90 * t1 + 298 * c1 + 45 * t1 ** 2 - 252 * ep2 - 3 * c1 ** 2) * d ** 6 / 720
  );
  const lon = longitudeOrigin + (
    d
    - (1 + 2 * t1 + c1) * d ** 3 / 6
    + (5 - 2 * c1 + 28 * t1 - 3 * c1 ** 2 + 8 * ep2 + 24 * t1 ** 2) * d ** 5 / 120
  ) / cosfp;
  return { latitudeRad: lat, longitudeRad: lon };
}

function geodeticToCartesian(latitudeRad, longitudeRad, ellipsoidA, invF) {
  const f = 1 / invF;
  const e2 = 2 * f - f ** 2;
  const sinLat = Math.sin(latitudeRad);
  const cosLat = Math.cos(latitudeRad);
  const sinLon = Math.sin(longitudeRad);
  const cosLon = Math.cos(longitudeRad);
  const n = ellipsoidA / Math.sqrt(1 - e2 * sinLat ** 2);
  return {
    x: n * cosLat * cosLon,
    y: n * cosLat * sinLon,
    z: n * (1 - e2) * sinLat,
  };
}

function cartesianToGeodetic({ x, y, z }, ellipsoidA, invF) {
  const f = 1 / invF;
  const e2 = 2 * f - f ** 2;
  const p = Math.sqrt(x ** 2 + y ** 2);
  let latitude = Math.atan2(z, p * (1 - e2));
  for (let index = 0; index < 8; index += 1) {
    const sinLat = Math.sin(latitude);
    const n = ellipsoidA / Math.sqrt(1 - e2 * sinLat ** 2);
    latitude = Math.atan2(z + e2 * n * sinLat, p);
  }
  return {
    latitude: latitude * 180 / Math.PI,
    longitude: Math.atan2(y, x) * 180 / Math.PI,
  };
}

function pulkovo1942ToWgs84(latitudeRad, longitudeRad) {
  const source = geodeticToCartesian(latitudeRad, longitudeRad, KRASSOWSKY_A, KRASSOWSKY_INV_F);
  const arcSecondToRad = Math.PI / (180 * 3600);
  const dx = 25;
  const dy = -141;
  const dz = -78.5;
  const rx = 0 * arcSecondToRad;
  const ry = -0.35 * arcSecondToRad;
  const rz = -0.736 * arcSecondToRad;
  const scale = 1;
  const target = {
    x: dx + scale * source.x - rz * source.y + ry * source.z,
    y: dy + rz * source.x + scale * source.y - rx * source.z,
    z: dz - ry * source.x + rx * source.y + scale * source.z,
  };
  return cartesianToGeodetic(target, WGS84_A, WGS84_INV_F);
}

export function convertKyrgyzGkToWgs84(x, y) {
  const easting = Number(x);
  const northing = Number(y);
  if (!looksLikeKyrgyzGkPair(easting, northing)) return null;
  const pulkovo = inverseTransverseMercator(easting, northing);
  const wgs84 = pulkovo1942ToWgs84(pulkovo.latitudeRad, pulkovo.longitudeRad);
  if (!Number.isFinite(wgs84.latitude)
    || !Number.isFinite(wgs84.longitude)
    || wgs84.longitude < KYRGYZ_BOUNDS.minLongitude
    || wgs84.longitude > KYRGYZ_BOUNDS.maxLongitude
    || wgs84.latitude < KYRGYZ_BOUNDS.minLatitude
    || wgs84.latitude > KYRGYZ_BOUNDS.maxLatitude) {
    return null;
  }
  return Object.freeze({
    latitude: wgs84.latitude,
    longitude: wgs84.longitude,
    crs: KYRGYZ_GK_OUTPUT_CRS,
  });
}


