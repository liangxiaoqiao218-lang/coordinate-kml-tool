import { finiteNumberOrNull } from '../coordinate-values.js';

export const BFTM_ITRF2008_CRS = Object.freeze({
  id: 'BFTM:ITRF2008',
  projection: 'transverse-mercator',
  axisOrder: 'easting_northing',
});

// Existing production BFTM parameters, moved server-side so Map and KML use
// the same WGS84 result. CRS selection remains explicit in the agentic router.
export function bftmToWgs84(easting, northing) {
  const eastingNumber = finiteNumberOrNull(easting);
  const northingNumber = finiteNumberOrNull(northing);
  if (eastingNumber === null || northingNumber === null) return null;

  const a = 6378137;
  const eccentricity = 0.0818191910428158;
  const e1sq = (eccentricity ** 2) / (1 - eccentricity ** 2);
  const scaleFactor = 0.9996;
  const x = eastingNumber - 600000;
  const y = northingNumber;
  const centralMeridian = -1.5;
  const m = y / scaleFactor;
  const mu = m / (a * (1 - (eccentricity ** 2) / 4
    - (3 * eccentricity ** 4) / 64
    - (5 * eccentricity ** 6) / 256));
  const e1 = (1 - Math.sqrt(1 - eccentricity ** 2)) / (1 + Math.sqrt(1 - eccentricity ** 2));
  const j1 = (3 * e1 / 2) - (27 * e1 ** 3 / 32);
  const j2 = (21 * e1 ** 2 / 16) - (55 * e1 ** 4 / 32);
  const j3 = 151 * e1 ** 3 / 96;
  const j4 = 1097 * e1 ** 4 / 512;
  const fp = mu + j1 * Math.sin(2 * mu) + j2 * Math.sin(4 * mu)
    + j3 * Math.sin(6 * mu) + j4 * Math.sin(8 * mu);
  const sinfp = Math.sin(fp);
  const cosfp = Math.cos(fp);
  const tanfp = Math.tan(fp);
  const c1 = e1sq * cosfp ** 2;
  const t1 = tanfp ** 2;
  const r1 = a * (1 - eccentricity ** 2) / ((1 - eccentricity ** 2 * sinfp ** 2) ** 1.5);
  const n1 = a / Math.sqrt(1 - eccentricity ** 2 * sinfp ** 2);
  const d = x / (n1 * scaleFactor);
  const q1 = n1 * tanfp / r1;
  const q2 = d ** 2 / 2;
  const q3 = (5 + 3 * t1 + 10 * c1 - 4 * c1 ** 2 - 9 * e1sq) * d ** 4 / 24;
  const q4 = (61 + 90 * t1 + 298 * c1 + 45 * t1 ** 2 - 252 * e1sq - 3 * c1 ** 2) * d ** 6 / 720;
  const latitudeRadians = fp - q1 * (q2 - q3 + q4);
  const q5 = d;
  const q6 = (1 + 2 * t1 + c1) * d ** 3 / 6;
  const q7 = (5 - 2 * c1 + 28 * t1 - 3 * c1 ** 2 + 8 * e1sq + 24 * t1 ** 2) * d ** 5 / 120;
  const longitude = centralMeridian + ((q5 - q6 + q7) / cosfp) * 180 / Math.PI;
  const latitude = latitudeRadians * 180 / Math.PI;
  return Number.isFinite(latitude) && Number.isFinite(longitude)
    ? { latitude, longitude, lat: latitude, lon: longitude }
    : null;
}
