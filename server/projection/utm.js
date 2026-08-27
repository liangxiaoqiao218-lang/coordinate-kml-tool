import { finiteNumberOrNull } from "../coordinate-values.js";

export const UTM30N_CRS = Object.freeze({
  id: "EPSG:32630",
  projection: "utm",
  zone: 30,
  hemisphere: "N",
  axisOrder: "easting_northing"
});

// Existing production UTM conversion, shared by MGRS and structured UTM rows.
export function utmToWgs84(zone, easting, northing, northernHemisphere = true) {
  const zoneNumber = finiteNumberOrNull(zone);
  const eastingNumber = finiteNumberOrNull(easting);
  const northingNumber = finiteNumberOrNull(northing);
  if (!Number.isInteger(zoneNumber) || zoneNumber < 1 || zoneNumber > 60
    || eastingNumber === null || northingNumber === null) {
    return null;
  }

  const a = 6378137;
  const e = 0.08181919084262149;
  const e1sq = 0.006739496742276434;
  const k0 = 0.9996;
  const x = eastingNumber - 500000;
  let y = northingNumber;

  if (!northernHemisphere) {
    y -= 10000000;
  }

  const longOrigin = (zoneNumber - 1) * 6 - 180 + 3;
  const m = y / k0;
  const mu = m / (a * (1 - (e ** 2) / 4 - (3 * e ** 4) / 64 - (5 * e ** 6) / 256));
  const e1 = (1 - Math.sqrt(1 - e ** 2)) / (1 + Math.sqrt(1 - e ** 2));
  const j1 = (3 * e1 / 2) - (27 * e1 ** 3 / 32);
  const j2 = (21 * e1 ** 2 / 16) - (55 * e1 ** 4 / 32);
  const j3 = 151 * e1 ** 3 / 96;
  const j4 = 1097 * e1 ** 4 / 512;
  const fp = mu + j1 * Math.sin(2 * mu) + j2 * Math.sin(4 * mu) + j3 * Math.sin(6 * mu) + j4 * Math.sin(8 * mu);
  const c1 = e1sq * Math.cos(fp) ** 2;
  const t1 = Math.tan(fp) ** 2;
  const n1 = a / Math.sqrt(1 - e ** 2 * Math.sin(fp) ** 2);
  const r1 = a * (1 - e ** 2) / ((1 - e ** 2 * Math.sin(fp) ** 2) ** 1.5);
  const d = x / (n1 * k0);
  const q1 = n1 * Math.tan(fp) / r1;
  const q2 = d ** 2 / 2;
  const q3 = (5 + 3 * t1 + 10 * c1 - 4 * c1 ** 2 - 9 * e1sq) * d ** 4 / 24;
  const q4 = (61 + 90 * t1 + 298 * c1 + 45 * t1 ** 2 - 252 * e1sq - 3 * c1 ** 2) * d ** 6 / 720;
  const lat = fp - q1 * (q2 - q3 + q4);
  const q5 = d;
  const q6 = (1 + 2 * t1 + c1) * d ** 3 / 6;
  const q7 = (5 - 2 * c1 + 28 * t1 - 3 * c1 ** 2 + 8 * e1sq + 24 * t1 ** 2) * d ** 5 / 120;
  const lon = (q5 - q6 + q7) / Math.cos(fp);
  const result = {
    lat: lat * 180 / Math.PI,
    lon: longOrigin + lon * 180 / Math.PI
  };

  return Number.isFinite(result.lat) && Number.isFinite(result.lon) ? result : null;
}
