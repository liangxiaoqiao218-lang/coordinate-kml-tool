import assert from "node:assert/strict";

import { resolveShadowUtmIntent } from "../server/utm-intent/shadow-resolver.js";
import { buildShadowTypedUtmResult } from "../server/utm-intent/typed-result.js";
import { compareLegacyAndTypedUtm } from "../server/utm-intent/legacy-compare.js";

const UTM30_POINTS = [
  [727250, 1219700],
  [728400, 1219700],
  [728400, 1219500],
  [728700, 1219500],
  [728700, 1220000],
  [729150, 1220000],
  [729150, 1219500],
  [729200, 1219500]
];

function legacyIndexUtmToWgs84(easting, northing, zone, northernHemisphere) {
  const a = 6378137;
  const e = 0.08181919084262149;
  const e1sq = (e * e) / (1 - e * e);
  const k0 = 0.9996;
  const x = easting - 500000;
  const y = northing - (northernHemisphere ? 0 : 10000000);
  const longitudeOrigin = (zone - 1) * 6 - 180 + 3;
  const m = y / k0;
  const mu = m / (a * (1 - (e * e) / 4 - 3 * e ** 4 / 64 - 5 * e ** 6 / 256));
  const e1 = (1 - Math.sqrt(1 - e * e)) / (1 + Math.sqrt(1 - e * e));
  const j1 = 3 * e1 / 2 - 27 * e1 ** 3 / 32;
  const j2 = 21 * e1 ** 2 / 16 - 55 * e1 ** 4 / 32;
  const j3 = 151 * e1 ** 3 / 96;
  const j4 = 1097 * e1 ** 4 / 512;
  const fp = mu + j1 * Math.sin(2 * mu) + j2 * Math.sin(4 * mu) + j3 * Math.sin(6 * mu) + j4 * Math.sin(8 * mu);
  const sinfp = Math.sin(fp);
  const cosfp = Math.cos(fp);
  const tanfp = Math.tan(fp);
  const c1 = e1sq * cosfp ** 2;
  const t1 = tanfp ** 2;
  const r1 = a * (1 - e * e) / (1 - e * e * sinfp ** 2) ** 1.5;
  const n1 = a / Math.sqrt(1 - e * e * sinfp ** 2);
  const d = x / (n1 * k0);
  const q1 = n1 * tanfp / r1;
  const q2 = d ** 2 / 2;
  const q3 = (5 + 3 * t1 + 10 * c1 - 4 * c1 ** 2 - 9 * e1sq) * d ** 4 / 24;
  const q4 = (61 + 90 * t1 + 298 * c1 + 45 * t1 ** 2 - 252 * e1sq - 3 * c1 ** 2) * d ** 6 / 720;
  const latitude = fp - q1 * (q2 - q3 + q4);
  const q5 = d;
  const q6 = (1 + 2 * t1 + c1) * d ** 3 / 6;
  const q7 = (5 - 2 * c1 + 28 * t1 - 3 * c1 ** 2 + 8 * e1sq + 24 * t1 ** 2) * d ** 5 / 120;
  return {
    longitude: longitudeOrigin + (q5 - q6 + q7) / cosfp * 180 / Math.PI,
    latitude: latitude * 180 / Math.PI
  };
}

function intentFrom(rawText) {
  return resolveShadowUtmIntent({ rawText }).shadowIntent;
}

const utm30Legacy = {
  precisionMode: "utm30n-projected-x-y",
  coordinates: UTM30_POINTS.map(([x, y]) => `${x},${y}`),
  transformedWgs84: UTM30_POINTS.map(([x, y]) => legacyIndexUtmToWgs84(x, y, 30, true))
};
const utm30Typed = buildShadowTypedUtmResult({
  shadowIntent: intentFrom("UTM WGS 1984 ZONE 30N"),
  projectedCoordinates: UTM30_POINTS
});
assert.equal(utm30Legacy.precisionMode, "utm30n-projected-x-y");
assert.equal(utm30Typed.typedUtmIntent.coordinateType, "utm_projected_xy");
assert.equal(utm30Typed.typedUtmIntent.epsg, "EPSG:32630");
const utm30Compare = compareLegacyAndTypedUtm({ legacyResult: utm30Legacy, typedResult: utm30Typed, tolerance: 1e-8 });
assert.equal(utm30Compare.status, "match");
console.log(`PASS UTM30 legacy vs V2 (${utm30Compare.pointCount} points, max difference ${utm30Compare.maximumDifference})`);

const mismatchedLegacy = structuredClone(utm30Legacy);
mismatchedLegacy.transformedWgs84[0].longitude += 2e-6;
const mismatch = compareLegacyAndTypedUtm({ legacyResult: mismatchedLegacy, typedResult: utm30Typed, tolerance: 1e-6 });
assert.equal(mismatch.status, "migration_compare_failed");
assert.equal(mismatch.reason, "transformation_mismatch");
console.log("PASS tolerance breach produces migration_compare_failed");

const utm50Typed = buildShadowTypedUtmResult({
  shadowIntent: intentFrom("UTM WGS 1984 ZONA 50S"),
  projectedCoordinates: [[779271.176, 9720912.526]]
});
assert.equal(utm50Typed.typedUtmIntent.coordinateType, "utm_projected_xy");
assert.equal(utm50Typed.typedUtmIntent.epsg, "EPSG:32750");
assert.ok(Math.abs(utm50Typed.transformedWgs84[0].longitude - 119.51135083333333) < 1e-6);
assert.ok(Math.abs(utm50Typed.transformedWgs84[0].latitude - (-2.522537222222222)) < 1e-6);
const utm50Compare = compareLegacyAndTypedUtm({
  legacyResult: { precisionMode: "projected-x-y", transformedWgs84: [] },
  typedResult: utm50Typed
});
assert.equal(utm50Compare.status, "not_comparable");
console.log("PASS UTM50S produces V2 EPSG:32750 while legacy remains unsupported");

for (const testCase of [
  ["BFTM", "Projection BFTM / ITRF 2008"],
  ["MGRS", "MGRS / UTM Grid Reference 47RLH 24469 42832"],
  ["Kyrgyz GK", "Gauss-Kruger rectangular coordinate system"]
]) {
  const typed = buildShadowTypedUtmResult({ shadowIntent: intentFrom(testCase[1]), projectedCoordinates: UTM30_POINTS });
  assert.equal(typed, null);
  console.log(`PASS ${testCase[0]} does not produce typed UTM`);
}

console.log("\nTyped UTM Result Shadow Regression: 6/6 PASS");
