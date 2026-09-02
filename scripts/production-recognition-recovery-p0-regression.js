import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  COORDINATE_CONFIRMATION_STATUS,
  COORDINATE_DECISION_STATE,
  COORDINATE_GATE_REASON,
  COORDINATE_QUALITY_GATE_STATUS,
  FINALIZED_COORDINATE_CRS,
  finalizeCoordinateResult
} from "../server/coordinate-finalizer/index.js";
import { buildFamilyAvailabilityBlockedEngine } from "../server/coordinate-finalizer/family-availability-policy.js";
import {
  buildMadagascarCadastralCellPolygons,
  collapseExactRepeatedCoordinateSequence,
  extractMadagascarCadastralRows,
  getIndonesiaUtm50Info,
  hasMadagascarMapGridTickTakeover,
  hasStrongPrintedProjectedTableEvidence
} from "../server/recognition/family-primary-routing.js";
import { utmToWgs84 } from "../server/projection/utm.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const goldenPath = path.join(root, "regression-samples", "production-recognition-recovery-p0", "golden-records.json");
const golden = JSON.parse(await readFile(goldenPath, "utf8"));
const cases = [];
function test(name, fn) { cases.push({ name, fn }); }

const polygon = Object.freeze({
  type: "Polygon",
  coordinates: Object.freeze([Object.freeze([
    Object.freeze([119.50, -2.51]), Object.freeze([119.52, -2.51]),
    Object.freeze([119.52, -2.53]), Object.freeze([119.50, -2.51])
  ])])
});
function candidate(overrides = {}) {
  return {
    resultId: "p0-recovery-result",
    resultRevision: 1,
    currentRevision: 1,
    confirmedRevision: null,
    sourceAuthority: "legacy",
    coordinateType: "indonesia_utm50_projected",
    precisionMode: "indonesia-utm50s-projected",
    family: "indonesia_utm50_projected",
    availabilityStatus: "AVAILABLE",
    crs: FINALIZED_COORDINATE_CRS,
    geometry: polygon,
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.PENDING,
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.REVIEW_REQUIRED,
    technicalKmlReady: true,
    currentAuthorizedGeometryExportable: true,
    requiresReview: true,
    kmlReady: false,
    groups: [{ groupId: "group_1", requiresReview: true, kmlReady: false }],
    ...overrides
  };
}

test("blocked payload engine is locally derived without unavailableEngine ReferenceError", () => {
  const engine = buildFamilyAvailabilityBlockedEngine({
    availability: { family: "madagascar_cadastral_grid" },
    coordinateType: "madagascar_cadastral_grid",
    precisionMode: "cadastral-grid-num-xv-yv"
  });
  assert.equal(engine.coordinate_type, "madagascar_cadastral_grid");
  assert.equal(engine.groups.length, 0);
});

test("Indonesia DMS real fixture preserves E/S signs and four polygon points", () => {
  const record = golden.records.find(item => item.id === "indonesia-dms-real-001");
  const text = `SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S\nNo. | X | Y | Latitude | Longitude\n${record.sourceRows.join("\n")}`;
  const info = getIndonesiaUtm50Info(text, { transform: utmToWgs84 });
  assert.equal(info.rowCount, 4);
  assert.equal(info.crs, "EPSG:32750");
  assert.equal(info.rows.every(row => row.lat < 0 && row.lon > 0), true);
});

test("Indonesia projected duplicate full sequence collapses to six without global dedupe", () => {
  const record = golden.records.find(item => item.id === "indonesia-projected-real-002");
  const duplicated = [...record.sourceRows, ...record.sourceRows];
  const text = `SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S\nNo. | X | Y\n${duplicated.join("\n")}`;
  const info = getIndonesiaUtm50Info(text, { transform: utmToWgs84 });
  assert.equal(info.rowCount, 6);
  assert.equal(info.duplicateSequenceCollapsed, true);
  assert.deepEqual(collapseExactRepeatedCoordinateSequence(["A", "B", "A"]), ["A", "B", "A"]);
  assert.deepEqual(collapseExactRepeatedCoordinateSequence(["A", "B", "C", "A"]), ["A", "B", "C", "A"]);
});

test("printed projected table is strong non-handwritten evidence", async () => {
  const record = golden.records.find(item => item.id === "indonesia-dms-real-001");
  const text = `SISTEM KOORDINAT: UTM WGS 1984 ZONA 50S\nNo. | X | Y | Latitude | Longitude\n${record.sourceRows.join("\n")}`;
  assert.equal(hasStrongPrintedProjectedTableEvidence(text), true);
  assert.equal(hasStrongPrintedProjectedTableEvidence(`1. 11°43'09.20\"N 09°00'56.03\"W\n2. 11°42'09.20\"N 09°01'56.03\"W`), false);
  const serverSource = await readFile(path.join(root, "server.js"), "utf8");
  assert.match(serverSource, /&& !hasStrongPrintedProjectedTable/);
});

test("Madagascar 32-row stable table is parsed and map ticks cannot synthesize rows", () => {
  const record = golden.records.find(item => item.id === "madagascar-cadastral-real-001");
  const text = `Liste_Carrés\nNC | XV | YV | CM_NOMFIR | num\n${record.sourceRows.map((row, index) => `${index + 1} | ${row.split(" | ").slice(1).join(" | ")} | Ilakaka | ${row.split(" | ")[0]}`).join("\n")}`;
  const rows = extractMadagascarCadastralRows(text);
  assert.equal(rows.length, 32);
  assert.deepEqual(rows[0], { num: "280", xv: "292812.5", yv: "360937.5" });
  const cells = buildMadagascarCadastralCellPolygons(rows);
  assert.equal(cells.length, 32);
  assert.equal(cells.every(cell => cell.points.length === 4), true);
  assert.equal(cells.every(cell => cell.points.every(point => Number.isFinite(point.lon) && Number.isFinite(point.lat))), true);
  const ticks = "290625 295625 300625\n535625 540625 545625 550625";
  assert.equal(hasMadagascarMapGridTickTakeover(ticks), true);
  assert.deepEqual(extractMadagascarCadastralRows(ticks), []);
});

test("review and confidence-only quality states do not block current authorized geometry KML", () => {
  const review = finalizeCoordinateResult(candidate(), { clock: () => "2026-09-02T00:00:00.000Z" });
  assert.equal(review.decisionState, COORDINATE_DECISION_STATE.REVIEW_REQUIRED);
  assert.equal(review.kmlReady, true);
  assert.equal(review.blockingReasons.length, 0);
  const quality = finalizeCoordinateResult(candidate({
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.FAILED,
    qualityFailureAuthorityImpact: "confidence_only"
  }), { clock: () => "2026-09-02T00:00:00.000Z" });
  assert.equal(quality.kmlReady, true);
});

test("confidence-only rejection and unavailable status do not retroactively revoke current geometry", () => {
  const rejected = finalizeCoordinateResult(candidate({
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.REJECTED,
    confirmationRejectionAuthorityImpact: "confidence_only"
  }), { clock: () => "2026-09-02T00:00:00.000Z" });
  assert.equal(rejected.kmlReady, true);
  const unavailable = finalizeCoordinateResult(candidate({
    availabilityStatus: "BLOCKED_BY_PROVIDER"
  }), { clock: () => "2026-09-02T00:00:00.000Z" });
  assert.equal(unavailable.kmlReady, true);
});

test("V3 non-authoritative output and invalid CRS confirmation remain hard blocked", () => {
  const v3 = finalizeCoordinateResult(candidate({
    sourceAuthority: "coordinate_engine_v3",
    v3ProductionAuthority: false,
    kmlAuthorityBlocked: true
  }), { clock: () => "2026-09-02T00:00:00.000Z" });
  assert.equal(v3.kmlReady, false);
  const invalidCrs = finalizeCoordinateResult(candidate({
    crs: { id: "UNCONFIRMED", axisOrder: "easting_northing" }
  }), { clock: () => "2026-09-02T00:00:00.000Z" });
  assert.equal(invalidCrs.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
  assert.equal(invalidCrs.kmlReady, false);
  const crsWarning = finalizeCoordinateResult(candidate({
    crs: { id: "UNCONFIRMED", axisOrder: "easting_northing" },
    crsUncertaintyConfidenceOnly: true
  }), { clock: () => "2026-09-02T00:00:00.000Z" });
  assert.equal(crsWarning.kmlReady, true);
});

test("invalid geometry and stale identity remain true KML hard blockers", () => {
  const invalid = finalizeCoordinateResult(candidate({ geometry: { type: "Point", coordinates: [999, 999] } }), { clock: () => "2026-09-02T00:00:00.000Z" });
  assert.equal(invalid.kmlReady, false);
  assert.ok(invalid.blockingReasons.some(reason => reason.code === COORDINATE_GATE_REASON.GEOMETRY_INVALID));
  const stale = finalizeCoordinateResult(candidate({ currentRevision: 2 }), { clock: () => "2026-09-02T00:00:00.000Z" });
  assert.equal(stale.kmlReady, false);
  assert.ok(stale.blockingReasons.some(reason => reason.code === COORDINATE_GATE_REASON.RESULT_REVISION_STALE));
});

test("all three real fixtures are byte-bound to frozen Golden records", async () => {
  assert.equal(golden.records.length, 3);
  for (const record of golden.records) {
    const fixturePath = path.resolve(path.dirname(goldenPath), record.fixture);
    const bytes = await readFile(fixturePath);
    assert.equal(bytes.length, record.bytes, record.id);
    assert.equal(createHash("sha256").update(bytes).digest("hex"), record.sha256, record.id);
  }
});

test("Quick Judge placeholder uses higher-specificity muted styling only", async () => {
  const html = await readFile(path.join(root, "index.html"), "utf8");
  assert.match(html, /\.judge-detail-content \.judge-detail-placeholder\s*\{\s*color:\s*#94a3b8;/);
  assert.match(html, /\.judge-detail-content p\s*\{[^}]*color:\s*#334155;/s);
});

let passed = 0;
for (const entry of cases) {
  await entry.fn();
  passed += 1;
  console.log(`PASS ${entry.name}`);
}
console.log(`Production recognition recovery P0 regression: ${passed}/${cases.length} PASS`);
