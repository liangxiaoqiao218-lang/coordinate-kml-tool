import assert from "node:assert/strict";
import test from "node:test";
import { adaptFinalizedCoordinateResult } from "../src/canonical-preview-adapter.js";
import { MAP_PREVIEW_GATE } from "../src/constants.js";
import { createGeometryRenderPlan } from "../src/geometry-render-plan.js";
import { finalized, geometries } from "./helpers.js";

test("only finalized_coordinate_result_v1 is accepted", async () => {
  const raw = await adaptFinalizedCoordinateResult({ text: "raw OCR", geometry: geometries.Point });
  assert.equal(raw.ok, false);
  assert.deepEqual(raw.reasonCodes, ["FINALIZED_COORDINATE_RESULT_V1_REQUIRED"]);
});

test("Point, LineString, Polygon and MultiPolygon produce one GeoJSON source", async t => {
  for (const [type, geometry] of Object.entries(geometries)) {
    await t.test(type, async () => {
      const input = await finalized(geometry);
      const before = structuredClone(input);
      const adapted = await adaptFinalizedCoordinateResult(input);
      const plan = createGeometryRenderPlan(adapted.preview.geometry);
      assert.equal(adapted.ok, true);
      assert.equal(adapted.generalMapGate, MAP_PREVIEW_GATE);
      assert.equal(plan.source.type, "geojson");
      assert.equal(plan.source.data.geometry.type, type);
      assert.deepEqual(input, before, "canonical authority must not mutate");
    });
  }
});

test("pending review with drawable geometry allows Map while KML stays blocked", async () => {
  const input = await finalized(geometries.Polygon, {
    confirmationStatus: "pending",
    qualityGateStatus: "review_required",
    decisionState: "REVIEW_REQUIRED",
    requiresReview: true,
    kmlReady: false
  });
  const adapted = await adaptFinalizedCoordinateResult(input);
  assert.equal(adapted.previewEligibility.allowed, true);
  assert.ok(adapted.preview.warnings.includes("REVIEW_REQUIRED"));
  assert.ok(adapted.preview.warnings.includes("KML_BLOCKED"));
  assert.equal(input.kmlReady, false);
  assert.equal(input.decisionState, "REVIEW_REQUIRED");
});

test("invalid geometry and stale geometry identity fail closed", async () => {
  const invalid = await finalized(geometries.Point, { geometry: { type: "Point", coordinates: [181, 0] } });
  assert.equal((await adaptFinalizedCoordinateResult(invalid)).ok, false);
  const stale = await finalized(geometries.Polygon, { geometryHash: "sha256:stale" });
  assert.deepEqual((await adaptFinalizedCoordinateResult(stale)).reasonCodes, ["GEOMETRY_HASH_MISMATCH"]);
});
