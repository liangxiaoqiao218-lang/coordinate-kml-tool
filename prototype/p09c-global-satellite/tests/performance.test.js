import assert from "node:assert/strict";
import test from "node:test";
import { performance } from "node:perf_hooks";
import { adaptFinalizedCoordinateResult } from "../src/canonical-preview-adapter.js";
import { createGeometryRenderPlan } from "../src/geometry-render-plan.js";
import { finalized, polygonWithVertices } from "./helpers.js";

for (const vertexCount of [8, 50, 100, 500, 1000]) {
  test(`${vertexCount} vertices use one source without authority mutation`, async () => {
    const geometry = polygonWithVertices(vertexCount);
    const input = await finalized(geometry, { kmlReady: false, decisionState: "REVIEW_REQUIRED", requiresReview: true });
    const before = structuredClone(input);
    const started = performance.now();
    const adapted = await adaptFinalizedCoordinateResult(input);
    const plan = createGeometryRenderPlan(adapted.preview.geometry);
    const elapsedMs = performance.now() - started;
    assert.equal(plan.sourceId, "p09c-canonical-geometry");
    assert.equal(plan.layers.length, 2);
    assert.equal(plan.source.data.geometry.coordinates[0].length, vertexCount + 1);
    assert.deepEqual(input, before);
    assert.ok(elapsedMs < 500, `render planning exceeded 500ms: ${elapsedMs}`);
  });
}
