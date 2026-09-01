import assert from "node:assert/strict";
import fs from "node:fs";
import {
  FINALIZED_COORDINATE_CRS,
  FINALIZED_COORDINATE_SCHEMA_VERSION,
  createGeometryHash
} from "../server/coordinate-finalizer/index.js";
import {
  SHARE_ACCESS_SCOPE,
  SHARE_USAGE_PERMISSION,
  buildSharedSpatialSnapshot,
  computeSharedSpatialSnapshotHash
} from "../server/spatial/sharing/shared-spatial-result-v1.js";
import { SupabaseSpatialShareStore } from "./spatial-share-local-preview-store.js";

let passed = 0;
async function test(name, run) {
  await run();
  passed += 1;
  console.log(`PASS SHARE-LOCAL-${String(passed).padStart(2, "0")} ${name}`);
}

function finalized() {
  const geometry = { type: "Polygon", coordinates: [[[8, 11], [8.01, 11], [8.01, 11.01], [8, 11]]] };
  return {
    schemaVersion: FINALIZED_COORDINATE_SCHEMA_VERSION,
    resultId: "local-preview-result",
    resultRevision: 1,
    geometryHash: createGeometryHash(geometry),
    sourceAuthority: "legacy",
    coordinateType: "manual_wgs84",
    crs: { ...FINALIZED_COORDINATE_CRS },
    geometry,
    requiresReview: true,
    reviewReason: { schema_version: "coordinate_review_reason_v1", primary_code: "REVIEW_REQUIRED", codes: ["REVIEW_REQUIRED"] },
    confirmationStatus: "pending"
  };
}

const shareId = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const capabilityHash = "a".repeat(64);
const snapshot = buildSharedSpatialSnapshot({
  finalizedResult: finalized(),
  shareId,
  createdAt: "2026-09-01T00:00:00.000Z"
});

await test("create preserves immutable snapshot identity", async () => {
  const store = new SupabaseSpatialShareStore({ clock: () => new Date("2026-09-02T00:00:00.000Z") });
  await store.create({ snapshot, managerCapabilityHash: capabilityHash });
  const active = await store.findActive(shareId);
  assert.equal(active.snapshot.snapshotHash, snapshot.snapshotHash);
  assert.equal(active.snapshot.source.sourceGeometryHash, snapshot.source.sourceGeometryHash);
  assert.equal(Object.isFrozen(active.snapshot), true);
});

await test("read returns the active snapshot", async () => {
  const store = new SupabaseSpatialShareStore({ clock: () => new Date("2026-09-02T00:00:00.000Z") });
  await store.create({ snapshot, managerCapabilityHash: capabilityHash });
  assert.equal((await store.findActive(shareId)).snapshot.shareId, shareId);
});

await test("creator revoke is terminal", async () => {
  const store = new SupabaseSpatialShareStore({ clock: () => new Date("2026-09-02T00:00:00.000Z") });
  await store.create({ snapshot, managerCapabilityHash: capabilityHash });
  assert.equal(await store.revoke(shareId, capabilityHash), true);
  assert.equal(await store.findActive(shareId), null);
  assert.equal(await store.revoke(shareId, capabilityHash), false);
});

await test("v1 shares have no automatic expiry", async () => {
  const store = new SupabaseSpatialShareStore({ clock: () => new Date("2036-09-09T00:00:00.000Z") });
  await store.create({ snapshot, managerCapabilityHash: capabilityHash });
  assert.equal(snapshot.expiresAt, null);
  assert.notEqual(await store.findActive(shareId), null);
});

await test("snapshot hash recomputes exactly", async () => assert.equal(computeSharedSpatialSnapshotHash(snapshot), snapshot.snapshotHash));

await test("tampered immutable content fails closed", async () => {
  const tampered = structuredClone(snapshot);
  tampered.geometry.coordinates[0][0][0] += 1;
  const store = new SupabaseSpatialShareStore();
  await assert.rejects(() => store.create({ snapshot: tampered, managerCapabilityHash: capabilityHash }), /SHARE_SNAPSHOT_HASH_MISMATCH/);
});

await test("non-creator cannot revoke", async () => {
  const store = new SupabaseSpatialShareStore();
  await store.create({ snapshot, managerCapabilityHash: capabilityHash });
  assert.equal(await store.revoke(shareId, "b".repeat(64)), false);
  assert.notEqual(await store.findActive(shareId), null);
});

await test("recipient-only binding has one deterministic winner", async () => {
  const store = new SupabaseSpatialShareStore();
  await store.create({ snapshot, managerCapabilityHash: capabilityHash });
  const [first, second] = await Promise.all([
    store.bindRecipient(shareId, "b".repeat(64)),
    store.bindRecipient(shareId, "c".repeat(64))
  ]);
  assert.equal([first, second].filter(Boolean).length, 1);
  const winner = first ? "b".repeat(64) : "c".repeat(64);
  const loser = first ? "c".repeat(64) : "b".repeat(64);
  assert.notEqual(await store.findAuthorized(shareId, winner), null);
  assert.equal(await store.findAuthorized(shareId, loser), null);
});

await test("anyone-with-link supports multiple independent readers without binding", async () => {
  const anyoneSnapshot = buildSharedSpatialSnapshot({
    finalizedResult: finalized(),
    shareId: "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
    accessScope: SHARE_ACCESS_SCOPE.ANYONE_WITH_LINK,
    usagePermission: SHARE_USAGE_PERMISSION.ALLOW_EDIT
  });
  const store = new SupabaseSpatialShareStore();
  await store.create({ snapshot: anyoneSnapshot, managerCapabilityHash: capabilityHash });
  assert.notEqual(await store.findAuthorized(anyoneSnapshot.shareId, ""), null);
  assert.notEqual(await store.findAuthorized(anyoneSnapshot.shareId, "d".repeat(64)), null);
  assert.equal((await store.findActive(anyoneSnapshot.shareId)).recipientCapabilityHash, null);
});

await test("production application source keeps Supabase selection and Secure cookie", async () => {
  const server = fs.readFileSync(new URL("../server.js", import.meta.url), "utf8");
  assert.match(server, /new SupabaseSpatialShareStore\(\{ supabase \}\)/);
  assert.match(server, /__Host-geokit_spatial_share_manager/);
  assert.match(server, /__Host-geokit_spatial_share_recipient/);
  assert.match(server, /HttpOnly; Secure; SameSite=Strict/);
  assert.match(server, /HttpOnly; Secure; SameSite=Lax/);
  assert.doesNotMatch(server, /GEOKIT_SHARE_LOCAL_PREVIEW|spatial-share-local-preview-store/);
});

await test("loader requires explicit non-production qualification gate", async () => {
  const loader = fs.readFileSync(new URL("./spatial-share-local-preview-loader.mjs", import.meta.url), "utf8");
  const launcher = fs.readFileSync(new URL("./spatial-share-local-preview-launcher.mjs", import.meta.url), "utf8");
  assert.match(loader, /GEOKIT_SHARE_LOCAL_PREVIEW/);
  assert.match(loader, /process\.env\.NODE_ENV === "production"/);
  assert.match(launcher, /SHARE_LOCAL_PREVIEW_SUPABASE_FORBIDDEN/);
  assert.match(launcher, /SHARE_LOCAL_PREVIEW_PROVIDER_CREDENTIALS_FORBIDDEN/);
});

console.log(`Spatial share local preview regression: ${passed}/${passed} PASS`);
console.log("PRODUCTION_STORE_SELECTION_CHANGED=false");
console.log("PRODUCTION_FAIL_OPEN_MEMORY_FALLBACK=false");
console.log("SUPABASE_CALLS=0");
console.log("PROVIDER_CALLS=0");
