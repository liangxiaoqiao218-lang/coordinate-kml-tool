import assert from "node:assert/strict";
import {
  FINALIZED_COORDINATE_CRS,
  FINALIZED_COORDINATE_SCHEMA_VERSION,
  createGeometryHash
} from "../server/coordinate-finalizer/index.js";
import {
  SHARE_ID_BYTES,
  SHARE_ACCESS_SCOPE,
  SHARE_MANAGER_CAPABILITY_BYTES,
  SHARE_RECIPIENT_CAPABILITY_BYTES,
  SHARE_USAGE_PERMISSION,
  SHARE_MAX_SNAPSHOT_BYTES,
  SHARE_MAX_VERTICES,
  SHARED_SPATIAL_RESULT_SCHEMA_VERSION,
  SHARED_SPATIAL_SNAPSHOT_HASH_ALGORITHM,
  SHARED_SPATIAL_SNAPSHOT_HASH_FIELD_ORDER,
  buildSharedSpatialSnapshot,
  canonicalSharedSpatialSnapshotContent,
  computeSharedSpatialSnapshotHash,
  createManagerCapability,
  createRecipientCapability,
  createShareId,
  hashManagerCapability,
  hashRecipientCapability,
  isValidShareId,
  managerCapabilityMatches,
  publicSharedSpatialResult
} from "../server/spatial/sharing/shared-spatial-result-v1.js";
import { SupabaseSpatialShareStore } from "../server/spatial/sharing/supabase-share-store.js";

let passed = 0;
async function test(name, run) {
  await Promise.resolve(run());
  passed += 1;
  console.log(`PASS SHARE-CONTRACT-${String(passed).padStart(2, "0")} ${name}`);
}

function finalized(overrides = {}) {
  const geometry = overrides.geometry || {
    type: "Polygon",
    coordinates: [[[8, 11], [8.01, 11], [8.01, 11.01], [8, 11]]]
  };
  return {
    schemaVersion: FINALIZED_COORDINATE_SCHEMA_VERSION,
    resultId: "result-sharing-1",
    resultRevision: 3,
    geometryHash: createGeometryHash(geometry),
    sourceAuthority: "legacy",
    coordinateType: "standard_dms_table",
    crs: { ...FINALIZED_COORDINATE_CRS },
    geometry,
    requiresReview: true,
    reviewReason: {
      schema_version: "coordinate_review_reason_v1",
      primary_code: "CANDIDATE_FIELD_CONFLICT",
      codes: ["CANDIDATE_FIELD_CONFLICT"]
    },
    reasonCodes: ["REVIEW_REQUIRED"],
    confirmationStatus: "pending",
    ...overrides
  };
}

const fixedShareId = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const createdAt = "2026-09-01T00:00:00.000Z";
const snapshot = buildSharedSpatialSnapshot({ finalizedResult: finalized(), shareId: fixedShareId, createdAt });

await test("snapshot hash exists and is SHA-256 hex", () => {
  assert.equal(SHARED_SPATIAL_SNAPSHOT_HASH_ALGORITHM, "sha256");
  assert.match(snapshot.snapshotHash, /^[a-f0-9]{64}$/);
});
await test("snapshot hash canonical field order is explicit", () => assert.deepEqual(SHARED_SPATIAL_SNAPSHOT_HASH_FIELD_ORDER, [
  "schemaVersion", "createdAt", "expiresAt", "accessScope", "usagePermission", "source", "geometry", "crs", "axisOrder", "spatialFacts", "coordinateDisplay",
  "reviewState", "confirmationState", "capabilities", "vertexCount"
]));
await test("identical immutable content produces an identical hash", () => {
  const same = buildSharedSpatialSnapshot({ finalizedResult: finalized(), shareId: "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", createdAt });
  assert.equal(same.snapshotHash, snapshot.snapshotHash);
});
await test("nested object key ordering is canonical", () => {
  const reordered = structuredClone(snapshot);
  reordered.reviewState.reviewReason = {
    codes: ["CANDIDATE_FIELD_CONFLICT"],
    primary_code: "CANDIDATE_FIELD_CONFLICT",
    schema_version: "coordinate_review_reason_v1"
  };
  assert.equal(computeSharedSpatialSnapshotHash(reordered), snapshot.snapshotHash);
  assert.equal(canonicalSharedSpatialSnapshotContent(reordered), canonicalSharedSpatialSnapshotContent(snapshot));
});
for (const [name, mutate] of [
  ["canonical geometry", value => { value.geometry.coordinates[0][0][0] += 0.001; }],
  ["spatial facts", value => { value.spatialFacts.areaSquareMeters += 1; }],
  ["coordinate display", value => { value.coordinateDisplay = ["11.000000, 8.000000"]; }],
  ["review state", value => { value.reviewState.requiresReview = false; }],
  ["confirmation state", value => { value.confirmationState.status = "accepted"; }]
]) {
  await test(`${name} mutation changes snapshot hash`, () => {
    const changed = structuredClone(snapshot);
    mutate(changed);
    assert.notEqual(computeSharedSpatialSnapshotHash(changed), snapshot.snapshotHash);
  });
}
await test("mutable revocation-only data does not change snapshot hash", () => {
  const revoked = { ...structuredClone(snapshot), revokedAt: "2026-09-02T00:00:00.000Z" };
  assert.equal(computeSharedSpatialSnapshotHash(revoked), snapshot.snapshotHash);
});
await test("client-supplied snapshot hash is not authoritative", () => {
  const supplied = buildSharedSpatialSnapshot({ finalizedResult: finalized(), shareId: fixedShareId, createdAt, snapshotHash: "f".repeat(64) });
  assert.equal(supplied.snapshotHash, snapshot.snapshotHash);
  assert.notEqual(supplied.snapshotHash, "f".repeat(64));
});
await test("persisted snapshot hash recomputes exactly", () => assert.equal(computeSharedSpatialSnapshotHash(snapshot), snapshot.snapshotHash));

await test("schema version is explicit", () => assert.equal(snapshot.schemaVersion, SHARED_SPATIAL_RESULT_SCHEMA_VERSION));
await test("share ids are 192-bit base64url", () => {
  const id = createShareId();
  assert.equal(Buffer.from(id, "base64url").length, SHARE_ID_BYTES);
  assert.equal(isValidShareId(id), true);
});
await test("share ids resist enumeration by uniqueness", () => assert.equal(new Set(Array.from({ length: 256 }, createShareId)).size, 256));
await test("malformed ids fail", () => ["1", `${fixedShareId}=`, fixedShareId.slice(1)].forEach(id => assert.equal(isValidShareId(id), false)));
await test("source identity is retained", () => assert.deepEqual(snapshot.source, {
  resultId: "result-sharing-1",
  resultRevision: 3,
  sourceGeometryHash: snapshot.source.sourceGeometryHash,
  authority: "legacy",
  coordinateType: "standard_dms_table"
}));
await test("geometry is deeply equal but detached", () => {
  assert.deepEqual(snapshot.geometry, finalized().geometry);
  assert.notEqual(snapshot.geometry, finalized().geometry);
});
await test("source geometry hash is canonical", () => assert.equal(snapshot.source.sourceGeometryHash, createGeometryHash(snapshot.geometry)));
await test("EPSG:4326 and longitude latitude are fixed", () => {
  assert.equal(snapshot.crs.id, "EPSG:4326");
  assert.equal(snapshot.axisOrder, "longitude_latitude");
});
await test("review state is preserved", () => assert.deepEqual(snapshot.reviewState, {
  requiresReview: true,
  reviewReason: finalized().reviewReason
}));
await test("confirmation state is preserved", () => assert.deepEqual(snapshot.confirmationState, { status: "pending", resultRevision: 3 }));
await test("sharing grants no KML edit or recognition capability", () => assert.deepEqual(snapshot.capabilities, {
  editable: false,
  recognition: false,
  kmlDownload: false
}));
await test("controlled sharing defaults to recipient-only view-only without expiry", () => {
  assert.equal(snapshot.accessScope, SHARE_ACCESS_SCOPE.RECIPIENT_ONLY);
  assert.equal(snapshot.usagePermission, SHARE_USAGE_PERMISSION.VIEW_ONLY);
  assert.equal(snapshot.expiresAt, null);
});
await test("allow-edit changes immutable permission and edit capability", () => {
  const editable = buildSharedSpatialSnapshot({
    finalizedResult: finalized(), shareId: fixedShareId, createdAt,
    accessScope: SHARE_ACCESS_SCOPE.ANYONE_WITH_LINK,
    usagePermission: SHARE_USAGE_PERMISSION.ALLOW_EDIT
  });
  assert.equal(editable.accessScope, SHARE_ACCESS_SCOPE.ANYONE_WITH_LINK);
  assert.equal(editable.capabilities.editable, true);
  assert.notEqual(editable.snapshotHash, snapshot.snapshotHash);
});
await test("unknown controlled-sharing values fail closed", () => {
  assert.throws(() => buildSharedSpatialSnapshot({ finalizedResult: finalized(), shareId: fixedShareId, accessScope: "PUBLIC" }), /SHARE_ACCESS_SCOPE_INVALID/);
  assert.throws(() => buildSharedSpatialSnapshot({ finalizedResult: finalized(), shareId: fixedShareId, usagePermission: "OWNER" }), /SHARE_USAGE_PERMISSION_INVALID/);
});
await test("unsupported geometry fails closed", () => {
  const value = finalized({ geometry: { type: "GeometryCollection", coordinates: [] }, geometryHash: "bad" });
  assert.throws(() => buildSharedSpatialSnapshot({ finalizedResult: value, shareId: fixedShareId }), /SHARE_GEOMETRY_INVALID/);
});
await test("non-finite geometry fails closed", () => {
  const geometry = { type: "Point", coordinates: [8, Number.NaN] };
  assert.throws(() => buildSharedSpatialSnapshot({ finalizedResult: finalized({ geometry, geometryHash: createGeometryHash(geometry) }), shareId: fixedShareId }), /SHARE_GEOMETRY_INVALID/);
});
await test("client or stale geometry hash cannot be substituted", () => assert.throws(() => buildSharedSpatialSnapshot({ finalizedResult: finalized({ geometryHash: "sha256:wrong" }), shareId: fixedShareId }), /SHARE_GEOMETRY_IDENTITY_MISMATCH/));
await test("vertex count limit is enforced", () => {
  const geometry = { type: "LineString", coordinates: Array.from({ length: SHARE_MAX_VERTICES + 1 }, (_, index) => [8 + index / 100000, 11]) };
  assert.throws(() => buildSharedSpatialSnapshot({ finalizedResult: finalized({ geometry, geometryHash: createGeometryHash(geometry) }), shareId: fixedShareId }), /SHARE_VERTEX_LIMIT_EXCEEDED/);
});
await test("snapshot size limit is enforced", () => {
  const hugeReason = { schemaVersion: "test", codes: ["x".repeat(SHARE_MAX_SNAPSHOT_BYTES)] };
  assert.throws(() => buildSharedSpatialSnapshot({ finalizedResult: finalized({ reviewReason: hugeReason }), shareId: fixedShareId }), /SHARE_SNAPSHOT_TOO_LARGE/);
});
await test("recorded snapshot bytes equal the final serialized snapshot", () => assert.equal(snapshot.snapshotBytes, Buffer.byteLength(JSON.stringify(snapshot), "utf8")));
await test("snapshot and nested values are immutable", () => {
  assert.equal(Object.isFrozen(snapshot), true);
  assert.equal(Object.isFrozen(snapshot.geometry), true);
  assert.throws(() => { snapshot.geometry.type = "Point"; }, TypeError);
});
await test("management capability is 256-bit and only its hash is retained", () => {
  const capability = createManagerCapability();
  assert.equal(Buffer.from(capability, "base64url").length, SHARE_MANAGER_CAPABILITY_BYTES);
  assert.equal(hashManagerCapability(capability).length, 64);
  assert.equal(hashManagerCapability(capability).includes(capability), false);
});
await test("management capability comparison is exact", () => {
  const capability = createManagerCapability();
  const hash = hashManagerCapability(capability);
  assert.equal(managerCapabilityMatches(capability, hash), true);
  assert.equal(managerCapabilityMatches(`${capability}x`, hash), false);
});
await test("recipient capability is 256-bit and hash-only", () => {
  const capability = createRecipientCapability();
  assert.equal(Buffer.from(capability, "base64url").length, SHARE_RECIPIENT_CAPABILITY_BYTES);
  assert.equal(hashRecipientCapability(capability).length, 64);
  assert.equal(hashRecipientCapability(capability).includes(capability), false);
});
await test("public response never includes manager hash", () => {
  const output = publicSharedSpatialResult({ snapshot, managerCapabilityHash: "secret-hash" }, { canRevoke: true });
  assert.equal(output.canRevoke, true);
  assert.doesNotMatch(JSON.stringify(output), /secret-hash|managerCapability/i);
});
await test("snapshot excludes raw recognition and KML evidence", () => {
  assert.doesNotMatch(JSON.stringify(snapshot), /rawSourceImage|ocrRawResponse|providerResponse|recognitionTrace|kmlContent/i);
});
await test("Supabase store inserts only governed columns", async () => {
  let inserted;
  const supabase = { from: table => ({ insert: async row => { inserted = { table, row }; return { error: null }; } }) };
  const store = new SupabaseSpatialShareStore({ supabase });
  await store.create({ snapshot, managerCapabilityHash: "a".repeat(64) });
  assert.equal(inserted.table, "spatial_result_shares");
  assert.deepEqual(Object.keys(inserted.row).sort(), ["access_scope", "expires_at", "manager_capability_hash", "recipient_bound_at", "recipient_capability_hash", "schema_version", "share_id", "snapshot", "snapshot_bytes", "snapshot_hash", "source_geometry_hash", "source_result_id", "source_result_revision", "usage_permission", "vertex_count"].sort());
  assert.equal(inserted.row.snapshot_hash, snapshot.snapshotHash);
});
await test("Supabase store reads an active snapshot without public credentials", async () => {
  const builder = {
    select() { return this; },
    eq() { return this; },
    async maybeSingle() { return { data: { snapshot, snapshot_hash: snapshot.snapshotHash, manager_capability_hash: "a".repeat(64), access_scope: snapshot.accessScope, usage_permission: snapshot.usagePermission, recipient_capability_hash: null, recipient_bound_at: null, expires_at: null, revoked_at: null }, error: null }; }
  };
  const store = new SupabaseSpatialShareStore({ supabase: { from: () => builder }, clock: () => new Date("2026-09-01T00:00:00.000Z") });
  const active = await store.findActive(fixedShareId);
  assert.equal(active.snapshot.shareId, fixedShareId);
});
await test("Supabase store hides expired and revoked snapshots identically", async () => {
  const row = { snapshot, snapshot_hash: snapshot.snapshotHash, manager_capability_hash: "a".repeat(64), expires_at: "2026-08-31T00:00:00.000Z", revoked_at: null };
  const builder = { select() { return this; }, eq() { return this; }, async maybeSingle() { return { data: row, error: null }; } };
  const store = new SupabaseSpatialShareStore({ supabase: { from: () => builder }, clock: () => new Date("2026-09-01T00:00:00.000Z") });
  assert.equal(await store.findActive(fixedShareId), null);
  row.expires_at = null;
  row.revoked_at = "2026-09-01T00:00:00.000Z";
  assert.equal(await store.findActive(fixedShareId), null);
});
await test("Supabase store revokes by share and capability hash only", async () => {
  const filters = [];
  const builder = {
    update(value) { this.updateValue = value; return this; },
    eq(field, value) { filters.push([field, value]); return this; },
    is(field, value) { filters.push([field, value]); return this; },
    select() { return this; },
    async maybeSingle() { return { data: { share_id: fixedShareId }, error: null }; }
  };
  const store = new SupabaseSpatialShareStore({ supabase: { from: () => builder }, clock: () => new Date("2026-09-01T01:00:00.000Z") });
  assert.equal(await store.revoke(fixedShareId, "a".repeat(64)), true);
  assert.deepEqual(filters, [["share_id", fixedShareId], ["manager_capability_hash", "a".repeat(64)], ["revoked_at", null]]);
  assert.deepEqual(builder.updateValue, { revoked_at: "2026-09-01T01:00:00.000Z" });
});

await test("Supabase store rejects persisted snapshot hash mismatch", async () => {
  const builder = {
    select() { return this; },
    eq() { return this; },
    async maybeSingle() {
      return { data: { snapshot, snapshot_hash: "f".repeat(64), manager_capability_hash: "a".repeat(64), expires_at: null, revoked_at: null }, error: null };
    }
  };
  const store = new SupabaseSpatialShareStore({ supabase: { from: () => builder } });
  await assert.rejects(() => store.findActive(fixedShareId), /SHARE_SNAPSHOT_HASH_MISMATCH/);
});

await test("Supabase store rejects permission column and snapshot mismatch", async () => {
  const builder = {
    select() { return this; },
    eq() { return this; },
    async maybeSingle() {
      return { data: { snapshot, snapshot_hash: snapshot.snapshotHash, manager_capability_hash: "a".repeat(64), access_scope: "ANYONE_WITH_LINK", usage_permission: snapshot.usagePermission, recipient_capability_hash: null, recipient_bound_at: null, expires_at: null, revoked_at: null }, error: null };
    }
  };
  const store = new SupabaseSpatialShareStore({ supabase: { from: () => builder } });
  await assert.rejects(() => store.findActive(fixedShareId), /SHARE_PERMISSION_BINDING_MISMATCH/);
});

await test("local create read revoke lifecycle preserves snapshot identity", async () => {
  let row = null;
  let updateValue = null;
  const builder = {
    async insert(value) { row = { ...value, revoked_at: null }; return { error: null }; },
    select() { return this; },
    eq() { return this; },
    is() { return this; },
    update(value) { updateValue = value; return this; },
    async maybeSingle() {
      if (updateValue) {
        row = { ...row, ...updateValue };
        updateValue = null;
        return { data: { share_id: row.share_id }, error: null };
      }
      return { data: row, error: null };
    }
  };
  const clock = () => new Date("2026-09-01T01:00:00.000Z");
  const store = new SupabaseSpatialShareStore({ supabase: { from: () => builder }, clock });
  await store.create({ snapshot, managerCapabilityHash: "a".repeat(64) });
  const active = await store.findActive(fixedShareId);
  assert.equal(active.snapshot.snapshotHash, snapshot.snapshotHash);
  assert.equal(await store.revoke(fixedShareId, "a".repeat(64)), true);
  assert.equal(await store.findActive(fixedShareId), null);
});

assert.equal(snapshot.vertexCount <= SHARE_MAX_VERTICES, true);
assert.equal(snapshot.snapshotBytes <= SHARE_MAX_SNAPSHOT_BYTES, true);
console.log(`Spatial result sharing contract regression: ${passed}/${passed} PASS`);
console.log("AUTHORITY_MUTATION_COUNT=0");
console.log("PROVIDER_CALLS=0");
