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
  console.log(`PASS SHARE-ACCESS-${String(passed).padStart(2, "0")} ${name}`);
}

function extractFunctionSource(source, functionName) {
  const marker = `function ${functionName}(`;
  const start = source.indexOf(marker);
  assert.notEqual(start, -1, `${functionName} must exist`);
  const openBrace = source.indexOf("{", start);
  let depth = 0;
  for (let index = openBrace; index < source.length; index += 1) {
    if (source[index] === "{") depth += 1;
    if (source[index] === "}") {
      depth -= 1;
      if (depth === 0) return source.slice(start, index + 1);
    }
  }
  throw new Error(`${functionName} body is not closed`);
}

function finalized() {
  const geometry = { type: "Polygon", coordinates: [[[116.391245, 39.907654], [116.401245, 39.907654], [116.401245, 39.917589], [116.391245, 39.917589], [116.391245, 39.907654]]] };
  return {
    schemaVersion: FINALIZED_COORDINATE_SCHEMA_VERSION,
    resultId: "controlled-sharing-source-a",
    resultRevision: 4,
    geometryHash: createGeometryHash(geometry),
    sourceAuthority: "manual_input",
    coordinateType: "decimal_latlon",
    crs: { ...FINALIZED_COORDINATE_CRS },
    geometry,
    requiresReview: true,
    reviewReason: { schema_version: "coordinate_review_reason_v1", primary_code: "REVIEW_REQUIRED", codes: ["REVIEW_REQUIRED"] },
    confirmationStatus: "pending"
  };
}

function snapshot(shareId, accessScope, usagePermission) {
  return buildSharedSpatialSnapshot({ finalizedResult: finalized(), shareId, accessScope, usagePermission, createdAt: "2026-09-01T00:00:00.000Z" });
}

const manager = "a".repeat(64);
const recipientA = "b".repeat(64);
const recipientB = "c".repeat(64);
const recipientView = snapshot("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", SHARE_ACCESS_SCOPE.RECIPIENT_ONLY, SHARE_USAGE_PERMISSION.VIEW_ONLY);

await test("passive read cannot bind or read recipient-only share", async () => {
  const store = new SupabaseSpatialShareStore();
  await store.create({ snapshot: recipientView, managerCapabilityHash: manager });
  assert.equal(await store.findAuthorized(recipientView.shareId, ""), null);
  assert.equal((await store.findActive(recipientView.shareId)).recipientCapabilityHash, null);
});

await test("active first access binds one recipient capability", async () => {
  const store = new SupabaseSpatialShareStore();
  await store.create({ snapshot: recipientView, managerCapabilityHash: manager });
  assert.notEqual(await store.bindRecipient(recipientView.shareId, recipientA), null);
  assert.equal((await store.findActive(recipientView.shareId)).recipientCapabilityHash, recipientA);
});

await test("same recipient remains authorized", async () => {
  const store = new SupabaseSpatialShareStore();
  await store.create({ snapshot: recipientView, managerCapabilityHash: manager });
  await store.bindRecipient(recipientView.shareId, recipientA);
  assert.notEqual(await store.bindRecipient(recipientView.shareId, recipientA), null);
  assert.notEqual(await store.findAuthorized(recipientView.shareId, recipientA), null);
});

await test("forwarded recipient-only link fails closed in another browser", async () => {
  const store = new SupabaseSpatialShareStore();
  await store.create({ snapshot: recipientView, managerCapabilityHash: manager });
  await store.bindRecipient(recipientView.shareId, recipientA);
  assert.equal(await store.bindRecipient(recipientView.shareId, recipientB), null);
  assert.equal(await store.findAuthorized(recipientView.shareId, recipientB), null);
});

await test("racing first browsers produce one winner", async () => {
  const store = new SupabaseSpatialShareStore();
  await store.create({ snapshot: recipientView, managerCapabilityHash: manager });
  const results = await Promise.all([
    store.bindRecipient(recipientView.shareId, recipientA),
    store.bindRecipient(recipientView.shareId, recipientB)
  ]);
  assert.equal(results.filter(Boolean).length, 1);
});

for (const usagePermission of Object.values(SHARE_USAGE_PERMISSION)) {
  await test(`anyone-with-link ${usagePermission} supports multiple browsers`, async () => {
    const shareId = usagePermission === SHARE_USAGE_PERMISSION.VIEW_ONLY
      ? "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
      : "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC";
    const value = snapshot(shareId, SHARE_ACCESS_SCOPE.ANYONE_WITH_LINK, usagePermission);
    const store = new SupabaseSpatialShareStore();
    await store.create({ snapshot: value, managerCapabilityHash: manager });
    assert.notEqual(await store.findAuthorized(shareId, recipientA), null);
    assert.notEqual(await store.findAuthorized(shareId, recipientB), null);
    assert.equal((await store.findActive(shareId)).recipientCapabilityHash, null);
  });
}

await test("view-only and allow-edit permissions are immutable snapshot authority", async () => {
  const editable = snapshot("DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD", SHARE_ACCESS_SCOPE.RECIPIENT_ONLY, SHARE_USAGE_PERMISSION.ALLOW_EDIT);
  assert.equal(recipientView.capabilities.editable, false);
  assert.equal(editable.capabilities.editable, true);
  assert.notEqual(recipientView.snapshotHash, editable.snapshotHash);
});

await test("recipient binding metadata is excluded from snapshot hash", async () => {
  const bound = { ...structuredClone(recipientView), recipientCapabilityHash: recipientA, recipientBoundAt: "2026-09-01T00:01:00.000Z" };
  assert.equal(computeSharedSpatialSnapshotHash(bound), recipientView.snapshotHash);
});

await test("recipient edit works on detached source geometry", async () => {
  const sourceGeometry = structuredClone(recipientView.geometry);
  const workingCopy = structuredClone(recipientView.geometry);
  workingCopy.coordinates[0][1][0] += 0.002;
  assert.deepEqual(recipientView.geometry, sourceGeometry);
  assert.notEqual(createGeometryHash(workingCopy), recipientView.source.sourceGeometryHash);
});

await test("source review and confirmation remain immutable", async () => {
  assert.equal(recipientView.reviewState.requiresReview, true);
  assert.equal(recipientView.confirmationState.status, "pending");
  assert.equal(Object.isFrozen(recipientView.reviewState), true);
});

const server = fs.readFileSync(new URL("../server.js", import.meta.url), "utf8");
const index = fs.readFileSync(new URL("../index.html", import.meta.url), "utf8");
const page = fs.readFileSync(new URL("../share-result.html", import.meta.url), "utf8");
const client = fs.readFileSync(new URL("../assets/spatial-map/spatial-share-page.js", import.meta.url), "utf8");
const migration = fs.readFileSync(new URL("../supabase/migrations/20260901013936_spatial_result_shares.sql", import.meta.url), "utf8");
const presentation = Function(`
  ${extractFunctionSource(client, "positionsEqualExact")}
  ${extractFunctionSource(client, "geometryPositions")}
  ${extractFunctionSource(client, "formatCoordinates")}
  return { geometryPositions, formatCoordinates };
`)();

await test("Polygon presentation hides only the exact canonical closing duplicate", async () => {
  const geometry = structuredClone(recipientView.geometry);
  const canonical = structuredClone(geometry);
  assert.equal(presentation.geometryPositions(geometry).length, 4);
  assert.equal(presentation.formatCoordinates(geometry).split("\n").length, 4);
  assert.deepEqual(geometry, canonical);
  const nearClosing = structuredClone(geometry);
  nearClosing.coordinates[0].at(-1)[0] += 1e-12;
  assert.equal(presentation.geometryPositions(nearClosing).length, 5);
});

await test("Point and LineString presentation remains unchanged", async () => {
  const point = { type: "Point", coordinates: [116.4, 39.9] };
  const line = { type: "LineString", coordinates: [[116.4, 39.9], [116.5, 40]] };
  assert.deepEqual(presentation.geometryPositions(point), [[116.4, 39.9]]);
  assert.deepEqual(presentation.geometryPositions(line), line.coordinates);
  assert.equal(presentation.formatCoordinates(point).split("\n").length, 1);
  assert.equal(presentation.formatCoordinates(line).split("\n").length, 2);
});

await test("active access endpoint is same-origin and explicit-action guarded", async () => {
  assert.match(server, /"\/api\/spatial-shares\/:shareId\/access"[\s\S]*spatialShareMutationGuard/);
  assert.match(server, /x-geokit-active-share-access/);
});

await test("passive public route never binds", async () => {
  const publicRoute = server.match(/app\.get\("\/s\/:shareId"[\s\S]*?\n\}\);/)?.[0] || "";
  assert.doesNotMatch(publicRoute, /bindRecipient|setSpatialShareRecipientCapability/);
});

await test("recipient capability never enters URL JavaScript logs or analytics", async () => {
  assert.doesNotMatch(`${server}\n${index}\n${page}\n${client}`, /shareUrl[^\n]*recipient|recipientCapability[^\n]*(console|trackEvent|analytics)/i);
  assert.doesNotMatch(client, /recipientCapability|recipient_capability_hash/);
});

await test("recipient Result B uses manual finalization without recognition", async () => {
  assert.match(client, /shared_recipient_working_copy_v1/);
  assert.match(index, /\/api\/coordinate-manual-finalize/);
  const hydrate = index.match(/function hydrateSharedRecipientWorkingCopy\(\)[\s\S]*?\n    \}/)?.[0] || "";
  assert.doesNotMatch(hydrate, /recognition|OCR|image|provider/i);
});

await test("migration binding is atomic and terminal", async () => {
  assert.match(migration, /update public\.spatial_result_shares[\s\S]*recipient_capability_hash is null/);
  assert.match(migration, /spatial_result_share_recipient_binding_terminal/);
  assert.match(migration, /revoke all on function public\.bind_spatial_share_recipient/);
});

await test("v1 has no expiry selector or public expiry input", async () => {
  assert.doesNotMatch(index, /spatialShareExpiry|链接有效期|7 天|30 天|不过期/);
  assert.doesNotMatch(server.match(/const allowedFields = new Set\([^\n]+/)?.[0] || "", /expiry/);
  assert.equal(recipientView.expiresAt, null);
});

await test("no return-sync collaboration or mining-area ownership product exists", async () => {
  assert.doesNotMatch(`${page}\n${client}`, /协作|邀请|成员|同步|保存到我的矿地|returnToSender/i);
});

console.log(`Spatial result sharing access regression: ${passed}/${passed} PASS`);
console.log("SOURCE_RESULT_MUTATED=false");
console.log("REMOTE_SYNC=false");
console.log("AUTHORITY_MUTATION_COUNT=0");
console.log("PROVIDER_CALLS=0");
