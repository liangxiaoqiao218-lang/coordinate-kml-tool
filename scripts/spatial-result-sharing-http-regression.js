import assert from "node:assert/strict";
import fs from "node:fs";

const server = fs.readFileSync(new URL("../server.js", import.meta.url), "utf8");
const index = fs.readFileSync(new URL("../index.html", import.meta.url), "utf8");
const page = fs.readFileSync(new URL("../share-result.html", import.meta.url), "utf8");
const client = fs.readFileSync(new URL("../assets/spatial-map/spatial-share-page.js", import.meta.url), "utf8");
const shareCss = fs.readFileSync(new URL("../assets/spatial-map/spatial-share.css", import.meta.url), "utf8");
const migration = fs.readFileSync(new URL("../supabase/migrations/20260901013936_spatial_result_shares.sql", import.meta.url), "utf8");

let passed = 0;
function test(name, run) {
  run();
  passed += 1;
  console.log(`PASS SHARE-HTTP-${String(passed).padStart(2, "0")} ${name}`);
}

test("create API exists", () => assert.match(server, /app\.post\(\s*"\/api\/spatial-shares"/));
test("read API exists", () => assert.match(server, /app\.get\("\/api\/spatial-shares\/:shareId"/));
test("revoke API exists", () => assert.match(server, /app\.delete\(\s*"\/api\/spatial-shares\/:shareId"/));
test("active recipient access API exists", () => assert.match(server, /app\.post\(\s*"\/api\/spatial-shares\/:shareId\/access"/));
test("public route exists", () => assert.match(server, /app\.get\("\/s\/:shareId"/));
test("create body accepts identity and two controlled-sharing dimensions only", () => assert.match(server, /new Set\(\["resultId", "resultRevision", "geometryHash", "accessScope", "usagePermission"\]\)/));
test("public create API rejects expiry", () => assert.doesNotMatch(server.match(/const allowedFields = new Set\([^\n]+/)?.[0] || "", /expiry/));
test("client cannot supply snapshot hash", () => {
  const allowedFields = server.match(/const allowedFields = new Set\([^\n]+/)?.[0] || "";
  assert.doesNotMatch(allowedFields, /snapshotHash|snapshot_hash/);
});
test("client geometry is rejected", () => assert.match(server, /CLIENT_GEOMETRY_FORBIDDEN/));
test("source identity is revalidated", () => assert.match(server, /validateFinalizedCoordinateIdentity\(identity\)/));
test("snapshot is server derived", () => assert.match(server, /buildSharedSpatialSnapshot\(\{\s*finalizedResult: current\.result/));
test("review reason is captured server-side and never accepted from client", () => {
  assert.match(server, /captureSpatialShareReviewReason/);
  assert.match(server, /payload\.coordinateEngineV2\.review_reason/);
  assert.doesNotMatch(server.match(/const allowedFields = new Set\([^\n]+/)?.[0] || "", /reviewReason/);
});
test("manager cookie is HttpOnly Secure Strict", () => assert.match(server, /HttpOnly; Secure; SameSite=Strict/));
test("recipient cookie is HttpOnly Secure Lax and separate", () => {
  assert.match(server, /__Host-geokit_spatial_share_recipient/);
  assert.match(server, /HttpOnly; Secure; SameSite=Lax/);
});
test("management token is absent from returned URL", () => assert.doesNotMatch(server, /shareUrl[^\n]+manager|shareUrl[^\n]+capability/i));
test("create rate limit is 5 per 10 minutes per IP", () => assert.match(server, /share-create-ip:[\s\S]*windowMs: 10 \* 60 \* 1000, max: 5/));
test("create manager limit is 20 daily", () => assert.match(server, /share-create-manager:[\s\S]*windowMs: 24 \* 60 \* 60 \* 1000, max: 20/));
test("read rate limit is 60 per minute", () => assert.match(server, /share-read-ip:[\s\S]*windowMs: 60 \* 1000, max: 60/));
test("revoke rate limit is 10 per 10 minutes", () => assert.match(server, /share-revoke-ip:[\s\S]*windowMs: 10 \* 60 \* 1000, max: 10/));
test("unknown expired and revoked use generic unavailable", () => assert.match(server, /SHARE_UNAVAILABLE/g));
test("shared routes are no-store", () => assert.match(server, /setSharedSpatialResultHeaders[\s\S]*Cache-Control", "no-store/));
test("shared routes prevent referrer leakage", () => assert.match(server, /Referrer-Policy", "no-referrer/));
test("shared routes cannot be framed", () => assert.match(server, /frame-ancestors 'none'/));
test("shared routes prevent sniffing", () => assert.match(server, /X-Content-Type-Options", "nosniff/));
test("shared routes send noindex", () => assert.match(server, /X-Robots-Tag", "noindex, nofollow, noarchive/));
test("robots disallows public share paths", () => assert.match(server, /Disallow: \/s\//));
test("sitemap excludes public share paths", () => assert.doesNotMatch(server.match(/app\.get\("\/sitemap\.xml"[\s\S]*?\n\}\);/)?.[0] || "", /\/s\//));
test("page has robots metadata", () => assert.match(page, /name="robots" content="noindex,nofollow,noarchive"/));
test("fresh page fetches snapshot by path id", () => assert.match(client, /fetch\(`\/api\/spatial-shares\/\$\{shareId\}`/));
test("shared page has no Coordinate Workbench", () => assert.doesNotMatch(page, /Coordinate Workbench|coordinatePage|imageInput/));
test("shared page has no KML action", () => assert.doesNotMatch(page, /KML|downloadKml|spatialKmlAction/));
test("shared page uses provider-neutral map module", () => assert.match(page, /spatial-map-product\.js/));
test("shared page uses approved provider failure copy", () => assert.match(page, /卫星地图暂时不可用[\s\S]*重试/));
test("shared page exposes one restrained acquisition CTA", () => {
  assert.match(page, /class="shared-acquisition-link"[^>]*>识别新的坐标/);
  assert.doesNotMatch(page, /用 GeoKit Lab 识别坐标/);
  assert.doesNotMatch(page, /shareId=.*shared_spatial_result/);
});
test("shared page renders text without raw HTML", () => assert.doesNotMatch(client, /innerHTML|insertAdjacentHTML|document\.write/));
test("Coordinate Result keeps Map primary and share secondary", () => assert.match(index, /id="mapPreviewAction"[\s\S]*class="coordinate-secondary-actions"[\s\S]*id="shareResultAction"/));
test("Map Result uses compact header share action", () => assert.match(index, /id="spatialResultCard"[\s\S]*id="spatialShareCardAction"[\s\S]*id="spatialResultSheetToggle"/));
test("share sheet exposes exactly access and usage controls", () => {
  assert.match(index, /谁可以查看[\s\S]*仅接收者[\s\S]*获得链接的人[\s\S]*允许对方[\s\S]*仅查看[\s\S]*允许修改/);
  assert.doesNotMatch(index, /spatialShareExpiry|链接有效期|option value="30_days"/);
});
test("native share has clipboard fallback", () => {
  assert.match(index, /navigator\.share/);
  assert.match(index, /navigator\.clipboard\?\.writeText|execCommand\("copy"\)/);
});
test("migration enables RLS", () => assert.match(migration, /alter table public\.spatial_result_shares enable row level security/));
test("migration blocks direct anon and authenticated access", () => assert.match(migration, /revoke all on table public\.spatial_result_shares from anon, authenticated/));
test("migration makes snapshot content immutable", () => assert.match(migration, /spatial_result_share_snapshot_immutable/));
test("migration allows terminal revocation only", () => assert.match(migration, /spatial_result_share_revocation_terminal/));
test("migration stores controlled sharing and hash-only recipient binding", () => {
  assert.match(migration, /access_scope text not null/);
  assert.match(migration, /usage_permission text not null/);
  assert.match(migration, /recipient_capability_hash text/);
  assert.match(migration, /recipient_bound_at timestamptz/);
  assert.match(migration, /snapshot ->> 'accessScope' = access_scope/);
  assert.match(migration, /snapshot ->> 'usagePermission' = usage_permission/);
});
test("atomic recipient binding is implemented in database function", () => {
  assert.match(migration, /function public\.bind_spatial_share_recipient/);
  assert.match(migration, /recipient_capability_hash is null/);
  assert.match(migration, /return share_scope = 'ANYONE_WITH_LINK'/);
  assert.match(server, /spatialShareStore\.bindRecipient/);
});
test("passive share page GET cannot bind recipient", () => {
  const publicRoute = server.match(/app\.get\("\/s\/:shareId"[\s\S]*?\n\}\);/)?.[0] || "";
  assert.doesNotMatch(publicRoute, /bindRecipient|recipientCapability/);
  assert.match(client, /method: "POST"/);
  assert.match(client, /x-geokit-active-share-access/);
});
test("recipient page exposes allow-edit only from immutable permission", () => {
  assert.match(page, /id="sharedEditAction"[^>]*hidden/);
  assert.match(client, /snapshot\.usagePermission !== "ALLOW_EDIT"/);
  assert.match(client, /shared_recipient_working_copy_v1/);
  assert.match(index, /shared_recipient_working_copy_v1/);
  assert.match(shareCss, /\.shared-page-actions \[hidden\] \{ display: none; \}/);
});
test("allow-edit is the only strong task action and acquisition remains lightweight", () => {
  assert.match(page, /id="sharedEditAction"[^>]*class="shared-edit-action"/);
  assert.match(page, /class="shared-acquisition-link"/);
  assert.match(shareCss, /\.shared-edit-action[^}]*background: #0f766e/);
  assert.match(shareCss, /\.shared-acquisition-link[^}]*width: fit-content/);
  assert.doesNotMatch(shareCss.match(/\.shared-acquisition-link[^}]*}/)?.[0] || "", /background:/);
});
test("coordinate details expose one copy-all action with concise feedback", () => {
  assert.equal((page.match(/id="sharedCopyCoordinatesAction"/g) || []).length, 1);
  assert.match(client, /navigator\.clipboard\.writeText\(coordinateText\)/);
  assert.match(client, /textContent = "已复制"/);
});
test("controlled sharing has no collaboration or mining-area ownership UI", () => {
  const sharingUi = `${page}\n${client}\n${index.match(/<dialog id="spatialShareDialog"[\s\S]*?<\/dialog>/)?.[0] || ""}`;
  assert.doesNotMatch(sharingUi, /协作|成员|同步|邀请|领取|保存到我的矿地/);
});
test("snapshot hash gate is persisted and immutable", () => {
  assert.match(migration, /snapshot_hash text not null check \(snapshot_hash ~ '\^\[a-f0-9\]\{64\}\$'\)/);
  assert.match(migration, /new\.snapshot_hash is distinct from old\.snapshot_hash/);
  assert.match(server, /buildSharedSpatialSnapshot/);
});
test("migration indexes manager capability hash", () => assert.match(migration, /create index if not exists spatial_result_shares_manager_capability_hash_idx[\s\S]*\(manager_capability_hash\)/));
test("migration indexes creation time", () => assert.match(migration, /create index if not exists spatial_result_shares_created_at_idx[\s\S]*\(created_at\)/));
test("no public sharing directory or analytics exists", () => assert.doesNotMatch(`${server}\n${page}\n${client}`, /share-directory|shareAnalytics|trackEvent\(/i));
test("no query-string management secret exists", () => assert.doesNotMatch(`${server}\n${client}`, /[?&](token|secret|capability)=/i));

console.log(`Spatial result sharing HTTP regression: ${passed}/${passed} PASS`);
console.log("AUTHORITY_MUTATION_COUNT=0");
console.log("PROVIDER_CALLS=0");
