import {
  SHARE_ACCESS_SCOPE,
  computeSharedSpatialSnapshotHash
} from "./shared-spatial-result-v1.js";

function verifySnapshotHash(snapshot, persistedHash = snapshot?.snapshotHash) {
  const expected = computeSharedSpatialSnapshotHash(snapshot);
  if (!/^[a-f0-9]{64}$/.test(String(persistedHash || "")) || persistedHash !== snapshot?.snapshotHash || persistedHash !== expected) {
    throw Object.assign(new Error("SHARE_SNAPSHOT_HASH_MISMATCH"), { code: "SHARE_SNAPSHOT_HASH_MISMATCH" });
  }
  return expected;
}

export class SupabaseSpatialShareStore {
  constructor({ supabase, table = "spatial_result_shares", clock = () => new Date() } = {}) {
    this.supabase = supabase;
    this.table = table;
    this.clock = clock;
  }

  isConfigured() {
    return Boolean(this.supabase);
  }

  async create({ snapshot, managerCapabilityHash }) {
    if (!this.supabase) throw Object.assign(new Error("SHARE_STORE_UNAVAILABLE"), { code: "SHARE_STORE_UNAVAILABLE" });
    verifySnapshotHash(snapshot);
    const row = {
      share_id: snapshot.shareId,
      schema_version: snapshot.schemaVersion,
      source_result_id: snapshot.source.resultId,
      source_result_revision: snapshot.source.resultRevision,
      source_geometry_hash: snapshot.source.sourceGeometryHash,
      manager_capability_hash: managerCapabilityHash,
      access_scope: snapshot.accessScope,
      usage_permission: snapshot.usagePermission,
      recipient_capability_hash: null,
      recipient_bound_at: null,
      snapshot_hash: snapshot.snapshotHash,
      snapshot,
      snapshot_bytes: snapshot.snapshotBytes,
      vertex_count: snapshot.vertexCount,
      expires_at: snapshot.expiresAt
    };
    const { error } = await this.supabase.from(this.table).insert(row);
    if (error) throw Object.assign(new Error("SHARE_STORE_CREATE_FAILED"), { code: "SHARE_STORE_CREATE_FAILED", cause: error });
    return Object.freeze({ snapshot, managerCapabilityHash });
  }

  async findActive(shareId) {
    if (!this.supabase) throw Object.assign(new Error("SHARE_STORE_UNAVAILABLE"), { code: "SHARE_STORE_UNAVAILABLE" });
    const { data, error } = await this.supabase
      .from(this.table)
      .select("share_id,snapshot,snapshot_hash,manager_capability_hash,access_scope,usage_permission,recipient_capability_hash,recipient_bound_at,expires_at,revoked_at")
      .eq("share_id", shareId)
      .maybeSingle();
    if (error) throw Object.assign(new Error("SHARE_STORE_READ_FAILED"), { code: "SHARE_STORE_READ_FAILED", cause: error });
    if (!data || data.revoked_at) return null;
    if (data.expires_at && new Date(data.expires_at).getTime() <= this.clock().getTime()) return null;
    verifySnapshotHash(data.snapshot, data.snapshot_hash);
    if (data.access_scope !== data.snapshot.accessScope || data.usage_permission !== data.snapshot.usagePermission) {
      throw Object.assign(new Error("SHARE_PERMISSION_BINDING_MISMATCH"), { code: "SHARE_PERMISSION_BINDING_MISMATCH" });
    }
    return Object.freeze({
      snapshot: data.snapshot,
      managerCapabilityHash: data.manager_capability_hash,
      recipientCapabilityHash: data.recipient_capability_hash || null,
      recipientBoundAt: data.recipient_bound_at || null
    });
  }

  async findAuthorized(shareId, recipientCapabilityHash = "") {
    const record = await this.findActive(shareId);
    if (!record) return null;
    if (record.snapshot.accessScope === SHARE_ACCESS_SCOPE.ANYONE_WITH_LINK) return record;
    if (!recipientCapabilityHash || record.recipientCapabilityHash !== recipientCapabilityHash) return null;
    return record;
  }

  async bindRecipient(shareId, recipientCapabilityHash) {
    if (!this.supabase) throw Object.assign(new Error("SHARE_STORE_UNAVAILABLE"), { code: "SHARE_STORE_UNAVAILABLE" });
    if (!/^[a-f0-9]{64}$/.test(String(recipientCapabilityHash || ""))) {
      throw Object.assign(new Error("SHARE_RECIPIENT_CAPABILITY_INVALID"), { code: "SHARE_RECIPIENT_CAPABILITY_INVALID" });
    }
    const { data, error } = await this.supabase.rpc("bind_spatial_share_recipient", {
      p_share_id: shareId,
      p_recipient_capability_hash: recipientCapabilityHash
    });
    if (error) throw Object.assign(new Error("SHARE_RECIPIENT_BIND_FAILED"), { code: "SHARE_RECIPIENT_BIND_FAILED", cause: error });
    if (data !== true) return null;
    return this.findAuthorized(shareId, recipientCapabilityHash);
  }

  async revoke(shareId, managerCapabilityHash) {
    if (!this.supabase) throw Object.assign(new Error("SHARE_STORE_UNAVAILABLE"), { code: "SHARE_STORE_UNAVAILABLE" });
    const revokedAt = this.clock().toISOString();
    const { data, error } = await this.supabase
      .from(this.table)
      .update({ revoked_at: revokedAt })
      .eq("share_id", shareId)
      .eq("manager_capability_hash", managerCapabilityHash)
      .is("revoked_at", null)
      .select("share_id")
      .maybeSingle();
    if (error) throw Object.assign(new Error("SHARE_STORE_REVOKE_FAILED"), { code: "SHARE_STORE_REVOKE_FAILED", cause: error });
    return Boolean(data?.share_id);
  }
}
