import crypto from "node:crypto";
import {
  SHARE_ACCESS_SCOPE,
  computeSharedSpatialSnapshotHash
} from "../server/spatial/sharing/shared-spatial-result-v1.js";

function clone(value) {
  return structuredClone(value);
}

function deepFreeze(value) {
  if (!value || typeof value !== "object" || Object.isFrozen(value)) return value;
  Object.values(value).forEach(deepFreeze);
  return Object.freeze(value);
}

function hashMatches(actual, expected) {
  if (!/^[a-f0-9]{64}$/.test(String(actual || "")) || !/^[a-f0-9]{64}$/.test(String(expected || ""))) return false;
  return crypto.timingSafeEqual(Buffer.from(actual, "hex"), Buffer.from(expected, "hex"));
}

function verifySnapshot(snapshot) {
  const recomputed = computeSharedSpatialSnapshotHash(snapshot);
  if (!hashMatches(snapshot?.snapshotHash, recomputed)) {
    throw Object.assign(new Error("SHARE_SNAPSHOT_HASH_MISMATCH"), { code: "SHARE_SNAPSHOT_HASH_MISMATCH" });
  }
}

export class SupabaseSpatialShareStore {
  #records = new Map();

  constructor({ clock = () => new Date() } = {}) {
    this.clock = clock;
  }

  isConfigured() {
    return true;
  }

  async create({ snapshot, managerCapabilityHash }) {
    verifySnapshot(snapshot);
    if (!/^[a-f0-9]{64}$/.test(String(managerCapabilityHash || "")) || this.#records.has(snapshot.shareId)) {
      throw Object.assign(new Error("SHARE_STORE_CREATE_FAILED"), { code: "SHARE_STORE_CREATE_FAILED" });
    }
    const immutableSnapshot = deepFreeze(clone(snapshot));
    this.#records.set(snapshot.shareId, {
      snapshot: immutableSnapshot,
      snapshotHash: immutableSnapshot.snapshotHash,
      managerCapabilityHash,
      recipientCapabilityHash: null,
      recipientBoundAt: null,
      revokedAt: null
    });
    return Object.freeze({ snapshot: immutableSnapshot, managerCapabilityHash });
  }

  async findActive(shareId) {
    const record = this.#records.get(shareId);
    if (!record || record.revokedAt) return null;
    if (record.snapshot.expiresAt && new Date(record.snapshot.expiresAt).getTime() <= this.clock().getTime()) return null;
    verifySnapshot(record.snapshot);
    if (!hashMatches(record.snapshotHash, record.snapshot.snapshotHash)) {
      throw Object.assign(new Error("SHARE_SNAPSHOT_HASH_MISMATCH"), { code: "SHARE_SNAPSHOT_HASH_MISMATCH" });
    }
    return Object.freeze({
      snapshot: record.snapshot,
      managerCapabilityHash: record.managerCapabilityHash,
      recipientCapabilityHash: record.recipientCapabilityHash,
      recipientBoundAt: record.recipientBoundAt
    });
  }

  async findAuthorized(shareId, recipientCapabilityHash = "") {
    const record = await this.findActive(shareId);
    if (!record) return null;
    if (record.snapshot.accessScope === SHARE_ACCESS_SCOPE.ANYONE_WITH_LINK) return record;
    return hashMatches(recipientCapabilityHash, record.recipientCapabilityHash) ? record : null;
  }

  async bindRecipient(shareId, recipientCapabilityHash) {
    const record = this.#records.get(shareId);
    if (!record || record.revokedAt || !/^[a-f0-9]{64}$/.test(String(recipientCapabilityHash || ""))) return null;
    verifySnapshot(record.snapshot);
    if (record.snapshot.accessScope === SHARE_ACCESS_SCOPE.ANYONE_WITH_LINK) return this.findActive(shareId);
    if (!record.recipientCapabilityHash) {
      record.recipientCapabilityHash = recipientCapabilityHash;
      record.recipientBoundAt = this.clock().toISOString();
    }
    return hashMatches(recipientCapabilityHash, record.recipientCapabilityHash)
      ? this.findActive(shareId)
      : null;
  }

  async revoke(shareId, managerCapabilityHash) {
    const record = this.#records.get(shareId);
    if (!record || record.revokedAt || !hashMatches(managerCapabilityHash, record.managerCapabilityHash)) return false;
    record.revokedAt = this.clock().toISOString();
    return true;
  }
}
