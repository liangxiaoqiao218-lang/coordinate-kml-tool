export const UTM_MIGRATION_MODES = Object.freeze(["legacy", "shadow", "controlled"]);

const ALLOWED_TRANSITIONS = Object.freeze({
  legacy: new Set(["shadow"]),
  shadow: new Set(["legacy", "controlled"]),
  controlled: new Set(["legacy", "shadow"])
});

function assertMode(mode) {
  if (!UTM_MIGRATION_MODES.includes(mode)) {
    throw new RangeError(`migration mode must be one of: ${UTM_MIGRATION_MODES.join(", ")}`);
  }
  return mode;
}

function assertReason(reason) {
  const value = String(reason || "").trim();
  if (!value) throw new TypeError("migration transition requires an audit reason");
  return value;
}

function frozenAuditEntry(sequence, from, to, reason, readinessDecision = null) {
  return Object.freeze({ sequence, from, to, reason, readinessDecision });
}

export function createUtmMigrationControl({ initialMode = "legacy" } = {}) {
  let mode = assertMode(initialMode);
  if (mode === "controlled") {
    throw new Error("controlled mode cannot be used as an initial migration mode");
  }
  let sequence = 0;
  const audit = [frozenAuditEntry(sequence, null, mode, "initial_state")];

  function getState() {
    return Object.freeze({
      mode,
      defaultMode: "legacy",
      audit: Object.freeze([...audit])
    });
  }

  function transition(nextMode, { reason, readinessDecision = null } = {}) {
    const target = assertMode(nextMode);
    const auditReason = assertReason(reason);
    if (!ALLOWED_TRANSITIONS[mode].has(target)) {
      throw new Error(`migration transition ${mode} -> ${target} is not allowed`);
    }
    if (target === "controlled" && readinessDecision !== "READY_FOR_CONTROLLED_MIGRATION") {
      throw new Error("controlled mode requires READY_FOR_CONTROLLED_MIGRATION approval");
    }
    const from = mode;
    mode = target;
    sequence += 1;
    const entry = frozenAuditEntry(sequence, from, target, auditReason, readinessDecision);
    audit.push(entry);
    return entry;
  }

  function resolveAuthority({ migrationGate = null, exportComparison = null } = {}) {
    if (mode === "legacy") {
      return Object.freeze({
        mode,
        authority: "legacy",
        canonicalObserved: false,
        reason: "KILL_SWITCH_LEGACY"
      });
    }
    if (mode === "shadow") {
      return Object.freeze({
        mode,
        authority: "legacy",
        canonicalObserved: true,
        reason: "SHADOW_LEGACY_AUTHORITATIVE"
      });
    }

    if (migrationGate?.migrationDecision === "BLOCKED") {
      return Object.freeze({
        mode,
        authority: "blocked",
        canonicalObserved: true,
        reason: "MIGRATION_GATE_BLOCKED"
      });
    }
    if (migrationGate?.migrationDecision === "LEGACY_ONLY") {
      return Object.freeze({
        mode,
        authority: migrationGate.legacyAvailable ? "legacy" : "blocked",
        canonicalObserved: true,
        reason: migrationGate.legacyAvailable ? "MIGRATION_GATE_LEGACY_ONLY" : "NO_SAFE_AUTHORITY"
      });
    }
    if (migrationGate?.migrationDecision !== "V2_ALLOWED") {
      return Object.freeze({
        mode,
        authority: "blocked",
        canonicalObserved: true,
        reason: "MIGRATION_GATE_REQUIRED"
      });
    }
    if (exportComparison?.status !== "MATCH") {
      return Object.freeze({
        mode,
        authority: "blocked",
        canonicalObserved: true,
        reason: "EXPORT_COMPARE_REQUIRED"
      });
    }
    return Object.freeze({
      mode,
      authority: "canonical",
      canonicalObserved: true,
      reason: "CONTROLLED_CANONICAL_ALLOWED"
    });
  }

  return Object.freeze({ getState, transition, resolveAuthority });
}
