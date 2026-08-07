# GeoKit Lab Deployment Version Manifest

## 1. Purpose

This manifest records the auditable identity of each GeoKit Lab environment. It distinguishes an original build commit from a later Git snapshot that records observable production state.

This file is a point-in-time record, not an automatic deployment status system. Every future production release must record the full source commit, immutable artifact identity, deployment time, verification result, and rollback target.

## 2. Manifest Snapshot

- Recorded at: `2026-08-08` (`Asia/Shanghai`)
- Governance source: `docs/DEPLOYMENT_STRATEGY.md`
- Production authority: Primary Production
- Current migration policy: Legacy remains authoritative; development branches do not enter production without approval.

## 3. Environment Summary

| Environment | Role | Recorded identity | Original build commit | Deployment status |
|---|---|---|---|---|
| `https://geokitlab.com` | Primary Production | `41b241539bfd7ebe40b7c192b5192f965b6a410a` — production snapshot | `UNKNOWN` | Online; observable state captured, not validated as release baseline |
| `https://coordinate-kml-tool.onrender.com` | Secondary Production | `207443f8c7e5645bed04872e00f21de5781e59a0` | `207443f8c7e5645bed04872e00f21de5781e59a0` | Online; current deployment mapping is auditable |
| `coordinate-engine-v2-utm-router` | V2 Development | `838e4bfedb159a6fe062db33c5f50eea95dbd667` | Not applicable | Development only; not production-approved |

## 4. Primary Production

### 4.1 Identity

- URL: `https://geokitlab.com`
- Role: Primary Production
- Business authority: official user, account, payment, order, entitlement, and operational-data environment
- Snapshot branch: `production/geokitlab-current-snapshot`
- Snapshot commit: `41b241539bfd7ebe40b7c192b5192f965b6a410a`
- Snapshot source: read-only public production capture
- Original build commit: `UNKNOWN`
- Deployment status: `ONLINE — SNAPSHOT CAPTURED`
- Release-baseline status: `NOT VALIDATED`

### 4.2 Snapshot Scope

The snapshot commit contains:

- exact publicly served `index.html` bytes;
- exact publicly served `server.js` bytes;
- exact publicly served `package.json` bytes;
- exact publicly served `package-lock.json` bytes;
- file hashes and observable deployment metadata;
- environment-variable names referenced by the application source, without values.

Snapshot evidence is stored under:

```text
production-snapshots/geokitlab/2026-08-08/
```

The snapshot intentionally excludes environment values, secrets, databases, user records, orders, payments, entitlements, host configuration, and private infrastructure configuration.

### 4.3 Identity Interpretation

The snapshot commit is the Git identity of the captured evidence. It is not proof that the running deployment was originally built from that commit.

Publicly served `server.js`, `package.json`, and `package-lock.json` normalize to the corresponding blobs in `origin/main` at `207443f8c7e5645bed04872e00f21de5781e59a0`. The served `index.html` does not match the committed `origin/main` blob.

Therefore:

```text
Observable production state: TRACKED BY SNAPSHOT COMMIT
Original production build commit: UNKNOWN
Approved release baseline: NOT ESTABLISHED
```

Primary Production must not be relabeled as `207443f` based only on partial file alignment.

## 5. Secondary Production

### 5.1 Identity

- URL: `https://coordinate-kml-tool.onrender.com`
- Role: Secondary Production
- Current commit: `207443f8c7e5645bed04872e00f21de5781e59a0`
- Source reference: `origin/main`
- Deployment status: `ONLINE — AUDITABLE`

### 5.2 Operating Boundary

- Secondary Production is not an independent product.
- It is not the authoritative payment or order environment.
- Payment remains disabled by default.
- It must ultimately run the same approved commit and artifact as Primary Production.
- It must not receive development-only UTM V2 behavior without an approved production migration.

## 6. V2 Development

### 6.1 Identity

- Worktree: `coordinate-engine-v2-utm-router`
- Branch: `v2/utm-intent-router`
- HEAD: `838e4bfedb159a6fe062db33c5f50eea95dbd667`
- Remote reference: `origin/v2/utm-intent-router`
- Role: development and migration validation
- Deployment status: `NOT PRODUCTION`

### 6.2 Purpose and Boundary

This branch contains UTM V2 evidence, typed-result, migration-observation, confirmation, migration-gate, control, rollback, and approval-governance work.

Its technical readiness does not authorize production deployment. Legacy behavior remains authoritative until the production migration approval gate is satisfied with real observation evidence.

## 7. Version Endpoint Observation

At snapshot time, both production URLs reported:

```text
2026-05-01-quota-contact-v2
```

This value is not a Git commit and cannot prove synchronization. A future version endpoint should expose non-secret build metadata containing:

- full source commit;
- immutable artifact or build digest;
- environment identifier;
- build timestamp with timezone;
- release identifier.

## 8. Version Rules

1. Primary Production is the official business and operational authority.
2. Secondary Production is the standby environment and must not become a separate product or business authority.
3. Both production environments must ultimately use the same approved source commit and immutable artifact.
4. Environment configuration may differ; long-lived application-code divergence is prohibited.
5. Development branches, including `v2/utm-intent-router`, do not enter production without explicit approval.
6. An original deployment without source provenance remains `UNKNOWN`, even after an observational snapshot is captured.
7. A snapshot commit identifies captured evidence; it does not retroactively become the original build commit.
8. Matching filenames, version strings, or a subset of files cannot establish complete deployment identity.
9. Production changes outside Git require snapshotting, review, and reconciliation.
10. Every future release must identify its rollback commit and artifact before deployment.

## 9. Current Decision

### 9.1 Production Baseline Status

```text
OBSERVABLE PRIMARY STATE CAPTURED
PRODUCTION RELEASE BASELINE NOT YET ESTABLISHED
```

The previous untracked `UNKNOWN` state now has a Git-tracked snapshot identity. The original deployment provenance remains unknown, so the snapshot cannot yet be promoted as the recommended release baseline.

### 9.2 Synchronization Status

```text
NOT PROVEN SYNCHRONIZED
```

The next governance step is to compare the snapshot with `origin/main`, classify every material difference, and approve a formal production baseline before any synchronization deployment, payment integration, or UTM V2 migration.
