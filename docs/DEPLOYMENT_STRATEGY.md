# GeoKit Lab Deployment Strategy

## 1. Purpose and Scope

This document defines the deployment governance rules for GeoKit Lab. It establishes one product, one source-controlled release, and two production environments with different operational roles.

These rules apply to application code, account and entitlement services, payment integration, databases, release artifacts, rollback, and version reporting. Environment-specific configuration may differ, but application code must not develop into separate domestic and overseas product lines.

## 2. Deployment Roles

### 2.1 Primary Production

- Environment: `geokitlab.com`
- Role: official production environment and authoritative user-facing service
- Authority: account, payment, order, entitlement, and product-operation decisions

### 2.2 Secondary Production

- Environment: `coordinate-kml-tool.onrender.com`
- Role: standby production environment
- Permitted uses:
  - service failover;
  - overseas-access continuity;
  - explicitly approved, limited-scope validation;
  - controlled rollout observation when a migration plan authorizes it.

### 2.3 Product Boundary

Primary Production and Secondary Production are two deployments of the same product. They must not be managed as independent products, independent code lines, or independent sources of business truth.

Primary Production is authoritative under normal operation. Secondary Production must not silently become authoritative through configuration drift, an untracked deployment, or a manual hotfix.

## 3. Business Ownership

Primary Production owns the official business flow for:

- user accounts and authentication;
- payments and payment callbacks;
- orders, refunds, and reconciliation;
- memberships, quotas, and other entitlements;
- product analytics and operational statistics.

Secondary Production is limited to:

- failover and disaster recovery;
- overseas access;
- approved gray or controlled validation;
- read-compatible access to the same authoritative user and entitlement state when required.

Secondary Production must not create a separate account, order, entitlement, payment, or analytics authority.

## 4. Code Synchronization Rules

### 4.1 Single Commit Rule

Both production environments must be deployed from the same approved Git commit. A release is identified by the full commit hash, not by a branch name, local working tree, ZIP filename, timestamp, or manually reported version string.

The deployable artifact must be built from a clean worktree. Uncommitted or untracked application files must never be used as a production release source.

### 4.2 Allowed Differences

The following may differ by environment when documented and managed outside application source:

- domain and public base URL;
- secrets and credentials;
- regional network settings;
- logging, monitoring, and resource limits;
- feature exposure explicitly authorized by a rollout record;
- payment enablement, which remains disabled on Secondary Production by default.

Environment variables must not be used to maintain permanent product forks or incompatible business behavior.

### 4.3 Prohibited Differences

The following are prohibited:

- long-lived domestic and overseas code forks;
- production hotfixes that do not exist in Git;
- deploying from a dirty working tree;
- different parser, Export, entitlement, or payment implementations for the same release;
- reporting the same version identifier for different application code;
- treating a successful deployment as synchronized without verifying its commit manifest.

### 4.4 V2 Migration Boundary

UTM V2 and other migration work must not enter either production environment solely because it exists on a development branch. Production deployment requires its own approval gate, release manifest, regression evidence, and rollback plan. Until that approval is recorded, the authorized production behavior remains the approved Legacy path.

## 5. Database Rules

### 5.1 Single Source of Truth

GeoKit Lab must define one authoritative data system for:

- user identities;
- account profiles;
- orders and payment transactions;
- memberships, quotas, and entitlements;
- audit and reconciliation records;
- product statistics whose values affect business decisions.

Both production environments must resolve user and business state from that authority. Regional caches or replicas may be introduced only when their consistency, write authority, recovery behavior, and monitoring are documented.

### 5.2 Prohibited Database Topology

The following are prohibited:

- independent domestic and overseas user databases;
- independent entitlement counters for the same user;
- accepting payments into databases that cannot be reconciled as one ledger;
- uncontrolled bidirectional writes;
- failover to a database whose state is not proven current;
- manual copying of order or entitlement state as a normal synchronization mechanism.

Any future database migration must have a separate data migration, reconciliation, rollback, and ownership plan.

## 6. Release Flow

The normal release flow is:

```text
Development branch
        ↓
Staging validation
        ↓
Primary Production (geokitlab.com)
        ↓
Secondary Production (coordinate-kml-tool.onrender.com)
```

### 6.1 Development

- Changes are implemented on scoped branches.
- Required automated and manual regression checks pass.
- The proposed release commit has a clean, reviewable history.

### 6.2 Staging

- The exact proposed commit is deployed.
- Business-critical flows, coordinate regression, Export, authentication, and applicable payment sandbox flows are verified.
- The release manifest is prepared before production promotion.

### 6.3 Primary Production

- The approved commit is deployed without source modification.
- Health checks, version identity, critical flows, and monitoring are verified.
- The release record is updated with the actual deployment result.

### 6.4 Secondary Production

- The same commit is deployed after Primary Production verification, unless an approved incident procedure requires Secondary Production first.
- Its environment-specific configuration is validated separately.
- Commit identity and artifact identity must match the Primary Production release.

A release is not complete until both environments have an explicit status in the version manifest. If one environment is intentionally held back, the manifest must record the reason, owner, and expiry or review time; the environments must not be described as synchronized.

## 7. Payment Rules

- Payment is integrated with and enabled only on Primary Production by default.
- Payment callbacks must target approved Primary Production endpoints.
- Orders, refunds, reconciliation, and entitlements must use the authoritative business database.
- Secondary Production must not expose active payment entry points, accept payment callbacks, or create an independent order ledger by default.
- Enabling payment on Secondary Production requires a separately approved failover plan covering callback routing, idempotency, credential custody, order authority, reconciliation, and rollback.
- A traffic failover must not implicitly enable payment on Secondary Production.

## 8. Rollback and Failover Rules

### 8.1 Release Rollback

Every production release must identify a previously verified rollback commit and artifact before deployment.

If a release fails health, regression, data-integrity, payment, entitlement, or coordinate-safety checks:

1. stop rollout expansion;
2. disable the affected feature through an approved kill switch when available;
3. redeploy the exact recorded rollback commit;
4. verify commit identity, database compatibility, critical user flows, and Export safety;
5. record the incident, rollback time, owner, and verification result in the manifest.

Rollback must not be implemented by editing production files in place.

### 8.2 Primary-to-Secondary Failover

Failover to Secondary Production is allowed only when:

- Secondary Production is on the same approved commit, or an explicitly approved rollback commit;
- required shared data is current and reachable;
- account and entitlement behavior is verified;
- payment remains disabled unless payment failover has separate approval;
- traffic routing and restoration steps are documented;
- an operator and rollback owner are assigned.

After Primary Production recovers, traffic restoration must follow a recorded procedure and include version and data consistency checks.

## 9. Version Manifest

Each release must have a version manifest entry containing at least:

| Field | Requirement |
|---|---|
| Release ID | Unique human-readable release identifier |
| Commit | Full Git commit hash |
| Artifact identity | Immutable artifact digest or equivalent build identifier |
| Environment | Staging, Primary Production, or Secondary Production |
| Deployment time | Timestamp with timezone |
| Deployment status | Planned, deploying, healthy, failed, rolled back, or retired |
| Source branch | Informational only; commit remains authoritative |
| Configuration profile | Non-secret environment profile identifier |
| Database authority | Authoritative database/service reference |
| Payment state | Enabled or disabled |
| Verification | Health, regression, Export, and business-flow results |
| Rollback target | Full commit and artifact identity |
| Owner | Release operator and rollback owner |
| Notes | Exceptions, incidents, approvals, and follow-up actions |

The application should expose an operational version endpoint or build metadata containing the release commit and environment identifier. A manually maintained semantic version alone is insufficient because different code can otherwise report the same version.

## 10. Synchronization and Audit Rules

- Production synchronization is confirmed only when both environments report the same approved commit and artifact identity.
- File comparison may support an audit but does not replace commit and artifact provenance.
- Any environment with an unknown commit is considered unsynchronized.
- Any deployed code absent from Git is an incident to be captured as a snapshot, reviewed, and reconciled before the next normal release.
- Deployment access, approvals, manifest updates, and rollback actions must be auditable.
- Secrets must never be stored in the version manifest or committed to Git.

## 11. Governance Decision Summary

- Official production authority: `geokitlab.com`.
- Standby production environment: `coordinate-kml-tool.onrender.com`.
- Product model: one product, one approved commit, two environment configurations.
- Business and data authority: Primary Production backed by one authoritative data system.
- Payment authority: Primary Production only by default.
- Release order: Development, Staging, Primary Production, Secondary Production.
- Synchronization proof: matching Git commit and immutable artifact identity.
- Rollback: exact recorded commit/artifact; never an in-place production edit.
- UTM V2: remains outside production until its migration approval requirements are satisfied.
