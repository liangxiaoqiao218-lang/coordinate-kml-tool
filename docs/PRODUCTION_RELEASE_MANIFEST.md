# GeoKit Lab Production Release Manifest

## 1. Purpose

This document defines the mandatory record for every GeoKit Lab production release. A completed release manifest connects an approved Git source state to an immutable build artifact, deployment targets, verification evidence, ownership, and a tested rollback target.

This document is a reusable template and governance contract. It does not authorize a deployment by itself and does not represent a completed release until all required fields and approvals are recorded.

## 2. Release Identity

Every release must have one immutable identity shared by Primary Production and Secondary Production.

| Field | Required value |
|---|---|
| Release ID | Unique identifier, for example `geokitlab-v1.2.0-20260808.1` |
| Version | Product version, for example `v1.2.0` |
| Release type | `PRODUCTION_RELEASE`, `HOTFIX`, or `ROLLBACK` |
| Manifest status | `DRAFT`, `APPROVED`, `DEPLOYING`, `HEALTHY`, `FAILED`, `ROLLED_BACK`, or `RETIRED` |
| Source repository | Canonical Git repository |
| Source branch | Informational branch name; not a substitute for the commit |
| Source commit | Full 40-character Git commit hash |
| Source state | Confirmation that the release was built from a clean worktree |
| Artifact name | Immutable artifact or image identifier |
| Artifact algorithm | `SHA-256` or another explicitly approved digest algorithm |
| Artifact hash | Full cryptographic artifact digest |
| Build time | Timestamp with timezone |
| Build system | CI job, build host, or approved manual-build record |
| Release notes | Link or path to reviewed release scope |
| Baseline reference | Previous production manifest and baseline report |

Branch names, semantic versions, ZIP filenames, file timestamps, and the public application version string are not sufficient release identities.

## 3. Deployment Targets

### 3.1 Primary Production

- URL: `https://geokitlab.com`
- Role: official production and business authority
- Owns: users, authentication, payment, orders, entitlements, and authoritative operational data
- Normal deployment order: first
- Payment state: recorded explicitly for every release

Required Primary deployment fields:

| Field | Required value |
|---|---|
| Target environment | `PRIMARY_PRODUCTION` |
| Planned deploy time | Timestamp with timezone |
| Actual deploy time | Timestamp with timezone |
| Deployed commit | Must equal the approved source commit |
| Deployed artifact hash | Must equal the approved artifact hash |
| Configuration profile | Non-secret profile identifier |
| Database authority | Non-secret authoritative service reference |
| Payment state | `ENABLED` or `DISABLED` |
| Deployment operator | Named owner |
| Health result | Evidence link and status |
| Verification result | Regression and business-flow evidence |
| Final state | `HEALTHY`, `FAILED`, or `ROLLED_BACK` |

### 3.2 Secondary Production

- URL: `https://coordinate-kml-tool.onrender.com`
- Role: standby production, overseas access, and approved validation
- Normal deployment order: after Primary Production is verified healthy
- Payment state: `DISABLED` by default

Required Secondary deployment fields are the same as Primary, except payment must remain disabled unless a separate payment-failover approval exists.

### 3.3 Cross-Environment Identity Rule

Primary Production and Secondary Production must use the same approved source commit and immutable artifact. Environment variables may differ, but application code must not diverge.

If Secondary Production is intentionally held back, the manifest must record:

- the current Secondary release;
- the reason for the hold;
- the responsible owner;
- the review or expiry time;
- the compatibility and failover impact.

Until both environments report the same approved commit and artifact, the release status must not claim full synchronization.

## 4. Release Scope

The manifest must list every material change included in the release:

- user-visible features;
- bug fixes;
- coordinate recognition or KML behavior;
- API changes;
- authentication and authorization changes;
- payment, order, membership, quota, or entitlement changes;
- database or storage changes;
- environment-variable additions or removals, without secret values;
- dependency changes;
- operational and monitoring changes;
- known limitations and deferred work.

Out-of-scope work must be recorded explicitly when confusion with another active branch is possible. Development-only UTM V2 work must not be included without its own migration approval evidence.

## 5. Release Checklist

All required items must be completed before the manifest status becomes `APPROVED`.

### 5.1 Source and Review

- [ ] Source commit is a full, valid Git commit.
- [ ] Worktree used for the build is clean.
- [ ] Release diff is reviewed and scoped.
- [ ] No unrelated or untracked files are included.
- [ ] Required code review is approved.
- [ ] Secrets and credentials are absent from Git and the artifact.
- [ ] Dependency changes are reviewed.

### 5.2 Regression and Safety

- [ ] Locked coordinate baselines pass.
- [ ] KML safety and Export checks pass.
- [ ] Authentication and authorization checks pass.
- [ ] User, quota, and entitlement checks pass when affected.
- [ ] Payment sandbox and callback checks pass when affected.
- [ ] Primary and Secondary artifact identity can be verified.
- [ ] Health checks and monitoring are ready.
- [ ] Known failures and accepted risks are recorded.

### 5.3 Database and Data

- [ ] Database migration requirement is classified as `NONE`, `FORWARD_ONLY`, or `REVERSIBLE`.
- [ ] Migration scripts are reviewed and identified by digest or commit.
- [ ] Backup or recovery checkpoint is verified when required.
- [ ] Schema compatibility with the rollback release is confirmed.
- [ ] Single authoritative user, payment, order, and entitlement state is preserved.
- [ ] No uncontrolled dual writes or independent production databases are introduced.

### 5.4 Environment Validation

- [ ] Primary configuration profile is validated without recording secret values.
- [ ] Secondary configuration profile is validated without recording secret values.
- [ ] Required environment-variable names are present.
- [ ] Domain, callback, canonical URL, CORS, and cookie settings are verified.
- [ ] Payment is enabled only on the approved Primary release.
- [ ] Secondary payment remains disabled unless separately approved.
- [ ] Runtime version metadata will expose commit, artifact, environment, and build time.

### 5.5 Rollback Readiness

- [ ] Previous healthy release is identified.
- [ ] Rollback commit is recorded.
- [ ] Rollback artifact hash is recorded.
- [ ] Database compatibility is confirmed.
- [ ] Rollback procedure is tested or rehearsed.
- [ ] Kill switches are identified where applicable.
- [ ] Rollback owner is assigned.
- [ ] Stop conditions are documented.

## 6. Database Migration Record

| Field | Required value |
|---|---|
| Migration required | `YES` or `NO` |
| Migration classification | `NONE`, `FORWARD_ONLY`, or `REVERSIBLE` |
| Migration ID | Unique migration identifier |
| Migration source | Commit and file path |
| Pre-migration checkpoint | Backup or recovery reference |
| Execution owner | Named operator |
| Execution time | Timestamp with timezone |
| Verification | Schema and data-integrity evidence |
| Rollback compatibility | Explicit result |
| Final status | `NOT_RUN`, `SUCCESS`, `FAILED`, or `ROLLED_BACK` |

Database credentials, connection secrets, user records, payment records, and entitlement data must never be copied into this manifest.

## 7. Payment and Entitlement Record

Payment and entitlement changes must be bound to a specific Primary Production release.

| Field | Required value |
|---|---|
| Payment impact | `NONE`, `CONFIG_ONLY`, or `CODE_CHANGE` |
| Primary payment state | `ENABLED` or `DISABLED` |
| Secondary payment state | Normally `DISABLED` |
| Provider mode | `SANDBOX` or `PRODUCTION` |
| Callback configuration | Non-secret endpoint and verification status |
| Idempotency validation | Evidence reference |
| Order reconciliation | Evidence reference |
| Entitlement issuance | Evidence reference |
| Refund/reversal validation | Evidence reference when applicable |
| Payment rollback plan | Explicit procedure and owner |

Payment, membership, quota, and entitlement behavior must not be released without an approved Primary release identity and rollback target.

## 8. Rollback Record

### 8.1 Planned Rollback Target

| Field | Required value |
|---|---|
| Previous release ID | Last verified healthy release |
| Rollback commit | Full 40-character Git commit |
| Rollback artifact | Immutable artifact identifier |
| Rollback artifact hash | Full cryptographic digest |
| Database compatibility | Verified result |
| Expected recovery time | Operational estimate |
| Rollback owner | Named operator |
| Verification steps | Health and business-flow checks |

### 8.2 Executed Rollback

Complete this section only if rollback occurs.

| Field | Recorded value |
|---|---|
| Trigger reason | Incident or stop condition |
| Decision time | Timestamp with timezone |
| Decision owner | Authorized approver |
| Rollback start | Timestamp with timezone |
| Rollback completion | Timestamp with timezone |
| Restored release | Release ID, commit, and artifact hash |
| Database action | None, reverted, or recovered |
| Verification result | Health and business-flow evidence |
| User impact | Concise impact statement |
| Incident reference | Link or identifier |
| Final status | `RECOVERED`, `PARTIAL`, or `FAILED` |

Rollback must use a recorded commit and artifact. Editing production files in place is prohibited.

## 9. Approval Record

| Field | Required value |
|---|---|
| Prepared by | Manifest owner |
| Technical reviewer | Code and architecture reviewer |
| Regression reviewer | Quality and baseline reviewer |
| Database reviewer | Required when data changes |
| Payment/business reviewer | Required when payment or entitlement changes |
| Operations reviewer | Deployment and rollback reviewer |
| Approval time | Timestamp with timezone |
| Decision | `APPROVED`, `REJECTED`, or `HOLD` |
| Conditions | Required follow-up or rollout restrictions |
| Evidence links | Review, CI, regression, and artifact references |

No person should approve a release while required evidence is missing. `HOLD` must be used instead of inventing or assuming evidence.

## 10. Deployment Execution Record

The following sequence is mandatory under normal operation:

```text
Approved source commit
        ↓
Immutable build artifact
        ↓
Staging verification
        ↓
Primary Production deployment
        ↓
Primary health and business verification
        ↓
Secondary Production deployment
        ↓
Cross-environment identity verification
        ↓
Manifest marked HEALTHY
```

For each step, record start time, completion time, operator, result, and evidence. Emergency failover or rollback may change the order only under an approved incident procedure.

## 11. Mandatory Rules

1. A production deployment without a completed release manifest cannot be accepted as an official release baseline.
2. The manifest must identify a full Git commit and immutable artifact hash before deployment.
3. The build must originate from a clean, approved source state.
4. Primary Production is deployed and verified before normal synchronization to Secondary Production.
5. Primary and Secondary must ultimately use the same approved commit and artifact.
6. Payment, orders, memberships, quotas, and entitlements must be bound to the approved Primary release.
7. Secondary payment is disabled by default.
8. Database changes require explicit migration and rollback compatibility records.
9. UTM V2 or any other migration branch requires its own approval gate before inclusion.
10. Missing evidence results in `HOLD`, not an inferred pass.
11. Secrets, credentials, and sensitive production data must not appear in the manifest.
12. A release is complete only after deployment results and rollback readiness are recorded.

## 12. Release Manifest Template

Copy this section into a release-specific record and replace every placeholder. Do not delete required fields.

```yaml
release:
  releaseId: REQUIRED
  version: REQUIRED
  releaseType: PRODUCTION_RELEASE | HOTFIX | ROLLBACK
  status: DRAFT | APPROVED | DEPLOYING | HEALTHY | FAILED | ROLLED_BACK | RETIRED
  sourceRepository: REQUIRED
  sourceBranch: REQUIRED
  sourceCommit: REQUIRED_FULL_40_CHAR_HASH
  cleanSourceState: true | false
  artifact:
    name: REQUIRED
    algorithm: SHA-256
    hash: REQUIRED
  buildTime: REQUIRED_TIMESTAMP_WITH_TIMEZONE
  buildSystem: REQUIRED
  releaseNotes: REQUIRED

scope:
  includedChanges: []
  excludedChanges: []
  knownLimitations: []
  environmentVariableNamesAdded: []
  environmentVariableNamesRemoved: []

database:
  migrationRequired: true | false
  classification: NONE | FORWARD_ONLY | REVERSIBLE
  migrationId: NONE | REQUIRED
  checkpoint: NONE | REQUIRED
  rollbackCompatible: true | false
  status: NOT_RUN | SUCCESS | FAILED | ROLLED_BACK

primary:
  environment: PRIMARY_PRODUCTION
  url: https://geokitlab.com
  plannedDeployTime: REQUIRED
  actualDeployTime: PENDING
  commit: REQUIRED_FULL_40_CHAR_HASH
  artifactHash: REQUIRED
  configurationProfile: REQUIRED_NON_SECRET_ID
  paymentState: ENABLED | DISABLED
  operator: REQUIRED
  verification: PENDING
  finalState: PENDING

secondary:
  environment: SECONDARY_PRODUCTION
  url: https://coordinate-kml-tool.onrender.com
  plannedDeployTime: REQUIRED
  actualDeployTime: PENDING
  commit: REQUIRED_FULL_40_CHAR_HASH
  artifactHash: REQUIRED
  configurationProfile: REQUIRED_NON_SECRET_ID
  paymentState: DISABLED
  operator: REQUIRED
  verification: PENDING
  finalState: PENDING

rollback:
  previousReleaseId: REQUIRED
  commit: REQUIRED_FULL_40_CHAR_HASH
  artifactHash: REQUIRED
  databaseCompatible: true | false
  owner: REQUIRED
  procedure: REQUIRED
  drillEvidence: REQUIRED

approval:
  preparedBy: REQUIRED
  technicalReviewer: REQUIRED
  regressionReviewer: REQUIRED
  databaseReviewer: NOT_APPLICABLE | REQUIRED
  paymentReviewer: NOT_APPLICABLE | REQUIRED
  operationsReviewer: REQUIRED
  approvalTime: PENDING
  decision: HOLD | APPROVED | REJECTED
  conditions: []
  evidence: []
```

## 13. Current Governance Boundary

The current Primary Production snapshot and baseline analysis provide evidence for planning but are not a completed release manifest:

```text
Primary snapshot: 41b241539bfd7ebe40b7c192b5192f965b6a410a
Candidate source: 207443f8c7e5645bed04872e00f21de5781e59a0
Source confidence: LIKELY
Original provenance: NOT CONFIRMED
```

The first future controlled production release must create a release-specific record using this template before deployment begins.
