# Coordinate Engine V2 RC Secondary Deployment Approval

## Approval Status

```text
Approval status: PENDING
Secondary deployment: NOT APPROVED
Secondary validation: INCOMPLETE
Primary Production: BLOCKED
UTM migration: HOLD_LEGACY
```

This record is the approval gate between the Secondary Deployment Plan and any
deployment action. Creating or committing this document does not grant approval.
Approval becomes effective only when every required evidence field is completed
and an authorized approver records an explicit `APPROVED` decision.

## 1. Approval Scope

The maximum permitted scope of this approval is a bounded Coordinate Engine V2
RC validation deployment to:

```text
https://coordinate-kml-tool.onrender.com
```

The approved purpose, if granted, is limited to:

- deploying one identified immutable RC artifact to Secondary;
- running the approved smoke and validation matrix;
- observing runtime, coordinate, KML, quota, and entitlement behavior;
- performing the recorded rollback drill;
- updating the Secondary Validation Report with observed evidence.

The following remain prohibited:

- deployment to Primary Production at `https://geokitlab.com`;
- merge into `main` as part of this approval;
- Primary DNS, traffic, database, or environment changes;
- enabling payment in Secondary;
- setting migration control to `controlled`;
- making canonical UTM authoritative for production;
- changing the `HOLD_LEGACY` migration decision;
- deploying an unrecorded commit or mutable artifact.

Secondary Deployment is not a Primary Release.

## 2. Deployment Identity

### 2.1 RC source

| Field | Required value | Current record |
|---|---|---|
| Branch | `v2/utm-intent-router` | Recorded |
| Planned RC branch commit | Exact immutable commit | `b868b315a153216b88bea04b168c5b967d77f3e3` |
| Arbitration implementation commit | Must be an ancestor | `095168fc44b697ab11f8e4af2277704e9bd240cc` |
| Remote equality | Local approved commit equals remote branch | Recorded before this approval draft |
| Build artifact ID | Required | `NOT_AVAILABLE` |
| Build artifact SHA-256 | Required | `NOT_AVAILABLE` |
| Build time | Required | `NOT_AVAILABLE` |
| Package-lock identity | Required | `NOT_RECORDED` |
| Runtime version | Required | `NOT_RECORDED` |

If the planned RC branch commit changes, this approval returns to `PENDING` and
the new commit must repeat scope, regression, artifact, and rollback review.

### 2.2 Current Secondary and rollback identity

| Field | Requirement | Current record |
|---|---|---|
| Current Secondary running commit | Must be verified from deployment evidence | `NOT_VERIFIED` |
| Current Secondary artifact ID | Must be recorded | `NOT_AVAILABLE` |
| Current Secondary artifact hash | Must be recorded | `NOT_AVAILABLE` |
| Last known healthy result | Must be recorded | `NOT_AVAILABLE` |
| Rollback artifact | Must be immutable and recoverable | `NOT_VERIFIED` |
| Rollback owner | Must be named | `NOT_ASSIGNED` |
| Maximum rollback time | Must be approved | `NOT_DEFINED` |

The Git reference
`838e4bfedb159a6fe062db33c5f50eea95dbd667` is a development comparison and
pre-Arbitration reference. It must not be recorded as the Secondary rollback
artifact unless deployment evidence proves that exact artifact is the approved
recoverable target.

### 2.3 Artifact requirements

The RC artifact must:

- be built from the approved exact commit;
- have an immutable identifier and SHA-256 hash;
- be reproducible from the recorded lockfile and runtime;
- contain no `.env` file, `.env.vision-test`, API key, local fixture, screenshot,
  production snapshot, or user data;
- expose a non-sensitive version identity sufficient to verify the running
  source and artifact;
- use only approved production dependencies;
- be retained for the validation and rollback evidence window.

## 3. Safety Checks

Every item must be `PASS` before approval can change to `APPROVED`.

| Check | Required state | Current state |
|---|---|---|
| Primary environment unchanged | Confirmed | `PENDING` |
| Secondary payment | Disabled | `PENDING` |
| Payment callbacks and purchase controls | Disabled | `PENDING` |
| UTM migration decision | `HOLD_LEGACY` | `PASS` |
| Migration runtime authority | Legacy or separately approved shadow | `PENDING` |
| Controlled migration startup | Impossible | `PENDING` |
| Regression-test mode in deployed runtime | Disabled | `PENDING` |
| Vision/OCR variables | Presence verified without values | `PENDING` |
| Supabase target | Approved governed source | `PENDING` |
| Pricing and entitlement configuration | Verified | `PENDING` |
| Secrets absent from artifact and logs | Verified | `PENDING` |
| Current Secondary artifact | Identified and healthy | `PENDING` |
| Rollback artifact | Recoverable | `PENDING` |
| Rollback owner and response time | Assigned and approved | `PENDING` |
| Validation fixtures and test identities | Ready | `PENDING` |
| Monitoring and deployment logs | Available | `PENDING` |

Environment checks record only presence, target, and status. Secret values must
not be copied into this approval, deployment log, issue, chat, or validation
report.

## 4. Deployment Preconditions

Approval may be granted only when all of the following are satisfied:

1. The exact RC source commit is frozen and equals the remote V2 branch used for
   the build.
2. The immutable RC artifact ID, hash, build time, runtime, and lockfile identity
   are recorded.
3. The exact currently running Secondary commit and artifact are verified.
4. The rollback artifact is retained, recoverable, and smoke-tested.
5. A rollback owner, validation owner, approver, deployment window, and maximum
   rollback time are recorded.
6. Payment and purchase callbacks are verified disabled in Secondary.
7. Migration authority is verified as legacy or separately approved shadow;
   controlled authority is impossible.
8. Required environment variables are present and target the approved services,
   without recording their values.
9. Primary Production is confirmed unchanged and excluded from the deployment.
10. The RC regressions and Live Image Gate associated with the approved commit
    remain passing.
11. The smoke matrix and complete Secondary Validation Plan are available to the
    operator.
12. Monitoring, logs, and immediate rollback controls are available during the
    entire window.
13. User-impact expectations for the Secondary Production role are recorded and
    accepted.

Failure to satisfy any precondition keeps the decision `PENDING` or changes it
to `REJECTED`. Missing evidence must not be marked as passed by inference from
local development results.

## 5. Abort Conditions

Do not begin deployment when:

- the RC commit, artifact ID, or hash is missing or inconsistent;
- the current Secondary artifact or rollback target cannot be proven;
- the rollback owner or deployment window is unavailable;
- payment is enabled or cannot be proven disabled;
- migration mode is controlled or untraceable;
- regression-test mode is enabled in the deployment configuration;
- required environment variables or dependencies are missing;
- Primary must be changed to complete the operation;
- the deployed version cannot be identified;
- the approver has not recorded `APPROVED`.

Abort validation and invoke rollback when:

- an unsafe or geographically incorrect KML is produced;
- explicit CRS conflict, transformation mismatch, or coordinate-order review is
  bypassed;
- UTM50S falls back to DMS or legacy UTM30;
- BFTM, MGRS, Kyrgyz GK, DMS, WGS84, or Legacy UTM30 behavior regresses;
- quota is double-charged or entitlement state is incorrect;
- payment becomes available;
- a credential or sensitive payload appears in logs or responses;
- the service fails health, runtime, latency, timeout, or error stop thresholds;
- the running artifact no longer matches the approval record;
- rollback cannot be completed within the approved maximum time;
- the validation or rollback owner requests termination.

Any safety, identity, entitlement, or rollback failure results in `FAIL` and
rollback. Missing non-safety evidence results in `INCOMPLETE` and Hold.

## 6. Approval Record

### 6.1 Required record

| Field | Value |
|---|---|
| Approval ID | `NOT_ASSIGNED` |
| Decision | `PENDING` |
| Approved RC commit | `NOT_APPROVED` |
| Approved artifact ID | `NOT_APPROVED` |
| Approved artifact hash | `NOT_APPROVED` |
| Secondary rollback artifact | `NOT_APPROVED` |
| Deployment window start | `NOT_SCHEDULED` |
| Deployment window end | `NOT_SCHEDULED` |
| Deployment owner | `NOT_ASSIGNED` |
| Validation owner | `NOT_ASSIGNED` |
| Rollback owner | `NOT_ASSIGNED` |
| Reviewer/approver | `NOT_ASSIGNED` |
| Approval timestamp | `NOT_AVAILABLE` |
| User-impact acceptance | `NOT_RECORDED` |
| Notes/evidence links | `NOT_AVAILABLE` |

### 6.2 Permitted decisions

```text
PENDING
APPROVED
REJECTED
REVOKED
```

- `PENDING`: prerequisites or authorization are incomplete; do not deploy.
- `APPROVED`: the exact recorded artifact may be deployed only within the
  approved window and scope.
- `REJECTED`: deployment must not proceed; record the blocking reason.
- `REVOKED`: a previous approval is withdrawn; stop or roll back as applicable.

Approval applies only to the exact commit, artifact, environment, window, and
scope recorded above. It cannot be reused for a rebuilt artifact, new commit,
Primary deployment, or controlled UTM migration.

## Current Decision

```text
Secondary Deployment Approval: PENDING
Secondary Deployment: NOT APPROVED
Secondary Validation: INCOMPLETE
Ready for Primary Review: NO
Primary Production: BLOCKED
UTM Migration: HOLD_LEGACY

Secondary Deployment != Primary Release
```
