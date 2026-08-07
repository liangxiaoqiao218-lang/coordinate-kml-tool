# UTM Migration Approval Gate Specification

## 1. Status and Scope

Phase 5-4 defines the final governance gate for deciding whether UTM V2 may
proceed to a separately approved controlled migration. It does not enable
`controlled`, change the authoritative path, or modify production behavior.

The current evaluation is:

```yaml
technicalReadiness: READY
productionMigrationReadiness: NOT_READY
decision: HOLD_LEGACY
```

This specification does not modify `server.js`, `index.html`, Export, parser
routing, `precisionMode`, the legacy UTM30 path, user data, or locked baselines.

## 2. Approval States

The Approval Gate produces exactly one state:

```text
HOLD_LEGACY
READY_FOR_CONTROLLED_MIGRATION
MIGRATION_BLOCKED
```

### 2.1 HOLD_LEGACY

`HOLD_LEGACY` means the unchanged legacy path remains authoritative because
required production evidence is missing, incomplete, stale, still under review,
or not repeatable.

Examples include:

- no approved real cohort or observation window;
- cohort sample count or duration not satisfied;
- real status counts are `NOT_AVAILABLE`;
- actual Legacy/Canonical Export comparison has not run;
- user confirmation and KML success telemetry is unavailable;
- rollback is documented or simulated but not proven against the target
  production architecture;
- required reviewers have not signed the approval record.

`HOLD_LEGACY` is not a failure state. It prevents a decision from being inferred
from regression fixtures, isolated drills, or partial evidence.

### 2.2 READY_FOR_CONTROLLED_MIGRATION

`READY_FOR_CONTROLLED_MIGRATION` means every required evidence category is
complete, internally consistent, current for the reviewed commit and target
environment, independently reviewed, and approved.

This state permits a separate controlled-migration change request. It does not
itself switch runtime mode, enable canonical output, change Export, or authorize
an unbounded rollout.

### 2.3 MIGRATION_BLOCKED

`MIGRATION_BLOCKED` means evidence demonstrates a known safety, integrity,
compatibility, privacy, or rollback failure that must be resolved before any
controlled migration can be considered.

Examples include:

- an unresolved real `CRS_CONFLICT`;
- an unresolved real `TRANSFORMATION_MISMATCH`;
- actual Legacy/Canonical Export mismatch;
- invalid or stale typed data passing Migration Gate;
- a blocked result reaching canonical Export;
- erroneous KML delivered during an authorized observation or controlled
  cohort;
- BFTM, MGRS, Kyrgyz GK, DMS, or unknown-X/Y protection regression;
- production rollback attempted but unable to restore legacy authority;
- observation data cannot be correlated safely, is materially incomplete, or
  violates the approved privacy and retention policy.

An unattempted prerequisite normally produces `HOLD_LEGACY`. A completed test
that demonstrates unsafe behavior produces `MIGRATION_BLOCKED`.

## 3. Required Evidence

All evidence must refer to an identified commit, release candidate, environment,
cohort, and observation window. Evidence from different point sequences,
requests, builds, or environments cannot be combined without an explicit
compatibility review.

### 3.1 Technical readiness

The approval record must link immutable results for:

- CRS Evidence Acquisition;
- UTM Intent Resolution;
- Typed UTM Result and transformation provenance;
- Legacy versus V2 comparison;
- Migration Observation;
- Migration Gate;
- CRS Confirmation;
- Structured Projected XY priority;
- Migration Infrastructure and semantic Export comparator;
- protected BFTM, MGRS, Kyrgyz GK, DMS, and unknown projected-X/Y regressions.

Every required suite must pass against the exact candidate commit. Development
results from an older commit are supporting evidence only unless reproducibility
and compatibility are explicitly demonstrated.

### 3.2 Real cohort observation

The completed cohort report must provide:

- approved cohort ID, environment, owner, reviewers, sampling rule, duration,
  minimum eligible sample count, and retention policy;
- real request counts separated from synthetic regression fixtures;
- counts for `MATCH`, `V2_ONLY`, `LEGACY_ONLY`, `CRS_CONFLICT`,
  `TRANSFORMATION_MISMATCH`, and `BLOCKED`;
- the real eligible UTM30 `MATCH` rate;
- review and resolution of every non-`MATCH` real UTM30 observation;
- CRS confirmation completion, cancellation, and manual-selection metrics;
- KML attempt and success metrics;
- proof that legacy remained authoritative and V2 failures had no user impact.

Observation thresholds must be approved before the window begins. They cannot
be reduced after results are known merely to obtain approval.

### 3.3 Export compare

The evidence must compare actual Legacy and shadow Canonical KML candidates from
the same real requests and accepted point sequences.

Required comparisons include:

- document and Placemark names;
- Placemark count;
- geometry type;
- point count and order;
- longitude, latitude, and altitude;
- approved tolerance and maximum difference;
- hashes of redacted/canonicalized comparison artifacts.

KML bodies and raw coordinates are not required in the approval record. Any
unresolved `EXPORT_COMPARE_FAILED` result prevents READY.

### 3.4 Migration Gate results

The cohort report must contain Gate counts for `V2_ALLOWED`, `LEGACY_ONLY`, and
`BLOCKED`, including stable reasons.

Evidence must show that:

- Gate input, Typed Result, Migration Observation, and Export candidate belong
  to the same request and point sequence;
- invalid projected coordinates, stale transformations, point-count mismatch,
  CRS conflict, and transformation mismatch are blocked;
- Quality Gate and explicit CRS confirmation remain mandatory;
- no `V2_ALLOWED` decision is interpreted as automatic production authority.

### 3.5 Rollback proof

The approval record must demonstrate rollback against the target controlled
migration architecture, not only an isolated in-memory controller.

Required proof includes:

- integrated kill-switch identifier and owner;
- transition audit for `legacy -> shadow -> controlled -> legacy` in the
  authorized drill environment;
- trigger, start time, completion time, and measured recovery duration;
- restoration of legacy UTM30 recognition, response, and Export;
- verification of BFTM, MGRS, Kyrgyz GK, DMS, and unknown-X/Y protections after
  rollback;
- confirmation that historical data, baselines, KML, and legacy identifiers
  were not rewritten;
- rollback logs and evidence hashes;
- named rollback owner and backup owner.

If rollback requires data reconstruction, a baseline rewrite, or deletion of
canonical observations, migration is not ready.

## 4. Decision Rules

The Gate evaluates evidence in this order.

### 4.1 Rule 1: known safety failure

If any unresolved blocking condition in section 2.3 is demonstrated, return:

```text
MIGRATION_BLOCKED
```

This rule has priority over high `MATCH` rates, successful fixtures, or partial
approvals.

### 4.2 Rule 2: incomplete evidence

If no known blocking failure exists but any required evidence is missing,
`NOT_AVAILABLE`, `NOT_RUN`, stale, incomplete, unsigned, or below the predefined
observation threshold, return:

```text
HOLD_LEGACY
```

Regression PASS, fixture Export Compare, or isolated rollback drill cannot fill
a missing production evidence field.

### 4.3 Rule 3: readiness review

Return `READY_FOR_CONTROLLED_MIGRATION` only when all of the following are true:

1. Technical readiness suites pass for the exact candidate commit.
2. The approved real cohort window and minimum eligible sample count are
   complete.
3. Every real UTM30 observation is accounted for and all approval thresholds
   pass.
4. There is no unresolved real `LEGACY_ONLY`, `CRS_CONFLICT`, or
   `TRANSFORMATION_MISMATCH` case in the migration scope.
5. Actual Legacy/Canonical Export comparison passes for the approved cohort.
6. Gate evidence proves that unsafe results cannot reach canonical Export.
7. The erroneous KML delivery count for the observed cohort is measured as
   zero.
8. The integrated rollback drill restores legacy authority within the approved
   operational target.
9. Privacy, retention, auditability, and performance requirements pass.
10. Required owners and independent reviewers sign the immutable approval
    record.

READY authorizes only a bounded controlled-migration proposal. The production
mode remains unchanged until that proposal is separately reviewed, deployed,
and monitored.

### 4.4 Current decision

The Phase 5-3 report contains no real cohort, actual production Export Compare,
user telemetry, integrated kill switch, or production rollback proof.
Therefore the current decision remains:

```text
HOLD_LEGACY
```

## 5. Approval Record

Every decision must be captured in an immutable, reviewable record.

```yaml
schemaVersion: utm_migration_approval_gate_v1
decision: HOLD_LEGACY|READY_FOR_CONTROLLED_MIGRATION|MIGRATION_BLOCKED
decisionReasonCodes: []
candidate:
  repository: required
  branch: required
  commit: required
  release: required
  environment: required
cohort:
  cohortId: required
  observationStart: required
  observationEnd: required
  eligibleSampleCount: required
  reportLink: required
evidence:
  technicalReadinessLink: required
  regressionReportLink: required
  cohortReportLink: required
  exportCompareReportLink: required
  migrationGateReportLink: required
  rollbackReportLink: required
  evidenceHashes: []
operations:
  killSwitchId: required
  rollbackOwner: required
  rollbackBackupOwner: required
  controlledCohortOwner: required
approval:
  requestedAt: required
  decidedAt: required
  expiresAt: required
  approvers: []
  independentReviewers: []
  signatures: []
```

Rules for the record:

1. Evidence links must resolve to retained, access-controlled artifacts.
2. Hashes must cover canonicalized evidence without exposing raw user data.
3. The candidate commit and deployed candidate must match exactly.
4. Any evidence change after approval invalidates signatures and requires a new
   decision.
5. Approval expires at the recorded time or when the candidate, environment,
   cohort definition, Gate rules, Export rules, or rollback mechanism changes.
6. `HOLD_LEGACY` and `MIGRATION_BLOCKED` records also require an owner and next
   review condition; they are not undocumented non-decisions.

## 6. Controlled Migration Authorization Boundary

Even after READY:

- default mode remains `legacy` until a separate deployment approval;
- direct initialization in `controlled` remains forbidden;
- rollout begins with an explicitly bounded cohort;
- Migration Gate and Export Compare remain mandatory per eligible request;
- blocked requests cannot fall through to unsafe canonical output;
- kill switch and rollback owner must be available throughout the window;
- expansion requires a new reviewed observation report;
- READY cannot authorize deletion of the legacy alias or historical data
  migration.

## 7. Freeze Conditions

UTM migration is complete and eligible for final freeze only after:

1. A valid `READY_FOR_CONTROLLED_MIGRATION` approval record authorized the
   controlled rollout.
2. The canonical `utm-projected-x-y` type and exact
   `utm30n-projected-x-y` legacy alias operate as approved.
3. The bounded controlled cohort completes its monitoring window without an
   unresolved rollback trigger.
4. Subsequent expansion stages meet their predefined observation thresholds.
5. Real Migration Gate and Export Compare evidence remains within approved
   limits.
6. No erroneous KML is delivered in the approved migration observation scope.
7. Historical data, legacy identifiers, locked baselines, and frozen KML remain
   readable and unchanged.
8. BFTM, MGRS, Kyrgyz GK, DMS, and unknown-X/Y protections remain intact.
9. Rollback is exercised against the final architecture and remains available
   for the documented compatibility-retention period.
10. Final regression, operational review, privacy review, evidence hashes,
    approvals, migration commit, and rollback ownership are recorded in the
    freeze report.

The legacy alias cannot be removed merely because the controlled cohort passes.
Alias retirement, if ever proposed, requires a separate compatibility and data
lifecycle decision.

## 8. Prohibited Approval Shortcuts

The Approval Gate must not:

- treat synthetic regression as real cohort evidence;
- treat `MATCH`, `V2_ALLOWED`, or `V2_ONLY` as standalone approval;
- convert `NOT_AVAILABLE` to zero or PASS;
- infer approval from country, coordinate range, northing, or legacy alias;
- ignore or average away conflict, mismatch, or blocked cases;
- lower thresholds after observing results;
- approve a commit different from the observed candidate;
- approve without rollback ownership and proof;
- switch production mode, modify Export, or rewrite data as part of this design
  phase.
