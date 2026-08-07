# UTM Controlled Cohort Observation Report

Report date: 2026-08-08

Phase: 5-3 Controlled Cohort Observation Execution

Branch: `v2/utm-intent-router`

Reviewed HEAD: `5c7175e21631299a35f5c1cde3288f0858b11f6d`

Authoritative path: `legacy`

## 1. Observation Window

```yaml
cohortId: NOT_AVAILABLE
environment: NOT_AVAILABLE
observationStart: NOT_AVAILABLE
observationEnd: NOT_AVAILABLE
minimumEligibleSampleCount: NOT_AVAILABLE
samplingRule: NOT_AVAILABLE
owner: NOT_AVAILABLE
reviewers: NOT_AVAILABLE
retentionPolicy: NOT_AVAILABLE
windowCompleted: false
```

No controlled cohort, production observation window, or approved sampling plan
was available in the workspace at the time of this report. This report does not
construct an observation window from regression execution dates.

## 2. Observation Data Source

Available sources:

- repository state and committed Phase 1 through Phase 5-2 specifications;
- deterministic synthetic regression scripts and their previously verified
  results;
- real-image regression records from earlier development validation.

Unavailable sources:

- production request telemetry;
- a redacted real-request cohort dataset;
- production Legacy/V2 paired observations;
- production KML comparison records;
- CRS confirmation interaction telemetry;
- production kill-switch or rollback records.

Synthetic regression and real-image development fixtures are retained as
engineering evidence only. They are not counted as real cohort observations.

## 3. Real Sample Count

```yaml
verifiedRealCohortRecordsAvailable: 0
realSampleCount: NOT_AVAILABLE
eligibleRealUtm30Samples: NOT_AVAILABLE
eligibleRealV2OnlySamples: NOT_AVAILABLE
safetyBlockSamples: NOT_AVAILABLE
excludedSamples: NOT_AVAILABLE
observationPipelineFailures: NOT_AVAILABLE
```

`verifiedRealCohortRecordsAvailable: 0` means that no cohort records were
provided for this review. It does not assert that production received zero UTM
requests.

## 4. Migration Results

Real cohort status counts are unavailable:

| Status | Real cohort count | Evidence status |
|---|---:|---|
| `MATCH` | `NOT_AVAILABLE` | No real Legacy/V2 paired observations |
| `V2_ONLY` | `NOT_AVAILABLE` | No real V2 cohort observations |
| `LEGACY_ONLY` | `NOT_AVAILABLE` | No real cohort observations |
| `CRS_CONFLICT` | `NOT_AVAILABLE` | No real cohort observations |
| `TRANSFORMATION_MISMATCH` | `NOT_AVAILABLE` | No real cohort observations |
| `BLOCKED` | `NOT_AVAILABLE` | No real Gate observation records |

```yaml
eligibleRealUtm30MatchRate: NOT_AVAILABLE
unexplainedLegacyOnlyUtm30: NOT_AVAILABLE
unresolvedCrsConflict: NOT_AVAILABLE
unresolvedTransformationMismatch: NOT_AVAILABLE
```

Synthetic reference results remain separate:

- UTM30 fixture: `MATCH`, eight points, maximum transformation difference `0`.
- UTM50S fixture: `V2_ONLY`, WGS84 UTM Zone 50 South, `EPSG:32750`.
- Migration Observation regression: 8/8 PASS.

These results validate classification logic but cannot establish a production
`MATCH` rate.

## 5. Export Compare Results

```yaml
realLegacyKmlCandidates: NOT_AVAILABLE
realCanonicalKmlCandidates: NOT_AVAILABLE
realExportComparisons: NOT_AVAILABLE
realExportMatches: NOT_AVAILABLE
realExportMismatches: NOT_AVAILABLE
maximumRealCoordinateDifference: NOT_AVAILABLE
result: NOT_AVAILABLE
```

Phase 5-1 synthetic Export Compare remains 4/4 PASS:

- matching UTM30 semantic KML is accepted;
- coordinate mismatch is blocked;
- geometry mismatch is blocked;
- inconsistent Typed CRS is blocked.

That fixture result does not replace comparison of actual legacy KML candidates
with shadow canonical KML candidates from the same real requests.

## 6. Migration Gate Results

| Gate decision | Real cohort count |
|---|---:|
| `V2_ALLOWED` | `NOT_AVAILABLE` |
| `LEGACY_ONLY` | `NOT_AVAILABLE` |
| `BLOCKED` | `NOT_AVAILABLE` |

```yaml
realInvalidProjectedCoordinatesBlocked: NOT_AVAILABLE
realStaleTransformationsBlocked: NOT_AVAILABLE
realPointCountMismatchesBlocked: NOT_AVAILABLE
realConflictExportAttemptsBlocked: NOT_AVAILABLE
```

The synthetic Migration Gate suite remains 17/17 PASS. It verifies Gate logic,
not real cohort behavior. No `V2_ALLOWED` fixture result is treated as
production migration authorization.

## 7. User Flow Metrics

```yaml
crsConfirmationPresented: NOT_AVAILABLE
crsConfirmationCompleted: NOT_AVAILABLE
crsConfirmationSuccessRate: NOT_AVAILABLE
userConfirmationCancelled: NOT_AVAILABLE
userCancellationRate: NOT_AVAILABLE
manualCrsSelectionCount: NOT_AVAILABLE
manualCrsSelectionRate: NOT_AVAILABLE
kmlGenerationAttempts: NOT_AVAILABLE
kmlGenerationSuccesses: NOT_AVAILABLE
kmlGenerationSuccessRate: NOT_AVAILABLE
```

No production interaction telemetry was available. The CRS Confirmation
synthetic regression remains 4/4 PASS but is not a user behavior measurement.

## 8. Rollback Availability

```yaml
currentAuthoritativeMode: legacy
legacyPathPreservedInBranch: true
productionKillSwitchIntegrated: false
productionControlledModeEnabled: false
isolatedControllerDrill: PASS
productionRollbackDrill: NOT_AVAILABLE
productionRollbackOwner: NOT_AVAILABLE
productionRollbackTime: NOT_AVAILABLE
productionRecoveryVerification: NOT_AVAILABLE
```

The isolated Phase 5-1 controller verified:

```text
legacy -> shadow -> controlled -> legacy
```

That drill proves the in-memory transition contract only. Because no production
control plane or canonical-authoritative cohort exists, it is not production
rollback evidence.

## 9. Safety Review

| Safety requirement | Result | Basis |
|---|---|---|
| Legacy remains authoritative | PASS for current branch | No default-path migration is present |
| Production canonical output disabled | PASS for current branch | Migration infrastructure is isolated |
| Real erroneous KML count | `NOT_AVAILABLE` | No production cohort telemetry |
| Real CRS conflict blocked before Export | `NOT_AVAILABLE` | No real Gate records |
| Real transformation mismatch blocked before Export | `NOT_AVAILABLE` | No real Gate records |
| BFTM isolation | PASS in regression | Synthetic protection evidence only |
| MGRS blocking | PASS in regression | Synthetic protection evidence only |
| Kyrgyz GK isolation | PASS in regression | Synthetic protection evidence only |
| Unknown projected X/Y blocking | PASS in regression | Synthetic protection evidence only |

The required safety target is:

```yaml
erroneousKmlDeliveredToUsers: 0
```

The observed value is `NOT_AVAILABLE`, not `0`. Without real cohort telemetry,
this report cannot claim that the target was measured.

## 10. Regression Evidence Kept Separate

| Suite | Synthetic/development result | Counts toward real cohort |
|---|---:|---|
| Shadow Intent | PASS, 13 cases | No |
| CRS Evidence | 7/7 PASS | No |
| Typed UTM | 6/6 PASS | No |
| Migration Observation | 8/8 PASS | No |
| Migration Gate | 17/17 PASS | No |
| CRS Confirmation | 4/4 PASS | No |
| Migration Infrastructure | 12/12 PASS | No |
| CRS Evidence real-image development regression | recorded 6/6 PASS | No |
| Structured Projected Priority development regression | previously reported 3/3 PASS | No |

Regression evidence supports starting a properly authorized observation window.
It does not satisfy that window.

## 11. Missing Evidence Required for Reassessment

Before this report can be reconsidered:

1. Define and approve cohort ID, environment, sampling rule, duration, minimum
   eligible sample count, owner, reviewers, and retention policy.
2. Collect redacted real-request Legacy/V2 paired observation records.
3. Compare actual Legacy and shadow Canonical KML candidates for the same
   requests.
4. Record Migration Gate decisions and all six final status counts.
5. Record CRS confirmation, cancellation, manual selection, and KML success
   metrics.
6. Demonstrate that conflicts and transformation mismatches cannot reach
   Export.
7. Integrate and exercise the production kill switch and rollback procedure.
8. Complete independent review and sign-off.

## 12. Final Decision

```text
HOLD_LEGACY
```

Rationale:

- No real cohort or observation window is available.
- All real migration status counts are `NOT_AVAILABLE`.
- Actual production Legacy/Canonical Export comparison is unavailable.
- User confirmation and KML success metrics are unavailable.
- Production kill-switch and rollback evidence are unavailable.
- The safety target of zero erroneous KML has not been measured on a real
  cohort.

Legacy UTM30 remains authoritative. This report does not authorize
`READY_FOR_CONTROLLED_MIGRATION` or any production mode change.
