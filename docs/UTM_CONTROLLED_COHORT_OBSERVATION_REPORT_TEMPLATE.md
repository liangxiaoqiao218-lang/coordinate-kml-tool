# UTM Controlled Cohort Observation Report

> Template status: incomplete. Blank, `TBD`, `NOT_RUN`, or `UNAVAILABLE`
> entries require the final decision to remain `HOLD_LEGACY`.

## 1. Report Metadata

```yaml
reportVersion: utm_controlled_cohort_observation_v1
cohortId: TBD
environment: TBD
branchOrRelease: TBD
commit: TBD
observationStart: TBD
observationEnd: TBD
minimumEligibleSampleCount: TBD
samplingRule: TBD
owner: TBD
reviewers: TBD
retentionPolicy: TBD
authoritativePath: legacy
```

## 2. Scope Confirmation

- [ ] Legacy UTM30 remained authoritative for every request.
- [ ] Canonical evaluation was shadow-only.
- [ ] Production `precisionMode` did not change.
- [ ] Legacy parser and Export behavior did not change.
- [ ] Observation failures could not affect the user response.
- [ ] Synthetic fixtures were excluded from real-cohort statistics.
- [ ] Observation data was redacted according to the approved policy.

## 3. Cohort Counts

| Metric | Count |
|---|---:|
| Requests considered | TBD |
| Eligible real observations | TBD |
| Excluded requests | TBD |
| Observation pipeline failures | TBD |
| Real UTM30 observations | TBD |
| Real V2-only UTM observations | TBD |
| Safety-block observations | TBD |

## 4. Status Summary

| Final status | Count | Reviewed | Unresolved |
|---|---:|---:|---:|
| `MATCH` | TBD | TBD | TBD |
| `V2_ONLY` | TBD | TBD | TBD |
| `LEGACY_ONLY` | TBD | TBD | TBD |
| `CRS_CONFLICT` | TBD | TBD | TBD |
| `TRANSFORMATION_MISMATCH` | TBD | TBD | TBD |
| `BLOCKED` | TBD | TBD | TBD |

```yaml
eligibleRealUtm30MatchRate: TBD
unexplainedLegacyOnlyUtm30: TBD
unresolvedCrsConflict: TBD
unresolvedTransformationMismatch: TBD
```

## 5. Sample Observation Records

Do not include raw coordinates, KML bodies, OCR text, image content, signed
URLs, secrets, or direct user identifiers.

| Observation ID | Sample class | Legacy Result | V2 Result | Gate | Export compare | Rollback available | Final status |
|---|---|---|---|---|---|---|---|
| TBD | `REAL_UTM30` | `utm30n-projected-x-y` | `EPSG:32630` | TBD | TBD | TBD | TBD |
| TBD | `REAL_V2_UTM` | unavailable | `EPSG:32750` | TBD | TBD | TBD | TBD |
| TBD | `SAFETY_BLOCK` | TBD | unavailable | `BLOCKED` | not run | TBD | `BLOCKED` |

## 6. Legacy versus V2 Compatibility

```yaml
utm30ComparedSamples: TBD
crsExactMatches: TBD
pointCountMatches: TBD
pointOrderMatches: TBD
maximumTransformationDifference: TBD
approvedTolerance: TBD
result: NOT_RUN
```

Non-`MATCH` real UTM30 observations:

| Observation ID | Status | Reason | User impact | Resolution | Reviewer |
|---|---|---|---|---|---|
| TBD | TBD | TBD | none expected | TBD | TBD |

## 7. Export Compare

```yaml
legacyKmlCandidatesCompared: TBD
canonicalKmlCandidatesCompared: TBD
documentNameMatches: TBD
placemarkCountMatches: TBD
geometryMatches: TBD
pointSequenceMatches: TBD
maximumCoordinateDifference: TBD
approvedTolerance: TBD
exportCompareFailures: TBD
result: NOT_RUN
```

Export mismatches:

| Observation ID | Reason | Legacy hash | Canonical hash | Resolution | Reviewer |
|---|---|---|---|---|---|
| TBD | TBD | TBD | TBD | TBD | TBD |

## 8. Migration Gate Decisions

| Gate decision | Count | Notes |
|---|---:|---|
| `V2_ALLOWED` | TBD | Diagnostic only; no canonical output |
| `LEGACY_ONLY` | TBD | TBD |
| `BLOCKED` | TBD | TBD |

Gate integrity checks:

- [ ] Every Gate input belongs to the same request and point sequence.
- [ ] Invalid projected coordinates were blocked.
- [ ] Stale transformations were blocked.
- [ ] Point-count mismatches were blocked.
- [ ] User confirmation and Quality Gate remained mandatory.

## 9. Safety Isolation

| Protection | Result | Evidence |
|---|---|---|
| BFTM isolation | NOT_RUN | TBD |
| MGRS blocking | NOT_RUN | TBD |
| Kyrgyz GK isolation | NOT_RUN | TBD |
| Unknown projected X/Y blocking | NOT_RUN | TBD |
| DMS behavior unchanged | NOT_RUN | TBD |
| Legacy UTM30 behavior unchanged | NOT_RUN | TBD |

## 10. Rollback Availability

```yaml
currentMode: legacy
legacyAuthorityReachable: TBD
productionKillSwitchIntegrated: false
lastRollbackDrillEnvironment: isolated
lastRollbackDrillAt: TBD
lastRollbackDrillOwner: TBD
lastRollbackDrillResult: ISOLATED_ONLY
productionRollbackResult: NOT_RUN
rollbackOwner: TBD
```

Rollback verification:

- [ ] `controlled -> legacy` was exercised against the target production
      architecture.
- [ ] Legacy UTM30 recognition and Export recovered.
- [ ] Protected non-UTM paths remained unchanged.
- [ ] Historical data, baselines, and KML were not rewritten.
- [ ] Rollback trigger, time, owner, and recovery evidence were recorded.

## 11. Regression Evidence

| Suite | Commit | Result | Evidence location |
|---|---|---|---|
| Shadow Intent | TBD | NOT_RUN | TBD |
| CRS Evidence | TBD | NOT_RUN | TBD |
| Typed UTM | TBD | NOT_RUN | TBD |
| Migration Observation | TBD | NOT_RUN | TBD |
| Migration Gate | TBD | NOT_RUN | TBD |
| CRS Confirmation | TBD | NOT_RUN | TBD |
| Migration Infrastructure | TBD | NOT_RUN | TBD |
| Real-image UTM | TBD | NOT_RUN | TBD |

## 12. Stop Conditions and Incidents

| Time | Observation ID | Trigger | Action | Legacy impact | Resolution |
|---|---|---|---|---|---|
| TBD | TBD | TBD | observation paused | none expected | TBD |

```yaml
stopConditionTriggered: TBD
unresolvedIncidentCount: TBD
observationWindowCompleted: false
```

## 13. Evidence and Sign-off

```yaml
observationDataHash: TBD
summaryArtifactHash: TBD
regressionReportHash: TBD
exportCompareReportHash: TBD
rollbackReportHash: TBD
ownerApproval: TBD
reviewerApprovals: TBD
approvalTime: TBD
```

## 14. Final Decision

```text
HOLD_LEGACY
```

Decision rationale:

- The template is incomplete until the real observation window, Export Compare,
  rollback evidence, regression evidence, and reviews are filled.
- `READY_FOR_CONTROLLED_MIGRATION` may replace `HOLD_LEGACY` only after every
  Phase 5-2 exit criterion is satisfied and independently approved.
- Completing this report does not switch the runtime to `controlled`.
