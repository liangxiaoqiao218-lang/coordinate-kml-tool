# UTM Controlled Cohort Observation Specification

## 1. Status and Boundary

Phase 5-2 defines a real-request observation contract while the migration
decision remains `HOLD_LEGACY`.

Despite the phase name, this observation does not enable canonical authority.
The required runtime relationship is:

```text
Real request
    |
    +--> Legacy path ----------> authoritative result and user output
    |
    +--> V2 canonical shadow --> compare only
                                  |
                                  +--> Migration Gate
                                  +--> Export Compare
                                  +--> Observation Report
```

The legacy UTM30 parser, `precisionMode`, Export path, API response, and user
flow remain unchanged. Canonical failures must be isolated from latency,
availability, and output of the authoritative legacy request.

Phase 5-2 cannot enter `controlled` mode. It collects evidence for a later
`READY_FOR_CONTROLLED_MIGRATION` decision.

## 2. Observation Goal

The cohort must determine whether real eligible requests reproduce the verified
fixture behavior:

- Legacy UTM30 and V2 WGS84 UTM Zone 30 North produce `MATCH`.
- Explicit Indonesia WGS84 UTM Zone 50 South produces `V2_ONLY` without
  changing legacy behavior.
- Incomplete CRS evidence produces `LEGACY_ONLY` or `BLOCKED`, never a guessed
  canonical result.
- CRS disagreement produces `CRS_CONFLICT`.
- point-count or coordinate disagreement produces `TRANSFORMATION_MISMATCH`.
- MGRS and unsafe projected-X/Y fallbacks remain `BLOCKED`.

The observation must also prove that Legacy/Canonical KML comparison and
rollback availability can be evaluated without returning canonical output to
the user.

## 3. Cohort Definition

Before collecting a real observation, the cohort owner must record:

```yaml
cohortId: required
environment: required
branchOrRelease: required
startTime: required
endTime: required
minimumEligibleSampleCount: required
samplingRule: required
owner: required
reviewers: required
retentionPolicy: required
```

No default duration or sample count is implied by this specification. Missing
cohort criteria keep the decision at `HOLD_LEGACY`.

### 3.1 Eligible requests

The initial cohort may observe:

- real requests for which the legacy path returns
  `utm30n-projected-x-y`;
- real requests with explicit, complete WGS84 UTM evidence that produce a V2
  typed result;
- real projected-X/Y requests that reach a documented safety block and are
  needed to measure negative protection.

### 3.2 Excluded requests

The cohort must exclude:

- synthetic regression fixtures from real-request statistics;
- requests without consent or authorization for operational observation;
- raw images, full OCR text, coordinates, or user identifiers from the summary
  report;
- requests whose legacy result was changed by unrelated code during the
  observation window;
- canonical results that bypass CRS confirmation, Quality Gate, Migration Gate,
  or Export Compare.

Regression fixtures remain a separate safety suite and must never inflate the
real cohort `MATCH` rate.

## 4. Observation Record

Each eligible request produces a redacted diagnostic record. It must not enter
the production API response.

```json
{
  "schemaVersion": "utm_controlled_cohort_observation_v1",
  "observationId": "opaque-id",
  "cohortId": "cohort-id",
  "observedAt": "ISO-8601",
  "sampleClass": "REAL_UTM30|REAL_V2_UTM|SAFETY_BLOCK",
  "authoritativePath": "legacy",
  "legacyResult": {
    "available": true,
    "type": "utm30n-projected-x-y",
    "pointCount": 8,
    "resultHash": "sha256"
  },
  "v2Result": {
    "available": true,
    "coordinateType": "utm_projected_xy",
    "datum": "WGS84",
    "zone": 30,
    "hemisphere": "north",
    "epsg": "EPSG:32630",
    "pointCount": 8,
    "resultHash": "sha256"
  },
  "migrationObservation": {
    "status": "MATCH",
    "maximumDifference": 0,
    "tolerance": 1e-8
  },
  "migrationGate": {
    "decision": "V2_ALLOWED",
    "reason": ["LEGACY_V2_MATCH"]
  },
  "exportCompare": {
    "status": "MATCH",
    "maximumDifference": 0,
    "geometryTypes": ["Polygon"]
  },
  "rollbackAvailability": {
    "mode": "legacy",
    "legacyReachable": true,
    "killSwitchAvailable": false,
    "lastDrillStatus": "ISOLATED_ONLY"
  },
  "finalStatus": "MATCH"
}
```

Hashes must be derived from canonicalized diagnostic data and cannot be used to
reconstruct coordinates. Secret values, signed URLs, raw OCR, image bytes, KML
content, and direct user identifiers are prohibited in observation records.

## 5. Required Status Classification

Every eligible record receives exactly one final status:

- `MATCH`: Legacy and V2 CRS, point count, transformed result, and Export
  comparison agree within approved tolerances.
- `V2_ONLY`: V2 has a confirmed typed result and legacy has no comparable UTM
  result. Canonical output remains shadow-only.
- `LEGACY_ONLY`: Legacy has a result but V2 lacks a complete eligible result.
- `CRS_CONFLICT`: explicit CRS evidence conflicts with legacy or typed CRS.
- `TRANSFORMATION_MISMATCH`: CRS agrees but transformed coordinates or point
  counts disagree.
- `BLOCKED`: Migration Gate, MGRS protection, unknown projected X/Y, invalid
  projected coordinates, stale transformation, or Export Compare prevents a
  safe result.

`BLOCKED` is an observation outcome, not a fallback permission. A blocked V2
result cannot alter the legacy result or trigger a DMS/projected-X/Y fallback.

## 6. Legacy and V2 Comparison

For each UTM30 observation:

1. Record the unchanged legacy type and point count.
2. Record the V2 typed CRS without raw coordinates.
3. Compare CRS fields exactly.
4. Compare transformed point count and order.
5. Compare longitude and latitude within the approved tolerance.
6. Run Migration Gate using the same accepted typed result.
7. Classify the record before any Export comparison.

For UTM50S and other `V2_ONLY` observations, the report must state that no
legacy compatibility claim is possible. `V2_ONLY` contributes capability
evidence but does not increase the UTM30 `MATCH` rate.

## 7. Export Compare Observation

Export observation must compare the actual legacy KML candidate with a
shadow-only canonical KML candidate for:

- document and Placemark names;
- Placemark count;
- geometry type;
- point count and order;
- longitude, latitude, and altitude;
- approved coordinate tolerance.

The canonical candidate must consume the accepted Typed Result. It must not
reparse OCR or infer CRS. Neither KML body is retained in the summary report;
only status, hashes, geometry, point counts, tolerance, and maximum difference
are retained.

`EXPORT_COMPARE_FAILED` maps to final status `BLOCKED` and triggers review. It
cannot degrade to `LEGACY_ONLY` merely to make a cohort pass.

## 8. Migration Gate Observation

The report records the Gate decision and stable reasons:

- `V2_ALLOWED` is diagnostic only and does not authorize canonical output in
  Phase 5-2.
- `LEGACY_ONLY` keeps legacy authoritative and requires classification review.
- `BLOCKED` prevents canonical use and may trigger an observation stop.

The Gate input, Typed Result, and Export Compare must refer to the same request
and point sequence. Stale or cross-request results invalidate the observation.

## 9. Rollback Availability

Every observation window records:

- current migration mode;
- whether legacy authority is reachable;
- whether the production kill switch is integrated;
- last rollback drill environment, time, owner, and result;
- recovery verification for Legacy UTM30 and protected non-UTM types.

During Phase 5-2 the expected mode is `legacy` or an explicitly approved
shadow-observation mode whose authority is still legacy. The isolated Phase 5-1
`controlled -> legacy` drill is recorded as `ISOLATED_ONLY`, not production
rollback proof.

If legacy authority is unavailable, observation must stop immediately. No
cohort result can be marked ready without an integrated and tested rollback.

## 10. Stop Conditions

Observation pauses and the project remains `HOLD_LEGACY` when any of these
occurs:

- unexplained real `CRS_CONFLICT`;
- unexplained real `TRANSFORMATION_MISMATCH`;
- canonical/legacy Export mismatch;
- canonical processing changes legacy latency, availability, response, or KML;
- invalid or stale typed data passes a Gate;
- BFTM, MGRS, Kyrgyz GK, DMS, or unknown-X/Y routing changes;
- observation records cannot be correlated safely to one request;
- redaction, retention, telemetry, or rollback availability fails.

The affected request remains on the existing safe legacy behavior. A stop does
not authorize a corrective production migration within this phase.

## 11. Report and Decision Rules

The completed report must include:

- cohort metadata and immutable reviewed scope;
- total, eligible, excluded, and failed-observation counts;
- counts for all six final statuses;
- UTM30 `MATCH` rate using only eligible real UTM30 observations;
- every non-`MATCH` real UTM30 observation and its resolution;
- Export Compare results;
- Gate decision counts;
- rollback availability and drill evidence;
- regression commit and results;
- reviewer sign-off.

The report decision is limited to:

```text
HOLD_LEGACY
READY_FOR_CONTROLLED_MIGRATION
```

Empty fields, an incomplete observation window, unavailable rollback, or an
unresolved stop condition require `HOLD_LEGACY`. A report may recommend
`READY_FOR_CONTROLLED_MIGRATION` only after all Phase 4D preconditions are
independently reviewed. It still does not switch the runtime mode.

## 12. Phase 5-2 Exit Criteria

Phase 5-2 is complete only when:

1. The predefined real observation window and minimum eligible sample count are
   satisfied.
2. Legacy remained authoritative for every observed request.
3. All eligible real UTM30 observations are accounted for, with no unresolved
   conflict or transformation mismatch.
4. Actual Legacy/Canonical Export comparison is complete for the defined UTM30
   cohort.
5. Negative-type routing and unknown-X/Y blocking remain unchanged.
6. The production rollback mechanism is available and its drill evidence is
   recorded, or the final decision remains `HOLD_LEGACY`.
7. The observation report is reviewed and signed off separately from any
   production migration implementation.

Until these conditions are met, the authoritative state remains:

```text
HOLD_LEGACY
```
