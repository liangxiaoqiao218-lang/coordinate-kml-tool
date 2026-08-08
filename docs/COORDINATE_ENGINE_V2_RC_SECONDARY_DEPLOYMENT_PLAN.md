# Coordinate Engine V2 RC Secondary Deployment Plan

## Status

```text
Plan status: READY FOR REVIEW
Deployment status: NOT STARTED
Secondary validation: INCOMPLETE
Primary Production: BLOCKED
UTM migration: HOLD_LEGACY
```

This document defines a controlled Secondary deployment procedure. It does not
authorize deployment, merge the V2 branch into `main`, or change production
migration authority.

## 1. Deployment Objective

The objective is to deploy an explicitly approved Coordinate Engine V2 release
candidate artifact to the Secondary environment for validation under its real
build and runtime conditions.

The deployment is intended to collect Secondary evidence for:

- coordinate-type arbitration;
- CRS confirmation and KML safety;
- protected legacy coordinate types;
- quota and entitlement behavior;
- artifact identity and runtime reproducibility;
- rollback execution.

Secondary is used only for RC validation. A successful Secondary deployment or
validation result does not constitute a Primary release, a production migration
approval, or authorization to make canonical UTM authoritative.

## 2. Secondary Environment

| Field | Value |
|---|---|
| URL | `https://coordinate-kml-tool.onrender.com` |
| Governance role | Secondary Production |
| RC role | Controlled validation environment |
| Business authority | None; Primary remains authoritative |
| Payment | Must remain disabled |
| Migration authority | Legacy or separately approved shadow only |
| Primary environment | `https://geokitlab.com` must remain unchanged |

Secondary Production is not a disposable staging environment. Deployment must
therefore use a bounded validation window, an identified rollback artifact, and
explicit abort conditions. User traffic must not be redirected from Primary as
part of this plan.

## 3. Pre-deployment Checklist

All checklist items require recorded evidence before deployment starts.

### 3.1 RC source identity

- [ ] Approved RC branch is `v2/utm-intent-router`.
- [ ] Planned RC branch commit is recorded. At plan creation it is
  `e30b2cc36a4e7f7fa327fc56f79aae7690937ad3`.
- [ ] Coordinate Type Arbitration implementation commit
  `095168fc44b697ab11f8e4af2277704e9bd240cc` is an ancestor of the planned RC.
- [ ] Remote branch and reviewed local commit are identical.
- [ ] Working tree used to build the artifact is clean.
- [ ] No unapproved commit is introduced after approval.

If the branch HEAD changes, record the new commit and repeat the approval and
regression review. Do not silently substitute a newer branch head.

### 3.2 Artifact identity

- [ ] Build the RC from the approved commit using the governed build process.
- [ ] Record artifact identifier and SHA-256 hash.
- [ ] Record build time, runtime version, package-lock identity, and build logs.
- [ ] Verify the artifact contains no `.env` files, credentials, local fixtures,
  screenshots, or user data.
- [ ] Verify the version endpoint or release manifest can identify the running
  RC after deployment.

### 3.3 Rollback target

- [ ] Record the exact commit and artifact currently running in Secondary.
- [ ] Confirm that the previous artifact is retained and deployable.
- [ ] Record its artifact hash and successful health-check result.
- [ ] Assign a rollback owner and approver.
- [ ] Define maximum rollback response time.
- [ ] Confirm Primary remains available and unchanged during the window.

The development reference
`838e4bfedb159a6fe062db33c5f50eea95dbd667` is not automatically the Secondary
rollback target. Rollback must restore the exact pre-deployment Secondary
artifact recorded immediately before execution.

### 3.4 Environment variables and controls

Record variable presence and intended environment, never secret values.

- [ ] Vision and OCR provider configuration is present.
- [ ] Supabase configuration points to the approved governed data source.
- [ ] runtime mode and public URL are correct for Secondary.
- [ ] pricing and entitlement configuration is validated.
- [ ] payment feature and payment callbacks are disabled in Secondary.
- [ ] migration control defaults to `legacy` or the separately approved
  Secondary shadow setting.
- [ ] controlled canonical authority cannot initialize through configuration.
- [ ] regression-test mode is disabled in the deployed service.
- [ ] admin, debug, and secret values are not exposed by logs or API responses.

### 3.5 Readiness and communication

- [ ] Secondary validation owner is identified.
- [ ] deployment window and expected duration are recorded.
- [ ] validation fixtures and approved test identities are ready.
- [ ] monitoring and error-log access is available.
- [ ] stakeholders understand that Secondary may be rolled back immediately.
- [ ] no Primary deployment, DNS change, or traffic failover is included.

## 4. Deployment Steps

Execution requires a separate approval record. After approval:

1. Freeze and record the exact Secondary pre-deployment state.
2. Confirm the rollback artifact can be selected without rebuilding it.
3. Confirm the RC source commit and artifact hash match the approved record.
4. Confirm payment is disabled and migration authority is not controlled.
5. Start the bounded Secondary validation window.
6. Deploy the approved immutable RC artifact to Secondary only.
7. Wait for the platform deployment to finish without changing Primary.
8. Record deployment ID, deployment time, region, runtime, and artifact identity.
9. Verify health and version endpoints before sending image test requests.
10. Run a minimal safety smoke test:
    - service health;
    - one protected legacy coordinate type;
    - one UTM50S request that remains blocked before confirmation;
    - payment remains disabled.
11. If the smoke test passes, execute the full Secondary Validation Plan.
12. Record all observations in the Secondary Validation Report.
13. At the end of the window, either retain the RC for an explicitly approved
    observation period or restore the recorded Secondary baseline.

Do not rebuild inside the running service, edit deployed files manually, change
environment variables without recording them, or deploy directly from an
uncommitted worktree.

## 5. Post-deployment Validation

### 5.1 Coordinate types

Run the approved matrix and record API and UI results:

- Indonesia UTM50S 01, 02, and 03;
- Legacy UTM30;
- BFTM;
- MGRS;
- Kyrgyz GK;
- ordinary and handwritten DMS;
- WGS84 table and WGS84 chat coordinates;
- Cote d'Ivoire dedicated geographic DMS samples.

For every sample verify `coordinateType`, `precisionMode`, `parserTrace`, CRS,
confirmation status, Quality Gate status, review status, KML readiness, and KML
semantic output where export is allowed.

### 5.2 KML flow

- verify UTM50S resolves to `EPSG:32750`;
- verify UTM50S is not KML-ready before user confirmation;
- verify accepted confirmation exports only the validated typed result;
- verify explicit CRS conflicts cannot export;
- verify transformation mismatches cannot export;
- verify possible swapped latitude/longitude cannot export;
- verify protected legacy types retain their expected KML behavior;
- verify KML filename, MIME type, geometry, and coordinate order.

No generated KML may be accepted solely because it downloads successfully. Its
CRS, geometry, coordinate order, and semantic comparison must pass.

### 5.3 Quota

- verify quota displays correctly before recognition;
- verify one successful recognition consumes exactly one expected unit;
- verify retry and error paths do not double-charge;
- verify blocked or repeated KML download does not consume another recognition
  unit;
- verify exhausted quota retains the existing API and UI behavior.

### 5.4 VIP and entitlement

- verify approved VIP and non-VIP test identities;
- verify the governed shared entitlement source remains authoritative;
- verify the RC creates no Secondary-only membership state;
- verify entitlement checks do not bypass CRS confirmation or KML safety;
- verify no payment control is enabled in Secondary.

### 5.5 Download

- verify valid KML can be downloaded through the normal user flow;
- verify blocked results produce no misleading file;
- verify repeat download behavior and quota invariants;
- verify browser and API behavior remain compatible with existing clients.

### 5.6 Runtime

- record Node.js and dependency versions;
- verify image-processing dependencies load successfully;
- verify Vision/OCR and approved database connectivity;
- observe memory, request duration, upload limits, provider errors, and timeouts;
- confirm logs contain no credentials or user coordinate payloads beyond the
  approved validation record;
- compare error and latency behavior with the recorded Secondary baseline.

## 6. Rollback Procedure

Rollback must be executed when an abort condition occurs or when the approved
validation window requires restoration.

1. Stop new RC validation requests.
2. Record the triggering observation and timestamp.
3. Select the pre-recorded immutable Secondary artifact.
4. Restore the pre-deployment environment configuration without copying RC-only
   flags into the rollback service.
5. Deploy the recorded rollback artifact to Secondary.
6. Verify health and version identity.
7. Run the legacy smoke matrix:
   - one ordinary DMS sample;
   - one protected projected type;
   - quota and entitlement read;
   - valid legacy KML download;
   - payment remains disabled.
8. Confirm no Primary change occurred.
9. Update the validation report with rollback duration, result, and evidence.
10. Mark Secondary validation `FAIL` or `INCOMPLETE`; do not resume RC traffic
    without a new approval.

Rollback is complete only when the running Secondary artifact matches the
recorded rollback identity and its smoke tests pass.

## 7. Abort Conditions

Abort deployment before traffic testing when:

- source commit or artifact hash does not match approval;
- the current Secondary rollback artifact cannot be identified or restored;
- required environment configuration is missing or points to an unapproved
  target;
- payment is enabled;
- migration control is `controlled`;
- regression-test mode is enabled in the deployed environment;
- the service cannot prove its running version;
- Primary configuration or routing would be changed.

Abort validation and roll back immediately when:

- an unsafe or geographically incorrect KML is produced;
- explicit CRS conflict, transformation mismatch, or swapped coordinate order
  bypasses the KML gate;
- UTM50S falls back to DMS or legacy UTM30;
- a protected legacy coordinate type changes unexpectedly;
- quota is double-charged or entitlement state is corrupted;
- a credential or sensitive payload appears in logs or responses;
- health, latency, timeout, or error rates exceed the approved stop threshold;
- the running artifact becomes untraceable;
- the rollback owner requests termination.

Missing non-safety evidence without a failure results in `INCOMPLETE` and Hold.
Any safety, identity, entitlement, or rollback failure results in `FAIL` and
rollback.

## Final Boundary

```text
Secondary deployment plan: READY FOR REVIEW
Secondary deployment: NOT STARTED
Secondary validation: INCOMPLETE
Primary Production: BLOCKED
UTM migration: HOLD_LEGACY

Secondary PASS != Primary Approval
```
