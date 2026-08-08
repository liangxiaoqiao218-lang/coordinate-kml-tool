# Coordinate Engine V2 RC Secondary Validation Plan

## 1. Validation Objective

The objective is to validate the Coordinate Engine V2 release candidate in an
isolated Secondary environment before any Primary Production proposal.

The validation must determine whether commit
`ac31b43582b4757c6a243dd5e3549043245afe44`, containing the packaged candidate
whose implementation commit is
`095168fc44b697ab11f8e4af2277704e9bd240cc`, behaves consistently under the
Secondary runtime, build, network, account, quota, and KML workflows.

The plan must provide evidence for:

- accurate coordinate-type arbitration;
- preservation of protected legacy coordinate types;
- correct CRS confirmation and KML blocking behavior;
- unchanged quota and entitlement behavior;
- reproducible build and runtime identity;
- an executable rollback path.

This plan does not authorize a deployment. Execution requires a separate
Secondary deployment approval and deployment record.

## 2. Secondary Environment Role

The Secondary environment is the RC validation target. It is not the business
authority and is not equivalent to Primary Production.

| Environment | Role during validation | Authority |
|---|---|---|
| Local V2 worktree | Source and regression reference | Development only |
| Secondary / `coordinate-kml-tool.onrender.com` | RC runtime validation | Validation only |
| Primary / `geokitlab.com` | Formal production service | Must remain unchanged |

Secondary validation must not:

- enable Primary traffic routing;
- change Primary code, data, or environment variables;
- enable payment processing in Secondary;
- make canonical UTM authoritative for production;
- merge the V2 branch into `main`;
- change the `HOLD_LEGACY` migration decision.

Secondary evidence must be labelled separately from local regression and
Primary Production evidence.

## 3. Validation Scope

### 3.1 Coordinate-type matrix

| Type | Required behavior | Safety assertion |
|---|---|---|
| Indonesia UTM50S | `utm_projected_xy`, `utm-projected-x-y`, `EPSG:32750` | Await confirmation and keep `kml_ready=false` before acceptance |
| Legacy UTM30 | Preserve `utm30n-projected-x-y` compatibility | No forced database, baseline, or historical-data migration |
| BFTM | Preserve `bftm-projected-x-y` | Must not be reclassified as UTM |
| MGRS | Preserve `mgrs-utm-grid-reference` | Must block numeric UTM takeover |
| Kyrgyz GK | Preserve `kyrgyz-gk-point-x-y` | Must not be inferred as UTM from numeric values |
| DMS | Preserve the applicable DMS dedicated or generic mode | Projected X/Y must not be replaced by unrelated DMS evidence |
| WGS84 table | Preserve `wgs84-table-coordinates` | Possible swapped latitude/longitude must block KML |
| WGS84 chat | Preserve `wgs84-chat-coordinates` | Possible swapped latitude/longitude must require review |

### 3.2 Required image coverage

At minimum, execute:

- Indonesia mine land 01, 02, and 03;
- the locked Legacy UTM30 sample;
- one locked BFTM sample;
- one locked MGRS sample;
- one locked Kyrgyz GK sample;
- representative ordinary and handwritten DMS samples;
- representative WGS84 table and chat-coordinate samples;
- Cote d'Ivoire 03 and 04 as dedicated geographic DMS protection samples.

For every result, record:

- sample identifier and non-sensitive fixture hash;
- `coordinateType`;
- `precisionMode`;
- CRS/EPSG when applicable;
- `parserTrace`;
- `confirmationStatus`;
- `qualityGateStatus`;
- `kml_ready` and `requires_review`;
- generated KML comparison status when export is permitted.

### 3.3 Arbitration and API assertions

The Secondary response must retain existing response fields while adding the V2
arbitration contract. Confirm that clients still receive `rawText`,
`coordinates`, `quota`, `parserTrace`, and `coordinateEngineV2` where applicable.

The following must be blocked:

- explicit CRS conflict;
- transformation mismatch;
- incomplete or unknown projected CRS;
- possible swapped latitude/longitude;
- canonical UTM export before required user confirmation.

## 4. Business Flow Validation

Business validation must use approved test identities and must not create real
payment transactions.

### 4.1 Quota

- confirm the initial quota is displayed correctly;
- confirm one successful recognition consumes the expected unit exactly once;
- confirm blocked KML generation does not incorrectly consume another image
  recognition unit;
- confirm exhausted quota produces the existing limit response and UI;
- confirm retry and error paths do not double-charge usage;
- compare Secondary behavior with the recorded legacy expectation.

### 4.2 VIP and entitlement

- confirm an approved VIP test account receives the configured recognition and
  export capabilities;
- confirm a non-VIP test account remains subject to the configured limits;
- confirm entitlement data comes from the governed shared source;
- confirm the RC adds no Secondary-specific membership state;
- confirm payment controls remain disabled in Secondary.

### 4.3 KML permission

- confirm ordinary validated legacy types retain their established KML path;
- confirm UTM50S remains blocked while confirmation is pending;
- confirm accepted CRS confirmation enables only the validated typed result;
- confirm explicit CRS conflict and swapped coordinate order remain blocked;
- confirm no API caller can bypass `confirmationStatus` or
  `qualityGateStatus` by skipping the UI.

### 4.4 Download flow

- confirm the expected filename, MIME type, geometry, and coordinate order;
- compare Legacy UTM30 KML with its established baseline;
- compare canonical UTM transformation output with the verified WGS84 reference;
- confirm cancelled, blocked, or failed validation creates no misleading KML;
- confirm repeated downloads do not create additional recognition charges.

## 5. Environment Validation

### 5.1 Version and build identity

Record before testing:

- RC source commit;
- build identifier and artifact hash;
- build time;
- deploy time;
- Secondary service identifier;
- previous Secondary artifact and commit;
- rollback target.

The running application version must match the approved RC source. A deployment
with an unknown commit or artifact is invalid for this plan.

### 5.2 Environment variables

Confirm presence, not values, for required configuration categories:

- Vision/OCR provider;
- Supabase URL and service credentials;
- pricing and entitlement configuration;
- runtime mode and public URL;
- feature and payment switches;
- migration control mode.

Do not record secret values in logs, screenshots, manifests, or reports.
Secondary payment must remain disabled, and migration authority must remain
legacy or shadow as approved. Controlled mode is not permitted by this plan.

### 5.3 Runtime and dependencies

Record and verify:

- Node.js runtime version;
- package lock identity;
- production dependency installation result;
- image-processing dependency availability;
- process start command;
- health/version endpoint result;
- outbound Vision Provider connectivity;
- database connectivity required for approved test identities;
- memory, timeout, and upload-size limits used by the service.

No dependency may be installed manually into a running instance without being
represented by the approved lockfile and build artifact.

## 6. Rollback Plan

Before RC validation begins:

1. identify and record the currently running Secondary commit and artifact;
2. verify that artifact is recoverable;
3. name the rollback owner;
4. define the rollback trigger and maximum response time;
5. confirm Primary remains untouched throughout the drill.

Rollback triggers include:

- unsafe or incorrect KML;
- quota or entitlement corruption;
- protected legacy type regression;
- elevated API errors or timeouts;
- inability to identify the running artifact;
- CRS conflict or review results bypassing the KML gate.

The drill sequence is:

```text
Secondary legacy baseline
    -> approved RC artifact
    -> validation smoke test
    -> rollback trigger
    -> previous Secondary artifact
    -> legacy smoke test
```

Record timestamps, artifact identities, service health, validation results, and
the person responsible. The pre-Arbitration development reference
`838e4bfedb159a6fe062db33c5f50eea95dbd667` is a code comparison reference; the
actual Secondary rollback target must be the artifact recorded immediately
before deployment.

## 7. Exit Criteria

Secondary validation may be marked `PASS` only when all of the following are
recorded:

- the running RC commit and artifact are proven;
- the complete coordinate-type matrix passes;
- Indonesia UTM50S produces `EPSG:32750` and cannot export before confirmation;
- Legacy UTM30 and all protected types retain their expected precision modes and
  parser traces;
- no CRS conflict, transformation mismatch, or coordinate-order warning reaches
  KML export;
- quota consumption is correct and contains no double charge;
- VIP and non-VIP entitlements behave as configured;
- payment remains disabled in Secondary;
- API and download flows pass;
- no unsafe KML is produced;
- the rollback drill restores the recorded Secondary baseline;
- all observations, failures, and deviations are documented;
- no unresolved severity-high finding remains.

Any missing evidence results in `INCOMPLETE`. Any unsafe KML, entitlement
corruption, protected-type regression, untraceable artifact, or failed rollback
results in `FAIL` and requires immediate rollback.

The final Secondary decision must be one of:

```text
PASS
FAIL
INCOMPLETE
```

`PASS` means only that the RC met the Secondary validation plan. It does not
approve a Primary release, change migration authority, or satisfy the required
production cohort evidence.

```text
Validation PASS != Primary Release Approval
Primary Production: BLOCKED
UTM Migration: HOLD_LEGACY
```
