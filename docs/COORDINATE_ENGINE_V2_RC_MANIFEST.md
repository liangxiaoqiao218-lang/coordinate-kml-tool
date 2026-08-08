# Coordinate Engine V2 Release Candidate Manifest

## Candidate Identity

| Field | Value |
|---|---|
| RC identifier | `coordinate-engine-v2-rc1` |
| Branch | `v2/utm-intent-router` |
| Source commit | `095168fc44b697ab11f8e4af2277704e9bd240cc` |
| Source message | `feat: add coordinate type arbitration layer` |
| Packaging date | `2026-08-08` |
| Candidate status | `ALLOWED` |
| Production status | `HOLD` |
| UTM migration status | `HOLD_LEGACY` |

## Candidate Scope

This candidate packages:

- UTM CRS Evidence Acquisition;
- UTM Intent resolution;
- typed UTM results and WGS84 transformation validation;
- Coordinate Type Arbitration;
- unified coordinate response finalization;
- CRS confirmation and KML safety gates;
- migration observation, validation, control, and rollback design.

It does not authorize a production migration or alter the governance decision
recorded by the UTM Migration Approval Gate.

## Validation Record

| Validation | Result |
|---|---|
| Live Vision Provider and OCR | `PASS` |
| Live CRS Evidence | `6/6 PASS` |
| Live structured UTM images | `3/3 PASS` |
| Live Arbitration and Finalizer images | `5/5 PASS` |
| Coordinate Type Arbitration | `5/5 PASS` |
| Dedicated type protection | `4/4 PASS` |
| Response Finalizer | `7/7 PASS` |
| Coordinate Verification | `9/9 PASS` |
| CRS Confirmation UI | `4/4 PASS` |
| Typed UTM | `6/6 PASS` |
| Migration Observation | `8/8 PASS` |
| Migration Gate | `17/17 PASS` |
| Migration Infrastructure | `12/12 PASS` |
| Working-tree diff check at implementation commit | `PASS` |

The Live Arbitration and Finalizer image set contains three Indonesia UTM50S
images and two Cote d'Ivoire DMS images. Indonesia resolves to `EPSG:32750` and
remains pending user confirmation; the Cote d'Ivoire samples retain their
dedicated geographic DMS type.

## Intended Test Environment

| Environment | Role | RC state |
|---|---|---|
| Local V2 worktree | Build and regression authority | Validated |
| Secondary Production / Render | Candidate test target only | Not deployed |
| Primary Production / `geokitlab.com` | Formal production authority | Deployment prohibited pending approval |

Any Secondary deployment requires a separate deployment record with the exact
source commit, artifact identity, deploy time, environment validation, and
rollback result. Packaging this manifest is not a deployment action.

## Compatibility and Risk Record

### Preserved contracts

- legacy response data is retained by the response finalizer;
- legacy `precisionMode` and `parserTrace` values are preserved for protected
  coordinate types;
- legacy UTM30 remains available as `utm30n-projected-x-y`;
- BFTM, MGRS, Kyrgyz GK, DMS, WGS84 table, WGS84 chat, Mozambique, and
  handwritten DMS protection regressions pass;
- quota, VIP, payment, entitlement, and Supabase business logic is outside the
  implementation diff.

### Intentional semantic change

Explicit UTM CRS evidence no longer falls back to DMS or UTM30. Unknown or
incomplete UTM evidence is not completed from country names, numeric ranges, or
northing values.

### Remaining release risks

- no Secondary observation window;
- no production cohort;
- no production Export Compare record;
- no production rollback proof;
- no formal migration approval record;
- target-environment account, quota, VIP, and billing smoke tests remain to be
  executed before any Primary release proposal.

## Rollback Reference

| Field | Value |
|---|---|
| Pre-Arbitration reference | `838e4bfedb159a6fe062db33c5f50eea95dbd667` |
| Rollback purpose | Restore the V2 branch state before Coordinate Type Arbitration integration |
| Production rollback target | Not applicable; this RC has not been deployed |

Rollback must use a reviewed revert or deployment of a previously recorded
artifact. This manifest does not authorize destructive history rewriting or a
force push.

## Promotion Gates

Before a controlled test deployment:

1. record the Secondary artifact and environment configuration;
2. confirm the Primary environment remains unchanged;
3. confirm rollback ownership and target;
4. run target-environment API, KML, quota, and entitlement smoke tests;
5. start an auditable observation window.

Before any Primary Production proposal:

1. complete the controlled cohort evidence;
2. complete production-equivalent Export Compare evidence;
3. demonstrate rollback in the controlled environment;
4. record formal approval under the Migration Approval Gate;
5. retain the legacy UTM30 alias and rollback path.

## Decision

```text
RC package: ALLOWED
Secondary deployment: REQUIRES SEPARATE APPROVAL AND RECORD
Primary deployment: BLOCKED
Canonical UTM production authority: BLOCKED
Current production authority: LEGACY
```
