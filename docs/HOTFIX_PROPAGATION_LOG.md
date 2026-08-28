# GeoKit Lab Hotfix Propagation Log

Document status: `APPROVED_GOVERNANCE_BASELINE`  
Log owner: `TRACK-G`  
Log mode: `APPEND_ONLY`

## Append-only protocol

From G-02 approval onward, existing entries must not be edited or deleted. A correction or changed judgment is added as a new entry with a new timestamp, a reference to the superseded entry, and the evidence that changed the judgment.

A propagation status of `AUDIT_REQUIRED` is not permission to cherry-pick, merge, implement, deploy, or mutate authority. Propagation occurs only through a separately approved change owned by the target track.

Propagation lifecycle:

```text
AUDIT_REQUIRED
  -> AUDIT_COMPLETE_NO_CHANGE_REQUIRED
  or CHANGE_REQUIRED
       -> TARGET_TRACK_IMPLEMENTED
       -> TARGET_TRACK_VALIDATED
       -> PROPAGATION_COMPLETE
```

No target may reach `PROPAGATION_COMPLETE` before the source hotfix has production regression closure. A correction is appended as a new entry; the original entry remains unchanged.

---

## Entry G01-HF-0001

```text
LOGGED_AT=2026-08-28
HOTFIX_ID=P08F_LEGACY_KML_REVIEW_CONFIRMATION_RESTORATION
SOURCE_TRACK=TRACK-L
SOURCE_COMMIT=1da2ff78ce5d025b2e14870d332cb5354cc5e422
SOURCE_STATUS=PATCHED_TESTED_COMMITTED_PUSHED_NOT_PRODUCTION_DEPLOYED
PROPAGATION_PERFORMED=false
PROPAGATION_TO_TRACK_E=AUDIT_REQUIRED
PROPAGATION_TO_TRACK_S=AUDIT_REQUIRED
```

### Semantic change

| State | Required outcome |
| --- | --- |
| `review_required + confirmation=pending` | KML blocked; `kmlReady=false` |
| `review_required + confirmation=accepted` plus technical/safety/account/source/availability/CRS/geometry authority checks passing and confirmation bound to the current revision | `kmlReady=true`; KML allowed |
| `review_required + confirmation=rejected` | KML blocked; `kmlReady=false` |
| `qualityGateStatus=failed` | KML blocked; `kmlReady=false` |

Accepted confirmation does not rewrite `qualityGateStatus`; it remains `review_required` for this flow.

This restores Legacy/server KML authority after accepted review confirmation. It does not make Map Preview a KML authority and does not grant V3 authority.

### Semantic implementation files named for propagation audit

- `index.html`
- `server/coordinate-finalizer/unified-gate.js`
- `server/coordinate-finalizer/finalizer-inputs.js`
- `server/coordinate-finalizer/finalized-coordinate-result-v1.js`

### Source-commit validation files

The source commit also adds or changes these test files. They are recorded for complete source evidence, but this entry does not propagate them:

- `scripts/p08f-review-confirm-kml-regression.js`
- `scripts/sr08b-confirmation-runtime-regression.js`

### TRACK-E propagation audit

```text
TARGET_TRACK=TRACK-E
CURRENT_MODE=TRUE_SHADOW_ONLY
CURRENT_PRIVATE_SCHEMA=coordinate_engine_v3_production_result_v1
CANONICAL_ADAPTER=NOT_IMPLEMENTED
RESULT_IDENTITY_REVISION_HASH_GAP=OPEN
PROPAGATION_DECISION=AUDIT_REQUIRED
IMPLEMENTATION_AUTHORIZED=false
```

Audit question: whether a future V3 canonical adapter can reproduce P08F review/confirmation/KML semantics without importing Legacy UI or private gate implementation into V3. This is an Authority Migration prerequisite, not a blocker for internal shadow module development.

### TRACK-S propagation audit

```text
TARGET_TRACK=TRACK-S
CURRENT_ROLE=DOWNSTREAM_CONSUMER
CURRENT_MAP_ADAPTER=MapPreviewAdapter
MAP_KML_PERMISSION_SEPARATION=REQUIRED
PROPAGATION_DECISION=AUDIT_REQUIRED
IMPLEMENTATION_AUTHORIZED=false
```

Audit question: whether TRACK-S tests or warning presentation need compatibility updates for the changed canonical `decisionState`/`kmlReady` combination after accepted confirmation. TRACK-S must not copy the KML gate or use the export-grade adapter as a general map gate.

### Governance impact

```text
CROSS_TRACK_AUTHORITY_MUTATION=false
CURRENT_PRODUCTION_AUTHORITY=LEGACY_V2
KML_SPATIAL_SIBLING_RULE=PRESERVED
V3_PRODUCTION_AUTHORITY=false
P09_STATUS=HOLD
P08G_GOVERNANCE_READINESS=READY
P08G_GOVERNANCE_AUTHORIZATION=AUTHORIZED
```

TRACK-G does not execute P08G. G-02 authorization permits TRACK-L to execute `P08G_CONTROLLED_PRODUCTION_REDEPLOYMENT`. TRACK-E and TRACK-S remain `AUDIT_REQUIRED`; propagation cannot be marked complete until P08G production regression closure and the later target audits.

---

## Entry G06-HF-0002

This append-only entry records the G-06 post-production audit and supersedes only
the pending propagation judgments in `G01-HF-0001`. It does not alter that
historical entry.

```text
LOGGED_AT=2026-08-28
AUDIT_ID=G06_P08F_P08H_P08M_G05_PRODUCTION_CLOSURE
SUPERSEDES_PROPAGATION_JUDGMENT=G01-HF-0001
SOURCE_TRACK=TRACK-L
SOURCE_PRODUCTION_RELEASE=e925bfba20acc08df3a720293bd010303fbda701
SOURCE_PRODUCTION_HASH=965b2eee4aa3b6a80a872bb622bd1152c5d8dbbcc63c60189ac2942eb326a5aa
SOURCE_RELEASE_GOVERNANCE_HASH=4684a01f151446833c58f7cf8f587c9fe0ed902189c82f072f951cdfc9b24df7
SOURCE_FIXTURE_SET_HASH=dfa774517eabb18bfc47981c1511be0cca4c01dd92e2592803820f0fab435697
SOURCE_PRODUCTION_REGRESSION=CLOSED_PASS
CURRENT_PRODUCTION_AUTHORITY=LEGACY_V2
V3_PRODUCTION_AUTHORITY=false
```

### TRACK-E audit closure

```text
TARGET_TRACK=TRACK-E
AUDITED_BRANCH=v3/isolated-recognizers
AUDITED_COMMIT=c565c950f95c2c999fe75058c195b3988e121fb5
CURRENT_MODE=TRUE_SHADOW_ONLY
PRIMARY_PROPAGATION_CLASS=CONTRACT_AWARENESS_REQUIRED
CODE_PROPAGATION_REQUIRED=false
TEST_PROPAGATION_REQUIRED=false
GOVERNANCE_AWARENESS_REQUIRED=true
LIFECYCLE_STATUS=CHANGE_REQUIRED
CHANGE_SCOPE=FUTURE_AUTHORITY_MIGRATION_PREREQUISITE_RECORD_ONLY
V3_SHADOW_CONTINUE=true
V3_CANARY=BLOCKED
V3_MIGRATION=BLOCKED
```

TRACK-E must preserve the distinction between private V3 `technicalKmlReady`
and authoritative Legacy/server `kmlReady`. P08F/P08H review, revision-bound
confirmation, edit invalidation, reconfirmation, and final KML authority belong
in future M1/M2/M3/M5/M6 canonical-adapter evidence. They do not justify
importing the Legacy finalizer or confirmation UI into the current shadow
runtime. TRACK-E contains no current release-identity authority calculator, so
G05 requires governance awareness only there.

### TRACK-S audit closure

```text
TARGET_TRACK=TRACK-S
AUDITED_BRANCH=integration/spatial-result-release
AUDITED_COMMIT=cd25d4478fad1ba41ee61afae6a7158476c6ef19
CURRENT_ROLE=DOWNSTREAM_CONSUMER
PRIMARY_PROPAGATION_CLASS=TEST_PROPAGATION_REQUIRED
BUSINESS_RUNTIME_CODE_PROPAGATION_REQUIRED=false
CONTRACT_AWARENESS_REQUIRED=true
TEST_PROPAGATION_REQUIRED=true
GOVERNANCE_AWARENESS_REQUIRED=true
LIFECYCLE_STATUS=CHANGE_REQUIRED
MAP_PREVIEW_REGRESSION=PASS_12_OF_12
MAP_KML_PERMISSION_SEPARATION=PRESERVED
P09_PHASE2_RUNTIME_CODE_BLOCKER=false
RELEASE_QUALIFICATION_TOOLING_ACTION_REQUIRED=true
```

The existing Phase 1 `MapPreviewAdapter` correctly allows drawable canonical
geometry while review or confirmation is pending and treats KML blocking as a
warning rather than a Map blocker. `FinalizedResultSpatialGeometryAdapter`
remains export-grade / `AUTO_EXPORT`-oriented and is not a general Satellite
eligibility adapter. Before TRACK-S performs any future frozen release-identity
qualification, its release-evidence tooling must adopt
`GIT_CANONICAL_RELEASE_TREE` as sole authority and demote working-tree-byte
calculations to diagnostic-only status. This tooling/test action does not block
the non-authority-changing P09B-R1 provider/license validation stage.

### P08M truth context and P09 decision

```text
P08M_CHANGE_SCOPE=TRACK_L_ACTIVE_TEST_EXPECTATIONS_ONLY
SHARED_TRUTH_PROPAGATION_REQUIRED=false
V3_STRUCT_REAL_019_LEVEL_1=UNCHANGED
P09_RESUME_ELIGIBILITY=true
P09_RESUME_GOVERNANCE_DECISION=AUTHORIZED
P09_RESUME_GATE=P09B_R1_PROVIDER_OR_LICENSE_VALIDATION
P09A_RERUN_AUTHORIZED=false
PRODUCTION_AUTHORITY_CHANGE_AUTHORIZED=false
```

P09 authorization resumes only at P09B-R1. It does not authorize Satellite
provider implementation, production deployment, KML authority changes, a V3
canary, or V3 migration. TRACK-S must use map/drawable eligibility for future
Satellite consumption and keep KML and Spatial as sibling consumers.
