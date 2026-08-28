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
