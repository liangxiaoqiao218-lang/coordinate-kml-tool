# GeoKit Lab Integration Constitution

Document status: `APPROVED_GOVERNANCE_BASELINE`  
Governance owner: `TRACK-G`  
Established by: `G-01 Cross-Track Integration Governance Establishment`  
Approved by: `G-02 Governance Consistency, Formalization & P08G Authorization Audit`  
Evidence snapshot date: `2026-08-28`

## 1. Purpose and authority boundary

This constitution governs cross-track contracts, shared semantics, hotfix propagation, release coordination, and future authority migration among TRACK-L, TRACK-E, and TRACK-S.

TRACK-G owns governance only. It does not own coordinate, KML, V3 recognition, Spatial, Satellite, OCR, CRS, database, or other business authority, and it must not implement business behavior directly.

Current authority state:

| State | Governed value |
| --- | --- |
| `CURRENT_PRODUCTION_AUTHORITY` | `LEGACY_V2` |
| `KML_AUTHORITY` | `LEGACY_SERVER_AUTHORITATIVE_KML_ELIGIBILITY` |
| `V3_PRODUCTION_AUTHORITY` | `false` |
| `SPATIAL_PRODUCTION_AUTHORITY` | `false` |

TRACK-G cannot change these values by editing this document. Any future change requires the gates in `AUTHORITY_MIGRATION_CHECKLIST.md`, explicit human approval, and implementation by the owning track.

## 2. Track charter

| Track | Role | Current state | Business authority |
| --- | --- | --- | --- |
| TRACK-L | Legacy Production / Coordinate / KML / Production Hotfix | Production authority; P08F commit `1da2ff78ce5d025b2e14870d332cb5354cc5e422` is patched, tested, committed, and pushed, but not production-deployed | Yes: Legacy/V2 coordinate result and server-authoritative KML eligibility |
| TRACK-E | Coordinate Engine V3 | Branch `v3/isolated-recognizers`, audited HEAD `c565c950f95c2c999fe75058c195b3988e121fb5`, `TRUE_SHADOW_ONLY` | No |
| TRACK-S | Spatial Result / Satellite Map | Branch `integration/spatial-result-release`, audited HEAD `cd25d4478fad1ba41ee61afae6a7158476c6ef19`; Phase 1 released; P09 Phase 2 `HOLD` | No; downstream consumer only |
| TRACK-G | Integration Governance | Cross-track contract and release governance | No |

## 3. One-way dependency constitution

The only permitted authority direction is:

```text
Recognition Producer
        -> Coordinate Authority
        -> Canonical Coordinate Result
        -> Consumers
```

The reverse direction is prohibited. A consumer can reject, warn, degrade, or decline to render its own output, but it cannot mutate coordinate authority, geometry authority, revision identity, confirmation authority, or another consumer's permission.

If governance review finds that a business-code change is required, TRACK-G records:

```text
CROSS_TRACK_CHANGE_REQUIRED=true
TARGET_TRACK=<TRACK-L|TRACK-E|TRACK-S>
```

The target track implements and validates the change. TRACK-G does not implement it.

## 4. Canonical result and private schemas

The current implemented, de facto canonical contract is the result schema `finalized_coordinate_result_v1`, emitted by the Legacy/V2 production path and consumed by the current Spatial path. `Canonical Coordinate Result Contract` is its approved governance name. Current runtime is not required to expose a separately named `CANONICAL_COORDINATE_RESULT_CONTRACT` object or to refactor merely because governance has formalized the name. Its approved definition is in `CANONICAL_COORDINATE_RESULT_CONTRACT.md`.

The authoritative geometry is WGS84 `EPSG:4326`, with coordinate positions ordered `longitude,latitude` (`crs.axisOrder=longitude_latitude`).

Engine-private schemas are producer-local. In particular, V3's current `coordinate_engine_v3_production_result_v1` is not the canonical contract and must not become a consumer contract by direct exposure.

The TRACK-E baseline does not contain the current finalized-result architecture or canonical `resultId`, `resultRevision`, and `geometryHash` identity. This is recorded as:

```text
CANONICAL_CONTRACT_GOVERNANCE_GAP=OPEN
CLASSIFICATION=AUTHORITY_MIGRATION_PREREQUISITE
SHADOW_MODULE_DEVELOPMENT_BLOCKER=false
```

## 5. KML and Spatial sibling rule

The permanent relationship is:

```text
Canonical Coordinate Result
          |          \
          v           v
         KML        Spatial
```

KML and Spatial are sibling consumers.

- `SPATIAL_RESULT_CONTROLS_KML=false`
- `MAP_GATE_CONTROLS_KML=false`
- `KML_CONTROLS_SPATIAL=false`
- `MAP_PERMISSION_SEPARATE_FROM_KML_PERMISSION=true`

`kmlReady` is a Legacy/server authority decision. Map preview eligibility is a Spatial consumer decision. Neither permission can substitute for, upgrade, or downgrade the other.

## 6. Spatial adapter constraint

`server/spatial/adapters/finalized-result-adapter.js` contains `FinalizedResultSpatialGeometryAdapter`, an older `AUTO_EXPORT` / export-grade adapter. The current Map Preview path uses `MapPreviewAdapter` instead.

The export-grade adapter must not be reused as a general map or Satellite gate. Future Satellite eligibility must follow drawable / map-preview semantics through a canonical-result consumer adapter, independently of KML permission. Any proposed reuse requires a TRACK-S change request and TRACK-G contract audit.

## 7. V3 migration rule

V3 release is not defined as merging a branch into production. The required sequence is:

```text
V3 Private Result
  -> V3 Canonical Adapter
  -> Canonical Coordinate Result Contract
  -> KML Compatibility
  -> Spatial Compatibility
  -> Canary
  -> Authority Swap
```

Before M11 Production Authority Swap is explicitly approved and executed:

- `V3_PRODUCTION_AUTHORITY=false`
- V3 may continue internal `TRUE_SHADOW_ONLY` development.
- V3 canary and migration remain blocked.
- V3 cannot control production coordinate output, KML, or Spatial.

## 8. Change and version governance

Changes to canonical fields or shared semantics require:

1. A cross-track change record naming the producer and all affected consumers.
2. An `IMPLEMENTED` versus `PROPOSED` audit against source code.
3. A compatibility judgment for KML and Spatial independently.
4. A versioning judgment: backward-compatible clarification, backward-compatible addition, or breaking change.
5. Owning-track implementation and tests.
6. TRACK-G integration review before release coordination.

A draft document cannot promote a `PROPOSED` field to `IMPLEMENTED`. Only verified source code on the applicable release baseline can do so.

Every Cross-Track Change Request must contain:

```text
REQUESTING_TRACK=
TARGET_TRACK=
SHARED_FILES=
SHARED_SEMANTICS=
CONTRACT_IMPACT=
AUTHORITY_IMPACT=
PRODUCTION_IMPACT=
MIGRATION_REQUIRED=
```

If any track needs another track's owned file or authority behavior changed, it must stop implementation at that boundary and submit the request. `STOP_AND_REQUEST=true`; direct mutation of another track's authority is prohibited.

## 9. Branch and integration governance

Long-lived TRACK-L, TRACK-E, and TRACK-S feature branches do not merge directly into one another. Cross-track validation uses a temporary integration branch in a clean, dedicated worktree, with the exact source commits recorded. Integration evidence does not transfer file ownership or production authority.

The following shortcuts are prohibited:

- V3 feature branch directly merged into production authority;
- Spatial or Satellite feature branch directly merged into production authority;
- cross-track hotfix propagation by unreviewed cherry-pick or direct merge;
- temporary integration branch treated as a permanent authority branch.

## 10. Runtime authority metadata

`coordinateAuthorityEngine` is not currently exposed as formal runtime metadata. It is recorded only as:

```text
RUNTIME_AUTHORITY_METADATA_STATUS=GOVERNANCE_ENHANCEMENT_CANDIDATE
IMPLEMENTATION_STATUS=PROPOSED
CURRENT_RUNTIME_REFACTOR_REQUIRED=false
```

If later implemented, it is diagnostic metadata and cannot itself perform or imply an authority swap.

## 11. Release coordination state

After G-02 approval:

- P08F is logged for propagation audit; no propagation is performed.
- `P08G_GOVERNANCE_READINESS=READY` because the contemplated deployment is limited to the TRACK-L hotfix, contains no cross-track authority mutation, preserves the KML/Spatial sibling boundary, leaves V3 in shadow, and leaves P09 on hold.
- `P08G_GOVERNANCE_AUTHORIZATION=AUTHORIZED` for TRACK-L to execute `P08G_CONTROLLED_PRODUCTION_REDEPLOYMENT`.
- TRACK-G does not execute P08G. Authorization does not mean deployment occurred.
- `P09_STATUS=HOLD`. Required order is P08G production regression closure PASS, then Hotfix Propagation Audit, then a later explicit P09 resume/hold decision.

## 12. Baseline status

These five documents are the G-02 approved governance baseline. Approval formalizes governance, not runtime structure, deployment state, authority migration, or hotfix propagation completion. G-03 remains the separately authorized future commit step.
