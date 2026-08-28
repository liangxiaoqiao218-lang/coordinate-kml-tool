# GeoKit Lab Shared File Registry

Document status: `APPROVED_GOVERNANCE_BASELINE`  
Registry owner: `TRACK-G`  
Evidence baselines: TRACK-L `1da2ff78ce5d025b2e14870d332cb5354cc5e422`; TRACK-E `c565c950f95c2c999fe75058c195b3988e121fb5`; TRACK-S `cd25d4478fad1ba41ee61afae6a7158476c6ef19`

## 1. Registry rules

Ownership means responsibility for implementing and testing business behavior. Cross-track sensitivity means a file can change a shared contract, shared semantic, application integration point, or another track's consumer behavior.

TRACK-G records and audits these boundaries. It does not edit business files. A cross-track-sensitive change requires review by the primary owner and every affected track before coordinated release.

`CROSS_TRACK_SENSITIVE` does not mean common ownership. Every entry has exactly one primary owner; secondary tracks consume behavior or integration surfaces without acquiring ownership.

Paths are repository-relative. A directory pattern counts as one registry entry, not as one entry per contained file.

## 2. TRACK-L owned

| Path or pattern | Scope | Cross-track condition |
| --- | --- | --- |
| `server/structured-coordinate-boundary.js` | Legacy/V2 structured coordinate boundary | Changes to the canonical producer input require TRACK-G contract review. |
| `server/manual-coordinate-input.js` | Manual coordinate production input | Changes that affect canonical geometry, identity, review, or KML require TRACK-G review. |
| `server/verification/**` | Production verification and finalized-result production integration | Canonical field or quality semantics are cross-track sensitive. |
| `scripts/kml-export-permission-regression.js` | TRACK-L KML permission validation | Evidence owned by TRACK-L; semantics are shared. |
| `scripts/p08f-review-confirm-kml-regression.js` | P08F hotfix validation | Propagation is audit-only until separately approved. |

## 3. TRACK-E owned

| Path or pattern | Scope | Cross-track condition |
| --- | --- | --- |
| `server/coordinate-engine-v3/**` | V3 recognizers, runner, shadow, private contracts, and current private production mapper | Remains private/`TRUE_SHADOW_ONLY`; exposure to canonical consumers requires M1-M4 governance. |
| `scripts/coordinate-engine-v3-*.js` | V3-specific regression and evidence scripts | V3 evidence cannot claim Legacy/KML/Spatial compatibility without the migration gates. |
| `PROJECT_STATE.md` | TRACK-E working state | Informational only; cannot change production authority. |

## 4. TRACK-S owned

| Path or pattern | Scope | Cross-track condition |
| --- | --- | --- |
| `server/spatial/adapters/map-preview-adapter.js` | Current Map Preview consumer adapter and independent map eligibility | Must not write KML or coordinate authority. |
| `server/spatial/spatial-facts.js` | Geometry-only Spatial Facts calculation | Must consume finalized/validated geometry and remain a downstream consumer. |
| `server/spatial/adapters/finalized-result-adapter.js` | Historical export-grade / `AUTO_EXPORT` adapter | Must not be reused as a general Map or Satellite gate. |
| `scripts/p04-map-preview-regression.js` | Map Preview consumer validation | Changes affecting canonical expectations require TRACK-G review. |

## 5. Shared / cross-track-sensitive registry

These ten entries define `SHARED_FILE_COUNT=10`. They are cross-track-sensitive because of concrete integration or contract effects, not merely because they are large files.

| # | Path / classification | Primary owner | Secondary consumers | Authority impact | Contract impact | Hotfix propagation policy |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `index.html` / `CROSS_TRACK_SENSITIVE` | TRACK-L | TRACK-S for Spatial UI/data flow; TRACK-E only for an approved future V3 integration | High when review, confirmation, or KML actions change; UI cannot create authority | Medium/high for finalized-result identity and consumer presentation | TRACK-L implements; audit TRACK-S and future TRACK-E effects; never treat as jointly owned |
| 2 | `server.js` / `CROSS_TRACK_SENSITIVE` | TRACK-L | TRACK-E producer hook; TRACK-S Map/Spatial endpoints | High for routing and authority selection | High for canonical HTTP lifecycle and consumer payloads | Temporary integration worktree required for cross-track changes; owner release only after affected-track audit |
| 3 | `package.json` / `CROSS_TRACK_SENSITIVE` | TRACK-L release baseline | TRACK-E and TRACK-S test/release commands | None unless a command or flag changes authority execution | Indirect through required validation gates | Propagate only the affected command/gate after audit; no wholesale cross-track merge |
| 4 | `server/coordinate-finalizer/reason-codes.js` / `CROSS_TRACK_SENSITIVE` | TRACK-L | TRACK-E future adapter; TRACK-S canonical consumer | High for decision, CRS, confirmation, and reason vocabulary | High: schema version and canonical enums | Mandatory contract-version audit; target-track compatibility evidence before propagation |
| 5 | `server/coordinate-finalizer/finalized-coordinate-result-v1.js` / `CROSS_TRACK_SENSITIVE` | TRACK-L | TRACK-E future adapter; TRACK-S current consumer | High: emits authoritative result fields | Critical: canonical result shape and identity | Log every semantic hotfix; TRACK-E/TRACK-S remain `AUDIT_REQUIRED` until post-deploy audit closure |
| 6 | `server/coordinate-finalizer/finalizer-inputs.js` / `CROSS_TRACK_SENSITIVE` | TRACK-L | TRACK-E future producer adapter; TRACK-S indirectly consumes outputs | High for producer mapping, review, confirmation, and technical readiness | High for canonical field population | Owner-only implementation; audit producer and consumer compatibility independently |
| 7 | `server/coordinate-finalizer/unified-gate.js` / `CROSS_TRACK_SENSITIVE` | TRACK-L | TRACK-S consumes outcomes; TRACK-E must reproduce semantics before migration | Critical: final decision and KML authority | High for decision/reason semantics | Append hotfix log; no direct cherry-pick into consumer tracks; close after production regression and target audits |
| 8 | `server/coordinate-finalizer/geometry-finalizer.js` / `CROSS_TRACK_SENSITIVE` | TRACK-L | TRACK-E future producer; TRACK-S geometry consumer | High for authoritative geometry acceptance | Critical for CRS, axis order, and geometry types | Any semantic change requires golden/real-sample compatibility as applicable before propagation |
| 9 | `server/coordinate-finalizer/geometry-hash.js` / `CROSS_TRACK_SENSITIVE` | TRACK-L | TRACK-E future adapter; TRACK-S identity validator | High for stale/altered geometry protection | Critical for identity tuple compatibility | Hash changes are migration changes; never propagate as an isolated utility edit |
| 10 | `server/coordinate-finalizer/confirmation-runtime.js` / `CROSS_TRACK_SENSITIVE` | TRACK-L | TRACK-S identity-aware flows; TRACK-E future confirmation compatibility | Critical for revision-bound confirmation | High for result identity and confirmation semantics | Requires P08F-compatible regression and target-track audit; no completion before source production closure |

## 6. Shared semantics registry

These eleven entries define `SHARED_SEMANTIC_COUNT=11` after G-02. Coordinate Authority Engine/Selection is included because unique authority selection is a real cross-track governance semantic; its runtime metadata remains proposed.

| # | Semantic | Canonical rule | Authority owner | Consumer constraint |
| --- | --- | --- | --- | --- |
| 1 | Axis Order | Coordinate positions are `longitude,latitude`; implemented as `crs.axisOrder=longitude_latitude` | TRACK-L / Coordinate Authority | Consumers validate; they do not reorder silently or redefine authority. |
| 2 | CRS | Canonical geometry is WGS84 `EPSG:4326` | TRACK-L / Coordinate Authority | Non-WGS84 geometry is not canonical until authority finalization transforms/validates it. |
| 3 | Geometry | Canonical supported types are Point, LineString, Polygon, MultiPolygon | TRACK-L / Coordinate Authority | Spatial/Satellite consume geometry without mutating the canonical result. |
| 4 | Review | Review state is an authority input to final decision/KML eligibility | TRACK-L | Map may display a warning but cannot clear review. |
| 5 | Confirmation | Acceptance/rejection is identity- and revision-bound | TRACK-L | Consumers cannot manufacture or reuse stale confirmation. |
| 6 | KML Eligibility | `kmlReady` is Legacy/server authoritative | TRACK-L | Spatial/Map cannot set or control it. |
| 7 | Map Eligibility | `previewEligibility.allowed` is an independent TRACK-S consumer decision | TRACK-S | It cannot grant KML permission or coordinate authority. |
| 8 | Result Identity | `resultId` identifies the logical finalized result | TRACK-L / Coordinate Authority | Consumers preserve and validate it. |
| 9 | Revision | `resultRevision` is a safe integer and confirmation/consumption must use the current revision | TRACK-L / Coordinate Authority | Stale revisions are rejected. |
| 10 | Geometry Hash | `geometryHash` binds identity/revision to canonical geometry | TRACK-L / Coordinate Authority | Map and future consumers recompute/compare it before using identity-sensitive data. |
| 11 | Coordinate Authority Engine / Selection | Current selection is uniquely `LEGACY_V2`; an authority swap is possible only through M11 | TRACK-L currently; TRACK-G governs migration gates only | TRACK-E and TRACK-S cannot self-select as authority. `coordinateAuthorityEngine` metadata is `PROPOSED`, not implemented. |

## 7. Change-control outcome

Changing an owned file without changing a shared semantic can remain within the owner track. Changing any shared-sensitive entry or shared semantic requires a cross-track change request. TRACK-G's response identifies affected tracks; implementation remains with the owner.

G-02 formalizes this registry through governance-document changes only. No registered application, runtime, or test file is modified.
