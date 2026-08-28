# Canonical Coordinate Result Contract

Document status: `APPROVED_GOVERNANCE_BASELINE`  
Contract name/version: `finalized_coordinate_result_v1`  
Current form: `DE_FACTO_CANONICAL_CONTRACT`  
Governance formalization: `APPROVED_GOVERNANCE_BASELINE`  
Current producer authority: `LEGACY_V2` through TRACK-L  
Audited consumer baseline: TRACK-S at `cd25d4478fad1ba41ee61afae6a7158476c6ef19`  
Audited TRACK-L hotfix baseline: `1da2ff78ce5d025b2e14870d332cb5354cc5e422`

## 1. Classification rules

`IMPLEMENTED` means the audited production-path source baseline emits or validates the field. It is not a claim that an undeployed commit is already running in production. `PROPOSED` means governance may require or consider the field later, but the canonical source does not currently emit it in that location.

The formalized governance name does not claim that runtime exposes a separately named `CANONICAL_COORDINATE_RESULT_CONTRACT` object. The implemented runtime schema remains `finalized_coordinate_result_v1`; no immediate runtime refactor is required.

`REQUIRED` means the member must be present in a conforming result object; the value may still be nullable where stated. `OPTIONAL` means the member may be absent without making the object structurally non-conforming.

The authority class means:

- `AUTHORITY`: produced by Coordinate Authority and used for authoritative release, review, confirmation, or KML decisions. Consumers may read it but cannot write or reinterpret it as their own authority.
- `CONSUMER_SAFE`: stable canonical data or identity that a consumer may validate and consume without becoming Coordinate Authority.
- `INTERNAL`: diagnostic or implementation detail; consumers must not build permission or authority logic on it.

## 2. Authoritative geometry invariant

```text
AUTHORITATIVE_GEOMETRY=WGS84 EPSG:4326 longitude,latitude
```

Implemented CRS representation:

```json
{
  "id": "EPSG:4326",
  "axisOrder": "longitude_latitude"
}
```

Supported implemented geometry types are `Point`, `LineString`, `Polygon`, and `MultiPolygon`. Every position is `[longitude, latitude]`; longitude must be in `[-180, 180]` and latitude in `[-90, 90]`. Polygon rings must be closed and contain at least three distinct positions.

## 3. Field audit

Presence requirements below describe the result object shape. `nullable` is stated separately to avoid treating a structurally required key as a guaranteed successful value.

| Field | Status | Presence | Authority class | Implemented meaning and constraints |
| --- | --- | --- | --- | --- |
| `schemaVersion` | IMPLEMENTED | REQUIRED | CONSUMER_SAFE | Exact value `finalized_coordinate_result_v1`. |
| `resultId` | IMPLEMENTED | REQUIRED | CONSUMER_SAFE | Non-empty result identity. Generated as UUID when not supplied; confirmation and map flows require it. |
| `resultRevision` | IMPLEMENTED | REQUIRED | CONSUMER_SAFE | Safe integer `>=1`; identifies the current revision of `resultId`. |
| `geometryHash` | IMPLEMENTED | REQUIRED, nullable when geometry is null | CONSUMER_SAFE | Hash of canonical geometry. Used with result identity to reject stale or mismatched data. |
| `geometry` | IMPLEMENTED | REQUIRED, nullable for blocked/no-geometry results | CONSUMER_SAFE | Canonical WGS84 geometry using supported types and `[longitude,latitude]` positions. |
| `crs` | IMPLEMENTED | REQUIRED, nullable only for an invalid/blocked candidate | CONSUMER_SAFE | Canonical CRS object; valid released geometry requires `EPSG:4326` and `longitude_latitude`. |
| `crs.id` | IMPLEMENTED | REQUIRED when `crs` is non-null | CONSUMER_SAFE | Exact value `EPSG:4326`. |
| `crs.axisOrder` | IMPLEMENTED | REQUIRED when `crs` is non-null | CONSUMER_SAFE | Exact value `longitude_latitude`. |
| `axisOrder` (top level) | PROPOSED | OPTIONAL | CONSUMER_SAFE | Not emitted at the canonical-result top level. TRACK-S emits a top-level axis order only in its derived map-preview object. |
| `coordinateAuthorityEngine` | PROPOSED | OPTIONAL | CONSUMER_SAFE | Governance enhancement candidate only; not currently exposed by canonical runtime. If implemented, diagnostic metadata cannot grant authority. |
| `sourceAuthority` | IMPLEMENTED | REQUIRED, nullable in structurally produced but invalid candidates | AUTHORITY | One of `legacy`, `manual_input`, `coordinate_engine_v2`, or `coordinate_engine_v3`; current production authority remains Legacy/V2. |
| `coordinateType` | IMPLEMENTED | REQUIRED, nullable | CONSUMER_SAFE | Canonical metadata copied from the finalized producer input. It does not grant authority. |
| `precisionMode` | IMPLEMENTED | REQUIRED, nullable | CONSUMER_SAFE | Canonical precision metadata. Consumers must not infer release permission from it. |
| `family` | IMPLEMENTED | REQUIRED, nullable | INTERNAL | Family-policy lookup metadata; not a consumer permission. |
| `availabilityStatus` | IMPLEMENTED | REQUIRED | AUTHORITY | Current family availability state; unavailable states force KML false and a failed quality state. |
| `availabilityReasonCode` | IMPLEMENTED | REQUIRED, nullable | AUTHORITY | Reason for an availability restriction. |
| `familyAvailabilityPolicy` | IMPLEMENTED | REQUIRED, nullable | INTERNAL | Frozen policy snapshot; consumer permission must come from its resulting canonical fields, not private policy interpretation. |
| `confirmationStatus` | IMPLEMENTED | REQUIRED, nullable only for malformed input | AUTHORITY | `not_required`, `pending`, `accepted`, or `rejected`. Accepted confirmation is revision-bound. |
| `qualityGateStatus` | IMPLEMENTED | REQUIRED, nullable only for malformed input | AUTHORITY | `passed`, `review_required`, `failed`, or `unknown`. |
| `decisionState` | IMPLEMENTED | REQUIRED | AUTHORITY | `AUTO_EXPORT`, `REVIEW_REQUIRED`, or `BLOCKED`; calculated by the unified gate. |
| `technicalKmlReady` | IMPLEMENTED | REQUIRED | AUTHORITY | Boolean technical precondition. It is not final KML permission. |
| `requiresReview` | IMPLEMENTED | REQUIRED | AUTHORITY | Boolean review state used by the authority gate. |
| `kmlReady` | IMPLEMENTED | REQUIRED | AUTHORITY | Final Legacy/server-authoritative KML eligibility. True only when the unified gate returns `AUTO_EXPORT`. |
| `reasonCodes` | IMPLEMENTED | REQUIRED | AUTHORITY | Deduplicated authoritative gate reason-code array. Empty for an unblocked result. |
| `blockingReasons` | IMPLEMENTED | REQUIRED | AUTHORITY | Objects shaped as `{code}` derived from `reasonCodes`. |
| `warnings` | IMPLEMENTED | REQUIRED | CONSUMER_SAFE | Deduplicated string warnings. Informational; they do not independently grant permission. |
| `limitations` | IMPLEMENTED | REQUIRED | INTERNAL | Deduplicated string limitations; not an authority decision. |
| `groups` | IMPLEMENTED | REQUIRED | AUTHORITY | Frozen group summaries. Current items may include `groupId`, `requiresReview`, and `kmlReady`. |
| `familySafetyPolicy` | IMPLEMENTED | REQUIRED, nullable | INTERNAL | Frozen family safety-policy snapshot. |
| `createdAt` | IMPLEMENTED | REQUIRED | CONSUMER_SAFE | Source result creation timestamp, defaulting to finalization time. |
| `finalizedAt` | IMPLEMENTED | REQUIRED | CONSUMER_SAFE | Finalization timestamp. |
| `gate` | IMPLEMENTED | REQUIRED | AUTHORITY | Summary object of decision, quality, confirmation, and availability state. |
| `gate.decisionState` | IMPLEMENTED | REQUIRED | AUTHORITY | Mirrors top-level `decisionState`. |
| `gate.qualityGateStatus` | IMPLEMENTED | REQUIRED | AUTHORITY | Mirrors top-level `qualityGateStatus`. |
| `gate.confirmationStatus` | IMPLEMENTED | REQUIRED | AUTHORITY | Mirrors top-level `confirmationStatus`. |
| `gate.availabilityStatus` | IMPLEMENTED | REQUIRED | AUTHORITY | Mirrors top-level `availabilityStatus`. |
| `gate.availabilityReasonCode` | IMPLEMENTED | REQUIRED, nullable | AUTHORITY | Mirrors top-level `availabilityReasonCode`. |

## 4. Identity and revision contract

The canonical identity tuple is:

```text
(resultId, resultRevision, geometryHash)
```

Consumers that depend on a particular result must compare all three values. A result ID/revision mismatch is stale identity; a geometry hash mismatch is stale or altered geometry. Confirmation acceptance is valid only for the same `resultRevision` and geometry hash.

TRACK-S Map Preview currently enforces the identity tuple, validates WGS84 CRS and geometry, and independently recomputes the geometry hash before allowing preview.

## 5. Permission separation

`technicalKmlReady` is only a technical precondition. `kmlReady` is the authoritative KML permission after quality, review, confirmation, revision, CRS, geometry, availability, and other gate checks.

Map Preview's `previewEligibility.allowed` belongs to the derived `map_preview_object_v1` consumer contract. It is not part of this canonical contract and cannot change `kmlReady`.

Spatial Facts consumes geometry derived through the current Map Preview path. It does not own coordinate or KML authority.

## 6. P08F semantic clarification

At TRACK-L commit `1da2ff78ce5d025b2e14870d332cb5354cc5e422`, the implemented gate has the following authority-controlled outcomes:

| Quality/review state | Confirmation | Other authority checks | Required KML outcome |
| --- | --- | --- | --- |
| `qualityGateStatus=review_required` | `pending` | Even when technical checks pass | `kmlReady=false`; KML blocked |
| `qualityGateStatus=review_required` | `accepted`, bound to the current `resultRevision` | Technical, safety, account, source, availability, CRS, and geometry checks pass | `decisionState=AUTO_EXPORT`; `kmlReady=true`; KML allowed |
| `qualityGateStatus=review_required` | `rejected` | Any | `kmlReady=false`; KML blocked |
| `qualityGateStatus=failed` | Any | Any | `kmlReady=false`; KML blocked |

Accepted confirmation does not rewrite the quality fact. `qualityGateStatus` may and, for this flow, does remain `review_required`; governance must not describe acceptance as `qualityGateStatus=passed`.

This does not make Map Preview a KML gate and does not grant V3 production authority.

## 7. V3 boundary and open gap

TRACK-E currently has a private result schema named `coordinate_engine_v3_production_result_v1`. Its fields and semantics are producer-private and do not replace this contract.

The audited V3 baseline lacks the finalized-result architecture and canonical identity fields `resultId`, `resultRevision`, and `geometryHash`. Therefore:

```text
V3_CANONICAL_ADAPTER=NOT_IMPLEMENTED
CANONICAL_CONTRACT_COMPATIBILITY=NOT_PROVEN
CLASSIFICATION=AUTHORITY_MIGRATION_PREREQUISITE
TRUE_SHADOW_ONLY_DEVELOPMENT_MAY_CONTINUE=true
```

Any V3 canonical adapter is `PROPOSED` until TRACK-E implements it under an approved cross-track change and TRACK-G verifies compatibility.

## 8. Versioning rule

- Clarifying existing semantics without changing accepted values: documentation clarification within v1.
- Adding a new optional consumer-safe member: v1-compatible addition after cross-track audit.
- Adding a required member, changing geometry/CRS/axis semantics, changing identity rules, or changing authority interpretation: breaking change requiring a new canonical contract version and compatibility migration.
- V3 private field names never implicitly version this contract.
- `coordinateAuthorityEngine` remains `PROPOSED` until verified runtime implementation and contract review; its absence does not block current Legacy operation or V3 shadow development.
