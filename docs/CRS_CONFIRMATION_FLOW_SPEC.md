# CRS Confirmation Flow Specification

## 1. Purpose and Status

This document defines the user confirmation contract for projected coordinate reference systems before canonical UTM migration. It closes the product-flow gap between CRS evidence detection and authorization to transform or export coordinates.

This is a Phase 4A design document only. It does not modify UI, API, parser routing, `precisionMode`, Quality Gate, KML, Export, database records, or legacy behavior.

The central rule is:

```text
System CRS confidence is not the same as user confirmation.
```

A complete, conflict-free CRS observation may be system-confirmed, but the initial canonical migration flow still requires an explicit user action before the CRS becomes accepted for transformation.

## 2. State Model

The confirmation flow uses these distinct concepts:

- `intentConfidence`: system assessment from evidence: `confirmed`, `candidate`, or `unknown`.
- `confirmationStatus`: user-flow state: `not_required`, `awaiting_confirmation`, `accepted`, `rejected`, or `cancelled`.
- `qualityGateStatus`: downstream validation state: `not_evaluated`, `accepted`, `review`, or `blocked`.
- `kmlReady`: final export authorization; false until every required gate passes.

During the shadow integration period, projected UTM results use `confirmationStatus: awaiting_confirmation` even when `intentConfidence: confirmed`. Shadow output must not change production KML readiness.

## 3. Confirmation View Model

The future UI must consume a structured review model rather than reparsing OCR or display text:

```json
{
  "coordinateGroupId": "group-1",
  "intentConfidence": "confirmed",
  "detectedCrs": {
    "projection": "utm",
    "datum": "WGS84",
    "zone": 50,
    "hemisphere": "south",
    "epsg": "EPSG:32750"
  },
  "missingFields": [],
  "evidence": [],
  "conflicts": [],
  "blockedFallbacks": [],
  "confirmationStatus": "awaiting_confirmation",
  "qualityGateStatus": "not_evaluated",
  "kmlReady": false
}
```

The model must preserve the original X/Y rows and an immutable evidence snapshot reference. Confirmation is scoped to one coordinate group and cannot silently apply to another group or image.

## 4. Flow A: Confirmed CRS Evidence

### 4.1 Input

Complete, explicit, consistent evidence is present, for example:

```text
UTM WGS 1984 ZONA 50S
```

Resolved system intent:

```yaml
projection: utm
datum: WGS84
zone: 50
hemisphere: south
epsg: EPSG:32750
intentConfidence: confirmed
conflicts: []
```

### 4.2 Display

The confirmation view must show all material fields together:

```text
Detected coordinate reference system

Projection: UTM
Datum: WGS84
Zone: 50
Hemisphere: South
EPSG: EPSG:32750

[Confirm use of this CRS]
[Review or change]
[Cancel]
```

Evidence may be expanded so the user can inspect the literal label and source region, such as `UTM WGS 1984 ZONA 50S` from `bottom_footer`.

### 4.3 Action

- `Confirm use of this CRS` creates an explicit user confirmation decision.
- `Review or change` opens the manual CRS editor without pre-authorizing transformation.
- `Cancel` preserves X/Y and exits without conversion.

Confirmation does not itself make KML ready. It authorizes construction of the confirmed typed CRS result, which must still pass row, transformation, geometry, and Export gates.

## 5. Flow B: Candidate CRS Evidence

### 5.1 Input

Partial evidence exists but required fields are missing, for example:

```text
UTM Zone 50
```

Possible review model:

```yaml
projection: utm
zone: 50
datum: null
hemisphere: null
epsg: null
intentConfidence: candidate
missingFields:
  - datum
  - hemisphere
```

### 5.2 Display

```text
Possible coordinate reference system

Projection: UTM
Zone: 50

Missing required information:
- Datum
- Hemisphere

Datum:      [Choose...]
Hemisphere: [North] [South]

[Confirm completed CRS]
[Cancel]
```

No option may be preselected from country, coordinate range, map location, filename, language, northing, or legacy behavior.

If the zone is missing, the user must explicitly enter or select a zone from 1 through 60. If projection is uncertain, the flow must return to projection selection rather than assuming UTM.

### 5.3 Completion

The interface derives EPSG only after the user has explicitly supplied a supported datum, valid zone, and hemisphere. It must display the derived EPSG before final confirmation.

User-supplied fields are recorded with provenance `user_confirmation`; they do not alter or replace the original OCR evidence.

## 6. Flow C: Unknown CRS

### 6.1 Input

Only projected X/Y values are available, for example:

```text
778000,9720000
```

There is no usable CRS evidence:

```yaml
intentConfidence: unknown
detectedCrs: null
missingFields:
  - projection
  - datum
  - zone_or_projection_parameters
  - hemisphere_if_required
```

### 6.2 Display

```text
The coordinate reference system could not be confirmed.
The X/Y values have been preserved and have not been converted.

Choose a coordinate system:
[UTM]
[Gauss-Kruger]
[Other]

[Cancel]
```

The selection order is deliberate:

1. Select the projection family.
2. Enter all parameters required by that projection.
3. Review the complete CRS summary.
4. Explicitly confirm.
5. Run Quality Gate and transformation validation.

The UI must not present a country-derived or numeric-range-derived CRS as recommended, likely, default, or preselected.

## 7. Flow D: CRS Conflict

Conflict is distinct from candidate or unknown. Examples include:

- explicit UTM and explicit BFTM evidence in one coordinate group;
- UTM projected X/Y and an MGRS token treated as the same representation;
- Zone 50 South combined with EPSG:32650;
- two explicit zones or hemispheres.

The UI must show the conflicting claims and their evidence separately:

```text
Conflicting coordinate reference system information was found.

Claim 1: WGS84 / UTM Zone 50 South
Evidence: "UTM WGS 1984 ZONA 50S"

Claim 2: EPSG:32650 / UTM Zone 50 North
Evidence: "EPSG:32650"

Automatic conversion is blocked.
[Resolve manually]
[Cancel]
```

A generic `Confirm` action is prohibited while conflicts remain. Manual resolution must select a complete CRS, retain the rejected claims for audit, and record that the decision was user-supplied.

## 8. User Confirmation Decision

An accepted decision must be structured and auditable:

```json
{
  "coordinateGroupId": "group-1",
  "confirmationStatus": "accepted",
  "selectedCrs": {
    "projection": "utm",
    "datum": "WGS84",
    "zone": 50,
    "hemisphere": "south",
    "epsg": "EPSG:32750"
  },
  "source": "user_confirmation",
  "evidenceSnapshotId": "immutable-reference",
  "confirmedAt": "timestamp",
  "confirmedBy": "current-user-reference"
}
```

Implementation may choose different storage details, but it must retain group scope, the complete selected CRS, provenance, and the evidence version the user reviewed.

If coordinates, grouping, source image, CRS evidence, or any selected CRS field changes after confirmation, the decision becomes stale and must return to `awaiting_confirmation`.

## 9. Safety Rules

The confirmation flow must never:

- infer CRS from country or administrative area;
- recommend CRS from country;
- infer zone from easting or northing ranges;
- infer hemisphere from northing magnitude;
- use a filename or language as CRS proof;
- use `utm30n-projected-x-y` or another legacy alias as evidence for a new image;
- silently copy a prior user's CRS choice to a new coordinate group;
- silently choose between conflicting evidence;
- transform or export candidate, unknown, rejected, cancelled, conflicted, or stale selections;
- allow edited display text to bypass the typed confirmation object;
- let Export rerun CRS inference.

The flow may:

- display literal evidence;
- accept explicit user-selected CRS parameters;
- derive EPSG deterministically from an explicitly selected supported datum, zone, and hemisphere;
- validate coordinate ranges only after CRS selection, as a Quality Gate check rather than CRS inference;
- preserve X/Y for later review when confirmation is incomplete.

## 10. Integration Boundary

The future data flow is:

```text
Original Image
  |-- Coordinate Vision --------------------> original X/Y rows
  `-- CRS Vision -> Evidence Collector ------> CRS evidence
                              |
                              v
                       Shadow Resolver
                              |
                              v
                   Confirmation View Model
                              |
                         user decision
                              |
                              v
                    Confirmed Typed Result
                              |
                              v
                         Quality Gate
                              |
                    accepted | blocked/review
                              |
                              v
                    WGS84 Transformation
                              |
                              v
                            Export
```

Integration rules:

- Shadow Resolver proposes; it does not authorize Export.
- Confirmation UI displays structured evidence and records the user's explicit decision; it does not parse coordinates or calculate projection by heuristic.
- Typed Result carries the selected CRS as data; it does not carry permission to export by itself.
- Quality Gate validates CRS consistency, rows, transformation, and geometry.
- Export consumes only the accepted transformed result and never reads raw OCR to reconstruct CRS.
- During Phase 4B shadow integration, all new confirmation data remains non-authoritative and production output remains legacy-controlled.

## 11. Relationship to Canonical Type Migration

`UTM_CANONICAL_TYPE_MIGRATION_SPEC.md` defines type identity and alias compatibility. This document defines how a new or incomplete CRS becomes explicitly accepted by a user.

The dependency is:

```text
Canonical Type Contract
        +
CRS Confirmation Contract
        |
        v
Phase 4B Shadow User Flow Integration
        |
        v
Phase 4C UTM30 Alias Migration Decision
```

Canonical registration alone cannot authorize new-image transformation. A legacy alias can preserve historical compatibility, but it cannot replace evidence or confirmation for a new image.

## 12. Phase 4B Entry Conditions

Before implementing Shadow User Flow Integration:

1. The canonical migration and confirmation-flow specifications are reviewed together.
2. Confirmed, candidate, unknown, and conflicted view-model fixtures are defined.
3. Confirmation invalidation and group scoping are testable.
4. Shadow data is excluded from production API contracts unless explicitly approved for a diagnostic endpoint.
5. No candidate or unknown CRS can reach transformation or Export.
6. Existing Review Mode and KML safety boundaries are preserved.
7. A rollback or kill switch leaves the legacy-authoritative path unchanged.

## 13. Non-Goals

This specification does not:

- design final visual styling;
- modify `index.html`;
- create a new API or database table;
- register `utm-projected-x-y` in production;
- migrate legacy data;
- change UTM30 output;
- enable UTM50S KML Export;
- define confirmation workflows for every non-UTM projection;
- authorize Phase 4C alias migration.
