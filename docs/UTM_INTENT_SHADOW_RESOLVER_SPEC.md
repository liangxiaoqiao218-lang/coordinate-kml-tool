# UTM Intent Shadow Resolver Specification

Status: Phase 1 Design

Scope: Coordinate Engine V2 diagnostics only

Parent specification: `UTM_INTENT_ROUTER_V2_SPEC.md`

## 1. Purpose

The Shadow UTM Intent Resolver proves that Coordinate Engine V2 can identify
an explicit UTM CRS before the new intent is allowed to control parsing,
transformation, or export.

Shadow mode exists to compare the new CRS decision with frozen legacy behavior
while preserving the current production result. It must answer:

> Given the OCR text and document context, what UTM CRS intent would V2
> produce?

It must not answer:

> Which legacy parser or export path should run?

The legacy UTM route is not replaced during Phase 1 because routing changes can
affect UTM30, BFTM, MGRS, Kyrgyz GK, generic projected X/Y, and downstream KML
generation. Shadow comparison isolates CRS-resolution defects before any such
migration begins.

Phase 1 therefore has the following invariants:

- No change to `precisionMode`.
- No change to `parserTrace`.
- No change to coordinate rows or point ordering.
- No change to KML or export readiness.
- No change to legacy parser selection.
- No Shadow Intent fields in the existing business API response.

The resolver is an independent diagnostic capability. Its output may be
written by a regression or diagnostic harness, but it is not a business result
and cannot authorize conversion.

## 2. Input

The resolver accepts two logical inputs:

```js
resolveShadowUtmIntent({
  rawText,
  coordinateContext
})
```

### 2.1 OCR `rawText`

`rawText` is the unmodified recognition text produced from the source image.
It may contain:

- Document CRS headings.
- Projection and datum labels.
- Zone and hemisphere declarations.
- EPSG identifiers.
- Table headings and coordinate rows.
- MGRS, BFTM, or other competing CRS evidence.

The resolver reads CRS evidence from this text. It does not repair coordinate
digits, parse X/Y rows, or transform coordinates.

### 2.2 Document CRS evidence

Document CRS evidence includes explicit text such as:

```text
UTM WGS 1984 ZONA 50S
WGS 1984 / UTM ZONE 30N
EPSG:32750
Projection BFTM
MGRS / UTM Grid Reference
Gauss-Kruger
```

Evidence must retain its source text so a shadow decision can be audited
against the OCR result and, later, the source-image region.

### 2.3 Coordinate context

`coordinateContext` may supply text already associated with the same document
or coordinate group, including:

- Extracted coordinate text.
- Projection labels.
- Table captions.
- Trusted contextual hints.

Coordinate context may contribute explicit CRS evidence. Numeric X/Y values
are validation context only and cannot complete a missing datum, zone, or
hemisphere.

Context from unrelated pages, filenames, countries, or projects must not be
merged into the same CRS decision.

## 3. Output Schema

The canonical Phase 1 diagnostic envelope is:

```json
{
  "shadowIntent": {
    "projection": "utm",
    "datum": "WGS84",
    "zone": 50,
    "hemisphere": "south",
    "epsg": "EPSG:32750",
    "confidence": "confirmed",
    "evidence": [
      {
        "source": "ocr_raw_text",
        "field": "zone_hemisphere",
        "value": "50S",
        "text": "ZONA 50S"
      }
    ],
    "conflicts": [],
    "blockedFallbacks": [
      "bftm_xy",
      "generic_projected_xy",
      "wgs84_chat_coordinates"
    ]
  }
}
```

Field contract:

- `projection`: `"utm"` only when UTM evidence exists; otherwise `null`.
- `datum`: normalized supported datum, initially `"WGS84"`, or `null`.
- `zone`: integer `1..60`, or `null`.
- `hemisphere`: `"north"`, `"south"`, or `null`.
- `epsg`: normalized EPSG identifier, or `null`.
- `confidence`: `"confirmed"`, `"candidate"`, or `"unknown"`.
- `evidence`: auditable source evidence used by the decision.
- `conflicts`: strong, incompatible CRS evidence; empty when none exists.
- `blockedFallbacks`: routes that would be forbidden if this intent later
  became authoritative. In Shadow mode this field is diagnostic only.

`confidence="confirmed"` requires explicit, consistent evidence for UTM,
datum, zone, and hemisphere. For WGS84 UTM, EPSG is then derived
deterministically:

```text
north: EPSG = 32600 + zone
south: EPSG = 32700 + zone
```

`confidence="candidate"` means UTM evidence exists but at least one required
CRS field is missing. EPSG must not be derived from incomplete evidence.

`confidence="unknown"` means no usable UTM intent exists or strong evidence is
conflicted. An unknown result must not convert X/Y.

Every resolver result must expose all fields in this schema, including empty
`conflicts` and `blockedFallbacks` arrays. This keeps diagnostics stable across
confirmed, candidate, unknown, excluded, and conflicted results.

## 4. Evidence Rules

### 4.1 Evidence allowed to confirm WGS84 UTM

Confirmation requires explicit evidence for all of the following:

- Projection: `UTM` or `Universal Transverse Mercator`.
- Datum: `WGS84`, `WGS 1984`, or `World Geodetic System 1984`.
- Zone label: `Zone`, `Zona`, or another frozen equivalent.
- Zone number: an integer from 1 through 60.
- Hemisphere: explicit `N`, `S`, `north`, `south`, or a frozen language
  equivalent within the CRS phrase.

An explicit EPSG identifier is strong evidence, but it must agree with the
datum, zone, and hemisphere evidence. It cannot silently override conflicting
document fields.

Evidence should come from the same document or coordinate group. Evidence
from separate coordinate tables must not be combined merely because it appears
in one OCR response.

### 4.2 Prohibited inference

The resolver must never:

- Infer CRS, zone, hemisphere, or datum from a country name.
- Infer a UTM zone from X/Y numeric ranges.
- Infer the southern hemisphere from a large northing or false northing.
- Infer the northern hemisphere from a small northing.
- Treat generic `X`, `Y`, or `Sommets` headings as UTM evidence.
- Treat a bare value such as `50S` as UTM without excluding MGRS structure.
- Generate EPSG when datum, zone, or hemisphere is missing.
- Use filenames or project names to complete missing CRS fields.

Numeric ranges may later validate coordinates after a CRS is confirmed. They
cannot select the CRS.

## 5. Conflict Handling

Shadow mode must preserve conflicts rather than choose the most convenient
interpretation.

Example:

```text
UTM WGS 1984 Zone 50S
50S AB 12345 67890
```

The first line resembles an explicit UTM projected CRS. The second line has
MGRS grid-zone, grid-square, and grid-digit structure. The result must record
the MGRS ambiguity as a conflict and must not authorize conversion.

Example conflict output:

```json
{
  "shadowIntent": {
    "projection": null,
    "datum": null,
    "zone": null,
    "hemisphere": null,
    "epsg": null,
    "confidence": "unknown",
    "evidence": [],
    "conflicts": [
      {
        "type": "crs_conflict",
        "sources": [
          "utm",
          "mgrs"
        ]
      }
    ],
    "blockedFallbacks": [
      "utm_projected_xy"
    ]
  }
}
```

Other mandatory conflicts include:

- Explicit UTM and explicit BFTM evidence in the same coordinate group.
- EPSG inconsistent with the stated zone or hemisphere.
- Multiple explicit zones or hemispheres in one group.
- Multiple datums in one group.

Conflict rules:

1. `conflicts` is non-empty.
2. `confidence` cannot be `confirmed`.
3. No EPSG may be derived from the disputed fields.
4. No parser, transformation, KML, Decimal, or Chat route is selected.
5. Original OCR text and X/Y rows remain available for review.

Explicit BFTM, MGRS, or Gauss-Kruger evidence without competing UTM evidence
is a negative UTM result, not necessarily a conflict. It should be recorded as
an exclusion in `evidence` and return `confidence="unknown"`.

When an explicit UTM intent exists without conflicts, the diagnostic blocked
fallbacks are:

```json
[
  "bftm_xy",
  "generic_projected_xy",
  "wgs84_chat_coordinates"
]
```

When MGRS structure is present, the diagnostic blocked fallback is:

```json
[
  "utm_projected_xy"
]
```

## 6. Regression Plan

### 6.1 Resolver-level text regression

The deterministic developer regression is:

```text
node scripts/utm-intent-shadow-regression.js
```

It must cover at least:

- WGS84 UTM Zone 50S -> `confirmed`, EPSG:32750.
- WGS84 UTM Zone 30N -> `confirmed`, EPSG:32630.
- Explicit BFTM -> not UTM.
- MGRS structure -> not UTM.
- Kyrgyz Gauss-Kruger -> not UTM.
- Generic X/Y -> `unknown`.
- Country-only context -> `unknown`.
- Missing hemisphere -> `candidate`, no EPSG.
- Conflicting EPSG and zone/hemisphere -> conflict, no conversion.
- Explicit UTM plus BFTM or MGRS -> conflict, no conversion.

This suite validates resolver logic only. It is not real-image acceptance.

### 6.2 Mandatory real-image regression

Phase 1 cannot be accepted or frozen until the full image path is tested:

```text
source image
  -> OCR
  -> rawText and document context
  -> Shadow UTM Intent Resolver
  -> recorded comparison result
```

Required real-image coverage:

- Indonesia mining document 01: WGS84 UTM Zone 50S.
- Indonesia mining document 02: WGS84 UTM Zone 50S.
- Indonesia mining document 03: WGS84 UTM Zone 50S.
- Burkina Faso 03: legacy WGS84 UTM Zone 30N.
- BFTM sample: negative UTM result.
- MGRS sample: negative UTM result.
- Kyrgyz GK sample: negative UTM result.

Each image regression record must retain:

- Source sample identifier and checksum.
- OCR raw text used by the resolver.
- Extracted CRS evidence.
- Full Shadow Intent output.
- Expected projection, datum, zone, hemisphere, EPSG, and confidence.
- Conflicts and blocked fallbacks.
- Existing `precisionMode`, `parserTrace`, point count, and KML hashes for
  comparison only.
- Confirmation that legacy outputs did not change.

Missing source images must be reported as unavailable. Hand-written OCR text
must not be relabeled as an image regression result.

### 6.3 Compatibility gate

Every Shadow Resolver change must run:

- The Shadow resolver regression.
- Existing Coordinate Verification regression.
- Existing evidence regressions.
- The locked coordinate-recognition baselines when their source images are
  available.

The Shadow suite passes only when intent expectations pass and the legacy
`precisionMode`, `parserTrace`, coordinate rows, KML, and API schema remain
unchanged.

## 7. Migration Plan

The required migration sequence is:

```text
Shadow Resolver
      |
      v
Real-image and legacy comparison
      |
      v
Typed CRS Intent
      |
      v
Generic UTM Projected XY Parser
      |
      v
Legacy UTM30 Alias Migration
      |
      v
Quality Gate and Export adoption
```

### Stage 1: Shadow

- Keep the resolver outside the business response and routing path.
- Record decisions only through dedicated diagnostics and regression tooling.
- Implement conflict and blocked-fallback diagnostics.
- Complete deterministic and real-image comparison.

### Stage 2: Compare

- Compare Shadow decisions with UTM30, BFTM, MGRS, Kyrgyz GK, and unknown X/Y.
- Investigate every disagreement before routing behavior changes.
- Freeze real-image expectations and source checksums.

### Stage 3: Typed Intent

- Promote only confirmed, conflict-free Shadow decisions into the canonical
  Typed CRS Intent schema.
- Keep incomplete and conflicted projected X/Y in manual review.
- Do not transform coordinates yet.

### Stage 4: Generic UTM parser

- Introduce one generic `utm_projected_xy` parser.
- Pass the confirmed datum, zone, hemisphere, and EPSG into the parser.
- Prohibit the parser from inferring CRS fields.

### Stage 5: Legacy alias migration

- Preserve `utm30n-projected-x-y` as a compatibility alias.
- Compare legacy UTM30 points and KML against the generic route.
- Migrate only after all locked baselines pass without unintended changes.

No stage may be skipped because a single country or zone sample succeeds.

## 8. Phase 1 Exit Criteria

Phase 1 is complete only when:

- The canonical Shadow schema includes `conflicts` and `blockedFallbacks`.
- Deterministic resolver regression passes.
- Required real-image regression passes.
- UTM30 agrees with the expected WGS84 Zone 30N intent.
- BFTM, MGRS, and Kyrgyz GK remain negative UTM cases.
- Generic X/Y and country-only inputs remain unknown.
- Existing API response, parser selection, coordinate output, and KML are
  unchanged.
- All available locked compatibility baselines pass.

Until these conditions are satisfied, Shadow output remains diagnostic and
must not participate in parsing, transformation, Quality Gate, or Export.
