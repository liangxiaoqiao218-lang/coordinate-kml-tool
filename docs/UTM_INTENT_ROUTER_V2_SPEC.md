# UTM Intent Router V2 Specification

Status: Design Frozen

Scope: Coordinate Engine V2

Canonical type: `utm-projected-x-y`

Engine coordinate type: `utm_projected_xy`

Precision mode: `utm-projected-x-y`

## 1. Purpose

This document defines the permanent V2 contract for recognizing and exporting
WGS84 UTM projected X/Y coordinates. It replaces the case-by-case model in
which a country, numeric range, or individual UTM zone becomes a separate
coordinate parser.

UTM Zone 50S is the first planned real-world validation case for this design.
It is not a separate coordinate type and must not introduce a dedicated
`utm50s` parser.

The required processing boundary is:

```text
Intent -> Parser -> Quality Gate -> Regression -> Freeze
```

## 2. Current Problem

### 2.1 Distributed UTM behavior

Current UTM behavior is distributed between backend recognition, frontend
projection selection, conversion helpers, V2 result normalization, and KML
export. No single typed CRS intent owns datum, zone, hemisphere, and EPSG.

The existing UTM30 route is primarily a numeric classifier. It recognizes
projected-looking X/Y rows inside fixed ranges and assigns `utm30n`. It does not
prove the zone, hemisphere, or datum from source evidence.

The frontend independently detects projection text, maintains a hidden
projection selection, reparses workspace rows, and performs the projected
conversion. This allows recognition and export to reach different CRS
decisions.

### 2.2 Numeric inference risk

Projected X/Y values can validate whether coordinates are plausible after a
CRS has been selected. They cannot determine the CRS.

The following must never be inferred from numeric X/Y ranges alone:

- UTM zone.
- Hemisphere.
- Datum.
- EPSG identifier.
- Country.

Many UTM zones share the same valid easting and northing ranges. A range that
looks like UTM30 may also be valid in UTM29, UTM36, UTM50, UTM51, or another
zone. Southern-hemisphere northing also uses a false northing and cannot be
safely distinguished without CRS evidence.

### 2.3 BFTM capture risk

Generic table features such as `X`, `Y`, or `Sommets` are layout evidence, not
sufficient BFTM CRS evidence. BFTM must not capture a projected table merely
because it contains X/Y columns or values overlapping the BFTM numeric range.

Explicit UTM CRS evidence must outrank weak BFTM layout and numeric evidence.
If explicit UTM evidence and explicit BFTM projection evidence both occur for
the same coordinate group, the result is a CRS conflict and must require
manual review.

### 2.4 Frontend reparse risk

Export must not reparse workspace text or independently infer a projection.
Recognition, Quality Gate, and Export must consume the same typed CRS intent
and accepted points.

Reparsing text during Export can:

- Select a different zone than the backend.
- Force the wrong hemisphere.
- Convert unknown projected X/Y as a known CRS.
- Allow edited display text to bypass Quality Gate.
- Produce KML that cannot be traced to the accepted recognition result.

## 3. V2 Architecture

```text
Image / OCR / user-supplied context
                |
                v
       CRS Evidence Collector
                |
                v
       UTM Intent Resolver
                |
                v
         Typed CRS Intent
                |
                v
 Generic UTM Projected XY Parser
                |
                v
          UTM Quality Gate
                |
                v
        WGS84 Transformation
                |
                v
      Accepted Typed Result
                |
                v
              Export
```

### 3.1 CRS Evidence Collector

The collector extracts evidence without choosing a coordinate type. Evidence
may come from document headings, table captions, explicit EPSG text, OCR,
trusted user selection, or context hints.

Evidence strength, from highest to lowest, is:

1. An explicit EPSG identifier consistent with the document CRS fields.
2. A complete CRS phrase containing datum, UTM, zone, and hemisphere.
3. Datum, zone, and hemisphere fields within the same table or document block.
4. An explicit trusted user CRS selection.
5. Filename, country, or project context as supporting evidence only.
6. Numeric range evidence for validation only.

Country, filename, and numeric ranges must never complete a missing zone,
hemisphere, or datum.

### 3.2 UTM Intent Resolver

The resolver consumes collected evidence and produces exactly one of:

- `confirmed`: all required CRS fields are present and consistent.
- `incomplete`: UTM intent exists, but a required CRS field is missing.
- `conflicted`: two or more strong pieces of CRS evidence disagree.
- `not_utm`: no UTM intent is established.

The resolver must not parse coordinate rows and must not transform
coordinates.

### 3.3 Generic UTM Projected XY Parser

There is one generic UTM projected X/Y parser for every zone and hemisphere.
It receives a confirmed typed UTM intent and parses only:

- Point or row label.
- Easting/X.
- Northing/Y.
- Row order.
- Coordinate groups.

It must not infer or repair datum, zone, hemisphere, EPSG, or country. OCR digit
repair, if ever allowed, must remain bounded by confirmed type context and
type-specific regression evidence.

### 3.4 Quality Gate and transformation

The Quality Gate owns export readiness. WGS84 transformation occurs only
after CRS and row gates pass. The transformed WGS84 points are then validated
before the result can be marked `kml_ready`.

### 3.5 Export

Export consumes only the accepted typed result. It must use Quality Gate
accepted WGS84 points and must not:

- Reparse workspace text.
- Run the Intent Router again.
- Infer a projection from X/Y ranges.
- Change zone, hemisphere, datum, axis order, or coordinate grouping.
- Fall through to Chat or another parser.

KML coordinate order is always `longitude,latitude,0`.

## 4. Typed Intent Schema

The canonical V2 shape is:

```json
{
  "type": "utm_projected_xy",
  "status": "confirmed",
  "confidence": 1,
  "projection": "utm",
  "datum": "WGS84",
  "zone": 50,
  "hemisphere": "south",
  "epsg": "EPSG:32750",
  "evidence": [
    {
      "source": "document_crs_label",
      "text": "UTM WGS 1984 ZONA 50S",
      "strength": "explicit"
    }
  ],
  "conflicts": [],
  "blockedFallbacks": [
    "bftm_xy",
    "generic_projected_xy",
    "wgs84_chat_coordinates"
  ]
}
```

Required fields for `status=confirmed`:

- `projection` must be `utm`.
- `datum` must be an explicitly supported datum.
- `zone` must be an integer from 1 through 60.
- `hemisphere` must be `north` or `south`.
- `epsg` must agree with datum, zone, and hemisphere.
- `evidence` must include the source for every required CRS decision.
- `conflicts` must be empty.

For WGS84 UTM, EPSG is derived deterministically only after all required
evidence is confirmed:

```text
north: EPSG = 32600 + zone
south: EPSG = 32700 + zone
```

Examples:

```json
{
  "projection": "utm",
  "datum": "WGS84",
  "zone": 30,
  "hemisphere": "north",
  "epsg": "EPSG:32630"
}
```

```json
{
  "projection": "utm",
  "datum": "WGS84",
  "zone": 50,
  "hemisphere": "south",
  "epsg": "EPSG:32750"
}
```

Initial automatic transformation scope is WGS84 UTM. A non-WGS84 datum may be
recorded in the intent schema, but it must remain `incomplete` or require
review until an authoritative transformation and regression baseline exist.

### 4.1 `50S` ambiguity

`50S` may mean UTM Zone 50 South in a projected CRS label, but `S` may be an
MGRS latitude band in a grid-zone designator. The resolver must use structure:

- A phrase such as `WGS 1984 / UTM Zone 50S` beside projected X/Y supports
  southern-hemisphere UTM.
- `50S` followed by an MGRS grid square and grid digits supports MGRS.
- Bare `50S` without sufficient structure is ambiguous and cannot authorize
  transformation.

## 5. Routing Priority

The normative priority is:

```text
MGRS structural intent
  > Explicit CRS Intent
  > Structured coordinate tables
  > Unknown projected XY
  > Decimal coordinates
  > Chat fallback
  > Manual review
```

Rules:

1. MGRS grammar is evaluated before UTM numeric X/Y to resolve grid-zone and
   latitude-band ambiguity.
2. A complete explicit UTM CRS intent outranks weak BFTM evidence such as X/Y
   headers, `Sommets`, or numeric ranges.
3. Explicit UTM and explicit BFTM evidence for the same group produce a
   conflict; neither silently wins.
4. Latitude/longitude columns accompanying an accepted UTM X/Y table are
   validation evidence, not a competing DMS or decimal route.
5. Independent tables or coordinate groups in one image may have different
   intents and must be routed separately.
6. Unknown projected X/Y remains typed projected data with no automatic
   transform.
7. Any structured intent blocks Chat, even when its own Quality Gate fails.

## 6. Quality Gate

### 6.1 CRS Gate

Automatic transformation requires all of the following:

- Projection is explicitly UTM.
- Datum is explicit and supported.
- Zone is an integer from 1 through 60.
- Hemisphere is explicit.
- EPSG agrees with datum, zone, and hemisphere.
- All coordinate rows in a group use one CRS.
- No unresolved CRS conflict exists.
- MGRS ambiguity has been excluded.

Missing or conflicting CRS evidence results in:

```text
requires_review = true
kml_ready = false
```

The original X/Y rows must be preserved for manual confirmation.

### 6.2 Row Gate

The row gate requires:

- Finite paired easting and northing values.
- Plausible UTM easting, normally `100000..900000`.
- Plausible UTM northing, `0..10000000`.
- Correct X/Y row pairing.
- No X/X or Y/Y column pairing.
- No OCR bounding-box pollution.
- Preserved labels, row order, and group boundaries.
- Sufficient points for the requested geometry.
- No unexplained missing or duplicate rows.

Numeric ranges validate a selected CRS; they never select it.

Fallback OCR results remain review-only unless a future frozen policy and
regression suite explicitly authorizes a narrower automatic path.

### 6.3 Transformation Gate

After CRS and row acceptance:

- Every X/Y point must transform successfully to finite WGS84 longitude and
  latitude.
- Latitude must agree with the declared hemisphere.
- The result must be consistent with the UTM zone area of use.
- All points must retain their source labels and group order.
- Geometry validation must pass.
- If the document also provides latitude/longitude, transformed points must
  agree within a tolerance derived from source precision.
- A failure or mismatch in any required row blocks the whole coordinate group
  from automatic KML export.

## 7. Migration Plan

### Phase 0: Baseline protection

- Preserve current BFTM, UTM30, MGRS, Kyrgyz GK, DMS, projected XY, and Chat
  baselines.
- Record current parser traces, precision modes, point counts, and KML output.
- Do not change runtime behavior.

### Phase 1: Shadow Intent Resolver

- Add the CRS Evidence Collector and UTM Intent Resolver in shadow mode.
- Emit datum, zone, hemisphere, EPSG, evidence, and conflicts for diagnostics.
- Do not alter current recognition, parser selection, conversion, or Export.
- Compare shadow decisions against existing UTM30, BFTM, MGRS, and Kyrgyz
  results.

### Phase 2: Generic UTM type and parser

- Add canonical `utm-projected-x-y` / `utm_projected_xy`.
- Route confirmed UTM intents to the generic UTM parser.
- Keep `utm30n-projected-x-y` as a frozen legacy alias.
- Preserve V1 output compatibility while V2 uses the canonical generic type.

The compatibility mapping is:

```text
legacy utm30n-projected-x-y
          |
          v
alias -> utm-projected-x-y
          |
          v
engine coordinate_type = utm_projected_xy
```

### Phase 3: Typed Quality Gate and Export

- Transform only confirmed and accepted UTM groups.
- Attach accepted WGS84 points to the typed result.
- Make V2 Export consume those accepted points.
- Disable frontend CRS re-detection for V2 typed UTM results.
- Retain a compatibility adapter for legacy V1 behavior until migration is
  frozen.

### Phase 4: UTM50S real-world validation

- Add the Indonesian WGS84 / UTM Zone 50S samples.
- Verify EPSG:32750 intent, X/Y row parsing, WGS84 transformation, and KML.
- Confirm no BFTM, MGRS, DMS, generic projected, or Chat capture.
- Lock the baseline only after repeated success and manual map verification.

### Phase 5: Registry and freeze

- Add the canonical generic UTM entry to the Coordinate Type Registry.
- Retain UTM30 as a documented legacy alias rather than deleting its history.
- Update Golden Baseline expectations only after shadow comparison and full
  regression pass.
- Freeze the generic type; do not add country- or zone-specific parsers.

## 8. Regression Plan

The future real sample directory is:

```text
regression-samples/UTM50S/
├── README.md
├── indonesia-mining-01.jpg
├── indonesia-mining-02.jpg
├── indonesia-mining-03.jpg
├── raw-ocr.json
├── expected.json
├── expected.kml
└── baseline.json
```

The sample contract must record:

- Canonical coordinate type and precision mode.
- Datum, zone, hemisphere, and EPSG.
- CRS evidence and parser trace.
- Original X/Y rows and point count.
- Expected first and last WGS84 points.
- Expected geometry and KML coordinate order.
- `requires_review` and `kml_ready`.
- Forbidden coordinate types and fallback modes.
- Coordinate summary and KML hashes.

`baseline.json` must not be created or locked before the real images pass
repeated recognition and manual KML verification.

### 8.1 Positive coverage

- Existing UTM30N sample remains correct through the legacy alias.
- WGS84 UTM Zone 50S resolves to EPSG:32750.
- At least one additional north zone and south zone text fixture proves that
  the resolver is dynamic rather than case-specific.
- X/Y with accompanying latitude/longitude validates within source precision.

### 8.2 Negative and conflict coverage

- Missing datum.
- Missing hemisphere.
- Missing zone.
- Zone 0 or 61.
- EPSG conflicts with zone or hemisphere.
- Bare `50S` with insufficient evidence.
- MGRS `50S` grid-zone/band structure.
- Explicit BFTM projection with X/Y.
- Explicit UTM and explicit BFTM in one group.
- Generic X/Y with no CRS evidence.
- Mixed zones or hemispheres in one group.
- X/X, Y/Y, or OCR bounding-box pollution.
- Transformed WGS84 disagrees with document latitude/longitude.
- Failed structured UTM intent attempts to fall through to Chat.

### 8.3 Mandatory compatibility suite

Before any UTM Router change can be frozen, all of the following must pass:

- BFTM.
- Legacy UTM30.
- MGRS / UTM Grid Reference.
- Kyrgyzstan GK.
- Madagascar cadastral.
- Generic/unknown projected XY.
- Standard and grouped DMS.
- WGS84 tables and Chat fallback.

## 9. Freeze Rules

The following are prohibited after this specification is adopted:

1. Inferring a UTM zone from numeric X/Y ranges.
2. Inferring CRS, zone, hemisphere, datum, or EPSG from country alone.
3. Treating UTM50S, UTM51S, UTM52S, or other zones as separate parsers.
4. Allowing generic X/Y headers or `Sommets` alone to establish BFTM intent.
5. Allowing BFTM numeric ranges to override explicit UTM CRS evidence.
6. Silently choosing UTM or BFTM when both have explicit conflicting evidence.
7. Treating `50S` as southern hemisphere without excluding MGRS band syntax.
8. Generating an EPSG identifier when datum, zone, or hemisphere is missing.
9. Applying WGS84 UTM transformation to an unsupported or unknown datum.
10. Re-parsing workspace or display text during Export.
11. Re-running CRS inference during Export.
12. Exporting projected X/Y that has not passed CRS, row, and transformation
    gates.
13. Falling through to Decimal or Chat after a structured UTM intent fails its
    Quality Gate.
14. Changing the frozen BFTM, UTM30, MGRS, or Kyrgyz GK behavior without
    explicit regression evidence and Registry review.

The safe failure state is always:

```json
{
  "coordinate_type": "utm_projected_xy",
  "crs_status": "incomplete_or_conflicted",
  "requires_review": true,
  "kml_ready": false,
  "points": [
    { "x": 778000, "y": 9720000 }
  ]
}
```

Preserving unconverted X/Y for review is preferable to generating a plausible
but incorrect KML.
