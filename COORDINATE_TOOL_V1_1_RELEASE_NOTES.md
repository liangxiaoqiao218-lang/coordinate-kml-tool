# Coordinate Tool v1.1 Release Notes

Status: Release Candidate / v1.1.0

Coordinate Tool v1.1 focuses on stabilizing coordinate recognition, KML generation readiness, and the quality gate around already-supported coordinate types. It is not a new coordinate-type expansion release.

## 1. Version Positioning

Coordinate Tool v1.1 is the Release Candidate for v1.1.0.

Goals:

- Stabilize recognized coordinate types that already have real samples.
- Keep KML generation safe and predictable.
- Use the Quality Gate as the release boundary.
- Separate stable recognition from experimental review workflows.
- Keep restricted regression images outside the public repository while validating them through the fixture manifest.

Non-goals:

- No new parser families.
- No new country-specific expansion.
- No Review Mode implementation.
- No default Image Pipeline rollout.

## 2. Officially Supported Types

### WGS84 Table

Support scope:

- Printed WGS84 coordinate tables.
- Explicit latitude / longitude or longitude / latitude table structures.
- Duplicate boundary points are allowed when the WGS84 table geometry policy validates the deduplicated geometry.

Current status: Stable

Known warnings:

- Country / region may be absent from the summary when evidence is insufficient.

### Mozambique Geographic Table

Support scope:

- Mozambique geographic coordinate tables with Order / Latitude / Longitude structure.
- Tete-style table samples covered by the Golden Baseline.
- Type lock prevents WGS84 Table / WGS84 Chat / decimal_latlon takeover after timeout or weak recognition.

Current status: Stable

Known warnings:

- Country display may be absent even when coordinate recognition is correct.
- Stability depends on the dedicated transcription path and row validation.

### Cote d'Ivoire

Support scope:

- Cote d'Ivoire geographic DMS table samples.
- Single-group and multi-group samples covered by the Quality Gate.
- Multi-group review flags are preserved at group level.
- Content-based routing starts the dedicated path without depending on a Chinese or country-specific upload filename.
- HANDWRITTEN retry routing cannot take over after a Cote d'Ivoire candidate is established.

Current status: Stable

Known warnings:

- V1 legacy precisionMode can differ from V2 coordinate_type.
- Country display may be absent in the product summary.
- Groups requiring review must remain visible and must not be silently flattened.

### BFTM

Support scope:

- Burkina Faso BFTM projected X/Y samples.
- Projected coordinate readiness is based on complete x / y / projection fields.

Current status: Stable

Known warnings:

- Country display may be absent.
- No projection conversion changes are included in v1.1; readiness only validates projected coordinate completeness.

### Kyrgyz GK

Support scope:

- Kyrgyz GK projected coordinate samples.
- Projected coordinate readiness is based on complete x / y / projection fields.

Current status: Stable

Known warnings:

- Country display may be absent.
- No projection conversion changes are included in v1.1; readiness only validates projected coordinate completeness.

### MGRS

Support scope:

- MGRS / UTM grid reference samples already covered by the Golden Baseline.
- MGRS integrity validation requires complete, unique, continuous labels and successful WGS84 conversion.
- Complete labels are normalized into point order before geometry validation.
- Missing labels, missing rows, duplicate points, conversion failures, and self-intersecting geometry remain review-blocked.

Current status: Stable

Known warnings:

- Country display may be absent.

### Standard DMS

Support scope:

- Standard DMS tables and grouped DMS samples that pass the current Quality Gate.
- Existing locked samples are supported when recognition and grouping match baseline.

Current status: Stable for locked grouped-DMS scope

Known warnings:

- Standard DMS is not a blanket guarantee for all DMS-looking images.
- Point/AZ DMS remains outside the v1.1 stable commitment.
- Low-clarity and oblique images remain outside the v1.1 stable commitment.

### Madagascar Cadastral Grid

Support scope:

- Madagascar cadastral grid samples covered by the current Golden Baseline.
- Grid rows are preserved without pretending that projected grid cells are automatically safe WGS84 polygons.

Current status: Candidate

Release boundary:

- Recognition and grid-row preservation are release-qualified for the current fixture.
- `kml_ready=false` remains the intentional safety result until a qualified conversion and geometry policy exists.

## 3. Experimental

### HANDWRITTEN_DMS

Support scope:

- Recognition assistance for handwritten DMS coordinate images.
- Group preservation and safety checks are part of the quality system.

Current status: Experimental

Release boundary:

- Requires human review.
- Does not guarantee automatic KML.
- Must not silently produce a final KML when `coordinate_type=handwritten_dms_experimental` and the result requires review or is not KML-ready.

Known warnings:

- Vision transcription can still change digits or DMS separators.
- Exact DMS row mismatches are classified as an Experimental Known Failure, not a Stable Release Blocker.
- v1.1 supports recognition assistance only; human confirmation remains required and automatic KML is not guaranteed.

## 4. Not Committed To In v1.1

The following are not part of the v1.1 stable promise:

- Point/AZ DMS
- Generic UTM30 projected X/Y
- Low-clarity images
- Oblique / skewed images

These may still be recognized in some cases, but they are not release-qualified until they pass locked baselines and repeatable gate runs.

## 5. Quality System

Coordinate Tool v1.1 is guarded by:

- Golden Baseline: machine-readable expected results for real samples.
- Regression Runner: repeatable PASS / PASS WITH WARNING / FAIL gate.
- Fixture Root: project-relative fixture resolution replaces personal absolute paths and Chinese-filename dependencies.
- Fixture Manifest: sample IDs, semantic upload filenames, file sizes, and SHA256 hashes are validated before the Gate starts.
- Error Library: historical regression tracking.
- Layer Classification: failures are classified as Vision, Parser, Engine, Resolver, KML, UI, or Gate.

Release interpretation:

- PASS: release-qualified for that sample.
- PASS WITH WARNING: usable, but warning must be understood before calling the type fully stable.
- FAIL: not release-qualified unless the failure is explicitly inside an Experimental scope and its safety boundary passes.

### v1.1.0 Final Gate

Validated commit:

- `e7e82d9484e81bf1257ea3c413b7dedfedb667dd`

Execution summary:

- 14/14 P0 image fixtures executed.
- 100/100 recognize requests returned HTTP 200 and entered the Vision pipeline.
- 0 rate-limit responses, 0 timeouts, and 0 fallback takeovers.
- Stable Release Blocker: 0.
- Experimental Known Failure: HANDWRITTEN_DMS exact-character mismatch.
- The 14 restricted fixture images remain private test assets and are not included in the source release or tag.

Release result:

- WGS84 Table, Mozambique Geographic Table, Cote d'Ivoire, BFTM, Kyrgyz GK, MGRS, and locked grouped Standard DMS satisfy the v1.1 stable release boundary.
- Madagascar remains a safe Candidate with KML disabled.
- HANDWRITTEN_DMS remains Experimental with mandatory review and KML blocking.

## 6. Next Stages

### v1.2

Priority candidates:

- Point/AZ DMS
- Generic UTM30 projected X/Y

Goals:

- Lock stable baselines.
- Resolve parser / engine gaps.
- Restore KML readiness only when baseline and geometry policies are safe.

### v2.0

Planned architecture work:

- Review Mode
- Vision Pipeline
- Image comparison and editable coordinate confirmation
- Manual correction state machine

Goals:

- Let users compare original image and editable coordinates side by side.
- Make human review a first-class workflow.
- Standardize image preprocessing, telemetry, and vision reliability experiments across products.
- Keep Review Mode development outside the v1.1 stable release line.
