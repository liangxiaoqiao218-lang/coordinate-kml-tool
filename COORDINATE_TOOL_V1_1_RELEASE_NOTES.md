# Coordinate Tool v1.1 Release Notes

Status: Stable Candidate

Coordinate Tool v1.1 focuses on stabilizing coordinate recognition, KML generation readiness, and the quality gate around already-supported coordinate types. It is not a new coordinate-type expansion release.

## 1. Version Positioning

Coordinate Tool v1.1 is a Stable Candidate release.

Goals:

- Stabilize recognized coordinate types that already have real samples.
- Keep KML generation safe and predictable.
- Use the Quality Gate as the release boundary.
- Separate stable recognition from experimental review workflows.

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

Current status: Stable Candidate

Known warnings:

- Country display may be absent even when coordinate recognition is correct.
- Stability depends on the dedicated transcription path and row validation.

### Cote d'Ivoire

Support scope:

- Cote d'Ivoire geographic DMS table samples.
- Single-group and multi-group samples covered by the Quality Gate.
- Multi-group review flags are preserved at group level.

Current status: Stable Candidate

Known warnings:

- V1 legacy precisionMode can differ from V2 coordinate_type.
- Country display may be absent in the product summary.
- Groups requiring review must remain visible and must not be silently flattened.

### BFTM

Support scope:

- Burkina Faso BFTM projected X/Y samples.
- Projected coordinate readiness is based on complete x / y / projection fields.

Current status: Stable Candidate

Known warnings:

- Country display may be absent.
- No projection conversion changes are included in v1.1; readiness only validates projected coordinate completeness.

### Kyrgyz GK

Support scope:

- Kyrgyz GK projected coordinate samples.
- Projected coordinate readiness is based on complete x / y / projection fields.

Current status: Stable Candidate

Known warnings:

- Country display may be absent.
- No projection conversion changes are included in v1.1; readiness only validates projected coordinate completeness.

### MGRS

Support scope:

- MGRS / UTM grid reference samples already covered by the Golden Baseline.

Current status: Stable Candidate

Known warnings:

- Country display may be absent.

### Standard DMS

Support scope:

- Standard DMS tables and grouped DMS samples that pass the current Quality Gate.
- Existing locked samples are supported when recognition and grouping match baseline.

Current status: Partial Stable Candidate

Known warnings:

- Standard DMS is not a blanket guarantee for all DMS-looking images.
- Point/AZ DMS remains outside the v1.1 stable commitment.
- Low-clarity and oblique images remain outside the v1.1 stable commitment.

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
- Exact DMS row matching is enforced by the Quality Gate and currently remains a blocker for full stable release.

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
- Error Library: historical regression tracking.
- Layer Classification: failures are classified as Vision, Parser, Engine, Resolver, KML, UI, or Gate.

Release interpretation:

- PASS: release-qualified for that sample.
- PASS WITH WARNING: usable, but warning must be understood before calling the type fully stable.
- FAIL: not release-qualified.

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

Goals:

- Let users compare original image and editable coordinates side by side.
- Make human review a first-class workflow.
- Standardize image preprocessing, telemetry, and vision reliability experiments across products.
