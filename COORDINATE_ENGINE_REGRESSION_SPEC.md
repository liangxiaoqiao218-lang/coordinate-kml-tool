# Coordinate Engine Regression Runner Specification

Status: Draft Specification  
Scope: Coordinate Engine regression verification only  
Applies to:
- Coordinate parser changes
- OCR / Vision Retry changes
- Quality Gate changes
- Frontend export changes
- New coordinate type onboarding

This document defines the required Regression Runner behavior for Coordinate Engine. It is a specification only and does not implement code.

## 1. Regression Runner Flow

The runner must execute each regression sample through the same logical stages as production:

```text
Sample
-> OCR / Dedicated Vision
-> Parser
-> Quality Gate
-> Compare Expected
-> Export Verification
-> PASS / FAIL Report
```

### Stage Responsibilities

#### Sample

The sample provides the input image or text fixture and its expected metadata.

#### OCR / Dedicated Vision

The runner invokes the same OCR or dedicated vision path used by production for that coordinate type.

If a sample allows OCR variance, the runner may compare only the final accepted structured result. If a sample does not allow OCR variance, the runner must also compare raw OCR / transcription output.

#### Parser

The runner verifies that the intended parser accepts the sample and that competing parsers do not take ownership.

#### Quality Gate

The runner verifies that the parser output passes the coordinate type's Quality Gate.

If the Quality Gate fails, the result must be `FAIL` unless the sample explicitly expects an unstable/no-KML result.

#### Compare Expected

The runner compares actual values against the sample's expected values:

- precisionMode
- parserTrace
- point count
- first coordinate
- last coordinate
- optional full coordinate list
- optional KML hash

#### Export Verification

The runner verifies that generated KML uses the Quality Gate accepted data and does not reparse raw text or display-only columns.

## 2. Regression Sample Metadata

Every regression sample must define the following fields.

```yaml
id: unique_sample_id
fileName: sample_image_or_text_name
inputType: image | text
samplePath: path_or_external_location
coordinateType: coordinate_type_id
expectedPrecisionMode: expected_precision_mode
expectedParserTrace:
  - OCR
  - TYPE:accepted
expectedPointCount: 0
expectedFirstCoordinate: longitude,latitude,0
expectedLastCoordinate: longitude,latitude,0
expectedGeometry: Point | LineString | Polygon | MultiPolygon
kmlHash: optional_hash
allowOcrVariance: true | false
requires20Run: true | false
expectedUnstable: true | false
notes: human_readable_context
```

### Required Fields

- `fileName`
- `inputType`
- `coordinateType`
- `expectedPrecisionMode`
- `expectedParserTrace`
- `expectedPointCount`
- `expectedFirstCoordinate`
- `expectedLastCoordinate`
- `expectedGeometry`
- `allowOcrVariance`
- `requires20Run`

### Optional Fields

- `samplePath`
- `kmlHash`
- `expectedFullCoordinates`
- `expectedRawTextSnippets`
- `expectedWarnings`
- `expectedGroups`
- `forbiddenPrecisionModes`
- `forbiddenParserTraceEntries`

### Text Fixture Limitation in V1 Runner

The V1 Regression Runner can call the image upload endpoint
`/api/recognize-coordinates` for `inputType: image` samples.

For `inputType: text` samples, V1 does not execute real parser logic yet because
there is no dedicated text parser endpoint. These samples must be reported as:

```text
SKIPPED_TEXT - text fixture requires text parser endpoint
```

`SKIPPED_TEXT` is not a PASS and does not count as a FAIL. It exists to keep
text fixtures visible in the matrix until a text parser endpoint or local parser
harness is added.

### Single-Type Runner Mode

The V1 Regression Runner supports focused runs with `--type`.

`--type` values match `regression-samples/<TYPE>` directory names, not Type IDs.
Matching is case-insensitive. Multiple directories can be supplied as a
comma-separated list.

Examples:

```text
node scripts/coordinate-regression-runner.js --type MGRS
node scripts/coordinate-regression-runner.js --type BFTM
node scripts/coordinate-regression-runner.js --type RC2
node scripts/coordinate-regression-runner.js --type MGRS,BFTM,RC2
```

If `--type` is omitted, the runner must keep the default full-matrix behavior.

If a requested directory does not exist, the runner must print the unknown type,
print the available type directories, and exit with code `1`.

The summary must count only the selected directories.

## 3. Required Validation Checks

The Regression Runner must check all of the following for every sample.

### precisionMode

The actual `precisionMode` must equal the expected value.

Example:

```text
expected: wgs84-table-coordinates
actual:   wgs84-chat-coordinates
result:   FAIL
```

### parserTrace

The actual parser trace must include the expected accepted parser path.

For structured coordinate types, the runner must also confirm that WGS84 Chat did not accept the sample unless the sample type is explicitly `WGS84 Chat`.

### Point Count

The actual accepted point count must match the expected point count.

Duplicate boundary points must be preserved when the coordinate type requires them.

### First Coordinate

The actual first KML coordinate must match the expected first coordinate.

Format:

```text
longitude,latitude,0
```

### Last Coordinate

The actual last KML coordinate must match the expected last coordinate.

For Polygon exports, closure should be checked separately from the logical last source point.

### Quality Gate

The runner must confirm that the coordinate type's Quality Gate accepted the result.

If the result is weak, unstable, or incomplete, the runner must fail unless the sample is explicitly testing an expected unstable result.

### Export

The runner must verify that KML export uses accepted coordinates from Quality Gate output.

Export validation must catch:

- latitude/longitude reversal
- flattened grouped polygons
- missing duplicate boundary points
- missing automatic polygon closure
- frontend fallback overwriting backend coordinates
- export from display-only WGS84 text instead of KML coordinate rows

## 4. Baseline Lock

Regression samples may include a locked baseline file:

```text
regression-samples/<TYPE>/baseline.json
```

`baseline.json` is the canonical comparison source once a sample has been
reviewed and frozen. `expected.json` remains the sample metadata and input
fixture descriptor, but `baseline.json` decides whether the current run is a
stable match or a baseline change.

### Baseline Fields

```json
{
  "typeId": "wgs84-table-coordinates",
  "precisionMode": "wgs84-table-coordinates",
  "parserTraceAlias": ["OCR", "WGS84_TABLE:accepted"],
  "pointCount": 11,
  "geometry": "Polygon",
  "firstCoordinate": "16.0320,3.7638,0",
  "lastCoordinate": "16.0348,3.7351,0",
  "coordinateSummaryHash": "sha256-of-normalized-coordinate-rows",
  "status": "locked",
  "baselineCommit": "commit-hash",
  "notes": "Human-reviewed stable baseline."
}
```

`kmlCoordinateHash` may be used instead of `coordinateSummaryHash` when the
baseline represents exported KML coordinates rather than accepted coordinate
rows.

### Baseline Status

- `locked`: the result is expected to match the baseline.
- `pending`: the sample exists but is not yet a release gate.
- `unstable`: the sample is known to be vision/OCR unstable and must be
  reported, but does not count as a hard fail unless it produces wrong KML.

### Baseline Missing

If a sample has `expected.json` but no `baseline.json`, the runner must report:

```text
BASELINE_MISSING
```

`BASELINE_MISSING` is not a parser failure and must not be counted as
`REAL FAIL`. It means the regression sample has not yet been reviewed and
locked.

### Baseline Changed

If `baseline.json` exists and the current run differs materially from the
baseline, the runner must report:

```text
BASELINE_CHANGED
```

Material changes include:

- `precisionMode` changed
- `pointCount` changed
- `parserTraceAlias` no longer matches
- first coordinate changed beyond tolerance
- last coordinate changed beyond tolerance
- `coordinateSummaryHash` / `kmlCoordinateHash` changed

`BASELINE_CHANGED` is a release-blocking result and must return exit code `1`.

### Baseline Normalize Rules

Baseline comparison keeps the existing normalization rules:

- `PROJECTED:655000,1333600` equals `655000,1333600`
- trailing KML altitude `,0` is ignored for coordinate comparison
- numeric coordinate tolerance defaults to `1e-6`
- parser traces support contains matching
- parser traces support aliases such as
  `DMS_GROUPED(blank_line):accepted` matching `DMS_GROUPED:accepted`

### Baseline Creation Policy

The V1 runner must not auto-generate or auto-update `baseline.json`.
Baselines must be created through explicit human review after the sample has a
trusted result.

## 5. 20-Run Stability Verification

Vision-dependent coordinate types require repeated validation.

If `requires20Run = true`, the runner must execute the same sample 20 times and report:

- accepted count
- expected point count match count
- precisionMode mismatch count
- parserTrace mismatch count
- OCR/vision unstable count
- Chat takeover count
- wrong KML count

### 20-Run Pass Rule

A 20-run sample passes only if:

```text
20 / 20 precisionMode matches
20 / 20 point count matches
20 / 20 Quality Gate passes
20 / 20 export validation passes
0 Chat takeovers for structured types
0 wrong KML outputs
```

If a sample intentionally expects instability, the runner must confirm that unstable results do not produce KML.

## 6. PASS / FAIL Output Format

Each sample result must produce a structured block.

### PASS Example

```text
PASS BFTM_LONG_TABLE
type: BFTM Projected XY
precisionMode: bftm-projected-x-y
parserTrace: OCR -> BFTM:accepted
points: 20 / 20
first: 655000,1333600
last: 655000,1356200
export: PASS
20-run: PASS
```

### FAIL Example

```text
FAIL RC2_WGS84_TABLE
type: WGS84 Table
expectedPrecisionMode: wgs84-table-coordinates
actualPrecisionMode: wgs84-chat-coordinates
expectedPoints: 11
actualPoints: 9
firstExpected: 16.0320,3.7638,0
firstActual: 3.7638,16.0320,0
failureReason: Chat parser captured structured WGS84 table
export: FAIL
```

### Regression Summary Example

```text
Regression Summary
total: 12
PASS: 8
REAL FAIL: 1
FORMAT DIFFERENCE: 1
API BLOCKED: 0
PENDING: 1
UNSTABLE: 1
SKIPPED_TEXT: 0
BASELINE_MISSING: 0
BASELINE_CHANGED: 0

Failed:
- Madagascar cadastral: stable sample missing

Unstable:
- Mozambique Geographic Table: 20-run rows=0 occurred
```

## 7. Release Policy

Any commit that changes coordinate recognition behavior must follow this release sequence:

```text
Code Change
-> Add / Update Regression Sample
-> Update Stable Paths
-> Run Regression Runner
-> All Required Samples PASS
-> 20-Run Stability PASS where required
-> Commit
-> Deploy
-> Online Verification
```

No parser, OCR, Vision Retry, Quality Gate, or export change may be merged unless the relevant Regression Runner result is PASS.

## 8. Merge Gate Requirements

A coordinate-related commit is mergeable only when:

- all affected samples pass
- all unaffected stable samples still pass
- structured samples do not fall into WGS84 Chat
- export KML matches accepted coordinates
- no unrelated files are included
- regression evidence is recorded

If any sample fails, the commit must not be merged unless:

- the failure is explicitly documented as sample missing or expected unstable
- no wrong KML is generated
- the issue is not caused by the current change

## 9. Coordinate Type Coverage

The Regression Runner must include at least these coordinate types:

1. BFTM Projected XY
2. Burkina UTM30 / X-Y
3. RC2 / WGS84 Table
4. Mozambique Geographic Table
5. MGRS / UTM Grid Reference
6. Kyrgyz GK
7. Madagascar Cadastral
8. French Perimeter DMS
9. Point A-Z DMS Table
10. DMS_GROUPED
11. Ordinary DMS
12. WGS84 Chat

## 10. Forbidden Regression Shortcuts

The Regression Runner must not allow:

- API-only validation when the bug is in frontend export
- frontend-only validation when the bug is in backend parser routing
- text-only simulation when the issue is OCR/vision-specific
- single-run validation for vision-dependent samples
- accepting a wrong precisionMode because KML "looks close"
- accepting missing duplicate boundary points
- accepting rows=0 for a structured table unless unstable/no-KML is expected

## 11. Artifact Storage

Each regression directory should contain:

```text
README.md
sample image path or image fixture
ocr_raw.txt
expected.json
baseline.json
expected.kml or expected_kml_coordinates.txt
latest_result.json
```

If real images cannot be committed, the README must record:

- external path
- sample owner
- expected precisionMode
- expected parserTrace
- expected point count
- expected first and last coordinates
- whether the sample is mandatory or pending

## 12. Governance

The Regression Runner is part of the Coordinate Engine Freeze Policy.

Future coordinate development must treat the runner as the release gate, not as a post-release diagnostic tool.

The safe default is:

```text
No Regression PASS
-> No Merge
-> No Deploy
```
