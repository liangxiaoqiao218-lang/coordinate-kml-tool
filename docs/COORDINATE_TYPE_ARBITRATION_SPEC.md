# Coordinate Type Arbitration Layer

## Purpose

Coordinate extraction may produce several valid-looking candidates from one image. The arbitration layer selects one authoritative coordinate type after evidence collection and parsing. Parsers do not select the final type, and Export does not reinterpret OCR text.

## Priority

1. Explicit CRS Evidence
2. Typed Projection
3. Structured XY
4. Validated WGS84
5. DMS
6. Chat

Dedicated BFTM, MGRS, and Kyrgyz GK type locks are explicit coordinate-system evidence. They remain isolated from generic UTM and generic projected-XY fallbacks.
Dedicated DMS table/prose locks likewise keep their parser contract when a generic WGS84 detector sees the same latitude/longitude values.

Explicit evidence and numeric heuristics are different authority levels. An accepted explicit Typed UTM result wins over a heuristic BFTM/UTM30 candidate. If two explicit CRS sources disagree, arbitration returns `crs_conflict`, requires review, and blocks KML.
If explicit UTM evidence is confirmed but a validated structured-X/Y result is unavailable, the result remains UTM in review state. It must not fall through to a heuristic BFTM, UTM30, DMS, or Chat interpretation.

## Canonical UTM Rule

When the same document supplies explicit `UTM`, `WGS84`, zone, hemisphere, and a structured X/Y table, the final result is:

```json
{
  "coordinateType": "utm_projected_xy",
  "precisionMode": "utm-projected-x-y",
  "lat_lon_role": "verification_only"
}
```

Latitude/Longitude columns validate the X/Y transformation. They never become the primary parser while the explicit typed UTM candidate is authoritative. A failed transformation comparison retains the UTM type, sets `requires_review=true`, and blocks KML; it does not fall back to DMS.

## Coordinate-order safety

Any `possible swapped lat/lon` result must set `requires_review=true` and `kml_allowed=false`. UI and Export must enforce the block before usage consumption or file generation.

## Readiness lifecycle

Arbitration eligibility is not export authorization:

```text
arbitrationEligible
  -> confirmationStatus
  -> qualityGateStatus
  -> kml_ready
```

An explicit UTM result starts with `arbitrationEligible=true`, `confirmationStatus=awaiting_confirmation`, and `kml_ready=false`. CRS evidence confirmation never substitutes for user confirmation.

## Unique response finalizer

Every successful recognition response passes through `finalizeCoordinateResponse` inside `buildCoordinateVerificationResponse`. This includes the main OCR path, Dedicated Type direct returns, timeout retries, and local OCR fallback. Existing payload fields and `parserTrace` are preserved; the finalizer adds or normalizes the arbitration and readiness fields.

## Compatibility locks

- Legacy `utm30n-projected-x-y` remains a compatibility alias until its migration is separately approved.
- BFTM stays on `bftm-projected-x-y`.
- MGRS stays on `mgrs-utm-grid-reference` and blocks generic UTM takeover.
- Kyrgyz GK stays on `kyrgyz-gk-point-x-y`.
- Dedicated and ordinary DMS paths remain below validated WGS84 but above Chat.

## Data flow

```text
OCR / Vision candidates
  -> CRS and format evidence
  -> dedicated parsers
  -> Coordinate Type Arbitration
  -> final coordinateType / precisionMode / requires_review / kml_allowed
  -> Confirmation and Quality Gate
  -> Export
```
