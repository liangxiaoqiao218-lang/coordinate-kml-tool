# Coordinate Engine V3 Production Result Contract

This document defines the internal V3 supported-scope production result mapper.
It does not enable V3 in public production and does not integrate V3 with
`server.js` or `index.html`.

## Boundary

- Public production integration: not implemented.
- Primary runtime: unchanged.
- Router thresholds: unchanged.
- Provider/model/acquisition behavior: unchanged.
- Recognizer and Runner behavior: unchanged.
- Retry/fallback behavior: unchanged.

The mapper consumes normalized V3 Runner / acquisition-adapter evidence and
produces a product-facing contract that separates recognition uncertainty from
technical KML constructability.

## Output states

### SUCCESS

The result is inside V3 supported scope, has valid normalized coordinates, has
technically constructable KML geometry, and has no review reason.

### REVIEW_REQUIRED

The result has meaningful coordinate evidence but cannot be silently treated as
production-supported success. This includes incomplete extraction, ambiguous
ownership, recognizer coverage gaps, provider timeout with partial data,
experimental acquisition paths, low-confidence evidence, or review-only scope.

Review does not make technically valid KML invalid. If the normalized geometry
is constructable, `technicalKmlReady` remains `true`.

### UNSUPPORTED

The mapper cannot produce a usable production result. This state is reserved
for no usable candidate, absent normalized coordinates without a reviewable
reason, invalid coordinate structure, invalid geometry, or unsupported coordinate
type/scope without sufficient product-safe evidence.

## Supported scope v1

The first V3 production-supported internal scope is:

- `cote_divoire_dms`
- `indonesia_utm`
- `wgs84_decimal`
- `generic_dms`
- `dms_grouped_coordinates`

These recognizers may map to `SUCCESS` when their normalized result is complete
and technically KML-ready.

## Review-only and experimental exclusions

Experimental acquisition paths must not silently become production success even
when the deterministic recognizer output is technically valid. They map to
`REVIEW_REQUIRED / EXPERIMENTAL_PATH_REQUIRED`.

Examples:

- `table_context_composite`
- `full_image_ocr`
- structural-router-selected path
- Indonesia #003 complex experimental path
- `qwen-vl-ocr-latest` experimental model path

Recognizers that are implemented but not yet in supported production scope map
to review-only output until separately authorized.

## Reason codes

Review and unsupported reason codes include:

- `INCOMPLETE_EXTRACTION`
- `RECOGNIZER_NOT_AVAILABLE`
- `AMBIGUOUS_COORDINATE_SYSTEM`
- `AMBIGUOUS_RECOGNIZER`
- `CANDIDATE_CONFLICT`
- `PROVIDER_TIMEOUT`
- `LOW_CONFIDENCE`
- `PARTIAL_ROWS_RECOVERED`
- `EXPERIMENTAL_PATH_REQUIRED`
- `UNVERIFIED_PRODUCTION_SCOPE`
- `NO_USABLE_CANDIDATE`
- `UNSUPPORTED_COORDINATE_TYPE`
- `NO_NORMALIZED_COORDINATES`
- `INVALID_COORDINATE_STRUCTURE`
- `INVALID_GEOMETRY`
- `UNSUPPORTED_PRODUCTION_SCOPE`

When partial evidence exists, the mapper reports the most useful recovery reason
instead of collapsing everything to "no coordinates." For example, a provider
timeout with partial data reports `PROVIDER_TIMEOUT`, and a not-yet-ported
recognizer reports `RECOGNIZER_NOT_AVAILABLE`.

## KML policy

Recognition certainty and KML export constructability are separate concepts.

Allowed technical hard-stop reasons are limited to:

- `NO_COORDINATES`
- `NON_NUMERIC_COORDINATES`
- `INSUFFICIENT_DATA_FOR_REQUESTED_GEOMETRY`
- `UNPARSABLE_COORDINATE_STRUCTURE`
- `INVALID_GEOMETRY`

The following are not technical KML hard stops by themselves:

- low confidence
- review required
- CRS uncertainty
- recognizer not available
- candidate conflict
- provider timeout with partial data

If geometry is technically valid, review remains warning-oriented and the mapper
preserves `technicalKmlReady=true`.

## Production readiness

This contract is internal only. It is a prerequisite for a future supported-scope
production integration, but it does not authorize public production routing,
deployment, or default V3 enablement.
