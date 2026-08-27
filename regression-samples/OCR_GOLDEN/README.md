# OCR Golden Records

This directory formalizes real-image evidence for the GeoKit Lab v1.0.3 OCR P0.
It does not contain OCR implementation code or copies of private source images.

## Storage rule

- Original images remain in the existing excluded evidence directory.
- Every record pins the original image with an absolute evidence locator, SHA-256,
  byte size, and resolution.
- An image must not be copied into Git without a separate privacy and storage
  approval.
- A missing image or SHA-256 mismatch invalidates the record.

## Evidence levels

- `confirmed`: the original file, source type, coordinate truth, CRS, axis order,
  WGS84 result, and KML expectation are supported by inspectable evidence.
- `baseline_only`: historical regression evidence exists, but at least one field
  required for a complete record is unresolved.
- `hypothesis`: the expected result has not been independently verified.

## Development entry rule

OCR implementation does not start merely because records exist. The initial
production scope requires at least three `confirmed` positive-path records that:

1. are JPEG or PNG images;
2. contain a clear printed WGS84 decimal or geographic DMS coordinate region;
3. have an explicit axis order or unambiguous headers;
4. resolve to one ordered Polygon;
5. contain complete expected WGS84 and closed KML coordinates.

Negative-boundary and future-CRS records are valuable regression evidence, but
they do not satisfy the initial production-scope count.

## Current gate

- Confirmed initial-scope positive records: **3**
- OCR Golden Record Gate: **READY**
- The qualifying set contains three clear geographic DMS tables with explicit
  latitude/longitude order, a single-area Polygon, verified WGS84 truth, and a
  closed KML expectation.
- The decimal-table record with unresolved multi-area grouping, the low-clarity
  Review boundary, and the UTM 50S future-scope record remain excluded from the
  positive count.

The formalized records are stored in `records.yaml`.
