# GeoKit Lab v1.0.2

WGS84 and KML Closure

This patch release restores the stable WGS84 recognition baseline and the KML export permission closure. It does not introduce new product features.

## Base version

- Production base commit: `c735b66e1ab6ee929bcc79a29471ab923faa8bad`
- Base release: GeoKit Lab v1.0.1 — Compliance / Commercial Foundation

## RC source

- RC commit: `24449ef7c3cb66ef6bca839cb68e9d446d7460db`
- RC tag: `v1.0.1-rc1`
- RC source artifact SHA-256: `6689c61fdd470c46e88336d0afae41324c21307b1d02d579cb9fad86b1a5cf5e`

## Fixed issues

- Restored the parser-order lock for unambiguous WGS84 Chat coordinates.
- Restored Point, LineString, and Polygon WGS84 golden regression coverage.
- Preserved Review for genuinely unresolved latitude/longitude ambiguity.
- Restored the KML export permission and quota-consumption regression closure.
- Added a local-only Usage fallback guarded by no Supabase, non-production mode, loopback address, and localhost hostname.
- Preserved production failure-closed behavior when Usage storage is unavailable.

## Regression evidence

- WGS84 golden samples: 4 PASS
- True WGS84 ambiguity Review: PASS
- Point KML: PASS
- LineString KML: PASS
- Polygon closure: PASS
- Quota consumption: PASS
- Quota exhaustion rejection: PASS (`CONVERT_QUOTA_EXHAUSTED`)
- Production fallback failure-closed check: PASS (`500 / db_disabled`)
- JavaScript syntax and Git diff checks: PASS

## Excluded scope

- Account and Billing changes
- Subscription changes
- Workspace, Organization, and Storage changes
- OCR changes
- UTM and MGRS changes
- Coordinate Intelligence experiments
- UI, Footer, compliance, database, and infrastructure changes
