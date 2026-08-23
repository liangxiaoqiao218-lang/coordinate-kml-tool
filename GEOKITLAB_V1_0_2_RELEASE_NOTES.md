# GeoKit Lab v1.0.2

WGS84 and KML Closure

This patch release restores the stable WGS84 recognition baseline and the KML export permission closure. It does not introduce new product features.

## Base version

- Production base commit: `c735b66e1ab6ee929bcc79a29471ab923faa8bad`
- Base release: GeoKit Lab v1.0.1 — Compliance / Commercial Foundation

## RC source

- Functional closure commit: `24449ef7c3cb66ef6bca839cb68e9d446d7460db`
- Version metadata closure commit: `f0853bbf6479118842b9bfac16150cc2bcf1db28`
- Final RC2 commit: `10cc06a506649e8e59b1f0b4590b55f762fc96db`
- Final RC2 tag: `v1.0.2-rc2`
- RC2 source artifact SHA-256: `31d55330cc5d20e10be656e4996abe535ce14e3ed19512365f8f05b99fdc3b5d`
- RC2 artifact manifest SHA-256: `190761f9724bd599883a67a14f966b641f9634034e95fb5dbecb0a176cacb4fa`

## Fixed issues

- Restored the parser-order lock for unambiguous WGS84 Chat coordinates.
- Restored Point, LineString, and Polygon WGS84 golden regression coverage.
- Preserved Review for genuinely unresolved latitude/longitude ambiguity.
- Restored the KML export permission and quota-consumption regression closure.
- Added a local-only Usage fallback guarded by no Supabase, non-production mode, loopback address, and localhost hostname.
- Preserved production failure-closed behavior when Usage storage is unavailable.
- Added an optional `RC_ALLOWED_ORIGIN` exact-HTTPS origin for RC Preview validation without changing the default production allowlist.

## Regression evidence

- WGS84 golden samples: 4 PASS
- True WGS84 ambiguity Review: PASS
- Point KML: PASS
- LineString KML: PASS
- Polygon closure: PASS
- Quota consumption: PASS
- Quota exhaustion rejection: PASS (`CONVERT_QUOTA_EXHAUSTED`)
- Production fallback failure-closed check: PASS (`500 / db_disabled`)
- RC Preview provenance: PASS (`coordinate-kml-tool-rc`, `release/wgs84-kml-closure`, `10cc06a506649e8e59b1f0b4590b55f762fc96db`)
- RC Origin Guard validation: PASS
- RC Entitlement and Usage validation: PASS
- RC quota consumption sequence: PASS (`3 -> 2 -> 1 -> 0`)
- RC Point, LineString, and Polygon KML export: PASS
- RC quota exhaustion rejection: PASS (`CONVERT_QUOTA_EXHAUSTED`)
- RC smoke source: `rc-v1.0.2-kml-smoke-20260823-01`
- JavaScript syntax and Git diff checks: PASS

## Excluded scope

- Account and Billing changes
- Subscription changes
- Workspace, Organization, and Storage changes
- OCR changes
- UTM and MGRS changes
- Coordinate Intelligence experiments
- UI layout, Footer compliance structure, database schema, and production infrastructure changes
