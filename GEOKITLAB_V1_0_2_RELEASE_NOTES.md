# GeoKit Lab v1.0.2

WGS84 and KML Closure

This patch release restores the stable WGS84 recognition baseline and the KML export permission closure. It does not introduce new product features.

## Release status

- Status: `PRODUCTION RELEASE COMPLETE`
- Release date: `2026-08-24`
- Formal tag: `v1.0.2`
- Production commit: `39abd7ac4ad937789c1fe04b5439721e3fcdb8ce`
- Production backup: `/opt/geokitlab/backups/pre-v1.0.2-20260824-004818`

## Base version

- Production base commit: `c735b66e1ab6ee929bcc79a29471ab923faa8bad`
- Base release: GeoKit Lab v1.0.1 — Compliance / Commercial Foundation

## RC source

- Functional closure commit: `24449ef7c3cb66ef6bca839cb68e9d446d7460db`
- Version metadata closure commit: `f0853bbf6479118842b9bfac16150cc2bcf1db28`
- RC Origin Guard closure commit: `10cc06a506649e8e59b1f0b4590b55f762fc96db`
- Release identity closure commit: `39abd7ac4ad937789c1fe04b5439721e3fcdb8ce`
- Final RC tag: `v1.0.2-rc3`
- Formal release tag: `v1.0.2`
- Frozen source artifact: `GeoKitLab-v1.0.2-rc3-source.zip`
- Source artifact SHA-256: `b9a7b686fc9223ebf0ffe2edd7624b1c990477371279b29cdb271104076de2db`
- Artifact manifest SHA-256: `443bebab4c0ceda6193ba7ecbfbef6f5a5d6a1a315099f7ec3e4fb6ae1cba654`

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
- RC3 Preview provenance: PASS (`coordinate-kml-tool-rc`, `release/wgs84-kml-closure`, `39abd7ac4ad937789c1fe04b5439721e3fcdb8ce`)
- RC Origin Guard validation: PASS
- RC Entitlement and Usage validation: PASS
- RC quota consumption sequence: PASS (`3 -> 2 -> 1 -> 0`)
- RC Point, LineString, and Polygon KML export: PASS
- RC quota exhaustion rejection: PASS (`CONVERT_QUOTA_EXHAUSTED`)
- RC smoke source: `rc-v1.0.2-kml-smoke-20260823-01`
- JavaScript syntax and Git diff checks: PASS

## Production deployment

- Release transition: `v1.0.2-rc3 → v1.0.2 → Production`
- Server: `120.24.174.202`
- Application directory: `/opt/geokitlab/app`
- Service: `geokitlab.service`
- Deployed files: `server.js`, `index.html`, `package.json`, `package-lock.json`
- Dependency installation: not required
- Database, environment file, and Nginx configuration changes: none
- Rollback backup: `/opt/geokitlab/backups/pre-v1.0.2-20260824-004818`

## Production smoke evidence

- Public and internal `/api/version`: PASS (`v1.0.2 — WGS84 and KML Closure`)
- Homepage and three tool routes: HTTP 200
- WGS84 Chat golden sample: PASS without unnecessary Review
- KML Entitlement and Usage path: PASS
- Production quota consumption: PASS (`3 → 2`)
- RC Preview Origin on Production: correctly rejected (`403 invalid_origin`)
- ICP, public-security filing, company information, and Footer: PASS
- `geokitlab.service`: active
- Nginx configuration: valid
- New service-log and browser-console errors: none

## Excluded scope

- Account and Billing changes
- Subscription changes
- Workspace, Organization, and Storage changes
- OCR changes
- UTM and MGRS changes
- Coordinate Intelligence experiments
- UI layout, Footer compliance structure, database schema, and production infrastructure changes
