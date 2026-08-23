# Changelog

## GeoKit Lab v1.0.2

WGS84 and KML Closure

- Status: **Released**
- Release date: `2026-08-24`
- Formal tag: `v1.0.2`
- Production commit: `39abd7ac4ad937789c1fe04b5439721e3fcdb8ce`

### Included

- Restored WGS84 chat coordinate axis lock.
- Restored WGS84 golden regression baseline.
- Restored KML export permission closure.
- Added local-only development quota fallback validation.
- Added an exact-HTTPS, environment-configurable RC origin allowlist while preserving the default production origin boundary.
- Closed the final release identity across the page, API, changelog, release notes, tag, artifact, and Production deployment.

### Production verification

- RC3 Preview validation: PASS
- WGS84 stable recognition baseline: PASS
- KML export, Entitlement, quota consumption, and Usage path: PASS
- Production `/api/version`: `v1.0.2 — WGS84 and KML Closure`
- Production quota smoke: `3 → 2`
- Production RC Origin boundary: PASS (`403 invalid_origin`)
- Homepage, three tool routes, compliance Footer, Nginx, service status, and production logs: PASS

### Excluded

- Account/Billing changes
- Workspace changes
- OCR
- UTM/MGRS
- Coordinate Intelligence experiments

## GeoKit Lab v1.0.1 — Compliance / Commercial Foundation

- Added the GeoKit Lab company and business introduction.
- Added company contact information, office address, site disclaimer, ICP filing link, and a stable public-security filing slot.
- Standardized the user-facing brand as GeoKit Lab.
- Added the formal release identity to `/api/version`.

