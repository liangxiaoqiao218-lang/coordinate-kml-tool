# Coordinate Engine V2 RC Artifact Record

## Status

Artifact Record: READY

This document records the local immutable artifact identity for Coordinate
Engine V2 release candidate validation. It is an audit record only.

It does not approve Secondary deployment, Primary release, or UTM migration.

## 1. Artifact Identity

| Field | Value |
| --- | --- |
| releaseVersion | `coordinate-engine-v2-rc1` |
| gitCommit | `d2d690f09fa12685a50d221db308e81268b70bcd` |
| artifactHash | `sha256:f8f550a1bacc1594f97a6a6b212b4b20751cedca4b76a0852b4a5921f41792b7` |
| packageHash | `sha256:a44705631c59e6387b065920ee0ed8f10652dd734bde37b07d9ebc9472b2ed50` |
| buildTime | `2026-08-09T07:18:28Z` |

Hash definitions:

- `artifactHash` is the runtime payload hash. It excludes
  `release-manifest.json`.
- `packageHash` is the packaged archive hash. It includes
  `release-manifest.json`.

## 2. Artifact Contents

Included runtime files:

- `index.html`
- `admin.html`
- `server.js`
- `package.json`
- `package-lock.json`
- `pricing-config.js`
- `server/**`
- `assets/**`
- required share and QR image assets
- generated `release-manifest.json` inside the local artifact

Excluded from the artifact:

- `.env`
- `.env.*`
- API keys
- secrets
- Supabase credentials
- `node_modules`
- docs
- scripts
- regression samples
- logs
- screenshots
- temporary files

The generated artifact and the generated `release-manifest.json` instance are
local build outputs and must not be committed to Git.

## 3. Deployment Status

| Target | Status |
| --- | --- |
| Secondary Deployment | `NOT STARTED` |
| Primary Production | `BLOCKED` |
| UTM Migration | `HOLD_LEGACY` |

Secondary deployment remains blocked until the Secondary readiness evidence is
complete, including current Secondary identity, rollback artifact, runtime
environment checks, and deployment approval.

## 4. Rollback Status

| Item | Status |
| --- | --- |
| rollback artifact | `NOT AVAILABLE` |
| current Secondary stable artifact | `UNKNOWN` |
| rollback package hash | `UNKNOWN` |

Rollback readiness is not yet proven. Secondary deployment must not start until
a rollback artifact or equivalent recoverable rollback target is available and
recorded.

## 5. Release Identity Validation

Release Identity v1 validation for the generated RC artifact:

| Check | Result |
| --- | --- |
| build identity loaded from `release-manifest.json` | `PASS` |
| releaseVersion present | `PASS` |
| gitCommit present | `PASS` |
| artifactHash present | `PASS` |
| buildTime present | `PASS` |
| releaseIdentity status | `complete` |

## Final Gate

Artifact Record: READY

Secondary Deployment Readiness: NOT READY

Deployment Allowed: NO
