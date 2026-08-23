# GeoKit Lab

**Product Operating System**
**Version 1.0**
**Day 0: 2026-08-14**

## Related Documents

- [GEOKITLAB_CONSTITUTION](GEOKITLAB_CONSTITUTION.md)
- [ENGINEERING_CONSTITUTION](ENGINEERING_CONSTITUTION.md)
- [PRODUCT_ROADMAP](PRODUCT_ROADMAP.md)
- [PROFIT_MAP](PROFIT_MAP.md)
- [PRODUCT_BACKLOG](PRODUCT_BACKLOG.md)
- [DECISION_LOG](DECISION_LOG.md)

## Decision Log

This is the project's single record for major product direction, product principles, engineering principles, brand decisions, commercialization decisions, and release decisions. It does not record ordinary development work.

## Day 0 Decisions

### DL-001 — Product mission

- Category: Product direction
- Decision: GeoKit Lab exists to help users make more money, lose less money, and make better decisions faster.
- Status: Active
- Date: 2026-08-14

### DL-002 — Product principles do not drift

- Category: Product principle
- Decision: Principles may be upgraded but must not change because of short-term pressure, individual requests, or temporary situations. Every exception requires a Decision Log entry.
- Status: Active
- Date: 2026-08-14

### DL-003 — Release Gate governs engineering

- Category: Engineering principle
- Decision: Every release must pass the Release Gate, use a clean working tree, preserve single-purpose commits, and keep bug fixes free of new functionality.
- Status: Active
- Date: 2026-08-14

### DL-004 — GeoKit Lab is the public brand

- Category: Brand decision
- Decision: User-visible product identity is consistently GeoKit Lab.
- Status: Active
- Date: 2026-08-14

### DL-005 — The roadmap is stage-driven

- Category: Product direction
- Decision: Product development progresses through Alpha, Commercial Foundation, Product-Market Fit, and Scale rather than calendar-year planning.
- Status: Active
- Date: 2026-08-14

### DL-006 — Commercial Foundation requires evidence

- Category: Commercialization decision
- Decision: Commercialization proceeds only after user value, operational readiness, compliance, and release evidence are validated.
- Status: Active
- Date: 2026-08-14

### DL-007 — v1.0.1 remains release-blocked

- Category: Release decision
- Decision: GeoKit Lab v1.0.1 must not be pushed or deployed until every P0 Release Gate blocker passes.
- Status: Superseded by DL-009 for the isolated compliance release only
- Date: 2026-08-14

### DL-008 — Product Operating System begins at Day 0

- Category: Product principle
- Decision: Product decisions, engineering rules, roadmap stages, profit evidence, backlog outcomes, and major decisions operate as one Product Operating System.
- Status: Active
- Date: 2026-08-14

## Compliance Closure Decisions

### DL-009 — ICP and public-security compliance workflow is closed

- Category: Release decision / compliance decision
- Decision: GeoKit Lab's domestic website compliance foundation is complete. ICP filing and public-security filing are both active, the Final Compliance Hotfix is live, compliance blockers are zero, and the Footer compliance structure is frozen.
- Context:
  - ICP filing: `粤ICP备2026099318号-1`
  - Public-security filing: `粤公网安备44030002015944号`
  - Production release source: `c735b66e1ab6ee929bcc79a29471ab923faa8bad`
  - Production backup: `/opt/geokitlab/backups/pre-final-public-security-filing-20260823-122024`
  - Final Compliance Hotfix: `PRODUCTION PASS`
- Consequences:
  - The Compliance Hotfix workflow is closed and the Alpha Closing mainline resumes.
  - Compliance blockers are zero.
  - Except for regulatory changes, company-entity changes, filing-information changes, or a severe compliance error, the Footer compliance structure must not be modified or redesigned.
  - This decision supersedes DL-007 only for the isolated v1.0.1 compliance release. Product-stability Release Gate blockers remain subject to a new evidence-based audit.
- Status: Active
- Date: 2026-08-23
- Supersedes: DL-007 for the isolated compliance release

## Environment Strategy Decisions

### DL-010 — Environment Strategy Decision — Lightweight Release Candidate

- Category: Engineering principle / release decision
- Decision: GeoKit Lab adopts **Option B — Lightweight Release Candidate** as its environment and release strategy.
- Context:
  - Date approved: `2026-08-23`
  - Status: `IMPLEMENTED`
  - First completed release: `GeoKit Lab v1.0.2`
  - Current Render service: `coordinate-kml-tool-rc`
  - Render responsibility during a formal release: Release Candidate Preview
  - Environment flow: `Local → Development Preview → release/vX.Y.Z → Render RC Preview → Release Artifact → ECS Production`
- Principles:
  - Content consistency does not equal release consistency.
  - A formal release must provide content consistency, traceable provenance, a frozen artifact, and a verified rollback path.
  - Development Preview and Release Candidate Preview must not share responsibility: experimental validation cannot approve a production release.
- Git responsibilities:
  - Do not establish a long-lived `develop` branch.
  - Keep the lightweight flow `feature/* → release/* → main`.
  - A `release/*` branch is temporary, contains only the frozen release scope, and is deleted after release completion.
- Render responsibilities:
  - `coordinate-kml-tool-rc` temporarily serves as Release Candidate Preview during a formal release.
  - Render RC Preview must identify its repository, branch, deployed commit, deployment ID, build time, and deploy time.
  - Render RC Preview must not also host experimental development work during RC validation.
- Production rules:
  - Production accepts only a commit that has passed the RC Gate, its corresponding Release Artifact, and a verified rollback point.
  - Production releases must retain the binding `Commit SHA + Release Artifact + Production Backup`.
  - Deploying the current Working Tree directly is prohibited.
  - Temporary business-code edits on ECS are prohibited.
- Release Gate responsibilities:
  - Local and Development Preview can provide development evidence but cannot approve Production.
  - Only Render RC Preview can approve promotion to Production.
  - Production verification confirms the release result; it does not replace RC approval.
- Consequences:
  - The strategy was first executed successfully for GeoKit Lab v1.0.2.
  - Future formal releases reuse the same lightweight RC flow unless a later Decision Log entry changes it.
- Status: Active / Implemented
- Implementation: Complete for v1.0.2
- Date: 2026-08-23
- Supersedes: None

## Production Release Closure Decisions

### DL-011 — GeoKit Lab v1.0.2 Production Release Closure

- Category: Release decision / engineering governance
- Decision: GeoKit Lab v1.0.2 is released to Production, and `39abd7ac4ad937789c1fe04b5439721e3fcdb8ce` becomes the only trusted Production Baseline for subsequent release work.
- Context:
  - Release: `GeoKit Lab v1.0.2 — WGS84 and KML Closure`
  - Formal tag: `v1.0.2`
  - Production commit: `39abd7ac4ad937789c1fe04b5439721e3fcdb8ce`
  - Final RC tag: `v1.0.2-rc3`
  - Frozen artifact: `GeoKitLab-v1.0.2-rc3-source.zip`
  - Artifact SHA-256: `b9a7b686fc9223ebf0ffe2edd7624b1c990477371279b29cdb271104076de2db`
  - Manifest SHA-256: `443bebab4c0ceda6193ba7ecbfbef6f5a5d6a1a315099f7ec3e4fb6ae1cba654`
  - Production server: `120.24.174.202`
  - Production directory: `/opt/geokitlab/app`
  - Production service: `geokitlab.service`
  - Rollback backup: `/opt/geokitlab/backups/pre-v1.0.2-20260824-004818`
  - Release chain: `c735b66 → 24449ef → f0853bb → 10cc06a → 39abd7a → v1.0.2 → Production`
- Validation evidence:
  - RC Preview provenance and RC3 smoke validation: PASS
  - WGS84 stable recognition baseline: PASS
  - KML export, Entitlement, quota consumption, and Usage path: PASS
  - Production `/api/version`: `v1.0.2 — WGS84 and KML Closure`
  - Production quota smoke: `3 → 2`
  - Production RC Origin rejection: `403 invalid_origin`
  - Homepage, three tool routes, compliance Footer, Nginx, service status, and production logs: PASS
- Principles:
  - A formal Release must simultaneously provide content consistency, traceable provenance, a frozen Artifact, and a recoverable Production state.
  - Production never accepts an uncommitted Working Tree as a release source.
  - A completed RC validation does not replace Production smoke verification, and Production smoke verification does not replace RC approval.
- Consequences:
  - All subsequent Feature and Patch work starts from a clean worktree based on `39abd7ac4ad937789c1fe04b5439721e3fcdb8ce`.
  - The previous mixed experimental workspace remains a research pool and must not be used as a Release source.
  - The Lightweight RC flow is now a proven, reusable GeoKit Lab release procedure.
  - v1.0.2 Production is frozen except for a separately reviewed release or an authorized severe-production incident response.
- Status: Active / Production Release Complete
- Date: 2026-08-24
- Supersedes: None

## Decision Entry Template

### DL-XXX — Decision title

- Category:
- Decision:
- Context:
- Consequences:
- Status: Proposed / Active / Superseded
- Date:
- Supersedes:

## Review Cycle

Update immediately after every major decision. Do not wait for a scheduled review.

## Version History

| Version | Date | Summary |
| --- | --- | --- |
| v1.3 | 2026-08-24 | v1.0.2 production release closure and first completed Lightweight RC execution |
| v1.2 | 2026-08-23 | Lightweight Release Candidate environment strategy decision |
| v1.1 | 2026-08-23 | Compliance closure and Alpha Closing resume decision |
| v1.0 | 2026-08-14 | Day 0 |
