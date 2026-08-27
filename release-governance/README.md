# SR-08D.5 Golden Governance

This directory contains human-approved release-policy overlays and runner semantics. It does not replace coordinate truth or fixture files.

- Coordinate truth remains in `COORDINATE_RECOGNITION_GOLDEN_BASELINE.json` and the existing regression manifests.
- `sr08d5-golden-policy.json` overlays release policy, maturity, and evidence metadata only.
- Provider variance never updates confirmed truth automatically.
- The release runner reports truth, policy, gate safety, metadata, and Provider variance independently.
- Timeout reliability is independent from truth and policy. A request-deadline response without a finalized coordinate result is `NOT_EVALUATED`, `FAIL_CLOSED_TIMEOUT`, and `RELIABILITY_TIMEOUT`.
- Only `CONFIRMED_TRUTH` plus truth-critical `BLOCKER` findings can produce a coordinate truth mismatch. `BASELINE_ONLY`, `PLACEHOLDER_POLICY`, `MISSING_TRUTH`, and legacy warnings cannot.
- `UNSAFE_GATE_FAILURE` requires runtime `AUTO_EXPORT` plus confirmed truth mismatch, Geometry/CRS blocker, or an approved review/confirmation policy violation.
- Missing finalizer evidence is reported as `FINALIZER_NOT_EVALUATED`; the runner does not substitute false or empty runtime values.
- Internal timeout stage is `UNKNOWN_AFTER_REQUEST_DEADLINE` unless production trace evidence supplies a stage. Post-deadline work remains `UNPROVEN`.
- A live run writes a secret-free evidence summary to `release-evidence/coordinate-regression-runner-latest.json`; `--dry-run` does not write it.
- Approval source: `SR-08D.4_HUMAN_APPROVAL`, approved on `2026-08-26`.

## Release Governance Hash

`RELEASE_GOVERNANCE_HASH` is SHA-256 over lines in the form
`relative-path:sha256(file-bytes)`, sorted by relative path and joined with LF.
Its scope is:

- `scripts/coordinate-regression-runner.js`
- every file under `release-governance/`, except transient generated output

This hash is independent from `PRODUCTION_SOURCE_HASH` and `FIXTURE_SET_HASH`.

## SR-08G.2 Family Availability Policy

`family-availability-policy-v1.json` records the approved family-scoped recognition availability contract.

- `kyrgyz_gk` and `madagascar_cadastral_grid` are `BLOCKED_BY_PROVIDER`.
- `handwritten_dms_experimental` is `TEMPORARILY_UNAVAILABLE`.
- Availability is independent from review: an unavailable recognition result is not represented as `REVIEW_REQUIRED`.
- Availability blockers outrank confirmation and prevent Provider calls, coordinate inference, `AUTO_EXPORT`, KML, and unrelated fallback.
- Family blocking does not classify the whole product as unsafe; unaffected families remain independently qualifiable.
