# Coordinate three-case RC release candidate

## Scope

This release candidate adds the already-reviewed `qwen3.8-flash` image-recognition adapter and a one-window RC admission layer for exactly three frozen images: Indonesia UTM50S, MGRS and BFTM. It does not enable Production, MapTiler, PDF upload, reverse KML import, UI redesign, model-comparison administration or a new map provider.

The current window is `coordinate-three-case-rc-20261011-r2`. The closed R1 run remains closed. R2 changes only client-side local path propagation, all-input preflight ordering, fail-fast sequencing and the minimum database binding needed for a fresh run.

The ordinary image path remains one-shot. Direct OCR calls and automatic model retries are both zero. The shared Provider boundary rejects every dispatch that is not bound to a current RC claim, the exact image digest, the exact case, the exact model and the active database run.

## Budget and admission invariants

- Indonesia uses the existing `indonesia-ab-20261010-v1` cumulative budget row as the single atomic six-call/CNY 5 authority. The closed Run04 single-run row is not reopened or updated; only the shared historical budget counters are reserved and settled when the new run dispatches. Counters are not reset.
- MGRS and BFTM continue to share the original `coordinate-three-case-rc-20261011-v1` CNY 2 ledger. R2 does not create or reset another budget row. Each dispatch reserves CNY 1; each case can be claimed and dispatched once.
- The run allows at most three claims, three dispatches and one active dispatch at a time.
- Missing Provider usage remains `UNKNOWN`; it is never rewritten as zero. Reservations are not released after an unknown-cost response.
- Closing the run is irreversible and immediately blocks new claims or dispatches. Unused claims are settled pre-Provider and unused cases are closed; an already-dispatched case remains `UNKNOWN` with its conservative reservation until its authoritative settlement arrives.

## Core path coverage

| Boundary | Result |
| --- | --- |
| `qwen3.8-flash` request/response compatibility | Implemented in the shared Provider adapter |
| Indonesia source-context diagnostics | Preserved as booleans and fixed codes |
| Mozambique legacy parsing | Existing deterministic post-Provider parser retained; no dedicated replacement |
| MGRS legacy OCR retry | Not selected in the RC because the first shared vision call consumes the one-call budget |
| BFTM legacy OCR/vision retries | Not selected in the RC for the same one-call reason |
| Mining judgeability / Agentic calls | Fail closed on the RC service unless bound to one of the three claims |
| Edited-coordinate organization | No additional model call is authorized or added |
| Map and KML | Continue to consume the same finalized result identity and geometry |

## Evidence class

The admission regression is offline and uses an in-memory fake RPC ledger. Product regressions use mock Provider responses. Neither is evidence of a new real recognition result, RC runtime acceptance or Production readiness.

## Enable and rollback

The RC is enabled only when all frozen non-secret fields match, including service name, Supabase project, model, endpoint hostname, run ID and time limits. The database run must also activate successfully. Any mismatch stops service startup.

Rollback is two-part:

1. close the database run through the authenticated internal close route after all dispatches settle;
2. set `COORDINATE_THREE_CASE_RC_ENABLED=false` and `INDONESIA_STRUCTURED_B_PRODUCT_ENABLED=false`, then deploy the disabled RC configuration.

The Production service and Production database are outside this release candidate.

The trusted PowerShell client resolves and validates all three frozen image paths and hashes before requesting the administrator password through a masked prompt. It runs the three cases serially without resubmission and stops immediately after either a thrown failure or a returned failed terminal result. It writes only a sanitized receipt outside the repository and closes the run in `finally`. It does not print the password, claim token, job token, coordinate payload or Provider response.

The added provider-free client preflight regression is `6/6 PASS`: prompted path write-back, three-path normalization, second-input failure stopping before the third, preflight-before-password ordering, returned `FAILED` fail-fast behavior, and close/evidence wiring. It performs zero HTTP and Provider calls. Existing 30/30 and 33/33 suites were not rerun for R2.

## Remaining gates

- The three real Provider outcomes are not known until the separately authorized one-shot trusted client run.
- RC success would qualify only these frozen images and this exact release candidate; it would not establish whole-site recovery.
- Any different image, model, endpoint, retry, fallback, call count, budget, run window or Production action requires a consolidated authorization difference.
