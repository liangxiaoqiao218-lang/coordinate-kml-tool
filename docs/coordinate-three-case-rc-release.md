# Coordinate three-case RC release candidate

## Scope

This release candidate adds the already-reviewed `qwen3.8-flash` image-recognition adapter and a one-window RC admission layer for exactly three frozen images: Indonesia UTM50S, MGRS and BFTM. It does not enable Production, MapTiler, PDF upload, reverse KML import, UI redesign, model-comparison administration or a new map provider.

The current window is `coordinate-three-case-rc-20261011-r3`. The closed R1 and R2 runs remain closed. R3 changes only the run/batch/database binding and the local white-listed result artifact needed to preserve the single authorized execution result without another model call.

The ordinary image path remains one-shot. Direct OCR calls and automatic model retries are both zero. The shared Provider boundary rejects every dispatch that is not bound to a current RC claim, the exact image digest, the exact case, the exact model and the active database run.

## Budget and admission invariants

- Indonesia uses the existing `indonesia-ab-20261010-v1` cumulative budget row as the single atomic six-call/CNY 5 authority. The closed Run04 single-run row is not reopened or updated; only the shared historical budget counters are reserved and settled when the new run dispatches. Counters are not reset.
- MGRS and BFTM continue to share the original `coordinate-three-case-rc-20261011-v1` CNY 2 ledger. R3 does not create or reset another budget row. Each dispatch reserves CNY 1; each case can be claimed and dispatched once.
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

The client also writes a separate private validated-result artifact outside the repository. It is atomically replaced and SHA-256 verified after each terminal case, so a completed result survives a later case failure or close failure. Only normalized finalized geometry, final/source CRS, current result identity/version, readiness/review state, fixed diagnostics and bounded Provider accounting are copied through an explicit whitelist. It excludes passwords, claim/job tokens, headers, request bodies, raw Provider output/text, source candidates and image payloads. The terminal receipt contains only the artifact verification boolean, case count and digest; it does not print the retained geometry.

The retained identity is evidence, not a durable server authorization. Recognition jobs and finalized identities remain process-local and time-bounded. A later browser Map/KML test must use the same live service/current identity before shutdown or be separately authorized; post-close offline validation must not be reported as a real browser download.

The previously accepted provider-free client preflight regression is `6/6 PASS`: prompted path write-back, three-path normalization, second-input failure stopping before the third, preflight-before-password ordering, returned `FAILED` fail-fast behavior, and close/evidence wiring. It performs zero HTTP and Provider calls. The existing 6/6, 30/30, 33/33 and result-artifact 11/11 evidence is reused rather than rerun for the R3 identity-only rebinding. The new R3 binding regression is `12/12 PASS` with zero HTTP and Provider calls.

## Remaining gates

- The three real Provider outcomes are not known until the separately authorized one-shot trusted client run.
- RC success would qualify only these frozen images and this exact release candidate; it would not establish whole-site recovery.
- Any different image, model, endpoint, retry, fallback, call count, budget, run window or Production action requires a consolidated authorization difference.
