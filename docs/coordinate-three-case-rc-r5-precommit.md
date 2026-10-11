# Coordinate Three-Case RC R5 Pre-Commit Evidence

## Scope

R5 is a new bounded RC window. It does not reopen R4 and does not reset either historical budget ledger. Its only runtime correction binds one existing controlled test visitor across the admin claim, actual PowerShell client envelope, async jobs ingress, internal forwarder and recognition handler, while preserving existing user permission and usage checks.

The frozen images, model, one-call-per-case limits, zero-retry rule, zero-fallback rule, provider endpoint, 30-minute window and historical budget pools remain unchanged.

## R4 evidence retained

R4 closed after its first request failed before Provider dispatch because no visitor identity reached the recognition handler. The sanitized receipt, validated-result artifact and database state remain historical evidence. R4 made zero Provider calls and is not rerun.

## Visitor and failure-contract correction

The controlled client now requires an explicit visitor ID and binds the same value to the admin claim, `x-visitor-id` and multipart `visitorId`. Admission binds a SHA-256 digest of that visitor to the one-time claim token in process memory and rejects missing, incomplete, mismatched or wrong-claim visitor envelopes before queue or Provider dispatch. The raw visitor value is never added to persisted RC evidence.

The controlled RC recognition handler preserves the pre-existing product permission path: `ensureUser()` hydrates the same process-local default user object and `getEffectivePermissions()` remains authoritative. This does not persist a user or permission change. The administrator-authenticated claim and visitor binding are identity constraints only and never grant OCR permission. A separate minimum `user_id`-only Supabase existence check prevents the later legacy `getOrCreateSupabaseUser()` usage path from creating a new durable user during this run; existence is not treated as permission. Unknown durable users, disabled users, explicitly denied users, unbound visitors or globally disabled OCR all fail closed. Permission failures retain `success:false`, a fixed code and an allowlisted reason through the actual uncharged-response settlement boundary.

## Incremental validation boundary

`scripts/coordinate-three-case-rc-client-preflight-regression.ps1` dynamically executes the actual PowerShell request-envelope helpers. `scripts/coordinate-three-case-rc-budget-context-regression.js` extracts and dynamically executes the current inline Express jobs callback and current full `recognizeCoordinatesHandler` declaration in a no-listener VM, together with the production visitor ingress and preflight middleware, production job runtime, production internal forwarder, production internal binding middleware, exported production permission helper, read-only existing-user lookup, request-owned budget, uncharged permission-failure settlement and real RC admission implementation. The full handler uses the existing provider-free `fast_success` regression branch; the separate budget test reaches the RC pre-Provider dispatch boundary without calling a Provider.

Database RPC results, localhost internal transport and final Provider transport are explicit in-process stubs. The inline jobs callback and recognition handler are production source, not test reimplementations. No listener, HTTP request, Provider call, durable user write or permission change is performed. Missing visitor, mismatched visitor, unknown durable user, disabled visitor and denied OCR permission all stop before the Provider boundary. The sanitized receipt and retained result artifact persist only fixed allowlisted reasons; arbitrary free text is replaced with `UNLISTED_FIXED_REASON`.

## Runtime boundary

R5 may be initialized only after the incremental suites and R5 binding regression pass. Initialization creates an `ARMED` run only. The new visible client must stop at its masked password prompt before the single enable deployment. The user enters the password privately only after the enabled deploy is live and the R5 run is active.

Success, failure or timeout must close the run and be followed by the single disabled deployment. Production remains unchanged.
