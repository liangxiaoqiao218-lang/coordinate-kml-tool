# Coordinate Three-Case RC R4 Pre-Commit Evidence

## Scope

R4 is a new bounded RC window. It does not reopen R3 and does not reset either historical budget ledger. Its only runtime correction restores the request-owned recognition deadline context before the internal long-recognition route binds the RC budget, then verifies that the budget request identity matches the current recognition request.

The frozen cases, image SHA-256 values, model, one-call limits, zero-retry rule, zero-fallback rule, provider endpoint, 30-minute window and historical budget pools are unchanged.

## R3 evidence retained

R3 closed after the Indonesia claim with `COORDINATE_THREE_CASE_RC_BUDGET_REQUIRED`. It dispatched no Provider call and left MGRS and BFTM unused. The R3 artifacts and database state remain historical evidence and are not altered by R4.

## Context correction

The internal route now takes its budget only from the context returned by `activateRecognitionDeadlineContext(req)`. It does not fall back to an ambient `AsyncLocalStorage` budget. Admission also requires the bound budget request ID to match the preflight recognition request ID.

This preserves fail-closed behavior for:

- a missing request context even while another request is ambient;
- a mismatched request ID;
- a mismatched frozen image or case binding;
- a mismatched RC run configuration;
- any attempt to reach Provider dispatch without a valid bound budget.

## Incremental validation boundary

`scripts/coordinate-three-case-rc-budget-context-regression.js` dynamically exercises the production job runtime, the production internal forwarder, the production internal binding middleware, the real deadline context and the real RC admission implementation. Network transport, database RPC results and the final Provider send are explicit offline stubs; no HTTP or Provider call occurs. The suite separately identifies the client-envelope/source-order assertion rather than presenting it as a runtime browser or PowerShell execution.

The retained 11/11 result-artifact evidence, R3 12/12 binding evidence, and historical 6/6, 30/30 and 33/33 suites are reused and are not rerun.

## Runtime boundary

R4 may be initialized only after the new incremental regression and R4 binding regression pass. Initialization creates an `ARMED` run only. The visible client must wait at the masked password prompt before the single enable deployment. The user enters the password privately only after the enabled deploy is LIVE and the R4 run is ACTIVE.

Success, failure or timeout must close the run and be followed by the single disabled deployment. Production remains unchanged.
