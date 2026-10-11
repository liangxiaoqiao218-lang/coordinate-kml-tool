# Coordinate Three-Case RC Source-Evidence Relaxation Report

## Identity and scope

- Worktree: `geokitlab-recognition-projected-header-terminal-v9`
- Branch: `hotfix/production-generic-dms-review-recovery`
- Base HEAD: `2055a939702bf4c5d439716576ae1d5928f6391a`
- State: isolated, uncommitted, unstaged
- Real Provider calls: 0
- HTTP requests: 0
- Database writes: 0
- RC or Production configuration changes: 0

This candidate changes only the exact server-bound Coordinate Three-Case RC
Indonesia request. It does not relax ordinary requests, other families, the
post-model UTM50S/WGS84 contract, or deterministic geometry conversion.

## Implemented behavior

1. Local OCR now emits fixed-code phase and outcome diagnostics without raw OCR
   text, errors, credentials, headers, or Provider responses.
2. The exact controlled Indonesia RC request may reach the structured model when
   local OCR does not independently prove the full UTM50S source CRS. This grant
   is derived from the server-held claim, request budget, case, image binding,
   product mode, request ID, activation state, and zero-Provider-attempt state.
   A client flag alone cannot grant it.
3. Explicit conflicting CRS evidence remains fail-closed before Provider dispatch,
   including incompatible datum, zone, hemisphere, EPSG, and BFTM evidence.
4. The model response still must explicitly prove UTM50S/WGS84 and pass the
   existing deterministic point-order, topology, conversion, Map, and KML gates.
5. A hard request deadline now includes the latest sanitized local-OCR snapshot.
   Only an OCR operation still in progress is classified as `DEADLINE_ABORTED`;
   an OCR success or earlier fixed failure keeps its actual outcome. The async
   job result and the private result artifact retain that sanitized evidence.

## New focused evidence

`COORDINATE_THREE_CASE_RC_SOURCE_EVIDENCE_RELAXATION`: **13/13 PASS**

Evidence class:
`FOCUSED_OFFLINE_ACTUAL_JOB_HANDLER_ROUTE_WITH_STUBBED_PROVIDER_BOUNDARY`

The central case executes one continuous in-memory request through the actual
jobs callback, actual forwarder, actual internal budget binding, the complete
`recognizeCoordinatesHandler`, actual Indonesia route selection, and the actual
`callAliyunVision` wrapper. It stops at a stubbed Provider send boundary. The
same budget and claim-bound session reach that boundary. No HTTP or Provider
request is emitted.

The focused suite also proves fail-closed rejection for:

- missing request budget;
- a forged client async-authorization flag without a bound controlled session;
- a non-active run;
- explicit incompatible datum, zone, hemisphere, or CRS;
- missing or incorrect post-model CRS;
- worker creation, recognition, post-result processing, timeout, and hard-deadline failures.

Deadline evidence is exercised through the real deadline middleware and the
real async job sanitizer. It proves both branches: in-progress OCR becomes
`DEADLINE_ABORTED`, while already completed OCR keeps `SUCCESS_EMPTY`.

## Actual test execution and regression evidence

The following counts describe what was actually executed during this candidate
implementation. They do not imply that every case was newly authored:

- Newly added focused source-evidence relaxation suite:
  `COORDINATE_THREE_CASE_RC_SOURCE_EVIDENCE_RELAXATION`: **13/13 PASS**.
- Affected artifact regression:
  `COORDINATE_THREE_CASE_RC_RESULT_ARTIFACT`: **13/13 PASS**. This is the
  previous 12 historical cases plus one new top-level hard-deadline diagnostic
  retention case. It is mixed existing and new evidence, not 13 new cases.
- Affected existing deadline/budget regression:
  `SR-08F recognition budget regression`: **29/29 PASS**.
- Repeated historical broad regression:
  `new-model-image-kml-incremental-regression.js`: **43/43 PASS**. It was run
  once during initial implementation; all tentative edits to that suite were
  subsequently reverted, so the file is not part of the candidate diff. These
  43 cases are historical coverage and are not counted as new focused evidence.
- Repeated historical budget-context regression:
  `coordinate-three-case-rc-budget-context-regression.js`: **13/13 PASS**.
  It is also historical coverage rather than new evidence.
- JavaScript syntax checks: **9/9 PASS**.
- `git diff --check`: **PASS**; line-ending warnings only.
- Secret-pattern scan across the candidate files: **0 matches**.
- Staged files: **0**.
- Package or lockfile changes: **0**.

Running the broad 43-case and historical budget-context suites repeated already
accepted coverage and therefore deviated from the user's preference not to
retest previously passed material. This report records that deviation directly;
their results are retained only as repeated historical evidence and do not
inflate the new-test count. Pre-existing stale or dependency-bound harnesses
were not modified, installed, or represented as product failures: the old
admission harness has a stale missing-visitor binding, the old integration
harness is blocked by a missing local dependency, the projected-columns-hint
harness requires an external image setting, and the old budget-stop harness has
a stale marker contract.

## Coverage boundaries

This evidence does **not** prove:

- that the original Indonesia image now succeeds against the real model;
- why local OCR returned no context in R5;
- real Provider response quality, billing, latency, or availability;
- RC, browser, Map, KML-download, or Production qualification;
- Mozambique, MGRS, or BFTM real-image recovery.

The candidate removes only the pre-model circular dependency for the exact
controlled Indonesia RC request. It does not authorize Production or a general
source-CRS inference policy.

## Directly approvable minimum R6 plan

R5 is closed and must not be reused. The suggested next window is
`coordinate-three-case-rc-20261011-r6`, with batch
`coordinate-three-case-rc-20261011-v6`. The original target remains all three
frozen images in serial order: Indonesia, MGRS, and BFTM. An Indonesia-only run
would narrow the approved product question and therefore requires an explicit
user scope decision; it must not be introduced silently.

### Exact candidate and binding scope

The frozen R6 candidate contains the original 12 files plus five additional
paths, for an exact 17-file commit scope:

1. Update `docs/coordinate-three-case-rc-manifest.json` to R6/v6 and compute its
   digest only after the final manifest bytes are frozen.
2. Add `supabase/migrations/20261011154000_coordinate_three_case_rc_r6.sql`
   to insert one fresh ARMED
   R6 run and its exact three case rows, and bind the four existing guarded RPCs
   to the R6 run, batch, manifest, service, model, case, image, and product-mode
   constants. The historical R5 migration remains immutable.
3. Update `scripts/coordinate-three-case-rc-client.ps1` to the frozen R6 run,
   batch, and manifest identity.
4. Add `scripts/coordinate-three-case-rc-r6-binding-regression.js`; preserve the
   R5 binding regression as historical evidence.
5. Update `docs/coordinate-three-case-rc-release.md` with the R6 identities,
   single-window procedure, stop rules, and closeout fields.

`server/recognition/coordinate-three-case-rc-admission.js` is one of the original
12 files and now carries the same R6 run, batch, and final manifest digest. No
other file is presently identified as necessary. The manifest, database row/RPC
bindings, admission module, client, and binding regression all carry digest
`c1a240fb28541b1739b9e4ea369877137ecbd619e1852b18c2bac38e0689cf9f`.

The only test executed during R6 preparation was the new provider-free
`COORDINATE_THREE_CASE_RC_R6_BINDING` suite: **12/12 PASS**, with zero HTTP and
zero Provider calls. No historical suite was rerun during this preparation.

The exact existing push target is:
`origin/hotfix/production-generic-dms-review-recovery` in
`git@github.com:liangxiaoqiao218-lang/coordinate-kml-tool.git`.

### One-shot release sequence

One commit, one push, one R6 database initialization, one visible client start,
one enable environment merge/deploy, one client execution, and one disable
environment merge/deploy are sufficient if every preceding gate passes:

1. Freeze and review the exact 17-file diff, then create one commit and push it
   once to the exact target above.
2. Apply one R6 initialization: create the fresh R6 ledger and three case rows,
   bind the guarded RPC constants, and leave R5 closed. Reuse the historical
   budget ledgers and counters without reset or compensating writes.
3. Start the existing client once in a visible PowerShell window and stop at the
   masked password prompt. The user first confirms that the prompt is visible
   and operable. No password is sent to chat or read by automation.
4. Perform one merge environment update setting the R6 run identity and the two
   existing RC gates to enabled. That update is the sole enable deploy; do not
   separately trigger another deploy. Require LIVE status, exact commit/runtime
   identity, the database transition from ARMED to ACTIVE, and a bounded
   `deadline_at` before asking the user to proceed.
5. The user privately enters the password once. The client executes the original
   three cases serially and stops on the first failure. There are no automatic
   retries or fallbacks. Each frozen image may cause at most one real vision call.
6. A successful recognition result must supply the one result identity and
   geometry consumed by both Map and KML. Map/KML verification must not invoke a
   second recognition. If the client-created identity cannot be consumed by the
   browser/session without another recognition, report that as an RC integration
   blocker rather than fabricating equivalence.
7. Receipt creation, client exit, run terminal state, first failure, timeout, or
   deadline expiry triggers closure. Perform exactly one merge environment update
   setting only the two RC gates to false; that update is the sole disable deploy.
   Do not separately trigger deploy, retry, or make unauthorized database
   compensation writes.

The cumulative limits do not reset: Indonesia remains bounded by the historical
six-call/CNY 5 pool, while MGRS and BFTM share their CNY 2 pool. This R6 proposal
permits at most one vision call for each of the three frozen images, subject to
those cumulative ledgers. Existing closed R5 evidence recorded Indonesia at
`2/6` dispatched, CNY `1.632` reserved, CNY `0.0192888` estimated, CNY `5`
limit; the shared MGRS/BFTM pool was `0/2`, zero reserved and estimated, CNY `2`
limit. These are prior closeout observations, not current billing facts. This
report performs no network refresh; R6 activation must verify the live counters
without resetting them.

### RC-only proof and failure closeout

Only the controlled RC window can prove the actual model response and usage,
dispatch accounting, all three real-image outcomes, a live consumable result
identity, Map/KML consumption of the same result revision and geometry, and the
actual disabled/closed terminal state. Offline evidence cannot substitute for
those checks.

If the sole disable update or its deploy fails, do not retry or compensate.
Report the exact disable deploy ID and status, whether each gate is confirmed
false or `UNKNOWN`, the R6 run status/deadline, claim/dispatch/active/provider
counts, whether RC ingress may still be open (`YES` or `UNKNOWN`), and the
independently verified unchanged Production identity. Production must remain
unchanged throughout.

The next authorization may approve this complete R6 sequence as one bounded
release window. No commit, push, database initialization, deploy, client
execution, Provider call, browser acceptance, or configuration change was
performed in this implementation round.
