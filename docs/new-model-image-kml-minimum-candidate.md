# New Model Image-to-KML Minimum Candidate

## Scope

This isolated candidate adapts the ordinary image route to `qwen3.8-flash` and the three retained OCR consumers to `qwen3.5-ocr`. It does not add PDF upload, reverse KML import, UI redesign, model-comparison administration, map-provider migration, Provider retry, or model fallback.

## Provider contracts

- Vision: `qwen3.8-flash`, OpenAI-compatible chat-completions protocol, `stream=false`, omitted thinking defaults to `false`. Existing high-resolution and JSON response parameters remain available to the Agentic one-shot request.
- OCR: `qwen3.5-ocr`, same protocol and endpoint, `stream=false`, no thinking, high-resolution, or response-schema parameters, and at most 8000 output tokens.
- Both managed models require an exact returned model name, one completed choice, consumable final content, and strictly numeric, internally consistent usage totals. Missing, null, boolean, or coerced usage values fail closed.
- Configured models outside these two names remain explicitly unmanaged by this candidate adapter; this change does not silently redefine their contract.

## Route compatibility

| Route | Candidate status | Change |
| --- | --- | --- |
| Ordinary image recognition | Adapted | Existing `qwen3.8-flash` default now receives the explicit request and response contract. |
| Mozambique late transcription | Adapted | Uses the OCR model through the shared adapter; existing deterministic parser and historical-row rescue remain unchanged and are not counted as model proof. |
| MGRS retry | Adapted | Uses OCR with the existing 1600-token request; parser unchanged. |
| BFTM row retry | Adapted | Uses OCR with an explicit 8000-token bound; parser unchanged. |
| Mining quick judgment | Compatibility preserved | Shares the vision helper; omitted thinking is normalized to false and its existing 360-token request remains unchanged. No mining workflow expansion. |
| Agentic image recognition | Compatibility preserved | Its existing 12000-token vision request, high-resolution flag, response schema, single call, zero retry, and zero fallback remain intact. |
| Edit-time organization | No new call added | This candidate adds no post-edit call. Existing Agentic finalization still reuses unchanged text with zero calls, while changed text can invoke its existing single model call and must receive separate runtime/budget authority. |

## Run04 diagnostic closure

The failed controlled run is not reopened. A sanitized diagnostic object now preserves, through the uncharged product response and async job result:

- whether local OCR context existed;
- whether local OCR was attempted;
- whether explicit UTM50S evidence existed;
- whether X/Y evidence existed;
- whether the controlled RC X/Y hint was authorized and applied;
- whether admission preflight was checked and passed;
- fixed preflight and route failure codes.

Provider accounting is derived from the request budget's actual dispatch counter: request-shape, missing-configuration, or budget failures before dispatch remain `providerCallCount=0 / NOT_STARTED`; a dispatched request is `providerCallCount=1` with its bounded completion state. Reserved budget is not relabeled as a Provider call.

The X/Y hint remains restricted to the controlled RC admission path. It does not replace the explicit UTM50S prerequisite. No country-specific crop, coordinate hard-coding, or historical result injection was added.

The historical Run04 receipt cannot identify which predicate failed, so the underlying failure remains `NOT_DETERMINED`. This change repairs the evidence path for a future separately authorized request; it does not retroactively convert Run04 into root-cause proof.

## Result consumers

The candidate does not create a second geometry authority. Existing Map and KML consumers continue to bind `resultId`, `resultRevision`, and `geometryHash` from the same finalized result. The incremental regression verifies these bindings statically; no browser, Provider, RC, or Production execution is claimed.

## Evidence classification

The new regression is offline only. Dynamic cases exercise pure request/response, route, failure-envelope, and async-job contracts with no network. Source assertions verify integration call shapes. Existing Run04 receipts remain historical runtime evidence and are not relabeled as a successful recognition.

## Remaining gates and release plan

The user and development commander subsequently authorized one exact commit, one fast-forward push, one isolated RC initialization, one enable deployment, one disable deployment and one bounded trusted-client execution. This document remains the pre-runtime design record; the actual counters and outcomes belong in the post-run receipt. Rollback is the previous RC artifact plus disabled candidate flags. Production remains unchanged until separately authorized.

Still unverified: current RC environment model overrides, real `qwen3.8-flash` and `qwen3.5-ocr` response compatibility, ordinary upload/async completion, the three legacy OCR routes with their representative fixtures, and a same-revision Map/KML browser download. These are runtime gates, not outcomes of the offline regression.

### Clean-production-base dependency audit

The eight changed files are **not proven directly applicable** to the current clean Production source reference at commit `45778224...`:

- `server.js`, `server/coordinate-usage-atomicity.js`, and `server/recognition/recognition-acquisition-job-runtime.js` differ materially between the Production and RC bases, so their current RC files cannot replace the Production versions.
- `server/recognition/indonesia-structured-product-bridge.js` is absent from the Production base. Although it is among the eight candidate files, its server integration has not been rebased onto Production.
- `server/recognition/qwen38-coordinate-structured-adapter.mjs` and `server/recognition/indonesia-structured-b-rc-admission.js` are RC dependencies imported by the candidate `server.js` but are absent from Production and are not among the eight changed files.
- The RC admission module is a single-run test control with ledger/RPC dependencies; it must not be silently promoted into an ordinary Production route.
- Shared `recognition-first-acquisition.js` and `recognition-deadline.js` are byte-identical between the checked Production and RC references and may be reused, but that does not resolve the missing B modules or server integration hunks.

```ini
EIGHT_FILE_DIRECT_APPLICABILITY=NOT_VERIFIED
CLEAN_PRODUCTION_BASE_HUNK_EXTRACTION=REQUIRED
FINAL_COMMIT_FILE_COUNT=NOT_DETERMINED
RC_SINGLE_RUN_ADMISSION_IN_PRODUCTION=NOT_AUTHORIZED
```

The authorized RC package is based on the current remote RC head rather than the older clean Production source reference. Its single new commit contains only the reviewed candidate and three-case admission changes; it does not fast-forward Production or claim direct Production applicability.

## Decision-ready minimum next stage

The smallest useful next stage is one new isolated RC qualification package. It is not a continuation or retry of Run04. The required operation counts are fixed at:

| Operation | Proposed count | Current authorization |
| --- | ---: | --- |
| Exact local commit on the current RC remote head | 1 | Authorized once |
| Push of the exact candidate commit | 1 | Authorized once |
| New isolated RC initialization | 1 | Authorized once |
| RC enable deployment | 1 | Authorized once |
| RC disable/close deployment | 1 | Authorized once |
| Controlled client execution | 1 | Authorized once |
| Automatic retry | 0 | Prohibited |

The commit must contain only the minimum clean-base extraction approved by the dependency audit. The eight current candidate files are its evidence source, not an already frozen deployable file set. The detached `de26300...` worktree must not be deployed wholesale.

### Samples and proposed model calls

| Path | Evidence/sample status | Proposed real call limit | Budget authority |
| --- | --- | ---: | --- |
| Ordinary image / Indonesia structured image | The Run04 original image identity is frozen as SHA-256 `41f2b211...f2`; the client-held original, not a repository substitute, is required. | 1 call to `qwen3.8-flash` | Counts against the existing Indonesia cumulative ceiling of 6 calls / CNY 5. The unused remainder is not authorization; this one call needs explicit approval. |
| MGRS ordinary product | `regression-samples/fixtures/缅甸坐标.jpg`; acceptance-ready, 7 points. | 1 call to primary `qwen3.8-flash` | Separate proposed cap: at most CNY 1 for this call; not covered by the Indonesia ceiling. |
| BFTM ordinary product | `regression-samples/fixtures/布基纳法索02.jpg`; acceptance-ready, 20 points. | 1 call to primary `qwen3.8-flash` | Separate proposed cap: at most CNY 1 for this call; not covered by the Indonesia ceiling. |
| Mozambique OCR | `regression-samples/fixtures/莫桑比克矿地.jpg` exists, but its catalog state is `UNSTABLE / NOT_ASSERTABLE` because historical output is not stable enough for a release gate. | 0 in the minimum package | First requires provider-free human truth stabilization. A later call limit of 1 must be separately authorized; it is not bundled here. |
| Mining quick judgment | No authorized representative real mining image is present in the coordinate acceptance catalog. Only shared-helper request-shape evidence is reusable. | 0 | Runtime inference remains unqualified; no unrelated mining sample or call is implied. |
| Agentic image recognition | Existing offline evidence proves single-call/zero-retry request shape, not real model behavior. | 0 | The minimum RC must prove the Agentic feature is disabled/not selected. If enabled, stop and request a separate one-call Agentic qualification rather than consuming another path's budget. |
| Agentic edit finalization | Existing evidence proves unchanged text reuses the recognition result with zero calls; changed text can invoke its existing single model call. | 0 | The minimum client run must not enter changed-text Agentic finalization. No hidden second call is authorized. |

The current ordinary upload always consumes its one allowed Provider attempt on the primary Vision request. After that call, `claimRequestRetry()` rejects downstream family calls because `providerAttemptCount >= 1`. Therefore the retained Mozambique, MGRS, and BFTM `qwen3.5-ocr` blocks are not reachable from the current ordinary product request after the primary call. Directly invoking those OCR blocks would prove only the Provider contract, not ordinary-user recovery, and is not proposed.

Therefore the corrected decision-ready minimum package proposes exactly three ordinary-product `qwen3.8-flash` calls: one Indonesia call under the existing cumulative ceiling, plus one MGRS and one BFTM call under a separately proposed total cap of CNY 2 (CNY 1 each). No `qwen3.5-ocr` real call is proposed. If either primary call demonstrates that an OCR fallback is required, the run stops; changing pre-routing or the one-shot attempt policy becomes one consolidated architecture difference rather than a hidden extra call.

```ini
ORDINARY_MGRS_TO_QWEN35_OCR_REACHABLE=false
ORDINARY_BFTM_TO_QWEN35_OCR_REACHABLE=false
ORDINARY_MOZAMBIQUE_TO_QWEN35_OCR_REACHABLE=false
QWEN35_OCR_REAL_PRODUCT_QUALIFICATION=NOT_PROVEN
NEW_NON_INDONESIA_CALL_CAP=2
NEW_NON_INDONESIA_COST_CAP_CNY=2
```

No UTM50S or X/Y policy adjustment is requested. The existing UTM50S prerequisite, controlled X/Y hint boundary, and post-model deterministic validation stay unchanged. A policy change should be requested only if the new diagnostic receipt proves that one of those rules—not implementation or evidence transport—is the blocker.

Formal Production deployment remains a later single approval after this RC package passes. That later approval must identify the exact production artifact, deployment count, ordinary-user smoke, KML download, rollback artifact, and stop conditions.

Passing this three-case RC package establishes only the bounded coordinate minimum candidate. It does not establish Mozambique recovery, Mining behavior, enabled Agentic recognition, changed-text Agentic finalization, all-site recognition recovery, or Production readiness. Mining has no authorized representative real sample; Agentic has only reusable offline single-call/zero-retry evidence and must remain disabled/not selected for this package. Any of those capabilities included in a later Production artifact remains an explicit unresolved gate rather than inheriting this RC result.

## Incremental execution receipt

No standalone runtime artifact was created because this was an offline no-Provider regression. The existing execution record is the Codex command transcript for this task plus this frozen delivery section:

```ini
COMMAND_RECORD_ID=NEW_MODEL_IMAGE_KML_INCREMENTAL_REGRESSION_20261011_R1
COMMAND=node scripts/new-model-image-kml-incremental-regression.js
WORKTREE=C:\Users\Mir-1\.codex\worktrees\indonesia-b-render-rc-compat-v1\geokitlab-recognition-projected-header-terminal-v9
EVIDENCE_CLASS=OFFLINE_DYNAMIC_AND_STATIC_NO_PROVIDER
DYNAMIC=19/19 PASS
STATIC=14/14 PASS
COMBINED=33/33 PASS
PROVIDER_CALLS=0
RERUN_FOR_DOCUMENTATION=false
```

Supporting checks in the same task command record: all changed JavaScript passed `node --check`; `git diff --check` passed with only the pre-existing LF-to-CRLF warning; the added-line/new-file secret-pattern scan passed.
