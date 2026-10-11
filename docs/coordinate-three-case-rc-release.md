# Coordinate three-case RC R7 release candidate

## Scope and evidence class

R7 is an isolated release candidate for exactly three frozen images: Indonesia UTM50S, MGRS and BFTM. It keeps the reviewed `qwen3.8-flash` adapter, one Provider call per image, no retry and no fallback. It does not enable Production, change the Production recognition authority, add PDF upload, reverse KML import, MapTiler, UI redesign or model-comparison administration.

The run identity is `coordinate-three-case-rc-20261011-r7`; its batch identity is `coordinate-three-case-rc-20261011-v7`. Closed R1–R6 runs remain closed. Local and mock regressions prove only deterministic contracts. They are not evidence of Provider success, RC activation, browser Map rendering, a downloaded KML file or Production readiness.

## Budget and admission invariants

- Indonesia continues to use `indonesia-ab-20261010-v1` as the historical six-call/CNY 5 authority. R7 does not reset it and reserves CNY 0.816 for the single frozen Indonesia image.
- MGRS and BFTM continue to use the existing shared ledger `coordinate-three-case-rc-20261011-v1`. The migration extends that same pool from two to four dispatches and from CNY 2 to CNY 4; it does not create a replacement pool or erase earlier use.
- Each frozen image may be claimed and dispatched once. The run permits three claims, three dispatches and one active dispatch at a time.
- Missing usage is `UNKNOWN`, never zero. There is no automatic retry or alternate model fallback.
- The migration and runtime RPCs are bound to the exact R7 run, batch, manifest, model, project and image digests. RPC execution remains limited to `service_role`.

## Browser-first Indonesia consumer gate

The Indonesia Provider call is not allowed to start until the controlled browser session already exists. The session can be created only when all of the following are true:

- the request is same-origin from `https://coordinate-kml-tool-rc.onrender.com`;
- the visitor is exactly `rc-b-release-20261010-4f125a9` and already exists;
- existing permissions allow coordinate recognition and KML export;
- existing conversion quota is positive;
- the R7 database run is active and has a current deadline;
- the selected map provider is OpenFreeMap, not AMap.

Authentication uses an HttpOnly, Secure, SameSite=Strict cookie and a CSRF token. Passwords are accepted only in the admin request header and are cleared from the browser input. The trusted client checks the server-side browser-session readiness endpoint before claiming the Indonesia case. A missing or expired browser session stops before Provider dispatch.

After a successful Indonesia job, the server registers one short-lived, one-use handoff. The handoff is bound to the exact run, batch, case, visitor, image digest, product mode, recognition request, job capability and current finalized result identity. Job expiry, finalized-result freshness and run deadline all constrain it. Closing the run revokes all R7 sessions and handoffs.

Only an explicit browser whitelist crosses the handoff. Passwords, claim/job tokens, request bodies, source text, raw Provider output, candidates and headers do not cross it.

## Same-result Map and KML contract

The browser installs the received result through the existing recognition workspace path. R7-specific relaxation is permitted only when the current finalized result matches the exact handoff `resultId`, revision and geometry hash, has finite EPSG:4326 longitude/latitude geometry, and remains technically KML-capable. Ordinary users and non-R7 results retain their existing gates.

Indonesia must then pass both real consumer checks before MGRS or BFTM can run:

1. Map must return `READY` from OpenFreeMap for the same result ID, revision and geometry hash, with `authorityMutationCount=0`. Local SVG fallback is not accepted for R7.
2. The browser must trigger an actual KML download. The downloaded XML is parsed outside the browser and its geometry must exactly match the retained finalized geometry for the same identity. Inner-ring geometry fails closed because the current product KML writer cannot preserve it losslessly.

The R7 KML is explicitly labeled `REVIEW_REQUIRED / UNVERIFIED`. If recognition usage is already recorded, KML does not deduct again. If it is explicitly not recorded, KML may consume at most once. An unknown usage state blocks export to avoid accidental double deduction.

The trusted client blocks the next case until the Indonesia browser evidence and downloaded KML pass. A Map or KML failure closes the run and leaves MGRS/BFTM unused.

## Core path coverage

| Boundary | R7 candidate behavior |
| --- | --- |
| `qwen3.8-flash` request/response | Existing shared adapter; exact model binding |
| Indonesia source evidence | Existing reviewed evidence rules; no filename or historic-output injection |
| Mozambique parser | Existing deterministic post-Provider path unchanged |
| MGRS legacy retry | No retry; the one shared vision call is authoritative for the RC case |
| BFTM legacy retries | No retry or fallback |
| Mining judgeability and Agentic recognition | Not initialized or called by R7 browser mode |
| Pricing, analytics, ordinary quota bootstrap | Suppressed in R7 browser mode |
| Map provider | OpenFreeMap only; AMap fails before Provider dispatch |
| Edit-time organization | No extra model call |
| Production | Unchanged |

The spatial-map module has no eager map-provider network call: it reads local runtime configuration only when Map is opened, and its external tile/style access occurs only during the authorized real Map consumer check.

## Controlled execution order

The broader user authorization already covers the one-shot commit, push, initialization, enable deployment, controlled client/browser execution and disable deployment described below. This local implementation phase performs none of them, and does not create a second set of counts or require the user to approve the same scope again. The authorized operator must preserve this order:

1. complete offline validation and review, then freeze the exact candidate;
2. perform the already-authorized commit and push, and initialize the R7 ledger once while the RC flags remain disabled;
3. start the one trusted client visibly and stop at its masked password prompt; the user confirms that this exact prompt is visible and operable without entering the password;
4. enable the two RC flags in one deployment, wait for that exact deploy to become live, activate R7 and record its deadline;
5. open `?coordinate-three-case-rc=r7` in the controlled browser, authenticate once, and verify server-side browser readiness;
6. tell the user to enter the administrator password privately in the already-open terminal and press Enter once;
7. let the client verify browser readiness, then execute Indonesia once;
8. receive the one-use result in that same browser, perform the real OpenFreeMap check, and use the product KML button to download the file;
9. let the client parse the actual downloaded XML, verify its geometry type and exact geometry against the same finalized identity, and only then permit MGRS and BFTM;
10. close the run, then disable both RC flags through one merged configuration update with no separate deploy trigger.

The browser evidence and KML download paths must be new and empty. Before the password prompt, the client creates the exact fresh `r7-browser-download` directory and prints it as `R7_BROWSER_DOWNLOAD_DIRECTORY=<absolute path>`. Before browser authentication, the controlled browser's download directory must be set to that exact path; this is preparation for the same authorized browser window, not a second client run. The product button must create the actual `coordinates.kml` file there. The browser sends its sanitized consumer evidence back through the same-origin, CSRF-bound in-memory handoff; the trusted client reads that evidence through the admin header route and writes the JSON artifact through its strict whitelist writer. The evidence schema contains only the run/case binding digest, identity-match booleans, Map state/provider, authority mutation count, KML trigger/file metadata and no coordinates or raw identity values. The private validated-result artifact remains outside the repository and retains the exact finalized geometry for comparison without printing it.

## Rollback and remaining differences

Rollback is fail-closed: close the database run, revoke browser handoffs, and set `COORDINATE_THREE_CASE_RC_ENABLED=false` plus `INDONESIA_STRUCTURED_B_PRODUCT_ENABLED=false` in one merged RC configuration update. No Production environment or database is changed.

No repeat authorization is required for the already-approved one-shot operations. Before execution, the operator still must freeze the exact reviewed commit and evidence paths, verify the approved ledger snapshot and budget headroom, and stop on any identity or snapshot drift. Any action outside that approved scope remains a separate difference and must not be inferred from this document.

An R7 pass would qualify only the three frozen inputs and this exact candidate. It would not by itself establish whole-site recovery or authorize Production deployment.
