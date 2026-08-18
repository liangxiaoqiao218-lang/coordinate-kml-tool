# Coordinate Engine V3 Foundation

This branch starts from the Primary stable core and defines an isolated-recognizer
foundation. The foundation commit did not port advanced recognizers into
Primary runtime; current isolated recognizer port status is listed below.

## Runtime boundary

The V3 engine is disabled by default and is not imported by `server.js`.
Primary recognition behavior remains the active runtime.

## Architecture

```text
type detection
↓
isolated recognizer
↓
normalized result
↓
optional verification
↓
warning metadata
↓
KML
```

## Normalized result contract

The normalized result contains:

- `coordinateType`
- `recognizerId`
- `coordinates`
- `geometryType`
- `crs`
- `precisionMode`
- `warnings`
- `suspectedPoints`
- `technicalKmlReady`

The following are not recognition authority in V3:

- `confirmationStatus`
- `shadowWinner`
- `migrationStatus`
- `arbitrationProposal`
- `dryRun`

## Recognizer isolation

Each recognizer owns:

- `canHandle(input, context)`
- `recognize(input, context)`
- `normalize(result, context)`
- `verify(result, context)`

A recognizer must not call another recognizer's provider acquisition, mutate
another recognizer's state, or allow generic fallback to override a clear type
match.

## Isolated runner

The V3 runner is internal-only and remains disconnected from `server.js`,
`index.html`, public API routes, OCR, Vision, KML download, confirmation,
migration, and V2 arbitration.

Runner responsibilities:

1. Receive input and context.
2. Create or consume a V3 latency budget.
3. Read dispatchable recognizers from the registry.
4. Call `canHandle`.
5. Classify dispatch as `NO_MATCH`, `MATCHED`, or `AMBIGUOUS`.
6. For one match, call `recognize`, `normalize`, and optional `verify`.
7. Return the recognizer's normalized result and verification metadata.

Dispatch rules:

- `0` matching recognizers -> `NO_MATCH`.
- `1` matching recognizer -> `MATCHED`.
- `>1` matching recognizers -> `AMBIGUOUS`, with candidate recognizer IDs.
- The runner does not automatically choose a winner for ambiguous matches.
- `NOT_PORTED` recognizers are not dispatched.
- `IMPLEMENTED` and future `STABLE` recognizers may be dispatched inside V3.

Deadline behavior:

- If the hard deadline has already expired before dispatch, return
  `DEADLINE_EXCEEDED`.
- Do not call recognizers after an expired runner deadline.
- Do not start a global retry chain.

Error isolation:

- A recognizer `canHandle` error is sanitized and does not crash the runner.
- A selected recognizer error is returned as sanitized runner metadata.
- Raw provider payloads, prompts, images, filesystem paths, and credentials are
  not exposed.

## Latency contract

- Target: `30000ms`
- Hard deadline: `60000ms`
- Each recognizer owns its provider budget.
- On deadline pressure, return the best available constructible result with a
  warning instead of running a global retry chain.

## KML contract

KML export authority is limited to:

- technical geometry constructibility
- account / quota permission

Recognition uncertainty produces warning metadata only. It does not block KML.

## Initial recognizer registry

The first recognizer may move from `NOT_PORTED` to `IMPLEMENTED` only inside
the V3 registry. `IMPLEMENTED` still means not production-stable and not wired
to Primary runtime.

Current registry:

- `wgs84_decimal`: `IMPLEMENTED`
- `wgs84_table`: `IMPLEMENTED`
- `generic_dms`: `IMPLEMENTED`
- `mgrs`: `IMPLEMENTED`
- `kyrgyzstan_gauss_kruger`: `IMPLEMENTED`
- `madagascar_cadastral`: `IMPLEMENTED`
- `indonesia_utm`: `NOT_PORTED`
- `cote_divoire_dms`: `NOT_PORTED`

Only real fixture validation can move a recognizer to `STABLE`.

## V2 asset inventory

| Asset | Classification | Reason |
|---|---|---|
| Release Identity V2 | PORT | Proven useful for deployment traceability. |
| Warning-only KML export | PORT | Matches product rule that user controls export when geometry is constructible. |
| Indonesia structured UTM | PORT | Real fixtures #001/#002 passed; #003 needs isolated revalidation. |
| Côte d'Ivoire deterministic DMS | PORT | Correctly preserves west hemisphere when structured DMS source is present. |
| Madagascar cadastral behavior | PORT | Preserve legacy stable route, not V2 cross-type repairs. |
| Point-specific warning metadata | PORT | Useful user-facing risk localization. |
| Real fixtures and ground truth | PORT | Required validation asset. |
| Controlled migration runtime | DROP | Too much governance for current runtime goal. |
| Confirmation gate | DROP | KML export should be warning-only unless geometry is impossible. |
| Broad arbitration runtime dependency | DROP | Over-couples recognizers. |
| Multi-minute retry chains | DROP | Violates latency architecture. |
| Cross-type semantic passes | DROP | Caused Madagascar/Indonesia coupling. |
| V2 evidence/shadow diagnostics | REFERENCE_ONLY | Useful for design, not runtime authority. |
