# Coordinate Engine V3 Foundation

This branch starts from the Primary stable core and defines an isolated-recognizer
foundation. It intentionally does not port Indonesia, Madagascar, Côte d'Ivoire,
or any other advanced recognizer in this phase.

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
- `wgs84_table`: `NOT_PORTED`
- `generic_dms`: `NOT_PORTED`
- `mgrs`: `NOT_PORTED`
- `kyrgyzstan_gauss_kruger`: `NOT_PORTED`
- `madagascar_cadastral`: `NOT_PORTED`
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
