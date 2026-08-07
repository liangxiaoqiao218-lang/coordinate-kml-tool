# UTM Migration Infrastructure Specification

## 1. Phase Boundary

Phase 5-1 adds isolated migration infrastructure while the project decision
remains `HOLD_LEGACY`. None of these modules is imported by production routing.
This phase does not change `server.js`, `index.html`, Export, the legacy UTM30
parser, `precisionMode`, locked baselines, or the authoritative result.

## 2. Canonical versus Legacy Export Compare

`server/utm-intent/export-compare.js` provides a shadow-only canonical KML
builder and semantic comparator.

The canonical builder consumes only an existing Typed UTM Result and its
accepted `transformedWgs84` points. It does not parse OCR, infer CRS, or
retransform projected points during Export.

The comparator accepts a legacy KML document and compares it with canonical KML
for:

- document and Placemark names;
- Placemark count;
- geometry type;
- point count and sequence;
- longitude, latitude, and altitude within `1e-8` through `1e-6` tolerance.

Possible results are `MATCH` and `EXPORT_COMPARE_FAILED` with a stable reason.
A mismatch cannot authorize canonical output.

The Phase 5-1 regression uses a frozen UTM30 legacy KML fixture. Production KML
capture and cohort comparison are not connected in this phase.

## 3. Migration Kill Switch

`server/utm-intent/migration-control.js` defines three modes:

```text
legacy
shadow
controlled
```

The default is always `legacy`.

- `legacy`: legacy is authoritative and canonical evaluation is not required.
- `shadow`: legacy remains authoritative while canonical results may be
  observed and compared.
- `controlled`: canonical may become authoritative only when the Migration Gate
  returns `V2_ALLOWED` and Export Compare returns `MATCH`.

Entering `controlled` requires an explicit
`READY_FOR_CONTROLLED_MIGRATION` approval. `HOLD_LEGACY` cannot enable it.
Transitions require an audit reason. Direct `legacy -> controlled` transition is
forbidden, and a controller cannot initialize directly in `controlled` mode.

This is an isolated controller contract, not a production feature flag. The
production path remains unchanged until a separately approved migration phase.

## 4. Rollback Drill

The regression performs a local in-memory drill:

```text
legacy -> shadow -> controlled -> legacy
```

It verifies:

- default legacy authority;
- shadow observation without authority change;
- rejection of controlled entry under `HOLD_LEGACY`;
- canonical authority only with Gate and Export `MATCH`;
- blocking on Export mismatch;
- `controlled -> legacy` restoration with an audit record.

The drill proves the isolated controller transition. It does not prove a
production rollback because no production canonical route or kill-switch
integration exists yet.

## 5. Shadow Observation Report

Phase 5-1 reuses `createUtmMigrationObservationReport()` from the Phase 3
observer. The infrastructure regression creates a report containing:

- `MATCH`;
- `V2_ONLY`;
- `LEGACY_ONLY`;
- `CRS_CONFLICT`;
- `TRANSFORMATION_MISMATCH`.

The report remains `shadowOnly: true`. Deliberate negative fixtures validate
classification and must not be counted as real production incidents.

## 6. Readiness Status

Phase 5-1 infrastructure improves migration testability but does not satisfy all
Phase 5-0 readiness conditions. The project remains:

```text
HOLD_LEGACY
```

Remaining prerequisites include production Legacy/Canonical Export observation,
a completed real Shadow window, an integrated and exercised kill switch, and a
rollback drill against the actual canonical-authority route.
