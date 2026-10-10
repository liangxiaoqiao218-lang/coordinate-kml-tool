# Coordinate three-case RC R2 pre-commit evidence

## Exact purpose

R2 corrects the trusted client's ordered-dictionary path write-back defect, requires all three local files and frozen SHA-256 values to pass before the password prompt, and stops the sequence after either an exception or a returned failed job. It does not change the model, prompt, retry policy, Production, Map/KML consumers or Run04.

## Changed boundary

- `scripts/coordinate-three-case-rc-client-preflight.ps1`: shared local preflight and terminal receipt gate.
- `scripts/coordinate-three-case-rc-client.ps1`: explicit returned-path use, preflight before credentials, failure receipt plus immediate stop, and durable sanitized evidence after close.
- `scripts/coordinate-three-case-rc-client-preflight-regression.ps1`: new provider-free dynamic and source-wiring checks.
- `server/recognition/coordinate-three-case-rc-admission.js`: R2 run, batch and manifest binding only.
- `docs/coordinate-three-case-rc-manifest.json`: R2 identity and explicit reuse of the original non-Indonesia budget row.
- `supabase/migrations/20261011023000_coordinate_three_case_rc_r2.sql`: new run/cases; no new budget pool; dispatch, settlement and status continue to lock the original Indonesia and MGRS/BFTM ledgers.

## New validation only

```text
COORDINATE_THREE_CASE_RC_CLIENT_PREFLIGHT: 6/6 PASS
PowerShell parse errors: 0
HTTP requests: 0
Provider calls: 0
```

The original Indonesia file was found at the authorized local path and matched its frozen SHA-256. MGRS and BFTM continue to use their previously frozen fixtures. No credential was read and no runtime request was made.

Existing 30/30, 33/33 and historical regressions were not rerun.

## Remaining runtime gate

This is pre-commit/offline evidence only. A real result requires the separately authorized R2 initialization, enable deploy and one trusted-client execution. Any outcome must be followed by the authorized run close and disabled deployment. Production remains out of scope.
