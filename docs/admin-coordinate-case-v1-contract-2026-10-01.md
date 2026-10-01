# Admin coordinate case library v1 contract

This first version keeps coordinate-recognition cases separate from `judge_cases` while reusing the existing administrator boundary and result identity.

## Stored data

- Request and job references; finalized `resultId`, revision, and geometry hash only when all three are present.
- Issue type and redacted summary, resolution, owner, progress, blocker, four independent delivery states, evidence scope, sample count, Golden/commit/receipt references, and timestamps.
- CRS evidence remains `UNKNOWN`, `WORKING_ASSUMPTION`, or `CONFIRMED`. A working assumption is never promoted by the case library.
- Original artifacts default to `NOT_SAVED`. The schema does not contain image bytes, full coordinates, Provider responses, credentials, headers, cookies, or environment snapshots.

## Authority boundaries

- The sealed recognition result remains the single coordinate authority. The case library stores references only and validates a complete identity against `private.coordinate_recognition_commits`.
- Browser roles have no direct table privileges. Access is through the existing Node `requireAdmin` route and server-side `service_role` client.
- `judge_cases` remains unchanged and continues to serve historical mining-judgement cases.
- `UNKNOWN`, missing evidence scope, zero samples, and unrun evidence never count as PASS coverage.
- PASS coverage also requires a verifiable, stage-owned reference. Delivery may use a receipt, problem resolution a commit, peer validation a Golden record, and production requires explicit `PRODUCTION_*` evidence. Evidence for one stage never proves another stage.
- Allowed text fields reject obvious coordinate tables, complete Provider JSON, data URLs, authorization or cookie material, credential assignments, token-shaped secrets, and environment snapshots without logging the rejected content.

## Model status

The admin endpoint returns only allowlisted model names. Configured model and last observed Provider response model are separate values; an absent observation is explicitly `UNKNOWN`. No key, endpoint configuration, or environment dump is returned.

The migration is repository-only in this change. It must not be applied to production without a separate authorization and rollback review.
