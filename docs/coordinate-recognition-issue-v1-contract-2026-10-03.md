# coordinate_recognition_issue_v1 contract

## Purpose

`coordinate_recognition_issue_v1` adds a minimal, request-bound problem record to the existing coordinate case library. It is an operations feedback record, not a training sample and not evidence that a solution has been learned.

## Eligible events

Exactly three event classes are eligible:

- `FAILED_CLOSED`, only when the terminal code is `COORDINATE_RECOGNITION_FAILED_CLOSED`;
- `REVIEW_REQUIRED`, when the terminal result or finalizer explicitly retains review authority;
- a request-bound `MANUAL_ASSISTANCE_REQUESTED` event.

Normal successful uploads are ignored. `recognition_request_id` is the unique deduplication key. `job_id` is an optional link to the asynchronous job and is not used as the unique identity.

## Stored fields and privacy boundary

The record stores only source, pipeline stage, a redacted machine error code, coarse coordinate type or `UNKNOWN`, workflow status, runtime commit, event time, request id and optional job id. It must not contain the image, complete coordinates, OCR or Provider response text, credentials, authorization material, environment configuration, or other user content. If the visual cause is not independently evidenced, it remains `UNKNOWN`.

The original artifact status defaults to `NOT_SAVED`. Creating a problem record does not grant retention, training, recognition, Map, KML, billing, or production authority.

## Resolution and rollout evidence

`RESOLVED` requires a fix commit, verification receipt, independent reference, positive sample count, evidence scope, and PASS evidence for problem resolution and peer validation. Same-class coverage (`peer_validation_status`) and production activation (`production_status`) remain separate. A fix may therefore be resolved offline while production activation is still `UNKNOWN`.

## Runtime behavior

Issue recording is best-effort and cannot rewrite a recognition response, consume quota, call a Provider, call a map service, or promote a review result. The supplied migration only defines the offline database change; it is not applied by this implementation phase.
