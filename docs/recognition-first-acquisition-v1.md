# Recognition-first acquisition v1

## Objective

This phase makes visual evidence acquisition complete before any coordinate-family, CRS, geometry, Map, KML, or usage authority decision. It does not weaken those downstream gates.

## Request path

1. Validate the upload and usage eligibility.
2. Decode the source once and build an acquisition package:
   - one whole-page overview for page structure, group headings, table headers, and CRS context;
   - overlapping high-resolution detail tiles for small text and long tables;
   - at most six images in source reading order.
3. Submit the overview and all tiles in one Provider request. Overlap is duplicate visual evidence, not permission to duplicate rows.
4. Preserve the exact Provider text, coordinate-bearing candidate lines, and visible CRS strings as evidence-only data.
5. Run family normalization, point-label continuity, explicit CRS, geometry, usage atomicity, Map, and KML gates after acquisition.

The acquisition prompt is intentionally generic. Local OCR can help describe visible structure but cannot block or replace the Provider acquisition call.

## Async long-task API

- `POST /api/recognize-coordinates/jobs` accepts the same multipart image and identity fields and returns `202` plus a job URL.
- The create response also returns a high-entropy `jobAccessToken`. `GET /api/recognize-coordinates/jobs/:jobId` requires it in the `x-recognition-job-token` header and returns `QUEUED`, `RUNNING`, `SUCCEEDED`, or `FAILED`; the token is never placed in the URL.
- Jobs are processed sequentially in this first version, kept in memory for 30 minutes, and limited to eight retained jobs.
- The internal long-running recognition path has a 180-second hard ceiling and still permits only one Provider call.

The ordinary synchronous endpoint keeps its existing sub-60-second deadline. Large, long, or high-byte images are marked `asyncRecommended` by the acquisition planner; a later UI phase can select the job endpoint automatically.

## Safety boundary

Acquisition evidence has `authority: EVIDENCE_ONLY`. It cannot authorize conversion, geometry, Map, or KML. Missing or conflicting CRS, labels, rows, groups, or geometry continue to fail closed after acquisition. Usage is committed only through the existing atomic settlement path.

## Blind baseline

The offline baseline covers DMS, WGS84 decimal degrees, BFTM, UTM, GK, MGRS, a long table, and a four-group image. Every record pins a repository fixture hash, expected point count, continuity expectation, CRS evidence expectation, geometry expectation, and KML safety state.

No Provider or production endpoint is called by the regression. Therefore:

- image decoding and overview/tile generation can be scored offline;
- Provider transcription accuracy, real latency, and billed token cost remain unmeasured until a separately authorized blind production run;
- payload bytes and image count are measured as cost/latency proxies, not presented as Provider billing.

## Observed offline result

On the pinned eight-record blind baseline:

- acquisition rendering passed `8/8` records (`100%`); this measures decoding and overview/tile coverage, not Provider transcription accuracy;
- six records required detail tiles and three were classified as async long tasks;
- preprocessing produced 23 submitted images (`2.875` images per request on average);
- source payload was 6,333,998 bytes and rendered submission payload was 3,454,547 bytes (`0.545x` of source bytes);
- local preprocessing took about 2.72 seconds in total and 340 ms per fixture on the final run;
- Provider transcription accuracy, network/model latency, and billed cost are intentionally `UNKNOWN` because Provider and production calls remained zero.

## Remaining risks

- The in-memory job queue is not durable across service restarts and is suitable only for the first architecture phase.
- Multi-image Provider limits and billing must be verified against the selected production model before deployment.
- Overlapping tiles improve legibility but can cause duplicated model output; downstream full-consumption normalization must continue to reject ambiguous duplication.
- A generic acquisition prompt may increase output length. The async path provides additional time but does not relax the single-call limit.
- Current legacy post-acquisition parsers still contain specialized branches; removing those is outside this acquisition-only phase.
