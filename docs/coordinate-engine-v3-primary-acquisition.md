# Coordinate Engine V3 Primary Acquisition

Phase 9C adds the first provider-facing acquisition module for V3, but keeps it
outside Primary runtime and outside the frozen deterministic recognizers.

## Scope

The primary acquisition module performs only:

```text
image
↓
one type-neutral provider call
↓
sanitized acquisition candidates
```

It does not call the V3 runner, select a recognizer, infer CRS authority,
convert coordinates, decide KML readiness, perform targeted acquisition, retry,
or use local OCR fallback.

## Provider call contract

```text
Maximum provider calls per lifecycle: 1
Primary timeout: 40000ms
Hard lifecycle deadline: 60000ms
Retry: absent
OCR fallback: absent
Targeted acquisition: absent
```

If the provider times out or returns invalid data, the module returns a failed or
deadline-exceeded acquisition result. It does not try a different prompt, model,
provider, local OCR path, or second call.

## Prompt contract

The primary prompt is type-neutral. It asks the provider to preserve visible
text, coordinate-like text, table headers, rows, labels, CRS/datum/zone cues,
decimal precision, and hemisphere notation.

It explicitly forbids coordinate conversion, coordinate-system choice, KML
generation, missing-value inference, suspicious-value correction, and authority
fields such as recognizer IDs or winners.

## Candidate construction

The module creates:

- one `whole_image` candidate from raw text and visible document cues;
- one candidate for each provider-returned type-neutral block, such as `table`,
  `text`, `coordinate_block`, or `header_block`.

Candidate count is intentionally small. The module does not generate many image
variants or synthetic candidate permutations.

Structured block candidates preserve table structure atomically when a provider
block contains both headers and rows. Visible document cues, table headers, all
table columns, row order, and original numeric precision are kept together in
the candidate text and structured rows regardless of whether the provider labels
the block as `table` or `coordinate_block`. This is a generic document-structure
rule; it does not choose or prefer any coordinate type.

## Security

Provider credentials, raw prompts, raw provider responses, base64 image payloads,
and local filesystem paths are not returned in acquisition candidates or
adapter-visible metadata.

## Validation boundary

Real-image validation scripts may load fixture files locally, but fixture paths
stay in test output only. Paths and image bytes are not inserted into acquisition
results.

If local credentials are unavailable, real provider validation must be reported
as `BLOCKED_BY_CREDENTIAL`, not as pass.

## Failure observability

Phase 9C.1 adds diagnostic-only failure metadata to primary acquisition results.
The metadata is sanitized, non-authoritative, and does not change provider calls,
timeout, prompt behavior, candidate construction semantics, adapter behavior, or
deterministic recognizer behavior.

The diagnostics classify:

```text
providerStatus
providerErrorCode
providerHttpStatus
providerResponseReceived
providerContentPresent
jsonParseStatus
jsonParseReason
schemaValidationStatus
schemaValidationReason
candidateConstructionStatus
candidateConstructionReason
candidateCount
```

The real-image matrix may display sanitized candidate summaries and adapter
candidate result summaries when available. It must not print API keys,
authorization headers, raw prompts, raw provider responses, image/base64 payloads,
or local filesystem paths from acquisition results.
