# Coordinate Engine V3 Acquisition Foundation

Phase 9B establishes the contract between image acquisition and the frozen V3
deterministic runner. It does not connect a real provider, OCR, Vision, image
preprocessing, Primary runtime, or frontend UI.

## Responsibility boundary

The acquisition layer answers only:

```text
What did the image extraction read?
```

It must not answer:

```text
Which coordinate recognizer owns this?
Can this generate KML?
Is this result confirmed?
Which candidate is the production winner?
```

Ownership authority remains with the V3 runner and each recognizer's
`canHandle()` contract.

## Candidate contract

An acquisition candidate is sanitized, non-authoritative input for the runner.

```js
{
  id,
  text,
  structuredRows,
  headers,
  documentCues,
  sourceType,
  provenance,
  confidence,
  timing,
  cropRegion
}
```

Allowed `sourceType` values are type-neutral:

- `whole_image`
- `table`
- `text_block`
- `coordinate_block`
- `targeted_region`

Allowed `provenance` values:

- `primary`
- `targeted`
- `local_fallback`

`confidence` is only extraction confidence. It is not recognizer priority,
coordinate ownership, KML permission, or correctness authority.

## Authority ban

Candidates reject authority fields:

- `recognizerId`
- `coordinateType`
- `winner`
- `owner`
- `confirmationStatus`
- `qualityGateStatus`
- `kmlReady`
- `kmlPermission`
- `shadowWinner`
- `arbitrationProposal`
- `migrationStatus`

Document cues such as `UTM`, `WGS 1984`, `ZONA 50S`, `Liste_Carrés`, `XV`,
`YV`, `Point`, `Nord`, or `Ouest` may be preserved as non-authoritative visible
text. They do not select a recognizer.

## Adapter boundary

The adapter:

1. Normalizes acquisition candidates.
2. Removes exact duplicate candidates.
3. Sends each candidate to the V3 runner.
4. Aggregates runner statuses.
5. Merges equivalent duplicate normalized outputs.
6. Surfaces conflicts without selecting a winner.

The adapter does not contain recognizer-specific routing, coordinate
interpretation, CRS inference, provider calls, KML authority, or UI state.

## Conflict behavior

If all candidates return `NO_MATCH`, the adapter returns:

```text
NO_RECOGNIZER_MATCH
```

If one logical normalized result is produced, the adapter returns:

```text
MATCHED_RESULT
```

If a candidate is ambiguous at the runner layer, the adapter returns:

```text
AMBIGUOUS_RECOGNIZER_MATCH
```

If multiple distinct normalized outputs are produced, including same-owner or
cross-type differences, the adapter returns:

```text
MULTIPLE_CANDIDATE_CONFLICT
```

It must not choose by first result, last result, or highest confidence.

## Targeted acquisition principles

Phase 9B defines the trigger contract only; it does not call a provider.

Targeted acquisition may be requested only when:

```text
no usable runner result
AND a specific incomplete structured candidate exists
AND provider budget remains
AND deadline budget is sufficient
```

It must not trigger on verification warnings, DMS reference mismatch, UTM
transform mismatch, suspected points, KML warning, or low confidence alone.

## Budget

Provider call limits are fixed:

```text
Primary Provider Calls: 1
Maximum Targeted Calls: 1
Maximum Total Provider Calls: 2
Target: 30000ms
Hard Deadline: 60000ms
```

The contract rejects a third provider call with:

```text
PROVIDER_CALL_LIMIT_EXCEEDED
```

## Primary acquisition

Phase 9C introduces primary acquisition as a provider-facing module that makes
at most one type-neutral full-image provider call and returns acquisition
candidates. It does not perform targeted acquisition, OCR fallback, provider
retry, recognizer-specific routing, coordinate conversion, or KML decisions.

## Security

Candidates and adapter metadata must not expose:

- API keys
- credentials
- raw prompts
- raw provider responses
- base64 image data
- local filesystem paths

Allowed metadata includes provider name, duration, candidate source type,
sanitized crop coordinates, confidence, candidate count, and runner result
counts.

## Deterministic freeze protection

Debugging rule:

```text
Image -> wrong candidate:
fix acquisition

Correct candidate -> runner NO_MATCH:
audit ownership contract

Recognizer owns correct candidate -> wrong coordinates:
audit the specific deterministic recognizer
```

OCR or Vision extraction failure must not be used as a reason to change frozen
recognizer math or runner dispatch semantics.
