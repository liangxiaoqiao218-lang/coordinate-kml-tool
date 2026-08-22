# Coordinate Engine V3 Complex Image Acquisition Experiments

## Phase 10B: Table Context Composite Experiment

Phase 10B is an experimental acquisition path for complex images where the full image is too visually dense for a single provider call, but the coordinate table is visibly present.

This phase changes only the image representation sent to the existing primary acquisition provider. It does not change the provider model, prompt, timeout, candidate schema, candidate shaping semantics, V3 Runner, recognizer ownership, CRS conversion, verification, KML behavior, or production runtime wiring.

## Contract

- Mode: `table_context_composite_experiment`
- Provider model: unchanged
- Provider prompt: unchanged
- Provider timeout: unchanged
- Provider calls per image: `1`
- Retries: `0`
- Targeted acquisition: `0`
- OCR fallback: `0`
- Runtime integration: none
- Primary default acquisition: unchanged

The preprocessing step is not authoritative. It may crop and compose visual regions, but it must not decide coordinate type, CRS, owner, winner, correctness, or KML readiness.

## Preprocessing Rules

The experimental preprocessor may:

- detect a visually strong table-like region using image structure;
- preserve the surrounding context needed by the unchanged provider prompt;
- produce one composite image for the unchanged primary provider call;
- return sanitized metadata about the selected region.

The experimental preprocessor must not:

- use filename, fixture name, country, CRS text, coordinate type, or recognizer-specific text to select a crop;
- use a fixed crop for a known image;
- call a provider;
- call OCR;
- retry;
- target individual rows;
- expose base64/image payload in diagnostics.

If no strong table region is detected, the experiment must return `PREPROCESSING_NO_STRONG_TABLE_REGION` with `providerCalls=0`. It must not silently fall back to full-image acquisition.

## Success Criteria

For the Indonesia #003 complex table fixture, the experiment is successful only if:

- preprocessing creates `COMPOSITE_CREATED`;
- exactly one provider call is made;
- the frozen V3 Runner selects `indonesia_utm`;
- the normalized result contains 16 points;
- ground truth is `PASS`;
- no deterministic recognizer or ownership rule is modified.

If the composite still times out or produces incomplete rows, that is acquisition evidence for the next phase. It is not a reason to unfreeze deterministic recognizers.

## Relationship to Phase 9C

Phase 9C validated the default full-image primary path for Côte d’Ivoire and Indonesia #001/#002, while Indonesia #003 remained a separate provider/acquisition latency issue.

Phase 10B does not invalidate Phase 9C. It tests one isolated variable for Indonesia #003:

```text
full image
↓
table-context composite image
```

Everything after the image representation remains the same.

## Phase 10C: Model-ID-only A/B Benchmark

Phase 10C tests whether a repository-backed challenger model can complete the same Indonesia #003 table-context composite workload under the existing architecture.

The experiment changes only the model id:

```text
Baseline: qwen-vl-plus
Challenger: qwen-vl-ocr-latest
```

The baseline is not rerun. The known baseline evidence is:

```text
qwen-vl-plus
table_context_composite
40000ms
PROVIDER_TIMEOUT
```

The challenger benchmark must keep all other variables frozen:

- fixture: Indonesia #003;
- input representation: `table_context_composite`;
- prompt: unchanged;
- timeout: `40000ms`;
- provider calls: `1`;
- retry: `0`;
- targeted acquisition: `0`;
- OCR fallback: `0`;
- candidate shaping: unchanged;
- adapter and Runner: unchanged;
- recognizers: unchanged;
- production default model: unchanged.

The challenger is successful only if it returns through the unchanged provider output contract and the frozen V3 Runner produces:

```text
owner=indonesia_utm
rows=16
groundTruth=PASS
technicalKmlReady=true
totalMs<=60000
```

If the challenger returns fast but violates JSON, schema, candidate construction, ownership, or ground-truth requirements, the result is a failed challenger. No model-specific parser or prompt adjustment is allowed in this phase.

Benchmark output must be saved to:

```text
artifacts/phase-10c-model-ab-matrix.txt
```

The artifact must remain sanitized and must not contain API keys, authorization headers, raw provider responses, raw prompt text, or base64 image data.
