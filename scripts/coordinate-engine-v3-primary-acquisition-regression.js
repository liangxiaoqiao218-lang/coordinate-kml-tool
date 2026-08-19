import assert from "node:assert/strict";

import {
  ACQUISITION_ADAPTER_STATUS,
  ACQUISITION_STATUS,
  PRIMARY_ACQUISITION_ERROR,
  PRIMARY_ACQUISITION_PROMPT,
  acquirePrimaryImage,
  createAcquisitionBudget,
  runAcquisitionCandidatesThroughRunner,
} from "../server/coordinate-engine-v3/index.js";
import {
  PRIMARY_CANDIDATE_CONSTRUCTION_STATUS,
  PRIMARY_JSON_PARSE_REASON,
  PRIMARY_JSON_PARSE_STATUS,
  PRIMARY_PROVIDER_STATUS,
  PRIMARY_SCHEMA_VALIDATION_STATUS,
} from "../server/coordinate-engine-v3/acquisition/primary.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

function providerWithText(text, recorder = { calls: 0 }) {
  return async ({ prompt, timeoutMs }) => {
    recorder.calls += 1;
    recorder.prompt = prompt;
    recorder.timeoutMs = timeoutMs;
    return Object.freeze({
      ok: true,
      provider: "mock",
      model: "mock-vl",
      text,
    });
  };
}

function providerFailure(errorCode, recorder = { calls: 0 }) {
  return async () => {
    recorder.calls += 1;
    return Object.freeze({
      ok: false,
      provider: "mock",
      model: "mock-vl",
      errorCode,
    });
  };
}

const tableJson = JSON.stringify({
  rawText: "Longitude | Latitude\n16.032000 | 3.763800\n16.034000 | 3.765800",
  blocks: [
    {
      type: "table",
      text: "Longitude | Latitude\n16.032000 | 3.763800\n16.034000 | 3.765800",
      headers: ["Longitude", "Latitude"],
      rows: [
        { label: "1", longitude: "16.032000", latitude: "3.763800" },
        { label: "2", longitude: "16.034000", latitude: "3.765800" },
      ],
      confidence: 0.97,
    },
  ],
  documentCues: ["WGS84", "Longitude", "Latitude"],
});

test("one provider call", async () => {
  const recorder = { calls: 0 };
  const result = await acquirePrimaryImage({ imageBase64: "stub", provider: providerWithText(tableJson, recorder) });
  assert.equal(recorder.calls, 1);
  assert.equal(result.providerCalls, 1);
});

test("no retry", async () => {
  const recorder = { calls: 0 };
  await acquirePrimaryImage({ imageBase64: "stub", provider: providerFailure(PRIMARY_ACQUISITION_ERROR.PROVIDER_REQUEST_FAILED, recorder) });
  assert.equal(recorder.calls, 1);
});

test("timeout no retry", async () => {
  const recorder = { calls: 0 };
  const result = await acquirePrimaryImage({ imageBase64: "stub", provider: providerFailure(PRIMARY_ACQUISITION_ERROR.PROVIDER_TIMEOUT, recorder) });
  assert.equal(recorder.calls, 1);
  assert.equal(result.status, ACQUISITION_STATUS.DEADLINE_EXCEEDED);
  assert.deepEqual(result.warnings, [PRIMARY_ACQUISITION_ERROR.PROVIDER_TIMEOUT]);
  assert.equal(result.diagnostics.providerStatus, PRIMARY_PROVIDER_STATUS.TIMEOUT);
  assert.equal(result.diagnostics.providerErrorCode, PRIMARY_ACQUISITION_ERROR.PROVIDER_TIMEOUT);
  assert.equal(result.diagnostics.candidateConstructionStatus, PRIMARY_CANDIDATE_CONSTRUCTION_STATUS.CANDIDATE_CONSTRUCTION_EMPTY);
});

test("invalid response no retry", async () => {
  const recorder = { calls: 0 };
  const result = await acquirePrimaryImage({ imageBase64: "stub", provider: providerWithText("not json", recorder) });
  assert.equal(recorder.calls, 1);
  assert.equal(result.status, ACQUISITION_STATUS.FAILED);
  assert.deepEqual(result.warnings, [PRIMARY_ACQUISITION_ERROR.PROVIDER_INVALID_RESPONSE]);
  assert.equal(result.diagnostics.providerStatus, PRIMARY_PROVIDER_STATUS.SUCCESS);
  assert.equal(result.diagnostics.providerContentPresent, true);
  assert.equal(result.diagnostics.jsonParseStatus, PRIMARY_JSON_PARSE_STATUS.JSON_PARSE_FAILED);
  assert.equal(result.diagnostics.jsonParseReason, PRIMARY_JSON_PARSE_REASON.NON_JSON_RESPONSE);
});

test("authority strip/reject", async () => {
  const result = await acquirePrimaryImage({
    imageBase64: "stub",
    provider: providerWithText(JSON.stringify({
      rawText: "12.319572, -11.178174",
      coordinateType: "wgs84_decimal",
      winner: "wgs84_decimal",
      blocks: [{ type: "text", text: "12.319572, -11.178174", recognizerId: "wgs84_decimal" }],
    })),
  });
  assert.equal(JSON.stringify(result).includes("coordinateType"), false);
  assert.equal(JSON.stringify(result).includes("recognizerId"), false);
  assert.equal(result.candidates.length, 2);
  assert.equal(result.diagnostics.schemaValidationStatus, PRIMARY_SCHEMA_VALIDATION_STATUS.SCHEMA_INVALID);
  assert.equal(result.diagnostics.schemaValidationReason.includes("AUTHORITY_FIELD_REJECTED"), true);
});

test("candidate construction", async () => {
  const result = await acquirePrimaryImage({ imageBase64: "stub", provider: providerWithText(tableJson) });
  assert.equal(result.status, ACQUISITION_STATUS.SUCCESS);
  assert.equal(result.candidates.length, 2);
  assert.equal(result.diagnostics.candidateConstructionStatus, PRIMARY_CANDIDATE_CONSTRUCTION_STATUS.CANDIDATE_CONSTRUCTION_SUCCESS);
  assert.equal(result.diagnostics.wholeImageCandidateCreated, true);
  assert.equal(result.diagnostics.structuredCandidateCount, 1);
  assert.equal(result.diagnostics.finalCandidateCount, 2);
});

test("whole-image candidate", async () => {
  const result = await acquirePrimaryImage({ imageBase64: "stub", provider: providerWithText(tableJson) });
  assert.equal(result.candidates[0].id, "primary_whole_image");
  assert.equal(result.candidates[0].sourceType, "whole_image");
});

test("table candidate", async () => {
  const result = await acquirePrimaryImage({ imageBase64: "stub", provider: providerWithText(tableJson) });
  assert.equal(result.candidates[1].sourceType, "table");
  assert.equal(result.candidates[1].structuredRows.length, 2);
});

test("header preservation", async () => {
  const result = await acquirePrimaryImage({ imageBase64: "stub", provider: providerWithText(tableJson) });
  assert.deepEqual(result.candidates[1].headers, ["Longitude", "Latitude"]);
});

test("row order", async () => {
  const result = await acquirePrimaryImage({ imageBase64: "stub", provider: providerWithText(tableJson) });
  assert.equal(result.candidates[1].structuredRows[0].label, "1");
  assert.equal(result.candidates[1].structuredRows[1].label, "2");
});

test("decimal precision", async () => {
  const result = await acquirePrimaryImage({ imageBase64: "stub", provider: providerWithText(tableJson) });
  assert.equal(result.candidates[1].structuredRows[0].longitude, "16.032000");
  assert.equal(result.candidates[1].structuredRows[0].latitude, "3.763800");
});

test("hemisphere preservation", async () => {
  const result = await acquirePrimaryImage({
    imageBase64: "stub",
    provider: providerWithText(JSON.stringify({
      rawText: "Point | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"",
      blocks: [{ type: "table", text: "Point | Latitude Nord | Longitude Ouest\nA | 10°52'15\" | 08°16'00\"", headers: ["Point", "Latitude Nord", "Longitude Ouest"], rows: [{ point: "A", latitude: "10°52'15\"", longitude: "08°16'00\"" }] }],
      documentCues: ["Nord", "Ouest"],
    })),
  });
  assert.deepEqual(result.candidates[1].headers, ["Point", "Latitude Nord", "Longitude Ouest"]);
  assert.equal(result.candidates[0].documentCues.includes("Ouest"), true);
});

test("document cues", async () => {
  const result = await acquirePrimaryImage({ imageBase64: "stub", provider: providerWithText(tableJson) });
  assert.deepEqual(result.candidates[0].documentCues, ["WGS84", "Longitude", "Latitude"]);
});

test("adapter integration", async () => {
  const acquisition = await acquirePrimaryImage({ imageBase64: "stub", provider: providerWithText(tableJson) });
  const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
  assert.equal(adapter.status, ACQUISITION_ADAPTER_STATUS.MATCHED_RESULT);
  assert.equal(adapter.recognizerId, "wgs84_table");
});

test("targeted call=0", async () => {
  const result = await acquirePrimaryImage({ imageBase64: "stub", provider: providerWithText(tableJson) });
  assert.equal(result.timing.targetedDurationMs, 0);
  assert.equal(JSON.stringify(result).includes("targeted_region"), false);
});

test("metrics", async () => {
  const acquisition = await acquirePrimaryImage({ imageBase64: "stub", provider: providerWithText(tableJson) });
  const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
  assert.equal(adapter.metrics.providerCalls, 1);
  assert.equal(adapter.metrics.candidateCount, 2);
});

test("security", async () => {
  const result = await acquirePrimaryImage({
    imageBase64: "stub",
    provider: providerWithText(JSON.stringify({
      rawText: "12.319572, -11.178174",
      rawPrompt: "secret prompt",
      base64: "AAAA",
      filesystemPath: "C:/secret.png",
      blocks: [{ type: "text", text: "12.319572, -11.178174", apiKey: "secret" }],
    })),
  });
  const serialized = JSON.stringify(result);
  assert.equal(serialized.includes("secret"), false);
  assert.equal(serialized.includes("base64"), false);
  assert.equal(serialized.includes("filesystemPath"), false);
});

test("deadline", async () => {
  const result = await acquirePrimaryImage({
    imageBase64: "stub",
    provider: providerWithText(tableJson),
    budget: createAcquisitionBudget({ maxProviderCalls: 1, startedAtMs: 0, clock: () => 60000 }),
    clock: () => 60000,
  });
  assert.equal(result.status, ACQUISITION_STATUS.DEADLINE_EXCEEDED);
  assert.equal(result.providerCalls, 0);
});

test("provider error classification", async () => {
  const result = await acquirePrimaryImage({ imageBase64: "stub", provider: providerFailure(PRIMARY_ACQUISITION_ERROR.PROVIDER_AUTH_ERROR) });
  assert.equal(result.status, ACQUISITION_STATUS.FAILED);
  assert.deepEqual(result.warnings, [PRIMARY_ACQUISITION_ERROR.PROVIDER_AUTH_ERROR]);
  assert.equal(result.diagnostics.providerStatus, PRIMARY_PROVIDER_STATUS.ERROR);
  assert.equal(result.diagnostics.providerErrorCode, PRIMARY_ACQUISITION_ERROR.PROVIDER_AUTH_ERROR);
});

test("deterministic freeze protection", async () => {
  assert.equal(PRIMARY_ACQUISITION_PROMPT.includes("Do not convert coordinates."), true);
  assert.equal(PRIMARY_ACQUISITION_PROMPT.includes("Do not choose a coordinate system."), true);
  assert.equal(PRIMARY_ACQUISITION_PROMPT.includes("recognizerId"), true);
});

test("provider empty response classification", async () => {
  const result = await acquirePrimaryImage({
    imageBase64: "stub",
    provider: async () => Object.freeze({
      ok: false,
      provider: "mock",
      model: "mock-vl",
      errorCode: PRIMARY_ACQUISITION_ERROR.PROVIDER_EMPTY_RESPONSE,
      responseReceived: true,
      status: 200,
    }),
  });
  assert.equal(result.status, ACQUISITION_STATUS.FAILED);
  assert.equal(result.diagnostics.providerStatus, PRIMARY_PROVIDER_STATUS.ERROR);
  assert.equal(result.diagnostics.providerErrorCode, PRIMARY_ACQUISITION_ERROR.PROVIDER_EMPTY_RESPONSE);
  assert.equal(result.diagnostics.providerResponseReceived, true);
  assert.equal(result.diagnostics.providerHttpStatus, 200);
});

test("malformed JSON diagnostic", async () => {
  const result = await acquirePrimaryImage({ imageBase64: "stub", provider: providerWithText("{ nope }") });
  assert.equal(result.status, ACQUISITION_STATUS.FAILED);
  assert.equal(result.diagnostics.jsonParseStatus, PRIMARY_JSON_PARSE_STATUS.JSON_PARSE_FAILED);
  assert.equal(result.diagnostics.jsonParseReason, PRIMARY_JSON_PARSE_REASON.MALFORMED_JSON);
});

test("truncated JSON diagnostic", async () => {
  const result = await acquirePrimaryImage({ imageBase64: "stub", provider: providerWithText('{"rawText":"abc"') });
  assert.equal(result.status, ACQUISITION_STATUS.FAILED);
  assert.equal(result.diagnostics.jsonParseStatus, PRIMARY_JSON_PARSE_STATUS.JSON_PARSE_FAILED);
  assert.equal(result.diagnostics.jsonParseReason, PRIMARY_JSON_PARSE_REASON.TRUNCATED_JSON);
});

test("schema invalid diagnostic", async () => {
  const result = await acquirePrimaryImage({
    imageBase64: "stub",
    provider: providerWithText(JSON.stringify({
      rawText: "",
      blocks: { bad: true },
    })),
  });
  assert.equal(result.diagnostics.schemaValidationStatus, PRIMARY_SCHEMA_VALIDATION_STATUS.SCHEMA_INVALID);
  assert.equal(result.diagnostics.schemaValidationReason.includes("MISSING_RAW_TEXT"), true);
  assert.equal(result.diagnostics.schemaValidationReason.includes("INVALID_BLOCKS"), true);
});

test("candidate construction empty diagnostic", async () => {
  const result = await acquirePrimaryImage({
    imageBase64: "stub",
    provider: providerWithText(JSON.stringify({
      rawText: "",
      blocks: [],
      documentCues: [],
    })),
  });
  assert.equal(result.diagnostics.candidateConstructionStatus, PRIMARY_CANDIDATE_CONSTRUCTION_STATUS.CANDIDATE_CONSTRUCTION_SUCCESS);
  assert.equal(result.diagnostics.wholeImageCandidateCreated, true);
  assert.equal(result.diagnostics.finalCandidateCount, 1);
});

test("sanitized candidate preview source", async () => {
  const result = await acquirePrimaryImage({ imageBase64: "stub", provider: providerWithText(tableJson) });
  const preview = String(result.candidates[1].text).slice(0, 300);
  assert.equal(preview.includes("Longitude | Latitude"), true);
  assert.equal(preview.length <= 300, true);
});

test("adapter candidateResults summary", async () => {
  const acquisition = await acquirePrimaryImage({ imageBase64: "stub", provider: providerWithText(tableJson) });
  const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
  assert.equal(Array.isArray(adapter.candidateResults), true);
  assert.equal(adapter.candidateResults.length >= 1, true);
  assert.equal(adapter.candidateResults.some((item) => item.candidateId === "primary_whole_image"), true);
});

test("no raw response leakage", async () => {
  const result = await acquirePrimaryImage({
    imageBase64: "stub",
    provider: providerWithText(JSON.stringify({
      rawText: "12.319572, -11.178174",
      rawProviderResponse: "raw-provider-secret",
      providerResponse: "raw-provider-secret",
      blocks: [{ type: "text", text: "12.319572, -11.178174" }],
    })),
  });
  assert.equal(JSON.stringify(result).includes("raw-provider-secret"), false);
});

test("no key leakage", async () => {
  const result = await acquirePrimaryImage({
    imageBase64: "stub",
    provider: providerWithText(JSON.stringify({
      rawText: "12.319572, -11.178174",
      apiKey: "dashscope-secret",
      credentials: "credential-secret",
      blocks: [{ type: "text", text: "12.319572, -11.178174", apiKey: "dashscope-secret" }],
    })),
  });
  assert.equal(JSON.stringify(result).includes("dashscope-secret"), false);
  assert.equal(JSON.stringify(result).includes("credential-secret"), false);
});

test("no prompt leakage", async () => {
  const result = await acquirePrimaryImage({
    imageBase64: "stub",
    provider: providerWithText(JSON.stringify({
      rawText: "12.319572, -11.178174",
      prompt: "hidden prompt",
      rawPrompt: "hidden prompt",
      blocks: [{ type: "text", text: "12.319572, -11.178174" }],
    })),
  });
  assert.equal(JSON.stringify(result).includes("hidden prompt"), false);
});

test("no base64 leakage", async () => {
  const result = await acquirePrimaryImage({
    imageBase64: "stub",
    provider: providerWithText(JSON.stringify({
      rawText: "12.319572, -11.178174",
      imageBase64: "AAAA-BBBB",
      base64: "AAAA-BBBB",
      blocks: [{ type: "text", text: "12.319572, -11.178174" }],
    })),
  });
  assert.equal(JSON.stringify(result).includes("AAAA-BBBB"), false);
  assert.equal(JSON.stringify(result).includes("imageBase64"), false);
});

let passed = 0;
for (const item of tests) {
  try {
    await item.fn();
    passed += 1;
    console.log(`PASS ${item.name}`);
  } catch (error) {
    console.error(`FAIL ${item.name}`);
    throw error;
  }
}

console.log(`Coordinate Engine V3 Primary Acquisition Regression: ${passed}/${tests.length} PASS`);
