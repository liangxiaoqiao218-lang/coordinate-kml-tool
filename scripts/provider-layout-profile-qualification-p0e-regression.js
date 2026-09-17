import assert from "node:assert/strict";
import {
  PROVIDER_LAYOUT_CLASSIFICATION_REASON,
  PROVIDER_LAYOUT_PROFILE_QUALIFICATION_STATUS,
  PROVIDER_LAYOUT_RESPONSE_CONTRACT,
  SERVER_LAYOUT_ROLE_CLASSIFICATION_CAPABILITY,
  ProviderLayoutProfileQualificationRuntime,
  classifyProviderLayoutRoles,
  collectProviderLayoutProfileQualification,
  getProductionProviderLayoutClassifierProfile,
  getProviderLayoutRoleClassificationFailureReason,
  hasProviderLayoutProfileQualificationCapability,
  isProviderLayoutQualificationReadAllowed,
  validateProviderLayoutProfileQualification
} from "../server/evidence-acquisition/index.js";
import { createCoordinateImageIdentity } from "../server/recognition/coordinate-image-safety.js";

const providerResponseId = "synthetic-p0e-provider-response";
const requestId = "00000000-0000-4000-8000-0000000000e1";
const forbiddenText = "35.4478191,83.1789913 PRIVATE_SYNTHETIC_TEXT";
const forbiddenBbox = "5,5,115,25";

function makeBmp(width = 120, height = 100, fill = 0) {
  const rowBytes = Math.floor(((24 * width) + 31) / 32) * 4;
  const pixelBytes = rowBytes * height;
  const buffer = Buffer.alloc(54 + pixelBytes, fill);
  buffer.write("BM", 0, "ascii");
  buffer.writeUInt32LE(buffer.length, 2);
  buffer.writeUInt32LE(54, 10);
  buffer.writeUInt32LE(40, 14);
  buffer.writeInt32LE(width, 18);
  buffer.writeInt32LE(height, 22);
  buffer.writeUInt16LE(1, 26);
  buffer.writeUInt16LE(24, 28);
  buffer.writeUInt32LE(pixelBytes, 34);
  return buffer;
}

function imageIdentity(fill = 0, request = requestId) {
  return createCoordinateImageIdentity(
    { buffer: makeBmp(120, 100, fill), mimetype: "image/bmp" },
    { requestId: request, page: 1 }
  );
}

const canonicalImage = imageIdentity();

function row(overrides = {}) {
  return {
    candidate_schema_version: "provider_layout_candidate_v1",
    id: "search",
    source_ref: "search",
    line_id: "line-search",
    source_line_id: "line-search",
    text: forbiddenText,
    bbox: [5, 5, 115, 25],
    coordinate_space: "ORIGINAL_IMAGE_PIXELS",
    page: 1,
    image_width: 120,
    image_height: 100,
    source_role: "MAP_SEARCH_BOX",
    ...overrides
  };
}

function response(rows = [
  row(),
  row({ id: "details", source_ref: "details", line_id: "line-details", source_line_id: "line-details", bbox: [5, 55, 115, 75], source_role: "MAP_PLACE_DETAILS" })
]) {
  return {
    id: providerResponseId,
    choices: [{ message: { content: `free-form ${forbiddenText}` } }],
    output: { layout: rows }
  };
}

function collect(overrides = {}) {
  return collectProviderLayoutProfileQualification({
    response: overrides.response ?? response(),
    providerId: overrides.providerId ?? "ALIYUN_DASHSCOPE",
    modelName: overrides.modelName ?? "qwen-vl-plus",
    responseContractId: overrides.responseContractId ?? PROVIDER_LAYOUT_RESPONSE_CONTRACT.DASHSCOPE_STRUCTURED_LAYOUT_V1,
    providerResponseId: overrides.providerResponseId ?? providerResponseId,
    imageIdentity: overrides.imageIdentity ?? canonicalImage,
    resultRevision: overrides.resultRevision ?? 1
  });
}

let passed = 0;
function test(name, action) {
  action();
  passed += 1;
  process.stdout.write(`PASS ${name}\n`);
}

test("valid redacted structure becomes qualification candidate only", () => {
  const evidence = collect();
  assert.equal(evidence.status, PROVIDER_LAYOUT_PROFILE_QUALIFICATION_STATUS.QUALIFICATION_CANDIDATE);
  assert.equal(hasProviderLayoutProfileQualificationCapability(evidence), true);
  assert.equal(evidence[SERVER_LAYOUT_ROLE_CLASSIFICATION_CAPABILITY], undefined);
  assert.equal(evidence.bbox_coordinate_space, "ORIGINAL_IMAGE_PIXELS");
  assert.match(evidence.qualification_sha256, /^[0-9a-f]{64}$/);
});

test("no structured layout is fixed no-layout status", () => {
  assert.equal(collect({ response: { id: providerResponseId, choices: [] } }).status, "NO_STRUCTURED_LAYOUT");
});

test("unknown response contract is unsupported", () => {
  assert.equal(collect({ responseContractId: "UNKNOWN_V9" }).status, "RESPONSE_CONTRACT_UNSUPPORTED");
});

test("unknown structured layout container is unsupported", () => {
  assert.equal(collect({ response: { id: providerResponseId, output: { layout_v2: [row()] } } }).status, "RESPONSE_CONTRACT_UNSUPPORTED");
});

test("unknown candidate field version is unsupported", () => {
  assert.equal(collect({ response: response([row({ candidate_schema_version: "provider_layout_candidate_v9" })]) }).status, "RESPONSE_CONTRACT_UNSUPPORTED");
});

test("missing bbox coordinate space is unproven", () => {
  assert.equal(collect({ response: response([row({ coordinate_space: undefined })]) }).status, "COORDINATE_SPACE_UNPROVEN");
});

test("missing page is unproven", () => {
  assert.equal(collect({ response: response([row({ page: undefined })]) }).status, "COORDINATE_SPACE_UNPROVEN");
});

test("missing source image dimensions are unproven", () => {
  assert.equal(collect({ response: response([row({ image_width: undefined })]) }).status, "COORDINATE_SPACE_UNPROVEN");
});

test("missing source reference conflicts", () => {
  assert.equal(collect({ response: response([row({ source_ref: undefined })]) }).status, "QUALIFICATION_CONFLICT");
  assert.equal(collect({ response: response([row({ source_line_id: undefined })]) }).status, "QUALIFICATION_CONFLICT");
});

test("duplicate source line conflicts", () => {
  assert.equal(collect({ response: response([
    row(),
    row({ id: "details", source_ref: "details", line_id: "line-search", source_line_id: "line-search", bbox: [5, 55, 115, 75] })
  ]) }).status, "QUALIFICATION_CONFLICT");
});

test("out-of-bounds bbox conflicts without exposing bbox values", () => {
  const evidence = collect({ response: response([row({ bbox: [5, 5, 130, 25] })]) });
  assert.equal(evidence.status, "QUALIFICATION_CONFLICT");
  assert.equal(JSON.stringify(evidence).includes('"bbox":[5,5,130,25]'), false);
});

test("unknown Provider and model family are unsupported", () => {
  assert.equal(collect({ providerId: "UNKNOWN" }).status, "RESPONSE_CONTRACT_UNSUPPORTED");
  assert.equal(collect({ modelName: "unknown-model" }).status, "RESPONSE_CONTRACT_UNSUPPORTED");
});

test("qualification validates only against exact Provider response binding", () => {
  const evidence = collect();
  assert.equal(validateProviderLayoutProfileQualification({
    evidence,
    providerResponseId,
    imageIdentity: canonicalImage,
    resultRevision: 1
  }).valid, true);
  assert.equal(validateProviderLayoutProfileQualification({
    evidence,
    providerResponseId: "other-response",
    imageIdentity: canonicalImage,
    resultRevision: 1
  }).reason, "QUALIFICATION_BINDING_MISMATCH");
});

test("collection rejects Provider response identity mismatch before qualification", () => {
  const evidence = collect({ providerResponseId: "arbitrary-other-response" });
  assert.equal(evidence.status, "QUALIFICATION_CONFLICT");
  assert.equal(validateProviderLayoutProfileQualification({
    evidence,
    providerResponseId: "arbitrary-other-response",
    imageIdentity: canonicalImage,
    resultRevision: 1
  }).valid, false);
});

test("image identity mismatch fails closed", () => {
  assert.equal(validateProviderLayoutProfileQualification({
    evidence: collect(),
    providerResponseId,
    imageIdentity: imageIdentity(1, "other-request"),
    resultRevision: 1
  }).reason, "QUALIFICATION_BINDING_MISMATCH");
});

test("result revision mismatch fails closed", () => {
  assert.equal(validateProviderLayoutProfileQualification({
    evidence: collect(),
    providerResponseId,
    imageIdentity: canonicalImage,
    resultRevision: 2
  }).reason, "QUALIFICATION_BINDING_MISMATCH");
});

test("JSON and structuredClone lose server qualification capability", () => {
  const evidence = collect();
  assert.equal(hasProviderLayoutProfileQualificationCapability(JSON.parse(JSON.stringify(evidence))), false);
  assert.equal(hasProviderLayoutProfileQualificationCapability(structuredClone(evidence)), false);
});

test("client and Provider role claims cannot create trusted role capability", () => {
  const outcome = classifyProviderLayoutRoles({
    profile: null,
    imageIdentity: canonicalImage,
    candidates: [{ ...row(), qualification: collect() }],
    providerResponseId,
    resultRevision: 1
  });
  assert.equal(outcome.ok, false);
  assert.equal(getProviderLayoutRoleClassificationFailureReason(outcome), PROVIDER_LAYOUT_CLASSIFICATION_REASON.EVIDENCE_MISSING);
});

test("serialized qualification leaks no response content, text, coordinates, bbox values, headers, or secrets", () => {
  const serialized = JSON.stringify(collect());
  for (const forbidden of [forbiddenText, forbiddenBbox, "message.content", "Authorization", "Bearer", "Cookie", "secret-key"]) {
    assert.equal(serialized.includes(forbidden), false, `must not expose ${forbidden}`);
  }
});

test("runtime allows one exact image-and-request-bound read", () => {
  let now = 1000;
  const runtime = new ProviderLayoutProfileQualificationRuntime({ now: () => now });
  assert.equal(runtime.capture({ evidence: collect(), requestId, imageSha256: canonicalImage.image_sha256 }).ok, true);
  assert.equal(runtime.consume({ requestId, imageSha256: "0".repeat(64) }).reason, "QUALIFICATION_READ_BINDING_MISMATCH");
  const consumed = runtime.consume({ requestId, imageSha256: canonicalImage.image_sha256 });
  assert.equal(consumed.ok, true);
  assert.equal(consumed.evidence.status, "QUALIFICATION_CANDIDATE");
  assert.equal(runtime.consume({ requestId, imageSha256: canonicalImage.image_sha256 }).reason, "QUALIFICATION_REPLAY_REJECTED");
  now += 1;
});

test("runtime expires at five-minute boundary and cannot capture twice", () => {
  let now = 1000;
  const runtime = new ProviderLayoutProfileQualificationRuntime({ now: () => now });
  assert.equal(runtime.capture({ evidence: collect(), requestId, imageSha256: canonicalImage.image_sha256 }).ok, true);
  assert.equal(runtime.capture({ evidence: collect(), requestId: requestId.replace(/e1$/, "e2"), imageSha256: canonicalImage.image_sha256 }).reason, "QUALIFICATION_CAPTURE_ALREADY_USED");
  now += 5 * 60 * 1000;
  assert.equal(runtime.consume({ requestId, imageSha256: canonicalImage.image_sha256 }).reason, "QUALIFICATION_EXPIRED");
});

test("forged client JSON cannot be captured as server qualification evidence", () => {
  const runtime = new ProviderLayoutProfileQualificationRuntime();
  const forged = JSON.parse(JSON.stringify(collect()));
  assert.equal(runtime.capture({ evidence: forged, requestId, imageSha256: canonicalImage.image_sha256 }).reason, "QUALIFICATION_CAPTURE_BINDING_INVALID");
  assert.equal(new ProviderLayoutProfileQualificationRuntime().capture({
    evidence: collect(),
    requestId,
    imageSha256: "0".repeat(64)
  }).reason, "QUALIFICATION_CAPTURE_BINDING_INVALID");
});

test("Production classifier profile remains unavailable", () => {
  assert.equal(getProductionProviderLayoutClassifierProfile(), null);
});

test("qualification read gate denies Production and non-loopback callers", () => {
  const base = {
    regressionTestHeader: "true",
    regressionTestModeEnabled: "true",
    nodeEnv: "test",
    remoteAddresses: ["127.0.0.1"]
  };
  assert.equal(isProviderLayoutQualificationReadAllowed(base), true);
  assert.equal(isProviderLayoutQualificationReadAllowed({ ...base, nodeEnv: "production" }), false);
  assert.equal(isProviderLayoutQualificationReadAllowed({ ...base, remoteAddresses: ["192.0.2.50"] }), false);
  assert.equal(isProviderLayoutQualificationReadAllowed({ ...base, regressionTestHeader: "false" }), false);
  assert.equal(isProviderLayoutQualificationReadAllowed({ ...base, regressionTestModeEnabled: "false" }), false);
});

console.log(JSON.stringify({
  suite: "provider-layout-profile-qualification-p0e-regression",
  passed,
  providerCalls: 0,
  externalNetworkCalls: 0,
  databaseOrUsageWrites: 0,
  productionOperations: 0
}, null, 2));
