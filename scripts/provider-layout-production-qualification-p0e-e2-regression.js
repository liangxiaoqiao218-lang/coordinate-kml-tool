import assert from "node:assert/strict";
import {
  PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_CHALLENGE_SCHEMA_VERSION,
  PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_GRANT_SCHEMA_VERSION,
  PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_OUTPUT_POLICY,
  PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_RESPONSE_CONTRACT,
  ProviderLayoutProductionQualificationGrantRuntime,
  authorizeProviderLayoutProductionQualificationGrant,
  executeProviderLayoutProductionQualificationProbe,
  hasProviderLayoutProductionQualificationGrantCapability,
  serializeProviderLayoutProductionQualificationGrant,
  verifyEd25519DetachedSignature,
  verifyProviderLayoutProductionQualificationGrantSignature
} from "../server/evidence-acquisition/index.js";
import { createCoordinateImageIdentity } from "../server/recognition/coordinate-image-safety.js";

const now = 2_000_000_000_000;
const requestId = "00000000-0000-4000-8000-0000000000e2";
const runtimeCommit = "4325535f2e49584023ad9189eac7a450874580a4";
const runtimeBranch = "release/wgs84-kml-closure";
const service = "coordinate-kml-tool-rc";
const bootA = "a".repeat(64);
const bootB = "b".repeat(64);
const challengeA = "c".repeat(64);
const challengeB = "d".repeat(64);
const forbidden = "35.4478191,83.1789913 PRIVATE_SYNTHETIC_TEXT";

function makeBmp(width = 120, height = 100) {
  const rowBytes = Math.floor(((24 * width) + 31) / 32) * 4;
  const pixelBytes = rowBytes * height;
  const buffer = Buffer.alloc(54 + pixelBytes, 0);
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

const imageIdentity = createCoordinateImageIdentity(
  { buffer: makeBmp(), mimetype: "image/bmp" },
  { requestId, page: 1 }
);

function makeRuntime({ currentTime = now, bootIdentity = bootA, challengeId = challengeA } = {}) {
  return new ProviderLayoutProductionQualificationGrantRuntime({
    now: () => currentTime,
    bootIdentity,
    challengeIdFactory: () => challengeId
  });
}

function issue(runtime, overrides = {}) {
  const outcome = runtime.issueChallenge({
    service: overrides.service ?? service,
    runtimeCommit: overrides.runtimeCommit ?? runtimeCommit,
    runtimeBranch: overrides.runtimeBranch ?? runtimeBranch,
    ttlMs: overrides.ttlMs
  });
  assert.equal(outcome.ok, true);
  return outcome.challenge;
}

const detachedChallenge = Object.freeze({
  schema_version: PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_CHALLENGE_SCHEMA_VERSION,
  service,
  runtime_commit: runtimeCommit,
  runtime_branch: runtimeBranch,
  boot_identity: bootA,
  challenge_id: challengeA,
  issued_at: now,
  expires_at: now + 60_000
});

function grant(challenge = detachedChallenge, overrides = {}) {
  return {
    schema_version: PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_GRANT_SCHEMA_VERSION,
    service,
    runtime_commit: runtimeCommit,
    runtime_branch: runtimeBranch,
    request_id: requestId,
    nonce: "p0e-e2-synthetic-nonce-0001",
    synthetic_image: true,
    canonical_image_identity: { ...imageIdentity },
    challenge: { ...challenge },
    provider_id: "ALIYUN_DASHSCOPE",
    model_family: "QWEN_VL",
    model_name: "qwen-vl-plus",
    response_contract_id: PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_RESPONSE_CONTRACT,
    max_provider_calls: 1,
    issued_at: now,
    expires_at: now + 60_000,
    output_policy: PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_OUTPUT_POLICY,
    ...overrides
  };
}

function authorize(overrides = {}) {
  const runtime = overrides.runtime ?? makeRuntime();
  const challenge = overrides.challenge ?? issue(runtime);
  const inputGrant = overrides.grant ?? grant(challenge, overrides.grantOverrides);
  const authorization = authorizeProviderLayoutProductionQualificationGrant({
    enabled: overrides.enabled ?? true,
    publicKeyPem: overrides.publicKeyPem ?? "synthetic-public-key-placeholder",
    grant: inputGrant,
    signatureBase64Url: "synthetic-signature",
    service: overrides.service ?? service,
    runtimeCommit: overrides.runtimeCommit ?? runtimeCommit,
    runtimeBranch: overrides.runtimeBranch ?? runtimeBranch,
    requestId: overrides.requestId ?? requestId,
    imageIdentity: overrides.imageIdentity ?? imageIdentity,
    providerId: overrides.providerId ?? "ALIYUN_DASHSCOPE",
    modelFamily: overrides.modelFamily ?? "QWEN_VL",
    modelName: overrides.modelName ?? "qwen-vl-plus",
    responseContractId: overrides.responseContractId ?? PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_RESPONSE_CONTRACT,
    runtime,
    now: overrides.now ?? now,
    verify: overrides.verify ?? (() => true)
  });
  return { ...authorization, runtime, challenge, inputGrant };
}

let passed = 0;
async function test(name, action) {
  await action();
  passed += 1;
  process.stdout.write(`PASS ${name}\n`);
}

await test("gate is disabled unless explicitly enabled", () => {
  const result = authorizeProviderLayoutProductionQualificationGrant({
    enabled: false,
    publicKeyPem: "present",
    grant: grant()
  });
  assert.equal(result.reason, "PRODUCTION_QUALIFICATION_DISABLED");
});

await test("missing public key fails before authorization", () => {
  const result = authorizeProviderLayoutProductionQualificationGrant({ enabled: true, grant: grant() });
  assert.equal(result.reason, "PRODUCTION_QUALIFICATION_PUBLIC_KEY_MISSING");
});

await test("missing challenge runtime fails before authorization", () => {
  const result = authorizeProviderLayoutProductionQualificationGrant({
    enabled: true,
    publicKeyPem: "present",
    grant: grant()
  });
  assert.equal(result.reason, "PRODUCTION_QUALIFICATION_RUNTIME_MISSING");
});

await test("server issues one boot-bound challenge and repeats only that descriptor", () => {
  const runtime = makeRuntime();
  const first = issue(runtime);
  const repeated = issue(runtime);
  assert.deepEqual(repeated, first);
  assert.equal(first.boot_identity, bootA);
  assert.equal(first.challenge_id, challengeA);
  assert.equal(first.schema_version, PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_CHALLENGE_SCHEMA_VERSION);
  assert.equal(first.expires_at - first.issued_at, 5 * 60 * 1000);
});

await test("challenge lifetime cannot exceed five minutes and expired challenge cannot be reissued", () => {
  const runtime = makeRuntime();
  assert.equal(runtime.issueChallenge({ service, runtimeCommit, runtimeBranch, ttlMs: (5 * 60 * 1000) + 1 }).ok, false);
  issue(runtime, { ttlMs: 1 });
  runtime.now = () => now + 1;
  assert.equal(runtime.issueChallenge({ service, runtimeCommit, runtimeBranch }).reason, "PRODUCTION_QUALIFICATION_CHALLENGE_EXPIRED");
});

await test("valid exact bindings mint an in-process grant capability", () => {
  const result = authorize();
  assert.equal(result.ok, true);
  assert.equal(hasProviderLayoutProductionQualificationGrantCapability(result.grant), true);
  assert.equal(Object.isFrozen(result.grant), true);
});

await test("signature verification is mandatory and invalid Ed25519 input fails closed", () => {
  assert.equal(authorize({ verify: () => false }).reason, "PRODUCTION_QUALIFICATION_SIGNATURE_INVALID");
  assert.equal(verifyProviderLayoutProductionQualificationGrantSignature({
    grant: grant(),
    signatureBase64Url: "not-a-signature",
    publicKeyPem: "not-a-public-key"
  }), false);
});

await test("Ed25519 public-key verification accepts the RFC 8032 public test vector", () => {
  const publicKeyHex = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";
  const signatureHex = "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e06522490155"
    + "5fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b";
  const spki = Buffer.concat([
    Buffer.from("302a300506032b6570032100", "hex"),
    Buffer.from(publicKeyHex, "hex")
  ]);
  const publicKeyPem = `-----BEGIN PUBLIC KEY-----\n${spki.toString("base64").match(/.{1,64}/g).join("\n")}\n-----END PUBLIC KEY-----`;
  assert.equal(verifyEd25519DetachedSignature({
    message: Buffer.alloc(0),
    signatureBase64Url: Buffer.from(signatureHex, "hex").toString("base64url"),
    publicKeyPem
  }), true);
});

await test("private-key PEM labels and non-Ed25519 public keys are rejected", () => {
  const signature = Buffer.alloc(64).toString("base64url");
  assert.equal(verifyEd25519DetachedSignature({
    message: Buffer.alloc(0),
    signatureBase64Url: signature,
    publicKeyPem: "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----"
  }), false);
  const p256Generator = Buffer.from(
    "3059301306072a8648ce3d020106082a8648ce3d03010703420004"
    + "6b17d1f2e12c4247f8bce6e563a440f277037d812deb33a0f4a13945d898c296"
    + "4fe342e2fe1a7f9b8ee7eb4a7c0f9e162bce33576b315ececbb6406837bf51f5",
    "hex"
  );
  const p256PublicKeyPem = `-----BEGIN PUBLIC KEY-----\n${p256Generator.toString("base64").match(/.{1,64}/g).join("\n")}\n-----END PUBLIC KEY-----`;
  assert.equal(verifyEd25519DetachedSignature({
    message: Buffer.alloc(0),
    signatureBase64Url: signature,
    publicKeyPem: p256PublicKeyPem
  }), false);
});

await test("service commit branch and request bindings fail closed", () => {
  assert.equal(authorize({ service: "other-service" }).ok, false);
  assert.equal(authorize({ runtimeCommit: "0".repeat(40) }).ok, false);
  assert.equal(authorize({ runtimeBranch: "other" }).ok, false);
  assert.equal(authorize({ requestId: requestId.replace(/e2$/, "e3") }).ok, false);
});

await test("challenge and grant identities cannot be substituted", () => {
  const runtime = makeRuntime();
  const challenge = issue(runtime);
  const substitutedChallenge = { ...challenge, challenge_id: "e".repeat(64) };
  assert.equal(authorize({ runtime, challenge, grant: grant(substitutedChallenge) }).ok, false);
  assert.equal(authorize({ runtime, challenge, grant: grant(challenge, { nonce: "different-grant-nonce-0002" }) }).ok, true);
});

await test("canonical image identity mismatch fails closed", () => {
  const other = createCoordinateImageIdentity(
    { buffer: Buffer.concat([makeBmp(), Buffer.from([0])]), mimetype: "image/bmp" },
    { requestId, page: 1 }
  );
  assert.equal(authorize({ imageIdentity: other }).reason, "PRODUCTION_QUALIFICATION_BINDING_MISMATCH");
});

await test("Provider model and family bindings fail closed", () => {
  assert.equal(authorize({ providerId: "OTHER" }).ok, false);
  assert.equal(authorize({ modelFamily: "OTHER" }).ok, false);
  assert.equal(authorize({ modelName: "other-model" }).ok, false);
});

await test("unproven or forged response contract cannot be promoted", () => {
  assert.equal(authorize({ grantOverrides: { response_contract_id: "DASHSCOPE_STRUCTURED_LAYOUT_V1" } }).ok, false);
  assert.equal(authorize({ responseContractId: "DASHSCOPE_STRUCTURED_LAYOUT_V1" }).ok, false);
});

await test("maximum Provider calls must be exactly one", () => {
  assert.equal(authorize({ grantOverrides: { max_provider_calls: 0 } }).ok, false);
  assert.equal(authorize({ grantOverrides: { max_provider_calls: 2 } }).ok, false);
});

await test("output policy is fixed and cannot be widened", () => {
  assert.equal(authorize({ grantOverrides: { output_policy: "RAW_PROVIDER_RESPONSE" } }).ok, false);
});

await test("grant lifetime is current bounded and nested within challenge lifetime", () => {
  assert.equal(authorize({ grantOverrides: { expires_at: now + (5 * 60 * 1000) + 1 } }).ok, false);
  assert.equal(authorize({ grantOverrides: { expires_at: now } }).ok, false);
  assert.equal(authorize({ grantOverrides: { issued_at: now + 1 } }).ok, false);
  assert.equal(authorize({ grantOverrides: { issued_at: now - 1 } }).ok, false);
});

await test("missing nonce non-synthetic and legacy grants fail closed", () => {
  assert.equal(authorize({ grantOverrides: { nonce: "" } }).ok, false);
  assert.equal(authorize({ grantOverrides: { synthetic_image: false } }).ok, false);
  const legacy = grant();
  delete legacy.challenge;
  legacy.schema_version = "provider_layout_production_qualification_grant_v1";
  assert.equal(authorize({ grant: legacy }).ok, false);
});

await test("JSON serialization and structuredClone lose grant capability", () => {
  const authorized = authorize().grant;
  assert.equal(hasProviderLayoutProductionQualificationGrantCapability(JSON.parse(JSON.stringify(authorized))), false);
  assert.equal(hasProviderLayoutProductionQualificationGrantCapability(structuredClone(authorized)), false);
});

await test("canonical serializer is stable across key order", () => {
  const first = grant();
  const reversed = Object.fromEntries(Object.entries(first).reverse());
  assert.equal(
    serializeProviderLayoutProductionQualificationGrant(first).toString("utf8"),
    serializeProviderLayoutProductionQualificationGrant(reversed).toString("utf8")
  );
});

await test("same signed grant cannot be reauthorized by a fresh runtime", () => {
  const first = authorize();
  const restarted = makeRuntime({ bootIdentity: bootB, challengeId: challengeB });
  const result = authorize({ runtime: restarted, challenge: first.challenge, grant: first.inputGrant });
  assert.equal(result.ok, false);
  assert.match(result.reason, /CHALLENGE_(?:MISSING|BINDING_MISMATCH)/);
});

await test("challenge routed to another instance fails closed before Provider", async () => {
  const first = authorize();
  const otherRuntime = makeRuntime({ bootIdentity: bootB, challengeId: challengeB });
  issue(otherRuntime);
  const otherAuthorization = authorize({ runtime: otherRuntime, challenge: first.challenge, grant: first.inputGrant });
  assert.equal(otherAuthorization.ok, false);
  let providerCalls = 0;
  const outcome = await executeProviderLayoutProductionQualificationProbe({
    grant: first.grant,
    runtime: otherRuntime,
    providerCall: async () => { providerCalls += 1; },
    collectQualification: () => ({ status: "RESPONSE_CONTRACT_UNSUPPORTED" })
  });
  assert.equal(outcome.ok, false);
  assert.equal(providerCalls, 0);
});

await test("different grants and nonces for one challenge permit at most one Provider call", async () => {
  const runtime = makeRuntime();
  const challenge = issue(runtime);
  const first = authorize({ runtime, challenge, grant: grant(challenge, { nonce: "nonce-one-00000001" }) });
  const second = authorize({ runtime, challenge, grant: grant(challenge, { nonce: "nonce-two-00000002" }) });
  let providerCalls = 0;
  const call = authorized => executeProviderLayoutProductionQualificationProbe({
    grant: authorized,
    runtime,
    providerCall: async () => { providerCalls += 1; return { id: "synthetic" }; },
    collectQualification: () => ({ status: "RESPONSE_CONTRACT_UNSUPPORTED" })
  });
  const [one, two] = await Promise.all([call(first.grant), call(second.grant)]);
  assert.equal(providerCalls, 1);
  assert.equal([one, two].filter(item => item.ok).length, 1);
});

await test("expired grant is rejected before Provider", async () => {
  const authorized = authorize();
  authorized.runtime.now = () => now + 60_001;
  let providerCalls = 0;
  const outcome = await executeProviderLayoutProductionQualificationProbe({
    grant: authorized.grant,
    runtime: authorized.runtime,
    providerCall: async () => { providerCalls += 1; },
    collectQualification: () => ({ status: "RESPONSE_CONTRACT_UNSUPPORTED" })
  });
  assert.equal(outcome.reason, "PRODUCTION_QUALIFICATION_EXPIRED");
  assert.equal(providerCalls, 0);
});

await test("serialized challenge and grant cannot restore server capabilities", async () => {
  const authorized = authorize();
  const persisted = JSON.parse(JSON.stringify(authorized.grant));
  const restarted = makeRuntime({ bootIdentity: bootB, challengeId: challengeB });
  let providerCalls = 0;
  const outcome = await executeProviderLayoutProductionQualificationProbe({
    grant: persisted,
    runtime: restarted,
    providerCall: async () => { providerCalls += 1; },
    collectQualification: () => ({ status: "RESPONSE_CONTRACT_UNSUPPORTED" })
  });
  assert.equal(outcome.reason, "PRODUCTION_QUALIFICATION_CAPABILITY_MISSING");
  assert.equal(providerCalls, 0);
});

await test("successful probe performs exactly one Provider call and emits only fixed status", async () => {
  const authorized = authorize();
  let providerCalls = 0;
  const outcome = await executeProviderLayoutProductionQualificationProbe({
    grant: authorized.grant,
    runtime: authorized.runtime,
    providerCall: async () => {
      providerCalls += 1;
      return { id: "synthetic-response", choices: [{ message: { content: forbidden } }] };
    },
    collectQualification: () => ({ status: "RESPONSE_CONTRACT_UNSUPPORTED", raw: forbidden })
  });
  assert.equal(providerCalls, 1);
  assert.deepEqual(outcome, {
    ok: true,
    status: "RESPONSE_CONTRACT_UNSUPPORTED",
    provider_attempted: true,
    provider_completed: true,
    provider_call_count: 1
  });
  assert.equal(JSON.stringify(outcome).includes(forbidden), false);
});

await test("Provider failure timeout and uncertain result each consume their process challenge", async () => {
  const scenarios = [
    {
      name: "failure",
      providerCall: async () => { throw new Error("sensitive Provider failure"); },
      collectQualification: () => { throw new Error("must not collect"); },
      providerCompleted: false
    },
    {
      name: "timeout",
      providerCall: async () => { const error = new Error("sensitive Provider timeout"); error.code = "ETIMEDOUT"; throw error; },
      collectQualification: () => { throw new Error("must not collect"); },
      providerCompleted: false
    },
    {
      name: "uncertain",
      providerCall: async () => ({ id: "uncertain-response" }),
      collectQualification: () => { throw new Error("sensitive uncertain result"); },
      providerCompleted: true
    }
  ];
  for (const [index, scenario] of scenarios.entries()) {
    const authorized = authorize({
      runtime: makeRuntime({
        bootIdentity: String(index + 1).repeat(64),
        challengeId: String(index + 4).repeat(64)
      })
    });
    let providerCalls = 0;
    const outcome = await executeProviderLayoutProductionQualificationProbe({
      grant: authorized.grant,
      runtime: authorized.runtime,
      providerCall: async () => { providerCalls += 1; return scenario.providerCall(); },
      collectQualification: scenario.collectQualification
    });
    assert.equal(providerCalls, 1, `${scenario.name} Provider call count`);
    assert.equal(outcome.provider_call_count, 1, `${scenario.name} fixed call count`);
    assert.equal(outcome.provider_completed, scenario.providerCompleted, `${scenario.name} completion state`);
    assert.equal(JSON.stringify(outcome).includes("sensitive"), false, `${scenario.name} redaction`);
    assert.equal(authorized.runtime.issueChallenge({ service, runtimeCommit, runtimeBranch }).reason,
      "PRODUCTION_QUALIFICATION_CHALLENGE_ALREADY_CONSUMED");
    const replay = authorize({ runtime: authorized.runtime, challenge: authorized.challenge, grant: authorized.inputGrant });
    assert.equal(replay.ok, false, `${scenario.name} replay authorization`);
  }
});

await test("non-fixed collector output is rejected without disclosure", async () => {
  const authorized = authorize();
  const outcome = await executeProviderLayoutProductionQualificationProbe({
    grant: authorized.grant,
    runtime: authorized.runtime,
    providerCall: async () => ({ id: "synthetic-response" }),
    collectQualification: () => ({ status: "RAW_PROVIDER_RESPONSE", raw: forbidden })
  });
  assert.equal(outcome.ok, false);
  assert.equal(JSON.stringify(outcome).includes(forbidden), false);
});

await test("ordinary objects cannot invoke the Provider", async () => {
  const runtime = makeRuntime();
  const challenge = issue(runtime);
  let providerCalls = 0;
  const outcome = await executeProviderLayoutProductionQualificationProbe({
    grant: grant(challenge),
    runtime,
    providerCall: async () => { providerCalls += 1; },
    collectQualification: () => ({ status: "RESPONSE_CONTRACT_UNSUPPORTED" })
  });
  assert.equal(outcome.ok, false);
  assert.equal(providerCalls, 0);
});

await test("fixed response contains no image Provider text bbox headers cookies keys or downstream authority", async () => {
  const authorized = authorize();
  const outcome = await executeProviderLayoutProductionQualificationProbe({
    grant: authorized.grant,
    runtime: authorized.runtime,
    providerCall: async () => ({
      id: "response",
      choices: [{ message: { content: forbidden } }],
      output: { layout: [{ text: forbidden, bbox: [1, 2, 3, 4] }] }
    }),
    collectQualification: () => ({ status: "RESPONSE_CONTRACT_UNSUPPORTED" })
  });
  const serialized = JSON.stringify(outcome);
  for (const value of [forbidden, "bbox", "Authorization", "Cookie", "secret", "trusted_layout", "Point Intent", "Map", "KML"]) {
    assert.equal(serialized.includes(value), false);
  }
});

console.log(JSON.stringify({
  suite: "provider-layout-production-qualification-p0e-e2-regression",
  passed,
  providerCalls: 0,
  externalNetworkCalls: 0,
  databaseOrUsageWrites: 0,
  productionOperations: 0
}, null, 2));
