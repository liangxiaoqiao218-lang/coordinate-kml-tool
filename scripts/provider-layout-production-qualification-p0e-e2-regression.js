import assert from "node:assert/strict";
import {
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
const runtimeCommit = "29b557ba1629a49b9b86e8160cdc01d1da6bc445";
const runtimeBranch = "release/wgs84-kml-closure";
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

function grant(overrides = {}) {
  return {
    schema_version: PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_GRANT_SCHEMA_VERSION,
    service: "coordinate-kml-tool-rc",
    runtime_commit: runtimeCommit,
    runtime_branch: runtimeBranch,
    request_id: requestId,
    nonce: "p0e-e2-synthetic-nonce-0001",
    synthetic_image: true,
    canonical_image_identity: { ...imageIdentity },
    provider_id: "ALIYUN_DASHSCOPE",
    model_family: "QWEN_VL",
    model_name: "qwen-vl-plus",
    response_contract_id: PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_RESPONSE_CONTRACT,
    max_provider_calls: 1,
    issued_at: now - 1000,
    expires_at: now + 60_000,
    output_policy: PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_OUTPUT_POLICY,
    ...overrides
  };
}

function authorize(overrides = {}) {
  return authorizeProviderLayoutProductionQualificationGrant({
    enabled: true,
    publicKeyPem: "synthetic-public-key-placeholder",
    grant: overrides.grant ?? grant(),
    signatureBase64Url: "synthetic-signature",
    service: overrides.service ?? "coordinate-kml-tool-rc",
    runtimeCommit: overrides.runtimeCommit ?? runtimeCommit,
    runtimeBranch: overrides.runtimeBranch ?? runtimeBranch,
    requestId: overrides.requestId ?? requestId,
    imageIdentity: overrides.imageIdentity ?? imageIdentity,
    providerId: overrides.providerId ?? "ALIYUN_DASHSCOPE",
    modelFamily: overrides.modelFamily ?? "QWEN_VL",
    modelName: overrides.modelName ?? "qwen-vl-plus",
    responseContractId: overrides.responseContractId ?? PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_RESPONSE_CONTRACT,
    now: overrides.now ?? now,
    verify: overrides.verify ?? (() => true)
  });
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
  assert.equal(authorize({ grant: grant({ response_contract_id: "DASHSCOPE_STRUCTURED_LAYOUT_V1" }) }).ok, false);
  assert.equal(authorize({ responseContractId: "DASHSCOPE_STRUCTURED_LAYOUT_V1" }).ok, false);
});

await test("maximum Provider calls must be exactly one", () => {
  assert.equal(authorize({ grant: grant({ max_provider_calls: 0 }) }).ok, false);
  assert.equal(authorize({ grant: grant({ max_provider_calls: 2 }) }).ok, false);
});

await test("output policy is fixed and cannot be widened", () => {
  assert.equal(authorize({ grant: grant({ output_policy: "RAW_PROVIDER_RESPONSE" }) }).ok, false);
});

await test("grant lifetime is at most five minutes and must be current", () => {
  assert.equal(authorize({ grant: grant({ expires_at: now + (5 * 60 * 1000) + 1 }) }).ok, false);
  assert.equal(authorize({ grant: grant({ expires_at: now }) }).ok, false);
  assert.equal(authorize({ grant: grant({ issued_at: now + 1 }) }).ok, false);
});

await test("missing nonce and non-synthetic grants fail closed", () => {
  assert.equal(authorize({ grant: grant({ nonce: "" }) }).ok, false);
  assert.equal(authorize({ grant: grant({ synthetic_image: false }) }).ok, false);
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

await test("nonce is consumed before the Provider and replay is rejected", () => {
  const authorized = authorize().grant;
  const runtime = new ProviderLayoutProductionQualificationGrantRuntime({ now: () => now });
  assert.equal(runtime.consumeBeforeProvider(authorized).ok, true);
  assert.equal(runtime.consumeBeforeProvider(authorized).reason, "PRODUCTION_QUALIFICATION_REPLAY_REJECTED");
});

await test("expired grants are rejected by the runtime", () => {
  const authorized = authorize().grant;
  const runtime = new ProviderLayoutProductionQualificationGrantRuntime({ now: () => now + 60_001 });
  assert.equal(runtime.consumeBeforeProvider(authorized).reason, "PRODUCTION_QUALIFICATION_EXPIRED");
});

await test("restart or serialized replay loses the process capability", () => {
  const persisted = JSON.parse(JSON.stringify(authorize().grant));
  const runtime = new ProviderLayoutProductionQualificationGrantRuntime({ now: () => now });
  assert.equal(runtime.consumeBeforeProvider(persisted).reason, "PRODUCTION_QUALIFICATION_CAPABILITY_MISSING");
});

await test("successful probe performs exactly one Provider call and emits only fixed status", async () => {
  let providerCalls = 0;
  const outcome = await executeProviderLayoutProductionQualificationProbe({
    grant: authorize().grant,
    runtime: new ProviderLayoutProductionQualificationGrantRuntime({ now: () => now }),
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

await test("Provider failure is never retried and returns fixed counters", async () => {
  let providerCalls = 0;
  const outcome = await executeProviderLayoutProductionQualificationProbe({
    grant: authorize().grant,
    runtime: new ProviderLayoutProductionQualificationGrantRuntime({ now: () => now }),
    providerCall: async () => {
      providerCalls += 1;
      throw new Error("sensitive Provider failure");
    },
    collectQualification: () => { throw new Error("must not collect"); }
  });
  assert.equal(providerCalls, 1);
  assert.equal(outcome.provider_call_count, 1);
  assert.equal(outcome.provider_completed, false);
  assert.equal(JSON.stringify(outcome).includes("sensitive"), false);
});

await test("failed Provider attempt consumes nonce and a second execution cannot call Provider", async () => {
  let providerCalls = 0;
  const runtime = new ProviderLayoutProductionQualificationGrantRuntime({ now: () => now });
  const authorized = authorize().grant;
  await executeProviderLayoutProductionQualificationProbe({
    grant: authorized,
    runtime,
    providerCall: async () => { providerCalls += 1; throw new Error("failed"); },
    collectQualification: () => ({ status: "RESPONSE_CONTRACT_UNSUPPORTED" })
  });
  const replay = await executeProviderLayoutProductionQualificationProbe({
    grant: authorized,
    runtime,
    providerCall: async () => { providerCalls += 1; },
    collectQualification: () => ({ status: "RESPONSE_CONTRACT_UNSUPPORTED" })
  });
  assert.equal(providerCalls, 1);
  assert.equal(replay.reason, "PRODUCTION_QUALIFICATION_REPLAY_REJECTED");
});

await test("non-fixed collector output is rejected without disclosure", async () => {
  const outcome = await executeProviderLayoutProductionQualificationProbe({
    grant: authorize().grant,
    runtime: new ProviderLayoutProductionQualificationGrantRuntime({ now: () => now }),
    providerCall: async () => ({ id: "synthetic-response" }),
    collectQualification: () => ({ status: "RAW_PROVIDER_RESPONSE", raw: forbidden })
  });
  assert.equal(outcome.ok, false);
  assert.equal(JSON.stringify(outcome).includes(forbidden), false);
});

await test("ordinary objects cannot invoke the Provider", async () => {
  let providerCalls = 0;
  const outcome = await executeProviderLayoutProductionQualificationProbe({
    grant: grant(),
    runtime: new ProviderLayoutProductionQualificationGrantRuntime({ now: () => now }),
    providerCall: async () => { providerCalls += 1; },
    collectQualification: () => ({ status: "RESPONSE_CONTRACT_UNSUPPORTED" })
  });
  assert.equal(outcome.ok, false);
  assert.equal(providerCalls, 0);
});

await test("fixed response contains no image Provider text bbox headers cookies keys or downstream authority", async () => {
  const outcome = await executeProviderLayoutProductionQualificationProbe({
    grant: authorize().grant,
    runtime: new ProviderLayoutProductionQualificationGrantRuntime({ now: () => now }),
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
