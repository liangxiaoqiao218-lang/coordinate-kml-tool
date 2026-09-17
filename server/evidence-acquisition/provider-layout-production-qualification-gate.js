import { createHash, createPublicKey, randomBytes, verify as verifySignature } from "node:crypto";

export const PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_GRANT_SCHEMA_VERSION =
  "provider_layout_production_qualification_grant_v2";
export const PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_CHALLENGE_SCHEMA_VERSION =
  "provider_layout_production_qualification_challenge_v1";
export const PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_OUTPUT_POLICY =
  "FIXED_REDACTED_QUALIFICATION_STATUS_V1";
export const PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_TTL_MS = 5 * 60 * 1000;
export const PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_RESPONSE_CONTRACT = "UNSUPPORTED";

const PRODUCTION_QUALIFICATION_GRANT_CAPABILITY = Symbol(
  "PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_GRANT_CAPABILITY"
);
const PRODUCTION_QUALIFICATION_CHALLENGE_CAPABILITY = Symbol(
  "PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_CHALLENGE_CAPABILITY"
);
const GRANT_DIGEST = Symbol("PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_GRANT_DIGEST");
const GRANT_BOOT_BINDING = Symbol("PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_GRANT_BOOT_BINDING");
const CHALLENGE_DIGEST = Symbol("PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_CHALLENGE_DIGEST");
const HEX_40 = /^[0-9a-f]{40}$/;
const HEX_64 = /^[0-9a-f]{64}$/;
const REQUEST_ID = /^[0-9a-f-]{16,128}$/;
const TOKEN = /^[A-Za-z0-9._:-]{8,256}$/;
const FIXED_QUALIFICATION_STATUSES = new Set([
  "NO_STRUCTURED_LAYOUT",
  "RESPONSE_CONTRACT_UNSUPPORTED",
  "COORDINATE_SPACE_UNPROVEN",
  "QUALIFICATION_CANDIDATE",
  "QUALIFICATION_CONFLICT"
]);
const CHALLENGE_FIELDS = Object.freeze([
  "boot_identity",
  "challenge_id",
  "expires_at",
  "issued_at",
  "runtime_branch",
  "runtime_commit",
  "schema_version",
  "service"
]);
const GRANT_FIELDS = Object.freeze([
  "canonical_image_identity",
  "challenge",
  "expires_at",
  "issued_at",
  "max_provider_calls",
  "model_family",
  "model_name",
  "nonce",
  "output_policy",
  "provider_id",
  "request_id",
  "response_contract_id",
  "runtime_branch",
  "runtime_commit",
  "schema_version",
  "service",
  "synthetic_image"
]);

function text(value) {
  return String(value ?? "").trim();
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function stableValue(value) {
  if (Array.isArray(value)) return value.map(stableValue);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.keys(value).sort().map(key => [key, stableValue(value[key])]));
}

function deepFreeze(value) {
  if (!value || typeof value !== "object" || Object.isFrozen(value)) return value;
  Object.values(value).forEach(deepFreeze);
  return Object.freeze(value);
}

function hasExactFields(value, fields) {
  return value
    && typeof value === "object"
    && !Array.isArray(value)
    && JSON.stringify(Object.keys(value).sort()) === JSON.stringify(fields);
}

function randomIdentity() {
  return randomBytes(32).toString("hex");
}

export function serializeProviderLayoutProductionQualificationGrant(value) {
  return Buffer.from(JSON.stringify(stableValue(value)), "utf8");
}

function imageBinding(imageIdentity) {
  if (!imageIdentity || typeof imageIdentity !== "object") return null;
  const binding = {
    schema_version: text(imageIdentity.schema_version),
    image_sha256: text(imageIdentity.image_sha256).toLowerCase(),
    byte_length: Number(imageIdentity.byte_length),
    mime_type: text(imageIdentity.mime_type).toLowerCase(),
    width: Number(imageIdentity.width),
    height: Number(imageIdentity.height),
    page: Number(imageIdentity.page),
    image_id: text(imageIdentity.image_id),
    request_asset_id: text(imageIdentity.request_asset_id)
  };
  return HEX_64.test(binding.image_sha256)
    && Number.isSafeInteger(binding.byte_length) && binding.byte_length > 0
    && Number.isSafeInteger(binding.width) && binding.width > 0
    && Number.isSafeInteger(binding.height) && binding.height > 0
    && Number.isSafeInteger(binding.page) && binding.page > 0
    && binding.schema_version && binding.mime_type && binding.image_id && binding.request_asset_id
    ? binding
    : null;
}

function challengeBinding(challenge) {
  if (!hasExactFields(challenge, CHALLENGE_FIELDS)) return null;
  const binding = {
    schema_version: text(challenge.schema_version),
    service: text(challenge.service),
    runtime_commit: text(challenge.runtime_commit).toLowerCase(),
    runtime_branch: text(challenge.runtime_branch),
    boot_identity: text(challenge.boot_identity).toLowerCase(),
    challenge_id: text(challenge.challenge_id).toLowerCase(),
    issued_at: Number(challenge.issued_at),
    expires_at: Number(challenge.expires_at)
  };
  return binding.schema_version === PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_CHALLENGE_SCHEMA_VERSION
    && binding.service
    && HEX_40.test(binding.runtime_commit)
    && binding.runtime_branch
    && HEX_64.test(binding.boot_identity)
    && HEX_64.test(binding.challenge_id)
    && Number.isSafeInteger(binding.issued_at)
    && Number.isSafeInteger(binding.expires_at)
    && binding.expires_at > binding.issued_at
    && binding.expires_at - binding.issued_at <= PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_TTL_MS
    ? binding
    : null;
}

function grantIdentity(grant) {
  return {
    schema_version: grant.schema_version,
    service: grant.service,
    runtime_commit: grant.runtime_commit,
    runtime_branch: grant.runtime_branch,
    request_id: grant.request_id,
    nonce: grant.nonce,
    synthetic_image: grant.synthetic_image,
    canonical_image_identity: grant.canonical_image_identity,
    challenge: grant.challenge,
    provider_id: grant.provider_id,
    model_family: grant.model_family,
    model_name: grant.model_name,
    response_contract_id: grant.response_contract_id,
    max_provider_calls: grant.max_provider_calls,
    issued_at: grant.issued_at,
    expires_at: grant.expires_at,
    output_policy: grant.output_policy
  };
}

function fixedFailure(reason) {
  return Object.freeze({ ok: false, reason, grant: null });
}

function challengeFailure(reason) {
  return Object.freeze({ ok: false, reason, challenge: null });
}

export function verifyProviderLayoutProductionQualificationGrantSignature({
  grant,
  signatureBase64Url = "",
  publicKeyPem = ""
} = {}) {
  if (!grant || typeof grant !== "object" || Array.isArray(grant)) return false;
  const signatureText = text(signatureBase64Url);
  const publicKey = text(publicKeyPem);
  if (!signatureText || !publicKey) return false;
  return verifyEd25519DetachedSignature({
    message: serializeProviderLayoutProductionQualificationGrant(grant),
    signatureBase64Url: signatureText,
    publicKeyPem: publicKey
  });
}

export function verifyEd25519DetachedSignature({
  message,
  signatureBase64Url = "",
  publicKeyPem = ""
} = {}) {
  const signatureText = text(signatureBase64Url);
  const publicKey = text(publicKeyPem);
  if ((!Buffer.isBuffer(message) && typeof message !== "string") || !signatureText || !publicKey) return false;
  if (!/^-----BEGIN PUBLIC KEY-----\r?\n[\s\S]+\r?\n-----END PUBLIC KEY-----$/.test(publicKey)) return false;
  try {
    const signature = Buffer.from(signatureText, "base64url");
    const key = createPublicKey(publicKey);
    return signature.length === 64
      && key.type === "public"
      && key.asymmetricKeyType === "ed25519"
      && verifySignature(null, message, key, signature);
  } catch {
    return false;
  }
}

export function authorizeProviderLayoutProductionQualificationGrant({
  enabled = false,
  publicKeyPem = "",
  grant,
  signatureBase64Url = "",
  service = "",
  runtimeCommit = "",
  runtimeBranch = "",
  requestId = "",
  imageIdentity,
  providerId = "",
  modelFamily = "",
  modelName = "",
  responseContractId = PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_RESPONSE_CONTRACT,
  runtime,
  now = Date.now(),
  verify = verifyProviderLayoutProductionQualificationGrantSignature
} = {}) {
  if (enabled !== true) return fixedFailure("PRODUCTION_QUALIFICATION_DISABLED");
  if (!text(publicKeyPem)) return fixedFailure("PRODUCTION_QUALIFICATION_PUBLIC_KEY_MISSING");
  if (!grant || typeof grant !== "object" || Array.isArray(grant)) {
    return fixedFailure("PRODUCTION_QUALIFICATION_GRANT_INVALID");
  }
  if (!(runtime instanceof ProviderLayoutProductionQualificationGrantRuntime)) {
    return fixedFailure("PRODUCTION_QUALIFICATION_RUNTIME_MISSING");
  }
  const canonicalImageIdentity = imageBinding(imageIdentity);
  const normalizedChallenge = challengeBinding(grant.challenge);
  const normalized = {
    ...grantIdentity(grant),
    schema_version: text(grant.schema_version),
    service: text(grant.service),
    runtime_commit: text(grant.runtime_commit).toLowerCase(),
    runtime_branch: text(grant.runtime_branch),
    request_id: text(grant.request_id).toLowerCase(),
    nonce: text(grant.nonce),
    synthetic_image: grant.synthetic_image === true,
    canonical_image_identity: grant.canonical_image_identity,
    challenge: normalizedChallenge,
    provider_id: text(grant.provider_id).toUpperCase(),
    model_family: text(grant.model_family).toUpperCase(),
    model_name: text(grant.model_name),
    response_contract_id: text(grant.response_contract_id),
    max_provider_calls: Number(grant.max_provider_calls),
    issued_at: Number(grant.issued_at),
    expires_at: Number(grant.expires_at),
    output_policy: text(grant.output_policy)
  };
  const expectedImage = imageBinding(normalized.canonical_image_identity);
  const issuedAt = normalized.issued_at;
  const expiresAt = normalized.expires_at;
  const exactBindings = hasExactFields(grant, GRANT_FIELDS)
    && normalized.schema_version === PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_GRANT_SCHEMA_VERSION
    && normalized.service === text(service)
    && normalized.runtime_commit === text(runtimeCommit).toLowerCase()
    && normalized.runtime_branch === text(runtimeBranch)
    && normalized.request_id === text(requestId).toLowerCase()
    && REQUEST_ID.test(normalized.request_id)
    && TOKEN.test(normalized.nonce)
    && normalized.synthetic_image === true
    && canonicalImageIdentity && expectedImage
    && serializeProviderLayoutProductionQualificationGrant(grant.canonical_image_identity)
      .equals(serializeProviderLayoutProductionQualificationGrant(expectedImage))
    && serializeProviderLayoutProductionQualificationGrant(expectedImage)
      .equals(serializeProviderLayoutProductionQualificationGrant(canonicalImageIdentity))
    && normalizedChallenge
    && normalized.provider_id === text(providerId).toUpperCase()
    && normalized.model_family === text(modelFamily).toUpperCase()
    && normalized.model_name === text(modelName)
    && normalized.response_contract_id === text(responseContractId)
    && normalized.response_contract_id === PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_RESPONSE_CONTRACT
    && normalized.max_provider_calls === 1
    && normalized.output_policy === PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_OUTPUT_POLICY
    && Number.isSafeInteger(issuedAt) && Number.isSafeInteger(expiresAt)
    && issuedAt <= now && expiresAt > now
    && expiresAt - issuedAt > 0
    && expiresAt - issuedAt <= PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_TTL_MS
    && normalizedChallenge.issued_at <= issuedAt
    && expiresAt <= normalizedChallenge.expires_at;
  if (!exactBindings) return fixedFailure("PRODUCTION_QUALIFICATION_BINDING_MISMATCH");
  const challenge = runtime.validateChallengeBinding(normalizedChallenge, {
    service,
    runtimeCommit,
    runtimeBranch
  });
  if (!challenge.ok) return fixedFailure(challenge.reason);
  normalized.canonical_image_identity = expectedImage;
  normalized.challenge = challenge.challenge;
  if (!verify({ grant: normalized, signatureBase64Url, publicKeyPem })) {
    return fixedFailure("PRODUCTION_QUALIFICATION_SIGNATURE_INVALID");
  }
  Object.defineProperty(normalized, PRODUCTION_QUALIFICATION_GRANT_CAPABILITY, {
    value: true,
    enumerable: false
  });
  Object.defineProperty(normalized, GRANT_DIGEST, {
    value: sha256(serializeProviderLayoutProductionQualificationGrant(normalized)),
    enumerable: false
  });
  Object.defineProperty(normalized, GRANT_BOOT_BINDING, {
    value: runtime.bootIdentity,
    enumerable: false
  });
  return Object.freeze({ ok: true, reason: null, grant: deepFreeze(normalized) });
}

export function hasProviderLayoutProductionQualificationGrantCapability(value) {
  return value?.[PRODUCTION_QUALIFICATION_GRANT_CAPABILITY] === true;
}

export class ProviderLayoutProductionQualificationGrantRuntime {
  constructor({
    now = () => Date.now(),
    bootIdentity = "",
    challengeIdFactory = randomIdentity
  } = {}) {
    this.now = now;
    this.bootIdentity = text(bootIdentity).toLowerCase() || randomIdentity();
    if (!HEX_64.test(this.bootIdentity)) throw new Error("PRODUCTION_QUALIFICATION_BOOT_IDENTITY_INVALID");
    this.challengeIdFactory = challengeIdFactory;
    this.challengeIssued = false;
    this.challengeRecord = null;
  }

  issueChallenge({
    service = "",
    runtimeCommit = "",
    runtimeBranch = "",
    ttlMs = PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_TTL_MS
  } = {}) {
    const normalizedService = text(service);
    const normalizedCommit = text(runtimeCommit).toLowerCase();
    const normalizedBranch = text(runtimeBranch);
    const normalizedTtl = Number(ttlMs);
    if (!normalizedService || !HEX_40.test(normalizedCommit) || !normalizedBranch
      || !Number.isSafeInteger(normalizedTtl) || normalizedTtl <= 0
      || normalizedTtl > PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_TTL_MS) {
      return challengeFailure("PRODUCTION_QUALIFICATION_CHALLENGE_BINDING_INVALID");
    }
    if (this.challengeIssued) {
      if (!this.challengeRecord?.[PRODUCTION_QUALIFICATION_CHALLENGE_CAPABILITY]) {
        return challengeFailure("PRODUCTION_QUALIFICATION_CHALLENGE_CAPABILITY_MISSING");
      }
      if (this.challengeRecord.consumed) {
        return challengeFailure("PRODUCTION_QUALIFICATION_CHALLENGE_ALREADY_CONSUMED");
      }
      if (this.challengeRecord.challenge.expires_at <= this.now()) {
        return challengeFailure("PRODUCTION_QUALIFICATION_CHALLENGE_EXPIRED");
      }
      const existing = this.challengeRecord.challenge;
      if (existing.service !== normalizedService
        || existing.runtime_commit !== normalizedCommit
        || existing.runtime_branch !== normalizedBranch) {
        return challengeFailure("PRODUCTION_QUALIFICATION_CHALLENGE_BINDING_MISMATCH");
      }
      return Object.freeze({ ok: true, reason: null, challenge: existing });
    }
    const issuedAt = this.now();
    const challengeId = text(this.challengeIdFactory()).toLowerCase();
    if (!HEX_64.test(challengeId)) {
      return challengeFailure("PRODUCTION_QUALIFICATION_CHALLENGE_ID_INVALID");
    }
    const challenge = deepFreeze({
      schema_version: PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_CHALLENGE_SCHEMA_VERSION,
      service: normalizedService,
      runtime_commit: normalizedCommit,
      runtime_branch: normalizedBranch,
      boot_identity: this.bootIdentity,
      challenge_id: challengeId,
      issued_at: issuedAt,
      expires_at: issuedAt + normalizedTtl
    });
    const record = { challenge, consumed: false };
    Object.defineProperty(record, PRODUCTION_QUALIFICATION_CHALLENGE_CAPABILITY, {
      value: true,
      enumerable: false
    });
    Object.defineProperty(record, CHALLENGE_DIGEST, {
      value: sha256(serializeProviderLayoutProductionQualificationGrant(challenge)),
      enumerable: false
    });
    this.challengeIssued = true;
    this.challengeRecord = record;
    return Object.freeze({ ok: true, reason: null, challenge });
  }

  validateChallengeBinding(challenge, {
    service = "",
    runtimeCommit = "",
    runtimeBranch = ""
  } = {}) {
    const record = this.challengeRecord;
    if (!record?.[PRODUCTION_QUALIFICATION_CHALLENGE_CAPABILITY]) {
      return Object.freeze({ ok: false, reason: "PRODUCTION_QUALIFICATION_CHALLENGE_MISSING" });
    }
    if (record.consumed) {
      return Object.freeze({ ok: false, reason: "PRODUCTION_QUALIFICATION_CHALLENGE_ALREADY_CONSUMED" });
    }
    if (record.challenge.expires_at <= this.now()) {
      return Object.freeze({ ok: false, reason: "PRODUCTION_QUALIFICATION_CHALLENGE_EXPIRED" });
    }
    const normalized = challengeBinding(challenge);
    if (!normalized
      || normalized.expires_at <= this.now()
      || normalized.boot_identity !== this.bootIdentity
      || normalized.service !== text(service)
      || normalized.runtime_commit !== text(runtimeCommit).toLowerCase()
      || normalized.runtime_branch !== text(runtimeBranch)
      || sha256(serializeProviderLayoutProductionQualificationGrant(normalized)) !== record[CHALLENGE_DIGEST]) {
      return Object.freeze({ ok: false, reason: "PRODUCTION_QUALIFICATION_CHALLENGE_BINDING_MISMATCH" });
    }
    return Object.freeze({ ok: true, reason: null, challenge: record.challenge });
  }

  consumeBeforeProvider(grant) {
    if (!hasProviderLayoutProductionQualificationGrantCapability(grant)
      || grant?.[GRANT_BOOT_BINDING] !== this.bootIdentity) {
      return Object.freeze({ ok: false, reason: "PRODUCTION_QUALIFICATION_CAPABILITY_MISSING" });
    }
    if (grant.expires_at <= this.now()) {
      return Object.freeze({ ok: false, reason: "PRODUCTION_QUALIFICATION_EXPIRED" });
    }
    if (grant[GRANT_DIGEST] !== sha256(serializeProviderLayoutProductionQualificationGrant(grant))) {
      return Object.freeze({ ok: false, reason: "PRODUCTION_QUALIFICATION_BINDING_MISMATCH" });
    }
    const challenge = this.validateChallengeBinding(grant.challenge, {
      service: grant.service,
      runtimeCommit: grant.runtime_commit,
      runtimeBranch: grant.runtime_branch
    });
    if (!challenge.ok) return challenge;
    this.challengeRecord.consumed = true;
    return Object.freeze({ ok: true, reason: null, grantDigest: grant[GRANT_DIGEST] });
  }
}

export async function executeProviderLayoutProductionQualificationProbe({
  grant,
  runtime,
  providerCall,
  collectQualification
} = {}) {
  const consumed = runtime?.consumeBeforeProvider(grant);
  if (!consumed?.ok) return Object.freeze({ ok: false, reason: consumed?.reason || "PRODUCTION_QUALIFICATION_RUNTIME_MISSING" });
  let response;
  try {
    response = await providerCall();
  } catch {
    return Object.freeze({
      ok: false,
      provider_attempted: true,
      provider_completed: false,
      provider_call_count: 1
    });
  }
  let status = "";
  try {
    status = text(collectQualification(response)?.status);
  } catch {
    return Object.freeze({
      ok: false,
      provider_attempted: true,
      provider_completed: true,
      provider_call_count: 1
    });
  } finally {
    response = null;
  }
  if (!FIXED_QUALIFICATION_STATUSES.has(status)) {
    return Object.freeze({
      ok: false,
      provider_attempted: true,
      provider_completed: true,
      provider_call_count: 1
    });
  }
  return Object.freeze({
    ok: true,
    status,
    provider_attempted: true,
    provider_completed: true,
    provider_call_count: 1
  });
}
