import { createHash, createPublicKey, verify as verifySignature } from "node:crypto";

export const PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_GRANT_SCHEMA_VERSION =
  "provider_layout_production_qualification_grant_v1";
export const PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_OUTPUT_POLICY =
  "FIXED_REDACTED_QUALIFICATION_STATUS_V1";
export const PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_TTL_MS = 5 * 60 * 1000;
export const PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_RESPONSE_CONTRACT = "UNSUPPORTED";

const PRODUCTION_QUALIFICATION_GRANT_CAPABILITY = Symbol(
  "PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_GRANT_CAPABILITY"
);
const GRANT_DIGEST = Symbol("PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_GRANT_DIGEST");
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
const GRANT_FIELDS = Object.freeze([
  "canonical_image_identity",
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
  now = Date.now(),
  verify = verifyProviderLayoutProductionQualificationGrantSignature
} = {}) {
  if (enabled !== true) return fixedFailure("PRODUCTION_QUALIFICATION_DISABLED");
  if (!text(publicKeyPem)) return fixedFailure("PRODUCTION_QUALIFICATION_PUBLIC_KEY_MISSING");
  if (!grant || typeof grant !== "object" || Array.isArray(grant)) {
    return fixedFailure("PRODUCTION_QUALIFICATION_GRANT_INVALID");
  }
  const canonicalImageIdentity = imageBinding(imageIdentity);
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
  const exactBindings = JSON.stringify(Object.keys(grant).sort()) === JSON.stringify(GRANT_FIELDS)
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
    && expiresAt - issuedAt <= PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_TTL_MS;
  if (!exactBindings) return fixedFailure("PRODUCTION_QUALIFICATION_BINDING_MISMATCH");
  normalized.canonical_image_identity = expectedImage;
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
  return Object.freeze({ ok: true, reason: null, grant: deepFreeze(normalized) });
}

export function hasProviderLayoutProductionQualificationGrantCapability(value) {
  return value?.[PRODUCTION_QUALIFICATION_GRANT_CAPABILITY] === true;
}

export class ProviderLayoutProductionQualificationGrantRuntime {
  constructor({ now = () => Date.now() } = {}) {
    this.now = now;
    this.consumedNonces = new Map();
  }

  consumeBeforeProvider(grant) {
    if (!hasProviderLayoutProductionQualificationGrantCapability(grant)) {
      return Object.freeze({ ok: false, reason: "PRODUCTION_QUALIFICATION_CAPABILITY_MISSING" });
    }
    for (const [key, expiresAt] of this.consumedNonces) {
      if (expiresAt <= this.now()) this.consumedNonces.delete(key);
    }
    if (grant.expires_at <= this.now()) {
      return Object.freeze({ ok: false, reason: "PRODUCTION_QUALIFICATION_EXPIRED" });
    }
    if (grant[GRANT_DIGEST] !== sha256(serializeProviderLayoutProductionQualificationGrant(grant))) {
      return Object.freeze({ ok: false, reason: "PRODUCTION_QUALIFICATION_BINDING_MISMATCH" });
    }
    const nonceDigest = sha256(Buffer.from(grant.nonce, "utf8"));
    if (this.consumedNonces.has(nonceDigest)) {
      return Object.freeze({ ok: false, reason: "PRODUCTION_QUALIFICATION_REPLAY_REJECTED" });
    }
    this.consumedNonces.set(nonceDigest, grant.expires_at);
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
