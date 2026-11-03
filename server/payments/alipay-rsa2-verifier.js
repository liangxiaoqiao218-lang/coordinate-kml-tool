import { constants, createPublicKey, verify } from "node:crypto";
import { TextDecoder } from "node:util";
import { PAYMENT_ERROR_CODES, paymentContractAssert } from "./errors.js";

function requireNotification(condition, message) {
  paymentContractAssert(condition, PAYMENT_ERROR_CODES.WEBHOOK_INVALID, message);
}

export function parseAlipayNotificationForm(rawBody) {
  requireNotification(typeof rawBody === "string" || rawBody instanceof Uint8Array, "notification body required");
  const bytes = typeof rawBody === "string" ? Buffer.from(rawBody, "utf8") : Buffer.from(rawBody);
  requireNotification(bytes.length > 0 && bytes.length <= 65536, "notification body size invalid");
  let text;
  try { text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes); }
  catch { requireNotification(false, "notification body must be valid UTF-8"); }
  requireNotification(!text.startsWith("\uFEFF"), "notification BOM rejected");
  const pairs = text.split("&");
  requireNotification(pairs.length <= 128, "too many notification fields");
  const fields = Object.create(null);
  for (const pair of pairs) {
    const separator = pair.indexOf("=");
    requireNotification(separator > 0, "notification field format invalid");
    let key;
    let value;
    try {
      key = decodeURIComponent(pair.slice(0, separator).replace(/\+/g, " "));
      value = decodeURIComponent(pair.slice(separator + 1).replace(/\+/g, " "));
    } catch { requireNotification(false, "notification form encoding invalid"); }
    requireNotification(/^[A-Za-z][A-Za-z0-9_]*$/.test(key), "notification field name invalid");
    requireNotification(!Object.hasOwn(fields, key), "duplicate notification field");
    requireNotification(!/[\u0000-\u001F\u007F]/.test(value), "notification control character rejected");
    fields[key] = value;
  }
  requireNotification(fields.sign_type === "RSA2", "only RSA2 notifications accepted");
  requireNotification(fields.charset?.toLowerCase() === "utf-8", "only UTF-8 notifications accepted");
  requireNotification(typeof fields.sign === "string" && fields.sign.length > 0, "notification signature required");
  return Object.freeze(fields);
}

export function canonicalizeAlipayNotification(fields) {
  return Object.keys(fields).filter(key => key !== "sign" && key !== "sign_type" && fields[key] !== "")
    .sort().map(key => `${key}=${fields[key]}`).join("&");
}

// The caller supplies a trusted public key. Notification fields never select a key.
export function createAlipayRsa2Verifier({ trustedPublicKey } = {}) {
  const publicKey = createPublicKey(trustedPublicKey);
  requireNotification(publicKey.asymmetricKeyType === "rsa", "RSA public key required");
  const modulusLength = publicKey.asymmetricKeyDetails?.modulusLength;
  requireNotification(Number.isSafeInteger(modulusLength) && modulusLength >= 2048, "RSA key must be at least 2048 bits");
  const signatureBytes = Math.ceil(modulusLength / 8);
  return function verifyAlipayNotification({ headers, rawBody } = {}) {
    const contentTypes = Object.entries(headers || {}).filter(([key]) => key.toLowerCase() === "content-type");
    requireNotification(contentTypes.length === 1, "one notification content type required");
    const contentType = String(contentTypes[0][1]);
    requireNotification(/^application\/x-www-form-urlencoded(?:\s*;\s*charset=utf-8)?\s*$/i.test(contentType), "notification content type rejected");
    const fields = parseAlipayNotificationForm(rawBody);
    requireNotification(/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(fields.sign), "signature base64 invalid");
    const signature = Buffer.from(fields.sign, "base64");
    requireNotification(signature.length === signatureBytes && signature.toString("base64") === fields.sign, "signature length or encoding invalid");
    return verify("RSA-SHA256", Buffer.from(canonicalizeAlipayNotification(fields), "utf8"), {
      key: publicKey,
      padding: constants.RSA_PKCS1_PADDING
    }, signature);
  };
}
