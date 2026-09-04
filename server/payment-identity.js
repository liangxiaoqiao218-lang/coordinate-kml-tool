import { createClient } from "@supabase/supabase-js";
import { createVerifiedPaymentIdentity } from "./payments/index.js";

export const PAYMENT_AUTH_ACCESS_COOKIE = "__Host-geokit_auth_access";
export const PAYMENT_AUTH_REFRESH_COOKIE = "__Host-geokit_auth_refresh";
export const PAYMENT_AUTH_MAX_ACCESS_SECONDS = 60 * 60;

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const SELF_ASSERTED_IDENTITY_FIELDS = new Set([
  "userId", "user_id", "accountId", "account_id", "visitorId", "visitor_id",
  "providerUserId", "provider_user_id", "email"
]);

export class PaymentIdentityError extends Error {
  constructor(code, status = 401) {
    super(code);
    this.name = "PaymentIdentityError";
    this.code = code;
    this.status = status;
  }
}

function fail(code, status) {
  throw new PaymentIdentityError(code, status);
}

function normalizeEmail(value) {
  const email = String(value || "").trim().toLowerCase();
  if (email.length > 254 || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) fail("AUTH_EMAIL_INVALID", 400);
  return email;
}

function normalizeOtp(value) {
  const token = String(value || "").trim();
  if (!/^\d{6,8}$/.test(token)) fail("AUTH_OTP_INVALID", 400);
  return token;
}

function decodeJwtPayload(token) {
  const parts = String(token || "").split(".");
  if (parts.length !== 3 || parts.some(part => !part)) fail("AUTH_TOKEN_MALFORMED", 401);
  try {
    return JSON.parse(Buffer.from(parts[1], "base64url").toString("utf8"));
  } catch (_) {
    return fail("AUTH_TOKEN_MALFORMED", 401);
  }
}

function validateAccessTokenEnvelope(token, nowMs) {
  const payload = decodeJwtPayload(token);
  const nowSeconds = Math.floor(nowMs / 1000);
  if (!Number.isFinite(payload.exp) || payload.exp <= nowSeconds) fail("AUTH_SESSION_EXPIRED", 401);
  if (!Number.isFinite(payload.iat) || payload.iat > nowSeconds + 60) fail("AUTH_TOKEN_MALFORMED", 401);
  if (payload.exp - payload.iat > PAYMENT_AUTH_MAX_ACCESS_SECONDS + 60) fail("AUTH_TOKEN_LIFETIME_INVALID", 401);
  return String(token);
}

function validateConfirmedEmailUser(user) {
  if (!user || !UUID_PATTERN.test(String(user.id || ""))) fail("AUTH_USER_INVALID", 401);
  if (user.is_anonymous === true) fail("AUTH_ANONYMOUS_FORBIDDEN", 401);
  if (!String(user.email || "").trim()) fail("AUTH_EMAIL_REQUIRED", 401);
  if (!user.email_confirmed_at) fail("AUTH_EMAIL_UNCONFIRMED", 401);
  const providers = Array.isArray(user.app_metadata?.providers) ? user.app_metadata.providers : [];
  const hasEmailIdentity = user.app_metadata?.provider === "email"
    || providers.includes("email")
    || (Array.isArray(user.identities) && user.identities.some(identity => identity?.provider === "email"));
  if (!hasEmailIdentity) fail("AUTH_EMAIL_IDENTITY_REQUIRED", 401);
  return user;
}

function assertNoSelfAssertedIdentity(req) {
  for (const source of [req?.body, req?.query]) {
    if (!source || typeof source !== "object") continue;
    const forbidden = Object.keys(source).filter(key => SELF_ASSERTED_IDENTITY_FIELDS.has(key));
    if (forbidden.length) fail("AUTH_SELF_ASSERTED_IDENTITY_FORBIDDEN", 400);
  }
}

export function parsePaymentAuthCookies(req) {
  const result = {};
  for (const part of String(req?.get?.("cookie") || req?.headers?.cookie || "").split(";")) {
    const separator = part.indexOf("=");
    if (separator < 0) continue;
    const name = part.slice(0, separator).trim();
    try {
      result[name] = decodeURIComponent(part.slice(separator + 1).trim());
    } catch (_) {
      result[name] = "";
    }
  }
  return result;
}

function appendCookie(res, value) {
  if (typeof res.append === "function") res.append("Set-Cookie", value);
  else {
    const existing = res.getHeader?.("Set-Cookie") || [];
    res.setHeader("Set-Cookie", [...(Array.isArray(existing) ? existing : [existing]), value]);
  }
}

export function writePaymentAuthCookies(res, session) {
  const accessMaxAge = Math.min(PAYMENT_AUTH_MAX_ACCESS_SECONDS, Math.max(1, Number(session.expires_in) || PAYMENT_AUTH_MAX_ACCESS_SECONDS));
  appendCookie(res, `${PAYMENT_AUTH_ACCESS_COOKIE}=${encodeURIComponent(session.access_token)}; Path=/; Max-Age=${accessMaxAge}; HttpOnly; Secure; SameSite=Lax`);
  appendCookie(res, `${PAYMENT_AUTH_REFRESH_COOKIE}=${encodeURIComponent(session.refresh_token)}; Path=/; Max-Age=2592000; HttpOnly; Secure; SameSite=Lax`);
}

export function clearPaymentAuthCookies(res) {
  const expired = "Path=/; Max-Age=0; Expires=Thu, 01 Jan 1970 00:00:00 GMT; HttpOnly; Secure; SameSite=Lax";
  appendCookie(res, `${PAYMENT_AUTH_ACCESS_COOKIE}=; ${expired}`);
  appendCookie(res, `${PAYMENT_AUTH_REFRESH_COOKIE}=; ${expired}`);
}

export function isAllowedPaymentAuthMutationOrigin({ origin = "", referer = "", allowedOrigin = "" } = {}) {
  if (!allowedOrigin) return false;
  const sources = [origin, referer].filter(Boolean);
  if (!sources.length) return false;
  try {
    return sources.every(source => new URL(source).origin === allowedOrigin);
  } catch (_) {
    return false;
  }
}

function publicUser(user) {
  return Object.freeze({ id: user.id, email: user.email });
}

function assertSession(session) {
  if (!session?.access_token || !session?.refresh_token) fail("AUTH_SESSION_INVALID", 401);
  return session;
}

export function createPaymentIdentityService({
  supabaseUrl,
  publishableKey,
  clientFactory = createClient,
  now = () => Date.now()
} = {}) {
  const configured = Boolean(String(supabaseUrl || "").trim() && String(publishableKey || "").trim());

  function authClient() {
    if (!configured) fail("AUTH_NOT_CONFIGURED", 503);
    return clientFactory(supabaseUrl, publishableKey, {
      auth: { autoRefreshToken: false, persistSession: false, detectSessionInUrl: false }
    });
  }

  async function verifyAccessToken(accessToken) {
    const token = validateAccessTokenEnvelope(accessToken, now());
    let result;
    try {
      result = await authClient().auth.getUser(token);
    } catch (_) {
      fail("AUTH_PROVIDER_UNAVAILABLE", 503);
    }
    if (result?.error || !result?.data?.user) fail("AUTH_TOKEN_INVALID", 401);
    const user = validateConfirmedEmailUser(result.data.user);
    const identity = createVerifiedPaymentIdentity({ userId: user.id, verification: "authenticated_session" });
    return Object.freeze({ identity, user: publicUser(user) });
  }

  async function requestOtp(emailInput) {
    const email = normalizeEmail(emailInput);
    let result;
    try {
      result = await authClient().auth.signInWithOtp({ email, options: { shouldCreateUser: true } });
    } catch (_) {
      fail("AUTH_PROVIDER_UNAVAILABLE", 503);
    }
    if (result?.error) fail("AUTH_PROVIDER_UNAVAILABLE", 503);
    return Object.freeze({ accepted: true });
  }

  async function verifyOtp(emailInput, otpInput) {
    const email = normalizeEmail(emailInput);
    const token = normalizeOtp(otpInput);
    let result;
    try {
      result = await authClient().auth.verifyOtp({ email, token, type: "email" });
    } catch (_) {
      fail("AUTH_PROVIDER_UNAVAILABLE", 503);
    }
    if (result?.error) fail("AUTH_OTP_INVALID", 401);
    const session = assertSession(result?.data?.session);
    const authenticated = await verifyAccessToken(session.access_token);
    if (result?.data?.user?.id && result.data.user.id !== authenticated.user.id) fail("AUTH_IDENTITY_MISMATCH", 401);
    return Object.freeze({ session, ...authenticated });
  }

  async function refresh(refreshToken) {
    const oldToken = String(refreshToken || "").trim();
    if (!oldToken || /\s/.test(oldToken)) fail("AUTH_REFRESH_MISSING", 401);
    let result;
    try {
      result = await authClient().auth.refreshSession({ refresh_token: oldToken });
    } catch (_) {
      fail("AUTH_PROVIDER_UNAVAILABLE", 503);
    }
    if (result?.error) fail("AUTH_SESSION_EXPIRED", 401);
    const session = assertSession(result?.data?.session);
    if (session.refresh_token === oldToken) fail("AUTH_REFRESH_NOT_ROTATED", 401);
    const authenticated = await verifyAccessToken(session.access_token);
    return Object.freeze({ session, ...authenticated });
  }

  async function authenticateRequest(req) {
    assertNoSelfAssertedIdentity(req);
    const accessToken = parsePaymentAuthCookies(req)[PAYMENT_AUTH_ACCESS_COOKIE];
    if (!accessToken) fail("AUTH_SESSION_MISSING", 401);
    return verifyAccessToken(accessToken);
  }

  async function signOut(accessToken, refreshToken) {
    if (!accessToken || !refreshToken || !configured) return;
    try {
      const client = authClient();
      if (typeof client.auth.setSession === "function") {
        const result = await client.auth.setSession({ access_token: accessToken, refresh_token: refreshToken });
        if (result?.error) return;
      }
      if (typeof client.auth.signOut === "function") await client.auth.signOut({ scope: "local" });
    } catch (_) {
      // Local cookies are still cleared. No immediate stolen-token revocation claim is made here.
    }
  }

  return Object.freeze({ authenticateRequest, refresh, requestOtp, signOut, verifyAccessToken, verifyOtp });
}

export function createPaymentIdentityServiceFromEnv(env = process.env, options = {}) {
  return createPaymentIdentityService({
    supabaseUrl: env.SUPABASE_URL,
    publishableKey: env.SUPABASE_PUBLISHABLE_KEY || env.SUPABASE_ANON_KEY,
    ...options
  });
}

function noStore(res) {
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Vary", "Cookie");
}

function sendError(res, error) {
  const status = error instanceof PaymentIdentityError ? error.status : 500;
  const code = error instanceof PaymentIdentityError ? error.code : "AUTH_INTERNAL_ERROR";
  return res.status(status).json({ success: false, code });
}

export function createPaymentAuthHandlers(service) {
  return Object.freeze({
    requestOtp: async (req, res) => {
      noStore(res);
      try {
        await service.requestOtp(req.body?.email);
        return res.status(202).json({ success: true, accepted: true });
      } catch (error) {
        return sendError(res, error);
      }
    },
    verifyOtp: async (req, res) => {
      noStore(res);
      try {
        const result = await service.verifyOtp(req.body?.email, req.body?.token);
        writePaymentAuthCookies(res, result.session);
        return res.json({ success: true, state: "authenticated", user: result.user });
      } catch (error) {
        clearPaymentAuthCookies(res);
        return sendError(res, error);
      }
    },
    refresh: async (req, res) => {
      noStore(res);
      try {
        const cookies = parsePaymentAuthCookies(req);
        const result = await service.refresh(cookies[PAYMENT_AUTH_REFRESH_COOKIE]);
        writePaymentAuthCookies(res, result.session);
        return res.json({ success: true, state: "authenticated", user: result.user });
      } catch (error) {
        clearPaymentAuthCookies(res);
        return sendError(res, error);
      }
    },
    logout: async (req, res) => {
      noStore(res);
      const cookies = parsePaymentAuthCookies(req);
      try {
        await service.signOut(cookies[PAYMENT_AUTH_ACCESS_COOKIE], cookies[PAYMENT_AUTH_REFRESH_COOKIE]);
      } catch (_) {
        // Logout remains fail-closed locally even when the provider is unavailable.
      } finally {
        clearPaymentAuthCookies(res);
      }
      return res.json({ success: true, state: "signed_out" });
    },
    me: async (req, res) => {
      noStore(res);
      try {
        const result = await service.authenticateRequest(req);
        return res.json({ success: true, state: "authenticated", user: result.user });
      } catch (error) {
        if (error instanceof PaymentIdentityError && error.code === "AUTH_SESSION_MISSING") {
          return res.json({ success: true, state: "signed_out", user: null });
        }
        return sendError(res, error);
      }
    }
  });
}

export function requireAuthenticatedPaymentUser(service) {
  return async function authenticatedPaymentUser(req, res, next) {
    try {
      const result = await service.authenticateRequest(req);
      req.paymentIdentity = result.identity;
      req.authenticatedPaymentUser = result.user;
      return next();
    } catch (error) {
      return sendError(res, error);
    }
  };
}
