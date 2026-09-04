import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { assertVerifiedPaymentIdentity } from "../server/payments/index.js";
import {
  PAYMENT_AUTH_ACCESS_COOKIE,
  PAYMENT_AUTH_REFRESH_COOKIE,
  PaymentIdentityError,
  clearPaymentAuthCookies,
  createPaymentAuthHandlers,
  createPaymentIdentityService,
  isAllowedPaymentAuthMutationOrigin,
  writePaymentAuthCookies
} from "../server/payment-identity.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const serverSource = fs.readFileSync(path.join(root, "server.js"), "utf8");
const identitySource = fs.readFileSync(path.join(root, "server/payment-identity.js"), "utf8");
const clientSource = fs.readFileSync(path.join(root, "assets/payment-auth.js"), "utf8");
const indexSource = fs.readFileSync(path.join(root, "index.html"), "utf8");
const cases = [];

async function test(name, fn) {
  try {
    await fn();
    cases.push({ name, pass: true });
  } catch (error) {
    cases.push({ name, pass: false, error: error?.stack || String(error) });
  }
}

function jwt({ iat = Math.floor(Date.now() / 1000), exp = iat + 1800 } = {}) {
  const encode = value => Buffer.from(JSON.stringify(value)).toString("base64url");
  return `${encode({ alg: "RS256", typ: "JWT" })}.${encode({ iat, exp, sub: "server-revalidated" })}.signature`;
}

function confirmedUser(overrides = {}) {
  return {
    id: "44e7c266-b086-4e2d-a492-fb41bc528ebe",
    email: "person@example.com",
    email_confirmed_at: "2026-09-04T00:00:00.000Z",
    is_anonymous: false,
    app_metadata: { provider: "email", providers: ["email"] },
    identities: [{ provider: "email" }],
    ...overrides
  };
}

function makeService(behavior = {}) {
  let clientCount = 0;
  const user = behavior.user || confirmedUser();
  const accessToken = behavior.accessToken || jwt();
  const session = behavior.session || { access_token: accessToken, refresh_token: "rotated-refresh", expires_in: 1800 };
  const service = createPaymentIdentityService({
    supabaseUrl: "https://example.supabase.co",
    publishableKey: "sb_publishable_test",
    clientFactory: (_url, _key, options) => {
      clientCount += 1;
      assert.equal(options.auth.persistSession, false);
      assert.equal(options.auth.autoRefreshToken, false);
      assert.equal(options.auth.detectSessionInUrl, false);
      return {
        auth: {
          getUser: async token => behavior.getUser ? behavior.getUser(token) : ({ data: { user }, error: null }),
          signInWithOtp: async payload => behavior.signInWithOtp ? behavior.signInWithOtp(payload) : ({ data: {}, error: null }),
          verifyOtp: async payload => behavior.verifyOtp ? behavior.verifyOtp(payload) : ({ data: { user, session }, error: null }),
          refreshSession: async payload => behavior.refreshSession ? behavior.refreshSession(payload) : ({ data: { user, session }, error: null }),
          setSession: async () => ({ data: {}, error: null }),
          signOut: async () => ({ error: null })
        }
      };
    },
    now: () => Date.now()
  });
  return { service, accessToken, getClientCount: () => clientCount };
}

function requestWithCookie(token, extra = {}) {
  return {
    body: {}, query: {}, headers: {},
    get(name) { return name.toLowerCase() === "cookie" ? `${PAYMENT_AUTH_ACCESS_COOKIE}=${encodeURIComponent(token)}` : ""; },
    ...extra
  };
}

async function expectCode(promise, code) {
  await assert.rejects(promise, error => error instanceof PaymentIdentityError && error.code === code);
}

function mockResponse() {
  const headers = new Map();
  return {
    statusCode: 200, body: null,
    setHeader(name, value) { headers.set(name.toLowerCase(), value); },
    getHeader(name) { return headers.get(name.toLowerCase()); },
    append(name, value) {
      const key = name.toLowerCase();
      const old = headers.get(key) || [];
      headers.set(key, [...(Array.isArray(old) ? old : [old]), value]);
    },
    status(code) { this.statusCode = code; return this; },
    json(value) { this.body = value; return this; },
    headers
  };
}

await test("missing session fails closed", async () => {
  const { service } = makeService();
  await expectCode(service.authenticateRequest(requestWithCookie("")), "AUTH_SESSION_MISSING");
});
await test("malformed access token rejected", async () => {
  const { service } = makeService();
  await expectCode(service.verifyAccessToken("not-a-jwt"), "AUTH_TOKEN_MALFORMED");
});
await test("expired access token rejected before provider", async () => {
  const { service } = makeService();
  await expectCode(service.verifyAccessToken(jwt({ iat: 1, exp: 2 })), "AUTH_SESSION_EXPIRED");
});
await test("overlong access token lifetime rejected", async () => {
  const now = Math.floor(Date.now() / 1000);
  const { service } = makeService();
  await expectCode(service.verifyAccessToken(jwt({ iat: now, exp: now + 7200 })), "AUTH_TOKEN_LIFETIME_INVALID");
});
await test("forged token rejected by getUser", async () => {
  const { service } = makeService({ getUser: async () => ({ data: { user: null }, error: { message: "bad jwt" } }) });
  await expectCode(service.verifyAccessToken(jwt()), "AUTH_TOKEN_INVALID");
});
await test("provider transport failure fails closed", async () => {
  const { service } = makeService({ getUser: async () => { throw new Error("network"); } });
  await expectCode(service.verifyAccessToken(jwt()), "AUTH_PROVIDER_UNAVAILABLE");
});
await test("anonymous user rejected", async () => {
  const { service } = makeService({ user: confirmedUser({ is_anonymous: true }) });
  await expectCode(service.verifyAccessToken(jwt()), "AUTH_ANONYMOUS_FORBIDDEN");
});
await test("unconfirmed email rejected", async () => {
  const { service } = makeService({ user: confirmedUser({ email_confirmed_at: null, confirmed_at: null }) });
  await expectCode(service.verifyAccessToken(jwt()), "AUTH_EMAIL_UNCONFIRMED");
});
await test("generic confirmed_at cannot replace email confirmation", async () => {
  const { service } = makeService({ user: confirmedUser({ email_confirmed_at: null, confirmed_at: "2026-09-04T00:00:00.000Z" }) });
  await expectCode(service.verifyAccessToken(jwt()), "AUTH_EMAIL_UNCONFIRMED");
});
await test("non UUID auth id rejected", async () => {
  const { service } = makeService({ user: confirmedUser({ id: "visitor-123" }) });
  await expectCode(service.verifyAccessToken(jwt()), "AUTH_USER_INVALID");
});
await test("non email identity rejected", async () => {
  const { service } = makeService({ user: confirmedUser({ app_metadata: {}, identities: [{ provider: "phone" }] }) });
  await expectCode(service.verifyAccessToken(jwt()), "AUTH_EMAIL_IDENTITY_REQUIRED");
});
await test("body user id is never identity authority", async () => {
  const { service, accessToken } = makeService();
  await expectCode(service.authenticateRequest(requestWithCookie(accessToken, { body: { userId: confirmedUser().id }, query: {} })), "AUTH_SELF_ASSERTED_IDENTITY_FORBIDDEN");
});
await test("query user id is never identity authority", async () => {
  const { service, accessToken } = makeService();
  await expectCode(service.authenticateRequest(requestWithCookie(accessToken, { body: {}, query: { user_id: confirmedUser().id } })), "AUTH_SELF_ASSERTED_IDENTITY_FORBIDDEN");
});
await test("visitor id is never identity authority", async () => {
  const { service, accessToken } = makeService();
  await expectCode(service.authenticateRequest(requestWithCookie(accessToken, { body: { visitorId: "browser-uuid" }, query: {} })), "AUTH_SELF_ASSERTED_IDENTITY_FORBIDDEN");
});
await test("email string is never payment identity authority", async () => {
  const { service, accessToken } = makeService();
  await expectCode(service.authenticateRequest(requestWithCookie(accessToken, { body: { email: "other@example.com" }, query: {} })), "AUTH_SELF_ASSERTED_IDENTITY_FORBIDDEN");
});
await test("provider user id is never request authority", async () => {
  const { service, accessToken } = makeService();
  await expectCode(service.authenticateRequest(requestWithCookie(accessToken, { body: { providerUserId: confirmedUser().id }, query: {} })), "AUTH_SELF_ASSERTED_IDENTITY_FORBIDDEN");
});
await test("verified auth user is accepted", async () => {
  const { service, accessToken } = makeService();
  const result = await service.authenticateRequest(requestWithCookie(accessToken));
  assert.equal(result.user.id, confirmedUser().id);
});
await test("payment identity id equals verified auth id", async () => {
  const { service } = makeService();
  const result = await service.verifyAccessToken(jwt());
  assert.equal(result.identity.userId, result.user.id);
  assertVerifiedPaymentIdentity(result.identity);
});
await test("PAY-01 verified identity factory is sole constructor", () => {
  assert.equal((identitySource.match(/createVerifiedPaymentIdentity\s*\(/g) || []).length, 1);
  assert.match(identitySource, /verification:\s*"authenticated_session"/);
});
await test("a fresh stateless auth client is created for revalidation", async () => {
  const fixture = makeService();
  await fixture.service.verifyAccessToken(jwt());
  await fixture.service.verifyAccessToken(jwt());
  assert.equal(fixture.getClientCount(), 2);
});
await test("OTP request uses permanent email flow", async () => {
  let observed;
  const { service } = makeService({ signInWithOtp: async payload => { observed = payload; return { data: {}, error: null }; } });
  assert.deepEqual(await service.requestOtp(" Person@Example.com "), { accepted: true });
  assert.deepEqual(observed, { email: "person@example.com", options: { shouldCreateUser: true } });
});
await test("OTP verify revalidates returned access token", async () => {
  let getUserToken = "";
  const { service, accessToken } = makeService({ getUser: async token => { getUserToken = token; return { data: { user: confirmedUser() }, error: null }; } });
  await service.verifyOtp("person@example.com", "123456");
  assert.equal(getUserToken, accessToken);
});
await test("OTP identity mismatch rejected", async () => {
  const other = confirmedUser({ id: "7c9be723-5c22-46cb-81eb-5962dccce0fa" });
  const { service } = makeService({ verifyOtp: async () => ({ data: { user: other, session: { access_token: jwt(), refresh_token: "r2" } }, error: null }) });
  await expectCode(service.verifyOtp("person@example.com", "123456"), "AUTH_IDENTITY_MISMATCH");
});
await test("refresh token must rotate", async () => {
  const { service } = makeService({ session: { access_token: jwt(), refresh_token: "same-refresh", expires_in: 1200 } });
  await expectCode(service.refresh("same-refresh"), "AUTH_REFRESH_NOT_ROTATED");
});
await test("refresh rotation is accepted and revalidated", async () => {
  const { service } = makeService();
  const result = await service.refresh("old-refresh");
  assert.equal(result.session.refresh_token, "rotated-refresh");
  assertVerifiedPaymentIdentity(result.identity);
});
await test("auth cookies use required secure attributes", () => {
  const res = mockResponse();
  writePaymentAuthCookies(res, { access_token: "access", refresh_token: "refresh", expires_in: 1200 });
  const values = res.getHeader("Set-Cookie");
  assert.equal(values.length, 2);
  for (const value of values) {
    assert.match(value, /Path=\/; Max-Age=\d+; HttpOnly; Secure; SameSite=Lax/);
    assert.doesNotMatch(value, /Domain=/i);
  }
});
await test("logout cookie clearing is complete", () => {
  const res = mockResponse();
  clearPaymentAuthCookies(res);
  const values = res.getHeader("Set-Cookie");
  assert.equal(values.length, 2);
  assert(values.some(value => value.startsWith(`${PAYMENT_AUTH_ACCESS_COOKIE}=`)));
  assert(values.some(value => value.startsWith(`${PAYMENT_AUTH_REFRESH_COOKIE}=`)));
  assert(values.every(value => value.includes("Max-Age=0")));
});
await test("OTP request response is enumeration resistant", async () => {
  const handlers = createPaymentAuthHandlers({ requestOtp: async () => ({ accepted: true }) });
  const res = mockResponse();
  await handlers.requestOtp({ body: { email: "person@example.com" } }, res);
  assert.equal(res.statusCode, 202);
  assert.deepEqual(res.body, { success: true, accepted: true });
});
await test("logout clears local session even without provider success", async () => {
  const handlers = createPaymentAuthHandlers({ signOut: async () => { throw new Error("provider down"); } });
  const res = mockResponse();
  await handlers.logout({ get: () => "" }, res);
  assert.equal(res.body.state, "signed_out");
  assert.equal(res.getHeader("Set-Cookie").length, 2);
});
await test("browser code never stores auth tokens", () => {
  assert.doesNotMatch(clientSource, /localStorage|sessionStorage|access_token|refresh_token/i);
  assert.doesNotMatch(indexSource, /localStorage[^\n]*(auth|token)|(auth|token)[^\n]*localStorage/i);
});
await test("browser auth calls are same-origin relative paths", () => {
  assert.doesNotMatch(clientSource, /https?:\/\//i);
  assert.match(clientSource, /credentials:\s*"include"/);
});
await test("server auth mutations require exact request origin", () => {
  assert.match(serverSource, /process\.env\.AUTH_ALLOWED_ORIGIN/);
  assert.match(serverSource, /isAllowedPaymentAuthMutationOrigin\(\{ origin, referer, allowedOrigin: authAllowedOrigin \}\)/);
  assert.doesNotMatch(serverSource.match(/function authMutationGuard[\s\S]*?\n\}/)?.[0] || "", /getRequestOrigin|x-forwarded-host|x-forwarded-proto/);
  assert.match(serverSource, /AUTH_ORIGIN_FORBIDDEN/);
});
await test("all auth endpoints have dedicated rate limits", () => {
  for (const endpoint of ["authOtpRequest", "authOtpVerify", "authSession"]) assert.match(serverSource, new RegExp(`${endpoint}: \\{ windowMs:`));
});
await test("only the five authorized auth routes are registered", () => {
  const routes = [...serverSource.matchAll(/app\.(?:get|post)\("(\/api\/auth\/[^\"]+)"/g)].map(match => match[1]);
  assert.deepEqual(routes, ["/api/auth/otp/request", "/api/auth/otp/verify", "/api/auth/refresh", "/api/auth/logout", "/api/auth/me"]);
});
await test("no payment execution endpoints were added", () => {
  assert.doesNotMatch(serverSource, /app\.(?:get|post|put|patch|delete)\("\/api\/(?:checkout|orders?|webhooks?|entitlements?|payments?)/i);
});
await test("auth API never returns or logs token material", () => {
  assert.doesNotMatch(identitySource, /console\.(?:log|info|warn|error)/);
  const responseLines = identitySource.split(/\r?\n/).filter(line => /\.json\(/.test(line)).join("\n");
  assert.doesNotMatch(responseLines, /access_token|refresh_token/);
});
await test("browser bundle has no service credential reference", () => {
  assert.doesNotMatch(clientSource + indexSource, /SUPABASE_(?:SERVICE|SECRET)|service_role|sb_secret_/i);
});
await test("UI contains only auth state and no payment outcome", () => {
  assert.match(indexSource, /购买需要登录/);
  assert.doesNotMatch(clientSource, /payment[_ -]?success|entitlement|providerOrderId/i);
});
await test("fixed auth origin accepts the configured production origin", () => {
  assert.equal(isAllowedPaymentAuthMutationOrigin({ origin: "https://geokitlab.com", allowedOrigin: "https://geokitlab.com" }), true);
  assert.equal(isAllowedPaymentAuthMutationOrigin({ referer: "https://geokitlab.com/account", allowedOrigin: "https://geokitlab.com" }), true);
});
await test("fixed auth origin rejects missing sources and configuration", () => {
  assert.equal(isAllowedPaymentAuthMutationOrigin({ allowedOrigin: "https://geokitlab.com" }), false);
  assert.equal(isAllowedPaymentAuthMutationOrigin({ origin: "https://geokitlab.com", allowedOrigin: "" }), false);
});
await test("fixed auth origin rejects malformed and mismatched sources", () => {
  assert.equal(isAllowedPaymentAuthMutationOrigin({ origin: "not-a-url", allowedOrigin: "https://geokitlab.com" }), false);
  assert.equal(isAllowedPaymentAuthMutationOrigin({ origin: "https://evil.example", allowedOrigin: "https://geokitlab.com" }), false);
  assert.equal(isAllowedPaymentAuthMutationOrigin({ origin: "https://geokitlab.com", referer: "https://evil.example/path", allowedOrigin: "https://geokitlab.com" }), false);
});
await test("initial signed-out load attempts controlled refresh", () => {
  assert.match(clientSource, /payload\.state === "authenticated"[\s\S]*refreshSession\("signed_out"\)/);
});
await test("expired access session attempts controlled refresh", () => {
  assert.match(clientSource, /error\.status === 401[\s\S]*refreshSession\("expired"\)/);
});
await test("successful refresh republishes authenticated state", () => {
  assert.match(clientSource, /api\("\/api\/auth\/refresh"[\s\S]*publish\("authenticated", payload\.user\)/);
});
await test("failed refresh clears cookies server-side before terminal state", async () => {
  const handlers = createPaymentAuthHandlers({ refresh: async () => { throw new PaymentIdentityError("AUTH_SESSION_EXPIRED", 401); } });
  const res = mockResponse();
  await handlers.refresh({ get: () => `${PAYMENT_AUTH_REFRESH_COOKIE}=expired` }, res);
  assert.equal(res.statusCode, 401);
  assert.equal(res.getHeader("Set-Cookie").length, 2);
  assert(res.getHeader("Set-Cookie").every(value => value.includes("Max-Age=0")));
});
await test("logout failure publishes uncertainty rather than signed out", () => {
  const logoutBlock = clientSource.match(/logoutButton\.addEventListener\([\s\S]*?\n  \}\);/)?.[0] || "";
  assert.match(logoutBlock, /catch \(_\) \{\s*publish\("unknown"/);
  assert.doesNotMatch(logoutBlock, /finally \{\s*publish\("signed_out"/);
});

const failures = cases.filter(item => !item.pass);
for (const item of cases) {
  console.log(`${item.pass ? "PASS" : "FAIL"} ${item.name}`);
  if (!item.pass) console.log(item.error);
}

console.log(`PAY_ID_01_CASES=${cases.length - failures.length}/${cases.length}_${failures.length ? "FAIL" : "PASS"}`);
console.log(`PAY_ID_01_IDENTITY_CHAIN=${failures.length ? "FAIL" : "PASS"}`);
console.log(`PAY_ID_01_COOKIE_SECURITY=${failures.length ? "FAIL" : "PASS"}`);
console.log(`PAY_ID_01_AUTHORITY_BOUNDARY=${failures.length ? "FAIL" : "PASS"}`);
console.log("STOLEN_ACCESS_TOKEN_IMMEDIATE_REVOCATION=NOT_CLAIMED_REQUIRES_LATER_GATE");
if (failures.length) process.exitCode = 1;
