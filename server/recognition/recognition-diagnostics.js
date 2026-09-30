import { AsyncLocalStorage } from 'node:async_hooks';
import { createHash } from 'node:crypto';

export const RECOGNITION_DIAGNOSTIC_VERSION = 'recognition_diagnostic_v1';
const captureScope = new AsyncLocalStorage();
const MAX_EVENTS = 96;
const MAX_ARRAY = 512;
const MAX_TEXT = 65536;
const MAX_BYTES = 2 * 1024 * 1024;
const privateKey = /^(?:authorization|headers?|cookies?|api[_-]?key|.*secret.*|.*tokens?|password|visitorId|userId|account.*|email|phone|originalname|fileName|quota|session(?:Id|Binding|Cookie)?)$/i;
const privateText = /(?:Bearer\s+\S+|\bsk-[A-Za-z0-9_-]{8,}|eyJ[A-Za-z0-9_-]{12,}\.|(?:api[_ -]?key|password|secret|authorization|cookie|account|user[_ -]?id|session[_ -]?id)\s*["']?\s*[:=]|[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}|https?:\/\/|data:image\/)/i;
const textKey = /^(?:raw|rawText|rawLine|rawProviderText|rawResponse|rawLiteral|rawRow|normalizedRow|decodedValue|normalizedValue|text|normalizedText|sourceText|sourceContextText|axisEvidenceText|latitudeSource|longitudeSource|coordinates|content|candidateEvidenceText)$/;
const hash = value => createHash('sha256').update(String(value)).digest('hex');

// An allowlisted call site passes only coordinate evidence, never req/headers/user
// objects. This second boundary withholds suspect content rather than redacting it
// into apparently replayable evidence. Default captures contain text hashes only.
function safeSnapshot(input, retainSourceText) {
  const withheld = [];
  function visit(value, pointer = '', key = '', depth = 0) {
    if (depth > 24) { withheld.push(pointer); return null; }
    if (privateKey.test(key)) { withheld.push(pointer); return undefined; }
    if (value === undefined) return undefined;
    if (value === null || typeof value === 'boolean') return value;
    if (typeof value === 'number') {
      if (!Number.isFinite(value)) withheld.push(pointer);
      return Number.isFinite(value) ? value : null;
    }
    if (typeof value === 'string') {
      const source = textKey.test(key) || /Text$/.test(key) || /\/(?:args|sourceRows)\/\d+$/.test(pointer) || key === 'result';
      if (value.length > MAX_TEXT || privateText.test(value) || (source && !retainSourceText)) {
        withheld.push(pointer);
        return { retention: 'WITHHELD', sha256: hash(value), length: value.length };
      }
      return value;
    }
    if (Array.isArray(value)) {
      if (value.length > MAX_ARRAY) withheld.push(pointer);
      return value.slice(0, MAX_ARRAY).map((entry, index) => {
        if (entry === undefined) withheld.push(`${pointer}/${index}`);
        return visit(entry, `${pointer}/${index}`, '', depth + 1);
      });
    }
    if (typeof value !== 'object') { withheld.push(pointer); return null; }
    if (Object.prototype.toString.call(value) !== '[object Object]') { withheld.push(pointer); return null; }
    const result = {};
    for (const [name, entry] of Object.entries(value)) {
      if (['__proto__', 'constructor', 'prototype'].includes(name)) { withheld.push(pointer); continue; }
      const next = visit(entry, `${pointer}/${name}`, name, depth + 1);
      if (next !== undefined) result[name] = next;
    }
    return result;
  }
  return { value: visit(input), withheld };
}

const disabled = Object.freeze({ enabled: false, event() {}, observe() {}, operation() {}, check() {} });

// Explicit in-process diagnostic scope only: no env toggle, request header,
// public endpoint, automatic file persistence or console output.
export async function withRecognitionDiagnostics(action, { retainSourceText = false, origin = 'UNKNOWN_ORIGIN', sourceDigest = null } = {}) {
  const scope = { retainSourceText: retainSourceText === true,
    origin: ['SYNTHETIC', 'CAPTURED_EVIDENCE'].includes(origin) ? origin : 'UNKNOWN_ORIGIN', sessions: [], truncated: false };
  const value = await captureScope.run(scope, action);
  return { value, diagnostics: JSON.parse(JSON.stringify({ schemaVersion: RECOGNITION_DIAGNOSTIC_VERSION,
    sourceDigest: /^[0-9a-f]{64}$/.test(String(sourceDigest || '')) ? sourceDigest : null,
    origin: scope.origin, truncated: scope.truncated, sessions: scope.sessions })) };
}

export function createRecognitionDiagnosticSession() {
  const scope = captureScope.getStore();
  if (!scope) return disabled;
  if (scope.sessions.length >= 16) { scope.truncated = true; return disabled; }
  const session = { sequence: scope.sessions.length + 1, events: [], truncated: false };
  scope.sessions.push(session);
  let byteCount = 0;
  function record(stage, data, operation = null) {
    try {
      if (session.events.length >= MAX_EVENTS || byteCount >= MAX_BYTES) { session.truncated = true; return; }
      const snapshot = safeSnapshot(data, scope.retainSourceText);
      const event = { sequence: session.events.length + 1, stage, operation,
        retention: snapshot.withheld.length ? 'PARTIAL' : 'COMPLETE', withheld: snapshot.withheld, data: snapshot.value };
      const bytes = Buffer.byteLength(JSON.stringify(event));
      if (bytes + byteCount > MAX_BYTES) { session.truncated = true; return; }
      byteCount += bytes;
      session.events.push(event);
    } catch { session.truncated = true; } // Observability must never alter the request.
  }
  return Object.freeze({ enabled: true,
    event(stage, data) { record(stage, data); },
    observe(stage, collect) { try { record(stage, collect()); } catch { session.truncated = true; } },
    operation(name, args, result) { record(name, { args, result }, name); },
    check(condition, passed, details = {}) { record('projected_check', { condition, passed, ...details }); }
  });
}

export function inspectDiagnosticArtifact(artifact) {
  if (!artifact || artifact.schemaVersion !== RECOGNITION_DIAGNOSTIC_VERSION || !Array.isArray(artifact.sessions)
    || artifact.sessions.length > 16) throw new Error('INVALID_DIAGNOSTIC_ARTIFACT');
  const snapshot = safeSnapshot(artifact, true);
  if (snapshot.withheld.length) throw new Error('UNSAFE_OR_OVERSIZED_DIAGNOSTIC_ARTIFACT');
  return snapshot.value;
}

// Only fields read by evaluateUnifiedRecognitionFinalAuthorization; transport,
// account and charging metadata never enter this replay argument.
export function authorizationDiagnosticBody(body = {}) {
  const selected = Object.fromEntries(['multiRepresentationEvidence',
    'requiresReview', 'authorizationStatus', 'resultStatus', 'mapReady', 'kmlReady', 'providerCallCount']
    .filter(key => body[key] !== undefined).map(key => [key, body[key]]));
  const finalized = body.finalizedCoordinateResult;
  if (finalized) selected.finalizedCoordinateResult = Object.fromEntries([
    'coordinateType', 'family', 'precisionMode', 'requiresReview', 'qualityGateStatus', 'decisionState',
    'kmlReady', 'geometry', 'crs', 'mapReady', 'confirmationStatus', 'sourceAuthority',
    'explicitAuthorityRejected', 'kmlAuthorityBlocked', 'technicalKmlReady'
  ].filter(key => finalized[key] !== undefined).map(key => [key, finalized[key]]));
  if (body.coordinateEngineV2) selected.coordinateEngineV2 = { source_crs: body.coordinateEngineV2.source_crs };
  return selected;
}
