// Local, explicit evidence replay. Never imported by server.js.
import './recognition-audit-offline-guard.cjs';
import { readFileSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createProductRuntime } from './recognition-architecture-probe.js';
import { inspectDiagnosticArtifact } from '../server/recognition/recognition-diagnostics.js';
import { normalizeProviderDmsReviewResult } from '../server/recognition/recognition-review-result.js';
import { extractProviderProjectedCoordinateEvidence } from '../server/evidence-acquisition/local-ocr-map-layout-classifier.js';
import { bindProjectedEvidenceToSourceContext } from '../server/recognition/projected-source-evidence.js';
import { bindProviderRepresentationsToSource } from '../server/recognition/multi-representation-source-evidence.js';
import { extractRecognitionCandidateEvidence } from '../server/recognition/recognition-candidate-evidence.js';
import { adaptRecognitionTableCandidates } from '../server/recognition/recognition-table-input.js';
import { evaluateUnifiedRecognitionAcquisition, evaluateUnifiedRecognitionFinalAuthorization } from '../server/recognition/recognition-first-acquisition.js';

const root = fileURLToPath(new URL('../', import.meta.url));
const BUNDLE_VERSION = 'recognition_replay_bundle_v1';
const MAX_INPUT_BYTES = 8 * 1024 * 1024;
const hash = value => createHash('sha256').update(value).digest('hex');
const plain = value => JSON.parse(JSON.stringify(value));

export function productFingerprint() {
  // Content hashes, not Git HEAD alone: dirty worktree instrumentation is covered.
  const files = ['server.js', 'package.json', 'package-lock.json',
    'scripts/recognition-architecture-probe.js', 'scripts/recognition-diagnostic-replay.js'];
  function walk(relative) {
    for (const entry of readdirSync(path.join(root, relative), { withFileTypes: true })) {
      const next = `${relative}/${entry.name}`;
      if (entry.isDirectory()) walk(next);
      else if (entry.isFile() && /\.(?:js|cjs|json)$/.test(entry.name)) files.push(next);
    }
  }
  walk('server'); walk('data/spatial-knowledge');
  const manifest = files.sort().map(file => [file, hash(readFileSync(path.join(root, file)))]);
  return { algorithm: 'sha256', digest: hash(JSON.stringify({ manifest, node: process.version })), node: process.version, manifest };
}

export function makeReplayBundle(diagnostics) {
  const checked = inspectDiagnosticArtifact(diagnostics);
  const current = productFingerprint();
  // Never retroactively attribute old/unknown evidence to the code present at save time.
  return { schemaVersion: BUNDLE_VERSION, source: checked.sourceDigest
    ? checked.sourceDigest === current.digest ? current : { digest: checked.sourceDigest } : null, diagnostics: checked };
}

function differences(expected, actual, pointer = '', result = []) {
  if (result.length >= 32 || Object.is(expected, actual)) return result;
  if (expected === null || actual === null || typeof expected !== 'object' || typeof actual !== 'object'
    || Array.isArray(expected) !== Array.isArray(actual)) { result.push(pointer || '/'); return result; }
  for (const key of new Set([...Object.keys(expected), ...Object.keys(actual)])) {
    const next = `${pointer}/${key.replace(/~/g, '~0').replace(/\//g, '~1')}`;
    if (!(key in expected) || !(key in actual)) result.push(next);
    else differences(expected[key], actual[key], next, result);
    if (result.length >= 32) break;
  }
  return result;
}

// Reports observed checks; does not manufacture a new eligibility decision.
export function diagnosticFindings(diagnostics) {
  const findings = [];
  for (const session of diagnostics.sessions) for (const event of session.events) {
    const at = { session: session.sequence, event: event.sequence, stage: event.stage };
    if (['row_contract_details', 'projected_value_details'].includes(event.stage)) {
      for (const check of event.data?.checks || []) if (check.passed === false) findings.push({ ...at, ...check });
    }
    if (event.stage === 'projected_check' && event.data?.passed === false) {
      findings.push({ ...at, condition: event.data.condition, field: event.data.field,
        originalCrs: event.data.sourceCrs || null });
    }
    if (event.stage === 'transform_geometry' && event.data?.referenceCheckExecuted === true && event.data.referencesMatch === false) {
      for (const row of event.data.rows || []) for (const [field, residual] of [
        ['referenceDms.latitudeDecimal', row.latitudeResidual], ['referenceDms.longitudeDecimal', row.longitudeResidual]
      ]) {
        if (residual === null || typeof residual !== 'number' || Math.abs(residual) > event.data.toleranceDegrees) {
          findings.push({ ...at, rowIndex: row.index, pointLabel: row.label, field,
            condition: residual === null ? 'REFERENCE_FIELD_UNAVAILABLE' : 'REFERENCE_TOLERANCE_EXCEEDED',
            residualDegrees: residual, toleranceDegrees: event.data.toleranceDegrees, originalCrs: event.data.sourceCrs });
        }
      }
    }
    const result = event.data?.result;
    if (event.operation === 'extractRecognitionCandidateEvidence') {
      for (const row of result?.rejectedRows || []) findings.push({ ...at, field: 'candidateRow',
        evidence: row });
    }
    if (event.operation === 'bindProjectedEvidenceToSourceContext' && result?.sourceContextBinding?.bound === false) {
      findings.push({ ...at, field: 'sourceContextBinding', condition: 'BINDING_NOT_ESTABLISHED',
        evidence: result.sourceContextBinding, originalProviderCrs: event.data.args?.[0]?.providerEvidence?.crsEvidence });
    }
    if (event.operation === 'bindProviderRepresentationsToSource' && ['CONFLICT', 'INCOMPLETE'].includes(result?.status)) {
      findings.push({ ...at, field: 'representationBinding', condition: result.status,
        reason: result.reason, reasons: result.reviewReasons, missingLabels: result.missingLabels, orderConflict: result.orderConflict });
    }
    if (event.operation === 'evaluateUnifiedRecognitionFinalAuthorization') {
      findings.push({ ...at, field: 'finalAuthorization', condition: 'RECORDED_FINAL_DECISION',
        reasons: result?.finalAuthorizationReasons, mapReady: result?.mapReady, kmlReady: result?.kmlReady });
    }
  }
  return findings;
}

export function replayDiagnosticBundle(bundle) {
  const base = { replayKind: 'LOCAL_STAGE_CALLS', productionReproduction: 'NOT_ESTABLISHED',
    realProviderCalls: 0, externalServicesUsed: false,
    exclusions: ['image decoding and OCR', 'Provider execution', 'full route and request timing', 'billing commit', 'browser lifecycle'] };
  if (!bundle) return { ...base, status: 'UNKNOWN', reason: 'HISTORICAL_RAW_RESPONSE_NOT_AVAILABLE', operations: [] };
  if (bundle.schemaVersion !== BUNDLE_VERSION) throw new Error('INVALID_REPLAY_BUNDLE');
  const diagnostics = inspectDiagnosticArtifact(bundle.diagnostics);
  if (!bundle.source || bundle.source.digest !== diagnostics.sourceDigest || bundle.source.digest !== productFingerprint().digest) {
    return { ...base, status: 'UNKNOWN', reason: 'SOURCE_VERSION_MISSING_OR_MISMATCH', operations: [] };
  }
  let runtime;
  const actualInline = name => (...args) => {
    runtime ||= createProductRuntime(readFileSync(path.join(root, 'server.js'), 'utf8'));
    return runtime[name](...args);
  };
  const registry = new Map(Object.entries({
    extractProviderMessageText: actualInline('extractProviderMessageText'),
    extractProviderDmsReviewEvidence: actualInline('extractProviderDmsReviewEvidence'),
    buildExplicitProjectedBoundaryAutoReleaseEngine: actualInline('buildExplicitProjectedBoundaryAutoReleaseEngine'),
    normalizeProviderDmsReviewResult, extractProviderProjectedCoordinateEvidence,
    bindProjectedEvidenceToSourceContext, bindProviderRepresentationsToSource, extractRecognitionCandidateEvidence,
    evaluateUnifiedRecognitionAcquisition, evaluateUnifiedRecognitionFinalAuthorization, adaptRecognitionTableCandidates
  }));
  const operations = [];
  let incomplete = diagnostics.sessions.length === 0 || diagnostics.truncated === true;
  let rawPresent = false;
  for (const session of diagnostics.sessions) {
    if (!Array.isArray(session.events) || session.events.length > 96) throw new Error('INVALID_EVENT_LIST');
    if (session.truncated) incomplete = true;
    for (const event of session.events) {
      if (event.retention !== 'COMPLETE') incomplete = true;
      if (!event.operation) continue;
      if (!registry.has(event.operation)) throw new Error('OPERATION_NOT_ALLOWLISTED');
      const entry = { session: session.sequence, event: event.sequence, operation: event.operation };
      if (event.retention !== 'COMPLETE' || event.withheld?.length) {
        operations.push({ ...entry, status: 'UNKNOWN', reason: 'EVIDENCE_WITHHELD' }); continue;
      }
      if (!Array.isArray(event.data?.args) || event.data.args.length > 4 || !Object.hasOwn(event.data, 'result')) {
        throw new Error('INVALID_OPERATION_ARGUMENTS');
      }
      if (event.operation === 'extractProviderMessageText') rawPresent = true;
      try {
        // Only installed product functions are executable. Artifacts cannot supply code.
        const result = plain(registry.get(event.operation)(...structuredClone(event.data.args)));
        const changedFields = differences(event.data.result, result);
        operations.push({ ...entry, status: changedFields.length ? 'DIFFERENT' : 'MATCH', changedFields });
      } catch {
        operations.push({ ...entry, status: 'UNKNOWN', reason: 'PRODUCT_STAGE_THROWN' });
        incomplete = true; // Never print an untrusted exception containing raw evidence.
      }
    }
  }
  return { ...base, originDeclaration: diagnostics.origin, rawResponseAvailable: rawPresent,
    status: operations.some(entry => entry.status === 'DIFFERENT') ? 'DIFFERENT'
      : !rawPresent ? 'UNKNOWN' : incomplete ? 'PARTIAL' : 'MATCHED_CAPTURED_STAGES',
    reason: !rawPresent ? 'HISTORICAL_RAW_RESPONSE_NOT_AVAILABLE' : 'NOT_A_FULL_PRODUCTION_REQUEST_REPLAY',
    operations, findings: diagnosticFindings(diagnostics) };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  for (const name of Object.keys(process.env)) {
    if (/PROVIDER|SUPABASE|ALIYUN|DASHSCOPE|OPENAI|ANTHROPIC|GEMINI|API_KEY|API_TOKEN|SECRET|USAGE|PASSWORD/i.test(name)) process.env[name] = '';
  }
  const args = process.argv.slice(2);
  let report;
  if (!args.length || args[0] === '--missing-history') report = replayDiagnosticBundle(null);
  else {
    if (args[0] !== '--input' || !args[1] || args.length !== 2 || /^(?:https?:|data:|\\\\)/i.test(args[1])) {
      throw new Error('USE_LOCAL_INPUT_FILE_ONLY');
    }
    const input = path.resolve(args[1]);
    if (!statSync(input).isFile() || statSync(input).size > MAX_INPUT_BYTES) throw new Error('INPUT_SIZE_LIMIT');
    report = replayDiagnosticBundle(JSON.parse(readFileSync(input, 'utf8')));
  }
  console.log(JSON.stringify(report, null, 2));
  if (report.status === 'DIFFERENT') process.exitCode = 1;
}

// Explicit export only, outside production. Never overwrite an existing capture.
export function saveLocalReplayBundle(destination, diagnostics) {
  if (/^(?:https?:|data:|\\\\)/i.test(destination)) throw new Error('USE_LOCAL_OUTPUT_FILE_ONLY');
  const text = JSON.stringify(makeReplayBundle(diagnostics), null, 2);
  if (Buffer.byteLength(text) > MAX_INPUT_BYTES) throw new Error('OUTPUT_SIZE_LIMIT');
  writeFileSync(path.resolve(destination), text, { encoding: 'utf8', flag: 'wx', mode: 0o600 });
}
