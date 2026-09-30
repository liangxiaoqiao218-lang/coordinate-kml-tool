// Phase C: offline observation adapter. Never import this from production.
import './recognition-audit-offline-guard.cjs';
import { readFileSync, statSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { inspectDiagnosticArtifact } from '../server/recognition/recognition-diagnostics.js';
import { replayDiagnosticBundle, diagnosticFindings } from './recognition-diagnostic-replay.js';

export const CONTRACT_VERSION = 'coordinate_result_observation_v1';
const clone = value => JSON.parse(JSON.stringify(value));
const digest = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const unknown = reason => ({ status: 'UNKNOWN', presence: 'ABSENT', value: null, source: null, reason });
const pointerPart = key => String(key).replace(/~/g, '~0').replace(/\//g, '~1');
function lookup(value, pointer) {
  if (!pointer) return { present: true, value };
  for (const key of pointer.slice(1).split('/').map(p => p.replace(/~1/g, '/').replace(/~0/g, '~'))) {
    if (!value || typeof value !== 'object' || !Object.hasOwn(value, key)) return { present: false };
    value = value[key];
  }
  return { present: true, value };
}
function leafDifferences(left, right, pointer = '', out = []) {
  if (isDeepStrictEqual(left, right)) return out;
  if (!left || !right || typeof left !== 'object' || typeof right !== 'object'
    || Array.isArray(left) !== Array.isArray(right)) { out.push(pointer || '/'); return out; }
  for (const key of new Set([...Object.keys(left), ...Object.keys(right)])) {
    const next = `${pointer}/${pointerPart(key)}`;
    if (!Object.hasOwn(left, key) || !Object.hasOwn(right, key)) out.push(next);
    else leafDifferences(left[key], right[key], next, out);
  }
  return out;
}

// No parsing, coordinate conversion, cross-row joins or authority decisions here.
// A source pointer identifies a parser observation, NOT proof of visual accuracy.
export function adaptDiagnosticBundle(bundle) {
  if (!bundle) return { schemaVersion: CONTRACT_VERSION, mode: 'OFFLINE_OBSERVATION_ONLY',
    status: 'UNKNOWN', reason: 'HISTORICAL_RAW_RESPONSE_NOT_AVAILABLE', sessions: [] };
  if (bundle.schemaVersion !== 'recognition_replay_bundle_v1') throw new Error('INVALID_REPLAY_BUNDLE');
  const diagnostics = inspectDiagnosticArtifact(bundle.diagnostics);
  const contract = { schemaVersion: CONTRACT_VERSION, mode: 'OFFLINE_OBSERVATION_ONLY',
    status: 'OBSERVATIONS_NOT_AUTHORITY', originDeclaration: diagnostics.origin,
    sourceDigest: diagnostics.sourceDigest, captureDigest: digest(diagnostics),
    productionReproduction: 'NOT_ESTABLISHED', sessions: [] };
  for (const [sessionIndex, session] of diagnostics.sessions.entries()) {
    const find = name => session.events.filter(e => e.stage === name || e.operation === name);
    const last = name => find(name).at(-1);
    function field(event, pointer) {
      if (!event) return unknown('STAGE_NOT_CAPTURED');
      const reference = { sessionIndex, eventIndex: session.events.indexOf(event),
        stage: event.stage, operation: event.operation, pointer };
      if (event.retention !== 'COMPLETE') return { ...unknown('EVIDENCE_WITHHELD_OR_TRUNCATED'), source: reference };
      const found = lookup(event.data, pointer);
      if (!found.present) return { ...unknown('FIELD_NOT_CAPTURED'), source: reference };
      return { status: found.value === null ? 'UNKNOWN' : 'OBSERVED', presence: 'PRESENT',
        value: clone(found.value), source: reference, reason: found.value === null ? 'EXPLICIT_NULL' : null };
    }
    function fields(event, base, names) {
      return Object.fromEntries(names.map(name => [name, field(event, `${base}/${pointerPart(name)}`)]));
    }
    const image = last('image_identity'), delivered = last('delivered_result');
    const final = last('evaluateUnifiedRecognitionFinalAuthorization');
    const binding = last('bindProjectedEvidenceToSourceContext');
    const extracted = last('extractProviderProjectedCoordinateEvidence');
    const candidate = last('adaptRecognitionTableCandidates') || last('extractRecognitionCandidateEvidence');
    const result = {
      captureSession: session.sequence,
      captureComplete: !diagnostics.truncated && !session.truncated && session.events.every(e => e.retention === 'COMPLETE'),
      identity: fields(image, '', ['image_sha256', 'page', 'regionId', 'tableId']),
      // Keep exact retained source, derived candidate text and parser outputs separate.
      rawEvidence: find('extractProviderMessageText').map(e => ({
        envelope: field(e, '/args/0'), extractedText: field(e, '/result') })),
      localOcr: find('local_ocr').map(e => field(e, '')),
      candidateInput: find('candidate_input_selection').map(e => fields(e, '',
        ['rawProviderText', 'candidateEvidenceText', 'selectedSource', 'visibleCrsEvidence'])),
      rowSets: [],
      crs: {
        providerOriginal: field(extracted, '/result/crsEvidence'),
        boundOriginal: field(binding, '/result/crsEvidence'),
        zone: field(binding, '/result/crsEvidence/zone'), hemisphere: field(binding, '/result/crsEvidence/hemisphere'),
        datum: field(binding, '/result/crsEvidence/datum'), axisOrder: field(binding, '/result/axisOrder'),
        contextBinding: field(binding, '/result/sourceContextBinding'),
        originalAtDelivery: field(delivered, '/sourceCrs'), targetAtDelivery: field(delivered, '/finalCrs')
      },
      bindings: find('bindProviderRepresentationsToSource').map(e => field(e, '/result')),
      conversions: find('transform_geometry').map(e => ({
        ...fields(e, '', ['sourceCrs', 'sourceCrsSelection', 'axisOrder', 'targetCrs', 'pointCountMatches',
          'boundaryValid', 'referenceCheckExecuted', 'referencesMatch', 'toleranceDegrees']), rows: field(e, '/rows') })),
      decision: {
        technical: fields(final, '/args/0/body/finalizedCoordinateResult', ['geometry', 'crs', 'technicalKmlReady']),
        evidenceIntegrity: { ...fields(candidate, '/result', ['diagnostics', 'reviewReasons', 'rejectedRows', 'unboundCandidates']),
          hasUnifiedEvidence: field(final, '/result/hasUnifiedEvidence') },
        temporaryOutputs: { ...fields(delivered, '', ['mapReady', 'kmlReady', 'mapStatus', 'kmlStatus']),
          evaluatedMapReady: field(final, '/result/mapReady'), evaluatedKmlReady: field(final, '/result/kmlReady'),
          provisionalKmlReady: field(final, '/result/provisionalKmlReady') },
        userReview: { ...fields(final, '/args/0/body/finalizedCoordinateResult', ['confirmationStatus', 'requiresReview']),
          acknowledgement: unknown('ACKNOWLEDGEMENT_NOT_CAPTURED_BY_PHASE_B') },
        formalAuthorization: { authorized: field(final, '/result/authorized'),
          reasons: field(final, '/result/finalAuthorizationReasons'),
          ...fields(final, '/args/0/body/finalizedCoordinateResult', ['decisionState', 'qualityGateStatus', 'sourceAuthority']) },
        usage: { ...fields(delivered, '', ['usageConsumed', 'userUsageConsumed']),
          transactionOutcome: unknown('BILLING_TRANSACTION_NOT_CAPTURED'),
          userTaskSuccess: unknown('DOWNLOAD_AND_USER_TASK_NOT_CAPTURED') },
        resultIdentity: fields(delivered, '', ['resultId', 'resultRevision', 'geometryHash'])
      },
      // Retain observations including arguments; never hide conflicting prior stages.
      stageRecords: session.events.map(e => ({ stage: e.stage, operation: e.operation,
        sequence: e.sequence, data: field(e, '') })),
      findings: diagnosticFindings({ sessions: [session] })
    };
    function rows(event, base, representation) {
      if (event.retention !== 'COMPLETE') return;
      const value = lookup(event.data, base).value;
      if (!Array.isArray(value)) return;
      result.rowSets.push({ observation: { sessionIndex, eventIndex: session.events.indexOf(event), pointer: base },
        representation, physicalTableIdentity: unknown('PARSER_GROUP_IS_NOT_PHYSICAL_TABLE_IDENTITY'),
        rows: value.map((row, index) => ({ observedOrder: index, originalSourceOrder: field(event, `${base}/${index}/sourceLineNumber`),
          fields: fields(event, `${base}/${index}`, Object.keys(row)),
          rawCharacterSpan: unknown('FIELD_CHARACTER_SPAN_NOT_CAPTURED'),
          visualValueAccuracy: unknown('NO_INDEPENDENT_IMAGE_TRANSCRIPTION') })) });
    }
    for (const e of session.events) {
      if (['extractRecognitionCandidateEvidence', 'adaptRecognitionTableCandidates'].includes(e.operation)) rows(e, '/result/candidateCoordinates', 'UNIFIED_CANDIDATES');
      if (e.operation === 'normalizeProviderDmsReviewResult' && e.retention === 'COMPLETE') {
        (e.data.result?.candidateGroups || []).forEach((_, i) => rows(e, `/result/candidateGroups/${i}/rows`, 'DMS_GROUP'));
        rows(e, '/result/unboundCandidates', 'DMS_UNBOUND');
      }
      if (['extractProviderProjectedCoordinateEvidence', 'bindProjectedEvidenceToSourceContext'].includes(e.operation)) {
        rows(e, '/result/rows', e.operation === 'extractProviderProjectedCoordinateEvidence' ? 'PROJECTED_EXTRACTED' : 'PROJECTED_BOUND');
      }
      if (e.operation === 'bindProviderRepresentationsToSource') {
        rows(e, '/result/rows', 'PRODUCT_RECONCILED'); rows(e, '/result/sourceEvidence/rows', 'LOCAL_SOURCE_TABLE');
      }
    }
    contract.sessions.push(result);
  }
  return contract;
}

function contractFindings(contract, diagnostics) {
  const findings = [];
  for (const [i, session] of contract.sessions.entries()) {
    const events = diagnostics.sessions[i].events;
    const report = (classification, condition, field, evidence, rowIndex = null) =>
      findings.push({ sessionIndex: i, classification, condition, field, rowIndex, evidence });
    if (!session.captureComplete) report('UNKNOWN', 'CAPTURE_INCOMPLETE', 'captureComplete', null);
    for (const name of ['regionId', 'tableId']) if (session.identity[name].status === 'UNKNOWN') {
      report('UNKNOWN', 'PHYSICAL_SCOPE_NOT_ESTABLISHED', `identity.${name}`, session.identity[name].source);
    }
    if (session.rawEvidence.length === 0) report('UNKNOWN', 'RAW_RESPONSE_NOT_CAPTURED', 'rawEvidence', null);
    const unified = events.filter(e => ['extractRecognitionCandidateEvidence', 'adaptRecognitionTableCandidates'].includes(e.operation) && e.retention === 'COMPLETE').at(-1);
    const grouped = events.filter(e => e.operation === 'normalizeProviderDmsReviewResult' && e.retention === 'COMPLETE').at(-1);
    if (unified && grouped && Array.isArray(unified.data.result?.candidateCoordinates)
      && typeof grouped.data.result?.candidatePointCount === 'number'
      && unified.data.result.candidateCoordinates.length !== grouped.data.result.candidatePointCount) {
      report('EXPLAINED_DIFFERENCE', 'PARSER_COVERAGE_DIFFERS', 'candidatePointCount', {
        stages: [grouped.stage, unified.stage], grouped: grouped.data.result.candidatePointCount,
        unified: unified.data.result.candidateCoordinates.length,
        explanation: 'Distinct existing parser entry contracts; not a new selection or physical row-count proof'
      });
    }
    for (const e of events) {
      if (e.retention !== 'COMPLETE') continue;
      if (e.operation === 'bindProjectedEvidenceToSourceContext') {
        const args = e.data.args?.[0];
        const image = args?.imageIdentity?.image_sha256, context = args?.sourceContextProvenance?.image_sha256;
        if (image && context && image !== context) report('EXPLAINED_CONFLICT', 'IMAGE_IDENTITY_MISMATCH',
          'binding.image_sha256', { stage: e.stage, event: e.sequence });
        for (const name of ['crsConflict', 'axisConflict']) if (e.data.result?.sourceContextBinding?.[name] === true) {
          report('EXPLAINED_CONFLICT', name === 'crsConflict' ? 'CRS_CONFLICT' : 'AXIS_CONFLICT',
            `crs.contextBinding.${name}`, { stage: e.stage, event: e.sequence });
        }
      }
    }
    const outputs = session.decision.temporaryOutputs;
    for (const kind of ['Map', 'Kml']) {
      const delivered = outputs[`${kind.toLowerCase()}Ready`], evaluated = outputs[`evaluated${kind}Ready`];
      if (delivered.status === 'OBSERVED' && evaluated.status === 'OBSERVED' && delivered.value !== evaluated.value) {
        report('UNEXPLAINED_SAFETY_DIFFERENCE', 'DELIVERY_DECISION_MISMATCH', `decision.temporaryOutputs.${kind}`, [evaluated.source, delivered.source]);
      }
    }
    const deliveries = events.filter(e => e.stage === 'delivered_result' && e.retention === 'COMPLETE');
    if (deliveries.length > 1) for (const field of ['resultId', 'resultRevision', 'geometryHash']) {
      const values = deliveries.filter(e => Object.hasOwn(e.data, field)).map(e => e.data[field]);
      if (new Set(values.map(v => JSON.stringify(v))).size > 1) report('EXPLAINED_CONFLICT',
        'MULTIPLE_DELIVERY_IDENTITIES', `decision.resultIdentity.${field}`, deliveries.map(e => e.sequence));
    }
    // A recorded user acknowledgement never supplies formal authorization.
    if (session.decision.formalAuthorization.authorized.value === true
      && session.decision.formalAuthorization.decisionState.value === 'REVIEW_REQUIRED') {
      report('UNEXPLAINED_SAFETY_DIFFERENCE', 'REVIEW_PROMOTED_TO_AUTHORITY', 'decision.formalAuthorization', null);
    }
    for (const rowSet of session.rowSets) {
      // Metadata explicitly states these missing links, without merging by count/order.
      if (rowSet.rows.length) report('UNKNOWN', 'ROW_FIELD_ORIGINAL_SPANS_NOT_CAPTURED', 'rowSets.rawCharacterSpan', rowSet.observation);
    }
  }
  return findings;
}

// Independent current-product replay validates stage outputs. Lossless adapter
// comparisons and cross-stage checks are reported separately from replay success.
export function compareShadow(bundle, shadow = adaptDiagnosticBundle(bundle)) {
  if (!bundle) return { status: 'UNKNOWN', reason: 'HISTORICAL_RAW_RESPONSE_NOT_AVAILABLE',
    realProviderCalls: 0, productionReproduction: 'NOT_ESTABLISHED', differences: [], findings: [] };
  const expected = adaptDiagnosticBundle(bundle);
  const diagnostics = inspectDiagnosticArtifact(bundle.diagnostics);
  const replay = replayDiagnosticBundle(bundle);
  const differences = leafDifferences(expected, shadow).map(field => ({
    classification: 'UNEXPLAINED_ADAPTER_DIFFERENCE', field, condition: 'LOSSLESS_OBSERVATION_PROJECTION_CHANGED'
  }));
  const findings = contractFindings(expected, diagnostics);
  const unexplained = differences.length + findings.filter(f => f.classification === 'UNEXPLAINED_SAFETY_DIFFERENCE').length
    + replay.operations.filter(op => op.status === 'DIFFERENT').length;
  const allSessionsHaveRaw = expected.sessions.length > 0 && expected.sessions.every(s => s.rawEvidence.length > 0);
  return { schemaVersion: 'coordinate_shadow_comparison_v1', status: unexplained ? 'DIFFERENT'
    : replay.status === 'MATCHED_CAPTURED_STAGES' && allSessionsHaveRaw ? 'MATCHED_OBSERVATIONS' : replay.status === 'PARTIAL' ? 'PARTIAL' : 'UNKNOWN',
    meaning: 'No new selection, output permission or billing decision is made',
    productionReproduction: 'NOT_ESTABLISHED', realProviderCalls: 0,
    replayStatus: replay.status, replayReason: replay.reason, operations: replay.operations,
    differences, unexplainedSafetyDifferences: unexplained, findings,
    recordedChecks: expected.sessions.flatMap(s => s.findings),
    unknownsAreNotApproval: true };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  for (const name of Object.keys(process.env)) if (/PROVIDER|SUPABASE|ALIYUN|DASHSCOPE|OPENAI|ANTHROPIC|GEMINI|API_KEY|API_TOKEN|SECRET|USAGE|PASSWORD/i.test(name)) process.env[name] = '';
  const args = process.argv.slice(2);
  let bundle = null;
  if (args.length && args[0] !== '--missing-history') {
    if (args[0] !== '--input' || !args[1] || ![2, 4].includes(args.length)
      || (args.length === 4 && args[2] !== '--out')) throw new Error('USE_INPUT_AND_OPTIONAL_OUT');
    for (const local of [args[1], args[3]].filter(Boolean)) if (/^(?:https?:|data:|\\\\|\/\/)/i.test(local)) throw new Error('LOCAL_FILES_ONLY');
    const input = path.resolve(args[1]);
    if (!statSync(input).isFile() || statSync(input).size > 8 * 1024 * 1024) throw new Error('INPUT_SIZE_LIMIT');
    bundle = JSON.parse(readFileSync(input, 'utf8'));
  } else if (args.length > 1) throw new Error('INVALID_ARGUMENTS');
  const contract = adaptDiagnosticBundle(bundle), comparison = compareShadow(bundle, contract);
  if (args[3]) writeFileSync(path.resolve(args[3]), JSON.stringify({ contract, comparison }, null, 2), { flag: 'wx', mode: 0o600 });
  // stdout is summary only: no raw coordinates, source text or account information.
  console.log(JSON.stringify({ status: comparison.status, replayStatus: comparison.replayStatus,
    unexplainedSafetyDifferences: comparison.unexplainedSafetyDifferences,
    differenceCount: comparison.differences.length, unknownCount: comparison.findings.filter(f => f.classification === 'UNKNOWN').length,
    productionReproduction: comparison.productionReproduction, realProviderCalls: 0 }, null, 2));
  if (comparison.status === 'DIFFERENT') process.exitCode = 1;
}
