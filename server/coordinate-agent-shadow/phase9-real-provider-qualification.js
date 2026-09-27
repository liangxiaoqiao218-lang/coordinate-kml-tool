import { createHash } from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

import {
  CoordinateAgentMultimodalProviderAdapter,
  CoordinateAgentToolRegistry,
  CoordinateIntelligenceAgentKernel,
  LocalImageWorkspace,
  createSharpImageOperations,
  registerCoordinateMathTools,
  registerGenericImageTools,
} from '../coordinate-agent/index.js';
import {
  DashScopeOpenAICompatibleTransport,
  normalizeCoordinateAgentProviderTimeoutMs,
} from '../coordinate-agent-transports/dashscope-openai-compatible-transport.js';
import { createCoordinateAgentProviderImageResolver } from '../coordinate-agent-transports/provider-image-budget.js';
import { summarizeCandidateRepresentation } from '../coordinate-agent/candidate-representation-summary.js';

export const PHASE9_QUALIFICATION_TIMEOUT_MS = 150_000;

function sumUsage(telemetry) {
  return telemetry.reduce((totals, item) => ({
    inputTokens: totals.inputTokens + Number(item.usage?.inputTokens || 0),
    outputTokens: totals.outputTokens + Number(item.usage?.outputTokens || 0),
    totalTokens: totals.totalTokens + Number(item.usage?.totalTokens || 0),
  }), { inputTokens: 0, outputTokens: 0, totalTokens: 0 });
}

function summarizeProjectionVerification(toolResults) {
  const result = toolResults.find(item => (
    item.actionId === 'safety-projection-transform'
    && item.toolName === 'projected_coordinate_transform_check'
  ));
  const output = result?.ok === true ? result.output : null;
  const diagnostic = output?.identityDiagnostic || null;
  return Object.freeze({
    attempted: Boolean(result),
    crsStatus: String(output?.crsStatus || 'not_attempted'),
    crsId: typeof output?.crsId === 'string' ? output.crsId : null,
    registryMatched: output?.registryMatched === true,
    axisStatus: String(output?.axisStatus || 'not_attempted'),
    pointCount: Number(output?.pointCount || 0),
    transformedPointCount: Number(output?.transformedPointCount || 0),
    roundTripVerifiedPointCount: Number(output?.roundTripVerifiedPointCount || 0),
    forwardStatus: String(output?.forwardStatus || 'not_attempted'),
    inverseStatus: String(output?.inverseStatus || 'not_attempted'),
    spatialStatus: String(output?.spatialStatus || 'not_attempted'),
    toleranceMeters: Number.isFinite(output?.toleranceMeters) ? output.toleranceMeters : null,
    maximumRoundTripErrorMeters: Number.isFinite(output?.maximumRoundTripErrorMeters)
      ? output.maximumRoundTripErrorMeters
      : null,
    identityDiagnostic: diagnostic ? Object.freeze({
      namePresent: diagnostic.namePresent === true,
      epsgPresent: diagnostic.epsgPresent === true,
      nameSyntax: String(diagnostic.nameSyntax || 'unavailable'),
      epsgSyntax: String(diagnostic.epsgSyntax || 'unavailable'),
      nameCrsId: typeof diagnostic.nameCrsId === 'string' ? diagnostic.nameCrsId : null,
      nameQualifiedEpsgId: typeof diagnostic.nameQualifiedEpsgId === 'string'
        ? diagnostic.nameQualifiedEpsgId
        : null,
      epsgCrsId: typeof diagnostic.epsgCrsId === 'string' ? diagnostic.epsgCrsId : null,
      normalizedCrsId: typeof diagnostic.normalizedCrsId === 'string' ? diagnostic.normalizedCrsId : null,
      normalizationStatus: String(diagnostic.normalizationStatus || 'unavailable'),
      identityConsistency: String(diagnostic.identityConsistency || 'unavailable'),
    }) : null,
  });
}

export async function runPhase9RealProviderQualification({
  root,
  caseId = 'eval-001',
  maxProviderCalls = 2,
  env = process.env,
  fetchImpl = fetch,
} = {}) {
  if (![1, 2].includes(Number(maxProviderCalls))) throw new Error('Phase 9 Provider call limit must be 1 or 2');
  const manifestPath = path.join(root, 'regression-samples', 'coordinate-agent-evaluation-manifest.v1.json');
  const manifest = JSON.parse(await fs.readFile(manifestPath, 'utf8'));
  const evaluationCase = manifest.cases.find(item => item.id === caseId);
  if (!evaluationCase) throw new Error('Registered Phase 9 evaluation case not found');
  const evaluationRoot = path.dirname(manifestPath);
  const imagePath = path.resolve(evaluationRoot, evaluationCase.image);
  const relative = path.relative(evaluationRoot, imagePath);
  if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) {
    throw new Error('Phase 9 evaluation image escapes its root');
  }
  const source = await fs.readFile(imagePath);
  const sourceHash = createHash('sha256').update(source).digest('hex');
  const requestTimeoutMs = normalizeCoordinateAgentProviderTimeoutMs(
    env.COORDINATE_AGENT_PROVIDER_TIMEOUT_MS || PHASE9_QUALIFICATION_TIMEOUT_MS,
  );
  const workspace = new LocalImageWorkspace();
  const imageRef = workspace.registerBuffer(source, { label: 'phase9-registered-evaluation' });
  const providerImageResolver = createCoordinateAgentProviderImageResolver({
    resolveImage: ref => workspace.resolve(ref),
  });
  const rawTransport = new DashScopeOpenAICompatibleTransport({
    fetchImpl,
    getAccessToken: () => env.ALIYUN_API_KEY || env.DASHSCOPE_API_KEY || '',
    endpoint: env.ALIYUN_BASE_URL || env.DASHSCOPE_BASE_URL || 'https://dashscope.aliyuncs.com/compatible-mode/v1',
    model: env.ALIYUN_VISION_MODEL || env.DASHSCOPE_VISION_MODEL || 'qwen3.8-flash',
    timeoutMs: requestTimeoutMs,
    maxTokens: 8_000,
  });
  let providerAttempts = 0;
  const transport = Object.freeze({
    async complete(request) {
      if (providerAttempts >= Number(maxProviderCalls)) throw new Error('Phase 9 Provider call budget exhausted');
      providerAttempts += 1;
      return rawTransport.complete(request);
    },
  });
  const adapter = new CoordinateAgentMultimodalProviderAdapter({
    transport,
    resolveImage: providerImageResolver.resolve,
  });
  const registry = new CoordinateAgentToolRegistry();
  registerGenericImageTools(registry, createSharpImageOperations({ workspace }));
  registerCoordinateMathTools(registry);
  const result = await new CoordinateIntelligenceAgentKernel({
    providerAdapter: adapter,
    toolRegistry: registry,
    maxProviderCalls: Number(maxProviderCalls),
    maxIterations: 3,
  }).run({ imageRef, requestId: `shadow:phase9:${caseId}` });
  const afterHash = createHash('sha256').update(await fs.readFile(imagePath)).digest('hex');
  const telemetry = rawTransport.telemetry();
  const imageMetrics = providerImageResolver.metrics();
  const pointCount = (result.coordinateResult?.groups || [])
    .reduce((sum, group) => sum + group.points.length, 0);
  return Object.freeze({
    schemaVersion: 'coordinate-agent-phase9-qualification/v1',
    status: 'COMPLETED',
    terminalState: result.terminalState,
    candidatePointCount: pointCount,
    verifiedPointCount: result.evidence.toolResults.filter(item => (
      String(item.actionId || '').startsWith('safety-math-')
      && item.toolName === 'coordinate_math_check'
      && item.ok === true
      && item.output?.valid === true
    )).length,
    candidateRepresentation: summarizeCandidateRepresentation(result.coordinateResult),
    projectionVerification: summarizeProjectionVerification(result.evidence.toolResults),
    realProviderCallCount: providerAttempts,
    automaticRetryCount: 0,
    requestTimeoutMs,
    imageMetrics: Object.freeze(imageMetrics.map(item => ({ ...item }))),
    usage: Object.freeze(sumUsage(telemetry)),
    billingStatus: telemetry.some(item => item.usageObserved)
      ? 'USAGE_REPORTED_EXACT_BILLING_NOT_VERIFIED'
      : 'UNKNOWN',
    mapAllowed: result.authorization.mapAllowed,
    kmlAllowed: result.authorization.kmlAllowed,
    stateHistory: Object.freeze(result.execution.stateHistory.map(item => ({
      from: item.from,
      to: item.to,
      reason: item.reason,
    }))),
    diagnostics: Object.freeze(result.execution.diagnostics.map(item => ({ ...item }))),
    providerRequests: Object.freeze(telemetry.map(item => ({
      ok: item.ok,
      httpStatus: item.httpStatus,
      errorCode: item.errorCode || null,
      usageObserved: item.usageObserved,
      durationMs: item.durationMs,
      timeoutMs: item.timeoutMs,
      requestBuildDurationMs: item.requestBuildDurationMs,
      requestBodyBytes: item.requestBodyBytes,
      imageBytes: item.imageBytes,
      schemaBytes: item.schemaBytes,
    }))),
    toolCalls: Object.freeze(result.evidence.toolResults.map(item => ({
      toolName: item.toolName,
      ok: item.ok,
    }))),
    validationFailureCodes: Object.freeze(result.evidence.uncertainties.map(item => item.code)),
    sourceUnchanged: sourceHash === afterHash,
  });
}
