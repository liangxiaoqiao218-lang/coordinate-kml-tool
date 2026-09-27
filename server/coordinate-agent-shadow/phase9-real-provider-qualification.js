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
import { DashScopeOpenAICompatibleTransport } from '../coordinate-agent-transports/dashscope-openai-compatible-transport.js';

function sumUsage(telemetry) {
  return telemetry.reduce((totals, item) => ({
    inputTokens: totals.inputTokens + Number(item.usage?.inputTokens || 0),
    outputTokens: totals.outputTokens + Number(item.usage?.outputTokens || 0),
    totalTokens: totals.totalTokens + Number(item.usage?.totalTokens || 0),
  }), { inputTokens: 0, outputTokens: 0, totalTokens: 0 });
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
  const workspace = new LocalImageWorkspace();
  const imageRef = workspace.registerBuffer(source, { label: 'phase9-registered-evaluation' });
  const rawTransport = new DashScopeOpenAICompatibleTransport({
    fetchImpl,
    getAccessToken: () => env.ALIYUN_API_KEY || env.DASHSCOPE_API_KEY || '',
    endpoint: env.ALIYUN_BASE_URL || env.DASHSCOPE_BASE_URL || 'https://dashscope.aliyuncs.com/compatible-mode/v1',
    model: env.ALIYUN_VISION_MODEL || env.DASHSCOPE_VISION_MODEL || 'qwen3.8-flash',
    timeoutMs: 55_000,
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
    resolveImage: ref => workspace.resolve(ref),
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
  const pointCount = (result.coordinateResult?.groups || [])
    .reduce((sum, group) => sum + group.points.length, 0);
  return Object.freeze({
    schemaVersion: 'coordinate-agent-phase9-qualification/v1',
    status: 'COMPLETED',
    terminalState: result.terminalState,
    candidatePointCount: pointCount,
    realProviderCallCount: providerAttempts,
    automaticRetryCount: 0,
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
    }))),
    toolCalls: Object.freeze(result.evidence.toolResults.map(item => ({
      toolName: item.toolName,
      ok: item.ok,
    }))),
    validationFailureCodes: Object.freeze(result.evidence.uncertainties.map(item => item.code)),
    sourceUnchanged: sourceHash === afterHash,
  });
}
