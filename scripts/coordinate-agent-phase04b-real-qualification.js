import { createHash } from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  CoordinateAgentMultimodalProviderAdapter,
  CoordinateAgentToolRegistry,
  CoordinateIntelligenceAgentKernel,
  LocalImageWorkspace,
  createSharpImageOperations,
  registerCoordinateMathTools,
  registerGenericImageTools,
} from '../server/coordinate-agent/index.js';
import { DashScopeOpenAICompatibleTransport } from '../server/coordinate-agent-transports/dashscope-openai-compatible-transport.js';

if (!process.argv.includes('--execute-real-provider')) {
  throw new Error('Real Provider execution requires --execute-real-provider');
}
const maxProviderCalls = process.argv.includes('--single-provider-call') ? 1 : 2;

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const manifestPath = path.join(root, 'regression-samples', 'coordinate-agent-evaluation-manifest.v1.json');
const manifest = JSON.parse(await fs.readFile(manifestPath, 'utf8'));
const caseId = 'eval-001';
const evaluationCase = manifest.cases.find(item => item.id === caseId);
if (!evaluationCase) throw new Error(`Registered evaluation case not found: ${caseId}`);
const evaluationRoot = path.dirname(manifestPath);
const imagePath = path.resolve(evaluationRoot, evaluationCase.image);
const relative = path.relative(evaluationRoot, imagePath);
if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) throw new Error('Evaluation image escapes its root');
const source = await fs.readFile(imagePath);
const sourceHash = createHash('sha256').update(source).digest('hex');

const getAccessToken = () => process.env.ALIYUN_API_KEY || process.env.DASHSCOPE_API_KEY || '';
const endpoint = process.env.ALIYUN_BASE_URL
  || process.env.DASHSCOPE_BASE_URL
  || 'https://dashscope.aliyuncs.com/compatible-mode/v1';
const model = process.env.ALIYUN_VISION_MODEL || process.env.DASHSCOPE_VISION_MODEL || 'qwen3.8-flash';
const workspace = new LocalImageWorkspace();
const imageRef = workspace.registerBuffer(source, { label: caseId });
const transport = new DashScopeOpenAICompatibleTransport({
  fetchImpl: fetch,
  getAccessToken,
  endpoint,
  model,
  timeoutMs: 55_000,
  maxTokens: 8_000,
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
  maxProviderCalls,
  maxIterations: 3,
}).run({ imageRef, requestId: 'qualification:phase04b:eval-001' });

const afterHash = createHash('sha256').update(await fs.readFile(imagePath)).digest('hex');
if (sourceHash !== afterHash) throw new Error('Evaluation source image was modified');
const telemetry = transport.telemetry();
const pointCount = (result.coordinateResult?.groups || [])
  .reduce((sum, group) => sum + group.points.length, 0);
const usage = telemetry.reduce((totals, item) => ({
  inputTokens: totals.inputTokens + (item.usage?.inputTokens || 0),
  outputTokens: totals.outputTokens + (item.usage?.outputTokens || 0),
  totalTokens: totals.totalTokens + (item.usage?.totalTokens || 0),
}), { inputTokens: 0, outputTokens: 0, totalTokens: 0 });

console.log(JSON.stringify({
  schemaVersion: 'coordinate-agent-provider-qualification/v1',
  caseId,
  model,
  providerCallLimit: maxProviderCalls,
  realProviderCallCount: telemetry.length,
  automaticRetryCount: 0,
  providerRequests: telemetry.map(item => ({
    ok: item.ok,
    httpStatus: item.httpStatus,
    durationMs: item.durationMs,
    usageObserved: item.usageObserved,
  })),
  usage,
  billingStatus: telemetry.some(item => item.usageObserved)
    ? 'USAGE_REPORTED_EXACT_BILLING_NOT_VERIFIED'
    : 'UNKNOWN',
  terminalState: result.terminalState,
  candidatePointCount: pointCount,
  mapAllowed: result.authorization.mapAllowed,
  kmlAllowed: result.authorization.kmlAllowed,
  stateHistory: result.execution.stateHistory,
  toolCalls: result.evidence.toolResults.map(item => ({
    actionId: item.actionId,
    toolName: item.toolName,
    ok: item.ok,
  })),
  validationFailures: result.evidence.uncertainties.map(item => ({
    code: item.code,
    message: item.message,
    blocking: item.blocking,
  })),
  reviewFieldPaths: result.evidence.reviewItems.map(item => item.fieldPath),
  sourceUnchanged: true,
}, null, 2));
