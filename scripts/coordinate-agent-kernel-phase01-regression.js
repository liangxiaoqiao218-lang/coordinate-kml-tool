import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  AGENT_STATE,
  CoordinateAgentToolRegistry,
  CoordinateIntelligenceAgentKernel,
  MockCoordinateAgentProviderAdapter,
  assertStrictCoordinateAgentResult,
  buildExistingSafetyArtifacts,
  registerCoordinateMathTools,
  registerGenericImageTools,
} from '../server/coordinate-agent/index.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

function geographicCandidate({ status = 'usable', needsReview = false } = {}) {
  return {
    success: true,
    resultStatus: status,
    displayText: 'P1 | source row',
    coordinateSystem: { kind: 'geographic', name: 'WGS 84', epsg: '4326', status: 'identified' },
    geometryType: 'Point',
    groups: [{ name: 'Observed table', points: [{ label: 'P1', sourceText: 'P1 | source row', x: null, y: null, latitude: 10, longitude: 20, needsReview }] }],
    warnings: needsReview ? ['Direction evidence requires review'] : [],
  };
}

function toolRegistry() {
  const registry = new CoordinateAgentToolRegistry();
  registerGenericImageTools(registry, {
    cropRegion: args => ({ imageRef: `${args.imageRef}#crop`, bbox: args.bbox }),
    zoomRegion: args => ({ imageRef: `${args.imageRef}#zoom`, bbox: args.bbox }),
    rotateImage: args => ({ imageRef: `${args.imageRef}#rotated`, degrees: args.degrees }),
    localOcrRegion: () => ({ text: 'supporting transcription', authoritative: false }),
    detectTableStructure: () => ({ rows: 2, columns: 3 }),
  });
  registerCoordinateMathTools(registry);
  return registry;
}

{
  const adapter = new MockCoordinateAgentProviderAdapter([{
    observation: { regions: [{ id: 'whole-1', kind: 'document', bbox: [0, 0, 1, 1], sourceText: '', semanticRole: 'whole image', confidence: 0.98 }] },
    plan: { rationale: 'The visible evidence is sufficient', actions: [] },
    candidate: geographicCandidate(),
  }]);
  const result = await new CoordinateIntelligenceAgentKernel({ providerAdapter: adapter, toolRegistry: toolRegistry() }).run({ imageRef: 'fixture://clear-table' });
  assert.equal(result.terminalState, AGENT_STATE.CONFIRMED);
  assert.deepEqual(result.authorization, { mapAllowed: true, kmlAllowed: true });
  assert.equal(result.execution.providerCallCount, 1);
  assert.equal(result.execution.toolCallCount, 1);
  assert.deepEqual(result.evidence.toolResults.map(item => item.toolName), ['coordinate_math_check']);
  const artifacts = buildExistingSafetyArtifacts({
    agentResult: result,
    documentRevision: 1,
    recognitionRequestId: '11111111-1111-4111-8111-111111111111',
    name: 'Offline fixture',
  });
  assert.deepEqual(artifacts.map.geometry, { type: 'Point', coordinates: [20, 10] });
  assert.match(artifacts.kml, /<Point>/);
  assert.equal(artifacts.usageAuthority.decisionState, 'AUTO_EXPORT');
}

{
  const adapter = new MockCoordinateAgentProviderAdapter([{
    observation: { regions: [] },
    plan: { rationale: 'Invalid extra field must fail closed', actions: [] },
    unexpectedField: true,
  }]);
  const result = await new CoordinateIntelligenceAgentKernel({ providerAdapter: adapter, toolRegistry: toolRegistry() }).run({ imageRef: 'fixture://invalid-turn' });
  assert.equal(result.terminalState, AGENT_STATE.FAILED_CLOSED);
  assert.deepEqual(result.authorization, { mapAllowed: false, kmlAllowed: false });
  assert.equal(result.evidence.uncertainties[0].code, 'AGENT_TURN_INVALID');
}

{
  const adapter = new MockCoordinateAgentProviderAdapter([
    {
      observation: { regions: [{ id: 'header-1', kind: 'header', bbox: [0.1, 0.1, 0.9, 0.3], sourceText: 'unclear direction', semanticRole: 'column header', confidence: 0.45 }] },
      plan: { rationale: 'Inspect the ambiguous header', actions: [{ id: 'crop-header', toolName: 'crop_region', objective: 'Inspect direction evidence', args: { imageRef: 'fixture://table', bbox: [0.1, 0.1, 0.9, 0.3] } }] },
    },
    {
      observation: { regions: [{ id: 'direction-1', kind: 'direction', bbox: [0.7, 0.1, 0.9, 0.3], sourceText: '?', semanticRole: 'direction marker', confidence: 0.4 }] },
      uncertainties: [{ code: 'DIRECTION_NOT_VISUALLY_RESOLVED', message: 'Direction remains ambiguous', evidenceRegionIds: ['direction-1'], blocking: true }],
      reviewItems: [{ fieldPath: 'coordinateGroups[0].points[0].longitude', question: 'Confirm the visible longitude direction', evidenceRegionIds: ['direction-1'], candidates: ['east', 'west'] }],
      plan: { rationale: 'No further local tool can resolve the evidence', actions: [] },
      candidate: geographicCandidate({ status: 'needs_review', needsReview: true }),
    },
  ]);
  const result = await new CoordinateIntelligenceAgentKernel({ providerAdapter: adapter, toolRegistry: toolRegistry() }).run({ imageRef: 'fixture://table' });
  assert.equal(result.terminalState, AGENT_STATE.REVIEW_REQUIRED);
  assert.deepEqual(result.authorization, { mapAllowed: false, kmlAllowed: false });
  assert.equal(result.execution.providerCallCount, 2);
  assert.equal(result.execution.toolCallCount, 2);
  assert.equal(result.evidence.reviewItems[0].fieldPath, 'coordinateGroups[0].points[0].longitude');
  assert.throws(() => buildExistingSafetyArtifacts({
    agentResult: result,
    documentRevision: 1,
    recognitionRequestId: '22222222-2222-4222-8222-222222222222',
  }), error => error?.code === 'COORDINATE_AGENT_SAFETY_GATE_CLOSED');
}

{
  const adapter = new MockCoordinateAgentProviderAdapter([{
    observation: { regions: [] },
    plan: { rationale: 'No reliable coordinate evidence is visible', actions: [] },
    candidate: { success: false, resultStatus: 'failed', displayText: '', coordinateSystem: { kind: 'unknown', name: null, epsg: null, status: 'unknown' }, geometryType: 'Unknown', groups: [], warnings: ['No reliable coordinate evidence'] },
  }]);
  const result = await new CoordinateIntelligenceAgentKernel({ providerAdapter: adapter, toolRegistry: toolRegistry() }).run({ imageRef: 'fixture://no-evidence' });
  assert.equal(result.terminalState, AGENT_STATE.FAILED_CLOSED);
  assert.deepEqual(result.authorization, { mapAllowed: false, kmlAllowed: false });
}

assert.throws(() => assertStrictCoordinateAgentResult({ unexpected: true }), /unknown properties/);

await assert.rejects(
  () => toolRegistry().execute({ toolName: 'crop_region', args: { imageRef: 'fixture://x', bbox: [0, 0, 1, 1], unknown: true } }),
  /unknown properties/,
);

const manifestPath = path.join(root, 'regression-samples', 'coordinate-agent-evaluation-manifest.v1.json');
const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
assert.ok(manifest.cases.length >= 10);
for (const entry of manifest.cases) {
  assert.deepEqual(Object.keys(entry).sort(), ['id', 'image']);
  assert.equal(fs.existsSync(path.join(root, 'regression-samples', entry.image)), true, entry.image);
}

const kernelRoot = path.join(root, 'server', 'coordinate-agent');
const source = fs.readdirSync(kernelRoot, { recursive: true })
  .filter(name => /\.js$/i.test(name))
  .map(name => fs.readFileSync(path.join(kernelRoot, name), 'utf8'))
  .join('\n');
for (const forbidden of ['knownRows', 'filename_keyword', 'countryProfile', 'fixedCoordinate', 'fixtureAnswer']) {
  assert.equal(source.includes(forbidden), false, `forbidden specialized dependency: ${forbidden}`);
}

console.log('coordinate-agent-kernel-phase01-regression: PASS');
