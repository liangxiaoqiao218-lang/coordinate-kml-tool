import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { runReadOnlyImageEvaluation } from '../server/coordinate-agent/evaluation-runner.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const result = await runReadOnlyImageEvaluation({
  manifestPath: path.join(root, 'regression-samples', 'coordinate-agent-evaluation-manifest.v1.json'),
});

console.log(JSON.stringify({
  schemaVersion: result.schemaVersion,
  caseCount: result.caseCount,
  providerCallCount: result.providerCallCount,
  ocrCallCount: result.ocrCallCount,
  sourceMutationCount: result.sourceMutationCount,
  workspaceImageCount: result.workspaceImageCount,
  cases: result.results.map(item => ({
    id: item.id,
    sourceWidth: item.sourceWidth,
    sourceHeight: item.sourceHeight,
    crop: item.crop,
    zoom: item.zoom,
    rotate: item.rotate,
    sourceUnchanged: item.sourceUnchanged,
  })),
}, null, 2));
