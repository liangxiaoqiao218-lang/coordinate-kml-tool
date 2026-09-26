import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import sharp from 'sharp';

import {
  CoordinateAgentToolRegistry,
  LocalImageWorkspace,
  createSharpImageOperations,
  registerGenericImageTools,
  runReadOnlyImageEvaluation,
} from '../server/coordinate-agent/index.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const source = await sharp({
  create: { width: 100, height: 80, channels: 3, background: '#ffffff' },
}).png().toBuffer();
const workspace = new LocalImageWorkspace();
const imageRef = workspace.registerBuffer(source, { label: 'synthetic' });
let terminated = false;
const operations = createSharpImageOperations({
  workspace,
  createOcrWorker: async () => ({
    recognize: async image => {
      assert.equal(Buffer.isBuffer(image), true);
      return { data: { text: 'supporting text', confidence: 82 } };
    },
    terminate: async () => { terminated = true; },
  }),
});

const crop = await operations.cropRegion({ imageRef, bbox: [0.1, 0.1, 0.6, 0.6] });
assert.deepEqual({ width: crop.width, height: crop.height }, { width: 50, height: 40 });
const zoom = await operations.zoomRegion({ imageRef, bbox: [0, 0, 0.5, 0.5], scale: 2 });
assert.deepEqual({ width: zoom.width, height: zoom.height }, { width: 100, height: 80 });
const rotate = await operations.rotateImage({ imageRef, degrees: 90 });
assert.deepEqual({ width: rotate.width, height: rotate.height }, { width: 80, height: 100 });
const ocr = await operations.localOcrRegion({ imageRef, bbox: [0, 0, 1, 1] });
assert.equal(ocr.text, 'supporting text');
assert.equal(ocr.confidence, 82);
assert.equal(ocr.authoritative, false);
assert.equal(terminated, true);

const registry = new CoordinateAgentToolRegistry();
registerGenericImageTools(registry, operations);
const registryCrop = await registry.execute({
  toolName: 'crop_region',
  args: { imageRef, bbox: [0, 0, 1, 1] },
});
assert.equal(registryCrop.width, 100);
assert.throws(() => workspace.resolve('https://example.invalid/image.png'), /Only local-image references/);
await assert.rejects(() => workspace.registerFile('https://example.invalid/image.png'), /Only a local image path/);

const evaluation = await runReadOnlyImageEvaluation({
  manifestPath: path.join(root, 'regression-samples', 'coordinate-agent-evaluation-manifest.v1.json'),
});
assert.equal(evaluation.caseCount, 14);
assert.equal(evaluation.providerCallCount, 0);
assert.equal(evaluation.ocrCallCount, 0);
assert.equal(evaluation.sourceMutationCount, 0);
assert.equal(evaluation.results.every(result => result.sourceUnchanged), true);

console.log('coordinate-agent-phase02-regression: PASS');
