import { createHash } from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

import sharp from 'sharp';

import { LocalImageWorkspace } from './local-image-workspace.js';
import { createSharpImageOperations } from './tools/sharp-image-operations.js';

function withinRoot(root, candidate) {
  const relative = path.relative(root, candidate);
  return relative && !relative.startsWith('..') && !path.isAbsolute(relative);
}

function sha256(buffer) {
  return createHash('sha256').update(buffer).digest('hex');
}

export async function runReadOnlyImageEvaluation({ manifestPath, sampleLimit = Infinity } = {}) {
  const absoluteManifest = path.resolve(String(manifestPath || ''));
  const evaluationRoot = path.dirname(absoluteManifest);
  const manifest = JSON.parse(await fs.readFile(absoluteManifest, 'utf8'));
  if (manifest?.schemaVersion !== 'coordinate-agent-evaluation-manifest/v1' || !Array.isArray(manifest.cases)) {
    throw new Error('Evaluation manifest is invalid');
  }
  const workspace = new LocalImageWorkspace();
  const operations = createSharpImageOperations({ workspace });
  const results = [];
  const cases = manifest.cases.slice(0, Math.max(0, Number(sampleLimit) || 0));

  for (const entry of cases) {
    if (!entry || Object.keys(entry).sort().join(',') !== 'id,image') {
      throw new Error('Evaluation cases may contain only id and image');
    }
    const sourcePath = path.resolve(evaluationRoot, String(entry.image || ''));
    if (!withinRoot(evaluationRoot, sourcePath)) throw new Error('Evaluation image escapes the manifest root');
    const before = await fs.readFile(sourcePath);
    const beforeHash = sha256(before);
    const imageRef = workspace.registerBuffer(before, { label: String(entry.id) });
    const metadata = await sharp(before, { failOn: 'warning' }).metadata();
    const crop = await operations.cropRegion({ imageRef, bbox: [0.1, 0.1, 0.9, 0.9] });
    const zoom = await operations.zoomRegion({ imageRef, bbox: [0.25, 0.25, 0.75, 0.75], scale: 1.5 });
    const rotate = await operations.rotateImage({ imageRef, degrees: 0 });
    const afterHash = sha256(await fs.readFile(sourcePath));
    if (beforeHash !== afterHash) throw new Error(`Evaluation source was modified: ${entry.id}`);
    results.push(Object.freeze({
      id: String(entry.id),
      sourceHash: beforeHash,
      sourceWidth: Number(metadata.width || 0),
      sourceHeight: Number(metadata.height || 0),
      crop: Object.freeze({ width: crop.width, height: crop.height }),
      zoom: Object.freeze({ width: zoom.width, height: zoom.height }),
      rotate: Object.freeze({ width: rotate.width, height: rotate.height }),
      sourceUnchanged: true,
    }));
  }

  return Object.freeze({
    schemaVersion: 'coordinate-agent-read-only-evaluation/v1',
    caseCount: results.length,
    providerCallCount: 0,
    ocrCallCount: 0,
    sourceMutationCount: 0,
    workspaceImageCount: workspace.size,
    results: Object.freeze(results),
  });
}
