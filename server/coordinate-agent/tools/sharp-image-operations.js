import sharp from 'sharp';

import { runCancellableOcrJob } from '../../recognition/cancellable-ocr.js';

function normalizedBbox(value) {
  if (!Array.isArray(value) || value.length !== 4) throw new Error('bbox must contain four normalized values');
  const bbox = value.map(Number);
  if (bbox.some(number => !Number.isFinite(number) || number < 0 || number > 1)
    || bbox[0] >= bbox[2]
    || bbox[1] >= bbox[3]) {
    throw new Error('bbox must be a non-empty normalized rectangle');
  }
  return bbox;
}

async function autoOrient(buffer) {
  return sharp(buffer, { failOn: 'warning' })
    .rotate()
    .png()
    .toBuffer({ resolveWithObject: true });
}

function pixelRegion(bbox, width, height) {
  const [x1, y1, x2, y2] = normalizedBbox(bbox);
  const left = Math.min(width - 1, Math.max(0, Math.floor(x1 * width)));
  const top = Math.min(height - 1, Math.max(0, Math.floor(y1 * height)));
  const right = Math.min(width, Math.max(left + 1, Math.ceil(x2 * width)));
  const bottom = Math.min(height, Math.max(top + 1, Math.ceil(y2 * height)));
  return { left, top, width: right - left, height: bottom - top };
}

function registerOutput(workspace, sourceRef, label, result) {
  const imageRef = workspace.registerBuffer(result.data, { label, derivedFrom: sourceRef });
  return Object.freeze({
    imageRef,
    width: result.info.width,
    height: result.info.height,
    bytes: result.data.length,
    mimeType: 'image/png',
  });
}

export function createSharpImageOperations({
  workspace,
  createOcrWorker = null,
  ocrTimeoutMs = 15_000,
  maxOutputDimension = 4096,
} = {}) {
  if (typeof workspace?.resolve !== 'function' || typeof workspace?.registerBuffer !== 'function') {
    throw new Error('A LocalImageWorkspace is required');
  }

  const cropRegion = async ({ imageRef, bbox }) => {
    const source = workspace.resolve(imageRef);
    const oriented = await autoOrient(source.buffer);
    const region = pixelRegion(bbox, oriented.info.width, oriented.info.height);
    const result = await sharp(oriented.data).extract(region).png().toBuffer({ resolveWithObject: true });
    return registerOutput(workspace, imageRef, `${source.label}:crop`, result);
  };

  const zoomRegion = async ({ imageRef, bbox, scale = 2 }) => {
    const source = workspace.resolve(imageRef);
    const oriented = await autoOrient(source.buffer);
    const region = pixelRegion(bbox, oriented.info.width, oriented.info.height);
    const factor = Math.min(8, Math.max(1, Number(scale) || 2));
    const width = Math.min(maxOutputDimension, Math.max(1, Math.round(region.width * factor)));
    const height = Math.min(maxOutputDimension, Math.max(1, Math.round(region.height * factor)));
    const result = await sharp(oriented.data)
      .extract(region)
      .resize({ width, height, fit: 'fill', kernel: sharp.kernel.lanczos3 })
      .png()
      .toBuffer({ resolveWithObject: true });
    return registerOutput(workspace, imageRef, `${source.label}:zoom`, result);
  };

  const rotateImage = async ({ imageRef, degrees }) => {
    const source = workspace.resolve(imageRef);
    const angle = Number(degrees);
    if (!Number.isFinite(angle)) throw new Error('degrees must be finite');
    const oriented = await autoOrient(source.buffer);
    const result = await sharp(oriented.data)
      .rotate(angle, { background: '#ffffff' })
      .flatten({ background: '#ffffff' })
      .png()
      .toBuffer({ resolveWithObject: true });
    return registerOutput(workspace, imageRef, `${source.label}:rotate`, result);
  };

  const localOcrRegion = async ({ imageRef, bbox }) => {
    if (typeof createOcrWorker !== 'function') throw new Error('Local OCR worker is not configured');
    const cropped = await cropRegion({ imageRef, bbox });
    const image = workspace.resolve(cropped.imageRef).buffer;
    const recognition = await runCancellableOcrJob({
      createWorker: createOcrWorker,
      image,
      timeoutMs: ocrTimeoutMs,
    });
    return Object.freeze({
      text: String(recognition?.data?.text || ''),
      confidence: Number.isFinite(Number(recognition?.data?.confidence))
        ? Number(recognition.data.confidence)
        : null,
      authoritative: false,
      source: 'local_ocr_supporting_evidence',
      imageRef: cropped.imageRef,
    });
  };

  const detectTableStructure = async ({ imageRef, bbox = [0, 0, 1, 1] }) => {
    const cropped = await cropRegion({ imageRef, bbox });
    return Object.freeze({
      imageRef: cropped.imageRef,
      width: cropped.width,
      height: cropped.height,
      status: 'visual_region_prepared',
      rows: null,
      columns: null,
      authoritative: false,
    });
  };

  return Object.freeze({ cropRegion, zoomRegion, rotateImage, localOcrRegion, detectTableStructure });
}
