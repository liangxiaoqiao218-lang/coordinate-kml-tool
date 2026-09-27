import sharp from 'sharp';

export const DEFAULT_PROVIDER_IMAGE_BYTE_BUDGET = 1_500_000;
export const DEFAULT_PROVIDER_JPEG_QUALITY = 88;

function normalizeResolvedImage(value) {
  const buffer = Buffer.isBuffer(value) ? value : value?.buffer;
  if (!Buffer.isBuffer(buffer) || buffer.length === 0) {
    throw new Error('Provider image resolver returned no bytes');
  }
  return {
    buffer,
    mimeType: String(value?.mimeType || 'image/png'),
  };
}

export async function prepareCoordinateAgentProviderImage(value, {
  byteBudget = DEFAULT_PROVIDER_IMAGE_BYTE_BUDGET,
  jpegQuality = DEFAULT_PROVIDER_JPEG_QUALITY,
} = {}) {
  const source = normalizeResolvedImage(value);
  const boundedBudget = Math.max(64 * 1024, Math.trunc(Number(byteBudget) || DEFAULT_PROVIDER_IMAGE_BYTE_BUDGET));
  const boundedQuality = Math.min(95, Math.max(70, Math.trunc(Number(jpegQuality) || DEFAULT_PROVIDER_JPEG_QUALITY)));
  const metadata = await sharp(source.buffer, { failOn: 'warning' }).metadata();
  if (source.buffer.length <= boundedBudget) {
    return Object.freeze({
      buffer: Buffer.from(source.buffer),
      mimeType: source.mimeType,
      metrics: Object.freeze({
        sourceBytes: source.buffer.length,
        transmittedBytes: source.buffer.length,
        optimized: false,
        width: Number(metadata.width || 0),
        height: Number(metadata.height || 0),
      }),
    });
  }

  const encoded = await sharp(source.buffer, { failOn: 'warning' })
    .rotate()
    .flatten({ background: '#ffffff' })
    .jpeg({ quality: boundedQuality, chromaSubsampling: '4:4:4', mozjpeg: true })
    .toBuffer({ resolveWithObject: true });
  const useEncoded = encoded.data.length < source.buffer.length;
  return Object.freeze({
    buffer: Buffer.from(useEncoded ? encoded.data : source.buffer),
    mimeType: useEncoded ? 'image/jpeg' : source.mimeType,
    metrics: Object.freeze({
      sourceBytes: source.buffer.length,
      transmittedBytes: useEncoded ? encoded.data.length : source.buffer.length,
      optimized: useEncoded,
      width: Number(useEncoded ? encoded.info.width : metadata.width || 0),
      height: Number(useEncoded ? encoded.info.height : metadata.height || 0),
    }),
  });
}

export function createCoordinateAgentProviderImageResolver({ resolveImage, ...options } = {}) {
  if (typeof resolveImage !== 'function') throw new Error('resolveImage is required');
  const cache = new Map();
  const metrics = [];
  return Object.freeze({
    async resolve(imageRef) {
      const key = String(imageRef || '');
      if (!cache.has(key)) {
        cache.set(key, Promise.resolve(resolveImage(imageRef))
          .then(value => prepareCoordinateAgentProviderImage(value, options))
          .then(prepared => {
            metrics.push(prepared.metrics);
            return prepared;
          }));
      }
      const prepared = await cache.get(key);
      return Object.freeze({ buffer: Buffer.from(prepared.buffer), mimeType: prepared.mimeType });
    },
    metrics() {
      return Object.freeze(metrics.map(item => Object.freeze({ ...item })));
    },
  });
}
