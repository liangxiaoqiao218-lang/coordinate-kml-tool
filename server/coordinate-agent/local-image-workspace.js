import { createHash } from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

const LOCAL_IMAGE_REF_PREFIX = 'local-image://';

function imageId(buffer) {
  return createHash('sha256')
    .update(buffer)
    .digest('hex');
}

export class LocalImageWorkspace {
  #images = new Map();

  constructor({ maxImageBytes = 25 * 1024 * 1024 } = {}) {
    this.maxImageBytes = Math.max(1, Number(maxImageBytes) || 25 * 1024 * 1024);
  }

  registerBuffer(buffer, { label = 'image', derivedFrom = null } = {}) {
    if (!Buffer.isBuffer(buffer) || buffer.length === 0) throw new Error('A non-empty image Buffer is required');
    if (buffer.length > this.maxImageBytes) throw new Error('Image exceeds the local workspace byte limit');
    const id = imageId(buffer);
    const imageRef = `${LOCAL_IMAGE_REF_PREFIX}${id}`;
    this.#images.set(imageRef, Object.freeze({
      buffer: Buffer.from(buffer),
      label: String(label),
      derivedFrom: derivedFrom ? String(derivedFrom) : null,
    }));
    return imageRef;
  }

  async registerFile(filePath) {
    const requestedPath = String(filePath || '').trim();
    if (!requestedPath || /^(?:https?|data|file):/i.test(requestedPath)) {
      throw new Error('Only a local image path is allowed');
    }
    const absolutePath = path.resolve(requestedPath);
    const buffer = await fs.readFile(absolutePath);
    return this.registerBuffer(buffer, { label: path.basename(absolutePath) });
  }

  resolve(imageRef) {
    const normalized = String(imageRef || '');
    if (!normalized.startsWith(LOCAL_IMAGE_REF_PREFIX)) throw new Error('Only local-image references are allowed');
    const entry = this.#images.get(normalized);
    if (!entry) throw new Error('Local image reference is unknown or expired');
    return Object.freeze({
      imageRef: normalized,
      buffer: Buffer.from(entry.buffer),
      label: entry.label,
      derivedFrom: entry.derivedFrom,
    });
  }

  get size() {
    return this.#images.size;
  }
}
