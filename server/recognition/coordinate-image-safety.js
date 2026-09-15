export const COORDINATE_IMAGE_SAFETY_LIMITS = Object.freeze({
  maxUploadBytes: 12 * 1024 * 1024,
  maxTrailingBytes: 64 * 1024,
  maxDimension: 16_384,
  maxPixels: 40_000_000
});

export const COORDINATE_IMAGE_SAFETY_STATUS = Object.freeze({
  JPEG_CANONICAL_UNCHANGED: "JPEG_CANONICAL_UNCHANGED",
  JPEG_TRAILING_DATA_DISCARDED_UNTRUSTED: "JPEG_TRAILING_DATA_DISCARDED_UNTRUSTED",
  NON_JPEG_UNCHANGED: "NON_JPEG_UNCHANGED"
});

export const COORDINATE_IMAGE_SAFETY_REASON = Object.freeze({
  COORDINATE_IMAGE_INVALID: "COORDINATE_IMAGE_INVALID",
  JPEG_TRAILING_DATA_LIMIT_EXCEEDED: "JPEG_TRAILING_DATA_LIMIT_EXCEEDED",
  JPEG_RESOURCE_LIMIT_EXCEEDED: "JPEG_RESOURCE_LIMIT_EXCEEDED",
  JPEG_CANONICAL_PREFIX_UNPROVEN: "JPEG_CANONICAL_PREFIX_UNPROVEN"
});

const JPEG_MIME_TYPES = new Set(["image/jpeg", "image/jpg"]);

function fail(reason) {
  return Object.freeze({ valid: false, reason });
}

function inspectJpegStructure(buffer, { allowTrailing = false } = {}) {
  if (!Buffer.isBuffer(buffer)
    || buffer.length < 32
    || buffer.length > COORDINATE_IMAGE_SAFETY_LIMITS.maxUploadBytes
    || buffer[0] !== 0xff
    || buffer[1] !== 0xd8) {
    return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
  }

  let offset = 2;
  const quantizationTables = new Set();
  const huffmanTables = new Set();
  const scannedComponents = new Set();
  let frameComponents = null;
  let frameMode = null;
  let sawScan = false;

  const finishAtEoi = endOffset => {
    if (!sawScan || !frameComponents || scannedComponents.size !== frameComponents.size) {
      return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
    }
    if (!allowTrailing && endOffset !== buffer.length) {
      return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
    }
    return Object.freeze({
      valid: true,
      reason: "JPEG_STRUCTURE_VALID",
      logicalEoiOffset: endOffset,
      trailingByteCount: buffer.length - endOffset
    });
  };

  jpegSegments: while (offset + 1 < buffer.length) {
    if (buffer[offset] !== 0xff) {
      return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
    }
    while (offset < buffer.length && buffer[offset] === 0xff) offset += 1;
    if (offset >= buffer.length) {
      return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
    }
    const marker = buffer[offset++];
    if (marker === 0xd9) return finishAtEoi(offset);
    if (marker === 0x01 || (marker >= 0xd0 && marker <= 0xd7)) continue;
    if (offset + 2 > buffer.length) {
      return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
    }

    const segmentLength = buffer.readUInt16BE(offset);
    if (segmentLength < 2 || offset + segmentLength > buffer.length) {
      return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
    }
    const segmentEnd = offset + segmentLength;

    if (marker === 0xdb) {
      let tableOffset = offset + 2;
      while (tableOffset < segmentEnd) {
        const tableInfo = buffer[tableOffset++];
        const precision = tableInfo >>> 4;
        const tableId = tableInfo & 0x0f;
        const tableBytes = precision === 0 ? 64 : precision === 1 ? 128 : 0;
        if (tableId > 3 || tableBytes === 0 || tableOffset + tableBytes > segmentEnd) {
          return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
        }
        for (let valueOffset = tableOffset; valueOffset < tableOffset + tableBytes; valueOffset += precision + 1) {
          const value = precision === 0 ? buffer[valueOffset] : buffer.readUInt16BE(valueOffset);
          if (value === 0) return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
        }
        quantizationTables.add(tableId);
        tableOffset += tableBytes;
      }
      if (tableOffset !== segmentEnd) {
        return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
      }
    } else if (marker === 0xc4) {
      let tableOffset = offset + 2;
      while (tableOffset < segmentEnd) {
        const tableInfo = buffer[tableOffset++];
        const tableClass = tableInfo >>> 4;
        const tableId = tableInfo & 0x0f;
        if (tableClass > 1 || tableId > 3 || tableOffset + 16 > segmentEnd) {
          return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
        }
        let symbolCount = 0;
        let remainingCodes = 1;
        for (let index = 0; index < 16; index += 1) {
          const count = buffer[tableOffset + index];
          symbolCount += count;
          remainingCodes = (remainingCodes * 2) - count;
          if (remainingCodes < 0) {
            return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
          }
        }
        tableOffset += 16;
        if (symbolCount <= 0 || symbolCount > 256 || tableOffset + symbolCount > segmentEnd) {
          return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
        }
        huffmanTables.add(`${tableClass}:${tableId}`);
        tableOffset += symbolCount;
      }
      if (tableOffset !== segmentEnd) {
        return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
      }
    } else if (marker === 0xc0 || marker === 0xc2) {
      if (frameComponents || segmentLength < 11 || buffer[offset + 2] !== 8) {
        return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
      }
      const height = buffer.readUInt16BE(offset + 3);
      const width = buffer.readUInt16BE(offset + 5);
      const componentCount = buffer[offset + 7];
      if (width <= 0
        || height <= 0
        || width > COORDINATE_IMAGE_SAFETY_LIMITS.maxDimension
        || height > COORDINATE_IMAGE_SAFETY_LIMITS.maxDimension
        || width * height > COORDINATE_IMAGE_SAFETY_LIMITS.maxPixels) {
        return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_RESOURCE_LIMIT_EXCEEDED);
      }
      if (![1, 3, 4].includes(componentCount) || segmentLength !== 8 + (3 * componentCount)) {
        return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
      }
      frameMode = marker === 0xc0 ? "BASELINE" : "PROGRESSIVE";
      frameComponents = new Map();
      let componentOffset = offset + 8;
      for (let index = 0; index < componentCount; index += 1) {
        const componentId = buffer[componentOffset];
        const sampling = buffer[componentOffset + 1];
        const quantizationTableId = buffer[componentOffset + 2];
        if (frameComponents.has(componentId)
          || (sampling >>> 4) === 0
          || (sampling & 0x0f) === 0
          || quantizationTableId > 3) {
          return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
        }
        frameComponents.set(componentId, quantizationTableId);
        componentOffset += 3;
      }
    } else if ([0xc1, 0xc3, 0xc5, 0xc6, 0xc7, 0xc9, 0xca, 0xcb, 0xcd, 0xce, 0xcf].includes(marker)) {
      return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
    }

    if (marker === 0xda) {
      if (!frameComponents) {
        return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
      }
      const scanComponentCount = buffer[offset + 2];
      if (scanComponentCount <= 0
        || scanComponentCount > frameComponents.size
        || segmentLength !== 6 + (2 * scanComponentCount)) {
        return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
      }
      let scanComponentOffset = offset + 3;
      const seenScanComponents = new Set();
      const scanSelectors = [];
      for (let index = 0; index < scanComponentCount; index += 1) {
        const componentId = buffer[scanComponentOffset];
        const tableSelectors = buffer[scanComponentOffset + 1];
        const dcTableId = tableSelectors >>> 4;
        const acTableId = tableSelectors & 0x0f;
        if (!frameComponents.has(componentId) || seenScanComponents.has(componentId)) {
          return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
        }
        seenScanComponents.add(componentId);
        scanSelectors.push({ componentId, dcTableId, acTableId });
        scanComponentOffset += 2;
      }
      const spectralStart = buffer[scanComponentOffset];
      const spectralEnd = buffer[scanComponentOffset + 1];
      const approximation = buffer[scanComponentOffset + 2];
      const approximationHigh = approximation >>> 4;
      const approximationLow = approximation & 0x0f;
      if (frameMode === "BASELINE") {
        if (scanComponentCount !== frameComponents.size
          || spectralStart !== 0
          || spectralEnd !== 63
          || approximation !== 0
          || scanSelectors.some(({ dcTableId, acTableId }) => !huffmanTables.has(`0:${dcTableId}`)
            || !huffmanTables.has(`1:${acTableId}`))) {
          return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
        }
      } else {
        if (spectralStart > spectralEnd
          || spectralEnd > 63
          || (spectralStart === 0 && spectralEnd !== 0)
          || (spectralStart > 0 && scanComponentCount !== 1)
          || approximationHigh > 13
          || approximationLow > 13) {
          return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
        }
        if (spectralStart === 0) {
          if (scanSelectors.some(({ dcTableId }) => !huffmanTables.has(`0:${dcTableId}`))) {
            return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
          }
        } else if (scanSelectors.some(({ acTableId }) => !huffmanTables.has(`1:${acTableId}`))) {
          return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
        }
      }
      if ([...frameComponents.values()].some(tableId => !quantizationTables.has(tableId))) {
        return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
      }

      const scanStart = segmentEnd;
      if (scanStart >= buffer.length - 2) {
        return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
      }
      let entropyBytes = 0;
      for (let scanOffset = scanStart; scanOffset < buffer.length - 1; scanOffset += 1) {
        if (buffer[scanOffset] !== 0xff) {
          entropyBytes += 1;
          continue;
        }
        const next = buffer[scanOffset + 1];
        if (next === 0x00 || (next >= 0xd0 && next <= 0xd7)) {
          entropyBytes += 1;
          scanOffset += 1;
          continue;
        }
        if (entropyBytes <= 0) {
          return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
        }
        sawScan = true;
        for (const componentId of seenScanComponents) scannedComponents.add(componentId);
        if (next === 0xd9) return finishAtEoi(scanOffset + 2);
        offset = scanOffset;
        continue jpegSegments;
      }
      return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
    }
    offset += segmentLength;
  }

  return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
}

export function hasValidJpegStructure(buffer) {
  return inspectJpegStructure(buffer, { allowTrailing: false }).valid === true;
}

export function canonicalizeCoordinateImageUpload(file) {
  const mimeType = String(file?.mimetype || "").toLowerCase();
  const buffer = file?.buffer;
  if (!Buffer.isBuffer(buffer) || buffer.length > COORDINATE_IMAGE_SAFETY_LIMITS.maxUploadBytes) {
    return fail(COORDINATE_IMAGE_SAFETY_REASON.COORDINATE_IMAGE_INVALID);
  }

  if (!JPEG_MIME_TYPES.has(mimeType)) {
    return Object.freeze({
      valid: true,
      reason: "NON_JPEG_PASSTHROUGH",
      status: COORDINATE_IMAGE_SAFETY_STATUS.NON_JPEG_UNCHANGED,
      file
    });
  }
  if (buffer.length < 2 || buffer[0] !== 0xff || buffer[1] !== 0xd8) {
    return fail(COORDINATE_IMAGE_SAFETY_REASON.COORDINATE_IMAGE_INVALID);
  }

  const inspection = inspectJpegStructure(buffer, { allowTrailing: true });
  if (!inspection.valid) return inspection;
  if (inspection.trailingByteCount === 0) {
    return Object.freeze({
      valid: true,
      reason: "JPEG_CANONICAL",
      status: COORDINATE_IMAGE_SAFETY_STATUS.JPEG_CANONICAL_UNCHANGED,
      file: Object.freeze({ ...file, buffer, size: buffer.length, mimetype: "image/jpeg" })
    });
  }
  if (inspection.trailingByteCount > COORDINATE_IMAGE_SAFETY_LIMITS.maxTrailingBytes) {
    return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_TRAILING_DATA_LIMIT_EXCEEDED);
  }

  const canonicalBuffer = Buffer.allocUnsafeSlow(inspection.logicalEoiOffset);
  buffer.copy(canonicalBuffer, 0, 0, inspection.logicalEoiOffset);
  if (!hasValidJpegStructure(canonicalBuffer)) {
    return fail(COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
  }
  return Object.freeze({
    valid: true,
    reason: "JPEG_CANONICALIZED",
    status: COORDINATE_IMAGE_SAFETY_STATUS.JPEG_TRAILING_DATA_DISCARDED_UNTRUSTED,
    file: Object.freeze({
      ...file,
      buffer: canonicalBuffer,
      size: canonicalBuffer.length,
      mimetype: "image/jpeg"
    })
  });
}
