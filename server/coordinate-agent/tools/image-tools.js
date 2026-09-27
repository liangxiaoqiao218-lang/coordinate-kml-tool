import { GENERIC_TOOL_NAMES } from '../constants.js';

const BOX_SCHEMA = Object.freeze({
  type: 'array',
  minItems: 4,
  maxItems: 4,
  items: { type: 'number', minimum: 0, maximum: 1 },
});

function requireOperation(operations, name) {
  const operation = operations?.[name];
  if (typeof operation !== 'function') {
    throw new Error(`Local image operation ${name} is not configured`);
  }
  return operation;
}

export function registerGenericImageTools(registry, operations = {}) {
  registry.register({
    name: GENERIC_TOOL_NAMES.CROP_REGION,
    description: 'Crop a normalized image region without interpreting its content.',
    inputSchema: { type: 'object', additionalProperties: false, required: ['imageRef', 'bbox'], properties: { imageRef: { type: 'string' }, bbox: BOX_SCHEMA } },
    execute: args => requireOperation(operations, 'cropRegion')(args),
  });
  registry.register({
    name: GENERIC_TOOL_NAMES.ZOOM_REGION,
    description: 'Enlarge a normalized image region for closer visual inspection.',
    inputSchema: { type: 'object', additionalProperties: false, required: ['imageRef', 'bbox'], properties: { imageRef: { type: 'string' }, bbox: BOX_SCHEMA, scale: { type: 'number', minimum: 1, maximum: 8 } } },
    execute: args => requireOperation(operations, 'zoomRegion')(args),
  });
  registry.register({
    name: GENERIC_TOOL_NAMES.ROTATE_IMAGE,
    description: 'Rotate an image by a requested angle without reading coordinates.',
    inputSchema: { type: 'object', additionalProperties: false, required: ['imageRef', 'degrees'], properties: { imageRef: { type: 'string' }, degrees: { type: 'number' } } },
    execute: args => requireOperation(operations, 'rotateImage')(args),
  });
  registry.register({
    name: GENERIC_TOOL_NAMES.LOCAL_OCR_REGION,
    description: 'Transcribe a selected region as non-authoritative supporting evidence.',
    inputSchema: { type: 'object', additionalProperties: false, required: ['imageRef', 'bbox'], properties: { imageRef: { type: 'string' }, bbox: BOX_SCHEMA } },
    execute: args => requireOperation(operations, 'localOcrRegion')(args),
  });
  registry.register({
    name: GENERIC_TOOL_NAMES.DETECT_TABLE_STRUCTURE,
    description: 'Locate generic rows, columns, and cells without classifying a country or format.',
    inputSchema: { type: 'object', additionalProperties: false, required: ['imageRef'], properties: { imageRef: { type: 'string' }, bbox: BOX_SCHEMA } },
    execute: args => requireOperation(operations, 'detectTableStructure')(args),
  });
  return registry;
}
