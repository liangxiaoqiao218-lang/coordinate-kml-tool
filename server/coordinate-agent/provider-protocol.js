import { COORDINATE_AGENT_TURN_SCHEMA, assertStrictCoordinateAgentTurn } from './schema.js';

export const COORDINATE_AGENT_MULTIMODAL_REQUEST_VERSION = 'coordinate-agent-multimodal-request/v1';

export const COORDINATE_AGENT_SYSTEM_PROMPT = `You are a coordinate intelligence agent. Your recognition core is multimodal reasoning over the image, not traditional OCR, a format classifier, or a sample-specific parser.

Follow this operating loop for every image:
1. Observe the complete image before extracting or concluding anything.
2. Build a provisional document structure and an explicit recognition plan from visible evidence.
3. Decide which listed tools, if any, are needed. Use focused crops, enlargement, rotation, table-region preparation, supporting OCR, coordinate math, and spatial checks only when they materially improve evidence.
4. Read and relate headers, rows, columns, direction markers, coordinate values, CRS evidence, grouping boundaries, and annotations.
5. When evidence conflicts or remains incomplete, inspect the relevant region again instead of guessing or forcing a format.
6. Use mathematical and spatial tools to verify explicit coordinate evidence; tools must not invent missing evidence.
7. Produce either a strictly structured coordinate conclusion or precise review questions tied to evidence regions.

Each follow-up turn must reconcile the new tool evidence with the whole-image structure. Do not bypass this loop with one-shot OCR text, regex extraction, filename routing, country routing, fixed coordinates, remembered examples, or a format-specific production parser.

Never infer a missing direction, datum, coordinate reference system, grouping boundary, or digit from geography, filenames, prior examples, or expected answers. Supporting OCR is non-authoritative. Preserve source text and attach uncertainties to precise evidence regions. If material evidence remains unresolved, return a needs_review candidate and focused reviewItems. Do not authorize Map or KML; deterministic safety code decides that after validating your structured result.

Return only one JSON object that conforms exactly to the supplied response schema. Do not include markdown, commentary, or properties outside that schema.

The top-level JSON object must contain observation and plan. Always include candidate, uncertainties, and reviewItems; use null or empty arrays when no value is available. observation.regions and plan.actions must always be arrays. Every region, action, uncertainty, review item, group, and point must include every field marked required by the supplied schema. Use only the supplied enum values and tool names. JSON numbers and booleans must not be quoted.`;

function clone(value) {
  return structuredClone(value);
}

function collectImageRefs(value, refs = new Set()) {
  if (Array.isArray(value)) {
    value.forEach(item => collectImageRefs(item, refs));
    return refs;
  }
  if (!value || typeof value !== 'object') return refs;
  for (const [key, child] of Object.entries(value)) {
    if (key === 'imageRef' && typeof child === 'string' && child.trim()) refs.add(child.trim());
    collectImageRefs(child, refs);
  }
  return refs;
}

function normalizeResolvedImage(value, imageRef) {
  const buffer = Buffer.isBuffer(value) ? value : value?.buffer;
  if (!Buffer.isBuffer(buffer) || buffer.length === 0) {
    throw new Error(`Image resolver returned no bytes for ${imageRef}`);
  }
  return {
    buffer,
    mimeType: String(value?.mimeType || 'image/png'),
  };
}

export async function buildCoordinateAgentProviderRequest({
  turnInput,
  resolveImage,
  systemPrompt = COORDINATE_AGENT_SYSTEM_PROMPT,
  maxAssets = 8,
  maxAssetBytes = 16 * 1024 * 1024,
  maxTotalAssetBytes = 40 * 1024 * 1024,
} = {}) {
  if (!turnInput || typeof turnInput !== 'object' || Array.isArray(turnInput)) {
    throw new Error('Coordinate agent turn input is required');
  }
  if (typeof resolveImage !== 'function') throw new Error('resolveImage is required');
  const primaryRef = String(turnInput.imageRef || '').trim();
  if (!primaryRef) throw new Error('turnInput.imageRef is required');

  const refs = collectImageRefs(turnInput.toolResults, new Set([primaryRef]));
  if (refs.size > maxAssets) throw new Error(`Coordinate agent asset limit exceeded: ${refs.size}`);
  const assets = [];
  let totalBytes = 0;
  for (const imageRef of refs) {
    const resolved = normalizeResolvedImage(await resolveImage(imageRef), imageRef);
    if (resolved.buffer.length > maxAssetBytes) throw new Error(`Coordinate agent image is too large: ${imageRef}`);
    totalBytes += resolved.buffer.length;
    if (totalBytes > maxTotalAssetBytes) throw new Error('Coordinate agent total image budget exceeded');
    assets.push(Object.freeze({
      id: imageRef,
      role: imageRef === primaryRef ? 'source' : 'tool_result',
      mimeType: resolved.mimeType,
      bytesBase64: resolved.buffer.toString('base64'),
    }));
  }

  const request = {
    protocolVersion: COORDINATE_AGENT_MULTIMODAL_REQUEST_VERSION,
    requestId: turnInput.requestId === null || turnInput.requestId === undefined
      ? null
      : String(turnInput.requestId),
    iteration: Number(turnInput.iteration),
    state: String(turnInput.state || ''),
    systemPrompt: String(systemPrompt),
    context: {
      evidence: clone(turnInput.evidence || { regions: [], uncertainties: [], reviewItems: [], toolResults: [] }),
      toolResults: clone(turnInput.toolResults || []),
    },
    tools: clone(turnInput.tools || []),
    assets,
    responseSchema: clone(COORDINATE_AGENT_TURN_SCHEMA),
  };
  return Object.freeze(request);
}

export function parseCoordinateAgentStructuredOutput(transportResponse) {
  if (!transportResponse || typeof transportResponse !== 'object' || Array.isArray(transportResponse)) {
    const error = new Error('Provider transport must return an object');
    error.code = 'PROVIDER_ENVELOPE_INVALID';
    error.path = 'transportResponse';
    error.expectedType = 'object';
    error.actualType = Array.isArray(transportResponse) ? 'array' : typeof transportResponse;
    throw error;
  }
  const keys = Object.keys(transportResponse);
  if (keys.length !== 1 || keys[0] !== 'structuredOutput') {
    const error = new Error('Provider transport response must contain only structuredOutput');
    error.code = 'PROVIDER_ENVELOPE_INVALID';
    error.path = 'transportResponse';
    error.expectedType = 'structuredOutput-only object';
    error.actualType = 'object';
    throw error;
  }
  let output = transportResponse.structuredOutput;
  if (typeof output === 'string') {
    try {
      output = JSON.parse(output);
    } catch {
      const error = new Error('Provider structuredOutput is not valid JSON');
      error.code = 'PROVIDER_STRUCTURED_OUTPUT_INVALID_JSON';
      error.path = 'structuredOutput';
      error.expectedType = 'JSON object';
      error.actualType = 'string';
      throw error;
    }
  }
  return assertStrictCoordinateAgentTurn(output);
}
