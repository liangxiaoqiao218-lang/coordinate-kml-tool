import { COORDINATE_AGENT_TURN_SCHEMA, assertStrictCoordinateAgentTurn } from './schema.js';

export const COORDINATE_AGENT_MULTIMODAL_REQUEST_VERSION = 'coordinate-agent-multimodal-request/v1';

export const COORDINATE_AGENT_SYSTEM_PROMPT = `You are a coordinate intelligence agent. Inspect the complete image before deciding what to do.

Use visible evidence, not assumptions. Understand document layout, headers, row and column relationships, direction markers, coordinate values, grouping, and uncertainty. You may request only tools listed in the request. Use focused crops, enlargement, rotation, and supporting OCR when they materially improve the evidence.

Never infer a missing direction, datum, coordinate reference system, grouping boundary, or digit from geography, filenames, prior examples, or expected answers. Supporting OCR is non-authoritative. Preserve source text and attach uncertainties to precise evidence regions. If material evidence remains unresolved, return a needs_review candidate and focused reviewItems. Do not authorize Map or KML; deterministic safety code decides that after validating your structured result.

Return only one JSON object that conforms exactly to the supplied response schema. Do not include markdown, commentary, or properties outside that schema.`;

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
    throw new Error('Provider transport must return an object');
  }
  const keys = Object.keys(transportResponse);
  if (keys.length !== 1 || keys[0] !== 'structuredOutput') {
    throw new Error('Provider transport response must contain only structuredOutput');
  }
  let output = transportResponse.structuredOutput;
  if (typeof output === 'string') {
    try {
      output = JSON.parse(output);
    } catch {
      throw new Error('Provider structuredOutput is not valid JSON');
    }
  }
  return assertStrictCoordinateAgentTurn(output);
}
