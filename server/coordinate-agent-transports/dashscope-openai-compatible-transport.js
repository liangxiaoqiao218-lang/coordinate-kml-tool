const TRANSPORT_NAME = 'dashscope-openai-compatible/v1';
export const DEFAULT_COORDINATE_AGENT_PROVIDER_TIMEOUT_MS = 90_000;
export const MIN_COORDINATE_AGENT_PROVIDER_TIMEOUT_MS = 1_000;
export const MAX_COORDINATE_AGENT_PROVIDER_TIMEOUT_MS = 180_000;

export function normalizeCoordinateAgentProviderTimeoutMs(value) {
  const numeric = Number(value);
  const requested = Number.isFinite(numeric) ? Math.trunc(numeric) : DEFAULT_COORDINATE_AGENT_PROVIDER_TIMEOUT_MS;
  return Math.min(
    MAX_COORDINATE_AGENT_PROVIDER_TIMEOUT_MS,
    Math.max(MIN_COORDINATE_AGENT_PROVIDER_TIMEOUT_MS, requested),
  );
}

function chatCompletionsUrl(endpoint) {
  const base = String(endpoint || '').trim().replace(/\/+$/, '');
  if (!base.startsWith('https://')) throw new Error('DashScope endpoint must use HTTPS');
  return base.endsWith('/chat/completions') ? base : `${base}/chat/completions`;
}

function jsonText(value) {
  return JSON.stringify(value, null, 2);
}

function decodedBase64Bytes(value) {
  const text = String(value || '');
  if (!text) return 0;
  const padding = text.endsWith('==') ? 2 : text.endsWith('=') ? 1 : 0;
  return Math.max(0, Math.floor((text.length * 3) / 4) - padding);
}

function userInstruction(request) {
  return [
    `Agent iteration: ${request.iteration}`,
    `Agent state: ${request.state}`,
    'Inspect the supplied image assets as a single evidence set.',
    'Use each asset id exactly as supplied when creating tool action args.imageRef.',
    'Region ids must be unique across the complete run.',
    'Return one JSON object only. It must conform to this response schema:',
    jsonText(request.responseSchema),
    'Available tools:',
    jsonText(request.tools),
    'Accumulated evidence and latest tool results:',
    jsonText(request.context),
    'Available image assets:',
    jsonText(request.assets.map(asset => ({ id: asset.id, role: asset.role, mimeType: asset.mimeType }))),
  ].join('\n\n');
}

function extractMessageContent(payload) {
  const content = payload?.choices?.[0]?.message?.content;
  if (typeof content === 'string' && content.trim()) return content.trim();
  if (Array.isArray(content)) {
    const text = content
      .filter(item => item?.type === 'text' && typeof item.text === 'string')
      .map(item => item.text)
      .join('\n')
      .trim();
    if (text) return text;
  }
  throw new Error('DashScope response contains no structured message content');
}

function normalizeUsage(usage) {
  if (!usage || typeof usage !== 'object' || Array.isArray(usage)) return null;
  const numeric = value => (Number.isFinite(Number(value)) ? Number(value) : null);
  return Object.freeze({
    inputTokens: numeric(usage.prompt_tokens ?? usage.input_tokens),
    outputTokens: numeric(usage.completion_tokens ?? usage.output_tokens),
    totalTokens: numeric(usage.total_tokens),
  });
}

export function mapCoordinateAgentRequestToDashScope(request, {
  model,
  maxTokens = 8_000,
  highResolutionImages = false,
} = {}) {
  if (!request || typeof request !== 'object') throw new Error('Coordinate agent request is required');
  const modelName = String(model || '').trim();
  if (!modelName) throw new Error('DashScope model is required');
  const content = [{ type: 'text', text: userInstruction(request) }];
  for (const asset of request.assets || []) {
    content.push({ type: 'text', text: `Image asset ${asset.id} (${asset.role})` });
    content.push({
      type: 'image_url',
      image_url: { url: `data:${asset.mimeType};base64,${asset.bytesBase64}` },
    });
  }
  const body = {
    model: modelName,
    messages: [
      { role: 'system', content: request.systemPrompt },
      { role: 'user', content },
    ],
    temperature: 0,
    max_tokens: Math.min(8_000, Math.max(1, Number(maxTokens) || 8_000)),
    response_format: { type: 'json_object' },
    enable_thinking: false,
  };
  if (highResolutionImages === true) body.vl_high_resolution_images = true;
  return Object.freeze(body);
}

export class DashScopeOpenAICompatibleTransport {
  #fetch;
  #getAccessToken;
  #endpoint;
  #model;
  #timeoutMs;
  #maxTokens;
  #highResolutionImages;
  #telemetry = [];

  constructor({
    fetchImpl,
    getAccessToken,
    endpoint,
    model,
    timeoutMs = DEFAULT_COORDINATE_AGENT_PROVIDER_TIMEOUT_MS,
    maxTokens = 8_000,
    highResolutionImages = false,
  } = {}) {
    if (typeof fetchImpl !== 'function') throw new Error('fetchImpl is required');
    if (typeof getAccessToken !== 'function') throw new Error('getAccessToken is required');
    this.#fetch = fetchImpl;
    this.#getAccessToken = getAccessToken;
    this.#endpoint = chatCompletionsUrl(endpoint);
    this.#model = String(model || '').trim();
    if (!this.#model) throw new Error('DashScope model is required');
    this.#timeoutMs = normalizeCoordinateAgentProviderTimeoutMs(timeoutMs);
    this.#maxTokens = maxTokens;
    this.#highResolutionImages = highResolutionImages === true;
  }

  async complete(request) {
    const accessToken = String(await this.#getAccessToken() || '').trim();
    if (!accessToken) throw new Error('DashScope access token is unavailable');
    const buildStartedAt = Date.now();
    const requestBody = mapCoordinateAgentRequestToDashScope(request, {
      model: this.#model,
      maxTokens: this.#maxTokens,
      highResolutionImages: this.#highResolutionImages,
    });
    const requestBodyJson = JSON.stringify(requestBody);
    const requestBuildDurationMs = Date.now() - buildStartedAt;
    const requestBodyBytes = Buffer.byteLength(requestBodyJson);
    const imageBytes = (request.assets || [])
      .reduce((total, asset) => total + decodedBase64Bytes(asset.bytesBase64), 0);
    const schemaBytes = Buffer.byteLength(JSON.stringify(request.responseSchema || {}));
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.#timeoutMs);
    const startedAt = Date.now();
    let response;
    let payload = {};
    try {
      response = await this.#fetch(this.#endpoint, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${accessToken}`,
          'Content-Type': 'application/json',
        },
        body: requestBodyJson,
        signal: controller.signal,
      });
      payload = await response.json().catch(() => ({}));
    } catch (error) {
      const normalized = new Error(error?.name === 'AbortError'
        ? 'DashScope transport timed out'
        : 'DashScope transport network failure');
      normalized.code = error?.name === 'AbortError' ? 'DASHSCOPE_TIMEOUT' : 'DASHSCOPE_NETWORK_ERROR';
      this.#telemetry.push(Object.freeze({
        transport: TRANSPORT_NAME,
        model: this.#model,
        ok: false,
        httpStatus: null,
        durationMs: Date.now() - startedAt,
        timeoutMs: this.#timeoutMs,
        requestBuildDurationMs,
        requestBodyBytes,
        imageBytes,
        schemaBytes,
        usageObserved: false,
        usage: null,
        errorCode: normalized.code,
      }));
      throw normalized;
    } finally {
      clearTimeout(timer);
    }

    const usage = normalizeUsage(payload?.usage);
    this.#telemetry.push(Object.freeze({
      transport: TRANSPORT_NAME,
      model: this.#model,
      ok: response.ok,
      httpStatus: Number(response.status),
      durationMs: Date.now() - startedAt,
      timeoutMs: this.#timeoutMs,
      requestBuildDurationMs,
      requestBodyBytes,
      imageBytes,
      schemaBytes,
      usageObserved: usage !== null,
      usage,
      errorCode: response.ok ? null : 'DASHSCOPE_HTTP_ERROR',
    }));
    if (!response.ok) {
      const error = new Error('DashScope transport HTTP failure');
      error.code = 'DASHSCOPE_HTTP_ERROR';
      error.status = Number(response.status);
      throw error;
    }
    return Object.freeze({ structuredOutput: extractMessageContent(payload) });
  }

  telemetry() {
    return Object.freeze(this.#telemetry.map(item => Object.freeze({ ...item })));
  }
}
