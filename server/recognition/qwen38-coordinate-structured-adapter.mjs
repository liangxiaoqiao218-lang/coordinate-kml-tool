import crypto from "node:crypto";

export const ADAPTER_VERSION = "qwen38-coordinate-structured-b/v1";
export const CAPABILITY = "coordinate_boundary_structured_b";
export const MODEL = "qwen3.8-flash";
export const STRUCTURED_RESULT_STATUS = Object.freeze({
  NOT_APPLICABLE: "NOT_APPLICABLE",
  VALID: "VALID_STRUCTURED",
  INVALID: "INVALID_STRUCTURED"
});
export const REQUEST_LIMITS = Object.freeze({
  temperature: 0,
  maxTokens: 8000,
  timeoutMs: 90000,
  enableThinking: false,
  highResolutionImages: false,
  retries: 0
});

export const STRUCTURED_PROMPT = "仅依据当前原图识别全部可见表格内容。不得使用外部资料，不得猜测或补齐缺失字符、坐标系或基准。只输出一个JSON对象，精确采用以下结构：{\"source\":{\"title\":string|null,\"coordinateSystemExplicit\":string|null,\"datumExplicit\":string|null,\"pointOrder\":string[],\"points\":[{\"id\":string,\"order\":number,\"xRaw\":string,\"yRaw\":string,\"latitudeRaw\":string,\"longitudeRaw\":string}]},\"objects\":[{\"objectId\":string,\"displayName\":string,\"geometryType\":\"Polygon\",\"outerRing\":string[],\"innerRings\":string[][]}],\"unresolvedItems\":string[]}。必须完整保留原始标题、全部点号、点序、X、Y、Latitude和Longitude；outerRing/innerRings只引用source.points中的id。不要输出Markdown代码围栏或JSON之外的文字。";

export class CoordinateStructuredAdapterError extends Error {
  constructor(code, details = {}) {
    super(code);
    this.name = "CoordinateStructuredAdapterError";
    this.code = code;
    this.details = Object.freeze({ ...details });
  }
}

function fail(code, details) {
  throw new CoordinateStructuredAdapterError(code, details);
}

function check(condition, code, details) {
  if (!condition) fail(code, details);
}

function exactKeys(value, keys, code) {
  check(value && !Array.isArray(value) && typeof value === "object", code);
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  check(actual.length === expected.length && actual.every((key, index) => key === expected[index]), code, { actual, expected });
}

function validateImageItems(imageItems) {
  check(Array.isArray(imageItems) && imageItems.length > 0, "E_IMAGE_ITEMS");
  return imageItems.map((item, index) => {
    const url = item?.image_url?.url;
    check(item?.type === "image_url" && typeof url === "string", "E_IMAGE_ITEM", { index });
    check(/^data:image\/(?:jpeg|jpg|png|webp);base64,[A-Za-z0-9+/=]+$/.test(url), "E_IMAGE_INLINE_BASE64", { index });
    return Object.freeze({
      type: "image_url",
      image_url: Object.freeze({ url })
    });
  });
}

export function buildCoordinateStructuredCall({
  capability = CAPABILITY,
  modelName = MODEL,
  imageItems,
  stageName = "generic_provider"
} = {}) {
  check(capability === CAPABILITY, "E_CAPABILITY_SCOPE");
  check(modelName === MODEL, "E_MODEL_SCOPE", { modelName });
  check(typeof stageName === "string" && /^[a-z][a-z0-9_]{2,63}$/.test(stageName), "E_STAGE_NAME");
  return Object.freeze({
    modelName: MODEL,
    prompt: STRUCTURED_PROMPT,
    imageItems: Object.freeze(validateImageItems(imageItems)),
    temperature: REQUEST_LIMITS.temperature,
    maxTokens: REQUEST_LIMITS.maxTokens,
    enableThinking: REQUEST_LIMITS.enableThinking,
    highResolutionImages: REQUEST_LIMITS.highResolutionImages,
    timeoutMs: REQUEST_LIMITS.timeoutMs,
    stageName,
    lowValue: false
  });
}

function finalText(content) {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content.map(item => {
      if (typeof item === "string") return item;
      if (typeof item?.text === "string") return item.text;
      if (typeof item?.content === "string") return item.content;
      return "";
    }).filter(Boolean).join("\n");
  }
  if (typeof content?.text === "string") return content.text;
  if (typeof content?.content === "string") return content.content;
  return "";
}

function normalizeUsage(usage) {
  check(usage && Number.isInteger(usage.prompt_tokens) && usage.prompt_tokens >= 0, "E_USAGE_PROMPT");
  check(Number.isInteger(usage.completion_tokens) && usage.completion_tokens >= 0, "E_USAGE_COMPLETION");
  check(Number.isInteger(usage.total_tokens)
    && usage.total_tokens === usage.prompt_tokens + usage.completion_tokens, "E_USAGE_TOTAL");
  const cached = usage?.prompt_tokens_details?.cached_tokens;
  check(cached === undefined || (Number.isInteger(cached) && cached >= 0), "E_USAGE_CACHE");
  return Object.freeze({
    promptTokens: usage.prompt_tokens,
    completionTokens: usage.completion_tokens,
    totalTokens: usage.total_tokens,
    cachedPromptTokens: cached ?? null
  });
}

function validateStructuredValue(value) {
  exactKeys(value, ["source", "objects", "unresolvedItems"], "E_TOP_LEVEL_SCHEMA");
  exactKeys(value.source, ["title", "coordinateSystemExplicit", "datumExplicit", "pointOrder", "points"], "E_SOURCE_SCHEMA");
  check(value.source.title === null || typeof value.source.title === "string", "E_SOURCE_TITLE");
  check(value.source.coordinateSystemExplicit === null || typeof value.source.coordinateSystemExplicit === "string", "E_SOURCE_CRS");
  check(value.source.datumExplicit === null || typeof value.source.datumExplicit === "string", "E_SOURCE_DATUM");
  check(Array.isArray(value.source.pointOrder) && Array.isArray(value.source.points) && value.source.points.length >= 3, "E_SOURCE_POINTS");

  const pointIds = value.source.points.map((point, index) => {
    exactKeys(point, ["id", "order", "xRaw", "yRaw", "latitudeRaw", "longitudeRaw"], "E_POINT_SCHEMA");
    check(typeof point.id === "string" && point.id.length > 0 && point.id.length <= 128, "E_POINT_ID", { index });
    check(Number.isInteger(point.order) && point.order === index + 1, "E_POINT_ORDER_NUMBER", { index });
    for (const field of ["xRaw", "yRaw", "latitudeRaw", "longitudeRaw"]) {
      check(typeof point[field] === "string" && point[field].length <= 512, "E_POINT_RAW_FIELD", { index, field });
    }
    return point.id;
  });
  check(new Set(pointIds).size === pointIds.length, "E_DUPLICATE_POINT_ID");
  check(value.source.pointOrder.length === pointIds.length
    && value.source.pointOrder.every((id, index) => id === pointIds[index]), "E_POINT_ORDER_BINDING");

  check(Array.isArray(value.objects) && value.objects.length > 0, "E_OBJECTS");
  const pointSet = new Set(pointIds);
  const objectIds = value.objects.map((object, objectIndex) => {
    exactKeys(object, ["objectId", "displayName", "geometryType", "outerRing", "innerRings"], "E_OBJECT_SCHEMA");
    check(typeof object.objectId === "string" && object.objectId.length > 0, "E_OBJECT_ID", { objectIndex });
    check(typeof object.displayName === "string", "E_OBJECT_NAME", { objectIndex });
    check(object.geometryType === "Polygon", "E_GEOMETRY_SCOPE", { objectIndex });
    check(Array.isArray(object.outerRing) && object.outerRing.length >= 3, "E_OUTER_RING", { objectIndex });
    check(Array.isArray(object.innerRings), "E_INNER_RINGS", { objectIndex });
    for (const [ringIndex, ring] of [object.outerRing, ...object.innerRings].entries()) {
      check(Array.isArray(ring) && ring.length >= 3, "E_RING_SIZE", { objectIndex, ringIndex });
      check(ring.every(id => typeof id === "string" && pointSet.has(id)), "E_RING_REFERENCE", { objectIndex, ringIndex });
    }
    return object.objectId;
  });
  check(new Set(objectIds).size === objectIds.length, "E_DUPLICATE_OBJECT_ID");
  check(Array.isArray(value.unresolvedItems)
    && value.unresolvedItems.every(item => typeof item === "string" && item.length > 0 && item.length <= 1024), "E_UNRESOLVED_ITEMS");
  return value;
}

export function parseCoordinateStructuredResponse(response, { expectedModel = MODEL } = {}) {
  check(expectedModel === MODEL, "E_EXPECTED_MODEL_SCOPE");
  check(response && typeof response === "object", "E_RESPONSE");
  check(response.model === expectedModel, "E_RETURNED_MODEL", { returnedModel: response.model });
  check(Array.isArray(response.choices) && response.choices.length === 1, "E_CHOICES");
  const choice = response.choices[0];
  check(choice?.finish_reason === "stop", choice?.finish_reason === "length" ? "E_TRUNCATED" : "E_FINISH_REASON");
  check(!choice?.message?.tool_calls && !choice?.message?.refusal, "E_UNEXPECTED_FINAL");
  const rawText = finalText(choice?.message?.content);
  check(rawText.trim().length > 0 && rawText.length <= 262144, "E_FINAL_TEXT");
  const jsonText = rawText.trim();
  check(!jsonText.startsWith("```") && !jsonText.endsWith("```"), "E_MARKDOWN_FENCE");
  check(!/[\u0000-\u0008\u000b-\u001f\u007f-\u009f\u202a-\u202e\u2066-\u2069]/.test(rawText), "E_FINAL_CONTROL_CHAR");
  let value;
  try {
    value = JSON.parse(jsonText);
  } catch {
    fail("E_FINAL_NON_JSON");
  }
  validateStructuredValue(value);
  const usage = normalizeUsage(response.usage);
  return Object.freeze({
    adapterVersion: ADAPTER_VERSION,
    capability: CAPABILITY,
    model: response.model,
    finishReason: "stop",
    rawText,
    rawTextSha256: crypto.createHash("sha256").update(rawText, "utf8").digest("hex"),
    value,
    usage,
    authority: Object.freeze({
      status: "REVIEW_REQUIRED",
      reason: "MODEL_OUTPUT_REQUIRES_DETERMINISTIC_VALIDATION_AND_USER_REVIEW",
      providerOutputGrantsGeometryOrCrsAuthority: false
    })
  });
}

export function classifyCoordinateStructuredProviderResult({
  selectedCapability,
  response,
  extractedText
} = {}) {
  if (selectedCapability !== CAPABILITY) {
    return Object.freeze({ status: STRUCTURED_RESULT_STATUS.NOT_APPLICABLE });
  }
  try {
    check(typeof extractedText === "string", "E_EXTRACTED_TEXT_TYPE");
    check(finalText(response?.choices?.[0]?.message?.content) === extractedText, "E_EXTRACTED_TEXT_MISMATCH");
    const parsed = parseCoordinateStructuredResponse(response);
    return Object.freeze({
      status: STRUCTURED_RESULT_STATUS.VALID,
      structured: parsed.value,
      rawText: parsed.rawText,
      rawTextSha256: parsed.rawTextSha256,
      model: parsed.model,
      usage: parsed.usage,
      authority: parsed.authority
    });
  } catch (error) {
    return Object.freeze({
      status: STRUCTURED_RESULT_STATUS.INVALID,
      reasonCode: error instanceof CoordinateStructuredAdapterError ? error.code : "E_STRUCTURED_ADAPTER_INTERNAL"
    });
  }
}

export function createCoordinateStructuredProvider({ providerCall, modelName = MODEL } = {}) {
  check(typeof providerCall === "function", "E_PROVIDER_CALL");
  check(modelName === MODEL, "E_MODEL_SCOPE", { modelName });
  return Object.freeze({
    recognize: async ({ imageItems, stageName } = {}) => {
      const request = buildCoordinateStructuredCall({ imageItems, stageName, modelName });
      const response = await providerCall(request);
      return parseCoordinateStructuredResponse(response, { expectedModel: modelName });
    }
  });
}

export function describeLegacyIsolation({ sharedVisionModel, legacyOcrModel } = {}) {
  check(typeof sharedVisionModel === "string" && sharedVisionModel.trim(), "E_SHARED_VISION_IDENTITY_UNKNOWN");
  check(typeof legacyOcrModel === "string" && legacyOcrModel.trim(), "E_LEGACY_OCR_IDENTITY_UNKNOWN");
  return Object.freeze({
    coordinateStructuredModel: MODEL,
    sharedVisionModel,
    legacyOcrModel,
    miningModelChanged: false,
    agenticModelChanged: false,
    legacyOcrModelChanged: false
  });
}
