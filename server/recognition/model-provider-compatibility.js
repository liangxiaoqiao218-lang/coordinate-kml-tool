export const COORDINATE_PROVIDER_MODELS = Object.freeze({
  VISION: "qwen3.8-flash",
  OCR: "qwen3.5-ocr"
});

export const PROVIDER_COMPATIBILITY_ERROR_CODE = Object.freeze({
  OCR_PARAMETER_UNSUPPORTED: "MODEL_OCR_PARAMETER_UNSUPPORTED",
  TOKEN_LIMIT_EXCEEDED: "MODEL_TOKEN_LIMIT_EXCEEDED",
  RESPONSE_MODEL_MISMATCH: "MODEL_RESPONSE_MODEL_MISMATCH",
  RESPONSE_CHOICE_COUNT_INVALID: "MODEL_RESPONSE_CHOICE_COUNT_INVALID",
  RESPONSE_NOT_FINAL: "MODEL_RESPONSE_NOT_FINAL",
  RESPONSE_CONTENT_EMPTY: "MODEL_RESPONSE_CONTENT_EMPTY",
  RESPONSE_USAGE_INVALID: "MODEL_RESPONSE_USAGE_INVALID"
});

const MAX_TARGET_MODEL_TOKENS = 8000;

function fixedError(code) {
  const error = new Error(code);
  error.code = code;
  error.reason = "provider_contract";
  return error;
}

function hasFinalContent(content) {
  if (typeof content === "string") return content.trim().length > 0;
  if (Array.isArray(content)) {
    return content.some(item => (
      typeof item === "string" && item.trim().length > 0
    ) || (
      typeof item?.text === "string" && item.text.trim().length > 0
    ) || (
      typeof item?.content === "string" && item.content.trim().length > 0
    ));
  }
  return typeof content?.text === "string" && content.text.trim().length > 0;
}

export function prepareCoordinateProviderRequest({
  modelName,
  maxTokens,
  responseFormat,
  enableThinking,
  highResolutionImages = false
} = {}) {
  const model = String(modelName || "").trim();
  const role = model === COORDINATE_PROVIDER_MODELS.VISION
    ? "VISION"
    : model === COORDINATE_PROVIDER_MODELS.OCR ? "OCR" : "UNMANAGED";
  if (role === "UNMANAGED") {
    return Object.freeze({
      applied: false,
      role,
      stream: null,
      enableThinking,
      includeHighResolutionImages: highResolutionImages === true,
      includeResponseFormat: Boolean(responseFormat && typeof responseFormat === "object")
    });
  }
  const tokenLimit = Number(maxTokens);
  if (role === "OCR" && Number.isFinite(tokenLimit) && tokenLimit > MAX_TARGET_MODEL_TOKENS) {
    throw fixedError(PROVIDER_COMPATIBILITY_ERROR_CODE.TOKEN_LIMIT_EXCEEDED);
  }
  if (role === "OCR" && (
    typeof enableThinking === "boolean"
    || highResolutionImages === true
    || Boolean(responseFormat && typeof responseFormat === "object")
  )) {
    throw fixedError(PROVIDER_COMPATIBILITY_ERROR_CODE.OCR_PARAMETER_UNSUPPORTED);
  }
  return Object.freeze({
    applied: true,
    role,
    stream: false,
    enableThinking: role === "VISION" ? enableThinking ?? false : undefined,
    includeHighResolutionImages: role === "VISION" && highResolutionImages === true,
    includeResponseFormat: role === "VISION" && Boolean(responseFormat && typeof responseFormat === "object")
  });
}

export function validateCoordinateProviderResponse({ modelName, payload } = {}) {
  const model = String(modelName || "").trim();
  if (![COORDINATE_PROVIDER_MODELS.VISION, COORDINATE_PROVIDER_MODELS.OCR].includes(model)) {
    return Object.freeze({ applied: false, role: "UNMANAGED" });
  }
  if (String(payload?.model || "").trim() !== model) {
    throw fixedError(PROVIDER_COMPATIBILITY_ERROR_CODE.RESPONSE_MODEL_MISMATCH);
  }
  if (!Array.isArray(payload?.choices) || payload.choices.length !== 1) {
    throw fixedError(PROVIDER_COMPATIBILITY_ERROR_CODE.RESPONSE_CHOICE_COUNT_INVALID);
  }
  const choice = payload.choices[0];
  if (String(choice?.finish_reason || "").trim() !== "stop") {
    throw fixedError(PROVIDER_COMPATIBILITY_ERROR_CODE.RESPONSE_NOT_FINAL);
  }
  if (!hasFinalContent(choice?.message?.content)) {
    throw fixedError(PROVIDER_COMPATIBILITY_ERROR_CODE.RESPONSE_CONTENT_EMPTY);
  }
  const promptTokens = payload?.usage?.prompt_tokens;
  const completionTokens = payload?.usage?.completion_tokens;
  const totalTokens = payload?.usage?.total_tokens;
  if (![promptTokens, completionTokens, totalTokens].every(value => (
    typeof value === "number" && Number.isSafeInteger(value)
  ))
    || promptTokens < 0 || completionTokens < 0 || totalTokens < 0
    || promptTokens + completionTokens !== totalTokens) {
    throw fixedError(PROVIDER_COMPATIBILITY_ERROR_CODE.RESPONSE_USAGE_INVALID);
  }
  return Object.freeze({
    applied: true,
    role: model === COORDINATE_PROVIDER_MODELS.VISION ? "VISION" : "OCR",
    responseContractValid: true
  });
}
