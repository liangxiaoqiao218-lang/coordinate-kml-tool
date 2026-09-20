import { buildAgenticCoordinateRecognitionPrompt } from "./prompt.js";
import { parseAgenticProviderResponse } from "./provider-response.js";

function assertImageDataUrl(imageDataUrl) {
  if (typeof imageDataUrl !== "string" || !/^data:image\/(?:jpeg|png);base64,/i.test(imageDataUrl)) {
    throw new Error("imageDataUrl must contain one JPEG or PNG image");
  }
}

export async function runAgenticCoordinateRecognition({
  imageDataUrl,
  modelName,
  providerCall
}) {
  assertImageDataUrl(imageDataUrl);
  if (typeof providerCall !== "function") throw new Error("providerCall is required");

  let providerCallCount = 0;
  const response = await (() => {
    providerCallCount += 1;
    return providerCall({
      modelName,
      prompt: buildAgenticCoordinateRecognitionPrompt(),
      imageItems: [
        {
          type: "image_url",
          image_url: { url: imageDataUrl }
        }
      ],
      temperature: 0,
      maxTokens: 5000,
      stageName: "agentic_one_shot",
      lowValue: false
    });
  })();

  const result = parseAgenticProviderResponse(response);
  return Object.freeze({
    result,
    execution: Object.freeze({
      providerCallCount,
      retryCount: 0,
      fallbackCount: 0
    })
  });
}

