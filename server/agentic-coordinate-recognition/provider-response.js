import { normalizeAgenticCoordinateResult } from "./contract.js";

function extractTextContent(response) {
  const content = response?.choices?.[0]?.message?.content;
  if (typeof content === "string") return content.trim();
  if (Array.isArray(content)) {
    return content
      .map(item => (typeof item === "string" ? item : item?.text))
      .filter(item => typeof item === "string")
      .join("")
      .trim();
  }
  return "";
}

function stripSingleJsonFence(text) {
  const match = /^```(?:json)?\s*([\s\S]*?)\s*```$/i.exec(text);
  return match ? match[1].trim() : text;
}

export function parseAgenticProviderResponse(response) {
  const content = stripSingleJsonFence(extractTextContent(response));
  if (!content) throw new Error("provider returned no JSON content");

  let payload;
  try {
    payload = JSON.parse(content);
  } catch {
    throw new Error("provider returned invalid JSON content");
  }

  return normalizeAgenticCoordinateResult(payload);
}

