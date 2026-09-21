import { normalizeAgenticCoordinateResult } from "./contract.js";
import { assertAgenticCoordinateConsistency } from "./consistency.js";

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

function preserveUnsupportedGroupingAsSingleSequence(payload) {
  if (payload?.success !== true || !Array.isArray(payload.groups) || payload.groups.length <= 1) {
    return payload;
  }

  const groupNames = payload.groups.map(group => String(group?.name || '').trim());
  const normalizedGroupNames = groupNames.map(name => name.toLocaleLowerCase());
  const hasExplicitDistinctNames = groupNames.every(Boolean)
    && new Set(normalizedGroupNames).size === normalizedGroupNames.length;
  if (hasExplicitDistinctNames) return payload;
  if (payload.warnings !== undefined
    && payload.warnings !== null
    && !Array.isArray(payload.warnings)) return payload;

  return {
    ...payload,
    resultStatus: 'needs_review',
    geometryType: 'Unknown',
    groups: [{
      name: null,
      points: payload.groups.flatMap(group => (
        Array.isArray(group?.points) ? group.points : []
      )),
    }],
    warnings: [
      ...(payload.warnings || []),
      'Grouping evidence is insufficient; all points were preserved in source order as one ungrouped sequence.',
    ],
  };
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

  const groupingSafePayload = preserveUnsupportedGroupingAsSingleSequence(payload);
  return assertAgenticCoordinateConsistency(normalizeAgenticCoordinateResult(groupingSafePayload));
}
