import {
  ACQUISITION_AUTHORITY_FIELDS,
  ACQUISITION_PROVENANCE,
  ACQUISITION_SENSITIVE_FIELDS,
  ACQUISITION_SOURCE_TYPE,
  ACQUISITION_STATUS,
  createAcquisitionCandidate,
  createAcquisitionResult,
} from "./contracts.js";
import {
  ACQUISITION_HARD_DEADLINE_MS,
  ACQUISITION_MAX_PROVIDER_CALLS,
  createAcquisitionBudget,
} from "./budget.js";

export const PRIMARY_ACQUISITION_PROVIDER_TIMEOUT_MS = 40000;
export const PRIMARY_ACQUISITION_MAX_PROVIDER_CALLS = 1;

export const PRIMARY_ACQUISITION_ERROR = Object.freeze({
  PROVIDER_TIMEOUT: "PROVIDER_TIMEOUT",
  PROVIDER_UNAVAILABLE: "PROVIDER_UNAVAILABLE",
  PROVIDER_AUTH_ERROR: "PROVIDER_AUTH_ERROR",
  PROVIDER_HTTP_ERROR: "PROVIDER_HTTP_ERROR",
  PROVIDER_EMPTY_RESPONSE: "PROVIDER_EMPTY_RESPONSE",
  PROVIDER_INVALID_RESPONSE: "PROVIDER_INVALID_RESPONSE",
  PROVIDER_REQUEST_FAILED: "PROVIDER_REQUEST_FAILED",
  DEADLINE_EXCEEDED: "DEADLINE_EXCEEDED",
  TEXT_EXTRACTION_FAILURE: "TEXT_EXTRACTION_FAILURE",
});

export const PRIMARY_PROVIDER_STATUS = Object.freeze({
  NOT_STARTED: "NOT_STARTED",
  SUCCESS: "SUCCESS",
  TIMEOUT: "TIMEOUT",
  ERROR: "ERROR",
});

export const PRIMARY_JSON_PARSE_STATUS = Object.freeze({
  JSON_PARSE_SUCCESS: "JSON_PARSE_SUCCESS",
  JSON_PARSE_FAILED: "JSON_PARSE_FAILED",
});

export const PRIMARY_JSON_PARSE_REASON = Object.freeze({
  NONE: "NONE",
  EMPTY_CONTENT: "EMPTY_CONTENT",
  MALFORMED_JSON: "MALFORMED_JSON",
  TRUNCATED_JSON: "TRUNCATED_JSON",
  NON_JSON_RESPONSE: "NON_JSON_RESPONSE",
});

export const PRIMARY_SCHEMA_VALIDATION_STATUS = Object.freeze({
  SCHEMA_VALID: "SCHEMA_VALID",
  SCHEMA_INVALID: "SCHEMA_INVALID",
});

export const PRIMARY_SCHEMA_VALIDATION_REASON = Object.freeze({
  NONE: "NONE",
  MISSING_RAW_TEXT: "MISSING_RAW_TEXT",
  INVALID_BLOCKS: "INVALID_BLOCKS",
  INVALID_ROWS: "INVALID_ROWS",
  INVALID_HEADERS: "INVALID_HEADERS",
  AUTHORITY_FIELD_REJECTED: "AUTHORITY_FIELD_REJECTED",
});

export const PRIMARY_CANDIDATE_CONSTRUCTION_STATUS = Object.freeze({
  CANDIDATE_CONSTRUCTION_SUCCESS: "CANDIDATE_CONSTRUCTION_SUCCESS",
  CANDIDATE_CONSTRUCTION_EMPTY: "CANDIDATE_CONSTRUCTION_EMPTY",
  CANDIDATE_CONSTRUCTION_REJECTED: "CANDIDATE_CONSTRUCTION_REJECTED",
});

export const PRIMARY_ACQUISITION_PROMPT = `You are extracting visible coordinate-related text and table structure from one image.

Return only JSON with this shape:
{
  "rawText": "all visible relevant text in source order",
  "blocks": [
    {
      "type": "text|table|coordinate_block|header_block",
      "text": "block text in source order",
      "headers": ["visible headers if any"],
      "rows": [{"label":"visible point label", "cells":["visible cell values in source order"]}],
      "confidence": 0.0
    }
  ],
  "documentCues": ["visible non-authoritative CRS/datum/zone/title/header cues"]
}

Rules:
- Preserve source order.
- Preserve decimal precision exactly as visible.
- Preserve hemisphere letters and words exactly as visible.
- Preserve point labels exactly as visible.
- Preserve table headers with their rows.
- Do not infer missing values.
- Do not correct suspicious values.
- Do not convert coordinates.
- Do not choose a coordinate system.
- Do not choose a coordinate type.
- Do not generate KML.
- Do not provide recognizerId, coordinateType, winner, owner, confirmationStatus, qualityGateStatus, kmlReady, kmlPermission, shadowWinner, arbitrationProposal, or migrationStatus.`;

const BLOCK_TYPE_TO_SOURCE_TYPE = Object.freeze({
  text: ACQUISITION_SOURCE_TYPE.TEXT_BLOCK,
  table: ACQUISITION_SOURCE_TYPE.TABLE,
  coordinate_block: ACQUISITION_SOURCE_TYPE.COORDINATE_BLOCK,
  header_block: ACQUISITION_SOURCE_TYPE.TEXT_BLOCK,
});

const PRIMARY_AUTHORITY_FIELD_SET = new Set(ACQUISITION_AUTHORITY_FIELDS);
const PRIMARY_SENSITIVE_FIELD_SET = new Set(ACQUISITION_SENSITIVE_FIELDS);

function cleanString(value, fallback = "") {
  const text = String(value ?? "")
    .replace(/\u0000/g, "")
    .replace(/[ \t]+/g, " ")
    .trim();
  return text || fallback;
}

function cleanOneLine(value, fallback = "") {
  const text = cleanString(value)
    .replace(/[\r\n]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return text || fallback;
}

function cleanStringArray(value) {
  if (!Array.isArray(value)) return [];
  return value.map((item) => cleanOneLine(item)).filter(Boolean);
}

function sanitizeProviderPrimitive(value) {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "boolean") return value;
  if (typeof value === "string") return cleanString(value);
  if (value === null || value === undefined) return null;
  return cleanString(value);
}

function stripProviderExtractionObject(value, depth = 0) {
  if (!value || typeof value !== "object" || depth > 6) return sanitizeProviderPrimitive(value);
  if (Array.isArray(value)) {
    return value.map((item) => stripProviderExtractionObject(item, depth + 1));
  }
  const output = {};
  for (const [key, raw] of Object.entries(value)) {
    const cleanKey = cleanOneLine(key);
    if (!cleanKey || PRIMARY_AUTHORITY_FIELD_SET.has(cleanKey) || PRIMARY_SENSITIVE_FIELD_SET.has(cleanKey)) {
      continue;
    }
    output[cleanKey] = stripProviderExtractionObject(raw, depth + 1);
  }
  return output;
}

function normalizeRows(rows = []) {
  if (!Array.isArray(rows)) return [];
  return rows
    .map((row, rowIndex) => {
      if (Array.isArray(row)) {
        return {
          rowIndex: rowIndex + 1,
          cells: row.map((cell) => cleanOneLine(cell)),
        };
      }
      if (row && typeof row === "object") {
        const output = {};
        for (const [key, value] of Object.entries(row)) {
          if (Array.isArray(value)) {
            output[cleanOneLine(key)] = value.map((cell) => cleanOneLine(cell));
          } else {
            output[cleanOneLine(key)] = cleanOneLine(value);
          }
        }
        return output;
      }
      const text = cleanOneLine(row);
      return text ? { rowIndex: rowIndex + 1, text } : null;
    })
    .filter(Boolean);
}

function getRowValue(row = {}, key = "") {
  if (!row || typeof row !== "object") return "";
  if (Object.prototype.hasOwnProperty.call(row, key)) return row[key];
  const normalizedKey = cleanOneLine(key).toLowerCase();
  const match = Object.keys(row).find((item) => cleanOneLine(item).toLowerCase() === normalizedKey);
  return match ? row[match] : "";
}

function rowValuesForHeaders(headers = [], row = {}) {
  if (!headers.length || !row || typeof row !== "object") return [];
  if (Array.isArray(row.cells)) {
    const cells = row.cells.map((cell) => cleanOneLine(cell));
    const label = cleanOneLine(row.label ?? row.point ?? row.no ?? row.number ?? row.rowIndex);
    if (label && cells.length === headers.length - 1) return [label, ...cells];
    return cells;
  }
  return headers.map((header) => cleanOneLine(getRowValue(row, header)));
}

function hasCaseInsensitiveKey(row = {}, key = "") {
  const normalizedKey = cleanOneLine(key).toLowerCase();
  return Boolean(normalizedKey)
    && Object.keys(row).some((item) => cleanOneLine(item).toLowerCase() === normalizedKey);
}

function expandRowByHeaders(row = {}, headers = []) {
  if (!row || typeof row !== "object" || !headers.length) return row;
  const output = {};
  const values = rowValuesForHeaders(headers, row);
  const headerKeys = new Set(headers.map((header) => cleanOneLine(header).toLowerCase()).filter(Boolean));
  const hasLabelHeader = headers.some((header) => /^(label|point|pt|no|no\.|number|num|id|n|n°|№ points)$/i.test(cleanOneLine(header)));
  for (const [key, value] of Object.entries(row)) {
    const normalizedKey = cleanOneLine(key).toLowerCase();
    if (!normalizedKey || normalizedKey === "cells" || (hasLabelHeader && ["label", "point", "no", "number", "rowindex"].includes(normalizedKey))) {
      continue;
    }
    output[cleanOneLine(key)] = Array.isArray(value)
      ? value.map((cell) => cleanOneLine(cell))
      : cleanOneLine(value);
  }
  headers.forEach((header, index) => {
    const key = cleanOneLine(header);
    const value = cleanOneLine(values[index]);
    if (key && value && !hasCaseInsensitiveKey(output, key)) output[key] = value;
  });
  if (!hasLabelHeader && !hasCaseInsensitiveKey(output, "label")) {
    const label = cleanOneLine(row.label ?? row.point ?? row.no ?? row.number);
    if (label) output.label = label;
  }
  return output;
}

function expandRowsByHeaders(rows = [], headers = []) {
  if (!Array.isArray(rows)) return [];
  return rows.map((row) => expandRowByHeaders(row, headers));
}

function joinRows(headers = [], rows = []) {
  const lines = [];
  if (headers.length) lines.push(headers.join(" | "));
  for (const row of rows) {
    const headerValues = rowValuesForHeaders(headers, row).filter(Boolean);
    if (headerValues.length) {
      lines.push(headerValues.join(" | "));
      continue;
    }
    const values = Object.values(row)
      .flatMap((value) => (Array.isArray(value) ? value : [value]))
      .map((value) => cleanOneLine(value))
      .filter(Boolean);
    if (values.length) lines.push(values.join(" | "));
  }
  return lines.join("\n");
}

function joinCandidateText(parts = []) {
  const lines = [];
  const seen = new Set();
  for (const part of parts) {
    const text = cleanString(part);
    if (!text) continue;
    for (const line of text.split(/\r?\n/)) {
      const normalized = cleanOneLine(line);
      if (!normalized || seen.has(normalized)) continue;
      seen.add(normalized);
      lines.push(line.trim());
    }
  }
  return lines.join("\n");
}

function isStructuredBlock(block = {}) {
  return Array.isArray(block.headers)
    && block.headers.length > 0
    && Array.isArray(block.rows)
    && block.rows.length > 0;
}

function parseJsonFromProviderText(value = "") {
  const text = String(value || "").trim();
  if (!text) {
    return Object.freeze({
      parsed: null,
      status: PRIMARY_JSON_PARSE_STATUS.JSON_PARSE_FAILED,
      reason: PRIMARY_JSON_PARSE_REASON.EMPTY_CONTENT,
      contentLength: 0,
    });
  }
  try {
    return Object.freeze({
      parsed: JSON.parse(text),
      status: PRIMARY_JSON_PARSE_STATUS.JSON_PARSE_SUCCESS,
      reason: PRIMARY_JSON_PARSE_REASON.NONE,
      contentLength: text.length,
    });
  } catch {
    const fenced = text.match(/```(?:json)?\s*([\s\S]*?)```/i);
    if (fenced) {
      try {
        return Object.freeze({
          parsed: JSON.parse(fenced[1].trim()),
          status: PRIMARY_JSON_PARSE_STATUS.JSON_PARSE_SUCCESS,
          reason: PRIMARY_JSON_PARSE_REASON.NONE,
          contentLength: text.length,
        });
      } catch {
        return Object.freeze({
          parsed: null,
          status: PRIMARY_JSON_PARSE_STATUS.JSON_PARSE_FAILED,
          reason: PRIMARY_JSON_PARSE_REASON.MALFORMED_JSON,
          contentLength: text.length,
        });
      }
    }
    const start = text.indexOf("{");
    const end = text.lastIndexOf("}");
    if (start >= 0 && end > start) {
      try {
        return Object.freeze({
          parsed: JSON.parse(text.slice(start, end + 1)),
          status: PRIMARY_JSON_PARSE_STATUS.JSON_PARSE_SUCCESS,
          reason: PRIMARY_JSON_PARSE_REASON.NONE,
          contentLength: text.length,
        });
      } catch {
        return Object.freeze({
          parsed: null,
          status: PRIMARY_JSON_PARSE_STATUS.JSON_PARSE_FAILED,
          reason: PRIMARY_JSON_PARSE_REASON.MALFORMED_JSON,
          contentLength: text.length,
        });
      }
    }
    return Object.freeze({
      parsed: null,
      status: PRIMARY_JSON_PARSE_STATUS.JSON_PARSE_FAILED,
      reason: start >= 0 && end < start
        ? PRIMARY_JSON_PARSE_REASON.TRUNCATED_JSON
        : PRIMARY_JSON_PARSE_REASON.NON_JSON_RESPONSE,
      contentLength: text.length,
    });
  }
}

function getProviderConfig(env = process.env) {
  const apiKey = env.ALIYUN_API_KEY || env.DASHSCOPE_API_KEY || "";
  return Object.freeze({
    available: Boolean(String(apiKey || "").trim()),
    apiKey,
    baseURL: env.ALIYUN_BASE_URL || env.DASHSCOPE_BASE_URL || "https://dashscope.aliyuncs.com/compatible-mode/v1",
    model: env.ALIYUN_VISION_MODEL || env.DASHSCOPE_VISION_MODEL || "qwen-vl-plus",
  });
}

function imageToDataUrl({ imageBase64, mimeType = "image/jpeg" } = {}) {
  const encoded = cleanString(imageBase64);
  if (!encoded) return "";
  return `data:${cleanOneLine(mimeType, "image/jpeg")};base64,${encoded}`;
}

export async function callPrimaryVisionProvider({
  imageBase64,
  mimeType = "image/jpeg",
  prompt = PRIMARY_ACQUISITION_PROMPT,
  timeoutMs = PRIMARY_ACQUISITION_PROVIDER_TIMEOUT_MS,
  env = process.env,
  fetchImpl = globalThis.fetch,
} = {}) {
  const config = getProviderConfig(env);
  if (!config.available) {
    return Object.freeze({
      ok: false,
      errorCode: PRIMARY_ACQUISITION_ERROR.PROVIDER_AUTH_ERROR,
      provider: "aliyun_dashscope_compatible",
      model: config.model,
    });
  }
  if (typeof fetchImpl !== "function") {
    return Object.freeze({
      ok: false,
      errorCode: PRIMARY_ACQUISITION_ERROR.PROVIDER_UNAVAILABLE,
      provider: "aliyun_dashscope_compatible",
      model: config.model,
    });
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.max(1, Number(timeoutMs) || PRIMARY_ACQUISITION_PROVIDER_TIMEOUT_MS));
  try {
    const response = await fetchImpl(`${String(config.baseURL).replace(/\/+$/, "")}/chat/completions`, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${config.apiKey}`,
        "Content-Type": "application/json",
      },
      signal: controller.signal,
      body: JSON.stringify({
        model: config.model,
        temperature: 0,
        messages: [
          {
            role: "user",
            content: [
              { type: "text", text: prompt },
              { type: "image_url", image_url: { url: imageToDataUrl({ imageBase64, mimeType }) } },
            ],
          },
        ],
      }),
    });

    if (!response.ok) {
      return Object.freeze({
        ok: false,
        errorCode: response.status === 401 || response.status === 403
          ? PRIMARY_ACQUISITION_ERROR.PROVIDER_AUTH_ERROR
          : PRIMARY_ACQUISITION_ERROR.PROVIDER_HTTP_ERROR,
        provider: "aliyun_dashscope_compatible",
        model: config.model,
        status: response.status,
        responseReceived: true,
      });
    }

    const payload = await response.json();
    const text = payload?.choices?.[0]?.message?.content;
    if (!cleanString(text)) {
      return Object.freeze({
        ok: false,
        errorCode: PRIMARY_ACQUISITION_ERROR.PROVIDER_EMPTY_RESPONSE,
        provider: "aliyun_dashscope_compatible",
        model: config.model,
        status: response.status,
        responseReceived: true,
      });
    }
    return Object.freeze({
      ok: true,
      provider: "aliyun_dashscope_compatible",
      model: config.model,
      text,
      status: response.status,
      responseReceived: true,
    });
  } catch (error) {
    return Object.freeze({
      ok: false,
      errorCode: error?.name === "AbortError"
        ? PRIMARY_ACQUISITION_ERROR.PROVIDER_TIMEOUT
        : PRIMARY_ACQUISITION_ERROR.PROVIDER_REQUEST_FAILED,
      provider: "aliyun_dashscope_compatible",
      model: config.model,
      message: cleanOneLine(error?.message).slice(0, 160),
    });
  } finally {
    clearTimeout(timer);
  }
}

function normalizeProviderBlock(block = {}, index = 0) {
  const sanitized = stripProviderExtractionObject(block);
  const type = cleanOneLine(sanitized.type, "text");
  const headers = cleanStringArray(sanitized.headers);
  const rows = normalizeRows(sanitized.rows);
  const text = cleanString(sanitized.text) || joinRows(headers, rows);
  return Object.freeze({
    id: cleanOneLine(sanitized.id, `block_${index + 1}`),
    type,
    text,
    headers,
    rows,
    confidence: Number.isFinite(Number(sanitized.confidence)) ? Number(sanitized.confidence) : null,
  });
}

function collectSchemaValidationReasons(value = {}) {
  const reasons = [];
  const source = value && typeof value === "object" ? value : {};
  if (!cleanString(source.rawText)) reasons.push(PRIMARY_SCHEMA_VALIDATION_REASON.MISSING_RAW_TEXT);
  if (source.blocks !== undefined && !Array.isArray(source.blocks)) {
    reasons.push(PRIMARY_SCHEMA_VALIDATION_REASON.INVALID_BLOCKS);
  }
  if (Array.isArray(source.blocks)) {
    for (const block of source.blocks) {
      if (block?.headers !== undefined && !Array.isArray(block.headers)) {
        reasons.push(PRIMARY_SCHEMA_VALIDATION_REASON.INVALID_HEADERS);
      }
      if (block?.rows !== undefined && !Array.isArray(block.rows)) {
        reasons.push(PRIMARY_SCHEMA_VALIDATION_REASON.INVALID_ROWS);
      }
      const authorityFields = block && typeof block === "object"
        ? Object.keys(block).filter((key) => PRIMARY_AUTHORITY_FIELD_SET.has(key))
        : [];
      if (authorityFields.length) reasons.push(PRIMARY_SCHEMA_VALIDATION_REASON.AUTHORITY_FIELD_REJECTED);
    }
  }
  const topLevelAuthorityFields = source && typeof source === "object"
    ? Object.keys(source).filter((key) => PRIMARY_AUTHORITY_FIELD_SET.has(key))
    : [];
  if (topLevelAuthorityFields.length) reasons.push(PRIMARY_SCHEMA_VALIDATION_REASON.AUTHORITY_FIELD_REJECTED);
  return Object.freeze([...new Set(reasons)]);
}

function normalizeProviderOutput(providerText = "") {
  const parse = parseJsonFromProviderText(providerText);
  const parsed = parse.parsed;
  if (!parsed || typeof parsed !== "object") {
    return Object.freeze({
      valid: false,
      reason: "provider_json_parse_failed",
      parseStatus: parse.status,
      parseReason: parse.reason,
      contentLength: parse.contentLength,
      schemaStatus: PRIMARY_SCHEMA_VALIDATION_STATUS.SCHEMA_INVALID,
      schemaReasons: Object.freeze([]),
      rawText: "",
      blocks: Object.freeze([]),
      documentCues: Object.freeze([]),
    });
  }
  const schemaReasons = collectSchemaValidationReasons(parsed);
  const sanitized = stripProviderExtractionObject(parsed);
  const blocks = Array.isArray(sanitized.blocks)
    ? sanitized.blocks.map(normalizeProviderBlock)
    : [];
  return Object.freeze({
    valid: true,
    reason: "provider_json_parse_succeeded",
    parseStatus: parse.status,
    parseReason: parse.reason,
    contentLength: parse.contentLength,
    schemaStatus: schemaReasons.length
      ? PRIMARY_SCHEMA_VALIDATION_STATUS.SCHEMA_INVALID
      : PRIMARY_SCHEMA_VALIDATION_STATUS.SCHEMA_VALID,
    schemaReasons,
    rawText: cleanString(sanitized.rawText),
    blocks: Object.freeze(blocks),
    documentCues: Object.freeze(cleanStringArray(sanitized.documentCues)),
  });
}

function buildCandidatesFromProviderOutput(output = {}, timing = {}) {
  const candidates = [];
  const structuredBlocks = output.blocks.filter(isStructuredBlock);
  const rawText = cleanString(output.rawText)
    || output.blocks.map((block) => block.text).filter(Boolean).join("\n\n");
  const expandedStructuredRows = structuredBlocks.flatMap((block) => expandRowsByHeaders(block.rows, block.headers));
  candidates.push(createAcquisitionCandidate({
    id: "primary_whole_image",
    text: joinCandidateText([output.documentCues.join("\n"), rawText]),
    structuredRows: expandedStructuredRows,
    headers: structuredBlocks[0]?.headers || [],
    documentCues: output.documentCues,
    sourceType: ACQUISITION_SOURCE_TYPE.WHOLE_IMAGE,
    provenance: ACQUISITION_PROVENANCE.PRIMARY,
    confidence: output.blocks.length ? Math.max(...output.blocks.map((block) => Number(block.confidence)).filter(Number.isFinite), 0) : null,
    timing: { durationMs: timing.durationMs },
  }));

  output.blocks.forEach((block, index) => {
    if (!block.text && block.rows.length === 0) return;
    const structuredRows = isStructuredBlock(block)
      ? expandRowsByHeaders(block.rows, block.headers)
      : block.rows;
    const text = isStructuredBlock(block)
      ? joinCandidateText([output.documentCues.join("\n"), block.text || joinRows(block.headers, block.rows)])
      : block.text;
    candidates.push(createAcquisitionCandidate({
      id: `primary_${block.id || index + 1}`,
      text,
      structuredRows,
      headers: block.headers,
      documentCues: output.documentCues,
      sourceType: BLOCK_TYPE_TO_SOURCE_TYPE[block.type] || ACQUISITION_SOURCE_TYPE.TEXT_BLOCK,
      provenance: ACQUISITION_PROVENANCE.PRIMARY,
      confidence: block.confidence,
      timing: { durationMs: timing.durationMs },
    }));
  });

  return candidates;
}

function makePrimaryDiagnostics(value = {}) {
  const candidateCount = Number(value.candidateCount);
  const structuredCandidateCount = Number(value.structuredCandidateCount);
  return Object.freeze({
    providerStatus: value.providerStatus || PRIMARY_PROVIDER_STATUS.NOT_STARTED,
    providerErrorCode: value.providerErrorCode || null,
    providerHttpStatus: Number.isFinite(Number(value.providerHttpStatus)) ? Number(value.providerHttpStatus) : null,
    providerDurationMs: Number.isFinite(Number(value.providerDurationMs)) ? Math.max(0, Number(value.providerDurationMs)) : 0,
    providerResponseReceived: value.providerResponseReceived === true,
    providerContentPresent: value.providerContentPresent === true,
    providerContentLength: Number.isFinite(Number(value.providerContentLength)) ? Math.max(0, Number(value.providerContentLength)) : 0,
    jsonParseStatus: value.jsonParseStatus || null,
    jsonParseReason: value.jsonParseReason || null,
    schemaValidationStatus: value.schemaValidationStatus || null,
    schemaValidationReason: cleanStringArray(Array.isArray(value.schemaValidationReason) ? value.schemaValidationReason : [value.schemaValidationReason]).join(",") || null,
    candidateConstructionStatus: value.candidateConstructionStatus || null,
    candidateConstructionReason: value.candidateConstructionReason || null,
    wholeImageCandidateCreated: value.wholeImageCandidateCreated === true,
    structuredCandidateCount: Number.isFinite(structuredCandidateCount) ? Math.max(0, structuredCandidateCount) : 0,
    finalCandidateCount: Number.isFinite(candidateCount) ? Math.max(0, candidateCount) : 0,
    candidateCount: Number.isFinite(candidateCount) ? Math.max(0, candidateCount) : 0,
  });
}

function withDiagnostics(result, diagnostics) {
  return Object.freeze({
    ...result,
    diagnostics: makePrimaryDiagnostics(diagnostics),
  });
}

export async function acquirePrimaryImage({
  imageBase64,
  mimeType = "image/jpeg",
  provider = callPrimaryVisionProvider,
  prompt = PRIMARY_ACQUISITION_PROMPT,
  budget = createAcquisitionBudget({ maxProviderCalls: PRIMARY_ACQUISITION_MAX_PROVIDER_CALLS }),
  clock = Date.now,
} = {}) {
  const start = Number(clock());
  const gate = budget.canStartProviderCall({
    minimumMs: 1,
    currentClock: clock,
  });
  if (!gate.allowed) {
    return withDiagnostics(createAcquisitionResult({
      status: ACQUISITION_STATUS.DEADLINE_EXCEEDED,
      candidates: [],
      timing: {
        totalDurationMs: Math.max(0, Number(clock()) - start),
        primaryDurationMs: 0,
        targetedDurationMs: 0,
      },
      providerCalls: 0,
      warnings: [gate.reason],
    }), {
      providerStatus: PRIMARY_PROVIDER_STATUS.NOT_STARTED,
      providerDurationMs: 0,
      candidateConstructionStatus: PRIMARY_CANDIDATE_CONSTRUCTION_STATUS.CANDIDATE_CONSTRUCTION_EMPTY,
      candidateConstructionReason: gate.reason,
      candidateCount: 0,
    });
  }

  const remainingMs = typeof budget.remainingMs === "function"
    ? budget.remainingMs(clock)
    : ACQUISITION_HARD_DEADLINE_MS;
  const timeoutMs = Math.min(PRIMARY_ACQUISITION_PROVIDER_TIMEOUT_MS, Math.max(1, remainingMs));
  const providerResult = await provider({
    imageBase64,
    mimeType,
    prompt,
    timeoutMs,
  });
  const durationMs = Math.max(0, Number(clock()) - start);

  if (!providerResult?.ok) {
    const warning = cleanOneLine(providerResult?.errorCode, PRIMARY_ACQUISITION_ERROR.PROVIDER_REQUEST_FAILED);
    return withDiagnostics(createAcquisitionResult({
      status: warning === PRIMARY_ACQUISITION_ERROR.PROVIDER_TIMEOUT
        ? ACQUISITION_STATUS.DEADLINE_EXCEEDED
        : ACQUISITION_STATUS.FAILED,
      candidates: [],
      timing: {
        totalDurationMs: durationMs,
        primaryDurationMs: durationMs,
        targetedDurationMs: 0,
      },
      providerCalls: 1,
      warnings: [warning],
    }), {
      providerStatus: warning === PRIMARY_ACQUISITION_ERROR.PROVIDER_TIMEOUT
        ? PRIMARY_PROVIDER_STATUS.TIMEOUT
        : PRIMARY_PROVIDER_STATUS.ERROR,
      providerErrorCode: warning,
      providerHttpStatus: providerResult?.status,
      providerDurationMs: durationMs,
      providerResponseReceived: providerResult?.responseReceived === true || Number.isFinite(Number(providerResult?.status)),
      providerContentPresent: false,
      candidateConstructionStatus: PRIMARY_CANDIDATE_CONSTRUCTION_STATUS.CANDIDATE_CONSTRUCTION_EMPTY,
      candidateConstructionReason: warning,
      candidateCount: 0,
    });
  }

  const output = normalizeProviderOutput(providerResult.text);
  if (!output.valid) {
    return withDiagnostics(createAcquisitionResult({
      status: ACQUISITION_STATUS.FAILED,
      candidates: [],
      timing: {
        totalDurationMs: durationMs,
        primaryDurationMs: durationMs,
        targetedDurationMs: 0,
      },
      providerCalls: 1,
      warnings: [PRIMARY_ACQUISITION_ERROR.PROVIDER_INVALID_RESPONSE],
    }), {
      providerStatus: PRIMARY_PROVIDER_STATUS.SUCCESS,
      providerHttpStatus: providerResult?.status,
      providerDurationMs: durationMs,
      providerResponseReceived: providerResult?.responseReceived === true,
      providerContentPresent: Boolean(cleanString(providerResult.text)),
      providerContentLength: output.contentLength,
      jsonParseStatus: output.parseStatus,
      jsonParseReason: output.parseReason,
      schemaValidationStatus: output.schemaStatus,
      schemaValidationReason: output.schemaReasons,
      candidateConstructionStatus: PRIMARY_CANDIDATE_CONSTRUCTION_STATUS.CANDIDATE_CONSTRUCTION_EMPTY,
      candidateConstructionReason: output.reason,
      candidateCount: 0,
    });
  }

  const candidates = buildCandidatesFromProviderOutput(output, { durationMs });
  return withDiagnostics(createAcquisitionResult({
    status: candidates.length ? ACQUISITION_STATUS.SUCCESS : ACQUISITION_STATUS.PARTIAL,
    candidates,
    timing: {
      totalDurationMs: durationMs,
      primaryDurationMs: durationMs,
      targetedDurationMs: 0,
    },
    providerCalls: 1,
    warnings: candidates.length ? [] : [PRIMARY_ACQUISITION_ERROR.TEXT_EXTRACTION_FAILURE],
  }), {
    providerStatus: PRIMARY_PROVIDER_STATUS.SUCCESS,
    providerHttpStatus: providerResult?.status,
    providerDurationMs: durationMs,
    providerResponseReceived: providerResult?.responseReceived === true,
    providerContentPresent: Boolean(cleanString(providerResult.text)),
    providerContentLength: output.contentLength,
    jsonParseStatus: output.parseStatus,
    jsonParseReason: output.parseReason,
    schemaValidationStatus: output.schemaStatus,
    schemaValidationReason: output.schemaReasons,
    candidateConstructionStatus: candidates.length
      ? PRIMARY_CANDIDATE_CONSTRUCTION_STATUS.CANDIDATE_CONSTRUCTION_SUCCESS
      : PRIMARY_CANDIDATE_CONSTRUCTION_STATUS.CANDIDATE_CONSTRUCTION_EMPTY,
    candidateConstructionReason: candidates.length ? "candidates_constructed" : PRIMARY_ACQUISITION_ERROR.TEXT_EXTRACTION_FAILURE,
    wholeImageCandidateCreated: candidates.some((candidate) => candidate.id === "primary_whole_image"),
    structuredCandidateCount: candidates.filter((candidate) => candidate.sourceType !== ACQUISITION_SOURCE_TYPE.WHOLE_IMAGE).length,
    candidateCount: candidates.length,
  });
}

export function getPrimaryProviderReadiness(env = process.env) {
  const config = getProviderConfig(env);
  return Object.freeze({
    available: config.available,
    provider: "aliyun_dashscope_compatible",
    model: config.model,
  });
}
