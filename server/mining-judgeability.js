export const MINING_JUDGEABILITY_CONTRACT_VERSION = "mining_judgeability_v1";

export const MINING_JUDGEABILITY_STATUS = Object.freeze({
  JUDGEABLE: "JUDGEABLE",
  FAILED: "FAILED"
});

export const MINING_JUDGEABILITY_FAILURE_REASON = Object.freeze({
  ANNOTATION_OCCLUDED: "ANNOTATION_OCCLUDED",
  UI_OCCLUDED: "UI_OCCLUDED",
  CLOUD_OBSCURED: "CLOUD_OBSCURED",
  TARGET_OCCLUDED: "TARGET_OCCLUDED",
  RESOLUTION_TOO_LOW: "RESOLUTION_TOO_LOW",
  TARGET_NOT_IDENTIFIABLE: "TARGET_NOT_IDENTIFIABLE",
  INSUFFICIENT_SPATIAL_CONTEXT: "INSUFFICIENT_SPATIAL_CONTEXT"
});

export const MINING_JUDGEABILITY_FAILURE_PRECEDENCE = Object.freeze([
  MINING_JUDGEABILITY_FAILURE_REASON.ANNOTATION_OCCLUDED,
  MINING_JUDGEABILITY_FAILURE_REASON.UI_OCCLUDED,
  MINING_JUDGEABILITY_FAILURE_REASON.CLOUD_OBSCURED,
  MINING_JUDGEABILITY_FAILURE_REASON.TARGET_OCCLUDED,
  MINING_JUDGEABILITY_FAILURE_REASON.RESOLUTION_TOO_LOW,
  MINING_JUDGEABILITY_FAILURE_REASON.TARGET_NOT_IDENTIFIABLE,
  MINING_JUDGEABILITY_FAILURE_REASON.INSUFFICIENT_SPATIAL_CONTEXT
]);

const SAFE_MESSAGE_BY_REASON = Object.freeze({
  ANNOTATION_OCCLUDED: "目标区域被不透明标注遮挡，当前图片不足以进行可靠判读。请关闭填充或标注后重新上传。",
  UI_OCCLUDED: "目标区域被界面元素遮挡，当前图片不足以进行可靠判读。请关闭菜单或面板后重新上传。",
  CLOUD_OBSCURED: "目标区域受到云层或云影遮挡，当前图片不足以进行可靠判读。请更换影像后重新上传。",
  TARGET_OCCLUDED: "目标区域存在遮挡，当前图片不足以进行可靠判读。请提供无遮挡图片后重试。",
  RESOLUTION_TOO_LOW: "图片清晰度或缩放级别不足，当前无法进行可靠判读。请上传更清晰的图片。",
  TARGET_NOT_IDENTIFIABLE: "当前图片中无法明确识别需要分析的目标区域。请标明目标并补充清晰图片。",
  INSUFFICIENT_SPATIAL_CONTEXT: "当前图片缺少可靠判读所需的周边空间信息。请补充更完整的区域截图。"
});

const REASON_SIGNAL_PATTERNS = Object.freeze({
  ANNOTATION_OCCLUDED: /ANNOTATION_OCCLUDED|(?:不透明|实心|填充|覆盖)[^\n。；;]{0,30}(?:标注|批注|多边形|polygon|图形|绘制面)|(?:标注|批注|多边形|polygon|图形|绘制面)[^\n。；;]{0,30}(?:不透明|实心|填充|覆盖)/i,
  UI_OCCLUDED: /UI_OCCLUDED|(?:菜单|弹窗|面板|图例|控件|界面|工具栏|窗口)[^\n。；;]{0,30}(?:遮挡|覆盖)|(?:遮挡|覆盖)[^\n。；;]{0,30}(?:菜单|弹窗|面板|图例|控件|界面|工具栏|窗口)/i,
  CLOUD_OBSCURED: /CLOUD_OBSCURED|(?:云层|云影|云雾|大气)[^\n。；;]{0,30}(?:遮挡|覆盖|不可见|看不清)|(?:遮挡|覆盖)[^\n。；;]{0,30}(?:云层|云影|云雾)/i,
  TARGET_OCCLUDED: /TARGET_OCCLUDED|(?:目标|目标区|矿区|区域)[^\n。；;]{0,30}(?:遮挡|覆盖|不可见)|(?:遮挡|覆盖)[^\n。；;]{0,30}(?:目标|目标区|矿区|区域)/i,
  RESOLUTION_TOO_LOW: /RESOLUTION_TOO_LOW|(?:分辨率|清晰度|像素|缩放|压缩)[^\n。；;]{0,30}(?:过低|太低|不足|模糊|看不清|无法辨认)|(?:模糊|看不清)[^\n。；;]{0,30}(?:目标|区域|细节)/i,
  TARGET_NOT_IDENTIFIABLE: /TARGET_NOT_IDENTIFIABLE|(?:目标|目标区|对象|矿区)[^\n。；;]{0,30}(?:无法识别|不能识别|无法确定|不可辨认|不明确)|(?:无法确定|不能确定)[^\n。；;]{0,30}(?:目标|分析区域)/i,
  INSUFFICIENT_SPATIAL_CONTEXT: /INSUFFICIENT_SPATIAL_CONTEXT|(?:空间|周边|上下游|尺度|方向|连续地貌|环境)[^\n。；;]{0,30}(?:信息|语境|范围|证据)?[^\n。；;]{0,12}(?:不足|缺少|不完整)|(?:缺少|不足)[^\n。；;]{0,30}(?:周边|上下游|尺度|方向|空间语境|连续地貌)/i
});

function extractUniqueBracketSection(text, sectionName) {
  const source = String(text || "");
  const matches = [];
  const sectionPattern = /【([^】]+)】\s*([\s\S]*?)(?=【[^】]+】|$)/g;
  let match;
  while ((match = sectionPattern.exec(source)) !== null) {
    if (String(match[1] || "").trim() === sectionName) {
      matches.push(String(match[2] || "").trim());
    }
  }
  return Object.freeze({
    count: matches.length,
    value: matches.length === 1 ? matches[0] : ""
  });
}

function normalizeJudgeabilityStatus(value) {
  const normalized = String(value || "").trim().replace(/\s+/g, " ").toUpperCase();
  return Object.values(MINING_JUDGEABILITY_STATUS).includes(normalized) ? normalized : "";
}

function collectFailureReasons(text) {
  const source = String(text || "");
  return MINING_JUDGEABILITY_FAILURE_PRECEDENCE.filter(reason => REASON_SIGNAL_PATTERNS[reason].test(source));
}

export function evaluateMiningJudgeability(providerOutput) {
  const source = String(providerOutput || "").trim();
  const statusSection = extractUniqueBracketSection(source, "可判读性");
  const reasonSection = extractUniqueBracketSection(source, "可判读性原因");

  if (statusSection.count !== 1 || reasonSection.count !== 1) {
    return Object.freeze({
      status: MINING_JUDGEABILITY_STATUS.FAILED,
      primaryReason: MINING_JUDGEABILITY_FAILURE_REASON.TARGET_NOT_IDENTIFIABLE,
      contractVersion: MINING_JUDGEABILITY_CONTRACT_VERSION
    });
  }

  const status = normalizeJudgeabilityStatus(statusSection.value);
  const reasons = collectFailureReasons(`${reasonSection.value}\n${source}`);

  if (reasons.length > 0) {
    return Object.freeze({
      status: MINING_JUDGEABILITY_STATUS.FAILED,
      primaryReason: reasons[0],
      contractVersion: MINING_JUDGEABILITY_CONTRACT_VERSION
    });
  }

  const normalizedReason = String(reasonSection.value || "").trim().replace(/\s+/g, " ").toUpperCase();
  if (status === MINING_JUDGEABILITY_STATUS.JUDGEABLE && normalizedReason === "JUDGEABLE") {
    return Object.freeze({
      status: MINING_JUDGEABILITY_STATUS.JUDGEABLE,
      primaryReason: null,
      contractVersion: MINING_JUDGEABILITY_CONTRACT_VERSION
    });
  }

  return Object.freeze({
    status: MINING_JUDGEABILITY_STATUS.FAILED,
    primaryReason: MINING_JUDGEABILITY_FAILURE_REASON.TARGET_NOT_IDENTIFIABLE,
    contractVersion: MINING_JUDGEABILITY_CONTRACT_VERSION
  });
}

export function buildMiningJudgeabilityFailurePayload(evaluation) {
  const primaryReason = MINING_JUDGEABILITY_FAILURE_PRECEDENCE.includes(evaluation?.primaryReason)
    ? evaluation.primaryReason
    : MINING_JUDGEABILITY_FAILURE_REASON.TARGET_NOT_IDENTIFIABLE;

  return Object.freeze({
    success: false,
    reason: "JUDGEABILITY_FAILED",
    judgeability: Object.freeze({
      status: MINING_JUDGEABILITY_STATUS.FAILED,
      primary_reason: primaryReason,
      contract_version: MINING_JUDGEABILITY_CONTRACT_VERSION
    }),
    safe_message: SAFE_MESSAGE_BY_REASON[primaryReason],
    retry: Object.freeze({
      eligible: true,
      user_initiated_only: true,
      user_charge: false
    })
  });
}

export function applyMiningJudgeabilityGate(providerOutput) {
  const evaluation = evaluateMiningJudgeability(providerOutput);
  if (evaluation.status === MINING_JUDGEABILITY_STATUS.JUDGEABLE) {
    return Object.freeze({ allowed: true, evaluation });
  }

  return Object.freeze({
    allowed: false,
    statusCode: 422,
    evaluation,
    payload: buildMiningJudgeabilityFailurePayload(evaluation)
  });
}
