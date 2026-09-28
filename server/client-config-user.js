const AUTHORITATIVE_USAGE_FIELDS = Object.freeze([
  "free_convert_count",
  "free_judge_count",
  "paid_convert_count",
  "paid_judge_count",
  "is_vip",
  "freeConvertCount",
  "freeJudgeCount",
  "paidConvertCount",
  "paidJudgeCount",
  "isVip",
  "convert_remaining",
  "judge_remaining",
  "convertRemaining",
  "judgeRemaining",
  "vip_convert_limit",
  "vip_judge_limit",
  "vip_convert_used",
  "vip_judge_used",
  "vip_convert_remaining",
  "vip_judge_remaining"
]);

export function applyAuthoritativeUsageQuota(clientUser, authoritativeQuota) {
  if (!clientUser || typeof clientUser !== "object") return clientUser ?? null;
  if (!authoritativeQuota || typeof authoritativeQuota !== "object") return clientUser;
  const merged = { ...clientUser };
  for (const field of AUTHORITATIVE_USAGE_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(authoritativeQuota, field)) {
      merged[field] = authoritativeQuota[field];
    }
  }
  return merged;
}
