import {
  FREE_DAY_TIMEZONE,
  USAGE_EVENT_TYPE,
  getFreeDayKey,
  isServiceOperationId,
  toSharedQuotaPayload
} from "./usage-policy.js";

const PRODUCT_BY_EVENT = Object.freeze({
  [USAGE_EVENT_TYPE.COORDINATE]: "coordinate",
  [USAGE_EVENT_TYPE.JUDGE]: "judge"
});

export class SupabaseUsageLedger {
  constructor({ supabase }) {
    this.supabase = supabase || null;
  }

  async read(usageIdentity, product = "coordinate", now = new Date()) {
    if (!this.supabase) return { success: false, allowed: false, reason: "db_disabled", quota: {} };
    const identity = String(usageIdentity || "").trim();
    if (!identity) return { success: false, allowed: false, reason: "missing_user", quota: {} };
    const { data, error } = await this.supabase.rpc("uq01_get_usage_quota", {
      p_usage_identity: identity,
      p_free_day: getFreeDayKey(now)
    });
    if (error) throw error;
    const quota = toSharedQuotaPayload(data || {}, product);
    const paid = product === "judge" ? quota.paid_judge_count : quota.paid_convert_count;
    const allowed = quota.free_shared_remaining > 0 || paid > 0;
    return { success: allowed, allowed, reason: allowed ? "ok" : "limit_exceeded", source: quota.free_shared_remaining > 0 ? "free" : paid > 0 ? "paid" : "none", quota };
  }

  async consume({ usageIdentity, serviceOperationId, usageEventType, now = new Date() }) {
    if (!this.supabase) return { success: false, allowed: false, reason: "db_disabled", quota: {} };
    const identity = String(usageIdentity || "").trim();
    const product = PRODUCT_BY_EVENT[usageEventType];
    if (!identity) return { success: false, allowed: false, reason: "missing_user", quota: {} };
    if (!isServiceOperationId(serviceOperationId)) throw new TypeError("Server service_operation_id is required");
    if (!product) throw new TypeError("Unsupported usage_event_type");
    const { data, error } = await this.supabase.rpc("uq01_consume_usage_event", {
      p_usage_identity: identity,
      p_service_operation_id: serviceOperationId,
      p_usage_event_type: usageEventType,
      p_free_day: getFreeDayKey(now)
    });
    if (error) throw error;
    const quota = toSharedQuotaPayload(data || {}, product);
    return {
      success: data?.success === true,
      allowed: data?.success === true,
      idempotent: data?.idempotent === true,
      reason: data?.reason || (data?.success ? "ok" : "limit_exceeded"),
      source: data?.charge_source || "none",
      eventId: data?.event_id || null,
      timezone: FREE_DAY_TIMEZONE,
      quota
    };
  }
}

export function createSupabaseUsageLedger(options) {
  return new SupabaseUsageLedger(options);
}
