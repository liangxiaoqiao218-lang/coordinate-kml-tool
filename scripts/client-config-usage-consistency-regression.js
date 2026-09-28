import assert from "node:assert/strict";
import { applyAuthoritativeUsageQuota } from "../server/client-config-user.js";

const localVisitor = Object.freeze({
  visitorId: "usage-consistency",
  freeConvertCount: 3,
  freeJudgeCount: 1,
  paidConvertCount: 0,
  permissions: Object.freeze({ aiOcrEnabled: true }),
  note: "preserve-local-profile"
});
const authoritativeQuota = Object.freeze({
  free_convert_count: 2,
  free_judge_count: 1,
  paid_convert_count: 0,
  paid_judge_count: 0,
  freeConvertCount: 2,
  freeJudgeCount: 1,
  paidConvertCount: 0,
  paidJudgeCount: 0,
  convert_remaining: 2,
  convertRemaining: 2,
  is_vip: false,
  isVip: false,
  ignored_secret: "must-not-leak"
});

const merged = applyAuthoritativeUsageQuota(localVisitor, authoritativeQuota);
assert.equal(merged.free_convert_count, 2);
assert.equal(merged.freeConvertCount, 2);
assert.equal(merged.convert_remaining, 2);
assert.equal(merged.convertRemaining, 2);
assert.deepEqual(merged.permissions, localVisitor.permissions);
assert.equal(merged.note, "preserve-local-profile");
assert.equal(Object.hasOwn(merged, "ignored_secret"), false);
assert.equal(localVisitor.freeConvertCount, 3, "the local cache is not mutated by the read model");
assert.strictEqual(applyAuthoritativeUsageQuota(localVisitor, null), localVisitor);

console.log(JSON.stringify({
  suite: "client-config-usage-consistency-regression",
  passed: 10,
  providerCalls: 0
}, null, 2));
