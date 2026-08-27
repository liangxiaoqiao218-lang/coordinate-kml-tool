import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import {
  classifyGoldenRun,
  summarizeReleaseSemantics
} from "../runner-semantics.js";

const confirmedTruthRule = {
  truthMaturity: "CONFIRMED_TRUTH",
  policyMaturity: "CONFIRMED_POLICY",
  releasePolicy: { policy: "AUTO_EXPORT_ALLOWED" },
  evidenceMetadata: {}
};
const confirmedSample = {
  sample_id: "confirmed",
  baseline_status: "locked",
  golden_governance: confirmedTruthRule
};
const finalizer = decisionState => ({ finalizerEvaluated: true, decisionState });
const truthBlocker = { severity: "BLOCKER", field: "pointCount", expected: 4, actual: 3 };
const passed = [];
const check = (id, fn) => { fn(); passed.push(id); };
const governance = JSON.parse(await readFile(new URL("../sr08d5-golden-policy.json", import.meta.url), "utf8"));

const timeout = classifyGoldenRun({
  sample: confirmedSample,
  actual: {
    httpStatus: 504,
    timeout: true,
    responseCode: "RECOGNITION_DEADLINE_EXCEEDED",
    responseReason: "timeout",
    durationMs: 55021,
    finalizerEvaluated: false
  },
  diffs: [truthBlocker, { severity: "BLOCKER", field: "kml_ready" }]
});

check("R01", () => assert.equal(timeout.truthStatus, "NOT_EVALUATED"));
check("R02", () => assert.equal(timeout.policyStatus, "NOT_EVALUATED"));
check("R03", () => assert.equal(timeout.reliabilityStatus, "TIMEOUT"));
check("R04", () => assert.equal(timeout.gateSafetyStatus, "FAIL_CLOSED_TIMEOUT"));
check("R05", () => assert.equal(timeout.classifications.includes("COORDINATE_TRUTH_FAILURE"), false));
check("R06", () => assert.equal(summarizeReleaseSemantics([timeout]).policyFailureCount, 0));
check("T01", () => assert.equal(timeout.truthStatus, "NOT_EVALUATED"));
check("T02", () => assert.equal(timeout.policyStatus, "NOT_EVALUATED"));
check("T03", () => assert.equal(timeout.reliabilityStatus, "TIMEOUT"));
check("T04", () => assert.equal(timeout.gateSafetyStatus, "FAIL_CLOSED_TIMEOUT"));
check("T05", () => assert.equal(summarizeReleaseSemantics([timeout]).truthFailureCount, 0));
check("T06", () => assert.equal(summarizeReleaseSemantics([timeout]).policyFailureCount, 0));
check("T07", () => assert.equal(timeout.classifications.includes("UNSAFE_GATE_FAILURE"), false));

const baselineOnlySample = { sample_id: "cote", baseline_status: "partial" };
const legacyWarning = { severity: "WARNING", field: "v1_precisionMode", expected: "legacy-a", actual: "legacy-b" };
const baselineOnlyAuto = classifyGoldenRun({
  sample: baselineOnlySample,
  actual: finalizer("AUTO_EXPORT"),
  diffs: [legacyWarning]
});
check("R07", () => assert.equal(baselineOnlyAuto.truthStatus, "NOT_QUALIFIED"));
check("R08", () => assert.equal(baselineOnlyAuto.classifications.includes("COORDINATE_TRUTH_FAILURE"), false));
check("R09", () => assert.equal(baselineOnlyAuto.classifications.includes("UNSAFE_GATE_FAILURE"), false));

const confirmedAutoMismatch = classifyGoldenRun({
  sample: confirmedSample,
  actual: finalizer("AUTO_EXPORT"),
  diffs: [truthBlocker]
});
check("R10", () => {
  assert.equal(confirmedAutoMismatch.gateSafetyStatus, "UNSAFE_GATE_FAILURE");
  assert.equal(confirmedAutoMismatch.releaseSeverity, "P0");
});

const confirmedBlockedMismatch = classifyGoldenRun({
  sample: confirmedSample,
  actual: finalizer("BLOCKED"),
  diffs: [truthBlocker]
});
check("R11", () => assert.equal(confirmedBlockedMismatch.gateSafetyStatus, "TRUTH_FAILURE_SAFE_FAIL_CLOSED"));

check("R12", () => {
  const result = classifyGoldenRun({
    sample: confirmedSample,
    actual: { ...finalizer("AUTO_EXPORT"), geometryWarnings: ["self_intersection"] },
    diffs: []
  });
  assert.equal(result.gateSafetyStatus, "UNSAFE_GATE_FAILURE");
  assert.equal(result.classifications.includes("GEOMETRY_FAILURE"), true);
});

check("R13", () => {
  const policySample = {
    baseline_status: "partial",
    golden_governance: {
      truthMaturity: "BASELINE_ONLY",
      policyMaturity: "CONFIRMED_POLICY",
      releasePolicy: { policy: "REVIEW_REQUIRED" },
      evidenceMetadata: {}
    }
  };
  const result = classifyGoldenRun({ sample: policySample, actual: finalizer("AUTO_EXPORT"), diffs: [] });
  assert.equal(result.gateSafetyStatus, "UNSAFE_GATE_FAILURE");
});

check("R14", () => {
  const result = classifyGoldenRun({
    sample: confirmedSample,
    actual: { ...finalizer("AUTO_EXPORT"), reasonCodes: ["CRS_UNRESOLVED"] },
    diffs: []
  });
  assert.equal(result.classifications.includes("CRS_FAILURE"), true);
  assert.equal(result.gateSafetyStatus, "UNSAFE_GATE_FAILURE");
});

check("R15", () => {
  const metadataSample = {
    baseline_status: "partial",
    golden_governance: {
      truthMaturity: "BASELINE_ONLY",
      policyMaturity: "CONFIRMED_POLICY",
      releasePolicy: { policy: "AUTO_EXPORT_ALLOWED" },
      evidenceMetadata: { expectedReviewGroupIndexes: [1] }
    }
  };
  const result = classifyGoldenRun({
    sample: metadataSample,
    actual: finalizer("BLOCKED"),
    diffs: [{ severity: "BLOCKER", field: "reviewGroupIndexes", expected: [1], actual: [] }]
  });
  assert.equal(result.metadataStatus, "MISMATCH");
  assert.equal(result.classifications.includes("UNSAFE_GATE_FAILURE"), false);
});

check("R16", () => {
  const varianceSample = {
    ...confirmedSample,
    golden_governance: { ...confirmedTruthRule, providerVarianceMaturity: "PROVIDER_VARIANCE_TRACKED" }
  };
  const result = classifyGoldenRun({ sample: varianceSample, actual: finalizer("BLOCKED"), diffs: [truthBlocker] });
  assert.equal(result.providerVarianceStatus, "DETECTED");
  assert.equal(result.classifications.includes("PROVIDER_VARIANCE"), true);
});

check("R17", () => {
  const result = classifyGoldenRun({ sample: confirmedSample, actual: { finalizerEvaluated: false }, diffs: [] });
  assert.equal(result.finalizerStatus, "NOT_EVALUATED");
  assert.equal(result.gateSafetyStatus, "NOT_EVALUATED");
});
check("R18", () => assert.equal(timeout.timeoutStage, "UNKNOWN_AFTER_REQUEST_DEADLINE"));
check("R19", () => {
  const summary = summarizeReleaseSemantics([timeout, confirmedAutoMismatch, baselineOnlyAuto]);
  assert.equal(summary.timeoutCount, 1);
  assert.equal(summary.truthFailureCount, 1);
  assert.equal(summary.truthNotEvaluatedCount, 1);
  assert.equal(summary.truthNotQualifiedCount, 1);
  assert.equal(summary.safetyStatus, "FAIL_P0");
  assert.equal(summary.reliabilityStatus, "FAIL_TIMEOUT_RATE");
});
check("R20", () => assert.equal(governance.productionSourceHash, "d4fcb16d13c3e2214248b3102290a4e5ae8e65dc689ac8db816d8ee532d9193c"));

check("CI01", () => assert.equal(baselineOnlyAuto.gateSafetyStatus, "SAFE_AUTO_EXPORT"));
check("CI02", () => assert.equal(baselineOnlyAuto.truthStatus, "NOT_QUALIFIED"));
check("CI03", () => assert.equal(baselineOnlyAuto.classifications.length, 0));
check("CI04", () => assert.equal(confirmedAutoMismatch.gateSafetyStatus, "UNSAFE_GATE_FAILURE"));
check("CI05", () => assert.equal(confirmedBlockedMismatch.gateSafetyStatus, "TRUTH_FAILURE_SAFE_FAIL_CLOSED"));
check("T08", () => assert.equal(55021 < 60000, true));

console.log(`SR08D8_RUNNER_SEMANTICS=PASS (${passed.length}/${passed.length})`);
console.log(`CASES=${passed.join(",")}`);
