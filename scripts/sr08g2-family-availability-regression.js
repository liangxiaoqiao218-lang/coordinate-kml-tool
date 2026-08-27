import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import {
  COORDINATE_CONFIRMATION_STATUS,
  COORDINATE_DECISION_STATE,
  COORDINATE_GATE_REASON,
  COORDINATE_QUALITY_GATE_STATUS,
  CoordinateConfirmationRuntime,
  FAMILY_AVAILABILITY_POLICY_ID,
  FAMILY_AVAILABILITY_POLICY_VERSION,
  FAMILY_AVAILABILITY_STATUS,
  FINALIZED_COORDINATE_CRS,
  consumeFinalizedGeometry,
  evaluateFamilyAvailability,
  finalizeCoordinateResult,
  getFamilyAvailability
} from "../server/coordinate-finalizer/index.js";

const governance = JSON.parse(await readFile(
  new URL("../release-governance/family-availability-policy-v1.json", import.meta.url),
  "utf8"
));
const serverSource = await readFile(new URL("../server.js", import.meta.url), "utf8");

const results = [];
let unsafeGateFailureCount = 0;

function check(id, name, fn) {
  try {
    fn();
    results.push({ id, name, status: "PASS" });
  } catch (error) {
    results.push({ id, name, status: "FAIL", error: error.message || String(error) });
  }
}

function policyEntry(family) {
  return governance.entries.find(entry => entry.family === family);
}

function simulateServerEnforcement(family) {
  let providerCallCount = 0;
  let unrelatedFallbackCount = 0;
  const availability = evaluateFamilyAvailability({ family, authoritativeEvidence: true });
  if (availability.providerCallAllowed) providerCallCount += 1;
  if (!availability.enforced) unrelatedFallbackCount += 1;
  return { availability, providerCallCount, unrelatedFallbackCount };
}

const geometry = Object.freeze({
  type: "Polygon",
  coordinates: Object.freeze([[[-1, 1], [-1, 2], [0, 2], [-1, 1]]])
});

function finalizedForAvailability(family, overrides = {}) {
  const availability = getFamilyAvailability(family);
  const result = finalizeCoordinateResult({
    resultRevision: 1,
    currentRevision: 1,
    sourceAuthority: "legacy",
    coordinateType: family,
    precisionMode: "deterministic-regression",
    family,
    availabilityStatus: availability.status,
    availabilityReasonCode: availability.reasonCode,
    familyAvailabilityPolicy: availability,
    crs: FINALIZED_COORDINATE_CRS,
    geometry,
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.NOT_REQUIRED,
    qualityGateStatus: COORDINATE_QUALITY_GATE_STATUS.PASSED,
    requiresReview: false,
    kmlReady: true,
    ...overrides
  });
  if (result.decisionState === COORDINATE_DECISION_STATE.AUTO_EXPORT
    && availability.status !== FAMILY_AVAILABILITY_STATUS.AVAILABLE) {
    unsafeGateFailureCount += 1;
  }
  return result;
}

check("A01", "Kyrgyz is BLOCKED_BY_PROVIDER", () => {
  const value = getFamilyAvailability("kyrgyz_gk");
  assert.equal(value.status, FAMILY_AVAILABILITY_STATUS.BLOCKED_BY_PROVIDER);
  assert.equal(value.reasonCode, COORDINATE_GATE_REASON.FAMILY_BLOCKED_BY_PROVIDER);
  assert.equal(policyEntry("kyrgyz_gk").status, value.status);
});

check("A02", "Kyrgyz provider calls are prevented", () => {
  assert.equal(simulateServerEnforcement("kyrgyz_gk").providerCallCount, 0);
  const enforcementIndex = serverSource.indexOf("const enforcedAvailability =");
  const firstProviderCallAfterEnforcement = serverSource.indexOf("callAliyunVision({", enforcementIndex);
  assert.ok(enforcementIndex > 0 && firstProviderCallAfterEnforcement > enforcementIndex);
  assert.ok(serverSource.slice(enforcementIndex, firstProviderCallAfterEnforcement).includes("return res.status(503)"));
  assert.ok(!serverSource.slice(enforcementIndex, firstProviderCallAfterEnforcement).includes("regressionSampleId"));
});

check("A03", "Kyrgyz KML is denied", () => {
  const result = finalizedForAvailability("kyrgyz_gk");
  assert.equal(result.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
  assert.equal(consumeFinalizedGeometry(result, () => "kml").consumed, false);
});

check("A04", "Madagascar is BLOCKED_BY_PROVIDER", () => {
  const value = getFamilyAvailability("madagascar_cadastral_grid");
  assert.equal(value.status, FAMILY_AVAILABILITY_STATUS.BLOCKED_BY_PROVIDER);
  assert.equal(value.reasonCode, COORDINATE_GATE_REASON.FAMILY_BLOCKED_BY_PROVIDER);
  assert.equal(policyEntry("madagascar_cadastral_grid").status, value.status);
});

check("A05", "Madagascar provider calls are prevented", () => {
  assert.equal(simulateServerEnforcement("madagascar_cadastral_grid").providerCallCount, 0);
});

check("A06", "Madagascar KML is denied", () => {
  const result = finalizedForAvailability("madagascar_cadastral_grid");
  assert.equal(result.kmlReady, false);
  assert.equal(consumeFinalizedGeometry(result, () => "kml").consumed, false);
});

check("A07", "Handwritten is TEMPORARILY_UNAVAILABLE", () => {
  const value = getFamilyAvailability("handwritten_dms_experimental");
  assert.equal(value.status, FAMILY_AVAILABILITY_STATUS.TEMPORARILY_UNAVAILABLE);
  assert.equal(value.reasonCode, COORDINATE_GATE_REASON.FAMILY_TEMPORARILY_UNAVAILABLE);
  assert.equal(policyEntry("handwritten_dms_experimental").status, value.status);
});

check("A08", "Handwritten provider calls are prevented", () => {
  assert.equal(simulateServerEnforcement("handwritten_dms_experimental").providerCallCount, 0);
});

check("A09", "Handwritten unavailable is not a fake review result", () => {
  const result = finalizedForAvailability("handwritten_dms_experimental", { requiresReview: false, kmlReady: false });
  assert.equal(result.requiresReview, false);
  assert.equal(result.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
  assert.notEqual(result.decisionState, COORDINATE_DECISION_STATE.REVIEW_REQUIRED);
});

check("A10", "Confirmation cannot override availability", () => {
  const runtime = new CoordinateConfirmationRuntime();
  const initial = finalizedForAvailability("kyrgyz_gk", {
    confirmationStatus: COORDINATE_CONFIRMATION_STATUS.PENDING,
    requiresReview: true
  });
  runtime.register(initial);
  const confirmed = runtime.confirm({
    resultId: initial.resultId,
    resultRevision: initial.resultRevision,
    geometryHash: initial.geometryHash,
    action: "accept"
  });
  assert.equal(confirmed.ok, true);
  assert.equal(confirmed.finalizedCoordinateResult.confirmationStatus, COORDINATE_CONFIRMATION_STATUS.ACCEPTED);
  assert.equal(confirmed.finalizedCoordinateResult.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
  assert.ok(confirmed.finalizedCoordinateResult.reasonCodes.includes(COORDINATE_GATE_REASON.FAMILY_BLOCKED_BY_PROVIDER));
});

check("A11", "Finalizer cannot override availability", () => {
  const result = finalizedForAvailability("madagascar_cadastral_grid");
  assert.equal(result.decisionState, COORDINATE_DECISION_STATE.BLOCKED);
  assert.ok(result.reasonCodes.includes(COORDINATE_GATE_REASON.FAMILY_BLOCKED_BY_PROVIDER));
});

check("A12", "Unavailable families do not enter unrelated fallback", () => {
  for (const family of ["kyrgyz_gk", "madagascar_cadastral_grid", "handwritten_dms_experimental"]) {
    assert.equal(simulateServerEnforcement(family).unrelatedFallbackCount, 0);
  }
});

for (const [id, family, label] of [
  ["A13", "wgs84_table", "WGS84"],
  ["A14", "mgrs", "MGRS"],
  ["A15", "utm30", "UTM30"],
  ["A16", "point_az_dms_table", "Point A-Z"],
  ["A17", "standard_dms", "standard DMS"],
  ["A18", "manual_input", "manual input"]
]) {
  check(id, `${label} remains available`, () => {
    const simulation = simulateServerEnforcement(family);
    assert.equal(simulation.availability.status, FAMILY_AVAILABILITY_STATUS.AVAILABLE);
    assert.equal(simulation.providerCallCount, 1);
  });
}

check("A19", "Existing finalized geometry consumer remains unchanged for available families", () => {
  const result = finalizedForAvailability("wgs84_table");
  assert.equal(result.decisionState, COORDINATE_DECISION_STATE.AUTO_EXPORT);
  assert.deepEqual(consumeFinalizedGeometry(result, () => ({ kml: true })), {
    consumed: true,
    value: { kml: true }
  });
});

check("A20", "Unsafe gate failure count remains zero", () => {
  assert.equal(unsafeGateFailureCount, 0);
  assert.equal(FAMILY_AVAILABILITY_POLICY_ID, governance.policyId);
  assert.equal(FAMILY_AVAILABILITY_POLICY_VERSION, governance.policyVersion);
});

for (const result of results) {
  console.log(`${result.id} ${result.status} ${result.name}${result.error ? `: ${result.error}` : ""}`);
}

const pass = results.filter(result => result.status === "PASS").length;
const fail = results.length - pass;
console.log(`TOTAL=${results.length}`);
console.log(`PASS=${pass}`);
console.log(`FAIL=${fail}`);
console.log(`PROVIDER_CALLS=0`);
console.log(`UNSAFE_GATE_FAILURE_COUNT=${unsafeGateFailureCount}`);

if (fail > 0) process.exitCode = 1;
