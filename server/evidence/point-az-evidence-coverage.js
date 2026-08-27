export const POINT_AZ_EVIDENCE_COVERAGE_CONTRACT = Object.freeze({
  contractId: "point_az_evidence_coverage",
  contractVersion: "1",
  family: "point-az-dms-table",
  expectedPointCount: 26,
  statuses: Object.freeze(["complete", "partial", "insufficient"]),
  labelPolicy: "EXPLICIT_LABELS_ONLY",
  ordinalComparisonIsLabelEvidence: false
});

function normalizeSource(source) {
  const present = source !== null && source !== undefined;
  const explicitLabels = Array.isArray(source?.explicitLabels)
    ? [...new Set(source.explicitLabels.map(value => String(value || "").toUpperCase()).filter(value => /^[A-Z]$/.test(value)))]
    : [];
  return Object.freeze({
    present,
    parseable: present && source?.parseable === true,
    rowCount: present && Number.isSafeInteger(source?.rowCount) ? source.rowCount : 0,
    explicitLabelCoverage: Object.freeze({
      count: explicitLabels.length,
      labels: Object.freeze(explicitLabels),
      complete: explicitLabels.length === POINT_AZ_EVIDENCE_COVERAGE_CONTRACT.expectedPointCount
    })
  });
}

export function evaluatePointAzEvidenceCoverage({ generalVision, finalVision, comparison = {} } = {}) {
  const expectedPointCount = POINT_AZ_EVIDENCE_COVERAGE_CONTRACT.expectedPointCount;
  const general = normalizeSource(generalVision);
  const final = normalizeSource(finalVision);
  const ordinalComparableRows = Number.isSafeInteger(comparison.ordinalComparableRows)
    ? Math.max(0, Math.min(expectedPointCount, comparison.ordinalComparableRows))
    : 0;
  const conflictingFields = Array.isArray(comparison.conflictingFields)
    ? comparison.conflictingFields.map(value => Object.freeze({ ...value }))
    : [];
  const missingRows = Math.max(0, expectedPointCount - ordinalComparableRows);
  const sourcesUsable = general.present && final.present && general.parseable && final.parseable;
  const explicitLabelsComplete = general.explicitLabelCoverage.complete && final.explicitLabelCoverage.complete;
  const complete = sourcesUsable
    && general.rowCount === expectedPointCount
    && final.rowCount === expectedPointCount
    && ordinalComparableRows === expectedPointCount
    && explicitLabelsComplete
    && conflictingFields.length === 0;
  const status = !sourcesUsable ? "insufficient" : complete ? "complete" : "partial";

  return Object.freeze({
    schemaVersion: "point_az_evidence_coverage_v1",
    contractId: POINT_AZ_EVIDENCE_COVERAGE_CONTRACT.contractId,
    contractVersion: POINT_AZ_EVIDENCE_COVERAGE_CONTRACT.contractVersion,
    family: POINT_AZ_EVIDENCE_COVERAGE_CONTRACT.family,
    expectedPointCount,
    generalVision: general,
    finalVision: final,
    comparison: Object.freeze({
      ordinalComparableRows,
      missingRows,
      conflictingFields: Object.freeze(conflictingFields),
      coverageRatio: Number((ordinalComparableRows / expectedPointCount).toFixed(6)),
      explicitLabelCoverageComplete: explicitLabelsComplete
    }),
    status,
    productionAuthority: false,
    affectsCoordinates: false,
    affectsGate: false
  });
}

