import { createTableContextComposite, TABLE_CONTEXT_COMPOSITE_STATUS } from "./table-context-composite.js";

export const ACQUISITION_STRATEGY_ID = Object.freeze({
  GENERAL_PRIMARY: "general_primary",
  COMPLEX_STRUCTURED_DOCUMENT: "complex_structured_document",
});

export const ACQUISITION_STRATEGY_REASON = Object.freeze({
  DEFAULT_GENERAL_IMAGE: "DEFAULT_GENERAL_IMAGE",
  COMPLEX_EMBEDDED_TABLE: "COMPLEX_EMBEDDED_TABLE",
  NO_STRONG_TABLE_REGION: "NO_STRONG_TABLE_REGION",
  PREPROCESSING_ERROR: "PREPROCESSING_ERROR",
});

export const ACQUISITION_STRATEGY_INPUT_MODE = Object.freeze({
  DEFAULT_PRIMARY: "default_primary",
  TABLE_CONTEXT_COMPOSITE: "table_context_composite",
});

export const ACQUISITION_STRATEGY_MODEL = Object.freeze({
  GENERAL_PRIMARY: "qwen-vl-plus",
  COMPLEX_STRUCTURED_DOCUMENT: "qwen-vl-ocr-latest",
});

export const EXPERIMENTAL_STRUCTURAL_ROUTER_CONTRACT = Object.freeze({
  experimental: true,
  mode: "DRY_RUN_ONLY",
  productionRouting: false,
  productionGeneralization: "UNPROVEN",
  thresholdFragility: "HIGH",
  realFixtureCount: 4,
  additionalRealFixturesRequired: true,
  thresholdClassification: "EXPERIMENTAL_THRESHOLDS",
});

export const EXPERIMENTAL_STRUCTURAL_ROUTER_THRESHOLDS = Object.freeze({
  minOriginalWidth: 1000,
  minOriginalHeight: 800,
  minTableRegionPercentage: 12,
  maxTableRegionPercentage: 40,
  minTableHeightRatio: 0.3,
  minCompositeHeightRatio: 0.45,
});

function toFiniteNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function roundMetric(value, digits = 4) {
  const number = Number(value);
  if (!Number.isFinite(number)) return 0;
  return Number(number.toFixed(digits));
}

function sanitizeDimensions(value = {}) {
  return Object.freeze({
    width: Math.max(0, Math.round(toFiniteNumber(value.width))),
    height: Math.max(0, Math.round(toFiniteNumber(value.height))),
  });
}

function sanitizeRegion(value = {}) {
  return Object.freeze({
    x: Math.max(0, Math.round(toFiniteNumber(value.x))),
    y: Math.max(0, Math.round(toFiniteNumber(value.y))),
    width: Math.max(0, Math.round(toFiniteNumber(value.width))),
    height: Math.max(0, Math.round(toFiniteNumber(value.height))),
  });
}

function createGeneralStrategy({ reasonCode, metrics, predicates }) {
  return Object.freeze({
    strategyId: ACQUISITION_STRATEGY_ID.GENERAL_PRIMARY,
    inputMode: ACQUISITION_STRATEGY_INPUT_MODE.DEFAULT_PRIMARY,
    model: ACQUISITION_STRATEGY_MODEL.GENERAL_PRIMARY,
    reasonCode,
    providerCalls: 0,
    predicates: Object.freeze(predicates),
    metrics: Object.freeze(metrics),
  });
}

function createComplexStrategy({ metrics, predicates }) {
  return Object.freeze({
    strategyId: ACQUISITION_STRATEGY_ID.COMPLEX_STRUCTURED_DOCUMENT,
    inputMode: ACQUISITION_STRATEGY_INPUT_MODE.TABLE_CONTEXT_COMPOSITE,
    model: ACQUISITION_STRATEGY_MODEL.COMPLEX_STRUCTURED_DOCUMENT,
    reasonCode: ACQUISITION_STRATEGY_REASON.COMPLEX_EMBEDDED_TABLE,
    providerCalls: 0,
    predicates: Object.freeze(predicates),
    metrics: Object.freeze(metrics),
  });
}

export function normalizeStructuralMetrics(input = {}) {
  const originalDimensions = sanitizeDimensions(input.originalDimensions);
  const tableRegion = input.detectedTableRegion ? sanitizeRegion(input.detectedTableRegion) : null;
  const compositeDimensions = sanitizeDimensions(input.compositeDimensions);
  const imageArea = originalDimensions.width * originalDimensions.height;
  const tableArea = tableRegion ? tableRegion.width * tableRegion.height : 0;
  const tableRegionPercentage = Number.isFinite(Number(input.tableRegionPercentage))
    ? toFiniteNumber(input.tableRegionPercentage)
    : (imageArea > 0 ? (tableArea / imageArea) * 100 : 0);
  const tableHeightRatio = originalDimensions.height > 0 && tableRegion
    ? tableRegion.height / originalDimensions.height
    : 0;
  const compositeHeightRatio = originalDimensions.height > 0
    ? compositeDimensions.height / originalDimensions.height
    : 0;
  const backgroundToTableRatio = tableArea > 0
    ? (imageArea - tableArea) / tableArea
    : 0;
  const detection = input.detection || {};
  return Object.freeze({
    tableDetected: tableRegion !== null && tableRegion.width > 0 && tableRegion.height > 0,
    originalDimensions,
    detectedTableRegion: tableRegion,
    tableRegionPercentage: roundMetric(tableRegionPercentage, 4),
    tableWidth: tableRegion?.width || 0,
    tableHeight: tableRegion?.height || 0,
    tableHeightRatio: roundMetric(tableHeightRatio, 4),
    compositeDimensions,
    compositeHeight: compositeDimensions.height,
    compositeHeightRatio: roundMetric(compositeHeightRatio, 4),
    backgroundToTableRatio: roundMetric(backgroundToTableRatio, 4),
    candidateRegionCount: Math.max(0, Math.round(toFiniteNumber(detection.candidateRegionCount))),
    detectorHorizontalSignals: Math.max(0, Math.round(toFiniteNumber(detection.horizontalLineSignals))),
    detectorVerticalSignals: Math.max(0, Math.round(toFiniteNumber(detection.verticalLineSignals))),
  });
}

export function chooseAcquisitionStrategyFromMetrics(input = {}, {
  thresholds = EXPERIMENTAL_STRUCTURAL_ROUTER_THRESHOLDS,
  preprocessingStatus = TABLE_CONTEXT_COMPOSITE_STATUS.CREATED,
} = {}) {
  const metrics = normalizeStructuralMetrics(input);
  if (preprocessingStatus === TABLE_CONTEXT_COMPOSITE_STATUS.ERROR) {
    return createGeneralStrategy({
      reasonCode: ACQUISITION_STRATEGY_REASON.PREPROCESSING_ERROR,
      metrics,
      predicates: {
        largeDocumentImage: false,
        substantialEmbeddedTable: false,
        largeVerticalTableExtent: false,
        largeCompositeRepresentation: false,
      },
    });
  }
  if (!metrics.tableDetected || preprocessingStatus !== TABLE_CONTEXT_COMPOSITE_STATUS.CREATED) {
    return createGeneralStrategy({
      reasonCode: ACQUISITION_STRATEGY_REASON.NO_STRONG_TABLE_REGION,
      metrics,
      predicates: {
        largeDocumentImage: false,
        substantialEmbeddedTable: false,
        largeVerticalTableExtent: false,
        largeCompositeRepresentation: false,
      },
    });
  }
  const predicates = {
    largeDocumentImage: metrics.originalDimensions.width >= thresholds.minOriginalWidth
      && metrics.originalDimensions.height >= thresholds.minOriginalHeight,
    substantialEmbeddedTable: metrics.tableRegionPercentage >= thresholds.minTableRegionPercentage
      && metrics.tableRegionPercentage <= thresholds.maxTableRegionPercentage,
    largeVerticalTableExtent: metrics.tableHeightRatio >= thresholds.minTableHeightRatio,
    largeCompositeRepresentation: metrics.compositeHeightRatio >= thresholds.minCompositeHeightRatio,
  };
  const complex = predicates.largeDocumentImage
    && predicates.substantialEmbeddedTable
    && predicates.largeVerticalTableExtent
    && predicates.largeCompositeRepresentation;
  if (!complex) {
    return createGeneralStrategy({
      reasonCode: ACQUISITION_STRATEGY_REASON.DEFAULT_GENERAL_IMAGE,
      metrics,
      predicates,
    });
  }
  return createComplexStrategy({ metrics, predicates });
}

export async function dryRunAcquisitionStrategy({
  imageBase64,
  mimeType = "image/jpeg",
  clock = Date.now,
} = {}) {
  const preprocessing = await createTableContextComposite({ imageBase64, mimeType, clock });
  const decision = chooseAcquisitionStrategyFromMetrics(preprocessing, {
    preprocessingStatus: preprocessing.status,
  });
  return Object.freeze({
    ...decision,
    mode: "DRY_RUN_ONLY",
    preprocessingStatus: preprocessing.status,
    preprocessingMode: preprocessing.preprocessingMode,
    preprocessingMs: Math.max(0, toFiniteNumber(preprocessing.preprocessingMs)),
    providerCalls: 0,
  });
}
