import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";

import {
  ACQUISITION_STRATEGY_ID,
  ACQUISITION_STRATEGY_INPUT_MODE,
  ACQUISITION_STRATEGY_MODEL,
  ACQUISITION_STRATEGY_REASON,
  EXPERIMENTAL_STRUCTURAL_ROUTER_CONTRACT,
  EXPERIMENTAL_STRUCTURAL_ROUTER_THRESHOLDS,
  chooseAcquisitionStrategyFromMetrics,
  dryRunAcquisitionStrategy,
} from "../server/coordinate-engine-v3/index.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

function assertStrategy(decision, strategyId) {
  assert.equal(decision.strategyId, strategyId);
  assert.equal(decision.providerCalls, 0);
  if (strategyId === ACQUISITION_STRATEGY_ID.GENERAL_PRIMARY) {
    assert.equal(decision.inputMode, ACQUISITION_STRATEGY_INPUT_MODE.DEFAULT_PRIMARY);
    assert.equal(decision.model, ACQUISITION_STRATEGY_MODEL.GENERAL_PRIMARY);
  } else {
    assert.equal(decision.inputMode, ACQUISITION_STRATEGY_INPUT_MODE.TABLE_CONTEXT_COMPOSITE);
    assert.equal(decision.model, ACQUISITION_STRATEGY_MODEL.COMPLEX_STRUCTURED_DOCUMENT);
  }
}

function syntheticMetrics({
  width = 1600,
  height = 1132,
  tablePct = 15,
  tableHeightRatio = 0.38,
  compositeHeightRatio = 0.53,
  tableAspect = 1.5,
  candidateRegionCount = 2,
} = {}) {
  const tableHeight = Math.round(height * tableHeightRatio);
  const tableArea = Math.max(1, Math.round(width * height * tablePct / 100));
  const tableWidth = Math.max(1, Math.round(tableArea / Math.max(1, tableHeight)));
  return {
    originalDimensions: { width, height },
    detectedTableRegion: {
      x: Math.max(0, width - tableWidth - 20),
      y: Math.max(0, height - tableHeight - 80),
      width: Math.round(tableHeight * tableAspect),
      height: tableHeight,
    },
    tableRegionPercentage: tablePct,
    compositeDimensions: {
      width: Math.max(width, Math.round(tableHeight * tableAspect) + 36),
      height: Math.round(height * compositeHeightRatio),
    },
    detection: {
      candidateRegionCount,
      horizontalLineSignals: 2,
      verticalLineSignals: 2,
    },
  };
}

function fixtureBase64(path) {
  assert.equal(existsSync(path), true, `fixture missing: ${path}`);
  return readFileSync(path).toString("base64");
}

test("PATH A output contract", () => {
  const decision = chooseAcquisitionStrategyFromMetrics(syntheticMetrics({ tablePct: 7, tableHeightRatio: 0.18, compositeHeightRatio: 0.32 }));
  assertStrategy(decision, ACQUISITION_STRATEGY_ID.GENERAL_PRIMARY);
  assert.equal(decision.reasonCode, ACQUISITION_STRATEGY_REASON.DEFAULT_GENERAL_IMAGE);
});

test("experimental contract metadata is explicit", () => {
  assert.equal(EXPERIMENTAL_STRUCTURAL_ROUTER_CONTRACT.experimental, true);
  assert.equal(EXPERIMENTAL_STRUCTURAL_ROUTER_CONTRACT.mode, "DRY_RUN_ONLY");
  assert.equal(EXPERIMENTAL_STRUCTURAL_ROUTER_CONTRACT.productionRouting, false);
  assert.equal(EXPERIMENTAL_STRUCTURAL_ROUTER_CONTRACT.productionGeneralization, "UNPROVEN");
  assert.equal(EXPERIMENTAL_STRUCTURAL_ROUTER_CONTRACT.thresholdFragility, "HIGH");
  assert.equal(EXPERIMENTAL_STRUCTURAL_ROUTER_CONTRACT.realFixtureCount, 4);
  assert.equal(EXPERIMENTAL_STRUCTURAL_ROUTER_CONTRACT.additionalRealFixturesRequired, true);
  assert.equal(EXPERIMENTAL_STRUCTURAL_ROUTER_CONTRACT.thresholdClassification, "EXPERIMENTAL_THRESHOLDS");
});

test("experimental threshold values remain unchanged", () => {
  assert.deepEqual(EXPERIMENTAL_STRUCTURAL_ROUTER_THRESHOLDS, {
    minOriginalWidth: 1000,
    minOriginalHeight: 800,
    minTableRegionPercentage: 12,
    maxTableRegionPercentage: 40,
    minTableHeightRatio: 0.3,
    minCompositeHeightRatio: 0.45,
  });
});

test("PATH B output contract", () => {
  const decision = chooseAcquisitionStrategyFromMetrics(syntheticMetrics());
  assertStrategy(decision, ACQUISITION_STRATEGY_ID.COMPLEX_STRUCTURED_DOCUMENT);
  assert.equal(decision.reasonCode, ACQUISITION_STRATEGY_REASON.COMPLEX_EMBEDDED_TABLE);
});

test("predicate count is four", () => {
  const decision = chooseAcquisitionStrategyFromMetrics(syntheticMetrics());
  assert.deepEqual(Object.keys(decision.predicates).sort(), [
    "largeCompositeRepresentation",
    "largeDocumentImage",
    "largeVerticalTableExtent",
    "substantialEmbeddedTable",
  ]);
});

test("large image plus tiny table stays PATH A", () => {
  assertStrategy(
    chooseAcquisitionStrategyFromMetrics(syntheticMetrics({ tablePct: 4, tableHeightRatio: 0.12, compositeHeightRatio: 0.25 })),
    ACQUISITION_STRATEGY_ID.GENERAL_PRIMARY,
  );
});

test("large image plus medium table stays PATH A", () => {
  assertStrategy(
    chooseAcquisitionStrategyFromMetrics(syntheticMetrics({ tablePct: 11.5, tableHeightRatio: 0.24, compositeHeightRatio: 0.39 })),
    ACQUISITION_STRATEGY_ID.GENERAL_PRIMARY,
  );
});

test("large image plus dense tall embedded table chooses PATH B", () => {
  assertStrategy(
    chooseAcquisitionStrategyFromMetrics(syntheticMetrics({ tablePct: 18, tableHeightRatio: 0.4, compositeHeightRatio: 0.55 })),
    ACQUISITION_STRATEGY_ID.COMPLEX_STRUCTURED_DOCUMENT,
  );
});

test("small full-frame table stays PATH A", () => {
  assertStrategy(
    chooseAcquisitionStrategyFromMetrics(syntheticMetrics({ width: 964, height: 229, tablePct: 100, tableHeightRatio: 1, compositeHeightRatio: 1.4 })),
    ACQUISITION_STRATEGY_ID.GENERAL_PRIMARY,
  );
});

test("large full-frame table stays PATH A", () => {
  assertStrategy(
    chooseAcquisitionStrategyFromMetrics(syntheticMetrics({ width: 1600, height: 1132, tablePct: 95, tableHeightRatio: 0.95, compositeHeightRatio: 1 })),
    ACQUISITION_STRATEGY_ID.GENERAL_PRIMARY,
  );
});

test("no table detected stays PATH A", () => {
  const decision = chooseAcquisitionStrategyFromMetrics({
    originalDimensions: { width: 1600, height: 1132 },
    compositeDimensions: { width: 0, height: 0 },
  }, { preprocessingStatus: "PREPROCESSING_NO_STRONG_TABLE_REGION" });
  assertStrategy(decision, ACQUISITION_STRATEGY_ID.GENERAL_PRIMARY);
  assert.equal(decision.reasonCode, ACQUISITION_STRATEGY_REASON.NO_STRONG_TABLE_REGION);
});

test("table detection error stays PATH A", () => {
  const decision = chooseAcquisitionStrategyFromMetrics({}, { preprocessingStatus: "PREPROCESSING_ERROR" });
  assertStrategy(decision, ACQUISITION_STRATEGY_ID.GENERAL_PRIMARY);
  assert.equal(decision.reasonCode, ACQUISITION_STRATEGY_REASON.PREPROCESSING_ERROR);
});

test("borderline below threshold stays PATH A", () => {
  assertStrategy(
    chooseAcquisitionStrategyFromMetrics(syntheticMetrics({ tablePct: 11.9, tableHeightRatio: 0.299, compositeHeightRatio: 0.449 })),
    ACQUISITION_STRATEGY_ID.GENERAL_PRIMARY,
  );
});

test("borderline above threshold chooses PATH B", () => {
  assertStrategy(
    chooseAcquisitionStrategyFromMetrics(syntheticMetrics({ tablePct: 12.1, tableHeightRatio: 0.301, compositeHeightRatio: 0.451 })),
    ACQUISITION_STRATEGY_ID.COMPLEX_STRUCTURED_DOCUMENT,
  );
});

test("high table percentage with low composite height stays PATH A", () => {
  assertStrategy(
    chooseAcquisitionStrategyFromMetrics(syntheticMetrics({ tablePct: 35, tableHeightRatio: 0.2, compositeHeightRatio: 0.28 })),
    ACQUISITION_STRATEGY_ID.GENERAL_PRIMARY,
  );
});

test("table percentage above embedded range stays PATH A", () => {
  assertStrategy(
    chooseAcquisitionStrategyFromMetrics(syntheticMetrics({ tablePct: 41, tableHeightRatio: 0.5, compositeHeightRatio: 0.6 })),
    ACQUISITION_STRATEGY_ID.GENERAL_PRIMARY,
  );
});

test("Côte d’Ivoire real fixture dry-run chooses PATH A", async () => {
  const decision = await dryRunAcquisitionStrategy({
    imageBase64: fixtureBase64("artifacts/fixtures/cote-divoire-dms-real-001.jpeg"),
  });
  assertStrategy(decision, ACQUISITION_STRATEGY_ID.GENERAL_PRIMARY);
  assert.equal(decision.mode, "DRY_RUN_ONLY");
});

test("Indonesia #001 real fixture dry-run chooses PATH A", async () => {
  const decision = await dryRunAcquisitionStrategy({
    imageBase64: fixtureBase64("artifacts/fixtures/indonesia-utm50s-real-001.jpg"),
  });
  assertStrategy(decision, ACQUISITION_STRATEGY_ID.GENERAL_PRIMARY);
});

test("Indonesia #002 real fixture dry-run chooses PATH A", async () => {
  const decision = await dryRunAcquisitionStrategy({
    imageBase64: fixtureBase64("artifacts/fixtures/indonesia-utm50s-real-002.jpg"),
  });
  assertStrategy(decision, ACQUISITION_STRATEGY_ID.GENERAL_PRIMARY);
  assert.equal(decision.metrics.tableRegionPercentage < EXPERIMENTAL_STRUCTURAL_ROUTER_THRESHOLDS.minTableRegionPercentage, true);
});

test("Indonesia #003 real fixture dry-run chooses PATH B", async () => {
  const decision = await dryRunAcquisitionStrategy({
    imageBase64: fixtureBase64("artifacts/fixtures/indonesia-utm50s-real-003.jpg"),
  });
  assertStrategy(decision, ACQUISITION_STRATEGY_ID.COMPLEX_STRUCTURED_DOCUMENT);
  assert.equal(decision.metrics.tableHeightRatio >= EXPERIMENTAL_STRUCTURAL_ROUTER_THRESHOLDS.minTableHeightRatio, true);
});

test("router source is semantically neutral", () => {
  const source = readFileSync("server/coordinate-engine-v3/acquisition/strategy-router.js", "utf8");
  assert.equal(/Indonesia|Madagascar|Kyrgyzstan|Côte|Ivoire|\bUTM\b|\bDMS\b|\bMGRS\b|WGS84|EPSG|Latitude|Longitude|recognizerId|filename|groundTruth|expected row/i.test(source), false);
});

test("router does not import Runner or recognizers", () => {
  const source = readFileSync("server/coordinate-engine-v3/acquisition/strategy-router.js", "utf8");
  assert.equal(/runner\.js|runCoordinateEngine|recognizers\//.test(source), false);
});

test("router does not call provider or primary acquisition", () => {
  const source = readFileSync("server/coordinate-engine-v3/acquisition/strategy-router.js", "utf8");
  assert.equal(/callPrimaryVisionProvider|acquirePrimaryImage|fetch\(|https?:\/\//.test(source), false);
});

let passed = 0;
for (const item of tests) {
  try {
    await item.fn();
    passed += 1;
    console.log(`PASS ${item.name}`);
  } catch (error) {
    console.error(`FAIL ${item.name}`);
    throw error;
  }
}

console.log(`Coordinate Engine V3 Strategy Router Regression: ${passed}/${tests.length} PASS`);
