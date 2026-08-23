import assert from "node:assert/strict";

import {
  ACQUISITION_PROVENANCE,
  ACQUISITION_SOURCE_TYPE,
  ACQUISITION_STATUS,
  canHandleDmsGroupedCoordinates,
  canHandleGenericDms,
  createAcquisitionResult,
  createLatencyBudget,
  normalizeDmsGroupedCoordinates,
  parseDmsGroupedCoordinates,
  recognizeDmsGroupedCoordinates,
  runAcquisitionCandidatesThroughRunner,
  runCoordinateEngineV3,
  toDmsGroupedCoordinatesKmlCoordinate,
  verifyDmsGroupedCoordinates,
  V3_RUNNER_STATUS,
} from "../server/coordinate-engine-v3/index.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

function assertNear(actual, expected, tolerance = 1e-8) {
  assert.equal(Math.abs(Number(actual) - Number(expected)) <= tolerance, true, `${actual} not within ${tolerance} of ${expected}`);
}

const STRUCT_REAL_007_TEXT = `gold mining agreement
Mining Area 1:
Certificate Holder: Guinea Brain Toch Mining License No.: A/2026/0189/MM
The coordinates are as follows:
1. 11°52'25.72"N, 08°53'13.39"W
2. 11°52'21.27"N, 08°53'11.78"W
3. 11°52'20.00"N, 08°53'28.00"W
4. 11°52'18.00"N, 08°53'25.00"W

Mining Area Two:
Certificate Holder: New Hope Of Africa Mining License No.: A/2026/0191/MM
The coordinates are as follows:
1. 11°52'11.93"N, 08°53'32.66"W
2. 11°52'17.21"N, 08°53'33.18"W
3. 11°52'12.57"N, 08°53'54.03"W
4. 11°52'07.65"N, 08°53'53.56"W`;

const STRUCT_REAL_007_BLOCK_1 = `Mining Area 1:
1. 11°52'25.72"N, 08°53'13.39"W
2. 11°52'21.27"N, 08°53'11.78"W
3. 11°52'20.00"N, 08°53'28.00"W
4. 11°52'18.00"N, 08°53'25.00"W`;

const STRUCT_REAL_007_BLOCK_2 = `Mining Area Two:
1. 11°52'11.93"N, 08°53'32.66"W
2. 11°52'17.21"N, 08°53'33.18"W
3. 11°52'12.57"N, 08°53'54.03"W
4. 11°52'07.65"N, 08°53'53.56"W`;

async function parse(input) {
  const result = await recognizeDmsGroupedCoordinates(input, {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock: () => 0 }),
  });
  const normalized = normalizeDmsGroupedCoordinates(result);
  const verification = await verifyDmsGroupedCoordinates(normalized);
  return { result, normalized, verification };
}

async function run(input) {
  return runCoordinateEngineV3(input, {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock: () => 0 }),
  });
}

test("STRUCT_REAL_007 ownership", async () => {
  const runner = await run({ text: STRUCT_REAL_007_TEXT });
  assert.equal(runner.status, V3_RUNNER_STATUS.MATCHED);
  assert.equal(runner.recognizerId, "dms_grouped_coordinates");
});

test("STRUCT_REAL_007 parses two groups", () => {
  const parsed = parseDmsGroupedCoordinates({ text: STRUCT_REAL_007_TEXT });
  assert.equal(parsed.groups.length, 2);
  assert.deepEqual(parsed.groups.map((group) => group.length), [4, 4]);
});

test("STRUCT_REAL_007 rows 8/8", async () => {
  const { normalized, verification } = await parse({ text: STRUCT_REAL_007_TEXT });
  assert.equal(normalized.coordinates.length, 8);
  assert.equal(verification.pointCount, 8);
  assert.equal(verification.groupCount, 2);
});

test("STRUCT_REAL_007 first frozen KML", async () => {
  const { normalized } = await parse({ text: STRUCT_REAL_007_TEXT });
  const first = normalized.coordinates[0];
  assertNear(first.latitude, 11.873811111111111);
  assertNear(first.longitude, -8.887052777777777);
  assert.equal(toDmsGroupedCoordinatesKmlCoordinate(first), "-8.887052777777777,11.873811111111111,0");
});

test("STRUCT_REAL_007 last frozen KML", async () => {
  const { normalized } = await parse({ text: STRUCT_REAL_007_TEXT });
  const last = normalized.coordinates.at(-1);
  assertNear(last.latitude, 11.868791666666667);
  assertNear(last.longitude, -8.898211111111111);
  assert.equal(toDmsGroupedCoordinatesKmlCoordinate(last), "-8.898211111111111,11.868791666666667,0");
});

test("technical KML ready", async () => {
  const { normalized, verification } = await parse({ text: STRUCT_REAL_007_TEXT });
  assert.equal(normalized.geometryType, "multipolygon");
  assert.equal(normalized.technicalKmlReady, true);
  assert.equal(verification.status, "pass");
});

test("generic_dms does not take grouped ownership", () => {
  assert.equal(canHandleGenericDms({ text: STRUCT_REAL_007_TEXT }), false);
  assert.equal(canHandleDmsGroupedCoordinates({ text: STRUCT_REAL_007_TEXT }), true);
});

test("plain DMS remains generic scope", async () => {
  const runner = await run({ text: "11°27'45\"N 08°36'30\"W" });
  assert.equal(runner.status, V3_RUNNER_STATUS.MATCHED);
  assert.equal(runner.recognizerId, "generic_dms");
  assert.equal(canHandleDmsGroupedCoordinates({ text: "11°27'45\"N 08°36'30\"W" }), false);
});

test("single Mining Area block is not grouped ownership", () => {
  assert.equal(canHandleDmsGroupedCoordinates({ text: STRUCT_REAL_007_BLOCK_1 }), false);
  assert.equal(canHandleDmsGroupedCoordinates({ text: STRUCT_REAL_007_BLOCK_2 }), false);
});

test("adapter produces one logical grouped result from existing acquisition shape", async () => {
  const acquisition = createAcquisitionResult({
    status: ACQUISITION_STATUS.SUCCESS,
    providerCalls: 1,
    timing: { totalDurationMs: 33919, primaryDurationMs: 33919, targetedDurationMs: 0 },
    candidates: [
      {
        id: "primary_whole_image",
        text: STRUCT_REAL_007_TEXT,
        structuredRows: [],
        headers: [],
        documentCues: ["gold mining agreement", "Mining Area 1", "Mining Area Two"],
        sourceType: ACQUISITION_SOURCE_TYPE.WHOLE_IMAGE,
        provenance: ACQUISITION_PROVENANCE.PRIMARY,
      },
      {
        id: "primary_block_1",
        text: STRUCT_REAL_007_BLOCK_1,
        structuredRows: STRUCT_REAL_007_BLOCK_1.split(/\n/).slice(1).map((line) => ({ text: line })),
        headers: [],
        documentCues: ["Mining Area 1"],
        sourceType: ACQUISITION_SOURCE_TYPE.COORDINATE_BLOCK,
        provenance: ACQUISITION_PROVENANCE.PRIMARY,
      },
      {
        id: "primary_block_2",
        text: STRUCT_REAL_007_BLOCK_2,
        structuredRows: STRUCT_REAL_007_BLOCK_2.split(/\n/).slice(1).map((line) => ({ text: line })),
        headers: [],
        documentCues: ["Mining Area Two"],
        sourceType: ACQUISITION_SOURCE_TYPE.COORDINATE_BLOCK,
        provenance: ACQUISITION_PROVENANCE.PRIMARY,
      },
    ],
  });
  const adapted = await runAcquisitionCandidatesThroughRunner(acquisition, {
    latencyBudget: createLatencyBudget({ startedAtMs: 0, clock: () => 0 }),
  });
  assert.equal(adapted.status, "MATCHED_RESULT");
  assert.equal(adapted.recognizerId, "dms_grouped_coordinates");
  assert.equal(adapted.normalized.coordinates.length, 8);
  assert.equal(adapted.technicalKmlReady, undefined);
  assert.equal(adapted.normalized.technicalKmlReady, true);
});

test("provider calls stay zero inside recognizer", async () => {
  const runner = await run({ text: STRUCT_REAL_007_TEXT });
  assert.equal(runner.providerCalls, 0);
  assert.equal(runner.visionCalls, 0);
  assert.equal(runner.ocrCalls, 0);
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

console.log(`Coordinate Engine V3 DMS Grouped Coordinates Regression: ${passed}/${tests.length} PASS`);
