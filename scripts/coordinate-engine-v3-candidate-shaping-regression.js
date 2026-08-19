import assert from "node:assert/strict";

import {
  ACQUISITION_ADAPTER_STATUS,
  acquirePrimaryImage,
  canHandleGenericDms,
  canHandleIndonesiaUtm,
  runAcquisitionCandidatesThroughRunner,
} from "../server/coordinate-engine-v3/index.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

function providerWithOutput(output) {
  return async () => Object.freeze({
    ok: true,
    provider: "mock",
    model: "mock-vl",
    text: JSON.stringify(output),
    status: 200,
    responseReceived: true,
  });
}

async function acquire(output) {
  return acquirePrimaryImage({
    imageBase64: "stub",
    provider: providerWithOutput(output),
  });
}

function tableCandidate(acquisition) {
  return acquisition.candidates.find((candidate) => candidate.sourceType === "table");
}

function structuredCandidate(acquisition, sourceType) {
  return acquisition.candidates.find((candidate) => candidate.sourceType === sourceType);
}

function indonesiaOutput({ rows, type = "table" } = {}) {
  return {
  rawText: "SISTEM KOORDINAT UTM WGS 1984 ZONA 50S",
  blocks: [
    {
      type,
      text: "",
      headers: ["No.", "X", "Y", "Latitude", "Longitude"],
      rows,
      confidence: 0.94,
    },
  ],
  documentCues: ["SISTEM KOORDINAT", "UTM WGS 1984 ZONA 50S"],
};
}

const indonesia001Rows = [
  { label: "1", cells: ["779271,176", "9720912,526", "2°31'21.134\" S", "119°30'40.863\" E"] },
  { label: "2", cells: ["779554,165", "9720912,526", "2°31'21.116\" S", "119°30'50.018\" E"] },
  { label: "3", cells: ["779554,165", "9720734,464", "2°31'26.910\" S", "119°30'50.029\" E"] },
  { label: "4", cells: ["779271,176", "9720734,464", "2°31'26.928\" S", "119°30'40.874\" E"] },
];

const indonesia002Rows = [
  { label: "1", cells: ["778984,492", "9721476,737", "2°31'2.805\" S", "119°30'25.820\" E"] },
  { label: "2", cells: ["779099,680", "9721476,848", "2°31'2.776\" S", "119°30'31.465\" E"] },
  { label: "3", cells: ["779099,680", "9721110,798", "2°31'14.824\" S", "119°30'31.454\" E"] },
  { label: "4", cells: ["778875,519", "9721110,798", "2°31'14.823\" S", "119°30'27.404\" E"] },
  { label: "5", cells: ["778875,519", "9721180,576", "2°31'12.394\" S", "119°30'27.392\" E"] },
  { label: "6", cells: ["778984,492", "9721180,576", "2°31'12.373\" S", "119°30'31.513\" E"] },
];

const indonesia002Output = indonesiaOutput({ rows: indonesia002Rows });
const indonesia001CoordinateBlockOutput = indonesiaOutput({ rows: indonesia001Rows, type: "coordinate_block" });
const indonesia002CoordinateBlockOutput = indonesiaOutput({ rows: indonesia002Rows, type: "coordinate_block" });

test("mock Indonesia #002 table candidate preserves adjacent context", async () => {
  const table = tableCandidate(await acquire(indonesia002Output));
  assert.equal(table.text.includes("SISTEM KOORDINAT"), true);
  assert.equal(table.text.includes("UTM WGS 1984 ZONA 50S"), true);
});

test("mock Indonesia #002 table candidate preserves headers", async () => {
  const table = tableCandidate(await acquire(indonesia002Output));
  assert.deepEqual(table.headers, ["No.", "X", "Y", "Latitude", "Longitude"]);
});

test("mock Indonesia #002 table candidate preserves all columns", async () => {
  const table = tableCandidate(await acquire(indonesia002Output));
  assert.equal(table.structuredRows[0]["No."], "1");
  assert.equal(table.structuredRows[0].X, "778984,492");
  assert.equal(table.structuredRows[0].Y, "9721476,737");
  assert.equal(table.structuredRows[0].Latitude, "2°31'2.805\" S");
  assert.equal(table.structuredRows[0].Longitude, "119°30'25.820\" E");
});

test("mock Indonesia #002 table candidate preserves row order", async () => {
  const table = tableCandidate(await acquire(indonesia002Output));
  assert.deepEqual(table.structuredRows.map((row) => row["No."]), ["1", "2", "3", "4", "5", "6"]);
});

test("mock Indonesia #002 table candidate preserves decimal comma precision", async () => {
  const table = tableCandidate(await acquire(indonesia002Output));
  assert.equal(table.structuredRows[0].X, "778984,492");
  assert.equal(table.structuredRows[0].Y, "9721476,737");
});

test("mock Indonesia #002 table candidate is owned by indonesia_utm", async () => {
  const acquisition = await acquire(indonesia002Output);
  const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
  assert.equal(adapter.status, ACQUISITION_ADAPTER_STATUS.MATCHED_RESULT);
  assert.equal(adapter.recognizerId, "indonesia_utm");
  assert.equal(adapter.normalized.coordinates.length, 6);
});

test("mock Indonesia #002 table candidate does not match generic_dms", async () => {
  const table = tableCandidate(await acquire(indonesia002Output));
  assert.equal(canHandleGenericDms(table), false);
  assert.equal(canHandleIndonesiaUtm(table), true);
});

test("mock Indonesia #002 has zero standard ambiguity", async () => {
  const acquisition = await acquire(indonesia002Output);
  const tableResult = (await runAcquisitionCandidatesThroughRunner(acquisition)).candidateResults
    .find((result) => result.candidateId !== "primary_whole_image");
  assert.deepEqual(tableResult.candidates.map((candidate) => candidate.recognizerId), ["indonesia_utm"]);
});

test("structured coordinate_block preserves adjacent context", async () => {
  const block = structuredCandidate(await acquire(indonesia002CoordinateBlockOutput), "coordinate_block");
  assert.equal(block.text.includes("SISTEM KOORDINAT"), true);
  assert.equal(block.text.includes("UTM WGS 1984 ZONA 50S"), true);
});

test("structured coordinate_block preserves all columns", async () => {
  const block = structuredCandidate(await acquire(indonesia002CoordinateBlockOutput), "coordinate_block");
  assert.equal(block.structuredRows[0]["No."], "1");
  assert.equal(block.structuredRows[0].X, "778984,492");
  assert.equal(block.structuredRows[0].Y, "9721476,737");
  assert.equal(block.structuredRows[0].Latitude, "2°31'2.805\" S");
  assert.equal(block.structuredRows[0].Longitude, "119°30'25.820\" E");
});

test("coordinate_block without rows is not structurally expanded", async () => {
  const acquisition = await acquire({
    rawText: "SISTEM KOORDINAT UTM WGS 1984 ZONA 50S",
    blocks: [{ type: "coordinate_block", headers: ["No.", "X", "Y"], rows: [] }],
    documentCues: ["UTM WGS 1984 ZONA 50S"],
  });
  const block = structuredCandidate(acquisition, "coordinate_block");
  assert.equal(block.structuredRows.length, 0);
});

test("coordinate_block without headers is not structurally expanded", async () => {
  const acquisition = await acquire({
    rawText: "SISTEM KOORDINAT UTM WGS 1984 ZONA 50S",
    blocks: [{ type: "coordinate_block", headers: [], rows: [{ label: "1", cells: ["778984,492", "9721476,737"] }] }],
    documentCues: ["UTM WGS 1984 ZONA 50S"],
  });
  const block = structuredCandidate(acquisition, "coordinate_block");
  assert.equal(block.structuredRows[0]["No."], undefined);
  assert.equal(block.structuredRows[0].X, undefined);
});

test("text_block without rows remains simple text", async () => {
  const acquisition = await acquire({
    rawText: "12.319572, -11.178174",
    blocks: [{ type: "text", text: "12.319572, -11.178174" }],
    documentCues: [],
  });
  const block = structuredCandidate(acquisition, "text_block");
  assert.equal(block.structuredRows.length, 0);
});

test("mock Indonesia #001 coordinate_block real-shape owner", async () => {
  const acquisition = await acquire(indonesia001CoordinateBlockOutput);
  const block = structuredCandidate(acquisition, "coordinate_block");
  const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
  assert.equal(adapter.recognizerId, "indonesia_utm");
  assert.equal(adapter.normalized.coordinates.length, 4);
  assert.equal(canHandleGenericDms(block), false);
});

test("mock Indonesia #002 coordinate_block real-shape owner", async () => {
  const acquisition = await acquire(indonesia002CoordinateBlockOutput);
  const block = structuredCandidate(acquisition, "coordinate_block");
  const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
  assert.equal(adapter.recognizerId, "indonesia_utm");
  assert.equal(adapter.normalized.coordinates.length, 6);
  assert.equal(canHandleGenericDms(block), false);
});

test("source type is metadata not authority", async () => {
  const table = tableCandidate(await acquire(indonesia002Output));
  const block = structuredCandidate(await acquire(indonesia002CoordinateBlockOutput), "coordinate_block");
  assert.equal(canHandleIndonesiaUtm(table), true);
  assert.equal(canHandleIndonesiaUtm(block), true);
  assert.equal(table.sourceType, "table");
  assert.equal(block.sourceType, "coordinate_block");
});

test("WGS84 table structural shaping", async () => {
  const acquisition = await acquire({
    rawText: "Coordinate list",
    blocks: [{ type: "table", headers: ["Point", "Longitude", "Latitude"], rows: [{ label: "A", cells: ["16.0320", "3.7638"] }] }],
    documentCues: ["WGS84"],
  });
  const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
  assert.equal(adapter.recognizerId, "wgs84_table");
});

test("Côte d’Ivoire structural shaping", async () => {
  const acquisition = await acquire({
    rawText: "Coordonnées géographiques",
    blocks: [{ type: "table", headers: ["Point", "Latitude Nord", "Longitude Ouest"], rows: [{ label: "1", cells: ["11°52'11.93\"", "08°53'32.66\""] }] }],
    documentCues: ["Latitude nord", "Longitude ouest"],
  });
  const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
  assert.equal(adapter.recognizerId, "cote_divoire_dms");
});

test("Madagascar structural shaping", async () => {
  const acquisition = await acquire({
    rawText: "Liste_Carrés",
    blocks: [{ type: "table", headers: ["NC", "XV", "YV", "CM_NOMFIR", "num"], rows: [
      { label: "1", cells: ["292812,5", "360937,5", "Ilakaka", "280"] },
      { label: "2", cells: ["292812,5", "361562,5", "Ilakaka", "281"] },
      { label: "3", cells: ["292812,5", "362187,5", "Ilakaka", "282"] },
    ] }],
    documentCues: ["Liste_Carrés"],
  });
  const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
  assert.equal(adapter.recognizerId, "madagascar_cadastral");
});

test("Kyrgyz structural shaping", async () => {
  const acquisition = await acquire({
    rawText: "Координаты угловых точек",
    blocks: [{ type: "table", headers: ["№ points", "X", "Y"], rows: [
      { label: "1", cells: ["13261341", "4607777"] },
      { label: "2", cells: ["13261345", "4607778"] },
      { label: "3", cells: ["13261350", "4607780"] },
    ] }],
    documentCues: ["Координаты угловых точек"],
  });
  const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
  assert.equal(adapter.recognizerId, "kyrgyzstan_gauss_kruger");
});

test("simple WGS84 decimal still works through whole image", async () => {
  const acquisition = await acquire({
    rawText: "12.319572, -11.178174",
    blocks: [{ type: "text", text: "12.319572, -11.178174" }],
    documentCues: [],
  });
  const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
  assert.equal(adapter.recognizerId, "wgs84_decimal");
});

test("simple MGRS still works through coordinate block", async () => {
  const acquisition = await acquire({
    rawText: "47RLH 24469 42832",
    blocks: [{ type: "coordinate_block", text: "47RLH 24469 42832" }],
    documentCues: [],
  });
  const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
  assert.equal(adapter.recognizerId, "mgrs");
});

test("simple generic DMS still works through coordinate block", async () => {
  const acquisition = await acquire({
    rawText: "11°27'45\"N 08°36'30\"W",
    blocks: [{ type: "coordinate_block", text: "11°27'45\"N 08°36'30\"W" }],
    documentCues: [],
  });
  const adapter = await runAcquisitionCandidatesThroughRunner(acquisition);
  assert.equal(adapter.recognizerId, "generic_dms");
});

test("whole image candidate remains present", async () => {
  const acquisition = await acquire(indonesia002Output);
  assert.equal(acquisition.candidates[0].id, "primary_whole_image");
  assert.equal(acquisition.candidates[0].sourceType, "whole_image");
});

test("no provider retry or targeted acquisition", async () => {
  const acquisition = await acquire(indonesia002Output);
  assert.equal(acquisition.providerCalls, 1);
  assert.equal(acquisition.timing.targetedDurationMs, 0);
});

test("security sanitization remains active", async () => {
  const acquisition = await acquire({
    rawText: "Longitude | Latitude\n16.0320 | 3.7638",
    rawProviderResponse: "secret-response",
    prompt: "secret-prompt",
    blocks: [{ type: "table", headers: ["Longitude", "Latitude"], rows: [{ longitude: "16.0320", latitude: "3.7638", apiKey: "secret-key" }] }],
    documentCues: ["WGS84"],
  });
  const serialized = JSON.stringify(acquisition);
  assert.equal(serialized.includes("secret-response"), false);
  assert.equal(serialized.includes("secret-prompt"), false);
  assert.equal(serialized.includes("secret-key"), false);
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

console.log(`Coordinate Engine V3 Candidate Shaping Regression: ${passed}/${tests.length} PASS`);
