import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import sharp from "sharp";

import {
  TABLE_CONTEXT_COMPOSITE_MODE,
  TABLE_CONTEXT_COMPOSITE_STATUS,
  acquirePrimaryImage,
  createTableContextComposite,
  detectTableContextRegions,
} from "../server/coordinate-engine-v3/index.js";

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

function tableSvg({
  width = 900,
  height = 620,
  tables = [{ x: 80, y: 250, width: 520, height: 260, rows: 8, columns: 5 }],
  footer = true,
} = {}) {
  const pieces = [
    `<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}">`,
    `<rect width="100%" height="100%" fill="#2f5f42"/>`,
    `<rect x="20" y="20" width="${width - 40}" height="${height - 60}" fill="#355f45"/>`,
  ];
  for (const table of tables) {
    const rowHeight = table.height / table.rows;
    const columnWidth = table.width / table.columns;
    pieces.push(`<rect x="${table.x}" y="${table.y}" width="${table.width}" height="${table.height}" fill="#ffffff" stroke="#111111" stroke-width="2"/>`);
    pieces.push(`<rect x="${table.x}" y="${table.y}" width="${table.width}" height="${rowHeight}" fill="#fff200" stroke="#111111" stroke-width="2"/>`);
    for (let row = 1; row < table.rows; row += 1) {
      const y = table.y + row * rowHeight;
      pieces.push(`<line x1="${table.x}" y1="${y}" x2="${table.x + table.width}" y2="${y}" stroke="#111111" stroke-width="1.6"/>`);
    }
    for (let column = 1; column < table.columns; column += 1) {
      const x = table.x + column * columnWidth;
      pieces.push(`<line x1="${x}" y1="${table.y}" x2="${x}" y2="${table.y + table.height}" stroke="#111111" stroke-width="1.6"/>`);
    }
    for (let row = 0; row < table.rows; row += 1) {
      for (let column = 0; column < table.columns; column += 1) {
        pieces.push(`<text x="${table.x + column * columnWidth + 8}" y="${table.y + row * rowHeight + 18}" font-family="Arial" font-size="13" fill="#111111">${row + 1}${column + 1}</text>`);
      }
    }
  }
  if (footer) {
    pieces.push(`<rect x="0" y="${height - 58}" width="${width}" height="58" fill="#ffffff" stroke="#111111" stroke-width="1"/>`);
    pieces.push(`<line x1="${width * 0.65}" y1="${height - 58}" x2="${width * 0.65}" y2="${height}" stroke="#111111" stroke-width="1"/>`);
    pieces.push(`<line x1="${width * 0.82}" y1="${height - 58}" x2="${width * 0.82}" y2="${height}" stroke="#111111" stroke-width="1"/>`);
  }
  pieces.push("</svg>");
  return pieces.join("");
}

async function makeImageBase64(svg) {
  return (await sharp(Buffer.from(svg)).jpeg({ quality: 92 }).toBuffer()).toString("base64");
}

async function makeNoTableBase64() {
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="900" height="620">
    <rect width="100%" height="100%" fill="#2f5f42"/>
    <circle cx="420" cy="260" r="90" fill="#407456"/>
    <path d="M100 500 C300 300 500 600 780 230" stroke="#1c3f2b" stroke-width="8" fill="none"/>
  </svg>`;
  return makeImageBase64(svg);
}

async function fakeProvider({ text }) {
  return Object.freeze({
    ok: true,
    status: 200,
    text,
    responseReceived: true,
  });
}

test("table-like region detected", async () => {
  const imageBase64 = await makeImageBase64(tableSvg());
  const result = await detectTableContextRegions({ imageBase64 });
  assert.equal(result.status, TABLE_CONTEXT_COMPOSITE_STATUS.CREATED);
  assert.equal(result.regions.length > 0, true);
});

test("no table region returns no strong table", async () => {
  const result = await detectTableContextRegions({ imageBase64: await makeNoTableBase64() });
  assert.equal(result.status, TABLE_CONTEXT_COMPOSITE_STATUS.NO_STRONG_TABLE_REGION);
});

test("multiple regions choose deterministic larger grid", async () => {
  const imageBase64 = await makeImageBase64(tableSvg({
    tables: [
      { x: 40, y: 80, width: 160, height: 110, rows: 4, columns: 3 },
      { x: 280, y: 240, width: 520, height: 260, rows: 8, columns: 5 },
    ],
  }));
  const result = await detectTableContextRegions({ imageBase64 });
  assert.equal(result.status, TABLE_CONTEXT_COMPOSITE_STATUS.CREATED);
  assert.equal(result.regions[0].region.width > 400, true);
  assert.equal(result.regions[0].region.height > 200, true);
});

test("deterministic ranking is stable", async () => {
  const imageBase64 = await makeImageBase64(tableSvg());
  const first = await detectTableContextRegions({ imageBase64 });
  const second = await detectTableContextRegions({ imageBase64 });
  assert.deepEqual(first.regions[0].region, second.regions[0].region);
});

test("crop stays within bounds", async () => {
  const imageBase64 = await makeImageBase64(tableSvg());
  const result = await detectTableContextRegions({ imageBase64 });
  const region = result.regions[0].region;
  assert.equal(region.x >= 0, true);
  assert.equal(region.y >= 0, true);
  assert.equal(region.x + region.width <= result.originalDimensions.width, true);
  assert.equal(region.y + region.height <= result.originalDimensions.height, true);
});

test("margin expansion covers table boundary", async () => {
  const imageBase64 = await makeImageBase64(tableSvg());
  const result = await detectTableContextRegions({ imageBase64 });
  const region = result.regions[0].region;
  assert.equal(region.x <= 80, true);
  assert.equal(region.y <= 250, true);
  assert.equal(region.x + region.width >= 600, true);
  assert.equal(region.y + region.height >= 510, true);
});

test("context strip included", async () => {
  const composite = await createTableContextComposite({ imageBase64: await makeImageBase64(tableSvg()) });
  assert.equal(composite.status, TABLE_CONTEXT_COMPOSITE_STATUS.CREATED);
  assert.equal(composite.contextRegions.length, 1);
  assert.equal(composite.contextRegions[0].role, "bottom_context_strip");
});

test("composite generation returns image", async () => {
  const composite = await createTableContextComposite({ imageBase64: await makeImageBase64(tableSvg()) });
  assert.equal(composite.preprocessingMode, TABLE_CONTEXT_COMPOSITE_MODE);
  assert.equal(composite.imageBase64.length > 100, true);
  assert.equal(composite.mimeType, "image/jpeg");
});

test("table resolution is preserved or expanded", async () => {
  const composite = await createTableContextComposite({ imageBase64: await makeImageBase64(tableSvg()) });
  assert.equal(composite.detectedTableRegion.width >= 520, true);
  assert.equal(composite.detectedTableRegion.height >= 260, true);
});

test("runtime preprocessing contains no type-specific string logic", () => {
  const source = readFileSync("server/coordinate-engine-v3/acquisition/table-context-composite.js", "utf8");
  assert.equal(/Indonesia|UTM|WGS|EPSG|ZONA|Latitude|Longitude/.test(source), false);
  assert.equal(/filename|fixture|003/.test(source), false);
});

test("preprocessing failure does not call provider fallback", async () => {
  const composite = await createTableContextComposite({ imageBase64: await makeNoTableBase64() });
  let providerCalls = 0;
  if (composite.status === TABLE_CONTEXT_COMPOSITE_STATUS.CREATED) {
    providerCalls += 1;
  }
  assert.equal(composite.status, TABLE_CONTEXT_COMPOSITE_STATUS.NO_STRONG_TABLE_REGION);
  assert.equal(providerCalls, 0);
  assert.equal(composite.providerCalls, 0);
});

test("successful experiment path calls provider once", async () => {
  const composite = await createTableContextComposite({ imageBase64: await makeImageBase64(tableSvg()) });
  let calls = 0;
  await acquirePrimaryImage({
    imageBase64: composite.imageBase64,
    mimeType: composite.mimeType,
    provider: async () => {
      calls += 1;
      return fakeProvider({
        text: JSON.stringify({
          rawText: "plain text",
          blocks: [{ type: "text", text: "plain text", headers: [], rows: [], confidence: 0.5 }],
          documentCues: [],
        }),
      });
    },
  });
  assert.equal(calls, 1);
});

test("metadata is sanitized and excludes image payload", async () => {
  const composite = await createTableContextComposite({ imageBase64: await makeImageBase64(tableSvg()) });
  const metadata = JSON.stringify({
    status: composite.status,
    preprocessingMode: composite.preprocessingMode,
    originalDimensions: composite.originalDimensions,
    detectedTableRegion: composite.detectedTableRegion,
    contextRegions: composite.contextRegions,
    compositeDimensions: composite.compositeDimensions,
  });
  assert.equal(metadata.includes("base64"), false);
  assert.equal(metadata.includes("imageBase64"), false);
});

test("provider call contract remains zero during preprocessing", async () => {
  const composite = await createTableContextComposite({ imageBase64: await makeImageBase64(tableSvg()) });
  assert.equal(composite.providerCalls, 0);
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

console.log(`Coordinate Engine V3 Table Context Composite Regression: ${passed}/${tests.length} PASS`);
