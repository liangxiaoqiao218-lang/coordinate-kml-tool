import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import {
  COORDINATE_IMAGE_SAFETY_LIMITS,
  COORDINATE_IMAGE_SAFETY_REASON,
  COORDINATE_IMAGE_SAFETY_STATUS,
  canonicalizeCoordinateImageUpload,
  hasValidJpegStructure
} from "../server/recognition/coordinate-image-safety.js";

function segment(marker, payload) {
  const length = Buffer.alloc(2);
  length.writeUInt16BE(payload.length + 2);
  return Buffer.concat([Buffer.from([0xff, marker]), length, payload]);
}

function makeDht(tableClass, counts = [1, ...Array(15).fill(0)]) {
  const symbolCount = counts.reduce((sum, count) => sum + count, 0);
  return segment(0xc4, Buffer.from([
    tableClass << 4,
    ...counts,
    ...Array(symbolCount).fill(0)
  ]));
}

function makeFrame({ marker = 0xc0, width = 1, height = 1 } = {}) {
  const payload = Buffer.alloc(9);
  payload[0] = 8;
  payload.writeUInt16BE(height, 1);
  payload.writeUInt16BE(width, 3);
  payload[5] = 1;
  payload[6] = 1;
  payload[7] = 0x11;
  payload[8] = 0;
  return segment(marker, payload);
}

function makeScan({ selectors = 0, entropy = Buffer.from([0x11, 0x22]) } = {}) {
  return Buffer.concat([
    segment(0xda, Buffer.from([1, 1, selectors, 0, 63, 0])),
    entropy
  ]);
}

function makeJpeg({
  appPayload = null,
  width = 1,
  height = 1,
  frameMarker = 0xc0,
  secondFrame = false,
  dqtValue = 1,
  dcCounts,
  scanSelectors = 0,
  entropy,
  includeEoi = true
} = {}) {
  const parts = [Buffer.from([0xff, 0xd8])];
  if (appPayload) parts.push(segment(0xe1, appPayload));
  parts.push(segment(0xdb, Buffer.from([0, ...Array(64).fill(dqtValue)])));
  parts.push(makeDht(0, dcCounts));
  parts.push(makeDht(1));
  parts.push(makeFrame({ marker: frameMarker, width, height }));
  if (secondFrame) parts.push(makeFrame({ width, height }));
  parts.push(makeScan({ selectors: scanSelectors, entropy }));
  if (includeEoi) parts.push(Buffer.from([0xff, 0xd9]));
  return Buffer.concat(parts);
}

function file(buffer, mimetype = "image/jpeg") {
  return { buffer, size: buffer.length, mimetype, originalname: "synthetic.jpg", fieldname: "image" };
}

const cases = [];
const test = (name, run) => cases.push({ name, run });
const canonical = makeJpeg();

test("canonical JPEG bytes remain unchanged", () => {
  assert.equal(hasValidJpegStructure(canonical), true);
  const result = canonicalizeCoordinateImageUpload(file(canonical));
  assert.equal(result.valid, true);
  assert.equal(result.status, COORDINATE_IMAGE_SAFETY_STATUS.JPEG_CANONICAL_UNCHANGED);
  assert.equal(result.file.buffer, canonical);
  assert.equal(result.file.size, canonical.length);
  assert.equal(result.file.mimetype, "image/jpeg");
});

test("bounded untrusted trailing data is discarded into an independent exact prefix", () => {
  const original = Buffer.concat([canonical, Buffer.from([7, 6, 5, 4, 3])]);
  assert.equal(hasValidJpegStructure(original), false);
  const result = canonicalizeCoordinateImageUpload(file(original, "image/jpg"));
  assert.equal(result.valid, true);
  assert.equal(result.status, COORDINATE_IMAGE_SAFETY_STATUS.JPEG_TRAILING_DATA_DISCARDED_UNTRUSTED);
  assert.equal(result.file.buffer.equals(canonical), true);
  assert.notEqual(result.file.buffer, original);
  assert.equal(result.file.buffer.buffer === original.buffer, false);
  assert.equal(result.file.size, canonical.length);
  assert.equal(result.file.mimetype, "image/jpeg");
  assert.equal(Object.hasOwn(result, "trailingData"), false);
});

test("trailing data limit is inclusive and excess fails closed", () => {
  const atLimit = canonicalizeCoordinateImageUpload(file(Buffer.concat([
    canonical,
    Buffer.alloc(COORDINATE_IMAGE_SAFETY_LIMITS.maxTrailingBytes, 0x5a)
  ])));
  assert.equal(atLimit.valid, true);
  const overLimit = canonicalizeCoordinateImageUpload(file(Buffer.concat([
    canonical,
    Buffer.alloc(COORDINATE_IMAGE_SAFETY_LIMITS.maxTrailingBytes + 1, 0x5a)
  ])));
  assert.equal(overLimit.valid, false);
  assert.equal(overLimit.reason, COORDINATE_IMAGE_SAFETY_REASON.JPEG_TRAILING_DATA_LIMIT_EXCEEDED);
});

test("EOI-like bytes inside an APP segment cannot become the logical boundary", () => {
  const withAppEoi = makeJpeg({ appPayload: Buffer.from([1, 0xff, 0xd9, 2, 3]) });
  const result = canonicalizeCoordinateImageUpload(file(Buffer.concat([withAppEoi, Buffer.from([9])])));
  assert.equal(result.valid, true);
  assert.equal(result.file.buffer.equals(withAppEoi), true);
});

test("stuffed entropy bytes and restart markers cannot become the logical boundary", () => {
  const withEntropyMarkers = makeJpeg({ entropy: Buffer.from([0x11, 0xff, 0x00, 0x22, 0xff, 0xd0, 0x33]) });
  const result = canonicalizeCoordinateImageUpload(file(Buffer.concat([withEntropyMarkers, Buffer.from([8, 7])])));
  assert.equal(result.valid, true);
  assert.equal(result.file.buffer.equals(withEntropyMarkers), true);
});

test("missing EOI fails closed", () => {
  const result = canonicalizeCoordinateImageUpload(file(makeJpeg({ includeEoi: false })));
  assert.equal(result.valid, false);
  assert.equal(result.reason, COORDINATE_IMAGE_SAFETY_REASON.JPEG_CANONICAL_PREFIX_UNPROVEN);
});

test("malformed quantization table fails closed", () => {
  assert.equal(canonicalizeCoordinateImageUpload(file(makeJpeg({ dqtValue: 0 }))).valid, false);
});

test("oversubscribed Huffman table fails closed", () => {
  const result = canonicalizeCoordinateImageUpload(file(makeJpeg({ dcCounts: [3, ...Array(15).fill(0)] })));
  assert.equal(result.valid, false);
});

test("scan referencing an unavailable Huffman table fails closed", () => {
  assert.equal(canonicalizeCoordinateImageUpload(file(makeJpeg({ scanSelectors: 0x11 }))).valid, false);
});

test("a second Frame fails closed", () => {
  assert.equal(canonicalizeCoordinateImageUpload(file(makeJpeg({ secondFrame: true }))).valid, false);
});

test("unsupported JPEG coding modes fail closed", () => {
  assert.equal(canonicalizeCoordinateImageUpload(file(makeJpeg({ frameMarker: 0xc1 }))).valid, false);
});

test("dimension limit fails closed with a fixed resource reason", () => {
  const result = canonicalizeCoordinateImageUpload(file(makeJpeg({ width: 16_385, height: 1 })));
  assert.equal(result.valid, false);
  assert.equal(result.reason, COORDINATE_IMAGE_SAFETY_REASON.JPEG_RESOURCE_LIMIT_EXCEEDED);
});

test("pixel limit fails closed with a fixed resource reason", () => {
  const result = canonicalizeCoordinateImageUpload(file(makeJpeg({ width: 8_000, height: 5_001 })));
  assert.equal(result.valid, false);
  assert.equal(result.reason, COORDINATE_IMAGE_SAFETY_REASON.JPEG_RESOURCE_LIMIT_EXCEEDED);
});

test("JPEG MIME and magic must agree", () => {
  const result = canonicalizeCoordinateImageUpload(file(Buffer.from("not-jpeg")));
  assert.equal(result.valid, false);
  assert.equal(result.reason, COORDINATE_IMAGE_SAFETY_REASON.COORDINATE_IMAGE_INVALID);
});

test("PNG and BMP stay outside the JPEG compatibility path", () => {
  for (const [buffer, mimetype] of [[Buffer.from("png"), "image/png"], [Buffer.from("bmp"), "image/bmp"]]) {
    const input = file(buffer, mimetype);
    const result = canonicalizeCoordinateImageUpload(input);
    assert.equal(result.valid, true);
    assert.equal(result.status, COORDINATE_IMAGE_SAFETY_STATUS.NON_JPEG_UNCHANGED);
    assert.equal(result.file, input);
  }
});

test("unsupported types receive no JPEG compatibility treatment", () => {
  for (const mimetype of ["image/gif", "image/webp", "image/heif", "application/octet-stream"]) {
    const input = file(canonical, mimetype);
    const result = canonicalizeCoordinateImageUpload(input);
    assert.equal(result.status, COORDINATE_IMAGE_SAFETY_STATUS.NON_JPEG_UNCHANGED);
    assert.equal(result.file, input);
  }
});

test("server installs the safety boundary before user data and every image consumer", async () => {
  const source = await readFile(new URL("../server.js", import.meta.url), "utf8");
  const routeStart = source.indexOf('app.post("/api/recognize-coordinates"');
  const route = source.slice(routeStart);
  const safety = route.indexOf("canonicalizeCoordinateImageUpload(req.file)");
  const replacement = route.indexOf("req.file = imageCanonicalization.file");
  assert.ok(routeStart >= 0 && safety >= 0 && replacement > safety);
  for (const marker of [
    "readAdminData()",
    "validateCoordinateImageUpload(req.file)",
    "req.file.buffer.toString(\"base64\")",
    "detectUploadTableStructure(req.file?.buffer",
    "runLocalOcrFallback(req.file.buffer"
  ]) {
    assert.ok(route.indexOf(marker) > replacement, `${marker} must use the canonical file`);
  }
  assert.match(source, /function hasValidJpegStructure\(buffer\)\s*{\s*return hasValidCanonicalJpegStructure\(buffer\);\s*}/);
  assert.doesNotMatch(source, /JPEG_TRAILING_DATA_DISCARDED_UNTRUSTED[\s\S]{0,120}(?:tail|trailingData)\s*:/i);
});

let passed = 0;
for (const entry of cases) {
  await entry.run();
  passed += 1;
  console.log(`PASS ${entry.name}`);
}
console.log(`Coordinate image safety P0 regression: ${passed}/${cases.length} PASS`);
