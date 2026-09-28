import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { deflateSync } from "node:zlib";
import {
  COORDINATE_IMAGE_SAFETY_LIMITS,
  COORDINATE_IMAGE_SAFETY_REASON,
  COORDINATE_IMAGE_SAFETY_STATUS,
  canonicalizeCoordinateImageUpload,
  createCoordinateImageIdentity,
  detectCoordinateImageMimeType,
  hasValidBmpStructure,
  hasValidJpegStructure,
  hasValidPngStructure,
  validateCoordinateImageUpload
} from "../server/recognition/coordinate-image-safety.js";

let pngCrcTable = null;
function pngCrc32(buffer) {
  if (!pngCrcTable) {
    pngCrcTable = Array.from({ length: 256 }, (_, value) => {
      let crc = value;
      for (let bit = 0; bit < 8; bit += 1) crc = (crc & 1) ? (0xedb88320 ^ (crc >>> 1)) : (crc >>> 1);
      return crc >>> 0;
    });
  }
  let crc = 0xffffffff;
  for (const byte of buffer) crc = pngCrcTable[(crc ^ byte) & 0xff] ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}

function pngChunk(type, data) {
  const typeBytes = Buffer.from(type, "ascii");
  const length = Buffer.alloc(4);
  length.writeUInt32BE(data.length);
  const crc = Buffer.alloc(4);
  crc.writeUInt32BE(pngCrc32(Buffer.concat([typeBytes, data])));
  return Buffer.concat([length, typeBytes, data, crc]);
}

function makePng({ colorType = 2, splitIdat = false, width = 2, height = 2 } = {}) {
  const channels = ({ 0: 1, 2: 3, 3: 1, 6: 4 })[colorType];
  const header = Buffer.alloc(13);
  header.writeUInt32BE(width, 0);
  header.writeUInt32BE(height, 4);
  header[8] = 8;
  header[9] = colorType;
  const rows = [];
  for (let row = 0; row < height; row += 1) {
    rows.push(Buffer.from([0, ...Array(width * channels).fill(row + 1)]));
  }
  const compressed = deflateSync(Buffer.concat(rows));
  const midpoint = Math.max(1, Math.floor(compressed.length / 2));
  const idatChunks = splitIdat
    ? [pngChunk("IDAT", compressed.subarray(0, midpoint)), pngChunk("IDAT", compressed.subarray(midpoint))]
    : [pngChunk("IDAT", compressed)];
  const palette = colorType === 3 ? [pngChunk("PLTE", Buffer.from([0, 0, 0, 255, 255, 255]))] : [];
  return Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    pngChunk("IHDR", header),
    ...palette,
    ...idatChunks,
    pngChunk("IEND", Buffer.alloc(0))
  ]);
}

function makeBmp(width = 2, height = 2) {
  const rowBytes = Math.floor(((24 * width) + 31) / 32) * 4;
  const pixelBytes = rowBytes * height;
  const buffer = Buffer.alloc(54 + pixelBytes);
  buffer.write("BM", 0, "ascii");
  buffer.writeUInt32LE(buffer.length, 2);
  buffer.writeUInt32LE(54, 10);
  buffer.writeUInt32LE(40, 14);
  buffer.writeInt32LE(width, 18);
  buffer.writeInt32LE(height, 22);
  buffer.writeUInt16LE(1, 26);
  buffer.writeUInt16LE(24, 28);
  return buffer;
}

async function listCoordinateImageFixtures(directory) {
  const files = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const absolute = path.join(directory, entry.name);
    if (entry.isDirectory()) files.push(...await listCoordinateImageFixtures(absolute));
    else if (/\.(?:png|jpe?g|bmp)$/i.test(entry.name)) files.push(absolute);
  }
  return files;
}

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

test("valid PNG and BMP receive complete structure validation", () => {
  for (const [buffer, mimetype] of [[makePng(), "image/png"], [makeBmp(), "image/bmp"]]) {
    const input = file(buffer, mimetype);
    const result = canonicalizeCoordinateImageUpload(input);
    assert.equal(result.valid, true);
    assert.equal(result.status, COORDINATE_IMAGE_SAFETY_STATUS.NON_JPEG_UNCHANGED);
    assert.equal(result.file.buffer, input.buffer);
    assert.equal(result.file.mimetype, mimetype);
  }
});

test("supported signatures override missing, generic, or incorrect multipart MIME metadata", () => {
  for (const [buffer, expectedMime] of [
    [canonical, "image/jpeg"],
    [makePng({ colorType: 6 }), "image/png"],
    [makeBmp(), "image/bmp"]
  ]) {
    for (const claimedMime of ["", "application/octet-stream", "text/plain", "image/gif", "image/jpeg"]) {
      const result = canonicalizeCoordinateImageUpload(file(buffer, claimedMime));
      assert.equal(result.valid, true, `${expectedMime} must not depend on ${claimedMime}`);
      assert.equal(result.file.mimetype, expectedMime);
      assert.equal(validateCoordinateImageUpload(result.file).mimeType, expectedMime);
    }
  }
});

test("PNG grayscale, RGB, RGBA, palette, and multi-IDAT variants pass", () => {
  for (const colorType of [0, 2, 3, 6]) {
    for (const splitIdat of [false, true]) {
      const png = makePng({ colorType, splitIdat });
      assert.equal(detectCoordinateImageMimeType(png), "image/png");
      assert.equal(hasValidPngStructure(png), true, `colorType=${colorType} split=${splitIdat}`);
      assert.equal(canonicalizeCoordinateImageUpload(file(png, "application/octet-stream")).valid, true);
    }
  }
});

test("valid BMP and the frozen Cote d'Ivoire PNG normalize from generic MIME", async () => {
  const bmp = makeBmp();
  assert.equal(hasValidBmpStructure(bmp), true);
  const frozenPng = await readFile(new URL("../regression-samples/fixtures/科特迪瓦02.png", import.meta.url));
  const result = canonicalizeCoordinateImageUpload(file(frozenPng, "application/octet-stream"));
  assert.equal(result.valid, true);
  assert.equal(result.file.mimetype, "image/png");
  const identity = createCoordinateImageIdentity(result.file, { requestId: "mime-regression", page: 1 });
  assert.equal(identity.mime_type, "image/png");
  assert.equal(identity.width, 620);
  assert.equal(identity.height, 269);
});

test("every repository coordinate image fixture passes generic ingress validation", async () => {
  const fixtureRoot = fileURLToPath(new URL("../regression-samples/", import.meta.url));
  const fixtures = await listCoordinateImageFixtures(fixtureRoot);
  assert.ok(fixtures.length > 0);
  for (const fixturePath of fixtures) {
    const buffer = await readFile(fixturePath);
    const result = canonicalizeCoordinateImageUpload(file(buffer, "application/octet-stream"));
    assert.equal(result.valid, true, path.relative(fixtureRoot, fixturePath));
    assert.equal(validateCoordinateImageUpload(result.file).valid, true, path.relative(fixtureRoot, fixturePath));
  }
  console.log(`Coordinate fixture ingress sweep: ${fixtures.length}/${fixtures.length} PASS`);
});

test("corrupt, disguised, unsupported, and resource-limit images fail closed", () => {
  const damagedPng = makePng();
  damagedPng[damagedPng.length - 1] ^= 1;
  const oversizedPng = makePng({ width: COORDINATE_IMAGE_SAFETY_LIMITS.maxDimension + 1, height: 1 });
  for (const buffer of [
    Buffer.from("not-an-image"),
    Buffer.from("GIF89a", "ascii"),
    damagedPng,
    oversizedPng,
    Buffer.concat([makeBmp(), Buffer.from([0])])
  ]) {
    for (const mimetype of ["image/png", "image/jpeg", "application/octet-stream"]) {
      const result = canonicalizeCoordinateImageUpload(file(buffer, mimetype));
      assert.equal(result.valid, false);
      assert.equal(result.reason, COORDINATE_IMAGE_SAFETY_REASON.COORDINATE_IMAGE_INVALID);
    }
  }
});

test("server installs the safety boundary before user data and every image consumer", async () => {
  const source = await readFile(new URL("../server.js", import.meta.url), "utf8");
  const routeStart = source.indexOf("async function recognizeCoordinatesHandler");
  const route = source.slice(routeStart);
  const safety = route.indexOf("canonicalizeCoordinateImageUpload(req.file)");
  const replacement = route.indexOf("req.file = imageCanonicalization.file");
  assert.ok(routeStart >= 0 && safety >= 0 && replacement > safety);
  for (const marker of [
    "readAdminData()",
    "validateCoordinateImageUpload(req.file)",
    "createRecognitionImageVariants({",
    "runLocalOcrFallback(req.file.buffer"
  ]) {
    assert.ok(route.indexOf(marker) > replacement, `${marker} must use the canonical file`);
  }
  assert.match(source, /createCoordinateImageIdentity,\s*validateCoordinateImageUpload/);
  const asyncRoute = source.slice(source.indexOf('app.post("\/api\/recognize-coordinates\/jobs"'));
  assert.ok(asyncRoute.indexOf("canonicalizeCoordinateImageUpload(req.file)") < asyncRoute.indexOf("recognitionAcquisitionJobRuntime.enqueue"));
  assert.doesNotMatch(source, /JPEG_TRAILING_DATA_DISCARDED_UNTRUSTED[\s\S]{0,120}(?:tail|trailingData)\s*:/i);
});

let passed = 0;
for (const entry of cases) {
  await entry.run();
  passed += 1;
  console.log(`PASS ${entry.name}`);
}
console.log(`Coordinate image safety P0 regression: ${passed}/${cases.length} PASS`);
