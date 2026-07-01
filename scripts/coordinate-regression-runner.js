import { access, readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '..');
const samplesRoot = path.join(repoRoot, 'regression-samples');
const defaultApiUrl = 'http://127.0.0.1:3000/api/recognize-coordinates';
const apiUrl = process.env.COORDINATE_REGRESSION_API_URL || defaultApiUrl;

const requiredFields = [
  'typeId',
  'sampleName',
  'inputType',
  'inputFile',
  'expectedPrecisionMode',
  'expectedParserTrace',
  'expectedPointCount',
  'expectedFirstKml',
  'expectedLastKml',
  'status',
  'notes',
];

function normalizeStatus(value) {
  return String(value || '').trim().toLowerCase();
}

function validateExpected(sample) {
  const missing = requiredFields.filter((field) => !(field in sample));
  const errors = [];

  if (missing.length) {
    errors.push(`missing fields: ${missing.join(', ')}`);
  }

  if (!Array.isArray(sample.expectedParserTrace)) {
    errors.push('expectedParserTrace must be an array');
  }

  if (!Number.isInteger(sample.expectedPointCount)) {
    errors.push('expectedPointCount must be an integer');
  }

  const status = normalizeStatus(sample.status);
  if (!['active', 'pending', 'unstable'].includes(status)) {
    errors.push('status must be active, pending, or unstable');
  }

  const inputType = String(sample.inputType || '').trim().toLowerCase();
  if (!['image', 'text'].includes(inputType)) {
    errors.push('inputType must be image or text');
  }

  return errors;
}

function isWindowsAbsolutePath(value) {
  return /^[a-zA-Z]:[\\/]/.test(value);
}

function resolveInputFile(inputFile) {
  const value = String(inputFile || '').trim();
  if (!value || value.startsWith('TEXT:') || value.startsWith('PENDING:')) {
    return value;
  }

  if (path.isAbsolute(value) || isWindowsAbsolutePath(value)) {
    return path.normalize(value);
  }

  return path.resolve(repoRoot, value);
}

function getMimeType(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  switch (ext) {
    case '.png':
      return 'image/png';
    case '.jpg':
    case '.jpeg':
      return 'image/jpeg';
    case '.webp':
      return 'image/webp';
    default:
      return 'application/octet-stream';
  }
}

function normalizeTrace(trace) {
  if (Array.isArray(trace)) {
    return trace.map((item) => String(item));
  }

  if (typeof trace === 'string') {
    return trace
      .split(/(?:->|\n|,)/)
      .map((item) => item.trim())
      .filter(Boolean);
  }

  return [];
}

function traceContains(actualTrace, expectedTrace) {
  const actualText = actualTrace.join(' -> ');
  return expectedTrace.every((entry) => actualText.includes(String(entry)));
}

function extractCoordinateLines(text) {
  return String(text || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .filter((line) => {
      if (/^(label|point|order|num)\b/i.test(line)) return false;
      return /[-+]?\d+(?:\.\d+)?/.test(line);
    });
}

function extractKmlCoordinates(text) {
  const coordinates = [];
  const lines = String(text || '').split(/\r?\n/);

  for (const line of lines) {
    const pipeParts = line.split('|').map((part) => part.trim());
    const kmlPart = pipeParts.find((part) => /^[-+]?\d+(?:\.\d+)?\s*,\s*[-+]?\d+(?:\.\d+)?\s*,\s*0\b/.test(part));
    if (kmlPart) {
      coordinates.push(kmlPart.replace(/\s+/g, ''));
      continue;
    }

    const bareKml = line.match(/[-+]?\d+(?:\.\d+)?\s*,\s*[-+]?\d+(?:\.\d+)?\s*,\s*0\b/);
    if (bareKml) {
      coordinates.push(bareKml[0].replace(/\s+/g, ''));
    }
  }

  return coordinates;
}

function extractProjectedOrDecimalRows(text) {
  return extractCoordinateLines(text)
    .map((line) => {
      const match = line.match(/(-?\d+(?:\.\d+)?)\s*[,| ]\s*(-?\d+(?:\.\d+)?)/);
      return match ? `${match[1]},${match[2]}` : '';
    })
    .filter(Boolean);
}

function summarizeApiResponse(data) {
  const coordinatesText = data?.coordinates || data?.formatted || data?.result || '';
  const kmlCoordinates = extractKmlCoordinates(coordinatesText);
  const fallbackRows = extractProjectedOrDecimalRows(coordinatesText);
  const coordinateRows = kmlCoordinates.length ? kmlCoordinates : fallbackRows;
  const pointCount = Number.isInteger(data?.pointCount)
    ? data.pointCount
    : Array.isArray(data?.points)
      ? data.points.length
      : coordinateRows.length;

  return {
    precisionMode: data?.precisionMode || '',
    parserTrace: normalizeTrace(data?.parserTrace),
    pointCount,
    firstCoordinate: coordinateRows[0] || '',
    lastCoordinate: coordinateRows[coordinateRows.length - 1] || '',
    coordinateRows,
  };
}

async function callRecognizeApi(expected) {
  if (String(expected.inputType || '').trim().toLowerCase() === 'text') {
    throw new Error('TEXT_FIXTURE_REQUIRES_TEXT_ENDPOINT');
  }

  const inputFile = resolveInputFile(expected.inputFile);

  if (String(inputFile).startsWith('TEXT:')) {
    throw new Error('TEXT fixture cannot call image-only /api/recognize-coordinates');
  }

  if (String(inputFile).startsWith('PENDING:')) {
    throw new Error('pending fixture has no input file');
  }

  await access(inputFile);
  const fileBuffer = await readFile(inputFile);
  const form = new FormData();
  const blob = new Blob([fileBuffer], { type: getMimeType(inputFile) });
  form.append('image', blob, path.basename(inputFile));
  form.append('visitorId', 'coordinate-regression-runner');

  const response = await fetch(apiUrl, {
    method: 'POST',
    body: form,
    headers: {
      'x-visitor-id': 'coordinate-regression-runner',
      'x-source': 'coordinate-regression-runner',
    },
  });

  const text = await response.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    throw new Error(`API returned non-JSON response (${response.status}): ${text.slice(0, 200)}`);
  }

  if (!response.ok) {
    throw new Error(`API error ${response.status}: ${data?.error || data?.message || text.slice(0, 200)}`);
  }

  return summarizeApiResponse(data);
}

function compareExpected(expected, actual) {
  const failures = [];

  if (actual.precisionMode !== expected.expectedPrecisionMode) {
    failures.push(`expected precisionMode: ${expected.expectedPrecisionMode}; actual: ${actual.precisionMode || '(empty)'}`);
  }

  if (!traceContains(actual.parserTrace, expected.expectedParserTrace)) {
    failures.push(`expected parserTrace contains: ${expected.expectedParserTrace.join(' -> ')}; actual: ${actual.parserTrace.join(' -> ') || '(empty)'}`);
  }

  if (actual.pointCount !== expected.expectedPointCount) {
    failures.push(`expected pointCount: ${expected.expectedPointCount}; actual: ${actual.pointCount}`);
  }

  if (actual.firstCoordinate !== expected.expectedFirstKml) {
    failures.push(`expected first KML: ${expected.expectedFirstKml}; actual: ${actual.firstCoordinate || '(empty)'}`);
  }

  if (actual.lastCoordinate !== expected.expectedLastKml) {
    failures.push(`expected last KML: ${expected.expectedLastKml}; actual: ${actual.lastCoordinate || '(empty)'}`);
  }

  return failures;
}

async function loadExpectedJson(sampleDir) {
  const expectedPath = path.join(samplesRoot, sampleDir, 'expected.json');
  const raw = await readFile(expectedPath, 'utf8');
  return JSON.parse(raw);
}

async function main() {
  const entries = await readdir(samplesRoot, { withFileTypes: true });
  const sampleDirs = entries
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .sort((a, b) => a.localeCompare(b));

  const rows = [];

  for (const sampleDir of sampleDirs) {
    try {
      const expected = await loadExpectedJson(sampleDir);
      const errors = validateExpected(expected);
      const status = normalizeStatus(expected.status);

      if (errors.length) {
        rows.push({
          directory: sampleDir,
          typeId: expected.typeId || '(missing typeId)',
          sampleName: expected.sampleName || '(missing sampleName)',
          result: 'FAIL',
          errors,
        });
        continue;
      }

      if (status === 'pending') {
        rows.push({
          directory: sampleDir,
          typeId: expected.typeId,
          sampleName: expected.sampleName,
          result: 'PENDING',
          errors: [],
        });
        continue;
      }

      if (String(expected.inputType || '').trim().toLowerCase() === 'text') {
        rows.push({
          directory: sampleDir,
          typeId: expected.typeId,
          sampleName: expected.sampleName,
          result: 'SKIPPED_TEXT',
          errors: ['text fixture requires text parser endpoint'],
        });
        continue;
      }

      let actual;
      let compareFailures;
      try {
        actual = await callRecognizeApi(expected);
        compareFailures = compareExpected(expected, actual);
      } catch (error) {
        compareFailures = [
          error.message === 'fetch failed'
            ? `API unavailable: ${apiUrl}`
            : error.message,
        ];
      }

      const result = status === 'unstable'
        ? 'UNSTABLE'
        : compareFailures.length
          ? 'FAIL'
          : 'PASS';

      rows.push({
        directory: sampleDir,
        typeId: expected.typeId,
        sampleName: expected.sampleName,
        result,
        errors: compareFailures,
        actual,
      });
    } catch (error) {
      rows.push({
        directory: sampleDir,
        typeId: '(missing expected.json)',
        sampleName: '(missing expected.json)',
        result: 'FAIL',
        errors: [error.code === 'ENOENT' ? 'expected.json not found' : error.message],
      });
    }
  }

  const summary = rows.reduce(
    (acc, row) => {
      acc.total += 1;
      acc[row.result.toLowerCase()] += 1;
      return acc;
    },
    { total: 0, pass: 0, fail: 0, pending: 0, unstable: 0, skipped_text: 0 },
  );

  console.log('Coordinate Engine Regression Runner');
  console.log(`API: ${apiUrl}`);
  console.log('');

  for (const row of rows) {
    const label = `${row.directory} ${row.result}`;
    console.log(label);
    console.log(`  typeId: ${row.typeId}`);
    console.log(`  sample: ${row.sampleName}`);
    if (row.actual) {
      console.log(`  actualPrecisionMode: ${row.actual.precisionMode || '(empty)'}`);
      console.log(`  actualPointCount: ${row.actual.pointCount}`);
      console.log(`  actualFirst: ${row.actual.firstCoordinate || '(empty)'}`);
      console.log(`  actualLast: ${row.actual.lastCoordinate || '(empty)'}`);
    }
    if (row.errors.length) {
      console.log(`  errors: ${row.errors.join('; ')}`);
    }
  }

  console.log('');
  console.log('Regression Summary');
  console.log(`total: ${summary.total}`);
  console.log(`pass: ${summary.pass}`);
  console.log(`fail: ${summary.fail}`);
  console.log(`pending: ${summary.pending}`);
  console.log(`unstable: ${summary.unstable}`);
  console.log(`skippedText: ${summary.skipped_text}`);

  if (summary.fail > 0) {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
