import { createHash } from 'node:crypto';
import { access, readdir, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '..');
const samplesRoot = path.join(repoRoot, 'regression-samples');
const defaultApiUrl = 'http://127.0.0.1:3000/api/recognize-coordinates';
const apiUrl = process.env.COORDINATE_REGRESSION_API_URL || defaultApiUrl;
const coordinateTolerance = Number(process.env.COORDINATE_REGRESSION_TOLERANCE || '1e-6');

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

const baselineFields = [
  'typeId',
  'precisionMode',
  'parserTraceAlias',
  'pointCount',
  'geometry',
  'firstCoordinate',
  'lastCoordinate',
  'status',
  'baselineCommit',
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

function validateBaseline(baseline) {
  const missing = baselineFields.filter((field) => !(field in baseline));
  const errors = [];

  if (missing.length) {
    errors.push(`missing baseline fields: ${missing.join(', ')}`);
  }

  if (!Number.isInteger(baseline.pointCount)) {
    errors.push('baseline pointCount must be an integer');
  }

  const status = normalizeStatus(baseline.status);
  if (!['locked', 'pending', 'unstable'].includes(status)) {
    errors.push('baseline status must be locked, pending, or unstable');
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

function canonicalTraceEntry(entry) {
  return String(entry || '')
    .trim()
    .replace(/([A-Z0-9_]+)\([^)]*\)(?=:)/g, '$1');
}

function traceEntryAliases(entry) {
  const canonical = canonicalTraceEntry(entry);
  const aliases = new Set([canonical]);

  if (/^[A-Z0-9_]+:retry_vision(?:_\d+)?$/i.test(canonical)) {
    aliases.add(canonical.replace(/:retry_vision(?:_\d+)?$/i, ':accepted'));
  }

  return [...aliases];
}

function traceContains(actualTrace, expectedTrace) {
  const canonicalActual = actualTrace.map(canonicalTraceEntry);
  const actualText = canonicalActual.join(' -> ');

  return expectedTrace.every((entry) => (
    traceEntryAliases(entry).some((alias) => actualText.includes(alias))
  ));
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

function normalizeCoordinateForCompare(value) {
  const raw = String(value || '').trim().replace(/^PROJECTED:/i, '').replace(/\s+/g, '');
  const parts = raw.split(',');

  if (parts.length >= 3 && Number(parts[2]) === 0) {
    parts.pop();
  }

  return parts.join(',');
}

function normalizeCoordinateListForHash(coordinates = []) {
  return coordinates.map(normalizeCoordinateForCompare).join('\n');
}

function hashCoordinateRows(coordinates = []) {
  return createHash('sha256')
    .update(normalizeCoordinateListForHash(coordinates))
    .digest('hex');
}

function inferGeometry(pointCount) {
  if (pointCount === 1) return 'Point';
  if (pointCount === 2) return 'LineString';
  if (pointCount >= 3) return 'Polygon';
  return '';
}

function coordinateEquals(actualValue, expectedValue) {
  const actual = normalizeCoordinateForCompare(actualValue);
  const expected = normalizeCoordinateForCompare(expectedValue);

  if (actual === expected) {
    return { matches: true, exact: actualValue === expectedValue };
  }

  const actualParts = actual.split(',').map(Number);
  const expectedParts = expected.split(',').map(Number);

  if (
    actualParts.length !== expectedParts.length
    || actualParts.some((part) => !Number.isFinite(part))
    || expectedParts.some((part) => !Number.isFinite(part))
  ) {
    return { matches: false, exact: false };
  }

  const matches = actualParts.every((part, index) => (
    Math.abs(part - expectedParts[index]) <= coordinateTolerance
  ));

  return { matches, exact: false };
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
    geometry: data?.geometry || inferGeometry(pointCount),
    firstCoordinate: coordinateRows[0] || '',
    lastCoordinate: coordinateRows[coordinateRows.length - 1] || '',
    coordinateRows,
    coordinateSummaryHash: hashCoordinateRows(coordinateRows),
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
      'x-regression-test': 'true',
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
    const error = new Error(`API error ${response.status}: ${data?.error || data?.message || text.slice(0, 200)}`);
    error.apiStatus = response.status;
    throw error;
  }

  return summarizeApiResponse(data);
}

function compareExpected(expected, actual) {
  const realFailures = [];
  const formatDifferences = [];

  if (actual.precisionMode !== expected.expectedPrecisionMode) {
    realFailures.push(`expected precisionMode: ${expected.expectedPrecisionMode}; actual: ${actual.precisionMode || '(empty)'}`);
  }

  if (!traceContains(actual.parserTrace, expected.expectedParserTrace)) {
    realFailures.push(`expected parserTrace contains: ${expected.expectedParserTrace.join(' -> ')}; actual: ${actual.parserTrace.join(' -> ') || '(empty)'}`);
  }

  if (actual.pointCount !== expected.expectedPointCount) {
    realFailures.push(`expected pointCount: ${expected.expectedPointCount}; actual: ${actual.pointCount}`);
  }

  const firstCoordinateCompare = coordinateEquals(actual.firstCoordinate, expected.expectedFirstKml);
  if (!firstCoordinateCompare.matches) {
    realFailures.push(`expected first KML: ${expected.expectedFirstKml}; actual: ${actual.firstCoordinate || '(empty)'}`);
  } else if (!firstCoordinateCompare.exact) {
    formatDifferences.push(`first KML normalized: expected ${expected.expectedFirstKml}; actual ${actual.firstCoordinate || '(empty)'}`);
  }

  const lastCoordinateCompare = coordinateEquals(actual.lastCoordinate, expected.expectedLastKml);
  if (!lastCoordinateCompare.matches) {
    realFailures.push(`expected last KML: ${expected.expectedLastKml}; actual: ${actual.lastCoordinate || '(empty)'}`);
  } else if (!lastCoordinateCompare.exact) {
    formatDifferences.push(`last KML normalized: expected ${expected.expectedLastKml}; actual ${actual.lastCoordinate || '(empty)'}`);
  }

  return { realFailures, formatDifferences };
}

function compareBaseline(baseline, actual) {
  const baselineChanged = [];
  const formatDifferences = [];

  if (actual.precisionMode !== baseline.precisionMode) {
    baselineChanged.push(`baseline precisionMode: ${baseline.precisionMode}; actual: ${actual.precisionMode || '(empty)'}`);
  }

  if (actual.pointCount !== baseline.pointCount) {
    baselineChanged.push(`baseline pointCount: ${baseline.pointCount}; actual: ${actual.pointCount}`);
  }

  if (actual.geometry !== baseline.geometry) {
    baselineChanged.push(`baseline geometry: ${baseline.geometry}; actual: ${actual.geometry || '(empty)'}`);
  }

  const parserTraceAlias = Array.isArray(baseline.parserTraceAlias)
    ? baseline.parserTraceAlias
    : String(baseline.parserTraceAlias || '')
      .split(/(?:->|\n|,)/)
      .map((entry) => entry.trim())
      .filter(Boolean);
  if (parserTraceAlias.length && !traceContains(actual.parserTrace, parserTraceAlias)) {
    baselineChanged.push(`baseline parserTraceAlias contains: ${parserTraceAlias.join(' -> ')}; actual: ${actual.parserTrace.join(' -> ') || '(empty)'}`);
  }

  const firstCoordinateCompare = coordinateEquals(actual.firstCoordinate, baseline.firstCoordinate);
  if (!firstCoordinateCompare.matches) {
    baselineChanged.push(`baseline firstCoordinate: ${baseline.firstCoordinate}; actual: ${actual.firstCoordinate || '(empty)'}`);
  } else if (!firstCoordinateCompare.exact) {
    formatDifferences.push(`first coordinate normalized: baseline ${baseline.firstCoordinate}; actual ${actual.firstCoordinate || '(empty)'}`);
  }

  const lastCoordinateCompare = coordinateEquals(actual.lastCoordinate, baseline.lastCoordinate);
  if (!lastCoordinateCompare.matches) {
    baselineChanged.push(`baseline lastCoordinate: ${baseline.lastCoordinate}; actual: ${actual.lastCoordinate || '(empty)'}`);
  } else if (!lastCoordinateCompare.exact) {
    formatDifferences.push(`last coordinate normalized: baseline ${baseline.lastCoordinate}; actual ${actual.lastCoordinate || '(empty)'}`);
  }

  const expectedCoordinateSummaryHash = String(baseline.coordinateSummaryHash || '').trim();
  if (expectedCoordinateSummaryHash && actual.coordinateSummaryHash !== expectedCoordinateSummaryHash) {
    baselineChanged.push(`baseline coordinateSummaryHash: ${expectedCoordinateSummaryHash}; actual: ${actual.coordinateSummaryHash}`);
  }

  const expectedKmlCoordinateHash = String(baseline.kmlCoordinateHash || '').trim();
  if (expectedKmlCoordinateHash && actual.coordinateSummaryHash !== expectedKmlCoordinateHash) {
    baselineChanged.push(`baseline kmlCoordinateHash: ${expectedKmlCoordinateHash}; actual: ${actual.coordinateSummaryHash}`);
  }

  return { baselineChanged, formatDifferences };
}

async function loadExpectedJson(sampleDir) {
  const expectedPath = path.join(samplesRoot, sampleDir, 'expected.json');
  const raw = await readFile(expectedPath, 'utf8');
  return JSON.parse(raw);
}

async function loadBaselineJson(sampleDir) {
  const baselinePath = path.join(samplesRoot, sampleDir, 'baseline.json');
  try {
    const raw = await readFile(baselinePath, 'utf8');
    return JSON.parse(raw);
  } catch (error) {
    if (error.code === 'ENOENT') {
      return null;
    }
    throw error;
  }
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
      const baseline = await loadBaselineJson(sampleDir);

      if (errors.length) {
        rows.push({
          directory: sampleDir,
          typeId: expected.typeId || '(missing typeId)',
          sampleName: expected.sampleName || '(missing sampleName)',
          result: 'REAL FAIL',
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

      if (!baseline) {
        if (status === 'unstable') {
          rows.push({
            directory: sampleDir,
            typeId: expected.typeId,
            sampleName: expected.sampleName,
            result: 'UNSTABLE',
            errors: ['baseline.json not found; unstable sample remains non-blocking'],
          });
          continue;
        }

        rows.push({
          directory: sampleDir,
          typeId: expected.typeId,
          sampleName: expected.sampleName,
          result: 'BASELINE_MISSING',
          errors: ['baseline.json not found; expected.json was used only as sample metadata'],
        });
        continue;
      }

      const baselineErrors = validateBaseline(baseline);
      if (baselineErrors.length) {
        rows.push({
          directory: sampleDir,
          typeId: baseline.typeId || expected.typeId,
          sampleName: expected.sampleName,
          result: 'REAL FAIL',
          errors: baselineErrors,
        });
        continue;
      }

      const baselineStatus = normalizeStatus(baseline.status);
      if (baselineStatus === 'pending') {
        rows.push({
          directory: sampleDir,
          typeId: baseline.typeId,
          sampleName: expected.sampleName,
          result: 'PENDING',
          errors: [],
        });
        continue;
      }

      let actual;
      let comparison = { realFailures: [], formatDifferences: [] };
      let baselineComparison = { baselineChanged: [], formatDifferences: [] };
      let apiBlocked = false;
      try {
        actual = await callRecognizeApi(expected);
        baselineComparison = compareBaseline(baseline, actual);
      } catch (error) {
        apiBlocked = error.apiStatus === 403;
        comparison.realFailures = [
          error.apiStatus === 403
            ? error.message
            : error.message === 'fetch failed'
            ? `API unavailable: ${apiUrl}`
            : error.message,
        ];
      }

      const result = apiBlocked
        ? 'API BLOCKED'
        : baselineStatus === 'unstable'
        ? 'UNSTABLE'
        : baselineComparison.baselineChanged.length
          ? 'BASELINE_CHANGED'
        : comparison.realFailures.length
          ? 'REAL FAIL'
          : baselineComparison.formatDifferences.length || comparison.formatDifferences.length
            ? 'FORMAT DIFFERENCE'
            : 'PASS';

      rows.push({
        directory: sampleDir,
        typeId: expected.typeId,
        sampleName: expected.sampleName,
        result,
        errors: [
          ...baselineComparison.baselineChanged,
          ...comparison.realFailures,
          ...baselineComparison.formatDifferences,
          ...comparison.formatDifferences,
        ],
        actual,
      });
    } catch (error) {
      rows.push({
        directory: sampleDir,
        typeId: '(missing expected.json)',
        sampleName: '(missing expected.json)',
        result: 'REAL FAIL',
        errors: [error.code === 'ENOENT' ? 'expected.json not found' : error.message],
      });
    }
  }

  const summary = rows.reduce(
    (acc, row) => {
      acc.total += 1;
      const key = row.result.toLowerCase().replace(/\s+/g, '_');
      acc[key] += 1;
      return acc;
    },
    {
      total: 0,
      pass: 0,
      real_fail: 0,
      format_difference: 0,
      api_blocked: 0,
      pending: 0,
      unstable: 0,
      skipped_text: 0,
      baseline_missing: 0,
      baseline_changed: 0,
    },
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
  console.log(`PASS: ${summary.pass}`);
  console.log(`REAL FAIL: ${summary.real_fail}`);
  console.log(`FORMAT DIFFERENCE: ${summary.format_difference}`);
  console.log(`API BLOCKED: ${summary.api_blocked}`);
  console.log(`PENDING: ${summary.pending}`);
  console.log(`UNSTABLE: ${summary.unstable}`);
  console.log(`SKIPPED_TEXT: ${summary.skipped_text}`);
  console.log(`BASELINE_MISSING: ${summary.baseline_missing}`);
  console.log(`BASELINE_CHANGED: ${summary.baseline_changed}`);

  if (summary.real_fail > 0 || summary.api_blocked > 0 || summary.baseline_changed > 0) {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
