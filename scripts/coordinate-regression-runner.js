import { access, readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { performance } from 'node:perf_hooks';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '..');
const defaultBaselinePath = path.join(repoRoot, 'COORDINATE_RECOGNITION_GOLDEN_BASELINE.json');
const defaultErrorLibraryPath = path.join(repoRoot, 'COORDINATE_ERROR_LIBRARY.json');
const defaultApiUrl = 'http://127.0.0.1:3000/api/recognize-coordinates';
const defaultTextApiUrl = 'http://127.0.0.1:3000/api/regression/parse-coordinate-text';
const apiUrl = process.env.COORDINATE_REGRESSION_API_URL || defaultApiUrl;
const textApiUrl = process.env.COORDINATE_REGRESSION_TEXT_API_URL || defaultTextApiUrl;
const coordinateTolerance = Number(process.env.COORDINATE_REGRESSION_TOLERANCE || '1e-6');
const fixtureRoot = process.env.COORDINATE_REGRESSION_FIXTURE_ROOT || 'D:\\关于西非的业务\\测试素材';

const knownLocalFixtureNames = {
  wgs84_table_rc2_congo_001: '刚果，两个坐标在同一张图.jpg',
  wgs84_table_timeout_rescue_001: '微信图片_20260503091216_182_19.jpg',
  bftm_burkina_002: '布基纳法索02.jpg',
  utm30_burkina_003: '布基纳法索03.png',
  mgrs_myanmar_001: '缅甸坐标.jpg',
  kyrgyz_gk_001: '吉尔吉斯斯坦矿地坐标.png',
  madagascar_cadastral_candidate_001: '马达加斯加坐标.png',
  mozambique_tete_001: '莫桑比克矿地.jpg',
  cote_divoire_single_01: '科特迪瓦01.png',
  cote_divoire_single_02: '科特迪瓦02.png',
  cote_divoire_single_03: '科特迪瓦03.png',
  cote_divoire_single_04: '科特迪瓦04.png',
  cote_divoire_multi_001: '科特迪瓦4个矿区坐标.jpg',
  handwritten_dms_001: '手写坐标.jpg',
  dms_grouped_two_areas_001: '两块矿地.jpg',
  point_az_dms_table_001: '微信图片_20260427122118_114_19.jpg',
  low_clarity_blurry_dms_001: '模糊坐标.jpg',
  oblique_dms_001: '斜拍坐标.jpg',
  long_coordinate_table_001: '长坐标.png',
  multi_group_two_plots_001: '两块矿地.jpg',
  low_clarity_blurry_001: '模糊坐标.jpg',
  oblique_photo_001: '斜拍坐标.jpg',
  small_text_long_table_001: '长坐标.png',
};

function parseArgs(argv) {
  const options = {
    baselinePath: defaultBaselinePath,
    errorLibraryPath: defaultErrorLibraryPath,
    sampleIds: new Set(),
    errorIds: new Set(),
    types: new Set(),
    statuses: new Set(),
    pathId: 'original',
    repeatOverride: null,
    full: false,
    gate: false,
    includeText: false,
    dryRun: false,
    maxSamples: null,
    list: false,
    listErrors: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const readValue = (name) => {
      const next = argv[index + 1];
      if (!next || next.startsWith('--')) {
        throw new Error(`${name} requires a value`);
      }
      index += 1;
      return next;
    };

    const addCsv = (target, value) => {
      String(value || '')
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean)
        .forEach((item) => target.add(item.toLowerCase()));
    };

    if (arg === '--baseline') {
      options.baselinePath = path.resolve(repoRoot, readValue('--baseline'));
    } else if (arg.startsWith('--baseline=')) {
      options.baselinePath = path.resolve(repoRoot, arg.slice('--baseline='.length));
    } else if (arg === '--error-library') {
      options.errorLibraryPath = path.resolve(repoRoot, readValue('--error-library'));
    } else if (arg.startsWith('--error-library=')) {
      options.errorLibraryPath = path.resolve(repoRoot, arg.slice('--error-library='.length));
    } else if (arg === '--sample') {
      addCsv(options.sampleIds, readValue('--sample'));
    } else if (arg.startsWith('--sample=')) {
      addCsv(options.sampleIds, arg.slice('--sample='.length));
    } else if (arg === '--error') {
      addCsv(options.errorIds, readValue('--error'));
    } else if (arg.startsWith('--error=')) {
      addCsv(options.errorIds, arg.slice('--error='.length));
    } else if (arg === '--type') {
      addCsv(options.types, readValue('--type'));
    } else if (arg.startsWith('--type=')) {
      addCsv(options.types, arg.slice('--type='.length));
    } else if (arg === '--status') {
      addCsv(options.statuses, readValue('--status'));
    } else if (arg.startsWith('--status=')) {
      addCsv(options.statuses, arg.slice('--status='.length));
    } else if (arg === '--path') {
      options.pathId = readValue('--path');
    } else if (arg.startsWith('--path=')) {
      options.pathId = arg.slice('--path='.length);
    } else if (arg === '--repeat') {
      options.repeatOverride = Number(readValue('--repeat'));
    } else if (arg.startsWith('--repeat=')) {
      options.repeatOverride = Number(arg.slice('--repeat='.length));
    } else if (arg === '--max-samples') {
      options.maxSamples = Number(readValue('--max-samples'));
    } else if (arg.startsWith('--max-samples=')) {
      options.maxSamples = Number(arg.slice('--max-samples='.length));
    } else if (arg === '--full') {
      options.full = true;
    } else if (arg === '--gate') {
      options.gate = true;
      options.full = true;
    } else if (arg === '--include-text') {
      options.includeText = true;
    } else if (arg === '--dry-run') {
      options.dryRun = true;
    } else if (arg === '--list') {
      options.list = true;
    } else if (arg === '--list-errors') {
      options.listErrors = true;
    } else if (arg === '--help' || arg === '-h') {
      printHelp();
      process.exit(0);
    } else {
      throw new Error(`Unknown argument: ${arg}`);
    }
  }

  if (options.repeatOverride !== null && (!Number.isInteger(options.repeatOverride) || options.repeatOverride < 1)) {
    throw new Error('--repeat must be a positive integer');
  }

  if (options.maxSamples !== null && (!Number.isInteger(options.maxSamples) || options.maxSamples < 1)) {
    throw new Error('--max-samples must be a positive integer');
  }

  if (options.gate) {
    options.includeText = true;
  }

  return options;
}

function printHelp() {
  console.log(`Coordinate Recognition Regression Runner

Usage:
  npm run regression -- [options]

Options:
  --dry-run                    Validate and list baseline samples without API calls
  --list                       List matching samples
  --sample <ids>               Comma-separated sample_id filter
  --type <types>               Comma-separated type filter
  --status <statuses>          Comma-separated baseline_status filter
  --path <id>                  Test path id from baseline, default: original
  --repeat <n>                 Override repeat count
  --full                       Use sample repeat_count values
  --gate                       Full blocking gate; fails incomplete blocking baselines
  --include-text               Also call text regression endpoint for text fixtures
  --max-samples <n>            Limit samples for smoke runs
  --baseline <file>            Baseline file, default: COORDINATE_RECOGNITION_GOLDEN_BASELINE.json
  --error-library <file>       Error library file, default: COORDINATE_ERROR_LIBRARY.json
  --list-errors                List historical error cases without API calls
  --error <error_ids>          Comma-separated error_id filter; runs linked regression samples

Examples:
  npm run regression -- --dry-run
  npm run regression -- --sample handwritten_dms_001 --repeat 1
  npm run regression -- --list-errors
  npm run regression -- --error CER-2026-07-11-002 --repeat 1
  npm run regression -- --type "Mozambique geographic table" --full
  npm run regression -- --gate
`);
}

async function readJson(filePath) {
  const raw = await readFile(filePath, 'utf8');
  return JSON.parse(raw);
}

function isWindowsAbsolutePath(value) {
  return /^[a-zA-Z]:[\\/]/.test(value);
}

function normalizePathSeparators(value) {
  return String(value || '').replace(/\//g, path.sep);
}

function getKnownFixturePath(sample) {
  const fileName = knownLocalFixtureNames[sample.sample_id];
  return fileName ? path.join(fixtureRoot, fileName) : '';
}

async function fileExists(filePath) {
  if (!filePath) return false;
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function resolveImageFixture(sample) {
  const candidates = [];

  const knownFixture = getKnownFixturePath(sample);
  if (knownFixture) candidates.push(knownFixture);

  if (sample.local_path) {
    const localPath = String(sample.local_path);
    if (path.isAbsolute(localPath) || isWindowsAbsolutePath(localPath)) {
      candidates.push(path.normalize(localPath));
    } else {
      candidates.push(path.resolve(repoRoot, normalizePathSeparators(localPath)));
    }
  }

  if (sample.fixture && /\.(png|jpe?g|webp)$/i.test(sample.fixture)) {
    candidates.push(path.resolve(repoRoot, normalizePathSeparators(sample.fixture)));
  }

  for (const candidate of candidates) {
    if (await fileExists(candidate)) {
      return candidate;
    }
  }

  return '';
}

function getMimeType(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === '.png') return 'image/png';
  if (ext === '.jpg' || ext === '.jpeg') return 'image/jpeg';
  if (ext === '.webp') return 'image/webp';
  return 'application/octet-stream';
}

function normalizeScalar(value) {
  if (value === undefined || value === null) return null;
  if (typeof value === 'string') return value.trim();
  return value;
}

function normalizeEnum(value) {
  return String(value || '').trim().toLowerCase();
}

function normalizeGeometry(value) {
  const normalized = normalizeEnum(value);
  if (normalized === 'linestring') return 'line';
  if (normalized === 'polygon') return 'polygon';
  if (normalized === 'point') return 'point';
  if (normalized === 'line') return 'line';
  return normalized;
}

function inferGeometry(pointCount) {
  if (pointCount === 1) return 'point';
  if (pointCount === 2) return 'line';
  if (pointCount >= 3) return 'polygon';
  return '';
}

function normalizeCoordinateForCompare(value) {
  const raw = String(value || '').trim().replace(/^PROJECTED:/i, '').replace(/\s+/g, '');
  const parts = raw.split(',');
  if (parts.length >= 3 && Number(parts[2]) === 0) parts.pop();
  return parts.join(',');
}

function coordinateEquals(actualValue, expectedValue) {
  if (expectedValue === null || expectedValue === undefined || expectedValue === '') {
    return { skipped: true, matches: true, exact: true };
  }

  const actual = normalizeCoordinateForCompare(actualValue);
  const expected = normalizeCoordinateForCompare(expectedValue);

  if (actual === expected) {
    return { matches: true, exact: true };
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

  return {
    matches: actualParts.every((part, index) => Math.abs(part - expectedParts[index]) <= coordinateTolerance),
    exact: false,
  };
}

function extractCoordinateLines(text) {
  return String(text || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .filter((line) => {
      if (/^(label|point|order|num|第\s*\d+\s*组|⚠)/i.test(line)) return false;
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
    if (bareKml) coordinates.push(bareKml[0].replace(/\s+/g, ''));
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

function pointToCoordinate(point) {
  if (!point || typeof point !== 'object') return '';

  const lon = Number(point.lon);
  const lat = Number(point.lat);
  if (Number.isFinite(lon) && Number.isFinite(lat)) {
    return `${lon},${lat},0`;
  }

  const x = Number(point.x);
  const y = Number(point.y);
  if (Number.isFinite(x) && Number.isFinite(y)) {
    return `${x},${y}`;
  }

  if (point.label && Number.isFinite(x)) {
    return `${point.label},${x}`;
  }

  return String(point.raw || '').trim();
}

function flattenV2Points(engine) {
  const groups = Array.isArray(engine?.groups) ? engine.groups : [];
  return groups.flatMap((group) => (Array.isArray(group.points) ? group.points : []));
}

function sumPointCount(groups, fallbackCount) {
  if (!Array.isArray(groups) || groups.length === 0) return fallbackCount;
  return groups.reduce((sum, group) => sum + (Array.isArray(group.points) ? group.points.length : 0), 0);
}

function getReviewGroupIndexes(groups) {
  if (!Array.isArray(groups)) return [];
  return groups
    .map((group, index) => (group?.requires_review ? index + 1 : null))
    .filter((value) => value !== null);
}

function getAllGroupsKmlReady(groups) {
  if (!Array.isArray(groups) || groups.length === 0) return null;
  return groups.every((group) => group?.kml_ready === true);
}

function getActualArea(engine) {
  const groups = Array.isArray(engine?.groups) ? engine.groups : [];
  const values = groups
    .map((group) => Number(group?.calculated_area_ha))
    .filter((value) => Number.isFinite(value) && value > 0);
  if (!values.length) return null;
  return values.reduce((sum, value) => sum + value, 0);
}

function getActualCountry(engine, data) {
  return normalizeScalar(
    engine?.country
    || engine?.region
    || engine?.spatial_knowledge_report?.top_country
    || engine?.spatial_knowledge_report?.country
    || data?.country
    || null,
  );
}

function isFallbackUsed(data, engine) {
  const haystack = [
    data?.precisionMode,
    data?.model,
    data?.warning,
    data?.message,
    ...(Array.isArray(data?.parserTrace) ? data.parserTrace : []),
  ].join(' ');
  return engine?.source?.fallback_used === true || /fallback|local-ocr|tesseract/i.test(haystack);
}

function getFallbackType(data) {
  const precisionMode = String(data?.precisionMode || '');
  if (/local-ocr|tesseract/i.test(precisionMode)) return 'local_ocr';
  if (/fallback/i.test(precisionMode)) return precisionMode;
  return '';
}

function isRetryUsed(data) {
  const haystack = [
    data?.model,
    data?.warning,
    data?.message,
    ...(Array.isArray(data?.parserTrace) ? data.parserTrace : []),
  ].join(' ');
  return /retry|rescue|handwrittenDmsRetry/i.test(haystack);
}

function isTimeout(data, text) {
  const haystack = [
    data?.code,
    data?.warning,
    data?.message,
    data?.error,
    text,
  ].join(' ');
  return /ALIYUN_TIMEOUT|TIMEOUT|超时/i.test(haystack);
}

function summarizeApiResponse(data, responseMeta = {}) {
  const engine = data?.coordinateEngineV2 || data?.coordinate_engine_v2 || {};
  const groups = Array.isArray(engine.groups) ? engine.groups : [];
  const coordinatesText = data?.coordinates || data?.formatted || data?.result || '';
  const kmlCoordinates = extractKmlCoordinates(coordinatesText);
  const fallbackRows = extractProjectedOrDecimalRows(coordinatesText);
  const textRows = kmlCoordinates.length ? kmlCoordinates : fallbackRows;
  const v2Rows = flattenV2Points(engine).map(pointToCoordinate).filter(Boolean);
  const coordinateRows = textRows.length ? textRows : v2Rows;
  const pointCount = sumPointCount(groups, Number.isInteger(data?.pointCount)
    ? data.pointCount
    : Array.isArray(data?.points)
      ? data.points.length
      : coordinateRows.length);
  const firstPoint = coordinateRows[0] || '';
  const lastPoint = coordinateRows[coordinateRows.length - 1] || '';
  const groupGeometries = groups.map((group) => normalizeGeometry(group?.geometry)).filter(Boolean);
  const uniqueGeometries = [...new Set(groupGeometries)];
  const geometry = uniqueGeometries.length === 1
    ? uniqueGeometries[0]
    : normalizeGeometry(data?.geometry || engine.geometry || inferGeometry(pointCount));
  const topRequiresReview = typeof engine.requires_review === 'boolean' ? engine.requires_review : null;
  const groupRequiresReview = groups.some((group) => group?.requires_review === true);
  const requiresReview = topRequiresReview ?? groupRequiresReview;
  const allGroupsKmlReady = getAllGroupsKmlReady(groups);

  return {
    httpStatus: responseMeta.httpStatus ?? null,
    durationMs: responseMeta.durationMs ?? null,
    model: data?.model || '',
    precisionMode: data?.precisionMode || '',
    v2PrecisionMode: engine.precision_mode || '',
    coordinateType: engine.coordinate_type || data?.coordinate_type || '',
    groupCount: groups.length || (pointCount > 0 ? 1 : 0),
    pointCount,
    geometry,
    requiresReview,
    kmlReady: typeof engine.kml_ready === 'boolean' ? engine.kml_ready : allGroupsKmlReady,
    firstPoint,
    lastPoint,
    reviewGroupIndexes: getReviewGroupIndexes(groups),
    country: getActualCountry(engine, data),
    areaHa: getActualArea(engine),
    orderStatus: engine.order_status || engine.coordinate_validation_report?.order_status || '',
    fallbackUsed: isFallbackUsed(data, engine),
    retryUsed: isRetryUsed(data),
    timeout: isTimeout(data, responseMeta.responseText || ''),
    fallbackType: getFallbackType(data),
    errorCode: data?.code || data?.error_code || '',
    raw: data,
  };
}

async function callImageApi(sample) {
  const inputFile = await resolveImageFixture(sample);
  if (!inputFile) {
    return {
      skipped: true,
      skipReason: 'fixture_not_found',
      error: `No local image fixture found for ${sample.sample_id}`,
    };
  }

  const fileBuffer = await readFile(inputFile);
  const visitorId = `coordinate-regression-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const form = new FormData();
  const blob = new Blob([fileBuffer], { type: getMimeType(inputFile) });
  form.append('image', blob, path.basename(inputFile));
  form.append('visitorId', visitorId);
  form.append('regressionSampleId', sample.sample_id);

  const startedAt = performance.now();
  const response = await fetch(apiUrl, {
    method: 'POST',
    body: form,
    headers: {
      'x-visitor-id': visitorId,
      'x-source': 'coordinate-regression-runner',
      'x-regression-test': 'true',
    },
  });
  const responseText = await response.text();
  const durationMs = Math.round(performance.now() - startedAt);

  let data;
  try {
    data = JSON.parse(responseText);
  } catch {
    return {
      httpStatus: response.status,
      durationMs,
      error: `API returned non-JSON response: ${responseText.slice(0, 240)}`,
    };
  }

  if (!response.ok) {
    return {
      httpStatus: response.status,
      durationMs,
      error: data?.error || data?.message || responseText.slice(0, 240),
      actual: summarizeApiResponse(data, { httpStatus: response.status, durationMs, responseText }),
    };
  }

  return summarizeApiResponse(data, { httpStatus: response.status, durationMs, responseText });
}

async function callTextApi(sample) {
  const visitorId = `coordinate-regression-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const fixture = await loadTextFixture(sample);
  if (!fixture) {
    return {
      skipped: true,
      skipReason: 'text_fixture_unavailable',
      error: `No text fixture found for ${sample.sample_id}`,
    };
  }

  const startedAt = performance.now();
  const response = await fetch(textApiUrl, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-visitor-id': visitorId,
      'x-source': 'coordinate-regression-runner',
      'x-regression-test': 'true',
    },
    body: JSON.stringify({ text: fixture, visitorId }),
  });
  const responseText = await response.text();
  const durationMs = Math.round(performance.now() - startedAt);

  let data;
  try {
    data = JSON.parse(responseText);
  } catch {
    return {
      httpStatus: response.status,
      durationMs,
      error: `Text API returned non-JSON response: ${responseText.slice(0, 240)}`,
    };
  }

  if (!response.ok) {
    return {
      httpStatus: response.status,
      durationMs,
      error: data?.error || data?.message || responseText.slice(0, 240),
      actual: summarizeApiResponse(data, { httpStatus: response.status, durationMs, responseText }),
    };
  }

  return summarizeApiResponse(data, { httpStatus: response.status, durationMs, responseText });
}

async function loadTextFixture(sample) {
  if (String(sample.fixture || '').startsWith('TEXT:')) {
    return String(sample.fixture).slice('TEXT:'.length);
  }

  const fixturePath = sample.fixture && sample.fixture.endsWith('.json')
    ? path.resolve(repoRoot, normalizePathSeparators(sample.fixture))
    : '';
  if (!fixturePath || !(await fileExists(fixturePath))) return '';

  const fixtureJson = await readJson(fixturePath);
  if (String(fixtureJson.inputFile || '').startsWith('TEXT:')) {
    return String(fixtureJson.inputFile).slice('TEXT:'.length);
  }
  return '';
}

function addFinding(findings, severity, field, expected, actual) {
  findings.push({ severity, field, expected, actual });
}

function compareField(diffs, field, expected, actual, options = {}) {
  const skipped = expected === undefined || expected === null || expected === '';
  if (skipped && options.skipWhenNull !== false) return;
  const severity = options.severity || 'BLOCKER';

  if (options.normalize === 'enum') {
    if (normalizeEnum(expected) !== normalizeEnum(actual)) {
      addFinding(diffs, severity, field, expected, actual || null);
    }
    return;
  }

  if (options.normalize === 'geometry') {
    if (normalizeGeometry(expected) !== normalizeGeometry(actual)) {
      addFinding(diffs, severity, field, expected, actual || null);
    }
    return;
  }

  if (options.normalize === 'boolean') {
    if (Boolean(expected) !== Boolean(actual)) {
      addFinding(diffs, severity, field, Boolean(expected), Boolean(actual));
    }
    return;
  }

  if (options.normalize === 'array') {
    const expectedArray = Array.isArray(expected) ? expected : [];
    const actualArray = Array.isArray(actual) ? actual : [];
    if (JSON.stringify(expectedArray) !== JSON.stringify(actualArray)) {
      addFinding(diffs, severity, field, expectedArray, actualArray);
    }
    return;
  }

  if (expected !== actual) {
    addFinding(diffs, severity, field, expected, actual);
  }
}

function compareGolden(sample, actual) {
  const diffs = [];
  const warnings = [];

  if (!actual || actual.error) {
    addFinding(diffs, 'BLOCKER', 'api', 'successful JSON response', actual?.error || 'no response');
    return { diffs, warnings };
  }

  compareField(diffs, 'coordinate_type', sample.expected_coordinate_type, actual.coordinateType, { normalize: 'enum' });
  compareField(diffs, 'v2_precision_mode', sample.expected_v2_precision_mode, actual.v2PrecisionMode, { severity: 'BLOCKER' });
  compareField(
    diffs,
    'v1_precisionMode',
    sample.expected_v1_precision_mode,
    actual.precisionMode,
    { severity: sample.v1_precision_mode_blocker === true ? 'BLOCKER' : 'WARNING' },
  );
  compareField(diffs, 'groupCount', sample.expected_group_count, actual.groupCount);
  compareField(diffs, 'pointCount', sample.expected_point_count, actual.pointCount);
  compareField(diffs, 'geometry', sample.expected_geometry, actual.geometry, { normalize: 'geometry' });
  compareField(diffs, 'requires_review', sample.expected_requires_review, actual.requiresReview, { normalize: 'boolean' });
  compareField(diffs, 'kml_ready', sample.expected_kml_ready, actual.kmlReady, { normalize: 'boolean' });
  compareField(diffs, 'reviewGroupIndexes', sample.expected_review_group_indexes, actual.reviewGroupIndexes, { normalize: 'array' });
  compareField(diffs, 'country', sample.expected_country, actual.country, { severity: 'WARNING' });

  const firstCompare = coordinateEquals(actual.firstPoint, sample.expected_first_point);
  if (!firstCompare.matches) {
    addFinding(diffs, 'BLOCKER', 'firstPoint', sample.expected_first_point, actual.firstPoint || null);
  } else if (!firstCompare.exact && !firstCompare.skipped) {
    warnings.push({ severity: 'INFO', field: 'firstPoint', message: `matches within tolerance; actual=${actual.firstPoint}` });
  }

  const lastCompare = coordinateEquals(actual.lastPoint, sample.expected_last_point);
  if (!lastCompare.matches) {
    addFinding(diffs, 'BLOCKER', 'lastPoint', sample.expected_last_point, actual.lastPoint || null);
  } else if (!lastCompare.exact && !lastCompare.skipped) {
    warnings.push({ severity: 'INFO', field: 'lastPoint', message: `matches within tolerance; actual=${actual.lastPoint}` });
  }

  const expectedArea = sample.expected_area_range;
  if (expectedArea && typeof expectedArea === 'object') {
    if (!Number.isFinite(actual.areaHa)) {
      addFinding(diffs, 'WARNING', 'area', expectedArea, null);
    } else if (
      Number.isFinite(Number(expectedArea.min_ha))
      && actual.areaHa < Number(expectedArea.min_ha)
    ) {
      addFinding(diffs, 'WARNING', 'area', `>= ${expectedArea.min_ha}`, actual.areaHa);
    } else if (
      Number.isFinite(Number(expectedArea.max_ha))
      && actual.areaHa > Number(expectedArea.max_ha)
    ) {
      addFinding(diffs, 'WARNING', 'area', `<= ${expectedArea.max_ha}`, actual.areaHa);
    } else if (
      Number.isFinite(Number(expectedArea.declared_area_ha))
      && Number.isFinite(Number(expectedArea.calculated_error_percent_max))
    ) {
      const declared = Number(expectedArea.declared_area_ha);
      const errorPercent = Math.abs(actual.areaHa - declared) / declared * 100;
      if (errorPercent > Number(expectedArea.calculated_error_percent_max)) {
        addFinding(
          diffs,
          'WARNING',
          'area',
          `declared ${declared} ha ±${expectedArea.calculated_error_percent_max}%`,
          `${actual.areaHa} ha (${errorPercent.toFixed(2)}% error)`,
        );
      }
    }
  }

  const forbiddenPrecisionModes = Array.isArray(sample.forbidden_precision_modes)
    ? sample.forbidden_precision_modes
    : [];
  if (forbiddenPrecisionModes.includes(actual.v2PrecisionMode)) {
    addFinding(diffs, 'BLOCKER', 'forbidden_v2_precision_mode', `not ${actual.v2PrecisionMode}`, actual.v2PrecisionMode);
  }
  if (forbiddenPrecisionModes.includes(actual.precisionMode)) {
    addFinding(
      diffs,
      sample.v1_precision_mode_blocker === true ? 'BLOCKER' : 'WARNING',
      'forbidden_v1_precisionMode',
      `not ${actual.precisionMode}`,
      actual.precisionMode,
    );
  }

  if (forbiddenPrecisionModes.includes('local-ocr-dms-fallback') && actual.fallbackUsed && /local-ocr|fallback/i.test(actual.precisionMode)) {
    addFinding(diffs, 'BLOCKER', 'fallback_takeover', 'no local OCR final result', actual.precisionMode);
  }

  return { diffs, warnings };
}

function getWorstSampleStatus(diffs, warnings) {
  if (diffs.some((diff) => diff.severity === 'BLOCKER')) return 'FAIL';
  if (
    diffs.some((diff) => diff.severity === 'WARNING')
    || warnings.some((warning) => warning.severity === 'WARNING')
  ) {
    return 'PASS WITH WARNING';
  }
  return 'PASS';
}

function validateBaseline(baseline) {
  const errors = [];
  if (baseline.schema_version !== 'coordinate_recognition_golden_baseline_v1') {
    errors.push(`unsupported schema_version: ${baseline.schema_version || '(missing)'}`);
  }
  if (!Array.isArray(baseline.samples)) {
    errors.push('samples must be an array');
    return errors;
  }

  const ids = new Set();
  for (const [index, sample] of baseline.samples.entries()) {
    const prefix = `samples[${index}]`;
    if (!sample.sample_id) errors.push(`${prefix}.sample_id is required`);
    if (sample.sample_id && ids.has(sample.sample_id)) errors.push(`${prefix}.sample_id duplicates ${sample.sample_id}`);
    ids.add(sample.sample_id);
    if (!sample.type) errors.push(`${prefix}.type is required`);
    if (!['image', 'text'].includes(String(sample.input_type || '').toLowerCase())) {
      errors.push(`${prefix}.input_type must be image or text`);
    }
    if (!sample.baseline_status) errors.push(`${prefix}.baseline_status is required`);
    if ('expected_precision_mode' in sample) {
      errors.push(`${prefix}.expected_precision_mode is deprecated; use expected_v1_precision_mode and expected_v2_precision_mode`);
    }
    if (!('expected_v1_precision_mode' in sample)) {
      errors.push(`${prefix}.expected_v1_precision_mode is required`);
    }
    if (!('expected_v2_precision_mode' in sample)) {
      errors.push(`${prefix}.expected_v2_precision_mode is required`);
    }
    if (!Number.isInteger(sample.expected_point_count) && sample.expected_point_count !== null) {
      errors.push(`${prefix}.expected_point_count must be integer or null`);
    }
    if (!Number.isInteger(sample.expected_group_count) && sample.expected_group_count !== null) {
      errors.push(`${prefix}.expected_group_count must be integer or null`);
    }
  }
  return errors;
}

function validateErrorLibrary(errorLibrary) {
  const errors = [];
  if (errorLibrary.schema_version !== 'coordinate_error_library_v1') {
    errors.push(`unsupported error library schema_version: ${errorLibrary.schema_version || '(missing)'}`);
  }
  if (!Array.isArray(errorLibrary.errors)) {
    errors.push('error library errors must be an array');
    return errors;
  }

  const requiredFields = Array.isArray(errorLibrary.required_fields)
    ? errorLibrary.required_fields
    : [
      'error_id',
      'sample_id',
      'date',
      'coordinate_type',
      'image',
      'expected',
      'actual',
      'root_cause',
      'fix_commit',
      'status',
      'regression_case',
    ];
  const ids = new Set();
  for (const [index, entry] of errorLibrary.errors.entries()) {
    const prefix = `errors[${index}]`;
    for (const field of requiredFields) {
      if (!(field in entry)) errors.push(`${prefix}.${field} is required`);
    }
    if (entry.error_id && ids.has(entry.error_id)) errors.push(`${prefix}.error_id duplicates ${entry.error_id}`);
    if (entry.error_id) ids.add(entry.error_id);
    if (entry.status && !['fixed', 'open', 'open_architecture', 'prevented_not_adopted'].includes(String(entry.status))) {
      errors.push(`${prefix}.status is unsupported: ${entry.status}`);
    }
  }
  return errors;
}

function getErrorLibraryStats(errorLibrary) {
  const stats = {
    total: Array.isArray(errorLibrary?.errors) ? errorLibrary.errors.length : 0,
    fixed: 0,
    open: 0,
    prevented_not_adopted: 0,
    open_architecture: 0,
  };
  for (const entry of errorLibrary.errors || []) {
    const status = String(entry.status || 'open');
    stats[status] = (stats[status] || 0) + 1;
  }
  return stats;
}

function printErrorLibraryStats(errorLibrary) {
  const stats = getErrorLibraryStats(errorLibrary);
  console.log(`Error Library: total=${stats.total} fixed=${stats.fixed || 0} open=${stats.open || 0} prevented_not_adopted=${stats.prevented_not_adopted || 0} open_architecture=${stats.open_architecture || 0}`);
}

function printErrorList(errorLibrary, selectedErrorIds = new Set()) {
  printErrorLibraryStats(errorLibrary);
  const errors = (errorLibrary.errors || [])
    .filter((entry) => !selectedErrorIds.size || selectedErrorIds.has(String(entry.error_id).toLowerCase()));
  for (const entry of errors) {
    const linkedSample = entry.regression_case?.baseline_sample_id || entry.sample_id || '(none)';
    console.log(`- ${entry.error_id} | ${entry.status} | sample=${linkedSample} | type=${entry.coordinate_type || '(unknown)'} | fix=${entry.fix_commit || '(none)'}`);
    console.log(`  root_cause: ${entry.root_cause || '(missing)'}`);
  }
}

function sampleMatchesFilters(sample, options) {
  if (options.sampleIds.size && !options.sampleIds.has(String(sample.sample_id).toLowerCase())) return false;
  if (options.types.size && !options.types.has(String(sample.type).toLowerCase())) return false;
  if (options.statuses.size && !options.statuses.has(String(sample.baseline_status).toLowerCase())) return false;
  return true;
}

function getErrorLinkedSampleId(errorEntry) {
  return errorEntry?.regression_case?.baseline_sample_id || errorEntry?.sample_id || '';
}

function getErrorsForSample(errorLibrary, sample, options) {
  const sampleId = String(sample.sample_id || '').toLowerCase();
  return (errorLibrary.errors || []).filter((entry) => {
    if (options.errorIds.size && !options.errorIds.has(String(entry.error_id).toLowerCase())) {
      return false;
    }
    return String(getErrorLinkedSampleId(entry)).toLowerCase() === sampleId;
  });
}

function normalizeComparable(value) {
  if (Array.isArray(value)) return value.map(normalizeComparable);
  if (value === undefined || value === null) return null;
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value;
  return String(value).trim();
}

function getActualValueForErrorField(actual, field) {
  const raw = actual?.raw || {};
  switch (field) {
    case 'coordinate_type':
      return actual.coordinateType;
    case 'precisionMode':
      return actual.precisionMode;
    case 'v1_precisionMode':
      return actual.precisionMode;
    case 'v2_precision_mode':
      return actual.v2PrecisionMode;
    case 'pointCount':
      return actual.pointCount;
    case 'groupCount':
      return actual.groupCount;
    case 'requires_review':
      return actual.requiresReview;
    case 'kml_ready':
      return actual.kmlReady;
    case 'fallback_used':
      return actual.fallbackUsed;
    case 'fallback_type':
      return actual.fallbackType;
    case 'error_code':
      return actual.errorCode;
    case 'firstPoint':
      return actual.firstPoint;
    case 'lastPoint':
      return actual.lastPoint;
    case 'order_status':
      return actual.orderStatus;
    case 'reviewGroupIndexes':
      return actual.reviewGroupIndexes;
    case 'error_message_contains':
      return [actual.error, raw.error, raw.message, raw.warning].filter(Boolean).join(' ');
    default:
      return actual[field] ?? raw[field] ?? null;
  }
}

function conditionMatches(actual, field, expected) {
  const actualValue = getActualValueForErrorField(actual, field);
  if (field === 'error_message_contains') {
    return String(actualValue || '').includes(String(expected || ''));
  }
  if (Array.isArray(expected)) {
    return JSON.stringify(normalizeComparable(actualValue)) === JSON.stringify(normalizeComparable(expected));
  }
  return normalizeComparable(actualValue) === normalizeComparable(expected);
}

function errorEntryMatchesActual(errorEntry, actual) {
  if (!actual || actual.skipped) return false;
  const conditions = errorEntry?.actual?.match_conditions;
  if (!conditions || typeof conditions !== 'object' || Array.isArray(conditions)) {
    return false;
  }
  const entries = Object.entries(conditions);
  if (!entries.length) return false;
  return entries.every(([field, expected]) => conditionMatches(actual, field, expected));
}

function getHistoricalErrorSeverity(errorEntry) {
  if (errorEntry.severity) return String(errorEntry.severity).toUpperCase();
  const status = String(errorEntry.status || '');
  if (status === 'open_architecture') return 'WARNING';
  return 'BLOCKER';
}

function getHistoricalErrorCode(errorEntry) {
  const status = String(errorEntry.status || '');
  if (status === 'fixed') return 'HISTORICAL_ERROR_REGRESSION';
  if (status === 'prevented_not_adopted') return 'HISTORICAL_ERROR_REGRESSION';
  if (status === 'open') return 'KNOWN_OPEN_ERROR';
  if (status === 'open_architecture') return 'KNOWN_OPEN_ARCHITECTURE_ERROR';
  return 'KNOWN_ERROR';
}

function buildHistoricalFinding(errorEntry, actual) {
  const severity = getHistoricalErrorSeverity(errorEntry);
  return {
    severity,
    code: getHistoricalErrorCode(errorEntry),
    error_id: errorEntry.error_id,
    sample_id: errorEntry.sample_id,
    coordinate_type: errorEntry.coordinate_type,
    status: errorEntry.status,
    root_cause: errorEntry.root_cause,
    fix_commit: errorEntry.fix_commit,
    expected: errorEntry.expected,
    actual: {
      precisionMode: actual.precisionMode,
      coordinate_type: actual.coordinateType,
      pointCount: actual.pointCount,
      groupCount: actual.groupCount,
      requires_review: actual.requiresReview,
      kml_ready: actual.kmlReady,
      fallback_used: actual.fallbackUsed,
      error_code: actual.errorCode,
    },
    message: errorEntry.regression_case?.runner_expectation || '',
  };
}

function getRepeatCount(sample, options) {
  if (options.repeatOverride !== null) return options.repeatOverride;
  if (options.full) return Number.isInteger(sample.repeat_count) && sample.repeat_count > 0 ? sample.repeat_count : 1;
  return 1;
}

function isBlockingSample(sample, options) {
  if (!options.gate) return false;
  return ['locked', 'locked_experimental'].includes(String(sample.baseline_status || '').toLowerCase());
}

async function runSample(sample, options, errorLibrary) {
  const repeatCount = getRepeatCount(sample, options);
  const runs = [];
  const sampleErrors = getErrorsForSample(errorLibrary, sample, options);

  if (options.pathId !== 'original') {
    return {
      sample,
      status: 'SKIP',
      historicalStatus: sampleErrors.length ? 'NOT CHECKED' : 'NOT APPLICABLE',
      historicalFindings: [],
      runs: [{
        skipped: true,
        skipReason: 'unsupported_test_path',
        error: `Runner currently executes real original uploads only. Requested path=${options.pathId}.`,
      }],
      diffs: [],
      warnings: [],
    };
  }

  if (sample.input_type === 'text' && !options.includeText) {
    return {
      sample,
      status: 'SKIP',
      historicalStatus: sampleErrors.length ? 'NOT CHECKED' : 'NOT APPLICABLE',
      historicalFindings: [],
      runs: [{
        skipped: true,
        skipReason: 'text_fixture_skipped',
        error: 'Text fixtures are skipped unless --include-text is supplied.',
      }],
      diffs: [],
      warnings: [],
    };
  }

  const allDiffs = [];
  const allWarnings = [];
  const historicalFindings = [];

  for (let runIndex = 0; runIndex < repeatCount; runIndex += 1) {
    const actual = sample.input_type === 'text'
      ? await callTextApi(sample)
      : await callImageApi(sample);
    const { diffs, warnings } = compareGolden(sample, actual.actual || actual);
    runs.push({ actual, diffs, warnings });
    diffs.forEach((diff) => allDiffs.push({ run: runIndex + 1, ...diff }));
    warnings.forEach((warning) => allWarnings.push({ run: runIndex + 1, ...warning }));

    const actualResult = actual.actual || actual;
    for (const errorEntry of sampleErrors) {
      if (errorEntryMatchesActual(errorEntry, actualResult)) {
        const existing = historicalFindings.some((finding) => (
          finding.error_id === errorEntry.error_id
        ));
        if (!existing) {
          historicalFindings.push({ run: runIndex + 1, ...buildHistoricalFinding(errorEntry, actualResult) });
        }
      }
    }
  }

  const statusFromBaseline = getWorstSampleStatus(allDiffs, allWarnings);
  const historicalWorst = historicalFindings.some((finding) => finding.severity === 'BLOCKER')
    ? 'FAIL'
    : historicalFindings.some((finding) => finding.severity === 'WARNING')
      ? 'PASS WITH WARNING'
      : 'PASS';
  const status = statusFromBaseline === 'FAIL' || historicalWorst === 'FAIL'
    ? 'FAIL'
    : statusFromBaseline === 'PASS WITH WARNING' || historicalWorst === 'PASS WITH WARNING'
      ? 'PASS WITH WARNING'
      : 'PASS';
  const historicalStatus = historicalFindings.length
    ? historicalFindings.some((finding) => finding.code === 'HISTORICAL_ERROR_REGRESSION')
      ? 'REGRESSION'
      : historicalFindings.some((finding) => finding.code === 'KNOWN_OPEN_ERROR')
        ? 'KNOWN OPEN'
        : historicalFindings.some((finding) => finding.code === 'KNOWN_OPEN_ARCHITECTURE_ERROR')
          ? 'KNOWN OPEN'
          : 'REGRESSION'
    : sampleErrors.length
      ? 'PASS'
      : 'NOT APPLICABLE';

  return {
    sample,
    status,
    historicalStatus,
    historicalFindings,
    runs,
    diffs: allDiffs,
    warnings: allWarnings,
  };
}

function printSampleList(samples) {
  console.log('Coordinate Recognition Golden Baseline Samples');
  for (const sample of samples) {
    console.log(`- ${sample.sample_id} | ${sample.type} | ${sample.input_type} | ${sample.baseline_status} | repeat=${sample.repeat_count || 1}`);
  }
}

function formatActualSummary(actual) {
  if (!actual) return 'no actual result';
  if (actual.actual) return formatActualSummary(actual.actual);
  if (actual.skipped) return `skipped: ${actual.skipReason}`;
  if (actual.error) return `error: ${actual.error}`;
  return [
    `http=${actual.httpStatus}`,
    `duration=${actual.durationMs}ms`,
    `v1_precisionMode=${actual.precisionMode || '(empty)'}`,
    `v2_precision_mode=${actual.v2PrecisionMode || '(empty)'}`,
    `coordinate_type=${actual.coordinateType || '(empty)'}`,
    `groups=${actual.groupCount}`,
    `points=${actual.pointCount}`,
    `geometry=${actual.geometry || '(empty)'}`,
    `requires_review=${actual.requiresReview}`,
    `kml_ready=${actual.kmlReady}`,
    `fallback=${actual.fallbackUsed}`,
    `timeout=${actual.timeout}`,
  ].join(' | ');
}

function printResult(result) {
  const { sample } = result;
  console.log(`\n[${result.status}] ${sample.sample_id} (${sample.type})`);

  if (result.status === 'SKIP') {
    console.log(`  ${result.runs[0]?.error || result.runs[0]?.skipReason}`);
    return;
  }

  result.runs.forEach((run, index) => {
    console.log(`  run ${index + 1}: ${formatActualSummary(run.actual)}`);
  });

  const v1Findings = result.diffs.filter((diff) => diff.field === 'v1_precisionMode');
  const v2Findings = result.diffs.filter((diff) => (
    diff.field === 'coordinate_type' || diff.field === 'v2_precision_mode'
  ));
  const v1Result = v1Findings.some((finding) => finding.severity === 'BLOCKER')
    ? 'FAIL'
    : v1Findings.length
      ? 'WARNING'
      : 'PASS';
  const v2Result = v2Findings.some((finding) => finding.severity === 'BLOCKER')
    ? 'FAIL'
    : 'PASS';
  console.log(`  V1 Legacy Result: ${v1Result}`);
  console.log(`  V2 Engine Result: ${v2Result}`);

  if (result.diffs.length) {
    console.log('  Diff:');
    for (const diff of result.diffs) {
      console.log(`    [${diff.severity || 'BLOCKER'}] run ${diff.run} ${diff.field}: expected ${JSON.stringify(diff.expected)}; actual ${JSON.stringify(diff.actual)}`);
    }
  }

  console.log(`  Historical Error Check: ${result.historicalStatus || 'NOT APPLICABLE'}`);
  if (result.historicalFindings?.length) {
    for (const finding of result.historicalFindings) {
      console.log(`    [${finding.severity}] ${finding.code}`);
      console.log(`      error_id: ${finding.error_id}`);
      console.log(`      sample_id: ${finding.sample_id}`);
      console.log(`      root_cause: ${finding.root_cause || '(missing)'}`);
      console.log(`      fix_commit: ${finding.fix_commit || '(none)'}`);
      console.log(`      expected: ${JSON.stringify(finding.expected)}`);
      console.log(`      actual: ${JSON.stringify(finding.actual)}`);
    }
  }

  if (result.warnings.length) {
    console.log('  Notes:');
    for (const warning of result.warnings) {
      console.log(`    [${warning.severity || 'INFO'}] run ${warning.run} ${warning.field}: ${warning.message}`);
    }
  }
}

function summarizeResults(results) {
  const summary = {
    pass: results.filter((result) => result.status === 'PASS').length,
    passWithWarning: results.filter((result) => result.status === 'PASS WITH WARNING').length,
    fail: results.filter((result) => result.status === 'FAIL').length,
    skipped: results.filter((result) => result.status === 'SKIP').length,
    regression: 0,
    historicalRegressions: 0,
    knownOpenErrors: 0,
    blockerCount: 0,
    warningCount: 0,
    infoCount: 0,
    durations: [],
    timeoutCount: 0,
    fallbackCount: 0,
  };

  for (const result of results) {
    const baselineStatus = String(result.sample.baseline_status || '').toLowerCase();
    if (result.status === 'FAIL' && ['locked', 'locked_experimental', 'unstable'].includes(baselineStatus)) {
      summary.regression += 1;
    }
    for (const finding of result.historicalFindings || []) {
      if (finding.code === 'HISTORICAL_ERROR_REGRESSION') summary.historicalRegressions += 1;
      if (finding.code === 'KNOWN_OPEN_ERROR' || finding.code === 'KNOWN_OPEN_ARCHITECTURE_ERROR') summary.knownOpenErrors += 1;
      if (finding.severity === 'BLOCKER') summary.blockerCount += 1;
      else if (finding.severity === 'WARNING') summary.warningCount += 1;
      else summary.infoCount += 1;
    }
    for (const diff of result.diffs) {
      if (diff.severity === 'BLOCKER') summary.blockerCount += 1;
      else if (diff.severity === 'WARNING') summary.warningCount += 1;
      else summary.infoCount += 1;
    }
    for (const warning of result.warnings) {
      if (warning.severity === 'WARNING') summary.warningCount += 1;
      else summary.infoCount += 1;
    }
    for (const run of result.runs) {
      const actual = run.actual?.actual || run.actual;
      if (Number.isFinite(actual?.durationMs)) summary.durations.push(actual.durationMs);
      if (actual?.timeout) summary.timeoutCount += 1;
      if (actual?.fallbackUsed) summary.fallbackCount += 1;
    }
  }

  summary.avgMs = summary.durations.length
    ? Math.round(summary.durations.reduce((sum, value) => sum + value, 0) / summary.durations.length)
    : null;
  summary.maxMs = summary.durations.length ? Math.max(...summary.durations) : null;
  summary.minMs = summary.durations.length ? Math.min(...summary.durations) : null;
  summary.gateResult = summary.fail > 0 || summary.regression > 0
    ? 'FAIL'
    : summary.passWithWarning > 0 || summary.warningCount > 0
      ? 'PASS WITH WARNING'
      : 'PASS';

  return summary;
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  let errorLibrary;
  try {
    errorLibrary = await readJson(options.errorLibraryPath);
  } catch (error) {
    console.error(`Error Library load failed: ${options.errorLibraryPath}`);
    console.error(error.message || String(error));
    process.exitCode = 1;
    return;
  }
  const errorLibraryErrors = validateErrorLibrary(errorLibrary);
  if (errorLibraryErrors.length) {
    console.error('Error Library validation failed:');
    errorLibraryErrors.forEach((error) => console.error(`- ${error}`));
    process.exitCode = 1;
    return;
  }

  if (options.listErrors) {
    printErrorList(errorLibrary, options.errorIds);
    return;
  }

  const baseline = await readJson(options.baselinePath);
  const baselineErrors = validateBaseline(baseline);
  if (baselineErrors.length) {
    console.error('Golden baseline validation failed:');
    baselineErrors.forEach((error) => console.error(`- ${error}`));
    process.exitCode = 1;
    return;
  }

  if (options.errorIds.size) {
    const linkedSampleIds = new Set(
      (errorLibrary.errors || [])
        .filter((entry) => options.errorIds.has(String(entry.error_id).toLowerCase()))
        .map((entry) => String(getErrorLinkedSampleId(entry) || '').toLowerCase())
        .filter(Boolean),
    );
    if (!linkedSampleIds.size) {
      console.error(`No runnable baseline sample is linked to error id(s): ${[...options.errorIds].join(', ')}`);
      process.exitCode = 1;
      return;
    }
    linkedSampleIds.forEach((sampleId) => options.sampleIds.add(sampleId));
  }

  let samples = baseline.samples.filter((sample) => sampleMatchesFilters(sample, options));
  if (options.maxSamples !== null) samples = samples.slice(0, options.maxSamples);

  if (options.list || options.dryRun) {
    printSampleList(samples);
    console.log(`\nBaseline OK: ${baseline.samples.length} total samples; ${samples.length} selected.`);
    if (options.list || options.dryRun) return;
  }

  if (!samples.length) {
    console.error('No samples selected.');
    process.exitCode = 1;
    return;
  }

  console.log('Coordinate Recognition Regression Report');
  console.log(`Baseline: ${path.relative(repoRoot, options.baselinePath)}`);
  console.log(`Error Library: ${path.relative(repoRoot, options.errorLibraryPath)}`);
  printErrorLibraryStats(errorLibrary);
  console.log(`API: ${apiUrl}`);
  console.log(`Path: ${options.pathId}`);
  console.log(`Mode: ${options.gate ? 'gate' : options.full ? 'full' : 'smoke'}`);
  console.log(`Samples: ${samples.length}`);

  const results = [];
  for (const sample of samples) {
    const result = await runSample(sample, options, errorLibrary);

    if (result.status === 'SKIP' && isBlockingSample(sample, options)) {
      result.status = 'FAIL';
      result.historicalStatus = result.historicalStatus || 'NOT CHECKED';
      result.diffs.push({
        severity: 'BLOCKER',
        run: 1,
        field: 'gate',
        expected: 'blocking sample executed',
        actual: result.runs[0]?.skipReason || 'skipped',
      });
    }

    results.push(result);
    printResult(result);
  }

  const summary = summarizeResults(results);
  console.log('\nRegression Summary');
  console.log(`Gate Result: ${summary.gateResult}`);
  console.log(`PASS: ${summary.pass}`);
  console.log(`PASS WITH WARNING: ${summary.passWithWarning}`);
  console.log(`FAIL: ${summary.fail}`);
  console.log(`SKIP: ${summary.skipped}`);
  console.log(`Regression: ${summary.regression}`);
  console.log(`Historical error regressions: ${summary.historicalRegressions}`);
  console.log(`Known open errors: ${summary.knownOpenErrors}`);
  console.log(`BLOCKER: ${summary.blockerCount}`);
  console.log(`WARNING: ${summary.warningCount}`);
  console.log(`INFO: ${summary.infoCount}`);
  console.log(`Timeout: ${summary.timeoutCount}`);
  console.log(`Fallback: ${summary.fallbackCount}`);
  console.log(`[INFO] Duration: avg=${summary.avgMs ?? 'n/a'}ms min=${summary.minMs ?? 'n/a'}ms max=${summary.maxMs ?? 'n/a'}ms`);

  if (summary.gateResult === 'FAIL') {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error.stack || error.message || String(error));
  process.exitCode = 1;
});
