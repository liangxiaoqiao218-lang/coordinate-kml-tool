import { access, mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { performance } from 'node:perf_hooks';
import {
  applyGoldenGovernance,
  classifyGoldenRun,
  summarizeReleaseSemantics,
  summarizeSemanticRuns,
  validateGoldenGovernance,
} from '../release-governance/runner-semantics.js';
import { validateReleaseEvidenceBinding } from '../release-governance/evidence-binding.js';
import { validateLocalPatchCandidateIdentity } from './local-patch-candidate-identity.js';
import {
  assertP0ReplayRuntimeSafety,
  isLoopbackUrl,
  loadP0ReleaseGateGovernance,
  loadP0ReplayManifest,
  validateP0ReplayFixture,
} from './p0-deterministic-replay.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, '..');
const defaultBaselinePath = path.join(repoRoot, 'COORDINATE_RECOGNITION_GOLDEN_BASELINE.json');
const defaultErrorLibraryPath = path.join(repoRoot, 'COORDINATE_ERROR_LIBRARY.json');
const defaultGovernancePath = path.join(repoRoot, 'release-governance', 'sr08d5-golden-policy.json');
const defaultEvidenceArtifactPath = path.join(repoRoot, 'release-evidence', 'coordinate-regression-runner-latest.json');
const defaultApiUrl = 'http://127.0.0.1:3000/api/recognize-coordinates';
const defaultTextApiUrl = 'http://127.0.0.1:3000/api/regression/parse-coordinate-text';
const apiUrl = process.env.COORDINATE_REGRESSION_API_URL || defaultApiUrl;
const textApiUrl = process.env.COORDINATE_REGRESSION_TEXT_API_URL || defaultTextApiUrl;
const apiOrigin = new URL(apiUrl).origin;
const versionApiUrl = `${apiOrigin}/api/version`;
const traceApiBaseUrl = `${apiOrigin}/api/regression/recognition-trace`;
const coordinateTolerance = Number(process.env.COORDINATE_REGRESSION_TOLERANCE || '1e-6');
const p0MetricsUrl = process.env.P0_REPLAY_METRICS_URL || 'http://127.0.0.1:32122';
const allowedLayers = new Set(['Vision', 'Parser', 'Engine', 'Resolver', 'KML', 'UI', 'Gate']);
export const RUNNER_TERMINAL_STATUSES = Object.freeze([
  'PASS',
  'PRODUCT_FAIL',
  'BLOCKED_NO_REPLAY',
  'BLOCKED_FIXTURE',
  'BASELINE_REVIEW_REQUIRED',
  'SKIP_OUT_OF_SCOPE',
]);
export const P0_REQUIRED_FIXTURE_SET = Object.freeze([
  'indonesia-dms-real-001',
  'indonesia-projected-real-002',
  'madagascar-cadastral-real-001',
]);

const fieldLayerMap = {
  api: 'Vision',
  coordinate_type: 'Engine',
  v2_precision_mode: 'Engine',
  v1_precisionMode: 'Gate',
  groupCount: 'Engine',
  pointCount: 'Parser',
  geometry: 'Engine',
  requires_review: 'Engine',
  kml_ready: 'KML',
  reviewGroupIndexes: 'Engine',
  country: 'Resolver',
  area: 'Engine',
  firstPoint: 'Parser',
  lastPoint: 'Parser',
  forbidden_v2_precision_mode: 'Engine',
  forbidden_v1_precisionMode: 'Gate',
  forbidden_coordinate_type: 'Engine',
  fallback_takeover: 'Vision',
  'grid row count': 'Parser',
  'grid duplicate row': 'Parser',
  'mozambique row count': 'Parser',
  'mozambique duplicate row': 'Parser',
  'exact_dms_row_accuracy': 'Vision',
  'dms row count': 'Vision',
};

function getFindingLayer(field, explicitLayer = '') {
  if (allowedLayers.has(explicitLayer)) return explicitLayer;
  const key = String(field || '');
  if (fieldLayerMap[key]) return fieldLayerMap[key];
  if (/grid row/i.test(key)) return 'Parser';
  if (/mozambique row/i.test(key)) return 'Parser';
  if (/dms row/i.test(key)) return 'Vision';
  if (/kml/i.test(key)) return 'KML';
  if (/order|axis|swapped|resolver/i.test(key)) return 'Resolver';
  if (/precision|coordinate_type|group|geometry|review/i.test(key)) return 'Engine';
  return 'Gate';
}
const fixtureRoot = process.env.COORDINATE_REGRESSION_FIXTURE_ROOT
  || path.join(repoRoot, 'regression-samples', 'fixtures');

const knownLocalFixtureNames = {
  wgs84_table_rc2_congo_001: '刚果，两个坐标在同一张图.jpg',
  wgs84_table_timeout_rescue_001: '微信图片_20260503091216_182_19.jpg',
  bftm_burkina_002: '布基纳法索02.jpg',
  utm30_burkina_003: '布基纳法索03.png',
  mgrs_myanmar_001: '缅甸坐标.jpg',
  kyrgyz_gk_001: '吉尔吉斯斯坦矿地坐标.png',
  'madagascar-cadastral-real-001': '马达加斯加坐标.png',
  'indonesia-dms-real-001': 'indonesia-utm50s-real-001.jpg',
  'indonesia-projected-real-002': 'indonesia-utm50s-real-002.jpg',
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
  const normalized = normalizeEnum(value && typeof value === 'object' ? value.type : value);
  if (normalized === 'linestring') return 'line';
  if (normalized === 'multipolygon') return 'multipolygon';
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

function normalizeDmsRow(value) {
  const compact = String(value || '')
    .replace(/[º˚]/g, '°')
    .replace(/[“”]/g, '"')
    .replace(/[‘’]/g, "'")
    .replace(/\s+/g, '')
    .replace(/，/g, ',')
    .toUpperCase();
  return compact.replace(/O/g, 'W');
}

function extractExactDmsRows(text) {
  return String(text || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => line.replace(/^\s*(?:point|pt|p|no|n[°o]?|#)?\s*[\dA-Z]{1,3}\s*[:.)-]\s*/i, ''))
    .filter((line) => {
      const normalized = normalizeDmsRow(line);
      const digitCount = (normalized.match(/\d/g) || []).length;
      const directions = (normalized.match(/[NSEW]/g) || []).length;
      return digitCount >= 8 && directions >= 2 && /[°'"]/.test(normalized);
    });
}

function extractExactDmsRowsFromV2Groups(groups) {
  if (!Array.isArray(groups)) return [];
  return groups
    .flatMap((group) => (Array.isArray(group?.points) ? group.points : []))
    .map((point) => point?.raw || '')
    .filter(Boolean)
    .filter((row) => extractExactDmsRows(row).length > 0);
}

function countCoordinateBlocks(text) {
  return String(text || '')
    .split(/\n\s*\n/g)
    .map((block) => extractCoordinateLines(block).length)
    .filter((count) => count > 0)
    .length;
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

function normalizeGridNumber(value) {
  const normalized = Number(String(value ?? '').replace(',', '.'));
  return Number.isFinite(normalized) ? normalized : null;
}

function normalizeGridRow(row) {
  if (!row || typeof row !== 'object') return null;
  const num = String(row.num ?? row.label ?? row.grid_cell ?? '').trim();
  const xv = normalizeGridNumber(row.xv ?? row.XV ?? row.x ?? row.X);
  const yv = normalizeGridNumber(row.yv ?? row.YV ?? row.y ?? row.Y);
  if (!num || xv === null || yv === null) return null;
  return { num, xv, yv };
}

function parseGridRowsFromText(text) {
  return String(text || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .map((line) => {
      if (/^num\s*\|/i.test(line)) return null;
      const pipe = line.match(/^([A-Za-z0-9-]+)\s*\|\s*([-+]?\d+(?:[.,]\d+)?)\s*\|\s*([-+]?\d+(?:[.,]\d+)?)/);
      if (!pipe) return null;
      return normalizeGridRow({ num: pipe[1], xv: pipe[2], yv: pipe[3] });
    })
    .filter(Boolean);
}

function extractGridRows(engine, coordinatesText) {
  if (normalizeEnum(engine?.coordinate_type) !== 'madagascar_cadastral_grid') return [];
  const fromPoints = flattenV2Points(engine).map(normalizeGridRow).filter(Boolean);
  const fromText = parseGridRowsFromText(coordinatesText);
  const pointsLookUsable = fromPoints.length > 0 && fromPoints.some((row) => row.xv !== 0 || row.yv !== 0);
  if (pointsLookUsable) return fromPoints;
  if (fromText.length) return fromText;
  return fromPoints;
}

function normalizeMozambiqueRow(row) {
  if (!row || typeof row !== 'object') return null;
  const order = Number(row.order ?? row.label ?? row.num);
  const latitude = Number(row.latitude ?? row.lat);
  const longitude = Number(row.longitude ?? row.lon);
  if (!Number.isInteger(order) || !Number.isFinite(latitude) || !Number.isFinite(longitude)) return null;
  return { order, latitude, longitude };
}

function parseMozambiqueRowsFromText(text) {
  return String(text || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .map((line) => {
      if (/^Mozambique\s+Geographic\s+Table/i.test(line)) return null;
      const pipe = line.match(/^(\d{1,3})\s*\|\s*([-+]?\d+(?:[.,]\d+)?)\s*\|\s*([-+]?\d+(?:[.,]\d+)?)/);
      if (!pipe) return null;
      return normalizeMozambiqueRow({
        order: pipe[1],
        latitude: String(pipe[2]).replace(',', '.'),
        longitude: String(pipe[3]).replace(',', '.'),
      });
    })
    .filter(Boolean);
}

function extractMozambiqueRows(engine, coordinatesText) {
  if (normalizeEnum(engine?.coordinate_type) !== 'mozambique_geographic_table') return [];
  const fromPoints = flattenV2Points(engine).map(normalizeMozambiqueRow).filter(Boolean);
  const fromText = parseMozambiqueRowsFromText(coordinatesText);
  if (fromPoints.length) return fromPoints;
  return fromText;
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
  const finalized = data?.finalizedCoordinateResult || {};
  const verification = data?.verification || {};
  const groups = Array.isArray(engine.groups) ? engine.groups : [];
  const finalizedGroups = Array.isArray(finalized.groups) ? finalized.groups : [];
  const coordinatesText = data?.coordinates || data?.formatted || data?.result || '';
  const gridRows = extractGridRows(engine, coordinatesText);
  const mozambiqueRows = extractMozambiqueRows(engine, coordinatesText);
  const kmlCoordinates = extractKmlCoordinates(coordinatesText);
  const fallbackRows = extractProjectedOrDecimalRows(coordinatesText);
  const textRows = kmlCoordinates.length ? kmlCoordinates : fallbackRows;
  const v2Rows = flattenV2Points(engine).map(pointToCoordinate).filter(Boolean);
  const rawTextDmsRows = extractExactDmsRows(data?.rawText || coordinatesText);
  const exactDmsRows = rawTextDmsRows.length ? rawTextDmsRows : extractExactDmsRowsFromV2Groups(groups);
  const coordinateRows = textRows.length ? textRows : v2Rows;
  const pointCount = sumPointCount(groups, Number.isInteger(data?.pointCount)
    ? data.pointCount
    : Array.isArray(data?.points)
      ? data.points.length
      : coordinateRows.length);
  const firstPoint = gridRows.length ? formatGridRow(gridRows[0]) : coordinateRows[0] || '';
  const lastPoint = gridRows.length ? formatGridRow(gridRows[gridRows.length - 1]) : coordinateRows[coordinateRows.length - 1] || '';
  const groupGeometries = groups.map((group) => normalizeGeometry(group?.geometry)).filter(Boolean);
  const uniqueGeometries = [...new Set(groupGeometries)];
  const geometry = normalizeGeometry(finalized?.geometry)
    || (uniqueGeometries.length === 1
      ? uniqueGeometries[0]
      : normalizeGeometry(data?.geometry || engine.geometry || inferGeometry(pointCount)));
  const topRequiresReview = typeof engine.requires_review === 'boolean' ? engine.requires_review : null;
  const groupRequiresReview = groups.some((group) => group?.requires_review === true);
  const authorityBlockingRequiresReview = typeof finalized.requiresReview === 'boolean'
    ? finalized.requiresReview
    : topRequiresReview ?? groupRequiresReview;
  const requiresReview = authorityBlockingRequiresReview === true
    || normalizeEnum(finalized.qualityGateStatus) === 'review_required';
  const allGroupsKmlReady = getAllGroupsKmlReady(groups);
  const effectiveReviewGroupIndexes = finalizedGroups.length
    ? finalizedGroups.map((group, index) => (group?.requiresReview === true ? index + 1 : null)).filter(value => value !== null)
    : getReviewGroupIndexes(groups);

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
    authorityBlockingRequiresReview,
    kmlReady: typeof finalized.kmlReady === 'boolean'
      ? finalized.kmlReady
      : typeof engine.kml_ready === 'boolean' ? engine.kml_ready : allGroupsKmlReady,
    firstPoint,
    lastPoint,
    groupPointCounts: groups.map((group) => (Array.isArray(group?.points) ? group.points.length : 0)),
    polygonCount: groups.length || countCoordinateBlocks(coordinatesText),
    reviewGroupIndexes: effectiveReviewGroupIndexes,
    decisionState: finalized.decisionState || '',
    confirmationStatus: finalized.confirmationStatus || '',
    qualityGateStatus: finalized.qualityGateStatus || '',
    reasonCodes: Array.isArray(finalized.reasonCodes) ? finalized.reasonCodes : [],
    blockingReasons: Array.isArray(finalized.blockingReasons) ? finalized.blockingReasons : [],
    finalizerWarnings: Array.isArray(finalized.warnings) ? finalized.warnings : [],
    finalizerEvaluated: Boolean(finalized?.schemaVersion && finalized?.decisionState),
    familyPolicyId: finalized.familySafetyPolicy?.policyId || '',
    familyPolicyVersion: finalized.familySafetyPolicy?.policyVersion || '',
    geometryWarnings: Array.isArray(verification.geometryWarnings) ? verification.geometryWarnings : [],
    country: getActualCountry(engine, data),
    areaHa: getActualArea(engine),
    gridRows,
    mozambiqueRows,
    exactDmsRows,
    coordinateBlockCount: countCoordinateBlocks(coordinatesText),
    orderStatus: engine.order_status || engine.coordinate_validation_report?.order_status || '',
    fallbackUsed: isFallbackUsed(data, engine),
    retryUsed: isRetryUsed(data),
    timeout: isTimeout(data, responseMeta.responseText || ''),
    responseCode: data?.code || data?.error_code || '',
    responseReason: data?.reason || '',
    requestStartedAt: responseMeta.requestStartedAt || null,
    deadlineTriggeredAt: data?.deadlineTriggeredAt || null,
    responseReturnedAt: responseMeta.responseReturnedAt || null,
    requestId: responseMeta.requestId || null,
    stageTrace: responseMeta.stageTrace || null,
    reportedElapsedMs: Number.isFinite(Number(data?.elapsedMs)) ? Number(data.elapsedMs) : null,
    handlerDeadlineMs: Number.isFinite(Number(data?.deadlineMs)) ? Number(data.deadlineMs) : null,
    runnerDeadlineMs: null,
    verificationStatus: verification?.status || null,
    verificationConflicts: Array.isArray(verification?.conflicts) ? verification.conflicts : [],
    providerRawEvidenceAvailable: Boolean(data?.rawText),
    normalizedEvidenceAvailable: Boolean(data?.coordinates || finalized?.geometry),
    fallbackType: getFallbackType(data),
    errorCode: data?.code || data?.error_code || '',
    raw: data,
  };
}

async function fetchRecognitionTrace(requestId, caseId) {
  if (!requestId) {
    const error = new Error(`Recognition response for ${caseId} did not include X-Recognition-Request-Id.`);
    error.code = 'TRACE_CORRELATION_MISSING';
    throw error;
  }
  for (let attempt = 0; attempt < 20; attempt += 1) {
    const response = await fetch(`${traceApiBaseUrl}/${encodeURIComponent(requestId)}`, {
      headers: {
        'x-regression-test': 'true',
        'x-regression-case-id': caseId,
      },
    });
    if (response.status === 404) {
      await new Promise(resolve => setTimeout(resolve, 10));
      continue;
    }
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload?.trace) {
      const error = new Error(`Recognition trace fetch failed for ${caseId}: HTTP ${response.status}`);
      error.code = 'TRACE_CORRELATION_MISSING';
      throw error;
    }
    if (payload.trace.requestId !== requestId || payload.trace.caseId !== caseId) {
      const error = new Error(`Recognition trace correlation mismatch for ${caseId}.`);
      error.code = 'TRACE_CORRELATION_MISMATCH';
      throw error;
    }
    return payload.trace;
  }
  const error = new Error(`Recognition trace was not persisted for ${caseId}.`);
  error.code = 'TRACE_CORRELATION_MISSING';
  throw error;
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

  const requestStartedAt = new Date().toISOString();
  const startedAt = performance.now();
  const response = await fetch(apiUrl, {
    method: 'POST',
    body: form,
    headers: {
      'x-visitor-id': visitorId,
      'x-source': 'coordinate-regression-runner',
      'x-regression-test': 'true',
      'x-regression-case-id': sample.sample_id,
    },
  });
  const responseText = await response.text();
  const durationMs = Math.round(performance.now() - startedAt);
  const responseReturnedAt = new Date().toISOString();
  const requestId = response.headers.get('x-recognition-request-id');
  const stageTrace = await fetchRecognitionTrace(requestId, sample.sample_id);

  let data;
  try {
    data = JSON.parse(responseText);
  } catch {
    return {
      httpStatus: response.status,
      durationMs,
      requestStartedAt,
      responseReturnedAt,
      requestId,
      stageTrace,
      error: `API returned non-JSON response: ${responseText.slice(0, 240)}`,
    };
  }

  if (!response.ok) {
    return {
      httpStatus: response.status,
      durationMs,
      error: data?.error || data?.message || responseText.slice(0, 240),
      actual: summarizeApiResponse(data, { httpStatus: response.status, durationMs, responseText, requestStartedAt, responseReturnedAt, requestId, stageTrace }),
    };
  }

  return summarizeApiResponse(data, { httpStatus: response.status, durationMs, responseText, requestStartedAt, responseReturnedAt, requestId, stageTrace });
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

  const requestStartedAt = new Date().toISOString();
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
  const responseReturnedAt = new Date().toISOString();

  let data;
  try {
    data = JSON.parse(responseText);
  } catch {
    return {
      httpStatus: response.status,
      durationMs,
      requestStartedAt,
      responseReturnedAt,
      error: `Text API returned non-JSON response: ${responseText.slice(0, 240)}`,
    };
  }

  if (!response.ok) {
    return {
      httpStatus: response.status,
      durationMs,
      error: data?.error || data?.message || responseText.slice(0, 240),
      actual: summarizeApiResponse(data, { httpStatus: response.status, durationMs, responseText, requestStartedAt, responseReturnedAt }),
    };
  }

  return summarizeApiResponse(data, { httpStatus: response.status, durationMs, responseText, requestStartedAt, responseReturnedAt });
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

function addFinding(findings, severity, field, expected, actual, layer = '') {
  findings.push({
    severity,
    layer: getFindingLayer(field, layer),
    field,
    expected,
    actual,
  });
}

function compareField(diffs, field, expected, actual, options = {}) {
  const skipped = expected === undefined || expected === null || expected === '';
  if (skipped && options.skipWhenNull !== false) return;
  const severity = options.severity || 'BLOCKER';
  const layer = options.layer || '';

  if (options.normalize === 'enum') {
    if (normalizeEnum(expected) !== normalizeEnum(actual)) {
      addFinding(diffs, severity, field, expected, actual || null, layer);
    }
    return;
  }

  if (options.normalize === 'geometry') {
    if (normalizeGeometry(expected) !== normalizeGeometry(actual)) {
      addFinding(diffs, severity, field, expected, actual || null, layer);
    }
    return;
  }

  if (options.normalize === 'boolean') {
    if (Boolean(expected) !== Boolean(actual)) {
      addFinding(diffs, severity, field, Boolean(expected), Boolean(actual), layer);
    }
    return;
  }

  if (options.normalize === 'array') {
    const expectedArray = Array.isArray(expected) ? expected : [];
    const actualArray = Array.isArray(actual) ? actual : [];
    if (JSON.stringify(expectedArray) !== JSON.stringify(actualArray)) {
      addFinding(diffs, severity, field, expectedArray, actualArray, layer);
    }
    return;
  }

  if (expected !== actual) {
    addFinding(diffs, severity, field, expected, actual, layer);
  }
}

function formatGridRow(row) {
  if (!row) return '(missing)';
  return `${row.num} | ${row.xv} | ${row.yv}`;
}

function gridRowsEqual(a, b, tolerance) {
  if (!a || !b) return false;
  return String(a.num) === String(b.num)
    && Math.abs(Number(a.xv) - Number(b.xv)) <= tolerance
    && Math.abs(Number(a.yv) - Number(b.yv)) <= tolerance;
}

function compareGridRows(diffs, expectedRows, actualRows, tolerance = 0.01) {
  if (!Array.isArray(expectedRows) || expectedRows.length === 0) return;
  const expected = expectedRows.map(normalizeGridRow);
  const actual = Array.isArray(actualRows) ? actualRows.map(normalizeGridRow).filter(Boolean) : [];

  if (actual.length !== expected.length) {
    addFinding(diffs, 'BLOCKER', 'grid row count', `${expected.length} rows`, `${actual.length} rows`);
  }

  const actualKeys = new Map();
  for (const row of actual) {
    const key = `${row.num}|${row.xv}|${row.yv}`;
    actualKeys.set(key, (actualKeys.get(key) || 0) + 1);
  }
  for (const [key, count] of actualKeys.entries()) {
    if (count > 1) {
      addFinding(diffs, 'BLOCKER', 'grid duplicate row', 'no duplicates', `${key} repeated ${count} times`);
    }
  }

  const max = Math.max(expected.length, actual.length);
  for (let index = 0; index < max; index += 1) {
    const expectedRow = expected[index];
    const actualRow = actual[index];
    if (!gridRowsEqual(expectedRow, actualRow, tolerance)) {
      addFinding(
        diffs,
        'BLOCKER',
        `grid row ${index + 1} mismatch`,
        formatGridRow(expectedRow),
        formatGridRow(actualRow),
      );
    }
  }
}

function formatMozambiqueRow(row) {
  if (!row) return '(missing)';
  return `${row.order} | ${row.latitude} | ${row.longitude}`;
}

function mozambiqueRowsEqual(a, b, tolerance) {
  if (!a || !b) return false;
  return Number(a.order) === Number(b.order)
    && Math.abs(Number(a.latitude) - Number(b.latitude)) <= tolerance
    && Math.abs(Number(a.longitude) - Number(b.longitude)) <= tolerance;
}

function compareMozambiqueRows(diffs, expectedRows, actualRows, tolerance = 0.0008) {
  if (!Array.isArray(expectedRows) || expectedRows.length === 0) return;
  const expected = expectedRows.map(normalizeMozambiqueRow).filter(Boolean);
  const actual = Array.isArray(actualRows) ? actualRows.map(normalizeMozambiqueRow).filter(Boolean) : [];

  if (actual.length !== expected.length) {
    addFinding(diffs, 'BLOCKER', 'mozambique row count', `${expected.length} rows`, `${actual.length} rows`);
  }

  const actualKeys = new Map();
  for (const row of actual) {
    const key = `${row.order}|${row.latitude.toFixed(6)}|${row.longitude.toFixed(6)}`;
    actualKeys.set(key, (actualKeys.get(key) || 0) + 1);
  }
  for (const [key, count] of actualKeys.entries()) {
    if (count > 1) {
      addFinding(diffs, 'BLOCKER', 'mozambique duplicate row', 'no duplicates', `${key} repeated ${count} times`);
    }
  }

  const max = Math.max(expected.length, actual.length);
  for (let index = 0; index < max; index += 1) {
    const expectedRow = expected[index];
    const actualRow = actual[index];
    if (!mozambiqueRowsEqual(expectedRow, actualRow, tolerance)) {
      addFinding(
        diffs,
        'BLOCKER',
        `mozambique row ${index + 1} mismatch`,
        formatMozambiqueRow(expectedRow),
        formatMozambiqueRow(actualRow),
      );
    }
  }
}

function compareExactDmsRows(diffs, expectedRows, actualRows) {
  if (!Array.isArray(expectedRows) || expectedRows.length === 0) return;
  const actual = Array.isArray(actualRows) ? actualRows : [];

  if (actual.length !== expectedRows.length) {
    addFinding(diffs, 'BLOCKER', 'dms row count', expectedRows.length, actual.length, 'Vision');
  }

  const max = Math.max(expectedRows.length, actual.length);
  for (let index = 0; index < max; index += 1) {
    const expected = expectedRows[index] || null;
    const actualRow = actual[index] || null;
    if (normalizeDmsRow(expected) !== normalizeDmsRow(actualRow)) {
      addFinding(diffs, 'BLOCKER', `dms row ${index + 1} mismatch`, expected, actualRow, 'Vision');
    }
  }
}

function compareGolden(sample, actual) {
  const diffs = [];
  const warnings = [];

  if (!actual || actual.error) {
    const errorText = actual?.error || 'no response';
    const layer = /fixture|runner|sample|path/i.test(errorText) ? 'Gate' : 'Vision';
    addFinding(diffs, 'BLOCKER', 'api', 'successful JSON response', errorText, layer);
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
  if (Array.isArray(sample.expected_decision_states) && !sample.expected_decision_states.includes(actual.decisionState)) {
    addFinding(diffs, 'BLOCKER', 'decision_state', sample.expected_decision_states, actual.decisionState || null, 'Gate');
  }
  compareField(diffs, 'confirmation_status', sample.expected_confirmation_status, actual.confirmationStatus, { normalize: 'enum', layer: 'Gate' });
  compareField(diffs, 'quality_gate_status', sample.expected_quality_gate_status, actual.qualityGateStatus, { normalize: 'enum', layer: 'Gate' });
  compareField(diffs, 'family_policy_id', sample.expected_family_policy_id, actual.familyPolicyId, { layer: 'Gate' });
  compareField(diffs, 'family_policy_version', sample.expected_family_policy_version, actual.familyPolicyVersion, { layer: 'Gate' });
  compareField(diffs, 'country', sample.expected_country, actual.country, { severity: 'WARNING' });
  compareGridRows(diffs, sample.expected_grid_rows, actual.gridRows, Number(sample.tolerance?.xv_yv ?? 0.01));
  compareMozambiqueRows(diffs, sample.expected_rows, actual.mozambiqueRows, Number(sample.tolerance?.lat_lon ?? 0.0008));
  compareExactDmsRows(diffs, sample.expected_exact_dms_rows, actual.exactDmsRows);
  compareField(diffs, 'group_point_counts', sample.expected_group_point_counts, actual.groupPointCounts, { normalize: 'array' });
  compareField(diffs, 'polygon_count', sample.expected_polygon_count, actual.polygonCount);

  if (sample.forbidden_flatten_to_single_group === true && actual.groupCount === 1 && Number(sample.expected_group_count) > 1) {
    addFinding(diffs, 'BLOCKER', 'flatten_to_single_group', `not 1 group; expected ${sample.expected_group_count}`, actual.groupCount);
  }
  if (sample.forbidden_cross_group_edges === true && actual.polygonCount === 1 && Number(sample.expected_polygon_count) > 1) {
    addFinding(diffs, 'BLOCKER', 'cross_group_edges', `no single polygon across ${sample.expected_group_count} groups`, `${actual.polygonCount} polygon`);
  }

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
  const forbiddenCoordinateTypes = Array.isArray(sample.forbidden_coordinate_types)
    ? sample.forbidden_coordinate_types
    : [];
  if (forbiddenCoordinateTypes.includes(actual.coordinateType)) {
    addFinding(diffs, 'BLOCKER', 'forbidden_coordinate_type', `not ${actual.coordinateType}`, actual.coordinateType);
  }
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

export function classifyAcquisitionTerminal(actualResult, { p0Critical = false, deterministicReplay = false } = {}) {
  const envelope = actualResult?.actual || actualResult || {};
  if (envelope.skipped) {
    return envelope.skipReason === 'fixture_not_found' || envelope.skipReason === 'text_fixture_unavailable'
      ? 'BLOCKED_FIXTURE'
      : 'SKIP_OUT_OF_SCOPE';
  }
  const httpFailure = Number.isInteger(envelope.httpStatus) && envelope.httpStatus >= 400;
  const invalidEnvelope = envelope.error
    || envelope.finalizerEvaluated !== true
    || envelope.normalizedEvidenceAvailable !== true;
  if (httpFailure || invalidEnvelope) {
    return p0Critical || deterministicReplay ? 'PRODUCT_FAIL' : 'BLOCKED_NO_REPLAY';
  }
  return null;
}

function buildBlockedResult(sample, status, reason, errorLibrary, options) {
  const sampleErrors = getErrorsForSample(errorLibrary, sample, options);
  return {
    sample,
    status,
    historicalStatus: sampleErrors.length ? 'NOT CHECKED' : 'NOT APPLICABLE',
    historicalFindings: [],
    runs: [{ skipped: true, skipReason: reason, error: reason }],
    diffs: [],
    warnings: [],
    semanticSummary: summarizeSemanticRuns([]),
  };
}

function validateBaseline(baseline) {
  const errors = [];
  if (baseline.schema_version !== 'coordinate_recognition_golden_baseline_v1') {
    errors.push(`unsupported schema_version: ${baseline.schema_version || '(missing)'}`);
  }
  const baselineLayers = baseline.layer_classification?.allowed_layers;
  if (!Array.isArray(baselineLayers) || baselineLayers.length === 0) {
    errors.push('layer_classification.allowed_layers is required');
  } else {
    for (const layer of baselineLayers) {
      if (!allowedLayers.has(layer)) {
        errors.push(`layer_classification.allowed_layers contains unsupported layer: ${layer}`);
      }
    }
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
      'layer',
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
    if (!entry.layer) {
      errors.push(`${prefix}.layer is required`);
    }
    if (entry.layer && !allowedLayers.has(entry.layer)) {
      errors.push(`${prefix}.layer is unsupported: ${entry.layer}`);
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
    layers: {},
  };
  for (const entry of errorLibrary.errors || []) {
    const status = String(entry.status || 'open');
    stats[status] = (stats[status] || 0) + 1;
    const layer = allowedLayers.has(entry.layer) ? entry.layer : 'Unclassified';
    stats.layers[layer] = (stats.layers[layer] || 0) + 1;
  }
  return stats;
}

function printErrorLibraryStats(errorLibrary) {
  const stats = getErrorLibraryStats(errorLibrary);
  console.log(`Error Library: total=${stats.total} fixed=${stats.fixed || 0} open=${stats.open || 0} prevented_not_adopted=${stats.prevented_not_adopted || 0} open_architecture=${stats.open_architecture || 0}`);
  console.log(`Error Library Layers: ${Object.entries(stats.layers).map(([layer, count]) => `${layer}=${count}`).join(' ') || '(none)'}`);
}

function printErrorList(errorLibrary, selectedErrorIds = new Set()) {
  printErrorLibraryStats(errorLibrary);
  const errors = (errorLibrary.errors || [])
    .filter((entry) => !selectedErrorIds.size || selectedErrorIds.has(String(entry.error_id).toLowerCase()));
  for (const entry of errors) {
    const linkedSample = entry.regression_case?.baseline_sample_id || entry.sample_id || '(none)';
    console.log(`- ${entry.error_id} | layer=${entry.layer || '(missing)'} | ${entry.status} | sample=${linkedSample} | type=${entry.coordinate_type || '(unknown)'} | fix=${entry.fix_commit || '(none)'}`);
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
    layer: getFindingLayer('', errorEntry.layer || 'Gate'),
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
      status: 'SKIP_OUT_OF_SCOPE',
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
      status: 'SKIP_OUT_OF_SCOPE',
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

  let replayValidation = null;
  if (options.p0ReplayManifest && sample.input_type === 'image') {
    replayValidation = await validateP0ReplayFixture(repoRoot, sample, options.p0ReplayManifest);
    if (replayValidation.status !== 'READY') {
      return buildBlockedResult(
        sample,
        replayValidation.status,
        replayValidation.reason || 'no_approved_deterministic_replay',
        errorLibrary,
        options,
      );
    }
  }

  const allDiffs = [];
  const allWarnings = [];
  const historicalFindings = [];

  for (let runIndex = 0; runIndex < repeatCount; runIndex += 1) {
    const actual = sample.input_type === 'text'
      ? await callTextApi(sample)
      : await callImageApi(sample);
    const acquisitionTerminal = classifyAcquisitionTerminal(actual, {
      p0Critical: sample.p0_release_critical === true,
      deterministicReplay: replayValidation?.status === 'READY',
    });
    if (acquisitionTerminal) {
      runs.push({ actual, diffs: [], warnings: [], semantics: {} });
      return {
        sample,
        status: acquisitionTerminal,
        historicalStatus: sampleErrors.length ? 'NOT CHECKED' : 'NOT APPLICABLE',
        historicalFindings: [],
        runs,
        diffs: [],
        warnings: [],
        semanticSummary: summarizeSemanticRuns(runs),
      };
    }
    const { diffs, warnings } = compareGolden(sample, actual.actual || actual);
    const semantics = classifyGoldenRun({ sample, actual: actual.actual || actual, diffs });
    runs.push({ actual, diffs, warnings, semantics });
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
  const status = sample.baseline_review_required === true
    ? 'BASELINE_REVIEW_REQUIRED'
    : statusFromBaseline === 'FAIL' || historicalWorst === 'FAIL'
      ? 'PRODUCT_FAIL'
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
    semanticSummary: summarizeSemanticRuns(runs),
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
    `gridRows=${Array.isArray(actual.gridRows) ? actual.gridRows.length : 0}`,
    `mozambiqueRows=${Array.isArray(actual.mozambiqueRows) ? actual.mozambiqueRows.length : 0}`,
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

  if (['SKIP_OUT_OF_SCOPE', 'BLOCKED_NO_REPLAY', 'BLOCKED_FIXTURE'].includes(result.status)) {
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
  const semantic = result.semanticSummary || summarizeSemanticRuns(result.runs);
  console.log(`  TRUTH_STATUS=${semantic.truthStatus}`);
  console.log(`  POLICY_STATUS=${semantic.policyStatus}`);
  console.log(`  GATE_SAFETY_STATUS=${semantic.gateSafetyStatus}`);
  console.log(`  METADATA_STATUS=${semantic.metadataStatus}`);
  console.log(`  PROVIDER_VARIANCE_STATUS=${semantic.providerVarianceStatus}`);
  console.log(`  RELIABILITY_STATUS=${semantic.reliabilityStatus}`);
  console.log(`  FINALIZER_STATUS=${semantic.finalizerStatus}`);
  console.log(`  TIMEOUT_STAGE=${semantic.timeoutStage}`);
  console.log(`  CLASSIFICATIONS=${semantic.classifications.join(',') || 'NONE'}`);

  if (result.diffs.length) {
    console.log('  Diff:');
    for (const diff of result.diffs) {
      console.log(`    [${diff.severity || 'BLOCKER'}][${diff.layer || getFindingLayer(diff.field)}] run ${diff.run} ${diff.field}: expected ${JSON.stringify(diff.expected)}; actual ${JSON.stringify(diff.actual)}`);
    }
  }

  console.log(`  Historical Error Check: ${result.historicalStatus || 'NOT APPLICABLE'}`);
  if (result.historicalFindings?.length) {
    for (const finding of result.historicalFindings) {
      console.log(`    [${finding.severity}][${finding.layer || 'Gate'}] ${finding.code}`);
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
      console.log(`    [${warning.severity || 'INFO'}][${warning.layer || getFindingLayer(warning.field)}] run ${warning.run} ${warning.field}: ${warning.message}`);
    }
  }
}

export function summarizeResults(results) {
  const summary = {
    pass: results.filter((result) => result.status === 'PASS').length,
    productFail: results.filter((result) => result.status === 'PRODUCT_FAIL').length,
    blockedNoReplay: results.filter((result) => result.status === 'BLOCKED_NO_REPLAY').length,
    blockedFixture: results.filter((result) => result.status === 'BLOCKED_FIXTURE').length,
    baselineReviewRequired: results.filter((result) => result.status === 'BASELINE_REVIEW_REQUIRED').length,
    skipOutOfScope: results.filter((result) => result.status === 'SKIP_OUT_OF_SCOPE').length,
    passWithWarning: 0,
    fail: results.filter((result) => result.status === 'PRODUCT_FAIL').length,
    skipped: results.filter((result) => result.status === 'SKIP_OUT_OF_SCOPE').length,
    fixtureIds: Object.fromEntries(RUNNER_TERMINAL_STATUSES.map(status => [status, results
      .filter(result => result.status === status)
      .map(result => result.sample.sample_id)])),
    regression: 0,
    historicalRegressions: 0,
    knownOpenErrors: 0,
    blockerCount: 0,
    warningCount: 0,
    infoCount: 0,
    layerCounts: {},
    durations: [],
    timeoutCount: 0,
    fallbackCount: 0,
    semanticClassifications: {},
    truthPassCount: 0,
    truthFailureCount: 0,
    truthEvaluatedCount: 0,
    truthNotEvaluatedCount: 0,
    truthNotQualifiedCount: 0,
    policyPassCount: 0,
    policyFailureCount: 0,
    policyEvaluatedCount: 0,
    policyNotEvaluatedCount: 0,
    policyNotQualifiedCount: 0,
    unsafeGateFailureCount: 0,
    conservativeReviewCount: 0,
    metadataMismatchCount: 0,
    providerVarianceCount: 0,
  };

  const countLayer = (layer) => {
    const normalizedLayer = allowedLayers.has(layer) ? layer : 'Gate';
    summary.layerCounts[normalizedLayer] = (summary.layerCounts[normalizedLayer] || 0) + 1;
  };

  for (const result of results) {
    const baselineStatus = String(result.sample.baseline_status || '').toLowerCase();
    if (result.status === 'PRODUCT_FAIL' && ['locked', 'locked_experimental', 'unstable'].includes(baselineStatus)) {
      summary.regression += 1;
    }
    for (const finding of result.historicalFindings || []) {
      if (finding.code === 'HISTORICAL_ERROR_REGRESSION') summary.historicalRegressions += 1;
      if (finding.code === 'KNOWN_OPEN_ERROR' || finding.code === 'KNOWN_OPEN_ARCHITECTURE_ERROR') summary.knownOpenErrors += 1;
      if (finding.severity === 'BLOCKER') summary.blockerCount += 1;
      else if (finding.severity === 'WARNING') summary.warningCount += 1;
      else summary.infoCount += 1;
      countLayer(finding.layer);
    }
    for (const diff of result.diffs) {
      if (diff.severity === 'BLOCKER') summary.blockerCount += 1;
      else if (diff.severity === 'WARNING') summary.warningCount += 1;
      else summary.infoCount += 1;
      countLayer(diff.layer || getFindingLayer(diff.field));
    }
    for (const warning of result.warnings) {
      if (warning.severity === 'WARNING') summary.warningCount += 1;
      else summary.infoCount += 1;
      countLayer(warning.layer || getFindingLayer(warning.field));
    }
    for (const run of result.runs) {
      const actual = run.actual?.actual || run.actual;
      if (Number.isFinite(actual?.durationMs)) summary.durations.push(actual.durationMs);
      if (actual?.timeout) summary.timeoutCount += 1;
      if (actual?.fallbackUsed) summary.fallbackCount += 1;
    }
    for (const classification of result.semanticSummary?.classifications || []) {
      summary.semanticClassifications[classification] = (summary.semanticClassifications[classification] || 0) + 1;
    }
    const semantic = result.semanticSummary || {};
    if (semantic.truthStatus === 'MATCH') summary.truthPassCount += 1;
    if (semantic.truthStatus === 'MISMATCH') summary.truthFailureCount += 1;
    if (['MATCH', 'MISMATCH'].includes(semantic.truthStatus)) summary.truthEvaluatedCount += 1;
    if (semantic.truthStatus === 'NOT_EVALUATED') summary.truthNotEvaluatedCount += 1;
    if (semantic.truthStatus === 'NOT_QUALIFIED') summary.truthNotQualifiedCount += 1;
    if (semantic.policyStatus === 'MATCH') summary.policyPassCount += 1;
    if (semantic.policyStatus === 'MISMATCH') summary.policyFailureCount += 1;
    if (['MATCH', 'MISMATCH'].includes(semantic.policyStatus)) summary.policyEvaluatedCount += 1;
    if (semantic.policyStatus === 'NOT_EVALUATED') summary.policyNotEvaluatedCount += 1;
    if (semantic.policyStatus === 'NOT_QUALIFIED') summary.policyNotQualifiedCount += 1;
    if (semantic.classifications?.includes('UNSAFE_GATE_FAILURE')) summary.unsafeGateFailureCount += 1;
    if (semantic.classifications?.includes('CONSERVATIVE_REVIEW')) summary.conservativeReviewCount += 1;
    if (semantic.classifications?.includes('METADATA_MISMATCH')) summary.metadataMismatchCount += 1;
    if (semantic.classifications?.includes('PROVIDER_VARIANCE')) summary.providerVarianceCount += 1;
  }

  summary.avgMs = summary.durations.length
    ? Math.round(summary.durations.reduce((sum, value) => sum + value, 0) / summary.durations.length)
    : null;
  summary.maxMs = summary.durations.length ? Math.max(...summary.durations) : null;
  summary.minMs = summary.durations.length ? Math.min(...summary.durations) : null;
  summary.gateResult = summary.productFail > 0 || summary.regression > 0 || summary.blockedFixture > 0 || summary.baselineReviewRequired > 0
    ? 'FAIL'
    : summary.warningCount > 0
      ? 'PASS WITH WARNING'
      : 'PASS';
  Object.assign(summary, summarizeReleaseSemantics(results));

  return summary;
}

export function evaluateP0ReleaseGate(results, evidenceBinding = {}, gateGovernance, providerMeasurement) {
  const summary = summarizeResults(results);
  const byId = new Map(results.map(result => [result.sample.sample_id, result]));
  const requiredStatuses = Object.fromEntries(P0_REQUIRED_FIXTURE_SET.map(id => [id, byId.get(id)?.status || 'MISSING']));
  const blockedNoReplayFixtures = summary.fixtureIds.BLOCKED_NO_REPLAY;
  const identityBindingPass = evidenceBinding.status === 'LOCAL_PATCH_CANDIDATE_BOUND'
    || evidenceBinding.status === 'PASS';
  const governedBlocked = gateGovernance?.blockedNoReplayFixtures || [];
  const governedById = new Map(governedBlocked.map(entry => [entry.fixtureId, entry]));
  const actualBlockedSet = new Set(blockedNoReplayFixtures);
  const unexpectedBlockedNoReplayFixtures = blockedNoReplayFixtures.filter(id => !governedById.has(id));
  const missingExpectedBlockedNoReplayFixtures = governedBlocked
    .map(entry => entry.fixtureId)
    .filter(id => !actualBlockedSet.has(id));
  const blockedNoReplayDetailsValid = blockedNoReplayFixtures.every((id) => {
    const result = byId.get(id);
    const governed = governedById.get(id);
    return Boolean(governed?.fixtureIdentity?.path)
      && Boolean(governed?.fixtureIdentity?.sha256)
      && result?.runs?.[0]?.skipReason === governed.reason;
  });
  const allNoReplayExplicitlyEnumerated = Boolean(gateGovernance)
    && unexpectedBlockedNoReplayFixtures.length === 0
    && missingExpectedBlockedNoReplayFixtures.length === 0
    && blockedNoReplayDetailsValid;
  const directlyAffectedBlockedWithoutSubstitute = governedBlocked.filter((entry) => (
    actualBlockedSet.has(entry.fixtureId)
    && entry.directlyAffectedAreas.some(area => !entry.approvedSubstituteCoverage.includes(area))
  )).map(entry => entry.fixtureId);
  const directlyAffectedIds = new Set([
    ...P0_REQUIRED_FIXTURE_SET,
    ...governedBlocked.filter(entry => entry.directlyAffectedAreas.length > 0).map(entry => entry.fixtureId),
  ]);
  const unresolvedDirectlyAffectedBaselineReviewRequiredCount = results.filter(result => (
    result.status === 'BASELINE_REVIEW_REQUIRED' && directlyAffectedIds.has(result.sample.sample_id)
  )).length;
  const noP0FixtureBlocked = P0_REQUIRED_FIXTURE_SET.every(id => requiredStatuses[id] === 'PASS');
  const providerCallsMeasured = providerMeasurement?.measurementActive === true
    && Number.isInteger(providerMeasurement?.observedProviderAcquisitionAttempts)
    && Number.isInteger(providerMeasurement?.authorizedReplayProviderCalls)
    && Number.isInteger(providerMeasurement?.unauthorizedProviderCalls);
  const unauthorizedProviderCalls = providerCallsMeasured ? providerMeasurement.unauthorizedProviderCalls : null;
  const pass = identityBindingPass
    && summary.productFail === 0
    && noP0FixtureBlocked
    && unresolvedDirectlyAffectedBaselineReviewRequiredCount === 0
    && allNoReplayExplicitlyEnumerated
    && directlyAffectedBlockedWithoutSubstitute.length === 0
    && providerCallsMeasured
    && unauthorizedProviderCalls === 0;
  return Object.freeze({
    status: pass ? 'PASS' : 'FAIL',
    identityBindingPass,
    requiredStatuses,
    blockedNoReplayFixtures,
    allNoReplayExplicitlyEnumerated,
    blockedNoReplayDetailsValid,
    unexpectedBlockedNoReplayFixtures,
    missingExpectedBlockedNoReplayFixtures,
    directlyAffectedBlockedWithoutSubstitute,
    unresolvedDirectlyAffectedBaselineReviewRequiredCount,
    noP0FixtureBlocked,
    providerCallsMeasured,
    providerMeasurement,
    unauthorizedProviderCalls,
  });
}

async function resetP0ProviderMeasurement() {
  if (!isLoopbackUrl(p0MetricsUrl)) throw new Error('P0_REPLAY_METRICS_LOOPBACK_REQUIRED');
  const response = await fetch(`${p0MetricsUrl}/reset`, { method: 'POST' });
  if (!response.ok) throw new Error(`P0_PROVIDER_MEASUREMENT_RESET_FAILED:${response.status}`);
}

async function readP0ProviderMeasurement() {
  if (!isLoopbackUrl(p0MetricsUrl)) throw new Error('P0_REPLAY_METRICS_LOOPBACK_REQUIRED');
  const response = await fetch(`${p0MetricsUrl}/metrics`, { headers: { 'cache-control': 'no-store' } });
  const payload = await response.json().catch(() => null);
  if (!response.ok || payload?.measurementActive !== true) throw new Error('P0_PROVIDER_MEASUREMENT_UNAVAILABLE');
  for (const field of ['observedProviderAcquisitionAttempts', 'authorizedReplayProviderCalls', 'unauthorizedProviderCalls']) {
    if (!Number.isInteger(payload[field]) || payload[field] < 0) throw new Error(`P0_PROVIDER_MEASUREMENT_INVALID:${field}`);
  }
  if (payload.observedProviderAcquisitionAttempts
    !== payload.authorizedReplayProviderCalls + payload.unauthorizedProviderCalls) {
    throw new Error('P0_PROVIDER_MEASUREMENT_ACCOUNTING_MISMATCH');
  }
  return Object.freeze({ ...payload });
}

function unwrapActual(run = {}) {
  return run.actual?.actual || run.actual || {};
}

export function buildEvidenceCase(result) {
  return {
    caseId: result.sample.sample_id,
    attempts: result.runs.map((run, index) => {
      const actual = unwrapActual(run);
      const semantics = run.semantics || {};
      return {
        attempt: index + 1,
        requestId: actual.requestId || null,
        stageTrace: actual.stageTrace || null,
        httpStatus: actual.httpStatus ?? null,
        elapsedMs: actual.durationMs ?? null,
        responseCode: actual.responseCode || actual.errorCode || null,
        responseReason: actual.responseReason || null,
        requestStartedAt: actual.requestStartedAt || null,
        deadlineTriggeredAt: actual.deadlineTriggeredAt || null,
        responseReturnedAt: actual.responseReturnedAt || null,
        handlerDeadlineMs: actual.handlerDeadlineMs ?? null,
        runnerDeadlineMs: actual.runnerDeadlineMs ?? null,
        timeoutStage: semantics.timeoutStage || 'NOT_APPLICABLE',
        truthMaturity: semantics.truthMaturity || null,
        policyMaturity: semantics.policyMaturity || null,
        truthStatus: semantics.truthStatus || 'NOT_EVALUATED',
        policyStatus: semantics.policyStatus || 'NOT_EVALUATED',
        gateSafetyStatus: semantics.gateSafetyStatus || 'NOT_EVALUATED',
        metadataStatus: semantics.metadataStatus || 'NOT_EVALUATED',
        providerVarianceStatus: semantics.providerVarianceStatus || 'NOT_EVALUATED',
        reliabilityStatus: semantics.reliabilityStatus || 'PASS',
        finalizerStatus: semantics.finalizerStatus || 'NOT_EVALUATED',
        runtime: {
          decisionState: actual.finalizerEvaluated ? actual.decisionState || null : null,
          requiresReview: actual.finalizerEvaluated ? actual.requiresReview : null,
          kmlReady: actual.finalizerEvaluated ? actual.kmlReady : null
        },
        finalizedCoordinateResult: actual.finalizerEvaluated ? {
          confirmationStatus: actual.confirmationStatus || null,
          qualityGateStatus: actual.qualityGateStatus || null,
          decisionState: actual.decisionState || null,
          blockingReasons: actual.blockingReasons || []
        } : 'FINALIZER_NOT_EVALUATED',
        verification: {
          status: actual.verificationStatus || null,
          conflicts: actual.verificationConflicts || [],
          geometryWarnings: actual.geometryWarnings || []
        },
        providerEvidenceAvailability: {
          raw: actual.providerRawEvidenceAvailable === true,
          normalized: actual.normalizedEvidenceAvailable === true
        },
        fallbackUsed: actual.fallbackUsed === true,
        timeout: actual.timeout === true
      };
    })
  };
}

export async function writeEvidenceArtifact(results, summary, governance, evidenceBinding) {
  const artifactPath = process.env.COORDINATE_REGRESSION_ARTIFACT_PATH || defaultEvidenceArtifactPath;
  const traces = results.flatMap(result => result.runs.map(run => unwrapActual(run).stageTrace).filter(Boolean));
  const postDeadlineWorkStatus = traces.length > 0
    && traces.every(trace => trace.postDeadlineWorkStatus === 'PROVEN_NONE')
    ? 'PROVEN_NONE'
    : 'UNPROVEN';
  const artifact = {
    schemaVersion: 'coordinate_regression_evidence_v3',
    generatedAt: new Date().toISOString(),
    approvalSource: governance.approval?.source || null,
    evidenceBindingStatus: evidenceBinding.status,
    productionSourceHash: evidenceBinding.productionSourceHash,
    runtimeSourceHash: evidenceBinding.runtimeSourceHash,
    releaseGovernanceHash: evidenceBinding.releaseGovernanceHash,
    fixtureSetHash: evidenceBinding.fixtureSetHash,
    postDeadlineWorkStatus,
    summary: {
      safetyStatus: summary.safetyStatus,
      truthStatus: summary.truthStatus,
      policyStatus: summary.policyStatus,
      reliabilityStatus: summary.reliabilityStatus,
      governanceStatus: summary.governanceStatus,
      liveCoordinateGoldenStatus: summary.liveCoordinateGoldenStatus,
      truthEvaluatedCount: summary.truthEvaluatedCount,
      truthNotEvaluatedCount: summary.truthNotEvaluatedCount,
      policyEvaluatedCount: summary.policyEvaluatedCount,
      policyNotEvaluatedCount: summary.policyNotEvaluatedCount,
      timeoutCount: summary.timeoutCount
    },
    terminalClassification: {
      passCount: summary.pass,
      productFailCount: summary.productFail,
      blockedNoReplayCount: summary.blockedNoReplay,
      blockedFixtureCount: summary.blockedFixture,
      baselineReviewRequiredCount: summary.baselineReviewRequired,
      skipOutOfScopeCount: summary.skipOutOfScope,
      fixtureIds: summary.fixtureIds,
    },
    cases: results.map(buildEvidenceCase)
  };
  await mkdir(path.dirname(artifactPath), { recursive: true });
  await writeFile(artifactPath, `${JSON.stringify(artifact, null, 2)}\n`, 'utf8');
  return artifactPath;
}

export function buildLocalPatchCandidateBindingInput(environment = process.env) {
  return {
    repoRoot,
    candidateSpecId: environment.LOCAL_PATCH_CANDIDATE_SPEC_ID,
    qualificationMode: String(environment.QUALIFICATION_MODE || '').trim().toUpperCase(),
    baseCommit: environment.BASE_COMMIT,
    candidateManifestSha256: environment.CANDIDATE_MANIFEST_SHA256,
    trackedPatchSha256: environment.TRACKED_PATCH_SHA256,
    candidateSourceHash: environment.CANDIDATE_SOURCE_HASH,
    frozenReleaseGovernanceHash: environment.FROZEN_RELEASE_GOVERNANCE_HASH,
    frozenFixtureSetHash: environment.FROZEN_FIXTURE_SET_HASH,
    nodeEnv: environment.NODE_ENV,
    apiUrl: environment.COORDINATE_REGRESSION_API_URL || defaultApiUrl,
    p0DeterministicReplay: environment.P0_DETERMINISTIC_REPLAY,
  };
}

export async function establishEvidenceBinding(environment = process.env) {
  const qualificationMode = String(environment.QUALIFICATION_MODE || 'FROZEN_PRODUCTION').trim().toUpperCase();
  if (qualificationMode === 'LOCAL_PATCH_CANDIDATE') {
    return validateLocalPatchCandidateIdentity(buildLocalPatchCandidateBindingInput(environment));
  }
  if (qualificationMode !== 'FROZEN_PRODUCTION') {
    const error = new Error(`Unsupported QUALIFICATION_MODE: ${qualificationMode}`);
    error.code = 'EVIDENCE_BINDING_MISMATCH';
    throw error;
  }
  const response = await fetch(versionApiUrl);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok || !payload?.runtimeIdentity) {
    const error = new Error(`EVIDENCE_BINDING_MISMATCH: /api/version returned HTTP ${response.status}`);
    error.code = 'EVIDENCE_BINDING_MISMATCH';
    throw error;
  }
  return validateReleaseEvidenceBinding({
    repoRoot,
    canonicalCommit: environment.CANONICAL_RELEASE_COMMIT,
    runtimeIdentity: payload.runtimeIdentity,
    frozenIdentity: {
      productionSourceHash: environment.FROZEN_PRODUCTION_SOURCE_HASH,
      releaseGovernanceHash: environment.FROZEN_RELEASE_GOVERNANCE_HASH,
      fixtureSetHash: environment.FROZEN_FIXTURE_SET_HASH,
    },
  });
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const p0ReplayMode = process.env.P0_DETERMINISTIC_REPLAY === '1';
  if (p0ReplayMode) {
    assertP0ReplayRuntimeSafety({
      apiUrl,
      qualificationMode: process.env.QUALIFICATION_MODE,
      replayEnabled: true,
      nodeEnv: process.env.NODE_ENV,
    });
    options.includeText = true;
    options.p0ReplayManifest = await loadP0ReplayManifest(repoRoot);
  }
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
  if (p0ReplayMode) {
    options.p0GateGovernance = await loadP0ReleaseGateGovernance(repoRoot, baseline);
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

  let governance;
  try {
    governance = await readJson(defaultGovernancePath);
  } catch (error) {
    console.error(`Golden governance load failed: ${defaultGovernancePath}`);
    console.error(error.message || String(error));
    process.exitCode = 1;
    return;
  }
  const governanceErrors = validateGoldenGovernance(governance);
  if (governanceErrors.length) {
    console.error('Golden governance validation failed:');
    governanceErrors.forEach(error => console.error(`- ${error}`));
    process.exitCode = 1;
    return;
  }
  let samples = baseline.samples
    .map(sample => applyGoldenGovernance(sample, governance))
    .filter((sample) => sampleMatchesFilters(sample, options));
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

  let evidenceBinding;
  try {
    evidenceBinding = await establishEvidenceBinding();
  } catch (error) {
    console.error(`EVIDENCE_BINDING_MISMATCH: ${error.message || String(error)}`);
    process.exitCode = 1;
    return;
  }

  console.log('Coordinate Recognition Regression Report');
  console.log(`Baseline: ${path.relative(repoRoot, options.baselinePath)}`);
  console.log(`Error Library: ${path.relative(repoRoot, options.errorLibraryPath)}`);
  console.log(`Golden Governance: ${path.relative(repoRoot, defaultGovernancePath)}`);
  printErrorLibraryStats(errorLibrary);
  console.log(`API: ${apiUrl}`);
  console.log(`Path: ${options.pathId}`);
  console.log(`Mode: ${options.gate ? 'gate' : options.full ? 'full' : 'smoke'}`);
  console.log(`P0 Deterministic Replay: ${p0ReplayMode ? 'ENABLED' : 'DISABLED'}`);
  console.log(`Qualification Mode: ${evidenceBinding.qualificationMode || 'FROZEN_PRODUCTION'}`);
  if (evidenceBinding.qualificationMode === 'LOCAL_PATCH_CANDIDATE') {
    console.log(`Candidate Spec ID: ${evidenceBinding.candidateSpecId}`);
    console.log(`Base Commit Match: ${evidenceBinding.baseCommitMatch}`);
    console.log(`Candidate Manifest SHA-256 Match: ${evidenceBinding.candidateManifestSha256Match}`);
    console.log(`Tracked Patch SHA-256 Match: ${evidenceBinding.trackedPatchSha256Match}`);
    if (evidenceBinding.candidateSourceHashMatch !== null) {
      console.log(`Candidate Source SHA-256 Match: ${evidenceBinding.candidateSourceHashMatch}`);
    }
  }
  console.log(`Samples: ${samples.length}`);

  if (p0ReplayMode) await resetP0ProviderMeasurement();

  const results = [];
  for (const sample of samples) {
    const result = await runSample(sample, options, errorLibrary);

    if (result.status === 'SKIP_OUT_OF_SCOPE' && isBlockingSample(sample, options)) {
      result.status = 'PRODUCT_FAIL';
      result.historicalStatus = result.historicalStatus || 'NOT CHECKED';
      result.diffs.push({
        severity: 'BLOCKER',
        layer: 'Gate',
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
  const providerMeasurement = p0ReplayMode ? await readP0ProviderMeasurement() : null;
  const p0Gate = p0ReplayMode
    ? evaluateP0ReleaseGate(results, evidenceBinding, options.p0GateGovernance, providerMeasurement)
    : null;
  console.log('\nRegression Summary');
  console.log(`Gate Result: ${p0Gate?.status || summary.gateResult}`);
  console.log(`PASS_COUNT=${summary.pass}`);
  console.log(`PRODUCT_FAIL_COUNT=${summary.productFail}`);
  console.log(`BLOCKED_NO_REPLAY_COUNT=${summary.blockedNoReplay}`);
  console.log(`BLOCKED_FIXTURE_COUNT=${summary.blockedFixture}`);
  console.log(`BASELINE_REVIEW_REQUIRED_COUNT=${summary.baselineReviewRequired}`);
  console.log(`SKIP_OUT_OF_SCOPE_COUNT=${summary.skipOutOfScope}`);
  for (const status of RUNNER_TERMINAL_STATUSES) {
    console.log(`${status}_FIXTURES=${summary.fixtureIds[status].join(',') || 'NONE'}`);
  }
  if (p0Gate) {
    console.log(`P0_REQUIRED_FIXTURE_SET=${P0_REQUIRED_FIXTURE_SET.join(',')}`);
    console.log(`P0_REQUIRED_FIXTURE_STATUSES=${JSON.stringify(p0Gate.requiredStatuses)}`);
    console.log(`IDENTITY_BINDING_PASS=${p0Gate.identityBindingPass}`);
    console.log(`ALL_BLOCKED_NO_REPLAY_EXPLICIT=${p0Gate.allNoReplayExplicitlyEnumerated}`);
    console.log(`BLOCKED_NO_REPLAY_ENUMERATION_GATE=${p0Gate.allNoReplayExplicitlyEnumerated ? 'PASS' : 'FAIL'}`);
    console.log(`UNEXPECTED_BLOCKED_NO_REPLAY_FIXTURES=${p0Gate.unexpectedBlockedNoReplayFixtures.join(',') || 'NONE'}`);
    console.log(`MISSING_EXPECTED_BLOCKED_NO_REPLAY_FIXTURES=${p0Gate.missingExpectedBlockedNoReplayFixtures.join(',') || 'NONE'}`);
    console.log(`DIRECTLY_AFFECTED_BLOCKED_NO_REPLAY_FIXTURES=${p0Gate.directlyAffectedBlockedWithoutSubstitute.join(',') || 'NONE_WITHOUT_SUBSTITUTE_DETERMINISTIC_COVERAGE'}`);
    console.log(`DIRECTLY_AFFECTED_BLOCKED_FIXTURE_GATE=${p0Gate.directlyAffectedBlockedWithoutSubstitute.length === 0 ? 'PASS' : 'FAIL'}`);
    console.log(`UNRESOLVED_DIRECTLY_AFFECTED_BASELINE_REVIEW_REQUIRED_COUNT=${p0Gate.unresolvedDirectlyAffectedBaselineReviewRequiredCount}`);
    console.log(`UNAUTHORIZED_PROVIDER_CALLS_MEASURED=${p0Gate.providerCallsMeasured}`);
    console.log(`OBSERVED_PROVIDER_ACQUISITION_ATTEMPTS=${p0Gate.providerMeasurement?.observedProviderAcquisitionAttempts ?? 'UNAVAILABLE'}`);
    console.log(`AUTHORIZED_REPLAY_PROVIDER_CALLS=${p0Gate.providerMeasurement?.authorizedReplayProviderCalls ?? 'UNAVAILABLE'}`);
    console.log(`UNAUTHORIZED_PROVIDER_CALLS=${p0Gate.unauthorizedProviderCalls}`);
    console.log(`P0_RELEASE_GATE=${p0Gate.status}`);
  }
  console.log(`Regression: ${summary.regression}`);
  console.log(`Historical error regressions: ${summary.historicalRegressions}`);
  console.log(`Known open errors: ${summary.knownOpenErrors}`);
  console.log(`BLOCKER: ${summary.blockerCount}`);
  console.log(`WARNING: ${summary.warningCount}`);
  console.log(`INFO: ${summary.infoCount}`);
  console.log(`Layers: ${Object.entries(summary.layerCounts).map(([layer, count]) => `${layer}=${count}`).join(' ') || '(none)'}`);
  console.log(`Timeout: ${summary.timeoutCount}`);
  console.log(`Fallback: ${summary.fallbackCount}`);
  console.log(`Semantic Classifications: ${Object.entries(summary.semanticClassifications).map(([name, count]) => `${name}=${count}`).join(' ') || 'NONE'}`);
  console.log(`SAFETY_STATUS=${summary.safetyStatus}`);
  console.log(`TRUTH_STATUS=${summary.truthStatus}`);
  console.log(`POLICY_STATUS=${summary.policyStatus}`);
  console.log(`RELIABILITY_STATUS=${summary.reliabilityStatus}`);
  console.log(`GOVERNANCE_STATUS=${summary.governanceStatus}`);
  console.log(`LIVE_COORDINATE_GOLDEN_STATUS=${summary.liveCoordinateGoldenStatus}`);
  console.log(`TRUTH_PASS_COUNT=${summary.truthPassCount}`);
  console.log(`TRUTH_FAILURE_COUNT=${summary.truthFailureCount}`);
  console.log(`TRUTH_EVALUATED_COUNT=${summary.truthEvaluatedCount}`);
  console.log(`TRUTH_NOT_EVALUATED_COUNT=${summary.truthNotEvaluatedCount}`);
  console.log(`TRUTH_NOT_QUALIFIED_COUNT=${summary.truthNotQualifiedCount}`);
  console.log(`POLICY_PASS_COUNT=${summary.policyPassCount}`);
  console.log(`POLICY_FAILURE_COUNT=${summary.policyFailureCount}`);
  console.log(`POLICY_EVALUATED_COUNT=${summary.policyEvaluatedCount}`);
  console.log(`POLICY_NOT_EVALUATED_COUNT=${summary.policyNotEvaluatedCount}`);
  console.log(`POLICY_NOT_QUALIFIED_COUNT=${summary.policyNotQualifiedCount}`);
  console.log(`TIMEOUT_COUNT=${summary.timeoutCount}`);
  console.log(`POST_DEADLINE_WORK_STATUS=${summary.postDeadlineWorkStatus}`);
  console.log(`[INFO] Duration: avg=${summary.avgMs ?? 'n/a'}ms min=${summary.minMs ?? 'n/a'}ms max=${summary.maxMs ?? 'n/a'}ms`);

  const evidenceArtifactPath = await writeEvidenceArtifact(results, summary, governance, evidenceBinding);
  console.log(`Evidence Artifact: ${path.relative(repoRoot, evidenceArtifactPath)}`);

  if ((p0Gate?.status || summary.gateResult) === 'FAIL') {
    process.exitCode = 1;
  }
}

if (path.resolve(process.argv[1] || '') === __filename) {
  main().catch((error) => {
    console.error(error.stack || error.message || String(error));
    process.exitCode = 1;
  });
}
