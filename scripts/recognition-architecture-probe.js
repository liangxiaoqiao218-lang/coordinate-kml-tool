// Offline characterization, not a replacement parser or production entry point.
import { readFileSync } from 'node:fs';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import vm from 'node:vm';
import crypto from 'node:crypto';
import * as values from '../server/coordinate-values.js';
import * as reasons from '../server/coordinate-review-reason.js';
import * as boundary from '../server/structured-coordinate-boundary.js';
import * as routing from '../server/recognition/family-primary-routing.js';
import * as structure from '../server/recognition/dms-source-structure.js';
import { normalizeRecognitionCandidateRowContent } from '../server/recognition/recognition-candidate-evidence.js';
import { utmToWgs84 } from '../server/projection/utm.js';
import { bftmToWgs84 } from '../server/projection/bftm.js';
import { LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS } from '../server/evidence-acquisition/local-ocr-map-layout-classifier.js';

export const retiredNames = [
  'groupEveryFourDmsLinesWhenLikely', 'looksLikeProjectedContext',
  'hasCompleteProviderDmsPair', 'normalizeCommaDmsCoordinateDisplayOrder'
];

export function declarations(source) {
  const entries = [];
  for (const start of source.matchAll(/^(?:async )?function (\w+)\(/gm)) {
    const tail = source.slice(start.index);
    for (const end of tail.matchAll(/^\}/gm)) {
      const text = tail.slice(0, end.index + 1);
      try { new vm.Script(text); entries.push({ name: start[1], start: start.index, text }); break; }
      catch { /* Continue to the complete top-level declaration. */ }
    }
  }
  return entries;
}

export function createProductRuntime(source) {
  const runtime = vm.createContext({ ...values, ...reasons, ...boundary, ...routing, ...structure,
    normalizeRecognitionCandidateRowContent,
    utmToWgs84, bftmToWgs84, LOCAL_OCR_STRUCTURE_NORMALIZATION_STATUS, crypto, Buffer,
    fs, path, __dirname: fileURLToPath(new URL('../', import.meta.url)),
    process: { env: {} } });
  vm.runInContext(source.match(/^let spatialKnowledgeBaseCache = null;$/m)[0], runtime);
  vm.runInContext(declarations(source).map(entry => entry.text).join('\n'), runtime);
  for (const name of ['coordinateEngineV2CountryProfiles', 'coordinateEngineV2ProjectedReadyTypes']) {
    const tail = source.slice(source.indexOf(`const ${name} =`));
    for (const end of tail.matchAll(/;\r?\n/g)) {
      const candidate = tail.slice(0, end.index + 1);
      try { new vm.Script(candidate); vm.runInContext(candidate, runtime); break; }
      catch (error) { if (!(error instanceof SyntaxError)) throw error; }
    }
  }
  for (const name of ['noCoordinatesText', 'MGRS_BANDS', 'MGRS_COLUMN_SETS', 'MGRS_ROW_SETS',
    'MOZAMBIQUE_TETE_KNOWN_ROW_TOLERANCE', 'PROJECTED_DMS_REFERENCE_TOLERANCE_DEGREES']) {
    vm.runInContext(source.match(new RegExp(`^const ${name} = .+;$`, 'm'))[0], runtime);
  }
  return runtime;
}

export function characterize(source) {
  const runtime = createProductRuntime(source);
  // Synthetic evidence across several zones, hemispheres and boundary sizes.
  // The actual production conversion, reference check and geometry check run unchanged.
  const cases = [];
  for (const [zone, hemisphere, size] of [[31, 'N', 300], [18, 'S', 750], [47, 'N', 125]]) {
    const rows = [[0,0], [size,0], [size,size], [0,size]].map(([dx,dy], index) => {
      const x = 510000 + dx, y = 2100000 + dy;
      const point = utmToWgs84(zone, x, y, hemisphere === 'N');
      return { label: String(index + 1), x: String(x), y: String(y),
        referenceDms: { latitudeDecimal: point.lat, longitudeDecimal: point.lon } };
    });
    const complete = { status: 'COMPLETE', rows, rowCount: rows.length, axisOrder: 'easting_northing',
      crsEvidence: { status: 'EXPLICIT', projection: 'utm', zone, hemisphere },
      diagnostics: { headerPresent: true, parsedProjectedRowCount: rows.length, rejectedProjectedCandidateLineCount: 0 } };
    for (const kind of ['consistent', 'reference_conflict', 'partial_reference', 'axis_missing', 'point_order_conflict', 'self_intersection']) {
      const evidence = structuredClone(complete);
      if (kind === 'reference_conflict') evidence.rows[1].referenceDms.latitudeDecimal += 0.00002;
      if (kind === 'partial_reference') delete evidence.rows[1].referenceDms;
      if (kind === 'axis_missing') evidence.axisOrder = null;
      if (kind === 'point_order_conflict') [evidence.rows[1], evidence.rows[2]] = [evidence.rows[2], evidence.rows[1]];
      if (kind === 'self_intersection') {
        [evidence.rows[1], evidence.rows[2]] = [evidence.rows[2], evidence.rows[1]];
        evidence.rows.forEach((row, index) => { row.label = String(index + 1); });
      }
      // Call converter separately as well: AutoRelease catches exceptions, which could
      // otherwise conceal a missing sandbox dependency as a genuine blocked outcome.
      const converted = runtime.buildConfirmedProjectedCoordinateEngine(evidence.rows, `utm${zone}${hemisphere.toLowerCase()}`, { axisOrder: 'easting_northing' });
      const points = converted?.groups?.[0]?.points || [];
      const engine = runtime.buildExplicitProjectedBoundaryAutoReleaseEngine({ evidence });
      cases.push({ zone, hemisphere, kind, parsedRows: rows.length, sourceCrs: evidence.crsEvidence.status,
        pointSequenceValid: runtime.hasContinuousProjectedPointNumbers(evidence.rows),
        referenceMatch: runtime.projectedDmsReferencesMatch(evidence.rows, points),
        boundaryValid: runtime.hasNonDegenerateProjectedBoundary(points),
        releaseEngine: engine === null ? null : { source_crs: engine.source_crs, points: engine.groups[0].points.map(p => [p.lon, p.lat]) } });
    }
  }
  return cases;
}

if (process.argv[1]?.endsWith('recognition-architecture-probe.js')) {
  const source = readFileSync(new URL('../server.js', import.meta.url), 'utf8');
  console.log(JSON.stringify({ cases: characterize(source), realProviderCalls: 0 }, null, 2));
}
