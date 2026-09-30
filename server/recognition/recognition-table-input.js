// Serialization adapter only. Coordinate parsing, grouping and label validation
// remain owned by recognition-candidate-evidence; no geometry or authority here.
import { extractRecognitionCandidateEvidence } from './recognition-candidate-evidence.js';
import { normalizeProviderDmsReviewResult } from './recognition-review-result.js';

export const TABLE_INPUT_VERSION = 'coordinate_table_input_v1';
const unknown = reason => ({ status: 'UNKNOWN', reason });
const fieldName = key => String(key).toLowerCase().replace(/[\s_.-]/g, '');
const aliases = new Map([
  ...['point', 'pointnumber', 'no', 'number', 'label', 'id'].map(k => [k, 'point']),
  ...['latitude', 'lat'].map(k => [k, 'latitude']),
  ...['longitude', 'lon', 'lng', 'long'].map(k => [k, 'longitude']),
  ...['x', 'easting'].map(k => [k, 'x']), ...['y', 'northing'].map(k => [k, 'y'])
]);
const headers = { point: 'No', latitude: 'Latitude', longitude: 'Longitude', x: 'X', y: 'Y' };
const escapePointer = s => String(s).replace(/~/g, '~0').replace(/\//g, '~1');
const issue = (condition, pointer, field = null) => ({ stage: 'TABLE_INPUT', condition, pointer, field });

// JSON.parse supplies grammar validation. This small syntax walker retains the
// scalar lexemes and duplicate keys that JSON.parse alone would erase. It does
// not interpret coordinates or select one of several row sets.
function jsonTree(text, offset) {
  JSON.parse(text);
  let cursor = 0;
  const ws = () => { while (/\s/.test(text[cursor] || '') && cursor < text.length) cursor++; };
  function scalar() {
    const match = /^(?:"(?:[^"\\]|\\.)*"|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?|true|false|null)/.exec(text.slice(cursor));
    if (!match) throw new Error('JSON_TOKEN_UNAVAILABLE');
    cursor += match[0].length;
    return { value: JSON.parse(match[0]), rawLiteral: match[0] };
  }
  function node(pointer, depth = 0) {
    if (depth > 24) throw new Error('JSON_DEPTH_LIMIT');
    ws(); const start = cursor; let result;
    if (text[cursor] === '{') {
      cursor++; ws(); const entries = [];
      while (text[cursor] !== '}') {
        ws(); const key = scalar().value; ws(); cursor++; // validated colon
        const child = node(`${pointer}/${escapePointer(key)}`, depth + 1);
        entries.push({ key, node: child }); ws();
        if (text[cursor] !== ',') break;
        cursor++;
      }
      cursor++; result = { kind: 'object', entries };
    } else if (text[cursor] === '[') {
      cursor++; ws(); const items = [];
      while (text[cursor] !== ']') {
        items.push(node(`${pointer}/${items.length}`, depth + 1)); ws();
        if (text[cursor] !== ',') break;
        cursor++;
      }
      cursor++; result = { kind: 'array', items };
    } else result = { kind: 'scalar', ...scalar() };
    return { ...result, pointer, span: { start: offset + start, end: offset + cursor, unit: 'UTF16_CODE_UNIT' } };
  }
  return node('');
}

function structuredInput(source) {
  const opening = source.search(/[\[{]/);
  if (opening < 0) return null;
  if (!/^[\[{]\s*(?:["{\[\]}]|$)/.test(source.slice(opening))) return null;
  const closing = source.lastIndexOf(source[opening] === '{' ? '}' : ']');
  if (closing < opening) return { error: 'JSON_INCOMPLETE', pointer: '' };
  try {
    const tree = jsonTree(source.slice(opening, closing + 1), opening);
    return { tree, outside: source.slice(0, opening) + source.slice(closing + 1) };
  } catch {
    // Do not turn ordinary bracketed headings into JSON. Explicit JSON inputs
    // that cannot be safely decoded retain a blocking structural issue.
    if (/^\s*(?:```(?:json)?\s*)?[\[{]/i.test(source)) return { error: 'JSON_INVALID_OR_LIMITED', pointer: '' };
    return null;
  }
}

function sourceField(node, role) {
  return { rawLiteral: node.rawLiteral ?? null,
    decodedValue: node.kind === 'scalar' ? node.value : null,
    normalizedValue: unknown('NOT_YET_PARSED'),
    source: { document: 'ADAPTER_INPUT', serialization: 'JSON', pointer: node.pointer, characterSpan: node.span }, role };
}

function jsonRows(tree, issues) {
  const groups = [];
  const coordinateObject = n => n.kind === 'object'
    && n.entries.some(e => ['latitude', 'longitude', 'x', 'y'].includes(aliases.get(fieldName(e.key))));
  function visit(n) {
    if (n.kind === 'array') {
      if (n.items.some(coordinateObject)) groups.push({ pointer: n.pointer, rows: n.items });
      else n.items.forEach(visit);
    } else if (n.kind === 'object') {
      const seen = new Set();
      for (const e of n.entries) {
        if (seen.has(e.key)) issues.push(issue('DUPLICATE_JSON_KEY', e.node.pointer, e.key));
        seen.add(e.key);
      }
      if (coordinateObject(n)) groups.push({ pointer: n.pointer, rows: [n] });
      else for (const e of n.entries) {
        if (e.node.kind !== 'scalar') visit(e.node);
        else if (!['crs', 'datum', 'projection', 'zone', 'hemisphere', 'axisorder', 'title', 'name'].includes(fieldName(e.key))) {
          issues.push(issue('UNCONSUMED_ENVELOPE_FIELD', e.node.pointer, e.key));
        }
      }
    }
  }
  visit(tree);
  if (groups.length !== 1) issues.push(issue('TABLE_BOUNDARY_UNRESOLVED', tree.pointer));
  return groups;
}

function blockedCandidates(rawText, visibleCrsEvidence, issues) {
  const empty = extractRecognitionCandidateEvidence({ rawText: '', visibleCrsEvidence });
  const rejectedRows = issues.map(i => ({ lineNumber: null, text: '', reason: i.condition, pointer: i.pointer, field: i.field }));
  return { ...empty, rejectedRows, reviewReasons: [...new Set([...empty.reviewReasons, 'CANDIDATE_NORMALIZATION_PARTIAL',
    ...issues.map(i => i.condition)])], diagnostics: { ...empty.diagnostics, rejectedRowCount: rejectedRows.length } };
}

function tableRows(source, candidates) {
  const lines = source.split(/\r?\n/), starts = [];
  let cursor = 0;
  for (const line of lines) { starts.push(cursor); cursor += line.length + (source.slice(cursor + line.length, cursor + line.length + 2) === '\r\n' ? 2 : 1); }
  const rows = candidates.candidateCoordinates.map(candidate => {
    const line = lines[candidate.sourceLineNumber - 1] ?? '';
    const fields = {};
    for (const [key, role] of [['sourceLabel', 'point'], ['latitudeSource', 'latitude'], ['longitudeSource', 'longitude'],
      ['latitude', 'latitude'], ['longitude', 'longitude'], ['x', 'x'], ['y', 'y']]) {
      if (!(key in candidate)) continue;
      const value = candidate[key], literal = typeof value === 'string' && line.includes(value) ? value : null;
      fields[role] = { rawLiteral: literal, decodedValue: literal, normalizedValue: value,
        source: { document: 'ADAPTER_INPUT', lineNumber: candidate.sourceLineNumber,
          characterSpan: unknown('FIELD_SPAN_NOT_PROVIDED_BY_EXISTING_PARSER'), candidateField: key }, role };
    }
    return { sourceLineNumber: candidate.sourceLineNumber, pointLabel: candidate.sourceLabel, representation: candidate.format,
      group: candidates.candidateCoordinateGroups.find(g => g.rows.some(r => r.sourceLineNumber === candidate.sourceLineNumber))?.groupId || null,
      physicalTableIdentity: unknown('PHYSICAL_TABLE_NOT_OBSERVED'),
      rawRow: line, rawRowSpan: { start: starts[candidate.sourceLineNumber - 1], end: starts[candidate.sourceLineNumber - 1] + line.length, unit: 'UTF16_CODE_UNIT' },
      // Preserve auxiliary and unknown columns too; do not manufacture their roles.
      sourceCells: line.split(/[|\t;]/).map(rawLiteral => ({ rawLiteral, role: unknown('COLUMN_ROLE_NOT_EXPOSED') })),
      fields, selectedCandidate: candidate };
  });
  const acceptedLines = new Set(rows.map(row => row.sourceLineNumber));
  for (const rejected of candidates.rejectedRows || []) {
    if (!Number.isSafeInteger(rejected?.lineNumber) || acceptedLines.has(rejected.lineNumber)) continue;
    const line = lines[rejected.lineNumber - 1] ?? String(rejected.text || '');
    rows.push({ sourceLineNumber: rejected.lineNumber, pointLabel: null, representation: 'UNKNOWN', group: null,
      physicalTableIdentity: unknown('PHYSICAL_TABLE_NOT_OBSERVED'),
      rawRow: line, rawRowSpan: { start: starts[rejected.lineNumber - 1], end: starts[rejected.lineNumber - 1] + line.length, unit: 'UTF16_CODE_UNIT' },
      sourceCells: line.split(/[|\t;]/).map(rawLiteral => ({ rawLiteral, role: unknown('COLUMN_ROLE_NOT_EXPOSED') })),
      fields: {}, selectedCandidate: null, rejectionReason: rejected.reason });
  }
  return rows.sort((left, right) => left.sourceLineNumber - right.sourceLineNumber)
    .map((row, index) => ({ ...row, rowOrdinal: index + 1 }));
}

export function adaptRecognitionTableInput({ rawText = '', visibleCrsEvidence = [] } = {}) {
  const source = String(rawText || ''), structured = structuredInput(source), issues = [];
  const evidence = { schemaVersion: TABLE_INPUT_VERSION, rawResponse: source,
    serialization: structured ? 'JSON' : /(^|\n)\s*\|?\s*:?-{3,}:?\s*\|/.test(source) ? 'MARKDOWN' : 'TABLE_OR_TEXT',
    imageIdentity: unknown('NOT_SUPPLIED_TO_INPUT_ADAPTER'), physicalTableIdentity: unknown('PHYSICAL_TABLE_NOT_OBSERVED'),
    visibleCrsEvidence, rows: [], issues, normalizedText: source, authority: 'EVIDENCE_ONLY' };
  if (!structured) {
    const candidates = extractRecognitionCandidateEvidence({ rawText: source, visibleCrsEvidence });
    evidence.rows = tableRows(source, candidates);
    evidence.issues = candidates.rejectedRows.map(row => issue(row.reason, `/lines/${row.lineNumber}`, 'row'));
    return { evidence, candidates };
  }
  if (structured.error) issues.push(issue(structured.error, structured.pointer));
  const groups = structured.tree ? jsonRows(structured.tree, issues) : [];
  if (structured.outside) {
    const outside = extractRecognitionCandidateEvidence({ rawText: structured.outside, visibleCrsEvidence });
    if (outside.candidateCoordinates.length || outside.rejectedRows.length) issues.push(issue('COORDINATES_OUTSIDE_JSON_TABLE', ''));
  }
  let layout = null;
  const normalizedLines = [];
  for (const group of groups) for (const [index, row] of group.rows.entries()) {
    const record = { rowOrdinal: index + 1, pointLabel: null, representation: 'UNKNOWN', group: group.pointer,
      physicalTableIdentity: unknown('JSON_CONTAINER_IS_NOT_PHYSICAL_TABLE'), rawRow: source.slice(row.span.start, row.span.end),
      rawRowSpan: row.span, sourceCells: [], fields: {} };
    evidence.rows.push(record);
    if (row.kind !== 'object') { issues.push(issue('ROW_NOT_OBJECT', row.pointer)); continue; }
    const roles = [], values = new Map();
    for (const { key, node } of row.entries) {
      const role = aliases.get(fieldName(key));
      record.sourceCells.push({ key, ...sourceField(node, role || 'UNKNOWN') });
      if (!role) { issues.push(issue('EXTRA_ROW_FIELD', node.pointer, key)); continue; }
      if (values.has(role)) { issues.push(issue('DUPLICATE_FIELD_ROLE', node.pointer, role)); continue; }
      const field = sourceField(node, role); record.fields[role] = field; roles.push(role);
      if (node.kind !== 'scalar' || !['string', 'number'].includes(typeof node.value)
        || /[|\r\n\t;]/.test(String(node.value))) { issues.push(issue('UNSAFE_OR_NONSCALAR_FIELD', node.pointer, role)); continue; }
      const literalValue = typeof node.value === 'number' ? node.rawLiteral : node.value;
      values.set(role, literalValue);
    }
    const geographic = values.has('latitude') || values.has('longitude'), projected = values.has('x') || values.has('y');
    const required = ['point', ...(geographic ? ['latitude', 'longitude'] : []), ...(projected ? ['x', 'y'] : [])];
    for (const role of required) if (!values.has(role)) issues.push(issue('MISSING_FIELD', row.pointer, role));
    if (!geographic && !projected) issues.push(issue('COORDINATE_PAIR_MISSING', row.pointer));
    // Key names must agree with DMS directions. Never repair a swapped field.
    for (const [role, directions] of [['latitude', /(?:N|S|NORTH|SOUTH|NORD|SUD)$/i], ['longitude', /(?:E|W|O|EAST|WEST|EST|OUEST)$/i]]) {
      if (values.has(role) && /[°º˚]/u.test(values.get(role)) && !directions.test(String(values.get(role)).trim())) {
        issues.push(issue('FIELD_DIRECTION_CONFLICT', row.pointer, role));
      }
    }
    if (geographic && ['latitude', 'longitude'].some(role => /[°º˚]/u.test(String(values.get(role) || '')))) {
      const checked = normalizeProviderDmsReviewResult(`${values.get('point') || ''} | ${values.get('latitude') || ''} | ${values.get('longitude') || ''}`);
      if (checked.candidatePointCount !== 1 || checked.rejectedRows?.length) issues.push(issue('DMS_VALUE_NOT_VALIDATED', row.pointer));
    }
    const columns = ['point', ...roles.filter(role => role !== 'point')];
    if (!layout) { layout = columns; normalizedLines.push(columns.map(r => headers[r]).join(' | ')); }
    if (JSON.stringify(columns) !== JSON.stringify(layout)) issues.push(issue('COLUMN_ORDER_CONFLICT', row.pointer));
    const rowText = columns.map(role => values.get(role) ?? '').join(' | ');
    normalizedLines.push(rowText);
    record.pointLabel = values.get('point') ?? null;
    record.representation = geographic ? projected ? 'MIXED' : 'GEOGRAPHIC' : 'PROJECTED_XY';
    record.normalizedRow = rowText;
  }
  evidence.normalizedText = normalizedLines.join('\n');
  let candidates = issues.length ? blockedCandidates(source, visibleCrsEvidence, issues)
    : extractRecognitionCandidateEvidence({ rawText: evidence.normalizedText, visibleCrsEvidence });
  for (const [index, row] of evidence.rows.entries()) {
    const selected = candidates.candidateCoordinates.find(r => r.sourceLineNumber === index + 2);
    row.selectedCandidate = selected || null;
    if (selected) for (const [role, field] of Object.entries(row.fields)) {
      const key = { point: 'sourceLabel', latitude: 'latitudeSource', longitude: 'longitudeSource', x: 'x', y: 'y' }[role];
      field.normalizedValue = selected[key] ?? selected[role] ?? unknown('NOT_SELECTED_BY_EXISTING_PARSER');
    }
  }
  evidence.issues.push(...candidates.rejectedRows.filter(r => r.lineNumber !== null)
    .map(row => issue(row.reason, `/normalizedLines/${row.lineNumber}`, 'row')));
  return { evidence, candidates };
}

export function adaptRecognitionTableCandidates(input) {
  return adaptRecognitionTableInput(input).candidates;
}
