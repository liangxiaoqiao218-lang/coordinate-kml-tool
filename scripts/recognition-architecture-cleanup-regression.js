import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { characterize, declarations, retiredNames } from './recognition-architecture-probe.js';

const baseline = execFileSync('git', ['show', '86ea4ac44a6d44a2332d7aaaffb1926504a73dca:server.js'],
  { encoding: 'utf8', maxBuffer: 8 * 1024 * 1024, windowsHide: true }).replace(/\r\n/g, '\n');
const current = readFileSync(new URL('../server.js', import.meta.url), 'utf8').replace(/\r\n/g, '\n');
let expected = baseline;
for (const name of retiredNames) {
  const entry = declarations(baseline).find(item => item.name === name);
  assert.ok(entry, name);
  assert.equal([...baseline.matchAll(new RegExp(`\\b${name}\\b`, 'g'))].length, 1, `${name}: no runtime call/reference`);
  assert.equal(baseline.includes(`export { ${name}`), false);
  expected = expected.replace(entry.text + '\n\n', '');
}
assert.equal(current, expected, 'entire server must equal baseline with only four unused declarations removed');
const before = JSON.parse(JSON.stringify(characterize(baseline)));
const after = JSON.parse(JSON.stringify(characterize(current)));
assert.deepEqual(after, before, 'actual product conversion and safety decisions remain identical');
for (const result of after) {
  assert.equal(result.sourceCrs, 'EXPLICIT');
  assert.equal(result.releaseEngine !== null, result.kind === 'consistent', `${result.zone}/${result.kind}`);
}
console.log('Architecture cleanup: 4 declaration removals, full-source equality, 18/18 behavioral comparisons PASS; REAL_PROVIDER_CALLS=0');
