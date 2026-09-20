import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
const server = fs.readFileSync(path.join(root, 'server.js'), 'utf8');

assert.match(html, /import\("\/assets\/agentic-coordinate\/agentic-coordinate-controller\.js"\)/);
assert.match(html, /agenticCoordinateController\?\.enabled[\s\S]*recognizeImageWithAgenticCoordinate/);
assert.match(html, /agenticCoordinateController\.edit\(input\.value\)/);
assert.match(html, /if \(agenticCoordinateController\?\.enabled\) return openAgenticSpatialResult\(\)/);
assert.match(html, /if \(agenticCoordinateController\?\.enabled\)[\s\S]*return downloadAgenticCoordinateKml\(\)/);
assert.match(html, /agenticCoordinateController\?\.enabled[\s\S]*consumeUsage\("convert"\)[\s\S]*downloadAgenticCoordinateKml/);
assert.match(html, /outcome\.map\.geometryHash[\s\S]*outcome\.kml\.geometryHash/);
assert.match(html, /input\.value = String\(workspace\.currentText \|\| ""\)/);

assert.match(server, /\/api\/agentic-coordinate\/v1\/status/);
assert.match(server, /AGENTIC_COORDINATE_V1_ENABLED/);
assert.match(server, /requireAgenticCoordinateApiEnabled/);

const scriptMatch = /<script>\s*const APP_VERSION = "v1\.0\.2";/.exec(html);
assert.ok(scriptMatch, 'main inline script must exist');
const sourceStart = scriptMatch.index + '<script>'.length;
const sourceEnd = html.indexOf('</script>', sourceStart);
assert.ok(sourceEnd > sourceStart, 'main inline script must close');
new vm.Script(html.slice(sourceStart, sourceEnd), { filename: 'index-inline.js' });

console.log('agentic coordinate UI v1 regression: PASS');
