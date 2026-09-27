import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const expectedIdentity = Object.freeze({
  commit: '3405614b227c2268691b3ac121eb6d4e96867201',
  branch: 'codex/coordinate-agent-phase8-shadow-deploy',
});
const child = spawn(
  process.execPath,
  [
    path.join(root, 'scripts', 'coordinate-agent-phase06-shadow-server.js'),
    '--enable-shadow',
    '--port=0',
    '--bind=0.0.0.0',
  ],
  {
    cwd: root,
    env: {
      ...process.env,
      RENDER_GIT_COMMIT: expectedIdentity.commit,
      RENDER_GIT_BRANCH: expectedIdentity.branch,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  },
);

let stderr = '';
child.stderr.setEncoding('utf8');
child.stderr.on('data', chunk => { stderr += chunk; });

const startup = await new Promise((resolve, reject) => {
  let stdout = '';
  const timeout = setTimeout(() => reject(new Error('Shadow startup timed out')), 15_000);
  child.stdout.setEncoding('utf8');
  child.stdout.on('data', chunk => {
    stdout += chunk;
    const newlineIndex = stdout.indexOf('\n');
    if (newlineIndex < 0) return;
    clearTimeout(timeout);
    try {
      resolve(JSON.parse(stdout.slice(0, newlineIndex)));
    } catch (error) {
      reject(error);
    }
  });
  child.once('error', error => {
    clearTimeout(timeout);
    reject(error);
  });
  child.once('exit', code => {
    if (code !== null) {
      clearTimeout(timeout);
      reject(new Error(`Shadow exited before startup (${code}): ${stderr}`));
    }
  });
});

assert.equal(startup.status, 'READY');
assert.equal(startup.bindAddress, '0.0.0.0');
assert.equal(startup.realProviderCallCount, 0);
assert.deepEqual(startup.runtimeIdentity, expectedIdentity);

const baseUrl = `http://127.0.0.1:${startup.port}${startup.routePrefix}`;
try {
  const healthResponse = await fetch(`${baseUrl}/health`);
  assert.equal(healthResponse.status, 200);
  const health = await healthResponse.json();
  assert.equal(health.status, 'READY');
  assert.equal(health.boundary.shadowOnly, true);
  assert.equal(health.boundary.providerMode, 'replay');
  assert.equal(health.boundary.affectsParser, false);
  assert.equal(health.boundary.affectsCoordinates, false);
  assert.equal(health.boundary.affectsMap, false);
  assert.equal(health.boundary.affectsKml, false);
  assert.deepEqual(health.runtimeIdentity, expectedIdentity);

  const evaluateResponse = await fetch(`${baseUrl}/evaluate`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ caseId: 'generic-confirmed-multigroup' }),
  });
  assert.equal(evaluateResponse.status, 200);
  const evaluation = await evaluateResponse.json();
  assert.equal(evaluation.boundary.providerMode, 'replay');
  assert.equal(evaluation.result.execution.realProviderCallCount, 0);
} finally {
  child.kill();
  await new Promise(resolve => child.once('exit', resolve));
}

console.log(JSON.stringify({
  suite: 'coordinate-agent-phase08-deployment-regression',
  status: 'PASS',
  bindAddress: startup.bindAddress,
  portSource: 'platform-compatible',
  runtimeIdentity: expectedIdentity,
  realProviderCallCount: 0,
  productionRouteMounted: false,
}, null, 2));
