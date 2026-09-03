import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

// A fresh child observes the real entrypoint, including the unmodified server import.
// No fixture acquisition or external network operation is permitted by this probe.
if (process.argv[2] === '--probe') {
  const { default: http } = await import('node:http');
  const { default: net } = await import('node:net');
  const { default: fsPromises } = await import('node:fs/promises');
  const { registerHooks, syncBuiltinESMExports } = await import('node:module');
  const observed = {
    originalNodeEnv: process.env.NODE_ENV,
    envWrites: [], fixtureLoads: 0, replayImports: 0, serverImports: 0,
    serverCreates: 0, listenCalls: 0, networkAttempts: 0, addresses: [],
    badBindRejected: [], error: null,
  };
  process.env = new Proxy(process.env, {
    set(target, key, value) {
      observed.envWrites.push(String(key));
      target[key] = value;
      return true;
    },
    deleteProperty(target, key) {
      observed.envWrites.push(String(key));
      return delete target[key];
    },
  });
  registerHooks({
    load(url, context, nextLoad) {
      if (url.endsWith('/server.js')) observed.serverImports += 1;
      if (url.endsWith('/p0-deterministic-replay.js')) observed.replayImports += 1;
      return nextLoad(url, context);
    },
  });
  const nativeRead = fsPromises.readFile;
  fsPromises.readFile = function (file, ...args) {
    if (/p0-deterministic-replay-manifest|regression-samples/.test(String(file))) observed.fixtureLoads += 1;
    return nativeRead.call(this, file, ...args);
  };
  syncBuiltinESMExports();
  const denyNetwork = () => {
    observed.networkAttempts += 1;
    throw new Error('ISOLATION_TEST_NETWORK_FORBIDDEN');
  };
  globalThis.fetch = denyNetwork;
  net.Socket.prototype.connect = denyNetwork;
  const servers = [];
  const nativeCreate = http.createServer;
  http.createServer = function (...args) {
    observed.serverCreates += 1;
    const server = nativeCreate.apply(this, args);
    servers.push(server);
    return server;
  };
  const nativeListen = http.Server.prototype.listen;
  http.Server.prototype.listen = function (...args) {
    observed.listenCalls += 1;
    return nativeListen.apply(this, args);
  };
  try {
    await import(pathToFileURL(path.join(repoRoot, 'scripts', process.argv[3])).href);
    await Promise.all(servers.map(server => server.listening ? Promise.resolve() :
      new Promise((resolve, reject) => { server.once('listening', resolve); server.once('error', reject); })));
    observed.addresses = servers.map(server => server.address().address);
    if (process.argv[3].includes('candidate')) {
      for (const host of ['0.0.0.0', '::', '192.168.1.2', '8.8.8.8']) {
        const server = http.createServer();
        try { server.listen(0, host); } catch (error) {
          if (error.message === 'P0_REPLAY_LOOPBACK_BIND_REQUIRED') observed.badBindRejected.push(host);
        }
      }
    }
  } catch (error) {
    observed.error = error.message;
  }
  await Promise.all(servers.filter(server => server.listening).map(server => new Promise(resolve => server.close(resolve))));
  console.log(`ISOLATION_PROBE=${JSON.stringify(observed)}`);
  process.exit(0);
}

let passed = 0;
async function test(name, fn) {
  await fn();
  console.log(`PASS ${name}`);
  passed += 1;
}

function probe(entrypoint, nodeEnv, bindHost = '127.0.0.1') {
  const result = spawnSync(process.execPath, [fileURLToPath(import.meta.url), '--probe', entrypoint], {
    cwd: repoRoot,
    env: {
      SystemRoot: process.env.SystemRoot, PATH: process.env.PATH,
      NODE_ENV: nodeEnv, P0_REPLAY_BIND_HOST: bindHost,
      P0_REPLAY_CANDIDATE_PORT: '0', P0_REPLAY_METRICS_PORT: '0', P0_REPLAY_PROVIDER_PORT: '0',
    },
    encoding: 'utf8', timeout: 30000, windowsHide: true,
  });
  assert.equal(result.status, 0, result.error?.message || 'probe did not terminate cleanly');
  const line = result.stdout.split(/\r?\n/).find(value => value.startsWith('ISOLATION_PROBE='));
  assert.ok(line, 'missing child observation');
  return JSON.parse(line.slice('ISOLATION_PROBE='.length));
}

function assertBeforeSideEffects(result, expectedError) {
  assert.equal(result.error, expectedError);
  assert.deepEqual(result.envWrites, []);
  for (const field of ['fixtureLoads', 'replayImports', 'serverImports', 'serverCreates', 'listenCalls', 'networkAttempts']) {
    assert.equal(result[field], 0, field);
  }
  assert.deepEqual(result.addresses, []);
}

for (const entrypoint of ['p0-deterministic-replay-candidate.js', 'p0-deterministic-replay-provider.js']) {
  await test(`${entrypoint}: original production rejects before mutation/import/fixture/listen`, () => {
    const result = probe(entrypoint, 'production');
    assert.equal(result.originalNodeEnv, 'production');
    assertBeforeSideEffects(result, 'P0_REPLAY_PRODUCTION_MODE_FORBIDDEN');
  });
  await test(`${entrypoint}: normalized production also rejects`, () => {
    assertBeforeSideEffects(probe(entrypoint, ' Production '), 'P0_REPLAY_PRODUCTION_MODE_FORBIDDEN');
  });
  for (const host of ['0.0.0.0', '::', '192.168.1.2', '8.8.8.8']) {
    await test(`${entrypoint}: rejects bind ${host} before side effects`, () => {
      assertBeforeSideEffects(probe(entrypoint, 'test', host), 'P0_REPLAY_LOOPBACK_BIND_REQUIRED');
    });
  }
  await test(`${entrypoint}: test startup binds real sockets to loopback only`, () => {
    const result = probe(entrypoint, 'test');
    assert.equal(result.error, null);
    assert.equal(result.networkAttempts, 0);
    assert.deepEqual(result.addresses, entrypoint.includes('candidate') ? ['127.0.0.1', '127.0.0.1'] : ['127.0.0.1']);
    if (entrypoint.includes('candidate')) {
      assert.equal(result.serverImports, 1);
      assert.equal(result.listenCalls, 2);
      assert.deepEqual(result.badBindRejected, ['0.0.0.0', '::', '192.168.1.2', '8.8.8.8']);
    } else {
      assert.equal(result.fixtureLoads, 1);
      assert.equal(result.serverImports, 0);
    }
  });
}

await test('production application source contains no replay runtime imports/references', async () => {
  const files = ['server.js', 'index.html'];
  async function walk(directory) {
    for (const entry of await readdir(path.join(repoRoot, directory), { withFileTypes: true })) {
      const relative = path.join(directory, entry.name);
      if (entry.isDirectory()) await walk(relative);
      else if (/\.(?:js|mjs|cjs|html)$/.test(entry.name)) files.push(relative);
    }
  }
  await walk('server');
  await walk('assets');
  for (const file of files) {
    assert.doesNotMatch(await readFile(path.join(repoRoot, file), 'utf8'), /p0-deterministic-replay(?:-candidate|-provider|-manifest)?/i, file);
  }
  const pkg = JSON.parse(await readFile(path.join(repoRoot, 'package.json'), 'utf8'));
  assert.equal(pkg.scripts.start, 'node server.js');
});

console.log(`Replay entrypoint isolation regression: ${passed}/${passed} PASS`);
