import http from 'node:http';
import { assertP0ReplayRuntimeSafety, isLoopbackUrl } from './p0-deterministic-replay.js';

process.env.PORT = process.env.P0_REPLAY_CANDIDATE_PORT || '32121';
process.env.ENABLE_REGRESSION_TEST_MODE = 'true';
process.env.REGRESSION_TEST_MODE = 'true';
process.env.NODE_ENV = 'test';
process.env.ALIYUN_API_KEY = 'local-deterministic-replay-placeholder';
process.env.DASHSCOPE_API_KEY = '';
process.env.ALIYUN_BASE_URL = `http://127.0.0.1:${process.env.P0_REPLAY_PROVIDER_PORT || '32120'}/v1`;

assertP0ReplayRuntimeSafety({
  apiUrl: `http://127.0.0.1:${process.env.PORT}/api/recognize-coordinates`,
  qualificationMode: 'LOCAL_PATCH_CANDIDATE',
  replayEnabled: true,
  nodeEnv: process.env.NODE_ENV,
});

const nativeFetch = globalThis.fetch.bind(globalThis);
const measurement = {
  measurementActive: true,
  observedProviderAcquisitionAttempts: 0,
  authorizedReplayProviderCalls: 0,
  unauthorizedProviderCalls: 0,
};
const replayProviderOrigin = new URL(process.env.ALIYUN_BASE_URL).origin;

globalThis.fetch = async (input, init) => {
  const value = typeof input === 'string' || input instanceof URL ? String(input) : String(input?.url || '');
  const url = new URL(value);
  if (/\/chat\/completions$/i.test(url.pathname)) {
    measurement.observedProviderAcquisitionAttempts += 1;
    if (url.origin === replayProviderOrigin && isLoopbackUrl(url.href)) {
      measurement.authorizedReplayProviderCalls += 1;
    } else {
      measurement.unauthorizedProviderCalls += 1;
      throw new Error('P0_UNAUTHORIZED_PROVIDER_CALL_BLOCKED');
    }
  } else if (!isLoopbackUrl(url.href)) {
    measurement.unauthorizedProviderCalls += 1;
    throw new Error('P0_NON_LOOPBACK_NETWORK_BLOCKED');
  }
  return nativeFetch(input, init);
};

const metricsPort = Number(process.env.P0_REPLAY_METRICS_PORT || 32122);
http.createServer((req, res) => {
  if (req.method === 'POST' && req.url === '/reset') {
    measurement.observedProviderAcquisitionAttempts = 0;
    measurement.authorizedReplayProviderCalls = 0;
    measurement.unauthorizedProviderCalls = 0;
    res.writeHead(204);
    res.end();
    return;
  }
  if (req.method === 'GET' && req.url === '/metrics') {
    res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    res.end(JSON.stringify(measurement));
    return;
  }
  res.writeHead(404);
  res.end();
}).listen(metricsPort, '127.0.0.1', () => console.log(`P0 provider-call measurement: ${metricsPort}`));

await import('../server.js');
