import http from 'node:http';
import { fileURLToPath } from 'node:url';

const loopbackHosts = new Set(['127.0.0.1', 'localhost', '::1']);

export function createMiningJudgeAcceptanceHarness({
  upstreamPort = 32110,
  listenPort = 32111,
  host = '127.0.0.1',
  enabled = process.env.ENABLE_MINING_JUDGE_ACCEPTANCE_HARNESS,
  nodeEnv = process.env.NODE_ENV,
} = {}) {
  if (String(enabled || '') !== '1') throw new Error('MINING_JUDGE_ACCEPTANCE_HARNESS_EXPLICIT_ENABLE_REQUIRED');
  if (String(nodeEnv || '').trim().toLowerCase() === 'production') throw new Error('MINING_JUDGE_ACCEPTANCE_HARNESS_PRODUCTION_FORBIDDEN');
  if (!loopbackHosts.has(String(host || '').toLowerCase())) throw new Error('MINING_JUDGE_ACCEPTANCE_HARNESS_LOOPBACK_REQUIRED');

  const json = (res, body) => {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(JSON.stringify(body));
  };

  return http.createServer((clientReq, clientRes) => {
    const url = new URL(clientReq.url || '/', `http://${host}:${listenPort}`);
    if (url.pathname === '/api/config') {
      clientReq.resume();
      json(clientRes, {
        permissions: { aiJudgeEnabled: true, aiOcrEnabled: true, goldCalculatorEnabled: true },
        featureFlags: { goldCalculatorEnabled: true },
      });
      return;
    }
    if (url.pathname === '/api/user-usage' || url.pathname === '/api/usage/quota') {
      clientReq.resume();
      json(clientRes, {
        success: true,
        convert_remaining: 99,
        judge_remaining: 99,
        quota: { convert_remaining: 99, judge_remaining: 99, is_vip: false },
      });
      return;
    }
    if (url.pathname === '/api/analyze-mining-image' && clientReq.method === 'POST') {
      clientReq.resume();
      json(clientRes, {
        success: true,
        caseId: 'local-qualification-synthetic-judge-result',
        result: [
          '结论：结构线索清晰，可进入下一步人工核验。',
          '等级：B',
          '关键依据：可见连续结构与明确边界。',
          '主要风险：本结果为本地验收用确定性合成响应，不代表真实矿业判断。',
          '下一步：结合现场与检测结果确认。',
        ].join('\n'),
        quota: { convert_remaining: 99, judge_remaining: 99, is_vip: false },
      });
      return;
    }

    const headers = { ...clientReq.headers, host: `127.0.0.1:${upstreamPort}` };
    const upstream = http.request({
      hostname: '127.0.0.1',
      port: upstreamPort,
      path: clientReq.url,
      method: clientReq.method,
      headers,
    }, response => {
      clientRes.writeHead(response.statusCode || 502, response.headers);
      response.pipe(clientRes);
    });
    upstream.on('error', () => {
      clientRes.writeHead(502, { 'content-type': 'application/json; charset=utf-8' });
      clientRes.end(JSON.stringify({ error: 'Local acceptance upstream unavailable.' }));
    });
    clientReq.pipe(upstream);
  });
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  const listenPort = Number(process.env.MINING_JUDGE_HARNESS_PORT || 32111);
  const upstreamPort = Number(process.env.MINING_JUDGE_UPSTREAM_PORT || 32110);
  const server = createMiningJudgeAcceptanceHarness({ listenPort, upstreamPort });
  server.listen(listenPort, '127.0.0.1', () => {
    console.log(`Mining Quick Judge acceptance harness: http://127.0.0.1:${listenPort}`);
  });
}
