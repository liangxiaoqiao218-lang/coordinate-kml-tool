// Independent guard; never rely on the candidate wrapper to protect this entrypoint.
if (String(process.env.NODE_ENV || '').trim().toLowerCase() === 'production') {
  throw new Error('P0_REPLAY_PRODUCTION_MODE_FORBIDDEN');
}
const bindHost = process.env.P0_REPLAY_BIND_HOST || '127.0.0.1';
if (bindHost !== '127.0.0.1') throw new Error('P0_REPLAY_LOOPBACK_BIND_REQUIRED');

const { createHash } = await import('node:crypto');
const { default: http } = await import('node:http');
const { default: path } = await import('node:path');
const { fileURLToPath } = await import('node:url');
const { loadP0ReplayManifest } = await import('./p0-deterministic-replay.js');

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const manifest = await loadP0ReplayManifest(repoRoot);
const byHash = new Map(manifest.records.map(record => [record.sha256, record]));
const port = Number(process.env.P0_REPLAY_PROVIDER_PORT || 32120);

http.createServer((req, res) => {
  const chunks = [];
  req.on('data', chunk => chunks.push(chunk));
  req.on('end', () => {
    let payload;
    try {
      payload = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    } catch {
      res.writeHead(400, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: { code: 'REPLAY_REQUEST_INVALID' } }));
      return;
    }
    const imageUrl = payload?.messages?.[0]?.content?.find(item => item?.type === 'image_url')?.image_url?.url || '';
    const match = imageUrl.match(/^data:[^;]+;base64,(.+)$/);
    const sha256 = match ? createHash('sha256').update(Buffer.from(match[1], 'base64')).digest('hex') : '';
    const record = byHash.get(sha256);
    if (!record) {
      res.writeHead(422, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: { code: 'P0_REPLAY_FIXTURE_NOT_APPROVED' } }));
      return;
    }
    res.writeHead(200, { 'content-type': 'application/json', 'x-p0-replay-case-id': record.caseId });
    res.end(JSON.stringify({
      request_id: `p0-replay-${sha256.slice(0, 12)}`,
      choices: [{ message: { content: record.approvedAcquisitionLines.join('\n') } }],
    }));
  });
}).listen(port, bindHost, () => console.log(`P0 deterministic acquisition replay provider: ${port}`));
