// Audit-only preload; never imported by production. Refuse non-loopback sockets,
// and preserve the guard even when an existing regression replaces a child's env.
const net = require('node:net');
const childProcess = require('node:child_process');
const { syncBuiltinESMExports } = require('node:module');
const guardOption = `--require=${JSON.stringify(__filename)}`;
const originalConnect = net.Socket.prototype.connect;
net.Socket.prototype.connect = function (...args) {
  const normalized = Array.isArray(args[0]) ? args[0] : args;
  const first = normalized[0];
  const options = first && typeof first === 'object' ? first : null;
  const host = options ? options.host : typeof normalized[1] === 'string' ? normalized[1] : 'localhost';
  if (host && !['localhost', '127.0.0.1', '::1', '[::1]', '::ffff:127.0.0.1'].includes(String(host))) {
    throw new Error('AUDIT_OFFLINE_NON_LOOPBACK_SOCKET_BLOCKED');
  }
  return originalConnect.apply(this, args);
};
const originalSpawn = childProcess.spawn;
childProcess.spawn = function (command, args, options = {}) {
  const env = { ...process.env, ...(options.env || {}) };
  // When a test supplies a minimal env, keep it minimal, not inherited credentials.
  const childEnv = options.env ? { ...options.env } : env;
  childEnv.NODE_OPTIONS = `${childEnv.NODE_OPTIONS || ''} ${guardOption}`.trim();
  return originalSpawn(command, args, { ...options, env: childEnv, windowsHide: true });
};
syncBuiltinESMExports();
