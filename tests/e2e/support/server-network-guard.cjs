const http = require("node:http");
const https = require("node:https");
const net = require("node:net");
const tls = require("node:tls");

const LOOPBACK_HOSTS = new Set(["127.0.0.1", "localhost", "::1", "[::1]"]);

function hostFromArgs(args) {
  const first = args[0];
  if (typeof first === "string") {
    try {
      return new URL(first).hostname;
    } catch {
      return first;
    }
  }

  if (first instanceof URL) return first.hostname;
  if (first && typeof first === "object") return first.hostname || first.host || "localhost";
  return "localhost";
}

function assertLoopback(args, protocol) {
  const rawHost = String(hostFromArgs(args));
  const host = rawHost.replace(/^\[/, "").replace(/\]$/, "").split(":")[0];
  if (!LOOPBACK_HOSTS.has(rawHost) && !LOOPBACK_HOSTS.has(host)) {
    const error = new Error(`E2E_EXTERNAL_NETWORK_BLOCKED: ${protocol}//${rawHost}`);
    error.code = "E2E_EXTERNAL_NETWORK_BLOCKED";
    throw error;
  }
}

function guardMethod(target, method, protocol) {
  const original = target[method];
  target[method] = function guardedNetworkCall(...args) {
    assertLoopback(args, protocol);
    return original.apply(this, args);
  };
}

guardMethod(http, "request", "http:");
guardMethod(http, "get", "http:");
guardMethod(https, "request", "https:");
guardMethod(https, "get", "https:");
guardMethod(net, "connect", "tcp:");
guardMethod(net, "createConnection", "tcp:");
guardMethod(tls, "connect", "tls:");

process.stderr.write("[playwright] server outbound network guard active: localhost only\n");
