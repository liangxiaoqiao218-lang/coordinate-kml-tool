import assert from "node:assert/strict";
import net from "node:net";
import path from "node:path";
import { spawn } from "node:child_process";

const rcOrigin = "https://coordinate-kml-tool-rc.onrender.com";
const productionOrigin = "https://geokitlab.com";
const invalidOrigin = "https://untrusted.example";
const disabledDotenvPath = path.resolve("scripts", ".env-origin-guard-regression-disabled");

async function getFreePort() {
  return await new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address ? address.port : 0;
      server.close(error => error ? reject(error) : resolve(port));
    });
  });
}

async function waitForServer(baseUrl, child, logs) {
  const deadline = Date.now() + 10000;

  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(`Server exited before readiness.\n${logs.join("")}`);
    }

    try {
      const response = await fetch(`${baseUrl}/api/version`);
      if (response.ok) return;
    } catch (_) {
      // The child process may still be binding its port.
    }

    await new Promise(resolve => setTimeout(resolve, 50));
  }

  throw new Error(`Server readiness timed out.\n${logs.join("")}`);
}

async function stopServer(child) {
  if (child.exitCode !== null) return;

  child.kill();
  await Promise.race([
    new Promise(resolve => child.once("exit", resolve)),
    new Promise(resolve => setTimeout(resolve, 2000))
  ]);

  if (child.exitCode === null) {
    child.kill("SIGKILL");
  }
}

async function runScenario({
  name,
  configuredOrigin,
  requestOrigin,
  expectedStatus,
  expectedReason
}) {
  const port = await getFreePort();
  const baseUrl = `http://127.0.0.1:${port}`;
  const logs = [];
  const env = {
    ...process.env,
    PORT: String(port),
    NODE_ENV: "production",
    DOTENV_CONFIG_PATH: disabledDotenvPath,
    SUPABASE_URL: "",
    SUPABASE_SERVICE_ROLE_KEY: ""
  };

  if (configuredOrigin === undefined) {
    delete env.RC_ALLOWED_ORIGIN;
  } else {
    env.RC_ALLOWED_ORIGIN = configuredOrigin;
  }

  const child = spawn(process.execPath, ["server.js"], {
    cwd: process.cwd(),
    env,
    stdio: ["ignore", "pipe", "pipe"]
  });
  child.stdout.on("data", chunk => logs.push(chunk.toString()));
  child.stderr.on("data", chunk => logs.push(chunk.toString()));

  try {
    await waitForServer(baseUrl, child, logs);
    const response = await fetch(`${baseUrl}/api/usage/quota?visitorId=origin-guard-regression`, {
      headers: { Origin: requestOrigin }
    });
    const payload = await response.json();

    assert.equal(response.status, expectedStatus, `${name}: unexpected status`);
    assert.equal(payload.reason, expectedReason, `${name}: unexpected reason`);

    return {
      id: name,
      status: "PASS",
      http_status: response.status,
      reason: payload.reason
    };
  } finally {
    await stopServer(child);
  }
}

const cases = [];
cases.push(await runScenario({
  name: "default_rejects_rc_origin",
  configuredOrigin: undefined,
  requestOrigin: rcOrigin,
  expectedStatus: 403,
  expectedReason: "invalid_origin"
}));
cases.push(await runScenario({
  name: "configured_rc_origin_passes_guard",
  configuredOrigin: rcOrigin,
  requestOrigin: rcOrigin,
  expectedStatus: 500,
  expectedReason: "db_disabled"
}));
cases.push(await runScenario({
  name: "production_origin_unchanged",
  configuredOrigin: undefined,
  requestOrigin: productionOrigin,
  expectedStatus: 500,
  expectedReason: "db_disabled"
}));
cases.push(await runScenario({
  name: "configured_rc_still_rejects_untrusted_origin",
  configuredOrigin: rcOrigin,
  requestOrigin: invalidOrigin,
  expectedStatus: 403,
  expectedReason: "invalid_origin"
}));
cases.push(await runScenario({
  name: "multi_value_configuration_is_rejected",
  configuredOrigin: `${rcOrigin},${invalidOrigin}`,
  requestOrigin: rcOrigin,
  expectedStatus: 403,
  expectedReason: "invalid_origin"
}));
cases.push(await runScenario({
  name: "wildcard_configuration_is_rejected",
  configuredOrigin: "https://*.onrender.com",
  requestOrigin: rcOrigin,
  expectedStatus: 403,
  expectedReason: "invalid_origin"
}));
cases.push(await runScenario({
  name: "non_origin_url_configuration_is_rejected",
  configuredOrigin: `${rcOrigin}/path?unexpected=true`,
  requestOrigin: rcOrigin,
  expectedStatus: 403,
  expectedReason: "invalid_origin"
}));

console.log(JSON.stringify({
  suite: "origin-guard-rc-allowlist-regression",
  passed: cases.length,
  cases
}, null, 2));
