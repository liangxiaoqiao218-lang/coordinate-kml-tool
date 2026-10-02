import assert from "node:assert/strict";
import { spawn, execFileSync } from "node:child_process";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const receiptRoot = path.resolve(String(process.env.RC_ASYNC_FOCUS_RECEIPT_DIR || "").trim());
if (!process.env.RC_ASYNC_FOCUS_RECEIPT_DIR || existsSync(receiptRoot)) {
  throw new Error("RC_ASYNC_FOCUS_NEW_RECEIPT_DIR_REQUIRED");
}
mkdirSync(receiptRoot, { recursive: false });

const chromePath = String(process.env.CHROME_PATH || "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe");
if (!existsSync(chromePath)) throw new Error("CHROME_NOT_FOUND");
const head = execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim();
const worktreeStatus = execFileSync("git", ["status", "--short"], { cwd: root, encoding: "utf8" }).trim();
const startedAt = new Date().toISOString();
const serverLogs = [];
const chromeLogs = [];
const results = [];

function sanitizedEnvironment(extra = {}) {
  const env = { ...process.env };
  for (const name of Object.keys(env)) {
    if (/PROVIDER|SUPABASE|ALIYUN|DASHSCOPE|OPENAI|ANTHROPIC|GEMINI|API_KEY|API_TOKEN|SECRET|USAGE|PASSWORD|COOKIE|AUTHORIZATION/i.test(name)) {
      env[name] = "";
    }
  }
  return {
    ...env,
    NODE_ENV: "test",
    DOTENV_CONFIG_PATH: path.join(root, "__no_rc_async_focus_environment__"),
    NODE_OPTIONS: `--require=${path.join(root, "scripts", "recognition-audit-offline-guard.cjs")}`,
    ...extra
  };
}

async function waitUntil(check, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs;
  let lastError;
  while (Date.now() < deadline) {
    try {
      const value = await check();
      if (value) return value;
    } catch (error) {
      lastError = error;
    }
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  throw new Error(`${label}_TIMEOUT${lastError ? `: ${lastError.message}` : ""}`);
}

async function withServer(port, env, callback) {
  const output = [];
  const server = spawn(process.execPath, ["server.js"], {
    cwd: root,
    env: sanitizedEnvironment({ PORT: String(port), ...env }),
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"]
  });
  server.stdout.on("data", chunk => output.push(String(chunk)));
  server.stderr.on("data", chunk => output.push(String(chunk)));
  try {
    await waitUntil(async () => {
      const response = await fetch(`http://127.0.0.1:${port}/api/version`);
      return response.ok;
    }, 30000, `SERVER_${port}`);
    return await callback(`http://127.0.0.1:${port}`);
  } finally {
    if (!server.killed) server.kill();
    serverLogs.push(`\n--- port ${port} ---\n${output.join("")}`);
  }
}

class CdpConnection {
  constructor(url) {
    this.nextId = 1;
    this.pending = new Map();
    this.socket = new WebSocket(url);
  }

  async open() {
    await new Promise((resolve, reject) => {
      this.socket.addEventListener("open", resolve, { once: true });
      this.socket.addEventListener("error", reject, { once: true });
    });
    this.socket.addEventListener("message", event => {
      const message = JSON.parse(String(event.data));
      if (!message.id || !this.pending.has(message.id)) return;
      const pending = this.pending.get(message.id);
      this.pending.delete(message.id);
      if (message.error) pending.reject(new Error(`${message.error.code}: ${message.error.message}`));
      else pending.resolve(message.result);
    });
  }

  send(method, params = {}, sessionId) {
    const id = this.nextId++;
    const payload = { id, method, params };
    if (sessionId) payload.sessionId = sessionId;
    const result = new Promise((resolve, reject) => this.pending.set(id, { resolve, reject }));
    this.socket.send(JSON.stringify(payload));
    return result;
  }
}

function evaluate(cdp, sessionId, expression) {
  return cdp.send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true }, sessionId)
    .then(result => {
      if (result.exceptionDetails) {
        throw new Error(result.exceptionDetails.exception?.description || result.exceptionDetails.text || "BROWSER_EVALUATION_FAILED");
      }
      return result.result.value;
    });
}

async function runBrowserQualification(baseUrl) {
  const chromeProfile = path.join(receiptRoot, "chrome-profile");
  mkdirSync(chromeProfile);
  const chrome = spawn(chromePath, [
    "--headless=new",
    "--disable-gpu",
    "--no-first-run",
    "--remote-debugging-port=0",
    `--user-data-dir=${chromeProfile}`,
    "about:blank"
  ], { windowsHide: true, stdio: ["ignore", "ignore", "pipe"] });
  let webSocketUrl = "";
  chrome.stderr.on("data", chunk => {
    const text = String(chunk);
    chromeLogs.push(text);
    const match = text.match(/DevTools listening on (ws:\/\/[^\s]+)/u);
    if (match) webSocketUrl = match[1];
  });
  await waitUntil(() => webSocketUrl, 10000, "CHROME_DEVTOOLS");
  const cdp = new CdpConnection(webSocketUrl);
  try {
    await cdp.open();
    const target = await cdp.send("Target.createTarget", { url: "about:blank" });
    const attached = await cdp.send("Target.attachToTarget", { targetId: target.targetId, flatten: true });
    const sessionId = attached.sessionId;
    await cdp.send("Page.enable", {}, sessionId);
    await cdp.send("Runtime.enable", {}, sessionId);
    await cdp.send("Emulation.setDeviceMetricsOverride", {
      width: 430,
      height: 932,
      deviceScaleFactor: 1,
      mobile: true
    }, sessionId);
    await cdp.send("Page.navigate", { url: `${baseUrl}/coordinate?rc-async-focus-test=1` }, sessionId);
    await waitUntil(
      () => evaluate(cdp, sessionId, "document.readyState === 'complete' && document.querySelector('#rcAsyncFocusSimulation')?.dataset.enabled === 'true'"),
      5000,
      "SIMULATION_PANEL"
    );
    const started = await evaluate(cdp, sessionId, `(() => {
      const input = document.querySelector('#coordinateInput');
      input.value = '116.391245,39.907654';
      input.dispatchEvent(new Event('input', { bubbles: true }));
      activeFinalizedCoordinateResult = {
        resultId: 'rc-current-point',
        resultRevision: 1,
        geometryHash: 'rc-current-point-hash'
      };
      input.focus();
      document.querySelector('#rcAsyncFocusSimulationStart').click();
      return {
        focused: document.activeElement === input,
        startedRevision: document.querySelector('#rcAsyncFocusSimulation').dataset.startedRevision
      };
    })()`);
    assert.equal(started.focused, true);
    assert.ok(started.startedRevision);
    await new Promise(resolve => setTimeout(resolve, 250));
    const editedValue = "116.391245,39.907654\n116.392245,39.908654";
    await evaluate(cdp, sessionId, `(() => {
      const input = document.querySelector('#coordinateInput');
      input.value = ${JSON.stringify(editedValue)};
      input.dispatchEvent(new Event('input', { bubbles: true }));
      activeFinalizedCoordinateResult = {
        resultId: 'rc-current-line',
        resultRevision: 2,
        geometryHash: 'rc-current-line-hash'
      };
      input.focus();
    })()`);
    await waitUntil(
      () => evaluate(cdp, sessionId, "document.querySelector('#rcAsyncFocusSimulation')?.dataset.result === 'pass'"),
      8000,
      "ASYNC_RESULT"
    );
    const outcome = await evaluate(cdp, sessionId, `(() => {
      const panel = document.querySelector('#rcAsyncFocusSimulation');
      const input = document.querySelector('#coordinateInput');
      return {
        value: input.value,
        focused: document.activeElement === input,
        editDetected: panel.dataset.editDetected,
        staleResultBlocked: panel.dataset.staleResultBlocked,
        textPreserved: panel.dataset.textPreserved,
        focusPreserved: panel.dataset.focusPreserved,
        currentIdentityPreservedAtCallback: panel.dataset.currentIdentityPreservedAtCallback,
        identityConflictBlocked: panel.dataset.identityConflictBlocked,
        result: panel.dataset.result,
        status: document.querySelector('#rcAsyncFocusSimulationStatus').textContent.trim(),
        visibleChecks: Array.from(document.querySelectorAll('#rcAsyncFocusSimulationChecks output')).map(output => ({
          key: output.dataset.check,
          result: output.dataset.result,
          text: output.textContent.trim()
        }))
      };
    })()`);
    assert.equal(outcome.value, editedValue);
    assert.equal(outcome.focused, true);
    for (const key of ["editDetected", "staleResultBlocked", "textPreserved", "focusPreserved", "currentIdentityPreservedAtCallback", "identityConflictBlocked"]) {
      assert.equal(outcome[key], "true", key);
    }
    assert.deepEqual(outcome.visibleChecks.map(item => item.key), [
      "editDetected",
      "staleResultBlocked",
      "textPreserved",
      "focusPreserved",
      "currentIdentityPreservedAtCallback",
      "identityConflictBlocked"
    ]);
    for (const item of outcome.visibleChecks) {
      assert.equal(item.result, "pass", `${item.key}:visible-result`);
      assert.equal(item.text, "通过", `${item.key}:visible-text`);
    }
    assert.equal(outcome.result, "pass");
    results.push({ test: "bounded-delayed-stale-result", result: "PASS", ...outcome });
    const capture = await cdp.send("Page.captureScreenshot", { format: "png", fromSurface: true }, sessionId);
    writeFileSync(path.join(receiptRoot, "async-focus-pass.png"), Buffer.from(capture.data, "base64"), { flag: "wx" });

    const diagnosticEditedValue = `${editedValue}\n116.393245,39.909654`;
    await evaluate(cdp, sessionId, `(() => {
      const input = document.querySelector('#coordinateInput');
      document.querySelector('#rcAsyncFocusSimulationStart').click();
      input.value = ${JSON.stringify(diagnosticEditedValue)};
      input.dispatchEvent(new Event('input', { bubbles: true }));
      input.blur();
    })()`);
    await waitUntil(
      () => evaluate(cdp, sessionId, "document.querySelector('#rcAsyncFocusSimulation')?.dataset.result === 'fail'"),
      8000,
      "ASYNC_FAILURE_DIAGNOSTIC"
    );
    const failureDiagnostic = await evaluate(cdp, sessionId, `(() => {
      const panel = document.querySelector('#rcAsyncFocusSimulation');
      return {
        editDetected: panel.dataset.editDetected,
        staleResultBlocked: panel.dataset.staleResultBlocked,
        textPreserved: panel.dataset.textPreserved,
        focusPreserved: panel.dataset.focusPreserved,
        currentIdentityPreservedAtCallback: panel.dataset.currentIdentityPreservedAtCallback,
        identityConflictBlocked: panel.dataset.identityConflictBlocked,
        result: panel.dataset.result,
        panelText: panel.textContent.trim(),
        visibleChecks: Array.from(document.querySelectorAll('#rcAsyncFocusSimulationChecks output')).map(output => ({
          key: output.dataset.check,
          result: output.dataset.result,
          text: output.textContent.trim()
        }))
      };
    })()`);
    assert.equal(failureDiagnostic.focusPreserved, "false");
    assert.equal(failureDiagnostic.result, "fail");
    for (const key of ["editDetected", "staleResultBlocked", "textPreserved", "currentIdentityPreservedAtCallback", "identityConflictBlocked"]) {
      assert.equal(failureDiagnostic[key], "true", `${key}:failure-diagnostic`);
    }
    assert.equal(failureDiagnostic.visibleChecks.find(item => item.key === "focusPreserved")?.result, "fail");
    assert.equal(failureDiagnostic.visibleChecks.find(item => item.key === "focusPreserved")?.text, "未通过");
    assert.doesNotMatch(failureDiagnostic.panelText, /116\.|39\.|rc-current|hash/u);
    results.push({ test: "visible-failure-diagnostics", result: "PASS", ...failureDiagnostic });
    const failureCapture = await cdp.send("Page.captureScreenshot", { format: "png", fromSurface: true }, sessionId);
    writeFileSync(path.join(receiptRoot, "async-focus-failure-diagnostic.png"), Buffer.from(failureCapture.data, "base64"), { flag: "wx" });

    await cdp.send("Page.navigate", { url: `${baseUrl}/coordinate` }, sessionId);
    await waitUntil(() => evaluate(cdp, sessionId, "document.readyState === 'complete'"), 5000, "DEFAULT_PAGE");
    const hiddenWithoutQuery = await evaluate(cdp, sessionId, "document.querySelector('#rcAsyncFocusSimulation').hidden === true");
    assert.equal(hiddenWithoutQuery, true);
    results.push({ test: "query-parameter-required", result: "PASS" });
    await cdp.send("Browser.close");
  } finally {
    if (cdp.socket?.readyState === WebSocket.OPEN) cdp.socket.close();
    if (!chrome.killed) chrome.kill();
  }
}

try {
  await withServer(18141, {}, async baseUrl => {
    const payload = await fetch(`${baseUrl}/api/version`).then(response => response.json());
    assert.equal(payload.runtimeIdentity.rcAsyncFocusSimulationEnabled, false);
    results.push({ test: "default-disabled", result: "PASS" });
  });
  await withServer(18142, {
    NODE_ENV: "production",
    ENABLE_RC_ASYNC_FOCUS_SIMULATION: "true",
    DEPLOYMENT_TIER: "production",
    RENDER_SERVICE_NAME: "coordinate-kml-tool",
    RENDER_GIT_BRANCH: "hotfix/production-generic-dms-review-recovery"
  }, async baseUrl => {
    const payload = await fetch(`${baseUrl}/api/version`).then(response => response.json());
    assert.equal(payload.runtimeIdentity.rcAsyncFocusSimulationEnabled, false);
    results.push({ test: "production-service-disabled", result: "PASS" });
  });
  await withServer(18144, {
    ENABLE_RC_ASYNC_FOCUS_SIMULATION: "true",
    DEPLOYMENT_TIER: "rc",
    RENDER_SERVICE_NAME: "coordinate-kml-tool-rc",
    RENDER_GIT_BRANCH: "codex/unapproved-branch"
  }, async baseUrl => {
    const payload = await fetch(`${baseUrl}/api/version`).then(response => response.json());
    assert.equal(payload.runtimeIdentity.rcAsyncFocusSimulationEnabled, false);
    results.push({ test: "unapproved-branch-disabled", result: "PASS" });
  });
  await withServer(18143, {
    ENABLE_RC_ASYNC_FOCUS_SIMULATION: "true",
    DEPLOYMENT_TIER: "rc",
    RENDER_SERVICE_NAME: "coordinate-kml-tool-rc",
    RENDER_GIT_BRANCH: "hotfix/production-generic-dms-review-recovery"
  }, async baseUrl => {
    const payload = await fetch(`${baseUrl}/api/version`).then(response => response.json());
    assert.equal(payload.runtimeIdentity.rcAsyncFocusSimulationEnabled, true);
    results.push({ test: "exact-rc-gate-enabled", result: "PASS" });
    await runBrowserQualification(baseUrl);
  });

  const receipt = {
    schemaVersion: "rc_async_focus_simulation_regression_v1",
    head,
    startedAt,
    completedAt: new Date().toISOString(),
    command: "node scripts/rc-async-focus-simulation-regression.js",
    worktreeStatusAtStart: worktreeStatus,
    results,
    realProviderCalls: 0,
    realMapServiceCalls: 0,
    uploads: 0,
    recognitionJobsCreated: 0,
    usageCharges: 0,
    persistentWrites: 0
  };
  writeFileSync(path.join(receiptRoot, "results.json"), JSON.stringify(receipt, null, 2), { flag: "wx" });
  console.log(`RC async focus simulation regression: ${results.length}/${results.length} PASS`);
  console.log(`HEAD=${head}`);
  console.log("REAL_PROVIDER_CALLS=0");
  console.log("REAL_MAP_SERVICE_CALLS=0");
} finally {
  writeFileSync(path.join(receiptRoot, "server.log"), serverLogs.join(""), { flag: "wx" });
  writeFileSync(path.join(receiptRoot, "chrome.log"), chromeLogs.join(""), { flag: "wx" });
}
