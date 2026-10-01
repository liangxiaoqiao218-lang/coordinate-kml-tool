import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { execFileSync } from "node:child_process";
import { mkdirSync, writeFileSync, existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const receiptRoot = path.resolve(String(process.env.MOBILE_UI_BROWSER_RECEIPT_DIR || "").trim());
if (!process.env.MOBILE_UI_BROWSER_RECEIPT_DIR || existsSync(receiptRoot)) {
  throw new Error("MOBILE_UI_NEW_BROWSER_RECEIPT_DIR_REQUIRED");
}
mkdirSync(receiptRoot, { recursive: false });
mkdirSync(path.join(receiptRoot, "390"));
mkdirSync(path.join(receiptRoot, "states"));

const chromePath = String(process.env.CHROME_PATH || "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe");
if (!existsSync(chromePath)) throw new Error("CHROME_NOT_FOUND");
const port = Number(process.env.MOBILE_UI_TEST_PORT || 18086);
const baseUrl = `http://127.0.0.1:${port}`;
const head = execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim();
const startedAt = new Date().toISOString();
const pages = [
  ["home", "/"],
  ["coordinate", "/coordinate"],
  ["judge", "/judge"],
  ["gold", "/gold"]
];
const widths = [320, 375, 390, 430, 1280];
const serverOutput = [];
const chromeOutput = [];

function sanitizedEnvironment() {
  const env = { ...process.env };
  for (const name of Object.keys(env)) {
    if (/PROVIDER|SUPABASE|ALIYUN|DASHSCOPE|OPENAI|ANTHROPIC|GEMINI|API_KEY|API_TOKEN|SECRET|USAGE|PASSWORD|COOKIE|AUTHORIZATION/i.test(name)) {
      env[name] = "";
    }
  }
  return {
    ...env,
    PORT: String(port),
    NODE_ENV: "test",
    DOTENV_CONFIG_PATH: path.join(root, "__no_mobile_ui_environment__"),
    NODE_OPTIONS: `--require=${path.join(root, "scripts", "recognition-audit-offline-guard.cjs")}`
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
  }

  send(method, params = {}, sessionId) {
    const id = this.nextId++;
    const payload = { id, method, params };
    if (sessionId) payload.sessionId = sessionId;
    const result = new Promise((resolve, reject) => this.pending.set(id, { resolve, reject }));
    this.socket.send(JSON.stringify(payload));
    return result;
  }

  listen() {
    this.socket.addEventListener("message", event => {
      const message = JSON.parse(String(event.data));
      if (!message.id || !this.pending.has(message.id)) return;
      const pending = this.pending.get(message.id);
      this.pending.delete(message.id);
      if (message.error) pending.reject(new Error(`${message.error.code}: ${message.error.message}`));
      else pending.resolve(message.result);
    });
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

async function navigate(cdp, sessionId, url) {
  await cdp.send("Page.navigate", { url }, sessionId);
  await waitUntil(
    () => evaluate(cdp, sessionId, "document.readyState === 'complete'"),
    5000,
    "PAGE_LOAD"
  );
}

async function screenshot(cdp, sessionId, outputFile) {
  const capture = await cdp.send("Page.captureScreenshot", { format: "png", fromSurface: true }, sessionId);
  writeFileSync(outputFile, Buffer.from(capture.data, "base64"), { flag: "wx" });
}

let server;
let chrome;
let cdp;
const results = [];
try {
  server = spawn(process.execPath, ["server.js"], {
    cwd: root,
    env: sanitizedEnvironment(),
    windowsHide: true,
    stdio: ["ignore", "pipe", "pipe"]
  });
  server.stdout.on("data", chunk => serverOutput.push(String(chunk)));
  server.stderr.on("data", chunk => serverOutput.push(String(chunk)));
  await waitUntil(async () => {
    const response = await fetch(`${baseUrl}/api/version`);
    return response.ok;
  }, 10000, "LOCAL_SERVER");

  const chromeProfile = path.join(receiptRoot, "chrome-profile");
  mkdirSync(chromeProfile);
  chrome = spawn(chromePath, [
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
    chromeOutput.push(text);
    const match = text.match(/DevTools listening on (ws:\/\/[^\s]+)/u);
    if (match) webSocketUrl = match[1];
  });
  await waitUntil(() => webSocketUrl, 10000, "CHROME_DEVTOOLS");
  cdp = new CdpConnection(webSocketUrl);
  await cdp.open();
  cdp.listen();
  const target = await cdp.send("Target.createTarget", { url: "about:blank" });
  const attached = await cdp.send("Target.attachToTarget", { targetId: target.targetId, flatten: true });
  const sessionId = attached.sessionId;
  await cdp.send("Page.enable", {}, sessionId);
  await cdp.send("Runtime.enable", {}, sessionId);

  for (const width of widths) {
    await cdp.send("Emulation.setDeviceMetricsOverride", {
      width,
      height: width === 1280 ? 900 : 844,
      deviceScaleFactor: 1,
      mobile: width < 720
    }, sessionId);
    for (const [name, pagePath] of pages) {
      await navigate(cdp, sessionId, `${baseUrl}${pagePath}?browser-regression=${head.slice(0, 8)}-${width}`);
      const layout = await evaluate(cdp, sessionId, `(() => {
        const clientWidth = document.documentElement.clientWidth;
        const offenders = [...document.querySelectorAll('main *, header *, footer *')]
          .filter(element => {
            const style = getComputedStyle(element);
            if (style.display === 'none' || style.visibility === 'hidden' || element.getAttribute('aria-hidden') === 'true') return false;
            const rect = element.getBoundingClientRect();
            return rect.width > 0 && (rect.left < -1 || rect.right > clientWidth + 1);
          })
          .map(element => ({ tag: element.tagName, id: element.id, className: String(element.className).slice(0, 80) }));
        return { clientWidth, scrollWidth: document.documentElement.scrollWidth, offenders };
      })()`);
      assert.equal(layout.scrollWidth, layout.clientWidth, `${name} ${width}px must not scroll horizontally`);
      assert.deepEqual(layout.offenders, [], `${name} ${width}px must not clip visible content`);
      results.push({ page: name, width, result: "PASS", ...layout });
      if (width === 390) await screenshot(cdp, sessionId, path.join(receiptRoot, "390", `${name}.png`));
    }
  }

  await cdp.send("Emulation.setDeviceMetricsOverride", { width: 390, height: 844, deviceScaleFactor: 1, mobile: true }, sessionId);
  await navigate(cdp, sessionId, `${baseUrl}/coordinate?browser-state=direct-paste`);
  const directPaste = await evaluate(cdp, sessionId, `(() => {
    const coordinateInput = document.querySelector('#coordinateInput');
    coordinateInput.value = '1,116.391245,39.907654\\n2,116.401245,39.917654';
    coordinateInput.dispatchEvent(new Event('input', { bubbles: true }));
    const focusProbe = document.createElement('input');
    document.body.append(focusProbe);
    focusProbe.focus();
    focusCoordinateInputAfterRecognition();
    const focusPreserved = document.activeElement === focusProbe;
    focusProbe.remove();
    coordinateInput.focus();
    return {
      visible: Boolean(coordinateInput.getClientRects().length),
      editable: !coordinateInput.readOnly && !coordinateInput.disabled,
      value: coordinateInput.value,
      copyVisible: !document.querySelector('#coordinateCopyAction').hidden,
      focusPreserved
    };
  })()`);
  assert.equal(directPaste.visible, true);
  assert.equal(directPaste.editable, true);
  assert.equal(directPaste.copyVisible, true);
  assert.equal(directPaste.focusPreserved, true);
  results.push({ state: "coordinate-direct-paste", result: "PASS", ...directPaste });
  await screenshot(cdp, sessionId, path.join(receiptRoot, "states", "coordinate-direct-paste.png"));

  const statusStates = await evaluate(cdp, sessionId, `(() => {
    const input = document.querySelector('#coordinateInput');
    showRecognitionProgress('正在查找坐标区域…', 'loading');
    const processing = document.querySelector('#recognitionProgress').textContent.trim();
    input.value = '1 | 116.391245 | 39.907654\\n2 | 116.401245 | 39.917654';
    input.dispatchEvent(new Event('input', { bubbles: true }));
    showRecognitionProgress('识别完成', 'success');
    const complete = document.querySelector('#recognitionProgress').textContent.trim();
    input.value += '\\n3 | 116.411245 | 39.927654';
    input.dispatchEvent(new Event('input', { bubbles: true }));
    const edited = input.value.endsWith('3 | 116.411245 | 39.927654');
    showUploadSupportMessage('已保留部分坐标，请核对后继续编辑。', 'warning');
    const partial = document.querySelector('#uploadMessage').textContent.trim();
    showUploadSupportMessage('图片识别未完成，请重新识别或使用人工协助。', 'error');
    const failed = document.querySelector('#uploadMessage').textContent.trim();
    return { processing, complete, edited, partial, failed, editable: !input.readOnly && !input.disabled };
  })()`);
  assert.match(statusStates.processing, /正在查找坐标区域/u);
  assert.match(statusStates.complete, /识别完成/u);
  assert.equal(statusStates.edited, true);
  assert.match(statusStates.partial, /部分坐标/u);
  assert.match(statusStates.failed, /人工协助/u);
  assert.equal(statusStates.editable, true);
  results.push({ state: "coordinate-lifecycle", result: "PASS", ...statusStates });
  await screenshot(cdp, sessionId, path.join(receiptRoot, "states", "coordinate-failure-with-results.png"));

  await navigate(cdp, sessionId, `${baseUrl}/gold?browser-state=calculator`);
  const gold = await evaluate(cdp, sessionId, `(() => {
    const weight = document.querySelector('#goldWeight');
    const water = document.querySelector('#waterDiff');
    weight.value = '10';
    water.value = '0.6';
    weight.dispatchEvent(new Event('input', { bubbles: true }));
    water.dispatchEvent(new Event('input', { bubbles: true }));
    return {
      resultVisible: Boolean(document.querySelector('#goldResultCard').getClientRects().length),
      quoteVisible: Boolean(document.querySelector('.quote-placeholder').getClientRects().length),
      copyVisible: Boolean(document.querySelector('#goldActions').getClientRects().length),
      purity: document.querySelector('#goldPurityResult').textContent.trim()
    };
  })()`);
  assert.equal(gold.resultVisible, true);
  assert.equal(gold.quoteVisible, true);
  assert.equal(gold.copyVisible, true);
  assert.notEqual(gold.purity, "--");
  results.push({ state: "gold-calculator", result: "PASS", ...gold });
  await screenshot(cdp, sessionId, path.join(receiptRoot, "states", "gold-calculated.png"));

  await cdp.send("Browser.close");
  const receipt = {
    schemaVersion: "mobile_ui_browser_regression_v1",
    head,
    startedAt,
    completedAt: new Date().toISOString(),
    widths,
    pages: pages.map(([name]) => name),
    results,
    realProviderCalls: 0,
    notes: {
      simulatedStates: true,
      realMobileKeyboard: "NOT_VERIFIED",
      realMobileBackGesture: "NOT_VERIFIED"
    }
  };
  writeFileSync(path.join(receiptRoot, "results.json"), JSON.stringify(receipt, null, 2), { flag: "wx" });
  console.log(`Mobile UI browser regression: ${results.length}/${results.length} PASS`);
  console.log(`HEAD=${head}`);
  console.log("REAL_PROVIDER_CALLS=0");
} finally {
  if (cdp?.socket?.readyState === WebSocket.OPEN) cdp.socket.close();
  if (chrome && !chrome.killed) chrome.kill();
  if (server && !server.killed) server.kill();
  writeFileSync(path.join(receiptRoot, "server.log"), serverOutput.join(""), { flag: "wx" });
  writeFileSync(path.join(receiptRoot, "chrome.log"), chromeOutput.join(""), { flag: "wx" });
}
