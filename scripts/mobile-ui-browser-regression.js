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
mkdirSync(path.join(receiptRoot, "full-390"));
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

async function fullScreenshot(cdp, sessionId, outputFile, width) {
  await evaluate(cdp, sessionId, "new Promise(resolve => { window.scrollTo(0, 0); requestAnimationFrame(() => requestAnimationFrame(resolve)); })");
  const height = await evaluate(cdp, sessionId, "Math.max(document.documentElement.scrollHeight, document.body.scrollHeight)");
  const capture = await cdp.send("Page.captureScreenshot", {
    format: "png",
    fromSurface: true,
    captureBeyondViewport: true,
    clip: { x: 0, y: 0, width, height, scale: 1 }
  }, sessionId);
  writeFileSync(outputFile, Buffer.from(capture.data, "base64"), { flag: "wx" });
}

async function markSyntheticState(cdp, sessionId, label) {
  await evaluate(cdp, sessionId, `(() => {
    let marker = document.querySelector('#syntheticUiReceiptMarker');
    if (!marker) {
      marker = document.createElement('div');
      marker.id = 'syntheticUiReceiptMarker';
      Object.assign(marker.style, {
        position: 'fixed', top: '8px', right: '8px', zIndex: '2147483647',
        padding: '5px 8px', borderRadius: '999px', background: '#334155',
        color: '#fff', fontSize: '11px', fontWeight: '700', boxShadow: '0 2px 8px rgba(15,23,42,.18)'
      });
      document.body.append(marker);
    }
    marker.textContent = ${JSON.stringify(`合成 UI 状态：${label}（非真实识别结果）`)};
  })()`);
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
        const navButtons = [...document.querySelectorAll('.view.active .page-nav .back-button')];
        const navLabels = navButtons.map(button => button.textContent.trim());
        const navRows = [...new Set(navButtons.map(button => Math.round(button.getBoundingClientRect().top)))];
        const coordinateInput = document.querySelector('#coordinateInput');
        const formatSelect = document.querySelector('#coordinateFormat');
        const footer = document.querySelector('.site-footer');
        const versionStamp = document.querySelector('.version-stamp');
        const main = document.querySelector('main.page');
        const activeNav = document.querySelector('.view.active .page-nav');
        const visibleRect = element => {
          if (!element || !element.getClientRects().length) return null;
          const rect = element.getBoundingClientRect();
          return { left: rect.left, right: rect.right, width: rect.width, height: rect.height };
        };
        const offenders = [...document.querySelectorAll('main *, header *, footer *')]
          .filter(element => {
            const style = getComputedStyle(element);
            if (style.display === 'none' || style.visibility === 'hidden' || element.getAttribute('aria-hidden') === 'true') return false;
            if (element.closest('.page-nav')) return false;
            const rect = element.getBoundingClientRect();
            return rect.width > 0 && (rect.left < -1 || rect.right > clientWidth + 1);
          })
          .map(element => ({ tag: element.tagName, id: element.id, className: String(element.className).slice(0, 80) }));
        return {
          clientWidth,
          scrollWidth: document.documentElement.scrollWidth,
          offenders,
          navLabels,
          navRows: navRows.length,
          navScrollable: document.querySelector('.view.active .page-nav')?.scrollWidth > document.querySelector('.view.active .page-nav')?.clientWidth,
          pageNavOverflow: getComputedStyle(document.querySelector('.view.active .page-nav') || document.body).overflowX,
          pageNavBackground: activeNav ? getComputedStyle(activeNav).backgroundColor : null,
          pageNavBoxShadow: activeNav ? getComputedStyle(activeNav).boxShadow : null,
          coordinateInput: visibleRect(coordinateInput),
          coordinatePlaceholder: coordinateInput?.getAttribute('placeholder') || '',
          coordinateExampleLinkVisible: Boolean([...document.querySelectorAll('summary,button,a')].find(item => item.textContent.trim() === '查看输入示例' && item.getClientRects().length)),
          formatSelect: visibleRect(formatSelect),
          footerMarginTop: footer ? getComputedStyle(footer).marginTop : '',
          footerBottomGap: footer?.getClientRects().length ? Math.round(innerHeight - footer.getBoundingClientRect().bottom) : null,
          versionBottomGap: versionStamp?.getClientRects().length ? Math.round(innerHeight - versionStamp.getBoundingClientRect().bottom) : null,
          contentToFooterGap: footer?.getClientRects().length && main?.getClientRects().length
            ? Math.round(footer.getBoundingClientRect().top - main.getBoundingClientRect().bottom)
            : null
        };
      })()`);
      assert.equal(layout.scrollWidth, layout.clientWidth, `${name} ${width}px must not scroll horizontally`);
      assert.deepEqual(layout.offenders, [], `${name} ${width}px must not clip visible content`);
      if (name !== "home") {
        assert.deepEqual(layout.navLabels, ["首页", "坐标识别", "矿地快判", "黄金成色计算器"], `${name} ${width}px keeps complete navigation labels`);
        assert.equal(layout.navRows, 1, `${name} ${width}px navigation remains on one row`);
        assert.equal(layout.pageNavOverflow, "auto", `${name} ${width}px navigation owns its horizontal scrolling`);
        if (width <= 430) {
          assert.equal(layout.pageNavBackground, "rgba(0, 0, 0, 0)", `${name} ${width}px navigation has no outer tray background`);
          assert.equal(layout.pageNavBoxShadow, "none", `${name} ${width}px navigation has no outer tray shadow`);
        }
      }
      if (name === "coordinate") {
        assert.ok(layout.coordinateInput?.height >= (width < 720 ? 215 : 250), `coordinate ${width}px restores the accepted input height`);
        assert.match(layout.coordinatePlaceholder, /粘贴坐标[\s\S]*116\.391245,39\.907654[\s\S]*31°15'30\.12"N,121°28'15\.45"E/u);
        assert.equal(layout.coordinateExampleLinkVisible, false, `coordinate ${width}px has no extra example disclosure`);
        assert.ok(layout.formatSelect?.left >= -1 && layout.formatSelect?.right <= layout.clientWidth + 1, `coordinate ${width}px format control is visible and unclipped`);
      }
      assert.ok(layout.contentToFooterGap >= 32 && layout.contentToFooterGap <= 48, `${name} ${width}px keeps a natural 32-48px content-to-footer gap`);
      results.push({ page: name, width, result: "PASS", ...layout });
      if (width === 390) {
        await screenshot(cdp, sessionId, path.join(receiptRoot, "390", `${name}.png`));
        await fullScreenshot(cdp, sessionId, path.join(receiptRoot, "full-390", `${name}.png`), width);
      }
    }
  }

  await cdp.send("Emulation.setDeviceMetricsOverride", { width: 390, height: 844, deviceScaleFactor: 1, mobile: true }, sessionId);
  await navigate(cdp, sessionId, `${baseUrl}/coordinate?browser-state=direct-paste`);
  await markSyntheticState(cdp, sessionId, "坐标空输入");
  await fullScreenshot(cdp, sessionId, path.join(receiptRoot, "states", "coordinate-empty-full.png"), 390);
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
    const toolButtons = [...document.querySelectorAll('.workspace-tools .icon-button')]
      .filter(button => getComputedStyle(button).display !== 'none');
    const toolMetrics = toolButtons.map(button => {
      const style = getComputedStyle(button);
      const rect = button.getBoundingClientRect();
      return {
        className: button.className,
        width: Math.round(rect.width),
        height: Math.round(rect.height),
        borderWidth: style.borderWidth,
        borderRadius: style.borderRadius,
        padding: style.padding,
        alignItems: style.alignItems,
        justifyContent: style.justifyContent,
        ariaLabel: button.getAttribute('aria-label')
      };
    });
    return {
      visible: Boolean(coordinateInput.getClientRects().length),
      editable: !coordinateInput.readOnly && !coordinateInput.disabled,
      value: coordinateInput.value,
      copyVisible: !document.querySelector('#coordinateCopyAction').hidden,
      focusPreserved,
      toolMetrics
    };
  })()`);
  assert.equal(directPaste.visible, true);
  assert.equal(directPaste.editable, true);
  assert.equal(directPaste.copyVisible, true);
  assert.equal(directPaste.focusPreserved, true);
  assert.match(directPaste.value, /116\.391245/u);
  assert.equal(directPaste.toolMetrics.length, 3);
  assert.ok(directPaste.toolMetrics.every(metric => metric.width === 40 && metric.height === 40));
  assert.ok(directPaste.toolMetrics.every(metric => metric.borderWidth === '1px' && metric.borderRadius === '8px'));
  assert.ok(directPaste.toolMetrics.every(metric => metric.padding === '0px' && metric.alignItems === 'center' && metric.justifyContent === 'center'));
  assert.equal(directPaste.toolMetrics.find(metric => metric.className.includes('clear-input-button'))?.ariaLabel, '清空坐标');
  results.push({ state: "coordinate-direct-paste", result: "PASS", ...directPaste });
  await screenshot(cdp, sessionId, path.join(receiptRoot, "states", "coordinate-direct-paste.png"));
  await screenshot(cdp, sessionId, path.join(receiptRoot, "states", "coordinate-tools.png"));

  const detailSizing = await evaluate(cdp, sessionId, `(() => {
    setDebug('正在查找坐标区域…');
    setDebugRunning(true);
    const panel = document.querySelector('#debugPanel');
    const detail = document.querySelector('#debugText');
    const shortHeight = Math.round(detail.getBoundingClientRect().height);
    const shortOpen = panel.open && !panel.hidden;
    setDebugRunning(false);
    setDebug(Array.from({ length: 30 }, (_, index) => '技术细节 ' + (index + 1)).join('\\n'));
    const longHeight = Math.round(detail.getBoundingClientRect().height);
    const longScrollHeight = Math.round(detail.scrollHeight);
    const longClientHeight = Math.round(detail.clientHeight);
    const resizeMode = getComputedStyle(detail).resize;
    setDebug('正在查找坐标区域…');
    setDebugRunning(true);
    const heading = document.querySelector('.workspace-title-row h1');
    const quota = document.querySelector('.coordinate-workspace .quota-entry-row');
    return {
      shortHeight,
      shortOpen,
      longHeight,
      longScrollHeight,
      longClientHeight,
      resizeMode,
      maxHeight: getComputedStyle(detail).maxHeight,
      backgroundColor: getComputedStyle(detail).backgroundColor,
      color: getComputedStyle(detail).color,
      headingWidth: Math.round(heading.getBoundingClientRect().width),
      headingWritingMode: getComputedStyle(heading).writingMode,
      headingTop: Math.round(heading.getBoundingClientRect().top),
      headingRight: Math.round(heading.getBoundingClientRect().right),
      quotaTop: Math.round(quota.getBoundingClientRect().top),
      quotaLeft: Math.round(quota.getBoundingClientRect().left)
    };
  })()`);
  assert.equal(detailSizing.shortOpen, true);
  assert.ok(detailSizing.shortHeight >= 200 && detailSizing.shortHeight <= 240);
  assert.ok(detailSizing.longHeight >= 200 && detailSizing.longHeight <= 240);
  assert.ok(detailSizing.longScrollHeight > detailSizing.longClientHeight);
  assert.equal(detailSizing.resizeMode, 'none');
  assert.equal(detailSizing.backgroundColor, 'rgb(15, 23, 42)');
  assert.equal(detailSizing.color, 'rgb(229, 231, 235)');
  assert.ok(detailSizing.headingWidth > 100);
  assert.equal(detailSizing.headingWritingMode, 'horizontal-tb');
  assert.ok(Math.abs(detailSizing.quotaTop - detailSizing.headingTop) <= 20);
  assert.ok(detailSizing.headingRight + 8 <= detailSizing.quotaLeft);
  results.push({ state: "coordinate-detail-sizing", result: "PASS", ...detailSizing });
  await markSyntheticState(cdp, sessionId, "加高识别详情");
  await fullScreenshot(cdp, sessionId, path.join(receiptRoot, "states", "coordinate-detail-tall-full.png"), 390);
  await markSyntheticState(cdp, sessionId, "坐标识别中");
  await screenshot(cdp, sessionId, path.join(receiptRoot, "states", "coordinate-processing.png"));
  await fullScreenshot(cdp, sessionId, path.join(receiptRoot, "states", "coordinate-processing-full.png"), 390);

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
    setDebug('识别未完成\\n请重新识别或使用人工协助');
    setDebugRunning(false);
    return { processing, complete, edited, partial, failed, editable: !input.readOnly && !input.disabled };
  })()`);
  assert.match(statusStates.processing, /正在查找坐标区域/u);
  assert.match(statusStates.complete, /识别完成/u);
  assert.equal(statusStates.edited, true);
  assert.match(statusStates.partial, /部分坐标/u);
  assert.match(statusStates.failed, /人工协助/u);
  assert.match(statusStates.failed, /本次图片识别未完成，请重试/u);
  assert.doesNotMatch(statusStates.failed, /图片识别未完成图片识别未完成/u);
  assert.equal(statusStates.editable, true);
  results.push({ state: "coordinate-lifecycle", result: "PASS", ...statusStates });
  await markSyntheticState(cdp, sessionId, "坐标识别失败");
  await screenshot(cdp, sessionId, path.join(receiptRoot, "states", "coordinate-failure-with-results.png"));
  await fullScreenshot(cdp, sessionId, path.join(receiptRoot, "states", "coordinate-failure-full.png"), 390);

  const feedbackContract = await evaluate(cdp, sessionId, `(() => {
    const input = document.querySelector('#coordinateInput');
    const debugPanel = document.querySelector('#debugPanel');
    clearUploadMessage();
    setDebug('识别完成\\n已识别 4 个坐标点\\n当前坐标需要核对');
    debugPanel.open = false;
    setDebugRunning(true);
    const reopened = debugPanel.open && !debugPanel.hidden;
    setDebugRunning(false);

    const revision = coordinateInputEditRevision;
    input.value = '116.391245,39.907654';
    input.dispatchEvent(new Event('input', { bubbles: true }));
    const staleDetected = hasCoordinateInputChangedSince(revision);

    const identity = { resultId: 'result-ui-1', resultRevision: 3, geometryHash: 'sha256:ui-1' };
    setRecognitionSummary({
      count: 2,
      geometry: 'LineString',
      coordinateType: 'decimal_latlon',
      engine: { groups: [{ geometry: 'LineString', validation: {}, warnings: [] }] },
      resultIdentity: identity
    });
    const lineText = document.querySelector('#recognitionSummary').textContent;
    const identityBound = { ...document.querySelector('#recognitionSummary').dataset };

    setRecognitionSummary({
      count: 4,
      geometry: 'Polygon',
      coordinateType: 'decimal_latlon',
      engine: { groups: [{ geometry: 'Polygon', kml_ready: true, requires_review: false, calculated_area_ha: 12.5, validation: {}, warnings: [] }] },
      resultIdentity: identity
    });
    const polygonText = document.querySelector('#recognitionSummary').textContent;

    setRecognitionSummary({
      count: 4,
      geometry: 'Polygon',
      coordinateType: 'standard_dms_table',
      requiresReview: true,
      engine: { groups: [{ geometry: 'Polygon', kml_ready: false, requires_review: true, validation: { self_intersecting: true }, warnings: [] }] },
      resultIdentity: identity
    });
    const selfIntersectingText = document.querySelector('#recognitionSummary').textContent;
    const summary = document.querySelector('#recognitionSummary');
    const summaryStyle = getComputedStyle(summary);
    const summaryMarkerStyle = getComputedStyle(summary, '::before');
    const summaryLabelMarkerStyle = getComputedStyle(summary.querySelector('.recognition-summary-label'), '::before');
    const summaryMetaStyle = getComputedStyle(summary.querySelector('.recognition-summary-meta'));
    const summaryCells = [...summary.querySelectorAll('.recognition-summary-pill')].map(cell => ({
      key: cell.dataset.summaryKey,
      value: cell.querySelector('.recognition-summary-value')?.textContent || '',
      tone: [...cell.classList].find(name => /^is-(valid|warning|neutral)$/u.test(name)) || ''
    }));
    return {
      reopened,
      staleDetected,
      lineText,
      polygonText,
      selfIntersectingText,
      summaryCells,
      identityBound,
      summaryBackground: summaryStyle.backgroundColor,
      summaryBorderWidth: summaryStyle.borderTopWidth,
      summaryMarkerDisplay: summaryMarkerStyle.display,
      summaryLabelMarkerBackground: summaryLabelMarkerStyle.backgroundColor,
      summaryMetaDisplay: summaryMetaStyle.display,
      summaryMetaWrap: summaryMetaStyle.flexWrap
    };
  })()`);
  assert.equal(feedbackContract.reopened, true);
  assert.equal(feedbackContract.staleDetected, true);
  assert.match(feedbackContract.lineText, /区域类型线/u);
  assert.match(feedbackContract.lineText, /坐标数量2/u);
  assert.match(feedbackContract.lineText, /坐标类型经纬度/u);
  assert.match(feedbackContract.lineText, /所属国家\/地区未知/u);
  assert.doesNotMatch(feedbackContract.lineText, /面积/u);
  assert.match(feedbackContract.polygonText, /面积12\.5 ha/u);
  assert.match(feedbackContract.selfIntersectingText, /区域类型多边形（坐标待核对）/u);
  assert.doesNotMatch(feedbackContract.selfIntersectingText, /面积/u);
  assert.deepEqual(feedbackContract.summaryCells, [
    { key: "区域类型", value: "多边形（坐标待核对）", tone: "is-warning" },
    { key: "所属国家/地区", value: "未知", tone: "is-neutral" },
    { key: "坐标数量", value: "4", tone: "is-valid" },
    { key: "坐标类型", value: "度分秒", tone: "is-valid" }
  ]);
  assert.equal(feedbackContract.summaryBackground, "rgb(240, 253, 244)");
  assert.equal(feedbackContract.summaryBorderWidth, "1px");
  assert.equal(feedbackContract.summaryMarkerDisplay, "none");
  assert.equal(feedbackContract.summaryLabelMarkerBackground, "rgb(22, 163, 74)");
  assert.equal(feedbackContract.summaryMetaDisplay, "flex");
  assert.equal(feedbackContract.summaryMetaWrap, "wrap");
  assert.deepEqual(feedbackContract.identityBound, { resultId: "result-ui-1", resultRevision: "3", geometryHash: "sha256:ui-1" });
  results.push({ state: "coordinate-feedback-contract", result: "PASS", ...feedbackContract });
  await markSyntheticState(cdp, sessionId, "新版区域概览");
  await screenshot(cdp, sessionId, path.join(receiptRoot, "states", "coordinate-result-overview.png"));
  await fullScreenshot(cdp, sessionId, path.join(receiptRoot, "states", "coordinate-overview-new-full.png"), 390);
  await evaluate(cdp, sessionId, `(() => {
    const style = document.createElement('style');
    style.id = 'synthetic-legacy-overview-style';
    style.textContent = '#recognitionSummary .recognition-summary-meta{display:grid;grid-template-columns:repeat(2,minmax(0,1fr))}#recognitionSummary .recognition-summary-pill{display:grid;min-height:54px;border-radius:10px;background:#fff}';
    document.head.appendChild(style);
    document.querySelector('#recognitionSummary .recognition-summary-label').textContent = '结果概览（旧版 2×2 对照）';
  })()`);
  await markSyntheticState(cdp, sessionId, "旧版2×2概览对照");
  await fullScreenshot(cdp, sessionId, path.join(receiptRoot, "states", "coordinate-overview-old-grid-comparison-full.png"), 390);
  await evaluate(cdp, sessionId, `(() => {
    document.querySelector('#synthetic-legacy-overview-style')?.remove();
    document.querySelector('#recognitionSummary .recognition-summary-label').textContent = '区域概览';
  })()`);
  await markSyntheticState(cdp, sessionId, "坐标结果待核对");
  await fullScreenshot(cdp, sessionId, path.join(receiptRoot, "states", "coordinate-review-full.png"), 390);
  const successOverview = await evaluate(cdp, sessionId, `(() => {
    setRecognitionSummary({
      count: 4,
      geometry: 'Polygon',
      coordinateType: 'decimal_latlon',
      engine: { groups: [{ geometry: 'Polygon', kml_ready: true, requires_review: false, calculated_area_ha: 12.5, validation: {}, warnings: [] }] },
      resultIdentity: { resultId: 'result-ui-success', resultRevision: 1, geometryHash: 'sha256:ui-success' }
    });
    const cells = [...document.querySelectorAll('#recognitionSummary .recognition-summary-pill')].map(cell => ({
      key: cell.dataset.summaryKey,
      value: cell.querySelector('.recognition-summary-value')?.textContent || '',
      tone: [...cell.classList].find(name => /^is-(valid|warning|neutral)$/u.test(name)) || ''
    }));
    return { cells };
  })()`);
  await markSyntheticState(cdp, sessionId, "坐标成功概览");
  await fullScreenshot(cdp, sessionId, path.join(receiptRoot, "states", "coordinate-success-full.png"), 390);
  assert.deepEqual(successOverview.cells, [
    { key: "区域类型", value: "多边形", tone: "is-valid" },
    { key: "所属国家/地区", value: "未知", tone: "is-neutral" },
    { key: "坐标数量", value: "4", tone: "is-valid" },
    { key: "坐标类型", value: "经纬度", tone: "is-valid" },
    { key: "面积", value: "12.5 ha", tone: "is-valid" }
  ]);
  results.push({ state: "coordinate-success-overview", result: "PASS", ...successOverview });
  const previousResult = await evaluate(cdp, sessionId, `(() => {
    const previousMeta = activeRecognitionSummaryMeta;
    const previousText = document.querySelector('#coordinateInput').value;
    clearRecognitionSummary();
    showUploadSupportMessage('本次图片识别未完成，请重试。');
    const restored = restorePreviousRecognitionSummary(previousMeta, previousText);
    const summary = document.querySelector('#recognitionSummary');
    return { restored, text: summary.textContent, identity: { ...summary.dataset } };
  })()`);
  assert.equal(previousResult.restored, true);
  assert.match(previousResult.text, /上次结果/u);
  assert.deepEqual(previousResult.identity, { resultId: "result-ui-success", resultRevision: "1", geometryHash: "sha256:ui-success" });
  results.push({ state: "coordinate-previous-result", result: "PASS", ...previousResult });
  await screenshot(cdp, sessionId, path.join(receiptRoot, "states", "coordinate-previous-result.png"));
  await fullScreenshot(cdp, sessionId, path.join(receiptRoot, "states", "coordinate-previous-result-full.png"), 390);
  const overviewCleared = await evaluate(cdp, sessionId, `(() => {
    clearRecognitionSummary();
    const summary = document.querySelector('#recognitionSummary');
    return summary.hidden && !summary.dataset.resultId && !summary.dataset.resultRevision && !summary.dataset.geometryHash;
  })()`);
  assert.equal(overviewCleared, true);

  const mapExpanded = await evaluate(cdp, sessionId, `(() => {
    showPage('spatialResult', '', false);
    renderSpatialResult({
      mapPreviewObject: {
        geometryType: 'Polygon',
        geometry: { type: 'Polygon', coordinates: [[[116.391245,39.907654],[116.392245,39.907654],[116.392245,39.908654],[116.391245,39.907654]]] },
        previewWarnings: []
      },
      spatialFactsStatus: 'available',
      spatialFacts: { pointCount: 3, areaMeters2: 125000, perimeterMeters: 1500, centroid: null },
      regionalViewCount: 0,
      kmlEligibility: { allowed: true }
    });
    spatialShareCardAction.hidden = false;
    setSpatialSheetExpanded(true);
    const share = spatialShareCardAction.getBoundingClientRect();
    const toggle = spatialResultSheetToggle.getBoundingClientRect();
    return {
      expanded: spatialResultSheetToggle.getAttribute('aria-expanded'),
      detailsHidden: spatialResultDetails.hidden,
      pointCount: spatialPointCount.textContent,
      centroidHidden: spatialCentroidFact.hidden,
      shareVisible: Boolean(spatialShareCardAction.getClientRects().length),
      controlGap: Math.round(Math.max(0, toggle.right - share.left)),
      sheetBackground: getComputedStyle(spatialResultSheetToggle).backgroundImage
    };
  })()`);
  await markSyntheticState(cdp, sessionId, "地图面板展开");
  await fullScreenshot(cdp, sessionId, path.join(receiptRoot, "states", "map-expanded-full.png"), 390);
  assert.equal(mapExpanded.expanded, "true");
  assert.equal(mapExpanded.detailsHidden, false);
  assert.equal(mapExpanded.pointCount, "3 个");
  assert.equal(mapExpanded.centroidHidden, true);
  assert.equal(mapExpanded.shareVisible, true);
  assert.match(mapExpanded.sheetBackground, /linear-gradient/u);
  results.push({ state: "map-expanded", result: "PASS", ...mapExpanded });
  const mapCollapsed = await evaluate(cdp, sessionId, `(() => {
    const beforeSummary = spatialCollapsedSummary.textContent;
    const shareWasHidden = spatialShareCardAction.hidden;
    setSpatialSheetExpanded(false);
    const collapsed = {
      expanded: spatialResultSheetToggle.getAttribute('aria-expanded'),
      detailsHidden: spatialResultDetails.hidden,
      summary: spatialCollapsedSummary.textContent,
      shareHidden: spatialShareCardAction.hidden
    };
    setSpatialSheetExpanded(true);
    return {
      ...collapsed,
      restoredExpanded: spatialResultSheetToggle.getAttribute('aria-expanded'),
      restoredDetailsHidden: spatialResultDetails.hidden,
      resultPreserved: spatialCollapsedSummary.textContent === beforeSummary,
      sharePreserved: spatialShareCardAction.hidden === shareWasHidden
    };
  })()`);
  assert.equal(mapCollapsed.expanded, "false");
  assert.equal(mapCollapsed.detailsHidden, true);
  assert.equal(mapCollapsed.restoredExpanded, "true");
  assert.equal(mapCollapsed.restoredDetailsHidden, false);
  assert.equal(mapCollapsed.resultPreserved, true);
  assert.equal(mapCollapsed.sharePreserved, true);
  await evaluate(cdp, sessionId, `setSpatialSheetExpanded(false)`);
  await markSyntheticState(cdp, sessionId, "地图面板收起");
  await fullScreenshot(cdp, sessionId, path.join(receiptRoot, "states", "map-collapsed-full.png"), 390);
  await evaluate(cdp, sessionId, `setSpatialSheetExpanded(true)`);
  results.push({ state: "map-collapse-expand-cycle", result: "PASS", ...mapCollapsed });

  await navigate(cdp, sessionId, `${baseUrl}/judge?browser-state=synthetic-result`);
  const judgeResultState = await evaluate(cdp, sessionId, `(() => {
    setQuotaStatus(judgeQuotaStatus, '使用情况', 8, false);
    judgeResult.value = '初筛结论：存在需要复核的矿化线索。\\n依据：颜色与纹理仅构成视觉线索。\\n建议：结合现场采样和检测结果确认。';
    const decision = renderJudgeDecisionCard(judgeResult.value, { grade: 'B', score: 72, confidence: '中等' });
    return {
      uploadLabelPresent: document.body.textContent.includes('上传区'),
      quotaColor: getComputedStyle(judgeQuotaStatus).color,
      summaryVisible: Boolean(judgeDecisionCard.getClientRects().length),
      detailVisible: Boolean(judgeResult.getClientRects().length),
      recommendation: document.querySelector('.judge-decision-recommendation')?.textContent.trim() || '',
      decisionGrade: decision?.grade || ''
    };
  })()`);
  await markSyntheticState(cdp, sessionId, "矿地快判结果");
  await fullScreenshot(cdp, sessionId, path.join(receiptRoot, "states", "judge-result-full.png"), 390);
  assert.equal(judgeResultState.uploadLabelPresent, false);
  assert.equal(judgeResultState.quotaColor, "rgb(100, 116, 139)");
  assert.equal(judgeResultState.summaryVisible, true);
  assert.equal(judgeResultState.detailVisible, true);
  assert.match(judgeResultState.recommendation, /建议/u);
  results.push({ state: "judge-result", result: "PASS", ...judgeResultState });

  await navigate(cdp, sessionId, `${baseUrl}/gold?browser-state=calculator`);
  const gold = await evaluate(cdp, sessionId, `(() => {
    const weight = document.querySelector('#goldWeight');
    const water = document.querySelector('#waterDiff');
    const copy = document.querySelector('#copyGoldButton');
    const initiallyDisabled = copy.disabled && copy.dataset.resultState !== 'valid';
    weight.value = '10';
    water.value = '0.6';
    weight.dispatchEvent(new Event('input', { bubbles: true }));
    water.dispatchEvent(new Event('input', { bubbles: true }));
    const validEnabled = !copy.disabled && copy.dataset.resultState === 'valid';
    water.value = '';
    water.dispatchEvent(new Event('input', { bubbles: true }));
    const staleDisabled = copy.disabled && copy.dataset.resultState === 'invalid';
    water.value = '0.6';
    water.dispatchEvent(new Event('input', { bubbles: true }));
    const quoteCurrency = document.querySelector('#quoteCurrency');
    const shopQuote = document.querySelector('#shopQuote');
    quoteCurrency.value = 'CNY';
    shopQuote.value = '980';
    shopQuote.dispatchEvent(new Event('input', { bubbles: true }));
    const metricNumber = document.querySelector('#goldPurityResult .gold-metric-number');
    const metricAffix = document.querySelector('#goldPurityResult .gold-metric-affix');
    return {
      resultVisible: Boolean(document.querySelector('#goldResultCard').getClientRects().length),
      quoteVisible: Boolean(document.querySelector('.quote-placeholder').getClientRects().length),
      realtimePriceHidden: !document.querySelector('#goldPriceInfo').getClientRects().length,
      manualQuoteVisible: Boolean(document.querySelector('#shopQuote').getClientRects().length),
      quoteCurrency: quoteCurrency.value,
      quoteValue: shopQuote.value,
      settlementText: document.querySelector('#settlementPreview').textContent.trim(),
      formulaArrow: getComputedStyle(document.querySelector('.formula-box > summary'), '::after').content,
      copyVisible: Boolean(document.querySelector('#goldActions').getClientRects().length),
      initiallyDisabled,
      validEnabled,
      staleDisabled,
      purity: document.querySelector('#goldPurityResult').textContent.trim(),
      metricNumberSize: metricNumber ? getComputedStyle(metricNumber).fontSize : null,
      metricNumberWeight: metricNumber ? getComputedStyle(metricNumber).fontWeight : null,
      metricAffixSize: metricAffix ? getComputedStyle(metricAffix).fontSize : null
    };
  })()`);
  assert.equal(gold.resultVisible, true);
  assert.equal(gold.quoteVisible, true);
  assert.equal(gold.realtimePriceHidden, true);
  assert.equal(gold.manualQuoteVisible, true);
  assert.equal(gold.quoteCurrency, "CNY");
  assert.equal(gold.quoteValue, "980");
  assert.match(gold.settlementText, /预计/u);
  assert.match(gold.formulaArrow, /⌄/u);
  assert.equal(gold.copyVisible, true);
  assert.equal(gold.initiallyDisabled, true);
  assert.equal(gold.validEnabled, true);
  assert.equal(gold.staleDisabled, true);
  assert.notEqual(gold.purity, "--");
  assert.equal(gold.metricNumberSize, "24px");
  assert.equal(gold.metricNumberWeight, "600");
  assert.equal(gold.metricAffixSize, "15px");
  results.push({ state: "gold-calculator", result: "PASS", ...gold });
  await markSyntheticState(cdp, sessionId, "黄金手动报价");
  await screenshot(cdp, sessionId, path.join(receiptRoot, "states", "gold-calculated.png"));
  await fullScreenshot(cdp, sessionId, path.join(receiptRoot, "states", "gold-quote-full.png"), 390);

  for (const [pageName, pagePath] of pages) {
    await navigate(cdp, sessionId, `${baseUrl}${pagePath}?browser-state=help-${pageName}`);
    const helpState = await evaluate(cdp, sessionId, `(() => {
      const visibleHelp = [...document.querySelectorAll('.page-help')].filter(item => getComputedStyle(item).display !== 'none');
      visibleHelp.forEach(item => { item.open = true; });
      return {
        visibleCount: visibleHelp.length,
        className: visibleHelp[0]?.className || '',
        text: visibleHelp[0]?.textContent.trim() || '',
        aboutContactCombined: document.body.textContent.includes('关于与联系'),
        manualSupportVisible: Boolean(document.querySelector('.footer-text-link')?.getClientRects().length)
      };
    })()`);
    await markSyntheticState(cdp, sessionId, `${pageName} 页面帮助`);
    await fullScreenshot(cdp, sessionId, path.join(receiptRoot, "states", `help-${pageName}-full.png`), 390);
    assert.equal(helpState.visibleCount, 1);
    assert.match(helpState.className, new RegExp(`page-help-${pageName}`, 'u'));
    assert.equal(helpState.aboutContactCombined, false);
    assert.equal(helpState.manualSupportVisible, true);
    results.push({ state: `help-${pageName}`, result: "PASS", ...helpState });
  }

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
