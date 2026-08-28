import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";
import { once } from "node:events";
import test from "node:test";
import { fileURLToPath } from "node:url";

const root = new URL("../", import.meta.url);

test("UI exposes layer switching, fullscreen, return flow, mobile layout and attribution", async () => {
  const [html, app, css] = await Promise.all([
    readFile(new URL("public/index.html", root), "utf8"),
    readFile(new URL("public/app.js", root), "utf8"),
    readFile(new URL("public/styles.css", root), "utf8")
  ]);
  assert.match(html, /data-style="satellite"/);
  assert.match(html, /data-style="hybrid"/);
  assert.match(html, /data-style="map"/);
  assert.match(html, /id="fullscreen-button"/);
  assert.match(html, /id="fit-button"/);
  assert.match(html, /id="return-button"/);
  assert.match(html, /id="attribution"/);
  assert.match(app, /requestFullscreen/);
  assert.match(app, /switchStyle/);
  assert.match(app, /renderer\.fitGeometry\(\)/);
  assert.match(css, /@media \(max-width: 640px\)/);
  assert.match(css, /#return-button \{ order: -1;/);
});

test("static prototype files contain no credential value or service token", async () => {
  const files = [
    "README.md", ".env.example", "server.mjs", "public/app.js", "public/index.html",
    "src/providers.js", "src/map-product-controller.js"
  ];
  const contents = await Promise.all(files.map(file => readFile(new URL(file, root), "utf8")));
  for (const content of contents) {
    assert.doesNotMatch(content, /MAPTILER_TEST_KEY[ \t]*=[ \t]*[^\s#]+/);
    assert.doesNotMatch(content, /Authorization:\s*Bearer/i);
    assert.doesNotMatch(content, /service[_ -]?token\s*[:=]\s*[A-Za-z0-9_-]{12,}/i);
  }
});

test("server health and logs expose configured status but not key value", async t => {
  const fakeSecret = "p09c-fake-secret-sentinel-do-not-log";
  const port = 33191;
  const child = spawn(process.execPath, [fileURLToPath(new URL("server.mjs", root))], {
    cwd: fileURLToPath(root),
    env: { ...process.env, MAPTILER_TEST_KEY: fakeSecret, P09C_PORT: String(port) },
    stdio: ["ignore", "pipe", "pipe"]
  });
  t.after(() => child.kill());
  let output = "";
  child.stdout.on("data", chunk => { output += chunk; });
  child.stderr.on("data", chunk => { output += chunk; });
  await Promise.race([
    once(child.stdout, "data"),
    once(child, "exit").then(([code]) => { throw new Error(`SERVER_EXITED_${code}: ${output}`); }),
    new Promise((_, reject) => setTimeout(() => reject(new Error("SERVER_START_TIMEOUT")), 3000))
  ]);
  const health = await (await fetch(`http://127.0.0.1:${port}/health`)).text();
  const mapLibreModule = await fetch(`http://127.0.0.1:${port}/vendor/maplibre-gl.mjs`);
  assert.match(health, /"mapTilerConfigured":true/);
  assert.equal(mapLibreModule.status, 200);
  assert.match(mapLibreModule.headers.get("content-type") || "", /text\/javascript/);
  assert.doesNotMatch(health, new RegExp(fakeSecret));
  assert.doesNotMatch(output, new RegExp(fakeSecret));
});
