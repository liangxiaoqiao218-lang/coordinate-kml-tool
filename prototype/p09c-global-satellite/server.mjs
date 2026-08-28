import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { extname, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL(".", import.meta.url));
const publicRoot = resolve(root, "public");
const sourceRoot = resolve(root, "src");
const vendorRoot = resolve(root, "node_modules", "maplibre-gl", "dist");
const port = Number.parseInt(process.env.P09C_PORT || "3000", 10);
const apiKey = String(process.env.MAPTILER_TEST_KEY || "").trim();

const contentTypes = Object.freeze({
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8"
});

function safeFile(base, relativePath) {
  const resolved = resolve(base, relativePath);
  if (resolved !== base && !resolved.startsWith(`${base}${sep}`)) return null;
  return resolved;
}

async function respondFile(response, base, relativePath) {
  const file = safeFile(base, relativePath);
  if (!file) {
    response.writeHead(403).end("Forbidden");
    return;
  }
  try {
    const body = await readFile(file);
    response.writeHead(200, {
      "Content-Type": contentTypes[extname(file)] || "application/octet-stream",
      "Cache-Control": "no-store"
    });
    response.end(body);
  } catch {
    response.writeHead(404).end("Not found");
  }
}

const server = createServer(async (request, response) => {
  const url = new URL(request.url || "/", "http://localhost");
  if (url.pathname === "/health") {
    response.writeHead(200, { "Content-Type": contentTypes[".json"], "Cache-Control": "no-store" });
    response.end(JSON.stringify({
      status: "ok",
      prototype: "P09C_GLOBAL_SATELLITE_PROTOTYPE",
      mapTilerConfigured: Boolean(apiKey),
      chinaProviderStatus: "PENDING_CREDENTIAL",
      productionDeploymentAuthorized: false
    }));
    return;
  }
  if (url.pathname === "/runtime-config.js") {
    response.writeHead(200, {
      "Content-Type": contentTypes[".js"],
      "Cache-Control": "no-store, max-age=0",
      Pragma: "no-cache"
    });
    response.end(`globalThis.__P09C_RUNTIME_CONFIG__ = Object.freeze(${JSON.stringify({
      mapTilerConfigured: Boolean(apiKey),
      mapTilerTestKey: apiKey || null
    })});`);
    return;
  }
  if (url.pathname === "/" || url.pathname === "/index.html") return respondFile(response, publicRoot, "index.html");
  if (url.pathname === "/app.js") return respondFile(response, publicRoot, "app.js");
  if (url.pathname === "/styles.css") return respondFile(response, publicRoot, "styles.css");
  if (url.pathname.startsWith("/src/")) return respondFile(response, sourceRoot, url.pathname.slice(5));
  if (url.pathname.startsWith("/vendor/")) return respondFile(response, vendorRoot, url.pathname.slice(8));
  response.writeHead(404).end("Not found");
});

server.listen(port, "127.0.0.1", () => {
  console.log(`P09C isolated prototype ready at http://localhost:${port}`);
  console.log(`MAPTILER_TEST_KEY configured = ${Boolean(apiKey)}`);
});
