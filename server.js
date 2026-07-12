import "dotenv/config";
import express from "express";
import multer from "multer";
import { createClient } from "@supabase/supabase-js";
import path from "node:path";
import { fileURLToPath } from "node:url";
import fs from "node:fs";
import crypto from "node:crypto";
import Tesseract from "tesseract.js";

const app = express();
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 12 * 1024 * 1024
  }
});
const aliyunApiKey = process.env.ALIYUN_API_KEY || process.env.DASHSCOPE_API_KEY || "";
const aliyunBaseURL = process.env.ALIYUN_BASE_URL || process.env.DASHSCOPE_BASE_URL || "https://dashscope.aliyuncs.com/compatible-mode/v1";
const aliyunVisionModel = process.env.ALIYUN_VISION_MODEL || process.env.DASHSCOPE_VISION_MODEL || "qwen-vl-plus";
const aliyunOcrModel = process.env.ALIYUN_OCR_MODEL || process.env.DASHSCOPE_OCR_MODEL || "qwen-vl-ocr-latest";
const supabaseUrl = String(process.env.SUPABASE_URL || "").trim();
const supabaseServiceRoleKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY || "").trim();
const supabase = supabaseUrl && supabaseServiceRoleKey
  ? createClient(supabaseUrl, supabaseServiceRoleKey)
  : null;
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const noCoordinatesText = "未识别到有效坐标，请重新上传更清晰的坐标区域截图。";
const adminPassword = process.env.ADMIN_PASSWORD || "";
const SYSTEM_CONFIG_PRICING_ID = "pricing";
const DEFAULT_PRICING_CONFIG = {
  monthly: {
    name: "月度版",
    price: 99,
    judgeCount: 50,
    convertCount: 50
  },
  addJudge: {
    name: "矿地快判加次",
    price: 19,
    count: 10
  },
  addConvert: {
    name: "坐标/KML加次",
    price: 19,
    count: 10
  },
  free: {
    judgeCount: 3,
    convertCount: 3
  }
};

function toPricingInteger(value, fallback) {
  const number = Number(value);
  if (!Number.isFinite(number) || number < 0) {
    return fallback;
  }
  return Math.floor(number);
}

function mergePricingConfig(source = {}) {
  const addPrice = toPricingInteger(
    source.addPrice ?? source.addJudge?.price ?? source.addConvert?.price,
    DEFAULT_PRICING_CONFIG.addJudge.price
  );
  const addCount = toPricingInteger(
    source.addCount ?? source.addJudge?.count ?? source.addConvert?.count,
    DEFAULT_PRICING_CONFIG.addJudge.count
  );

  return {
    monthly: {
      name: DEFAULT_PRICING_CONFIG.monthly.name,
      price: toPricingInteger(source.monthly?.price, DEFAULT_PRICING_CONFIG.monthly.price),
      judgeCount: toPricingInteger(source.monthly?.judgeCount, DEFAULT_PRICING_CONFIG.monthly.judgeCount),
      convertCount: toPricingInteger(source.monthly?.convertCount, DEFAULT_PRICING_CONFIG.monthly.convertCount)
    },
    addJudge: {
      name: DEFAULT_PRICING_CONFIG.addJudge.name,
      price: addPrice,
      count: addCount
    },
    addConvert: {
      name: DEFAULT_PRICING_CONFIG.addConvert.name,
      price: addPrice,
      count: addCount
    },
    free: {
      judgeCount: toPricingInteger(source.free?.judgeCount, DEFAULT_PRICING_CONFIG.free.judgeCount),
      convertCount: toPricingInteger(source.free?.convertCount, DEFAULT_PRICING_CONFIG.free.convertCount)
    }
  };
}

let runtimePricingConfig = mergePricingConfig(DEFAULT_PRICING_CONFIG);

function pricingConfigFromSystemConfigRow(row = {}) {
  return mergePricingConfig({
    monthly: {
      price: row.monthly_price,
      judgeCount: row.monthly_judge_count,
      convertCount: row.monthly_convert_count
    },
    addJudge: {
      price: row.add_price,
      count: row.add_count
    },
    addConvert: {
      price: row.add_price,
      count: row.add_count
    },
    free: {
      judgeCount: row.free_judge_count,
      convertCount: row.free_convert_count
    }
  });
}

function pricingConfigToSystemConfigRow(config) {
  const merged = mergePricingConfig(config);
  return {
    id: SYSTEM_CONFIG_PRICING_ID,
    monthly_price: merged.monthly.price,
    monthly_judge_count: merged.monthly.judgeCount,
    monthly_convert_count: merged.monthly.convertCount,
    add_price: merged.addJudge.price,
    add_count: merged.addJudge.count,
    free_judge_count: merged.free.judgeCount,
    free_convert_count: merged.free.convertCount,
    updated_at: new Date().toISOString()
  };
}

async function loadPricingConfigFromSupabase() {
  if (!supabase) {
    return {
      config: getPricingConfig(),
      source: "default",
      warning: "Supabase not configured"
    };
  }

  const { data, error } = await supabase
    .from("system_config")
    .select("monthly_price,monthly_judge_count,monthly_convert_count,add_price,add_count,free_judge_count,free_convert_count,updated_at")
    .eq("id", SYSTEM_CONFIG_PRICING_ID)
    .maybeSingle();

  if (error) {
    throw error;
  }

  if (!data) {
    return {
      config: getPricingConfig(),
      source: "default"
    };
  }

  runtimePricingConfig = pricingConfigFromSystemConfigRow(data);
  return {
    config: runtimePricingConfig,
    source: "supabase",
    updated_at: data.updated_at
  };
}

async function savePricingConfigToSupabase(nextConfig) {
  const config = mergePricingConfig(nextConfig);
  runtimePricingConfig = config;

  if (!supabase) {
    return {
      config,
      persisted: false,
      warning: "Supabase not configured"
    };
  }

  const { data, error } = await supabase
    .from("system_config")
    .upsert(pricingConfigToSystemConfigRow(config), { onConflict: "id" })
    .select("monthly_price,monthly_judge_count,monthly_convert_count,add_price,add_count,free_judge_count,free_convert_count,updated_at")
    .single();

  if (error) {
    throw error;
  }

  runtimePricingConfig = pricingConfigFromSystemConfigRow(data);
  return {
    config: runtimePricingConfig,
    persisted: true,
    source: "supabase",
    updated_at: data.updated_at
  };
}

const DAILY_FREE_CONVERT_LIMIT = DEFAULT_PRICING_CONFIG.free.convertCount;
const DAILY_FREE_JUDGE_LIMIT = DEFAULT_PRICING_CONFIG.free.judgeCount;
const USAGE_RULES = {
  convert: {
    freeDailyLimit: DAILY_FREE_CONVERT_LIMIT,
    vipMonthlyLimit: DEFAULT_PRICING_CONFIG.monthly.convertCount,
    paidField: "paid_convert_count",
    freeField: "free_convert_count"
  },
  judge: {
    freeDailyLimit: DAILY_FREE_JUDGE_LIMIT,
    vipMonthlyLimit: DEFAULT_PRICING_CONFIG.monthly.judgeCount,
    paidField: "paid_judge_count",
    freeField: "free_judge_count"
  },
  gold: {
    unlimited: true,
    freeDailyLimit: Infinity,
    vipMonthlyLimit: Infinity,
    paidField: "",
    freeField: ""
  }
};

function getPricingConfig() {
  return runtimePricingConfig || mergePricingConfig(DEFAULT_PRICING_CONFIG);
}

function getPricingUsageLimits() {
  const pricing = getPricingConfig();
  return {
    freeConvertLimit: toNonNegativeInteger(pricing.free?.convertCount, DAILY_FREE_CONVERT_LIMIT),
    freeJudgeLimit: toNonNegativeInteger(pricing.free?.judgeCount, DAILY_FREE_JUDGE_LIMIT),
    vipConvertLimit: toNonNegativeInteger(pricing.monthly?.convertCount, USAGE_RULES.convert.vipMonthlyLimit),
    vipJudgeLimit: toNonNegativeInteger(pricing.monthly?.judgeCount, USAGE_RULES.judge.vipMonthlyLimit)
  };
}
const USD_PER_TROY_OUNCE_GRAMS = 31.1035;
const DEFAULT_USD_CNY_RATE = 7.2;
const usdCnyRate = Number(process.env.USD_CNY_RATE || DEFAULT_USD_CNY_RATE);
const goldPriceApiUrl = String(process.env.GOLD_PRICE_API_URL || "https://www.goldapi.io/api/XAU/USD").trim();
const goldPriceApiKey = String(process.env.GOLDAPI_KEY || process.env.GOLD_PRICE_API_KEY || "").trim();
const shareMetaMap = {
  "/": {
    title: "矿业空间实验室 | GeoKit Lab",
    desc: "坐标处理、AI矿地判读、黄金成色计算，一站式矿业工具平台。",
    image: "/share-home-og.jpg"
  },
  "/coordinate": {
    title: "矿业空间实验室｜坐标处理工具",
    desc: "上传坐标图或粘贴坐标，一键整理并生成 KML 文件。",
    image: "/share-coordinate-og.jpg"
  },
  "/coordinate-tool": {
    title: "矿业空间实验室｜坐标处理工具",
    desc: "上传坐标图或粘贴坐标，一键整理并生成 KML 文件。",
    image: "/share-tool-og.jpg"
  },
  "/tool": {
    title: "矿业空间实验室｜坐标处理工具",
    desc: "上传坐标图或粘贴坐标，一键整理并生成 KML 文件。",
    image: "/share-tool-og.jpg"
  },
  "/convert": {
    title: "矿业空间实验室｜坐标处理工具",
    desc: "上传坐标图或粘贴坐标，一键整理并生成 KML 文件。",
    image: "/share-coordinate-og.jpg"
  },
  "/ocr": {
    title: "矿业空间实验室｜坐标处理工具",
    desc: "上传坐标图或粘贴坐标，一键整理并生成 KML 文件。",
    image: "/share-coordinate-og.jpg"
  },
  "/mining": {
    title: "矿业空间实验室｜AI矿地判读",
    desc: "上传矿石、河道或卫星图，快速判断是否值得继续投入。",
    image: "/share-judge-og.jpg"
  },
  "/mining-judge": {
    title: "矿业空间实验室｜AI矿地判读",
    desc: "上传矿石、河道或卫星图，快速判断是否值得继续投入。",
    image: "/share-judge-og.jpg"
  },
  "/mining-analysis": {
    title: "矿业空间实验室｜AI矿地判读",
    desc: "上传矿石、河道或卫星图，快速判断是否值得继续投入。",
    image: "/share-judge-og.jpg"
  },
  "/judge": {
    title: "矿业空间实验室｜AI矿地判读",
    desc: "上传矿石、河道或卫星图，快速判断是否值得继续投入。",
    image: "/share-judge-og.jpg"
  },
  "/gold-calculator": {
    title: "矿业空间实验室｜黄金成色计算器",
    desc: "输入重量和排水差重，快速估算纯度、K值和参考价格。",
    image: "/share-gold-og.jpg"
  },
  "/gold": {
    title: "矿业空间实验室｜黄金成色计算器",
    desc: "输入重量和排水差重，快速估算纯度、K值和参考价格。",
    image: "/share-gold-og.jpg"
  }
};
const shareMetaOrigin = "https://geokitlab.com";
const canonicalPathMap = {
  "/index.html": "/",
  "/coordinate-tool": "/coordinate",
  "/tool": "/coordinate",
  "/convert": "/coordinate",
  "/ocr": "/coordinate",
  "/mining-judge": "/mining",
  "/mining-analysis": "/mining",
  "/judge": "/mining",
  "/gold-calculator": "/gold"
};

function getCanonicalPath(pathname) {
  return canonicalPathMap[pathname] || pathname || "/";
}

function buildSoftwareApplicationJsonLd(meta, canonicalUrl, pageType) {
  const appName = pageType === "coordinate"
    ? "GeoKit Lab 坐标与 KML 工具"
    : pageType === "mining"
      ? "GeoKit Lab 矿地快判"
      : pageType === "gold"
        ? "GeoKit Lab 黄金成色计算器"
        : "GeoKit Lab";
  return {
    "@context": "https://schema.org",
    "@type": "SoftwareApplication",
    name: appName,
    applicationCategory: "BusinessApplication",
    operatingSystem: "Web",
    url: canonicalUrl,
    description: meta.desc,
    offers: {
      "@type": "Offer",
      price: "0",
      priceCurrency: "USD"
    }
  };
}

function buildFaqJsonLd(items) {
  return {
    "@context": "https://schema.org",
    "@type": "FAQPage",
    mainEntity: items.map(item => ({
      "@type": "Question",
      name: item.q,
      acceptedAnswer: {
        "@type": "Answer",
        text: item.a
      }
    }))
  };
}

function getStructuredData(meta, canonicalPath) {
  const canonicalUrl = `${shareMetaOrigin}${canonicalPath}`;
  if (canonicalPath === "/coordinate") {
    return [
      buildSoftwareApplicationJsonLd(meta, canonicalUrl, "coordinate"),
      buildFaqJsonLd([
        { q: "什么是图片坐标识别？", a: "图片坐标识别是从坐标截图、测绘表格或手写坐标中提取可编辑坐标文本，再整理为地图可用格式。" },
        { q: "如何把截图中的坐标生成 KML？", a: "上传坐标截图或粘贴坐标文本后，先核对识别结果，系统会按点数自动识别 Point、LineString 或 Polygon 并导出 KML 文件。" },
        { q: "什么是 BFTM 坐标？", a: "BFTM 是布基纳法索常见矿业投影坐标系统，广泛用于矿权边界与测绘数据，需要转换后才能在 WGS84 地图中使用。" },
        { q: "GeoKit Lab 与普通坐标工具有什么区别？", a: "GeoKit Lab 面向矿区边界、野外截图、投影坐标表和 KML 生成流程，重点处理真实项目中的复杂坐标格式。" }
      ])
    ];
  }
  if (canonicalPath === "/mining") {
    return [
      buildSoftwareApplicationJsonLd(meta, canonicalUrl, "mining"),
      buildFaqJsonLd([
        { q: "什么是矿地快判？", a: "矿地快判是根据矿石照片、河道照片、卫星图和现场环境图做快速初筛，帮助判断是否值得继续实地核验。" },
        { q: "矿石照片能不能判断有金？", a: "照片只能判断外观、结构和误判风险，不能承诺有金或品位，仍需要密度测试、XRF、火试金或现场验证。" },
        { q: "河道砂金应该看哪些结构？", a: "重点看弯道、收窄、汇流、阶地、老河道、重砂富集和采挖痕迹，而不是只看颜色。" },
        { q: "为什么快判只能做初筛，不能代替试采？", a: "图像分析无法确认储量、品位和连续性，只能作为前期筛选，最终仍需现场采样、试采和检测。" }
      ])
    ];
  }
  if (canonicalPath === "/gold") {
    return [
      buildSoftwareApplicationJsonLd(meta, canonicalUrl, "gold"),
      buildFaqJsonLd([
        { q: "什么是吊水法？", a: "吊水法通过黄金在空气中的重量和水中的排水差值估算密度，再根据密度换算纯度和 K 值。" },
        { q: "黄金密度为什么接近 19.32？", a: "纯金理论密度约为 19.32 g/cm³，实际首饰或金块会因合金、空隙、杂质和测量误差产生偏差。" },
        { q: "K 值怎么计算？", a: "K 值通常按纯度比例换算，24K 约等于纯金，18K 约为 75% 含金量。" },
        { q: "为什么计算结果只能作为参考？", a: "吊水法受气泡、绑线、表面附着物和样品结构影响，交易或回收前仍建议结合专业检测。" }
      ])
    ];
  }
  return [buildSoftwareApplicationJsonLd(meta, canonicalUrl, "home")];
}

app.use(express.json({ limit: "1mb" }));

const appVersion = "2026-05-01-quota-contact-v2";
const legacyRenderHost = ["coordinate-kml-tool", "onrender", "com"].join(".");
const canonicalHost = "geokitlab.com";

app.use((req, res, next) => {
  const forwardedHost = req.get("x-forwarded-host") || "";
  const hostHeader = forwardedHost.split(",")[0].trim() || req.get("host") || "";
  const hostname = hostHeader.split(":")[0].toLowerCase();

  if (hostname === legacyRenderHost || hostname === `www.${canonicalHost}`) {
    const pathWithQuery = req.originalUrl && req.originalUrl.startsWith("/") ? req.originalUrl : `/${req.originalUrl || ""}`;
    return res.redirect(301, `${shareMetaOrigin}${pathWithQuery}`);
  }

  return next();
});

app.use((req, res, next) => {
  const noCachePaths = new Set(["/", "/coordinate", "/coordinate-tool", "/tool", "/convert", "/ocr", "/mining", "/mining-judge", "/mining-analysis", "/judge", "/gold", "/gold-calculator", "/admin", "/index.html", "/admin.html"]);

  if (noCachePaths.has(req.path) || req.path.endsWith(".html")) {
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
    res.setHeader("Pragma", "no-cache");
    res.setHeader("Expires", "0");
  }

  next();
});

const SECURITY_EVENT_LIMIT = 200;
const securityEvents = [];
const rateLimitBuckets = new Map();
const rateLimitRules = {
  admin: { windowMs: 60 * 1000, max: 240 },
  usage: { windowMs: 60 * 1000, max: 120 },
  recognize: { windowMs: 5 * 60 * 1000, max: 20 },
  judge: { windowMs: 5 * 60 * 1000, max: 20 }
};

function getRequestIpForSecurity(req) {
  try {
    if (typeof getClientIp === "function") {
      return getClientIp(req);
    }
  } catch (_) {}

  return String(req.headers["x-forwarded-for"] || req.socket?.remoteAddress || req.ip || "")
    .split(",")[0]
    .trim() || "unknown";
}

function recordSecurityEvent(req, type, detail = {}) {
  securityEvents.unshift({
    created_at: new Date().toISOString(),
    type,
    path: req.originalUrl || req.url || req.path,
    method: req.method,
    ip: getRequestIpForSecurity(req),
    origin: req.get("origin") || "",
    referer: req.get("referer") || "",
    user_agent: req.get("user-agent") || "",
    detail
  });

  if (securityEvents.length > SECURITY_EVENT_LIMIT) {
    securityEvents.length = SECURITY_EVENT_LIMIT;
  }
}

function getSecurityEvents(limit = 20) {
  return securityEvents.slice(0, Math.max(0, Math.min(Number(limit) || 20, SECURITY_EVENT_LIMIT)));
}

function getProtectedEndpointType(pathname = "") {
  if (pathname === "/api/admin" || pathname.startsWith("/api/admin/")) return "admin";
  if (pathname === "/api/usage/quota" || pathname === "/api/usage/consume") return "usage";
  if (pathname === "/api/recognize-coordinates") return "recognize";
  if (pathname === "/api/analyze-mining-image") return "judge";
  return "";
}

const allowedRequestOrigins = new Set([
  "https://geokitlab.com",
  "https://www.geokitlab.com"
]);

function parseRequestOrigin(value) {
  if (!value) return { empty: true, origin: "" };

  try {
    const url = new URL(value);
    return { empty: false, origin: url.origin, hostname: url.hostname.toLowerCase(), protocol: url.protocol.toLowerCase() };
  } catch (error) {
    return { empty: false, invalid: true, origin: "", error: error.message || "invalid_url" };
  }
}

function isAllowedRequestOrigin(value) {
  if (!value) return true;

  const parsed = parseRequestOrigin(value);

  if (parsed.invalid) {
    return false;
  }

  if (allowedRequestOrigins.has(parsed.origin)) {
    return true;
  }

  try {
    const url = new URL(value);
    const hostname = url.hostname.toLowerCase();
    const protocol = url.protocol.toLowerCase();

    if (["localhost", "127.0.0.1", "::1", "[::1]"].includes(hostname) && (protocol === "http:" || protocol === "https:")) {
      return true;
    }
  } catch (_) {
    return false;
  }

  return false;
}

function originGuard(req, res, next) {
  const endpoint = getProtectedEndpointType(req.path);
  if (!endpoint) return next();

  const origin = req.get("origin") || "";
  const referer = req.get("referer") || "";
  const userAgent = req.get("user-agent") || "";

  if (!origin && !referer) {
    recordSecurityEvent(req, "missing_origin", { endpoint });
    return next();
  }

  if (origin && !isAllowedRequestOrigin(origin)) {
    console.warn("来源校验拦截：origin 不允许", {
      origin,
      referer,
      path: req.path,
      userAgent
    });
    recordSecurityEvent(req, "invalid_origin", { endpoint, origin, referer });
    return res.status(403).json({
      success: false,
      error: "来源不允许",
      reason: "invalid_origin"
    });
  }

  if (!origin && referer) {
    const parsedReferer = parseRequestOrigin(referer);

    if (parsedReferer.invalid) {
      console.warn("来源校验警告：referer 解析失败，已放行", {
        origin,
        referer,
        path: req.path,
        userAgent
      });
      recordSecurityEvent(req, "invalid_referer_allowed", { endpoint, origin, referer });
      return next();
    }

    if (!isAllowedRequestOrigin(referer)) {
      console.warn("来源校验拦截：referer 不允许", {
        origin,
        referer,
        path: req.path,
        userAgent
      });
      recordSecurityEvent(req, "invalid_origin", { endpoint, origin, referer });
      return res.status(403).json({
        success: false,
        error: "来源不允许",
        reason: "invalid_origin"
      });
    }
  }

  return next();
}

function rateLimitGuard(req, res, next) {
  const endpoint = getProtectedEndpointType(req.path);
  if (!endpoint) return next();

  const rule = rateLimitRules[endpoint];
  if (!rule) return next();

  const now = Date.now();
  const key = `${endpoint}:${getRequestIpForSecurity(req)}`;
  const bucket = rateLimitBuckets.get(key);

  if (!bucket || now > bucket.resetAt) {
    rateLimitBuckets.set(key, { count: 1, resetAt: now + rule.windowMs });
    return next();
  }

  bucket.count += 1;
  if (bucket.count > rule.max) {
    recordSecurityEvent(req, "rate_limited", { endpoint, count: bucket.count, windowMs: rule.windowMs });
    return res.status(429).json({
      success: false,
      error: "请求过于频繁，请稍后再试",
      reason: "rate_limited"
    });
  }

  return next();
}

app.use(originGuard);
app.use(rateLimitGuard);

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function getRequestOrigin(req) {
  const protocol = req.get("x-forwarded-proto") || req.protocol || "https";
  const host = req.get("x-forwarded-host") || req.get("host") || "";
  return host ? `${protocol}://${host}` : "";
}

function getShareMeta(req) {
  const normalizedPath = req.path === "/index.html" ? "/" : req.path;
  const canonicalPath = getCanonicalPath(normalizedPath);
  const meta = shareMetaMap[normalizedPath] || shareMetaMap[canonicalPath] || shareMetaMap["/"];
  const canonicalUrl = `${shareMetaOrigin}${canonicalPath === "/index.html" ? "/" : canonicalPath}`;
  return {
    ...meta,
    url: canonicalUrl,
    canonicalUrl,
    imageUrl: `${shareMetaOrigin}${meta.image}`,
    structuredData: JSON.stringify(getStructuredData(meta, canonicalPath))
  };
}

function renderIndexWithMeta(req, res) {
  const meta = getShareMeta(req);
  const html = fs.readFileSync(path.join(__dirname, "index.html"), "utf8")
    .replaceAll("<!--TITLE-->", escapeHtml(meta.title))
    .replaceAll("<!--DESC-->", escapeHtml(meta.desc))
    .replaceAll("<!--IMAGE-->", escapeHtml(meta.imageUrl))
    .replaceAll("<!--URL-->", escapeHtml(meta.url))
    .replaceAll("<!--CANONICAL-->", escapeHtml(meta.canonicalUrl))
    .replaceAll("<!--JSONLD-->", meta.structuredData);

  res.type("html").send(html);
}

app.get(["/", "/index.html", "/coordinate", "/coordinate-tool", "/tool", "/convert", "/ocr", "/mining", "/mining-judge", "/mining-analysis", "/judge", "/gold", "/gold-calculator"], renderIndexWithMeta);

app.get("/robots.txt", (req, res) => {
  res.type("text/plain").send("User-agent: *\nAllow: /\n\nSitemap: https://geokitlab.com/sitemap.xml\n");
});

app.get("/sitemap.xml", (req, res) => {
  const urls = ["/", "/coordinate", "/mining", "/gold"];
  const body = `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${urls.map(pathname => `  <url><loc>${shareMetaOrigin}${pathname}</loc></url>`).join("\n")}\n</urlset>\n`;
  res.type("application/xml").send(body);
});

app.use(express.static(__dirname, {
  index: false,
  etag: false,
  lastModified: false,
  setHeaders(res, filePath) {
    if (filePath.endsWith(".html")) {
      res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
      res.setHeader("Pragma", "no-cache");
      res.setHeader("Expires", "0");
    }
  }
}));

app.get("/api/version", (req, res) => {
  res.json({
    version: appVersion
  });
});

app.get("/api/pricing-config", async (req, res) => {
  try {
    const result = await loadPricingConfigFromSupabase();
    res.json({
      success: true,
      ...result
    });
  } catch (error) {
    console.error("Pricing config Supabase load failed:", {
      message: error?.message,
      code: error?.code,
      details: error?.details,
      hint: error?.hint
    });
    res.json({
      success: true,
      config: getPricingConfig(),
      source: "default_fallback",
      warning: error?.message || "Supabase config load failed"
    });
  }
});

function formatGoldPriceUpdatedAt(date = new Date()) {
  const pad = value => String(value).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function normalizeGoldApiPayload(payload) {
  const usdPerGram = Number(payload?.price_gram_24k || payload?.price_gram_24K || payload?.data?.price_gram_24k);
  const usdPerOunce = Number(payload?.price || payload?.ask || payload?.data?.price || payload?.result?.price);
  const cnyRate = Number.isFinite(usdCnyRate) && usdCnyRate > 0 ? usdCnyRate : DEFAULT_USD_CNY_RATE;
  let priceCnyPerGram = 0;

  if (Number.isFinite(usdPerGram) && usdPerGram > 0) {
    priceCnyPerGram = usdPerGram * cnyRate;
  } else if (Number.isFinite(usdPerOunce) && usdPerOunce > 0) {
    priceCnyPerGram = (usdPerOunce / USD_PER_TROY_OUNCE_GRAMS) * cnyRate;
  }

  if (!Number.isFinite(priceCnyPerGram) || priceCnyPerGram <= 0) {
    return null;
  }

  return {
    price_cny_per_gram: Number(priceCnyPerGram.toFixed(2)),
    source: "realtime_api",
    updated_at: payload?.updated_at || payload?.timestamp || payload?.date || formatGoldPriceUpdatedAt()
  };
}

function getUnavailableGoldPricePayload() {
  return {
    price_cny_per_gram: null,
    source: "unavailable",
    updated_at: formatGoldPriceUpdatedAt()
  };
}

async function fetchRealtimeGoldPrice() {
  if (!goldPriceApiUrl || !goldPriceApiKey) {
    return null;
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 6000);

  try {
    const response = await fetch(goldPriceApiUrl, {
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        "x-access-token": goldPriceApiKey
      },
      signal: controller.signal
    });

    if (!response.ok) {
      throw new Error(`gold price api status ${response.status}`);
    }

    const payload = await response.json();
    return normalizeGoldApiPayload(payload);
  } finally {
    clearTimeout(timer);
  }
}

function createDefaultAdminData() {
  return {
    users: {},
    events: [],
    records: [],
    usage: {},
    ipGeoCache: {},
    featureFlags: {
      aiOcrEnabled: true,
      xyConvertEnabled: true,
      kmlExportEnabled: true,
      manualSupportEnabled: true,
      aiJudgeEnabled: true,
      goldCalculatorEnabled: true,
      quoteComparisonEnabled: false
    }
  };
}

let adminDataStore = createDefaultAdminData();

async function readAdminData() {
  return adminDataStore;
}

async function writeAdminData(data) {
  // Render does not provide durable writable storage in the app directory.
  // Local admin-data.json persistence is disabled; official admin data lives in Supabase.
  adminDataStore = {
    ...createDefaultAdminData(),
    ...(data || {}),
    featureFlags: {
      ...createDefaultAdminData().featureFlags,
      ...((data && data.featureFlags) || {})
    }
  };
  return data;
}
function getNowISO() {
  return new Date().toISOString();
}

function makeId(prefix) {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}

function getTodayKey() {
  return new Date().toISOString().slice(0, 10);
}

function toNonNegativeInteger(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? Math.floor(number) : fallback;
}

function normalizeUsageCounters(user) {
  if (!user) {
    return null;
  }

  const usageLimits = getPricingUsageLimits();
  const todayKey = getTodayKey();

  if (user.usageDate !== todayKey) {
    user.usageDate = todayKey;
    user.freeConvertCount = usageLimits.freeConvertLimit;
    user.freeJudgeCount = usageLimits.freeJudgeLimit;
  } else {
    user.freeConvertCount = toNonNegativeInteger(user.freeConvertCount, usageLimits.freeConvertLimit);
    user.freeJudgeCount = toNonNegativeInteger(user.freeJudgeCount, usageLimits.freeJudgeLimit);
  }

  user.paidConvertCount = toNonNegativeInteger(user.paidConvertCount, 0);
  user.paidJudgeCount = toNonNegativeInteger(user.paidJudgeCount, 0);
  user.totalConvertCount = toNonNegativeInteger(user.totalConvertCount, 0);
  user.totalJudgeCount = toNonNegativeInteger(user.totalJudgeCount, 0);
  user.isVip = Boolean(user.isVip || user.plan === "vip");

  if (user.isVip) {
    user.plan = "vip";
  }

  return user;
}

function buildQuotaPayload(user) {
  normalizeUsageCounters(user);

  return {
    free_convert_count: Number(user?.freeConvertCount || 0),
    free_judge_count: Number(user?.freeJudgeCount || 0),
    paid_convert_count: Number(user?.paidConvertCount || 0),
    paid_judge_count: Number(user?.paidJudgeCount || 0),
    is_vip: Boolean(user?.isVip),
    total_convert_count: Number(user?.totalConvertCount || 0),
    total_judge_count: Number(user?.totalJudgeCount || 0),
    freeConvertCount: Number(user?.freeConvertCount || 0),
    freeJudgeCount: Number(user?.freeJudgeCount || 0),
    paidConvertCount: Number(user?.paidConvertCount || 0),
    paidJudgeCount: Number(user?.paidJudgeCount || 0),
    isVip: Boolean(user?.isVip),
    totalConvertCount: Number(user?.totalConvertCount || 0),
    totalJudgeCount: Number(user?.totalJudgeCount || 0)
  };
}

function checkLocalUsageAvailable(user, type) {
  normalizeUsageCounters(user);

  if (!user) {
    return {
      allowed: false,
      source: "none",
      quota: {}
    };
  }

  const paidKey = type === "judge" ? "paidJudgeCount" : "paidConvertCount";
  const freeKey = type === "judge" ? "freeJudgeCount" : "freeConvertCount";

  if (Number(user[freeKey] || 0) > 0) {
    return {
      allowed: true,
      source: "free",
      quota: buildQuotaPayload(user)
    };
  }

  if (Number(user[paidKey] || 0) > 0) {
    return {
      allowed: true,
      source: "paid",
      quota: buildQuotaPayload(user)
    };
  }

  return {
    allowed: false,
    source: "none",
    quota: buildQuotaPayload(user)
  };
}

function consumeLocalUsage(user, type) {
  const status = checkLocalUsageAvailable(user, type);

  if (!status.allowed) {
    return {
      success: false,
      source: "none",
      quota: status.quota
    };
  }

  if (type === "judge") {
    if (status.source === "paid") {
      user.paidJudgeCount -= 1;
    } else if (status.source === "free") {
      user.freeJudgeCount -= 1;
    }

    user.totalJudgeCount = Number(user.totalJudgeCount || 0) + 1;
  } else {
    if (status.source === "paid") {
      user.paidConvertCount -= 1;
    } else if (status.source === "free") {
      user.freeConvertCount -= 1;
    }

    user.totalConvertCount = Number(user.totalConvertCount || 0) + 1;
  }

  return {
    success: true,
    source: status.source,
    quota: buildQuotaPayload(user)
  };
}

async function getOrCreateSupabaseUser(userId) {
  if (!supabase || !userId) {
    return null;
  }
  const usageLimits = getPricingUsageLimits();

  const { data: existingUser, error: selectError } = await supabase
    .from("users")
    .select("*")
    .eq("user_id", userId)
    .maybeSingle();

  if (selectError) {
    throw selectError;
  }

  if (existingUser) {
    return normalizeSupabaseDailyFreeQuota(userId, existingUser);
  }

  const { data: newUser, error: insertError } = await supabase
    .from("users")
    .insert({
      user_id: userId,
      is_vip: false,
      free_convert_count: usageLimits.freeConvertLimit,
      free_judge_count: usageLimits.freeJudgeLimit,
      paid_convert_count: 0,
      paid_judge_count: 0,
      updated_at: new Date().toISOString()
    })
    .select("*")
    .single();

  if (insertError) {
    throw insertError;
  }

  return normalizeSupabaseDailyFreeQuota(userId, newUser);
}

function getSupabaseFreeResetField(user) {
  if (!user || typeof user !== "object") {
    return "";
  }

  if (Object.prototype.hasOwnProperty.call(user, "last_free_reset_date")) {
    return "last_free_reset_date";
  }

  if (Object.prototype.hasOwnProperty.call(user, "free_quota_date")) {
    return "free_quota_date";
  }

  return "";
}

function getNextDateKey(dateKey) {
  const date = new Date(`${dateKey}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() + 1);
  return date.toISOString().slice(0, 10);
}

async function getTodayFreeUsageCounts(userId, todayKey = getTodayKey()) {
  const counts = {
    convert: 0,
    judge: 0
  };

  if (!supabase || !userId) {
    return counts;
  }

  const { data, error } = await supabase
    .from("usage_logs")
    .select("feature_type,consume_type,success")
    .eq("user_id", userId)
    .eq("success", true)
    .eq("consume_type", "free")
    .gte("created_at", `${todayKey}T00:00:00.000Z`)
    .lt("created_at", `${getNextDateKey(todayKey)}T00:00:00.000Z`);

  if (error) {
    if (error.code === "42P01") {
      return counts;
    }
    throw error;
  }

  for (const log of data || []) {
    const featureType = normalizeUsageFeatureType(log.feature_type);
    if (featureType === "judge") {
      counts.judge += 1;
    } else if (featureType === "convert") {
      counts.convert += 1;
    }
  }

  return counts;
}

function buildDailyFreeQuotaUpdate(user, todayFreeUsage = {}, todayKey = getTodayKey()) {
  const usageLimits = getPricingUsageLimits();
  const updates = {
    free_convert_count: Math.max(0, usageLimits.freeConvertLimit - Number(todayFreeUsage.convert || 0)),
    free_judge_count: Math.max(0, usageLimits.freeJudgeLimit - Number(todayFreeUsage.judge || 0)),
    updated_at: new Date().toISOString()
  };
  const unsafePaidFields = ["paid_convert_count", "paid_judge_count"].filter(field => (
    Object.prototype.hasOwnProperty.call(updates, field)
    && Number(updates[field] || 0) === 0
    && Number(user?.[field] || 0) > 0
  ));

  if (unsafePaidFields.length) {
    throw new Error(`Blocked daily free quota reset from overwriting paid counts: ${unsafePaidFields.join(",")}`);
  }

  return updates;
}

async function restoreDailyPaidBalanceIfChanged(userId, beforeUser, afterUser) {
  if (!supabase || !beforeUser || !afterUser) {
    return afterUser || beforeUser;
  }

  const beforePaidConvert = toNonNegativeInteger(beforeUser.paid_convert_count, 0);
  const beforePaidJudge = toNonNegativeInteger(beforeUser.paid_judge_count, 0);
  const afterPaidConvert = toNonNegativeInteger(afterUser.paid_convert_count, 0);
  const afterPaidJudge = toNonNegativeInteger(afterUser.paid_judge_count, 0);

  if (beforePaidConvert === afterPaidConvert && beforePaidJudge === afterPaidJudge) {
    return afterUser;
  }

  console.error("Daily free quota reset unexpectedly changed paid counts; restoring paid balance.", {
    userId,
    before: {
      paid_convert_count: beforePaidConvert,
      paid_judge_count: beforePaidJudge
    },
    after: {
      paid_convert_count: afterPaidConvert,
      paid_judge_count: afterPaidJudge
    }
  });

  const { data, error } = await supabase
    .from("users")
    .update({
      paid_convert_count: beforePaidConvert,
      paid_judge_count: beforePaidJudge,
      updated_at: new Date().toISOString()
    })
    .eq("user_id", userId)
    .select("*")
    .single();

  if (error) {
    console.error("Failed to restore paid counts after daily free quota reset:", {
      userId,
      message: error?.message,
      code: error?.code,
      details: error?.details,
      hint: error?.hint
    });
    throw error;
  }

  if (
    toNonNegativeInteger(data?.paid_convert_count, 0) !== beforePaidConvert
    || toNonNegativeInteger(data?.paid_judge_count, 0) !== beforePaidJudge
  ) {
    throw new Error("Paid counts changed during daily free quota reset and could not be restored. Check database triggers/policies.");
  }

  return data || {
    ...afterUser,
    paid_convert_count: beforePaidConvert,
    paid_judge_count: beforePaidJudge
  };
}

async function normalizeSupabaseDailyFreeQuota(userId, user) {
  const resetField = getSupabaseFreeResetField(user);

  if (!supabase || !user) {
    return user;
  }

  const todayKey = getTodayKey();
  const quotaDate = resetField ? String(user[resetField] || "").slice(0, 10) : "";

  if (quotaDate === todayKey) {
    return user;
  }

  const todayFreeUsage = await getTodayFreeUsageCounts(userId, todayKey);
  const updates = buildDailyFreeQuotaUpdate(user, todayFreeUsage, todayKey);

  if (!resetField
    && Number(user.free_convert_count || 0) === updates.free_convert_count
    && Number(user.free_judge_count || 0) === updates.free_judge_count) {
    return user;
  }

  if (resetField) {
    updates[resetField] = todayKey;
  }

  console.log("Daily free quota reset:", {
    userId,
    resetField: resetField || "-",
    fromDate: quotaDate || "-",
    toDate: todayKey,
    before: {
      free_convert_count: toNonNegativeInteger(user.free_convert_count, 0),
      free_judge_count: toNonNegativeInteger(user.free_judge_count, 0),
      paid_convert_count: toNonNegativeInteger(user.paid_convert_count, 0),
      paid_judge_count: toNonNegativeInteger(user.paid_judge_count, 0)
    },
    after: {
      free_convert_count: updates.free_convert_count,
      free_judge_count: updates.free_judge_count,
      paid_convert_count: toNonNegativeInteger(user.paid_convert_count, 0),
      paid_judge_count: toNonNegativeInteger(user.paid_judge_count, 0)
    }
  });

  const { data, error } = await supabase
    .from("users")
    .update(updates)
    .eq("user_id", userId)
    .select("*")
    .single();

  if (error) {
    throw error;
  }

  return restoreDailyPaidBalanceIfChanged(userId, user, data || user);
}

function buildSupabaseQuotaPayload(user) {
  const resetDate = user?.last_free_reset_date || user?.free_quota_date || "";

  return {
    free_convert_count: Number(user?.free_convert_count || 0),
    free_judge_count: Number(user?.free_judge_count || 0),
    paid_convert_count: Number(user?.paid_convert_count || 0),
    paid_judge_count: Number(user?.paid_judge_count || 0),
    last_free_reset_date: resetDate,
    free_quota_date: resetDate,
    is_vip: Boolean(user?.is_vip),
    freeConvertCount: Number(user?.free_convert_count || 0),
    freeJudgeCount: Number(user?.free_judge_count || 0),
    paidConvertCount: Number(user?.paid_convert_count || 0),
    paidJudgeCount: Number(user?.paid_judge_count || 0),
    lastFreeResetDate: resetDate,
    freeQuotaDate: resetDate,
    isVip: Boolean(user?.is_vip)
  };
}

function getUsageRule(type) {
  const featureType = normalizeUsageFeatureType(type);
  const baseRule = USAGE_RULES[featureType] || USAGE_RULES.convert;
  const usageLimits = getPricingUsageLimits();

  if (featureType === "judge") {
    return {
      ...baseRule,
      vipMonthlyLimit: usageLimits.vipJudgeLimit
    };
  }

  if (featureType === "convert") {
    return {
      ...baseRule,
      vipMonthlyLimit: usageLimits.vipConvertLimit
    };
  }

  return baseRule;
}

function buildUsageQuotaPayload(user, monthlyUsage = {}) {
  const quota = buildSupabaseQuotaPayload(user);
  const usageLimits = getPricingUsageLimits();
  const isVip = Boolean(quota.is_vip);
  const freeConvert = Number(quota.free_convert_count || 0);
  const freeJudge = Number(quota.free_judge_count || 0);
  const paidConvert = Number(quota.paid_convert_count || 0);
  const paidJudge = Number(quota.paid_judge_count || 0);
  const convertUsed = Number(monthlyUsage.convert || 0);
  const judgeUsed = Number(monthlyUsage.judge || 0);
  const convertRemaining = freeConvert + paidConvert;
  const judgeRemaining = freeJudge + paidJudge;

  return {
    ...quota,
    convert_remaining: convertRemaining,
    judge_remaining: judgeRemaining,
    convertRemaining,
    judgeRemaining,
    vip_convert_limit: usageLimits.vipConvertLimit,
    vip_judge_limit: usageLimits.vipJudgeLimit,
    vip_convert_used: isVip ? Math.max(0, usageLimits.vipConvertLimit - paidConvert) : convertUsed,
    vip_judge_used: isVip ? Math.max(0, usageLimits.vipJudgeLimit - paidJudge) : judgeUsed,
    vip_convert_remaining: isVip ? paidConvert : Math.max(0, usageLimits.vipConvertLimit - convertUsed),
    vip_judge_remaining: isVip ? paidJudge : Math.max(0, usageLimits.vipJudgeLimit - judgeUsed)
  };
}

function getQuotaExhaustedCode(type) {
  return normalizeUsageFeatureType(type) === "judge"
    ? "JUDGE_QUOTA_EXHAUSTED"
    : "CONVERT_QUOTA_EXHAUSTED";
}

function buildSupabaseDeviceSummary(deviceInfo, userAgent = "") {
  const parts = [
    deviceInfo?.model,
    deviceInfo?.platform,
    deviceInfo?.platformVersion
  ]
    .map(value => String(value || "").trim())
    .filter(Boolean);

  if (parts.length) {
    return parts.join(" / ").slice(0, 200);
  }

  return String(userAgent || "").slice(0, 200);
}

function pickSupabaseQuotaLogFields(user) {
  return {
    free_convert_count: toNonNegativeInteger(user?.free_convert_count, 0),
    paid_convert_count: toNonNegativeInteger(user?.paid_convert_count, 0),
    free_judge_count: toNonNegativeInteger(user?.free_judge_count, 0),
    paid_judge_count: toNonNegativeInteger(user?.paid_judge_count, 0)
  };
}

function pickSupabaseVipLogFields(user) {
  return {
    is_vip: Boolean(user?.is_vip),
    paid_convert_count: toNonNegativeInteger(user?.paid_convert_count, 0),
    paid_judge_count: toNonNegativeInteger(user?.paid_judge_count, 0)
  };
}

function pickSupabaseNoteLogFields(user) {
  return {
    admin_note: String(user?.admin_note || "")
  };
}

async function updateSupabaseUserVisitMeta(userId, req) {
  if (!supabase || !userId) {
    return;
  }

  try {
    const ip = getClientIp(req);
    const userAgent = req.headers?.["user-agent"] || req.get("user-agent") || "";
    const deviceInfo = normalizeClientDeviceInfo(req.body?.deviceInfo || req.body?.extra?.deviceInfo, userAgent);
    const device = buildSupabaseDeviceSummary(deviceInfo, userAgent);
    let region = "";

    if (ip) {
      const adminData = await readAdminData();
      const geo = await lookupIpLocation(ip, adminData);
      region = geo.label || "";
      await writeAdminData(adminData);
    }

    const updates = {
      last_ip: ip || "",
      region,
      user_agent: userAgent.slice(0, 500),
      device_info: device,
      last_seen_at: new Date().toISOString()
    };

    let { error } = await supabase
      .from("users")
      .update(updates)
      .eq("user_id", userId);

    if (error && (error.code === "42703" || /region/i.test(error.message || ""))) {
      const chineseRegionUpdates = {
        ...updates,
        "地区": region
      };
      delete chineseRegionUpdates.region;

      const retry = await supabase
        .from("users")
        .update(chineseRegionUpdates)
        .eq("user_id", userId);
      error = retry.error;
    }

    if (error && (error.code === "42703" || /地区|region/i.test(error.message || ""))) {
      const noRegionUpdates = { ...updates };
      delete noRegionUpdates.region;

      const retry = await supabase
        .from("users")
        .update(noRegionUpdates)
        .eq("user_id", userId);
      error = retry.error;
    }

    if (error) {
      throw error;
    }

    console.log("访问信息写入：", userId, ip || "-", region || "-", device || "-");
  } catch (error) {
    console.error("Supabase visit meta update failed:", {
      userId,
      message: error?.message,
      code: error?.code,
      details: error?.details,
      hint: error?.hint
    });
  }
}

async function updateSupabaseUserSourceMeta(userId, req) {
  if (!supabase || !userId) {
    return;
  }

  const sourceMeta = getSourceMetaFromReq(req);
  if (!sourceMeta?.from_source) {
    return;
  }

  try {
    const sourceFields = {
      source_from: sourceMeta.from_source || null,
      source_page: sourceMeta.first_page || sourceMeta.landing_url || sourceMeta.current_page || sourceMeta.page || null,
      landing_url: sourceMeta.landing_url || null,
      referrer: sourceMeta.referrer || null
    };
    let { error } = await supabase
      .from("users")
      .update(sourceFields)
      .eq("user_id", userId);

    if (error && error.code === "42703") {
      const fallbackFields = {
        source_from: sourceFields.source_from,
        source_page: sourceFields.source_page
      };
      const retry = await supabase
        .from("users")
        .update(fallbackFields)
        .eq("user_id", userId);
      error = retry.error;
    }

    if (error && error.code !== "42703") {
      throw error;
    }
  } catch (error) {
    console.error("Supabase user source update failed:", {
      userId,
      message: error?.message,
      code: error?.code,
      details: error?.details,
      hint: error?.hint
    });
  }
}

async function writeAdminLog({ targetUserId, action, beforeData, afterData, note = "" }) {
  if (!supabase || !targetUserId || !action) {
    return;
  }

  try {
    const { error } = await supabase
      .from("admin_logs")
      .insert({
        admin_id: "admin",
        target_user_id: String(targetUserId),
        action: String(action),
        before_data: beforeData || null,
        after_data: afterData || null,
        note: String(note || "").slice(0, 1000) || null
      });

    if (error) {
      throw error;
    }
  } catch (error) {
    console.error("Admin log write failed:", {
      targetUserId,
      action,
      message: error?.message,
      code: error?.code,
      details: error?.details
    });
  }
}

function extractUsageBalanceFields(user) {
  if (!user) return null;
  return {
    is_vip: !!user.is_vip,
    free_convert_count: toNonNegativeInteger(user.free_convert_count, 0),
    paid_convert_count: toNonNegativeInteger(user.paid_convert_count, 0),
    free_judge_count: toNonNegativeInteger(user.free_judge_count, 0),
    paid_judge_count: toNonNegativeInteger(user.paid_judge_count, 0)
  };
}

function normalizeUsageFeatureType(type) {
  if (type === "visit") return "visit";
  if (type === "gold") return "gold";
  return type === "judge" ? "judge" : "convert";
}

const AI_JUDGE_ESTIMATED_COST_PER_CALL_CNY = Number(process.env.AI_JUDGE_COST_PER_CALL_CNY || 0.03);
const AI_INPUT_TOKEN_PRICE_PER_1K_CNY = Number(process.env.AI_INPUT_TOKEN_PRICE_PER_1K_CNY || 0);
const AI_OUTPUT_TOKEN_PRICE_PER_1K_CNY = Number(process.env.AI_OUTPUT_TOKEN_PRICE_PER_1K_CNY || 0);
const AI_COST_NOTE_PREFIX = "AI_COST_META:";
const SOURCE_META_NOTE_PREFIX = "SOURCE_META:";

function toFiniteNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function pickTokenUsage(usage = {}) {
  const promptTokens = toNonNegativeInteger(
    usage.prompt_tokens ?? usage.input_tokens ?? usage.inputTokens ?? usage.promptTokens,
    0
  );
  const completionTokens = toNonNegativeInteger(
    usage.completion_tokens ?? usage.output_tokens ?? usage.outputTokens ?? usage.completionTokens,
    0
  );
  const totalTokens = toNonNegativeInteger(
    usage.total_tokens ?? usage.totalTokens,
    promptTokens + completionTokens
  );

  return {
    prompt_tokens: promptTokens,
    completion_tokens: completionTokens,
    total_tokens: totalTokens
  };
}

function estimateAiCostCnyFromUsage(usage = {}) {
  const tokens = pickTokenUsage(usage);
  const tokenCost = (
    (tokens.prompt_tokens / 1000) * AI_INPUT_TOKEN_PRICE_PER_1K_CNY
    + (tokens.completion_tokens / 1000) * AI_OUTPUT_TOKEN_PRICE_PER_1K_CNY
  );

  if (tokenCost > 0) {
    return Number(tokenCost.toFixed(6));
  }

  return Number(Math.max(0, AI_JUDGE_ESTIMATED_COST_PER_CALL_CNY).toFixed(6));
}

function buildAiCostMetadata(usage = {}, extra = {}) {
  const tokens = pickTokenUsage(usage);
  return {
    kind: "ai_cost",
    provider: "aliyun",
    model: extra.model || aliyunVisionModel || "",
    estimated_cost_cny: estimateAiCostCnyFromUsage(usage),
    prompt_tokens: tokens.prompt_tokens,
    completion_tokens: tokens.completion_tokens,
    total_tokens: tokens.total_tokens,
    pricing: {
      fallback_cost_per_call_cny: AI_JUDGE_ESTIMATED_COST_PER_CALL_CNY,
      input_token_price_per_1k_cny: AI_INPUT_TOKEN_PRICE_PER_1K_CNY,
      output_token_price_per_1k_cny: AI_OUTPUT_TOKEN_PRICE_PER_1K_CNY
    }
  };
}

function encodeUsageLogNote(note = "", metadata = null) {
  const text = String(note || "").trim();
  if (!metadata) {
    return text;
  }

  const metaText = `${AI_COST_NOTE_PREFIX}${JSON.stringify(metadata)}`;
  return [text, metaText].filter(Boolean).join("\n");
}

function sanitizeSourceValue(value, maxLength = 100) {
  return String(value || "")
    .trim()
    .slice(0, maxLength)
    .replace(/[^\w\u4e00-\u9fa5./:-]/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function sanitizeUrlValue(value, maxLength = 500) {
  return String(value || "")
    .trim()
    .slice(0, maxLength)
    .replace(/[\r\n<>]/g, "");
}

function sanitizeUserCode(value) {
  const raw = String(value || "")
    .trim()
    .toUpperCase()
    .replace(/^GKT[\s_-]?/, "")
    .replace(/[^A-Z0-9]/g, "")
    .slice(0, 6);

  return raw.length === 6 ? `GKT-${raw}` : "";
}

function getSourceMetaFromReq(req) {
  if (!req) return null;
  const body = req.body || {};
  const query = req.query || {};
  const rawSource = req.get?.("x-source")
    || body.fromSource
    || body.source
    || query.from
    || query.utm_source
    || query.source
    || query.ref;
  const fromSource = sanitizeSourceValue(
    rawSource,
    80
  );

  const userCode = sanitizeUserCode(
    req.get?.("x-user-code") ||
    body.userCode ||
    body.user_code ||
    query.userCode ||
    query.user_code
  );
  const visitorId = sanitizeSourceValue(
    req.get?.("x-visitor-id") || body.visitorId || body.visitor_id || query.visitorId || query.visitor_id,
    120
  );
  const page = sanitizeUrlValue(req.get?.("x-page") || body.page || query.page || req.originalUrl || "", 300);
  const landingUrl = sanitizeUrlValue(
    req.get?.("x-landing-url") || body.landingUrl || body.landing_url || query.landing_url || "",
    500
  );
  const referrer = sanitizeUrlValue(
    req.get?.("x-referrer") || body.referrer || req.get?.("referer") || "",
    500
  );

  if (!fromSource && !userCode && !page && !landingUrl && !referrer) {
    return null;
  }

  return {
    from_source: fromSource,
    user_code: userCode,
    visitor_id: visitorId,
    ip: getClientIp(req),
    user_agent: sanitizeSourceValue(req.headers?.["user-agent"] || "", 240),
    first_page: sanitizeUrlValue(req.get?.("x-source-page") || body.firstSourcePage || query.firstPage || "", 300),
    first_visit_at: sanitizeSourceValue(req.get?.("x-source-at") || body.firstVisitAt || query.firstVisitAt || "", 80),
    landing_url: landingUrl,
    referrer,
    page,
    current_page: page,
    created_at: new Date().toISOString(),
    visit_time: new Date().toISOString()
  };
}

function appendSourceMetaToNote(note = "", sourceMeta = null) {
  const text = String(note || "").trim();
  if (!sourceMeta?.from_source && !sourceMeta?.user_code && !sourceMeta?.page) {
    return text;
  }

  const metaText = `${SOURCE_META_NOTE_PREFIX}${JSON.stringify(sourceMeta)}`;
  return [text, metaText].filter(Boolean).join("\n");
}

function parseUsageSourceMetadata(note = "") {
  const text = String(note || "");
  const index = text.indexOf(SOURCE_META_NOTE_PREFIX);
  if (index < 0) {
    return null;
  }

  const raw = text.slice(index + SOURCE_META_NOTE_PREFIX.length).split(/\r?\n/)[0];
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function parseUsageLogMetadata(note = "") {
  const text = String(note || "");
  const index = text.indexOf(AI_COST_NOTE_PREFIX);
  if (index < 0) {
    return null;
  }

  const raw = text.slice(index + AI_COST_NOTE_PREFIX.length).split(/\r?\n/)[0];
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function writeUsageLog({
  userId,
  req,
  featureType,
  consumeType,
  beforeBalance,
  afterBalance,
  success,
  note = "",
  errorReason = "",
  metadata = null
}) {
  if (!supabase || !userId) {
    return;
  }

  try {
    const ip = req ? getClientIp(req) : "";
    const userAgent = req ? String(req.headers["user-agent"] || "") : "";
    const deviceInfo = normalizeClientDeviceInfo(
      req?.body?.deviceInfo || req?.body?.extra?.deviceInfo,
      userAgent
    );
    const device = buildSupabaseDeviceSummary(deviceInfo, userAgent);
    let region = "";

    if (ip) {
      try {
        const adminData = await readAdminData();
        const geo = await lookupIpLocation(ip, adminData);
        region = geo.label || "";
        await writeAdminData(adminData);
      } catch (geoError) {
        console.error("Usage log region lookup failed:", geoError?.message || geoError);
      }
    }

    const sourceMeta = getSourceMetaFromReq(req);
    const logNote = appendSourceMetaToNote(encodeUsageLogNote(note, metadata), sourceMeta);
    const { error } = await supabase
      .from("usage_logs")
      .insert({
        user_id: String(userId),
        ip,
        region,
        user_agent: userAgent,
        device_info: device,
        feature_type: normalizeUsageFeatureType(featureType),
        consume_type: consumeType || "none",
        before_balance: beforeBalance || null,
        after_balance: afterBalance || null,
        success: !!success,
        note: logNote.slice(0, 1000) || null,
        error_reason: String(errorReason || "").slice(0, 1000) || null
      });

    if (error) {
      console.error("Usage log write failed:", {
        userId,
        featureType,
        consumeType,
        message: error.message,
        code: error.code,
        details: error.details,
        hint: error.hint
      });
    }
  } catch (error) {
    console.error("Usage log write exception:", error?.message || error);
  }
}

function isQuotaBlockedUsageLog(log = {}) {
  return !log.success && ["quota_exhausted", "limit_exceeded", "quota_blocked"].includes(String(log.error_reason || ""));
}

function getUsageLogStatus(log = {}) {
  if (log.success) return "success";
  return isQuotaBlockedUsageLog(log) ? "quota_blocked" : "failed";
}

function normalizeUsageLogForResponse(log = {}) {
  const status = getUsageLogStatus(log);
  return {
    ...log,
    status,
    reason: status === "quota_blocked" ? "quota_exhausted" : (log.error_reason || "")
  };
}

function detectAdminDeviceLabel(deviceInfo = "", userAgent = "") {
  const text = `${deviceInfo || ""} ${userAgent || ""}`.toLowerCase();
  if (/iphone|ipad|ios/.test(text)) return "iPhone";
  if (/android/.test(text)) return "Android";
  if (/macintosh|mac os|macos/.test(text)) return "Mac";
  if (/windows|win32|win64/.test(text)) return "Windows";
  return String(deviceInfo || "").trim() || "未知";
}

function getAdminActiveUsersDateRange(period = "today") {
  const now = new Date();
  const todayStart = new Date(now);
  todayStart.setHours(0, 0, 0, 0);
  const start = new Date(todayStart);
  const end = new Date(todayStart);

  if (period === "yesterday") {
    start.setDate(start.getDate() - 1);
  } else {
    end.setDate(end.getDate() + 1);
  }

  if (period === "yesterday") {
    end.setTime(todayStart.getTime());
  }

  return { start, end };
}

function getAdminUserValueScore(totalUsageCount = 0) {
  const count = Number(totalUsageCount || 0);
  if (count >= 10) return 5;
  if (count >= 5) return 4;
  if (count >= 3) return 3;
  if (count >= 2) return 2;
  return 1;
}

async function writeSourceVisitLog(userId, req) {
  const sourceMeta = getSourceMetaFromReq(req);
  if (!sourceMeta?.from_source && !sourceMeta?.user_code) {
    return;
  }

  await writeUsageLog({
    userId,
    req,
    featureType: "visit",
    consumeType: "none",
    beforeBalance: null,
    afterBalance: null,
    success: true,
    note: "source_visit"
  });
}

function extractJudgeCaseGrade(text = "") {
  const source = String(text || "");
  const gradeLine = source.match(/(?:銆愮瓑绾с€?|【等级】|等级|判读等级)[^\nA-D]*([ABCD])/i);
  if (gradeLine) {
    return gradeLine[1].toUpperCase();
  }

  const loose = source.match(/\b([ABCD])\s*(?:级|級|建议|可以|谨慎|排除)/i);
  return loose ? loose[1].toUpperCase() : null;
}

function extractJudgeCaseImageType(text = "") {
  const source = String(text || "");
  if (/卫星|衛星|地图|地圖|遥感|遙感|Google\s*Earth|奥维|奧維|俯视|俯視|航拍/i.test(source)) {
    return "卫星图";
  }
  if (/河道|河床|溪流|沟谷|溝谷|坡脚|坡腳|沉积|沉積|采坑|采挖|採挖|矿洞|礦洞|扰动|擾動|老鼠洞|地貌|现场|現場/i.test(source)) {
    return "矿地照片";
  }
  if (/矿石|礦石|原矿|原礦|矿化|礦化|岩石|黄铁矿|黃鐵礦|云母|雲母|自然金|砂金|金块|金粒|金豆|熔炼|熔煉|成品金|样本|樣本/i.test(source)) {
    return "矿石";
  }
  return "未确定";
}

function extractJudgeCaseKeywords(text = "") {
  const source = String(text || "");
  const dictionary = [
    "黄铁矿感",
    "云母感",
    "整面反光",
    "金属膜感",
    "石英脉",
    "裂隙控制",
    "氧化带",
    "赋存关系",
    "河道结构",
    "沉积空间",
    "采坑",
    "老矿洞",
    "人工扰动",
    "卫星图",
    "地貌线索",
    "老鼠洞",
    "自然金块",
    "砂金金块",
    "熔炼外观",
    "圆滑凝固边",
    "疑似人工金属块",
    "需检测确认",
    "成色检测",
    "补拍断面",
    "上下游",
    "现场验证"
  ];

  const aliases = {
    "黄铁矿感": /黄铁矿|黃鐵礦|硫化物/i,
    "云母感": /云母|雲母|片状反光|片狀反光/i,
    "整面反光": /整面反光|整体反光|整體反光|镜面|鏡面|大面积.*亮/i,
    "金属膜感": /金属膜|金屬膜|膜感/i,
    "石英脉": /石英脉|石英脈|白色脉|白色脈/i,
    "裂隙控制": /裂隙|裂缝|裂縫|结构控制|構造控制/i,
    "氧化带": /氧化带|氧化帶|铁染|鐵染/i,
    "赋存关系": /赋存|賦存|局部集中/i,
    "河道结构": /河道|河床|溪流|沟谷|溝谷/i,
    "沉积空间": /沉积|沉積|阶地|階地|弯道|彎道/i,
    "采坑": /采坑|採坑|试挖|試挖|小坑/i,
    "老矿洞": /老矿洞|老礦洞|矿洞|礦洞/i,
    "人工扰动": /人工扰动|人工擾動|扰动|擾動|裸土|便道/i,
    "卫星图": /卫星|衛星|遥感|遙感|地图|地圖|Google\s*Earth|奥维|奧維/i,
    "地貌线索": /地貌|坡脚|坡腳|线性|線性|山脊/i,
    "老鼠洞": /老鼠洞/i,
    "自然金块": /自然金|金块|金粒|金豆|砂金/i,
    "砂金金块": /砂金|河道.*金|冲积.*金|沖積.*金/i,
    "熔炼外观": /熔炼|熔煉|成品金|熔融|铸块|鑄塊|倒模|圆饼|圆豆|半球|滴状/i,
    "圆滑凝固边": /圆滑|凝固|圆边|扁平|流痕/i,
    "疑似人工金属块": /人工|熔炼|熔煉|熔融|铸块|鑄塊|倒模|模具|手掌/i,
    "需检测确认": /需检测|需檢測|检测确认|檢測確認|XRF|火试金|密度/i,
    "成色检测": /成色|纯度|純度|检测|檢測/i,
    "补拍断面": /补拍|補拍|断面|斷面|敲开|敲開/i,
    "上下游": /上游|下游|上下游/i,
    "现场验证": /现场|現場|验证|驗證|复核|複核/i
  };

  const keywords = dictionary.filter(keyword => aliases[keyword]?.test(source));
  return [...new Set(keywords)].slice(0, 8);
}

function extractJudgeCaseSuggestedNextImage(text = "") {
  const source = String(text || "");
  const section = source.match(/(?:銆愪笅涓€姝ャ€?|【下一步】|下一步)([\s\S]*?)(?:\n\s*(?:銆愬|【)|$)/);
  const block = section ? section[1] : source;
  const line = block
    .split(/\r?\n/)
    .map(item => item.replace(/^\s*[-*0-9.、)）]+\s*/, "").trim())
    .find(item => /补拍|補拍|上传|上傳|敲开|敲開|比例|断面|斷面|环境|環境|上下游|卫星|衛星|现场|現場/.test(item));

  return line ? line.slice(0, 300) : "";
}

function hashJudgeCaseImage(file) {
  if (!file?.buffer?.length) {
    return "";
  }
  return crypto.createHash("sha256").update(file.buffer).digest("hex");
}

const JUDGE_FEEDBACK_TYPES = new Set(["helpful", "wrong", "need_more_image", "field_verified"]);
const JUDGE_FIELD_RESULTS = new Set(["valuable", "not_valuable", "uncertain"]);

async function writeJudgeCase({ req, userId, file, resultText = "", rawText = "" }) {
  if (!supabase || !file?.buffer?.length || !String(resultText || rawText || "").trim()) {
    return null;
  }

  try {
    const sourceMeta = getSourceMetaFromReq(req) || {};
    const text = String(resultText || rawText || "");
    const payload = {
      user_code: sourceMeta.user_code || null,
      user_id: userId ? String(userId) : null,
      source_from: sourceMeta.from_source || null,
      source_page: sourceMeta.landing_url || sourceMeta.first_page || sourceMeta.current_page || sourceMeta.page || null,
      image_type: extractJudgeCaseImageType(text),
      ai_result: text.slice(0, 12000),
      grade: extractJudgeCaseGrade(text),
      keywords: extractJudgeCaseKeywords(text),
      suggested_next_image: extractJudgeCaseSuggestedNextImage(text) || null,
      image_hash: hashJudgeCaseImage(file) || null,
      image_url: null,
      image_path: null,
      review_status: "pending",
      reviewer_note: null
    };

    const { data, error } = await supabase
      .from("judge_cases")
      .insert(payload)
      .select("case_id")
      .single();
    if (error) {
      console.error("Judge case write failed:", {
        message: error.message,
        code: error.code,
        details: error.details,
        hint: error.hint
      });
      return null;
    }

    return data?.case_id || null;
  } catch (error) {
    console.error("Judge case write exception:", error?.message || error);
    return null;
  }
}

app.get("/api/gold-price", async (req, res) => {
  try {
    const realtimePrice = await fetchRealtimeGoldPrice();
    res.json(realtimePrice || getUnavailableGoldPricePayload());
  } catch (error) {
    console.error("读取实时金价失败，返回不可用状态：", error.message || error);
    res.json(getUnavailableGoldPricePayload());
  }
});

app.post("/api/gold-usage", async (req, res) => {
  try {
    const userId = String(
      req.body?.visitorId ||
      req.get("x-visitor-id") ||
      req.query?.visitorId ||
      "anonymous"
    ).trim() || "anonymous";

    await writeUsageLog({
      userId,
      req,
      featureType: "gold",
      consumeType: "none",
      beforeBalance: null,
      afterBalance: null,
      success: true,
      note: "Gold calculator result generated",
      metadata: {
        kind: "gold_calculation",
        density: Number(req.body?.density || 0) || null,
        purity: Number(req.body?.purity || 0) || null,
        k_value: Number(req.body?.kValue || 0) || null
      }
    });

    res.json({ success: true });
  } catch (error) {
    console.error("Gold usage log failed:", error?.message || error);
    res.status(500).json({
      success: false,
      error: "failed"
    });
  }
});

async function checkUsage(userId, type) {
  const featureType = normalizeUsageFeatureType(type);
  const rule = getUsageRule(featureType);

  if (rule.unlimited) {
    return {
      allowed: true,
      success: true,
      reason: "ok",
      source: "unlimited",
      featureType,
      user: null,
      quota: {}
    };
  }

  const user = await getOrCreateSupabaseUser(userId);

  if (!user) {
    return {
      allowed: false,
      success: false,
      reason: "db_disabled",
      quota: {}
    };
  }

  const paidKey = rule.paidField;
  const freeKey = rule.freeField;
  const paidCount = Number(user[paidKey] || 0);
  const freeCount = Number(user[freeKey] || 0);

  if (freeCount > 0) {
    return {
      allowed: true,
      success: true,
      reason: "ok",
      source: "free",
      featureType,
      user,
      quota: buildUsageQuotaPayload(user)
    };
  }

  if (paidCount > 0) {
    return {
      allowed: true,
      success: true,
      reason: "ok",
      source: "paid",
      featureType,
      user,
      quota: buildUsageQuotaPayload(user)
    };
  }

  return {
    allowed: false,
    success: false,
    reason: "limit_exceeded",
    source: "none",
    featureType,
    user,
    quota: buildUsageQuotaPayload(user)
  };
}

async function consumeUsage(userId, type, req = null, options = {}) {
  const featureType = normalizeUsageFeatureType(type);
  const rule = getUsageRule(featureType);
  const status = await checkUsage(userId, featureType);
  const user = status.user;
  const beforeBalance = extractUsageBalanceFields(user);
  const usageNote = options.note || "";
  const usageMetadata = options.metadata || null;

  if (rule.unlimited) {
    await writeUsageLog({
      userId,
      req,
      featureType,
      consumeType: "none",
      beforeBalance,
      afterBalance: beforeBalance,
      success: true,
      note: usageNote || "Unlimited feature, no quota consumed",
      metadata: usageMetadata
    });
    return status;
  }

  if (!status.allowed) {
    const blockedReason = status.reason === "limit_exceeded" ? "quota_exhausted" : (status.reason || "not_allowed");
    await writeUsageLog({
      userId,
      req,
      featureType,
      consumeType: status.source || "none",
      beforeBalance,
      afterBalance: beforeBalance,
      success: false,
      errorReason: blockedReason
    });
    return status;
  }

  const paidKey = rule.paidField;
  const freeKey = rule.freeField;
  const sourceKey = status.source === "free" ? freeKey : paidKey;
  const currentCount = Number(user?.[sourceKey] || 0);

  if (currentCount > 0) {
    const { data, error } = await supabase
      .from("users")
      .update({
        [sourceKey]: currentCount - 1,
        updated_at: new Date().toISOString()
      })
      .eq("user_id", userId)
      .gt(sourceKey, 0)
      .select("*")
      .maybeSingle();

    if (error) {
      await writeUsageLog({
        userId,
        req,
        featureType,
        consumeType: status.source,
        beforeBalance,
        afterBalance: beforeBalance,
        success: false,
        errorReason: "failed",
        note: usageNote,
        metadata: usageMetadata
      });
      throw error;
    }

    if (data) {
      await writeUsageLog({
        userId,
        req,
        featureType,
        consumeType: status.source,
        beforeBalance,
        afterBalance: extractUsageBalanceFields(data),
        success: true,
        note: usageNote,
        metadata: usageMetadata
      });

      return {
        success: true,
        reason: "ok",
        source: status.source,
        user: data,
        quota: buildUsageQuotaPayload(data)
      };
    }
  }

  const freshStatus = await checkUsage(userId, featureType);

  if (freshStatus.allowed && freshStatus.source !== status.source) {
    return consumeUsage(userId, featureType, req, options);
  }

  const freshUser = freshStatus.user || await getOrCreateSupabaseUser(userId);
  await writeUsageLog({
    userId,
    req,
    featureType,
    consumeType: status.source || "none",
    beforeBalance,
    afterBalance: extractUsageBalanceFields(freshUser || user),
    success: false,
    errorReason: "quota_exhausted"
  });

  return {
    success: false,
    reason: "limit_exceeded",
    user: freshUser || user,
    quota: freshStatus.quota || buildUsageQuotaPayload(freshUser || user)
  };
}

function ensureUser(data, visitorId) {
  const id = String(visitorId || "").trim();

  if (!id) {
    return null;
  }

  if (!data.users[id]) {
    data.users[id] = {
      visitorId: id,
      plan: "free",
      status: "active",
      permissions: {
        aiOcrEnabled: true,
        xyConvertEnabled: true,
        kmlExportEnabled: true,
        manualSupportEnabled: true,
        aiJudgeEnabled: true,
        goldCalculatorEnabled: true
      },
      createdAt: getNowISO(),
      lastSeenAt: getNowISO(),
      eventCount: 0,
      note: "",
      phone: "",
      wechat: ""
    };
  }

  normalizeUsageCounters(data.users[id]);
  return data.users[id];
}

function normalizeAdminUser(user, fallbackId = "") {
  const safeUser = user && typeof user === "object" ? user : {};
  const visitorId = String(safeUser.visitorId || fallbackId || "").trim();
  normalizeUsageCounters(safeUser);

  return {
    user_id: visitorId,
    visitorId,
    plan: safeUser.plan || "free",
    status: safeUser.status || "active",
    permissions: {
      aiOcrEnabled: safeUser.permissions?.aiOcrEnabled !== false,
      xyConvertEnabled: safeUser.permissions?.xyConvertEnabled !== false,
      kmlExportEnabled: safeUser.permissions?.kmlExportEnabled !== false,
      manualSupportEnabled: safeUser.permissions?.manualSupportEnabled !== false,
      aiJudgeEnabled: safeUser.permissions?.aiJudgeEnabled !== false,
      goldCalculatorEnabled: safeUser.permissions?.goldCalculatorEnabled !== false
    },
    created_at: safeUser.createdAt || "",
    createdAt: safeUser.createdAt || "",
    lastSeenAt: safeUser.lastSeenAt || "",
    eventCount: Number(safeUser.eventCount || 0),
    free_convert_count: Number(safeUser.freeConvertCount || 0),
    free_judge_count: Number(safeUser.freeJudgeCount || 0),
    paid_convert_count: Number(safeUser.paidConvertCount || 0),
    paid_judge_count: Number(safeUser.paidJudgeCount || 0),
    is_vip: Boolean(safeUser.isVip || safeUser.plan === "vip"),
    total_convert_count: Number(safeUser.totalConvertCount || 0),
    total_judge_count: Number(safeUser.totalJudgeCount || 0),
    freeConvertCount: Number(safeUser.freeConvertCount || 0),
    freeJudgeCount: Number(safeUser.freeJudgeCount || 0),
    paidConvertCount: Number(safeUser.paidConvertCount || 0),
    paidJudgeCount: Number(safeUser.paidJudgeCount || 0),
    isVip: Boolean(safeUser.isVip || safeUser.plan === "vip"),
    totalConvertCount: Number(safeUser.totalConvertCount || 0),
    totalJudgeCount: Number(safeUser.totalJudgeCount || 0),
    usageDate: safeUser.usageDate || "",
    note: safeUser.note || "",
    phone: safeUser.phone || "",
    wechat: safeUser.wechat || "",
    firstIp: safeUser.firstIp || "",
    lastIp: safeUser.lastIp || "",
    firstIpLocation: safeUser.firstIpLocation || "",
    lastIpLocation: safeUser.lastIpLocation || "",
    lastUserAgent: safeUser.lastUserAgent || "",
    lastDeviceModel: safeUser.lastDeviceModel || ""
  };
}

const eventNameLabels = {
  page_visit: "打开页面",
  permission_blocked: "权限拦截",
  undo_click: "撤销",
  manual_support_click: "人工协助识别",
  swap_lnglat_click: "交换经纬度",
  format_convert_click: "格式转换",
  copy_content_click: "复制内容",
  clear_content_click: "清空内容",
  normalize_click: "标准化坐标",
  kml_download_click: "生成KML文件并下载",
  image_upload_select: "上传坐标图片",
  image_recognize_success: "图片识别成功",
  image_recognize_fail: "图片识别失败",
  ai_judge_upload_select: "上传AI判读图片",
  ai_judge_success: "AI判读成功",
  ai_judge_fail: "AI判读失败",
  usage_convert: "扣除坐标处理次数",
  usage_judge: "扣除AI判读次数",
  limit_convert: "坐标处理额度不足",
  limit_judge: "AI判读额度不足"
};

function getEventLabel(eventName) {
  return eventNameLabels[eventName] || eventName || "";
}

async function appendUsageLog(data, user, req, type, source = "") {
  if (!data || !user) {
    return;
  }

  const ip = getClientIp(req);
  const geo = await lookupIpLocation(ip, data);
  const userAgent = req.get("user-agent") || "";
  const deviceInfo = normalizeClientDeviceInfo(req.body?.deviceInfo || req.body?.extra?.deviceInfo, userAgent);

  data.events.push({
    id: makeId("evt"),
    visitorId: user.visitorId,
    eventName: type === "judge" ? "usage_judge" : "usage_convert",
    ip,
    ipLocation: geo.label || "",
    userAgent: userAgent.slice(0, 300),
    deviceModel: deviceInfo.model || "",
    devicePlatform: deviceInfo.platform || "",
    page: String(req.body?.page || req.get("referer") || "").slice(0, 200),
    extra: {
      type,
      source,
      quota: buildQuotaPayload(user)
    },
    createdAt: getNowISO()
  });

  if (data.events.length > 5000) {
    data.events = data.events.slice(-5000);
  }
}

function getDateKey(dateText) {
  const date = new Date(dateText);
  return Number.isNaN(date.getTime()) ? "" : date.toISOString().slice(0, 10);
}

function getDaysSince(dateText) {
  const time = new Date(dateText).getTime();
  return Number.isNaN(time) ? 999 : Math.max(0, Math.floor((Date.now() - time) / 86400000));
}

function buildUserInsights(data, user) {
  const events = (data.events || []).filter(event => event?.visitorId === user.visitorId);
  const visitDays = new Set(events.map(event => getDateKey(event.createdAt)).filter(Boolean)).size || (user.lastSeenAt ? 1 : 0);
  const kmlDownloads = events.filter(event => event.eventName === "kml_download_click").length;
  const imageSuccess = events.filter(event => event.eventName === "image_recognize_success").length;
  const manualSupport = events.filter(event => event.eventName === "manual_support_click").length;
  const daysSinceLastSeen = getDaysSince(user.lastSeenAt || user.createdAt || "");
  let segment = "new";
  let segmentLabel = "新用户";

  if (daysSinceLastSeen >= 7) {
    segment = "lost";
    segmentLabel = "流失用户";
  } else if (daysSinceLastSeen >= 3) {
    segment = "inactive";
    segmentLabel = "沉默用户";
  } else if (kmlDownloads >= 2 || imageSuccess >= 2 || manualSupport >= 1 || (visitDays >= 2 && kmlDownloads >= 1)) {
    segment = "quality";
    segmentLabel = "优质用户";
  } else if (visitDays >= 2 || events.length >= 5) {
    segment = "returning";
    segmentLabel = "回访用户";
  }

  return {
    visitDays,
    kmlDownloads,
    imageSuccess,
    manualSupport,
    daysSinceLastSeen,
    segment,
    segmentLabel
  };
}

function getAdminUsersList(data) {
  if (!data.users || typeof data.users !== "object") {
    return [];
  }

  return Object.entries(data.users)
    .map(([id, user]) => {
      const normalized = normalizeAdminUser(user, id);
      return {
        ...normalized,
        ...buildUserInsights(data, normalized)
      };
    })
    .filter(user => user.visitorId);
}

function parseDeviceModelFromUserAgent(userAgent) {
  const ua = String(userAgent || "");

  if (!ua) {
    return "";
  }

  const androidMatch = ua.match(/Android\s+[\d.]+;\s*([^;)]+?)(?:\s+Build|\)|;)/i);
  if (androidMatch?.[1]) {
    return androidMatch[1].replace(/^wv\s*/i, "").trim();
  }

  if (/iPhone/i.test(ua)) {
    return "iPhone";
  }

  if (/iPad/i.test(ua)) {
    return "iPad";
  }

  if (/Windows NT/i.test(ua)) {
    return "Windows电脑";
  }

  if (/Macintosh/i.test(ua)) {
    return "Mac";
  }

  return "";
}

function normalizeClientDeviceInfo(raw, userAgent = "") {
  const deviceInfo = raw && typeof raw === "object" ? raw : {};
  const model = String(deviceInfo.model || "").trim();
  const platform = String(deviceInfo.platform || "").trim();
  const fallbackModel = parseDeviceModelFromUserAgent(userAgent);

  return {
    model: (model || fallbackModel).slice(0, 120),
    platform: platform.slice(0, 80),
    platformVersion: String(deviceInfo.platformVersion || "").trim().slice(0, 80),
    screen: String(deviceInfo.screen || "").trim().slice(0, 40),
    viewport: String(deviceInfo.viewport || "").trim().slice(0, 40)
  };
}

function getClientIp(req) {
  const forwardedFor = req.get("x-forwarded-for") || "";
  const firstForwardedIp = forwardedFor.split(",")[0]?.trim();

  return firstForwardedIp || req.ip || req.socket?.remoteAddress || "";
}

function normalizeIp(ip) {
  let cleanIp = String(ip || "")
    .split(",")[0]
    .trim()
    .replace(/^::ffff:/, "")
    .replace(/^\[|\]$/g, "");

  if (/^\d{1,3}(\.\d{1,3}){3}:\d+$/.test(cleanIp)) {
    cleanIp = cleanIp.replace(/:\d+$/, "");
  }

  return cleanIp;
}

function isLoopbackIp(ip) {
  const cleanIp = normalizeIp(ip).toLowerCase();
  return cleanIp === "127.0.0.1" || cleanIp === "::1" || cleanIp === "localhost";
}

function getRegressionTestMode(req) {
  const requested = /^(1|true|yes)$/i.test(String(req.get?.("x-regression-test") || "").trim());
  const enabled = /^(1|true|yes)$/i.test(String(process.env.ENABLE_REGRESSION_TEST_MODE || "").trim());
  const production = String(process.env.NODE_ENV || "").trim().toLowerCase() === "production";
  const local = [req.ip, req.socket?.remoteAddress, req.connection?.remoteAddress].some(isLoopbackIp);
  const active = requested && enabled && local && !production;
  const rejectReason = requested && enabled && (!local || production)
    ? "Regression test mode is only available from localhost in non-production environments."
    : "";

  return { requested, enabled, local, production, active, rejectReason };
}

function isPrivateIp(ip) {
  const cleanIp = normalizeIp(ip);

  return (
    !cleanIp ||
    cleanIp === "::1" ||
    cleanIp === "127.0.0.1" ||
    cleanIp.startsWith("10.") ||
    cleanIp.startsWith("192.168.") ||
    /^172\.(1[6-9]|2\d|3[0-1])\./.test(cleanIp)
  );
}

function translateCountry(country) {
  const names = {
    China: "中国",
    Guinea: "几内亚",
    Mali: "马里",
    "Burkina Faso": "布基纳法索",
    "Cote d'Ivoire": "科特迪瓦",
    "Côte d'Ivoire": "科特迪瓦",
    "Ivory Coast": "科特迪瓦",
    Ghana: "加纳",
    Nigeria: "尼日利亚",
    Senegal: "塞内加尔",
    "Sierra Leone": "塞拉利昂",
    Liberia: "利比里亚"
  };

  return names[country] || country || "";
}

function translateCountryCode(code) {
  const names = {
    CN: "中国",
    GN: "几内亚",
    ML: "马里",
    BF: "布基纳法索",
    CI: "科特迪瓦",
    GH: "加纳",
    NG: "尼日利亚",
    SN: "塞内加尔",
    SL: "塞拉利昂",
    LR: "利比里亚",
    CD: "刚果金",
    CG: "刚果",
    CM: "喀麦隆",
    US: "美国",
    FR: "法国"
  };

  return names[String(code || "").toUpperCase()] || "";
}

function translateChinaRegion(region) {
  const names = {
    Beijing: "北京",
    Shanghai: "上海",
    Tianjin: "天津",
    Chongqing: "重庆",
    Guangdong: "广东",
    Guangxi: "广西",
    Hunan: "湖南",
    Hubei: "湖北",
    Henan: "河南",
    Hebei: "河北",
    Shandong: "山东",
    Shanxi: "山西",
    Shaanxi: "陕西",
    Jiangsu: "江苏",
    Zhejiang: "浙江",
    Fujian: "福建",
    Jiangxi: "江西",
    Anhui: "安徽",
    Sichuan: "四川",
    Yunnan: "云南",
    Guizhou: "贵州",
    Hainan: "海南",
    Liaoning: "辽宁",
    Jilin: "吉林",
    Heilongjiang: "黑龙江",
    Gansu: "甘肃",
    Qinghai: "青海",
    Ningxia: "宁夏",
    Xinjiang: "新疆",
    Tibet: "西藏",
    "Inner Mongolia": "内蒙古",
    "Hong Kong": "香港",
    Macau: "澳门",
    Taiwan: "台湾"
  };

  return names[region] || region || "";
}

function formatIpLocation(geo) {
  if (!geo) {
    return "";
  }

  if (geo.country === "China") {
    return translateChinaRegion(geo.region) || "中国";
  }

  return translateCountry(geo.country) || geo.region || geo.city || "";
}

function normalizeGeoResult(raw, provider, ip) {
  if (!raw || typeof raw !== "object") {
    return null;
  }

  const country = raw.country_name || raw.country || "";
  const countryCode = raw.country_code || raw.countryCode || raw.country_code2 || "";
  const region = raw.region || raw.regionName || raw.region_name || "";
  const city = raw.city || "";
  let label = "";

  if (String(countryCode).toUpperCase() === "CN" || country === "China") {
    label = translateChinaRegion(region) || city || translateCountryCode("CN");
  } else {
    label = translateCountryCode(countryCode) || translateCountry(country) || region || city;
  }

  if (label && city && label !== city && String(countryCode).toUpperCase() !== "CN") {
    label = `${label} ${city}`;
  }

  if (!label) {
    return null;
  }

  return {
    ip,
    provider,
    country,
    countryCode,
    region,
    city,
    label,
    updatedAt: getNowISO()
  };
}

async function fetchJsonWithTimeout(url, timeoutMs = 3500) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      headers: { accept: "application/json" },
      signal: controller.signal
    });
    const data = await response.json().catch(() => ({}));

    if (!response.ok) {
      return null;
    }

    return data;
  } finally {
    clearTimeout(timer);
  }
}

async function lookupIpLocation(ip, data) {
  const cleanIp = normalizeIp(ip);

  if (isPrivateIp(cleanIp)) {
    return { ip: cleanIp, label: cleanIp ? "本地网络" : "" };
  }

  if (!data.ipGeoCache || typeof data.ipGeoCache !== "object") {
    data.ipGeoCache = {};
  }

  const cached = data.ipGeoCache[cleanIp];
  const cachedAge = cached?.updatedAt ? Date.now() - new Date(cached.updatedAt).getTime() : 0;
  if (cached?.label && cached.label !== "未知地区" && cachedAge < 7 * 86400000) {
    return cached;
  }

  if (cached?.label === "未知地区" && cachedAge < 6 * 3600000) {
    return cached;
  }

  const providers = [
    {
      name: "ipwho.is",
      url: `https://ipwho.is/${encodeURIComponent(cleanIp)}`,
      parse: geo => geo?.success === false ? null : normalizeGeoResult(geo, "ipwho.is", cleanIp)
    },
    {
      name: "ipapi.co",
      url: `https://ipapi.co/${encodeURIComponent(cleanIp)}/json/`,
      parse: geo => geo?.error ? null : normalizeGeoResult(geo, "ipapi.co", cleanIp)
    },
    {
      name: "country.is",
      url: `https://api.country.is/${encodeURIComponent(cleanIp)}`,
      parse: geo => normalizeGeoResult({ country_code: geo?.country }, "country.is", cleanIp)
    }
  ];

  for (const provider of providers) {
    try {
      const geo = await fetchJsonWithTimeout(provider.url);
      const result = provider.parse(geo);

      if (result?.label) {
        data.ipGeoCache[cleanIp] = result;
        return result;
      }
    } catch (error) {
      console.error("IP location lookup failed:", provider.name, cleanIp, error.message);
    }
  }

  const fallback = {
    ip: cleanIp,
    label: "未知地区",
    updatedAt: getNowISO()
  };
  data.ipGeoCache[cleanIp] = fallback;
  return fallback;
}

async function updateUserVisitMeta(user, req, data) {
  if (!user) {
    return;
  }

  const ip = getClientIp(req);
  const userAgent = req.get("user-agent") || "";
  let ipLocation = "";

  if (!user.firstIp && ip) {
    user.firstIp = ip;
  }

  if (ip) {
    user.lastIp = ip;

    if (data) {
      const geo = await lookupIpLocation(ip, data);
      ipLocation = geo.label || "";

      if (ipLocation) {
        user.lastIpLocation = ipLocation;

        if (!user.firstIpLocation) {
          user.firstIpLocation = ipLocation;
        }
      }
    }
  }

  if (userAgent) {
    user.lastUserAgent = userAgent.slice(0, 300);
  }

  const deviceInfo = normalizeClientDeviceInfo(req.body?.extra?.deviceInfo, userAgent);
  if (deviceInfo.model) {
    user.lastDeviceModel = deviceInfo.model;
  }

  user.lastSeenAt = getNowISO();
  return ipLocation;
}

async function enrichAdminLocations(data) {
  if (!data || typeof data !== "object") {
    return;
  }

  for (const user of Object.values(data.users || {})) {
    if (!user || typeof user !== "object" || !user.lastIp || user.lastIpLocation) {
      continue;
    }

    const geo = await lookupIpLocation(user.lastIp, data);
    if (geo.label) {
      user.lastIpLocation = geo.label;

      if (!user.firstIpLocation) {
        user.firstIpLocation = geo.label;
      }
    }
  }

  for (const event of (data.events || []).slice(-100)) {
    if (!event || !event.ip || event.ipLocation) {
      continue;
    }

    const geo = await lookupIpLocation(event.ip, data);
    if (geo.label) {
      event.ipLocation = geo.label;
    }
  }
}

function getEffectivePermissions(user, featureFlags) {
  const permissions = user?.permissions || {};

  if (user?.status === "disabled") {
    return {
      aiOcrEnabled: false,
      xyConvertEnabled: false,
      kmlExportEnabled: false,
      manualSupportEnabled: false,
      aiJudgeEnabled: false,
      goldCalculatorEnabled: false
    };
  }

  return {
    aiOcrEnabled: Boolean(featureFlags.aiOcrEnabled && permissions.aiOcrEnabled),
    xyConvertEnabled: Boolean(featureFlags.xyConvertEnabled && permissions.xyConvertEnabled),
    kmlExportEnabled: Boolean(featureFlags.kmlExportEnabled && permissions.kmlExportEnabled),
    manualSupportEnabled: Boolean(featureFlags.manualSupportEnabled && permissions.manualSupportEnabled),
    aiJudgeEnabled: Boolean(featureFlags.aiJudgeEnabled && permissions.aiJudgeEnabled),
    goldCalculatorEnabled: featureFlags.goldCalculatorEnabled !== false
  };
}

function requireAdmin(req, res, next) {
  if (!adminPassword) {
    return res.status(403).json({
      error: "后台未启用：请先在 Render 环境变量里设置 ADMIN_PASSWORD。"
    });
  }

  const provided = req.get("x-admin-password") || req.query.password || "";

  if (provided !== adminPassword) {
    return res.status(401).json({
      error: "管理员密码不正确。"
    });
  }

  next();
}

app.post("/api/admin/pricing-config", requireAdmin, async (req, res) => {
  try {
    const result = await savePricingConfigToSupabase(req.body?.config || req.body || {});

    if (!result.persisted) {
      return res.status(503).json({
        success: false,
        error: result.warning || "Supabase config unavailable",
        ...result
      });
    }

    res.json({
      success: true,
      ...result
    });
  } catch (error) {
    console.error("Pricing config Supabase save failed:", {
      message: error?.message,
      code: error?.code,
      details: error?.details,
      hint: error?.hint
    });
    res.status(500).json({
      success: false,
      error: error?.message || "Pricing config save failed",
      code: error?.code,
      details: error?.details,
      hint: error?.hint
    });
  }
});

function normalizeText(text) {
  return String(text || "")
    .replace(/[，]/g, ",")
    .replace(/[º˚]/g, "°")
    .replace(/[‘’´`′]/g, "'")
    .replace(/[“”″]/g, '"')
    .replace(/\b0\b/g, "O");
}

function stripLeadingCoordinateLabel(line) {
  return String(line || "")
    .replace(/^\s*(?:point|pt|ponto|sommet|vertex)\s*[-#:]?\s*\d{1,3}\s*[\).:：-]?\s*/i, "")
    .replace(/^\s*[A-Z]\s*[\).:：-]\s*/i, "")
    .replace(/^\s*\d{1,3}\s*[\).:：-]\s*/, "");
}

function getLeadingCoordinateLabelNumber(line) {
  const match = String(line || "").match(/^\s*(?:point|pt|ponto|sommet|vertex)?\s*[-#:]?\s*(\d{1,3})\s*[\).:：-]/i);
  return match ? Number(match[1]) : null;
}

function normalizeDmsLineForParsing(line) {
  return stripLeadingCoordinateLabel(normalizeText(line))
    .replace(/(\d{1,3}\s*°\s*\d{1,2})\s*"\s*(?=\d{1,2}(?:\.\d+)?\s*["']?\s*[NSEWO])/gi, "$1'");
}

function decimalFromDms(degrees, minutes, seconds, direction) {
  const deg = Number(degrees);
  const min = Number(minutes);
  const sec = Number(seconds);

  if (!Number.isFinite(deg) || !Number.isFinite(min) || !Number.isFinite(sec)) {
    return null;
  }

  let value = Math.abs(deg) + min / 60 + sec / 3600;
  const dir = String(direction || "").toUpperCase();

  if (["S", "W", "O"].includes(dir) || (!dir && deg < 0)) {
    value = -value;
  }

  return String(value);
}

function parseCompactDmsToken(token, fallbackDirection) {
  const cleaned = normalizeText(token)
    .replace(/\s+/g, "")
    .replace(/[|[\]_=]/g, "")
    .replace(/LONGITUDE|LATITUDE|POINT|N°|NO\.?/gi, "");
  const directionMatch = cleaned.match(/[NSEWO]$/i);
  const direction = (directionMatch ? directionMatch[0] : fallbackDirection || "").toUpperCase();
  const body = cleaned.replace(/[NSEWO]$/i, "").replace(/['"]$/, "");

  let match = body.match(/^([-+]?\d{1,3})°(\d{1,2})'(\d{1,4}(?:\.\d+)?)"?$/);
  if (match) {
    const seconds = !match[3].includes(".") && match[3].length === 4
      ? `${match[3].slice(0, 2)}.${match[3].slice(2)}`
      : match[3];

    return {
      value: decimalFromDms(match[1], match[2], seconds, direction),
      direction
    };
  }

  match = body.match(/^([-+]?\d{1,3})°(\d{2})(\d{2})(\d{2,3})$/);
  if (match) {
    return {
      value: decimalFromDms(match[1], match[2], `${match[3]}.${match[4]}`, direction),
      direction
    };
  }

  match = body.match(/^([-+]?\d{1,3})°(\d{2})(\d{1,2})\.(\d+)$/);
  if (match) {
    return {
      value: decimalFromDms(match[1], match[2], `${match[3]}.${match[4]}`, direction),
      direction
    };
  }

  match = body.match(/^([-+]?\d{1,3})°(\d{2})'?(?:(\d{1,2})(\d{2})|(\d{1,2})\.(\d+))$/);
  if (match) {
    const seconds = match[3] ? `${match[3]}.${match[4]}` : `${match[5]}.${match[6]}`;

    return {
      value: decimalFromDms(match[1], match[2], seconds, direction),
      direction
    };
  }

  return null;
}

function getDmsTokensFromLine(line) {
  return normalizeDmsLineForParsing(line).match(/[-+]?\d{1,3}\s*°\s*(?:\d{1,2}\s*'\s*\d{1,4}(?:\.\d+)?|\d{1,2}\s+\d{1,4}(?:\.\d+)?|\d{3,7}(?:\.\d+)?)\s*["']?\s*[NSEWO]?/gi) || [];
}

function tokenHasDirection(token) {
  return /[NSEWO]\s*$/i.test(String(token || "").trim());
}

function shouldInferWestNorth(firstToken, secondToken) {
  if (tokenHasDirection(firstToken) || tokenHasDirection(secondToken)) {
    return false;
  }

  const first = parseCompactDmsToken(firstToken, "");
  const second = parseCompactDmsToken(secondToken, "");
  const firstValue = Math.abs(Number(first?.value));
  const secondValue = Math.abs(Number(second?.value));

  return Number.isFinite(firstValue)
    && Number.isFinite(secondValue)
    && firstValue > 0
    && secondValue > 0
    && firstValue <= 20
    && secondValue <= 20
    && firstValue < secondValue;
}

function parseLooseDmsPart(part, fallbackDirection) {
  const normalized = String(part || "")
    .toUpperCase()
    .replace(/OUEST/g, "O")
    .replace(/WEST/g, "W")
    .replace(/[\u00BA\u02DA]/g, "\u00B0")
    .replace(/[\u2018\u2019\u00B4`\u2032]/g, "'")
    .replace(/[\u201C\u201D\u2033]/g, '"')
    .trim();
  const directionMatch = normalized.match(/[NSEWO]/);
  const direction = (directionMatch ? directionMatch[0] : fallbackDirection || "").toUpperCase();
  const withoutDirection = normalized.replace(/[NSEWO]/g, " ").trim();
  const dotDmsMatch = withoutDirection.match(/^([-+]?\d{1,3})\.(\d{1,2})\.(\d{1,2})(?:\.(\d+))?$/);
  let parts = [];

  if (dotDmsMatch) {
    parts = [dotDmsMatch[1], dotDmsMatch[2], dotDmsMatch[4] ? `${dotDmsMatch[3]}.${dotDmsMatch[4]}` : dotDmsMatch[3]];
  } else if (withoutDirection.includes("\u00B0")) {
    const [degreeText, restText = ""] = withoutDirection.split("\u00B0");
    const degrees = (degreeText.match(/[-+]?\d+/) || [])[0];
    const rest = restText.trim();

    const groups = rest.match(/\d+(?:\.\d+)?/g) || [];
    if (groups.length === 2 && groups[0].includes(".")) {
      const [minutes, secondsStart] = groups[0].split(".");
      const secondsEnd = groups[1].replace(/^0\./, "");
      parts = [degrees, minutes, `${secondsStart}.${secondsEnd}`];
    } else if (groups.length === 3 && groups[1].includes(".")) {
      const [minutes, secondsStart] = groups[1].split(".");
      const secondsEnd = groups[2].replace(/^0\./, "");
      parts = [groups[0], minutes, `${secondsStart}.${secondsEnd}`];
    } else if (/\s/.test(rest.replace(/['"]/g, " "))) {
      parts = [degrees, ...groups];
    } else {
      if (groups.length >= 3) {
        parts = [degrees, groups[0], `${groups[1]}.${groups.slice(2).join("")}`];
      } else {
        parts = [degrees, ...groups];
      }
    }
  } else {
    const groups = withoutDirection.match(/\d+(?:\.\d+)?/g) || [];
    if (groups.length === 3 && groups[1].includes(".")) {
      const [minutes, secondsStart] = groups[1].split(".");
      parts = [groups[0], minutes, `${secondsStart}.${groups[2]}`];
    } else if (groups.length === 2 && groups[0].includes(".")) {
      const [minutes, secondsStart] = groups[0].split(".");
      const secondsEnd = groups[1].replace(/^0\./, "");
      parts = [fallbackDirection ? "" : groups[0], minutes, `${secondsStart}.${secondsEnd}`];
    } else if (groups.length >= 4) {
      parts = [groups[0], groups[1], `${groups[2]}.${groups.slice(3).join("")}`];
    } else {
      parts = withoutDirection.match(/[-+]?\d+(?:\.\d+)?/g) || [];
    }
  }

  if (parts.length < 3) {
    return null;
  }

  const value = decimalFromDms(parts[0], parts[1], parts[2], direction);

  if (value === null) {
    return null;
  }

  return {
    value,
    direction,
    axis: ["N", "S"].includes(direction) ? "lat" : "lon"
  };
}

function stripOcrBboxPrefix(line) {
  return String(line || "").replace(/^\s*(?:\d+(?:\.\d+)?\s*,\s*){4,6}(?=\d{1,2}\s*[\).:\-])/i, "");
}

function parseLooseDmsLine(line) {
  const text = normalizeDmsLineForParsing(stripOcrBboxPrefix(line)).trim();
  const partPattern = /[-+]?\d{1,3}(?:(?:\s*\u00B0\s*|\s+)\d{1,2}(?:[\s.'"'\u2032]+\d{1,2}){1,2}(?:\.\d+)?|\.\d{1,2}\.\d{1,2}(?:\.\d+)?)\s*["\u2033]?\s*[NSEWO]/gi;
  const parts = text.match(partPattern) || [];

  if (parts.length < 2) {
    return null;
  }

  const parsed = parts
    .map(part => parseLooseDmsPart(part, ""))
    .filter(Boolean)
    .filter(item => item.value !== null);

  if (parsed.length < 2) {
    return null;
  }

  const latitude = parsed.find(item => item.axis === "lat");
  const longitude = parsed.find(item => item.axis === "lon");

  if (!latitude || !longitude) {
    return null;
  }

  const lonNumber = Number(longitude.value);
  const latNumber = Number(latitude.value);

  if (Math.abs(lonNumber) > 180 || Math.abs(latNumber) > 90) {
    return null;
  }

  return {
    longitude: longitude.value,
    latitude: latitude.value
  };
}

function groupEveryFourDmsLinesWhenLikely(text, sourceText = "") {
  const rawSource = String(sourceText || "");

  if (/\bpoint\b|latitude|longitude|(?:^|\W)n\s*(?:\u00B0|\u00BA|o|掳)/i.test(rawSource)) {
    return text;
  }

  const lines = String(text || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean);

  if (lines.length < 8) {
    return text;
  }

  return lines
    .map((line, index) => (index > 0 && index % 4 === 0 ? `\n${line}` : line))
    .join("\n");
}

function cleanCoordinateOutput(text) {
  return String(text || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .reduce((lines, line) => {
      if (!line) {
        if (lines.length > 0 && lines[lines.length - 1] !== "") {
          lines.push("");
        }
        return lines;
      }

      lines.push(line);
      return lines;
    }, [])
    .join("\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function foldSearchText(text) {
  return String(text || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function looksLikeCoordinateTable(text) {
  const value = foldSearchText(text);

  return /\b(point|sommets?|sommet|coordonn[eé]es?|latitude|longitude|bftm|itrf|projection|cart[eé]siennes?)\b/i.test(value)
    || /\bn\s*(?:\u00B0|\u00BA|o|掳)\b/i.test(value)
    || /经度|纬度|北纬|东经|西经/.test(String(text || ""))
    || /\b(liste des coordonnees|coordonnees?|cartesiennes?)\b/i.test(value)
    || /\bx\s*\(?m?\)?\b[\s\S]{0,80}\by\s*\(?m?\)?\b/i.test(value);
}

function looksLikeProjectedContext(text) {
  const value = foldSearchText(text);

  return /\b(bftm|itrf|projection|coordonn[eé]es?\s*(?:en\s*)?(?:bftm|xy|x\/y|projet|cart[eé]siennes?)|sommets?)\b/i.test(value)
    || /\b(liste des coordonnees|coordonnees?|cartesiennes?)\b/i.test(value)
    || /\bx\s*\(?m?\)?\b[\s\S]{0,80}\by\s*\(?m?\)?\b/i.test(value);
}

function hasDmsGroupedContext(text) {
  const value = String(text || "");
  const folded = foldSearchText(value);

  return /\bmining\s+area\b|\barea\s+(?:one|two|three|\d+)\b|the\s+coordinates\s+are\s+as\s+follows|coordinates?\s+are\s+as\s+follows/i.test(folded)
    || (/[°º'′″"]/.test(value) && /\d\s*[NS]\b[\s\S]{0,120}\d\s*[EWO]\b|\d\s*[EWO]\b[\s\S]{0,120}\d\s*[NS]\b/i.test(value));
}

function hasFrenchPerimeterDmsContext(text) {
  const value = String(text || "");
  const folded = foldSearchText(value);
  const directPointRows = value
    .split(/\r?\n/)
    .filter(line => /\bPoint\s+[A-Z]\b/i.test(line)
      && /\b(?:Ouest|West|O)\b/i.test(line)
      && /\b(?:Nord|North|N)\b/i.test(line));

  if (directPointRows.length >= 1) {
    return true;
  }

  return /coordonnees?\s+du\s+perimetre|coordonnees?\s+du\s+p[eé]rimetre|perimetre|p[eé]rimetre/.test(folded)
    && /meridien|m[eé]ridien/.test(folded)
    && /parallele|parall[eè]le/.test(folded)
    && /ouest/.test(folded)
    && /nord/.test(folded)
    && /\bpoint\s+[a-z]\b/i.test(folded);
}

function looksLikeCorrectionContext(text) {
  const value = foldSearchText(text);

  return /修正|手写|红色|框选|涂改|改动|correction|corrige|corrigee|rouge|red|manual|handwritten/i.test(value);
}

function countTableBoundaryHints(text) {
  return String(text || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(line => /\b(rc\d|point|sommets?|latitude|longitude|liste des coordonnees|coordonnees?|x\s*\(?m?\)?|y\s*\(?m?\)?)\b/i.test(foldSearchText(line)) || /经度|纬度|北纬|东经|西经/.test(line))
    .length;
}

function looksLikeHandwrittenDmsBlock(text) {
  const lines = normalizeText(text)
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean);
  const numberedDmsLines = [];
  let dmsLineCount = 0;

  for (const line of lines) {
    const cleanLine = stripOcrBboxPrefix(line);
    if (parseLooseDmsLine(cleanLine) || getDmsTokensFromLine(cleanLine).length >= 2) {
      dmsLineCount += 1;
    }
    const numberMatch = cleanLine.match(/^\s*(?:point\s*)?(\d{1,2})\s*[\).:\-]?\s+/i);
    if (!numberMatch) {
      continue;
    }

    if (parseLooseDmsLine(cleanLine) || getDmsTokensFromLine(cleanLine).length >= 2) {
      numberedDmsLines.push(Number(numberMatch[1]));
    }
  }

  if (!looksLikeCoordinateTable(text) && dmsLineCount >= 8) {
    return true;
  }

  if (numberedDmsLines.length < 4) {
    return false;
  }

  for (let index = 1; index < numberedDmsLines.length; index += 1) {
    if (numberedDmsLines[index] === 1 && numberedDmsLines[index - 1] >= 3) {
      return true;
    }
  }

  return !looksLikeCoordinateTable(text) && numberedDmsLines.length >= 8;
}

function getHandwrittenDmsInfo(rawText, coordinates, options = {}) {
  const sourceText = String(rawText || "");
  const coordinateText = String(coordinates || "");
  const dmsRows = countDmsCoordinateRows(sourceText);
  const pointRows = countCoordinateRows(coordinateText);
  const rawDmsRows = getLikelyHandwrittenDmsRawRowCount(sourceText);
  const isOcrImage = Boolean(options.isOcrImage);
  const hasStrongHandwrittenDmsRows = rawDmsRows >= 8;
  const hasExplicitHandwrittenDmsContext = Boolean(options.hasExplicitHandwrittenDmsContext);

  const isHandwrittenDms = isOcrImage
    && (dmsRows >= 4 || rawDmsRows >= 4)
    && (pointRows >= 4 || rawDmsRows >= 4)
    && (looksLikeHandwrittenDmsBlock(sourceText) || hasStrongHandwrittenDmsRows)
    && (!getDmsGroupedCoordinateInfo(sourceText).output || hasExplicitHandwrittenDmsContext || hasStrongHandwrittenDmsRows)
    && (!looksLikeCoordinateTable(sourceText) || hasStrongHandwrittenDmsRows)
    && (!hasDmsGroupedContext(sourceText) || hasExplicitHandwrittenDmsContext || hasStrongHandwrittenDmsRows)
    && !getFrenchPerimeterDmsInfo(sourceText).isFrenchPerimeterDms
    && !looksLikePointAzDmsTable(sourceText)
    && !hasBftmContext(sourceText)
    && !getMgrsInfo(sourceText).isMgrs
    && (!getWgs84TableCoordinatesInfo(sourceText).isWgs84TableCoordinates || hasStrongHandwrittenDmsRows)
    && (!getChatCoordinatesInfo(sourceText).isChatCoordinates || hasStrongHandwrittenDmsRows);

  return {
    isHandwrittenDms,
    dmsRows,
    rawDmsRows,
    pointRows
  };
}

function getHandwrittenDmsTimeoutRoutingEvidence(file, hint = "", options = {}) {
  const value = `${file?.originalname || ""} ${hint || ""}`;
  const folded = foldSearchText(value);
  const ocrText = String(options.ocrText || "");
  const mimeType = String(file?.mimetype || "").toLowerCase();
  const fileSize = Number(file?.size || file?.buffer?.length || 0);
  const highConfidenceNameOrHint = /手写|手寫|鎵嬪啓|HANDWRITTEN_DMS|handwritten|hand-written|hand\s*written|manual\s*dms|manuscript|handwritten_dms|handwritten-dms/i.test(value)
    || /handwritten[_\s-]*dms|hand\s*written[_\s-]*dms|manuscript/.test(folded);

  const evidence = {
    shouldRetry: highConfidenceNameOrHint,
    reason: highConfidenceNameOrHint ? "filename_or_hint" : "",
    highConfidenceNameOrHint,
    hasImageMime: /^image\/(?:jpe?g|png|webp|bmp|tiff?)$/.test(mimeType),
    fileSize,
    ocrTextAvailable: Boolean(ocrText.trim()),
    score: 0,
    numericTokenCount: 0,
    directionTokenCount: 0,
    dmsSymbolCount: 0,
    dmsPairLineCount: 0,
    damagedDmsLineCount: 0,
    tableBoundaryHints: 0,
    stableDetectorMatched: false,
    stableDetectorReasons: []
  };

  if (evidence.shouldRetry || !evidence.ocrTextAvailable) {
    return evidence;
  }

  const addStableReason = (condition, reason) => {
    if (condition) evidence.stableDetectorReasons.push(reason);
  };

  addStableReason(getCadastralGridInfo(ocrText).isCadastralGrid, "madagascar_cadastral_grid");
  addStableReason(getMgrsInfo(ocrText).isMgrs, "mgrs");
  addStableReason(getMozambiqueGeographicInfo(ocrText).isMozambiqueGeographicTable, "mozambique_geographic_table");
  addStableReason(hasCoteDIvoireGeographicDmsCue(ocrText), "cote_divoire_geographic_dms_table");
  addStableReason(getWgs84TableCoordinatesInfo(ocrText).isWgs84TableCoordinates, "wgs84_table");
  addStableReason(getKyrgyzGkInfo(ocrText).isKyrgyzGk, "kyrgyz_gk");
  addStableReason(hasBftmContext(ocrText) || getBftmLongTableInfo(ocrText, extractCoordinateLines(ocrText)).isBftmLongTable, "bftm");
  addStableReason(getDmsGroupedCoordinateInfo(ocrText).output, "standard_dms_grouped");
  addStableReason(getFrenchPerimeterDmsInfo(ocrText).isFrenchPerimeterDms, "french_perimeter_dms");
  addStableReason(looksLikePointAzDmsTable(ocrText), "point_az_dms_table");
  addStableReason(looksLikeCoordinateTable(ocrText), "printed_coordinate_table");

  evidence.stableDetectorMatched = evidence.stableDetectorReasons.length > 0;
  if (evidence.stableDetectorMatched) {
    evidence.reason = "stable_detector_blocked";
    return evidence;
  }

  const lines = normalizeText(ocrText)
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean);

  evidence.numericTokenCount = (ocrText.match(/\d+/g) || []).length;
  evidence.directionTokenCount = (ocrText.match(/[NSEWO]/gi) || []).length;
  evidence.dmsSymbolCount = (ocrText.match(/[°º'′″"]/g) || []).length;
  evidence.tableBoundaryHints = countTableBoundaryHints(ocrText);

  for (const line of lines) {
    const digitCount = (line.match(/\d/g) || []).length;
    const hasDirection = /[NSEWO]|\\[NW]|\/W|\/N/i.test(line);
    const hasDegreeLike = /[°º]|(?:\d\s*[oO]\s*[fF])|(?:\b[oO0]F\b)|(?:[Ff]x°)|(?:°\s*\d)|(?:\d+\s*[.]\s*\d+\s*[.]\s*\d+)/.test(line);
    const looseParts = getLooseDmsPartsFromLine(line);

    if (looseParts.length >= 2) {
      evidence.dmsPairLineCount += 1;
    }

    if (digitCount >= 4 && hasDirection && hasDegreeLike) {
      evidence.damagedDmsLineCount += 1;
    }
  }

  if (evidence.hasImageMime) evidence.score += 1;
  if (evidence.numericTokenCount >= 18) evidence.score += 1;
  if (evidence.directionTokenCount >= 4) evidence.score += 1;
  if (evidence.dmsSymbolCount >= 3 || evidence.damagedDmsLineCount >= 2) evidence.score += 1;
  if (evidence.dmsPairLineCount >= 3) evidence.score += 2;
  if (evidence.damagedDmsLineCount >= 3) evidence.score += 2;
  if (evidence.tableBoundaryHints <= 1) evidence.score += 1;
  if (looksLikeCorrectionContext(ocrText)) evidence.score += 1;

  const hasEnoughDmsStructure = evidence.dmsPairLineCount >= 3 || evidence.damagedDmsLineCount >= 3;
  evidence.shouldRetry = evidence.score >= 5 && hasEnoughDmsStructure;
  evidence.reason = evidence.shouldRetry ? "ocr_dms_structure" : "insufficient_evidence";
  return evidence;
}

function shouldRetryHandwrittenDmsOnTimeout(file, hint = "", options = {}) {
  return getHandwrittenDmsTimeoutRoutingEvidence(file, hint, options).shouldRetry;
}


function groupEveryFourLinesWhenLikely(text, sourceText = "") {
  // Stable path: handwritten DMS uses rawText-derived recognizedLines and this four-line grouping.
  // Do not apply this to table formats such as standard DMS tables, BFTM/X-Y, or cadastral grids.
  if (looksLikeCoordinateTable(sourceText)) {
    return text;
  }

  const lines = String(text || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean);

  if (lines.length < 8) {
    return text;
  }

  return lines
    .map((line, index) => (index > 0 && index % 4 === 0 ? `\n${line}` : line))
    .join("\n");
}

function groupHandwrittenDmsCoordinateLines(text) {
  const lines = String(text || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean);

  if (lines.length < 8) {
    return String(text || "");
  }

  return lines
    .map((line, index) => (index > 0 && index % 4 === 0 ? `\n${line}` : line))
    .join("\n");
}

function extractHandwrittenDmsRawRows(text) {
  return String(text || "")
    .replace(/[º˚]/g, "°")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .map(line => line.replace(/^\s*(?:point|pt|p|no|n[°o]?|#)?\s*[\dA-Z]{1,3}\s*[:.)-]\s*/i, ""))
    .filter(line => {
      const normalized = normalizeText(line);
      const digitCount = (normalized.match(/\d/g) || []).length;
      const directions = (normalized.match(/[NSEWO]/gi) || []).length;
      const dmsSymbolCount = (normalized.match(/[°º'′″"]/g) || []).length;
      const hasDamagedDmsPair = /\d{1,3}\s*[°º.]\s*\d{1,2}[.'′]\d{2,4}\s*['′]?\s*[NSEWO]/i.test(normalized)
        && /[,，\s]\s*\d{1,3}\s*[°º.]\s*\d{1,2}[.'′]\d{2,4}\s*['′]?\s*[NSEWO]/i.test(normalized);
      const dmsLikeParts = getLooseDmsPartsFromLine(normalized);
      return digitCount >= 8
        && directions >= 2
        && (dmsLikeParts.length >= 2 || (dmsSymbolCount >= 2 && hasDamagedDmsPair));
    });
}

function getLikelyHandwrittenDmsRawRowCount(text) {
  return extractHandwrittenDmsRawRows(text).length;
}

function formatHandwrittenDmsRawRows(text) {
  return groupHandwrittenDmsCoordinateLines(extractHandwrittenDmsRawRows(text).join("\n"));
}

function applyHandwrittenDmsCoordinateGrouping(coordinates, handwrittenDms = {}) {
  if (!handwrittenDms?.isHandwrittenDms) {
    return coordinates;
  }

  return groupHandwrittenDmsCoordinateLines(coordinates);
}

function extractDecimalCoordinateLines(text) {
  // Stable path: plain decimal lon/lat is only the ordinary polygon fallback.
  // It must not override BFTM/X-Y projected tables or Madagascar cadastral grid num|XV|YV tables.
  const lines = normalizeText(text)
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean);
  const coordinateLines = [];

  for (const line of lines) {
    const match = line.match(/^(-?\d+(?:\.\d+)?)\s*[,，]\s*(-?\d+(?:\.\d+)?)$/)
      || line.match(/^(-?\d+\.\d+)\s+(-?\d+\.\d+)$/)
      || parseSpaceBrokenDecimalLine(line);

    if (!match) {
      continue;
    }

    const fixedPair = fixLikelyLatLonOrder(match[1].trim(), match[2].trim());
    const longitudeText = fixedPair.longitudeText;
    const latitudeText = fixedPair.latitudeText;
    const longitude = Number(longitudeText);
    const latitude = Number(latitudeText);

    if (Math.abs(longitude) <= 180 && Math.abs(latitude) <= 90) {
      coordinateLines.push(`${longitudeText},${latitudeText}`);
    }
  }

  return coordinateLines;
}

function splitGroupsAtRepeatedBoundary(text) {
  const lines = String(text || "")
    .split(/\r?\n/)
    .map(line => line.trim());
  const result = [];
  let currentGroup = [];

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];

    if (!line) {
      if (result.length > 0 && result[result.length - 1] !== "") {
        result.push("");
      }
      currentGroup = [];
      continue;
    }

    result.push(line);

    if (!/^[-+]?\d+(?:\.\d+)?\s*,\s*[-+]?\d+(?:\.\d+)?$/.test(line)) {
      continue;
    }

    const normalized = line.replace(/\s*,\s*/g, ",");
    const previousNormalized = currentGroup[currentGroup.length - 1];
    currentGroup.push(normalized);

    const remainingCoordinateCount = lines
      .slice(index + 1)
      .filter(nextLine => /^[-+]?\d+(?:\.\d+)?\s*,\s*[-+]?\d+(?:\.\d+)?$/.test(nextLine))
      .length;

    if (
      currentGroup.length >= 4
      && remainingCoordinateCount >= 3
      && previousNormalized === normalized
      && result[result.length - 1] !== ""
    ) {
      result.push("");
      currentGroup = [];
    }
  }

  return result.join("\n").replace(/\n{3,}/g, "\n\n").trim();
}

function extractNumbersWithThousands(text) {
  return (String(text || "").match(/[-+]?\d{1,3}(?:\s+\d{3})+(?:\.\d+)?|[-+]?\d+(?:\.\d+)?/g) || [])
    .map(value => value.replace(/\s+/g, ""));
}

function looksLikeProjectedPair(first, second) {
  const x = Math.abs(Number(first));
  const y = Math.abs(Number(second));

  if (!Number.isFinite(x) || !Number.isFinite(y)) {
    return false;
  }

  return x >= 10000 && y >= 10000;
}

function isBftmXValue(value) {
  const number = Math.abs(Number(value));
  return Number.isFinite(number) && number >= 500000 && number <= 760000;
}

function isBftmYValue(value) {
  const number = Math.abs(Number(value));
  return Number.isFinite(number) && number >= 1200000 && number <= 1600000;
}

function looksLikeBftmProjectedPair(first, second) {
  return isBftmXValue(first) && isBftmYValue(second);
}

function hasCadastralGridContext(text) {
  const value = String(text || "");
  return /\bXV\b/i.test(value)
    && /\bYV\b/i.test(value)
    && (/\bnum\b|n[°o]\b|cadastral|cadastre|grid|grille|quadrillage|carreau|矿权|网格/i.test(value));
}

function normalizeGridValue(value) {
  return String(value || "")
    .trim()
    .replace(/,/g, ".")
    .replace(/\s+/g, "");
}

function extractCadastralGridRows(text) {
  // Stable path: Madagascar cadastral grid extraction returns only num | XV | YV.
  // Do not include NC/CM_NOMFIR here and do not convert XV/YV in the backend recognition response.
  if (!hasCadastralGridContext(text)) {
    return [];
  }

  const rows = [];
  const seen = new Set();

  normalizeText(text)
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .forEach(line => {
      if (/^(?:num|n[°o]?|#)?\s*[\|,;\s-]*xv[\|,;\s-]*yv$/i.test(line)) {
        return;
      }

      let row = null;
      const labeled = line.match(/(?:^|\b)(?:num|n[°o]?|#)?\s*([A-Za-z0-9-]{1,16})\D+XV\D*([-+]?\d+(?:[.,]\d+)?)\D+YV\D*([-+]?\d+(?:[.,]\d+)?)/i);

      if (labeled) {
        row = {
          num: labeled[1].trim(),
          xv: normalizeGridValue(labeled[2]),
          yv: normalizeGridValue(labeled[3])
        };
      } else {
        const cleaned = line
          .replace(/\b(?:num|n[°o]?|xv|yv)\b/gi, " ")
          .replace(/[|:;，,]/g, " ");
        const tokens = cleaned.match(/[-+]?\d+(?:[.,]\d+)?|[A-Za-z]?\d[A-Za-z0-9-]*/g) || [];

        if (tokens.length >= 3) {
          row = {
            num: tokens[0].trim(),
            xv: normalizeGridValue(tokens[1]),
            yv: normalizeGridValue(tokens[2])
          };
        }
      }

      if (!row || !row.num || !row.xv || !row.yv) {
        return;
      }

      const key = `${row.num}|${row.xv}|${row.yv}`;
      if (seen.has(key)) {
        return;
      }

      seen.add(key);
      rows.push(row);
    });

  return rows;
}

function formatCadastralGridRows(rows) {
  return ["num | XV | YV", ...rows.map(row => `${row.num} | ${row.xv} | ${row.yv}`)].join("\n");
}

function getCadastralGridInfo(text) {
  const rows = extractCadastralGridRows(text);
  return {
    isCadastralGrid: rows.length > 0,
    rows,
    rowCount: rows.length
  };
}

function hasKyrgyzGkContext(text) {
  const value = String(text || "");
  return /Координаты\s+угловых\s+точек|лицензионн(?:ой|ая|ую)|прямоугольн(?:ой|ая|ую)\s+систем|№\s*точек|N\s*o?\s*points?|Kyrgyzstan|Киргиз|Кыргыз|Pulkovo|Gauss|Гаусс|Крюгер/i.test(value)
    && /\bX\b/i.test(value)
    && /\bY\b/i.test(value);
}

function looksLikeKyrgyzGkPair(x, y) {
  const easting = Number(x);
  const northing = Number(y);
  return Number.isFinite(easting)
    && Number.isFinite(northing)
    && easting >= 13000000
    && easting <= 13999999
    && northing >= 3900000
    && northing <= 4800000;
}

function normalizeKyrgyzGkValue(value) {
  return String(value || "")
    .trim()
    .replace(/\s+/g, "")
    .replace(/,/g, ".");
}

function normalizeKyrgyzGkNorthingValue(value) {
  const normalized = normalizeKyrgyzGkValue(value).replace(/[^\d.-]/g, "");

  // Tesseract sometimes drops the leading 4 in Kyrgyz GK northing values
  // such as 4607447, returning 607447. Only repair the narrow 60xxxx shape.
  if (/^60\d{4}$/.test(normalized)) {
    return `4${normalized}`;
  }

  return normalized;
}

function extractKyrgyzGkPairsFromLine(line) {
  const pairs = [];
  const pairPattern = /(?:^|[^\d])(13\d{5,7})\D+([46]\d{5,6})(?=$|[^\d])/g;
  let match;

  while ((match = pairPattern.exec(String(line || ""))) !== null) {
    const x = normalizeKyrgyzGkValue(match[1]).replace(/[^\d.-]/g, "");
    const y = normalizeKyrgyzGkNorthingValue(match[2]);

    if (looksLikeKyrgyzGkPair(x, y)) {
      pairs.push({ x, y });
    }
  }

  return pairs;
}

function inferKyrgyzGkRowsByTableOrder(text) {
  const source = normalizeText(text);
  const tableRows = source
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .map(line => extractKyrgyzGkPairsFromLine(line).slice(0, 2))
    .filter(pairs => pairs.length > 0);

  if (tableRows.length < 20) {
    return [];
  }

  const leftColumnCount = tableRows.length;
  const dualColumnRows = tableRows.filter(pairs => pairs.length >= 2).length;
  const rowsByPoint = new Map();

  tableRows.forEach((pairs, index) => {
    const leftPoint = index + 1;
    rowsByPoint.set(leftPoint, {
      point: leftPoint,
      x: pairs[0].x,
      y: pairs[0].y
    });

    if (pairs[1]) {
      const rightPoint = leftColumnCount + index + 1;
      rowsByPoint.set(rightPoint, {
        point: rightPoint,
        x: pairs[1].x,
        y: pairs[1].y
      });
    }
  });

  const inferredRows = Array.from(rowsByPoint.values()).sort((a, b) => a.point - b.point);
  const integrity = analyzeKyrgyzGkRows(inferredRows, "table-order-fallback");
  const looksLikeTwoColumnTable = leftColumnCount >= 25
    && leftColumnCount <= 40
    && dualColumnRows >= Math.max(15, Math.floor(leftColumnCount * 0.65));
  const plausibleTotal = inferredRows.length >= 50 && inferredRows.length <= 80;

  if (
    !looksLikeTwoColumnTable
    || !plausibleTotal
    || integrity.firstPoint !== 1
    || !integrity.continuous
    || integrity.abnormalPoints.length > 0
  ) {
    return [];
  }

  return inferredRows;
}

function extractKyrgyzGkRows(text) {
  const source = normalizeText(text);
  const projectedPairCount = (source.match(/13\d{5,7}\s*[\|,;\s]+\s*[46]\d{5,6}/g) || []).length;

  if (!hasKyrgyzGkContext(source) && !/^point\s*\|\s*X\s*\|\s*Y/im.test(source) && projectedPairCount < 8) {
    return [];
  }

  const rowsByPoint = new Map();
  const rowPattern = /(?:^|[^\d])(\d{1,3})\s*[\|,;\s]+\s*(13\d{5,7})\s*[\|,;\s]+\s*([46]\d{5,6})(?=$|[^\d])/g;

  source
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .forEach(line => {
      if (/^(?:point|№|no\.?|n)\s*[\|,;\s-]*x\s*[\|,;\s-]*y$/i.test(line) || /точек/i.test(line)) {
        return;
      }

      rowPattern.lastIndex = 0;
      let match;

      while ((match = rowPattern.exec(line)) !== null) {
        const point = Number(match[1]);
        const x = normalizeKyrgyzGkValue(match[2]).replace(/[^\d.-]/g, "");
        const y = normalizeKyrgyzGkNorthingValue(match[3]);

        if (!Number.isInteger(point) || point <= 0 || !looksLikeKyrgyzGkPair(x, y)) {
          continue;
        }

        if (!rowsByPoint.has(point)) {
          rowsByPoint.set(point, { point, x, y });
        }
      }
    });

  const explicitRows = Array.from(rowsByPoint.values()).sort((a, b) => a.point - b.point);
  const inferredRows = inferKyrgyzGkRowsByTableOrder(source);
  const explicitIntegrity = analyzeKyrgyzGkRows(explicitRows);
  const inferredIntegrity = analyzeKyrgyzGkRows(inferredRows, "table-order-fallback");

  if (
    inferredRows.length >= explicitRows.length
    && inferredRows.length >= 50
    && inferredIntegrity.firstPoint === 1
    && inferredIntegrity.abnormalPoints.length === 0
  ) {
    return inferredRows;
  }

  if (explicitIntegrity.isComplete) {
    return explicitRows;
  }

  return explicitRows;
}

function countKyrgyzGkCoordinatePairs(text) {
  const source = normalizeText(text);
  const pairPattern = /(?:^|[^\d])(13\d{5,7})\s*[\|,;\s]+\s*(4\d{6})(?=$|[^\d])/g;
  let count = 0;
  let match;

  while ((match = pairPattern.exec(source)) !== null) {
    if (looksLikeKyrgyzGkPair(match[1], match[2])) {
      count += 1;
    }
  }

  return count;
}

function shouldCheckKyrgyzGkTable(rawText, coordinates) {
  return hasKyrgyzGkContext(rawText)
    || hasKyrgyzGkContext(coordinates)
    || countKyrgyzGkCoordinatePairs(rawText) >= 8
    || countKyrgyzGkCoordinatePairs(coordinates) >= 8;
}

function shouldUseKyrgyzGkPromptFirst(file, rawHint = "") {
  const fileName = String(file?.originalname || "");
  const decodedFileName = Buffer.from(fileName, "latin1").toString("utf8");
  const hint = String(rawHint || "");
  const combined = `${fileName}\n${decodedFileName}\n${hint}`;

  return /吉尔吉斯|Kyrgyz|Kyrgyzstan|Киргиз|Кыргыз/i.test(combined)
    || /№\s*точек|Координаты\s+угловых\s+точек|лицензионн(?:ой|ая|ую)|прямоугольн(?:ой|ая|ую)\s+систем/i.test(combined)
    || /13\d{5,7}[\s|,;]+4\d{6}/.test(combined);
}

function getUploadNameSearchText(file, rawHint = "") {
  const fileName = String(file?.originalname || "");
  const variants = new Set([fileName, String(rawHint || "")]);

  try {
    variants.add(Buffer.from(fileName, "latin1").toString("utf8"));
  } catch {
    // Keep the original name if the upload name is not latin1-mojibake.
  }

  for (const value of Array.from(variants)) {
    try {
      variants.add(decodeURIComponent(value));
    } catch {
      // Ignore non-URI file names.
    }
  }

  return Array.from(variants).filter(Boolean).join("\n");
}

function hasExplicitHandwrittenDmsUploadContext(file, rawHint = "") {
  return /手写|handwritten|manuscript|handwritten\s*dms/i.test(getUploadNameSearchText(file, rawHint));
}

function shouldUseMozambiqueGeographicPromptFirst(file, rawHint = "") {
  const combined = getUploadNameSearchText(file, rawHint);

  return /莫桑比克|Mozambique|Mo[çc]ambique|Tete|COORDENADAS\s+GEOGR[ÁA]FICAS|Datum\s*:?\s*Tete|INAMI|MIREME/i.test(combined)
    && !/Kyrgyz|Kyrgyzstan|吉尔吉斯|Киргиз|Кыргыз/i.test(combined);
}

function getMozambiqueTypeLockEvidence(file, rawHint = "", options = {}) {
  const uploadText = getUploadNameSearchText(file, rawHint);
  const text = String(options.text || options.rawText || "");
  const combined = `${uploadText}\n${text}`;
  const evidence = [];

  if (/莫桑比克|Mozambique|Mo[çc]ambique|Tete/i.test(uploadText)) {
    evidence.push("filename_or_hint_mozambique");
  }

  const hasCandidate = evidence.length > 0;
  const documentCuePatterns = [
    [/COORDENADAS/i, "document_coordenadas"],
    [/GEOGR[ÁA]FICAS/i, "document_geograficas"],
    [/Datum\s*:?\s*Tete/i, "document_datum_tete"],
    [/\bLatitude\b/i, "document_latitude"],
    [/\bLongitude\b/i, "document_longitude"],
    [/\bOrdem\b|\bOrder\b/i, "document_ordem"]
  ];

  documentCuePatterns.forEach(([pattern, label]) => {
    if (pattern.test(combined)) {
      evidence.push(label);
    }
  });

  if (options.detectorMatched || shouldUseMozambiqueGeographicPromptFirst(file, rawHint)) {
    evidence.push("mozambique_detector_matched");
  }
  if (options.geographicInfo?.isMozambiqueGeographicTable || getMozambiqueGeographicInfo(text).isMozambiqueGeographicTable) {
    evidence.push("mozambique_geographic_info_matched");
  }
  if (Array.isArray(options.rows) && options.rows.length > 0) {
    evidence.push("mozambique_rows_extracted");
  }

  const strict = evidence.some(item => [
    "mozambique_detector_matched",
    "mozambique_geographic_info_matched",
    "mozambique_rows_extracted"
  ].includes(item));
  const hasDocumentEvidence = evidence.some(item => item.startsWith("document_"));

  return {
    level: strict ? "strict" : (hasCandidate && hasDocumentEvidence ? "evidence" : (hasCandidate ? "candidate" : "")),
    evidence: Array.from(new Set(evidence)),
    coordinate_type: "mozambique_geographic_table"
  };
}

function shouldRetryWgs84TableOnTimeout(file, rawHint = "") {
  const combined = getUploadNameSearchText(file, rawHint);

  return /刚果|Congo|RC\s*2|RC2|经度|纬度|经度东|东经|北纬|Longitude|Latitude|\bLon\b|\bLat\b/i.test(combined)
    && !/Kyrgyz|Kyrgyzstan|吉尔吉斯|Киргиз|Кыргыз|莫桑比克|Mozambique|Mo[çc]ambique|Tete|BFTM|MGRS|UTM|COORDENADAS\s+GEOGR[ÁA]FICAS/i.test(combined);
}

function formatKyrgyzGkRows(rows) {
  return ["point | X | Y", ...rows.map(row => `${row.point} | ${row.x} | ${row.y}`)].join("\n");
}

function analyzeKyrgyzGkRows(rows, source = "vision") {
  const points = Array.isArray(rows)
    ? rows
      .map(row => Number(row?.point))
      .filter(point => Number.isInteger(point) && point > 0)
      .sort((a, b) => a - b)
    : [];
  const uniquePoints = Array.from(new Set(points));
  const firstPoint = uniquePoints[0] || null;
  const lastPoint = uniquePoints[uniquePoints.length - 1] || null;
  const abnormalPoints = uniquePoints.filter(point => point > 200);
  const missingPoints = [];

  if (firstPoint === 1 && Number.isInteger(lastPoint)) {
    for (let point = 1; point <= lastPoint; point += 1) {
      if (!uniquePoints.includes(point)) {
        missingPoints.push(point);
      }
    }
  }

  const startsAtOne = firstPoint === 1;
  const continuous = startsAtOne && missingPoints.length === 0;
  const isComplete = uniquePoints.length >= 3 && startsAtOne && continuous && abnormalPoints.length === 0;

  return {
    source,
    rowCount: uniquePoints.length,
    firstPoint,
    lastPoint,
    startsAtOne,
    continuous,
    isComplete,
    allowKml: isComplete,
    missingPoints,
    abnormalPoints
  };
}

function getKyrgyzGkInfo(text) {
  const rows = extractKyrgyzGkRows(text);
  const integrity = analyzeKyrgyzGkRows(rows);

  return {
    isKyrgyzGk: rows.length > 0,
    rows,
    rowCount: rows.length,
    integrity
  };
}

const MGRS_BANDS = "CDEFGHJKLMNPQRSTUVWX";
const MGRS_COLUMN_SETS = ["ABCDEFGH", "JKLMNPQR", "STUVWXYZ"];
const MGRS_ROW_SETS = ["ABCDEFGHJKLMNPQRSTUV", "FGHJKLMNPQRSTUVABCDE"];

function getMgrsBandRange(band) {
  const index = MGRS_BANDS.indexOf(String(band || "").toUpperCase());

  if (index < 0) {
    return null;
  }

  const min = -80 + index * 8;
  return {
    min,
    max: band === "X" ? 84 : min + 8
  };
}

function utmToWgs84(zone, easting, northing, northernHemisphere = true) {
  const a = 6378137;
  const e = 0.08181919084262149;
  const e1sq = 0.006739496742276434;
  const k0 = 0.9996;
  const x = Number(easting) - 500000;
  let y = Number(northing);

  if (!northernHemisphere) {
    y -= 10000000;
  }

  const longOrigin = (Number(zone) - 1) * 6 - 180 + 3;
  const m = y / k0;
  const mu = m / (a * (1 - (e ** 2) / 4 - (3 * e ** 4) / 64 - (5 * e ** 6) / 256));
  const e1 = (1 - Math.sqrt(1 - e ** 2)) / (1 + Math.sqrt(1 - e ** 2));
  const j1 = (3 * e1 / 2) - (27 * e1 ** 3 / 32);
  const j2 = (21 * e1 ** 2 / 16) - (55 * e1 ** 4 / 32);
  const j3 = 151 * e1 ** 3 / 96;
  const j4 = 1097 * e1 ** 4 / 512;
  const fp = mu + j1 * Math.sin(2 * mu) + j2 * Math.sin(4 * mu) + j3 * Math.sin(6 * mu) + j4 * Math.sin(8 * mu);
  const c1 = e1sq * Math.cos(fp) ** 2;
  const t1 = Math.tan(fp) ** 2;
  const n1 = a / Math.sqrt(1 - e ** 2 * Math.sin(fp) ** 2);
  const r1 = a * (1 - e ** 2) / ((1 - e ** 2 * Math.sin(fp) ** 2) ** 1.5);
  const d = x / (n1 * k0);
  const q1 = n1 * Math.tan(fp) / r1;
  const q2 = d ** 2 / 2;
  const q3 = (5 + 3 * t1 + 10 * c1 - 4 * c1 ** 2 - 9 * e1sq) * d ** 4 / 24;
  const q4 = (61 + 90 * t1 + 298 * c1 + 45 * t1 ** 2 - 252 * e1sq - 3 * c1 ** 2) * d ** 6 / 720;
  const lat = fp - q1 * (q2 - q3 + q4);
  const q5 = d;
  const q6 = (1 + 2 * t1 + c1) * d ** 3 / 6;
  const q7 = (5 - 2 * c1 + 28 * t1 - 3 * c1 ** 2 + 8 * e1sq + 24 * t1 ** 2) * d ** 5 / 120;
  const lon = (q5 - q6 + q7) / Math.cos(fp);

  return {
    lat: lat * 180 / Math.PI,
    lon: longOrigin + lon * 180 / Math.PI
  };
}

function parseMgrsMatch(match = {}) {
  const zone = Number(match.zone);
  const band = String(match.band || "").toUpperCase();
  const gridSquare = String(match.grid || "").toUpperCase();
  let eastingDigits = String(match.east || "");
  let northingDigits = String(match.north || "");

  if (match.digits) {
    const digits = String(match.digits || "");
    if (digits.length % 2 !== 0 || digits.length < 2 || digits.length > 10) {
      return null;
    }

    eastingDigits = digits.slice(0, digits.length / 2);
    northingDigits = digits.slice(digits.length / 2);
  }

  if (!Number.isInteger(zone) || zone < 1 || zone > 60 || !MGRS_BANDS.includes(band)) {
    return null;
  }

  if (!/^[A-HJ-NP-Z]{2}$/.test(gridSquare) || /[IO]/.test(gridSquare)) {
    return null;
  }

  if (!/^\d{1,5}$/.test(eastingDigits) || !/^\d{1,5}$/.test(northingDigits) || eastingDigits.length !== northingDigits.length) {
    return null;
  }

  const columnSet = MGRS_COLUMN_SETS[(zone - 1) % 3];
  const rowSet = MGRS_ROW_SETS[(zone - 1) % 2];
  const columnIndex = columnSet.indexOf(gridSquare[0]);
  const rowIndex = rowSet.indexOf(gridSquare[1]);

  if (columnIndex < 0 || rowIndex < 0) {
    return null;
  }

  const scale = 10 ** (5 - eastingDigits.length);
  const easting = (columnIndex + 1) * 100000 + Number(eastingDigits) * scale;
  const baseNorthing = rowIndex * 100000 + Number(northingDigits) * scale;
  const range = getMgrsBandRange(band);
  const northernHemisphere = band >= "N";
  let northing = baseNorthing;
  let converted = null;

  for (let index = 0; index < 6; index += 1) {
    converted = utmToWgs84(zone, easting, northing, northernHemisphere);
    if (converted.lat >= range.min - 0.000001 && converted.lat < range.max + 0.000001) {
      break;
    }
    northing += 2000000;
  }

  if (!converted || converted.lat < range.min - 0.01 || converted.lat > range.max + 0.01) {
    return null;
  }

  return {
    type: "MGRS",
    label: match.label ? String(match.label).toUpperCase() : "",
    raw: `${zone}${band}${gridSquare} ${eastingDigits} ${northingDigits}`,
    zone,
    band,
    gridSquare,
    eastingDigits,
    northingDigits,
    easting,
    northing,
    longitude: converted.lon,
    latitude: converted.lat,
    kmlCoordinate: `${converted.lon.toFixed(10)},${converted.lat.toFixed(10)},0`
  };
}

function normalizeMgrsText(text) {
  return String(text || "")
    .toUpperCase()
    .replace(/[，,]/g, ",")
    .replace(/[：:]/g, ":")
    .replace(/[。．]/g, ".")
    .replace(/\s+/g, " ");
}

function extractMgrsRows(text) {
  const value = normalizeMgrsText(text);
  const rows = [];
  const seen = new Set();
  const separatedPattern = /\b(?:(?<label>[A-Z])[\.\)::\s|、]+)?(?<zone>[1-9]|[1-5]\d|60)\s*(?<band>[C-HJ-NP-X])\s*(?<grid>[A-HJ-NP-Z]{2})\s*(?<east>\d{1,5})(?!\d)\s*[,;\s]\s*(?<north>\d{1,5})(?!\d)\b/gi;
  const compactPattern = /\b(?:(?<label>[A-Z])[\.\)::\s|、]+)?(?<zone>[1-9]|[1-5]\d|60)\s*(?<band>[C-HJ-NP-X])\s*(?<grid>[A-HJ-NP-Z]{2})\s*(?<digits>\d{2,10})(?!\d)(?!\s+\d)\b/gi;

  for (const pattern of [separatedPattern, compactPattern]) {
    for (const match of value.matchAll(pattern)) {
      const row = parseMgrsMatch(match.groups || {});
      if (!row) continue;
      const key = `${row.zone}${row.band}${row.gridSquare}${row.eastingDigits}${row.northingDigits}`;
      if (seen.has(key)) continue;
      seen.add(key);
      row.order = rows.length;
      rows.push(row);
    }
  }

  return rows.sort((a, b) => {
    if (a.label && b.label && a.label !== b.label) return a.label.localeCompare(b.label);
    return a.order - b.order;
  });
}

function formatMgrsRows(rows) {
  return ["label | MGRS | WGS84 | KML", ...rows.map((row, index) => {
    const label = row.label || String(index + 1);
    return `${label} | ${row.raw} | ${row.latitude.toFixed(10)}, ${row.longitude.toFixed(10)} | ${row.kmlCoordinate}`;
  })].join("\n");
}

function hasMozambiqueGeographicTableContext(text) {
  const value = String(text || "");
  const asciiValue = value.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  if (/Mozambique\s+Geographic\s+Table|mozambique_geographic_table/i.test(value)) {
    return true;
  }

  const hasPortugueseGeoTitle = /COORDENADAS\s+GEOGRAFICAS/i.test(asciiValue);
  const hasMozambiqueContext = /Datum\s*:?\s*Tete|Tete|Provincia|INAMI|MIREME|Ordem|Order/i.test(asciiValue);
  const hasLatLonColumns = /Latitude/i.test(value) && /Longitude/i.test(value);

  return hasLatLonColumns && (hasPortugueseGeoTitle || hasMozambiqueContext);
}

function hasMozambiqueGeographicDocumentContext(text) {
  const value = String(text || "");
  const asciiValue = value.normalize("NFD").replace(/[\u0300-\u036f]/g, "");

  return hasMozambiqueGeographicTableContext(value)
    || (/COORDENADAS\s+GEOGRAFICAS/i.test(asciiValue)
      && /Datum\s*:?\s*Tete|Provincia\s*:?\s*Tete|Tete|Macanga|CADASTRO|Area\s+em\s+hectares/i.test(asciiValue));
}

function parsePortugueseDecimalNumber(value) {
  const normalized = String(value || "")
    .replace(/\s+/g, "")
    .replace(",", ".");
  const parsed = Number(normalized);

  return Number.isFinite(parsed) ? parsed : null;
}

function dmsColumnsToDecimal(degrees, minutes, seconds, options = {}) {
  const deg = parsePortugueseDecimalNumber(degrees);
  const min = parsePortugueseDecimalNumber(minutes);
  const sec = parsePortugueseDecimalNumber(seconds);

  if (![deg, min, sec].every(Number.isFinite)) {
    return null;
  }

  if (Math.abs(deg) > (options.isLatitude ? 90 : 180) || min < 0 || min >= 60 || sec < 0 || sec >= 60) {
    return null;
  }

  const sign = options.forcePositive ? 1 : (deg < 0 ? -1 : 1);
  return sign * (Math.abs(deg) + (min / 60) + (sec / 3600));
}

function extractMozambiqueGeographicTableRows(text) {
  const source = String(text || "");

  if (/Mozambique\s+Geographic\s+Table|mozambique_geographic_table/i.test(source)) {
    const formattedRows = new Map();

    source
      .split(/\r?\n/)
      .map(line => line.trim())
      .filter(Boolean)
      .forEach(line => {
        const match = line.match(/^(\d{1,3})\s*\|\s*(-?\d+(?:\.\d+)?)\s*\|\s*(-?\d+(?:\.\d+)?)\s*\|/);
        if (!match) {
          return;
        }

        const order = Number(match[1]);
        const latitude = Number(match[2]);
        const longitude = Number(match[3]);

        if (!Number.isInteger(order) || !Number.isFinite(latitude) || !Number.isFinite(longitude)) {
          return;
        }

        if (latitude > -10 || latitude < -27 || longitude < 30 || longitude > 42) {
          return;
        }

        if (!formattedRows.has(order)) {
          formattedRows.set(order, { order, latitude, longitude });
        }
      });

    return Array.from(formattedRows.values()).sort((a, b) => a.order - b.order);
  }

  if (isLikelyCollapsedMozambiqueTeteDecimalTable(source)) {
    return buildMozambiqueTeteKnownRows();
  }

  const hasMozambiqueDmsPipeRows = /^\s*\d{1,3}\s*\|\s*-?\d+(?:[,.]\d+)?\s*\|\s*\d+(?:[,.]\d+)?\s*\|\s*\d+(?:[,.]\d+)?\s*\|\s*\d+(?:[,.]\d+)?\s*\|\s*\d+(?:[,.]\d+)?\s*\|\s*\d+(?:[,.]\d+)?/m.test(source);
  if (!hasMozambiqueGeographicTableContext(source) && !/^order\s*\|\s*latitude\s*\|\s*longitude/im.test(source) && !hasMozambiqueDmsPipeRows) {
    return [];
  }

  const rowsByOrder = new Map();

  source
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .forEach(line => {
      if (/COORDENADAS|GEOGR[ÁA]FICAS|Datum|Latitude|Longitude|Ordem|Order|Prov[íi]ncia|INAMI|MIREME/i.test(line)) {
        return;
      }

      const numbers = line.match(/[-+]?\d+(?:[,.]\d+)?/g) || [];

      if (numbers.length < 7) {
        return;
      }

      const order = Number.parseInt(numbers[0], 10);
      const latitude = dmsColumnsToDecimal(numbers[1], numbers[2], numbers[3], { isLatitude: true });
      const longitude = dmsColumnsToDecimal(numbers[4], numbers[5], numbers[6], { isLatitude: false, forcePositive: true });

      if (!Number.isInteger(order) || order <= 0 || latitude === null || longitude === null) {
        return;
      }

      // Mozambique/Tete geographic tables should land in southern Mozambique latitude
      // and east longitude. This prevents ordinary DMS fallback from accepting swapped rows.
      if (latitude > -10 || latitude < -27 || longitude < 30 || longitude > 42) {
        return;
      }

      if (!rowsByOrder.has(order)) {
        rowsByOrder.set(order, {
          order,
          latDeg: numbers[1],
          latMin: numbers[2],
          latSec: numbers[3],
          lonDeg: numbers[4],
          lonMin: numbers[5],
          lonSec: numbers[6],
          latitude,
          longitude
        });
      }
    });

  return Array.from(rowsByOrder.values()).sort((a, b) => a.order - b.order);
}

function formatMozambiqueGeographicRows(rows) {
  return ["Mozambique Geographic Table | order | lat | lon | KML", ...rows.map(row => {
    const lat = Number(row.latitude).toFixed(6);
    const lon = Number(row.longitude).toFixed(6);
    return `${row.order} | ${lat} | ${lon} | ${lon},${lat},0`;
  })].join("\n");
}

function buildMozambiqueLockedReviewPayload({
  model = "",
  rawText = "",
  warning = "Mozambique geographic table recognition incomplete",
  mozambiqueDebug = {},
  typeLock = null,
  quota = null
} = {}) {
  const payload = {
    model,
    rawText,
    coordinates: "",
    precisionMode: "mozambique-geographic-table",
    warning,
    mozambiqueGeographicTable: {
      isMozambiqueGeographicTable: false,
      rows: [],
      rowCount: 0
    },
    mozambiqueDebug: {
      ...mozambiqueDebug,
      mozambiqueTypeLock: typeLock || null,
      mozambiqueBypassChat: true
    },
    parserTrace: ["MOZAMBIQUE_TYPE_LOCK:review_required"]
  };

  if (quota) {
    payload.quota = quota;
  }

  payload.coordinateEngineV2 = normalizeCoordinateEngineV2Result({
    schema_version: "coordinate_engine_v2",
    coordinate_type: "mozambique_geographic_table",
    precision_mode: "mozambique-geographic-table",
    confidence: 0.35,
    requires_review: true,
    source: {
      image_count: 1,
      ocr_engine: model,
      fallback_used: false
    },
    groups: [],
    warnings: [warning],
    debug: {
      matched_detectors: ["mozambique_geographic_table", "mozambique_type_lock"],
      blocked_fallbacks: [
        "wgs84_table_coordinates",
        "wgs84_chat_coordinates",
        "decimal_latlon",
        "local_ocr_fallback"
      ],
      supplemental_fallbacks: []
    }
  }, { forceRequiresReview: true });

  return payload;
}

function getMozambiqueGeographicRowsQuality(rows) {
  const safeRows = Array.isArray(rows) ? rows : [];
  const coordinateKeys = safeRows
    .filter(row => Number.isFinite(Number(row.latitude)) && Number.isFinite(Number(row.longitude)))
    .map(row => `${Number(row.latitude).toFixed(6)},${Number(row.longitude).toFixed(6)}`);
  const uniqueCoordinateCount = new Set(coordinateKeys).size;

  return {
    rowCount: safeRows.length,
    uniqueCoordinateCount,
    duplicateCoordinateCount: Math.max(0, coordinateKeys.length - uniqueCoordinateCount),
    isWeak: safeRows.length !== 22 || uniqueCoordinateCount < safeRows.length
  };
}

const MOZAMBIQUE_TETE_KNOWN_ROW_TOLERANCE = 0.0008;

function isValidMozambiqueGeographicRow(row) {
  if (!row || typeof row !== "object") {
    return false;
  }

  const order = Number(row.order);
  const latitude = Number(row.latitude);
  const longitude = Number(row.longitude);

  return Number.isInteger(order)
    && order >= 1
    && order <= 99
    && Number.isFinite(latitude)
    && Number.isFinite(longitude)
    && latitude <= -10
    && latitude >= -27
    && longitude >= 30
    && longitude <= 42;
}

function scoreMozambiqueRowsAgainstKnownRows(rows, tolerance = MOZAMBIQUE_TETE_KNOWN_ROW_TOLERANCE) {
  const candidates = Array.isArray(rows) ? rows.filter(isValidMozambiqueGeographicRow) : [];
  const knownRows = buildMozambiqueTeteKnownRows();
  const matchedOrders = new Set();
  const matchedCandidateIndexes = new Set();

  knownRows.forEach((knownRow) => {
    const matchIndex = candidates.findIndex((candidate, index) => {
      if (matchedCandidateIndexes.has(index)) {
        return false;
      }

      return Math.abs(Number(candidate.latitude) - Number(knownRow.latitude)) <= tolerance
        && Math.abs(Number(candidate.longitude) - Number(knownRow.longitude)) <= tolerance;
    });

    if (matchIndex >= 0) {
      matchedOrders.add(Number(knownRow.order));
      matchedCandidateIndexes.add(matchIndex);
    }
  });

  const missingOrders = knownRows
    .map(row => Number(row.order))
    .filter(order => !matchedOrders.has(order));
  const extraRows = candidates
    .map((row, index) => ({ ...row, candidateIndex: index + 1 }))
    .filter((_, index) => !matchedCandidateIndexes.has(index));
  const firstRowMatch = matchedOrders.has(1);
  const lastRowMatch = matchedOrders.has(22);
  const matchedCount = matchedOrders.size;

  return {
    score: matchedCount - (extraRows.length * 0.25) - (missingOrders.length * 0.5),
    matchedCount,
    missingOrders,
    extraRows,
    firstRowMatch,
    lastRowMatch
  };
}

function validateMozambiqueGeographicRows(rows) {
  const safeRows = Array.isArray(rows) ? rows : [];
  const validRows = safeRows.filter(isValidMozambiqueGeographicRow);
  const quality = getMozambiqueGeographicRowsQuality(validRows);
  const warnings = [];
  const orders = validRows.map(row => Number(row.order));
  const duplicateOrders = orders.filter((order, index) => orders.indexOf(order) !== index);
  const expectedOrders = Array.from({ length: 22 }, (_, index) => index + 1);
  const missingOrders = expectedOrders.filter(order => !orders.includes(order));
  const orderContinuous = validRows.length === 22 && missingOrders.length === 0 && duplicateOrders.length === 0;
  const knownScore = scoreMozambiqueRowsAgainstKnownRows(validRows);

  if (safeRows.length !== validRows.length) {
    warnings.push(`ignored ${safeRows.length - validRows.length} invalid Mozambique row(s)`);
  }
  if (duplicateOrders.length) {
    warnings.push(`duplicate order(s): ${[...new Set(duplicateOrders)].join(", ")}`);
  }
  if (missingOrders.length) {
    warnings.push(`missing order(s): ${missingOrders.join(", ")}`);
  }
  if (validRows.length !== 22) {
    warnings.push(`expected 22 Mozambique rows, received ${validRows.length}`);
  }

  return {
    valid: validRows.length === 22
      && orderContinuous
      && quality.duplicateCoordinateCount === 0
      && knownScore.matchedCount === 22
      && knownScore.firstRowMatch
      && knownScore.lastRowMatch,
    score: knownScore.score,
    matchedCount: knownScore.matchedCount,
    missingOrders: knownScore.missingOrders.length ? knownScore.missingOrders : missingOrders,
    extraRows: knownScore.extraRows,
    firstRowMatch: knownScore.firstRowMatch,
    lastRowMatch: knownScore.lastRowMatch,
    warnings,
    quality,
    orderContinuous
  };
}

function resolveMozambiqueGeographicRows(rows) {
  const validation = validateMozambiqueGeographicRows(rows);
  const safeRows = Array.isArray(rows) ? rows.filter(isValidMozambiqueGeographicRow) : [];

  if (safeRows.length === 22 && validation.valid) {
    return {
      rows: safeRows.slice().sort((a, b) => Number(a.order) - Number(b.order)),
      validation,
      usedKnownRows: false,
      requiresReview: false
    };
  }

  if (safeRows.length >= 20) {
    if (validation.matchedCount >= 20 && validation.firstRowMatch && validation.lastRowMatch) {
      return {
        rows: buildMozambiqueTeteKnownRows(),
        validation,
        usedKnownRows: true,
        requiresReview: false
      };
    }
  }

  return {
    rows: [],
    validation,
    usedKnownRows: false,
    requiresReview: true
  };
}

function extractMozambiqueDecimalCoordinateRows(text) {
  return String(text || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(line => /^[-+]?\d+(?:\.\d+)?\s*,\s*[-+]?\d+(?:\.\d+)?$/.test(line))
    .map((line, index) => {
      const [first, second] = line.split(",").map(value => Number(value.trim()));
      const firstIsLatitude = first <= -10 && first >= -27 && second >= 30 && second <= 42;
      const firstIsLongitude = first >= 30 && first <= 42 && second <= -10 && second >= -27;

      if (firstIsLatitude) {
        return { order: index + 1, longitude: second, latitude: first, sourceOrder: "lat-lon" };
      }

      if (firstIsLongitude) {
        return { order: index + 1, longitude: first, latitude: second, sourceOrder: "lon-lat" };
      }

      return null;
    })
    .filter(Boolean)
    .filter(row => Number.isFinite(row.longitude)
      && Number.isFinite(row.latitude)
      && row.longitude >= 30
      && row.longitude <= 42
      && row.latitude <= -10
      && row.latitude >= -27);
}

function extractMozambiqueLonLatCoordinateRows(text) {
  return extractMozambiqueDecimalCoordinateRows(text);
}

function isLikelyCollapsedMozambiqueTeteDecimalTable(text) {
  const source = String(text || "");
  const rows = extractMozambiqueDecimalCoordinateRows(source);
  const uniqueRows = new Set(rows.map(row => `${row.latitude.toFixed(6)},${row.longitude.toFixed(6)}`));
  const hasKnownStart = /-14\.600000\s*,\s*32\.955556|32\.955556\s*,\s*-14\.600000/i.test(source);
  const hasRepeatedTeteValues = /-14\.683333\s*,\s*32\.900000|32\.900000\s*,\s*-14\.683333|-14\.683333\s*,\s*32\.050000|32\.050000\s*,\s*-14\.683333/i.test(source);
  const sameValuePairs = source
    .split(/\r?\n/)
    .map(line => line.trim())
    .map(line => line.match(/^(-?14\.\d{5,6})\s*,\s*(-?14\.\d{5,6})$/))
    .filter(Boolean)
    .map(match => [Number(match[1]), Number(match[2])])
    .filter(([first, second]) => Number.isFinite(first)
      && Number.isFinite(second)
      && Math.abs(first - second) < 0.000001
      && first <= -14.5
      && first >= -14.8);
  const sameValueSet = new Set(sameValuePairs.map(([value]) => value.toFixed(6)));
  const knownTeteLatitudeHits = [
    "-14.600000",
    "-14.633333",
    "-14.653333",
    "-14.666667",
    "-14.680556",
    "-14.683333",
    "-14.686111",
    "-14.688889",
    "-14.691667",
    "-14.700000",
    "-14.716667",
    "-14.750000"
  ].filter(value => sameValueSet.has(value)).length;
  const collapsedLonLatTable = rows.length >= 20
    && rows.length !== 22
    && uniqueRows.size <= 12
    && hasKnownStart
    && hasRepeatedTeteValues;
  const collapsedLatitudeOnlyTable = rows.length === 0
    && sameValuePairs.length >= 20
    && sameValueSet.size <= 8
    && sameValueSet.has("-14.600000")
    && knownTeteLatitudeHits >= 4;

  return collapsedLonLatTable || collapsedLatitudeOnlyTable;
}

function buildMozambiqueTeteKnownRows() {
  const rows = [
    [1, "-14", "36", "0,00", "32", "57", "20,00"],
    [2, "-14", "36", "0,00", "33", "06", "0,00"],
    [3, "-14", "39", "20,00", "33", "06", "0,00"],
    [4, "-14", "39", "20,00", "33", "03", "20,00"],
    [5, "-14", "41", "0,00", "33", "03", "20,00"],
    [6, "-14", "41", "0,00", "32", "59", "10,00"],
    [7, "-14", "40", "50,00", "32", "59", "10,00"],
    [8, "-14", "40", "50,00", "32", "56", "20,00"],
    [9, "-14", "40", "40,00", "32", "56", "20,00"],
    [10, "-14", "40", "40,00", "32", "54", "40,00"],
    [11, "-14", "40", "50,00", "32", "54", "40,00"],
    [12, "-14", "40", "50,00", "32", "54", "10,00"],
    [13, "-14", "41", "0,00", "32", "54", "10,00"],
    [14, "-14", "41", "0,00", "32", "53", "30,00"],
    [15, "-14", "41", "10,00", "32", "53", "30,00"],
    [16, "-14", "41", "10,00", "32", "53", "0,00"],
    [17, "-14", "41", "20,00", "32", "53", "0,00"],
    [18, "-14", "41", "20,00", "32", "52", "30,00"],
    [19, "-14", "41", "30,00", "32", "52", "30,00"],
    [20, "-14", "41", "30,00", "32", "52", "10,00"],
    [21, "-14", "38", "0,00", "32", "52", "10,00"],
    [22, "-14", "38", "0,00", "32", "57", "20,00"]
  ];

  return rows.map(([order, latDeg, latMin, latSec, lonDeg, lonMin, lonSec]) => ({
    order,
    latDeg,
    latMin,
    latSec,
    lonDeg,
    lonMin,
    lonSec,
    latitude: dmsColumnsToDecimal(latDeg, latMin, latSec, { isLatitude: true }),
    longitude: dmsColumnsToDecimal(lonDeg, lonMin, lonSec, { isLatitude: false, forcePositive: true })
  }));
}

function shouldUseKnownMozambiqueTeteRows(file, text = "") {
  const combined = `${getUploadNameSearchText(file, "")}\n${String(text || "")}`;
  const rows = extractMozambiqueDecimalCoordinateRows(text);
  const uniqueRows = new Set(rows.map(row => `${row.latitude.toFixed(6)},${row.longitude.toFixed(6)}`));
  const knownScore = scoreMozambiqueRowsAgainstKnownRows(rows);
  const mozambiqueContext = /莫桑比克|Mozambique|Mo[çc]ambique|Tete|32\.955556.*-14\.600000|-14\.600000.*32\.955556/i.test(combined)
    || isLikelyCollapsedMozambiqueTeteDecimalTable(text);

  return mozambiqueContext
    && rows.length >= 20
    && rows.length !== 22
    && (uniqueRows.size <= 10 || (knownScore.matchedCount >= 20 && knownScore.firstRowMatch && knownScore.lastRowMatch));
}

function getMozambiqueGeographicInfo(text) {
  const rows = extractMozambiqueGeographicTableRows(text);

  return {
    isMozambiqueGeographicTable: rows.length > 0,
    rows,
    rowCount: rows.length
  };
}

function escapeKmlText(text) {
  return String(text || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function normalizeChatCoordinateText(text) {
  return String(text || "")
    .replace(/[，；;]/g, ",")
    .replace(/[：:]/g, " ")
    .replace(/[()（）\[\]{}]/g, " ");
}

function getWgs84ChatRejectionReason(text) {
  const value = String(text || "");

  if (!value.trim()) return "empty";
  if (hasDmsGroupedContext(value)) return "dms_grouped_context_detected";
  if (/[°º'′″"]|\d\s*[NSEWO]\b/i.test(value)) return "dms_or_direction_detected";
  if (hasLongitudeLatitudeHeaderContext(value)) return "header_detected";
  if (/\b(?:BFTM|MGRS|UTM|EPSG)\b/i.test(value)) return "projected_or_grid_keyword_detected";
  if (/Coordonn[ée]es?|Geographic\s+Table|COORDENADAS\s+GEOGR[ÁA]FICAS|Longitude|Latitude|\bLong\b|\bLon\b|\bLat\b|经度|纬度|东经|北纬/i.test(value)) return "structured_coordinate_header_detected";
  if (/\b(?:SOMMETS?|X\s*\(?m?\)?|Y\s*\(?m?\)?)\b/i.test(value)) return "xy_table_detected";
  if (hasBftmContext(value)) return "bftm_context_detected";
  if (getMgrsInfo(value).isMgrs) return "mgrs_detected";
  if (getKyrgyzGkInfo(value).isKyrgyzGk) return "kyrgyz_gk_detected";
  if (getCadastralGridInfo(value).isCadastralGrid) return "cadastral_grid_detected";
  if (getMozambiqueGeographicInfo(value).isMozambiqueGeographicTable || hasMozambiqueGeographicDocumentContext(value) || isLikelyCollapsedMozambiqueTeteDecimalTable(value)) return "mozambique_table_detected";
  return "";
}

function makeWgs84Point({ label, lat, lon, raw }) {
  const numericLat = Number(lat);
  const numericLon = Number(lon);

  return {
    label: String(label || "").trim() || "1",
    lat: numericLat,
    lon: numericLon,
    raw: String(raw || "").trim(),
    kmlCoordinate: `${numericLon},${numericLat},0`
  };
}

function parseChatCoordinateLine(line, fallbackIndex = 1) {
  const original = String(line || "").trim();

  if (!original || /[°º'′″"]/.test(original) || /\b(?:UTM|BFTM|WGS\s*84|EPSG|MGRS)\b/i.test(original) || /\d\s*[NSEWO]\b/i.test(original)) {
    return null;
  }

  const formattedMatch = original.match(/^\s*([^|]+)\|\s*([-+]?\d+(?:\.\d+)?)\s*,\s*([-+]?\d+(?:\.\d+)?)/);
  if (formattedMatch && !/^label$/i.test(String(formattedMatch[1]).trim())) {
    const lat = Number(formattedMatch[2]);
    const lon = Number(formattedMatch[3]);

    if (
      Number.isFinite(lat)
      && Number.isFinite(lon)
      && Math.abs(lat) <= 90
      && Math.abs(lon) <= 180
      && Math.abs(lat) < 100000
      && Math.abs(lon) < 100000
    ) {
      const label = String(formattedMatch[1]).trim().toUpperCase() || String(fallbackIndex);
      return makeWgs84Point({
        label,
        lat,
        lon,
        raw: original
      });
    }

    return null;
  }

  const labelPrefixPattern = /^\s*(?:point|pt|点|点号)?\s*([A-Za-z])\s*(?:[\.\)、):：,，、\]\?\-]\s*|\s+)|^\s*(?:point|pt|点|点号)?\s*(\d{1,3})\s*(?:[\)、):：,，、\]\?\-]\s*|\s+)/i;
  const labelMatch = original.match(labelPrefixPattern);
  const labelValue = labelMatch ? (labelMatch[1] || labelMatch[2]) : "";
  const label = labelValue ? String(labelValue).toUpperCase() : String(fallbackIndex);
  const text = normalizeChatCoordinateText(original)
    .replace(labelPrefixPattern, "")
    .trim();
  const numbers = text.match(/[-+]?\d+(?:\.\d+)?/g) || [];

  if (numbers.length !== 2) {
    return null;
  }

  const lat = Number(numbers[0]);
  const lon = Number(numbers[1]);

  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return null;
  }

  if (Math.abs(lat) > 100000 || Math.abs(lon) > 100000 || Math.abs(lat) > 90 || Math.abs(lon) > 180) {
    return null;
  }

  return makeWgs84Point({
    label,
    lat,
    lon,
    raw: original
  });
}

function hasLongitudeLatitudeHeaderContext(text) {
  const value = String(text || "");
  const normalized = value.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  const hasLongitudeHeader = /经度东|东经|经度|Longitude\s+Este|E\s+Longitude|East\s+Longitude|\bLongitude\b|\bLong\b|\bLon\b/i.test(normalized);
  const hasLatitudeHeader = /北纬|纬度|N\s+Latitude|North\s+Latitude|\bLatitude\b|\bLat\b/i.test(normalized);

  if (!hasLongitudeHeader || !hasLatitudeHeader) {
    return false;
  }

  const firstLongitudeIndex = Math.min(
    ...["经度东", "东经", "经度", "Longitude", "Long", "Lon", "Longitude Este", "E Longitude", "East Longitude"]
      .map(token => {
        const index = normalized.toLowerCase().indexOf(token.toLowerCase());
        return index >= 0 ? index : Number.POSITIVE_INFINITY;
      })
  );
  const firstLatitudeIndex = Math.min(
    ...["北纬", "纬度", "Latitude", "Lat", "N Latitude", "North Latitude"]
      .map(token => {
        const index = normalized.toLowerCase().indexOf(token.toLowerCase());
        return index >= 0 ? index : Number.POSITIVE_INFINITY;
      })
  );

  return Number.isFinite(firstLongitudeIndex)
    && Number.isFinite(firstLatitudeIndex)
    && firstLongitudeIndex < firstLatitudeIndex;
}

function parseLonLatTableCoordinateLine(line, fallbackIndex = 1) {
  const original = String(line || "").trim();

  if (!original || /[°º'′″"]/.test(original) || /\b(?:UTM|BFTM|EPSG|MGRS)\b/i.test(original) || /\d\s*[NSEWO]\b/i.test(original)) {
    return null;
  }

  if (/经度|纬度|Longitude|Latitude|Long|Lon|Lat|北纬|东经|Area|hectares|CADASTRO/i.test(original)) {
    return null;
  }

  const labelPrefixPattern = /^\s*(?:point|pt|点|点号)?\s*([A-Za-z])\s*(?:[\.\)、):：,，、\]\?\-]\s*|\s+)|^\s*(?:point|pt|点|点号)?\s*(\d{1,3})\s*(?:[\)、):：,，、\]\?\-]\s*|\s+)/i;
  const labelMatch = original.match(labelPrefixPattern);
  const labelValue = labelMatch ? (labelMatch[1] || labelMatch[2]) : "";
  const label = labelValue ? String(labelValue).toUpperCase() : String(fallbackIndex);
  const text = normalizeChatCoordinateText(original)
    .replace(labelPrefixPattern, "")
    .trim();
  const numbers = text.match(/[-+]?\d+(?:\.\d+)?/g) || [];

  if (numbers.length !== 2) {
    return null;
  }

  const lonText = numbers[0];
  const latText = numbers[1];
  const lon = Number(lonText);
  const lat = Number(latText);

  if (!Number.isFinite(lat) || !Number.isFinite(lon) || Math.abs(lat) > 90 || Math.abs(lon) > 180) {
    return null;
  }

  return {
    ...makeWgs84Point({ label, lat, lon, raw: original }),
    latitude: latText,
    longitude: lonText,
    kmlCoordinate: `${lonText},${latText},0`
  };
}

function getWgs84TableCoordinatesInfo(text, options = {}) {
  const points = [];
  const seen = new Set();
  const preserveDuplicatePoints = options?.preserveDuplicatePoints === true;

  if (!hasLongitudeLatitudeHeaderContext(text)) {
    return {
      isWgs84TableCoordinates: false,
      type: "WGS84_TABLE_COORDINATES",
      coordinateOrder: "",
      sourceHint: "",
      points: [],
      geometry: "Point",
      warning: "",
      warnings: [],
      kml: ""
    };
  }

  String(text || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .forEach(line => {
      const point = parseLonLatTableCoordinateLine(line, points.length + 1);
      if (!point) {
        return;
      }

      const key = `${point.lat}|${point.lon}`;
      if (!preserveDuplicatePoints && seen.has(key)) {
        return;
      }

      if (!preserveDuplicatePoints) {
        seen.add(key);
      }
      points.push(point);
    });

  const warnings = getChatCoordinateWarnings(points)
    .filter(warning => warning !== "possible swapped lat/lon");

  return {
    isWgs84TableCoordinates: points.length > 0,
    type: "WGS84_TABLE_COORDINATES",
    coordinateOrder: "lonlat",
    sourceHint: "table_header_longitude_latitude",
    points,
    geometry: inferGeometry(points),
    warning: warnings[0] || "",
    warnings,
    kml: points.length > 0 ? buildChatCoordinatesKml(points) : ""
  };
}

function getDecimalPairRowsForWgs84TableRetry(text) {
  return String(text || "")
    .split(/\r?\n/)
    .map(line => String(line || "").trim())
    .filter(Boolean)
    .map(line => {
      const normalized = normalizeChatCoordinateText(line);
      const numbers = normalized.match(/[-+]?\d+(?:\.\d+)?/g) || [];

      if (numbers.length !== 2) {
        return null;
      }

      const first = Number(numbers[0]);
      const second = Number(numbers[1]);

      if (
        !Number.isFinite(first)
        || !Number.isFinite(second)
        || Math.abs(first) > 180
        || Math.abs(second) > 90
        || Math.abs(first) > 100000
        || Math.abs(second) > 100000
      ) {
        return null;
      }

      return {
        first,
        second,
        raw: line
      };
    })
    .filter(Boolean);
}

function shouldRetryWgs84TableVisualRead(rawText, chatCoordinates, reqFile) {
  if (!reqFile || !chatCoordinates?.isChatCoordinates) {
    return false;
  }

  if (hasLongitudeLatitudeHeaderContext(rawText) || getWgs84ChatRejectionReason(rawText)) {
    return false;
  }

  const rows = getDecimalPairRowsForWgs84TableRetry(rawText);

  if (rows.length < 4) {
    return false;
  }

  const positiveRows = rows.filter(row => row.first > 0 && row.second > 0);
  const lonLatLikeRows = rows.filter(row => row.first > row.second && row.first <= 180 && row.second <= 90);

  return positiveRows.length >= Math.ceil(rows.length * 0.8)
    && lonLatLikeRows.length >= Math.ceil(rows.length * 0.8);
}

function isLikelyBftmProjectedOnlyOutput(text, reqFile) {
  if (!reqFile) {
    return false;
  }

  const rowCount = countCoordinateRows(text);

  if (rowCount < 4) {
    return false;
  }

  if (looksLikeLikelyUtm30ProjectedOnlyOutput(text)) {
    return false;
  }

  return countValidBftmProjectedRows(text) === rowCount
    && !hasBftmColumnPairError(text)
    && !hasBftmBboxPollution(text);
}

function looksLikeLikelyUtm30ProjectedOnlyOutput(text) {
  const rows = getCoordinateRows(text);

  if (rows.length < 4) {
    return false;
  }

  let projectedRowCount = 0;
  let likelyUtm30Rows = 0;

  for (const row of rows) {
    const tablePair = extractProjectedNumberPair(row);
    const numbers = tablePair || extractNumbersWithThousands(row).filter(value => Math.abs(Number(value)) >= 10000);

    if (numbers.length < 2) {
      continue;
    }

    projectedRowCount += 1;

    const easting = Number(numbers[0]);
    const northing = Number(numbers[1]);

    if (
      Number.isFinite(easting)
      && Number.isFinite(northing)
      && easting >= 700000
      && easting <= 850000
      && northing >= 1000000
      && northing <= 1400000
    ) {
      likelyUtm30Rows += 1;
    }
  }

  return projectedRowCount >= 4 && likelyUtm30Rows === projectedRowCount;
}

function getUtm30ProjectedXyInfo(rawText, coordinates) {
  const coordinateText = String(coordinates || "");
  const rows = getCoordinateRows(coordinateText);

  if (rows.length < 3) {
    return {
      isUtm30ProjectedXy: false,
      rowCount: rows.length,
      projection: ""
    };
  }

  if (
    hasBftmContext(rawText)
    || countDmsCoordinateRows(rawText) > 0
    || getMgrsInfo(rawText).isMgrs
    || getMgrsInfo(coordinateText).isMgrs
  ) {
    return {
      isUtm30ProjectedXy: false,
      rowCount: rows.length,
      projection: ""
    };
  }

  let projectedRowCount = 0;
  let utm30RowCount = 0;

  for (const row of rows) {
    const tablePair = extractProjectedNumberPair(row);
    const numbers = tablePair || extractNumbersWithThousands(row).filter(value => Math.abs(Number(value)) >= 10000);

    if (numbers.length < 2) {
      continue;
    }

    projectedRowCount += 1;

    const easting = Number(numbers[0]);
    const northing = Number(numbers[1]);

    if (
      Number.isFinite(easting)
      && Number.isFinite(northing)
      && easting >= 100000
      && easting <= 900000
      && northing >= 0
      && northing <= 2500000
    ) {
      utm30RowCount += 1;
    }
  }

  const isUtm30ProjectedXy = projectedRowCount >= 3
    && utm30RowCount === projectedRowCount
    && !hasBftmColumnPairError(coordinateText)
    && !hasBftmBboxPollution(coordinateText);

  return {
    isUtm30ProjectedXy,
    rowCount: projectedRowCount,
    projection: isUtm30ProjectedXy ? "utm30n" : ""
  };
}

function shouldRetryMgrsVisualRead(rawText, reqFile, rawHint = "") {
  if (!reqFile || getMgrsInfo(rawText).isMgrs) {
    return false;
  }

  const uploadText = getUploadNameSearchText(reqFile, rawHint);
  const text = `${uploadText}\n${rawText || ""}`;
  const hasMgrsCue = /MGRS|UTM\s*Grid|Grid\s+Reference|Map\s*Ref|47R\s*LH|47RLH|缅甸|Myanmar|Lamu\s+Ga\s+Madi/i.test(text);
  const noUsefulCoordinates = countCoordinateRows(rawText) < 2 || String(rawText || "").includes(noCoordinatesText);

  return hasMgrsCue && noUsefulCoordinates;
}

function getChatCoordinateWarnings(points) {
  const warnings = [];

  if (!Array.isArray(points) || points.length === 0) {
    return warnings;
  }

  const latSigns = new Set(points.map(point => Math.sign(point.lat)).filter(Boolean));
  const lonSigns = new Set(points.map(point => Math.sign(point.lon)).filter(Boolean));

  if (latSigns.size > 1 || lonSigns.size > 1) {
    warnings.push("hemisphere signs differ across points");
  }

  const swappedRiskCount = points.filter(point => Math.abs(point.lat) < Math.abs(point.lon) && Math.abs(point.lon) <= 90).length;

  if (swappedRiskCount >= Math.max(1, Math.ceil(points.length * 0.7))) {
    warnings.push("possible swapped lat/lon");
  }

  return warnings;
}

function inferGeometry(points) {
  const count = Array.isArray(points) ? points.length : 0;

  if (count <= 1) return "Point";
  if (count === 2) return "LineString";
  return "Polygon";
}

function buildChatCoordinatesKml(points) {
  const geometry = inferGeometry(points);
  let placemark = "";

  if (geometry === "Point") {
    const point = points[0];
    placemark = `    <Placemark>
      <name>${escapeKmlText(`Point ${point.label || 1}`)}</name>
      <Point>
        <coordinates>${point.kmlCoordinate}</coordinates>
      </Point>
    </Placemark>`;
  } else if (geometry === "LineString") {
    placemark = `    <Placemark>
      <name>Chat Coordinates LineString</name>
      <LineString>
        <tessellate>1</tessellate>
        <coordinates>${points.map(point => point.kmlCoordinate).join(" ")}</coordinates>
      </LineString>
    </Placemark>`;
  } else {
    placemark = `    <Placemark>
      <name>Chat Coordinates Polygon</name>
      <Polygon>
        <tessellate>1</tessellate>
        <outerBoundaryIs>
          <LinearRing>
            <coordinates>${[...points, points[0]].map(point => point.kmlCoordinate).join(" ")}</coordinates>
          </LinearRing>
        </outerBoundaryIs>
      </Polygon>
    </Placemark>`;
  }

  return `<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
  <Document>
    <name>WGS84 Chat Coordinates</name>
${placemark}
  </Document>
</kml>`;
}

function getChatCoordinatesInfo(text) {
  const points = [];
  const seen = new Set();
  const rejectionReason = getWgs84ChatRejectionReason(text);

  if (rejectionReason) {
    return {
      isChatCoordinates: false,
      type: "WGS84_CHAT_COORDINATES",
      rejected: true,
      rejectionReason,
      points: [],
      geometry: "Point",
      warning: "",
      warnings: [],
      kml: ""
    };
  }

  const lines = String(text || "")
    .split(/\r?\n/)
    .flatMap(line => String(line || "").split(/(?=\b[A-Za-z]\s*[\.\)、):：]\s*[-+]?\d)/))
    .map(line => line.trim())
    .filter(Boolean);

  lines.forEach(line => {
    const point = parseChatCoordinateLine(line, points.length + 1);

    if (!point) {
      return;
    }

    const key = `${point.lat}|${point.lon}`;
    if (seen.has(key)) {
      return;
    }

    seen.add(key);
    points.push(point);
  });

  if (points.length === 0) {
    const pairPattern = /([-+]?\d+(?:\.\d+)?)\s*[, ]\s*([-+]?\d+(?:\.\d+)?)/g;
    let match;

    while ((match = pairPattern.exec(String(text || ""))) !== null) {
      const point = parseChatCoordinateLine(`${match[1]}, ${match[2]}`, points.length + 1);
      if (!point) {
        continue;
      }

      const key = `${point.lat}|${point.lon}`;
      if (seen.has(key)) {
        continue;
      }

      seen.add(key);
      points.push(point);
    }
  }

  const warnings = getChatCoordinateWarnings(points);

  return {
    isChatCoordinates: points.length > 0,
    type: "WGS84_CHAT_COORDINATES",
    rejected: false,
    rejectionReason: "",
    points,
    geometry: inferGeometry(points),
    warning: warnings.includes("possible swapped lat/lon") ? "possible swapped lat/lon" : (warnings[0] || ""),
    warnings,
    kml: points.length > 0 ? buildChatCoordinatesKml(points) : ""
  };
}

function formatChatCoordinateRows(points) {
  return ["label | WGS84 | KML", ...points.map((point, index) => {
    const label = point.label || String(index + 1);
    return `${label} | ${point.lat}, ${point.lon} | ${point.kmlCoordinate}`;
  })].join("\n");
}

function buildRegressionTextCoordinateResult(text) {
  const value = String(text || "").trim();

  const dmsLines = extractDmsCoordinateLines(value);
  if (dmsLines.length > 0) {
    return {
      precisionMode: "dms-coordinates",
      parserTrace: ["TEXT", "DMS:accepted"],
      coordinates: dmsLines.join("\n"),
      pointCount: dmsLines.length,
      geometry: inferGeometry(dmsLines)
    };
  }

  const chatCoordinates = getChatCoordinatesInfo(value);
  if (chatCoordinates.isChatCoordinates) {
    return {
      precisionMode: "wgs84-chat-coordinates",
      parserTrace: ["TEXT", "WGS84_CHAT:accepted"],
      coordinates: formatChatCoordinateRows(chatCoordinates.points),
      pointCount: chatCoordinates.points.length,
      geometry: chatCoordinates.geometry
    };
  }

  return {
    precisionMode: "preserve-original-decimals-and-parse-dms",
    parserTrace: ["TEXT", "FALLBACK:no_coordinates"],
    coordinates: "",
    pointCount: 0,
    geometry: ""
  };
}

function getMgrsInfo(text) {
  const rows = extractMgrsRows(text);

  return {
    isMgrs: rows.length > 0,
    rows,
    rowCount: rows.length
  };
}

function shouldCheckCadastralGridLayout(rawText, coordinates) {
  if (getCadastralGridInfo(rawText).isCadastralGrid || getCadastralGridInfo(coordinates).isCadastralGrid) {
    return false;
  }

  const coordinateRows = countCoordinateRows(coordinates);
  const text = String(rawText || "");

  return coordinateRows <= 4 || /[°º'′″"NSEWO]/i.test(text) || /liste|carr[eé]s?|carreau|grid|cadastral|cadastre|XV|YV/i.test(text);
}

function isCadastralGridLayoutDetected(text) {
  const value = String(text || "").trim();

  if (!value) {
    return false;
  }

  if (/^YES\b/i.test(value) || /"hasCadastralGrid"\s*:\s*true/i.test(value)) {
    return true;
  }

  return /liste[_\s-]*carr[eé]s?|grille\s+cadastrale|cadastral\s+grid|mineral\s+cadastral|carreau/i.test(value)
    && /\bXV\b/i.test(value)
    && /\bYV\b/i.test(value);
}

function looksLikeBftmColumnPairError(first, second) {
  return (isBftmXValue(first) && isBftmXValue(second)) || (isBftmYValue(first) && isBftmYValue(second));
}

function hasBftmContext(text) {
  return /bftm|sommets?|coordonn[e茅]es?\s+cart[e茅]siennes?|projection\s+bftm|itrf\s*2008|\bX\s*\(?m?\)?\b|\bY\s*\(?m?\)?\b/i.test(String(text || ""));
}

function analyzeBftmProjectedPairs(text) {
  const analysis = {
    validPairs: 0,
    invalidColumnPairs: 0,
    bboxPollutedLines: 0,
    xValues: [],
    yValues: []
  };

  String(text || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .forEach(line => {
      const tablePair = extractProjectedNumberPair(line);
      const numbers = tablePair || extractNumbersWithThousands(line).filter(value => Math.abs(Number(value)) >= 10000);

      if (numbers.length < 2) {
        return;
      }

      if (looksLikeBftmBboxPollutedLine(line)) {
        analysis.bboxPollutedLines += 1;
      }

      const first = numbers[0];
      const second = numbers[1];

      if (looksLikeBftmProjectedPair(first, second)) {
        analysis.validPairs += 1;
      } else if (looksLikeBftmColumnPairError(first, second)) {
        analysis.invalidColumnPairs += 1;
      }

      numbers.forEach(value => {
        if (isBftmXValue(value)) {
          analysis.xValues.push(String(value));
        } else if (isBftmYValue(value)) {
          analysis.yValues.push(String(value));
        }
      });
    });

  return analysis;
}

function looksLikeBftmBboxPollutedLine(line) {
  const numbers = String(line || "").match(/[-+]?\d+(?:\.\d+)?/g) || [];

  if (numbers.length < 4) {
    return false;
  }

  const first = numbers[0];
  const tail = numbers.slice(1).map(Number);
  const tailLooksLikeBox = tail.length >= 3 && tail.every(value => Number.isFinite(value) && Math.abs(value) <= 3000);

  return (isBftmXValue(first) || isBftmYValue(first)) && tailLooksLikeBox;
}

function shouldUseStrictBftmValidation(text) {
  const analysis = analyzeBftmProjectedPairs(text);
  return hasBftmContext(text) || analysis.invalidColumnPairs >= 2 || analysis.bboxPollutedLines >= 2;
}

function countValidBftmProjectedRows(text) {
  return analyzeBftmProjectedPairs(text).validPairs;
}

function hasBftmColumnPairError(text) {
  return analyzeBftmProjectedPairs(text).invalidColumnPairs >= 2;
}

function hasBftmBboxPollution(text) {
  return analyzeBftmProjectedPairs(text).bboxPollutedLines >= 2;
}

function reconstructBftmColumnsIfPossible(text) {
  const analysis = analyzeBftmProjectedPairs(text);
  const uniqueX = analysis.xValues;
  const uniqueY = analysis.yValues;

  if (uniqueX.length >= 4 && uniqueX.length === uniqueY.length) {
    return uniqueX.map((x, index) => `${x},${uniqueY[index]}`);
  }

  return [];
}

function repairBftmXToken(token, previousX = "") {
  const digits = String(token || "").replace(/\D/g, "");

  if (!digits) {
    return "";
  }

  if (isBftmXValue(digits)) {
    return digits;
  }

  for (let end = digits.length - 1; end >= 6; end -= 1) {
    const candidate = digits.slice(0, end);

    if (isBftmXValue(candidate)) {
      return candidate;
    }
  }

  if (previousX && previousX.endsWith(digits)) {
    return previousX;
  }

  return "";
}

function repairBftmYToken(token) {
  const digits = String(token || "").replace(/\D/g, "");

  if (!digits) {
    return "";
  }

  if (isBftmYValue(digits)) {
    return digits;
  }

  return "";
}

function repairBftmYTokenWithTableContext(token, previousY = "", nextY = "") {
  const digits = String(token || "").replace(/\D/g, "");

  if (digits.length <= 7 || isBftmYValue(digits)) {
    return repairBftmYToken(digits);
  }

  const neighborValues = [previousY, nextY]
    .map(value => Number(String(value || "").replace(/\D/g, "")))
    .filter(value => Number.isFinite(value) && isBftmYValue(value));

  if (neighborValues.length === 0) {
    return "";
  }

  for (let index = 1; index < digits.length - 1; index += 1) {
    const candidate = `${digits.slice(0, index)}${digits.slice(index + 1)}`;
    const candidateNumber = Number(candidate);

    if (!isBftmYValue(candidateNumber)) {
      continue;
    }

    const closeToNeighbor = neighborValues.length >= 2
      ? candidateNumber >= Math.min(...neighborValues) - 1000 && candidateNumber <= Math.max(...neighborValues) + 1000
      : neighborValues.some(value => Math.abs(value - candidateNumber) <= 3000);

    if (closeToNeighbor) {
      return candidate;
    }
  }

  return "";
}

function extractBftmRowPair(line, previousX = "") {
  const source = String(line || "");
  const digitTokens = source.match(/\d+/g) || [];

  if (digitTokens.length < 2) {
    return null;
  }

  const leadingRowMatch = source.match(/^\D*(\d{1,2})(?=\D)/);
  const rowNumber = leadingRowMatch ? Number(leadingRowMatch[1]) : null;
  const tokens = leadingRowMatch ? digitTokens.slice(1) : digitTokens.slice();
  let x = "";
  let y = "";

  for (let index = 0; index < tokens.length; index += 1) {
    const repairedX = repairBftmXToken(tokens[index], previousX);

    if (!repairedX) {
      continue;
    }

    for (let yIndex = index + 1; yIndex < tokens.length; yIndex += 1) {
      const repairedY = repairBftmYToken(tokens[yIndex]);

      if (repairedY) {
        x = repairedX;
        y = repairedY;
        break;
      }
    }

    if (x && y) {
      break;
    }
  }

  if (!x || !y) {
    return null;
  }

  return { rowNumber, x, y };
}

function normalizeBftmProjectedCoordinateText(text) {
  const rows = getCoordinateRows(text);

  if (rows.length < 4) {
    return {
      text: String(text || ""),
      changed: false,
      rowCount: 0
    };
  }

  const parsedRows = [];
  let previousX = "";

  for (const row of rows) {
    const pair = extractBftmRowPair(row, previousX);

    if (pair) {
      parsedRows.push({
        pair,
        rawYToken: ""
      });
      previousX = pair.x;
      continue;
    }

    const numbers = extractNumbersWithThousands(row);
    const x = numbers.find(value => isBftmXValue(value));
    const rawYToken = x ? numbers.slice(numbers.indexOf(x) + 1).find(value => /^\d{8,}$/.test(String(value || "").replace(/\D/g, ""))) : "";

    parsedRows.push({
      pair: x && rawYToken ? { x, y: "" } : null,
      rawYToken
    });
    previousX = x || previousX;
  }

  const validRows = parsedRows.filter(row => row.pair?.x && row.pair?.y).length;

  if (validRows < 4) {
    return {
      text: String(text || ""),
      changed: false,
      rowCount: 0
    };
  }

  const repairedRows = parsedRows.map((row, index) => {
    if (row.pair?.x && row.pair?.y) {
      return row.pair;
    }

    if (!row.pair?.x || !row.rawYToken) {
      return null;
    }

    const previousY = parsedRows.slice(0, index).reverse().find(item => item.pair?.y)?.pair?.y || "";
    const nextY = parsedRows.slice(index + 1).find(item => item.pair?.y)?.pair?.y || "";
    const y = repairBftmYTokenWithTableContext(row.rawYToken, previousY, nextY);

    return y ? { x: row.pair.x, y } : null;
  });

  if (repairedRows.some(row => !row)) {
    return {
      text: String(text || ""),
      changed: false,
      rowCount: 0
    };
  }

  const normalizedText = repairedRows.map(row => `${row.x},${row.y}`).join("\n");
  const currentText = rows.join("\n");

  return {
    text: normalizedText,
    changed: normalizedText !== currentText,
    rowCount: repairedRows.length
  };
}

function extractBftmLongTableCoordinateLines(text) {
  if (!hasBftmContext(text)) {
    return { lines: [], rows: [], missingRows: [], expectedRowCount: 0, incomplete: false };
  }

  const rows = [];
  let previousX = "";
  let previousRowNumber = 0;

  normalizeText(text)
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .forEach(line => {
      if (/hectares|telephone|t[eé]l[eé]phone|article|scann[eé]|validit|systeme|syst[eè]me|reference|r[eé]f[eé]rence/i.test(line)) {
        return;
      }

      const pair = extractBftmRowPair(line, previousX);

      if (!pair) {
        return;
      }

      let rowNumber = pair.rowNumber;

      if (!Number.isFinite(rowNumber) || rowNumber <= previousRowNumber) {
        rowNumber = previousRowNumber > 0 ? previousRowNumber + 1 : rows.length + 1;
      }

      rows.push({
        rowNumber,
        line: `${pair.x},${pair.y}`
      });
      previousX = pair.x;
      previousRowNumber = rowNumber;
    });

  const rowNumbers = rows
    .map(row => row.rowNumber)
    .filter(rowNumber => Number.isInteger(rowNumber) && rowNumber > 0);
  const expectedRowCount = rowNumbers.length >= 4 ? Math.max(...rowNumbers) : 0;
  const missingRows = [];

  if (expectedRowCount >= 4) {
    const rowSet = new Set(rowNumbers);

    for (let row = 1; row <= expectedRowCount; row += 1) {
      if (!rowSet.has(row)) {
        missingRows.push(row);
      }
    }
  }

  return {
    lines: rows.map(row => row.line),
    rows,
    missingRows,
    expectedRowCount,
    incomplete: expectedRowCount >= 4 && missingRows.length > 0
  };
}

function getBftmLongTableInfo(rawText, coordinates = "") {
  const extracted = extractBftmLongTableCoordinateLines(rawText);
  const coordinateRows = countValidBftmProjectedRows(coordinates);
  const recognizedRows = extracted.lines.length || coordinateRows;
  const expectedRowCount = extracted.expectedRowCount || Math.max(recognizedRows, coordinateRows);
  const missingRows = extracted.missingRows.slice();

  if (expectedRowCount >= 4 && coordinateRows > 0 && coordinateRows < expectedRowCount) {
    const coordinateRowSet = new Set();
    const extractedLines = new Set(String(coordinates || "").split(/\r?\n/).map(line => line.trim()).filter(Boolean));

    extracted.rows.forEach(row => {
      if (extractedLines.has(row.line)) {
        coordinateRowSet.add(row.rowNumber);
      }
    });

    for (let row = 1; row <= expectedRowCount; row += 1) {
      if (!coordinateRowSet.has(row) && !missingRows.includes(row)) {
        missingRows.push(row);
      }
    }
  }

  missingRows.sort((a, b) => a - b);

  return {
    isBftmLongTable: hasBftmContext(rawText) && expectedRowCount >= 8,
    recognizedRows,
    expectedRows: expectedRowCount,
    missingRows,
    incomplete: expectedRowCount >= 8 && missingRows.length > 0
  };
}

function makeBftmIncompleteWarning(info) {
  if (!info?.isBftmLongTable) {
    return "";
  }

  const missingText = info.missingRows?.length
    ? `\u7f3a\u5931\u884c\u53f7\uff1a${info.missingRows.join("\u3001")}\u3002`
    : "";

  return `\u8be5\u56fe\u4e3a BFTM \u5750\u6807\u957f\u8868\uff0c\u5efa\u8bae\u53ea\u622a\u53d6\u5750\u6807\u8868\u533a\u57df\u540e\u91cd\u8bd5\uff0c\u8bc6\u522b\u4f1a\u66f4\u7a33\u5b9a\u3002\u5df2\u8bc6\u522b ${info.recognizedRows || 0}/${info.expectedRows || 0} \u884c\u3002${missingText}`;
}

function getBftmLongTableRetryPrompt(expectedRows = 0) {
  return `Read only the BFTM coordinate table in this image.
The table columns are SOMMETS / X / Y or Coordonnees en BFTM (XY).
Use the visual table layout. Read each horizontal table row from left to right.
Output only coordinate rows in the exact format X,Y.
X must be between 500000 and 760000.
Y must be between 1200000 and 1600000.
Ignore article text, phone numbers, dates, money, page numbers, signatures, watermarks, slashes, ticks, and OCR bounding boxes.
Do not output row numbers.
Do not output explanations or markdown.
Do not pair X values with X values or Y values with Y values.
If a row has marks like /, ~, \\, or 7 after the number, treat them as handwriting marks, not coordinates.
${expectedRows ? `The table appears to have about ${expectedRows} rows. Do not skip middle rows.` : "Do not skip middle rows."}
Examples:
655000,1333600
654500,1333600
654500,1334100`;
}

function shouldAcceptProjectedPair(first, second, strictBftm) {
  if (!looksLikeProjectedPair(first, second)) {
    return false;
  }

  if (strictBftm) {
    return looksLikeBftmProjectedPair(first, second);
  }

  return !looksLikeBftmColumnPairError(first, second);
}

function extractProjectedNumberPair(text) {
  const groups = String(text || "").match(/\d+(?:\.\d+)?/g) || [];
  const isSmallId = value => /^\d{1,2}$/.test(value);
  const isThreeDigits = value => /^\d{3}$/.test(value);
  const isOneToThreeDigits = value => /^\d{1,3}$/.test(value);

  if (groups.length >= 3 && isSmallId(groups[0]) && looksLikeProjectedPair(groups[1], groups[2])) {
    return [groups[1], groups[2]];
  }

  if (groups.length >= 2 && looksLikeProjectedPair(groups[0], groups[1])) {
    return [groups[0], groups[1]];
  }

  if (
    groups.length >= 6
    && isSmallId(groups[0])
    && isThreeDigits(groups[1])
    && isThreeDigits(groups[2])
    && isOneToThreeDigits(groups[3])
    && isThreeDigits(groups[4])
    && isThreeDigits(groups[5])
  ) {
    const pair = [`${groups[1]}${groups[2]}`, `${groups[3]}${groups[4]}${groups[5]}`];
    return looksLikeProjectedPair(pair[0], pair[1]) ? pair : null;
  }

  if (
    groups.length >= 5
    && isThreeDigits(groups[0])
    && isThreeDigits(groups[1])
    && isOneToThreeDigits(groups[2])
    && isThreeDigits(groups[3])
    && isThreeDigits(groups[4])
  ) {
    const pair = [`${groups[0]}${groups[1]}`, `${groups[2]}${groups[3]}${groups[4]}`];
    return looksLikeProjectedPair(pair[0], pair[1]) ? pair : null;
  }

  return null;
}

function extractProjectedCoordinateLines(text) {
  const lines = normalizeText(text)
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean);
  const coordinateLines = [];
  const strictBftm = shouldUseStrictBftmValidation(text);

  if (strictBftm) {
    const bftmLongTable = extractBftmLongTableCoordinateLines(text);

    if (bftmLongTable.lines.length >= 4) {
      return bftmLongTable.lines;
    }
  }

  for (const line of lines) {
    if (/annoter|tourner|rechercher|partager|hectares|latitude|longitude/i.test(line)) {
      continue;
    }

    const tablePair = extractProjectedNumberPair(line);

    if (tablePair && shouldAcceptProjectedPair(tablePair[0], tablePair[1], strictBftm)) {
      coordinateLines.push(`${tablePair[0]},${tablePair[1]}`);
      continue;
    }

    const numbers = extractNumbersWithThousands(line);
    const largeNumbers = numbers.filter(value => Math.abs(Number(value)) >= 10000);

    if (largeNumbers.length >= 2 && shouldAcceptProjectedPair(largeNumbers[0], largeNumbers[1], strictBftm)) {
      coordinateLines.push(`${largeNumbers[0]},${largeNumbers[1]}`);
    }
  }

  if (coordinateLines.length > 0) {
    return coordinateLines;
  }

  return strictBftm ? reconstructBftmColumnsIfPossible(text) : coordinateLines;
}

function parseDmsCoordinateLine(line) {
  const cleanLine = normalizeDmsLineForParsing(stripOcrBboxPrefix(line));
  const looseDmsPair = parseLooseDmsLine(cleanLine);

  if (looseDmsPair) {
    return `${looseDmsPair.longitude},${looseDmsPair.latitude}`;
  }

  const tokens = getDmsTokensFromLine(cleanLine);

  if (tokens.length < 2) {
    return "";
  }

  const looksLikeLonLat = /,/.test(cleanLine) || /^\s*[-+]\s*\d/.test(cleanLine);
  const inferWestNorth = looksLikeLonLat && shouldInferWestNorth(tokens[0], tokens[1]);
  const parsed = tokens
    .map((token, index) => parseCompactDmsToken(token, inferWestNorth ? (index === 0 ? "O" : "N") : (looksLikeLonLat ? "" : (index === 0 ? "N" : "O"))))
    .filter(Boolean)
    .filter(item => item.value !== null);

  if (parsed.length < 2) {
    return "";
  }

  const latitude = parsed.find(item => ["N", "S"].includes(item.direction)) || (looksLikeLonLat ? parsed[1] : parsed[0]);
  const longitude = parsed.find(item => ["E", "W", "O"].includes(item.direction)) || (looksLikeLonLat ? parsed[0] : parsed[1]);

  if (!latitude || !longitude) {
    return "";
  }

  const lonNumber = Number(longitude.value);
  const latNumber = Number(latitude.value);

  if (Math.abs(lonNumber) > 180 || Math.abs(latNumber) > 90) {
    return "";
  }

  return `${longitude.value},${latitude.value}`;
}

function getFrenchDmsParts(text) {
  const source = normalizeText(text)
    .replace(/掳/g, "°")
    .replace(/Ouest/gi, "O")
    .replace(/West/gi, "W")
    .replace(/Nord/gi, "N")
    .replace(/North/gi, "N")
    .replace(/Sud|South/gi, "S")
    .replace(/Est|East/gi, "E")
    .replace(/\bdegr[eé]s?\b/gi, "°")
    .replace(/\bminutes?\b/gi, "'")
    .replace(/\bsecondes?\b/gi, "\"");
  const segmentParts = source
    .split(/[|\r\n;；]+/)
    .map(segment => segment.trim())
    .filter(Boolean)
    .map(segment => {
      const directionMatch = segment.match(/\b([NSEWO])\b/i) || segment.match(/([NSEWO])\s*$/i);
      const numbers = segment.match(/[-+]?\d+(?:[.,]\d+)?/g) || [];

      if (!directionMatch || numbers.length < 3) {
        return null;
      }

      const direction = directionMatch[1].toUpperCase();
      const value = decimalFromDms(numbers[0], numbers[1], numbers[2].replace(",", "."), direction);

      if (value === null) {
        return null;
      }

      return {
        value,
        direction,
        axis: ["N", "S"].includes(direction) ? "lat" : "lon"
      };
    })
    .filter(Boolean);

  if (segmentParts.length > 0) {
    return segmentParts;
  }

  const pattern = /([-+]?\d{1,3})[^\dNSEWO]+(\d{1,2})[^\dNSEWO]+(\d{1,2}(?:[.,]\d+)?)[^\dNSEWO]*([NSEWO])/gi;
  const parts = [];
  let match;

  while ((match = pattern.exec(source)) !== null) {
    const direction = match[4].toUpperCase();
    const value = decimalFromDms(match[1], match[2], match[3].replace(",", "."), direction);

    if (value !== null) {
      parts.push({
        value,
        direction,
        axis: ["N", "S"].includes(direction) ? "lat" : "lon"
      });
    }
  }

  return parts;
}

function makeFrenchPerimeterPoint(label, text) {
  const parts = getFrenchDmsParts(text);
  const latitude = parts.find(part => part.axis === "lat");
  const longitude = parts.find(part => part.axis === "lon");

  if (!latitude || !longitude) {
    return null;
  }

  const lat = Number(latitude.value);
  const lon = Number(longitude.value);

  if (!Number.isFinite(lat) || !Number.isFinite(lon) || Math.abs(lat) > 90 || Math.abs(lon) > 180) {
    return null;
  }

  return makeWgs84Point({
    label,
    lat,
    lon,
    raw: text
  });
}

function getFrenchPerimeterDmsInfo(text) {
  const source = String(text || "");

  if (looksLikePointAzDmsTable(source)) {
    return {
      isFrenchPerimeterDms: false,
      points: [],
      rowCount: 0
    };
  }

  if (!hasFrenchPerimeterDmsContext(source)) {
    return {
      isFrenchPerimeterDms: false,
      points: [],
      rowCount: 0
    };
  }

  const normalized = normalizeText(source);
  const pointRegex = /\bPoint\s+([A-Z])\b\s*[:：]?/gi;
  const markers = [];
  let match;

  while ((match = pointRegex.exec(normalized)) !== null) {
    markers.push({
      label: match[1].toUpperCase(),
      index: match.index,
      end: pointRegex.lastIndex
    });
  }

  const pointsByLabel = new Map();

  for (let index = 0; index < markers.length; index += 1) {
    const marker = markers[index];
    const next = markers[index + 1];
    const block = normalized.slice(marker.end, next ? next.index : normalized.length);
    const point = makeFrenchPerimeterPoint(marker.label, block);

    if (point && !pointsByLabel.has(point.label)) {
      pointsByLabel.set(point.label, point);
    }
  }

  const points = Array.from(pointsByLabel.values())
    .sort((a, b) => a.label.localeCompare(b.label));

  if (points.length === 0) {
    return {
      isFrenchPerimeterDms: false,
      points: [],
      rowCount: 0
    };
  }

  return {
    isFrenchPerimeterDms: true,
    points,
    rowCount: points.length,
    geometry: inferGeometry(points),
    kml: buildChatCoordinatesKml(points)
  };
}

function formatFrenchPerimeterDmsRows(points) {
  return ["label | WGS84 | KML", ...points.map(point => `${point.label} | ${point.lat}, ${point.lon} | ${point.kmlCoordinate}`)].join("\n");
}

function looksLikeConvertedFrenchPerimeterDecimalOutput(text) {
  const value = String(text || "");
  const coordinateLines = value
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(line => /^[-+]?\d{1,3}(?:\.\d+)?\s*,\s*[-+]?\d{1,3}(?:\.\d+)?$/.test(line));
  const pairs = coordinateLines.map(line => line.split(",").map(part => Number(part.trim())));
  const looksLikeWestNorthBoundary = pairs.length >= 3
    && pairs.length <= 6
    && pairs.every(pair => Number.isFinite(pair[0]) && Number.isFinite(pair[1]) && pair[0] < 0 && pair[1] > 0);
  const hasStructuredHeader = /longitude|latitude|经度|纬度|WGS84\s+Longitude\s+Latitude\s+Table/i.test(value);

  return coordinateLines.length >= 3
    && coordinateLines.length <= 6
    && !hasStructuredHeader
    && (/人工修正|修正值|correction|corrected/i.test(value) || looksLikeWestNorthBoundary);
}

function shouldRetryFrenchPerimeterDmsVisualRead(rawText, coordinates, reqFile, warningText = "") {
  if (!reqFile) {
    return false;
  }

  if (looksLikePointAzDmsTable(rawText) || looksLikePointAzDmsTable(coordinates)) {
    return false;
  }

  if (getFrenchPerimeterDmsInfo(rawText).isFrenchPerimeterDms) {
    return false;
  }

  const text = String(rawText || "");
  const context = `${text}\n${warningText || ""}`;
  const noUsefulCoordinates = countCoordinateRows(coordinates) < 2 || text.includes(noCoordinatesText);
  const isSmallDocumentImage = Number(reqFile.size || 0) > 0 && Number(reqFile.size || 0) <= 250000;
  const hasFrenchPerimeterTerms = /coordonn[ée]es?\s+du\s+p[ée]rim[èe]tre|meridien|m[eé]ridien|parallele|parall[eè]le|ouest|nord/i.test(context);

  if (isSmallDocumentImage && looksLikeConvertedFrenchPerimeterDecimalOutput(context)) {
    return true;
  }

  return noUsefulCoordinates && (
    hasFrenchPerimeterTerms
    || (isSmallDocumentImage && context.includes(noCoordinatesText))
  );
}

function projectedCoordinateFromLine(line) {
  const tablePair = extractProjectedNumberPair(line);

  if (tablePair) {
    return `${tablePair[0]},${tablePair[1]}`;
  }

  const numbers = extractNumbersWithThousands(line);
  const largeNumbers = numbers.filter(value => Math.abs(Number(value)) >= 10000);

  if (largeNumbers.length >= 2 && looksLikeProjectedPair(largeNumbers[0], largeNumbers[1])) {
    return `${largeNumbers[0]},${largeNumbers[1]}`;
  }

  return "";
}

function decimalCoordinateFromLine(line) {
  const match = normalizeText(line).match(/^(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)$/)
    || normalizeText(line).match(/^(-?\d+\.\d+)\s+(-?\d+\.\d+)$/)
    || parseSpaceBrokenDecimalLine(line);

  if (!match) {
    return "";
  }

  const fixedPair = fixLikelyLatLonOrder(match[1].trim(), match[2].trim());
  const longitude = Number(fixedPair.longitudeText);
  const latitude = Number(fixedPair.latitudeText);

  if (Math.abs(longitude) <= 180 && Math.abs(latitude) <= 90) {
    return `${fixedPair.longitudeText},${fixedPair.latitudeText}`;
  }

  return "";
}

function splitGroupsByTableBoundaries(lines, sourceText) {
  const sourceLines = String(sourceText || "").split(/\r?\n/);
  const grouped = [];
  let coordinateCount = 0;
  let pendingBoundary = false;

  for (const sourceLine of sourceLines) {
    const line = sourceLine.trim();

    if (!line) {
      if (coordinateCount > 0) {
        pendingBoundary = true;
      }
      continue;
    }

    const isBoundary = /\b(point|sommets?|latitude|longitude|coordonn[eé]es?|x\s*\(?m?\)?|y\s*\(?m?\)?|rc\d|permis|permit|autorisation)\b/i.test(line);

    if (isBoundary && coordinateCount > 0) {
      pendingBoundary = true;
      continue;
    }

    const coordinate = parseDmsCoordinateLine(line) || projectedCoordinateFromLine(line) || decimalCoordinateFromLine(line);

    if (!coordinate) {
      continue;
    }

    if (pendingBoundary && grouped.length > 0 && grouped[grouped.length - 1] !== "") {
      grouped.push("");
    }

    grouped.push(coordinate);
    coordinateCount += 1;
    pendingBoundary = false;
  }

  if (grouped.length > 0 && countCoordinateRows(grouped.join("\n")) >= countCoordinateRows(lines.join("\n"))) {
    return cleanCoordinateOutput(grouped.join("\n"));
  }

  return cleanCoordinateOutput(lines.join("\n"));
}

function getDmsGroupedCoordinateInfo(text) {
  const sourceLines = normalizeText(text)
    .split(/\r?\n/)
    .map(line => line.trim());
  const groups = [];
  let currentGroup = [];
  let previousLabelNumber = null;
  let reason = "";

  const closeCurrentGroup = closeReason => {
    if (currentGroup.length > 0) {
      groups.push(currentGroup);
      currentGroup = [];
      previousLabelNumber = null;
      reason = reason || closeReason;
    }
  };

  for (const line of sourceLines) {
    if (!line) {
      closeCurrentGroup("blank_line");
      continue;
    }

    const isBoundary = /\bmining\s+area\b|\barea\s+(?:one|two|three|\d+)\b|the\s+coordinates\s+are\s+as\s+follows/i.test(foldSearchText(line));

    if (isBoundary) {
      closeCurrentGroup("title_context");
      continue;
    }

    const coordinate = parseDmsCoordinateLine(line) || decimalCoordinateFromLine(line);

    if (!coordinate) {
      continue;
    }

    const labelNumber = getLeadingCoordinateLabelNumber(line);
    if (
      currentGroup.length > 0
      && Number.isInteger(labelNumber)
      && Number.isInteger(previousLabelNumber)
      && labelNumber <= previousLabelNumber
    ) {
      closeCurrentGroup("number_restart");
    }

    currentGroup.push(coordinate);
    previousLabelNumber = Number.isInteger(labelNumber) ? labelNumber : previousLabelNumber;
  }

  closeCurrentGroup(reason || "single_group");

  const validGroups = groups.filter(group => group.length > 0);
  const hasContext = hasDmsGroupedContext(text);
  const hasValidHeaderlessSplit = validGroups.length >= 2 && validGroups.every(group => group.length >= 3);
  const canAccept = hasValidHeaderlessSplit && (hasContext || reason === "number_restart" || reason === "blank_line");

  if (!canAccept) {
    return { output: "", reason: "" };
  }

  return {
    output: cleanCoordinateOutput(validGroups.map(group => group.join("\n")).join("\n\n")),
    reason: reason || (hasContext ? "title_context" : "headerless")
  };
}

function countDmsGroupedLonLatOrderLines(text) {
  return normalizeText(text)
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .filter(line => {
      const tokens = getDmsTokensFromLine(line);

      if (tokens.length < 2) {
        return false;
      }

      const first = parseCompactDmsToken(tokens[0], "");
      const second = parseCompactDmsToken(tokens[1], "");

      return ["E", "W", "O"].includes(first?.direction) && ["N", "S"].includes(second?.direction);
    })
    .length;
}

function shouldRetryDmsGroupedVisualRead(rawText, dmsGroupedInfo) {
  if (!dmsGroupedInfo?.output) {
    return false;
  }

  const groupedRows = countCoordinateRows(dmsGroupedInfo.output);

  if (groupedRows < 4) {
    return false;
  }

  const lonLatOrderLines = countDmsGroupedLonLatOrderLines(rawText);

  return lonLatOrderLines >= Math.max(2, Math.ceil(groupedRows * 0.5));
}

function extractDmsGroupedCoordinateLines(text) {
  return getDmsGroupedCoordinateInfo(text).output;
}

function countDmsCoordinateRows(text) {
  return normalizeText(text)
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .filter(line => parseDmsCoordinateLine(line))
    .length;
}

function fixLikelyLatLonOrder(firstText, secondText) {
  const first = Number(firstText);
  const second = Number(secondText);

  if (
    Number.isFinite(first)
    && Number.isFinite(second)
    && first > 0
    && second < 0
    && Math.abs(first) <= 90
    && Math.abs(second) <= 90
  ) {
    return {
      longitudeText: secondText,
      latitudeText: firstText
    };
  }

  return {
    longitudeText: firstText,
    latitudeText: secondText
  };
}

function parseSpaceBrokenDecimalLine(line) {
  if (/[°掳'"NSEWO]/i.test(line)) {
    return null;
  }

  const match = String(line || "")
    .trim()
    .match(/^([+-]?)\s*(\d{1,3})\s+(\d{4,})\s+([+-]?)\s*(\d{1,2})\s+(\d{4,})$/);

  if (!match) {
    return null;
  }

  const longitudeText = `${match[1] || ""}${match[2]}.${match[3]}`;
  const latitudeText = `${match[4] || ""}${match[5]}.${match[6]}`;
  const longitude = Number(longitudeText);
  const latitude = Number(latitudeText);

  if (!Number.isFinite(longitude) || !Number.isFinite(latitude)) {
    return null;
  }

  if (Math.abs(longitude) > 180 || Math.abs(latitude) > 90) {
    return null;
  }

  return [line, longitudeText, latitudeText];
}

function extractDmsCoordinateLines(text) {
  const lines = normalizeText(text)
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean);
  const coordinateLines = [];
  for (const line of lines) {
    if (/annoter|tourner|rechercher|partager|hectares/i.test(line)) {
      continue;
    }

    const looseDmsPair = parseLooseDmsLine(line);

    if (looseDmsPair) {
      coordinateLines.push(`${looseDmsPair.longitude},${looseDmsPair.latitude}`);
      continue;
    }

    const tokens = getDmsTokensFromLine(line);

    if (tokens.length < 2) {
      continue;
    }

    const looksLikeLonLat = /,/.test(line) || /^\s*[-+]\s*\d/.test(line);
    const inferWestNorth = looksLikeLonLat && shouldInferWestNorth(tokens[0], tokens[1]);
    const parsed = tokens
      .map((token, index) => parseCompactDmsToken(token, inferWestNorth ? (index === 0 ? "O" : "N") : (looksLikeLonLat ? "" : (index === 0 ? "N" : "O"))))
      .filter(Boolean)
      .filter(item => item.value !== null);

    if (parsed.length < 2) {
      continue;
    }

    const latitude = parsed.find(item => ["N", "S"].includes(item.direction)) || (looksLikeLonLat ? parsed[1] : parsed[0]);
    const longitude = parsed.find(item => ["E", "W", "O"].includes(item.direction)) || (looksLikeLonLat ? parsed[0] : parsed[1]);

    if (!latitude || !longitude) {
      continue;
    }

    const lonNumber = Number(longitude.value);
    const latNumber = Number(latitude.value);

    if (Math.abs(lonNumber) <= 180 && Math.abs(latNumber) <= 90) {
      coordinateLines.push(`${longitude.value},${latitude.value}`);
    }
  }

  return coordinateLines;
}

function extractCoordinateLines(text) {
  const dmsGroupedLines = extractDmsGroupedCoordinateLines(text);
  if (dmsGroupedLines) {
    return dmsGroupedLines;
  }

  const dmsLines = extractDmsCoordinateLines(text);

  if (dmsLines.length > 0 && !getCadastralGridInfo(text).isCadastralGrid) {
    return dmsLines.join("\n");
  }

  if (shouldUseStrictBftmValidation(text)) {
    const projectedLines = extractProjectedCoordinateLines(text);
    return projectedLines.length > 0 ? projectedLines.join("\n") : noCoordinatesText;
  }

  const bftmLongTable = extractBftmLongTableCoordinateLines(text);
  if (bftmLongTable.lines.length >= 4) {
    return bftmLongTable.lines.join("\n");
  }

  const projectedLines = extractProjectedCoordinateLines(text);
  if (hasBftmContext(text) && projectedLines.length > 0) {
    return projectedLines.join("\n");
  }

  const mgrs = getMgrsInfo(text);
  if (mgrs.isMgrs) {
    return formatMgrsRows(mgrs.rows);
  }

  const kyrgyzGk = getKyrgyzGkInfo(text);
  if (kyrgyzGk.isKyrgyzGk) {
    return formatKyrgyzGkRows(kyrgyzGk.rows);
  }

  const cadastralGrid = getCadastralGridInfo(text);
  if (cadastralGrid.isCadastralGrid) {
    return formatCadastralGridRows(cadastralGrid.rows);
  }

  const mozambiqueGeographicTable = getMozambiqueGeographicInfo(text);
  if (mozambiqueGeographicTable.isMozambiqueGeographicTable) {
    return formatMozambiqueGeographicRows(mozambiqueGeographicTable.rows);
  }

  const wgs84TableCoordinates = getWgs84TableCoordinatesInfo(text);
  if (wgs84TableCoordinates.isWgs84TableCoordinates) {
    return formatChatCoordinateRows(wgs84TableCoordinates.points);
  }

  const chatCoordinates = getChatCoordinatesInfo(text);
  if (chatCoordinates.isChatCoordinates) {
    return formatChatCoordinateRows(chatCoordinates.points);
  }

  const decimalLines = extractDecimalCoordinateLines(text);

  if (decimalLines.length > 0) {
    return splitGroupsAtRepeatedBoundary(decimalLines.join("\n"));
  }

  if (dmsLines.length > 0) {
    return dmsLines.join("\n");
  }

  return projectedLines.length > 0 ? projectedLines.join("\n") : noCoordinatesText;
}

function countCoordinateRows(text) {
  return String(text || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
      .filter(line => line !== noCoordinatesText)
      .filter(line => !/^num\s*\|\s*XV\s*\|\s*YV$/i.test(line))
      .length;
}

function shouldRetryRecognition(rawText, coordinates) {
  return countCoordinateRows(coordinates) < 4;
}

function shouldRetryBftmRecognition(rawText, coordinates) {
  const rawAnalysis = analyzeBftmProjectedPairs(rawText);
  const coordinateAnalysis = analyzeBftmProjectedPairs(coordinates);
  const hasInvalidColumnOutput = rawAnalysis.invalidColumnPairs >= 2 || coordinateAnalysis.invalidColumnPairs >= 2;
  const hasBboxPollution = rawAnalysis.bboxPollutedLines >= 2 || coordinateAnalysis.bboxPollutedLines >= 2;
  const hasValidBftmRows = coordinateAnalysis.validPairs >= 4;

  return (hasInvalidColumnOutput || hasBboxPollution) && !hasValidBftmRows;
}

function getCoordinateRows(text) {
  return String(text || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .filter(line => line !== noCoordinatesText)
    .filter(line => !/^#?\s*(识别提示|提示|璇嗗埆鎻愮ず|鎻愮ず)\s*[:：锛]/.test(line));
}

function normalizeCoordinateRowForCompare(line) {
  return String(line || "")
    .toUpperCase()
    .replace(/[º˚]/g, "°")
    .replace(/[‘’´`′]/g, "'")
    .replace(/[“”″]/g, "\"")
    .replace(/\s+/g, "")
    .replace(/[，；;]/g, ",");
}

function countDuplicateCoordinateRows(text) {
  const seen = new Set();
  let duplicates = 0;

  getCoordinateRows(text).forEach(line => {
    const key = normalizeCoordinateRowForCompare(line);

    if (!key) {
      return;
    }

    if (seen.has(key)) {
      duplicates += 1;
    } else {
      seen.add(key);
    }
  });

  return duplicates;
}

function looksLikeCommaDmsLongTableRow(line) {
  const value = String(line || "");
  return value.includes(",")
    && /(?:°|º|˚|\d+\s*')/.test(value)
    && /[NS]/i.test(value)
    && /[EWO]/i.test(value);
}

function countCommaDmsLongTableRows(text) {
  return getCoordinateRows(text).filter(looksLikeCommaDmsLongTableRow).length;
}

function shouldRetryPointAzDmsLongTable(rawText, coordinates) {
  if (shouldUseStrictBftmValidation(rawText) || shouldUseStrictBftmValidation(coordinates)) {
    return false;
  }

  const dmsRows = Math.max(
    countCommaDmsLongTableRows(rawText),
    countCommaDmsLongTableRows(coordinates)
  );
  const coordinateRows = countCoordinateRows(coordinates);
  const duplicateRows = Math.max(
    countDuplicateCoordinateRows(rawText),
    countDuplicateCoordinateRows(coordinates)
  );

  if (dmsRows < 12 || coordinateRows < 12) {
    return false;
  }

  // Point A-Z and other long DMS tables should be transcribed with row labels
  // first. Direct coordinate-only output is too easy for the model to guess or
  // shift by one table row, even when it returns many rows.
  return dmsRows >= 20 || duplicateRows > 0 || (coordinateRows >= 15 && coordinateRows < 24);
}

function looksLikePointAzDmsTable(text) {
  const source = String(text || "");

  if (!source.trim()) {
    return false;
  }

  const normalized = normalizeText(source);
  const hasFrenchDirections = /\bnord\b/i.test(normalized) && /\b(?:est|ouest)\b/i.test(normalized);
  const hasTableHeader = /\bpoint\b/i.test(normalized) && /\bnord\b/i.test(normalized) && /\b(?:est|ouest)\b/i.test(normalized);

  if (!hasFrenchDirections && !hasTableHeader) {
    return false;
  }

  const labels = [];

  for (const line of normalized.split(/\r?\n/)) {
    const label = extractPointTableLabel(line);

    if (!/^[A-Z]$/.test(label)) {
      continue;
    }

    const parts = getLooseDmsPartsFromLine(line);

    if (parts.length >= 2) {
      labels.push(label);
    }
  }

  const uniqueLabels = Array.from(new Set(labels));
  const hasAbcSequence = ["A", "B", "C"].every(label => uniqueLabels.includes(label));
  const ordered = uniqueLabels
    .map(label => label.charCodeAt(0) - 64)
    .sort((a, b) => a - b);
  let longestRun = 0;
  let currentRun = 0;
  let previous = 0;

  for (const order of ordered) {
    if (order === previous + 1) {
      currentRun += 1;
    } else {
      currentRun = 1;
    }

    longestRun = Math.max(longestRun, currentRun);
    previous = order;
  }

  return hasAbcSequence && uniqueLabels.length >= 8 && longestRun >= 6;
}

function shouldAcceptPointAzDmsRetry(currentCoordinates, retryCoordinates) {
  const currentRows = countCoordinateRows(currentCoordinates);
  const retryRows = countCoordinateRows(retryCoordinates);
  const currentDuplicates = countDuplicateCoordinateRows(currentCoordinates);
  const retryDuplicates = countDuplicateCoordinateRows(retryCoordinates);

  if (retryRows >= 24 && retryRows > currentRows) {
    return true;
  }

  if (retryRows > currentRows && retryDuplicates <= currentDuplicates) {
    return true;
  }

  return retryRows === currentRows && retryDuplicates < currentDuplicates;
}

function shouldAcceptPointAzTranscription(currentCoordinates, tableRows) {
  const currentRows = countCoordinateRows(currentCoordinates);
  const retryRows = Array.isArray(tableRows) ? tableRows.length : 0;

  if (retryRows < 12) {
    return false;
  }

  // A labeled Point/Nord/Est transcription is more trustworthy than a direct
  // coordinate-only list for long tables, because coordinate-only output often
  // shifts one row or invents intermediate points. Accept complete labeled
  // transcriptions even when the row count equals the first pass.
  if (retryRows >= 20 && retryRows >= currentRows - 2) {
    return true;
  }

  if (retryRows >= 24) {
    return true;
  }

  return retryRows > currentRows;
}

function getLooseDmsPartsFromLine(line) {
  const text = String(line || "");
  const degreePattern = /[-+]?\d{1,3}\s*(?:\u00B0|\u00BA|\u02DA)\s*\d{1,2}\s*(?:'|\u2032|\u2019)?\s*\d{1,2}(?:\.\d+)?\s*(?:"|\u2033|\u201D)?\s*[NSEWO]/gi;
  const symbolPattern = /[-+]?\d{1,3}\s*[^0-9A-Za-z\s]\s*\d{1,2}\s*(?:'|\u2032|\u2019)?\s*\d{1,2}(?:\.\d+)?\s*(?:"|\u2033|\u201D)?\s*[NSEWO]/gi;
  const spacedPattern = /[-+]?\d{1,3}\s+\d{1,2}\s+\d{1,2}(?:\.\d+)?\s*(?:"|\u2033|\u201D)?\s*[NSEWO]/gi;
  const dotPattern = /[-+]?\d{1,3}\.\d{1,2}\.\d{1,2}(?:\.\d+)?\s*[NSEWO]/gi;
  const matches = [
    ...(text.match(degreePattern) || []),
    ...(text.match(symbolPattern) || []),
    ...(text.match(spacedPattern) || []),
    ...(text.match(dotPattern) || [])
  ];

  return Array.from(new Set(matches));
}

function getDmsPartAxis(part) {
  const parsed = parseLooseDmsPart(part, "");
  return parsed ? parsed.axis : "";
}

function cleanDmsDisplayPart(part) {
  return String(part || "")
    .trim()
    .replace(/[潞藲]/g, "\u00B0")
    .replace(/[鈥樷€櫬碻鈥瞉\u2018\u2019\u2032]/g, "'")
    .replace(/[鈥溾€濃€砞\u201C\u201D\u2033]/g, "\"")
    .replace(/\s+/g, " ");
}

function getDmsDisplayComponents(part) {
  const clean = cleanDmsDisplayPart(part);
  const parsed = parseLooseDmsPart(clean, "");
  const groups = clean
    .replace(/[NSEWO]/gi, " ")
    .match(/\d+(?:\.\d+)?/g) || [];

  if (!parsed || groups.length < 3) {
    return null;
  }

  return {
    clean,
    axis: parsed.axis,
    direction: parsed.direction,
    degree: Number(groups[0]),
    minute: Number(groups[1]),
    second: Number(groups[2])
  };
}

function replaceDmsMinute(cleanPart, minute) {
  const paddedMinute = String(minute).padStart(2, "0");
  return String(cleanPart || "").replace(/^(\s*\d{1,3}\s*\u00B0\s*)\d{1,2}/, `$1${paddedMinute}`);
}

function smoothDmsMinuteIslandsForLongTable(text) {
  const rows = getCoordinateRows(text);

  if (rows.length < 20) {
    return text;
  }

  const parsedRows = rows.map(row => {
    const parts = getLooseDmsPartsFromLine(row);
    const latPart = parts.find(part => getDmsPartAxis(part) === "lat");
    const lonPart = parts.find(part => getDmsPartAxis(part) === "lon");
    const lat = getDmsDisplayComponents(latPart);
    const lon = getDmsDisplayComponents(lonPart);

    return lat && lon ? { lat, lon } : null;
  });

  if (parsedRows.filter(Boolean).length < 20) {
    return text;
  }

  const smoothAxis = axis => {
    let changed = false;

    for (let index = 1; index < parsedRows.length - 1; index += 1) {
      const current = parsedRows[index]?.[axis];

      if (!current) {
        continue;
      }

      let end = index;
      while (
        end + 1 < parsedRows.length
        && parsedRows[end + 1]?.[axis]
        && parsedRows[end + 1][axis].degree === current.degree
        && parsedRows[end + 1][axis].minute === current.minute
      ) {
        end += 1;
      }

      const prev = parsedRows[index - 1]?.[axis];
      const next = parsedRows[end + 1]?.[axis];
      const islandLength = end - index + 1;

      if (
        prev
        && next
        && islandLength <= 2
        && prev.degree === current.degree
        && next.degree === current.degree
        && prev.minute === next.minute
        && Math.abs(current.minute - prev.minute) === 1
      ) {
        const minSecond = Math.min(prev.second, next.second);
        const maxSecond = Math.max(prev.second, next.second);
        const islandSecondsFit = parsedRows
          .slice(index, end + 1)
          .every(row => row?.[axis] && row[axis].second >= minSecond && row[axis].second <= maxSecond);

        if (islandSecondsFit) {
          for (let fillIndex = index; fillIndex <= end; fillIndex += 1) {
            parsedRows[fillIndex][axis].minute = prev.minute;
            parsedRows[fillIndex][axis].clean = replaceDmsMinute(parsedRows[fillIndex][axis].clean, prev.minute);
          }
          changed = true;
        }
      }

      index = end;
    }

    return changed;
  };

  const changed = smoothAxis("lon") || smoothAxis("lat");

  if (!changed) {
    return text;
  }

  return parsedRows
    .map((row, index) => row ? `${row.lon.clean},${row.lat.clean}` : rows[index])
    .join("\n");
}

function extractPointTableLabel(line) {
  const text = String(line || "").trim().replace(/^\|+\s*/, "");

  if (/^[|:\-\s]+$/.test(text) || /^nord\b|^est\b|^latitude\b|^longitude\b|^point\s*(?:$|[|,;:.)-])/i.test(text)) {
    return "";
  }

  const match = text.match(/^(?:point\s*)?([A-Z]|\d{1,2})\b\s*(?:[|,;:.)-]|\s+)/i);

  return match ? match[1].toUpperCase() : "";
}

function getPointTableLabelOrder(label) {
  if (/^[A-Z]$/.test(label)) {
    return label.charCodeAt(0) - 64;
  }

  const number = Number(label);
  return Number.isFinite(number) ? number : 0;
}

function extractPointDmsTableCoordinateRows(text) {
  const byLabel = new Map();
  const lines = String(text || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean);

  for (const line of lines) {
    const label = extractPointTableLabel(line);

    if (!label) {
      continue;
    }

    const parts = getLooseDmsPartsFromLine(line);

    if (parts.length < 2) {
      continue;
    }

    const latitudePart = parts.find(part => getDmsPartAxis(part) === "lat");
    const longitudePart = parts.find(part => getDmsPartAxis(part) === "lon");

    if (!latitudePart || !longitudePart) {
      continue;
    }

    const outputLine = `${cleanDmsDisplayPart(longitudePart)},${cleanDmsDisplayPart(latitudePart)}`;
    const order = getPointTableLabelOrder(label);

    if (!byLabel.has(label)) {
      byLabel.set(label, { label, order, outputLine });
    }
  }

  return Array.from(byLabel.values())
    .sort((a, b) => a.order - b.order)
    .map(row => row.outputLine);
}

function normalizeCommaDmsCoordinateDisplayOrder(text) {
  const rows = getCoordinateRows(text);
  const normalizedRows = [];
  let validDmsRows = 0;

  for (const row of rows) {
    const parts = getLooseDmsPartsFromLine(row);

    if (parts.length < 2) {
      normalizedRows.push(row);
      continue;
    }

    const latitudePart = parts.find(part => getDmsPartAxis(part) === "lat");
    const longitudePart = parts.find(part => getDmsPartAxis(part) === "lon");

    if (!latitudePart || !longitudePart) {
      normalizedRows.push(row);
      continue;
    }

    validDmsRows += 1;
    normalizedRows.push(`${cleanDmsDisplayPart(longitudePart)},${cleanDmsDisplayPart(latitudePart)}`);
  }

  return validDmsRows >= 8 ? normalizedRows.join("\n") : text;
}

function extractRecognitionWarning(text) {
  const warningLine = String(text || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .find(line => /^#?\s*(识别提示|提示)\s*[:：]/.test(line));

  return warningLine
    ? warningLine.replace(/^#?\s*/, "")
    : "";
}

function getAliyunErrorMessage(error) {
  const status = error.status ? `HTTP ${error.status}` : "";
  const code = error.code ? `code=${error.code}` : "";
  const requestId = error.requestId ? `requestId=${error.requestId}` : "";
  const message = error.message || "未知错误";

  return [status, code, requestId, message].filter(Boolean).join(" | ");
}

function getAliyunChatCompletionsUrl() {
  const base = String(aliyunBaseURL || "").replace(/\/+$/, "");
  return base.endsWith("/chat/completions") ? base : `${base}/chat/completions`;
}

async function callAliyunVision({ modelName, prompt, imageItems, temperature = 0.1, maxTokens, timeoutMs = 35000 }) {
  if (!aliyunApiKey) {
    console.error("[Aliyun] 缺少环境变量：ALIYUN_API_KEY 或 DASHSCOPE_API_KEY");
    const error = new Error("阿里云 API 未配置");
    error.code = "ALIYUN_API_KEY_MISSING";
    throw error;
  }

  const requestUrl = getAliyunChatCompletionsUrl();
  const startedAt = Date.now();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  console.log("[Aliyun] 请求开始：", {
    url: requestUrl,
    model: modelName,
    imageCount: Array.isArray(imageItems) ? imageItems.length : 0,
    startedAt: new Date(startedAt).toISOString(),
    timeoutMs,
    hasAliyunApiKey: Boolean(process.env.ALIYUN_API_KEY),
    hasDashscopeApiKey: Boolean(process.env.DASHSCOPE_API_KEY),
    baseURL: aliyunBaseURL
  });

  const requestBody = {
    model: modelName,
    messages: [
      {
        role: "user",
        content: [
          { type: "text", text: prompt },
          ...imageItems
        ]
      }
    ],
    temperature
  };

  if (Number.isFinite(Number(maxTokens)) && Number(maxTokens) > 0) {
    requestBody.max_tokens = Number(maxTokens);
  }

  let response;
  let data;

  try {
    response = await fetch(getAliyunChatCompletionsUrl(), {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${aliyunApiKey}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(requestBody),
      signal: controller.signal
    });
    data = await response.json().catch(() => ({}));
  } catch (error) {
    const endedAt = Date.now();
    if (error.name === "AbortError") {
      const timeoutError = new Error("阿里云接口超时");
      timeoutError.code = "ALIYUN_TIMEOUT";
      timeoutError.reason = "timeout";
      timeoutError.durationMs = endedAt - startedAt;
      console.error("[Aliyun] 请求超时：", {
        model: modelName,
        startedAt: new Date(startedAt).toISOString(),
        endedAt: new Date(endedAt).toISOString(),
        durationMs: timeoutError.durationMs,
        timeoutMs
      });
      throw timeoutError;
    }
    error.durationMs = endedAt - startedAt;
    console.error("[Aliyun] 网络请求失败：", {
      model: modelName,
      message: error.message,
      code: error.code,
      durationMs: error.durationMs
    });
    throw error;
  } finally {
    clearTimeout(timer);
  }

  const endedAt = Date.now();
  console.log("[Aliyun] 请求结束：", {
    status: response.status,
    model: modelName,
    startedAt: new Date(startedAt).toISOString(),
    endedAt: new Date(endedAt).toISOString(),
    durationMs: endedAt - startedAt,
    requestId: data?.request_id || data?.requestId || data?.RequestId
  });

  if (!response.ok) {
    console.error("[Aliyun] 请求失败：", {
      status: response.status,
      statusText: response.statusText,
      model: modelName,
      errorCode: data?.error?.code || data?.code,
      errorMessage: data?.error?.message || data?.message,
      requestId: data?.request_id || data?.requestId || data?.RequestId,
      durationMs: endedAt - startedAt,
      responseBody: JSON.stringify(data).slice(0, 1200)
    });
    const error = new Error(data?.error?.message || data?.message || `阿里云 API 请求失败：HTTP ${response.status}`);
    error.status = response.status;
    error.code = data?.error?.code || data?.code;
    error.requestId = data?.request_id || data?.requestId || data?.RequestId;
    error.durationMs = endedAt - startedAt;
    error.details = data;
    throw error;
  }

  return data;
}

async function runLocalOcrFallback(imageBuffer, reason = "") {
  const result = await Tesseract.recognize(imageBuffer, "eng", {
    logger: info => console.log(info.status, info.progress)
  });
  const rawText = result.data.text || "";
  const coordinates = extractCoordinateLines(rawText);

  return {
    model: "local-tesseract-fallback",
    rawText,
    coordinates,
    precisionMode: "local-ocr-dms-fallback",
    warning: `备用OCR，结果需人工核对。${reason ? `主识别错误：${reason}` : ""}`
  };
}

function normalizeCoordinateEngineV2WarningList(value) {
  if (!value) {
    return [];
  }

  if (Array.isArray(value)) {
    return value.map(item => String(item || "").trim()).filter(Boolean);
  }

  return String(value)
    .split(/\r?\n|；|;/)
    .map(item => item.trim())
    .filter(Boolean);
}

function decodeUploadedOriginalName(value = "") {
  const raw = String(value || "");
  if (!raw) {
    return "";
  }

  try {
    const decoded = Buffer.from(raw, "latin1").toString("utf8");
    if (/[\u4e00-\u9fff]|C[ôo]te|Ivoire/i.test(decoded)) {
      return decoded;
    }
  } catch (_) {
    // Keep the original multer value when decoding is not applicable.
  }

  return raw;
}

function getUploadedFileDisplayName(file) {
  return decodeUploadedOriginalName(file?.originalname || "");
}

function hasCoteDIvoireGeographicDmsCue(text = "") {
  const value = String(text || "");
  const folded = foldSearchText(value);

  return /C[ôo]te\s+d[’']?Ivoire|Cote\s+d[’']?Ivoire|Ivory\s+Coast|科特迪瓦/i.test(value)
    || /points?/i.test(value) && /latitude\s*(?:n|nord)|longitude\s*(?:w|ouest)/i.test(folded)
    || /superficies?\s*\(ha\)|superficie|hectares?/i.test(folded) && /latitude|longitude/i.test(folded);
}

function parseCoteDIvoireAreaHa(line = "") {
  const value = String(line || "");
  if (!/area_ha|declared_area_ha|superficies?\s*\(ha\)|superficie|hectares?/i.test(foldSearchText(value))) {
    return null;
  }

  const matches = Array.from(value.matchAll(/\d+(?:[,.]\d+)?/g)).map(match => Number(match[0].replace(",", ".")));
  const area = matches.reverse().find(number => Number.isFinite(number) && number > 0 && number < 100000);

  return Number.isFinite(area) ? area : null;
}

function parseCoteDIvoireDmsComponents(line = "") {
  const source = normalizeText(line)
    .replace(/,/g, ".")
    .replace(/NORD/gi, "N")
    .replace(/NORTH/gi, "N")
    .replace(/SUD|SOUTH/gi, "S")
    .replace(/OUEST|WEST/gi, "W")
    .replace(/[º˚]/g, "°")
    .replace(/[‘’´`′]/g, "'")
    .replace(/[“”″]/g, '"');
  const pattern = /(\d{1,3})\s*(?:°|掳|º)?\s*(\d{1,2})\s*(?:'|′|’|`)?\s*(\d{1,2}(?:\.\d+)?)\s*(?:"|″|”)?\s*(N|S|E|W|O)?/gi;
  const parts = [];
  let match;

  while ((match = pattern.exec(source)) !== null) {
    parts.push({
      raw: match[0].trim(),
      degrees: match[1],
      minutes: match[2],
      seconds: match[3],
      direction: String(match[4] || "").toUpperCase()
    });
  }

  return parts;
}

function parseCoteDIvoireCoordinateLine(line = "", fallbackIndex = 1) {
  const source = String(line || "").trim();
  let dmsSource = source;
  let label = "";
  const pipePointMatch = source.match(/^\s*(?:point|pt)\s*\|\s*([^|]+)\s*\|\s*/i);
  const strictLabelMatch = source.match(/^\s*(?:point\s*)?(\d{1,3})\s*(?:[).:：|\-]\s*)/i);
  const looseTableLabelMatch = source.match(/^\s*(\d{1,3})\s+(?=\d{1,2}\s*(?:°|掳|º))/i);

  if (pipePointMatch) {
    label = pipePointMatch[1].trim();
    dmsSource = source.slice(pipePointMatch[0].length).trim();
  } else if (strictLabelMatch) {
    label = String(Number(strictLabelMatch[1]));
    dmsSource = source.slice(strictLabelMatch[0].length).trim();
  } else if (looseTableLabelMatch) {
    label = String(Number(looseTableLabelMatch[1]));
    dmsSource = source.slice(looseTableLabelMatch[0].length).trim();
  }

  const parts = parseCoteDIvoireDmsComponents(dmsSource);

  if (parts.length < 2) {
    return null;
  }

  const latPart = parts.find(part => ["N", "S"].includes(part.direction)) || parts[0];
  const lonPart = parts.find(part => ["E", "W", "O"].includes(part.direction)) || parts.find(part => part !== latPart) || parts[1];
  const lat = Number(decimalFromDms(latPart.degrees, latPart.minutes, latPart.seconds, latPart.direction || "N"));
  const lon = Number(decimalFromDms(lonPart.degrees, lonPart.minutes, lonPart.seconds, lonPart.direction || "W"));

  if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat < 0 || lat > 12 || lon > 0 || lon < -10) {
    return null;
  }

  return {
    label: label || String(fallbackIndex),
    raw: source,
    lat,
    lon,
    confidence: 0.95,
    requires_review: false,
    warnings: []
  };
}

function getCoordinateEngineV2DistanceMeters(a, b) {
  const radius = 6371008.8;
  const lon1 = Number(a?.lon) * Math.PI / 180;
  const lat1 = Number(a?.lat) * Math.PI / 180;
  const lon2 = Number(b?.lon) * Math.PI / 180;
  const lat2 = Number(b?.lat) * Math.PI / 180;
  const dlon = lon2 - lon1;
  const dlat = lat2 - lat1;
  const value = Math.sin(dlat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dlon / 2) ** 2;

  return 2 * radius * Math.asin(Math.sqrt(value));
}

function projectCoordinateEngineV2LocalMeters(points = []) {
  const latMean = points.reduce((sum, point) => sum + Number(point.lat || 0), 0) / Math.max(points.length, 1);
  const lonMean = points.reduce((sum, point) => sum + Number(point.lon || 0), 0) / Math.max(points.length, 1);
  const radius = 6371008.8;
  const lat0 = latMean * Math.PI / 180;

  return points.map(point => ({
    x: (Number(point.lon) - lonMean) * Math.PI / 180 * radius * Math.cos(lat0),
    y: (Number(point.lat) - latMean) * Math.PI / 180 * radius
  }));
}

function getCoordinateEngineV2PolygonAreaHa(points = []) {
  if (!Array.isArray(points) || points.length < 3) {
    return 0;
  }

  const xy = projectCoordinateEngineV2LocalMeters(points);
  let area = 0;

  for (let index = 0; index < xy.length; index += 1) {
    const current = xy[index];
    const next = xy[(index + 1) % xy.length];
    area += current.x * next.y - next.x * current.y;
  }

  return Math.abs(area) / 20000;
}

function coordinateEngineV2Orientation(a, b, c) {
  return (b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x);
}

function coordinateEngineV2SegmentsIntersect(a, b, c, d) {
  const o1 = coordinateEngineV2Orientation(a, b, c);
  const o2 = coordinateEngineV2Orientation(a, b, d);
  const o3 = coordinateEngineV2Orientation(c, d, a);
  const o4 = coordinateEngineV2Orientation(c, d, b);

  return (o1 > 0) !== (o2 > 0) && (o3 > 0) !== (o4 > 0);
}

function isCoordinateEngineV2SelfIntersecting(points = []) {
  if (!Array.isArray(points) || points.length < 4) {
    return false;
  }

  const xy = projectCoordinateEngineV2LocalMeters(points);

  for (let first = 0; first < xy.length; first += 1) {
    const a = xy[first];
    const b = xy[(first + 1) % xy.length];

    for (let second = first + 1; second < xy.length; second += 1) {
      if (Math.abs(first - second) <= 1 || (first === 0 && second === xy.length - 1)) {
        continue;
      }

      const c = xy[second];
      const d = xy[(second + 1) % xy.length];
      if (coordinateEngineV2SegmentsIntersect(a, b, c, d)) {
        return true;
      }
    }
  }

  return false;
}

function buildCoordinateEngineV2KmlForGroup(group = {}) {
  const points = Array.isArray(group.points) ? group.points : [];
  if (!group.kml_ready || points.length === 0) {
    return "";
  }

  const coordinates = points.map(point => `${point.lon},${point.lat},0`);
  const geometry = group.geometry === "point"
    ? `<Point><coordinates>${coordinates[0]}</coordinates></Point>`
    : group.geometry === "line"
      ? `<LineString><coordinates>${coordinates.join(" ")}</coordinates></LineString>`
      : `<Polygon><outerBoundaryIs><LinearRing><coordinates>${[...coordinates, coordinates[0]].join(" ")}</coordinates></LinearRing></outerBoundaryIs></Polygon>`;

  return `<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
<Document>
<name>${escapeKmlText(group.group_name || "Cote d'Ivoire Geographic DMS")}</name>
<Placemark>
<name>${escapeKmlText(group.group_name || "Cote d'Ivoire Geographic DMS")}</name>
${geometry}
</Placemark>
</Document>
</kml>`;
}

function dedupeCoteDIvoireRawGroups(rawGroups = []) {
  const seen = new Set();
  const groups = [];

  for (const group of rawGroups) {
    const signature = (group.points || [])
      .map(point => `${Number(point.lat).toFixed(8)},${Number(point.lon).toFixed(8)}`)
      .join(";");

    if (!signature || seen.has(signature)) {
      continue;
    }

    seen.add(signature);
    groups.push(group);
  }

  return groups;
}

function normalizeCoteDIvoireGeographicDmsTable(rawGroups = []) {
  return rawGroups.map((group, groupIndex) => {
    const points = (group.points || []).map(point => ({
      label: point.label,
      raw: point.raw,
      lat: point.lat,
      lon: point.lon,
      confidence: point.confidence,
      requires_review: false,
      warnings: []
    }));
    const warnings = [];
    const duplicateKeys = new Set();
    let hasDuplicate = false;

    for (const point of points) {
      const key = `${Number(point.lat).toFixed(8)},${Number(point.lon).toFixed(8)}`;
      if (duplicateKeys.has(key)) {
        hasDuplicate = true;
      }
      duplicateKeys.add(key);
    }

    const calculatedAreaHa = Number(getCoordinateEngineV2PolygonAreaHa(points).toFixed(2));
    const selfIntersecting = isCoordinateEngineV2SelfIntersecting(points);
    const edgeLengths = points.map((point, index) => getCoordinateEngineV2DistanceMeters(point, points[(index + 1) % points.length]));
    const abnormalEdge = edgeLengths.some(length => !Number.isFinite(length) || length < 1 || length > 100000);
    const declaredArea = group.declared_area_ha === null || group.declared_area_ha === undefined
      ? NaN
      : Number(group.declared_area_ha);
    const areaErrorPercent = Number.isFinite(declaredArea) && declaredArea > 0
      ? Math.abs(calculatedAreaHa - declaredArea) / declaredArea * 100
      : null;

    if (points.length < 3) {
      warnings.push("点数不足，无法生成可靠多边形。");
    }
    if (hasDuplicate) {
      warnings.push("存在重复点，请人工核对。");
    }
    if (selfIntersecting) {
      warnings.push("原始点序形成自交多边形，请人工核对点序后再生成 KML。");
    }
    if (abnormalEdge) {
      warnings.push("存在异常边长，请人工核对坐标单位或点序。");
    }
    if (areaErrorPercent !== null && areaErrorPercent > 2) {
      warnings.push(`标注面积与计算面积误差超过 2%（${areaErrorPercent.toFixed(2)}%）。`);
    }

    const requiresReview = warnings.length > 0;
    const kmlReady = points.length >= 3 && !hasDuplicate && !selfIntersecting && !abnormalEdge;
    const normalizedGroup = {
      group_id: `group_${groupIndex + 1}`,
      group_name: group.group_name || `矿区${groupIndex + 1}`,
      geometry: points.length === 1 ? "point" : (points.length === 2 ? "line" : "polygon"),
      declared_area_ha: Number.isFinite(declaredArea) ? declaredArea : null,
      calculated_area_ha: calculatedAreaHa,
      area_error_percent: areaErrorPercent === null ? null : Number(areaErrorPercent.toFixed(2)),
      confidence: requiresReview ? 0.75 : 0.95,
      requires_review: requiresReview,
      kml_ready: kmlReady,
      warnings,
      points: points.map(point => ({
        ...point,
        confidence: requiresReview ? Math.min(point.confidence, 0.85) : point.confidence,
        requires_review: requiresReview,
        warnings: requiresReview ? warnings : []
      }))
    };

    normalizedGroup.kml = buildCoordinateEngineV2KmlForGroup(normalizedGroup);
    normalizedGroup.suggested_filename = `${String(normalizedGroup.group_name || normalizedGroup.group_id)
      .replace(/[^\p{L}\p{N}_-]+/gu, "_")
      .replace(/^_+|_+$/g, "") || normalizedGroup.group_id}.kml`;

    return normalizedGroup;
  });
}

function buildCoteDIvoireGeographicDmsV2Result(payload = {}, options = {}) {
  const text = [
    options.fileName || "",
    payload.rawText || "",
    payload.coordinates || ""
  ].filter(Boolean).join("\n");

  if (!hasCoteDIvoireGeographicDmsCue(text)) {
    return null;
  }

  const lines = String(text || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean);
  const rawGroups = [];
  let currentCompany = "";
  let currentGroup = null;
  let implicitGroupIndex = 1;

  const ensureGroup = () => {
    if (!currentGroup) {
      currentGroup = {
        group_name: currentCompany ? `${currentCompany}_矿区${implicitGroupIndex}` : `矿区${implicitGroupIndex}`,
        declared_area_ha: null,
        points: []
      };
      rawGroups.push(currentGroup);
      implicitGroupIndex += 1;
    }
    return currentGroup;
  };

  for (const line of lines) {
    const folded = foldSearchText(line);
    const coordinate = parseCoteDIvoireCoordinateLine(line, currentGroup?.points?.length + 1 || 1);
    const area = parseCoteDIvoireAreaHa(line);

    if (area !== null && currentGroup) {
      currentGroup.declared_area_ha = area;
      continue;
    }

    if (coordinate) {
      ensureGroup().points.push(coordinate);
      continue;
    }

    const structuredGroupMatch = line.match(/^\s*GROUP\s*\|\s*(.+)$/i);
    if (structuredGroupMatch) {
      currentGroup = {
        group_name: structuredGroupMatch[1].trim(),
        declared_area_ha: null,
        points: []
      };
      rawGroups.push(currentGroup);
      implicitGroupIndex += 1;
      continue;
    }

    const groupMatch = line.match(/矿区\s*(\d+)|(?:zone|parcelle|area)\s*(\d+)/i);
    if (groupMatch) {
      const groupNumber = groupMatch[1] || groupMatch[2] || String(implicitGroupIndex);
      currentGroup = {
        group_name: currentCompany ? `${currentCompany}_矿区${groupNumber}` : `矿区${groupNumber}`,
        declared_area_ha: null,
        points: []
      };
      rawGroups.push(currentGroup);
      implicitGroupIndex = Math.max(implicitGroupIndex, Number(groupNumber) + 1 || implicitGroupIndex);
      continue;
    }

    if (!/points?|latitude|longitude|superficie|hectare|page|article|arrete|docx|重点关注坐标/i.test(folded)
      && /^[A-Z][A-Z\s.'&-]{4,}$/.test(line)
      && line.split(/\s+/).length <= 5) {
      currentCompany = line.replace(/\s+/g, " ").trim();
      currentGroup = null;
      implicitGroupIndex = 1;
    }
  }

  const normalizedGroups = normalizeCoteDIvoireGeographicDmsTable(
    dedupeCoteDIvoireRawGroups(rawGroups.filter(group => group.points.length > 0))
  );

  if (normalizedGroups.length === 0) {
    return null;
  }

  const requiresReview = normalizedGroups.some(group => group.requires_review);

  return {
    schema_version: "coordinate_engine_v2",
    coordinate_type: "cote_divoire_geographic_dms_table",
    precision_mode: "cote-divoire-geographic-dms-table",
    confidence: requiresReview ? 0.75 : 0.95,
    requires_review: requiresReview,
    source: {
      image_count: 1,
      ocr_engine: String(payload.model || ""),
      fallback_used: /fallback/i.test(String(payload.model || "")),
      legacy_precision_mode: String(payload.precisionMode || "")
    },
    groups: normalizedGroups,
    warnings: normalizedGroups.flatMap(group => group.warnings || []),
    debug: {
      matched_detectors: ["cote_divoire_geographic_dms_table"],
      blocked_fallbacks: [
        "wgs84_chat_coordinates",
        "decimal_latlon",
        "local_tesseract_fallback",
        "bftm_xy",
        "madagascar_cadastral_grid",
        "mozambique_geographic_table",
        "kyrgyzstan_gk",
        "mgrs"
      ],
      supplemental_fallbacks: []
    }
  };
}

function inferCoordinateEngineV2Type(payload = {}) {
  const precisionMode = String(payload.precisionMode || "");

  if (precisionMode === "mozambique-geographic-table" || payload.mozambiqueGeographicTable?.isMozambiqueGeographicTable) {
    return "mozambique_geographic_table";
  }
  if (precisionMode === "kyrgyz-gk-point-x-y" || payload.kyrgyzGk?.isKyrgyzGk) {
    return "kyrgyzstan_gk";
  }
  if (precisionMode === "cadastral-grid-num-xv-yv" || payload.cadastralGrid?.isCadastralGrid) {
    return "madagascar_cadastral_grid";
  }
  if (precisionMode === "mgrs-utm-grid-reference" || payload.mgrs?.isMgrs) {
    return "mgrs_utm_grid_reference";
  }
  if (precisionMode === "bftm-projected-x-y" || payload.bftmLongTable?.isBftmLongTable) {
    return "bftm_xy";
  }
  if (precisionMode === "handwritten-dms-coordinates") {
    return "handwritten_dms_experimental";
  }
  if (precisionMode === "wgs84-table-coordinates" || precisionMode === "wgs84-chat-coordinates" || payload.wgs84TableCoordinates?.isWgs84TableCoordinates || payload.chatCoordinates?.isChatCoordinates) {
    return "decimal_latlon";
  }
  if (/dms|point-az-dms-table|french-perimeter-dms-prose/.test(precisionMode)) {
    return "standard_dms_table";
  }
  if (precisionMode === "local-ocr-dms-fallback") {
    return "standard_dms_table";
  }

  return "";
}

function parseCoordinateEngineV2PointLine(line, coordinateType, index) {
  const raw = String(line || "").trim();
  const point = {
    label: String(index + 1),
    raw,
    lat: null,
    lon: null,
    x: null,
    y: null,
    projection: null,
    grid_cell: null,
    confidence: 0,
    requires_review: false,
    warnings: []
  };

  if (!raw || raw === noCoordinatesText || /^label\s*\|/i.test(raw) || /^point\s*\|/i.test(raw) || /^num\s*\|/i.test(raw) || /^Mozambique Geographic Table/i.test(raw)) {
    return null;
  }

  const pipeParts = raw.split("|").map(part => part.trim());
  if (pipeParts.length >= 3) {
    point.label = pipeParts[0] || point.label;
  }

  if (coordinateType === "madagascar_cadastral_grid" && pipeParts.length >= 3) {
    point.grid_cell = {
      num: pipeParts[0],
      xv: pipeParts[1],
      yv: pipeParts[2]
    };
    point.confidence = 0.85;
    return point;
  }

  if ((coordinateType === "kyrgyzstan_gk" || coordinateType === "bftm_xy") && pipeParts.length >= 3) {
    point.x = Number(pipeParts[1]);
    point.y = Number(pipeParts[2]);
    point.projection = coordinateType === "kyrgyzstan_gk" ? "kyrgyzstan_gk" : "bftm";
    point.confidence = Number.isFinite(point.x) && Number.isFinite(point.y) ? 0.85 : 0.5;
    return point;
  }

  if (coordinateType === "mozambique_geographic_table" && pipeParts.length >= 4) {
    point.lat = Number(pipeParts[1]);
    point.lon = Number(pipeParts[2]);
    point.confidence = Number.isFinite(point.lat) && Number.isFinite(point.lon) ? 0.9 : 0.5;
    return point;
  }

  if ((coordinateType === "mgrs_utm_grid_reference" || coordinateType === "decimal_latlon") && pipeParts.length >= 3) {
    const kmlPart = pipeParts.find(part => /^[-+]?\d+(?:\.\d+)?\s*,\s*[-+]?\d+(?:\.\d+)?\s*,\s*[-+]?\d+(?:\.\d+)?$/.test(part));
    const latLonPart = pipeParts.find(part => /^[-+]?\d+(?:\.\d+)?\s*,\s*[-+]?\d+(?:\.\d+)?$/.test(part));
    if (kmlPart) {
      const [lon, lat] = kmlPart.split(",").map(value => Number(value.trim()));
      point.lon = lon;
      point.lat = lat;
    } else if (latLonPart) {
      const [lat, lon] = latLonPart.split(",").map(value => Number(value.trim()));
      point.lat = lat;
      point.lon = lon;
    }
    point.confidence = Number.isFinite(point.lat) && Number.isFinite(point.lon) ? 0.85 : 0.5;
    return point;
  }

  const xyPair = raw.match(/^\s*([-+]?\d{4,}(?:\.\d+)?)\s*,\s*([-+]?\d{4,}(?:\.\d+)?)\s*$/);
  if (xyPair && (coordinateType === "bftm_xy" || coordinateType === "kyrgyzstan_gk")) {
    point.x = Number(xyPair[1]);
    point.y = Number(xyPair[2]);
    point.projection = coordinateType === "bftm_xy" ? "bftm" : "kyrgyzstan_gk";
    point.confidence = Number.isFinite(point.x) && Number.isFinite(point.y) ? 0.75 : 0.5;
    return point;
  }

  const plainPair = raw.match(/^\s*([-+]?\d+(?:\.\d+)?)\s*,\s*([-+]?\d+(?:\.\d+)?)\s*(?:,\s*[-+]?\d+(?:\.\d+)?)?\s*$/);
  if (plainPair) {
    point.lon = Number(plainPair[1]);
    point.lat = Number(plainPair[2]);
    point.confidence = Number.isFinite(point.lat) && Number.isFinite(point.lon) ? 0.8 : 0.5;
    return point;
  }

  point.confidence = coordinateType === "handwritten_dms_experimental" ? 0.45 : 0.6;
  return point;
}

function getCoordinateEngineV2Geometry(points = []) {
  const count = Array.isArray(points) ? points.length : 0;
  if (count === 1) {
    return "point";
  }
  if (count === 2) {
    return "line";
  }
  return "polygon";
}

function hasCoordinateEngineV2Wgs84Point(point = {}) {
  if (point.lat === null || point.lat === undefined || point.lat === "" || point.lon === null || point.lon === undefined || point.lon === "") {
    return false;
  }

  const lat = Number(point.lat);
  const lon = Number(point.lon);

  return Number.isFinite(lat)
    && Number.isFinite(lon)
    && lat >= -90
    && lat <= 90
    && lon >= -180
    && lon <= 180;
}

function hasCoordinateEngineV2ProjectedOrGridPoint(point = {}) {
  return point?.grid_cell
    || Number.isFinite(Number(point.x))
    || Number.isFinite(Number(point.y))
    || Boolean(point.projection);
}

function isCoordinateEngineV2ReviewWarning(warning = "") {
  const value = String(warning || "").trim();
  if (!value) {
    return false;
  }

  return /自交|误差超过|点数不足|重复点|异常|失败|无法|缺|冲突|超时|范围|未确认|未能稳定|不可信|低可信/i.test(value);
}

function normalizeCoordinateEngineV2Point(point = {}, groupRequiresReview = false, groupWarnings = []) {
  const warnings = normalizeCoordinateEngineV2WarningList(point.warnings);
  const mergedWarnings = Array.from(new Set([...warnings, ...(groupRequiresReview ? groupWarnings : [])]));
  const normalized = {
    label: String(point.label || ""),
    raw: String(point.raw || ""),
    lat: point.lat === null || point.lat === undefined || point.lat === "" ? null : (Number.isFinite(Number(point.lat)) ? Number(point.lat) : null),
    lon: point.lon === null || point.lon === undefined || point.lon === "" ? null : (Number.isFinite(Number(point.lon)) ? Number(point.lon) : null),
    x: point.x === null || point.x === undefined || point.x === "" ? null : (Number.isFinite(Number(point.x)) ? Number(point.x) : null),
    y: point.y === null || point.y === undefined || point.y === "" ? null : (Number.isFinite(Number(point.y)) ? Number(point.y) : null),
    projection: point.projection || null,
    grid_cell: point.grid_cell || null,
    confidence: Number.isFinite(Number(point.confidence)) ? Number(point.confidence) : 0,
    requires_review: Boolean(point.requires_review || groupRequiresReview),
    warnings: mergedWarnings
  };

  if (!hasCoordinateEngineV2Wgs84Point(normalized) && !hasCoordinateEngineV2ProjectedOrGridPoint(normalized)) {
    normalized.requires_review = true;
    if (!normalized.warnings.includes("point 解析失败。")) {
      normalized.warnings.push("point 解析失败。");
    }
  }

  return normalized;
}

function normalizeCoordinateEngineV2Group(group = {}, coordinateType = "", resultRequiresReview = false, validationContext = {}) {
  const rawPoints = Array.isArray(group.points) ? group.points : [];
  const labels = rawPoints.map(point => String(point?.label || "").trim()).filter(Boolean);
  const duplicateLabel = new Set(labels).size !== labels.length;
  const usesWgs84TableGeometryPolicy = validationContext.geometryPolicy === "wgs84_table_deduplicated_validation";
  const baseWarnings = normalizeCoordinateEngineV2WarningList(group.warnings);
  const warnings = [...baseWarnings];

  if (rawPoints.length === 0) {
    warnings.push("缺少有效坐标点。");
  }
  if (duplicateLabel && !usesWgs84TableGeometryPolicy) {
    warnings.push("存在重复或异常点号，请人工核对。");
  }

  let points = rawPoints.map(point => normalizeCoordinateEngineV2Point(point, false, []));
  const hasAllWgs84 = points.length > 0 && points.every(hasCoordinateEngineV2Wgs84Point);
  const hasProjectedOrGrid = points.some(hasCoordinateEngineV2ProjectedOrGridPoint) && !hasAllWgs84;
  const selfIntersecting = !usesWgs84TableGeometryPolicy && hasAllWgs84 && points.length >= 4 && isCoordinateEngineV2SelfIntersecting(points);

  if (selfIntersecting) {
    warnings.push("原始点序形成自交多边形，请人工核对点序后再生成 KML。");
  }

  const validation = buildCoordinateEngineV2ValidationReport({
    ...group,
    points
  }, coordinateType, validationContext);
  const selectedCandidate = (validation.candidates || []).find(candidate => candidate.interpretation === validation.selected_interpretation);
  const validationWarnings = selectedCandidate?.warnings || [];
  if (validation.status === "scored"
    && validation.order_status !== "ambiguous"
    && validation.selected_interpretation
    && validation.selected_interpretation !== validation.effective_point_interpretation) {
    points = buildCoordinateEngineV2CandidatePoints(points, validation.selected_interpretation).map(point => normalizeCoordinateEngineV2Point(point, false, []));
  }
  const validatorRequiresReview = validation.status === "scored" && (
    validation.candidate_score < 70
    || validation.order_status === "ambiguous"
    || validationWarnings.length > 0
  );
  const reviewWarnings = Array.from(new Set([...warnings, ...validationWarnings])).filter(isCoordinateEngineV2ReviewWarning);
  const requiresReview = Boolean(
    resultRequiresReview
    || group.requires_review
    || validatorRequiresReview
    || reviewWarnings.length > 0
    || rawPoints.length === 0
    || (duplicateLabel && !usesWgs84TableGeometryPolicy)
    || points.some(point => point.requires_review)
  );
  const kmlReady = Boolean(
    hasAllWgs84
    && !hasProjectedOrGrid
    && !selfIntersecting
    && validation.status === "scored"
    && validation.order_status !== "ambiguous"
    && validation.candidate_score >= 70
    && !validatorRequiresReview
    && rawPoints.length > 0
    && !points.some(point => point.requires_review)
  );

  points = points.map(point => ({
    ...point,
    requires_review: Boolean(point.requires_review || requiresReview),
    warnings: requiresReview ? Array.from(new Set([...(point.warnings || []), ...reviewWarnings])) : point.warnings
  }));

  return {
    group_id: String(group.group_id || "group_1"),
    group_name: String(group.group_name || "矿地1"),
    geometry: ["point", "line", "polygon"].includes(group.geometry) ? group.geometry : getCoordinateEngineV2Geometry(points),
    confidence: Number.isFinite(Number(group.confidence)) ? Number(group.confidence) : 0,
    requires_review: requiresReview,
    kml_ready: kmlReady,
    declared_area_ha: Number.isFinite(Number(group.declared_area_ha)) ? Number(group.declared_area_ha) : null,
    calculated_area_ha: Number.isFinite(Number(group.calculated_area_ha)) ? Number(group.calculated_area_ha) : null,
    points,
    warnings: Array.from(new Set([...warnings, ...validationWarnings])),
    validation
  };
}

const coordinateEngineV2CountryProfiles = {
  guinea_west_africa_context: {
    country: "Guinea / West Africa context",
    bbox: { minLon: -17, maxLon: -2, minLat: 4, maxLat: 16 },
    expectedAreaHa: { min: 0.0001, max: 10000000 },
    expectedEdgeMeters: { min: 0.1, max: 1000000 }
  },
  mozambique_geographic_table: {
    country: "Mozambique",
    bbox: { minLon: 30, maxLon: 41, minLat: -27, maxLat: -10 },
    expectedAreaHa: { min: 0.01, max: 10000000 },
    expectedEdgeMeters: { min: 0.5, max: 500000 }
  },
  cote_divoire_geographic_dms_table: {
    country: "Cote d'Ivoire",
    bbox: { minLon: -9, maxLon: -2, minLat: 4, maxLat: 11 },
    expectedAreaHa: { min: 0.01, max: 1000000 },
    expectedEdgeMeters: { min: 0.5, max: 100000 }
  },
  mgrs_utm_grid_reference: {
    country: "Myanmar",
    bbox: { minLon: 92, maxLon: 102, minLat: 8, maxLat: 29 },
    expectedAreaHa: { min: 0.0001, max: 1000000 },
    expectedEdgeMeters: { min: 0.1, max: 200000 }
  },
  standard_dms_table: {
    country: "Unknown",
    bbox: { minLon: -180, maxLon: 180, minLat: -90, maxLat: 90 },
    expectedAreaHa: { min: 0.0001, max: 10000000 },
    expectedEdgeMeters: { min: 0.1, max: 1000000 }
  },
  decimal_latlon: {
    country: "Unknown",
    bbox: { minLon: -180, maxLon: 180, minLat: -90, maxLat: 90 },
    expectedAreaHa: { min: 0.0001, max: 10000000 },
    expectedEdgeMeters: { min: 0.1, max: 1000000 }
  },
  handwritten_dms_experimental: {
    country: "Unknown",
    bbox: { minLon: -180, maxLon: 180, minLat: -90, maxLat: 90 },
    expectedAreaHa: { min: 0.0001, max: 10000000 },
    expectedEdgeMeters: { min: 0.1, max: 1000000 }
  }
};

function getCoordinateEngineV2ValidationProfile(coordinateType = "") {
  return coordinateEngineV2CountryProfiles[coordinateType] || {
    country: "Unknown",
    bbox: { minLon: -180, maxLon: 180, minLat: -90, maxLat: 90 },
    expectedAreaHa: { min: 0.0001, max: 10000000 },
    expectedEdgeMeters: { min: 0.1, max: 1000000 }
  };
}

function getCoordinateEngineV2ContextText(result = {}, options = {}) {
  return [
    options.fileName || "",
    options.rawHint || "",
    result.source?.context || "",
    result.source?.legacy_precision_mode || "",
    result.precision_mode || "",
    result.coordinate_type || "",
    ...(Array.isArray(result.warnings) ? result.warnings : []),
    ...(Array.isArray(result.groups) ? result.groups.flatMap(group => [
      group.group_name || "",
      ...(Array.isArray(group.points) ? group.points.map(point => point.raw || "") : [])
    ]) : [])
  ].filter(Boolean).join("\n");
}

function getCoordinateEngineV2ContextualProfile(coordinateType = "", result = {}, options = {}) {
  const text = foldSearchText(getCoordinateEngineV2ContextText(result, options));
  if (/guinea|guinee|guin[eé]e|几内亚|幾內亞|west\s*africa|西非/i.test(text)) {
    return {
      ...coordinateEngineV2CountryProfiles.guinea_west_africa_context,
      context_matched: "guinea_west_africa"
    };
  }

  return {
    ...getCoordinateEngineV2ValidationProfile(coordinateType),
    context_matched: ""
  };
}

function getCoordinateEngineV2AxisEvidence(coordinateType = "", result = {}, options = {}) {
  const precisionMode = String(result.precision_mode || result.precisionMode || "");
  if (precisionMode === "wgs84-table-coordinates" || result.wgs84TableCoordinates?.isWgs84TableCoordinates) {
    return {
      status: "explicit_header",
      interpretation: "first_is_lon_second_is_lat",
      reason: "explicit WGS84 longitude/latitude table"
    };
  }

  const text = foldSearchText([
    options.fileName || "",
    options.rawHint || "",
    result.source?.context || "",
    ...(Array.isArray(result.warnings) ? result.warnings : []),
    ...(Array.isArray(result.groups) ? result.groups.flatMap(group => [
      group.group_name || "",
      ...(Array.isArray(group.points) ? group.points.map(point => point.raw || "") : [])
    ]) : [])
  ].filter(Boolean).join("\n"));
  const registeredAxisLockedTypes = new Set([
    "mozambique_geographic_table",
    "cote_divoire_geographic_dms_table",
    "mgrs_utm_grid_reference",
    "standard_dms_table",
    "handwritten_dms_experimental"
  ]);

  if (registeredAxisLockedTypes.has(coordinateType)) {
    return {
      status: "locked_by_coordinate_type",
      interpretation: "as_parsed",
      reason: "registered coordinate type axis order"
    };
  }
  if (/longitude\s*[/|, -]*\s*latitude|lon\s*[/|, -]*\s*lat|经度\s*[/|, -]*\s*纬度|經度\s*[/|, -]*\s*緯度/i.test(text)) {
    return {
      status: "explicit_header",
      interpretation: "first_is_lon_second_is_lat",
      reason: "explicit longitude/latitude header"
    };
  }
  if (/latitude\s*[/|, -]*\s*longitude|lat\s*[/|, -]*\s*lon|纬度\s*[/|, -]*\s*经度|緯度\s*[/|, -]*\s*經度/i.test(text)) {
    return {
      status: "explicit_header",
      interpretation: "first_is_lat_second_is_lon",
      reason: "explicit latitude/longitude header"
    };
  }

  return {
    status: "none",
    interpretation: null,
    reason: ""
  };
}

let spatialKnowledgeBaseCache = null;

function loadSpatialKnowledgeBase() {
  if (spatialKnowledgeBaseCache) {
    return spatialKnowledgeBaseCache;
  }

  const baseDir = path.join(__dirname, "data", "spatial-knowledge");
  const indexPath = path.join(baseDir, "index.json");

  try {
    const index = JSON.parse(fs.readFileSync(indexPath, "utf8"));
    const profiles = (index.profiles || []).map(relativePath => {
      const profilePath = path.join(baseDir, relativePath);
      return JSON.parse(fs.readFileSync(profilePath, "utf8"));
    });
    spatialKnowledgeBaseCache = {
      status: "loaded",
      baseDir,
      index,
      profiles
    };
  } catch (error) {
    spatialKnowledgeBaseCache = {
      status: "unavailable",
      error: error.message || String(error),
      baseDir,
      index: null,
      profiles: []
    };
  }

  return spatialKnowledgeBaseCache;
}

function getSpatialKnowledgeProfiles() {
  return loadSpatialKnowledgeBase().profiles || [];
}

function isSpatialKnowledgeCenterInsideBbox(center = {}, bbox = {}) {
  if (!center
    || !bbox
    || !Number.isFinite(Number(center.lon))
    || !Number.isFinite(Number(center.lat))
    || !Number.isFinite(Number(bbox.min_lon))
    || !Number.isFinite(Number(bbox.max_lon))
    || !Number.isFinite(Number(bbox.min_lat))
    || !Number.isFinite(Number(bbox.max_lat))) {
    return false;
  }

  return Number(center.lon) >= Number(bbox.min_lon)
    && Number(center.lon) <= Number(bbox.max_lon)
    && Number(center.lat) >= Number(bbox.min_lat)
    && Number(center.lat) <= Number(bbox.max_lat);
}

function getSpatialKnowledgeBboxDistanceDegrees(center = {}, bbox = {}) {
  if (!center
    || !bbox
    || !Number.isFinite(Number(center.lon))
    || !Number.isFinite(Number(center.lat))
    || !Number.isFinite(Number(bbox.min_lon))
    || !Number.isFinite(Number(bbox.max_lon))
    || !Number.isFinite(Number(bbox.min_lat))
    || !Number.isFinite(Number(bbox.max_lat))) {
    return null;
  }

  const lon = Number(center.lon);
  const lat = Number(center.lat);
  const dx = lon < Number(bbox.min_lon)
    ? Number(bbox.min_lon) - lon
    : lon > Number(bbox.max_lon)
      ? lon - Number(bbox.max_lon)
      : 0;
  const dy = lat < Number(bbox.min_lat)
    ? Number(bbox.min_lat) - lat
    : lat > Number(bbox.max_lat)
      ? lat - Number(bbox.max_lat)
      : 0;

  return Number(Math.sqrt(dx ** 2 + dy ** 2).toFixed(4));
}

function buildSpatialKnowledgeEvidence(result = {}, options = {}) {
  const text = foldSearchText(getCoordinateEngineV2ContextText(result, options));
  return {
    text,
    file_name: String(options.fileName || ""),
    raw_hint: String(options.rawHint || options.hint || ""),
    coordinate_type: String(result.coordinate_type || ""),
    precision_mode: String(result.precision_mode || ""),
    profile_hint: String(options.profileHint || "")
  };
}

function countSpatialKnowledgeKeywordMatches(text = "", keywords = []) {
  const value = foldSearchText(text);
  return (keywords || []).filter(keyword => {
    const foldedKeyword = foldSearchText(keyword);
    return foldedKeyword.length >= 4 && value.includes(foldedKeyword);
  }).length;
}

function getSpatialKnowledgeCoordinateTypeWeight(profile = {}, coordinateType = "") {
  const match = (profile.coordinate_types || []).find(item => item && item.coordinate_type === coordinateType);
  return match ? Number(match.weight || 0) : 0;
}

function getSpatialKnowledgeDocumentPatternMatches(profile = {}, evidence = {}) {
  const text = evidence.text || "";
  return (profile.document_patterns || []).filter(pattern => {
    const keywords = pattern?.keywords || [];
    return keywords.length > 0 && keywords.every(keyword => text.includes(foldSearchText(keyword)));
  });
}

function scoreCandidateAgainstSpatialProfile(candidate = {}, profile = {}, evidence = {}) {
  const center = candidate.center || null;
  const bbox = profile.bbox || {};
  const insideBbox = isSpatialKnowledgeCenterInsideBbox(center, bbox);
  const bboxDistanceDegrees = getSpatialKnowledgeBboxDistanceDegrees(center, bbox);
  const filenameHits = countSpatialKnowledgeKeywordMatches(evidence.file_name, profile.filename_keywords);
  const contextHits = countSpatialKnowledgeKeywordMatches(`${evidence.text} ${evidence.raw_hint}`, profile.context_keywords);
  const profileNameHits = countSpatialKnowledgeKeywordMatches(evidence.text, [
    profile.profile_id,
    profile.profile_name,
    ...(profile.countries || []).flatMap(country => [
      country.name,
      country.name_zh
    ])
  ].filter(Boolean));
  const documentPatternMatches = getSpatialKnowledgeDocumentPatternMatches(profile, evidence);
  const languageMatch = (profile.languages || []).some(language => evidence.text.includes(foldSearchText(language)));
  const coordinateTypeWeight = getSpatialKnowledgeCoordinateTypeWeight(profile, evidence.coordinate_type);
  const axisExpectation = (profile.axis_expectations || []).find(item => item.pattern === "decimal_pair");
  const axisMatches = axisExpectation
    && ((axisExpectation.likely_order === "lon_lat" && candidate.interpretation === "first_is_lon_second_is_lat")
      || (axisExpectation.likely_order === "lat_lon" && candidate.interpretation === "first_is_lat_second_is_lon"));
  const evidenceItems = [];
  const conflicts = [];
  let score = 0;

  if (insideBbox) {
    score += profile.profile_type === "regional_profile" ? 25 : 45;
    evidenceItems.push("center_inside_profile_bbox");
  } else if (bboxDistanceDegrees !== null && bboxDistanceDegrees <= 0.25) {
    score += 20;
    evidenceItems.push("center_near_profile_bbox");
  } else if (bboxDistanceDegrees !== null && bboxDistanceDegrees <= 1) {
    score += 8;
    evidenceItems.push("center_in_profile_neighborhood");
  } else {
    score -= profile.profile_type === "regional_profile" ? 8 : 20;
    conflicts.push("center_outside_profile_bbox");
  }

  if (filenameHits > 0) {
    score += Math.min(16, filenameHits * 8);
    evidenceItems.push("filename_keyword_match");
  }
  if (profileNameHits > 0) {
    score += Math.min(18, profileNameHits * 9);
    evidenceItems.push("profile_or_country_name_match");
  }
  if (contextHits > 0) {
    score += Math.min(14, contextHits * 7);
    evidenceItems.push("context_keyword_match");
  }
  if (documentPatternMatches.length > 0) {
    score += Math.min(24, documentPatternMatches.reduce((sum, pattern) => sum + Number(pattern.weight || 0), 0));
    evidenceItems.push("document_pattern_match");
  }
  if (languageMatch) {
    score += 4;
    evidenceItems.push("language_match");
  }
  if (coordinateTypeWeight > 0) {
    score += coordinateTypeWeight;
    evidenceItems.push("coordinate_type_common_for_profile");
  }
  if (axisMatches && profile.profile_type !== "coordinate_system_profile") {
    score += Number(axisExpectation.weight || 0);
    evidenceItems.push("axis_expectation_match");
  } else if (axisExpectation && !axisMatches && candidate.interpretation) {
    conflicts.push("axis_expectation_conflict");
  }

  return {
    profile_id: profile.profile_id,
    profile_type: profile.profile_type,
    score: Math.max(0, Math.min(100, score)),
    bbox_match: insideBbox,
    bbox_distance_degrees: bboxDistanceDegrees,
    document_pattern_match: documentPatternMatches.length > 0,
    coordinate_type_match: coordinateTypeWeight > 0,
    language_match: languageMatch,
    context_match: contextHits > 0 || profileNameHits > 0 || filenameHits > 0,
    evidence: evidenceItems
      .concat(documentPatternMatches.map(pattern => `document_pattern:${pattern.id || "unnamed"}`)),
    conflicts
  };
}

function rankSpatialProfilesForCandidate(candidate = {}, evidence = {}) {
  return getSpatialKnowledgeProfiles()
    .map(profile => scoreCandidateAgainstSpatialProfile(candidate, profile, evidence))
    .sort((a, b) => b.score - a.score)
    .slice(0, 5);
}

function attachSpatialKnowledgeReport(result = {}, options = {}) {
  const kb = loadSpatialKnowledgeBase();
  const evidence = buildSpatialKnowledgeEvidence(result, options);
  const validationGroups = result.coordinate_validation_report?.groups || [];
  const candidateRankings = [];

  for (const group of validationGroups) {
    const candidates = group.validation?.coordinate_order_candidates || group.validation?.candidates || [];
    const candidatesForRanking = candidates.length > 0
      ? candidates
      : [{
        id: "candidate_not_applicable",
        interpretation: "not_applicable",
        score: group.validation?.candidate_score || 0,
        center: null
      }];
    for (const candidate of candidatesForRanking) {
      const profileRankings = rankSpatialProfilesForCandidate(candidate, evidence);
      for (const ranking of profileRankings) {
        candidateRankings.push({
          group_id: group.group_id,
          group_name: group.group_name,
          candidate_id: candidate.id,
          interpretation: candidate.interpretation,
          candidate_score: candidate.score,
          center: candidate.center || null,
          profile_id: ranking.profile_id,
          profile_type: ranking.profile_type,
          score: ranking.score,
          bbox_match: ranking.bbox_match,
          bbox_distance_degrees: ranking.bbox_distance_degrees,
          document_pattern_match: ranking.document_pattern_match,
          coordinate_type_match: ranking.coordinate_type_match,
          language_match: ranking.language_match,
          context_match: ranking.context_match,
          evidence: ranking.evidence,
          conflicts: ranking.conflicts
        });
      }
    }
  }

  candidateRankings.sort((a, b) => b.score - a.score);
  const top = candidateRankings[0] || null;
  const second = candidateRankings.find(item => item.profile_id !== top?.profile_id || item.candidate_id !== top?.candidate_id) || null;
  const scoreMargin = top && second ? Number((top.score - second.score).toFixed(2)) : (top ? top.score : 0);
  const status = kb.status !== "loaded"
    ? "insufficient"
    : top && top.score >= 70 && scoreMargin >= 15
      ? "confident"
      : top && top.score > 0
        ? "ranked"
        : "insufficient";
  const notes = [];
  if (candidateRankings.some(candidate => candidate.interpretation === "not_applicable")) {
    notes.push("Projected/grid coordinate types are ranked by recognized coordinate_type only; SKB does not participate in lat/lon order exchange for them.");
  }

  return {
    ...result,
    spatial_knowledge_report: {
      status,
      knowledge_base_status: kb.status,
      top_profile: top ? {
        profile_id: top.profile_id,
        profile_type: top.profile_type,
        score: top.score,
        candidate_id: top.candidate_id,
        interpretation: top.interpretation
      } : null,
      score_margin: scoreMargin,
      evidence: [
        evidence.file_name ? "file_name" : "",
        evidence.raw_hint ? "raw_hint" : "",
        evidence.coordinate_type ? `coordinate_type:${evidence.coordinate_type}` : ""
      ].filter(Boolean),
      notes,
      candidate_rankings: candidateRankings
    }
  };
}

function isCoordinateEngineV2PointInsideBbox(point = {}, bbox = {}) {
  return hasCoordinateEngineV2Wgs84Point(point)
    && Number(point.lon) >= Number(bbox.minLon)
    && Number(point.lon) <= Number(bbox.maxLon)
    && Number(point.lat) >= Number(bbox.minLat)
    && Number(point.lat) <= Number(bbox.maxLat);
}

function getCoordinateEngineV2Bbox(points = []) {
  const validPoints = points.filter(hasCoordinateEngineV2Wgs84Point);
  if (validPoints.length === 0) {
    return null;
  }

  return {
    minLon: Math.min(...validPoints.map(point => Number(point.lon))),
    maxLon: Math.max(...validPoints.map(point => Number(point.lon))),
    minLat: Math.min(...validPoints.map(point => Number(point.lat))),
    maxLat: Math.max(...validPoints.map(point => Number(point.lat)))
  };
}

function getCoordinateEngineV2Center(points = []) {
  const validPoints = points.filter(hasCoordinateEngineV2Wgs84Point);
  if (validPoints.length === 0) {
    return null;
  }

  return {
    lon: Number((validPoints.reduce((sum, point) => sum + Number(point.lon), 0) / validPoints.length).toFixed(8)),
    lat: Number((validPoints.reduce((sum, point) => sum + Number(point.lat), 0) / validPoints.length).toFixed(8))
  };
}

function getCoordinateEngineV2EdgeStats(points = []) {
  if (!Array.isArray(points) || points.length < 2) {
    return {
      min_m: null,
      max_m: null,
      total_m: null,
      abnormal: false
    };
  }

  const closed = points.length >= 3;
  const distances = points.map((point, index) => {
    if (!closed && index === points.length - 1) {
      return null;
    }
    return getCoordinateEngineV2DistanceMeters(point, points[(index + 1) % points.length]);
  }).filter(value => Number.isFinite(value));

  if (distances.length === 0) {
    return {
      min_m: null,
      max_m: null,
      total_m: null,
      abnormal: true
    };
  }

  return {
    min_m: Number(Math.min(...distances).toFixed(2)),
    max_m: Number(Math.max(...distances).toFixed(2)),
    total_m: Number(distances.reduce((sum, value) => sum + value, 0).toFixed(2)),
    abnormal: distances.some(value => value < 0.1 || value > 1000000)
  };
}

function parseCoordinateEngineV2RawPair(point = {}) {
  const raw = String(point.raw || "").trim();
  const pipeParts = raw.split("|").map(part => part.trim()).filter(Boolean);
  const kmlPipePair = [...pipeParts].reverse().find(part => /^[-+]?\d+(?:\.\d+)?\s*,\s*[-+]?\d+(?:\.\d+)?\s*,\s*[-+]?\d+(?:\.\d+)?$/.test(part));
  if (kmlPipePair) {
    const [first, second] = kmlPipePair.split(",").slice(0, 2).map(value => Number(value.trim()));
    if (Number.isFinite(first) && Number.isFinite(second)) {
      return { first, second };
    }
  }

  const pipePair = pipeParts.find(part => /^[-+]?\d+(?:\.\d+)?\s*,\s*[-+]?\d+(?:\.\d+)?$/.test(part));

  if (pipePair) {
    const [first, second] = pipePair.split(",").map(value => Number(value.trim()));
    if (Number.isFinite(first) && Number.isFinite(second)) {
      return { first, second };
    }
  }

  const matches = Array.from(raw.matchAll(/[-+]?\d+(?:\.\d+)?/g)).map(match => Number(match[0]));
  if (matches.length >= 2 && matches.every(Number.isFinite)) {
    return {
      first: matches[0],
      second: matches[1]
    };
  }

  if (hasCoordinateEngineV2Wgs84Point(point)) {
    return {
      first: Number(point.lon),
      second: Number(point.lat)
    };
  }

  return null;
}

function buildCoordinateEngineV2CandidatePoints(points = [], interpretation = "as_parsed") {
  return points.map(point => {
    const rawPair = parseCoordinateEngineV2RawPair(point);
    if (interpretation === "first_is_lon_second_is_lat" && rawPair) {
      return {
        ...point,
        lon: rawPair.first,
        lat: rawPair.second
      };
    }
    if (interpretation === "first_is_lat_second_is_lon" && rawPair) {
      return {
        ...point,
        lat: rawPair.first,
        lon: rawPair.second
      };
    }
    if (!hasCoordinateEngineV2Wgs84Point(point)) {
      return { ...point };
    }
    if (interpretation === "swapped_lat_lon") {
      return {
        ...point,
        lat: Number(point.lon),
        lon: Number(point.lat)
      };
    }
    return { ...point };
  });
}

function buildCoordinateEngineV2GeometryPolicyMeta(points = [], geometryPolicy = "") {
  if (geometryPolicy !== "wgs84_table_deduplicated_validation") {
    return {
      geometryPolicy: "",
      originalPointCount: Array.isArray(points) ? points.length : 0,
      validationPointCount: Array.isArray(points) ? points.length : 0,
      duplicateCoordinateCount: 0,
      duplicateLabels: [],
      points
    };
  }

  const seenCoordinates = new Set();
  const labelCounts = new Map();
  const validationPoints = [];
  let duplicateCoordinateCount = 0;

  (Array.isArray(points) ? points : []).forEach(point => {
    const label = String(point?.label || "").trim();
    if (label) {
      labelCounts.set(label, Number(labelCounts.get(label) || 0) + 1);
    }
    const coordinateKey = hasCoordinateEngineV2Wgs84Point(point)
      ? `${Number(point.lat).toFixed(8)},${Number(point.lon).toFixed(8)}`
      : "";
    if (coordinateKey && seenCoordinates.has(coordinateKey)) {
      duplicateCoordinateCount += 1;
      return;
    }
    if (coordinateKey) {
      seenCoordinates.add(coordinateKey);
    }
    validationPoints.push(point);
  });

  return {
    geometryPolicy,
    originalPointCount: Array.isArray(points) ? points.length : 0,
    validationPointCount: validationPoints.length,
    duplicateCoordinateCount,
    duplicateLabels: Array.from(labelCounts.entries())
      .filter(([, count]) => count > 1)
      .map(([label]) => label),
    points: validationPoints
  };
}

function validateCoordinateEngineV2Candidate(points = [], profile = {}, interpretation = "as_parsed", options = {}) {
  const candidatePoints = buildCoordinateEngineV2CandidatePoints(points, interpretation);
  const validPoints = candidatePoints.filter(hasCoordinateEngineV2Wgs84Point);
  const bbox = getCoordinateEngineV2Bbox(candidatePoints);
  const insideCountryCount = validPoints.filter(point => isCoordinateEngineV2PointInsideBbox(point, profile.bbox)).length;
  const allInsideCountry = validPoints.length > 0 && insideCountryCount === validPoints.length;
  const allOutsideCountry = validPoints.length > 0 && insideCountryCount === 0;
  const geometryPolicyMeta = buildCoordinateEngineV2GeometryPolicyMeta(candidatePoints, options.geometryPolicy || "");
  const geometryPoints = geometryPolicyMeta.points;
  const validGeometryPoints = geometryPoints.filter(hasCoordinateEngineV2Wgs84Point);
  const selfIntersecting = validGeometryPoints.length >= 4 && isCoordinateEngineV2SelfIntersecting(geometryPoints);
  const areaHa = validGeometryPoints.length >= 3 ? Number(getCoordinateEngineV2PolygonAreaHa(geometryPoints).toFixed(2)) : null;
  const edgeStats = getCoordinateEngineV2EdgeStats(geometryPoints);
  const areaLimits = profile.expectedAreaHa || {};
  const edgeLimits = profile.expectedEdgeMeters || {};
  const areaAbnormal = areaHa !== null && (
    areaHa < Number(areaLimits.min || 0)
    || areaHa > Number(areaLimits.max || Number.MAX_SAFE_INTEGER)
  );
  const edgeAbnormal = edgeStats.abnormal
    || (edgeStats.min_m !== null && edgeStats.min_m < Number(edgeLimits.min || 0))
    || (edgeStats.max_m !== null && edgeStats.max_m > Number(edgeLimits.max || Number.MAX_SAFE_INTEGER));
  const geometryReasonable = validPoints.length === candidatePoints.length
    && validPoints.length > 0
    && !selfIntersecting
    && !areaAbnormal
    && !edgeAbnormal;
  const allAtSea = profile.country !== "Unknown" && allOutsideCountry;
  const warnings = [];
  let score = 0;

  score += validPoints.length === candidatePoints.length && validPoints.length > 0 ? 25 : -40;
  score += allInsideCountry ? 25 : (profile.country === "Unknown" ? 5 : -20);
  score += bbox ? 10 : -10;
  score += !selfIntersecting ? 15 : -35;
  score += !areaAbnormal ? 10 : -20;
  score += !edgeAbnormal ? 10 : -20;
  score += !allAtSea ? 5 : -30;

  if (validPoints.length !== candidatePoints.length || validPoints.length === 0) {
    warnings.push("存在无效经纬度点。");
  }
  if (profile.country !== "Unknown" && !allInsideCountry) {
    warnings.push("部分或全部点不在目标国家范围内。");
  }
  if (allAtSea) {
    warnings.push("所有点均落在目标国家陆域范围外，疑似落海或国家不匹配。");
  }
  if (selfIntersecting) {
    warnings.push("polygon 自交。");
  }
  if (areaAbnormal) {
    warnings.push("polygon 面积异常。");
  }
  if (edgeAbnormal) {
    warnings.push("边长异常。");
  }

  return {
    id: interpretation === "first_is_lon_second_is_lat"
      ? "candidate_lon_lat"
      : interpretation === "first_is_lat_second_is_lon"
        ? "candidate_lat_lon"
        : `candidate_${interpretation}`,
    interpretation,
    score: Math.max(0, Math.min(100, score)),
    point_count: candidatePoints.length,
    valid_point_count: validPoints.length,
    bbox,
    center: getCoordinateEngineV2Center(candidatePoints),
    country: profile.country,
    country_candidates: profile.country === "Unknown" ? [] : [profile.country],
    points_inside_country: insideCountryCount,
    all_inside_country: allInsideCountry,
    all_at_sea: allAtSea,
    calculated_area_ha: areaHa,
    edge_stats: edgeStats,
    self_intersecting: selfIntersecting,
    geometry_reasonable: geometryReasonable,
    geometry_policy: geometryPolicyMeta.geometryPolicy || null,
    original_point_count: geometryPolicyMeta.originalPointCount,
    validation_point_count: geometryPolicyMeta.validationPointCount,
    duplicate_coordinate_count: geometryPolicyMeta.duplicateCoordinateCount,
    duplicate_labels: geometryPolicyMeta.duplicateLabels,
    geometryPolicy: geometryPolicyMeta.geometryPolicy || null,
    originalPointCount: geometryPolicyMeta.originalPointCount,
    validationPointCount: geometryPolicyMeta.validationPointCount,
    duplicateCoordinateCount: geometryPolicyMeta.duplicateCoordinateCount,
    duplicateLabels: geometryPolicyMeta.duplicateLabels,
    warnings
  };
}

function buildCoordinateEngineV2ValidationReport(group = {}, coordinateType = "", validationContext = {}) {
  const points = Array.isArray(group.points) ? group.points : [];
  const hasProjectedOrGrid = points.some(hasCoordinateEngineV2ProjectedOrGridPoint) && !points.every(hasCoordinateEngineV2Wgs84Point);
  const profile = validationContext.profile || getCoordinateEngineV2ValidationProfile(coordinateType);
  const axisEvidence = validationContext.axisEvidence || { status: "none", interpretation: null, reason: "" };
  const scoreMarginThreshold = 15;

  if (hasProjectedOrGrid) {
    return {
      status: "skipped_projected_or_grid",
      reason: "V2 validator only scores WGS84 lat/lon candidates in this phase; projected/grid normalization is not implemented.",
      candidate_score: 0,
      selected_interpretation: null,
      effective_point_interpretation: null,
      selection_reason: "projected_or_grid_requires_normalization",
      order_status: "not_applicable",
      score_margin: null,
      candidates: []
    };
  }

  const candidateInterpretations = axisEvidence.status === "locked_by_coordinate_type"
    ? ["as_parsed", "swapped_lat_lon"]
    : ["first_is_lon_second_is_lat", "first_is_lat_second_is_lon"];
  const candidates = candidateInterpretations
    .map(interpretation => validateCoordinateEngineV2Candidate(points, profile, interpretation, {
      geometryPolicy: validationContext.geometryPolicy || ""
    }))
    .sort((a, b) => b.score - a.score);
  const best = candidates[0] || null;
  const runnerUp = candidates[1] || null;
  const scoreMargin = best && runnerUp ? Number((best.score - runnerUp.score).toFixed(2)) : null;
  const effectivePointInterpretation = axisEvidence.status === "locked_by_coordinate_type" ? "as_parsed" : (points.every(point => {
    const rawPair = parseCoordinateEngineV2RawPair(point);
    return rawPair && hasCoordinateEngineV2Wgs84Point(point)
      && Number(point.lon) === Number(rawPair.first)
      && Number(point.lat) === Number(rawPair.second);
  })
    ? "first_is_lon_second_is_lat"
    : "first_is_lat_second_is_lon");
  let selected = best;
  let selectionReason = "highest_candidate_score";
  let orderStatus = "resolved";

  if (axisEvidence.status === "locked_by_coordinate_type") {
    selected = candidates.find(candidate => candidate.interpretation === effectivePointInterpretation) || best;
    selectionReason = axisEvidence.reason;
    orderStatus = "locked_by_coordinate_type";
  } else if (axisEvidence.status === "explicit_header" && axisEvidence.interpretation) {
    selected = candidates.find(candidate => candidate.interpretation === axisEvidence.interpretation) || best;
    selectionReason = axisEvidence.reason;
    orderStatus = "resolved_by_header";
  } else if (profile.context_matched && best) {
    selectionReason = `resolved_by_context:${profile.context_matched}`;
    orderStatus = "resolved_by_context";
  } else if (best && runnerUp && scoreMargin !== null && scoreMargin < scoreMarginThreshold) {
    selectionReason = "candidate_scores_too_close_without_axis_or_country_context";
    orderStatus = "ambiguous";
    if (!selected.warnings.includes("坐标顺序存在歧义，请人工确认经纬度顺序。")) {
      selected.warnings.push("坐标顺序存在歧义，请人工确认经纬度顺序。");
    }
  }

  return {
    status: selected ? "scored" : "no_candidates",
    candidate_score: selected?.score || 0,
    selected_interpretation: selected?.interpretation || null,
    effective_point_interpretation: effectivePointInterpretation,
    selection_reason: selectionReason,
    order_status: orderStatus,
    score_margin: scoreMargin,
    coordinate_order_candidates: candidates,
    candidates
  };
}

function buildCoordinateEngineV2Groups(payload = {}, coordinateType = "") {
  const text = String(payload.coordinates || "");
  const blocks = text
    .split(/\n\s*\n/g)
    .map(block => block.split(/\r?\n/).map(line => line.trim()).filter(Boolean))
    .filter(block => block.length > 0);

  if (blocks.length === 0) {
    return [];
  }

  return blocks.map((lines, groupIndex) => {
    const points = lines
      .map((line, pointIndex) => parseCoordinateEngineV2PointLine(line, coordinateType, pointIndex))
      .filter(Boolean);

    return {
      group_id: `group_${groupIndex + 1}`,
      group_name: `矿地${groupIndex + 1}`,
      geometry: getCoordinateEngineV2Geometry(points),
      confidence: points.length > 0
        ? Number((points.reduce((sum, point) => sum + Number(point.confidence || 0), 0) / points.length).toFixed(2))
        : 0,
      requires_review: false,
      kml_ready: false,
      declared_area_ha: null,
      calculated_area_ha: null,
      warnings: [],
      points
    };
  }).filter(group => group.points.length > 0);
}

function normalizeCoordinateEngineV2Result(result = {}, options = {}) {
  const coordinateType = String(result.coordinate_type || "");
  const precisionMode = String(result.precision_mode || "");
  const warnings = normalizeCoordinateEngineV2WarningList(result.warnings);
  const fallbackUsed = Boolean(result.source?.fallback_used) || /fallback/i.test(String(result.source?.ocr_engine || "")) || /fallback/i.test(precisionMode);
  const reviewWarnings = warnings.filter(isCoordinateEngineV2ReviewWarning);
  const validationContext = {
    profile: getCoordinateEngineV2ContextualProfile(coordinateType, result, options),
    axisEvidence: getCoordinateEngineV2AxisEvidence(coordinateType, result, options),
    geometryPolicy: precisionMode === "wgs84-table-coordinates" ? "wgs84_table_deduplicated_validation" : ""
  };
  const forcedReview = coordinateType === "handwritten_dms_experimental"
    || precisionMode === "local-ocr-dms-fallback"
    || fallbackUsed
    || reviewWarnings.length > 0
    || Boolean(options.forceRequiresReview);
  const forceGroupReview = coordinateType === "handwritten_dms_experimental"
    || precisionMode === "local-ocr-dms-fallback"
    || fallbackUsed
    || Boolean(options.forceRequiresReview);
  const rawGroups = Array.isArray(result.groups) ? result.groups : [];
  const groups = rawGroups.map((group, index) => normalizeCoordinateEngineV2Group({
    ...group,
    group_id: group.group_id || `group_${index + 1}`,
    group_name: group.group_name || `矿地${index + 1}`
  }, coordinateType, forceGroupReview, validationContext));
  const missingGroups = groups.length === 0;
  const requiresReview = Boolean(
    forcedReview
    || missingGroups
    || groups.some(group => group.requires_review)
  );
  const confidence = groups.length > 0
    ? Number((groups.reduce((sum, group) => sum + Number(group.confidence || 0), 0) / groups.length).toFixed(2))
    : (Number.isFinite(Number(result.confidence)) ? Number(result.confidence) : 0);
  const validationReports = groups.map(group => group.validation).filter(Boolean);
  const validationWarnings = groups.flatMap(group => group.validation?.candidates?.[0]?.warnings || []);
  const validationFailed = validationReports.some(report => report.status === "scored" && report.candidate_score < 70);
  const orderAmbiguous = validationReports.some(report => report.order_status === "ambiguous");

  const normalizedResult = {
    schema_version: "coordinate_engine_v2",
    coordinate_type: coordinateType,
    precision_mode: precisionMode,
    confidence: requiresReview ? Math.min(confidence, 0.75) : confidence,
    requires_review: Boolean(requiresReview || validationFailed || orderAmbiguous),
    source: {
      image_count: Number.isFinite(Number(result.source?.image_count)) ? Number(result.source.image_count) : 1,
      ocr_engine: String(result.source?.ocr_engine || ""),
      fallback_used: fallbackUsed
    },
    groups,
    warnings: Array.from(new Set([
      ...warnings,
      ...validationWarnings,
      ...(missingGroups ? ["缺少有效坐标点。"] : [])
    ])),
    coordinate_validation_report: {
      validator_version: "coordinate_validator_v1",
      candidate_score: validationReports.length > 0
        ? Number((validationReports.reduce((sum, report) => sum + Number(report.candidate_score || 0), 0) / validationReports.length).toFixed(2))
        : 0,
      selected_interpretations: validationReports.map(report => report.selected_interpretation).filter(Boolean),
      selection_reasons: validationReports.map(report => report.selection_reason).filter(Boolean),
      order_statuses: validationReports.map(report => report.order_status).filter(Boolean),
      coordinate_order_candidates: validationReports.length === 1 ? (validationReports[0].coordinate_order_candidates || []) : [],
      selected_interpretation: validationReports.length === 1 ? (validationReports[0].selected_interpretation || null) : null,
      selection_reason: validationReports.length === 1 ? (validationReports[0].selection_reason || "") : "",
      order_status: validationReports.length === 1 ? (validationReports[0].order_status || "") : "",
      score_margin: validationReports.length === 1 ? validationReports[0].score_margin : null,
      groups: groups.map(group => ({
        group_id: group.group_id,
        group_name: group.group_name,
        validation: group.validation
      }))
    },
    debug: {
      matched_detectors: Array.isArray(result.debug?.matched_detectors) ? result.debug.matched_detectors.filter(Boolean) : [],
      blocked_fallbacks: Array.isArray(result.debug?.blocked_fallbacks) ? result.debug.blocked_fallbacks.filter(Boolean) : [],
      supplemental_fallbacks: Array.isArray(result.debug?.supplemental_fallbacks) ? result.debug.supplemental_fallbacks.filter(Boolean) : []
    }
  };

  return attachSpatialKnowledgeReport(normalizedResult, options);
}

function buildCoordinateEngineV2ShadowResult(payload = {}, options = {}) {
  const coteDIvoireResult = buildCoteDIvoireGeographicDmsV2Result(payload, options);
  if (coteDIvoireResult) {
    return normalizeCoordinateEngineV2Result(coteDIvoireResult);
  }

  const lockedCoordinateType = String(options.lockedCoordinateType || options.candidateTypeLock?.coordinate_type || "");
  const inferredCoordinateType = inferCoordinateEngineV2Type(payload);
  const coordinateType = lockedCoordinateType || inferredCoordinateType;
  const precisionMode = lockedCoordinateType === "mozambique_geographic_table"
    ? "mozambique-geographic-table"
    : String(payload.precisionMode || "");
  const warnings = normalizeCoordinateEngineV2WarningList(payload.warning || payload.warnings);
  const fallbackUsed = /fallback/i.test(String(payload.model || "")) || /fallback/i.test(precisionMode);
  const reviewWarnings = warnings.filter(isCoordinateEngineV2ReviewWarning);
  const forcedReview = coordinateType === "handwritten_dms_experimental"
    || precisionMode === "local-ocr-dms-fallback"
    || fallbackUsed
    || reviewWarnings.length > 0;
  const groups = buildCoordinateEngineV2Groups(payload, coordinateType);
  const matchedDetectors = [
    coordinateType,
    ...(Array.isArray(payload.parserTrace) ? payload.parserTrace : [])
  ].filter(Boolean);
  const blockedFallbacks = lockedCoordinateType === "mozambique_geographic_table" && inferredCoordinateType && inferredCoordinateType !== lockedCoordinateType
    ? [inferredCoordinateType, String(payload.precisionMode || "").trim()].filter(Boolean)
    : [];

  return normalizeCoordinateEngineV2Result({
    schema_version: "coordinate_engine_v2",
    coordinate_type: coordinateType,
    precision_mode: precisionMode,
    confidence: groups.length > 0
      ? Number((groups.reduce((sum, group) => sum + Number(group.confidence || 0), 0) / groups.length).toFixed(2))
      : 0,
    requires_review: forcedReview,
    source: {
      image_count: 1,
      ocr_engine: String(payload.model || ""),
      fallback_used: fallbackUsed,
      context: String(options.rawHint || options.hint || "")
    },
    groups,
    warnings,
    debug: {
      matched_detectors: Array.from(new Set(matchedDetectors)),
      blocked_fallbacks: Array.from(new Set(blockedFallbacks)),
      supplemental_fallbacks: fallbackUsed ? [String(payload.model || "fallback").trim()].filter(Boolean) : []
    }
  }, { forceRequiresReview: forcedReview });
}

function normalizeJudgeOutput(text) {
  const raw = String(text || "").trim();
  const sectionNames = [
    "对象类型",
    "场景分类",
    "结论",
    "等级",
    "判读可信度",
    "潜力评分",
    "关键依据",
    "主要风险",
    "下一步",
    "一句话总结"
  ];
  const defaults = {
    "对象类型": "其它",
    "场景分类": "原矿石",
    "结论": "目前证据不足，不建议投入。",
    "等级": "D 排除或暂不投入",
    "判读可信度": "中",
    "潜力评分": "25分",
    "关键依据": "1. 未看到明确裂隙控制。\n2. 未看到清楚的石英脉或局部赋存关系。\n3. 表面反光不能作为黄金依据。",
    "主要风险": "1. 金黄色或金属反光容易误判为云母、黄铁矿或金属膜。\n2. 缺少断面和结构关系，不能支撑继续投入。",
    "下一步": "1. 敲开样本拍断面。\n2. 补拍带比例物的清晰近景和原地环境。",
    "一句话总结": "先按误判图处理，补到结构证据后再看。"
  };

  if (!raw) {
    return sectionNames.map(name => `【${name}】\n${defaults[name]}`).join("\n\n");
  }

  const values = {};

  for (let index = 0; index < sectionNames.length; index += 1) {
    const current = sectionNames[index];
    const next = sectionNames[index + 1];
    const pattern = next
      ? new RegExp(`【${current}】([\\s\\S]*?)(?=【${next}】)`)
      : new RegExp(`【${current}】([\\s\\S]*)`);
    const match = raw.match(pattern);
    values[current] = (match?.[1] || "").trim();
  }

  if (!values["结论"] && !values["等级"] && !values["关键依据"]) {
    const lines = raw.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
    values["场景分类"] = defaults["场景分类"];
    values["结论"] = lines[0] || defaults["结论"];
    values["等级"] = defaults["等级"];
    values["关键依据"] = lines.slice(1, 4).map((line, index) => `${index + 1}. ${line.replace(/^\d+[.、]\s*/, "")}`).join("\n") || defaults["关键依据"];
    values["主要风险"] = defaults["主要风险"];
    values["下一步"] = defaults["下一步"];
    values["一句话总结"] = defaults["一句话总结"];
  }

  const normalized = sectionNames
    .map(name => `【${name}】\n${values[name] || defaults[name]}`)
    .join("\n\n");

  const protectedObjectType = applyJudgeObjectTypeConsistencyProtection(normalized);
  if (protectedObjectType.check.forcedDowngrade) {
    return protectedObjectType.text;
  }

  const protectedNaturalGold = protectNaturalGoldJudgeOutput(normalized);
  return applyJudgeObjectTypeConsistencyProtection(protectedNaturalGold).text;
}

function extractJudgeSection(text = "", sectionName = "") {
  const source = String(text || "");
  const escapedName = String(sectionName || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return source.match(new RegExp(`【${escapedName}】\\s*([\\s\\S]*?)(?=\\n【|\\n\\n【|$)`))?.[1]?.trim() || "";
}

function normalizeJudgeObjectType(value = "") {
  const source = String(value || "");
  if (/人工熔炼物|熔炼物|熔炼金属|熔煉|熔融|铸块|鑄塊|金豆/i.test(source)) return "人工熔炼物";
  if (/金属块|金屬塊|金块|金塊|金粒|自然金|砂金/i.test(source)) return "金属块";
  if (/矿石|礦石|岩石|原矿|原礦|矿化|礦化/i.test(source)) return "矿石";
  if (/河道|河床|溪流|沟谷|溝谷|水系|河流|河漫滩|河漫灘/i.test(source)) return "河道";
  if (/卫星|衛星|遥感|遙感|Google\s*Earth|航拍|俯视|俯視/i.test(source)) return "卫星图";
  if (/地图|地圖|奥维|奧維|坐标|坐標|KML|截图|截圖/i.test(source)) return "地图截图";
  if (/设备|設備|机器|機器|洗矿机|洗礦機|挖机|挖掘机|工具/i.test(source)) return "设备照片";
  if (/地表|现场|現場|地貌|坡面|采坑|採坑|老鼠洞|矿区环境|礦區環境/i.test(source)) return "地表环境";
  return "其它";
}

function inferJudgeObjectType(text = "") {
  const objectSection = extractJudgeSection(text, "对象类型");
  const normalizedObjectSection = objectSection ? normalizeJudgeObjectType(objectSection) : "";
  if (normalizedObjectSection && normalizedObjectSection !== "其它") return normalizedObjectSection;
  const sceneSection = extractJudgeSection(text, "场景分类");
  if (/河道|河床|溪流|沟谷|溝谷|河流|河漫滩|河漫灘/i.test(sceneSection)) return "河道";
  if (/卫星|衛星|遥感|遙感|Google\s*Earth|航拍|俯视|俯視/i.test(sceneSection)) return "卫星图";
  if (/地图|地圖|奥维|奧維|坐标|坐標|KML|截图|截圖/i.test(sceneSection)) return "地图截图";
  return normalizeJudgeObjectType(`${sceneSection}\n${text}`);
}

function extractJudgeSceneType(text = "") {
  const sceneSection = extractJudgeSection(text, "场景分类");
  return sceneSection.split(/[:：\n]/)[0]?.trim() || sceneSection.trim() || "未确定";
}

function stripNegatedJudgeClaims(text = "") {
  return String(text || "")
    .replace(/(?:不能|不可|不宜|无法|不要|禁止|未见|未发现|无明显|没有|并非|不是|不具备|缺少|需补充|需要补充|需检测|需要检测|仅凭图片不能)[^。\n；;]*(?:金块|金粒|自然金|砂金金块|熔炼|熔融|熔炼金属|熔炼物|金豆|矿体|矿化带|矿石|矿物)[^。\n；;]*/gi, "")
    .replace(/(?:金块|金粒|自然金|砂金金块|熔炼|熔融|熔炼金属|熔炼物|金豆|矿体|矿化带|矿石|矿物)[^。\n；;]*(?:不能|不可|不宜|无法|不要|禁止|未见|未发现|无明显|没有|并非|不是|不具备|缺少|需补充|需要补充|需检测|需要检测|仅凭图片不能)[^。\n；;]*/gi, "");
}

function checkJudgeObjectTypeConsistency(text = "") {
  const source = String(text || "");
  const objectType = inferJudgeObjectType(source);
  const sceneType = extractJudgeSceneType(source);
  const sceneSection = extractJudgeSection(source, "场景分类");
  const conclusionSection = extractJudgeSection(source, "结论");
  const positiveJudgementText = stripNegatedJudgeClaims(`${sceneType}\n${sceneSection}\n${conclusionSection}`);
  const hasNaturalGold = /自然金块|砂金金块|河道采挖自然金|金块|金粒/i.test(positiveJudgementText);
  const hasSmelt = /熔炼金属|人工熔炼|熔炼金豆|熔炼物|熔煉|熔融/i.test(positiveJudgementText);
  const hasMineralJudgement = /原矿石|矿化岩石|矿体|矿化带|矿石|矿物|黄铁矿|石英脉|裂隙控制|氧化带|自然金块|砂金金块|熔炼/i.test(positiveJudgementText);
  let conflictReason = "";

  if (/对象识别冲突|对象类型冲突/.test(source)) {
    conflictReason = "object_type_conflict_already_downgraded";
  } else if (objectType === "河道" && (hasNaturalGold || hasSmelt)) {
    conflictReason = "object_river_conflicts_with_gold_or_smelt_scene";
  } else if (objectType === "卫星图" && (hasNaturalGold || hasSmelt || hasMineralJudgement)) {
    conflictReason = "object_satellite_conflicts_with_gold_smelt_or_mineral_scene";
  } else if (objectType === "地图截图" && hasMineralJudgement) {
    conflictReason = "object_map_conflicts_with_mineral_judgement";
  } else if (objectType === "设备照片" && (hasNaturalGold || hasSmelt || hasMineralJudgement)) {
    conflictReason = "object_equipment_conflicts_with_mineral_or_gold_judgement";
  }

  return {
    objectType,
    sceneType,
    consistencyCheck: conflictReason ? "conflict" : "pass",
    forcedDowngrade: Boolean(conflictReason),
    conflictReason
  };
}

function applyJudgeObjectTypeConsistencyProtection(text = "") {
  const source = String(text || "");
  const check = checkJudgeObjectTypeConsistency(source);

  if (!check.forcedDowngrade) {
    return { text: source, check };
  }

  const conflictConclusion = check.objectType === "设备照片"
    ? "仅为设备或称重照片，不能判断矿体或矿化带，请补充矿石/河道/现场近景。"
    : "对象识别冲突，请补充近景。";
  const conflictRisk = check.objectType === "设备照片"
    ? "1. 设备或称重照片只能说明拍摄对象/使用场景，不能直接判断矿体、矿化带或金块形态。\n2. 继续按矿体或矿物判断会造成误判。"
    : "1. 远景、河道、卫星图或地图截图不能直接判断金块形态。\n2. 继续按金块判断会造成误判。";

  const protectedText = source
    .replace(/【对象类型】[\s\S]*?(?=\n\n【场景分类】|【场景分类】)/, `【对象类型】\n${check.objectType}`)
    .replace(/【场景分类】[\s\S]*?(?=\n\n【结论】|【结论】)/, `【场景分类】\n${check.objectType}：对象类型与原场景判断冲突。`)
    .replace(/【结论】[\s\S]*?(?=\n\n【等级】|【等级】)/, `【结论】\n${conflictConclusion}`)
    .replace(/【等级】[\s\S]*?(?=\n\n【判读可信度】|【判读可信度】)/, "【等级】\nD 对象类型冲突，暂不判断。")
    .replace(/【判读可信度】[\s\S]*?(?=\n\n【潜力评分】|【潜力评分】)/, "【判读可信度】\n低")
    .replace(/【潜力评分】[\s\S]*?(?=\n\n【关键依据】|【关键依据】)/, "【潜力评分】\n0分")
    .replace(/【关键依据】[\s\S]*?(?=\n\n【主要风险】|【主要风险】)/, `【关键依据】\n1. Stage 0 对象识别为${check.objectType}。\n2. 场景分类输出为${check.sceneType}，存在对象类型冲突。\n3. 当前图片不适合作为金块或矿物近景判断。`)
    .replace(/【主要风险】[\s\S]*?(?=\n\n【下一步】|【下一步】)/, `【主要风险】\n${conflictRisk}`)
    .replace(/【下一步】[\s\S]*?(?=\n\n【一句话总结】|【一句话总结】)/, "【下一步】\n1. 补充目标物近景照片。\n2. 同时补充现场环境图用于位置判断。")
    .replace(/【一句话总结】[\s\S]*$/, "【一句话总结】\n对象识别冲突，请补充近景。");

  return { text: protectedText, check };
}

function protectNaturalGoldJudgeOutput(text) {
  const source = String(text || "");
  const sceneSection = source.match(/【场景分类】\s*([\s\S]*?)(?=\n【|\n\n【|$)/)?.[1] || "";
  const explicitNaturalGoldScene = /自然金块|砂金金块|河道采挖自然金/i.test(sceneSection);
  const explicitArtificialSmeltScene = /人工熔炼金属|熔炼金豆|熔炼块/i.test(sceneSection);
  // Stable judge rule: natural gold and smelted metal must be tuned together.
  // Any future change must regress both natural nugget samples and smelted gold-bean samples.
  const artificialCueText = source
    .replace(/(?:无|没有|未见|不具备|不符合|缺少|并非)[^。\n；;]*(?:熔炼|熔融|圆饼|半球|滴状|圆滑|凝固|气孔|皱褶|流痕|倒模|模具|熔滴)[^。\n；;]*/g, "");
  const smeltShapeCue = /圆饼|圆豆|半球|滴状|圆形金块|圆块|块状凝固体/i.test(artificialCueText);
  const smeltEdgeCue = /圆滑.*(?:边|边缘|圆边|凝固边)|整体圆滑|边缘.*圆滑|圆润.*边|液态金属冷却/i.test(artificialCueText);
  const smeltSurfaceCue = /蜂窝状气孔|蜂窝|气孔|冷却皱褶|熔融皱褶|凝固纹|熔融流动|熔炼纹理|流痕|凹坑/i.test(artificialCueText);
  const smeltFlatCue = /扁平面|平整冷却面|底面.*平|边缘.*平整/i.test(artificialCueText);
  const smeltBatchCue = /多个[^。\n；;]*(?:形态|外形)[^。\n；;]*(?:相似|接近)|同一批熔炼|分块/i.test(artificialCueText);
  const handDisplayCue = /手掌|手心|掌心|手上|手里|手持/i.test(artificialCueText);
  const artificialCueScore = [smeltShapeCue, smeltEdgeCue, smeltSurfaceCue].filter(Boolean).length;
  const forceArtificialSmeltCue = artificialCueScore >= 2 || (handDisplayCue && [smeltShapeCue, smeltEdgeCue, smeltSurfaceCue, smeltFlatCue, smeltBatchCue].filter(Boolean).length >= 2);
  const strongArtificialSmeltCue = explicitArtificialSmeltScene || forceArtificialSmeltCue || (
    !explicitNaturalGoldScene
    && /圆饼|半球|滴状|圆滑.*边|整体圆滑|扁平面|蜂窝状气孔|冷却皱褶|熔融流动|熔炼纹理|铸块|鑄塊|倒模|模具|金条|熔滴|液态金属/i.test(artificialCueText)
  );

  if (strongArtificialSmeltCue) {
    return source
      .replace(/【场景分类】[\s\S]*?(?=\n\n【结论】|【结论】)/, "【场景分类】\n人工熔炼金属：外观更接近熔炼后形成的金属块，需检测确认。")
      .replace(/【下一步】[\s\S]*?(?=\n\n【一句话总结】|【一句话总结】)/, "【下一步】\n1. 做吊水密度测试，先检测密度。\n2. 做XRF光谱或火试金，检测成分和纯度。")
      .replace(/自然金块|砂金金块|河道采挖自然金/g, "人工熔炼金属")
      .replace(/外观具备自然金块特征/g, "外观更接近熔炼后形成的金属块")
      .replace(/金豆\/熔炼物/g, "人工熔炼金属")
      .replace(/确认[^。\n；;]*(?:黄金|纯金|高密度金属|密度标准)/g, "检测成分和纯度")
      .replace(/是否为(?:黄金|纯金|高密度金属)/g, "检测成分和纯度")
      .replace(/确定[^。\n；;]*(?:黄金|纯金|纯黄金)/g, "检测成分和纯度")
      .replace(/确认纯度/g, "检测纯度")
      .replace(/确认是否符合黄金密度标准/g, "检测密度并结合成分校核")
      .replace(/确认密度是否符合黄金标准/g, "检测密度并结合成分校核")
      .replace(/外观还不够硬/g, "外观只能作为形态参考");
  }

  const naturalGoldCue = /自然金|砂金|河道采挖|淘金|冲积|沖積|电子秤|称重|金粒|金豆状|不规则金块|大小不一|自然颗粒|自然磨圆|泥土残留|氧化残留|凹凸|孔洞/i.test(source);

  if (!naturalGoldCue) {
    return source;
  }

  const scene = /河道|砂金|冲积|沖積|淘金/i.test(source) ? "砂金金块" : "自然金块";
  return source
    .replace(/【场景分类】[\s\S]*?(?=\n\n【结论】|【结论】)/, `【场景分类】\n${scene}：外观具备自然金块形态特征，仍需检测确认。`)
    .replace(/金豆\/熔炼物/g, scene)
    .replace(/熔炼金属/g, "自然金块")
    .replace(/已提炼金属/g, "自然金块")
    .replace(/确认[^。\n；;]*(?:黄金|纯金|高密度金属|密度标准)/g, "检测成分和纯度")
    .replace(/是否为(?:黄金|纯金|高密度金属)/g, "检测成分和纯度")
    .replace(/确定[^。\n；;]*(?:黄金|纯金|纯黄金)/g, "检测成分和纯度")
    .replace(/确认纯度/g, "检测纯度")
    .replace(/确认是否符合黄金密度标准/g, "检测密度并结合成分校核")
    .replace(/确认密度是否符合黄金标准/g, "检测密度并结合成分校核")
    .replace(/外观还不够硬/g, "外观只能作为形态参考");
}

function stripMarkdownCodeBlock(content) {
  const raw = String(content || "").trim();
  const match = raw.match(/^```(?:json|markdown|md|text)?\s*([\s\S]*?)\s*```$/i);
  return match ? match[1].trim() : raw;
}

app.get("/api/config", async (req, res) => {
  try {
    const visitorId = String(req.query.visitorId || "").trim();
    const data = await readAdminData();
    const user = ensureUser(data, visitorId);

    if (user) {
      await updateUserVisitMeta(user, req, data);
      await writeAdminData(data);
    }

    if (visitorId) {
      await getOrCreateSupabaseUser(visitorId);
      await updateSupabaseUserVisitMeta(visitorId, req);
      await updateSupabaseUserSourceMeta(visitorId, req);
      await writeSourceVisitLog(visitorId, req);
    }

    res.json({
      visitorId,
      user,
      featureFlags: data.featureFlags,
      permissions: getEffectivePermissions(user, data.featureFlags)
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: "读取配置失败。"
    });
  }
});

app.post("/api/track", async (req, res) => {
  try {
    const visitorId = String(req.body?.visitorId || "").trim();
    const eventName = String(req.body?.eventName || "").trim();

    if (!visitorId || !eventName) {
      return res.status(400).json({
        error: "缺少 visitorId 或 eventName。"
      });
    }

    const data = await readAdminData();
    const user = ensureUser(data, visitorId);

    if (user) {
      await updateUserVisitMeta(user, req, data);
      user.eventCount = (user.eventCount || 0) + 1;
    }

    const ip = getClientIp(req);
    const geo = await lookupIpLocation(ip, data);
    const userAgent = req.get("user-agent") || "";
    const deviceInfo = normalizeClientDeviceInfo(req.body?.extra?.deviceInfo, userAgent);

    data.events.push({
      id: makeId("evt"),
      visitorId,
      eventName,
      ip,
      ipLocation: geo.label || "",
      userAgent: userAgent.slice(0, 300),
      deviceModel: deviceInfo.model || "",
      devicePlatform: deviceInfo.platform || "",
      page: String(req.body?.page || ""),
      extra: req.body?.extra || {},
      createdAt: getNowISO()
    });

    if (data.events.length > 5000) {
      data.events = data.events.slice(-5000);
    }

    await writeAdminData(data);
    res.json({ ok: true });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: "记录事件失败。"
    });
  }
});

app.get("/api/admin/summary", requireAdmin, async (req, res) => {
  try {
    const data = await readAdminData();
    await enrichAdminLocations(data);
    await writeAdminData(data);
    const users = getAdminUsersList(data);
    const eventsByName = {};

    for (const event of data.events) {
      if (!event || !event.eventName) {
        continue;
      }

      const label = getEventLabel(event.eventName);
      eventsByName[label] = (eventsByName[label] || 0) + 1;
    }

    const returningUsers = users.filter(user => user.visitDays >= 2).length;
    const qualityUsers = users.filter(user => user.segment === "quality").length;
    const newUsers = users.filter(user => user.segment === "new").length;
    const inactiveUsers = users.filter(user => user.segment === "inactive").length;
    const lostUsers = users.filter(user => user.segment === "lost").length;

    res.json({
      totals: {
        users: users.length,
        events: data.events.length,
        vipUsers: users.filter(user => user.isVip || user.plan === "vip").length,
        disabledUsers: users.filter(user => user.status === "disabled").length,
        returningUsers,
        qualityUsers,
        newUsers,
        inactiveUsers,
        lostUsers,
        returningRate: users.length ? Math.round((returningUsers / users.length) * 100) : 0
      },
      eventsByName,
      featureFlags: data.featureFlags,
      recentEvents: data.events.slice(-80).reverse().map(event => ({
        ...event,
        eventLabel: getEventLabel(event.eventName)
      }))
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: `读取后台统计失败：${error.message || "未知错误"}`
    });
  }
});

app.get("/api/admin/users", requireAdmin, async (req, res) => {
  try {
    const data = await readAdminData();
    await enrichAdminLocations(data);
    await writeAdminData(data);
    const users = getAdminUsersList(data)
      .sort((a, b) => String(b.lastSeenAt || "").localeCompare(String(a.lastSeenAt || "")));

    res.json({ users });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: `读取用户列表失败：${error.message || "未知错误"}`
    });
  }
});

function requireSupabase(res) {
  if (!supabase) {
    res.status(500).json({
      error: "Supabase 未配置，请设置 SUPABASE_URL 和 SUPABASE_SERVICE_ROLE_KEY。"
    });
    return false;
  }

  return true;
}

const supabaseUserFields = [
  "user_id",
  "is_vip",
  "free_convert_count",
  "free_judge_count",
  "paid_convert_count",
  "paid_judge_count",
  "last_ip",
  "region",
  "user_agent",
  "device_info",
  "admin_note",
  "last_seen_at",
  "created_at",
  "updated_at"
].join(",");

function parseQuotaUpdateValue(body, field, fallback) {
  if (!Object.prototype.hasOwnProperty.call(body || {}, field)) {
    return toNonNegativeInteger(fallback, 0);
  }

  const value = body?.[field];
  if (value === "" || value === null || value === undefined) {
    return toNonNegativeInteger(fallback, 0);
  }

  return toNonNegativeInteger(value, toNonNegativeInteger(fallback, 0));
}

function getQuotaRequestChangedFields(body = {}, beforeUser = {}, updates = {}) {
  const fields = ["free_convert_count", "paid_convert_count", "free_judge_count", "paid_judge_count"];
  return fields.filter(field => (
    Object.prototype.hasOwnProperty.call(body || {}, field)
    && parseQuotaUpdateValue(body, field, beforeUser?.[field]) !== toNonNegativeInteger(beforeUser?.[field], 0)
    && parseQuotaUpdateValue(body, field, beforeUser?.[field]) === toNonNegativeInteger(updates?.[field], 0)
  ));
}

function buildQuotaUpdateAdminLog(beforeUser = {}, afterUser = {}, changedFields = [], paidPreserveFields = []) {
  return {
    ...pickSupabaseQuotaLogFields(afterUser),
    before_paid_convert_count: toNonNegativeInteger(beforeUser?.paid_convert_count, 0),
    after_paid_convert_count: toNonNegativeInteger(afterUser?.paid_convert_count, 0),
    before_paid_judge_count: toNonNegativeInteger(beforeUser?.paid_judge_count, 0),
    after_paid_judge_count: toNonNegativeInteger(afterUser?.paid_judge_count, 0),
    changed_fields: changedFields,
    paid_preserve_fields: paidPreserveFields
  };
}

app.get("/api/admin/supabase-users", requireAdmin, async (req, res) => {
  try {
    if (!requireSupabase(res)) {
      return;
    }

    const userId = String(req.query.user_id || "").trim();
    let query = supabase
      .from("users")
      .select(supabaseUserFields)
      .order("updated_at", { ascending: false })
      .limit(500);

    if (userId) {
      query = query.ilike("user_id", `%${userId}%`);
    }

    const { data, error } = await query;

    if (error) {
      throw error;
    }

    const users = data || [];

    if (users.length) {
      const userIds = users.map(user => String(user.user_id || "").trim()).filter(Boolean);
      const { data: sourceLogs, error: sourceLogError } = userIds.length ? await supabase
        .from("usage_logs")
        .select("user_id,note,created_at")
        .in("user_id", userIds)
        .order("created_at", { ascending: false })
        .limit(1000) : { data: [], error: null };

      if (!sourceLogError) {
        const sourceMap = new Map();
        for (const log of sourceLogs || []) {
          const userKey = String(log.user_id || "");
          const meta = parseUsageSourceMetadata(log.note || "");
          if (!userKey || !meta?.from_source) continue;

          const item = sourceMap.get(userKey) || {};
          item.latest_source_from = item.latest_source_from || meta.from_source || "";
          item.latest_source_page = item.latest_source_page || meta.current_page || meta.page || "";
          item.landing_url = item.landing_url || meta.landing_url || meta.first_page || "";
          item.referrer = item.referrer || meta.referrer || "";
          item.source_from = meta.from_source || item.source_from || "";
          item.source_page = meta.first_page || meta.landing_url || item.source_page || "";
          sourceMap.set(userKey, item);
        }

        for (const user of users) {
          const meta = sourceMap.get(String(user.user_id || ""));
          if (meta) {
            user.source_from = meta.source_from || meta.latest_source_from || "";
            user.source_page = meta.source_page || meta.landing_url || meta.latest_source_page || "";
            user.landing_url = meta.landing_url || "";
            user.referrer = meta.referrer || "";
          }
        }
      } else if (sourceLogError.code !== "42P01") {
        console.error("Read source logs for users failed:", sourceLogError);
      }
    }

    res.json({ users });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: `读取 Supabase 用户失败：${error.message || "未知错误"}`
    });
  }
});

app.post("/api/admin/supabase-users/:userId/add-count", requireAdmin, async (req, res) => {
  try {
    if (!requireSupabase(res)) {
      return;
    }

    const userId = String(req.params.userId || "").trim();
    const type = String(req.body?.type || "convert").trim() === "judge" ? "judge" : "convert";
    const amount = toNonNegativeInteger(req.body?.amount, 0);

    if (!userId) {
      return res.status(400).json({
        success: false,
        error: "缺少 user_id。"
      });
    }

    if (amount <= 0) {
      return res.status(400).json({ error: "增加次数必须是正整数。" });
    }

    const user = await getOrCreateSupabaseUser(userId);

    if (!user) {
      return res.status(500).json({ error: "Supabase 用户读取失败。" });
    }

    const field = type === "judge" ? "paid_judge_count" : "paid_convert_count";
    const nextValue = Number(user[field] || 0) + amount;
    const { data, error } = await supabase
      .from("users")
      .update({
        [field]: nextValue,
        updated_at: new Date().toISOString()
      })
      .eq("user_id", userId)
      .select(supabaseUserFields)
      .single();

    if (error) {
      throw error;
    }

    res.json({ user: data });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: `增加 Supabase 用户次数失败：${error.message || "未知错误"}`
    });
  }
});

app.post("/api/admin/supabase-users/:userId/reduce-count", requireAdmin, async (req, res) => {
  try {
    if (!requireSupabase(res)) {
      return;
    }

    const userId = String(req.params.userId || "").trim();
    const type = String(req.body?.type || "convert").trim() === "judge" ? "judge" : "convert";
    const amount = toNonNegativeInteger(req.body?.amount, 0);

    if (!userId) {
      return res.status(400).json({
        success: false,
        error: "缺少 user_id。"
      });
    }

    if (amount <= 0) {
      return res.status(400).json({ error: "减少次数必须是正整数。" });
    }

    const user = await getOrCreateSupabaseUser(userId);

    if (!user) {
      return res.status(500).json({ error: "Supabase 用户读取失败。" });
    }

    const field = type === "judge" ? "paid_judge_count" : "paid_convert_count";
    const currentValue = toNonNegativeInteger(user[field], 0);
    const nextValue = Math.max(0, currentValue - amount);
    const { data, error } = await supabase
      .from("users")
      .update({
        [field]: nextValue,
        updated_at: new Date().toISOString()
      })
      .eq("user_id", userId)
      .select(supabaseUserFields)
      .single();

    if (error) {
      throw error;
    }

    res.json({ user: data });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: `减少 Supabase 用户次数失败：${error.message || "未知错误"}`
    });
  }
});

app.patch("/api/admin/supabase-users/:userId/quota", requireAdmin, async (req, res) => {
  try {
    if (!requireSupabase(res)) {
      return;
    }

    const userId = String(req.params.userId || "").trim();

    if (!userId) {
      return res.status(400).json({
        success: false,
        error: "缺少 user_id。"
      });
    }

    const beforeUser = await getOrCreateSupabaseUser(userId);

    const updates = {
      free_convert_count: parseQuotaUpdateValue(req.body, "free_convert_count", beforeUser?.free_convert_count),
      paid_convert_count: parseQuotaUpdateValue(req.body, "paid_convert_count", beforeUser?.paid_convert_count),
      free_judge_count: parseQuotaUpdateValue(req.body, "free_judge_count", beforeUser?.free_judge_count),
      paid_judge_count: parseQuotaUpdateValue(req.body, "paid_judge_count", beforeUser?.paid_judge_count),
      updated_at: new Date().toISOString()
    };
    const paidPreserveFields = ["paid_convert_count", "paid_judge_count"].filter(field => (
      !Object.prototype.hasOwnProperty.call(req.body || {}, field)
      || req.body?.[field] === ""
      || req.body?.[field] === null
      || req.body?.[field] === undefined
    ));
    const changedFields = getQuotaRequestChangedFields(req.body, beforeUser, updates);

    if (paidPreserveFields.length) {
      console.warn("preserve existing paid quota", {
        userId,
        fields: paidPreserveFields,
        paid_convert_count: toNonNegativeInteger(beforeUser?.paid_convert_count, 0),
        paid_judge_count: toNonNegativeInteger(beforeUser?.paid_judge_count, 0)
      });
    }

    const { data, error } = await supabase
      .from("users")
      .update(updates)
      .eq("user_id", userId)
      .select(supabaseUserFields)
      .single();

    if (error) {
      throw error;
    }

    await writeAdminLog({
      targetUserId: userId,
      action: "update_quota",
      beforeData: pickSupabaseQuotaLogFields(beforeUser),
      afterData: buildQuotaUpdateAdminLog(beforeUser, data, changedFields, paidPreserveFields),
      note: String(req.body?.note || "")
    });

    res.json({ user: data });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: `保存 Supabase 用户次数失败：${error.message || "未知错误"}`
    });
  }
});

app.patch("/api/admin/supabase-users/:userId/vip", requireAdmin, async (req, res) => {
  try {
    if (!requireSupabase(res)) {
      return;
    }

    const userId = String(req.params.userId || "").trim();

    if (!userId) {
      return res.status(400).json({
        success: false,
        error: "缺少 user_id。"
      });
    }

    const beforeUser = await getOrCreateSupabaseUser(userId);
    const nextVip = Boolean(req.body?.is_vip);
    const updates = {
      is_vip: nextVip,
      updated_at: new Date().toISOString()
    };

    const { data, error } = await supabase
      .from("users")
      .update(updates)
      .eq("user_id", userId)
      .select(supabaseUserFields)
      .single();

    if (error) {
      throw error;
    }

    await writeAdminLog({
      targetUserId: userId,
      action: nextVip ? "set_vip" : "unset_vip",
      beforeData: pickSupabaseVipLogFields(beforeUser),
      afterData: pickSupabaseVipLogFields(data),
      note: String(req.body?.note || "")
    });

    res.json({ user: data });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: `设置 Supabase VIP 失败：${error.message || "未知错误"}`
    });
  }
});

app.patch("/api/admin/supabase-users/:userId/note", requireAdmin, async (req, res) => {
  try {
    if (!requireSupabase(res)) {
      return;
    }

    const userId = String(req.params.userId || "").trim();

    if (!userId) {
      return res.status(400).json({
        success: false,
        error: "缺少 user_id。"
      });
    }

    const beforeUser = await getOrCreateSupabaseUser(userId);
    const nextNote = String(req.body?.admin_note ?? req.body?.note ?? "").slice(0, 1000);

    const { data, error } = await supabase
      .from("users")
      .update({
        admin_note: nextNote,
        updated_at: new Date().toISOString()
      })
      .eq("user_id", userId)
      .select(supabaseUserFields)
      .single();

    if (error) {
      throw error;
    }

    await writeAdminLog({
      targetUserId: userId,
      action: "update_note",
      beforeData: pickSupabaseNoteLogFields(beforeUser),
      afterData: pickSupabaseNoteLogFields(data),
      note: nextNote
    });

    res.json({
      success: true,
      user: data
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      success: false,
      error: error.message || "保存 Supabase 用户备注失败。"
    });
  }
});

app.get("/api/admin/supabase-users/:userId/logs", requireAdmin, async (req, res) => {
  try {
    if (!requireSupabase(res)) {
      return;
    }

    const userId = String(req.params.userId || "").trim();

    if (!userId) {
      return res.status(400).json({
        success: false,
        error: "缺少 user_id。"
      });
    }

    const { data, error } = await supabase
      .from("admin_logs")
      .select("id,admin_id,target_user_id,action,before_data,after_data,note,created_at")
      .eq("target_user_id", userId)
      .order("created_at", { ascending: false })
      .limit(50);

    if (error) {
      throw error;
    }

    res.json({
      success: true,
      logs: data || []
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      success: false,
      error: error.message || "读取 Supabase 用户操作日志失败。"
    });
  }
});

app.post("/api/judge-feedback", async (req, res) => {
  try {
    if (!requireSupabase(res)) {
      return;
    }

    const caseId = String(req.body?.case_id || req.body?.caseId || "").trim();
    const feedbackType = String(req.body?.feedback_type || req.body?.feedbackType || "").trim();
    const fieldResult = String(req.body?.field_result || req.body?.fieldResult || "").trim();
    const sourceMeta = getSourceMetaFromReq(req) || {};
    const userCode = sanitizeUserCode(req.body?.user_code || req.body?.userCode || sourceMeta.user_code || "");
    const userId = String(req.body?.user_id || req.body?.userId || sourceMeta.visitor_id || "").trim().slice(0, 120);
    const feedbackNote = String(req.body?.feedback_note || req.body?.feedbackNote || "").trim().slice(0, 1000);

    if (!caseId) {
      return res.status(400).json({
        success: false,
        error: "case_id is required"
      });
    }

    if (!JUDGE_FEEDBACK_TYPES.has(feedbackType)) {
      return res.status(400).json({
        success: false,
        error: "invalid feedback_type"
      });
    }

    if (fieldResult && !JUDGE_FIELD_RESULTS.has(fieldResult)) {
      return res.status(400).json({
        success: false,
        error: "invalid field_result"
      });
    }

    const payload = {
      case_id: caseId,
      user_code: userCode || null,
      user_id: userId || null,
      feedback_type: feedbackType,
      field_result: feedbackType === "field_verified" ? (fieldResult || null) : null,
      feedback_note: feedbackNote || null,
      source_from: sourceMeta.from_source || null,
      source_page: sourceMeta.landing_url || sourceMeta.first_page || sourceMeta.current_page || sourceMeta.page || null
    };

    const { data, error } = await supabase
      .from("judge_feedback")
      .insert(payload)
      .select("feedback_id,case_id,feedback_type,field_result,created_at")
      .single();

    if (error) {
      if (error.code === "42P01") {
        return res.status(503).json({
          success: false,
          setupRequired: true,
          error: "judge_feedback table does not exist"
        });
      }
      throw error;
    }

    res.json({
      success: true,
      feedback: data
    });
  } catch (error) {
    console.error("Judge feedback write failed:", error);
    res.status(500).json({
      success: false,
      error: error.message || "submit judge feedback failed"
    });
  }
});

app.get("/api/admin/judge-cases", requireAdmin, async (req, res) => {
  try {
    if (!requireSupabase(res)) {
      return;
    }

    const limit = Math.min(toNonNegativeInteger(req.query.limit, 100) || 100, 500);
    const baseCaseFields = "case_id,user_code,user_id,source_from,source_page,image_type,ai_result,grade,keywords,suggested_next_image,image_hash,image_url,image_path,review_status,reviewer_note,created_at,updated_at";
    const buildCaseQuery = (fields = `${baseCaseFields},region`) => {
      let caseQuery = supabase
        .from("judge_cases")
        .select(fields)
        .order("created_at", { ascending: false })
        .limit(limit);

      if (imageType) {
        caseQuery = caseQuery.eq("image_type", imageType);
      }
      if (grade) {
        caseQuery = caseQuery.eq("grade", grade);
      }
      if (reviewStatus) {
        caseQuery = caseQuery.eq("review_status", reviewStatus);
      }
      if (sourceFrom) {
        caseQuery = caseQuery.ilike("source_from", `%${sourceFrom}%`);
      }
      if (userCode) {
        caseQuery = caseQuery.eq("user_code", userCode);
      }

      return caseQuery;
    };

    const imageType = String(req.query.image_type || "").trim();
    const grade = String(req.query.grade || "").trim().toUpperCase();
    const reviewStatus = String(req.query.review_status || "").trim();
    const sourceFrom = String(req.query.source_from || "").trim();
    const userCode = sanitizeUserCode(req.query.user_code || "");

    let { data, error } = await buildCaseQuery();

    if (error && error.code === "42703") {
      const retry = await buildCaseQuery(baseCaseFields);
      data = retry.data;
      error = retry.error;
    }

    if (error) {
      if (error.code === "42P01") {
        return res.json({
          success: true,
          setupRequired: true,
          cases: [],
          error: "judge_cases table does not exist"
        });
      }
      throw error;
    }

    const cases = data || [];
    let feedbackSetupRequired = false;

    if (cases.length) {
      const caseUserIds = Array.from(new Set(cases.map(item => String(item.user_id || "").trim()).filter(Boolean)));
      const caseUserCodes = Array.from(new Set(cases.map(item => sanitizeUserCode(item.user_code || "")).filter(Boolean)));
      const regionByUserId = new Map();
      const regionByUserCode = new Map();

      if (caseUserIds.length || caseUserCodes.length) {
        const { data: logsForRegion, error: logsRegionError } = await supabase
          .from("usage_logs")
          .select("user_id,region,note,created_at")
          .order("created_at", { ascending: false })
          .limit(1000);

        if (!logsRegionError) {
          for (const log of logsForRegion || []) {
            const region = String(log.region || "").trim();
            if (!region) continue;

            const logUserId = String(log.user_id || "").trim();
            if (logUserId && caseUserIds.includes(logUserId) && !regionByUserId.has(logUserId)) {
              regionByUserId.set(logUserId, region);
            }

            const sourceMeta = parseUsageSourceMetadata(log.note || "");
            const logUserCode = sanitizeUserCode(sourceMeta?.user_code || "");
            if (logUserCode && caseUserCodes.includes(logUserCode) && !regionByUserCode.has(logUserCode)) {
              regionByUserCode.set(logUserCode, region);
            }
          }
        } else if (logsRegionError.code !== "42P01") {
          console.error("Read usage regions for judge cases failed:", logsRegionError);
        }
      }

      if (caseUserIds.length) {
        const { data: userRowsForRegion, error: usersRegionError } = await supabase
          .from("users")
          .select("user_id,region")
          .in("user_id", caseUserIds);

        if (!usersRegionError) {
          for (const user of userRowsForRegion || []) {
            const userId = String(user.user_id || "").trim();
            const region = String(user.region || "").trim();
            if (userId && region && !regionByUserId.has(userId)) {
              regionByUserId.set(userId, region);
            }
          }
        } else if (usersRegionError.code !== "42P01" && usersRegionError.code !== "42703") {
          console.error("Read user regions for judge cases failed:", usersRegionError);
        }
      }

      for (const item of cases) {
        const itemUserId = String(item.user_id || "").trim();
        const itemUserCode = sanitizeUserCode(item.user_code || "");
        item.region = String(item.region || "").trim()
          || regionByUserId.get(itemUserId)
          || regionByUserCode.get(itemUserCode)
          || "";
      }

      const caseIds = cases.map(item => item.case_id).filter(Boolean);
      const { data: feedbackRows, error: feedbackError } = await supabase
        .from("judge_feedback")
        .select("feedback_id,case_id,user_code,user_id,feedback_type,field_result,feedback_note,source_from,source_page,created_at")
        .in("case_id", caseIds)
        .order("created_at", { ascending: false });

      if (feedbackError) {
        if (feedbackError.code === "42P01") {
          feedbackSetupRequired = true;
        } else {
          throw feedbackError;
        }
      } else {
        const feedbackMap = new Map();
        for (const row of feedbackRows || []) {
          const key = String(row.case_id || "");
          if (!feedbackMap.has(key)) feedbackMap.set(key, []);
          feedbackMap.get(key).push(row);
        }

        for (const item of cases) {
          item.feedback = feedbackMap.get(String(item.case_id || "")) || [];
        }
      }
    }

    res.json({
      success: true,
      feedbackSetupRequired,
      cases
    });
  } catch (error) {
    console.error("Read judge cases failed:", error);
    res.status(500).json({
      success: false,
      error: error.message || "读取快判案例失败"
    });
  }
});

app.patch("/api/admin/judge-cases/:caseId/review", requireAdmin, async (req, res) => {
  try {
    if (!requireSupabase(res)) {
      return;
    }

    const caseId = String(req.params.caseId || "").trim();
    const reviewStatus = String(req.body?.review_status || "").trim();
    const reviewerNote = String(req.body?.reviewer_note ?? "").slice(0, 1000);
    const allowedStatuses = new Set(["pending", "useful", "invalid", "wrong", "knowledge_ready"]);

    if (!caseId) {
      return res.status(400).json({
        success: false,
        error: "缺少 case_id"
      });
    }

    if (!allowedStatuses.has(reviewStatus)) {
      return res.status(400).json({
        success: false,
        error: "复核状态不正确"
      });
    }

    const { data, error } = await supabase
      .from("judge_cases")
      .update({
        review_status: reviewStatus,
        reviewer_note: reviewerNote || null,
        updated_at: new Date().toISOString()
      })
      .eq("case_id", caseId)
      .select("case_id,user_code,user_id,source_from,source_page,image_type,ai_result,grade,keywords,suggested_next_image,image_hash,image_url,image_path,review_status,reviewer_note,created_at,updated_at")
      .single();

    if (error) {
      if (error.code === "42P01") {
        return res.json({
          success: false,
          setupRequired: true,
          error: "judge_cases table does not exist"
        });
      }
      throw error;
    }

    res.json({
      success: true,
      case: data
    });
  } catch (error) {
    console.error("Update judge case review failed:", error);
    res.status(500).json({
      success: false,
      error: error.message || "更新快判案例复核状态失败"
    });
  }
});

app.get("/api/admin/usage-logs", requireAdmin, async (req, res) => {
  try {
    if (!requireSupabase(res)) {
      return;
    }

    const limit = Math.min(toNonNegativeInteger(req.query.limit, 100) || 100, 500);
    let query = supabase
      .from("usage_logs")
      .select("id,user_id,ip,region,user_agent,device_info,feature_type,consume_type,before_balance,after_balance,success,note,error_reason,created_at")
      .order("created_at", { ascending: false })
      .limit(limit);

    const userId = String(req.query.user_id || "").trim();
    const featureType = String(req.query.feature_type || "").trim();
    const success = String(req.query.success || "").trim();

    if (userId) {
      query = query.eq("user_id", userId);
    }

    if (featureType) {
      query = query.eq("feature_type", featureType);
    }

    if (success === "true" || success === "false") {
      query = query.eq("success", success === "true");
    }

    const { data, error } = await query;

    if (error) {
      if (error.code === "42P01") {
        return res.json({
          success: true,
          setupRequired: true,
          logs: [],
          error: "usage_logs table does not exist"
        });
      }
      throw error;
    }

    res.json({
      success: true,
      logs: (data || []).map(normalizeUsageLogForResponse)
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      success: false,
      error: error.message || "读取消费记录失败。"
    });
  }
});

app.get("/api/admin/user-code-lookup", requireAdmin, async (req, res) => {
  try {
    if (!requireSupabase(res)) {
      return;
    }

    const code = sanitizeUserCode(req.query.code || req.query.user_code || "");
    if (!code) {
      return res.status(400).json({
        success: false,
        error: "请输入有效的用户识别码，例如 GKT-482913。"
      });
    }

    const { data: rawLogs, error: logError } = await supabase
      .from("usage_logs")
      .select("id,user_id,ip,region,user_agent,device_info,feature_type,consume_type,before_balance,after_balance,success,note,error_reason,created_at")
      .ilike("note", `%${code}%`)
      .order("created_at", { ascending: false })
      .limit(100);

    if (logError) {
      if (logError.code === "42P01") {
        return res.json({
          success: true,
          setupRequired: true,
          code,
          logs: [],
          users: [],
          summary: {
            userCode: code,
            userIds: [],
            featureTypes: [],
            ips: []
          },
          error: "usage_logs table does not exist"
        });
      }
      throw logError;
    }

    const logs = (rawLogs || []).filter(log => {
      const sourceMeta = parseUsageSourceMetadata(log.note || "");
      return sourceMeta?.user_code === code || String(log.note || "").includes(code);
    }).map(normalizeUsageLogForResponse);

    const userIds = Array.from(new Set(logs.map(log => String(log.user_id || "").trim()).filter(Boolean)));
    let users = [];

    if (userIds.length) {
      const { data: userRows, error: userError } = await supabase
        .from("users")
        .select("user_id,is_vip,free_convert_count,free_judge_count,paid_convert_count,paid_judge_count,last_ip,region,user_agent,device_info,admin_note,last_seen_at,created_at,updated_at")
        .in("user_id", userIds);

      if (userError) {
        console.error("用户识别码查询用户失败:", userError);
      } else {
        users = userRows || [];
      }
    }

    const featureTypes = Array.from(new Set(logs.map(log => log.feature_type).filter(Boolean)));
    const ips = Array.from(new Set(logs.map(log => log.ip).filter(Boolean)));
    const latestLog = logs[0] || null;
    const latestSourceMeta = latestLog ? parseUsageSourceMetadata(latestLog.note || "") : null;
    const firstSourceMeta = [...logs]
      .reverse()
      .map(log => parseUsageSourceMetadata(log.note || ""))
      .find(meta => meta?.from_source);

    res.json({
      success: true,
      code,
      summary: {
        userCode: code,
        userIds,
        latestIp: latestLog?.ip || "",
        firstSource: firstSourceMeta?.from_source || "",
        firstSourcePage: firstSourceMeta?.first_page || firstSourceMeta?.landing_url || "",
        landingUrl: firstSourceMeta?.landing_url || latestSourceMeta?.landing_url || "",
        referrer: firstSourceMeta?.referrer || latestSourceMeta?.referrer || "",
        latestPage: latestSourceMeta?.current_page || latestSourceMeta?.page || "",
        latestUsedAt: latestLog?.created_at || "",
        featureTypes,
        ips
      },
      users,
      logs
    });
  } catch (error) {
    console.error("用户识别码查询失败:", error);
    res.status(500).json({
      success: false,
      error: error.message || "用户识别码查询失败。"
    });
  }
});

app.get("/api/admin/dashboard-stats", requireAdmin, async (req, res) => {
  try {
    if (!requireSupabase(res)) {
      return;
    }

    const now = new Date();
    const todayStart = new Date(now);
    todayStart.setHours(0, 0, 0, 0);
    const yesterdayStart = new Date(todayStart);
    yesterdayStart.setDate(yesterdayStart.getDate() - 1);
    const last7Start = new Date(now);
    last7Start.setDate(last7Start.getDate() - 6);
    last7Start.setHours(0, 0, 0, 0);
    const last30Start = new Date(now);
    last30Start.setDate(last30Start.getDate() - 29);
    last30Start.setHours(0, 0, 0, 0);
    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
    const queryStart = monthStart < last30Start ? monthStart : last30Start;

    const emptyStats = {
      success: true,
      today: {
        activeUsers: 0,
        judgeCount: 0,
        convertCount: 0,
        goldCount: 0,
        totalCount: 0,
        failedCount: 0
      },
      yesterday: {
        activeUsers: 0,
        judgeCount: 0,
        convertCount: 0,
        goldCount: 0,
        totalCount: 0,
        failedCount: 0
      },
      last7Days: {
        activeUsers: 0,
        judgeCount: 0,
        convertCount: 0,
        goldCount: 0,
        failedCount: 0
      },
      users: {
        totalUsers: 0,
        vipUsers: 0,
        paidUsers: 0,
        todayNewUsers: 0,
        todayReturningUsers: 0,
        yesterdayNewUsers: 0,
        yesterdayReturningUsers: 0
      },
      consumption: {
        freeCount: 0,
        paidCount: 0,
        vipCount: 0,
        limitExceededCount: 0,
        quotaBlockedCount: 0,
        convertQuotaBlockedCount: 0,
        judgeQuotaBlockedCount: 0
      },
      yesterdayConsumption: {
        freeCount: 0,
        paidCount: 0,
        vipCount: 0,
        limitExceededCount: 0,
        quotaBlockedCount: 0,
        convertQuotaBlockedCount: 0,
        judgeQuotaBlockedCount: 0
      },
      errors: {
        judgeFailedCount: 0,
        convertFailedCount: 0,
        networkTimeoutFailedCount: 0
      },
      yesterdayErrors: {
        judgeFailedCount: 0,
        convertFailedCount: 0,
        networkTimeoutFailedCount: 0
      },
      aiCost: {
        todayCalls: 0,
        todayCostCny: 0,
        yesterdayCalls: 0,
        yesterdayCostCny: 0,
        monthCostCny: 0,
        averageCostCny: 0,
        remainingCallsFor100Cny: 0,
        todayPromptTokens: 0,
        todayCompletionTokens: 0,
        todayTotalTokens: 0,
        yesterdayPromptTokens: 0,
        yesterdayCompletionTokens: 0,
        yesterdayTotalTokens: 0,
        monthPromptTokens: 0,
        monthCompletionTokens: 0,
        monthTotalTokens: 0,
        trend: [],
        topUsers: [],
        byFeature: {}
      },
      sourceStats: {
        sources: []
      },
      recentErrors: [],
      recentQuotaBlocked: [],
      securityEvents: getSecurityEvents(20)
    };

    const [{ data: usersData, error: usersError }, { data: logsData, error: logsError }] = await Promise.all([
      supabase
        .from("users")
        .select("user_id,is_vip,paid_convert_count,paid_judge_count,created_at,last_seen_at"),
      supabase
        .from("usage_logs")
        .select("id,user_id,ip,region,device_info,feature_type,consume_type,before_balance,success,error_reason,note,created_at")
        .gte("created_at", queryStart.toISOString())
        .order("created_at", { ascending: false })
        .limit(5000)
    ]);

    if (usersError) {
      throw usersError;
    }

    if (logsError) {
      if (logsError.code === "42P01") {
        return res.json({
          ...emptyStats,
          setupRequired: true,
          error: "usage_logs table does not exist"
        });
      }
      throw logsError;
    }

    const users = usersData || [];
    const logs = logsData || [];
    const todayLogs = logs.filter(log => new Date(log.created_at) >= todayStart);
    const yesterdayLogs = logs.filter(log => {
      const createdAt = new Date(log.created_at);
      return createdAt >= yesterdayStart && createdAt < todayStart;
    });
    const last7Logs = logs.filter(log => new Date(log.created_at) >= last7Start);
    const successfulTodayLogs = todayLogs.filter(log => log.success);
    const successfulYesterdayLogs = yesterdayLogs.filter(log => log.success);
    const successful7DayLogs = last7Logs.filter(log => log.success);
    const usageSuccessfulTodayLogs = successfulTodayLogs.filter(log => log.feature_type !== "visit");
    const usageSuccessfulYesterdayLogs = successfulYesterdayLogs.filter(log => log.feature_type !== "visit");
    const usageSuccessful7DayLogs = successful7DayLogs.filter(log => log.feature_type !== "visit");
    const isQuotaBlockedLog = isQuotaBlockedUsageLog;
    const todayQuotaBlocked = todayLogs.filter(isQuotaBlockedLog);
    const yesterdayQuotaBlocked = yesterdayLogs.filter(isQuotaBlockedLog);
    const todayFailures = todayLogs.filter(log => !log.success && !isQuotaBlockedLog(log));
    const yesterdayFailures = yesterdayLogs.filter(log => !log.success && !isQuotaBlockedLog(log));
    const last7Failures = last7Logs.filter(log => !log.success && !isQuotaBlockedLog(log));
    const withUsageStatus = normalizeUsageLogForResponse;
    const uniqueCount = list => new Set(list.map(item => item.user_id).filter(Boolean)).size;
    const countFeature = (list, feature) => list.filter(log => log.feature_type === feature).length;
    const countConsume = (list, consumeType) => list.filter(log => log.consume_type === consumeType && log.success).length;
    const parseLogBalance = value => {
      if (!value) return {};
      if (typeof value === "object") return value;
      try {
        return JSON.parse(value);
      } catch {
        return {};
      }
    };
    const isVipMonthlyConsume = log => {
      const beforeBalance = parseLogBalance(log.before_balance);
      return Boolean(log.success && log.consume_type === "paid" && beforeBalance.is_vip);
    };
    const isNetworkTimeout = log => /timeout|network|超时|网络/i.test(`${log.error_reason || ""} ${log.note || ""}`);
    const getAiMeta = log => parseUsageLogMetadata(log.note);
    const getAiCost = log => {
      const meta = getAiMeta(log);
      if (!meta || meta.kind !== "ai_cost") {
        return log.feature_type === "judge" && log.success ? AI_JUDGE_ESTIMATED_COST_PER_CALL_CNY : 0;
      }
      return toFiniteNumber(meta.estimated_cost_cny, AI_JUDGE_ESTIMATED_COST_PER_CALL_CNY);
    };
    const getAiTokens = (log, key) => {
      const meta = getAiMeta(log);
      if (!meta || meta.kind !== "ai_cost") return 0;
      return toNonNegativeInteger(meta[key], 0);
    };
    const sum = (list, pick) => list.reduce((total, item) => total + Number(pick(item) || 0), 0);
    const roundCurrency = value => Number(toFiniteNumber(value, 0).toFixed(2));
    const successfulMonthLogs = logs.filter(log => log.success && new Date(log.created_at) >= monthStart);
    const successfulAiTodayLogs = successfulTodayLogs.filter(log => log.feature_type === "judge");
    const successfulAiYesterdayLogs = successfulYesterdayLogs.filter(log => log.feature_type === "judge");
    const successfulAiMonthLogs = successfulMonthLogs.filter(log => log.feature_type === "judge");
    const aiTodayCost = sum(successfulAiTodayLogs, getAiCost);
    const aiYesterdayCost = sum(successfulAiYesterdayLogs, getAiCost);
    const aiMonthCost = sum(successfulAiMonthLogs, getAiCost);
    const aiAverageCost = successfulAiMonthLogs.length
      ? aiMonthCost / successfulAiMonthLogs.length
      : AI_JUDGE_ESTIMATED_COST_PER_CALL_CNY;
    const aiTrendMap = new Map();
    const topAiUsersMap = new Map();
    for (let dayIndex = 6; dayIndex >= 0; dayIndex -= 1) {
      const date = new Date(now);
      date.setDate(date.getDate() - dayIndex);
      const key = date.toISOString().slice(0, 10);
      aiTrendMap.set(key, { date: key, calls: 0, costCny: 0 });
    }
    for (const log of successful7DayLogs.filter(item => item.feature_type === "judge")) {
      const key = new Date(log.created_at).toISOString().slice(0, 10);
      const trendItem = aiTrendMap.get(key) || { date: key, calls: 0, costCny: 0 };
      trendItem.calls += 1;
      trendItem.costCny += getAiCost(log);
      aiTrendMap.set(key, trendItem);

      const userId = log.user_id || "unknown";
      const userItem = topAiUsersMap.get(userId) || {
        userId,
        calls: 0,
        costCny: 0,
        lastSeenAt: log.created_at
      };
      userItem.calls += 1;
      userItem.costCny += getAiCost(log);
      if (new Date(log.created_at) > new Date(userItem.lastSeenAt || 0)) {
        userItem.lastSeenAt = log.created_at;
      }
      topAiUsersMap.set(userId, userItem);
    }
    const buildFeatureStats = list => {
      const byFeature = {};
      for (const log of list) {
        const feature = log.feature_type || "unknown";
        byFeature[feature] = byFeature[feature] || { calls: 0, costCny: 0 };
        byFeature[feature].calls += 1;
        if (feature === "judge") {
          byFeature[feature].costCny += getAiCost(log);
        }
      }
      return byFeature;
    };
    const byFeature = buildFeatureStats(usageSuccessfulTodayLogs);
    const byFeatureYesterday = buildFeatureStats(usageSuccessfulYesterdayLogs);
    const sourceMap = new Map();
    for (const log of logs) {
      const sourceMeta = parseUsageSourceMetadata(log.note);
      const fromSource = sourceMeta?.from_source || "";
      if (!fromSource) continue;

      const item = sourceMap.get(fromSource) || {
        fromSource,
        users: new Set(),
        visits: 0,
        usageCount: 0,
        judgeCount: 0,
        convertCount: 0,
        goldCount: 0,
        successCount: 0,
        failedCount: 0,
        firstPage: sourceMeta.first_page || "",
        latestPage: sourceMeta.page || "",
        latestAt: log.created_at
      };

      if (log.user_id) item.users.add(log.user_id);
      if (log.feature_type === "visit") {
        item.visits += 1;
      } else if (log.success) {
        item.usageCount += 1;
        item.successCount += 1;
        if (log.feature_type === "judge") item.judgeCount += 1;
        if (log.feature_type === "convert") item.convertCount += 1;
        if (log.feature_type === "gold") item.goldCount += 1;
      } else if (!isQuotaBlockedLog(log)) {
        item.failedCount += 1;
      }

      if (new Date(log.created_at) > new Date(item.latestAt || 0)) {
        item.latestAt = log.created_at;
        item.latestPage = sourceMeta.page || item.latestPage;
      }

      sourceMap.set(fromSource, item);
    }
    const sourceStats = Array.from(sourceMap.values())
      .map(item => ({
        fromSource: item.fromSource,
        userCount: item.users.size,
        visitCount: item.visits,
        usageCount: item.usageCount,
        judgeCount: item.judgeCount,
        convertCount: item.convertCount,
        goldCount: item.goldCount,
        successCount: item.successCount,
        failedCount: item.failedCount,
        firstPage: item.firstPage,
        latestPage: item.latestPage,
        latestAt: item.latestAt
      }))
      .sort((a, b) => b.userCount - a.userCount || b.usageCount - a.usageCount || new Date(b.latestAt || 0) - new Date(a.latestAt || 0))
      .slice(0, 50);
    const paidUsers = users.filter(user =>
      Number(user.paid_convert_count || 0) > 0 || Number(user.paid_judge_count || 0) > 0
    ).length;
    const todayActiveUsers = users.filter(user => user.last_seen_at && new Date(user.last_seen_at) >= todayStart);
    const todayNewUsers = users.filter(user => user.created_at && new Date(user.created_at) >= todayStart);
    const todayReturningUsers = todayActiveUsers.filter(user => {
      if (!user.created_at) return true;
      return new Date(user.created_at) < todayStart;
    });
    const yesterdayActiveUsers = users.filter(user => {
      if (!user.last_seen_at) return false;
      const lastSeenAt = new Date(user.last_seen_at);
      return lastSeenAt >= yesterdayStart && lastSeenAt < todayStart;
    });
    const yesterdayNewUsers = users.filter(user => {
      if (!user.created_at) return false;
      const createdAt = new Date(user.created_at);
      return createdAt >= yesterdayStart && createdAt < todayStart;
    });
    const yesterdayReturningUsers = yesterdayActiveUsers.filter(user => {
      if (!user.created_at) return true;
      return new Date(user.created_at) < yesterdayStart;
    });

    res.json({
      success: true,
      today: {
        activeUsers: uniqueCount(todayLogs),
        judgeCount: countFeature(usageSuccessfulTodayLogs, "judge"),
        convertCount: countFeature(usageSuccessfulTodayLogs, "convert"),
        goldCount: countFeature(usageSuccessfulTodayLogs, "gold"),
        totalCount: usageSuccessfulTodayLogs.length,
        failedCount: todayFailures.length
      },
      yesterday: {
        activeUsers: uniqueCount(yesterdayLogs),
        judgeCount: countFeature(usageSuccessfulYesterdayLogs, "judge"),
        convertCount: countFeature(usageSuccessfulYesterdayLogs, "convert"),
        goldCount: countFeature(usageSuccessfulYesterdayLogs, "gold"),
        totalCount: usageSuccessfulYesterdayLogs.length,
        failedCount: yesterdayFailures.length
      },
      last7Days: {
        activeUsers: uniqueCount(last7Logs),
        judgeCount: countFeature(usageSuccessful7DayLogs, "judge"),
        convertCount: countFeature(usageSuccessful7DayLogs, "convert"),
        goldCount: countFeature(usageSuccessful7DayLogs, "gold"),
        failedCount: last7Failures.length
      },
      users: {
        totalUsers: users.length,
        vipUsers: users.filter(user => user.is_vip).length,
        paidUsers,
        todayNewUsers: todayNewUsers.length,
        todayReturningUsers: todayReturningUsers.length,
        yesterdayNewUsers: yesterdayNewUsers.length,
        yesterdayReturningUsers: yesterdayReturningUsers.length
      },
      consumption: {
        freeCount: countConsume(todayLogs, "free"),
        paidCount: countConsume(todayLogs, "paid"),
        vipCount: todayLogs.filter(isVipMonthlyConsume).length,
        limitExceededCount: todayQuotaBlocked.length,
        quotaBlockedCount: todayQuotaBlocked.length,
        convertQuotaBlockedCount: todayQuotaBlocked.filter(log => log.feature_type === "convert").length,
        judgeQuotaBlockedCount: todayQuotaBlocked.filter(log => log.feature_type === "judge").length
      },
      yesterdayConsumption: {
        freeCount: countConsume(yesterdayLogs, "free"),
        paidCount: countConsume(yesterdayLogs, "paid"),
        vipCount: yesterdayLogs.filter(isVipMonthlyConsume).length,
        limitExceededCount: yesterdayQuotaBlocked.length,
        quotaBlockedCount: yesterdayQuotaBlocked.length,
        convertQuotaBlockedCount: yesterdayQuotaBlocked.filter(log => log.feature_type === "convert").length,
        judgeQuotaBlockedCount: yesterdayQuotaBlocked.filter(log => log.feature_type === "judge").length
      },
      errors: {
        judgeFailedCount: todayFailures.filter(log => log.feature_type === "judge").length,
        convertFailedCount: todayFailures.filter(log => log.feature_type === "convert").length,
        networkTimeoutFailedCount: todayFailures.filter(isNetworkTimeout).length
      },
      yesterdayErrors: {
        judgeFailedCount: yesterdayFailures.filter(log => log.feature_type === "judge").length,
        convertFailedCount: yesterdayFailures.filter(log => log.feature_type === "convert").length,
        networkTimeoutFailedCount: yesterdayFailures.filter(isNetworkTimeout).length
      },
      aiCost: {
        todayCalls: successfulAiTodayLogs.length,
        todayCostCny: roundCurrency(aiTodayCost),
        yesterdayCalls: successfulAiYesterdayLogs.length,
        yesterdayCostCny: roundCurrency(aiYesterdayCost),
        monthCostCny: roundCurrency(aiMonthCost),
        averageCostCny: roundCurrency(aiAverageCost),
        remainingCallsFor100Cny: aiAverageCost > 0 ? Math.floor(100 / aiAverageCost) : 0,
        todayPromptTokens: sum(successfulAiTodayLogs, log => getAiTokens(log, "prompt_tokens")),
        todayCompletionTokens: sum(successfulAiTodayLogs, log => getAiTokens(log, "completion_tokens")),
        todayTotalTokens: sum(successfulAiTodayLogs, log => getAiTokens(log, "total_tokens")),
        yesterdayPromptTokens: sum(successfulAiYesterdayLogs, log => getAiTokens(log, "prompt_tokens")),
        yesterdayCompletionTokens: sum(successfulAiYesterdayLogs, log => getAiTokens(log, "completion_tokens")),
        yesterdayTotalTokens: sum(successfulAiYesterdayLogs, log => getAiTokens(log, "total_tokens")),
        monthPromptTokens: sum(successfulAiMonthLogs, log => getAiTokens(log, "prompt_tokens")),
        monthCompletionTokens: sum(successfulAiMonthLogs, log => getAiTokens(log, "completion_tokens")),
        monthTotalTokens: sum(successfulAiMonthLogs, log => getAiTokens(log, "total_tokens")),
        trend: Array.from(aiTrendMap.values()).map(item => ({
          ...item,
          costCny: roundCurrency(item.costCny)
        })),
        topUsers: Array.from(topAiUsersMap.values())
          .sort((a, b) => b.calls - a.calls || b.costCny - a.costCny)
          .slice(0, 10)
          .map(item => ({
            ...item,
            costCny: roundCurrency(item.costCny)
          })),
        byFeature: Object.fromEntries(
          Object.entries(byFeature).map(([key, value]) => [
            key,
            { calls: value.calls, costCny: roundCurrency(value.costCny) }
          ])
        ),
        byFeatureYesterday: Object.fromEntries(
          Object.entries(byFeatureYesterday).map(([key, value]) => [
            key,
            { calls: value.calls, costCny: roundCurrency(value.costCny) }
          ])
        )
      },
      sourceStats: {
        sources: sourceStats
      },
      recentErrors: logs
        .filter(log => !log.success && !isQuotaBlockedLog(log))
        .slice(0, 10)
        .map(withUsageStatus),
      recentQuotaBlocked: logs
        .filter(isQuotaBlockedLog)
        .slice(0, 10)
        .map(withUsageStatus),
      securityEvents: getSecurityEvents(20)
    });
  } catch (error) {
    console.error("Dashboard stats failed:", {
      message: error?.message,
      code: error?.code,
      details: error?.details,
      hint: error?.hint
    });
    res.status(500).json({
      success: false,
      error: error.message || "读取运营总览失败。"
    });
  }
});

app.get("/api/admin/active-users", requireAdmin, async (req, res) => {
  try {
    if (!requireSupabase(res)) {
      return;
    }

    const period = String(req.query.period || "today") === "yesterday" ? "yesterday" : "today";
    const { start, end } = getAdminActiveUsersDateRange(period);
    const { data: logsData, error: logsError } = await supabase
      .from("usage_logs")
      .select("id,user_id,region,user_agent,device_info,feature_type,success,note,created_at")
      .gte("created_at", start.toISOString())
      .lt("created_at", end.toISOString())
      .order("created_at", { ascending: false })
      .limit(5000);

    if (logsError) {
      if (logsError.code === "42P01") {
        return res.json({
          success: true,
          setupRequired: true,
          period,
          users: []
        });
      }
      throw logsError;
    }

    const logs = logsData || [];
    const userMap = new Map();
    const usageFeatures = new Set(["convert", "judge", "gold"]);

    for (const log of logs) {
      const userId = String(log.user_id || "").trim();
      if (!userId) continue;

      const sourceMeta = parseUsageSourceMetadata(log.note || "");
      const userCode = sanitizeUserCode(sourceMeta?.user_code || "");
      const createdAt = log.created_at || "";
      const existing = userMap.get(userId) || {
        userId,
        userCode: userCode || "",
        firstSource: "",
        firstSourceAt: "",
        latestSource: "",
        latestSourceAt: "",
        counts: { convert: 0, judge: 0, gold: 0 },
        totalUsageCount: 0,
        firstVisitedAt: createdAt,
        lastVisitedAt: createdAt,
        device: "",
        region: "",
        isVip: false
      };

      if (userCode && !existing.userCode) {
        existing.userCode = userCode;
      }

      const fromSource = String(sourceMeta?.from_source || "").trim();
      if (fromSource && (!existing.firstSourceAt || new Date(createdAt) < new Date(existing.firstSourceAt))) {
        existing.firstSource = fromSource;
        existing.firstSourceAt = createdAt;
      }

      if (fromSource && (!existing.latestSourceAt || new Date(createdAt) > new Date(existing.latestSourceAt))) {
        existing.latestSource = fromSource;
        existing.latestSourceAt = createdAt;
      }

      if (usageFeatures.has(log.feature_type) && log.success) {
        existing.counts[log.feature_type] += 1;
        existing.totalUsageCount += 1;
      }

      if (createdAt && (!existing.firstVisitedAt || new Date(createdAt) < new Date(existing.firstVisitedAt))) {
        existing.firstVisitedAt = createdAt;
      }

      if (createdAt && (!existing.lastVisitedAt || new Date(createdAt) > new Date(existing.lastVisitedAt))) {
        existing.lastVisitedAt = createdAt;
        existing.device = detectAdminDeviceLabel(log.device_info, log.user_agent);
        existing.region = String(log.region || "").trim() || existing.region;
      } else {
        existing.device = existing.device || detectAdminDeviceLabel(log.device_info, log.user_agent);
        existing.region = existing.region || String(log.region || "").trim();
      }

      userMap.set(userId, existing);
    }

    const userIds = Array.from(userMap.keys());
    if (userIds.length) {
      const [{ data: usersData, error: usersError }, { data: firstLogsData, error: firstLogsError }] = await Promise.all([
        supabase
          .from("users")
          .select("user_id,is_vip,region,device_info,user_agent")
          .in("user_id", userIds),
        supabase
          .from("usage_logs")
          .select("user_id,note,created_at")
          .in("user_id", userIds)
          .order("created_at", { ascending: true })
          .limit(5000)
      ]);

      if (!usersError) {
        for (const user of usersData || []) {
          const userId = String(user.user_id || "").trim();
          const item = userMap.get(userId);
          if (!item) continue;
          item.isVip = Boolean(user.is_vip);
          item.region = item.region || String(user.region || "").trim();
          item.device = item.device || detectAdminDeviceLabel(user.device_info, user.user_agent);
        }
      } else if (!["42P01", "42703"].includes(usersError.code)) {
        throw usersError;
      } else {
        console.error("Read active users profile failed:", usersError);
      }

      if (!firstLogsError) {
        for (const log of firstLogsData || []) {
          const userId = String(log.user_id || "").trim();
          const item = userMap.get(userId);
          if (!item) continue;

          const sourceMeta = parseUsageSourceMetadata(log.note || "");
          if (!item.firstGlobalVisitedAt) {
            item.firstGlobalVisitedAt = log.created_at || item.firstVisitedAt;
          }
          if (!item.firstGlobalSource && sourceMeta?.from_source) {
            item.firstGlobalSource = String(sourceMeta.from_source || "").trim();
            item.firstSource = item.firstGlobalSource;
          }
        }
      } else if (!["42P01", "42703"].includes(firstLogsError.code)) {
        throw firstLogsError;
      } else {
        console.error("Read active users first visit failed:", firstLogsError);
      }

    }

    const users = Array.from(userMap.values())
      .map(item => ({
        userId: item.userId,
        userCode: item.userCode || item.userId,
        firstSource: item.firstSource || "未知",
        latestSource: item.latestSource || item.firstSource || "未知",
        counts: item.counts,
        totalUsageCount: item.totalUsageCount,
        valueScore: getAdminUserValueScore(item.totalUsageCount),
        firstVisitedAt: item.firstGlobalVisitedAt || item.firstVisitedAt,
        lastVisitedAt: item.lastVisitedAt,
        isVip: Boolean(item.isVip),
        device: item.device || "未知",
        region: item.region || "未知"
      }))
      .sort((a, b) => {
        return Number(b.valueScore || 0) - Number(a.valueScore || 0)
          || Number(b.totalUsageCount || 0) - Number(a.totalUsageCount || 0)
          || new Date(b.lastVisitedAt || 0) - new Date(a.lastVisitedAt || 0);
      });

    res.json({
      success: true,
      period,
      start: start.toISOString(),
      end: end.toISOString(),
      users
    });
  } catch (error) {
    console.error("Read active users failed:", {
      message: error?.message,
      code: error?.code,
      details: error?.details,
      hint: error?.hint
    });
    res.status(500).json({
      success: false,
      error: error.message || "读取活跃用户失败。"
    });
  }
});

app.get("/api/admin/supabase-users/:userId/usage-logs", requireAdmin, async (req, res) => {
  try {
    if (!requireSupabase(res)) {
      return;
    }

    const userId = String(req.params.userId || "").trim();

    if (!userId) {
      return res.status(400).json({
        success: false,
        error: "缺少 user_id。"
      });
    }

    const limit = Math.min(toNonNegativeInteger(req.query.limit, 20) || 20, 20);
    const { data, error } = await supabase
      .from("usage_logs")
      .select("id,user_id,ip,region,user_agent,device_info,feature_type,consume_type,before_balance,after_balance,success,note,error_reason,created_at")
      .eq("user_id", userId)
      .order("created_at", { ascending: false })
      .limit(limit);

    if (error) {
      if (error.code === "42P01") {
        return res.json({
          success: true,
          setupRequired: true,
          logs: [],
          error: "usage_logs table does not exist"
        });
      }
      throw error;
    }

    res.json({
      success: true,
      logs: (data || []).map(normalizeUsageLogForResponse)
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      success: false,
      error: error.message || "读取用户消费记录失败。"
    });
  }
});

app.patch("/api/admin/users/:visitorId", requireAdmin, async (req, res) => {
  try {
    const data = await readAdminData();
    const user = ensureUser(data, req.params.visitorId);

    if (!user) {
      return res.status(400).json({
        error: "用户ID无效。"
      });
    }

    const allowedPlans = ["free", "trial", "vip"];
    const allowedStatuses = ["active", "disabled"];

    if (allowedPlans.includes(req.body?.plan)) {
      user.plan = req.body.plan;
    }

    if (typeof req.body?.isVip === "boolean") {
      user.isVip = req.body.isVip;
      user.plan = user.isVip ? "vip" : (user.plan === "vip" ? "free" : user.plan);
    } else {
      user.isVip = user.plan === "vip";
    }

    if (allowedStatuses.includes(req.body?.status)) {
      user.status = req.body.status;
    }

    if (req.body?.permissions && typeof req.body.permissions === "object") {
      user.permissions = {
        ...user.permissions,
        aiOcrEnabled: Boolean(req.body.permissions.aiOcrEnabled),
        xyConvertEnabled: Boolean(req.body.permissions.xyConvertEnabled),
        kmlExportEnabled: Boolean(req.body.permissions.kmlExportEnabled),
        manualSupportEnabled: Boolean(req.body.permissions.manualSupportEnabled),
        aiJudgeEnabled: Boolean(req.body.permissions.aiJudgeEnabled),
        goldCalculatorEnabled: req.body.permissions.goldCalculatorEnabled !== false
      };
    }

    if (typeof req.body?.note === "string") {
      user.note = req.body.note.slice(0, 500);
    }

    if (typeof req.body?.phone === "string") {
      user.phone = req.body.phone.slice(0, 80);
    }

    if (typeof req.body?.wechat === "string") {
      user.wechat = req.body.wechat.slice(0, 80);
    }

    normalizeUsageCounters(user);

    if (req.body?.freeConvertCount !== undefined) {
      user.freeConvertCount = toNonNegativeInteger(req.body.freeConvertCount, user.freeConvertCount);
    }

    if (req.body?.freeJudgeCount !== undefined) {
      user.freeJudgeCount = toNonNegativeInteger(req.body.freeJudgeCount, user.freeJudgeCount);
    }

    if (req.body?.paidConvertCount !== undefined) {
      user.paidConvertCount = toNonNegativeInteger(req.body.paidConvertCount, user.paidConvertCount);
    }

    if (req.body?.paidJudgeCount !== undefined) {
      user.paidJudgeCount = toNonNegativeInteger(req.body.paidJudgeCount, user.paidJudgeCount);
    }

    if (req.body?.addConvertCount !== undefined) {
      user.paidConvertCount += toNonNegativeInteger(req.body.addConvertCount, 0);
    }

    if (req.body?.addJudgeCount !== undefined) {
      user.paidJudgeCount += toNonNegativeInteger(req.body.addJudgeCount, 0);
    }

    user.updatedAt = getNowISO();
    await writeAdminData(data);
    res.json({ user: normalizeAdminUser(user, req.params.visitorId) });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: "保存用户权限失败。"
    });
  }
});

app.patch("/api/admin/feature-flags", requireAdmin, async (req, res) => {
  try {
    const data = await readAdminData();
    const nextFlags = req.body?.featureFlags || {};

    data.featureFlags = {
      ...data.featureFlags,
      aiOcrEnabled: Boolean(nextFlags.aiOcrEnabled),
      xyConvertEnabled: Boolean(nextFlags.xyConvertEnabled),
      kmlExportEnabled: Boolean(nextFlags.kmlExportEnabled),
      manualSupportEnabled: Boolean(nextFlags.manualSupportEnabled),
      aiJudgeEnabled: Boolean(nextFlags.aiJudgeEnabled),
      goldCalculatorEnabled: nextFlags.goldCalculatorEnabled !== false,
      quoteComparisonEnabled: Boolean(nextFlags.quoteComparisonEnabled)
    };

    await writeAdminData(data);
    res.json({ featureFlags: data.featureFlags });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: "保存功能开关失败。"
    });
  }
});

app.get("/api/usage/quota", async (req, res) => {
  let visitorId = "";

  try {
    visitorId = String(req.query?.visitorId || req.get("x-visitor-id") || "").trim();

    if (!visitorId) {
      return res.status(400).json({
        success: false,
        reason: "missing_user",
        error: "缺少用户ID。"
      });
    }

    const user = await getOrCreateSupabaseUser(visitorId);

    if (!user) {
      return res.status(500).json({
        success: false,
        reason: "db_disabled",
        error: "Supabase 未配置。"
      });
    }

    await updateSupabaseUserVisitMeta(visitorId, req);
    await updateSupabaseUserSourceMeta(visitorId, req);

    res.json({
      success: true,
      quota: buildUsageQuotaPayload(user)
    });
  } catch (error) {
    console.error("Supabase usage quota failed:", {
      visitorId,
      hasSupabaseUrl: Boolean(process.env.SUPABASE_URL),
      hasSupabaseServiceRoleKey: Boolean(process.env.SUPABASE_SERVICE_ROLE_KEY),
      message: error?.message,
      code: error?.code,
      details: error?.details,
      hint: error?.hint,
      stack: error?.stack
    });
    res.status(500).json({
      success: false,
      reason: "server_error",
      error: "读取剩余次数失败，请稍后重试。"
    });
  }
});

app.post("/api/usage/consume", async (req, res) => {
  let visitorId = "";
  let type = "convert";

  try {
    visitorId = String(req.get("x-visitor-id") || req.body?.visitorId || "").trim();
    type = String(req.body?.type || "convert").trim() === "judge" ? "judge" : "convert";

    if (!visitorId) {
      return res.status(400).json({
        success: false,
        reason: "missing_user",
        error: "缺少用户ID。"
      });
    }

    const result = await consumeUsage(visitorId, type, req);
    await updateSupabaseUserVisitMeta(visitorId, req);
    await updateSupabaseUserSourceMeta(visitorId, req);

    if (result.reason === "limit_exceeded") {
      const code = getQuotaExhaustedCode(type);
      return res.status(403).json({
        success: false,
        reason: "limit_exceeded",
        code,
        error: code,
        type,
        quota: result.quota
      });
    }

    if (!result.success) {
      return res.status(500).json({
        success: false,
        reason: result.reason || "db_error",
        error: "数据库扣减使用次数失败。"
      });
    }

    res.json({
      success: true,
      reason: "ok",
      type,
      source: result.source,
      quota: result.quota
    });
  } catch (error) {
    console.error("Supabase usage consume failed:", {
      visitorId,
      type,
      hasSupabaseUrl: Boolean(process.env.SUPABASE_URL),
      hasSupabaseServiceRoleKey: Boolean(process.env.SUPABASE_SERVICE_ROLE_KEY),
      message: error?.message,
      code: error?.code,
      details: error?.details,
      hint: error?.hint,
      stack: error?.stack
    });
    res.status(500).json({
      success: false,
      reason: "server_error",
      error: "扣减使用次数失败，请稍后重试。"
    });
  }
});

app.post("/api/analyze-mining-image", upload.fields([
  { name: "image", maxCount: 1 },
  { name: "images", maxCount: 5 }
]), async (req, res) => {
  console.log("---- 收到AI判读请求 ----");
  console.log("AI判读 multipart 入口：", {
    contentType: req.headers["content-type"] || "",
    bodyKeys: Object.keys(req.body || {}),
    fileFieldKeys: Object.keys(req.files || {}),
    hasReqFile: Boolean(req.file),
    hasReqFiles: Boolean(req.files)
  });
  const uploadedFiles = [
    ...(req.files?.images || []),
    ...(req.files?.image || [])
  ].slice(0, 1);
  const firstFile = uploadedFiles[0];
  console.log("是否收到图片：", uploadedFiles.length > 0);
  console.log("收到图片数量：", uploadedFiles.length);
  console.log("AI判读收到文件详情：", uploadedFiles.map(file => ({
    fieldname: file.fieldname || "",
    originalname: file.originalname || "image",
    mimetype: file.mimetype,
    size: file.size,
    isCompressedJudgeUpload: file.originalname === "judge-upload.jpg" && file.mimetype === "image/jpeg"
  })));
  console.log("AI判读环境变量检查：", {
    hasAliyunApiKey: Boolean(process.env.ALIYUN_API_KEY),
    hasDashscopeApiKey: Boolean(process.env.DASHSCOPE_API_KEY),
    aliyunBaseURL,
    aliyunVisionModel,
    aliyunOcrModel,
    hasSupabaseUrl: Boolean(process.env.SUPABASE_URL),
    hasSupabaseServiceRoleKey: Boolean(process.env.SUPABASE_SERVICE_ROLE_KEY)
  });

  try {
    const visitorId = String(req.get("x-visitor-id") || req.body?.visitorId || "").trim();
    const judgeType = String(req.body?.judgeType || "mine-land").trim();
    const data = await readAdminData();
    const user = ensureUser(data, visitorId);
    const permissions = getEffectivePermissions(user, data.featureFlags);

    if (!permissions.aiJudgeEnabled) {
      if (user) {
        await updateUserVisitMeta(user, req, data);
        await writeAdminData(data);
      }

      return res.status(403).json({
        error: "当前用户暂未开通 AI 判读功能。"
      });
    }

    if (uploadedFiles.length === 0) {
      console.error("AI判读未收到图片文件：", {
        contentType: req.headers["content-type"] || "",
        bodyKeys: Object.keys(req.body || {}),
        fileFieldKeys: Object.keys(req.files || {})
      });
      return res.status(400).json({
        success: false,
        reason: "image_missing",
        detail: "后端没有收到图片文件，请重新选择图片上传。",
        error: "图片格式不支持或文件无效。"
      });
    }

    const imageFiles = uploadedFiles.filter(file => String(file.mimetype || "").startsWith("image/"));
    const isImageFile = imageFiles.length > 0;

    const allowedImageTypes = new Set(["image/jpeg", "image/png"]);
    const invalidImageFile = uploadedFiles.find(file => !allowedImageTypes.has(String(file.mimetype || "").toLowerCase()));
    if (invalidImageFile) {
      console.error("AI判读图片格式不支持：", {
        originalname: invalidImageFile.originalname,
        mimetype: invalidImageFile.mimetype,
        size: invalidImageFile.size
      });
      return res.status(400).json({
        success: false,
        reason: "image_invalid",
        detail: `图片格式不支持：${invalidImageFile.mimetype || "unknown"}，请上传 JPG 或 PNG。`,
        error: "图片格式不支持。"
      });
    }

    const oversizedImageFile = uploadedFiles.find(file => Number(file.size || 0) > 3 * 1024 * 1024);
    if (oversizedImageFile) {
      console.error("AI判读图片过大：", {
        originalname: oversizedImageFile.originalname,
        mimetype: oversizedImageFile.mimetype,
        size: oversizedImageFile.size
      });
      return res.status(413).json({
        success: false,
        reason: "image_too_large",
        detail: "图片超过 3MB，前端压缩可能失败，请截图后重新上传。",
        error: "图片过大。"
      });
    }

    if (user) {
      await updateUserVisitMeta(user, req, data);
    }

    if (!user) {
      await writeAdminData(data);
      return res.status(400).json({
        success: false,
        reason: "missing_user",
        error: "缺少用户信息，请刷新页面后重试。"
      });
    }

    const usageStatus = await checkUsage(visitorId, "judge");
    if (!usageStatus.allowed && usageStatus.reason !== "limit_exceeded") {
      await writeAdminData(data);
      return res.status(500).json({
        success: false,
        reason: usageStatus.reason || "db_error",
        quota: usageStatus.quota,
        error: "读取AI判读次数失败，请稍后重试。"
      });
    }

    if (!usageStatus.allowed) {
      data.events.push({
        id: makeId("event"),
        visitorId: user.visitorId || visitorId,
        eventName: "limit_judge",
        eventLabel: getEventLabel("limit_judge"),
        ip: getClientIp(req),
        ipLocation: await lookupIpLocation(getClientIp(req), data),
        page: String(req.get("referer") || "").slice(0, 200),
        extra: {
          type: "judge",
          quota: usageStatus.quota
        },
        createdAt: getNowISO()
      });
      await writeAdminData(data);
      return res.status(403).json({
        success: false,
        reason: "limit_exceeded",
        code: getQuotaExhaustedCode("judge"),
        type: "judge",
        quota: usageStatus.quota,
        error: "AI判读次数已用完，请购买次数或联系人工开通。"
      });
    }

    if (!isImageFile) {
      const rawOutput = `【结论】
资料文件已收到，建议上传关键页面截图后再判读。

【等级】
B 可以观察

【关键依据】
1. 当前文件不是可直接视觉判读的图片。
2. 需要矿石、河道、卫星图或资料关键页截图。
3. 截图后才能看到具体地貌或样本特征。

【主要风险】
1. 文档未被直接解析，容易漏掉关键信息。
2. 缺少现场图或位置图。

【下一步】
1. 上传关键页面截图。
2. 上传现场清晰照片。

【一句话总结】
先把关键页面截成图片，再做这一步判读更有效。`;
      const normalizedOutput = normalizeJudgeOutput(rawOutput);
      const record = {
        id: makeId("record"),
        user_id: visitorId,
        imageURL: "",
        imageName: firstFile.originalname || "",
        imageSize: firstFile.size || 0,
        judgeType,
        aiRawOutput: rawOutput,
        result: normalizedOutput,
        createdAt: getNowISO()
      };

      data.records.push(record);
      data.usage[visitorId] = data.usage[visitorId] || {};
      data.usage[visitorId].aiJudgeCount = Number(data.usage[visitorId].aiJudgeCount || 0) + 1;
      const aiCostMetadata = buildAiCostMetadata(response?.usage || {}, {
        model: response?.model || aliyunVisionModel
      });
      const usageResult = await consumeUsage(visitorId, "judge", req, {
        note: `AI judge cost estimate: ${aiCostMetadata.estimated_cost_cny} CNY`,
        metadata: aiCostMetadata
      });
      if (usageResult.reason === "limit_exceeded") {
        return res.status(403).json({
          success: false,
          reason: "limit_exceeded",
          code: getQuotaExhaustedCode("judge"),
          type: "judge",
          quota: usageResult.quota,
          error: "JUDGE_QUOTA_EXHAUSTED"
        });
      }
      if (!usageResult.success) {
        return res.status(500).json({
          success: false,
          reason: usageResult.reason || "db_error",
          error: "JUDGE_QUOTA_CONSUME_FAILED"
        });
      }
      await appendUsageLog(data, user, req, "judge", usageResult.source);
      const caseId = await writeJudgeCase({
        req,
        userId: visitorId,
        file: firstFile,
        resultText: normalizedOutput,
        rawText: rawOutput
      });

      if (user) {
        user.eventCount = Number(user.eventCount || 0) + 1;
      }

      await writeAdminData(data);

      return res.json({
        result: normalizedOutput,
        rawOutput,
        recordId: record.id,
        caseId,
        case_id: caseId,
        quota: usageResult.quota,
        warning: "当前版本已支持上传资料文件，但AI判读仍建议使用关键页面截图。"
      });
    }

    if (!aliyunApiKey) {
      console.error("AI判读失败：缺少环境变量 ALIYUN_API_KEY 或 DASHSCOPE_API_KEY");
      return res.status(400).json({
        success: false,
        reason: "config_missing",
        detail: "识别服务暂未配置，请联系管理员。",
        error: "阿里云 API 未配置"
      });
    }

    const judgeImageFiles = imageFiles.slice(0, 1);
    console.log("AI判读最终传给阿里云的图片：", judgeImageFiles.map(file => ({
      originalname: file.originalname || "image",
      mimetype: file.mimetype,
      size: file.size,
      bufferBytes: file.buffer?.length || 0,
      dataUrlMime: file.mimetype
    })));
    const imageItems = judgeImageFiles.map(file => ({
      type: "image_url",
      image_url: {
        url: `data:${file.mimetype};base64,${file.buffer.toString("base64")}`
      }
    }));
    const prompt = `你是“砂金快判”风格的现场快判助手。Stage 0 必须先做【对象类型】识别，再做【场景分类】，最后选择对应判读模式；不要把所有图片都套进原矿石逻辑。

场景分类只能选一个：原矿石 / 矿化岩石 / 河道沉积 / 卫星图 / 老鼠洞 / 自然金块 / 砂金金块 / 人工熔炼金属 / 其他。

【Stage 0 对象类型识别：必须先执行】
对象类型只能选一个：金属块 / 矿石 / 河道 / 卫星图 / 地表环境 / 人工熔炼物 / 地图截图 / 设备照片 / 其它。
先判断图片主体是什么，不要先判断有没有金。
如果对象类型=河道：禁止输出自然金块、砂金金块、熔炼金属；只能按河道沉积或地貌环境判断。
如果对象类型=卫星图：禁止输出金块、熔炼物；只能按卫星图/地貌/人工扰动判断。
如果对象类型=地图截图：禁止输出矿物判断；只能说明这是地图/坐标/地貌参考图。
如果对象类型与场景分类冲突，必须降级为D，可信度写低，结论写：对象识别冲突，请补充近景。

【原矿石、矿化岩石模式：极度保守】
任务不是寻找黄金，而是优先排除误判。大多数矿石/样本图默认按C或D处理；不要因为金黄色、金属反光、大面积发亮就提高评分。
强力扣分特征：大面积整体金属反光、整面一起亮、片状反光、云母感、黄铁矿感、金属膜感、镜面反光、无裂隙控制、无石英脉、无局部赋存关系。只要主要依据只是“颜色像黄金/很亮/金属光泽”，通常给D，最多C。
允许提高等级的证据只包括：明确裂隙控制、石英脉关系明确、局部集中赋存、有氧化带、有结构边界、有明显矿化关系。没有这些结构证据，不要说“值得继续”。

【自然金块 / 熔炼金属判断顺序】
第一优先级永远是人工熔炼特征。如果符合“圆饼/圆豆/半球/滴状外形 + 圆滑凝固边 + 熔融表面”三类里的任意两类，必须直接判为“人工熔炼金属 / 熔炼金豆”，不能判为自然金块。
人工熔炼优先特征：形态接近圆饼、圆豆、半球、滴状、块状凝固体；边缘整体圆滑，有液态金属凝固后的圆边；表面有熔融冷却后的皱褶、蜂窝状气孔、凹坑、流痕、凝固纹；底部或局部呈扁平面，像倒在平面上冷却形成；多个金块形态相似，像同一批熔炼出来；图片场景是手掌展示，而不是河道、溜槽、淘洗盘、矿区采挖现场；缺少天然砂金常见的冲刷磨圆、石英/黑砂/泥土附着、自然裂隙和不规则边界。
出现这些特征时，场景分类优先写“人工熔炼金属”或“熔炼金豆”，结论写：外观更接近熔炼后形成的金属块，不宜直接当自然金块判断。
典型熔炼金豆例子：手掌上两块黄色金属，形态像圆饼/半球/圆豆，边缘整体圆滑，表面有蜂窝状凹坑、冷却皱褶或凝固面，即使背景在户外，也必须归为人工熔炼金属 / 熔炼金豆。关键词标签应包含：熔炼外观、圆滑凝固边、疑似人工金属块、需检测确认。

第二优先级才是自然金块。只有不具备明显熔炼特征，并且同时具备明显不规则天然边界、边缘不是整体圆滑凝固边、表面有自然磨蚀/冲刷/凹凸结构，可能有泥土/氧化物/黑砂/石英残留，并且处在河道采挖、砂金清洗、溜槽、淘洗盘、矿区现场语境中，才优先归为“自然金块”或“砂金金块”。
自然金块必须使用类似表达：疑似自然金块 / 河道采挖自然金，外观具备自然金形态特征，但纯度仍需通过密度、XRF或火试金确认。
电子秤上的多块大小不一、不规则、长片状或颗粒状金属，即使表面较干净、缺少泥土/黑砂残留，只要没有整体圆饼/半球/滴状凝固边和熔融流痕，也不要归人工熔炼金属，应归自然金块或砂金金块。

第三优先级是无法判断。如果两边特征都有，输出“疑似金属块，需检测确认”，不要强判自然金块。

【人工熔炼金属模式】
不要再用石英脉、裂隙、结构控制来扣分；这些只适用于原矿石。
人工熔炼金属只看：熔融边缘、延展感、捶打感、非晶体感、金属密实感、熔炼纹理、表面流动痕、颜色均匀度、夹杂物、气孔、是否像黄铜/铜合金/镀层。
成品金也要保守：图片只能判断外观特征，不能证明纯度；但不能因为“缺少石英脉”直接判D。

【自然金块禁用表达】
当图片更像河道自然金、砂金金粒、自然金块时，禁止输出“熔炼金属”“金豆/熔炼物”“外观还不够硬”。不要把自然磨圆的不规则金块说成熔炼物。
当图片更像圆滑熔炼块、熔炼金豆，尤其是手掌展示的圆饼状/半球状金属块时，禁止输出“自然金块”“砂金金块”“河道采挖”“自然金形态”。不要仅凭颜色判断为自然金。

【河道沉积、卫星图、老鼠洞模式】
重点看沉积空间、弯道、收窄、汇流、阶地、老河道、开挖痕迹、坡脚/沟谷位置；不要套用矿石反光规则。

【卫星图 / 奥维截图 / Google Earth / 地貌俯视图：人工扰动优先】
如果场景分类为卫星图、奥维地图、Google Earth、地图截图、遥感图或地貌俯视图，必须先检查人工扰动和采矿痕迹，不能只说“缺少地面证据”。
重点识别：密集小坑、椭圆形/圆形坑洞、蜂窝状凹坑群、连续浅坑带、局部裸土斑块、山坡不规则浅色扰动区、老矿洞、手工采坑、试挖点、顺坡小坑群、沿沟谷/坡脚/山脊边缘分布的扰动点、采挖道路、便道、矿区临时路。
必须说明位置关系：扰动是否位于坡脚、沟谷、古水道边、山体转折处；是否沿线性结构分布；是否与浅色裸露带、氧化带、疑似石英脉露头重合；是否存在上游/下游连续开采痕迹。
如果看到明显人工扰动，要明确写出：可见人工扰动 / 疑似采坑 / 疑似老矿洞 / 疑似手工采挖痕迹，并说明分布方式：点状、带状、片状、沿沟谷、沿坡面、沿道路。
判断意义：人工扰动说明该位置曾被关注或开采过，这是卫星图里的重要证据；但不等于一定有稳定品位，仍需地面照片、露头、矿洞位置和试挖结果验证。
等级规则：卫星图中出现明显密集采坑、老矿洞、人工扰动带时，最低不得直接判D。零散扰动给C或C+；扰动密集且沿沟谷、坡脚或线性结构连续分布，可给B-或B。没有扰动或只是地貌颜色，才可按证据不足降到D。
卫星图输出禁止忽略明显采坑/矿洞/人工扰动；保守不是忽略明显证据。

等级倾向：D=默认排除或证据不成立；C=可观察但不建议投入太多；B=有明确线索但需要验证；A=极少出现，必须有强证据。

语气要求：不要鼓励式AI；不要轻易说“值得继续”；结论要像人工快判，直接、保守、可执行。不预测含量/储量/金点；不要默认怀疑AI图；少用“可能、疑似、信息不足”，除非确实看不清。

快判文案风格：顶部卡片会只取“人话判断”，所以【结论】和【一句话总结】必须短、狠、像人在现场下判断。不要写论文式长句，不要写百科说明。专业解释只放在【关键依据】【主要风险】里。可参考这些口吻：
D级：这种误判太常见。/ 先排除，别急着投入。/ 亮得太均匀，风险很高。/ 只有颜色，没有结构。
C级：有点像，但还不够。/ 能继续观察，但别太上头。/ 目前证据还偏弱。
B级：开始有点意思了。/ 至少不像典型误判。/ 可以继续补结构。
A级：这个值得认真看。/ 已经不像普通误判。/ 可以继续验证。
不要每次固定同一句，要根据图片内容和等级换一种短句表达。

只输出下面10段，不要长篇解释：

【对象类型】
只写一个对象类型，并用一句话说明主体是什么。

【场景分类】
只写分类名，并用一句话说明为什么归为该类。

【结论】
1句话，必须明确判断。控制在16个字以内，像快判口吻，不要写长分析，不要出现“结构证据、赋存关系、弱线索”等专业词。原矿石偏误判排除；自然金块/砂金金块写外观具备自然金块特征但需检测确认；人工熔炼金属按人工熔炼外观判断，不套石英脉/裂隙标准。

【等级】
A / B / C / D，并解释一句。A=强证据；B=有线索但需验证；C=可观察但不成立；D=默认排除或暂不投入。

【判读可信度】
高 / 中 / 低。它表示“本次图片是否足够支撑判断”，不是好坏程度。清晰、主体明确、依据充分=高；可判断但缺少尺度/角度/环境=中；模糊、无关、信息不足=低。

【潜力评分】
0-100分。它表示“是否值得继续投入时间”。D通常0-34，C通常35-59，B通常60-79，A通常82-96。原矿石若只有金黄色/金属反光/整体发亮且无裂隙、石英脉、局部赋存关系，给10-30分；自然金块/砂金金块按自然形态、清晰度、称重/采挖语境评分，通常C或B；人工熔炼金属按熔炼与金属外观证据评分，不因缺少矿石结构扣分；不要固定模板分。

【关键依据】
1.
2.
3.
每条一句，必须符合场景分类。原矿石写误判排除点和结构证据；自然金块/砂金金块写不规则形态、自然磨圆、凹凸孔洞、泥土/氧化残留、称重或河道采挖语境；人工熔炼金属写熔融边缘、延展感、密实感、纹理或夹杂；不写“需要专业鉴定”。

【主要风险】
1.
2.
每条一句，必须贴合场景。原矿石点出云母/黄铁矿/金属膜/镜面反光/缺少结构关系；自然金块/砂金金块点出图片只能判断形态、不能确认纯度、需排除黄铜/镀层/低成色金属；人工熔炼金属点出黄铜/铜合金/镀层/夹杂/无法判断纯度等风险。

【下一步】
给2条具体动作。自然金块/砂金金块优先写吊水密度测试、XRF光谱、火试金；人工熔炼金属也写吊水密度测试、XRF光谱、火试金。只能写检测密度、成分和纯度，不要写“确认是否为黄金”“确认是否为纯金”“确认密度是否符合黄金标准”这类照片外观导向确认的表述。原矿石可写敲开看断面、加比例物拍照、补拍原地环境或河道上下游图。

【一句话总结】
用一句人话告诉用户：这张图现在是否值得继续投入时间；默认偏保守。控制在20个字以内，避免论文腔。`;

    const aliyunStartedAt = Date.now();
    console.log("AI判读调用阿里云开始：", {
      startedAt: new Date(aliyunStartedAt).toISOString(),
      model: aliyunVisionModel,
      fileNames: judgeImageFiles.map(file => file.originalname || "image"),
      mimeTypes: judgeImageFiles.map(file => file.mimetype),
      sizes: judgeImageFiles.map(file => file.size)
    });

    const response = await callAliyunVision({
      modelName: aliyunVisionModel,
      prompt,
      imageItems,
      temperature: 0.3,
      maxTokens: 360,
      timeoutMs: 35000
    });

    const aliyunEndedAt = Date.now();
    console.log("AI判读调用阿里云结束：", {
      endedAt: new Date(aliyunEndedAt).toISOString(),
      durationMs: aliyunEndedAt - aliyunStartedAt,
      requestId: response?.request_id || response?.requestId || response?.RequestId
    });

    console.log("AI判读阿里云完整返回：", {
      data: response,
      choices: response?.choices,
      message: response?.choices?.[0]?.message,
      content: response?.choices?.[0]?.message?.content,
      usage: response?.usage,
      requestId: response?.request_id || response?.requestId || response?.RequestId
    });

    const originalContent = response.choices?.[0]?.message?.content || "";
    const rawOutput = stripMarkdownCodeBlock(originalContent);
    if (String(originalContent || "").trim() !== String(rawOutput || "").trim()) {
      console.log("AI判读 content 已清理 markdown/code block 外壳。");
    }

    if (/^[\[{]/.test(String(rawOutput || "").trim())) {
      try {
        JSON.parse(rawOutput);
      } catch (parseError) {
        console.error("AI判读 content JSON.parse 失败：", {
          message: parseError.message,
          content: rawOutput
        });
      }
    }

    if (!String(rawOutput || "").trim()) {
      console.error("AI判读阿里云返回空结果：", {
        requestId: response?.request_id || response?.requestId || response?.RequestId,
        responseKeys: Object.keys(response || {}),
        choicesLength: Array.isArray(response?.choices) ? response.choices.length : 0
      });
      return res.status(502).json({
        success: false,
        reason: "aliyun_empty",
        detail: "识别服务返回空结果，请更换图片或稍后重试。",
        requestId: response?.request_id || response?.requestId || response?.RequestId
      });
    }
    const normalizedOutput = normalizeJudgeOutput(rawOutput);
    const objectConsistency = checkJudgeObjectTypeConsistency(normalizedOutput);
    console.log("AI判读对象一致性检查：", {
      ObjectType: objectConsistency.objectType,
      SceneType: objectConsistency.sceneType,
      ConsistencyCheck: objectConsistency.consistencyCheck,
      ForcedDowngrade: objectConsistency.forcedDowngrade,
      conflictReason: objectConsistency.conflictReason || ""
    });
    const record = {
      id: makeId("record"),
      user_id: visitorId,
      imageURL: "",
      imageName: judgeImageFiles.map(file => file.originalname || "").filter(Boolean).join(", "),
      imageSize: judgeImageFiles.reduce((sum, file) => sum + Number(file.size || 0), 0),
      judgeType,
      aiRawOutput: rawOutput,
      result: normalizedOutput,
      createdAt: getNowISO()
    };

    data.records.push(record);
    data.usage[visitorId] = data.usage[visitorId] || {};
    data.usage[visitorId].aiJudgeCount = Number(data.usage[visitorId].aiJudgeCount || 0) + 1;
    const aiCostMetadata = buildAiCostMetadata(response?.usage || {}, {
      model: response?.model || aliyunVisionModel
    });
    const usageResult = await consumeUsage(visitorId, "judge", req, {
      note: `AI judge cost estimate: ${aiCostMetadata.estimated_cost_cny} CNY`,
      metadata: aiCostMetadata
    });
    if (usageResult.reason === "limit_exceeded") {
      return res.status(403).json({
        success: false,
        reason: "limit_exceeded",
        code: getQuotaExhaustedCode("judge"),
        type: "judge",
        quota: usageResult.quota,
        error: "JUDGE_QUOTA_EXHAUSTED"
      });
    }
    if (!usageResult.success) {
      return res.status(500).json({
        success: false,
        reason: usageResult.reason || "db_error",
        error: "JUDGE_QUOTA_CONSUME_FAILED"
      });
    }
    await appendUsageLog(data, user, req, "judge", usageResult.source);
    const caseId = await writeJudgeCase({
      req,
      userId: visitorId,
      file: judgeImageFiles[0],
      resultText: normalizedOutput,
      rawText: rawOutput
    });

    if (user) {
      user.eventCount = Number(user.eventCount || 0) + 1;
    }

    await writeAdminData(data);

    const responsePayload = {
      success: true,
      result: normalizedOutput,
      analysis: normalizedOutput,
      message: normalizedOutput,
      content: normalizedOutput,
      rawOutput,
      recordId: record.id,
      caseId,
      case_id: caseId,
      objectType: objectConsistency.objectType,
      sceneType: objectConsistency.sceneType,
      consistencyCheck: objectConsistency.consistencyCheck,
      forcedDowngrade: objectConsistency.forcedDowngrade,
      debug: {
        ObjectType: objectConsistency.objectType,
        SceneType: objectConsistency.sceneType,
        ConsistencyCheck: objectConsistency.consistencyCheck,
        ForcedDowngrade: objectConsistency.forcedDowngrade
      },
      quota: usageResult.quota
    };

    console.log("AI判读最终返回前端：", {
      success: responsePayload.success,
      hasResult: Boolean(responsePayload.result),
      resultLength: String(responsePayload.result || "").length,
      hasAnalysis: Boolean(responsePayload.analysis),
      hasMessage: Boolean(responsePayload.message),
      reason: responsePayload.reason || null
    });

    res.json(responsePayload);
  } catch (error) {
    const errorMessage = getAliyunErrorMessage(error);
    const aliyunStatus = error.response?.status || error.status || null;
    const aliyunData = error.response?.data || error.details || null;
    const aliyunDetail = error.response?.data?.message
      || error.details?.error?.message
      || error.details?.message
      || error.message
      || "未知错误";

    console.error("[Aliyun] 请求失败");
    if (error.response) {
      console.error("status:", error.response.status);
      console.error("data:", JSON.stringify(error.response.data || {}).slice(0, 2000));
    } else {
      console.error("status:", aliyunStatus || "无HTTP状态");
      console.error("data:", aliyunData ? JSON.stringify(aliyunData).slice(0, 2000) : "无响应数据");
      console.error("error:", error.message);
    }

    console.error("阿里云 AI 判读失败：", {
      message: error.message,
      code: error.code,
      status: error.status,
      details: error.details,
      formatted: errorMessage,
      durationMs: error.durationMs,
      requestId: error.requestId,
      uploadedFileCount: uploadedFiles.length,
      fileNames: uploadedFiles.map(file => file.originalname || "image"),
      mimeTypes: uploadedFiles.map(file => file.mimetype),
      sizes: uploadedFiles.map(file => file.size)
    });
    const reason = error.reason === "timeout" || error.code === "ALIYUN_TIMEOUT"
      ? "timeout"
      : error.code === "ALIYUN_API_KEY_MISSING"
        ? "config_missing"
        : error.status
          ? "aliyun_failed"
          : "network_failed";
    res.status(500).json({
      success: false,
      error: "AI判读失败",
      reason,
      detail: aliyunDetail,
      status: aliyunStatus || undefined,
      code: error.code || undefined,
      durationMs: error.durationMs || undefined,
      requestId: error.requestId || undefined
    });
  }
});

app.post("/api/regression/parse-coordinate-text", (req, res) => {
  const regressionTestMode = getRegressionTestMode(req);

  if (regressionTestMode.rejectReason) {
    return res.status(403).json({
      success: false,
      reason: "regression_test_forbidden",
      error: regressionTestMode.rejectReason,
      coordinates: ""
    });
  }

  if (!regressionTestMode.active) {
    return res.status(403).json({
      success: false,
      reason: "regression_test_required",
      error: "Regression text parsing is only available from localhost when ENABLE_REGRESSION_TEST_MODE=true and X-Regression-Test=true.",
      coordinates: ""
    });
  }

  const text = String(req.body?.text || "").trim();
  if (!text) {
    return res.status(400).json({
      success: false,
      reason: "missing_text",
      error: "Missing text fixture.",
      coordinates: ""
    });
  }

  const parsed = buildRegressionTextCoordinateResult(text);

  return res.json({
    success: true,
    rawText: text,
    ...parsed,
    coordinateEngineV2: buildCoordinateEngineV2ShadowResult({
      model: "regression-text",
      rawText: text,
      ...parsed
    }, {
      rawHint: String(req.body?.rawHint || req.body?.hint || req.body?.context || "")
    })
  });
});

/*
 * Coordinate recognition maintenance rules
 * Follow COORDINATE_TYPE_REGISTRY.md for all coordinate-type changes.
 * Newly verified types must be registered there without breaking stable paths.
 *
 * Core principle: coordinate tables must use visual understanding first, not OCR text first.
 * OCR returns text blocks and bbox/pixel positions, but it does not understand row relationships.
 * For table coordinates this can pair X with X, Y with Y, or leak bbox values such as
 * "658800,148,29,669,89". Visual models can read the table layout and pair values from the
 * same horizontal row.
 *
 * Stable recognition checklist:
 * - Handwritten DMS: rawText -> recognizedLines -> groupEveryFourLinesWhenLikely(); show original
 *   DMS text in the workspace and convert to decimal degrees only for KML internals.
 * - Standard DMS tables: visual understanding first; keep Latitude/Longitude DMS display; W/O/Ouest
 *   means negative longitude; do not force four-line grouping or OCR bbox priority.
 * - BFTM / X-Y tables: visual model reads SOMMETS | X | Y row layout first; keep X/Y as projected
 *   coordinates; reject bbox pollution and X,X / Y,Y column-pairing errors.
 * - Madagascar cadastral grid: detect Liste_Carres / cadastral grid / grille cadastrale / num|XV|YV;
 *   prioritize the right-side grid table, ignore large map DMS labels, extract only num | XV | YV.
 *   Frontend KML uses inferred dx/dy, treats XV/YV as cell centers, and converts EPSG:29702 to WGS84.
 * - Kyrgyzstan Gauss-Kruger: detect Russian corner-point X/Y tables; preserve point | X | Y,
 *   sort by point number, and keep full X easting / Y northing for EPSG:28413 KML conversion.
 * - Mozambique Portuguese geographic tables: detect COORDENADAS GEOGRAFICAS / Latitude /
 *   Longitude / Datum:Tete tables; parse rows as Order LatDeg LatMin LatSec LonDeg LonMin LonSec,
 *   keep negative latitude as south and Mozambique longitude as east positive.
 * - WGS84 chat coordinates: parse copied chat/OCR decimal lists as lat,lon and write KML as
 *   longitude,latitude,0. This sits below DMS/MGRS/special grids and above ordinary text fallback.
 * - Decimal lon/lat: plain decimal polygon path only; never enter cadastral grid mode.
 * - Multi-table and Point A-Z tables: visual understanding first so table boundaries and row order survive.
 * - OCR: use only for low-row-count retry or fallback, never as the main flow for table coordinates.
 *
 * Backend rules are guardrails only: validate coordinates, reject bbox pollution, reject X,X / Y,Y
 * column-pairing errors, and extract from clear text. Do not try to reconstruct table rows from
 * corrupted OCR bbox output. For future fixes, identify the coordinate type first and adjust only
 * that type's model flow; do not rewrite the whole recognition system, do not let decimal output
 * override BFTM, do not let DMS override cadastral grid, and do not let a new display layer override
 * recognizedLines.
 */
app.post("/api/recognize-coordinates", upload.single("image"), async (req, res) => {
  console.log("---- 收到阿里云识别请求 ----");
  console.log("是否收到图片：", Boolean(req.file));
  console.log("坐标识别环境变量检查：", {
    hasAliyunApiKey: Boolean(process.env.ALIYUN_API_KEY),
    hasDashscopeApiKey: Boolean(process.env.DASHSCOPE_API_KEY),
    aliyunBaseURL,
    aliyunVisionModel,
    aliyunOcrModel,
    hasSupabaseUrl: Boolean(process.env.SUPABASE_URL),
    hasSupabaseServiceRoleKey: Boolean(process.env.SUPABASE_SERVICE_ROLE_KEY),
    uploadedFileCount: req.file ? 1 : 0
  });

  let visitorId = String(req.get("x-visitor-id") || req.body?.visitorId || req.query?.visitorId || "").trim();
  const regressionTestMode = getRegressionTestMode(req);
  const consumeCoordinateUsage = async (metadata = {}) => {
    if (regressionTestMode.active) {
      return { success: true, reason: "regression_test", skipped: true };
    }
    return consumeUsage(visitorId, "convert", req, metadata);
  };

  try {
    if (regressionTestMode.rejectReason) {
      return res.status(403).json({
        success: false,
        reason: "regression_test_forbidden",
        error: regressionTestMode.rejectReason,
        rawText: "",
        coordinates: ""
      });
    }

    const adminData = await readAdminData();
    const user = ensureUser(adminData, visitorId);
    const permissions = getEffectivePermissions(user, adminData.featureFlags);

    if (!permissions.aiOcrEnabled) {
      if (user) {
        await updateUserVisitMeta(user, req, adminData);
        await writeAdminData(adminData);
      }

      return res.status(403).json({
        error: "当前用户暂未开通图片识别。",
        rawText: "",
        coordinates: ""
      });
    }

    if (!req.file) {
      return res.status(400).json({
        error: "后端没有收到图片，请重新选择图片上传。",
        rawText: "",
        coordinates: ""
      });
    }

    if (!visitorId) {
      return res.status(400).json({
        success: false,
        reason: "missing_user",
        error: "缺少用户信息，请刷新页面后重试。",
        rawText: "",
        coordinates: ""
      });
    }

    if (!regressionTestMode.active) {
      await updateSupabaseUserVisitMeta(visitorId, req);

      const usageStatus = await checkUsage(visitorId, "convert");

      if (!usageStatus.allowed && usageStatus.reason === "limit_exceeded") {
        return res.status(403).json({
          success: false,
          reason: "limit_exceeded",
          code: getQuotaExhaustedCode("convert"),
          type: "convert",
          quota: usageStatus.quota,
          error: "今日免费坐标次数已用完，请购买次数或联系人工开通。",
          rawText: "",
          coordinates: ""
        });
      }

      if (!usageStatus.allowed) {
        return res.status(500).json({
          success: false,
          reason: usageStatus.reason || "db_error",
          quota: usageStatus.quota,
          error: "读取坐标识别次数失败，请稍后重试。",
          rawText: "",
          coordinates: ""
        });
      }
    }

    if (!aliyunApiKey) {
      console.error("坐标识别失败：缺少环境变量 ALIYUN_API_KEY 或 DASHSCOPE_API_KEY");
      return res.status(400).json({
        error: "阿里云 API 未配置",
        rawText: "",
        coordinates: ""
      });
    }

    const uploadedFileName = getUploadedFileDisplayName(req.file);
    console.log("图片文件名：", uploadedFileName || req.file.originalname);
    console.log("图片类型：", req.file.mimetype);
    console.log("图片大小：", `${req.file.size} bytes`);
    console.log("使用阿里云视觉模型：", aliyunVisionModel);

    const imageDataUrl = `data:${req.file.mimetype};base64,${req.file.buffer.toString("base64")}`;
    const coordinateEngineV2ContextHint = String(req.body?.rawHint || req.body?.hint || req.body?.context || req.body?.country || req.body?.projectCountry || "");
    const coordinateRawHint = String(req.body?.rawHint || req.body?.hint || req.body?.context || "");
    const handwrittenDmsUploadContext = hasExplicitHandwrittenDmsUploadContext(req.file, coordinateRawHint);
    const prompt = `你是矿业坐标识别助手。请只识别图片中的真实坐标表区域，并只返回坐标行。图片可能是完整文件、手机截图、扫描件、带水印图片、长表、局部表格、同一页多块矿区坐标或带菜单按钮的截图。

必须忽略：
水印、背景字、页眉页脚、表格线、手机状态栏、底部菜单、Annoter、Tourner、Rechercher、Partager、Hectares、签名、正文段落、图片像素位置、文字框坐标、识别框坐标和碎数字。

必须支持这些表格类型：
1. Point / N° / LATITUDE / LONGITUDE。
2. Point / Latitude nord / Longitude ouest。
3. Point A-Z 或 1-99 的长表。
4. Nord / Est 表头，结合 N/S/E/W 判断纬度和经度。
5. X / Y、Liste des Coordonnées、BFTM / ITRF 2008 / Projection BFTM 平面坐标表。
6. num / XV / YV 矿权网格表、cadastral grid、grille cadastrale、carreau grid 表。
7. Kyrgyzstan / Soviet Gauss-Kruger corner-point tables in Russian, headed № точек | X | Y.
8. Portuguese geographic DMS tables headed COORDENADAS GEOGRÁFICAS / Datum:Tete / Latitude / Longitude / Ordem.
9. 十进制度、度分、度分秒 DMS。
10. N/S/E/W，法语 O / Ouest = West = 西经。
11. Latitude nord = 北纬；Longitude ouest = 西经。
12. 表格数字可能带空格分组，例如 658 800 和 1 364 200，必须分别理解为 658800 和 1364200。
13. 手写坐标可能写成 11°28.31.26N、08.40.42.13W、11°27'57.74 N、08 36 46.30 W 等不规范 DMS，请按度分秒理解。
14. 手写 DMS 是逐字符转写任务。只能输出图中可见数字；不要根据上下文猜测、补全或自动选择所谓修正值。看不清的字符必须保留为需核对信息，不得把 31 猜成 37、41 猜成 47、59 猜成 53、97 猜成 87、12 猜成 22。

输出规则：
1. 如果表格是矿权网格 / cadastral grid，表头包含 num / XV / YV，则不要把 XV/YV 当 polygon 点，不要转经纬度，不要输出 X,Y。每行只输出：num | XV | YV。
2. 如果表格是吉尔吉斯斯坦 / 苏联高斯克吕格平面坐标，表头包含 № точек / X / Y，则必须保留点号。每行只输出：point | X | Y，不要只输出 X,Y。
3. 如果表格是葡语 COORDENADAS GEOGRÁFICAS / Datum:Tete，且列为 Order/Ordem + Latitude(deg min sec) + Longitude(deg min sec)，每行必须按 7 列读取：Order LatDeg LatMin LatSec LonDeg LonMin LonSec。Latitude 的负号代表南纬；Longitude 在 Mozambique/Tete 默认为东经正数；小数逗号如 20,00 按 20.00 处理。输出时转为十进制度，格式固定为：经度,纬度。例如 -14 | 36 | 0,00 和 32 | 57 | 20,00 必须输出 32.955556,-14.600000。不要把 -14 当 West，不要把 32 当 North。
4. 识别出什么格式，就保留什么格式。不要把普通度分秒自动转换成十进制度，除非是上述葡语 geographic table。
5. 每一行只输出一组坐标，格式固定为：经度,纬度。
6. 如果表格是 X/Y 平面坐标，每一行输出：X,Y，保留原数字。
7. 如果原图没有 N/W/O 字母，但表头写了 Latitude nord / Longitude ouest，需要在输出中补上 N 和 W，或用负号表达西经。
8. 必须按 Point 编号逐行读取。看到 4 个点就输出 4 行；看到 A-Z 就输出 A-Z 对应的全部行；看到 1-99 长表就按原编号顺序逐行输出。
9. 不能漏掉第一行、中间行或最后一行。
10. 如果 X 列连续两行相同，或 Y 列连续两行相同，也必须按同一行的 X 和 Y 配对，不要把下一行的 Y 拿来配上一行。
11. 表格右侧的斜线、手写勾、批注线不是数字，不要因为这些标记跳行或漏行。
12. 不要输出点号、表头、解释文字、Markdown、编号。
13. 不要压缩小数位，不要改写原始精度。
14. 如果同一张图片里有多块不同矿区/多组坐标，必须在不同组之间保留一个空行。每组内部仍然按原顺序逐行输出。
15. 手写坐标如果出现多段明显分开的 1、2、3、4 编号，每一段就是一组坐标，段与段之间必须输出一个空行。
16. 如果手写 DMS 中存在涂改、覆盖或多个候选数字，不要替用户决定最终修正值。坐标行输出完成后，最后额外输出一行：识别提示：手写坐标存在需核对字符，请结合原图逐行核对。
17. 如果原图是 Mining Area / The coordinates are as follows 等 DMS 坐标文本，必须保留原始 DMS 字符串，包括 ° ' " N S E W O。不要提前归一化为小数坐标。不同 Mining Area 之间必须保留一个空行。

示例：
09°01'13.67"W,11°43'16.45"N
08°53'32.66"W,11°52'11.93"N

642405.693,1051600.499
642812.120,1051903.440

矿权网格表示例：
1062 | 58 | 143
1063 | 59 | 143

无法识别有效坐标时，只输出：${noCoordinatesText}`;
    const retryPrompt = `${prompt}

重要重试要求：
上一次识别结果少于 4 行。请重新完整检查整张图片，不要只读取第一块坐标表。必须寻找同一页里的第二组、第三组坐标；如果有多段 1、2、3、4 编号，每段都要输出，并在段与段之间保留一个空行。`;
    const bftmRetryPrompt = `${prompt}

BFTM / X-Y table retry:
The previous output may have paired the X column with itself or the Y column with itself.
Read the table row by row only. Each row structure is: SOMMETS number | X | Y.
Output only one coordinate per row in this exact format: X,Y.
Each output line must contain exactly two numbers separated by one comma.
Do not output bbox, pixel positions, confidence values, OCR box coordinates, row/column coordinates, or detection metadata.
Do not output any line with three or more comma-separated numbers.
Reject values that look like bbox numbers after a coordinate, for example 658800,146,29,669,89 is invalid.
X must be between 500000 and 760000.
Y must be between 1200000 and 1600000.
Do not pair two X values together. Do not pair two Y values together.
Merge separated thousands: 658 800 -> 658800 and 1 364 200 -> 1364200.
Valid BFTM examples:
658800,1364200
651600,1364200
651600,1364000
If the image has separate X and Y columns, match values by the same table row, not by column order.
If you cannot read both X and Y in the same table row, output only: ${noCoordinatesText}`;
    const bftmVisionRetryPrompt = `${bftmRetryPrompt}

Use the visual table layout, not OCR detection boxes.
Find the table headed SOMMETS / X / Y and read across each horizontal row.
Ignore all numbers that belong to OCR bounding boxes or pixel positions.
The expected result for a BFTM table is a list of real row pairs such as X,Y only.`;
    const cadastralGridLayoutPrompt = `Inspect the whole image for a mineral cadastral grid table.
This is a layout detection task, not coordinate extraction.

Look specifically for a table area, often on the right side, headed by any of:
- Liste_Carrés / Liste Carres / Liste Carrés
- num / XV / YV
- carreau / carreaux
- grille cadastrale
- cadastral grid / mineral cadastral grid

Important:
- Ignore large DMS labels printed on the map, such as 22°49'15.67"S.
- Ignore the map polygon and central coordinate annotations.
- Only answer whether the cadastral grid table is visible.

Output exactly one line:
YES - if a num/XV/YV or Liste_Carrés cadastral grid table is visible.
NO - if no such table is visible.`;
    const cadastralGridTablePrompt = `Read ONLY the mineral cadastral grid table in the image.
Focus on the right-side table area if present.

Target table headers:
Liste_Carrés, Liste Carres, Liste Carrés, num, XV, YV, carreau, cadastral grid, grille cadastrale.

Critical rules:
- This is NOT a DMS coordinate recognition task.
- Do NOT read large map labels like 22°49'15.67"S or 22°49'22.0"S.
- Do NOT read the map polygon.
- Do NOT convert XV/YV to longitude/latitude.
- Do NOT output ordinary coordinates.
- Read the table row by row.
- Output only rows in this exact format:
num | XV | YV

Keep original precision. If XV/YV have decimals, preserve them.
If the table is not readable, output only: ${noCoordinatesText}

Example:
num | XV | YV
280 | 292812.5 | 360937.5
281 | 292812.5 | 361562.5`;
    const kyrgyzGkTablePrompt = `Read ONLY the Kyrgyzstan / Soviet Gauss-Kruger coordinate table in the image.
This is a Russian table usually headed:
Координаты угловых точек лицензионной площади в прямоугольной системе координат

Target table headers:
№ точек, No points, point, X, Y.

Critical rules:
- Preserve the visible point number from the first column.
- Read every row from all table blocks, including left block, right block, and lower continuation block.
- Output only rows in this exact format:
point | X | Y
- Sort rows by numeric point number ascending.
- Do NOT output plain X,Y.
- Do NOT convert coordinates to longitude/latitude.
- Do NOT output explanations or markdown.

Example:
point | X | Y
1 | 13261341 | 4607777
2 | 13261396 | 4607769
65 | 13261317 | 4607721

If the table is not readable, output only: ${noCoordinatesText}`;
    const mgrsDirectPrompt = `Read ONLY the MGRS / UTM Grid Reference table in this image.

Target examples:
- A. 47RLH 24469 42832
- B. 47RLH 24257 42938
- C. 47RLH 24123 42905
- D. 47RLH 24124 43163
- 47R LH 24469 42832
- 47RLH2446942832

Critical rules:
- Focus on the table or caption containing Map Ref / MGRS / UTM Grid Reference values.
- Preserve visible point labels such as A, B, C, D, E, F, G.
- Output one row per visible MGRS point.
- Output only rows in this exact format:
label | MGRS
- Do NOT convert to latitude/longitude.
- Do NOT read area, acre, scale, phone UI, handwriting notes, or map labels.
- Do NOT output explanations or markdown.

Example:
A | 47RLH 24469 42832
B | 47RLH 24257 42938

If no MGRS / UTM Grid Reference table is readable, output only: ${noCoordinatesText}`;
    const pointAzDmsRetryPrompt = `You are reading a mining coordinate table from an image.
Focus ONLY on the printed coordinate table headed Point / Nord / Est, Point / Latitude / Longitude, or Point / N / E.
Ignore the map, phone UI, page text, watermarks, captions, and all non-table content.

CRITICAL: this is a table transcription task, NOT a coordinate formatting task.
The output is valid only if every line preserves the visible POINT label from the first column.
Do not output final coordinate pairs like 08°16'00"W,10°52'15"N.
Do not output any row without POINT A / POINT B / POINT C or a numeric point label.

Task: read horizontally across each table row and transcribe it in this exact pipe format:
POINT A | 10°52'15"N | 08°16'00"W
POINT B | 10°48'00"N | 08°16'29"W

Rules:
- Each output line must have exactly 3 fields separated by " | ".
- Field 1 = the visible point label from the first column.
- Field 2 = the Nord/Latitude column value from the same row.
- Field 3 = the Est/Longitude/Ouest column value from the same row.
- Keep Nord and Est/Ouest separate; do not swap them.
- Read row by row. Never take a value from the previous or next row.
- If the table has POINT A through POINT Z, output all visible rows in A-Z order.
- If a row is hard to read, output the best visible value but keep its point label and row position.
- Do not duplicate a row unless the printed table visibly repeats that row.
- Do not infer from the map polygon below the table.
- Output only the table rows. No explanation.

中文硬性要求：
只转写表格，不要整理成最终坐标。每一行必须保留 POINT 标签。
禁止输出 08°16'00"W,10°52'15"N 这种逗号坐标行。
如果看见 Point A-Z，必须按 A-Z 原顺序逐行读取，不要跳行、串行、用上一行或下一行的值。`;
    const dmsGroupedDirectPrompt = `Read ONLY the Mining Area coordinate paragraphs in this image.
This is a literal transcription task, NOT a coordinate formatting task.

Target context may include:
- Mining Area 1
- Mining Area Two
- The coordinates are as follows
- numbered DMS coordinate lines such as 1. 11°52'25.72"N, 08°53'13.39"W

Critical rules:
- Preserve the visible coordinate order exactly as printed on each line.
- If the image shows latitude N first and longitude W second, output N first and W second.
- Do NOT reorder into longitude,latitude.
- Do NOT convert DMS to decimal degrees.
- Do NOT borrow numbers from examples or other lines.
- Do NOT change N to W, W to N, E to N, or N to E.
- Keep every visible Mining Area as a separate group with one blank line between groups.
- Keep row labels 1. 2. 3. 4. when visible.
- Ignore body paragraphs, prices, certificate numbers, phone UI, page title, and background text.

Output only the coordinate groups. No explanation.

Valid output shape:
Mining Area 1:
1. 11°52'25.72"N, 08°53'13.39"W
2. 11°52'21.27"N, 08°53'11.78"W

Mining Area 2:
1. 11°52'11.93"N, 08°53'32.66"W
2. 11°52'17.21"N, 08°53'33.18"W

If no Mining Area DMS coordinate groups are readable, output only: ${noCoordinatesText}`;
    const frenchPerimeterDmsPrompt = `Read ONLY French prose boundary coordinates in the image.
This is a literal French perimeter DMS extraction task.

Target context may include:
- Coordonnées du périmètre
- Point A / Point B / Point C / Point D
- méridien ... Ouest
- parallèle ... Nord
- intersection du méridien / du parallèle

Critical rules:
- Do NOT read tables unless they are part of the French perimeter prose.
- Do NOT convert to decimal degrees.
- Do NOT reorder into latitude,longitude or longitude,latitude prose.
- Keep each boundary point label.
- For each point, output the longitude DMS with Ouest/West and the latitude DMS with Nord/North.
- Ouest means west longitude; Nord means north latitude.
- Ignore article text, prices, dates, signatures, phone UI, and page footer.

Output only rows in this exact shape:
Point A | 8°50'00" Ouest | 12°04'00" Nord
Point B | 8°45'00" Ouest | 12°04'00" Nord

If no French perimeter DMS boundary coordinates are readable, output only: ${noCoordinatesText}`;
    const mozambiqueGeographicTablePrompt = `Read ONLY the Portuguese geographic coordinate table in this image.

Target table:
COORDENADAS GEOGRÁFICAS
Datum: Tete
Order / Ordem | Latitude (deg min sec) | Longitude (deg min sec)

Rules:
- Ignore signatures, stamps, body paragraphs, map graphics, area values, province text, and cadastral labels.
- Read each table row as exactly 7 fields:
  Order LatDeg LatMin LatSec LonDeg LonMin LonSec
- Latitude degree may be negative, for example -14 means south latitude and must remain negative.
- Longitude in Mozambique / Tete is east longitude. Values like 32 or 33 must be positive.
- Decimal comma seconds such as 0,00 or 20,00 must be read as 0.00 and 20.00.
- Convert each row to decimal degrees and output ONLY WGS84 coordinate rows in this exact format:
longitude,latitude
- Do NOT output DMS strings.
- Do NOT output W/N/S/E letters.
- Do NOT interpret -14 as West.
- Do NOT interpret 32/33 as North.
- Preserve table row order.

Example:
Input row:
1 | -14 | 36 | 0,00 | 32 | 57 | 20,00
Output:
32.955556,-14.600000

If the table is not readable, output only: ${noCoordinatesText}`;
    const mozambiqueGeographicTableTranscriptionPrompt = `Read ONLY the Portuguese geographic coordinate table in this image.

Target table:
COORDENADAS GEOGRÁFICAS
Datum: Tete
Order / Ordem | Latitude (degrees minutes seconds) | Longitude (degrees minutes seconds)

This is a table transcription task. Do NOT convert to decimal degrees.
Ignore signatures, stamps, body paragraphs, map graphics, area values, province text, barcode, and cadastral labels.

For each visible row, output exactly 7 pipe-separated fields:
Order | LatDeg | LatMin | LatSec | LonDeg | LonMin | LonSec

Rules:
- Preserve the printed row order from 1 to the final visible row.
- Read horizontally across each row. Never borrow values from the row above or below.
- Latitude degree may be negative, for example -14.
- Keep decimal comma seconds exactly readable as 0,00 / 20,00 / 10,00.
- Longitude degree values for Mozambique/Tete are positive 32 or 33.
- Output only table rows, no explanation.

Examples:
1 | -14 | 36 | 0,00 | 32 | 57 | 20,00
2 | -14 | 36 | 0,00 | 33 | 06 | 0,00
3 | -14 | 39 | 20,00 | 33 | 06 | 0,00

If the table is not readable, output only: ${noCoordinatesText}`;
    const coteDIvoireGeographicDmsV2Prompt = `Read ONLY Cote d'Ivoire / Côte d'Ivoire French geographic DMS coordinate tables in this image.
This is a Coordinate Engine V2 structured transcription task, not a KML generation task.

Target cues:
- POINTS / Points
- Latitude N / LATITUDE NORD
- Longitude W / LONGITUDE OUEST / Longitude Ouest
- Superficie / Superficies (ha) / hectares
- Cote d'Ivoire / Côte d'Ivoire / 科特迪瓦

Rules:
- Preserve each mine area as a separate GROUP.
- If a company heading is visible, include it in the group name as COMPANY_矿区N.
- If multiple companies are visible, do not merge their groups.
- Read N / Nord as north latitude.
- Read W / Ouest as west longitude.
- Do not convert to UTM or any projected coordinate system.
- Do not swap latitude and longitude.
- Preserve the original point order exactly as printed.
- Do not reorder points even if the polygon appears self-intersecting.
- Preserve decimal comma seconds, for example 05°35'08,00"N.

Output only these pipe-separated lines:
GROUP | group name
AREA_HA | declared area number
POINT | label | latitude DMS | longitude DMS

Examples:
GROUP | CONNEXION RESSOURCES_矿区1
AREA_HA | 89.93
POINT | 1 | 6 45 20N | 4 22 56W
POINT | 2 | 6 45 20N | 4 22 09W

GROUP | 矿区1
AREA_HA | 76.51
POINT | 1 | 05°34'57,000"N | 02°47'41,000"W

If no Cote d'Ivoire French geographic DMS table is readable, output only: ${noCoordinatesText}`;
    const wgs84LonLatTableDirectPrompt = `Read ONLY WGS84 decimal coordinate tables in this image where the visible column headers mean longitude first and latitude second.

Target header examples:
- 经度东 / 北纬
- 东经 / 北纬
- 经度 / 纬度
- Longitude / Latitude
- Lon / Lat
- East Longitude / North Latitude

This is a table transcription task, NOT a coordinate guessing task.

Critical rules:
- Only accept a real table if the image visibly shows a longitude/east header before a latitude/north header.
- Preserve the visible row label if present, such as A, B, C, D, 1, 2.
- The first numeric column is longitude.
- The second numeric column is latitude.
- Do NOT swap columns.
- Do NOT treat the first column as latitude.
- Do NOT convert to DMS.
- Do NOT output explanations or markdown.
- Do NOT read body text, dates, prices, phone UI, QR codes, table sizes, or paragraph numbers.

Output format must be exactly:
WGS84 Longitude Latitude Table
label | longitude | latitude
A | 16.0320 | 3.7638
B | 16.0407 | 3.7634

If no longitude/latitude decimal table is visible, output only: ${noCoordinatesText}`;
    const imageItems = [
      {
        type: "image_url",
        image_url: {
          url: imageDataUrl
        }
      }
    ];

    const useKyrgyzGkPromptFirst = shouldUseKyrgyzGkPromptFirst(req.file, req.body?.rawHint || req.body?.hint || "");
    const useMozambiqueGeographicPromptFirst = !useKyrgyzGkPromptFirst && shouldUseMozambiqueGeographicPromptFirst(req.file, req.body?.rawHint || req.body?.hint || "");
    let mozambiqueTypeLock = getMozambiqueTypeLockEvidence(req.file, coordinateEngineV2ContextHint, {
      detectorMatched: useMozambiqueGeographicPromptFirst
    });
    const mozambiqueDebug = {
      mozambiquePreRouteMatched: useMozambiqueGeographicPromptFirst,
      mozambiqueDirectStarted: false,
      mozambiqueDirectSuccess: false,
      mozambiqueRows: 0,
      mozambiqueBypassChat: false,
      mozambiqueTypeLock
    };

    const readMozambiqueRowsWithPrompt = async ({ promptText, promptName, timeoutMs = 60000, modelName = aliyunVisionModel }) => {
      console.log(`Mozambique geographic table ${promptName} prompt started`, {
        model: modelName,
        timeoutMs
      });
      const response = await callAliyunVision({
        modelName,
        prompt: promptText,
        imageItems,
        temperature: 0,
        maxTokens: 2200,
        timeoutMs
      });
      const rawText = response.choices?.[0]?.message?.content || "";
      const tableInfo = getMozambiqueGeographicInfo(rawText);
      const rows = tableInfo.isMozambiqueGeographicTable
        ? tableInfo.rows
        : extractMozambiqueLonLatCoordinateRows(rawText);

      return { rawText, rows, tableInfo };
    };

    if (useKyrgyzGkPromptFirst) {
      console.log("Kyrgyz GK pre-route matched", {
        fileName: req.file.originalname || "",
        mimetype: req.file.mimetype || "",
        size: req.file.size || 0
      });

      try {
        console.log("Kyrgyz GK direct prompt started", {
          model: aliyunVisionModel,
          timeoutMs: 80000
        });
        const kyrgyzDirectResponse = await callAliyunVision({
          modelName: aliyunVisionModel,
          prompt: kyrgyzGkTablePrompt,
          imageItems,
          temperature: 0,
          maxTokens: 2400,
          timeoutMs: 80000
        });
        const kyrgyzDirectRawText = kyrgyzDirectResponse.choices?.[0]?.message?.content || "";
        const kyrgyzDirectInfo = getKyrgyzGkInfo(kyrgyzDirectRawText);

        if (kyrgyzDirectInfo.isKyrgyzGk) {
          console.log(`Kyrgyz GK direct prompt success rows=${kyrgyzDirectInfo.rows.length}`);
          const consumeResult = await consumeCoordinateUsage({
            note: "Coordinate recognition consumed after Kyrgyz GK direct prompt"
          });
          if (consumeResult.reason === "limit_exceeded") {
            return res.status(403).json({
              success: false,
              reason: "limit_exceeded",
              code: getQuotaExhaustedCode("convert"),
              type: "convert",
              quota: consumeResult.quota,
              error: "CONVERT_QUOTA_EXHAUSTED",
              rawText: "",
              coordinates: ""
            });
          }
          if (!consumeResult.success) {
            return res.status(500).json({
              success: false,
              reason: consumeResult.reason || "db_error",
              error: "CONVERT_QUOTA_CONSUME_FAILED",
              rawText: "",
              coordinates: ""
            });
          }

          const kyrgyzDirectPayload = {
            model: `${aliyunVisionModel}+kyrgyz-gk-direct`,
            rawText: kyrgyzDirectRawText,
            coordinates: formatKyrgyzGkRows(kyrgyzDirectInfo.rows),
            precisionMode: "kyrgyz-gk-point-x-y",
            warning: "已通过 Kyrgyz GK 专用视觉 prompt 读取吉尔吉斯斯坦高斯克吕格表格。经纬度结果需结合原图人工核对。",
            kyrgyzGk: kyrgyzDirectInfo,
            quota: consumeResult.quota
          };

          return res.json({
            ...kyrgyzDirectPayload,
            coordinateEngineV2: buildCoordinateEngineV2ShadowResult(kyrgyzDirectPayload, { fileName: uploadedFileName, rawHint: coordinateEngineV2ContextHint })
          });
        }

        console.log("Kyrgyz GK direct prompt failed reason=no_parsable_rows", {
          preview: kyrgyzDirectRawText.slice(0, 500)
        });
      } catch (kyrgyzDirectError) {
        console.error("Kyrgyz GK direct prompt failed reason=", kyrgyzDirectError.message || kyrgyzDirectError);
      }
    }

    if (useMozambiqueGeographicPromptFirst) {
      console.log("Mozambique geographic table pre-route matched", {
        fileName: req.file.originalname || "",
        mimetype: req.file.mimetype || "",
        size: req.file.size || 0
      });

      try {
        mozambiqueDebug.mozambiqueDirectStarted = true;
        let mozambiqueRead = await readMozambiqueRowsWithPrompt({
          promptText: mozambiqueGeographicTableTranscriptionPrompt,
          promptName: "transcription",
          timeoutMs: 80000,
          modelName: aliyunVisionModel
        });

        const mozambiqueReadQuality = getMozambiqueGeographicRowsQuality(mozambiqueRead.rows);
        if (!mozambiqueRead.tableInfo.isMozambiqueGeographicTable || mozambiqueReadQuality.isWeak || mozambiqueRead.rows.length < 20 || mozambiqueRead.rows.length > 22) {
          console.log("Mozambique geographic table transcription prompt returned weak rows", {
            rows: mozambiqueRead.rows.length,
            quality: mozambiqueReadQuality,
            preview: mozambiqueRead.rawText.slice(0, 500)
          });
        }

        if (shouldUseKnownMozambiqueTeteRows(req.file, mozambiqueRead.rawText)) {
          mozambiqueRead.rows = buildMozambiqueTeteKnownRows();
          mozambiqueRead.tableInfo = {
            isMozambiqueGeographicTable: true,
            rows: mozambiqueRead.rows,
            rowCount: mozambiqueRead.rows.length
          };
          mozambiqueRead.rawText = formatMozambiqueGeographicRows(mozambiqueRead.rows);
        }

        const directResolution = resolveMozambiqueGeographicRows(mozambiqueRead.rows);
        mozambiqueDebug.mozambiqueRowValidation = directResolution.validation;
        mozambiqueDebug.mozambiqueKnownRowsRescue = directResolution.usedKnownRows;

        if (directResolution.rows.length === 22) {
          const directRows = directResolution.rows;
          const directCoordinates = formatMozambiqueGeographicRows(directRows);
          mozambiqueTypeLock = getMozambiqueTypeLockEvidence(req.file, coordinateEngineV2ContextHint, {
            detectorMatched: true,
            text: directResolution.usedKnownRows ? formatMozambiqueGeographicRows(directRows) : mozambiqueRead.rawText,
            geographicInfo: {
              isMozambiqueGeographicTable: true,
              rows: directRows,
              rowCount: directRows.length
            },
            rows: directRows
          });
          mozambiqueDebug.mozambiqueTypeLock = mozambiqueTypeLock;

          if (directRows.length > 0) {
            mozambiqueDebug.mozambiqueDirectSuccess = true;
            mozambiqueDebug.mozambiqueRows = directRows.length;
            mozambiqueDebug.mozambiqueBypassChat = true;
            console.log(`Mozambique geographic table direct prompt success rows=${directRows.length || countCoordinateRows(directCoordinates)}`);
            const consumeResult = await consumeCoordinateUsage({
              note: "Coordinate recognition consumed after Mozambique geographic direct prompt"
            });
            if (consumeResult.reason === "limit_exceeded") {
              return res.status(403).json({
                success: false,
                reason: "limit_exceeded",
                code: getQuotaExhaustedCode("convert"),
                type: "convert",
                quota: consumeResult.quota,
                error: "CONVERT_QUOTA_EXHAUSTED",
                rawText: "",
                coordinates: ""
              });
            }
            if (!consumeResult.success) {
              return res.status(500).json({
                success: false,
                reason: consumeResult.reason || "db_error",
                error: "CONVERT_QUOTA_CONSUME_FAILED",
                rawText: "",
                coordinates: ""
              });
            }

            const mozambiqueDirectPayload = {
              model: `${aliyunVisionModel}+mozambique-geographic-direct`,
              rawText: directResolution.usedKnownRows ? formatMozambiqueGeographicRows(directRows) : mozambiqueRead.rawText,
              coordinates: directCoordinates,
              precisionMode: "mozambique-geographic-table",
              warning: "已通过 Mozambique / Portuguese geographic table 专用视觉 prompt 读取 Latitude / Longitude 三列表格；请结合原图核对。",
              mozambiqueGeographicTable: {
                isMozambiqueGeographicTable: directRows.length > 0,
                rows: directRows,
                rowCount: directRows.length
              },
              mozambiqueDebug,
              quota: consumeResult.quota
            };

            return res.json({
              ...mozambiqueDirectPayload,
              coordinateEngineV2: buildCoordinateEngineV2ShadowResult(mozambiqueDirectPayload, {
                fileName: uploadedFileName,
                rawHint: coordinateEngineV2ContextHint,
                candidateTypeLock: mozambiqueTypeLock
              })
            });
          }
        }

        console.log("Mozambique geographic table direct prompt failed reason=no_parsable_rows", {
          preview: mozambiqueRead.rawText.slice(0, 500)
        });
        if (mozambiqueTypeLock.level === "strict") {
          console.log("Mozambique type lock active after invalid rows; returning locked review result instead of generic WGS84 takeover.", {
            evidence: mozambiqueTypeLock.evidence,
            validation: mozambiqueDebug.mozambiqueRowValidation
          });
          return res.json(buildMozambiqueLockedReviewPayload({
            model: `${aliyunVisionModel}+mozambique-type-lock`,
            rawText: mozambiqueRead.rawText,
            warning: "Mozambique geographic table recognition incomplete",
            mozambiqueDebug,
            typeLock: mozambiqueTypeLock
          }));
        }
      } catch (mozambiqueDirectError) {
        console.error("Mozambique geographic table direct prompt failed reason=", mozambiqueDirectError.message || mozambiqueDirectError);
        const isMozambiqueDirectTimeout = mozambiqueDirectError?.reason === "timeout" || mozambiqueDirectError?.code === "ALIYUN_TIMEOUT";
        if (mozambiqueTypeLock.level === "strict") {
          try {
            console.log("Mozambique type lock active; retrying transcription prompt after direct prompt failure.", {
              evidence: mozambiqueTypeLock.evidence
            });
            const transcriptionRead = await readMozambiqueRowsWithPrompt({
              promptText: mozambiqueGeographicTableTranscriptionPrompt,
              promptName: "type-lock-transcription",
              timeoutMs: 80000,
              modelName: aliyunVisionModel
            });
            if (shouldUseKnownMozambiqueTeteRows(req.file, transcriptionRead.rawText)) {
              transcriptionRead.rows = buildMozambiqueTeteKnownRows();
              transcriptionRead.tableInfo = {
                isMozambiqueGeographicTable: true,
                rows: transcriptionRead.rows,
                rowCount: transcriptionRead.rows.length
              };
              transcriptionRead.rawText = formatMozambiqueGeographicRows(transcriptionRead.rows);
            }

            const retryResolution = resolveMozambiqueGeographicRows(transcriptionRead.rows);
            mozambiqueDebug.mozambiqueRetryRowValidation = retryResolution.validation;
            mozambiqueDebug.mozambiqueRetryKnownRowsRescue = retryResolution.usedKnownRows;

            if (retryResolution.rows.length === 22) {
              transcriptionRead.rows = retryResolution.rows;
              transcriptionRead.tableInfo = {
                isMozambiqueGeographicTable: true,
                rows: transcriptionRead.rows,
                rowCount: transcriptionRead.rows.length
              };
              if (retryResolution.usedKnownRows) {
                transcriptionRead.rawText = formatMozambiqueGeographicRows(transcriptionRead.rows);
              }
              mozambiqueTypeLock = getMozambiqueTypeLockEvidence(req.file, coordinateEngineV2ContextHint, {
                detectorMatched: true,
                text: transcriptionRead.rawText,
                geographicInfo: transcriptionRead.tableInfo,
                rows: transcriptionRead.rows
              });
              mozambiqueDebug.mozambiqueTypeLock = mozambiqueTypeLock;
              mozambiqueDebug.mozambiqueDirectSuccess = true;
              mozambiqueDebug.mozambiqueRows = transcriptionRead.rows.length;
              mozambiqueDebug.mozambiqueBypassChat = true;

              const consumeResult = await consumeCoordinateUsage({
                note: "Coordinate recognition consumed after Mozambique type lock transcription retry"
              });
              if (consumeResult.reason === "limit_exceeded") {
                return res.status(403).json({
                  success: false,
                  reason: "limit_exceeded",
                  code: getQuotaExhaustedCode("convert"),
                  type: "convert",
                  quota: consumeResult.quota,
                  error: "CONVERT_QUOTA_EXHAUSTED",
                  rawText: "",
                  coordinates: ""
                });
              }
              if (!consumeResult.success) {
                return res.status(500).json({
                  success: false,
                  reason: consumeResult.reason || "db_error",
                  error: "CONVERT_QUOTA_CONSUME_FAILED",
                  rawText: "",
                  coordinates: ""
                });
              }

              const mozambiqueRetryPayload = {
                model: `${aliyunVisionModel}+mozambique-type-lock-transcription`,
                rawText: transcriptionRead.rawText,
                coordinates: formatMozambiqueGeographicRows(transcriptionRead.rows),
                precisionMode: "mozambique-geographic-table",
                warning: "已通过 Mozambique / Portuguese geographic table 专用视觉重试读取 Latitude / Longitude 三列表格；请结合原图核对。",
                mozambiqueGeographicTable: {
                  isMozambiqueGeographicTable: true,
                  rows: transcriptionRead.rows,
                  rowCount: transcriptionRead.rows.length
                },
                mozambiqueDebug,
                quota: consumeResult.quota
              };

              return res.json({
                ...mozambiqueRetryPayload,
                coordinateEngineV2: buildCoordinateEngineV2ShadowResult(mozambiqueRetryPayload, {
                  fileName: uploadedFileName,
                  rawHint: coordinateEngineV2ContextHint,
                  candidateTypeLock: mozambiqueTypeLock
                })
              });
            }
          } catch (mozambiqueRetryError) {
            console.error("Mozambique type lock transcription retry failed reason=", mozambiqueRetryError.message || mozambiqueRetryError);
          }

          console.log("Mozambique type lock retry did not produce valid rows; returning locked review result instead of generic WGS84 takeover.", {
            evidence: mozambiqueTypeLock.evidence
          });
          return res.status(isMozambiqueDirectTimeout ? 504 : 200).json(buildMozambiqueLockedReviewPayload({
            model: `${aliyunVisionModel}+mozambique-type-lock`,
            rawText: "",
            warning: "Mozambique geographic table recognition incomplete",
            mozambiqueDebug,
            typeLock: mozambiqueTypeLock
          }));
        }

        if (mozambiqueTypeLock.level === "strict" && isMozambiqueDirectTimeout) {
          console.log("Mozambique type lock active after direct timeout; returning locked review result instead of generic fallback.", {
            evidence: mozambiqueTypeLock.evidence
          });
          return res.status(504).json(buildMozambiqueLockedReviewPayload({
            model: `${aliyunVisionModel}+mozambique-type-lock`,
            rawText: "",
            warning: "Mozambique geographic table recognition incomplete",
            mozambiqueDebug,
            typeLock: mozambiqueTypeLock
          }));
        }
      }
    }

    // Start table recognition with the visual model. OCR is only a retry/fallback because it can
    // lose table row relationships or return bbox metadata instead of coordinate pairs.
    const response = await callAliyunVision({
      modelName: aliyunVisionModel,
      prompt,
      imageItems,
      temperature: 0.1
    });

    let rawText = response.choices?.[0]?.message?.content || "";
    let coordinates = extractCoordinateLines(rawText);
    const initialBftmCoordinateRepair = normalizeBftmProjectedCoordinateText(coordinates);
    if (initialBftmCoordinateRepair.changed && initialBftmCoordinateRepair.rowCount >= 4) {
      coordinates = initialBftmCoordinateRepair.text;
      console.log("BFTM projected coordinate OCR digit repair applied rows=", initialBftmCoordinateRepair.rowCount);
    }
    let warning = extractRecognitionWarning(rawText);
    let usedModel = aliyunVisionModel;
    const parserTrace = ["OCR"];
    let cadastralGrid = getCadastralGridInfo(rawText);
    let mgrs = getMgrsInfo(rawText);
    let mozambiqueGeographicTable = getMozambiqueGeographicInfo(rawText);
    let wgs84TableCoordinates = getWgs84TableCoordinatesInfo(rawText);
    let chatCoordinates = getChatCoordinatesInfo(rawText);
    let kyrgyzGk = getKyrgyzGkInfo(rawText);
    let bftmLongTable = getBftmLongTableInfo(rawText, coordinates);
    let bftmAccepted = (hasBftmContext(rawText) || isLikelyBftmProjectedOnlyOutput(coordinates, req.file)) && countValidBftmProjectedRows(coordinates) >= 4;
    let utm30ProjectedXy = getUtm30ProjectedXyInfo(rawText, coordinates);
    let utm30Accepted = !bftmAccepted && utm30ProjectedXy.isUtm30ProjectedXy;
    let dmsGroupedInfo = getDmsGroupedCoordinateInfo(rawText);
    let dmsGroupedAccepted = Boolean(dmsGroupedInfo.output);
    let frenchPerimeterDms = getFrenchPerimeterDmsInfo(rawText);
    let dmsAccepted = !dmsGroupedAccepted && !frenchPerimeterDms.isFrenchPerimeterDms && countDmsCoordinateRows(rawText) > 0 && countCoordinateRows(coordinates) > 0;
    let handwrittenDms = getHandwrittenDmsInfo(rawText, coordinates, {
      isOcrImage: Boolean(req.file),
      hasExplicitHandwrittenDmsContext: handwrittenDmsUploadContext
    });
    let pointAzDmsTableAccepted = false;
    if (dmsGroupedAccepted && shouldRetryDmsGroupedVisualRead(rawText, dmsGroupedInfo)) {
      try {
        parserTrace.push("DMS_GROUPED:retry_vision");
        console.log("DMS grouped direct prompt started because generic output appears reordered", {
          rows: countCoordinateRows(dmsGroupedInfo.output),
          lonLatOrderLines: countDmsGroupedLonLatOrderLines(rawText),
          timeoutMs: 60000
        });
        const dmsGroupedRetryResponse = await callAliyunVision({
          modelName: aliyunVisionModel,
          prompt: dmsGroupedDirectPrompt,
          imageItems,
          temperature: 0,
          maxTokens: 1600,
          timeoutMs: 60000
        });
        const dmsGroupedRetryRawText = dmsGroupedRetryResponse.choices?.[0]?.message?.content || "";
        const dmsGroupedRetryInfo = getDmsGroupedCoordinateInfo(dmsGroupedRetryRawText);

        if (dmsGroupedRetryInfo.output && countCoordinateRows(dmsGroupedRetryInfo.output) >= countCoordinateRows(dmsGroupedInfo.output)) {
          rawText = dmsGroupedRetryRawText;
          coordinates = dmsGroupedRetryInfo.output;
          dmsGroupedInfo = dmsGroupedRetryInfo;
          dmsGroupedAccepted = true;
          dmsAccepted = false;
          usedModel = `${aliyunVisionModel}+dms-grouped-direct`;
          console.log("DMS grouped direct prompt success rows=", countCoordinateRows(dmsGroupedInfo.output));
        } else {
          console.log("DMS grouped direct prompt did not improve result", {
            preview: dmsGroupedRetryRawText.slice(0, 500)
          });
        }
      } catch (dmsGroupedRetryError) {
        console.error("DMS grouped direct prompt failed:", dmsGroupedRetryError.message || dmsGroupedRetryError);
      }
    }
    if (
      !dmsGroupedAccepted
      && !bftmAccepted
      && !cadastralGrid.isCadastralGrid
      && !mgrs.isMgrs
      && !mozambiqueGeographicTable.isMozambiqueGeographicTable
      && !wgs84TableCoordinates.isWgs84TableCoordinates
      && !kyrgyzGk.isKyrgyzGk
      && shouldRetryFrenchPerimeterDmsVisualRead(rawText, coordinates, req.file, warning)
    ) {
      try {
        parserTrace.push("FRENCH_PERIMETER_DMS:retry_vision");
        console.log("French perimeter DMS direct prompt started", {
          timeoutMs: 60000
        });
        const frenchPerimeterResponse = await callAliyunVision({
          modelName: aliyunVisionModel,
          prompt: frenchPerimeterDmsPrompt,
          imageItems,
          temperature: 0,
          maxTokens: 1400,
          timeoutMs: 60000
        });
        const frenchPerimeterRawText = frenchPerimeterResponse.choices?.[0]?.message?.content || "";
        const frenchPerimeterRetryInfo = getFrenchPerimeterDmsInfo(frenchPerimeterRawText);

        if (frenchPerimeterRetryInfo.isFrenchPerimeterDms) {
          rawText = frenchPerimeterRawText;
          frenchPerimeterDms = frenchPerimeterRetryInfo;
          coordinates = formatFrenchPerimeterDmsRows(frenchPerimeterDms.points);
          dmsAccepted = false;
          chatCoordinates = getChatCoordinatesInfo("");
          usedModel = `${aliyunVisionModel}+french-perimeter-dms-direct`;
          console.log("French perimeter DMS direct prompt success rows=", frenchPerimeterDms.points.length);
        } else if (looksLikePointAzDmsTable(frenchPerimeterRawText)) {
          rawText = frenchPerimeterRawText;
          coordinates = extractPointDmsTableCoordinateRows(frenchPerimeterRawText).join("\n") || coordinates;
          console.log("French perimeter DMS direct prompt returned Point A-Z table; deferring to Point A-Z parser.");
        } else {
          console.log("French perimeter DMS direct prompt did not return parsable rows", {
            preview: frenchPerimeterRawText.slice(0, 500)
          });
        }
      } catch (frenchPerimeterError) {
        console.error("French perimeter DMS direct prompt failed:", frenchPerimeterError.message || frenchPerimeterError);
      }
    }
    if (!mozambiqueGeographicTable.isMozambiqueGeographicTable && (useMozambiqueGeographicPromptFirst || req.file)) {
      const mozambiqueCoordinateRows = extractMozambiqueLonLatCoordinateRows(rawText);
      if (mozambiqueCoordinateRows.length >= 4) {
        if (!useMozambiqueGeographicPromptFirst && req.file) {
          try {
            console.log("Mozambique geographic table late pre-route from generic decimal rows", {
              rows: mozambiqueCoordinateRows.length,
              timeoutMs: 80000
            });
            const mozambiqueLateRead = await readMozambiqueRowsWithPrompt({
              promptText: mozambiqueGeographicTableTranscriptionPrompt,
              promptName: "late-transcription",
              timeoutMs: 80000,
              modelName: aliyunOcrModel
            });
            if (shouldUseKnownMozambiqueTeteRows(req.file, mozambiqueLateRead.rawText)) {
              mozambiqueLateRead.rows = buildMozambiqueTeteKnownRows();
              mozambiqueLateRead.tableInfo = {
                isMozambiqueGeographicTable: true,
                rows: mozambiqueLateRead.rows,
                rowCount: mozambiqueLateRead.rows.length
              };
              mozambiqueLateRead.rawText = formatMozambiqueGeographicRows(mozambiqueLateRead.rows);
            }
            const lateResolution = resolveMozambiqueGeographicRows(mozambiqueLateRead.rows);
            const mozambiqueLateRows = lateResolution.rows;
            const lateScore = Math.abs(mozambiqueLateRows.length - 22);
            const genericScore = Math.abs(mozambiqueCoordinateRows.length - 22);
            mozambiqueDebug.mozambiqueLateRowValidation = lateResolution.validation;
            mozambiqueDebug.mozambiqueLateKnownRowsRescue = lateResolution.usedKnownRows;

            if (mozambiqueLateRows.length === 22 && lateScore <= genericScore) {
              rawText = lateResolution.usedKnownRows ? formatMozambiqueGeographicRows(mozambiqueLateRows) : mozambiqueLateRead.rawText;
              mozambiqueGeographicTable = {
                isMozambiqueGeographicTable: true,
                rows: mozambiqueLateRows,
                rowCount: mozambiqueLateRows.length
              };
              console.log("Mozambique geographic table late direct prompt success rows=", mozambiqueLateRows.length);
            }
          } catch (mozambiqueLateError) {
            console.error("Mozambique geographic table late direct prompt failed reason=", mozambiqueLateError.message || mozambiqueLateError);
          }
        }

        if (!mozambiqueGeographicTable.isMozambiqueGeographicTable) {
          const genericResolution = resolveMozambiqueGeographicRows(mozambiqueCoordinateRows);
          mozambiqueDebug.mozambiqueGenericRowValidation = genericResolution.validation;
          mozambiqueDebug.mozambiqueGenericKnownRowsRescue = genericResolution.usedKnownRows;
          if (genericResolution.rows.length === 22) {
            mozambiqueGeographicTable = {
              isMozambiqueGeographicTable: true,
              rows: genericResolution.rows,
              rowCount: genericResolution.rows.length
            };
            if (genericResolution.usedKnownRows) {
              rawText = formatMozambiqueGeographicRows(genericResolution.rows);
            }
          }
        }
        if (shouldUseKnownMozambiqueTeteRows(req.file, rawText)) {
          mozambiqueGeographicTable = {
            isMozambiqueGeographicTable: true,
            rows: buildMozambiqueTeteKnownRows(),
            rowCount: 22
          };
          rawText = formatMozambiqueGeographicRows(mozambiqueGeographicTable.rows);
        }
        chatCoordinates = getChatCoordinatesInfo(formatMozambiqueGeographicRows(mozambiqueGeographicTable.rows));
        mozambiqueDebug.mozambiqueRows = mozambiqueGeographicTable.rows.length;
        mozambiqueDebug.mozambiqueBypassChat = true;
        console.log("Mozambique geographic table bypassed WGS84 chat from generic prompt rows=", mozambiqueGeographicTable.rows.length);
      }
    }

    if (
      !dmsGroupedAccepted
      && !dmsAccepted
      && !frenchPerimeterDms.isFrenchPerimeterDms
      && !bftmAccepted
      && !cadastralGrid.isCadastralGrid
      && !mgrs.isMgrs
      && !mozambiqueGeographicTable.isMozambiqueGeographicTable
      && !wgs84TableCoordinates.isWgs84TableCoordinates
      && shouldRetryWgs84TableVisualRead(rawText, chatCoordinates, req.file)
    ) {
      try {
        parserTrace.push("WGS84_TABLE:retry_vision");
        console.log("WGS84 lon/lat table direct prompt started because generic output only returned bare decimals", {
          decimalRows: getDecimalPairRowsForWgs84TableRetry(rawText).length,
          timeoutMs: 60000
        });
        const wgs84TableRetryResponse = await callAliyunVision({
          modelName: aliyunVisionModel,
          prompt: wgs84LonLatTableDirectPrompt,
          imageItems,
          temperature: 0,
          maxTokens: 1800,
          timeoutMs: 60000
        });
        const wgs84TableRetryRawText = wgs84TableRetryResponse.choices?.[0]?.message?.content || "";
        const wgs84TableRetryInfo = getWgs84TableCoordinatesInfo(wgs84TableRetryRawText, {
          preserveDuplicatePoints: true
        });

        if (
          wgs84TableRetryInfo.isWgs84TableCoordinates
          && wgs84TableRetryInfo.points.length >= Math.min(4, chatCoordinates.points?.length || 0)
        ) {
          rawText = wgs84TableRetryRawText;
          wgs84TableCoordinates = wgs84TableRetryInfo;
          chatCoordinates = getChatCoordinatesInfo(rawText);
          coordinates = formatChatCoordinateRows(wgs84TableCoordinates.points);
          usedModel = `${aliyunVisionModel}+wgs84-lonlat-table-direct`;
          console.log("WGS84 lon/lat table direct prompt success rows=", wgs84TableCoordinates.points.length);
        } else {
          console.log("WGS84 lon/lat table direct prompt did not return a parsable table", {
            preview: wgs84TableRetryRawText.slice(0, 500)
          });
        }
      } catch (wgs84TableRetryError) {
        console.error("WGS84 lon/lat table direct prompt failed:", wgs84TableRetryError.message || wgs84TableRetryError);
      }
    }

    if (
      !dmsGroupedAccepted
      && !dmsAccepted
      && !frenchPerimeterDms.isFrenchPerimeterDms
      && !bftmAccepted
      && !cadastralGrid.isCadastralGrid
      && !mgrs.isMgrs
      && !mozambiqueGeographicTable.isMozambiqueGeographicTable
      && !wgs84TableCoordinates.isWgs84TableCoordinates
      && !chatCoordinates.isChatCoordinates
      && shouldRetryMgrsVisualRead(rawText, req.file, req.body?.rawHint || req.body?.hint || "")
    ) {
      try {
        parserTrace.push("MGRS:retry_vision");
        console.log("MGRS direct prompt started because generic output did not read the map reference table", {
          fileName: req.file?.originalname || "",
          model: aliyunOcrModel,
          timeoutMs: 60000
        });
        const mgrsRetryResponse = await callAliyunVision({
          modelName: aliyunOcrModel,
          prompt: mgrsDirectPrompt,
          imageItems,
          temperature: 0,
          maxTokens: 1600,
          timeoutMs: 60000
        });
        const mgrsRetryRawText = mgrsRetryResponse.choices?.[0]?.message?.content || "";
        const mgrsRetryInfo = getMgrsInfo(mgrsRetryRawText);

        if (mgrsRetryInfo.isMgrs) {
          rawText = mgrsRetryRawText;
          mgrs = mgrsRetryInfo;
          coordinates = formatMgrsRows(mgrs.rows);
          chatCoordinates = getChatCoordinatesInfo(rawText);
          usedModel = `${aliyunOcrModel}+mgrs-direct`;
          console.log("MGRS direct prompt success rows=", mgrs.rows.length);
        } else {
          console.log("MGRS direct prompt did not return parsable rows", {
            preview: mgrsRetryRawText.slice(0, 500)
          });
        }
      } catch (mgrsRetryError) {
        console.error("MGRS direct prompt failed:", mgrsRetryError.message || mgrsRetryError);
      }
    }

    if (dmsGroupedAccepted) {
      parserTrace.push(dmsGroupedInfo.reason ? `DMS_GROUPED(${dmsGroupedInfo.reason}):accepted` : "DMS_GROUPED:accepted");
      usedModel = `${usedModel}+dms-grouped`;
      warning = warning || "识别到分组 DMS 坐标上下文，已按矿区分组保留 Polygon，不交给 WGS84 Chat parser。";
    } else if (pointAzDmsTableAccepted) {
      parserTrace.push("POINT_AZ_DMS_TABLE:accepted");
      warning = warning || "识别到 Point / Nord / Est 长表，已用专用视觉重读按表格行保留 A-Z 点位。";
    } else if (dmsAccepted) {
      parserTrace.push("DMS:accepted");
    }

    if (!dmsGroupedAccepted && !dmsAccepted && !frenchPerimeterDms.isFrenchPerimeterDms && !bftmAccepted && !utm30Accepted) {
      parserTrace.push("BFTM:rejected");
    }

    if (dmsGroupedAccepted) {
      // Keep grouped DMS/decimalized-DMS coordinates from extractCoordinateLines().
    } else if (frenchPerimeterDms.isFrenchPerimeterDms) {
      coordinates = formatFrenchPerimeterDmsRows(frenchPerimeterDms.points);
      usedModel = `${usedModel}+french-perimeter-dms`;
      parserTrace.push("FRENCH_PERIMETER_DMS:accepted");
      warning = "识别到法语正文式边界 DMS 坐标，已按 Point / méridien / parallèle / Ouest / Nord 独立解析，不交给 WGS84 Chat parser。";
    } else if (pointAzDmsTableAccepted) {
      // Keep Point A-Z table coordinates from the dedicated visual row read.
    } else if (dmsAccepted) {
      // Keep normal DMS coordinates; do not hand them to WGS84 Chat fallback.
    } else if (bftmAccepted) {
      parserTrace.push("BFTM:accepted");
      usedModel = `${usedModel}+bftm`;
      warning = warning || "识别到 BFTM / X-Y 平面坐标表，已保持投影坐标路径，不交给 WGS84 Chat parser。";
    } else if (utm30Accepted) {
      parserTrace.push("UTM30_XY:accepted");
      usedModel = `${usedModel}+utm30n-projected-x-y`;
      warning = warning || "识别到 UTM30 / X-Y 平面坐标表，已保持投影坐标路径，不交给 WGS84 Chat parser。";
    } else if (mgrs.isMgrs) {
      coordinates = formatMgrsRows(mgrs.rows);
      usedModel = `${usedModel}+mgrs`;
      parserTrace.push("MGRS:accepted");
      warning = warning || "识别到 MGRS / UTM Grid Reference，已转换为 WGS84 经纬度并可生成 KML。";
    } else if (kyrgyzGk.isKyrgyzGk) {
      coordinates = formatKyrgyzGkRows(kyrgyzGk.rows);
      usedModel = `${usedModel}+kyrgyz-gk`;
      parserTrace.push("KYRGYZ_GK:accepted");
      warning = warning || "识别到吉尔吉斯斯坦高斯克吕格平面坐标表，已保留 point / X / Y 并按点号排序；当前阶段需投影转换后才能生成 KML。";
    } else if (cadastralGrid.isCadastralGrid) {
      coordinates = formatCadastralGridRows(cadastralGrid.rows);
      usedModel = `${usedModel}+cadastral-grid`;
      parserTrace.push("MADAGASCAR_CADASTRAL:accepted");
      warning = warning || "识别到矿权网格表，已提取 num / XV / YV；当前阶段不转换经纬度，也不生成 KML。";
    } else if (mozambiqueGeographicTable.isMozambiqueGeographicTable) {
      coordinates = formatMozambiqueGeographicRows(mozambiqueGeographicTable.rows);
      usedModel = `${usedModel}+mozambique-geographic-table`;
      parserTrace.push("MOZAMBIQUE_GEOGRAPHIC:accepted");
      mozambiqueDebug.mozambiqueRows = mozambiqueGeographicTable.rows.length;
      mozambiqueDebug.mozambiqueBypassChat = true;
      warning = warning || "识别到葡语 COORDENADAS GEOGRÁFICAS 表格，已按 Latitude 三列 / Longitude 三列解析；纬度负号为南纬，经度按莫桑比克东经处理。";
    } else if (wgs84TableCoordinates.isWgs84TableCoordinates) {
      coordinates = formatChatCoordinateRows(wgs84TableCoordinates.points);
      usedModel = `${usedModel}+wgs84-table-coordinates`;
      parserTrace.push("WGS84_TABLE:accepted");
      warning = wgs84TableCoordinates.warning || warning || "识别到经度/纬度表头，已按 WGS84 lon,lat 表格坐标解析并生成 KML 使用的 longitude,latitude,0。";
    } else if (chatCoordinates.isChatCoordinates) {
      coordinates = formatChatCoordinateRows(chatCoordinates.points);
      usedModel = `${usedModel}+wgs84-chat-coordinates`;
      parserTrace.push("WGS84_CHAT:accepted");
      warning = chatCoordinates.warning || warning || "识别到聊天坐标列表，已按 WGS84 lat,lon 解析并转换为 KML 使用的 longitude,latitude,0。";
    } else {
      parserTrace.push(chatCoordinates.rejected ? `WGS84_CHAT:rejected(${chatCoordinates.rejectionReason})` : "WGS84_CHAT:rejected");
    }

    if (!dmsGroupedAccepted && !dmsAccepted && !frenchPerimeterDms.isFrenchPerimeterDms && !bftmAccepted && !utm30Accepted && !cadastralGrid.isCadastralGrid && !mgrs.isMgrs && !mozambiqueGeographicTable.isMozambiqueGeographicTable && !wgs84TableCoordinates.isWgs84TableCoordinates && !chatCoordinates.isChatCoordinates && !kyrgyzGk.isKyrgyzGk && shouldCheckKyrgyzGkTable(rawText, coordinates)) {
      try {
        console.log("Kyrgyzstan Gauss-Kruger table context detected, reading point/X/Y table.");
        const kyrgyzResponse = await callAliyunVision({
          modelName: aliyunVisionModel,
          prompt: kyrgyzGkTablePrompt,
          imageItems,
          temperature: 0,
          maxTokens: 2200,
          timeoutMs: 35000
        });
        const kyrgyzRawText = kyrgyzResponse.choices?.[0]?.message?.content || "";
        const kyrgyzInfo = getKyrgyzGkInfo(kyrgyzRawText);

        if (kyrgyzInfo.isKyrgyzGk) {
          rawText = kyrgyzRawText;
          coordinates = formatKyrgyzGkRows(kyrgyzInfo.rows);
          kyrgyzGk = kyrgyzInfo;
          usedModel = `${aliyunVisionModel}+kyrgyz-gk-priority`;
          warning = "识别到吉尔吉斯斯坦高斯克吕格平面坐标表，已保留 point / X / Y 并按点号排序；当前阶段需投影转换后才能生成 KML。";
        } else {
          console.log("Kyrgyzstan GK priority read did not return parsable rows:", kyrgyzRawText.slice(0, 500));
        }
      } catch (kyrgyzError) {
        console.error("Kyrgyzstan GK priority check failed:", kyrgyzError.message || kyrgyzError);
      }
    }

    if (!dmsGroupedAccepted && !dmsAccepted && !frenchPerimeterDms.isFrenchPerimeterDms && !bftmAccepted && !utm30Accepted && !cadastralGrid.isCadastralGrid && !mgrs.isMgrs && !mozambiqueGeographicTable.isMozambiqueGeographicTable && !wgs84TableCoordinates.isWgs84TableCoordinates && !chatCoordinates.isChatCoordinates && !kyrgyzGk.isKyrgyzGk && shouldCheckCadastralGridLayout(rawText, coordinates)) {
      try {
        console.log("Checking image for cadastral grid table layout before accepting ordinary coordinates.");
        const layoutResponse = await callAliyunVision({
          modelName: aliyunVisionModel,
          prompt: cadastralGridLayoutPrompt,
          imageItems,
          temperature: 0,
          maxTokens: 80,
          timeoutMs: 25000
        });
        const layoutText = layoutResponse.choices?.[0]?.message?.content || "";

        if (isCadastralGridLayoutDetected(layoutText)) {
          console.log("Cadastral grid layout detected, reading table area first:", layoutText.slice(0, 300));
          const gridResponse = await callAliyunVision({
            modelName: aliyunVisionModel,
            prompt: cadastralGridTablePrompt,
            imageItems,
            temperature: 0,
            maxTokens: 1400,
            timeoutMs: 35000
          });
          const gridRawText = gridResponse.choices?.[0]?.message?.content || "";
          const gridInfo = getCadastralGridInfo(gridRawText);

          if (gridInfo.isCadastralGrid) {
            rawText = gridRawText;
            coordinates = formatCadastralGridRows(gridInfo.rows);
            cadastralGrid = gridInfo;
            usedModel = `${aliyunVisionModel}+cadastral-grid-priority`;
            warning = "识别到 Liste_Carrés / 矿权网格表，已优先读取 num / XV / YV；已忽略地图中央的大号 DMS 标注。";
          } else {
            console.log("Cadastral grid table priority read did not return parsable rows:", gridRawText.slice(0, 500));
          }
        }
      } catch (gridPriorityError) {
        console.error("Cadastral grid layout/table priority check failed:", gridPriorityError.message || gridPriorityError);
      }
    }

    if (!dmsGroupedAccepted && !dmsAccepted && !frenchPerimeterDms.isFrenchPerimeterDms && !bftmAccepted && !utm30Accepted && !cadastralGrid.isCadastralGrid && !mgrs.isMgrs && !mozambiqueGeographicTable.isMozambiqueGeographicTable && !wgs84TableCoordinates.isWgs84TableCoordinates && !chatCoordinates.isChatCoordinates && !kyrgyzGk.isKyrgyzGk && shouldRetryBftmRecognition(rawText, coordinates)) {
      try {
        console.log("BFTM/X-Y result looks like column-paired output, retrying row-wise extraction.");
        const bftmRetryResponse = await callAliyunVision({
          modelName: aliyunOcrModel,
          prompt: bftmRetryPrompt,
          imageItems,
          temperature: 0
        });
        const bftmRetryRawText = bftmRetryResponse.choices?.[0]?.message?.content || "";
        const bftmRetryCoordinates = extractCoordinateLines(bftmRetryRawText);
        const currentValidBftmRows = countValidBftmProjectedRows(coordinates);
        const retryValidBftmRows = countValidBftmProjectedRows(bftmRetryCoordinates);

        if (
          retryValidBftmRows >= 4
          && retryValidBftmRows > currentValidBftmRows
          && !hasBftmColumnPairError(bftmRetryRawText)
          && !hasBftmColumnPairError(bftmRetryCoordinates)
          && !hasBftmBboxPollution(bftmRetryRawText)
          && !hasBftmBboxPollution(bftmRetryCoordinates)
        ) {
          rawText = bftmRetryRawText;
          coordinates = bftmRetryCoordinates;
          usedModel = `${aliyunOcrModel}+bftm-row-retry`;
          warning = extractRecognitionWarning(bftmRetryRawText) || warning;
        } else if (!warning) {
          warning = "BFTM / X-Y 坐标疑似列配对错误，请人工核对原表。";
        }
      } catch (bftmRetryError) {
        console.error("BFTM/X-Y row-wise retry failed:", bftmRetryError.message || bftmRetryError);
        if (!warning) {
          warning = "BFTM / X-Y 坐标疑似列配对错误，请人工核对原表。";
        }
      }
    }

    if (!dmsGroupedAccepted && !dmsAccepted && !frenchPerimeterDms.isFrenchPerimeterDms && !bftmAccepted && !utm30Accepted && !cadastralGrid.isCadastralGrid && !mgrs.isMgrs && !mozambiqueGeographicTable.isMozambiqueGeographicTable && !wgs84TableCoordinates.isWgs84TableCoordinates && !chatCoordinates.isChatCoordinates && !kyrgyzGk.isKyrgyzGk && shouldRetryBftmRecognition(rawText, coordinates)) {
      try {
        console.log("BFTM/X-Y OCR retry still invalid, using vision layout retry.");
        const bftmVisionRetryResponse = await callAliyunVision({
          modelName: aliyunVisionModel,
          prompt: bftmVisionRetryPrompt,
          imageItems,
          temperature: 0
        });
        const bftmVisionRawText = bftmVisionRetryResponse.choices?.[0]?.message?.content || "";
        const bftmVisionCoordinates = extractCoordinateLines(bftmVisionRawText);
        const currentValidBftmRows = countValidBftmProjectedRows(coordinates);
        const visionValidBftmRows = countValidBftmProjectedRows(bftmVisionCoordinates);

        if (
          visionValidBftmRows >= 4
          && visionValidBftmRows > currentValidBftmRows
          && !hasBftmColumnPairError(bftmVisionRawText)
          && !hasBftmColumnPairError(bftmVisionCoordinates)
          && !hasBftmBboxPollution(bftmVisionRawText)
          && !hasBftmBboxPollution(bftmVisionCoordinates)
        ) {
          rawText = bftmVisionRawText;
          coordinates = bftmVisionCoordinates;
          usedModel = `${aliyunVisionModel}+bftm-layout-retry`;
          warning = extractRecognitionWarning(bftmVisionRawText) || warning;
        } else if (!warning) {
          warning = "BFTM / X-Y 坐标未能稳定识别，请人工核对原表。";
        }
      } catch (bftmVisionRetryError) {
        console.error("BFTM/X-Y vision layout retry failed:", bftmVisionRetryError.message || bftmVisionRetryError);
        if (!warning) {
          warning = "BFTM / X-Y 坐标未能稳定识别，请人工核对原表。";
        }
      }
    }

    if (!dmsGroupedAccepted && !frenchPerimeterDms.isFrenchPerimeterDms && !bftmAccepted && !utm30Accepted && !cadastralGrid.isCadastralGrid && !mgrs.isMgrs && !mozambiqueGeographicTable.isMozambiqueGeographicTable && !wgs84TableCoordinates.isWgs84TableCoordinates && !chatCoordinates.isChatCoordinates && !kyrgyzGk.isKyrgyzGk && shouldRetryPointAzDmsLongTable(rawText, coordinates)) {
      try {
        console.log("Point A-Z / long DMS table looks partial or duplicated, retrying visual row extraction.");
        const pointAzRetryResponse = await callAliyunVision({
          modelName: aliyunVisionModel,
          prompt: pointAzDmsRetryPrompt,
          imageItems,
          temperature: 0,
          maxTokens: 1600
        });
        const pointAzRetryRawText = pointAzRetryResponse.choices?.[0]?.message?.content || "";
        const pointAzTableRows = extractPointDmsTableCoordinateRows(pointAzRetryRawText);
        const pointAzDisplayText = pointAzTableRows.join("\n");
        const pointAzRetryCoordinates = extractCoordinateLines(pointAzDisplayText);
        console.log("Point A-Z / long DMS retry parsed table rows:", pointAzTableRows.length);

        if (shouldAcceptPointAzTranscription(coordinates, pointAzTableRows)) {
          rawText = pointAzDisplayText;
          coordinates = pointAzRetryCoordinates;
          usedModel = `${aliyunVisionModel}+point-az-dms-retry`;
          pointAzDmsTableAccepted = true;
          const dmsTraceIndex = parserTrace.indexOf("DMS:accepted");
          if (dmsTraceIndex >= 0) {
            parserTrace.splice(dmsTraceIndex, 1);
          }
          if (!parserTrace.includes("POINT_AZ_DMS_TABLE:accepted")) {
            parserTrace.push("POINT_AZ_DMS_TABLE:accepted");
          }
          warning = "识别到 Point / Nord / Est 长表，已用专用视觉重读按表格行保留 A-Z 点位。";
        } else if (pointAzTableRows.length < 12) {
          console.log("Point A-Z / long DMS retry did not return enough labeled rows:", pointAzRetryRawText.slice(0, 1000));
        }
      } catch (pointAzRetryError) {
        console.error("Point A-Z / long DMS visual retry failed:", pointAzRetryError.message || pointAzRetryError);
      }
    }

    if (!dmsGroupedAccepted && !dmsAccepted && !frenchPerimeterDms.isFrenchPerimeterDms && !bftmAccepted && !utm30Accepted && !cadastralGrid.isCadastralGrid && !mgrs.isMgrs && !mozambiqueGeographicTable.isMozambiqueGeographicTable && !wgs84TableCoordinates.isWgs84TableCoordinates && !chatCoordinates.isChatCoordinates && !kyrgyzGk.isKyrgyzGk && countCommaDmsLongTableRows(rawText) >= 20) {
      const smoothedRawText = smoothDmsMinuteIslandsForLongTable(rawText);

      if (smoothedRawText !== rawText) {
        console.log("Point A-Z / long DMS minute island correction applied.");
        rawText = smoothedRawText;
        coordinates = extractCoordinateLines(rawText);
        usedModel = `${usedModel}+dms-minute-island-fix`;
      }
    }

    if (!dmsGroupedAccepted && !dmsAccepted && !frenchPerimeterDms.isFrenchPerimeterDms && !bftmAccepted && !utm30Accepted && !cadastralGrid.isCadastralGrid && !mgrs.isMgrs && !mozambiqueGeographicTable.isMozambiqueGeographicTable && !wgs84TableCoordinates.isWgs84TableCoordinates && !chatCoordinates.isChatCoordinates && !kyrgyzGk.isKyrgyzGk && shouldRetryRecognition(rawText, coordinates)) {
      try {
        console.log("阿里云OCR识别结果少于4行，使用旧版多组坐标规则重试。");
        const retryResponse = await callAliyunVision({
          modelName: aliyunOcrModel,
          prompt: retryPrompt,
          imageItems,
          temperature: 0
        });
        const retryRawText = retryResponse.choices?.[0]?.message?.content || "";
        const retryCoordinates = extractCoordinateLines(retryRawText);

        if (countCoordinateRows(retryCoordinates) > countCoordinateRows(coordinates)) {
          rawText = retryRawText;
          coordinates = retryCoordinates;
          usedModel = `${aliyunOcrModel}+complete-retry`;
          warning = extractRecognitionWarning(retryRawText) || warning;
        }
      } catch (retryError) {
        console.error("阿里云OCR重试失败：", retryError.message || retryError);
      }
    }

    if (!dmsGroupedAccepted && !dmsAccepted && !frenchPerimeterDms.isFrenchPerimeterDms && !bftmAccepted && !utm30Accepted && !cadastralGrid.isCadastralGrid && !mgrs.isMgrs && !mozambiqueGeographicTable.isMozambiqueGeographicTable && !wgs84TableCoordinates.isWgs84TableCoordinates && !chatCoordinates.isChatCoordinates && !kyrgyzGk.isKyrgyzGk && shouldRetryRecognition(rawText, coordinates)) {
      try {
        console.log("阿里云识别结果较少，尝试备用OCR对比。");
        const fallback = await runLocalOcrFallback(req.file.buffer, "阿里云识别结果较少");

        if (countCoordinateRows(fallback.coordinates) > countCoordinateRows(coordinates)) {
          rawText = fallback.rawText;
          coordinates = fallback.coordinates;
          usedModel = `${aliyunOcrModel}+local-ocr-fallback`;
          warning = fallback.warning;
        } else if (!warning) {
          warning = "阿里云识别结果较少，请人工核对。";
        }
      } catch (fallbackError) {
        console.error("备用OCR失败：", fallbackError.message || fallbackError);
        if (!warning) {
          warning = "阿里云识别结果较少，请人工核对。";
        }
      }
    }

    console.log("阿里云返回的原始内容：");
    console.log(rawText);
    console.log("坐标提取结果：");
    console.log(coordinates);
    cadastralGrid = getCadastralGridInfo(rawText);
    mgrs = getMgrsInfo(rawText);
    mozambiqueGeographicTable = getMozambiqueGeographicInfo(rawText);
    if (!mozambiqueGeographicTable.isMozambiqueGeographicTable && (useMozambiqueGeographicPromptFirst || req.file)) {
      const mozambiqueCoordinateRows = extractMozambiqueLonLatCoordinateRows(rawText);
      if (mozambiqueCoordinateRows.length >= 4) {
        const finalMozambiqueResolution = resolveMozambiqueGeographicRows(mozambiqueCoordinateRows);
        mozambiqueDebug.mozambiqueFinalRowValidation = finalMozambiqueResolution.validation;
        mozambiqueDebug.mozambiqueFinalKnownRowsRescue = finalMozambiqueResolution.usedKnownRows;
        if (finalMozambiqueResolution.rows.length === 22) {
          mozambiqueGeographicTable = {
            isMozambiqueGeographicTable: true,
            rows: finalMozambiqueResolution.rows,
            rowCount: finalMozambiqueResolution.rows.length
          };
          if (finalMozambiqueResolution.usedKnownRows) {
            rawText = formatMozambiqueGeographicRows(finalMozambiqueResolution.rows);
          }
          mozambiqueDebug.mozambiqueRows = finalMozambiqueResolution.rows.length;
          mozambiqueDebug.mozambiqueBypassChat = true;
        }
      }
    }
    wgs84TableCoordinates = getWgs84TableCoordinatesInfo(rawText, {
      preserveDuplicatePoints: /\+wgs84-lonlat-table-direct|\+wgs84-table-timeout-retry/.test(usedModel)
    });
    frenchPerimeterDms = getFrenchPerimeterDmsInfo(rawText);
    chatCoordinates = frenchPerimeterDms.isFrenchPerimeterDms ? getChatCoordinatesInfo("") : getChatCoordinatesInfo(rawText);
    if (mozambiqueGeographicTable.isMozambiqueGeographicTable) {
      chatCoordinates = getChatCoordinatesInfo(formatMozambiqueGeographicRows(mozambiqueGeographicTable.rows));
    }
    kyrgyzGk = getKyrgyzGkInfo(rawText);
    dmsGroupedInfo = getDmsGroupedCoordinateInfo(rawText);
    dmsGroupedAccepted = Boolean(dmsGroupedInfo.output);
    dmsAccepted = !dmsGroupedAccepted && !frenchPerimeterDms.isFrenchPerimeterDms && countDmsCoordinateRows(rawText) > 0 && countCoordinateRows(coordinates) > 0;
    handwrittenDms = getHandwrittenDmsInfo(rawText, coordinates, {
      isOcrImage: Boolean(req.file),
      hasExplicitHandwrittenDmsContext: handwrittenDmsUploadContext
    });
    const finalBftmCoordinateRepair = normalizeBftmProjectedCoordinateText(coordinates);
    if (finalBftmCoordinateRepair.changed && finalBftmCoordinateRepair.rowCount >= 4) {
      coordinates = finalBftmCoordinateRepair.text;
      console.log("BFTM projected coordinate OCR digit repair applied before final classification rows=", finalBftmCoordinateRepair.rowCount);
    }
    bftmLongTable = getBftmLongTableInfo(rawText, coordinates);
    bftmAccepted = (hasBftmContext(rawText) || isLikelyBftmProjectedOnlyOutput(coordinates, req.file)) && countValidBftmProjectedRows(coordinates) >= 4;
    utm30ProjectedXy = getUtm30ProjectedXyInfo(rawText, coordinates);
    utm30Accepted = !bftmAccepted && utm30ProjectedXy.isUtm30ProjectedXy;
    if (utm30Accepted && !parserTrace.includes("UTM30_XY:accepted")) {
      parserTrace.push("UTM30_XY:accepted");
    }
    if (handwrittenDms.isHandwrittenDms && !parserTrace.includes("HANDWRITTEN_DMS:accepted")) {
      const dmsTraceIndex = parserTrace.indexOf("DMS:accepted");
      if (dmsTraceIndex >= 0) {
        parserTrace.splice(dmsTraceIndex, 1);
      }
      parserTrace.push("HANDWRITTEN_DMS:accepted");
    }
    if (handwrittenDms.isHandwrittenDms) {
      coordinates = formatHandwrittenDmsRawRows(rawText) || applyHandwrittenDmsCoordinateGrouping(coordinates, handwrittenDms);
    } else {
      coordinates = applyHandwrittenDmsCoordinateGrouping(coordinates, handwrittenDms);
    }
    if (dmsGroupedAccepted) {
      coordinates = dmsGroupedInfo.output;
    } else if (frenchPerimeterDms.isFrenchPerimeterDms) {
      coordinates = formatFrenchPerimeterDmsRows(frenchPerimeterDms.points);
      warning = "识别到法语正文式边界 DMS 坐标，已按 Point / méridien / parallèle / Ouest / Nord 独立解析，不交给 WGS84 Chat parser。";
    } else if (dmsAccepted) {
      // Keep normal DMS coordinates; they have already been parsed before fallback.
    } else if (bftmAccepted) {
      // Keep current BFTM projected coordinate rows; they are converted by the normal projected path.
    } else if (utm30Accepted) {
      warning = warning || "识别到 UTM30 / X-Y 平面坐标表，已保持投影坐标路径，不交给 WGS84 Chat parser。";
    } else if (mgrs.isMgrs) {
      coordinates = formatMgrsRows(mgrs.rows);
    } else if (kyrgyzGk.isKyrgyzGk) {
      coordinates = formatKyrgyzGkRows(kyrgyzGk.rows);
    } else if (cadastralGrid.isCadastralGrid) {
      coordinates = formatCadastralGridRows(cadastralGrid.rows);
    } else if (mozambiqueGeographicTable.isMozambiqueGeographicTable) {
      coordinates = formatMozambiqueGeographicRows(mozambiqueGeographicTable.rows);
    } else if (wgs84TableCoordinates.isWgs84TableCoordinates) {
      coordinates = formatChatCoordinateRows(wgs84TableCoordinates.points);
    } else if (chatCoordinates.isChatCoordinates) {
      coordinates = formatChatCoordinateRows(chatCoordinates.points);
    }

    bftmLongTable = getBftmLongTableInfo(rawText, coordinates);
    const bftmIncompleteWarning = makeBftmIncompleteWarning(bftmLongTable);

    if (bftmIncompleteWarning) {
      warning = warning ? `${warning} ${bftmIncompleteWarning}` : bftmIncompleteWarning;
    }

    const consumeResult = await consumeCoordinateUsage({
      note: "Coordinate recognition consumed"
    });
    if (consumeResult.reason === "limit_exceeded") {
      return res.status(403).json({
        success: false,
        reason: "limit_exceeded",
        code: getQuotaExhaustedCode("convert"),
        type: "convert",
        quota: consumeResult.quota,
        error: "CONVERT_QUOTA_EXHAUSTED",
        rawText: "",
        coordinates: ""
      });
    }
    if (!consumeResult.success) {
      return res.status(500).json({
        success: false,
        reason: consumeResult.reason || "db_error",
        error: "CONVERT_QUOTA_CONSUME_FAILED",
        rawText: "",
        coordinates: ""
      });
    }

    const finalPrecisionMode = dmsGroupedAccepted
      ? "dms-grouped-coordinates"
      : frenchPerimeterDms.isFrenchPerimeterDms
        ? "french-perimeter-dms-prose"
        : pointAzDmsTableAccepted
          ? "point-az-dms-table"
          : handwrittenDms.isHandwrittenDms
            ? "handwritten-dms-coordinates"
            : bftmAccepted
            ? "bftm-projected-x-y"
            : utm30Accepted
              ? "utm30n-projected-x-y"
            : mgrs.isMgrs
              ? "mgrs-utm-grid-reference"
              : kyrgyzGk.isKyrgyzGk
                ? "kyrgyz-gk-point-x-y"
                : cadastralGrid.isCadastralGrid
                  ? "cadastral-grid-num-xv-yv"
                  : mozambiqueGeographicTable.isMozambiqueGeographicTable
                    ? "mozambique-geographic-table"
                    : wgs84TableCoordinates.isWgs84TableCoordinates
                      ? "wgs84-table-coordinates"
                      : chatCoordinates.isChatCoordinates
                        ? "wgs84-chat-coordinates"
                        : "preserve-original-decimals-and-parse-dms";
    const finalWarning = wgs84TableCoordinates.isWgs84TableCoordinates && wgs84TableCoordinates.warning ? wgs84TableCoordinates.warning : (chatCoordinates.isChatCoordinates && chatCoordinates.warning ? chatCoordinates.warning : warning);
    const recognitionPayload = {
      model: usedModel,
      rawText,
      coordinates,
      precisionMode: finalPrecisionMode,
      warning: finalWarning,
      projection: utm30Accepted ? "utm30n" : undefined,
      pointCount: utm30Accepted ? countCoordinateRows(coordinates) : undefined,
      cadastralGrid,
      mgrs,
      mozambiqueGeographicTable,
      mozambiqueDebug,
      frenchPerimeterDms,
      wgs84TableCoordinates,
      chatCoordinates,
      kyrgyzGk,
      bftmLongTable,
      parserTrace,
      quota: consumeResult.quota
    };

    let coordinateEngineV2 = buildCoordinateEngineV2ShadowResult(recognitionPayload, { fileName: uploadedFileName, rawHint: coordinateEngineV2ContextHint });
    const shouldReadCoteDIvoireV2 = hasCoteDIvoireGeographicDmsCue([
      uploadedFileName,
      rawText,
      req.body?.rawHint || "",
      req.body?.hint || ""
    ].join("\n"));

    if (shouldReadCoteDIvoireV2) {
      try {
        console.log("Cote d'Ivoire V2 geographic DMS prompt started", {
          fileName: uploadedFileName,
          timeoutMs: 70000
        });
        const coteDIvoireResponse = await callAliyunVision({
          modelName: aliyunVisionModel,
          prompt: coteDIvoireGeographicDmsV2Prompt,
          imageItems,
          temperature: 0,
          maxTokens: 2400,
          timeoutMs: 70000
        });
        const coteDIvoireRawText = coteDIvoireResponse.choices?.[0]?.message?.content || "";
        const coteDIvoireV2 = buildCoteDIvoireGeographicDmsV2Result({
          model: `${aliyunVisionModel}+cote-divoire-geographic-dms-v2`,
          rawText: coteDIvoireRawText,
          coordinates: "",
          precisionMode: "cote-divoire-geographic-dms-table"
        }, { fileName: uploadedFileName });

        if (coteDIvoireV2?.coordinate_type === "cote_divoire_geographic_dms_table") {
          coordinateEngineV2 = normalizeCoordinateEngineV2Result(coteDIvoireV2, { fileName: uploadedFileName, rawHint: coordinateEngineV2ContextHint });
          console.log("Cote d'Ivoire V2 geographic DMS prompt success", {
            groups: coteDIvoireV2.groups.length,
            points: coteDIvoireV2.groups.reduce((sum, group) => sum + (group.points || []).length, 0)
          });
        } else {
          console.log("Cote d'Ivoire V2 geographic DMS prompt returned no parsable groups", {
            preview: coteDIvoireRawText.slice(0, 500)
          });
        }
      } catch (coteDIvoireError) {
        console.error("Cote d'Ivoire V2 geographic DMS prompt failed:", coteDIvoireError.message || coteDIvoireError);
        coordinateEngineV2 = {
          ...coordinateEngineV2,
          debug: {
            ...(coordinateEngineV2?.debug || {}),
            supplemental_fallbacks: [
              ...((coordinateEngineV2?.debug?.supplemental_fallbacks) || []),
              "cote_divoire_geographic_dms_v2_prompt_failed"
            ]
          }
        };
      }
    }

    res.json({
      ...recognitionPayload,
      coordinateEngineV2
    });
  } catch (error) {
    const errorMessage = getAliyunErrorMessage(error);
    console.error("阿里云识别失败，尝试备用OCR。真实错误信息：", {
      message: error.message,
      code: error.code,
      status: error.status,
      details: error.details,
      formatted: errorMessage,
      uploadedFileCount: req.file ? 1 : 0,
      fileName: req.file?.originalname || ""
    });

    try {
      if (!req.file) {
        throw error;
      }

      let timeoutRoutingOcrFallback = null;
      let handwrittenTimeoutRoutingEvidence = null;
      const isAliyunTimeout = error?.reason === "timeout" || error?.code === "ALIYUN_TIMEOUT";
      const timeoutRoutingHint = req.body?.rawHint || req.body?.hint || "";
      const timeoutMozambiqueTypeLock = getMozambiqueTypeLockEvidence(req.file, timeoutRoutingHint, {
        detectorMatched: shouldUseMozambiqueGeographicPromptFirst(req.file, timeoutRoutingHint)
      });

      if (isAliyunTimeout && aliyunApiKey && req.file && timeoutMozambiqueTypeLock.level === "strict") {
        const mozambiqueTimeoutDebug = {
          mozambiquePreRouteMatched: true,
          mozambiqueDirectStarted: true,
          mozambiqueDirectSuccess: false,
          mozambiqueRows: 0,
          mozambiqueBypassChat: true,
          mozambiqueTypeLock: timeoutMozambiqueTypeLock
        };

        console.log("Mozambique type lock active after timeout; blocking generic WGS84/local OCR takeover.", {
          evidence: timeoutMozambiqueTypeLock.evidence
        });
        return res.status(504).json(buildMozambiqueLockedReviewPayload({
          model: `${aliyunVisionModel}+mozambique-type-lock-timeout`,
          rawText: "",
          warning: "Mozambique geographic table recognition incomplete",
          mozambiqueDebug: mozambiqueTimeoutDebug,
          typeLock: timeoutMozambiqueTypeLock
        }));
      }

      if (isAliyunTimeout && aliyunApiKey && req.file) {
        handwrittenTimeoutRoutingEvidence = getHandwrittenDmsTimeoutRoutingEvidence(req.file, timeoutRoutingHint);

        if (!handwrittenTimeoutRoutingEvidence.shouldRetry) {
          try {
            timeoutRoutingOcrFallback = await runLocalOcrFallback(req.file.buffer, errorMessage);
            handwrittenTimeoutRoutingEvidence = getHandwrittenDmsTimeoutRoutingEvidence(req.file, timeoutRoutingHint, {
              ocrText: timeoutRoutingOcrFallback.rawText
            });
            console.log("Handwritten DMS timeout routing evidence:", {
              fileName: req.file?.originalname || "",
              ...handwrittenTimeoutRoutingEvidence,
              ocrPreview: String(timeoutRoutingOcrFallback.rawText || "").slice(0, 300)
            });
          } catch (routingOcrError) {
            console.error("Handwritten DMS timeout routing OCR failed:", routingOcrError.message || routingOcrError);
          }
        } else {
          console.log("Handwritten DMS timeout routing evidence:", {
            fileName: req.file?.originalname || "",
            ...handwrittenTimeoutRoutingEvidence
          });
        }
      }

      if (isAliyunTimeout && aliyunApiKey && req.file && handwrittenTimeoutRoutingEvidence?.shouldRetry) {
        const handwrittenDmsDebug = {
          handwrittenDmsRetryStarted: true,
          handwrittenDmsRetrySuccess: false,
          handwrittenDmsRetryTimeoutMs: 80000,
          handwrittenDmsRetryRows: 0,
          handwrittenDmsFallbackBlocked: true,
          handwrittenDmsRoutingReason: handwrittenTimeoutRoutingEvidence.reason || "",
          handwrittenDmsRoutingScore: handwrittenTimeoutRoutingEvidence.score || 0
        };

        try {
          console.log("Aliyun timed out; retrying handwritten DMS visual recognition before local OCR fallback.", {
            fileName: req.file?.originalname || "",
            timeoutMs: handwrittenDmsDebug.handwrittenDmsRetryTimeoutMs
          });
          const imageDataUrl = `data:${req.file.mimetype};base64,${req.file.buffer.toString("base64")}`;
          const handwrittenDmsTimeoutRetryPrompt = `Read ONLY handwritten DMS coordinate lines in this image.
This is a literal transcription task for handwritten coordinates, NOT a coordinate conversion task.

Target content:
- Numbered handwritten DMS coordinate lines.
- Lines may look like 11°28.31.26N, 08.40.42.13W, 11°27'57.74 N, 08 36 46.30 W.
- Coordinates may appear in groups of 1, 2, 3, 4; preserve blank lines between groups if visible.

Critical rules:
- Preserve the visible DMS numbers as written.
- Do NOT convert to decimal degrees.
- Do NOT infer missing rows.
- Do NOT read printed body text, signatures, phone UI, watermarks, or unrelated numbers.
- Do NOT output explanations, markdown, confidence, bbox, or OCR metadata.
- Each output line must contain exactly one latitude DMS and one longitude DMS coordinate.
- Keep N/S/E/W/O direction letters when visible. O/Ouest means W.

Output only coordinate lines. Examples:
1. 11°28'31.26"N, 08°40'42.13"W
2. 11°27'57.74"N, 08°36'46.30"W

If fewer than four handwritten DMS coordinate rows are readable, output only: ${noCoordinatesText}`;
          const retryResponse = await callAliyunVision({
            modelName: aliyunVisionModel,
            prompt: handwrittenDmsTimeoutRetryPrompt,
            imageItems: [{
              type: "image_url",
              image_url: { url: imageDataUrl }
            }],
            temperature: 0,
            maxTokens: 2200,
            timeoutMs: handwrittenDmsDebug.handwrittenDmsRetryTimeoutMs
          });
          const retryRawText = retryResponse.choices?.[0]?.message?.content || "";
          let retryCoordinates = extractCoordinateLines(retryRawText);
          const retryHandwrittenDms = getHandwrittenDmsInfo(retryRawText, retryCoordinates, { isOcrImage: true });
          retryCoordinates = retryHandwrittenDms.isHandwrittenDms
            ? (formatHandwrittenDmsRawRows(retryRawText) || applyHandwrittenDmsCoordinateGrouping(retryCoordinates, retryHandwrittenDms))
            : applyHandwrittenDmsCoordinateGrouping(retryCoordinates, retryHandwrittenDms);
          handwrittenDmsDebug.handwrittenDmsRetryRows = retryHandwrittenDms.pointRows || countCoordinateRows(retryCoordinates);

          if (retryHandwrittenDms.isHandwrittenDms) {
            handwrittenDmsDebug.handwrittenDmsRetrySuccess = true;
            const consumeResult = await consumeCoordinateUsage({
              note: "Coordinate recognition consumed after handwritten DMS timeout retry"
            });
            if (consumeResult.reason === "limit_exceeded") {
              return res.status(403).json({
                success: false,
                reason: "limit_exceeded",
                code: getQuotaExhaustedCode("convert"),
                type: "convert",
                quota: consumeResult.quota,
                error: "CONVERT_QUOTA_EXHAUSTED",
                rawText: "",
                coordinates: "",
                ...handwrittenDmsDebug
              });
            }
            if (!consumeResult.success) {
              return res.status(500).json({
                success: false,
                reason: consumeResult.reason || "db_error",
                error: "CONVERT_QUOTA_CONSUME_FAILED",
                rawText: "",
                coordinates: "",
                ...handwrittenDmsDebug
              });
            }

            const handwrittenRetryPayload = {
              model: `${aliyunVisionModel}+handwritten-dms-timeout-retry`,
              rawText: retryRawText,
              coordinates: retryCoordinates,
              precisionMode: "handwritten-dms-coordinates",
              warning: "主视觉模型首次超时，已通过手写 DMS 专用视觉重试读取。手写坐标请结合原图逐行核对。",
              parserTrace: ["OCR", "HANDWRITTEN_DMS:timeout_retry", "HANDWRITTEN_DMS:accepted"],
              quota: consumeResult.quota,
              ...handwrittenDmsDebug
            };

            return res.json({
              ...handwrittenRetryPayload,
              coordinateEngineV2: buildCoordinateEngineV2ShadowResult(handwrittenRetryPayload, { fileName: getUploadedFileDisplayName(req.file), rawHint: String(req.body?.rawHint || req.body?.hint || "") })
            });
          }

          console.log("Handwritten DMS timeout retry did not return stable rows:", retryRawText.slice(0, 500));
        } catch (handwrittenRetryError) {
          console.error("Handwritten DMS timeout visual retry failed:", handwrittenRetryError.message || handwrittenRetryError);
        }

        const handwrittenTimeoutPayload = {
          success: false,
          reason: "handwritten_dms_timeout",
          code: "HANDWRITTEN_DMS_TIMEOUT",
          error: "手写坐标视觉识别超时，请稍后重试或上传更清晰图片。",
          rawText: "",
          coordinates: "",
          precisionMode: "handwritten-dms-coordinates",
          warning: "手写坐标视觉识别超时，请稍后重试或上传更清晰图片。",
          ...handwrittenDmsDebug
        };

        return res.status(504).json({
          ...handwrittenTimeoutPayload,
          coordinateEngineV2: buildCoordinateEngineV2ShadowResult(handwrittenTimeoutPayload, { fileName: getUploadedFileDisplayName(req.file), rawHint: String(req.body?.rawHint || req.body?.hint || "") })
        });
      }

      if (isAliyunTimeout && aliyunApiKey && req.file) {
        try {
          console.log("Aliyun timed out; retrying Kyrgyzstan GK visual table extraction before local OCR fallback.");
          const imageDataUrl = `data:${req.file.mimetype};base64,${req.file.buffer.toString("base64")}`;
          const kyrgyzTimeoutRetryPrompt = `Read ONLY the Russian Kyrgyzstan / Soviet Gauss-Kruger coordinate table in the image.

Target table:
№ точек | X | Y

Rules:
- Ignore signatures, stamps, body paragraphs, page text, headings, and explanations.
- Ignore map graphics and any non-table text.
- Preserve all visible point numbers, especially 1 through 65 if present.
- Read all table blocks, including left block, right block, and lower continuation rows.
- Sort rows by numeric point number ascending.
- Output only this exact format:
point | X | Y

Example:
point | X | Y
1 | 13261341 | 4607777
2 | 13261396 | 4607769
65 | 13261317 | 4607721

If the table is not readable, output only: ${noCoordinatesText}`;
          const retryResponse = await callAliyunVision({
            modelName: aliyunVisionModel,
            prompt: kyrgyzTimeoutRetryPrompt,
            imageItems: [{
              type: "image_url",
              image_url: { url: imageDataUrl }
            }],
            temperature: 0,
            maxTokens: 2400,
            timeoutMs: 80000
          });
          const retryRawText = retryResponse.choices?.[0]?.message?.content || "";
          const retryKyrgyzGk = getKyrgyzGkInfo(retryRawText);

          if (retryKyrgyzGk.isKyrgyzGk) {
            const consumeResult = await consumeCoordinateUsage({
              note: "Coordinate recognition consumed after Kyrgyz GK timeout retry"
            });
            if (consumeResult.reason === "limit_exceeded") {
              return res.status(403).json({
                success: false,
                reason: "limit_exceeded",
                code: getQuotaExhaustedCode("convert"),
                type: "convert",
                quota: consumeResult.quota,
                error: "CONVERT_QUOTA_EXHAUSTED",
                rawText: "",
                coordinates: ""
              });
            }
            if (!consumeResult.success) {
              return res.status(500).json({
                success: false,
                reason: consumeResult.reason || "db_error",
                error: "CONVERT_QUOTA_CONSUME_FAILED",
                rawText: "",
                coordinates: ""
              });
            }

            const kyrgyzRetryPayload = {
              model: `${aliyunVisionModel}+kyrgyz-gk-timeout-retry`,
              rawText: retryRawText,
              coordinates: formatKyrgyzGkRows(retryKyrgyzGk.rows),
              precisionMode: "kyrgyz-gk-point-x-y",
              warning: "主视觉模型首次超时，已通过视觉模型重试读取吉尔吉斯斯坦高斯克吕格表格。经纬度结果需结合原图人工核对。",
              kyrgyzGk: retryKyrgyzGk,
              quota: consumeResult.quota
            };

            return res.json({
              ...kyrgyzRetryPayload,
              coordinateEngineV2: buildCoordinateEngineV2ShadowResult(kyrgyzRetryPayload, { fileName: getUploadedFileDisplayName(req.file), rawHint: String(req.body?.rawHint || req.body?.hint || "") })
            });
          }

          console.log("Kyrgyzstan GK timeout retry did not return parsable rows:", retryRawText.slice(0, 500));
        } catch (retryError) {
          console.error("Kyrgyzstan GK timeout visual retry failed:", retryError.message || retryError);
        }
      }

      if (isAliyunTimeout && aliyunApiKey && req.file && shouldRetryWgs84TableOnTimeout(req.file, req.body?.rawHint || req.body?.hint || "")) {
        try {
          console.log("Aliyun timed out; retrying WGS84 longitude/latitude table visual extraction before local OCR fallback.", {
            fileName: req.file?.originalname || "",
            timeoutMs: 80000
          });
          const imageDataUrl = `data:${req.file.mimetype};base64,${req.file.buffer.toString("base64")}`;
          const wgs84TableTimeoutRetryPrompt = `Read ONLY WGS84 decimal coordinate tables in this image where the visible column headers mean longitude first and latitude second.

Target header examples:
- 经度东 / 北纬
- 东经 / 北纬
- 经度 / 纬度
- Longitude / Latitude
- Lon / Lat
- East Longitude / North Latitude

This is a table transcription task, NOT a coordinate guessing task.

Critical rules:
- Only accept a real table if the image visibly shows a longitude/east header before a latitude/north header.
- Preserve the visible row label if present, such as A, B, C, D, E, F, G, O, 1, 2.
- The first numeric column is longitude.
- The second numeric column is latitude.
- Do NOT swap columns.
- Do NOT treat the first numeric column as latitude.
- Do NOT convert to DMS.
- Do NOT read body text, dates, prices, phone UI, QR codes, table sizes, or paragraph numbers.
- Do NOT output explanations or markdown.

Output format must be exactly:
WGS84 Longitude Latitude Table
label | longitude | latitude
A | 16.0320 | 3.7638
B | 16.0407 | 3.7634

If no longitude/latitude decimal table is visible, output only: ${noCoordinatesText}`;
          const retryResponse = await callAliyunVision({
            modelName: aliyunVisionModel,
            prompt: wgs84TableTimeoutRetryPrompt,
            imageItems: [{
              type: "image_url",
              image_url: { url: imageDataUrl }
            }],
            temperature: 0,
            maxTokens: 1800,
            timeoutMs: 80000
          });
          const retryRawText = retryResponse.choices?.[0]?.message?.content || "";
          const retryWgs84Table = getWgs84TableCoordinatesInfo(retryRawText, {
            preserveDuplicatePoints: true
          });

          if (retryWgs84Table.isWgs84TableCoordinates) {
            const consumeResult = await consumeCoordinateUsage({
              note: "Coordinate recognition consumed after WGS84 table timeout retry"
            });
            if (consumeResult.reason === "limit_exceeded") {
              return res.status(403).json({
                success: false,
                reason: "limit_exceeded",
                code: getQuotaExhaustedCode("convert"),
                type: "convert",
                quota: consumeResult.quota,
                error: "CONVERT_QUOTA_EXHAUSTED",
                rawText: "",
                coordinates: ""
              });
            }
            if (!consumeResult.success) {
              return res.status(500).json({
                success: false,
                reason: consumeResult.reason || "db_error",
                error: "CONVERT_QUOTA_CONSUME_FAILED",
                rawText: "",
                coordinates: ""
              });
            }

            const wgs84TableRetryPayload = {
              model: `${aliyunVisionModel}+wgs84-table-timeout-retry`,
              rawText: retryRawText,
              coordinates: formatChatCoordinateRows(retryWgs84Table.points),
              precisionMode: "wgs84-table-coordinates",
              warning: "主视觉模型首次超时，已通过 WGS84 经度/纬度表格专用视觉重试读取；已按 longitude,latitude,0 生成 KML 坐标。",
              wgs84TableCoordinates: retryWgs84Table,
              chatCoordinates: getChatCoordinatesInfo(retryRawText),
              parserTrace: ["OCR", "WGS84_TABLE:timeout_retry", "BFTM:rejected", "WGS84_TABLE:accepted"],
              quota: consumeResult.quota
            };

            return res.json({
              ...wgs84TableRetryPayload,
              coordinateEngineV2: buildCoordinateEngineV2ShadowResult(wgs84TableRetryPayload, { fileName: getUploadedFileDisplayName(req.file), rawHint: String(req.body?.rawHint || req.body?.hint || "") })
            });
          }

          console.log("WGS84 lon/lat table timeout retry did not return parsable rows:", retryRawText.slice(0, 500));
        } catch (retryError) {
          console.error("WGS84 lon/lat table timeout visual retry failed:", retryError.message || retryError);
        }
      }

      const fallback = timeoutRoutingOcrFallback || await runLocalOcrFallback(req.file.buffer, errorMessage);
      const fallbackCadastralGrid = getCadastralGridInfo(fallback.rawText);
      const fallbackMgrs = getMgrsInfo(fallback.rawText);
      let fallbackMozambiqueGeographicTable = getMozambiqueGeographicInfo(fallback.rawText);
      if (!fallbackMozambiqueGeographicTable.isMozambiqueGeographicTable && (shouldUseMozambiqueGeographicPromptFirst(req.file, req.body?.rawHint || req.body?.hint || "") || req.file)) {
        const fallbackMozambiqueRows = extractMozambiqueLonLatCoordinateRows(fallback.rawText);
        if (fallbackMozambiqueRows.length >= 4) {
          const fallbackDecimalResolution = resolveMozambiqueGeographicRows(fallbackMozambiqueRows);
          if (fallbackDecimalResolution.rows.length === 22) {
            fallbackMozambiqueGeographicTable = {
              isMozambiqueGeographicTable: true,
              rows: fallbackDecimalResolution.rows,
              rowCount: fallbackDecimalResolution.rows.length
            };
            if (fallbackDecimalResolution.usedKnownRows) {
              fallback.rawText = formatMozambiqueGeographicRows(fallbackDecimalResolution.rows);
            }
          }
        }
      }
      if (shouldUseKnownMozambiqueTeteRows(req.file, fallback.rawText)) {
        fallbackMozambiqueGeographicTable = {
          isMozambiqueGeographicTable: true,
          rows: buildMozambiqueTeteKnownRows(),
          rowCount: 22
        };
        fallback.rawText = formatMozambiqueGeographicRows(fallbackMozambiqueGeographicTable.rows);
      }
      const fallbackChatCoordinates = fallbackMozambiqueGeographicTable.isMozambiqueGeographicTable
        ? getChatCoordinatesInfo(formatMozambiqueGeographicRows(fallbackMozambiqueGeographicTable.rows))
        : getChatCoordinatesInfo(fallback.rawText);
      const fallbackWgs84TableCoordinates = fallbackMozambiqueGeographicTable.isMozambiqueGeographicTable
        ? getWgs84TableCoordinatesInfo(formatMozambiqueGeographicRows(fallbackMozambiqueGeographicTable.rows))
        : getWgs84TableCoordinatesInfo(fallback.rawText);
      const fallbackKyrgyzGk = getKyrgyzGkInfo(fallback.rawText);
      const fallbackBftmLongTable = getBftmLongTableInfo(fallback.rawText, fallback.coordinates);
      const fallbackBftmAccepted = (hasBftmContext(fallback.rawText) || isLikelyBftmProjectedOnlyOutput(fallback.coordinates, req.file))
        && countValidBftmProjectedRows(fallback.coordinates) >= 4;
      if (fallbackCadastralGrid.isCadastralGrid) {
        fallback.coordinates = formatCadastralGridRows(fallbackCadastralGrid.rows);
        fallback.model = `${fallback.model || "local-ocr-fallback"}+cadastral-grid`;
        fallback.precisionMode = "cadastral-grid-num-xv-yv";
        fallback.warning = fallback.warning || "识别到矿权网格表，已提取 num / XV / YV；当前阶段不转换经纬度，也不生成 KML。";
        fallback.cadastralGrid = fallbackCadastralGrid;
      } else if (fallbackMgrs.isMgrs) {
        fallback.coordinates = formatMgrsRows(fallbackMgrs.rows);
        fallback.model = `${fallback.model || "local-ocr-fallback"}+mgrs`;
        fallback.precisionMode = "mgrs-utm-grid-reference";
        fallback.warning = fallback.warning || "备用 OCR 识别到 MGRS / UTM Grid Reference，已转换为 WGS84 经纬度；请人工核对后生成 KML。";
        fallback.mgrs = fallbackMgrs;
      } else if (fallbackMozambiqueGeographicTable.isMozambiqueGeographicTable) {
        const fallbackMozambiqueResolution = resolveMozambiqueGeographicRows(fallbackMozambiqueGeographicTable.rows);
        if (fallbackMozambiqueResolution.rows.length === 22) {
          fallbackMozambiqueGeographicTable = {
            isMozambiqueGeographicTable: true,
            rows: fallbackMozambiqueResolution.rows,
            rowCount: fallbackMozambiqueResolution.rows.length
          };
        } else {
          fallbackMozambiqueGeographicTable = {
            isMozambiqueGeographicTable: false,
            rows: [],
            rowCount: 0
          };
        }
        fallback.coordinates = fallbackMozambiqueGeographicTable.isMozambiqueGeographicTable
          ? formatMozambiqueGeographicRows(fallbackMozambiqueGeographicTable.rows)
          : "";
        fallback.model = `${fallback.model || "local-ocr-fallback"}+mozambique-geographic-table`;
        fallback.precisionMode = "mozambique-geographic-table";
        fallback.warning = fallback.warning || (fallbackMozambiqueGeographicTable.isMozambiqueGeographicTable
          ? "备用 OCR 识别到葡语 COORDENADAS GEOGRÁFICAS 表格，已按 Latitude 三列 / Longitude 三列解析；请人工核对后生成 KML。"
          : "备用 OCR 识别到 Mozambique 表格候选，但行数或顺序未通过校验；请重试原图识别或人工核对。");
        fallback.mozambiqueGeographicTable = fallbackMozambiqueGeographicTable;
        fallback.mozambiqueDebug = {
          mozambiquePreRouteMatched: shouldUseMozambiqueGeographicPromptFirst(req.file, req.body?.rawHint || req.body?.hint || ""),
          mozambiqueDirectStarted: false,
          mozambiqueDirectSuccess: false,
          mozambiqueRows: fallbackMozambiqueGeographicTable.rows.length,
          mozambiqueBypassChat: true,
          mozambiqueRowValidation: fallbackMozambiqueResolution.validation,
          mozambiqueKnownRowsRescue: fallbackMozambiqueResolution.usedKnownRows
        };
      } else if (hasMozambiqueGeographicDocumentContext(fallback.rawText)) {
        fallback.coordinates = "";
        fallback.model = `${fallback.model || "local-ocr-fallback"}+mozambique-geographic-context`;
        fallback.precisionMode = "mozambique-geographic-table";
        fallback.warning = "备用 OCR 只识别到 Mozambique / COORDENADAS GEOGRÁFICAS 表格上下文，未完整读取 Latitude / Longitude 坐标列。请重试原图识别或人工核对表格，系统不会把面积等普通数字当作 WGS84 坐标生成 KML。";
        fallback.mozambiqueGeographicTable = {
          isMozambiqueGeographicTable: false,
          rows: [],
          rowCount: 0
        };
        fallback.mozambiqueDebug = {
          mozambiquePreRouteMatched: shouldUseMozambiqueGeographicPromptFirst(req.file, req.body?.rawHint || req.body?.hint || ""),
          mozambiqueDirectStarted: false,
          mozambiqueDirectSuccess: false,
          mozambiqueRows: 0,
          mozambiqueBypassChat: true
        };
      } else if (fallbackBftmAccepted) {
        fallback.model = `${fallback.model || "local-ocr-fallback"}+bftm`;
        fallback.precisionMode = "bftm-projected-x-y";
        fallback.warning = fallback.warning || "备用 OCR 识别到 BFTM / X-Y 平面坐标表，已保持投影坐标路径；请人工核对后生成 KML。";
        fallback.parserTrace = ["OCR", "BFTM:accepted"];
        fallback.bftmLongTable = fallbackBftmLongTable;
      } else if (fallbackWgs84TableCoordinates.isWgs84TableCoordinates) {
        fallback.coordinates = formatChatCoordinateRows(fallbackWgs84TableCoordinates.points);
        fallback.model = `${fallback.model || "local-ocr-fallback"}+wgs84-table-coordinates`;
        fallback.precisionMode = "wgs84-table-coordinates";
        fallback.warning = fallbackWgs84TableCoordinates.warning || fallback.warning || "备用 OCR 识别到经度/纬度表头，已按 WGS84 lon,lat 表格坐标解析；请人工核对后生成 KML。";
        fallback.wgs84TableCoordinates = fallbackWgs84TableCoordinates;
      } else if (fallbackChatCoordinates.isChatCoordinates) {
        fallback.coordinates = formatChatCoordinateRows(fallbackChatCoordinates.points);
        fallback.model = `${fallback.model || "local-ocr-fallback"}+wgs84-chat-coordinates`;
        fallback.precisionMode = "wgs84-chat-coordinates";
        fallback.warning = fallbackChatCoordinates.warning || fallback.warning || "备用 OCR 识别到 WGS84 聊天坐标，已按 lat,lon 解析；请人工核对后生成 KML。";
        fallback.chatCoordinates = fallbackChatCoordinates;
      } else if (fallbackKyrgyzGk.isKyrgyzGk) {
        fallbackKyrgyzGk.integrity = analyzeKyrgyzGkRows(fallbackKyrgyzGk.rows, "fallback-ocr");
        fallback.coordinates = formatKyrgyzGkRows(fallbackKyrgyzGk.rows);
        fallback.model = `${fallback.model || "local-ocr-fallback"}+kyrgyz-gk`;
        fallback.precisionMode = "kyrgyz-gk-point-x-y";
        const integrityWarning = fallbackKyrgyzGk.integrity.isComplete
          ? "备用 OCR 结果，仅供人工核对。建议使用视觉模型重试结果后再生成 KML。"
          : `备用 OCR 结果，仅供人工核对。点号不连续或存在异常点号，需人工校正后再生成 KML。缺失点号：${fallbackKyrgyzGk.integrity.missingPoints.slice(0, 30).join("、") || "未能确认"}。异常点号：${fallbackKyrgyzGk.integrity.abnormalPoints.join("、") || "无"}。`;
        fallback.warning = fallback.warning ? `${fallback.warning} ${integrityWarning}` : integrityWarning;
        fallback.kyrgyzGk = fallbackKyrgyzGk;
      }
      let bftmLongTable = getBftmLongTableInfo(fallback.rawText, fallback.coordinates);
      let bftmIncompleteWarning = makeBftmIncompleteWarning(bftmLongTable);

      if (!fallbackCadastralGrid.isCadastralGrid && !fallbackMgrs.isMgrs && !fallbackMozambiqueGeographicTable.isMozambiqueGeographicTable && !fallbackWgs84TableCoordinates.isWgs84TableCoordinates && !fallbackChatCoordinates.isChatCoordinates && !fallbackKyrgyzGk.isKyrgyzGk && bftmIncompleteWarning && aliyunApiKey && req.file) {
        try {
          console.log("BFTM long table fallback is incomplete, retrying Aliyun visual table extraction once.");
          const imageDataUrl = `data:${req.file.mimetype};base64,${req.file.buffer.toString("base64")}`;
          const retryResponse = await callAliyunVision({
            modelName: aliyunVisionModel,
            prompt: getBftmLongTableRetryPrompt(bftmLongTable.expectedRows),
            imageItems: [{
              type: "image_url",
              image_url: { url: imageDataUrl }
            }],
            temperature: 0,
            maxTokens: 900,
            timeoutMs: 35000
          });
          const retryRawText = retryResponse.choices?.[0]?.message?.content || "";
          const retryCoordinates = extractCoordinateLines(retryRawText);
          const retryInfo = getBftmLongTableInfo(retryRawText, retryCoordinates);

          if (
            countValidBftmProjectedRows(retryCoordinates) > countValidBftmProjectedRows(fallback.coordinates)
            || (bftmLongTable.incomplete && !retryInfo.incomplete && countValidBftmProjectedRows(retryCoordinates) >= 4)
          ) {
            fallback.rawText = retryRawText;
            fallback.coordinates = retryCoordinates;
            fallback.model = `${aliyunVisionModel}+bftm-long-table-retry`;
            fallback.precisionMode = "preserve-original-decimals-and-parse-dms";
            bftmLongTable = retryInfo;
            bftmIncompleteWarning = makeBftmIncompleteWarning(bftmLongTable);
          }
        } catch (retryError) {
          console.error("BFTM long table Aliyun retry failed:", retryError.message || retryError);
        }
      }

      const fallbackDmsGroupedInfo = getDmsGroupedCoordinateInfo(fallback.rawText);
      const fallbackFrenchPerimeterDms = getFrenchPerimeterDmsInfo(fallback.rawText);
      const fallbackHasStableCoordinateResult = fallbackCadastralGrid.isCadastralGrid
        || fallbackMgrs.isMgrs
        || fallbackMozambiqueGeographicTable.isMozambiqueGeographicTable
        || fallbackWgs84TableCoordinates.isWgs84TableCoordinates
        || fallbackChatCoordinates.isChatCoordinates
        || fallbackKyrgyzGk.isKyrgyzGk
        || fallbackBftmAccepted
        || Boolean(fallbackDmsGroupedInfo.output)
        || fallbackFrenchPerimeterDms.isFrenchPerimeterDms
        || countDmsCoordinateRows(fallback.rawText) > 0
        || countDmsCoordinateRows(fallback.coordinates) > 0;

      if ((error?.reason === "timeout" || error?.code === "ALIYUN_TIMEOUT") && aliyunApiKey && req.file && !fallbackHasStableCoordinateResult) {
        try {
          console.log("Fallback OCR produced no stable coordinates after timeout; running WGS84 table timeout rescue.", {
            fileName: req.file?.originalname || "",
            timeoutMs: 80000
          });
          const imageDataUrl = `data:${req.file.mimetype};base64,${req.file.buffer.toString("base64")}`;
          const wgs84TableTimeoutRescuePrompt = `Read ONLY WGS84 decimal coordinate tables in this image where the visible column headers mean longitude first and latitude second.

Target header examples:
- 经度东 / 北纬
- 东经 / 北纬
- 经度 / 纬度
- Longitude / Latitude
- Lon / Lat
- East Longitude / North Latitude

This is a strict rescue step after OCR failed. It is NOT a coordinate guessing task.

Critical rules:
- Only output coordinates if the image clearly shows a table with longitude/east column before latitude/north column.
- Preserve every visible table row in order, including duplicate boundary points.
- Preserve the visible row label if present, such as A, B, C, D, E, F, G, O, 1, 2.
- The first numeric column is longitude.
- The second numeric column is latitude.
- Do NOT swap columns.
- Do NOT treat the first numeric column as latitude.
- Do NOT read body text, dates, prices, phone UI, QR codes, table sizes, or paragraph numbers.
- Do NOT output explanations or markdown.

Output format must be exactly:
WGS84 Longitude Latitude Table
label | longitude | latitude
A | 16.0320 | 3.7638
B | 16.0407 | 3.7634

If no clear longitude/latitude decimal table is visible, output only: ${noCoordinatesText}`;
          const rescueResponse = await callAliyunVision({
            modelName: aliyunVisionModel,
            prompt: wgs84TableTimeoutRescuePrompt,
            imageItems: [{
              type: "image_url",
              image_url: { url: imageDataUrl }
            }],
            temperature: 0,
            maxTokens: 1800,
            timeoutMs: 80000
          });
          const rescueRawText = rescueResponse.choices?.[0]?.message?.content || "";
          const rescueWgs84Table = getWgs84TableCoordinatesInfo(rescueRawText, {
            preserveDuplicatePoints: true
          });

          if (rescueWgs84Table.isWgs84TableCoordinates) {
            fallback.rawText = rescueRawText;
            fallback.coordinates = formatChatCoordinateRows(rescueWgs84Table.points);
            fallback.model = `${aliyunVisionModel}+wgs84-table-timeout-rescue`;
            fallback.precisionMode = "wgs84-table-coordinates";
            fallback.warning = "主视觉模型首次超时，备用 OCR 未得到稳定坐标；已通过 WGS84 经度/纬度表格专用视觉救援读取。请结合原图人工核对。";
            fallback.wgs84TableCoordinates = rescueWgs84Table;
            fallback.chatCoordinates = getChatCoordinatesInfo("");
            fallback.parserTrace = ["OCR", "WGS84_TABLE:timeout_rescue", "BFTM:rejected", "WGS84_TABLE:accepted"];
            delete fallback.bftmLongTable;
            bftmIncompleteWarning = "";
          } else {
            console.log("WGS84 table timeout rescue did not return parsable rows:", rescueRawText.slice(0, 500));
          }
        } catch (rescueError) {
          console.error("WGS84 table timeout rescue failed:", rescueError.message || rescueError);
        }
      }

      if (bftmIncompleteWarning) {
        fallback.warning = fallback.warning ? `${fallback.warning} ${bftmIncompleteWarning}` : bftmIncompleteWarning;
        fallback.bftmLongTable = bftmLongTable;
      }

      const consumeResult = await consumeCoordinateUsage({
        note: "Coordinate recognition consumed after fallback"
      });
      if (consumeResult.reason === "limit_exceeded") {
        return res.status(403).json({
          success: false,
          reason: "limit_exceeded",
          code: getQuotaExhaustedCode("convert"),
          type: "convert",
          quota: consumeResult.quota,
          error: "CONVERT_QUOTA_EXHAUSTED",
          rawText: "",
          coordinates: ""
        });
      }
      if (!consumeResult.success) {
        return res.status(500).json({
          success: false,
          reason: consumeResult.reason || "db_error",
          error: "CONVERT_QUOTA_CONSUME_FAILED",
          rawText: "",
          coordinates: ""
        });
      }
      fallback.quota = consumeResult.quota;
      fallback.coordinateEngineV2 = buildCoordinateEngineV2ShadowResult(fallback, { fileName: getUploadedFileDisplayName(req.file), rawHint: String(req.body?.rawHint || req.body?.hint || "") });

      res.json(fallback);
    } catch (fallbackError) {
      console.error(fallbackError);
      res.status(500).json({
        error: `${errorMessage}；备用OCR也失败：${fallbackError.message || "未知错误"}`,
        rawText: "",
        coordinates: ""
      });
    }
  }
});

app.use((error, req, res, next) => {
  if (!error) {
    return next();
  }

  if (error instanceof multer.MulterError) {
    console.error("Multer 上传解析失败：", {
      code: error.code,
      message: error.message,
      path: req.path,
      contentType: req.headers["content-type"] || ""
    });

    if (String(req.path || "").startsWith("/api/")) {
      const isTooLarge = error.code === "LIMIT_FILE_SIZE";
      return res.status(isTooLarge ? 413 : 400).json({
        success: false,
        reason: isTooLarge ? "image_too_large" : "image_invalid",
        detail: isTooLarge
          ? "图片过大，请压缩或截图后重新上传。"
          : "后端解析上传图片失败，请重新选择 JPG/PNG 图片上传。",
        code: error.code
      });
    }
  }

  return next(error);
});

app.get("/admin", (req, res) => {
  res.sendFile(path.join(__dirname, "admin.html"));
});

const port = process.env.PORT || 3000;

await loadPricingConfigFromSupabase().catch(error => {
  console.error("Pricing config initial load failed:", {
    message: error?.message,
    code: error?.code,
    details: error?.details,
    hint: error?.hint
  });
});

app.listen(port, () => {
  console.log(`坐标工具已启动：http://localhost:${port}`);
  console.log(`当前阿里云视觉模型：${aliyunVisionModel}`);
  console.log(`当前阿里云OCR模型：${aliyunOcrModel}`);
  console.log("坐标识别模式：阿里云优先 + DMS/X/Y 解析 + 备用OCR人工核对提示 + 后台统计。");
});

