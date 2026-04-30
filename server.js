import "dotenv/config";
import express from "express";
import multer from "multer";
import OpenAI from "openai";
import path from "node:path";
import { fileURLToPath } from "node:url";
import fs from "node:fs/promises";
import Tesseract from "tesseract.js";

const app = express();
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 12 * 1024 * 1024
  }
});
const model = process.env.OPENAI_MODEL || "gpt-4o-mini";
const openAIBaseURL = process.env.OPENAI_BASE_URL || "https://api.openai.com/v1";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const noCoordinatesText = "未识别到有效坐标，请重新上传更清晰的坐标区域截图。";
const adminDataFile = path.join(__dirname, "admin-data.json");
const adminPassword = process.env.ADMIN_PASSWORD || "";

app.use(express.json({ limit: "1mb" }));
app.use(express.static(__dirname));

app.get(["/convert", "/ocr", "/judge"], (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

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
      aiJudgeEnabled: true
    }
  };
}

async function readAdminData() {
  try {
    const text = await fs.readFile(adminDataFile, "utf8");
    const data = JSON.parse(text || "{}");
    const defaults = createDefaultAdminData();

    return {
      ...defaults,
      ...data,
      users: data.users || {},
      events: Array.isArray(data.events) ? data.events : [],
      records: Array.isArray(data.records) ? data.records : [],
      usage: data.usage && typeof data.usage === "object" ? data.usage : {},
      ipGeoCache: data.ipGeoCache && typeof data.ipGeoCache === "object" ? data.ipGeoCache : {},
      featureFlags: {
        ...defaults.featureFlags,
        ...(data.featureFlags || {})
      }
    };
  } catch (error) {
    if (error.code === "ENOENT") {
      return createDefaultAdminData();
    }

    if (error instanceof SyntaxError) {
      const backupFile = `${adminDataFile}.${Date.now()}.broken`;

      try {
        await fs.rename(adminDataFile, backupFile);
        console.error(`admin-data.json 已损坏，已备份到 ${backupFile} 并重建。`);
      } catch (renameError) {
        console.error("admin-data.json 损坏，备份失败，将直接重建。", renameError);
      }

      return createDefaultAdminData();
    }

    throw error;
  }
}

async function writeAdminData(data) {
  const tempFile = `${adminDataFile}.tmp`;
  await fs.writeFile(tempFile, JSON.stringify(data, null, 2), "utf8");
  await fs.rename(tempFile, adminDataFile);
}

function getNowISO() {
  return new Date().toISOString();
}

function makeId(prefix) {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
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
        aiJudgeEnabled: true
      },
      createdAt: getNowISO(),
      lastSeenAt: getNowISO(),
      eventCount: 0,
      note: "",
      phone: "",
      wechat: ""
    };
  }

  return data.users[id];
}

function normalizeAdminUser(user, fallbackId = "") {
  const safeUser = user && typeof user === "object" ? user : {};
  const visitorId = String(safeUser.visitorId || fallbackId || "").trim();

  return {
    visitorId,
    plan: safeUser.plan || "free",
    status: safeUser.status || "active",
    permissions: {
      aiOcrEnabled: safeUser.permissions?.aiOcrEnabled !== false,
      xyConvertEnabled: safeUser.permissions?.xyConvertEnabled !== false,
      kmlExportEnabled: safeUser.permissions?.kmlExportEnabled !== false,
      manualSupportEnabled: safeUser.permissions?.manualSupportEnabled !== false,
      aiJudgeEnabled: safeUser.permissions?.aiJudgeEnabled !== false
    },
    createdAt: safeUser.createdAt || "",
    lastSeenAt: safeUser.lastSeenAt || "",
    eventCount: Number(safeUser.eventCount || 0),
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
  ai_judge_fail: "AI判读失败"
};

function getEventLabel(eventName) {
  return eventNameLabels[eventName] || eventName || "";
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
      aiJudgeEnabled: false
    };
  }

  return {
    aiOcrEnabled: Boolean(featureFlags.aiOcrEnabled && permissions.aiOcrEnabled),
    xyConvertEnabled: Boolean(featureFlags.xyConvertEnabled && permissions.xyConvertEnabled),
    kmlExportEnabled: Boolean(featureFlags.kmlExportEnabled && permissions.kmlExportEnabled),
    manualSupportEnabled: Boolean(featureFlags.manualSupportEnabled && permissions.manualSupportEnabled),
    aiJudgeEnabled: Boolean(featureFlags.aiJudgeEnabled && permissions.aiJudgeEnabled)
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

function normalizeText(text) {
  return String(text || "")
    .replace(/[，]/g, ",")
    .replace(/[º˚]/g, "°")
    .replace(/[‘’´`′]/g, "'")
    .replace(/[“”″]/g, '"')
    .replace(/\b0\b/g, "O");
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
  return String(line || "").match(/[-+]?\d{1,3}\s*°\s*(?:\d{1,2}\s*'\s*\d{1,4}(?:\.\d+)?|\d{3,7}(?:\.\d+)?)\s*["']?\s*[NSEWO]?/gi) || [];
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

function extractDecimalCoordinateLines(text) {
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

function extractProjectedNumberPair(text) {
  const groups = String(text || "").match(/\d+(?:\.\d+)?/g) || [];
  const isSmallId = value => /^\d{1,2}$/.test(value);
  const isThreeDigits = value => /^\d{3}$/.test(value);
  const isOneToThreeDigits = value => /^\d{1,3}$/.test(value);

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

  for (const line of lines) {
    if (/annoter|tourner|rechercher|partager|hectares|latitude|longitude/i.test(line)) {
      continue;
    }

    const tablePair = extractProjectedNumberPair(line);

    if (tablePair) {
      coordinateLines.push(`${tablePair[0]},${tablePair[1]}`);
      continue;
    }

    const numbers = extractNumbersWithThousands(line);
    const largeNumbers = numbers.filter(value => Math.abs(Number(value)) >= 10000);

    if (largeNumbers.length >= 2 && looksLikeProjectedPair(largeNumbers[0], largeNumbers[1])) {
      coordinateLines.push(`${largeNumbers[0]},${largeNumbers[1]}`);
    }
  }

  return coordinateLines;
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
  const decimalLines = extractDecimalCoordinateLines(text);

  if (decimalLines.length > 0) {
    return splitGroupsAtRepeatedBoundary(decimalLines.join("\n"));
  }

  const dmsLines = extractDmsCoordinateLines(text);

  if (dmsLines.length > 0) {
    return dmsLines.join("\n");
  }

  const projectedLines = extractProjectedCoordinateLines(text);

  return projectedLines.length > 0 ? projectedLines.join("\n") : noCoordinatesText;
}

function countCoordinateRows(text) {
  return String(text || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .filter(line => line !== noCoordinatesText)
    .length;
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

function getOpenAIErrorMessage(error) {
  const status = error.status ? `HTTP ${error.status}` : "";
  const code = error.code ? `code=${error.code}` : "";
  const type = error.type ? `type=${error.type}` : "";
  const message = error.message || "未知错误";

  return [status, code, type, message].filter(Boolean).join(" | ");
}

function normalizeJudgeOutput(text) {
  const raw = String(text || "").trim();

  if (!raw) {
    return "【判读结论】不确定\n【是否建议继续】谨慎\n【风险】图片信息不足\n【依据】需要更清晰图片或现场核对";
  }

  const sectionNames = ["判读结论", "是否建议继续", "风险", "依据"];
  const values = {};

  for (let index = 0; index < sectionNames.length; index += 1) {
    const current = sectionNames[index];
    const next = sectionNames[index + 1];
    const pattern = next
      ? new RegExp(`【${current}】([\\s\\S]*?)(?=【${next}】)`)
      : new RegExp(`【${current}】([\\s\\S]*)`);
    const match = raw.match(pattern);
    values[current] = (match?.[1] || "")
      .split(/\r?\n/)
      .map(line => line.trim())
      .filter(Boolean)
      .slice(0, 1)
      .join(" ");
  }

  if (!values["判读结论"] && !values["是否建议继续"] && !values["风险"] && !values["依据"]) {
    const lines = raw.split(/\r?\n/).map(line => line.trim()).filter(Boolean).slice(0, 4);
    values["判读结论"] = lines[0] || "不确定";
    values["是否建议继续"] = lines[1] || "谨慎";
    values["风险"] = lines[2] || "图片信息不足";
    values["依据"] = lines[3] || "需要更清晰图片或现场核对";
  }

  return [
    `【判读结论】${values["判读结论"] || "不确定"}`,
    `【是否建议继续】${values["是否建议继续"] || "谨慎"}`,
    `【风险】${values["风险"] || "图片信息不足"}`,
    `【依据】${values["依据"] || "需要更清晰图片或现场核对"}`
  ].join("\n");
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
        vipUsers: users.filter(user => user.plan === "vip").length,
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
        aiJudgeEnabled: Boolean(req.body.permissions.aiJudgeEnabled)
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

    user.updatedAt = getNowISO();
    await writeAdminData(data);
    res.json({ user });
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
      aiJudgeEnabled: Boolean(nextFlags.aiJudgeEnabled)
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

app.post("/api/analyze-mining-image", upload.fields([
  { name: "image", maxCount: 1 },
  { name: "images", maxCount: 5 }
]), async (req, res) => {
  console.log("---- 收到AI判读请求 ----");
  const uploadedFiles = [
    ...(req.files?.images || []),
    ...(req.files?.image || [])
  ].slice(0, 5);
  const firstFile = uploadedFiles[0];
  console.log("是否收到图片：", uploadedFiles.length > 0);
  console.log("收到图片数量：", uploadedFiles.length);
  console.log("图片大小：", uploadedFiles.map(file => `${file.originalname || "image"}=${file.size} bytes`).join(", "));

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
      return res.status(400).json({
        error: "后端没有收到文件，请重新选择图片或资料文件上传。"
      });
    }

    const imageFiles = uploadedFiles.filter(file => String(file.mimetype || "").startsWith("image/"));
    const isImageFile = imageFiles.length > 0;

    if (user) {
      await updateUserVisitMeta(user, req, data);
    }

    if (!isImageFile) {
      const rawOutput = `【判读结论】已收到资料文件，当前版本暂不能直接解析文档内容
【是否建议继续】谨慎，建议上传关键页面截图
【风险】文件内容未进入视觉判读
【依据】请截取矿地、河道、矿石或坐标表关键页再判读`;
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

      if (user) {
        user.eventCount = Number(user.eventCount || 0) + 1;
      }

      await writeAdminData(data);

      return res.json({
        result: normalizedOutput,
        rawOutput,
        recordId: record.id,
        warning: "当前版本已支持上传资料文件，但AI判读仍建议使用关键页面截图。"
      });
    }

    if (!process.env.OPENAI_API_KEY) {
      return res.status(400).json({
        error: "AI判读需要配置 OPENAI_API_KEY。"
      });
    }

    const imageItems = imageFiles.map(file => ({
      type: "image_url",
      image_url: {
        url: `data:${file.mimetype};base64,${file.buffer.toString("base64")}`
      }
    }));
    const openai = new OpenAI({
      apiKey: process.env.OPENAI_API_KEY,
      baseURL: openAIBaseURL
    });

    const prompt = `你是矿业空间判读助手，只做矿业决策辅助。

请根据上传的1到5张图片做保守初筛。图片可能是矿地、河道、卫星图、地形图、矿石照片或资料页截图。

只允许输出下面4行，不能增加其他标题：
【判读结论】一句短话
【是否建议继续】建议 / 谨慎 / 不建议 + 很短理由
【风险】一句短话
【依据】一句短话

规则：
1. 必须控制在4行以内，每行一个标题和一句短话。
2. 【是否建议继续】只能写：建议 / 谨慎 / 不建议，再补充很短理由。
3. 不允许输出含量预测、储量预测。
4. 不允许报具体金点、坐标点或采样点。
5. 看不清或证据不足时，必须写“不确定”或“谨慎”。
6. 如果图片看起来像AI生成图、过度美化图、合成图或不真实场景，必须在【风险】里提示“疑似AI图或非真实现场图，需人工核对”。
7. 偏向保守判断，宁可少说，不要乱说。`;

    const response = await openai.chat.completions.create({
      model,
      messages: [
        {
          role: "user",
          content: [
            { type: "text", text: prompt },
            ...imageItems
          ]
        }
      ],
      temperature: 0.1
    });

    const rawOutput = response.choices?.[0]?.message?.content || "";
    const normalizedOutput = normalizeJudgeOutput(rawOutput);
    const record = {
      id: makeId("record"),
      user_id: visitorId,
      imageURL: "",
      imageName: imageFiles.map(file => file.originalname || "").filter(Boolean).join(", "),
      imageSize: imageFiles.reduce((sum, file) => sum + Number(file.size || 0), 0),
      judgeType,
      aiRawOutput: rawOutput,
      result: normalizedOutput,
      createdAt: getNowISO()
    };

    data.records.push(record);
    data.usage[visitorId] = data.usage[visitorId] || {};
    data.usage[visitorId].aiJudgeCount = Number(data.usage[visitorId].aiJudgeCount || 0) + 1;

    if (user) {
      user.eventCount = Number(user.eventCount || 0) + 1;
    }

    await writeAdminData(data);

    res.json({
      result: normalizedOutput,
      rawOutput,
      recordId: record.id
    });
  } catch (error) {
    const errorMessage = getOpenAIErrorMessage(error);
    console.error("AI判读失败：", errorMessage);
    res.status(500).json({
      error: errorMessage || "AI判读失败，请稍后重试。"
    });
  }
});

app.post("/api/recognize-coordinates", upload.single("image"), async (req, res) => {
  console.log("---- 收到识别请求 ----");
  console.log("是否收到图片：", Boolean(req.file));

  try {
    const visitorId = String(req.get("x-visitor-id") || req.body?.visitorId || "").trim();
    const adminData = await readAdminData();
    const user = ensureUser(adminData, visitorId);
    const permissions = getEffectivePermissions(user, adminData.featureFlags);

    if (!permissions.aiOcrEnabled) {
      if (user) {
        await updateUserVisitMeta(user, req, adminData);
        await writeAdminData(adminData);
      }

      return res.status(403).json({
        error: "当前用户暂未开通 AI 图片识别。",
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

    console.log("图片文件名：", req.file.originalname);
    console.log("图片类型：", req.file.mimetype);
    console.log("图片大小：", `${req.file.size} bytes`);
    console.log("使用模型：", model);

    if (!process.env.OPENAI_API_KEY) {
      console.log("未配置 OPENAI_API_KEY，自动切换到本地 OCR 兜底识别。");

      const result = await Tesseract.recognize(req.file.buffer, "eng", {
        logger: info => console.log(info.status, info.progress)
      });
      const rawText = result.data.text || "";
      const coordinates = extractCoordinateLines(rawText);

      console.log("本地 OCR 返回的原始内容：");
      console.log(rawText);
      console.log("坐标提取结果：");
      console.log(coordinates);

      return res.json({
        model: "local-tesseract-fallback",
        rawText,
        coordinates,
        precisionMode: "local-ocr-dms-fallback",
        warning: "未配置 OPENAI_API_KEY，当前使用本地 OCR 兜底识别。复杂图片建议使用人工协助或配置 OpenAI API Key。"
      });
    }

    const imageBase64 = req.file.buffer.toString("base64");
    const imageDataUrl = `data:${req.file.mimetype};base64,${imageBase64}`;

    console.log("base64 image_url 已生成，长度：", imageDataUrl.length);
    console.log("正在调用 OpenAI...");

    const openai = new OpenAI({
      apiKey: process.env.OPENAI_API_KEY,
      baseURL: openAIBaseURL
    });

    const createVisionRequest = modelName => openai.chat.completions.create({
      model: modelName,
      messages: [
        {
          role: "user",
          content: [
            {
              type: "text",
              text: `你是矿业坐标识别助手。请从图片中找到真正的坐标表，并只返回坐标行。图片可能是完整文件、手机截图、扫描件、带水印图片、长表、局部表格或带菜单按钮的截图。

必须忽略：
水印、背景字、页眉页脚、表格线、手机状态栏、底部菜单、Annoter、Tourner、Rechercher、Partager、Hectares、签名、正文段落。

必须支持这些表格类型：
1. Point / N° / LATITUDE / LONGITUDE。
2. Point / Latitude nord / Longitude ouest。
3. Point A-Z 或 1-99 的长表。
4. Nord / Est 表头，结合 N/S/E/W 判断纬度和经度。
5. X / Y、Liste des Coordonnées、BFTM / ITRF 2008 / Projection BFTM 平面坐标表。
6. 十进制度、度分、度分秒 DMS。
7. N/S/E/W，法语 O / Ouest = West = 西经。
8. Latitude nord = 北纬；Longitude ouest = 西经。
9. 表格数字可能带空格分组，例如 658 800 和 1 364 200，必须分别理解为 658800 和 1364200。
10. 手写坐标可能写成 11°28.31.26N 08.40.42.13W、11°27'57.74 N 08 36 46.30 W 等不规范 DMS，请按度分秒理解。
11. 如果表格里有红色/手写/框选修正标记，例如把打印的 11° 手工改成 10°，优先按修正后的值识别；同时在最后增加一行识别提示，提醒用户核对。

输出规则：
1. 识别出什么格式，就保留什么格式。不要把度分秒自动转换成十进制度。
2. 每一行只输出一组坐标，格式固定为：经度,纬度。
3. 如果表格是 X/Y 平面坐标，每一行输出：X,Y，保留原数字。
4. 如果原图没有 N/W/O 字母，但表头写了 Latitude nord / Longitude ouest，需要在输出中补上 N 和 W，或用负号表达西经。
5. 必须按 Point 编号逐行输出，不能漏掉第一行、中间行或最后一行。看到 4 个点就输出 4 行；看到 A-Z 就输出 A-Z 对应的全部行。
6. 如果 X 列连续两行相同，或 Y 列连续两行相同，也必须按同一行的 X 和 Y 配对，不要把下一行的 Y 拿来配上一行。
7. 表格右侧的斜线、手写勾、批注线不是数字，不要因为这些标记跳行或漏行。
8. 不要输出点号、表头、解释文字、Markdown、编号、空行。
9. 不要压缩小数位，不要改写原始精度。
10. 不要输出图片中的像素位置、文字框坐标、识别框坐标或碎数字。例如 41,320,21,42,90 不是地理坐标，必须忽略。
11. 如果同一张图片里有多块不同矿区/多组坐标，必须在不同组之间保留一个空行。每组内部仍然按原顺序逐行输出。
12. 手写坐标如果出现多段明显分开的 1、2、3、4 编号，每一段就是一组坐标，段与段之间必须输出一个空行。
13. 如果采用了手写/红色/框选修正，坐标行输出完成后，最后额外输出一行：识别提示：发现疑似人工修正，已按修正值识别，请核对。

示例：
09°01'13.67"W,11°43'16.45"N
08°53'32.66"W,11°52'11.93"N
642405.693,1051600.499

无法识别有效坐标时，只输出：${noCoordinatesText}`
            },
            {
              type: "image_url",
              image_url: {
                url: imageDataUrl
              }
            }
          ]
        }
      ]
    });

    let response = await createVisionRequest(model);
    let usedModel = model;

    console.log("调用 OpenAI 是否成功：是");

    let rawText = response.choices?.[0]?.message?.content || "";
    let coordinates = extractCoordinateLines(rawText);
    const ocrRetryModel = process.env.OPENAI_OCR_MODEL || (openAIBaseURL.includes("dashscope") ? "qwen-vl-ocr" : "");

    if (countCoordinateRows(coordinates) < 4 && ocrRetryModel && ocrRetryModel !== model) {
      console.log(`识别结果少于 4 组，使用 OCR 模型重试：${ocrRetryModel}`);
      const retryResponse = await createVisionRequest(ocrRetryModel);
      const retryRawText = retryResponse.choices?.[0]?.message?.content || "";
      const retryCoordinates = extractCoordinateLines(retryRawText);

      if (countCoordinateRows(retryCoordinates) > countCoordinateRows(coordinates)) {
        rawText = retryRawText;
        coordinates = retryCoordinates;
        usedModel = ocrRetryModel;
      }
    }

    if (countCoordinateRows(coordinates) < 4) {
      console.log("AI 识别少于 4 组，追加本地 OCR 对比行数。");
      const localResult = await Tesseract.recognize(req.file.buffer, "eng", {
        logger: info => console.log(info.status, info.progress)
      });
      const localRawText = localResult.data.text || "";
      const localCoordinates = extractCoordinateLines(localRawText);

      if (countCoordinateRows(localCoordinates) > countCoordinateRows(coordinates)) {
        rawText = localRawText;
        coordinates = localCoordinates;
        usedModel = `${usedModel}+local-ocr-more-rows`;
      }
    }

    /*
    const response = await openai.chat.completions.create({
      model: usedModel,
      messages: [
        {
          role: "user",
          content: [
            {
              type: "text",
              text: `你是矿业坐标识别助手。请只识别图片中的坐标表区域，忽略水印、表格线、页眉页脚、手机底部菜单、Annoter、Tourner、Rechercher、Partager、Hectares 等无关文字。

重点处理：
1. 坐标表通常包含 N° / Latitude / Longitude 三列。
2. Latitude 是纬度，Longitude 是经度。
3. 支持十进制度和度分秒 DMS。
4. 支持 N/S/E/W。
5. 法语 O 或 Ouest = West = 西经 = 负经度。
6. Latitude nord = 北纬 = 正纬度。
7. Longitude ouest = 西经 = 负经度。
8. 如果方向字母被 OCR 漏掉，但表头是 Longitude 或 Longitude ouest，经度应按西经负号处理。
9. 如果秒的小数点、分秒符号被 OCR 粘连，例如 11°342050'N，应理解为 11°34'20.50"N；8°502258'O 应理解为 8°50'22.58"O。
10. 必须按 Point 编号逐行读取，Point 1、2、3、4 都要输出，不能漏掉中间行或最后一行。
11. 输出顺序必须是 Longitude,Latitude，也就是经度在前、纬度在后。Latitude nord 不能放在第一列。
12. 如果表头是 Longitude ouest，所有经度都必须是负数。

输出必须只返回：
经度,纬度
经度,纬度

不要解释，不要表头，不要点号。不要压缩小数位。无法识别有效坐标时，只输出：${noCoordinatesText}`
            },
            {
              type: "image_url",
              image_url: {
                url: imageDataUrl
              }
            }
          ]
        }
      ]
    });

    console.log("调用 OpenAI 是否成功：是");

    const rawText = response.choices?.[0]?.message?.content || "";
    const coordinates = extractCoordinateLines(rawText);

    */
    console.log("OpenAI 返回的原始内容：");
    console.log(rawText);
    console.log("坐标提取结果：");
    console.log(coordinates);

    const recognitionWarning = extractRecognitionWarning(rawText);

    res.json({
      model,
      rawText,
      coordinates,
      precisionMode: "preserve-original-decimals-and-parse-dms",
      warning: recognitionWarning
    });
  } catch (error) {
    const errorMessage = getOpenAIErrorMessage(error);

    console.error("AI 识别失败，尝试本地 OCR 兜底。真实错误信息：", errorMessage);

    try {
      if (!req.file) {
        throw error;
      }

      const result = await Tesseract.recognize(req.file.buffer, "eng", {
        logger: info => console.log(info.status, info.progress)
      });
      const rawText = result.data.text || "";
      const coordinates = extractCoordinateLines(rawText);

      res.json({
        model: "local-tesseract-fallback",
        rawText,
        coordinates,
        precisionMode: "local-ocr-dms-fallback",
        warning: `AI 识别失败，已改用本地 OCR 兜底。AI 错误：${errorMessage}`
      });
    } catch (fallbackError) {
      console.error(fallbackError);
      res.status(500).json({
        error: `${errorMessage}；本地 OCR 兜底也失败：${fallbackError.message || "未知错误"}`,
        rawText: "",
        coordinates: ""
      });
    }
  }
});

const port = process.env.PORT || 3000;

app.listen(port, () => {
  console.log(`坐标工具已启动：http://localhost:${port}`);
  console.log(`当前视觉模型：${model}`);
  console.log("坐标识别模式：原图识别 + DMS/X/Y 兜底解析 + 后台统计。");
});
