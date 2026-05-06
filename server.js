import "dotenv/config";
import express from "express";
import multer from "multer";
import { createClient } from "@supabase/supabase-js";
import path from "node:path";
import { fileURLToPath } from "node:url";
import fs from "node:fs";
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
const DAILY_FREE_CONVERT_LIMIT = 3;
const DAILY_FREE_JUDGE_LIMIT = 2;
const USD_PER_TROY_OUNCE_GRAMS = 31.1035;
const DEFAULT_USD_CNY_RATE = 7.2;
const usdCnyRate = Number(process.env.USD_CNY_RATE || DEFAULT_USD_CNY_RATE);
const goldPriceApiUrl = String(process.env.GOLD_PRICE_API_URL || "https://www.goldapi.io/api/XAU/USD").trim();
const goldPriceApiKey = String(process.env.GOLDAPI_KEY || process.env.GOLD_PRICE_API_KEY || "").trim();
const shareMetaMap = {
  "/": {
    title: "矿业空间工具 Geo Tool",
    desc: "坐标处理、AI矿地判读、黄金成色估算，一页进入。",
    image: "/share-home.png"
  },
  "/tool": {
    title: "坐标处理工具",
    desc: "上传坐标图或粘贴坐标，一键整理并生成 KML 文件。",
    image: "/share-tool.png"
  },
  "/convert": {
    title: "坐标处理工具",
    desc: "上传坐标图或粘贴坐标，一键整理并生成 KML 文件。",
    image: "/share-tool.png"
  },
  "/ocr": {
    title: "坐标处理工具",
    desc: "上传坐标图或粘贴坐标，一键整理并生成 KML 文件。",
    image: "/share-tool.png"
  },
  "/judge": {
    title: "AI矿地判读",
    desc: "上传一张图，快速判断是否值得继续投入。",
    image: "/share-judge.png"
  },
  "/gold": {
    title: "黄金成色计算器",
    desc: "输入黄金重量和排水差重，快速估算成色、K值和参考总价。",
    image: "/share-gold.png"
  }
};
const shareImageVersion = "20260505";

app.use(express.json({ limit: "1mb" }));

const appVersion = "2026-05-01-quota-contact-v2";

app.use((req, res, next) => {
  const noCachePaths = new Set(["/", "/tool", "/convert", "/ocr", "/judge", "/gold", "/admin", "/index.html", "/admin.html"]);

  if (noCachePaths.has(req.path) || req.path.endsWith(".html")) {
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
    res.setHeader("Pragma", "no-cache");
    res.setHeader("Expires", "0");
  }

  next();
});

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
  const meta = shareMetaMap[normalizedPath] || shareMetaMap["/"];
  const origin = getRequestOrigin(req);
  return {
    ...meta,
    url: `${origin}${normalizedPath === "/index.html" ? "/" : normalizedPath}`,
    imageUrl: `${origin}${meta.image}?v=${shareImageVersion}`
  };
}

function renderIndexWithMeta(req, res) {
  const meta = getShareMeta(req);
  const html = fs.readFileSync(path.join(__dirname, "index.html"), "utf8")
    .replaceAll("<!--TITLE-->", escapeHtml(meta.title))
    .replaceAll("<!--DESC-->", escapeHtml(meta.desc))
    .replaceAll("<!--IMAGE-->", escapeHtml(meta.imageUrl))
    .replaceAll("<!--URL-->", escapeHtml(meta.url));

  res.type("html").send(html);
}

app.get(["/", "/index.html", "/tool", "/convert", "/ocr", "/judge", "/gold"], renderIndexWithMeta);

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

app.get("/api/gold-price", async (req, res) => {
  try {
    const realtimePrice = await fetchRealtimeGoldPrice();
    res.json(realtimePrice || getUnavailableGoldPricePayload());
  } catch (error) {
    console.error("读取实时金价失败，返回不可用状态：", error.message || error);
    res.json(getUnavailableGoldPricePayload());
  }
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
      aiJudgeEnabled: true,
      goldCalculatorEnabled: true
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

  const todayKey = getTodayKey();

  if (user.usageDate !== todayKey) {
    user.usageDate = todayKey;
    user.freeConvertCount = DAILY_FREE_CONVERT_LIMIT;
    user.freeJudgeCount = DAILY_FREE_JUDGE_LIMIT;
  } else {
    user.freeConvertCount = toNonNegativeInteger(user.freeConvertCount, DAILY_FREE_CONVERT_LIMIT);
    user.freeJudgeCount = toNonNegativeInteger(user.freeJudgeCount, DAILY_FREE_JUDGE_LIMIT);
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

function checkUsageAvailable(user, type) {
  normalizeUsageCounters(user);

  if (!user) {
    return {
      allowed: false,
      source: "none",
      quota: {}
    };
  }

  if (user.isVip) {
    return {
      allowed: true,
      source: "vip",
      quota: buildQuotaPayload(user)
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

function consumeUsage(user, type) {
  const status = checkUsageAvailable(user, type);

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
      free_convert_count: DAILY_FREE_CONVERT_LIMIT,
      free_judge_count: DAILY_FREE_JUDGE_LIMIT,
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

async function normalizeSupabaseDailyFreeQuota(userId, user) {
  if (!supabase || !user || !Object.prototype.hasOwnProperty.call(user, "free_quota_date")) {
    return user;
  }

  const todayKey = getTodayKey();
  const quotaDate = String(user.free_quota_date || "").slice(0, 10);

  if (quotaDate === todayKey) {
    return user;
  }

  const { data, error } = await supabase
    .from("users")
    .update({
      free_convert_count: DAILY_FREE_CONVERT_LIMIT,
      free_judge_count: DAILY_FREE_JUDGE_LIMIT,
      free_quota_date: todayKey,
      updated_at: new Date().toISOString()
    })
    .eq("user_id", userId)
    .select("*")
    .single();

  if (error) {
    throw error;
  }

  return data || user;
}

function buildSupabaseQuotaPayload(user) {
  return {
    free_convert_count: Number(user?.free_convert_count || 0),
    free_judge_count: Number(user?.free_judge_count || 0),
    paid_convert_count: Number(user?.paid_convert_count || 0),
    paid_judge_count: Number(user?.paid_judge_count || 0),
    free_quota_date: user?.free_quota_date || "",
    is_vip: Boolean(user?.is_vip),
    freeConvertCount: Number(user?.free_convert_count || 0),
    freeJudgeCount: Number(user?.free_judge_count || 0),
    paidConvertCount: Number(user?.paid_convert_count || 0),
    paidJudgeCount: Number(user?.paid_judge_count || 0),
    freeQuotaDate: user?.free_quota_date || "",
    isVip: Boolean(user?.is_vip)
  };
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
    is_vip: Boolean(user?.is_vip)
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

async function checkSupabaseUsageAvailable(userId, type) {
  const user = await getOrCreateSupabaseUser(userId);

  if (!user) {
    return {
      allowed: false,
      success: false,
      reason: "db_disabled",
      quota: {}
    };
  }

  if (user.is_vip) {
    return {
      allowed: true,
      success: true,
      reason: "ok",
      source: "vip",
      user,
      quota: buildSupabaseQuotaPayload(user)
    };
  }

  const paidKey = type === "judge" ? "paid_judge_count" : "paid_convert_count";
  const freeKey = type === "judge" ? "free_judge_count" : "free_convert_count";
  const paidCount = Number(user[paidKey] || 0);
  const freeCount = Number(user[freeKey] || 0);

  if (freeCount > 0) {
    return {
      allowed: true,
      success: true,
      reason: "ok",
      source: "free",
      user,
      quota: buildSupabaseQuotaPayload(user)
    };
  }

  if (paidCount > 0) {
    return {
      allowed: true,
      success: true,
      reason: "ok",
      source: "paid",
      user,
      quota: buildSupabaseQuotaPayload(user)
    };
  }

  return {
    allowed: false,
    success: false,
    reason: "limit_exceeded",
    source: "none",
    user,
    quota: buildSupabaseQuotaPayload(user)
  };
}

async function consumeSupabaseUsage(userId, type) {
  const status = await checkSupabaseUsageAvailable(userId, type);
  const user = status.user;

  if (!status.allowed) {
    return status;
  }

  if (status.source === "vip") {
    return status;
  }

  const paidKey = type === "judge" ? "paid_judge_count" : "paid_convert_count";
  const freeKey = type === "judge" ? "free_judge_count" : "free_convert_count";
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
      throw error;
    }

    if (data) {
      return {
        success: true,
        reason: "ok",
        source: status.source,
        user: data,
        quota: buildSupabaseQuotaPayload(data)
      };
    }
  }

  const freshStatus = await checkSupabaseUsageAvailable(userId, type);

  if (freshStatus.allowed && freshStatus.source !== status.source) {
    return consumeSupabaseUsage(userId, type);
  }

  const freshUser = freshStatus.user || await getOrCreateSupabaseUser(userId);

  return {
    success: false,
    reason: "limit_exceeded",
    user: freshUser || user,
    quota: buildSupabaseQuotaPayload(freshUser || user)
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
  const text = stripOcrBboxPrefix(line).trim();
  const partPattern = /[-+]?\d{1,3}(?:(?:\s*\u00B0\s*|\s+)\d{1,2}(?:[\s.'\u2032]+\d{1,2}){1,2}(?:\.\d+)?|\.\d{1,2}\.\d{1,2}(?:\.\d+)?)\s*["\u2033]?\s*[NSEWO]/gi;
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

function groupEveryFourLinesWhenLikely(text, sourceText = "") {
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

  for (let start = 1; start < digits.length - 5; start += 1) {
    const candidate = digits.slice(start);

    if (isBftmYValue(candidate)) {
      return candidate;
    }
  }

  for (let end = digits.length - 1; end >= 7; end -= 1) {
    const candidate = digits.slice(0, end);

    if (isBftmYValue(candidate)) {
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
  const cleanLine = stripOcrBboxPrefix(line);
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
  if (shouldUseStrictBftmValidation(text)) {
    const projectedLines = extractProjectedCoordinateLines(text);
    return projectedLines.length > 0 ? projectedLines.join("\n") : noCoordinatesText;
  }

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

function normalizeJudgeOutput(text) {
  const raw = String(text || "").trim();
  const sectionNames = [
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
    "结论": "需要补充现场照片后再判断。",
    "等级": "C 谨慎",
    "判读可信度": "中",
    "潜力评分": "45分",
    "关键依据": "1. 图片清晰度有限。\n2. 缺少明确尺度参照。\n3. 需要更多现场角度。",
    "主要风险": "1. 缺少断面信息，无法判断内部结构。\n2. 单张图容易误判表面反光。",
    "下一步": "1. 加硬币或手指做比例后补拍。\n2. 敲开样本拍断面。",
    "一句话总结": "先补充关键照片，再决定是否继续投入时间。"
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
    values["结论"] = lines[0] || defaults["结论"];
    values["等级"] = defaults["等级"];
    values["关键依据"] = lines.slice(1, 4).map((line, index) => `${index + 1}. ${line.replace(/^\d+[.、]\s*/, "")}`).join("\n") || defaults["关键依据"];
    values["主要风险"] = defaults["主要风险"];
    values["下一步"] = defaults["下一步"];
    values["一句话总结"] = defaults["一句话总结"];
  }

  return sectionNames
    .map(name => `【${name}】\n${values[name] || defaults[name]}`)
    .join("\n\n");
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
      .limit(100);

    if (userId) {
      query = query.ilike("user_id", `%${userId}%`);
    }

    const { data, error } = await query;

    if (error) {
      throw error;
    }

    res.json({ users: data || [] });
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
      free_convert_count: toNonNegativeInteger(req.body?.free_convert_count, 0),
      paid_convert_count: toNonNegativeInteger(req.body?.paid_convert_count, 0),
      free_judge_count: toNonNegativeInteger(req.body?.free_judge_count, 0),
      paid_judge_count: toNonNegativeInteger(req.body?.paid_judge_count, 0),
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
      action: "update_quota",
      beforeData: pickSupabaseQuotaLogFields(beforeUser),
      afterData: pickSupabaseQuotaLogFields(data),
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

    const { data, error } = await supabase
      .from("users")
      .update({
        is_vip: nextVip,
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
      goldCalculatorEnabled: nextFlags.goldCalculatorEnabled !== false
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

    res.json({
      success: true,
      quota: buildSupabaseQuotaPayload(user)
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

    const result = await consumeSupabaseUsage(visitorId, type);
    await updateSupabaseUserVisitMeta(visitorId, req);

    if (result.reason === "limit_exceeded") {
      return res.status(403).json({
        success: false,
        reason: "limit_exceeded",
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

    const usageStatus = await checkSupabaseUsageAvailable(visitorId, "judge");
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
      const usageResult = await consumeSupabaseUsage(visitorId, "judge");
      await appendUsageLog(data, user, req, "judge", usageResult.source);

      if (user) {
        user.eventCount = Number(user.eventCount || 0) + 1;
      }

      await writeAdminData(data);

      return res.json({
        result: normalizedOutput,
        rawOutput,
        recordId: record.id,
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
    const prompt = `你是矿业空间判读助手。直接根据这一张图片输出短而有价值的初筛结果。

规则：不预测含量/储量/金点；不要默认怀疑AI图；少用“可能、疑似、信息不足”，除非确实看不清。结论要明确，依据要具体，下一步要可执行。

只输出下面8段，不要长篇解释：

【结论】
1句话。

【等级】
A / B / C / D，并解释一句。A=明显值得继续；B=有潜力但需要验证；C=风险较大不建议投入太多；D=直接排除。

【判读可信度】
高 / 中 / 低。它表示“本次图片是否足够支撑判断”，不是好坏程度。清晰、主体明确、依据充分=高；可判断但缺少尺度/角度/环境=中；模糊、无关、信息不足=低。

【潜力评分】
0-100分。它表示“是否值得继续投入时间”。A通常80-95，B通常60-79，C通常35-59，D通常0-34；同等级内部要根据图片内容给不同分数，不要固定模板分。

【关键依据】
1.
2.
3.
每条一句，写具体观察，不写“需要专业鉴定”。

【主要风险】
1.
2.
每条一句。

【下一步】
给2条具体动作，如敲开看断面、加比例物拍照、补拍原地环境或河道上下游图。

【一句话总结】
用一句人话告诉用户：这张图现在值不值得继续投入时间。`;

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

    const rawOutput = response.choices?.[0]?.message?.content || "";
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
    const usageResult = await consumeSupabaseUsage(visitorId, "judge");
    await appendUsageLog(data, user, req, "judge", usageResult.source);

    if (user) {
      user.eventCount = Number(user.eventCount || 0) + 1;
    }

    await writeAdminData(data);

    res.json({
      result: normalizedOutput,
      rawOutput,
      recordId: record.id,
      quota: usageResult.quota
    });
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

/*
 * Coordinate recognition maintenance rules
 *
 * Core principle: coordinate tables must use visual understanding first, not OCR text first.
 * OCR returns text blocks and bbox/pixel positions, but it does not understand row relationships.
 * For table coordinates this can pair X with X, Y with Y, or leak bbox values such as
 * "658800,148,29,669,89". Visual models can read the table layout and pair values from the
 * same horizontal row.
 *
 * Stable recognition checklist:
 * - Handwritten DMS: keep the old recognizedLines display path and groupEveryFourLinesWhenLikely().
 * - Standard DMS tables: visual understanding first.
 * - BFTM / X-Y tables: visual model reads the table layout first; OCR is only fallback.
 * - Multi-table and Point A-Z tables: visual understanding first so table boundaries and row order survive.
 * - OCR: use only for low-row-count retry or fallback, never as the main flow for table coordinates.
 *
 * Backend rules are guardrails only: validate coordinates, reject bbox pollution, reject X,X / Y,Y
 * column-pairing errors, and extract from clear text. Do not try to reconstruct table rows from
 * corrupted OCR bbox output. For future fixes, identify the image type first and adjust only that
 * type's model flow; do not rewrite the whole recognition system.
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

    if (!visitorId) {
      return res.status(400).json({
        success: false,
        reason: "missing_user",
        error: "缺少用户信息，请刷新页面后重试。",
        rawText: "",
        coordinates: ""
      });
    }

    await updateSupabaseUserVisitMeta(visitorId, req);

    const usageStatus = await checkSupabaseUsageAvailable(visitorId, "convert");

    if (!usageStatus.allowed && usageStatus.reason === "limit_exceeded") {
      return res.status(403).json({
        success: false,
        reason: "limit_exceeded",
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

    if (!aliyunApiKey) {
      console.error("坐标识别失败：缺少环境变量 ALIYUN_API_KEY 或 DASHSCOPE_API_KEY");
      return res.status(400).json({
        error: "阿里云 API 未配置",
        rawText: "",
        coordinates: ""
      });
    }

    console.log("图片文件名：", req.file.originalname);
    console.log("图片类型：", req.file.mimetype);
    console.log("图片大小：", `${req.file.size} bytes`);
    console.log("使用阿里云视觉模型：", aliyunVisionModel);

    const imageDataUrl = `data:${req.file.mimetype};base64,${req.file.buffer.toString("base64")}`;
    const prompt = `你是矿业坐标识别助手。请只识别图片中的真实坐标表区域，并只返回坐标行。图片可能是完整文件、手机截图、扫描件、带水印图片、长表、局部表格、同一页多块矿区坐标或带菜单按钮的截图。

必须忽略：
水印、背景字、页眉页脚、表格线、手机状态栏、底部菜单、Annoter、Tourner、Rechercher、Partager、Hectares、签名、正文段落、图片像素位置、文字框坐标、识别框坐标和碎数字。

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
10. 手写坐标可能写成 11°28.31.26N、08.40.42.13W、11°27'57.74 N、08 36 46.30 W 等不规范 DMS，请按度分秒理解。
11. 如果表格里有红色、手写、框选修正标记，例如把打印的 11° 手工改成 10°，优先按修正后的值识别；同时在最后增加一行识别提示，提醒用户核对。

输出规则：
1. 识别出什么格式，就保留什么格式。不要把度分秒自动转换成十进制度。
2. 每一行只输出一组坐标，格式固定为：经度,纬度。
3. 如果表格是 X/Y 平面坐标，每一行输出：X,Y，保留原数字。
4. 如果原图没有 N/W/O 字母，但表头写了 Latitude nord / Longitude ouest，需要在输出中补上 N 和 W，或用负号表达西经。
5. 必须按 Point 编号逐行读取。看到 4 个点就输出 4 行；看到 A-Z 就输出 A-Z 对应的全部行；看到 1-99 长表就按原编号顺序逐行输出。
6. 不能漏掉第一行、中间行或最后一行。
7. 如果 X 列连续两行相同，或 Y 列连续两行相同，也必须按同一行的 X 和 Y 配对，不要把下一行的 Y 拿来配上一行。
8. 表格右侧的斜线、手写勾、批注线不是数字，不要因为这些标记跳行或漏行。
9. 不要输出点号、表头、解释文字、Markdown、编号。
10. 不要压缩小数位，不要改写原始精度。
11. 如果同一张图片里有多块不同矿区/多组坐标，必须在不同组之间保留一个空行。每组内部仍然按原顺序逐行输出。
12. 手写坐标如果出现多段明显分开的 1、2、3、4 编号，每一段就是一组坐标，段与段之间必须输出一个空行。
13. 如果采用了手写、红色或框选修正，坐标行输出完成后，最后额外输出一行：识别提示：发现疑似人工修正，已按修正值识别，请核对。

示例：
09°01'13.67"W,11°43'16.45"N
08°53'32.66"W,11°52'11.93"N

642405.693,1051600.499
642812.120,1051903.440

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
    const imageItems = [
      {
        type: "image_url",
        image_url: {
          url: imageDataUrl
        }
      }
    ];

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
    let warning = extractRecognitionWarning(rawText);
    let usedModel = aliyunVisionModel;

    if (shouldRetryBftmRecognition(rawText, coordinates)) {
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

    if (shouldRetryBftmRecognition(rawText, coordinates)) {
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

    if (shouldRetryRecognition(rawText, coordinates)) {
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

    if (shouldRetryRecognition(rawText, coordinates)) {
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

    const bftmLongTable = getBftmLongTableInfo(rawText, coordinates);
    const bftmIncompleteWarning = makeBftmIncompleteWarning(bftmLongTable);

    if (bftmIncompleteWarning) {
      warning = warning ? `${warning} ${bftmIncompleteWarning}` : bftmIncompleteWarning;
    }

    res.json({
      model: usedModel,
      rawText,
      coordinates,
      precisionMode: "preserve-original-decimals-and-parse-dms",
      warning,
      bftmLongTable,
      quota: usageStatus.quota
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

      const fallback = await runLocalOcrFallback(req.file.buffer, errorMessage);
      let bftmLongTable = getBftmLongTableInfo(fallback.rawText, fallback.coordinates);
      let bftmIncompleteWarning = makeBftmIncompleteWarning(bftmLongTable);

      if (bftmIncompleteWarning && aliyunApiKey && req.file) {
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

      if (bftmIncompleteWarning) {
        fallback.warning = fallback.warning ? `${fallback.warning} ${bftmIncompleteWarning}` : bftmIncompleteWarning;
        fallback.bftmLongTable = bftmLongTable;
      }

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

app.listen(port, () => {
  console.log(`坐标工具已启动：http://localhost:${port}`);
  console.log(`当前阿里云视觉模型：${aliyunVisionModel}`);
  console.log(`当前阿里云OCR模型：${aliyunOcrModel}`);
  console.log("坐标识别模式：阿里云优先 + DMS/X/Y 解析 + 备用OCR人工核对提示 + 后台统计。");
});
