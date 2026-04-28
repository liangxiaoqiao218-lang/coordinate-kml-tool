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

function createDefaultAdminData() {
  return {
    users: {},
    events: [],
    featureFlags: {
      aiOcrEnabled: true,
      xyConvertEnabled: true,
      kmlExportEnabled: true,
      manualSupportEnabled: true
    }
  };
}

async function readAdminData() {
  try {
    const text = await fs.readFile(adminDataFile, "utf8");
    const data = JSON.parse(text);
    const defaults = createDefaultAdminData();

    return {
      ...defaults,
      ...data,
      users: data.users || {},
      events: Array.isArray(data.events) ? data.events : [],
      featureFlags: {
        ...defaults.featureFlags,
        ...(data.featureFlags || {})
      }
    };
  } catch (error) {
    if (error.code === "ENOENT") {
      return createDefaultAdminData();
    }

    throw error;
  }
}

async function writeAdminData(data) {
  await fs.writeFile(adminDataFile, JSON.stringify(data, null, 2), "utf8");
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
        manualSupportEnabled: true
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

function getClientIp(req) {
  const forwardedFor = req.get("x-forwarded-for") || "";
  const firstForwardedIp = forwardedFor.split(",")[0]?.trim();

  return firstForwardedIp || req.ip || req.socket?.remoteAddress || "";
}

function updateUserVisitMeta(user, req) {
  if (!user) {
    return;
  }

  const ip = getClientIp(req);
  const userAgent = req.get("user-agent") || "";

  if (!user.firstIp && ip) {
    user.firstIp = ip;
  }

  if (ip) {
    user.lastIp = ip;
  }

  if (userAgent) {
    user.lastUserAgent = userAgent.slice(0, 300);
  }

  user.lastSeenAt = getNowISO();
}

function getEffectivePermissions(user, featureFlags) {
  const permissions = user?.permissions || {};

  return {
    aiOcrEnabled: Boolean(featureFlags.aiOcrEnabled && permissions.aiOcrEnabled),
    xyConvertEnabled: Boolean(featureFlags.xyConvertEnabled && permissions.xyConvertEnabled),
    kmlExportEnabled: Boolean(featureFlags.kmlExportEnabled && permissions.kmlExportEnabled),
    manualSupportEnabled: Boolean(featureFlags.manualSupportEnabled && permissions.manualSupportEnabled)
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
  const dmsTokenPattern = /[-+]?\d{1,3}\s*°\s*(?:\d{1,2}\s*'\s*\d{1,4}(?:\.\d+)?|\d{3,7}(?:\.\d+)?)\s*["']?\s*[NSEWO]?/gi;

  for (const line of lines) {
    if (/annoter|tourner|rechercher|partager|hectares/i.test(line)) {
      continue;
    }

    const tokens = line.match(dmsTokenPattern) || [];

    if (tokens.length < 2) {
      continue;
    }

    const looksLikeLonLat = /,/.test(line) || /^\s*[-+]\s*\d/.test(line);
    const parsed = tokens
      .map((token, index) => parseCompactDmsToken(token, looksLikeLonLat ? "" : (index === 0 ? "N" : "O")))
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
    return decimalLines.join("\n");
  }

  const dmsLines = extractDmsCoordinateLines(text);

  return dmsLines.length > 0 ? dmsLines.join("\n") : noCoordinatesText;
}

function countCoordinateRows(text) {
  return String(text || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .filter(line => line !== noCoordinatesText)
    .length;
}

function getOpenAIErrorMessage(error) {
  const status = error.status ? `HTTP ${error.status}` : "";
  const code = error.code ? `code=${error.code}` : "";
  const type = error.type ? `type=${error.type}` : "";
  const message = error.message || "未知错误";

  return [status, code, type, message].filter(Boolean).join(" | ");
}

app.get("/api/config", async (req, res) => {
  try {
    const visitorId = String(req.query.visitorId || "").trim();
    const data = await readAdminData();
    const user = ensureUser(data, visitorId);

    if (user) {
      updateUserVisitMeta(user, req);
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
      updateUserVisitMeta(user, req);
      user.eventCount = (user.eventCount || 0) + 1;
    }

    data.events.push({
      id: makeId("evt"),
      visitorId,
      eventName,
      ip: getClientIp(req),
      userAgent: (req.get("user-agent") || "").slice(0, 300),
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
    const users = Object.values(data.users);
    const eventsByName = {};

    for (const event of data.events) {
      eventsByName[event.eventName] = (eventsByName[event.eventName] || 0) + 1;
    }

    res.json({
      totals: {
        users: users.length,
        events: data.events.length,
        vipUsers: users.filter(user => user.plan === "vip").length,
        disabledUsers: users.filter(user => user.status === "disabled").length
      },
      eventsByName,
      featureFlags: data.featureFlags,
      recentEvents: data.events.slice(-80).reverse()
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: "读取后台统计失败。"
    });
  }
});

app.get("/api/admin/users", requireAdmin, async (req, res) => {
  try {
    const data = await readAdminData();
    const users = Object.values(data.users)
      .sort((a, b) => String(b.lastSeenAt || "").localeCompare(String(a.lastSeenAt || "")));

    res.json({ users });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: "读取用户列表失败。"
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
        manualSupportEnabled: Boolean(req.body.permissions.manualSupportEnabled)
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
      manualSupportEnabled: Boolean(nextFlags.manualSupportEnabled)
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

app.post("/api/recognize-coordinates", upload.single("image"), async (req, res) => {
  console.log("---- 收到识别请求 ----");
  console.log("是否收到图片：", Boolean(req.file));

  try {
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
5. X / Y 或 Liste des Coordonnées 平面坐标表。
6. 十进制度、度分、度分秒 DMS。
7. N/S/E/W，法语 O / Ouest = West = 西经。
8. Latitude nord = 北纬；Longitude ouest = 西经。

输出规则：
1. 识别出什么格式，就保留什么格式。不要把度分秒自动转换成十进制度。
2. 每一行只输出一组坐标，格式固定为：经度,纬度。
3. 如果表格是 X/Y 平面坐标，每一行输出：X,Y，保留原数字。
4. 如果原图没有 N/W/O 字母，但表头写了 Latitude nord / Longitude ouest，需要在输出中补上 N 和 W，或用负号表达西经。
5. 必须按 Point 编号逐行输出，不能漏掉第一行、中间行或最后一行。看到 4 个点就输出 4 行；看到 A-Z 就输出 A-Z 对应的全部行。
6. 不要输出点号、表头、解释文字、Markdown、编号、空行。
7. 不要压缩小数位，不要改写原始精度。

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

    res.json({
      model,
      rawText,
      coordinates,
      precisionMode: "preserve-original-decimals-and-parse-dms"
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
