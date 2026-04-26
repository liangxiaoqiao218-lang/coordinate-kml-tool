# 坐标转 KML 小工具

一个面向矿业坐标整理的网页工具，支持手动坐标整理、图片识别尝试、人工协助入口和 KML 导出。

## 功能

- 坐标工作区：手动输入或粘贴坐标
- 复制内容
- 清空内容
- 标准化坐标为 `经度,纬度`
- 交换经纬度
- 导出 Point / LineString / Polygon KML
- 上传坐标图片尝试识别
- 自动识别失败时，引导用户添加微信人工协助

## 本地运行

```bash
npm install
npm start
```

然后打开：

```text
http://localhost:3000
```

## OpenAI 视觉识别

如果配置了 OpenAI API Key，后端会优先调用 OpenAI 视觉模型：

```bash
OPENAI_API_KEY=你的Key
OPENAI_MODEL=gpt-4o-mini
```

如果没有配置 `OPENAI_API_KEY`，项目会自动使用本地 OCR 兜底识别，但复杂图片建议走人工协助。

## 部署到 Render

1. 把项目上传到 GitHub。
2. 在 Render 新建 Web Service。
3. 连接这个 GitHub 仓库。
4. 设置：

```text
Build Command: npm install
Start Command: npm start
```

5. 如果要启用 OpenAI 视觉识别，在 Environment Variables 中添加：

```text
OPENAI_API_KEY=你的Key
OPENAI_MODEL=gpt-4o-mini
```

## 上传到 GitHub 时包含

```text
index.html
server.js
package.json
package-lock.json
wechat-qr.jpg
README.md
.gitignore
```

不要上传：

```text
node_modules
eng.traineddata
.env
*.log
```

