import { defineConfig } from "@playwright/test";

const port = 43127;
const baseURL = `http://127.0.0.1:${port}`;
const baselineCommit = "21c2ab031abdbede6ee339ad1d3a4956eee8b017";

const isolatedEnvironment = {
  ...process.env,
  PORT: String(port),
  NODE_ENV: "test",
  ENABLE_REGRESSION_TEST_MODE: "true",
  REGRESSION_TEST_MODE: "true",
  GIT_COMMIT: baselineCommit,
  GIT_BRANCH: "codex/playwright-acceptance-gate",
  WORKING_TREE_DIRTY: "true",
  OPENAI_API_KEY: "",
  ALIYUN_API_KEY: "",
  DASHSCOPE_API_KEY: "",
  ALIYUN_BASE_URL: "http://127.0.0.1:9",
  DASHSCOPE_BASE_URL: "http://127.0.0.1:9",
  SUPABASE_URL: "",
  SUPABASE_SERVICE_ROLE_KEY: "",
  AMAP_WEB_JS_KEY: "",
  AMAP_SECURITY_JSCODE: "",
  GOLDAPI_KEY: "",
  GOLD_PRICE_API_KEY: "",
  GOLD_PRICE_API_URL: "",
  ADMIN_PASSWORD: "",
  RC_ALLOWED_ORIGIN: "",
  PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_ENABLED: "false",
  PROVIDER_LAYOUT_PROFILE_QUALIFICATION_ENABLED: "false",
  P0_QUALIFICATION_ACQUISITION_ENABLED: "false"
};

export default defineConfig({
  testDir: "./tests/e2e",
  outputDir: "artifacts/playwright/test-results",
  fullyParallel: false,
  workers: 1,
  retries: 0,
  timeout: 45_000,
  expect: { timeout: 8_000 },
  reporter: [
    ["list"],
    ["html", { outputFolder: "artifacts/playwright/report", open: "never" }],
    ["json", { outputFile: "artifacts/playwright/results.json" }]
  ],
  use: {
    baseURL,
    acceptDownloads: true,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure"
  },
  webServer: {
    command: "node --require ./tests/e2e/support/server-network-guard.cjs server.js",
    url: `${baseURL}/api/version`,
    reuseExistingServer: false,
    timeout: 120_000,
    env: isolatedEnvironment
  },
  projects: [
    {
      name: "chromium-local",
      use: {
        browserName: "chromium",
        viewport: { width: 1440, height: 1000 }
      }
    }
  ]
});
