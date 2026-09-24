import { test as base, expect } from "@playwright/test";
import { installLocalhostOnlyNetworkGuard } from "./support/network-guard.mjs";

export const test = base.extend({
  diagnostics: [async ({ context, page }, use, testInfo) => {
    const diagnostics = {
      blockedRequests: [],
      consoleErrors: [],
      pageErrors: [],
      failedRequests: []
    };

    await installLocalhostOnlyNetworkGuard(context, diagnostics.blockedRequests);
    page.on("console", message => {
      if (message.type() === "error") diagnostics.consoleErrors.push(message.text());
    });
    page.on("pageerror", error => diagnostics.pageErrors.push(error.message));
    page.on("requestfailed", request => {
      diagnostics.failedRequests.push({
        url: request.url(),
        errorText: request.failure()?.errorText || "unknown"
      });
    });

    await use(diagnostics);

    await testInfo.attach("browser-diagnostics.json", {
      body: Buffer.from(JSON.stringify(diagnostics, null, 2)),
      contentType: "application/json"
    });
  }, { auto: true }]
});

export { expect };
