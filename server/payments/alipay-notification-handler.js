// Handler factory only. No public route or runtime configuration is installed.
export function createAlipayNotificationHandler({ orchestrator } = {}) {
  if (typeof orchestrator?.confirmProviderNotification !== "function") throw new TypeError("notification orchestrator required");
  return async function alipayNotification(req, res) {
    if (req.method !== "POST") return res.status(405).type("text/plain").send("failure");
    const rawBody = req.rawBody;
    if (!(typeof rawBody === "string" || rawBody instanceof Uint8Array)) {
      return res.status(400).type("text/plain").send("failure");
    }
    try {
      const result = await orchestrator.confirmProviderNotification({ envelope: { headers: req.headers, rawBody } });
      if (result?.acknowledge !== true) return res.status(503).type("text/plain").send("failure");
      return res.status(200).type("text/plain").send("success");
    } catch (_) {
      // No exception, raw payload, signature, identity or configuration is logged.
      return res.status(503).type("text/plain").send("failure");
    }
  };
}
