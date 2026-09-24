const LOOPBACK_HOSTS = new Set(["127.0.0.1", "localhost", "[::1]"]);
const LOCAL_PROTOCOLS = new Set(["about:", "blob:", "data:"]);

export function isLocalTestUrl(rawUrl) {
  const url = new URL(rawUrl);
  return LOCAL_PROTOCOLS.has(url.protocol) || LOOPBACK_HOSTS.has(url.hostname);
}

export async function installLocalhostOnlyNetworkGuard(context, blockedRequests) {
  await context.route("**/*", async route => {
    const request = route.request();
    if (isLocalTestUrl(request.url())) {
      await route.continue();
      return;
    }

    blockedRequests.push({
      method: request.method(),
      resourceType: request.resourceType(),
      url: request.url()
    });
    await route.abort("blockedbyclient");
  });
}
