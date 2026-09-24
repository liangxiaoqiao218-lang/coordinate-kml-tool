# GeoKit Lab Playwright Acceptance Gate

This gate exercises the local application with Playwright without using the
ChatGPT browser-control bridge.

## Safety boundary

- The test server binds to `127.0.0.1:43127`.
- Provider credentials are blanked in the child server environment.
- Browser requests are allowed only for `localhost`, `127.0.0.1`, `::1`, and
  browser-local `about:`, `blob:`, and `data:` URLs.
- The local server is launched through `server-network-guard.cjs`, which rejects
  outbound HTTP, HTTPS, TCP, and TLS connections to non-loopback hosts.
- The suite does not access Render, ECS, production, OpenAI, Supabase, AMap,
  GoldAPI, or any other external provider.
- Reports, screenshots, videos, downloads, and traces are written below
  `artifacts/playwright/`, which is ignored by Git.

## Commands

```powershell
npm.cmd run test:e2e
```

For a visible local browser run:

```powershell
npm.cmd run test:e2e:headed
```

To open the latest HTML report:

```powershell
npm.cmd run test:e2e:report
```

## Initial coverage

- Runtime identity and local regression-mode proof.
- Home, coordinate, judge, and gold routes.
- Desktop and mobile coordinate-page layout evidence.
- WGS84 locked text fixture through the localhost-only regression API.
- Point, LineString, and Polygon manual-input KML downloads.
- Fail-closed empty coordinate actions.
- Gold-calculator success and invalid-input behavior.
- An explicit proof that a non-local browser request is blocked.
- A disposable server-side proof command can confirm that non-local requests
  fail with `E2E_EXTERNAL_NETWORK_BLOCKED`.

## Not covered in phase one

- Real OCR or multimodal providers.
- Supabase-backed accounts, billing, quotas, or storage.
- Real map tiles or satellite providers.
- Render RC, ECS production, or public-domain smoke tests.
- CAPTCHA, payment, or other human authorization steps.
