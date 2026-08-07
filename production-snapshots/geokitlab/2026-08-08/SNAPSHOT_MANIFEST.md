# geokitlab.com Primary Production Snapshot

## Snapshot Classification

- Environment: `https://geokitlab.com`
- Role: Primary Production
- Capture time: `2026-08-08T03:21:55+08:00`
- Capture method: read-only HTTPS retrieval of explicitly named public application files
- Snapshot type: public observable application snapshot
- Release status: not validated as a recommended production baseline
- Deployment impact: none

This snapshot gives the observable production state a Git-tracked identity. It does not prove the original build commit and must not be described as a complete server, database, container, or environment backup.

## Capture Scope

The following files were retrieved directly from the Primary Production origin and preserved byte-for-byte under `public-files/`:

| File | Bytes | SHA-256 | Exact Git blob | Normalized comparison blob |
|---|---:|---|---|---|
| `index.html` | 430012 | `f8e39ba7025a55a940d1313cc04417e2087ef36b5eb38ad7326253a18c6f6a5d` | `67f9229f894b2ad6bd7fe1c577f4c28d5c9011d1` | `0592c6aa28762b55735461b7150abfc2c47be151` |
| `package.json` | 653 | `a921b3999675b48a378be679870a76014528a5da9a292a54fce37e2bf9c15e60` | `445cee632138366148d10e9402b5090458b1d194` | `4bc671fb69b6a1f8d76c485d2227846c4e5d89d9` |
| `package-lock.json` | 48903 | `92269f5ee59d6ab45fd1072b6079fc69ed6e702c70e28492a42b0fc5a2da8a59` | `4dbc4fbb192b879634a15f25960fcccbe8abaaf3` | `98e8a91d141a298c2db0dc22b4ea7f4165b42362` |
| `server.js` | 536899 | `7d257622bbae03559dbddc1af513f9b8a4959e9506660ccdf4d598e19327e075` | `01e93c2178fb67889a593a2a9867b2c300356b74` | `3af4a2b24266e5c9b27521386112f32ba16d2b93` |

SHA-256 and the exact Git blob record the downloaded bytes. The directory-level `.gitattributes` disables text conversion for captured public files so Git preserves those bytes. The normalized comparison blob is recorded separately because line-ending normalization can make a downloaded text file align with a repository blob even when its raw byte hash differs.

## Comparison with origin/main

Reference commit:

```text
207443f8c7e5645bed04872e00f21de5781e59a0
```

| File | Snapshot vs `origin/main` |
|---|---|
| `server.js` | Normalized Git blob matches |
| `package.json` | Normalized Git blob matches |
| `package-lock.json` | Normalized Git blob matches |
| `index.html` | Does not match the committed Git blob |

This partial alignment does not establish `207443f` as the Primary Production deployment commit. The served `index.html` can be affected by runtime or environment-specific rendering, and no release record proves that the entire deployment was built from one clean commit.

## Observable Deployment Information

| Field | Observed value |
|---|---|
| DNS A | `120.24.174.202` |
| HTTP status | `200` |
| Server header | `nginx/1.18.0 (Ubuntu)` |
| Application header | `Express` |
| Root content type | `text/html; charset=utf-8` |
| Root content length | `430012` |
| Root cache policy | `no-store, no-cache, must-revalidate, proxy-revalidate` |
| Public application version | `2026-05-01-quota-contact-v2` |
| Original deployment time | `UNKNOWN` |
| Original build commit | `UNKNOWN` |
| Original artifact digest | `UNKNOWN` |
| Deployment operator/record | `UNKNOWN` |

The public application version is not a commit identifier and cannot establish source provenance.

## Environment Variable Inventory

Only variable names referenced by the captured application source are recorded. No environment values were requested, captured, or committed.

```text
ADMIN_PASSWORD
AI_INPUT_TOKEN_PRICE_PER_1K_CNY
AI_JUDGE_COST_PER_CALL_CNY
AI_OUTPUT_TOKEN_PRICE_PER_1K_CNY
ALIYUN_API_KEY
ALIYUN_BASE_URL
ALIYUN_OCR_MODEL
ALIYUN_VISION_MODEL
DASHSCOPE_API_KEY
DASHSCOPE_BASE_URL
DASHSCOPE_OCR_MODEL
DASHSCOPE_VISION_MODEL
ENABLE_REGRESSION_TEST_MODE
GOLD_PRICE_API_KEY
GOLD_PRICE_API_URL
GOLDAPI_KEY
NODE_ENV
PORT
SUPABASE_SERVICE_ROLE_KEY
SUPABASE_URL
USD_CNY_RATE
```

## Secret and Data Boundary

- No environment values are included.
- No database contents are included.
- No user, payment, order, entitlement, or usage records are included.
- No production credentials are included.
- A limited pattern scan found no embedded private key, OpenAI-style secret key, AWS access key, or literal password/secret assignment in the captured files.

The pattern scan is a safety check, not a guarantee that the captured source is free from every possible sensitive value.

## Known Limitations

- The production host does not expose a trusted directory inventory.
- Files not explicitly listed above were not assumed to exist or to match Git.
- Reverse-proxy, process-manager, operating-system, container, and infrastructure configuration are not captured.
- Environment values are intentionally excluded.
- Database schema and data are intentionally excluded.
- The original deployment timestamp cannot be recovered from the available HTTP metadata.
- The snapshot commit records observable state; it does not retroactively prove the source of the running deployment.

## Governance Status

```text
PRODUCTION SNAPSHOT CAPTURED
NOT VALIDATED AS RELEASE BASELINE
NOT AUTHORIZED FOR REDEPLOYMENT
```

Any later comparison, synchronization, or redeployment must use this snapshot as evidence of the observed state, not as an automatic approval to reproduce it.
