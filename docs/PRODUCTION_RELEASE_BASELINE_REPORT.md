# GeoKit Lab Production Release Baseline Report

## 1. Report Purpose

This report evaluates whether the observable `geokitlab.com` Primary Production snapshot can be associated with a source-controlled release commit.

The report does not change application code, deploy either production environment, synchronize versions, or authorize UTM V2 migration. Its conclusion is limited to the evidence available from the public production snapshot and the Git history fetched on `2026-08-08`.

Allowed source judgments are:

- `CONFIRMED`
- `LIKELY`
- `UNKNOWN`

## 2. Primary Snapshot Identity

- Environment: `https://geokitlab.com`
- Role: Primary Production
- Snapshot branch: `production/geokitlab-current-snapshot`
- Snapshot commit: `41b241539bfd7ebe40b7c192b5192f965b6a410a`
- Snapshot manifest: `production-snapshots/geokitlab/2026-08-08/SNAPSHOT_MANIFEST.md`
- Capture type: read-only public observable application snapshot
- Capture time: `2026-08-08T03:21:55+08:00`
- Original deployment time: `UNKNOWN`
- Original build commit: `UNKNOWN`
- Original artifact digest: `UNKNOWN`

The snapshot commit identifies the captured evidence. It does not retroactively become the original source commit of the running deployment.

## 3. Snapshot File Evidence

| File | Exact snapshot SHA-256 | Repository-normalized comparison blob |
|---|---|---|
| `server.js` | `7d257622bbae03559dbddc1af513f9b8a4959e9506660ccdf4d598e19327e075` | `3af4a2b24266e5c9b27521386112f32ba16d2b93` |
| `index.html` | `f8e39ba7025a55a940d1313cc04417e2087ef36b5eb38ad7326253a18c6f6a5d` | Runtime-rendered output; see Section 4.2 |
| `package.json` | `a921b3999675b48a378be679870a76014528a5da9a292a54fce37e2bf9c15e60` | `4bc671fb69b6a1f8d76c485d2227846c4e5d89d9` |
| `package-lock.json` | `92269f5ee59d6ab45fd1072b6079fc69ed6e702c70e28492a42b0fc5a2da8a59` | `98e8a91d141a298c2db0dc22b4ea7f4165b42362` |

Line-ending normalization is treated separately from semantic content. Raw SHA-256 values identify the exact downloaded bytes.

## 4. Source Commit Analysis

### 4.1 Candidate: `origin/main@207443f`

Full commit:

```text
207443f8c7e5645bed04872e00f21de5781e59a0
```

| File | Result | Evidence |
|---|---|---|
| `server.js` | `MATCH` | Git blob equals the normalized production snapshot blob |
| `package.json` | `MATCH` | Git blob equals the normalized production snapshot blob |
| `package-lock.json` | `MATCH` | Git blob equals the normalized production snapshot blob |
| `index.html` template | `MATCH` | Rendering the committed placeholders with the observed production metadata reproduces the snapshot exactly after CRLF/LF normalization |

The rendered-template verification produced:

```text
rendered_equals_snapshot=true
rendered_normalized_sha256=096af6dca6b60827bb3f9ea393a595bd15b49ae7ee8ee741c7ba688e8b6f13e8
snapshot_normalized_sha256=096af6dca6b60827bb3f9ea393a595bd15b49ae7ee8ee741c7ba688e8b6f13e8
```

The source template contains placeholders for title, description, image, URL, canonical URL, and JSON-LD. `server.js` replaces them in `renderIndexWithMeta()`. The apparent frontend diff is entirely explained by this runtime rendering plus line-ending differences; no UI, parser, KML, or user-flow difference was found.

### 4.2 Candidate: `origin/main-v1.1-clean`

Full commit:

```text
5a5e9951f67f1e1cd18a0cf9043b5cb0a887a935
```

| File | Result |
|---|---|
| `server.js` | `DIFFERENT` |
| `index.html` | `DIFFERENT` |
| `package.json` | `MATCH` |
| `package-lock.json` | `MATCH` |

This candidate does not reproduce the Primary Production application entry files and is rejected as the current source baseline.

### 4.3 Other Named Branch Candidates

| Candidate | `server.js` | `index.html` | `package.json` | `package-lock.json` | Assessment |
|---|---|---|---|---|---|
| `origin/release/v1.1-domain@fb8f6a8` | Different | Different | Different | Match | Rejected |
| `origin/release/v1.0@1b4a9fc` | Different | Different | Different | Match | Rejected |
| `origin/dev@f83cf23` | Different | Different | Different | Match | Rejected |
| `origin/v2/utm-intent-router@838e4bf` | Different | Different | Different | Different | Rejected as production source and not production-approved |

### 4.4 Historical Commit Scan

All 388 commits reachable from current `origin/*` references were checked for the four application file identities.

Seven commits contain the same four root application blobs as `207443f`:

```text
207443f  fix: separate handwritten dms review confirmation from editing
65ee327  docs: add UTM intent router V2 specification
52e06fe  feat: add UTM CRS evidence acquisition shadow pipeline
d6b15a6  feat: add typed UTM result shadow migration
39024f6  feat: add UTM migration observation shadow layer
4fc5649  docs: define UTM canonical migration and CRS confirmation flow
890c8e6  docs: define deployment governance strategy
```

`207443f` is the earliest reachable commit with the exact four-file application identity. The later commits inherited those entry files while adding documentation or shadow-only modules elsewhere in the tree.

Selected files unique to the later commits were tested through the production static-file path and returned `404`, including:

```text
docs/DEPLOYMENT_STRATEGY.md
docs/UTM_INTENT_ROUTER_V2_SPEC.md
server/crs-evidence/crs-vision-pass.js
server/utm-intent/typed-result.js
```

This supports `207443f` over the later commits. It is not conclusive because an unknown deployment process could have selectively excluded files from a later source checkout.

### 4.5 Earlier Near Matches

Several earlier commits matched `server.js`, `package.json`, and `package-lock.json` but had a different `index.html` template. They are rejected because the `207443f` template is required to reproduce the observed production page.

Examples include:

```text
1b9d6ef
2bb2f88
ede10dd
```

## 5. Source Judgment

```text
LIKELY
```

Likely source commit:

```text
207443f8c7e5645bed04872e00f21de5781e59a0
```

### 5.1 Why the Judgment Is `LIKELY`

- all four captured application files are reproduced by `207443f`;
- the runtime-rendered HTML is semantically and line-for-line identical after normalizing line endings;
- `207443f` is the earliest reachable commit with the complete four-file identity;
- named stable, release, development, and final UTM V2 heads do not match the captured application files;
- public evidence for files added by later source-equivalent commits is absent;
- Secondary Production is already auditable against `origin/main@207443f` for the same application entry files.

### 5.2 Why the Judgment Is Not `CONFIRMED`

- no original deployment manifest exists;
- no build artifact digest exists;
- no deployment log links `geokitlab.com` to the commit;
- the snapshot contains only publicly observable files, not the complete host filesystem or build context;
- the original deployment time is unknown;
- a dirty or selectively copied source tree cannot be excluded from the available evidence;
- multiple later commits retained the same four entry-file identities.

## 6. Release Baseline Recommendation

### 6.1 Recommended Source Baseline

```text
Commit: 207443f8c7e5645bed04872e00f21de5781e59a0
Confidence: LIKELY
Purpose: candidate source baseline for controlled release reconstruction
```

This recommendation means that `207443f` is the strongest reproducible Git source for the observed production application. It does not authorize immediate deployment and does not change the original provenance judgment to `CONFIRMED`.

### 6.2 Snapshot Role

```text
Snapshot commit: 41b241539bfd7ebe40b7c192b5192f965b6a410a
Role: evidence and comparison anchor
Redeployment authority: NONE
```

The snapshot must remain available as rollback and comparison evidence. It must not be treated as a reviewed release artifact without a separate security, completeness, and regression review.

### 6.3 Required Baseline Validation Before Deployment

1. Build an immutable release artifact from a clean `207443f` checkout.
2. Record its full commit and artifact digest.
3. Compare the built application with the production snapshot using normalized template and line-ending rules.
4. Run the locked coordinate, KML safety, authentication, entitlement, and applicable business-flow regressions.
5. Verify environment configuration without exposing secret values.
6. Define the rollback commit, artifact, operator, and procedure.
7. Approve the release baseline before changing either production environment.

## 7. Future Release Rule

Every future Primary Production deployment must record:

| Field | Required evidence |
|---|---|
| Git commit | Full 40-character source commit |
| Source state | Clean worktree and approved branch/review record |
| Build artifact | Immutable artifact name and cryptographic digest |
| Build time | Timestamp with timezone |
| Deploy time | Timestamp with timezone |
| Environment | Explicit Primary, Secondary, or Staging identifier |
| Configuration profile | Non-secret configuration profile identifier |
| Database authority | Authoritative service reference without credentials |
| Payment state | Enabled or disabled |
| Verification | Health, regression, Export, and business-flow results |
| Rollback target | Full commit and immutable artifact digest |
| Ownership | Deployment operator, approver, and rollback owner |
| Outcome | Healthy, failed, rolled back, or retired |

The runtime version endpoint must expose non-secret commit, artifact, environment, and build-time metadata. A manually maintained application version is not sufficient.

## 8. Final Baseline Status

```text
Primary snapshot identity: ESTABLISHED
Candidate source baseline: 207443f8c7e5645bed04872e00f21de5781e59a0
Source confidence: LIKELY
Original provenance: NOT CONFIRMED
Deployment synchronization: NOT AUTHORIZED
Payment integration: NOT AUTHORIZED BY THIS REPORT
UTM V2 production migration: NOT AUTHORIZED BY THIS REPORT
```
