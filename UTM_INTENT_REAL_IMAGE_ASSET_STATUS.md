# UTM Intent Real Image Asset Status

Status: Phase 1.5 Evidence Acquisition Pass; Phase 1 Freeze Pending UTM30 Explicit-Evidence Coverage

Audit date: 2026-08-07

Scope: Coordinate Engine V2 UTM Intent Router Phase 1

## Phase 1.5 Update

The dedicated shadow-only CRS Vision pass now reads the original image independently of the legacy coordinate-row prompt. It is not integrated into the API, parser routing, `precisionMode`, `parserTrace`, KML, or Export.

Real-image results on 2026-08-07:

| Sample | CRS Vision evidence | Shadow result | Status |
|---|---|---|---|
| Indonesia UTM50S 01 | `UTM WGS 1984 ZONA 50S` | confirmed, `EPSG:32750` | Pass |
| Indonesia UTM50S 02 | `UTM WGS 1984 ZONA 50S` | confirmed, `EPSG:32750` | Pass |
| Indonesia UTM50S 03 | `UTM WGS 1984 ZONA 50S` | confirmed, `EPSG:32750` | Pass |
| BFTM negative | explicit BFTM or no UTM evidence | not confirmed UTM | Pass |
| MGRS negative | MGRS evidence or no UTM evidence | not confirmed UTM | Pass |
| Kyrgyz GK negative | GK evidence or no UTM evidence | not confirmed UTM | Pass |

Result: `Real Image CRS Evidence Regression: 6/6 PASS`.

This supersedes the legacy-only image-to-OCR findings below. Those findings are retained to document why the independent CRS Vision pass was required.

This temporary report records the real-image assets and the observed
image-to-OCR-to-Shadow results. Benchmark text, expected JSON, baseline JSON,
and hand-written OCR fixtures are not accepted as substitutes for this path.

## Found Assets

All seven required real-image assets are currently accessible. No image was
copied into the V2 worktree.

### Positive UTM assets

#### Burkina UTM30

```text
Path:
D:\萨赫勒数字科技有限公司\关于西非的业务\测试素材\布基纳法索03.png

Bytes:
448040

SHA-256:
d2952399ac7a5222ec60f9a8f5ba0a4776b6508199dbe6e72e2474659313440e
```

#### Indonesia UTM50S 01

```text
Path:
D:\萨赫勒数字科技有限公司\关于西非的业务\测试素材\印尼矿地01.jpg

Bytes:
277506

SHA-256:
2f508653305fee7c08470218f9bf94f75b56d26d7b28edcd7d8d68cd8f88eaf6
```

#### Indonesia UTM50S 02

```text
Path:
D:\萨赫勒数字科技有限公司\关于西非的业务\测试素材\印尼矿地02.jpg

Bytes:
288226

SHA-256:
707e971aef6e5a6744cbd860cf701e41218fe6fb9a609b88e8bd121d03348b5a
```

#### Indonesia UTM50S 03

```text
Path:
D:\萨赫勒数字科技有限公司\关于西非的业务\测试素材\印尼矿地03.jpg

Bytes:
302795

SHA-256:
41f2b2117667fb92f6a4eb703822b1893e29c985be2e14f7b20fbda103b66cf2
```

### Negative-protection assets

| Coverage | Path | SHA-256 |
|---|---|---|
| BFTM | `C:\Users\Mir-1\Documents\Codex\2026-08-05\coordinate-kml-tool-v11-main-v1.1-clean\test-fixtures\coordinate-recognition\bftm\bftm_burkina_002.jpg` | `c9409a89af333cfb9204cca15cbbb0e0198b31371dd9336fbefdf53847a82474` |
| MGRS | `C:\Users\Mir-1\Documents\Codex\2026-08-05\coordinate-kml-tool-v11-main-v1.1-clean\test-fixtures\coordinate-recognition\mgrs\mgrs_myanmar_001.jpg` | `af999328e3232af304e03901c5e8d58cad794ea588ee31709aef6668974f4004` |
| Kyrgyz GK | `C:\Users\Mir-1\Documents\Codex\2026-08-05\coordinate-kml-tool-v11-main-v1.1-clean\test-fixtures\coordinate-recognition\kyrgyz\kyrgyz_gk_001.png` | `94522774b1311a48f44b8c52370639add50cb8eb7734bbd812cad2fb6f954235` |

## Missing Assets

None of the seven required filenames is currently missing.

```yaml
missing: []
```

The previous missing-asset blocker is resolved by the source directory:

```text
D:\萨赫勒数字科技有限公司\关于西非的业务\测试素材
```

## Pre-Phase 1.5 Legacy-Only Results (Historical)

The positive assets were sent through the existing image-recognition API. The
returned OCR `rawText` and coordinate context were then passed to the standalone
Shadow UTM Intent Resolver. The resolver did not alter the API response object.

| Sample | Legacy result | OCR CRS evidence delivered to Shadow | Shadow result | Expected | Status |
|---|---|---|---|---|---|
| Indonesia UTM50S 01 | Generic projected-looking X/Y | None; coordinate rows only | `unknown`, no EPSG | `confirmed`, EPSG:32750 | Not met |
| Indonesia UTM50S 02 | DMS accepted from latitude/longitude columns | None; coordinate and DMS rows only | `unknown`, no EPSG | `confirmed`, EPSG:32750 | Not met |
| Indonesia UTM50S 03 | Generic projected-looking X/Y | None; coordinate rows only | `unknown`, no EPSG | `confirmed`, EPSG:32750 | Not met |
| Burkina UTM30 | `utm30n-projected-x-y` | None; coordinate rows only | `unknown`, no EPSG | `confirmed`, EPSG:32630 | Not met |
| BFTM negative | BFTM | No explicit UTM evidence | `unknown`, no EPSG | Not confirmed UTM | Pass |
| MGRS negative | MGRS | MGRS tokens retained | `unknown`; blocks `utm_projected_xy` | Not UTM | Pass |
| Kyrgyz GK negative | Kyrgyz GK | No explicit UTM evidence | `unknown`, no EPSG | Not UTM | Pass |

All seven Shadow invocations preserved their legacy response objects. Shadow
did not change `precisionMode`, `parserTrace`, coordinates, or KML data.

## Evidence Findings

### Indonesia UTM50S

All three source images visibly contain the footer label:

```text
UTM WGS 1984 ZONA 50S
```

The recognition API's returned `rawText` omitted that footer and returned only
coordinate rows, with DMS rows also returned for sample 02. Consequently, the
Shadow Resolver received no explicit UTM, WGS84, zone, or hemisphere evidence.
Its `unknown` result is the required safe behavior and must not be changed into
a numeric or country-based guess.

### Burkina UTM30

The source image visibly states that the coordinates are UTM, but it does not
visibly state WGS84, Zone 30N, or EPSG:32630. The recognition API returned only
the eight X/Y rows. Legacy selected `utm30n` through its existing projected-X/Y
route, while Shadow correctly refused to adopt that inference.

This image can protect legacy UTM30 behavior, but by itself it cannot prove a
complete explicit WGS84 UTM Zone 30N intent under the V2 evidence rules. A
document with explicit datum, zone, and hemisphere evidence is required for a
positive confirmed UTM30 intent baseline.

## Remaining Phase 1 Freeze Boundary

Phase 1.5 Evidence Acquisition is ready for review. Overall Phase 1 remains **NOT READY TO FREEZE** until explicit positive UTM30 evidence coverage is available.

The asset-availability and Indonesian CRS-acquisition blockers are resolved. The remaining blocker is:

1. The Burkina image does not contain sufficient explicit CRS fields to confirm
   WGS84 UTM Zone 30N without prohibited country or numeric inference.

This is not a Shadow Resolver rule failure. The resolver correctly returned
`unknown` when explicit evidence was absent from its input.

Overall Phase 1 freeze still requires:

- A real UTM30 image containing explicit WGS84, zone 30, and north-hemisphere
  evidence, or another authoritative evidence source allowed by the frozen
  specification.
- Repeated positive and negative image regression with unchanged legacy output.

No benchmark text, country inference, numeric-range inference, or manual OCR
substitution may be used to manufacture a passing result.
