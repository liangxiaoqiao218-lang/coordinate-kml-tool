# Coordinate Engine V3 Generic DMS Recognizer

Status: `IMPLEMENTED`

This recognizer ports deterministic DMS text parsing into the isolated V3
recognizer architecture. It is not connected to `server.js`, `index.html`,
public API routes, Vision, OCR, provider acquisition, V2 evidence, arbitration,
migration, or confirmation flows.

## Supported formats

- `11°27'45.54"N 08°36'30.76"W`
- `11°27 45 09 N 08°36 30.76 W`
- `11 27 45.09 N 08 36 30.76 W`
- `11°28.31.26N 08°36.30.76W`
- `11°27'45.54"N,08°36'30.76"W`
- `A: 11°27'45.54"N 08°36'30.76"W`
- `1. 11°27'45.54"N 08°36'30.76"W`
- Multiple labeled rows.
- Two-line latitude/longitude pairs when each line contains one explicit DMS
  token.

## Hemisphere normalization

- `N`, `North`, `Nord` -> positive latitude
- `S`, `South`, `Sud` -> negative latitude
- `E`, `East`, `Est` -> positive longitude
- `W`, `West`, `Ouest`, `O` -> negative longitude

Role is inferred from hemisphere, not from token order. Longitude-first input is
therefore accepted when one token has an east/west hemisphere and the other has
a north/south hemisphere.

## Validation

The recognizer rejects:

- missing hemisphere
- minutes `>= 60`
- seconds `>= 60`
- latitude degrees `> 90`
- longitude degrees `> 180`
- duplicate latitude roles
- duplicate longitude roles
- incomplete DMS tokens
- invalid numeric tokens

## Conversion

DMS conversion is deterministic:

```text
decimal = degrees + minutes / 60 + seconds / 3600
```

The hemisphere sign is applied after conversion.

## Geometry

The recognizer uses the shared V3 normalized result contract:

- 1 point -> `Point`
- 2 points -> `LineString`
- 3+ points -> `Polygon`

## KML order

Normalized coordinates store:

```text
latitude
longitude
```

KML coordinates use:

```text
longitude,latitude,0
```

## Rejected non-scope formats

The recognizer does not handle:

- WGS84 decimal pairs
- MGRS
- projected UTM X/Y
- Madagascar cadastral rows
- Kyrgyzstan Gauss-Kruger
- WGS84 table header parsing
- Côte d'Ivoire table acquisition
- structured coordinate table candidates with both headers and structured rows

## Isolation boundary

Generic DMS only recognizes already-present DMS text tokens. It does not own
table acquisition, image reading, handwriting review, OCR confidence, evidence
arbitration, shadow decisions, migration, or KML export policy.

When acquisition has already produced structured table metadata, the generic
DMS recognizer treats that input as outside plain-DMS scope even if one or more
table cells contain DMS reference text. Dedicated table recognizers own those
structured candidates through their own `canHandle()` contracts; the runner
still does not apply recognizer priority or precedence.

Future dedicated recognizers such as `cote_divoire_dms` may reuse pure DMS
conversion semantics, but Côte d'Ivoire table structure and source acquisition
must remain outside this generic recognizer.

## Latency

Provider calls, Vision calls, and OCR calls are always `0`. If the V3 latency
budget is already expired before parsing, the recognizer returns
`deadline_exceeded` without starting work.
