# P09C Global Satellite Prototype

This is an isolated TRACK-S prototype. It is not wired into the GeoKit Lab
production application, routes, DNS, Nginx, ECS, Coordinate, KML, or V3 runtime.

## Local run

Set `MAPTILER_TEST_KEY` in the current shell without writing it to a file, then:

```powershell
npm.cmd install
npm.cmd start
```

Open exactly `http://localhost:3000`. The MapTiler test key must be restricted
to `localhost:3000` in MapTiler Cloud. If the variable is missing or the provider
cannot initialize within the bounded timeout, the prototype displays the local
SVG geometry fallback.

The key is emitted only into a no-store runtime configuration response needed by
the browser. It is never logged, persisted, included in screenshots, or written
to evidence. MapTiler Service Tokens are not supported.

## Authority boundary

Only `finalized_coordinate_result_v1` input with a valid WGS84 geometry identity
is accepted. Map eligibility is independent from KML eligibility. This prototype
does not mutate or derive `kmlReady`, `decisionState`, confirmation, quality gate,
result identity, revision, geometry hash, or authoritative geometry.

The China route is a non-network stub and returns `PROVIDER_PENDING` or the local
SVG fallback. AMap, Esri, and Tianditu are not connected.
