import assert from "node:assert/strict";
import {
  AGENTIC_COORDINATE_CONTRACT_VERSION,
  assertAgenticCoordinateConsistency,
  buildAgenticCoordinateRecognitionPrompt,
  normalizeAgenticCoordinateResult,
  parseAgenticProviderResponse,
  runAgenticCoordinateRecognition
} from "../server/agentic-coordinate-recognition/index.js";

const DATA_URL = "data:image/jpeg;base64,/9j/2Q==";

function providerResponse(payload) {
  return {
    choices: [
      {
        message: {
          content: JSON.stringify(payload)
        }
      }
    ]
  };
}

function geographicPoint(label, sourceText, latitude, longitude, needsReview = false) {
  return {
    label,
    sourceText,
    x: null,
    y: null,
    latitude,
    longitude,
    needsReview
  };
}

const singleBoundary = {
  success: true,
  resultStatus: "usable",
  displayText: "POINT A | 10°52′15″N | 08°16′00″W\nPOINT B | 10°52′19″N | 08°16′40″W",
  coordinateSystem: {
    kind: "geographic",
    name: "WGS84",
    epsg: "EPSG:4326",
    status: "identified"
  },
  geometryType: "Polygon",
  groups: [
    {
      name: null,
      points: [
        geographicPoint("POINT A", "10°52′15″N, 08°16′00″W", 10.870833, -8.266667),
        geographicPoint("POINT B", "10°52′19″N, 08°16′40″W", 10.871944, -8.277778),
        geographicPoint("POINT C", "10°52′30″N, 08°16′20″W", 10.875, -8.272222)
      ]
    }
  ],
  warnings: []
};

{
  let callCount = 0;
  const outcome = await runAgenticCoordinateRecognition({
    imageDataUrl: DATA_URL,
    modelName: "fixture-model",
    providerCall: async request => {
      callCount += 1;
      assert.equal(request.temperature, 0);
      assert.equal(request.imageItems.length, 1);
      assert.equal(request.maxTokens, 12000);
      assert.deepEqual(request.responseFormat, { type: "json_object" });
      assert.equal(request.enableThinking, false);
      assert.equal(request.highResolutionImages, true);
      assert.equal(request.timeoutMs, 90000);
      return providerResponse(singleBoundary);
    }
  });

  assert.equal(callCount, 1);
  assert.equal(outcome.execution.providerCallCount, 1);
  assert.equal(outcome.execution.retryCount, 0);
  assert.equal(outcome.execution.fallbackCount, 0);
  assert.equal(outcome.result.contractVersion, AGENTIC_COORDINATE_CONTRACT_VERSION);
  assert.equal(outcome.result.summary.groupCount, 1);
  assert.equal(outcome.result.summary.pointCount, 3);
  assert.equal(
    outcome.result.displayText,
    "POINT A | 10°52′15″N, 08°16′00″W\nPOINT B | 10°52′19″N, 08°16′40″W\nPOINT C | 10°52′30″N, 08°16′20″W"
  );
}

{
  const grouped = structuredClone(singleBoundary);
  grouped.resultStatus = "needs_review";
  grouped.geometryType = "MultiPolygon";
  grouped.groups = [
    {
      name: "SITES1",
      points: [
        geographicPoint("1", "12°00′36.9″N, 9°09′40.8″W", 12.01025, -9.161333),
        geographicPoint("2", "12°00′34.0″N, 9°09′22.0″W", 12.009444, -9.156111),
        geographicPoint("3", "12°00′48.1″N, 9°08′32.7″W", 12.013361, -9.142417)
      ]
    },
    {
      name: "SITES2",
      points: [
        geographicPoint("1", "11°59′46.7″N, 9°07′27.0″W", 11.996306, -9.124167, true),
        geographicPoint("2", "12°00′54.7″N, 9°05′59.9″W", 12.015194, -9.099972),
        geographicPoint("3", "12°00′36.1″N, 9°05′56.5″W", 12.010028, -9.099028)
      ]
    }
  ];
  grouped.warnings = ["SITES2 point 1 needs review"];

  const parsed = parseAgenticProviderResponse(providerResponse(grouped));
  assert.equal(parsed.summary.groupCount, 2);
  assert.equal(parsed.summary.pointCount, 6);
  assert.equal(parsed.groups[0].name, "SITES1");
  assert.equal(parsed.groups[1].name, "SITES2");
  assert.match(parsed.displayText, /^SITES1\n1 \| 12°00′36\.9″N, 9°09′40\.8″W/m);
  assert.match(parsed.displayText, /\n\nSITES2\n1 \| 11°59′46\.7″N, 9°07′27\.0″W/);
}

{
  const decimalDisplayButSourceDms = structuredClone(singleBoundary);
  decimalDisplayButSourceDms.displayText = "10.870833,-8.266667\n10.871944,-8.277778\n10.875,-8.272222";
  const parsed = parseAgenticProviderResponse(providerResponse(decimalDisplayButSourceDms));
  assert.match(parsed.displayText, /POINT A \| 10°52′15″N, 08°16′00″W/);
  assert.match(parsed.displayText, /POINT C \| 10°52′30″N, 08°16′20″W/);
  assert.doesNotMatch(parsed.displayText, /10\.870833,-8\.266667/);
  assert.equal(parsed.groups[0].points[0].latitude, 10.870833);
  assert.equal(parsed.groups[0].points[0].longitude, -8.266667);
}

{
  const inventedFixedChunks = structuredClone(singleBoundary);
  inventedFixedChunks.resultStatus = "needs_review";
  inventedFixedChunks.geometryType = "MultiPolygon";
  inventedFixedChunks.groups = [
    {
      name: null,
      points: [
        geographicPoint("1", "10°52′15″N, 08°18′00″W", 10.870833, -8.3),
        geographicPoint("2", "10°48′00″N, 08°17′00″W", 10.8, -8.283333),
        geographicPoint("3", "10°51′20″N, 08°17′51″W", 10.855556, -8.2975),
        geographicPoint("4", "10°51′30″N, 08°17′41″W", 10.858333, -8.294722),
      ],
    },
    {
      name: null,
      points: [
        geographicPoint("5", "10°51′40″N, 08°17′30″W", 10.861111, -8.291667),
        geographicPoint("6", "10°51′30″N, 08°17′51″W", 10.858333, -8.2975),
        geographicPoint("7", "10°51′40″N, 08°17′41″W", 10.861111, -8.294722),
        geographicPoint("8", "10°51′50″N, 08°17′20″W", 10.863889, -8.288889),
      ],
    },
  ];
  inventedFixedChunks.warnings = ["Grouping uncertain"];
  const preserved = parseAgenticProviderResponse(providerResponse(inventedFixedChunks));
  assert.equal(preserved.resultStatus, "needs_review");
  assert.equal(preserved.geometryType, "Unknown");
  assert.equal(preserved.summary.groupCount, 1);
  assert.equal(preserved.summary.pointCount, 8);
  assert.equal(preserved.groups[0].name, null);
  assert.deepEqual(
    preserved.groups[0].points.map(point => point.label),
    ["1", "2", "3", "4", "5", "6", "7", "8"]
  );
  assert.match(preserved.warnings.at(-1), /preserved in source order as one ungrouped sequence/);
  assert.throws(
    () => assertAgenticCoordinateConsistency(normalizeAgenticCoordinateResult(inventedFixedChunks)),
    error => error.code === "AGENTIC_RESULT_INCONSISTENT"
      && /explicit visible name for every group/.test(error.message)
  );
}

{
  const duplicatedGroupNames = structuredClone(singleBoundary);
  duplicatedGroupNames.resultStatus = "needs_review";
  duplicatedGroupNames.geometryType = "MultiPolygon";
  duplicatedGroupNames.groups = [
    { name: "SITE", points: duplicatedGroupNames.groups[0].points },
    { name: "site", points: duplicatedGroupNames.groups[0].points },
  ];
  duplicatedGroupNames.warnings = ["Grouping uncertain"];
  const preserved = parseAgenticProviderResponse(providerResponse(duplicatedGroupNames));
  assert.equal(preserved.resultStatus, "needs_review");
  assert.equal(preserved.geometryType, "Unknown");
  assert.equal(preserved.summary.groupCount, 1);
  assert.equal(preserved.summary.pointCount, 6);
}

{
  const projected = {
    success: true,
    resultStatus: "needs_review",
    displayText: "1 | 4065838.00 | 14654007.00",
    coordinateSystem: {
      kind: "projected",
      name: null,
      epsg: null,
      status: "needs_confirmation"
    },
    geometryType: "Point",
    groups: [
      {
        name: null,
        points: [
          {
            label: "1",
            sourceText: "4065838.00, 14654007.00",
            x: 4065838,
            y: 14654007,
            latitude: null,
            longitude: null,
            needsReview: false
          }
        ]
      }
    ],
    warnings: ["CRS needs confirmation"]
  };
  const parsed = parseAgenticProviderResponse(providerResponse(projected));
  assert.equal(parsed.coordinateSystem.kind, "projected");
  assert.equal(parsed.coordinateSystem.status, "needs_confirmation");
  assert.equal(parsed.groups[0].points[0].x, 4065838);
}

{
  const invalidPoint = structuredClone(singleBoundary);
  invalidPoint.geometryType = "Point";
  assert.throws(
    () => parseAgenticProviderResponse(providerResponse(invalidPoint)),
    error => error.code === "AGENTIC_RESULT_INCONSISTENT"
      && /Point requires exactly one group with one point/.test(error.message)
  );
}

{
  const invalidKind = structuredClone(singleBoundary);
  invalidKind.coordinateSystem.kind = "projected";
  assert.throws(
    () => parseAgenticProviderResponse(providerResponse(invalidKind)),
    error => error.code === "AGENTIC_RESULT_INCONSISTENT"
      && /projected result requires X and Y/.test(error.message)
  );
}

{
  const invalidCrs = structuredClone(singleBoundary);
  invalidCrs.coordinateSystem = {
    kind: "geographic",
    name: null,
    epsg: null,
    status: "needs_confirmation"
  };
  assert.throws(
    () => parseAgenticProviderResponse(providerResponse(invalidCrs)),
    error => error.code === "AGENTIC_RESULT_INCONSISTENT"
      && /usable result requires an identified coordinate system/.test(error.message)
  );
}

{
  const ambiguousGeometry = structuredClone(singleBoundary);
  ambiguousGeometry.resultStatus = "needs_review";
  ambiguousGeometry.geometryType = "Unknown";
  ambiguousGeometry.warnings = ["Geometry intent needs review"];
  const parsed = parseAgenticProviderResponse(providerResponse(ambiguousGeometry));
  assert.equal(parsed.geometryType, "Unknown");
  assert.equal(parsed.resultStatus, "needs_review");
  assert.equal(parsed.summary.pointCount, 3);
  assert.equal(parsed.summary.groupCount, 1);
}

{
  let callCount = 0;
  await assert.rejects(
    runAgenticCoordinateRecognition({
      imageDataUrl: DATA_URL,
      modelName: "fixture-model",
      providerCall: async () => {
        callCount += 1;
        return { choices: [{ message: { content: "not-json" } }] };
      }
    }),
    /invalid JSON/
  );
  assert.equal(callCount, 1, "invalid output must not trigger a retry");
}

{
  const prompt = buildAgenticCoordinateRecognitionPrompt();
  assert.match(prompt, /return exactly one group/i);
  assert.match(prompt, /must never replace a DMS or projected X\/Y source row/i);
  assert.match(prompt, /Never split rows every four points/i);
  assert.match(prompt, /every group\.name must copy its visible title exactly/i);
  assert.match(prompt, /return one group, preserve every row in source order/i);
  assert.match(prompt, /Observation\/sample locations are points/i);
  assert.match(prompt, /two side-by-side column sets/i);
  assert.match(prompt, /structured numeric fields must agree/i);
  assert.match(prompt, /visible semantic role/i);
  assert.match(prompt, /set geometryType=Unknown and resultStatus=needs_review/i);
  assert.match(prompt, /Never guess a geometry merely from point count/i);
  assert.match(prompt, /final visual self-check/i);
  assert.match(prompt, /Do not infer a projection, zone, hemisphere, datum, or EPSG/i);
  assert.match(prompt, /Do not use markdown fences/i);
}

console.log("agentic-coordinate-recognition-v1-regression: PASS");
