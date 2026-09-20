import assert from "node:assert/strict";
import {
  AGENTIC_COORDINATE_CONTRACT_VERSION,
  buildAgenticCoordinateRecognitionPrompt,
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
        geographicPoint("POINT B", "10°52′19″N, 08°16′40″W", 10.871944, -8.277778)
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
      return providerResponse(singleBoundary);
    }
  });

  assert.equal(callCount, 1);
  assert.equal(outcome.execution.providerCallCount, 1);
  assert.equal(outcome.execution.retryCount, 0);
  assert.equal(outcome.execution.fallbackCount, 0);
  assert.equal(outcome.result.contractVersion, AGENTIC_COORDINATE_CONTRACT_VERSION);
  assert.equal(outcome.result.summary.groupCount, 1);
  assert.equal(outcome.result.summary.pointCount, 2);
  assert.equal(outcome.result.displayText, singleBoundary.displayText);
}

{
  const grouped = structuredClone(singleBoundary);
  grouped.resultStatus = "needs_review";
  grouped.geometryType = "MultiPolygon";
  grouped.groups = [
    {
      name: "SITES1",
      points: [geographicPoint("1", "12°00′36.9″N, 9°09′40.8″W", 12.01025, -9.161333)]
    },
    {
      name: "SITES2",
      points: [geographicPoint("1", "11°59′46.7″N, 9°07′27.0″W", 11.996306, -9.124167, true)]
    }
  ];
  grouped.warnings = ["SITES2 point 1 needs review"];

  const parsed = parseAgenticProviderResponse(providerResponse(grouped));
  assert.equal(parsed.summary.groupCount, 2);
  assert.equal(parsed.summary.pointCount, 2);
  assert.equal(parsed.groups[0].name, "SITES1");
  assert.equal(parsed.groups[1].name, "SITES2");
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
    geometryType: "Polygon",
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
  assert.match(prompt, /Never split rows every four points/i);
  assert.match(prompt, /Observation\/sample locations are points/i);
  assert.match(prompt, /Do not guess BFTM, UTM, GK/i);
  assert.match(prompt, /Do not use markdown fences/i);
}

console.log("agentic-coordinate-recognition-v1-regression: PASS");

