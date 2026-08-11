import assert from "node:assert/strict";
import test from "node:test";
import {
  CADASTRAL_SEMANTIC_VISION_SCHEMA_VERSION,
  parseCadastralSemanticVisionOutput
} from "../server/coordinate-evidence/index.js";

test("complete num XV YV Liste_Carres header is detected", () => {
  const result = parseCadastralSemanticVisionOutput(`
Visible cadastral table headers:
num
XV
YV
Liste_Carrés
`);

  assert.equal(result.schemaVersion, CADASTRAL_SEMANTIC_VISION_SCHEMA_VERSION);
  assert.equal(result.status, "observed");
  assert.equal(result.detected, true);
  assert.equal(result.tableType, "num_xv_yv");
  assert.deepEqual(result.indicators, ["num", "XV", "YV"]);
  assert.equal(result.layoutHints.hasListeCarres, true);
  assert.equal(result.layoutHints.hasTableStructure, true);
  assert.equal(result.confidence, "high");
});

test("French Liste Carres with numero sign XV YV is detected", () => {
  const result = parseCadastralSemanticVisionOutput(`
Liste Carres
№
X V
Y V
`);

  assert.equal(result.status, "observed");
  assert.equal(result.detected, true);
  assert.equal(result.tableType, "num_xv_yv");
  assert.deepEqual(result.indicators, ["num", "XV", "YV"]);
  assert.equal(result.layoutHints.hasListeCarres, true);
});

test("JSON string semantic output is normalized", () => {
  const result = parseCadastralSemanticVisionOutput(`
{
  "status": "observed",
  "headers": ["Number", "XV", "YV"],
  "caption": "cadastral grid"
}
`);

  assert.equal(result.status, "observed");
  assert.equal(result.detected, true);
  assert.equal(result.layoutHints.hasCadastralGrid, true);
  assert.deepEqual(result.indicators, ["num", "XV", "YV"]);
});

test("object semantic output is normalized without exposing source text", () => {
  const result = parseCadastralSemanticVisionOutput({
    tableType: "cadastral",
    observations: [
      { text: "No." },
      { text: "XV" },
      { text: "YV" }
    ],
    layout: "mineral cadastral grid"
  });

  assert.equal(result.status, "observed");
  assert.equal(result.detected, true);
  assert.equal(result.tableType, "num_xv_yv");
  assert.equal("observations" in result, false);
  assert.equal("sourceText" in result, false);
});

test("projected coordinate rows only are not detected", () => {
  const result = parseCadastralSemanticVisionOutput(`
292812.5
360937.5
294062.5
367187.5
`);

  assert.equal(result.status, "not_detected");
  assert.equal(result.detected, false);
  assert.equal(result.tableType, "unknown");
  assert.deepEqual(result.indicators, []);
});

test("normal UTM CRS text is not detected as cadastral semantic", () => {
  const result = parseCadastralSemanticVisionOutput(`
UTM
WGS84
Zone 50S
EPSG:32750
`);

  assert.equal(result.status, "not_detected");
  assert.equal(result.detected, false);
  assert.equal(result.layoutHints.hasTableStructure, false);
});

test("parser rejects invalid inputs with invalid status", () => {
  const result = parseCadastralSemanticVisionOutput(null);

  assert.equal(result.status, "invalid");
  assert.equal(result.detected, false);
  assert.equal(result.reason, "invalid_input");
});

test("parser output is semantic-only and does not expose raw OCR, prompts, model responses, credentials, or image data", () => {
  const result = parseCadastralSemanticVisionOutput({
    headers: ["num", "XV", "YV"],
    caption: "Liste_Carrés",
    rawOcr: "num | XV | YV | 280 | 292812.5 | 360937.5",
    prompt: "read this cadastral image",
    modelResponse: "full model response",
    token: "abc",
    Authorization: "Bearer abc.def",
    image: "base64data",
    buffer: "binary"
  });
  const serialized = JSON.stringify(result);

  assert.equal(result.status, "observed");
  assert.equal(result.detected, true);
  assert.doesNotMatch(serialized, /292812\.5|360937\.5|read this cadastral image|full model response|Bearer abc\.def|base64data|binary/i);
  assert.doesNotMatch(serialized, /rawOcr|prompt|modelResponse|Authorization|token|image|buffer/i);
  assert.equal("coordinates" in result, false);
  assert.equal("coordinateResult" in result, false);
  assert.equal("coordinateEvidenceCandidates" in result, false);
});

test("parser output remains shadow-only and does not include legacy state fields", () => {
  const result = parseCadastralSemanticVisionOutput("num\nXV\nYV\nListe_Carrés");

  assert.equal(result.affectsLegacyWinner, false);
  assert.equal(result.affectsCoordinateResult, false);
  assert.equal(result.affectsKml, false);
  assert.equal("coordinateType" in result, false);
  assert.equal("precisionMode" in result, false);
  assert.equal("coordinateResult" in result, false);
  assert.equal("kml_ready" in result, false);
});
