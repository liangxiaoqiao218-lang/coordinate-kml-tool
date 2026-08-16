import assert from "node:assert/strict";
import fs from "node:fs";

const indexHtml = fs.readFileSync("index.html", "utf8");

function extractFunctionBody(source, functionName) {
  const marker = `function ${functionName}(`;
  const start = source.indexOf(marker);
  assert.notEqual(start, -1, `${functionName} must exist`);

  const signatureEnd = source.indexOf(") {", start);
  assert.notEqual(signatureEnd, -1, `${functionName} must have a signature`);
  const openBrace = source.indexOf("{", signatureEnd);
  assert.notEqual(openBrace, -1, `${functionName} must have a body`);

  let depth = 0;
  for (let index = openBrace; index < source.length; index += 1) {
    const char = source[index];
    if (char === "{") depth += 1;
    if (char === "}") {
      depth -= 1;
      if (depth === 0) return source.slice(openBrace + 1, index);
    }
  }

  throw new Error(`${functionName} body is not closed`);
}

const downloadKmlBody = extractFunctionBody(indexHtml, "downloadKmlInternal");
const refreshButtonBody = extractFunctionBody(indexHtml, "refreshKmlButtonState");
const warningStateBody = extractFunctionBody(indexHtml, "getKmlWarningOnlyExportState");
const technicalPredicateBody = extractFunctionBody(indexHtml, "canTechnicallyGenerateKml");
const overrideBody = extractFunctionBody(indexHtml, "recordKmlWarningOverride");
const renderPanelBody = extractFunctionBody(indexHtml, "renderCrsConfirmationPanel");

function test(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (error) {
    console.error(`FAIL ${name}`);
    console.error(error);
    process.exitCode = 1;
  }
}

test("technical export predicate exists and checks geometry only", () => {
  assert.match(indexHtml, /function canTechnicallyGenerateKml\(\)/);
  assert.match(technicalPredicateBody, /NO_COORDINATES/);
  assert.match(technicalPredicateBody, /UNPARSABLE_COORDINATE_STRUCTURE/);
  assert.match(technicalPredicateBody, /NO_TRANSFORM_AVAILABLE_FOR_PROJECTED_COORDINATES/);
  assert.doesNotMatch(technicalPredicateBody, /confirmationStatus|qualityGateStatus|requires_review|BLOCKED_REVIEW/);
});

test("verified UTM can initialize KML source without server confirmation", () => {
  const verifiedBody = extractFunctionBody(indexHtml, "isVerifiedUtmAutoExportResult");
  assert.match(verifiedBody, /qualityGateStatus === "passed"/);
  assert.match(verifiedBody, /structuredUtmTable\?\.accepted === true/);
  assert.match(verifiedBody, /String\(coordinateResult\.state \|\| ""\)\.trim\(\) === "AUTO_EXPORT"/);
});

test("review and high-risk states use warning-only button label", () => {
  assert.match(refreshButtonBody, /warningOnly/);
  assert.match(refreshButtonBody, /仍按当前结果生成 KML/);
  assert.match(refreshButtonBody, /dataset\.reviewBlocked = "false"/);
});

test("download preserves account and quota permission hard stop", () => {
  assert.match(downloadKmlBody, /canUse\("kmlExportEnabled"/);
  assert.match(downloadKmlBody, /showKmlPermissionFailure/);
  assert.match(downloadKmlBody, /return/);
});

test("handwritten review does not hard block export", () => {
  assert.match(warningStateBody, /HANDWRITTEN_REVIEW/);
  assert.doesNotMatch(downloadKmlBody, /KML_REVIEW_REQUIRED/);
});

test("UTM review and mismatch do not hard block export", () => {
  assert.match(warningStateBody, /UTM_REVIEW_WARNING/);
  assert.doesNotMatch(downloadKmlBody, /KML_UTM_REVIEW_REQUIRED/);
});

test("possible swapped lat lon does not hard block export", () => {
  assert.match(warningStateBody, /POSSIBLE_SWAPPED_LAT_LON/);
  assert.match(downloadKmlBody, /setCoordinateOrderReviewVisible\(true\)/);
  assert.doesNotMatch(downloadKmlBody, /KML_COORDINATE_ORDER_REVIEW_REQUIRED/);
});

test("CRS uncertainty does not block when projected transform is available", () => {
  assert.match(downloadKmlBody, /tryPrepareProjectedKmlSourceFromAvailableCrs\(\)/);
  assert.match(downloadKmlBody, /recordKmlWarningOverride\(getKmlWarningOnlyExportState\(\)\)/);
});

test("projected coordinates without transform remain hard stopped", () => {
  assert.match(downloadKmlBody, /NO_TRANSFORM_AVAILABLE_FOR_PROJECTED_COORDINATES/);
  assert.match(downloadKmlBody, /缺少坐标转换信息/);
});

test("no coordinates still hard stop", () => {
  assert.match(downloadKmlBody, /KML_INSUFFICIENT_POINTS/);
  assert.match(downloadKmlBody, /没有可生成的坐标/);
});

test("user override records warning acknowledgement locally", () => {
  assert.match(indexHtml, /let lastKmlWarningOverride = null/);
  assert.match(overrideBody, /userOverride: true/);
  assert.match(overrideBody, /warningAcknowledged: true/);
  assert.match(overrideBody, /kml_warning_override/);
});

test("suspected point warning is sanitized and user-facing", () => {
  const suspectedBody = extractFunctionBody(indexHtml, "getSanitizedSuspectedPointWarnings");
  assert.match(suspectedBody, /point/);
  assert.match(suspectedBody, /suspectedField/);
  assert.match(suspectedBody, /currentValue/);
  assert.match(suspectedBody, /与参考经纬度转换结果不一致/);
  assert.doesNotMatch(suspectedBody, /raw|prompt|model|base64|credential|secret/i);
});

test("review panel exposes edit and override actions", () => {
  assert.match(renderPanelBody, /修改异常坐标/);
  assert.match(renderPanelBody, /仍按当前结果生成 KML/);
  assert.match(renderPanelBody, /downloadKml/);
  assert.match(renderPanelBody, /当前值/);
  assert.match(renderPanelBody, /与图中参考坐标校验不一致/);
  assert.doesNotMatch(renderPanelBody, /参考纬度|参考经度|最大差异|重新验证/);
});

test("detected verified flow presents direct KML action", () => {
  assert.match(renderPanelBody, /生成 KML/);
  assert.match(renderPanelBody, /confirmButton\.addEventListener\("click", downloadKml\)/);
});

test("edit clears stale warning override", () => {
  const editBody = extractFunctionBody(indexHtml, "markCoordinateTextChanged");
  assert.match(editBody, /lastKmlWarningOverride = null/);
});

test("confirmation infrastructure remains compatible but not required for main export", () => {
  assert.match(indexHtml, /\/api\/confirm-coordinate-result/);
  assert.match(indexHtml, /async function confirmCrsIntent/);
  assert.doesNotMatch(downloadKmlBody, /confirm-coordinate-result/);
});

test("security-sensitive payloads are not introduced", () => {
  const addedSurface = [
    technicalPredicateBody,
    warningStateBody,
    overrideBody,
    renderPanelBody
  ].join("\n");
  assert.doesNotMatch(addedSurface, /api[_-]?key|credential|secret|base64|raw prompt|model response|raw ocr/i);
});

if (process.exitCode) {
  process.exit(process.exitCode);
}

console.log("KML Warning-only Export Regression: 17/17 PASS");
