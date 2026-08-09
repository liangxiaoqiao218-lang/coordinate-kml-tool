import fs from "node:fs";
import assert from "node:assert/strict";

const indexHtml = fs.readFileSync("index.html", "utf8");
const serverJs = fs.readFileSync("server.js", "utf8");

function extractFunctionBody(source, functionName) {
  const marker = `function ${functionName}(`;
  const start = source.indexOf(marker);
  assert.notEqual(start, -1, `${functionName} must exist`);

  const openBrace = source.indexOf("{", start);
  assert.notEqual(openBrace, -1, `${functionName} must have a body`);

  let depth = 0;
  for (let index = openBrace; index < source.length; index += 1) {
    const char = source[index];
    if (char === "{") depth += 1;
    if (char === "}") {
      depth -= 1;
      if (depth === 0) {
        return source.slice(openBrace + 1, index);
      }
    }
  }

  throw new Error(`${functionName} body is not closed`);
}

const downloadKmlBody = extractFunctionBody(indexHtml, "downloadKmlInternal");
const markCoordinateTextChangedBody = extractFunctionBody(indexHtml, "markCoordinateTextChanged");

const checks = [
  {
    name: "KML export does not consume convert quota",
    run: () => assert.equal(downloadKmlBody.includes('consumeUsage("convert")'), false)
  },
  {
    name: "KML export still checks CRS confirmation gate",
    run: () => assert.match(downloadKmlBody, /requiresCrsConfirmationForCurrentInput\(\)/)
  },
  {
    name: "KML export still checks permission gate",
    run: () => assert.match(downloadKmlBody, /canUse\("kmlExportEnabled"/)
  },
  {
    name: "Recognition session state exists in frontend",
    run: () => {
      assert.match(indexHtml, /let currentRecognitionSession = loadRecognitionSession\(\)/);
      assert.match(indexHtml, /function normalizeRecognitionSession/);
      assert.match(indexHtml, /function saveRecognitionSession/);
    }
  },
  {
    name: "CRS confirmation is bound to recognition session",
    run: () => {
      assert.match(indexHtml, /recognitionSessionId: currentRecognitionSession\?\.id/);
      assert.match(indexHtml, /function isCurrentCrsConfirmationSession/);
      assert.match(downloadKmlBody, /KML_CRS_CONFIRMATION_STALE/);
    }
  },
  {
    name: "Coordinate edits invalidate CRS confirmation but preserve recognition session",
    run: () => {
      assert.match(markCoordinateTextChangedBody, /activeCrsConfirmationState = createCrsConfirmationState\(\)/);
      assert.equal(markCoordinateTextChangedBody.includes("saveRecognitionSession(null)"), false);
    }
  },
  {
    name: "New upload clears stale recognition session",
    run: () => assert.match(indexHtml, /activeCoordinateArbitration = null;\s*saveRecognitionSession\(null\);\s*resetHandwrittenDmsReviewState\(\);/s)
  },
  {
    name: "Server creates immutable per-recognition session id",
    run: () => {
      assert.match(serverJs, /function createRecognitionSessionId\(\)/);
      assert.match(serverJs, /crypto\.randomUUID\(\)/);
      assert.match(serverJs, /const recognitionSessionId = createRecognitionSessionId\(\);/);
    }
  },
  {
    name: "Server attaches recognition session quota metadata to success payloads",
    run: () => {
      assert.match(serverJs, /function attachRecognitionSessionMetadata/);
      assert.match(serverJs, /quotaChargeType: "convert"/);
      assert.match(serverJs, /quotaChargeReason: "coordinate_recognition"/);
      const attachCount = (serverJs.match(/attachRecognitionSessionMetadata/g) || []).length;
      assert.ok(attachCount >= 8, `expected recognition session metadata on success exits, got ${attachCount}`);
    }
  }
];

let passed = 0;

for (const check of checks) {
  try {
    check.run();
    passed += 1;
    console.log(`PASS ${check.name}`);
  } catch (error) {
    console.error(`FAIL ${check.name}`);
    console.error(error.message);
    process.exitCode = 1;
    break;
  }
}

if (!process.exitCode) {
  console.log(`Quota/KML Transaction Regression: ${passed}/${checks.length} PASS`);
}
