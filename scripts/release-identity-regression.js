import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  buildReleaseIdentity,
  buildVersionResponse,
  loadBuildIdentityManifest
} from "../server/release-identity/index.js";

const commit = "095168fc44b697ab11f8e4af2277704e9bd240cc";
const artifactHash = `sha256:${"a".repeat(64)}`;
const validManifest = {
  releaseVersion: "coordinate-engine-v2-rc1",
  gitCommit: commit,
  artifactHash,
  buildTime: "2026-08-08T00:00:00Z"
};
const validDeployment = {
  RELEASE_IDENTITY_ENVIRONMENT: "secondary",
  RELEASE_IDENTITY_DEPLOYMENT_TIME: "2026-08-08T00:30:00Z"
};
const temporaryDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "release-identity-regression-"));
const manifestPath = path.join(temporaryDirectory, "release-manifest.json");

function writeManifest(manifest) {
  fs.writeFileSync(manifestPath, JSON.stringify(manifest), "utf8");
}

const cases = [
  {
    name: "manifest produces complete immutable build identity",
    run() {
      writeManifest(validManifest);
      const result = loadBuildIdentityManifest(manifestPath);
      assert.deepEqual(result, {
        ...validManifest,
        identityStatus: "complete",
        manifestStatus: "loaded",
        missingFields: [],
        invalidFields: []
      });
      assert.equal(Object.isFrozen(result), true);
      assert.equal(Object.isFrozen(result.missingFields), true);
    }
  },
  {
    name: "runtime build override attempt is ignored",
    run() {
      writeManifest(validManifest);
      const buildIdentity = loadBuildIdentityManifest(manifestPath);
      const attackerCommit = "f".repeat(40);
      const result = buildReleaseIdentity({
        buildIdentity,
        deploymentSource: {
          ...validDeployment,
          RELEASE_IDENTITY_VERSION: "runtime-override",
          RELEASE_IDENTITY_GIT_COMMIT: attackerCommit,
          RELEASE_IDENTITY_ARTIFACT_HASH: `sha256:${"f".repeat(64)}`,
          RELEASE_IDENTITY_BUILD_TIME: "2030-01-01T00:00:00Z"
        }
      });
      assert.equal(result.buildIdentity.releaseVersion, validManifest.releaseVersion);
      assert.equal(result.buildIdentity.gitCommit, validManifest.gitCommit);
      assert.equal(result.buildIdentity.artifactHash, validManifest.artifactHash);
      assert.equal(result.buildIdentity.buildTime, validManifest.buildTime);
      assert.equal(JSON.stringify(result).includes(attackerCommit), false);
    }
  },
  {
    name: "loaded build identity remains stable after manifest changes",
    run() {
      writeManifest(validManifest);
      const buildIdentity = loadBuildIdentityManifest(manifestPath);
      writeManifest({
        ...validManifest,
        gitCommit: "e".repeat(40)
      });
      assert.equal(buildIdentity.gitCommit, validManifest.gitCommit);
    }
  },
  {
    name: "missing manifest remains explicitly incomplete",
    run() {
      const result = loadBuildIdentityManifest(path.join(temporaryDirectory, "missing.json"));
      assert.equal(result.identityStatus, "incomplete");
      assert.equal(result.manifestStatus, "missing");
      assert.deepEqual(result.missingFields, ["releaseVersion", "gitCommit", "artifactHash", "buildTime"]);
    }
  },
  {
    name: "deployment identity uses only approved runtime metadata",
    run() {
      writeManifest(validManifest);
      const secretMarkers = ["vision-secret-marker", "database-secret-marker", "payment-secret-marker"];
      const result = buildReleaseIdentity({
        buildIdentity: loadBuildIdentityManifest(manifestPath),
        deploymentSource: {
          ...validDeployment,
          DASHSCOPE_API_KEY: secretMarkers[0],
          SUPABASE_SERVICE_ROLE_KEY: secretMarkers[1],
          PAYMENT_SECRET: secretMarkers[2]
        }
      });
      assert.equal(result.identityStatus, "complete");
      assert.equal(result.deploymentIdentity.environment, "secondary");
      const serialized = JSON.stringify(result);
      for (const marker of secretMarkers) assert.equal(serialized.includes(marker), false);
    }
  },
  {
    name: "missing deployment metadata remains explicitly incomplete",
    run() {
      writeManifest(validManifest);
      const result = buildReleaseIdentity({
        buildIdentity: loadBuildIdentityManifest(manifestPath),
        deploymentSource: {}
      });
      assert.equal(result.identityStatus, "incomplete");
      assert.equal(result.buildIdentity.identityStatus, "complete");
      assert.equal(result.deploymentIdentity.identityStatus, "incomplete");
      assert.deepEqual(result.deploymentIdentity.missingFields, ["environment", "deploymentTime"]);
    }
  },
  {
    name: "legacy version response remains additive",
    run() {
      writeManifest(validManifest);
      const result = buildVersionResponse("2026-05-01-quota-contact-v2", {
        buildIdentity: loadBuildIdentityManifest(manifestPath),
        deploymentSource: validDeployment
      });
      assert.equal(result.version, "2026-05-01-quota-contact-v2");
      assert.equal(result.releaseIdentity.identityStatus, "complete");
      assert.equal(result.releaseIdentity.buildIdentity.gitCommit, commit);
    }
  },
  {
    name: "server endpoint uses release identity response builder",
    run() {
      const source = fs.readFileSync(new URL("../server.js", import.meta.url), "utf8");
      assert.match(source, /app\.get\("\/api\/version"[\s\S]*buildVersionResponse\(appVersion\)/u);
      assert.match(source, /Cache-Control", "no-store"/u);
    }
  }
];

let passed = 0;
try {
  for (const testCase of cases) {
    testCase.run();
    passed += 1;
    console.log(`PASS: ${testCase.name}`);
  }
} finally {
  fs.rmSync(temporaryDirectory, { recursive: true, force: true });
}

console.log(`Release Identity Regression: ${passed}/${cases.length} PASS`);

