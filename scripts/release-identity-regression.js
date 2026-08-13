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
const branch = "v2/utm-intent-router";
const artifactHash = `sha256:${"a".repeat(64)}`;
const validManifest = {
  releaseVersion: "coordinate-engine-v2-rc1",
  commit,
  branch,
  artifactHash,
  buildTime: "2026-08-08T00:00:00Z",
  buildId: "build-123"
};
const validDeployment = {
  RELEASE_IDENTITY_ENVIRONMENT: "secondary",
  RELEASE_IDENTITY_DEPLOY_TARGET: "coordinate-kml-tool-rc.onrender.com",
  RELEASE_IDENTITY_DEPLOYMENT_ID: "deploy-123",
  RELEASE_IDENTITY_DEPLOYMENT_TIME: "2026-08-08T00:30:00Z",
  RENDER_EXTERNAL_HOSTNAME: "coordinate-kml-tool-rc.onrender.com"
};
const temporaryDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "release-identity-regression-"));
const manifestPath = path.join(temporaryDirectory, "release-manifest.json");

function writeManifest(manifest) {
  fs.writeFileSync(manifestPath, JSON.stringify(manifest), "utf8");
}

const cases = [
  {
    name: "complete identity returns release_identity_v2 response shape",
    run() {
      writeManifest(validManifest);
      const result = buildReleaseIdentity({
        buildIdentity: loadBuildIdentityManifest(manifestPath),
        deploymentSource: validDeployment
      });
      assert.equal(result.schemaVersion, "release_identity_v2");
      assert.equal(result.identityStatus, "complete");
      assert.equal(result.commit, commit);
      assert.equal(result.branch, branch);
      assert.equal(result.buildTime, validManifest.buildTime);
      assert.equal(result.environment, "secondary");
      assert.equal(result.deployTarget, "coordinate-kml-tool-rc.onrender.com");
      assert.deepEqual(result.missingFields, []);
      assert.deepEqual(result.invalidFields, []);
      assert.equal(result.releaseIdentity.releaseVersion, validManifest.releaseVersion);
      assert.equal(result.releaseIdentity.artifactHash, artifactHash);
      assert.equal(result.releaseIdentity.buildId, "build-123");
      assert.equal(result.releaseIdentity.deploymentId, "deploy-123");
      assert.equal(result.releaseIdentity.deploymentTime, validDeployment.RELEASE_IDENTITY_DEPLOYMENT_TIME);
      assert.equal(result.releaseIdentity.deploymentTimeSource, "RELEASE_IDENTITY_DEPLOYMENT_TIME");
      assert.equal(result.identitySources.commit, "release_manifest.commit");
      assert.equal(result.identitySources.branch, "release_manifest.branch");
    }
  },
  {
    name: "legacy version compatibility remains additive",
    run() {
      writeManifest(validManifest);
      const result = buildVersionResponse("2026-05-01-quota-contact-v2", {
        buildIdentity: loadBuildIdentityManifest(manifestPath),
        deploymentSource: validDeployment
      });
      assert.equal(result.version, "2026-05-01-quota-contact-v2");
      assert.equal(result.schemaVersion, "release_identity_v2");
      assert.equal(result.identityStatus, "complete");
      assert.equal(result.commit, commit);
    }
  },
  {
    name: "deployment time is optional when platform timestamp is unavailable",
    run() {
      writeManifest(validManifest);
      const {
        RELEASE_IDENTITY_DEPLOYMENT_TIME: _deploymentTime,
        ...deploymentWithoutTime
      } = validDeployment;
      const result = buildReleaseIdentity({
        buildIdentity: loadBuildIdentityManifest(manifestPath),
        deploymentSource: deploymentWithoutTime
      });
      assert.equal(result.identityStatus, "complete");
      assert.equal(result.releaseIdentity.deploymentTime, null);
      assert.equal(result.releaseIdentity.deploymentTimeSource, "not_provided");
      assert.deepEqual(result.missingFields, []);
      assert.deepEqual(result.invalidFields, []);
    }
  },
  {
    name: "invalid deployment time is rejected when provided",
    run() {
      writeManifest(validManifest);
      const result = buildReleaseIdentity({
        buildIdentity: loadBuildIdentityManifest(manifestPath),
        deploymentSource: {
          ...validDeployment,
          RELEASE_IDENTITY_DEPLOYMENT_TIME: "2026-08-08 00:30:00Z"
        }
      });
      assert.equal(result.identityStatus, "incomplete");
      assert.equal(result.releaseIdentity.deploymentTime, null);
      assert.equal(result.invalidFields.includes("deploymentTime"), true);
    }
  },
  {
    name: "missing commit remains explicitly incomplete",
    run() {
      writeManifest({
        ...validManifest,
        commit: undefined
      });
      const result = buildReleaseIdentity({
        buildIdentity: loadBuildIdentityManifest(manifestPath),
        deploymentSource: validDeployment
      });
      assert.equal(result.identityStatus, "incomplete");
      assert.equal(result.commit, null);
      assert.equal(result.missingFields.includes("commit"), true);
    }
  },
  {
    name: "invalid short commit is rejected",
    run() {
      writeManifest({
        ...validManifest,
        commit: "abc123"
      });
      const result = buildReleaseIdentity({
        buildIdentity: loadBuildIdentityManifest(manifestPath),
        deploymentSource: validDeployment
      });
      assert.equal(result.identityStatus, "incomplete");
      assert.equal(result.commit, null);
      assert.equal(result.invalidFields.includes("commit"), true);
    }
  },
  {
    name: "invalid environment is rejected",
    run() {
      writeManifest(validManifest);
      const result = buildReleaseIdentity({
        buildIdentity: loadBuildIdentityManifest(manifestPath),
        deploymentSource: {
          ...validDeployment,
          RELEASE_IDENTITY_ENVIRONMENT: "production"
        }
      });
      assert.equal(result.identityStatus, "incomplete");
      assert.equal(result.environment, null);
      assert.equal(result.invalidFields.includes("environment"), true);
    }
  },
  {
    name: "missing deploy target remains explicitly incomplete",
    run() {
      writeManifest(validManifest);
      const {
        RELEASE_IDENTITY_DEPLOY_TARGET: _deployTarget,
        ...deploymentWithoutTarget
      } = validDeployment;
      const result = buildReleaseIdentity({
        buildIdentity: loadBuildIdentityManifest(manifestPath),
        deploymentSource: deploymentWithoutTarget
      });
      assert.equal(result.identityStatus, "incomplete");
      assert.equal(result.deployTarget, null);
      assert.equal(result.missingFields.includes("deployTarget"), true);
    }
  },
  {
    name: "legacy gitCommit manifest remains accepted for transition but branch is required",
    run() {
      writeManifest({
        releaseVersion: validManifest.releaseVersion,
        gitCommit: validManifest.commit,
        sourceBranch: validManifest.branch,
        artifactHash: validManifest.artifactHash,
        buildTime: validManifest.buildTime
      });
      const result = buildReleaseIdentity({
        buildIdentity: loadBuildIdentityManifest(manifestPath),
        deploymentSource: validDeployment
      });
      assert.equal(result.identityStatus, "complete");
      assert.equal(result.commit, commit);
      assert.equal(result.branch, branch);
    }
  },
  {
    name: "runtime build override attempt is ignored",
    run() {
      writeManifest(validManifest);
      const attackerCommit = "f".repeat(40);
      const result = buildReleaseIdentity({
        buildIdentity: loadBuildIdentityManifest(manifestPath),
        deploymentSource: {
          ...validDeployment,
          RELEASE_IDENTITY_COMMIT: attackerCommit,
          RELEASE_IDENTITY_BRANCH: "evil/main",
          RELEASE_IDENTITY_ARTIFACT_HASH: `sha256:${"f".repeat(64)}`,
          RELEASE_IDENTITY_BUILD_TIME: "2030-01-01T00:00:00Z"
        }
      });
      assert.equal(result.commit, validManifest.commit);
      assert.equal(result.branch, validManifest.branch);
      assert.equal(JSON.stringify(result).includes(attackerCommit), false);
    }
  },
  {
    name: "security response excludes secret-like deployment source values",
    run() {
      writeManifest(validManifest);
      const secretMarkers = [
        "credential-secret-marker",
        "token-secret-marker",
        "database-url-secret-marker",
        "C:\\Users\\Mir-1\\secret"
      ];
      const result = buildReleaseIdentity({
        buildIdentity: loadBuildIdentityManifest(manifestPath),
        deploymentSource: {
          ...validDeployment,
          API_CREDENTIAL: secretMarkers[0],
          ACCESS_TOKEN: secretMarkers[1],
          DATABASE_URL: secretMarkers[2],
          FILESYSTEM_PATH: secretMarkers[3]
        }
      });
      const serialized = JSON.stringify(result);
      for (const marker of secretMarkers) assert.equal(serialized.includes(marker), false);
      assert.equal(/API_CREDENTIAL|ACCESS_TOKEN|DATABASE_URL|FILESYSTEM_PATH/u.test(serialized), false);
    }
  },
  {
    name: "server endpoint uses no-store release identity response builder",
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
