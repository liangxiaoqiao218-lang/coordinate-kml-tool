import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync, spawnSync } from "node:child_process";
import { verifyDeploymentIdentity } from "../server/release-identity/index.js";

const baseIdentity = Object.freeze({
  schemaVersion: "release_identity_v2",
  identityStatus: "complete",
  commit: "095168fc44b697ab11f8e4af2277704e9bd240cc",
  branch: "v2/utm-intent-router",
  buildTime: "2026-08-08T00:00:00Z",
  environment: "secondary",
  deployTarget: "coordinate-kml-tool-rc.onrender.com",
  releaseIdentity: Object.freeze({
    releaseVersion: "coordinate-engine-v2-rc1",
    artifactHash: `sha256:${"a".repeat(64)}`,
    buildId: "build-123",
    deploymentId: "deploy-123",
    deploymentTime: "2026-08-08T00:30:00Z"
  }),
  missingFields: Object.freeze([]),
  invalidFields: Object.freeze([])
});

function withChange(identity, patch) {
  return {
    ...identity,
    ...patch,
    releaseIdentity: {
      ...identity.releaseIdentity,
      ...(patch.releaseIdentity ?? {})
    }
  };
}

const cases = [
  {
    name: "complete matching deployment identity returns MATCH",
    run() {
      const result = verifyDeploymentIdentity(baseIdentity, baseIdentity);
      assert.equal(result.status, "MATCH");
      assert.deepEqual(result.mismatches, []);
      assert.deepEqual(result.missingFields, []);
      assert.equal(result.comparedFields.includes("artifactHash"), true);
    }
  },
  {
    name: "commit mismatch returns DEPLOYMENT_IDENTITY_MISMATCH",
    run() {
      const result = verifyDeploymentIdentity(baseIdentity, withChange(baseIdentity, {
        commit: "f".repeat(40)
      }));
      assert.equal(result.status, "DEPLOYMENT_IDENTITY_MISMATCH");
      assert.deepEqual(result.mismatches, ["commit"]);
    }
  },
  {
    name: "branch mismatch returns DEPLOYMENT_IDENTITY_MISMATCH",
    run() {
      const result = verifyDeploymentIdentity(baseIdentity, withChange(baseIdentity, {
        branch: "main"
      }));
      assert.equal(result.status, "DEPLOYMENT_IDENTITY_MISMATCH");
      assert.deepEqual(result.mismatches, ["branch"]);
    }
  },
  {
    name: "environment mismatch returns DEPLOYMENT_IDENTITY_MISMATCH",
    run() {
      const result = verifyDeploymentIdentity(baseIdentity, withChange(baseIdentity, {
        environment: "staging"
      }));
      assert.equal(result.status, "DEPLOYMENT_IDENTITY_MISMATCH");
      assert.deepEqual(result.mismatches, ["environment"]);
    }
  },
  {
    name: "deploy target mismatch returns DEPLOYMENT_IDENTITY_MISMATCH",
    run() {
      const result = verifyDeploymentIdentity(baseIdentity, withChange(baseIdentity, {
        deployTarget: "coordinate-kml-tool.onrender.com"
      }));
      assert.equal(result.status, "DEPLOYMENT_IDENTITY_MISMATCH");
      assert.deepEqual(result.mismatches, ["deployTarget"]);
    }
  },
  {
    name: "deploy target mismatch with Render external hostname is detected",
    run() {
      const runtime = withChange(baseIdentity, {
        externalHostname: "coordinate-kml-tool.onrender.com"
      });
      const result = verifyDeploymentIdentity(baseIdentity, runtime);
      assert.equal(result.status, "DEPLOYMENT_IDENTITY_MISMATCH");
      assert.equal(result.mismatches.includes("externalHostname"), true);
      assert.equal(result.comparedFields.includes("externalHostname"), true);
    }
  },
  {
    name: "artifact hash mismatch is compared when both sides exist",
    run() {
      const result = verifyDeploymentIdentity(baseIdentity, withChange(baseIdentity, {
        releaseIdentity: {
          artifactHash: `sha256:${"b".repeat(64)}`
        }
      }));
      assert.equal(result.status, "DEPLOYMENT_IDENTITY_MISMATCH");
      assert.deepEqual(result.mismatches, ["artifactHash"]);
    }
  },
  {
    name: "missing runtime required field returns IDENTITY_INCOMPLETE",
    run() {
      const result = verifyDeploymentIdentity(baseIdentity, withChange(baseIdentity, {
        deployTarget: null,
        identityStatus: "incomplete"
      }));
      assert.equal(result.status, "IDENTITY_INCOMPLETE");
      assert.equal(result.missingFields.includes("runtime.deployTarget"), true);
    }
  },
  {
    name: "missing expected required field returns IDENTITY_INCOMPLETE",
    run() {
      const result = verifyDeploymentIdentity(withChange(baseIdentity, {
        commit: null,
        identityStatus: "incomplete"
      }), baseIdentity);
      assert.equal(result.status, "IDENTITY_INCOMPLETE");
      assert.equal(result.missingFields.includes("expected.commit"), true);
    }
  },
  {
    name: "CLI helper returns MATCH for matching identity files",
    run() {
      const tempDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "deployment-identity-regression-"));
      try {
        const expectedPath = path.join(tempDirectory, "expected.json");
        const runtimePath = path.join(tempDirectory, "runtime.json");
        fs.writeFileSync(expectedPath, JSON.stringify(baseIdentity), "utf8");
        fs.writeFileSync(runtimePath, JSON.stringify(baseIdentity), "utf8");
        const output = execFileSync("node", [
          "scripts/verify-deployment-identity.js",
          "--expected",
          expectedPath,
          "--runtime",
          runtimePath
        ], {
          encoding: "utf8"
        });
        assert.equal(JSON.parse(output).status, "MATCH");
      } finally {
        fs.rmSync(tempDirectory, { recursive: true, force: true });
      }
    }
  },
  {
    name: "CLI helper exits non-zero for commit mismatch",
    run() {
      const tempDirectory = fs.mkdtempSync(path.join(os.tmpdir(), "deployment-identity-regression-"));
      try {
        const expectedPath = path.join(tempDirectory, "expected.json");
        const runtimePath = path.join(tempDirectory, "runtime.json");
        fs.writeFileSync(expectedPath, JSON.stringify(baseIdentity), "utf8");
        fs.writeFileSync(runtimePath, JSON.stringify(withChange(baseIdentity, {
          commit: "f".repeat(40)
        })), "utf8");
        const result = spawnSync("node", [
          "scripts/verify-deployment-identity.js",
          "--expected",
          expectedPath,
          "--runtime",
          runtimePath
        ], {
          encoding: "utf8"
        });
        assert.notEqual(result.status, 0);
        assert.equal(JSON.parse(result.stdout).status, "DEPLOYMENT_IDENTITY_MISMATCH");
      } finally {
        fs.rmSync(tempDirectory, { recursive: true, force: true });
      }
    }
  }
];

let passed = 0;
for (const testCase of cases) {
  testCase.run();
  passed += 1;
  console.log(`PASS: ${testCase.name}`);
}

console.log(`Deployment Identity Verification Regression: ${passed}/${cases.length} PASS`);
