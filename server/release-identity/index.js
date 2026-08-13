import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const RELEASE_IDENTITY_SCHEMA_VERSION = "release_identity_v2";
const BUILD_FIELDS = ["releaseVersion", "commit", "branch", "artifactHash", "buildTime"];
const DEPLOYMENT_FIELDS = ["environment", "deployTarget", "deploymentTime"];
const OPTIONAL_BUILD_FIELDS = ["buildId"];
const OPTIONAL_DEPLOYMENT_FIELDS = ["deploymentId"];
const APPROVED_ENVIRONMENTS = new Set([
  "local",
  "staging",
  "secondary",
  "primary_production"
]);
const APPROVED_DEPLOY_TARGETS = new Set([
  "local",
  "geokitlab.com",
  "coordinate-kml-tool.onrender.com",
  "coordinate-kml-tool-rc.onrender.com",
  "domestic",
  "overseas"
]);
const DEPLOYMENT_METADATA_SOURCES = Object.freeze({
  environment: "RELEASE_IDENTITY_ENVIRONMENT",
  deployTarget: "RELEASE_IDENTITY_DEPLOY_TARGET",
  deploymentId: "RELEASE_IDENTITY_DEPLOYMENT_ID",
  deploymentTime: "RELEASE_IDENTITY_DEPLOYMENT_TIME"
});
const moduleDirectory = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_MANIFEST_PATH = path.resolve(moduleDirectory, "../../release-manifest.json");

function readBoundedString(value, maxLength = 200) {
  const normalized = typeof value === "string" ? value.trim() : "";
  if (!normalized || normalized.length > maxLength || /[\u0000-\u001f\u007f]/u.test(normalized)) {
    return null;
  }
  return normalized;
}

function hasProvidedValue(value) {
  return typeof value === "string" && value.trim().length > 0;
}

function readReleaseVersion(value) {
  const normalized = readBoundedString(value, 100);
  return normalized && /^[A-Za-z0-9][A-Za-z0-9._+-]*$/u.test(normalized) ? normalized : null;
}

function readBranch(value) {
  const normalized = readBoundedString(value, 200);
  return normalized && /^[A-Za-z0-9][A-Za-z0-9._+/-]*$/u.test(normalized) ? normalized : null;
}

function readGitCommit(value) {
  const normalized = readBoundedString(value, 40)?.toLowerCase();
  return normalized && /^[0-9a-f]{40}$/u.test(normalized) ? normalized : null;
}

function readArtifactHash(value) {
  const normalized = readBoundedString(value, 71)?.toLowerCase();
  return normalized && /^sha256:[0-9a-f]{64}$/u.test(normalized) ? normalized : null;
}

function readUtcTimestamp(value) {
  const normalized = readBoundedString(value, 40);
  if (!normalized || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/u.test(normalized)) {
    return null;
  }
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== normalized.slice(0, 10)) {
    return null;
  }
  return normalized;
}

function readEnvironment(value) {
  const normalized = readBoundedString(value, 32)?.toLowerCase();
  return normalized && APPROVED_ENVIRONMENTS.has(normalized) ? normalized : null;
}

function readDeployTarget(value) {
  const normalized = readBoundedString(value, 100)?.toLowerCase();
  return normalized && APPROVED_DEPLOY_TARGETS.has(normalized) ? normalized : null;
}

function readOptionalIdentifier(value, maxLength = 120) {
  const normalized = readBoundedString(value, maxLength);
  return normalized && /^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$/u.test(normalized) ? normalized : null;
}

function freezeIdentity(identity) {
  return Object.freeze({
    ...identity,
    missingFields: Object.freeze([...identity.missingFields]),
    invalidFields: Object.freeze([...identity.invalidFields])
  });
}

function normalizeBuildIdentity(manifest, manifestStatus) {
  const source = manifest && typeof manifest === "object" && !Array.isArray(manifest) ? manifest : {};
  const rawCommit = source.commit ?? source.gitCommit;
  const rawBranch = source.branch ?? source.sourceBranch;
  const identity = {
    releaseVersion: readReleaseVersion(source.releaseVersion),
    commit: readGitCommit(rawCommit),
    branch: readBranch(rawBranch),
    artifactHash: readArtifactHash(source.artifactHash),
    buildTime: readUtcTimestamp(source.buildTime),
    buildId: readOptionalIdentifier(source.buildId)
  };
  const rawValues = {
    releaseVersion: source.releaseVersion,
    commit: rawCommit,
    branch: rawBranch,
    artifactHash: source.artifactHash,
    buildTime: source.buildTime,
    buildId: source.buildId
  };
  const missingFields = BUILD_FIELDS.filter(field => !hasProvidedValue(rawValues[field]));
  const invalidFields = [...BUILD_FIELDS, ...OPTIONAL_BUILD_FIELDS]
    .filter(field => hasProvidedValue(rawValues[field]) && identity[field] === null);

  return freezeIdentity({
    ...identity,
    identityStatus: manifestStatus === "loaded" && missingFields.length === 0 && invalidFields.length === 0
      ? "complete"
      : "incomplete",
    manifestStatus,
    missingFields,
    invalidFields
  });
}

export function loadBuildIdentityManifest(manifestPath = DEFAULT_MANIFEST_PATH) {
  try {
    const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
    return normalizeBuildIdentity(manifest, "loaded");
  } catch (error) {
    return normalizeBuildIdentity({}, error?.code === "ENOENT" ? "missing" : "invalid");
  }
}

export function buildDeploymentIdentity(source = process.env) {
  const rawEnvironment = source?.[DEPLOYMENT_METADATA_SOURCES.environment];
  const rawDeployTarget = source?.[DEPLOYMENT_METADATA_SOURCES.deployTarget];
  const rawDeploymentTime = source?.[DEPLOYMENT_METADATA_SOURCES.deploymentTime];
  const rawDeploymentId = source?.[DEPLOYMENT_METADATA_SOURCES.deploymentId];
  const identity = {
    environment: readEnvironment(rawEnvironment),
    deployTarget: readDeployTarget(rawDeployTarget),
    deploymentTime: readUtcTimestamp(rawDeploymentTime),
    deploymentId: readOptionalIdentifier(rawDeploymentId)
  };
  const rawValues = {
    environment: rawEnvironment,
    deployTarget: rawDeployTarget,
    deploymentTime: rawDeploymentTime,
    deploymentId: rawDeploymentId
  };
  const missingFields = DEPLOYMENT_FIELDS.filter(field => !hasProvidedValue(rawValues[field]));
  const invalidFields = [...DEPLOYMENT_FIELDS, ...OPTIONAL_DEPLOYMENT_FIELDS]
    .filter(field => hasProvidedValue(rawValues[field]) && identity[field] === null);

  return freezeIdentity({
    ...identity,
    identityStatus: missingFields.length === 0 && invalidFields.length === 0 ? "complete" : "incomplete",
    missingFields,
    invalidFields
  });
}

// Loaded once at process initialization. Runtime configuration cannot replace
// or mutate these build identity values after the artifact starts.
const runtimeBuildIdentity = loadBuildIdentityManifest();

export function buildReleaseIdentity({
  buildIdentity = runtimeBuildIdentity,
  deploymentSource = process.env
} = {}) {
  const deploymentIdentity = buildDeploymentIdentity(deploymentSource);
  const missingFields = [
    ...buildIdentity.missingFields,
    ...deploymentIdentity.missingFields
  ];
  const invalidFields = [
    ...buildIdentity.invalidFields,
    ...deploymentIdentity.invalidFields
  ];
  return Object.freeze({
    schemaVersion: RELEASE_IDENTITY_SCHEMA_VERSION,
    identityStatus: buildIdentity.identityStatus === "complete" && deploymentIdentity.identityStatus === "complete"
      ? "complete"
      : "incomplete",
    commit: buildIdentity.commit,
    branch: buildIdentity.branch,
    buildTime: buildIdentity.buildTime,
    environment: deploymentIdentity.environment,
    deployTarget: deploymentIdentity.deployTarget,
    releaseIdentity: Object.freeze({
      releaseVersion: buildIdentity.releaseVersion,
      artifactHash: buildIdentity.artifactHash,
      buildId: buildIdentity.buildId,
      deploymentId: deploymentIdentity.deploymentId,
      deploymentTime: deploymentIdentity.deploymentTime
    }),
    missingFields: Object.freeze(missingFields),
    invalidFields: Object.freeze(invalidFields)
  });
}

export function buildVersionResponse(version, options) {
  return {
    version,
    ...buildReleaseIdentity(options)
  };
}

function identityHasRequiredFields(identity) {
  return Boolean(
    identity
    && identity.identityStatus === "complete"
    && identity.commit
    && identity.branch
    && identity.environment
    && identity.deployTarget
  );
}

export function verifyDeploymentIdentity(expectedIdentity, runtimeIdentity) {
  const expected = expectedIdentity && typeof expectedIdentity === "object" ? expectedIdentity : {};
  const runtime = runtimeIdentity && typeof runtimeIdentity === "object" ? runtimeIdentity : {};
  const comparedFields = ["commit", "branch", "environment", "deployTarget"];
  const mismatches = [];
  const missingFields = [];

  if (!identityHasRequiredFields(expected)) {
    for (const field of comparedFields) {
      if (!expected[field]) missingFields.push(`expected.${field}`);
    }
  }
  if (!identityHasRequiredFields(runtime)) {
    for (const field of comparedFields) {
      if (!runtime[field]) missingFields.push(`runtime.${field}`);
    }
  }

  const expectedArtifactHash = expected.releaseIdentity?.artifactHash;
  const runtimeArtifactHash = runtime.releaseIdentity?.artifactHash;
  for (const field of comparedFields) {
    if (expected[field] && runtime[field] && expected[field] !== runtime[field]) {
      mismatches.push(field);
    }
  }
  if (expectedArtifactHash && runtimeArtifactHash && expectedArtifactHash !== runtimeArtifactHash) {
    mismatches.push("artifactHash");
  }

  const status = missingFields.length > 0
    ? "IDENTITY_INCOMPLETE"
    : mismatches.length > 0
      ? "DEPLOYMENT_IDENTITY_MISMATCH"
      : "MATCH";

  return Object.freeze({
    schemaVersion: "deployment_identity_verification_v1",
    status,
    comparedFields: Object.freeze([
      ...comparedFields,
      ...(expectedArtifactHash && runtimeArtifactHash ? ["artifactHash"] : [])
    ]),
    mismatches: Object.freeze(mismatches),
    missingFields: Object.freeze(missingFields)
  });
}

export const releaseIdentityManifestPath = DEFAULT_MANIFEST_PATH;
export const releaseIdentityDeploymentEnvironmentVariables = DEPLOYMENT_METADATA_SOURCES;
