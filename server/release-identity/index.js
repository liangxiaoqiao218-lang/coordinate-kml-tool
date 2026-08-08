import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const RELEASE_IDENTITY_SCHEMA_VERSION = "release_identity_v1";
const BUILD_FIELDS = ["releaseVersion", "gitCommit", "artifactHash", "buildTime"];
const DEPLOYMENT_FIELDS = ["environment", "deploymentTime"];
const APPROVED_ENVIRONMENTS = new Set([
  "development",
  "staging",
  "secondary",
  "primary"
]);
const DEPLOYMENT_METADATA_SOURCES = Object.freeze({
  environment: "RELEASE_IDENTITY_ENVIRONMENT",
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

function freezeIdentity(identity) {
  return Object.freeze({
    ...identity,
    missingFields: Object.freeze([...identity.missingFields]),
    invalidFields: Object.freeze([...identity.invalidFields])
  });
}

function normalizeBuildIdentity(manifest, manifestStatus) {
  const source = manifest && typeof manifest === "object" && !Array.isArray(manifest) ? manifest : {};
  const identity = {
    releaseVersion: readReleaseVersion(source.releaseVersion),
    gitCommit: readGitCommit(source.gitCommit),
    artifactHash: readArtifactHash(source.artifactHash),
    buildTime: readUtcTimestamp(source.buildTime)
  };
  const missingFields = BUILD_FIELDS.filter(field => !hasProvidedValue(source[field]));
  const invalidFields = BUILD_FIELDS.filter(field => hasProvidedValue(source[field]) && identity[field] === null);

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
  const rawDeploymentTime = source?.[DEPLOYMENT_METADATA_SOURCES.deploymentTime];
  const normalizedEnvironment = readBoundedString(rawEnvironment, 32)?.toLowerCase();
  const identity = {
    environment: normalizedEnvironment && APPROVED_ENVIRONMENTS.has(normalizedEnvironment)
      ? normalizedEnvironment
      : null,
    deploymentTime: readUtcTimestamp(rawDeploymentTime)
  };
  const rawValues = { environment: rawEnvironment, deploymentTime: rawDeploymentTime };
  const missingFields = DEPLOYMENT_FIELDS.filter(field => !hasProvidedValue(rawValues[field]));
  const invalidFields = DEPLOYMENT_FIELDS.filter(field => hasProvidedValue(rawValues[field]) && identity[field] === null);

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
  return Object.freeze({
    schemaVersion: RELEASE_IDENTITY_SCHEMA_VERSION,
    buildIdentity,
    deploymentIdentity,
    identityStatus: buildIdentity.identityStatus === "complete" && deploymentIdentity.identityStatus === "complete"
      ? "complete"
      : "incomplete"
  });
}

export function buildVersionResponse(version, options) {
  return {
    version,
    releaseIdentity: buildReleaseIdentity(options)
  };
}

export const releaseIdentityManifestPath = DEFAULT_MANIFEST_PATH;
export const releaseIdentityDeploymentEnvironmentVariables = DEPLOYMENT_METADATA_SOURCES;

