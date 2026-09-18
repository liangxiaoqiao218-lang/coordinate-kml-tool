import { buildImageTextObservations } from "./evidence-acquisition-adapter.js";
import { locateCoordinateRows, locateTrustedCoordinateRows } from "./row-locator.js";
import {
  TRUSTED_LAYOUT_ATTESTATION_CAPABILITY,
  validateTrustedLayoutAttestation
} from "./trusted-layout-attestation.js";
import { getProviderLayoutRoleClassificationFailureReason } from "./provider-layout-role-classifier.js";

export const EVIDENCE_ACQUISITION_SCHEMA_VERSION = "evidence_acquisition_v1";

export { buildImageTextObservations } from "./evidence-acquisition-adapter.js";
export {
  LOCAL_OCR_MAP_LAYOUT_CLASSIFIER_VERSION,
  LOCAL_OCR_STRUCTURED_LAYOUT_CAPABILITY,
  LOCAL_OCR_STRUCTURED_LAYOUT_SOURCE_TYPE,
  createLocalOcrMapLayoutRows,
  extractLocalOcrLayoutLines,
  hasLocalOcrStructuredLayoutCapability,
  isLocalOcrMapLayoutCandidate
} from "./local-ocr-map-layout-classifier.js";
export {
  TRUSTED_LAYOUT_ATTESTATION_CAPABILITY,
  TRUSTED_LAYOUT_ATTESTATION_SCHEMA_VERSION,
  TRUSTED_ROW_BINDING_SCHEMA_VERSION,
  createServerClassifiedLayoutRows,
  createTrustedLayoutAttestation,
  extractProviderLayoutCandidates,
  validateTrustedLayoutAttestation
} from "./trusted-layout-attestation.js";
export {
  PROVIDER_LAYOUT_CLASSIFICATION_REASON,
  PROVIDER_LAYOUT_ROLE_CLASSIFICATION_SCHEMA_VERSION,
  SERVER_LAYOUT_ROLE_CLASSIFICATION_CAPABILITY,
  STRUCTURED_PROVIDER_LAYOUT_EXTRACTION_CAPABILITY,
  classifyProviderLayoutRoles,
  createServerOwnedLayoutClassifierProfile,
  getProductionProviderLayoutClassifierProfile,
  getProviderLayoutRoleClassificationFailureReason,
  validateProviderLayoutRoleClassification
} from "./provider-layout-role-classifier.js";
export {
  PROVIDER_LAYOUT_PROFILE_QUALIFICATION_COLLECTOR_VERSION,
  PROVIDER_LAYOUT_PROFILE_QUALIFICATION_SCHEMA_VERSION,
  PROVIDER_LAYOUT_PROFILE_QUALIFICATION_STATUS,
  PROVIDER_LAYOUT_PROFILE_QUALIFICATION_TTL_MS,
  PROVIDER_LAYOUT_RESPONSE_CONTRACT,
  ProviderLayoutProfileQualificationRuntime,
  collectProviderLayoutProfileQualification,
  hasProviderLayoutProfileQualificationCapability,
  isProviderLayoutQualificationReadAllowed,
  validateProviderLayoutProfileQualification
} from "./provider-layout-profile-qualification.js";
export {
  PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_CHALLENGE_SCHEMA_VERSION,
  PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_GRANT_SCHEMA_VERSION,
  PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_OUTPUT_POLICY,
  PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_RESPONSE_CONTRACT,
  PROVIDER_LAYOUT_PRODUCTION_QUALIFICATION_TTL_MS,
  ProviderLayoutProductionQualificationGrantRuntime,
  authorizeProviderLayoutProductionQualificationGrant,
  executeProviderLayoutProductionQualificationProbe,
  hasProviderLayoutProductionQualificationGrantCapability,
  serializeProviderLayoutProductionQualificationGrant,
  verifyEd25519DetachedSignature,
  verifyProviderLayoutProductionQualificationGrantSignature
} from "./provider-layout-production-qualification-gate.js";
export {
  IMAGE_OBSERVATION_SCHEMA_VERSION,
  ORIGINAL_IMAGE_OBSERVATION_ATTESTATION,
  ORIGINAL_IMAGE_PIXEL_SPACE,
  SERVER_PROVENANCE_ATTESTATION,
  createImageTextObservation
} from "./observation-schema.js";

export function buildEvidenceAcquisition({ recognitionResult = {}, coordinateEngineV2 = {}, resultRevision = 1 } = {}) {
  const shadowObservations = buildImageTextObservations({ recognitionResult });
  const trustedLayoutValidation = validateTrustedLayoutAttestation({
    attestation: recognitionResult.trustedLayoutAttestation,
    imageIdentity: recognitionResult.imageMetadata,
    coordinateEngineV2,
    resultRevision
  });
  const trustedObservations = trustedLayoutValidation.valid
    ? trustedLayoutValidation.attestation.observations
    : [];
  const trustedIds = new Set(trustedObservations.map(item => item.observation_id));
  const observations = [
    ...trustedObservations,
    ...shadowObservations.filter(item => !trustedIds.has(item.observation_id))
  ];
  const trustedRowBindings = locateTrustedCoordinateRows({ trustedLayoutValidation });
  const rowBindings = trustedRowBindings || locateCoordinateRows({ coordinateEngineV2, observations });
  const trustedClassifierFailureReason = getProviderLayoutRoleClassificationFailureReason(
    recognitionResult.providerLayoutClassificationOutcome
  );
  const evidence = {
    schema_version: EVIDENCE_ACQUISITION_SCHEMA_VERSION,
    observations,
    rowBindings,
    pixel_bbox_available: rowBindings.some(binding => binding.location_status === "PIXEL_BBOX"),
    shadow_only: true,
    affects_coordinates: false,
    affects_kml: false,
    trusted_layout_status: trustedLayoutValidation.valid ? "ATTESTED" : "UNATTESTED",
    trusted_layout_reason: trustedLayoutValidation.valid
      ? null
      : (trustedClassifierFailureReason || trustedLayoutValidation.reason),
    trusted_layout_attestation_sha256: trustedLayoutValidation.valid
      ? trustedLayoutValidation.attestation.attestation_sha256
      : null
  };
  if (trustedLayoutValidation.valid) {
    Object.defineProperty(evidence, TRUSTED_LAYOUT_ATTESTATION_CAPABILITY, { value: true, enumerable: false });
    Object.defineProperty(evidence, "trustedLayoutAttestation", {
      value: trustedLayoutValidation.attestation,
      enumerable: false
    });
  }
  return evidence;
}
