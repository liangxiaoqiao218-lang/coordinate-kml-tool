import { buildImageTextObservations } from "./evidence-acquisition-adapter.js";
import { locateCoordinateRows, locateTrustedCoordinateRows } from "./row-locator.js";
import {
  TRUSTED_LAYOUT_ATTESTATION_CAPABILITY,
  validateTrustedLayoutAttestation
} from "./trusted-layout-attestation.js";

export const EVIDENCE_ACQUISITION_SCHEMA_VERSION = "evidence_acquisition_v1";

export { buildImageTextObservations } from "./evidence-acquisition-adapter.js";
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
  const evidence = {
    schema_version: EVIDENCE_ACQUISITION_SCHEMA_VERSION,
    observations,
    rowBindings,
    pixel_bbox_available: rowBindings.some(binding => binding.location_status === "PIXEL_BBOX"),
    shadow_only: true,
    affects_coordinates: false,
    affects_kml: false,
    trusted_layout_status: trustedLayoutValidation.valid ? "ATTESTED" : "UNATTESTED",
    trusted_layout_reason: trustedLayoutValidation.reason,
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
