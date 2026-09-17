import { buildImageTextObservations } from "./evidence-acquisition-adapter.js";
import { locateCoordinateRows } from "./row-locator.js";

export const EVIDENCE_ACQUISITION_SCHEMA_VERSION = "evidence_acquisition_v1";

export { buildImageTextObservations } from "./evidence-acquisition-adapter.js";
export {
  IMAGE_OBSERVATION_SCHEMA_VERSION,
  ORIGINAL_IMAGE_OBSERVATION_ATTESTATION,
  ORIGINAL_IMAGE_PIXEL_SPACE,
  SERVER_PROVENANCE_ATTESTATION,
  createImageTextObservation
} from "./observation-schema.js";

export function buildEvidenceAcquisition({ recognitionResult = {}, coordinateEngineV2 = {} } = {}) {
  const observations = buildImageTextObservations({ recognitionResult });
  const rowBindings = locateCoordinateRows({ coordinateEngineV2, observations });
  return {
    schema_version: EVIDENCE_ACQUISITION_SCHEMA_VERSION,
    observations,
    rowBindings,
    pixel_bbox_available: rowBindings.some(binding => binding.location_status === "PIXEL_BBOX"),
    shadow_only: true,
    affects_coordinates: false,
    affects_kml: false
  };
}
