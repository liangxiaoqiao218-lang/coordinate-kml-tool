import { buildImageTextObservations } from "./evidence-acquisition-adapter.js";
import { locateCoordinateRows } from "./row-locator.js";

export const EVIDENCE_ACQUISITION_SCHEMA_VERSION = "evidence_acquisition_v1";

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
