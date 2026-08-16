import {
  createNormalizedCoordinateResult,
  createWarningMetadata,
  RECOGNIZER_PORT_STATUS,
  RECOGNIZER_TYPES,
  validateNormalizedCoordinateResult,
} from "./contracts.js";
import { createLatencyBudget, allocateRecognizerBudget } from "./latency-budget.js";
import { createRecognizerContract } from "./recognizer-contract.js";
import { createDefaultRecognizerRegistry, getRecognizerRegistrySummary, validateRecognizerRegistry } from "./registry.js";
import { runCoordinateEngineV3, V3_RUNNER_STATUS } from "./runner.js";
import {
  canHandleGenericDms,
  dmsToDecimal,
  genericDmsRecognizer,
  normalizeGenericDms,
  parseDmsTokens,
  parseGenericDmsRows,
  recognizeGenericDms,
  toGenericDmsKmlCoordinate,
  verifyGenericDms,
} from "./recognizers/generic-dms/index.js";
import {
  canHandleKyrgyzGk,
  convertKyrgyzGkToWgs84,
  KYRGYZ_GK_CRS,
  KYRGYZ_GK_PRECISION_MODE,
  kyrgyzGkRecognizer,
  looksLikeKyrgyzGkPair,
  normalizeKyrgyzGk,
  parseKyrgyzGkRows,
  recognizeKyrgyzGk,
  toKyrgyzGkKmlCoordinate,
  verifyKyrgyzGk,
} from "./recognizers/kyrgyzstan-gauss-kruger/index.js";
import {
  buildMadagascarCadastralCellPolygons,
  canHandleMadagascarCadastral,
  convertMadagascarCadastralToWgs84,
  formatMadagascarCadastralRows,
  inferMadagascarCadastralCellGeometry,
  MADAGASCAR_CADASTRAL_CELL_SEMANTICS,
  MADAGASCAR_CADASTRAL_OUTPUT_CRS,
  MADAGASCAR_CADASTRAL_PRECISION_MODE,
  MADAGASCAR_CADASTRAL_RECOGNIZER_ID,
  MADAGASCAR_CADASTRAL_SOURCE_CRS,
  madagascarCadastralRecognizer,
  normalizeMadagascarCadastral,
  parseMadagascarCadastralRows,
  recognizeMadagascarCadastral,
  toMadagascarCadastralKmlCoordinate,
  verifyMadagascarCadastral,
} from "./recognizers/madagascar-cadastral/index.js";
import {
  canHandleMgrs,
  mgrsRecognizer,
  normalizeMgrs,
  parseMgrsRows,
  recognizeMgrs,
  toMgrsKmlCoordinate,
  verifyMgrs,
} from "./recognizers/mgrs/index.js";
import {
  canHandleWgs84Decimal,
  normalizeWgs84Decimal,
  recognizeWgs84Decimal,
  toKmlCoordinate,
  verifyWgs84Decimal,
  wgs84DecimalRecognizer,
} from "./recognizers/wgs84-decimal/index.js";

export const COORDINATE_ENGINE_V3_DISABLED_REASON = "coordinate_engine_v3_not_enabled";

export function isCoordinateEngineV3Enabled(env = process.env) {
  return String(env.ENABLE_COORDINATE_ENGINE_V3 || "").trim().toLowerCase() === "true";
}

export async function recognizeWithIsolatedRecognizers(input = {}, {
  env = process.env,
  registry = createDefaultRecognizerRegistry(),
  latencyBudget = createLatencyBudget(),
} = {}) {
  if (!isCoordinateEngineV3Enabled(env)) {
    return Object.freeze({
      handled: false,
      reason: COORDINATE_ENGINE_V3_DISABLED_REASON,
      registry: getRecognizerRegistrySummary(registry),
    });
  }

  return runCoordinateEngineV3(input, { registry, latencyBudget });
}

export {
  allocateRecognizerBudget,
  createDefaultRecognizerRegistry,
  createLatencyBudget,
  createRecognizerContract,
  createNormalizedCoordinateResult,
  createWarningMetadata,
  getRecognizerRegistrySummary,
  runCoordinateEngineV3,
  RECOGNIZER_PORT_STATUS,
  RECOGNIZER_TYPES,
  validateNormalizedCoordinateResult,
  validateRecognizerRegistry,
  V3_RUNNER_STATUS,
  canHandleGenericDms,
  dmsToDecimal,
  genericDmsRecognizer,
  normalizeGenericDms,
  parseDmsTokens,
  parseGenericDmsRows,
  recognizeGenericDms,
  toGenericDmsKmlCoordinate,
  verifyGenericDms,
  canHandleKyrgyzGk,
  convertKyrgyzGkToWgs84,
  KYRGYZ_GK_CRS,
  KYRGYZ_GK_PRECISION_MODE,
  kyrgyzGkRecognizer,
  looksLikeKyrgyzGkPair,
  normalizeKyrgyzGk,
  parseKyrgyzGkRows,
  recognizeKyrgyzGk,
  toKyrgyzGkKmlCoordinate,
  verifyKyrgyzGk,
  buildMadagascarCadastralCellPolygons,
  canHandleMadagascarCadastral,
  convertMadagascarCadastralToWgs84,
  formatMadagascarCadastralRows,
  inferMadagascarCadastralCellGeometry,
  MADAGASCAR_CADASTRAL_CELL_SEMANTICS,
  MADAGASCAR_CADASTRAL_OUTPUT_CRS,
  MADAGASCAR_CADASTRAL_PRECISION_MODE,
  MADAGASCAR_CADASTRAL_RECOGNIZER_ID,
  MADAGASCAR_CADASTRAL_SOURCE_CRS,
  madagascarCadastralRecognizer,
  normalizeMadagascarCadastral,
  parseMadagascarCadastralRows,
  recognizeMadagascarCadastral,
  toMadagascarCadastralKmlCoordinate,
  verifyMadagascarCadastral,
  canHandleMgrs,
  mgrsRecognizer,
  normalizeMgrs,
  parseMgrsRows,
  recognizeMgrs,
  toMgrsKmlCoordinate,
  verifyMgrs,
  canHandleWgs84Decimal,
  normalizeWgs84Decimal,
  recognizeWgs84Decimal,
  toKmlCoordinate,
  verifyWgs84Decimal,
  wgs84DecimalRecognizer,
};
