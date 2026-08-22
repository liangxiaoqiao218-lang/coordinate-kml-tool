export {
  ACQUISITION_AUTHORITY_FIELDS,
  ACQUISITION_CANDIDATE_SCHEMA_VERSION,
  ACQUISITION_PROVENANCE,
  ACQUISITION_SCHEMA_VERSION,
  ACQUISITION_SENSITIVE_FIELDS,
  ACQUISITION_SOURCE_TYPE,
  ACQUISITION_STATUS,
  acquisitionCandidateToRunnerInput,
  calculateCandidateCompleteness,
  createAcquisitionCandidate,
  createAcquisitionResult,
  dedupeAcquisitionCandidates,
  getCandidateDedupeKey,
  stripSensitiveAcquisitionMetadata,
  validateAcquisitionCandidate,
  validateAcquisitionResult,
} from "./contracts.js";

export {
  ACQUISITION_HARD_DEADLINE_MS,
  ACQUISITION_MAX_PROVIDER_CALLS,
  ACQUISITION_TARGET_MS,
  createAcquisitionBudget,
} from "./budget.js";

export {
  ACQUISITION_ADAPTER_STATUS,
  runAcquisitionCandidatesThroughRunner,
  shouldRequestTargetedAcquisition,
} from "./adapter.js";

export {
  createAcquisitionAdapterMetrics,
} from "./metrics.js";

export {
  PRIMARY_CANDIDATE_CONSTRUCTION_STATUS,
  PRIMARY_ACQUISITION_ERROR,
  PRIMARY_ACQUISITION_MAX_PROVIDER_CALLS,
  PRIMARY_ACQUISITION_PROMPT,
  PRIMARY_ACQUISITION_PROVIDER_TIMEOUT_MS,
  PRIMARY_JSON_PARSE_REASON,
  PRIMARY_JSON_PARSE_STATUS,
  PRIMARY_PROVIDER_STATUS,
  PRIMARY_SCHEMA_VALIDATION_REASON,
  PRIMARY_SCHEMA_VALIDATION_STATUS,
  acquirePrimaryImage,
  callPrimaryVisionProvider,
  getPrimaryProviderReadiness,
} from "./primary.js";

export {
  TABLE_CONTEXT_COMPOSITE_MODE,
  TABLE_CONTEXT_COMPOSITE_STATUS,
  createTableContextComposite,
  detectTableContextRegions,
} from "./table-context-composite.js";

export {
  ACQUISITION_STRATEGY_ID,
  ACQUISITION_STRATEGY_INPUT_MODE,
  ACQUISITION_STRATEGY_MODEL,
  ACQUISITION_STRATEGY_REASON,
  EXPERIMENTAL_STRUCTURAL_ROUTER_CONTRACT,
  EXPERIMENTAL_STRUCTURAL_ROUTER_THRESHOLDS,
  chooseAcquisitionStrategyFromMetrics,
  dryRunAcquisitionStrategy,
  normalizeStructuralMetrics,
} from "./strategy-router.js";
