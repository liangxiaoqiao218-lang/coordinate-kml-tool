export const COORDINATE_AGENT_SCHEMA_VERSION = 'coordinate-intelligence-agent/v1';

export const AGENT_STATE = Object.freeze({
  INITIALIZED: 'INITIALIZED',
  OBSERVING: 'OBSERVING',
  PLANNING: 'PLANNING',
  ACTING: 'ACTING',
  RECONCILING: 'RECONCILING',
  VERIFYING: 'VERIFYING',
  CONFIRMED: 'CONFIRMED',
  REVIEW_REQUIRED: 'REVIEW_REQUIRED',
  FAILED_CLOSED: 'FAILED_CLOSED',
});

export const TERMINAL_STATES = Object.freeze(new Set([
  AGENT_STATE.CONFIRMED,
  AGENT_STATE.REVIEW_REQUIRED,
  AGENT_STATE.FAILED_CLOSED,
]));

export const EVIDENCE_KIND = Object.freeze({
  DOCUMENT: 'document',
  HEADER: 'header',
  TABLE: 'table',
  ROW: 'row',
  CELL: 'cell',
  DIRECTION: 'direction',
  COORDINATE: 'coordinate',
  TOOL_RESULT: 'tool_result',
  OTHER: 'other',
});

export const GENERIC_TOOL_NAMES = Object.freeze({
  CROP_REGION: 'crop_region',
  ZOOM_REGION: 'zoom_region',
  ROTATE_IMAGE: 'rotate_image',
  LOCAL_OCR_REGION: 'local_ocr_region',
  DETECT_TABLE_STRUCTURE: 'detect_table_structure',
  COORDINATE_MATH_CHECK: 'coordinate_math_check',
  SPATIAL_CONSISTENCY_CHECK: 'spatial_consistency_check',
  PROJECTED_COORDINATE_TRANSFORM_CHECK: 'projected_coordinate_transform_check',
});
