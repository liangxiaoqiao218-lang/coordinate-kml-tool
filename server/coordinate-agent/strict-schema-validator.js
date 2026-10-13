function matchesType(value, type) {
  if (type === 'null') return value === null;
  if (type === 'array') return Array.isArray(value);
  if (type === 'object') return value !== null && typeof value === 'object' && !Array.isArray(value);
  if (type === 'number') return typeof value === 'number' && Number.isFinite(value);
  if (type === 'integer') return Number.isInteger(value);
  return typeof value === type;
}

function valueType(value) {
  if (value === undefined) return 'missing';
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  if (typeof value === 'number' && Number.isInteger(value)) return 'integer';
  return typeof value;
}

export class CoordinateAgentSchemaValidationError extends Error {
  constructor(message, { path, keyword, expectedType, actualType } = {}) {
    super(message);
    this.name = 'CoordinateAgentSchemaValidationError';
    this.code = 'COORDINATE_AGENT_SCHEMA_VALIDATION_FAILED';
    this.path = String(path || 'value');
    this.field = this.path.split('.').at(-1)?.replace(/\[\d+\]$/u, '') || null;
    this.keyword = String(keyword || 'schema');
    this.expectedType = expectedType === undefined ? null : String(expectedType);
    this.actualType = actualType === undefined ? null : String(actualType);
  }
}

function fail(message, details) {
  throw new CoordinateAgentSchemaValidationError(message, details);
}

function validate(schema, value, path) {
  if (!schema || typeof schema !== 'object') return;
  if (Array.isArray(schema.anyOf)) {
    const errors = [];
    for (const candidate of schema.anyOf) {
      try {
        validate(candidate, value, path);
        return;
      } catch (error) {
        errors.push(error.message);
      }
    }
    fail(`${path} does not match any allowed schema: ${errors.join(' | ')}`, {
      path, keyword: 'anyOf', expectedType: 'allowed schema', actualType: valueType(value),
    });
  }
  if (Object.hasOwn(schema, 'const') && value !== schema.const) fail(`${path} must equal the schema constant`, {
    path, keyword: 'const', expectedType: 'schema constant', actualType: valueType(value),
  });
  if (Array.isArray(schema.enum) && !schema.enum.includes(value)) fail(`${path} is not an allowed value`, {
    path, keyword: 'enum', expectedType: 'allowed enum value', actualType: valueType(value),
  });

  const types = schema.type === undefined ? [] : (Array.isArray(schema.type) ? schema.type : [schema.type]);
  if (types.length && !types.some(type => matchesType(value, type))) fail(`${path} has an invalid type`, {
    path, keyword: 'type', expectedType: types.join('|'), actualType: valueType(value),
  });
  if (value === null) return;

  if (types.includes('object')) {
    const properties = schema.properties || {};
    if (schema.additionalProperties === false) {
      const unknown = Object.keys(value).filter(key => !Object.hasOwn(properties, key));
      if (unknown.length) fail(`${path} contains unknown properties: ${unknown.join(', ')}`, {
        path: `${path}.${unknown[0]}`, keyword: 'additionalProperties', expectedType: 'no additional property', actualType: valueType(value[unknown[0]]),
      });
    }
    for (const key of schema.required || []) {
      if (!Object.hasOwn(value, key)) fail(`${path}.${key} is required`, {
        path: `${path}.${key}`, keyword: 'required', expectedType: 'required field', actualType: 'missing',
      });
    }
    for (const [key, childSchema] of Object.entries(properties)) {
      if (Object.hasOwn(value, key)) validate(childSchema, value[key], `${path}.${key}`);
    }
  }
  if (types.includes('array')) {
    if (schema.minItems !== undefined && value.length < schema.minItems) fail(`${path} has too few items`, {
      path, keyword: 'minItems', expectedType: `at least ${schema.minItems} items`, actualType: `array(${value.length})`,
    });
    if (schema.maxItems !== undefined && value.length > schema.maxItems) fail(`${path} has too many items`, {
      path, keyword: 'maxItems', expectedType: `at most ${schema.maxItems} items`, actualType: `array(${value.length})`,
    });
    if (schema.items) value.forEach((item, index) => validate(schema.items, item, `${path}[${index}]`));
  }
  if ((types.includes('number') || types.includes('integer')) && typeof value === 'number') {
    if (schema.minimum !== undefined && value < schema.minimum) fail(`${path} is below minimum`, {
      path, keyword: 'minimum', expectedType: `number >= ${schema.minimum}`, actualType: valueType(value),
    });
    if (schema.maximum !== undefined && value > schema.maximum) fail(`${path} is above maximum`, {
      path, keyword: 'maximum', expectedType: `number <= ${schema.maximum}`, actualType: valueType(value),
    });
  }
}

export function assertJsonSchema(schema, value, path = 'value') {
  validate(schema, value, path);
  return value;
}
