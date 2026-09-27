function matchesType(value, type) {
  if (type === 'null') return value === null;
  if (type === 'array') return Array.isArray(value);
  if (type === 'object') return value !== null && typeof value === 'object' && !Array.isArray(value);
  if (type === 'number') return typeof value === 'number' && Number.isFinite(value);
  if (type === 'integer') return Number.isInteger(value);
  return typeof value === type;
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
    throw new Error(`${path} does not match any allowed schema: ${errors.join(' | ')}`);
  }
  if (Object.hasOwn(schema, 'const') && value !== schema.const) throw new Error(`${path} must equal the schema constant`);
  if (Array.isArray(schema.enum) && !schema.enum.includes(value)) throw new Error(`${path} is not an allowed value`);

  const types = schema.type === undefined ? [] : (Array.isArray(schema.type) ? schema.type : [schema.type]);
  if (types.length && !types.some(type => matchesType(value, type))) throw new Error(`${path} has an invalid type`);
  if (value === null) return;

  if (types.includes('object')) {
    const properties = schema.properties || {};
    if (schema.additionalProperties === false) {
      const unknown = Object.keys(value).filter(key => !Object.hasOwn(properties, key));
      if (unknown.length) throw new Error(`${path} contains unknown properties: ${unknown.join(', ')}`);
    }
    for (const key of schema.required || []) {
      if (!Object.hasOwn(value, key)) throw new Error(`${path}.${key} is required`);
    }
    for (const [key, childSchema] of Object.entries(properties)) {
      if (Object.hasOwn(value, key)) validate(childSchema, value[key], `${path}.${key}`);
    }
  }
  if (types.includes('array')) {
    if (schema.minItems !== undefined && value.length < schema.minItems) throw new Error(`${path} has too few items`);
    if (schema.maxItems !== undefined && value.length > schema.maxItems) throw new Error(`${path} has too many items`);
    if (schema.items) value.forEach((item, index) => validate(schema.items, item, `${path}[${index}]`));
  }
  if ((types.includes('number') || types.includes('integer')) && typeof value === 'number') {
    if (schema.minimum !== undefined && value < schema.minimum) throw new Error(`${path} is below minimum`);
    if (schema.maximum !== undefined && value > schema.maximum) throw new Error(`${path} is above maximum`);
  }
}

export function assertJsonSchema(schema, value, path = 'value') {
  validate(schema, value, path);
  return value;
}
