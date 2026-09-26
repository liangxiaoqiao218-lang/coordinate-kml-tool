export class CoordinateAgentToolRegistry {
  #tools = new Map();

  register({ name, description, inputSchema, execute }) {
    const normalizedName = String(name || '').trim();
    if (!/^[a-z][a-z0-9_]*$/.test(normalizedName)) throw new Error('Tool name is invalid');
    if (this.#tools.has(normalizedName)) throw new Error(`Duplicate tool: ${normalizedName}`);
    if (typeof execute !== 'function') throw new Error(`Tool ${normalizedName} requires execute()`);
    this.#tools.set(normalizedName, Object.freeze({
      name: normalizedName,
      description: String(description || ''),
      inputSchema: Object.freeze(inputSchema || { type: 'object', additionalProperties: false }),
      execute,
    }));
    return this;
  }

  has(name) {
    return this.#tools.has(String(name || ''));
  }

  definitions() {
    return Object.freeze([...this.#tools.values()].map(tool => Object.freeze({
      name: tool.name,
      description: tool.description,
      inputSchema: tool.inputSchema,
    })));
  }

  async execute(action, context = {}) {
    const toolName = String(action?.toolName || '');
    const tool = this.#tools.get(toolName);
    if (!tool) throw new Error(`Unknown coordinate agent tool: ${toolName}`);
    assertInput(tool.inputSchema, action?.args || {}, `tool.${toolName}.args`);
    return tool.execute(Object.freeze({ ...(action?.args || {}) }), context);
  }
}

function matchesType(value, type) {
  if (type === 'array') return Array.isArray(value);
  if (type === 'object') return value !== null && typeof value === 'object' && !Array.isArray(value);
  if (type === 'number') return typeof value === 'number' && Number.isFinite(value);
  if (type === 'integer') return Number.isInteger(value);
  if (type === 'null') return value === null;
  return typeof value === type;
}

function assertInput(schema, value, path) {
  const types = Array.isArray(schema?.type) ? schema.type : [schema?.type || 'object'];
  if (!types.some(type => matchesType(value, type))) throw new Error(`${path} has an invalid type`);
  if (value === null) return;
  if (types.includes('object') && !Array.isArray(value)) {
    const properties = schema.properties || {};
    for (const required of schema.required || []) {
      if (!(required in value)) throw new Error(`${path}.${required} is required`);
    }
    if (schema.additionalProperties === false) {
      const unknown = Object.keys(value).filter(key => !(key in properties));
      if (unknown.length) throw new Error(`${path} contains unknown properties: ${unknown.join(', ')}`);
    }
    for (const [key, child] of Object.entries(properties)) {
      if (key in value) assertInput(child, value[key], `${path}.${key}`);
    }
  }
  if (Array.isArray(value)) {
    if (schema.minItems !== undefined && value.length < schema.minItems) throw new Error(`${path} has too few items`);
    if (schema.maxItems !== undefined && value.length > schema.maxItems) throw new Error(`${path} has too many items`);
    if (schema.items) value.forEach((item, index) => assertInput(schema.items, item, `${path}[${index}]`));
  }
  if (typeof value === 'number') {
    if (schema.minimum !== undefined && value < schema.minimum) throw new Error(`${path} is below minimum`);
    if (schema.maximum !== undefined && value > schema.maximum) throw new Error(`${path} is above maximum`);
  }
}
