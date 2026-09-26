export class MockCoordinateAgentProviderAdapter {
  #turns;
  #calls = [];

  constructor(turns = []) {
    if (!Array.isArray(turns)) throw new Error('Mock turns must be an array');
    this.#turns = [...turns];
  }

  async runTurn(input) {
    this.#calls.push(input);
    if (this.#turns.length === 0) throw new Error('Mock provider has no scripted turn remaining');
    const next = this.#turns.shift();
    return typeof next === 'function' ? next(input) : structuredClone(next);
  }

  get callCount() {
    return this.#calls.length;
  }

  calls() {
    return Object.freeze([...this.#calls]);
  }
}
