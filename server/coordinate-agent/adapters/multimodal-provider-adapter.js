import {
  buildCoordinateAgentProviderRequest,
  parseCoordinateAgentStructuredOutput,
} from '../provider-protocol.js';

export class CoordinateAgentMultimodalProviderAdapter {
  #transport;
  #resolveImage;
  #requestOptions;

  constructor({ transport, resolveImage, requestOptions = {} } = {}) {
    if (typeof transport?.complete !== 'function') {
      throw new Error('An injected transport.complete(request) function is required');
    }
    if (typeof resolveImage !== 'function') throw new Error('An injected resolveImage(imageRef) function is required');
    this.#transport = transport;
    this.#resolveImage = resolveImage;
    this.#requestOptions = Object.freeze({ ...requestOptions });
  }

  async runTurn(turnInput) {
    const request = await buildCoordinateAgentProviderRequest({
      ...this.#requestOptions,
      turnInput,
      resolveImage: this.#resolveImage,
    });
    const response = await this.#transport.complete(request);
    return parseCoordinateAgentStructuredOutput(response);
  }
}
