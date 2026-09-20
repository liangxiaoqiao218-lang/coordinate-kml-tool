import { parseAgenticProviderResponse } from '../agentic-coordinate-recognition/provider-response.js';
import { buildAgenticCoordinateFinalizationPrompt } from './prompt.js';

export async function runAgenticCoordinateFinalization({
  currentText,
  recognitionContext = null,
  documentRevision,
  modelName,
  providerCall,
}) {
  if (typeof currentText !== 'string' || !currentText.trim()) {
    throw new Error('currentText must be a non-empty string');
  }
  if (!Number.isInteger(documentRevision) || documentRevision < 1) {
    throw new Error('documentRevision must be a positive integer');
  }
  if (typeof providerCall !== 'function') throw new Error('providerCall is required');

  let providerCallCount = 0;
  const response = await (() => {
    providerCallCount += 1;
    return providerCall({
      modelName,
      prompt: buildAgenticCoordinateFinalizationPrompt({ currentText, recognitionContext }),
      imageItems: [],
      temperature: 0,
      maxTokens: 5000,
      stageName: 'agentic_text_finalize',
      lowValue: false,
    });
  })();

  return Object.freeze({
    documentRevision,
    result: parseAgenticProviderResponse(response),
    execution: Object.freeze({
      providerCallCount,
      retryCount: 0,
      fallbackCount: 0,
    }),
  });
}

