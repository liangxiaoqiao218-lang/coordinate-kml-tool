function createStopError(code, message, reason) {
  const error = new Error(message);
  error.code = code;
  error.reason = reason;
  return error;
}

export async function runCancellableOcrJob({
  createWorker,
  image,
  signal = null,
  timeoutMs,
  deadlineCode = "RECOGNITION_DEADLINE_EXCEEDED",
  timeoutCode = "RECOGNITION_BUDGET_EXHAUSTED"
}) {
  if (typeof createWorker !== "function") throw new TypeError("createWorker is required");
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) throw new RangeError("timeoutMs must be positive");

  let worker = null;
  let terminationPromise = null;
  let stopped = false;
  let timer = null;
  let abortListener = null;

  const terminate = () => {
    stopped = true;
    if (worker && !terminationPromise) {
      terminationPromise = Promise.resolve(worker.terminate()).catch(() => {});
    }
    return terminationPromise || Promise.resolve();
  };

  try {
    const operation = (async () => {
      worker = await createWorker();
      if (stopped || signal?.aborted) {
        await terminate();
        throw createStopError(deadlineCode, "OCR job aborted.", "request_aborted");
      }
      return worker.recognize(image);
    })();

    const cancellation = new Promise((_, reject) => {
      timer = setTimeout(() => {
        void terminate();
        reject(createStopError(timeoutCode, "OCR job timed out.", "stage_timeout"));
      }, timeoutMs);
      abortListener = () => {
        void terminate();
        reject(createStopError(deadlineCode, "OCR job aborted.", "request_aborted"));
      };
      signal?.addEventListener("abort", abortListener, { once: true });
    });

    return await Promise.race([operation, cancellation]);
  } finally {
    clearTimeout(timer);
    if (abortListener) signal?.removeEventListener("abort", abortListener);
    await terminate();
  }
}
