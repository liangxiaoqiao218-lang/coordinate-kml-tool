function createStopError(code, message, reason) {
  const error = new Error(message);
  error.code = code;
  error.reason = reason;
  return error;
}

export const LOCAL_OCR_FAILURE_CODE = "LOCAL_OCR_FAILED";

export async function runCancellableOcrJob({
  createWorker,
  image,
  recognizeOptions = {},
  recognizeOutput = undefined,
  signal = null,
  timeoutMs,
  terminationTimeoutMs = 250,
  deadlineCode = "RECOGNITION_DEADLINE_EXCEEDED",
  timeoutCode = "RECOGNITION_BUDGET_EXHAUSTED"
}) {
  if (typeof createWorker !== "function") throw new TypeError("createWorker is required");
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) throw new RangeError("timeoutMs must be positive");
  if (!Number.isFinite(terminationTimeoutMs) || terminationTimeoutMs <= 0) throw new RangeError("terminationTimeoutMs must be positive");

  let worker = null;
  let terminationPromise = null;
  let stopped = false;
  let timer = null;
  let abortListener = null;

  const terminate = () => {
    stopped = true;
    if (worker && !terminationPromise) {
      // Defer the invocation itself so a worker implementation that throws
      // synchronously cannot escape a timeout callback or mask the primary
      // recognition result/error.
      terminationPromise = Promise.resolve()
        .then(() => worker.terminate())
        .catch(() => {});
    }
    return terminationPromise || Promise.resolve();
  };

  const terminateWithinBound = async () => {
    let boundTimer = null;
    try {
      await Promise.race([
        terminate(),
        new Promise(resolve => {
          boundTimer = setTimeout(resolve, terminationTimeoutMs);
        })
      ]);
    } finally {
      clearTimeout(boundTimer);
    }
  };

  try {
    const operation = (async () => {
      worker = await createWorker();
      if (stopped || signal?.aborted) {
        await terminate();
        throw createStopError(deadlineCode, "OCR job aborted.", "request_aborted");
      }
      return worker.recognize(image, recognizeOptions, recognizeOutput);
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

    try {
      return await Promise.race([operation, cancellation]);
    } catch (error) {
      if (error?.code === deadlineCode || error?.code === timeoutCode) throw error;
      throw createStopError(LOCAL_OCR_FAILURE_CODE, "Local OCR failed.", "worker_error");
    }
  } finally {
    clearTimeout(timer);
    if (abortListener) signal?.removeEventListener("abort", abortListener);
    await terminateWithinBound();
  }
}
