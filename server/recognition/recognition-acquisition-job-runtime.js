import { randomBytes, randomUUID, timingSafeEqual } from "node:crypto";

export const RECOGNITION_ACQUISITION_JOB_STATUS = Object.freeze({
  QUEUED: "QUEUED",
  RUNNING: "RUNNING",
  SUCCEEDED: "SUCCEEDED",
  FAILED: "FAILED"
});

function normalizeHttpStatus(value, fallback = 500) {
  const status = Number(value);
  return Number.isInteger(status) && status >= 100 && status <= 599 ? status : fallback;
}

function safeFailureCode(value, fallback = "RECOGNITION_ASYNC_JOB_FAILED") {
  const code = String(value || "").trim();
  return /^[A-Za-z0-9_.:-]{1,160}$/u.test(code) ? code : fallback;
}

function buildSafeFailureResult(output, fallbackCode) {
  const source = output?.result && typeof output.result === "object" ? output.result : {};
  const result = {
    success: false,
    reason: safeFailureCode(
      source.reason || source.responseCode || source.code || output?.code,
      fallbackCode
    )
  };
  if (typeof source.userUsageConsumed === "boolean") result.userUsageConsumed = source.userUsageConsumed;
  if (typeof source.usageConsumed === "boolean") result.usageConsumed = source.usageConsumed;
  if (/^[0-9a-f]{8}-[0-9a-f-]{27}$/iu.test(String(source.requestId || ""))) {
    result.requestId = source.requestId;
  }
  return Object.freeze(result);
}

export function getRecognitionAcquisitionJobHttpStatus(job) {
  if (!job) return 404;
  if (job.status !== RECOGNITION_ACQUISITION_JOB_STATUS.FAILED) return 200;
  const status = normalizeHttpStatus(job.httpStatus, 500);
  return status >= 300 ? status : 500;
}

export function createRecognitionAcquisitionJobRuntime({
  execute,
  ttlMs = 30 * 60 * 1000,
  maxJobs = 64,
  now = () => Date.now()
} = {}) {
  if (typeof execute !== "function") throw new TypeError("recognition_job_execute_required");
  const jobs = new Map();
  const queue = [];
  let running = false;

  function tokenMatches(job, token) {
    const actual = Buffer.from(String(job?.accessToken || ""));
    const supplied = Buffer.from(String(token || ""));
    return actual.length > 0
      && actual.length === supplied.length
      && timingSafeEqual(actual, supplied);
  }

  function publicSnapshot(job) {
    if (!job) return null;
    return Object.freeze({
      jobId: job.jobId,
      status: job.status,
      createdAt: job.createdAt,
      startedAt: job.startedAt,
      completedAt: job.completedAt,
      httpStatus: job.httpStatus,
      result: [
        RECOGNITION_ACQUISITION_JOB_STATUS.SUCCEEDED,
        RECOGNITION_ACQUISITION_JOB_STATUS.FAILED
      ].includes(job.status) ? job.result : null,
      error: job.status === RECOGNITION_ACQUISITION_JOB_STATUS.FAILED ? job.error : null
    });
  }

  function prune() {
    const cutoff = now() - ttlMs;
    for (const [id, job] of jobs) {
      if (job.completedAt && job.completedAt < cutoff) jobs.delete(id);
    }
    const overflow = Math.max(0, jobs.size - maxJobs);
    if (overflow > 0) {
      [...jobs.values()]
        .filter(job => job.completedAt)
        .sort((a, b) => a.completedAt - b.completedAt)
        .slice(0, overflow)
        .forEach(job => jobs.delete(job.jobId));
    }
  }

  function makeRoomForOneJob() {
    prune();
    while (jobs.size >= maxJobs) {
      const completed = [...jobs.values()]
        .filter(job => job.completedAt)
        .sort((a, b) => a.completedAt - b.completedAt)[0];
      if (!completed) return false;
      jobs.delete(completed.jobId);
    }
    return true;
  }

  async function drain() {
    if (running) return;
    running = true;
    try {
      while (queue.length) {
        const job = queue.shift();
        if (!job || job.status !== RECOGNITION_ACQUISITION_JOB_STATUS.QUEUED) continue;
        job.status = RECOGNITION_ACQUISITION_JOB_STATUS.RUNNING;
        job.startedAt = now();
        try {
          const output = await execute(job.input, { jobId: job.jobId });
          job.httpStatus = normalizeHttpStatus(output?.httpStatus, 500);
          if (job.httpStatus >= 200 && job.httpStatus < 300) {
            job.status = RECOGNITION_ACQUISITION_JOB_STATUS.SUCCEEDED;
            job.result = output?.result ?? output;
          } else {
            job.status = RECOGNITION_ACQUISITION_JOB_STATUS.FAILED;
            job.result = buildSafeFailureResult(output, "RECOGNITION_ASYNC_HTTP_FAILED");
            job.error = Object.freeze({
              code: job.result.reason,
              message: "Recognition acquisition job failed"
            });
          }
        } catch (error) {
          job.status = RECOGNITION_ACQUISITION_JOB_STATUS.FAILED;
          job.httpStatus = normalizeHttpStatus(error?.httpStatus, 500);
          job.result = buildSafeFailureResult(error, "RECOGNITION_ASYNC_JOB_FAILED");
          job.error = Object.freeze({
            code: job.result.reason,
            message: "Recognition acquisition job failed"
          });
        } finally {
          job.completedAt = now();
          job.input = null;
          prune();
        }
      }
    } finally {
      running = false;
    }
  }

  return Object.freeze({
    enqueue(input) {
      if (!makeRoomForOneJob()) {
        const error = new Error("recognition_job_capacity_reached");
        error.code = "RECOGNITION_JOB_CAPACITY_REACHED";
        throw error;
      }
      const job = {
        jobId: randomUUID(),
        accessToken: randomBytes(24).toString("base64url"),
        status: RECOGNITION_ACQUISITION_JOB_STATUS.QUEUED,
        createdAt: now(),
        startedAt: null,
        completedAt: null,
        httpStatus: null,
        result: null,
        error: null,
        input
      };
      jobs.set(job.jobId, job);
      queue.push(job);
      queueMicrotask(drain);
      return Object.freeze({
        ...publicSnapshot(job),
        jobAccessToken: job.accessToken
      });
    },
    get(jobId, accessToken) {
      prune();
      const job = jobs.get(String(jobId || ""));
      return tokenMatches(job, accessToken) ? publicSnapshot(job) : null;
    },
    get size() {
      prune();
      return jobs.size;
    }
  });
}
