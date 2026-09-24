import { randomBytes, randomUUID, timingSafeEqual } from "node:crypto";

export const RECOGNITION_ACQUISITION_JOB_STATUS = Object.freeze({
  QUEUED: "QUEUED",
  RUNNING: "RUNNING",
  SUCCEEDED: "SUCCEEDED",
  FAILED: "FAILED"
});

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
      result: job.status === RECOGNITION_ACQUISITION_JOB_STATUS.SUCCEEDED ? job.result : null,
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
          job.status = RECOGNITION_ACQUISITION_JOB_STATUS.SUCCEEDED;
          job.httpStatus = Number(output?.httpStatus || 200);
          job.result = output?.result ?? output;
        } catch (error) {
          job.status = RECOGNITION_ACQUISITION_JOB_STATUS.FAILED;
          job.httpStatus = Number(error?.httpStatus || 500);
          job.error = Object.freeze({
            code: String(error?.code || "RECOGNITION_ASYNC_JOB_FAILED"),
            message: String(error?.message || "Recognition acquisition job failed")
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
      prune();
      if (jobs.size >= maxJobs) {
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
