import { pathToFileURL } from "node:url";
import { randomUUID } from "node:crypto";

function abortError() {
  const error = new Error("Job cancelled by user");
  error.name = "AbortError";
  return error;
}

function createAsyncQueue() {
  const messages = [];
  const waiting = [];

  return {
    push(item) {
      if (waiting.length > 0) {
        waiting.shift()(item);
      } else {
        messages.push(item);
      }
    },

    next() {
      if (messages.length > 0) {
        return Promise.resolve(messages.shift());
      }

      return new Promise((resolve) => {
        waiting.push(resolve);
      });
    },
  };
}

function sleep(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(abortError());
      return;
    }

    let settled = false;

    const timer = setTimeout(() => {
      if (settled) return;

      settled = true;
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);

    function onAbort() {
      if (settled) return;

      settled = true;
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      reject(abortError());
    }

    signal?.addEventListener("abort", onAbort, {
      once: true,
    });

    if (signal?.aborted) {
      onAbort();
    }
  });
}

async function runWithTimeout(operation, timeout, jobSignal) {
  if (jobSignal.aborted) {
    throw abortError();
  }

  const attemptController = new AbortController();

  let timer;
  let onCancel;

  const interrupt = new Promise((_, reject) => {
    onCancel = () => {
      reject(abortError());
      attemptController.abort();
    };

    jobSignal.addEventListener("abort", onCancel, {
      once: true,
    });
  });

  const timeoutPromise = new Promise((_, reject) => {
    timer = setTimeout(() => {
      const error = new Error(`Step timed out after ${timeout}ms`);

      error.name = "TimeoutError";

      reject(error);
      attemptController.abort();
    }, timeout);
  });

  const operationPromise = Promise.resolve().then(() => {
    if (attemptController.signal.aborted) {
      throw abortError();
    }

    return operation(attemptController.signal);
  });

  try {
    return await Promise.race([operationPromise, timeoutPromise, interrupt]);
  } catch (error) {
    if (jobSignal.aborted) {
      throw abortError();
    }

    throw error;
  } finally {
    clearTimeout(timer);
    jobSignal.removeEventListener("abort", onCancel);
  }
}

const reportDatabase = new Map();

async function searchNews(job, signal) {
  await sleep(job.task === "slow search" ? 1200 : 200, signal);

  return [`Article 1 about ${job.task}`, `Article 2 about ${job.task}`];
}

async function summarizeNews(job, signal) {
  await sleep(300, signal);

  if (job.task === "AI news" && job.retries === 0) {
    const error = new Error("Temporary LLM service failure");

    error.retryable = true;

    throw error;
  }

  return `Summary: ${job.context.articles.join("; ")}`;
}

async function saveReport(job, signal) {
  await sleep(150, signal);

  if (signal.aborted) {
    throw abortError();
  }

  const report = {
    id: job.id,
    topic: job.task,
    articles: job.context.articles,
    summary: job.context.summary,
  };

  reportDatabase.set(job.id, report);

  return report;
}

export function createAgentEngine(options = {}) {
  const {
    concurrency = 2,
    maxRetries = 3,
    baseDelay = 1000,
    timeout = 5000,
  } = options;

  if (
    !Number.isInteger(concurrency) ||
    concurrency < 1 ||
    !Number.isInteger(maxRetries) ||
    maxRetries < 0 ||
    !Number.isFinite(baseDelay) ||
    baseDelay < 0 ||
    !Number.isFinite(timeout) ||
    timeout <= 0
  ) {
    throw new Error("Invalid engine options");
  }

  const queue = createAsyncQueue();
  const jobs = new Map();
  const subscribers = new Map();

  const finished = new Set(["completed", "failed", "cancelled"]);

  const steps = [
    {
      name: "search",
      run: searchNews,
      output: "articles",
    },
    {
      name: "summarize",
      run: summarizeNews,
      output: "summary",
    },
    {
      name: "save",
      run: saveReport,
      output: "report",
    },
  ];

  function getStatus(id) {
    const job = jobs.get(id);

    if (!job) return null;

    return {
      id: job.id,
      task: job.task,
      status: job.status,
      currentStep: steps[job.stepIndex]?.name ?? null,
      completedSteps: steps.slice(0, job.stepIndex).map((step) => step.name),
      retries: job.retries,
      attempts: job.attempts,
      result: job.result === null ? null : structuredClone(job.result),
      error: job.error,
    };
  }

  function subscribe(event, handler) {
    if (typeof handler !== "function")
      throw new TypeError("Subscriber must be a function");
    if (!subscribers.has(event)) {
      subscribers.set(event, new Set());
    }

    subscribers.get(event).add(handler);

    return () => {
      subscribers.get(event)?.delete(handler);
    };
  }

  function publish(event, data) {
    const handlers = [...(subscribers.get(event) ?? [])];

    const promises = handlers.map((fn) => {
      return Promise.resolve().then(() => fn(data));
    });

    return Promise.allSettled(promises);
  }

  function finishJob(job, status, error = null) {
    if (finished.has(job.status)) {
      return;
    }

    if (job.retryTimer !== null) {
      clearTimeout(job.retryTimer);
    }

    job.retryTimer = null;

    if (job.retryAbortListener) {
      job.controller.signal.removeEventListener(
        "abort",
        job.retryAbortListener,
      );

      job.retryAbortListener = null;
    }

    job.status = status;
    job.error =
      error == null
        ? null
        : error instanceof Error
          ? error.message
          : String(error);

    if (status === "completed") {
      job.result = job.context.report;
    }

    const snapshot = getStatus(job.id);

    job.resolveDone(snapshot);

    void publish(`job.${status}`, snapshot);
  }

  function scheduleRetry(job) {
    const ms = Math.min(2147483647, baseDelay * 2 ** (job.retries - 1));

    job.status = "retry_wait";

    function onAbort() {
      if (job.retryTimer !== null) {
        clearTimeout(job.retryTimer);
      }

      job.retryTimer = null;

      job.controller.signal.removeEventListener("abort", onAbort);

      job.retryAbortListener = null;
    }

    job.retryAbortListener = onAbort;

    job.controller.signal.addEventListener("abort", onAbort, { once: true });

    job.retryTimer = setTimeout(() => {
      onAbort();

      if (job.controller.signal.aborted || finished.has(job.status)) {
        return;
      }

      job.status = "queued";
      queue.push(job);
    }, ms);

    if (job.controller.signal.aborted) {
      onAbort();
    }
  }

  async function worker() {
    while (true) {
      const job = await queue.next();

      if (finished.has(job.status) || job.controller.signal.aborted) {
        continue;
      }

      const step = steps[job.stepIndex];

      job.status = "running";
      job.attempts++;

      try {
        const value = await runWithTimeout(
          (signal) => step.run(job, signal),
          timeout,
          job.controller.signal,
        );

        if (job.controller.signal.aborted || finished.has(job.status)) {
          continue;
        }

        job.context[step.output] = value;

        job.stepIndex++;
        job.retries = 0;

        if (job.stepIndex === steps.length) {
          finishJob(job, "completed");
        } else {
          job.status = "queued";
          queue.push(job);
        }
      } catch (error) {
        if (job.controller.signal.aborted) {
          finishJob(job, "cancelled", abortError());
          continue;
        }

        const retryable =
          error?.name === "TimeoutError" || error?.retryable === true;

        if (retryable && job.retries < maxRetries) {
          job.retries++;
          scheduleRetry(job);
        } else {
          finishJob(job, "failed", error);
        }
      }
    }
  }

  for (let i = 0; i < concurrency; i++) {
    void worker();
  }

  return {
    submit({ task }) {
      if (typeof task !== "string" || !task.trim()) {
        throw new Error("Task must be a non-empty string");
      }

      let resolveDone;

      const done = new Promise((resolve) => {
        resolveDone = resolve;
      });

      const job = {
        id: randomUUID(),
        task: task.trim(),
        controller: new AbortController(),
        status: "queued",
        stepIndex: 0,
        retries: 0,
        attempts: 0,
        context: {},
        result: null,
        error: null,
        retryTimer: null,
        retryAbortListener: null,
        resolveDone,
        done,
      };

      jobs.set(job.id, job);
      queue.push(job);

      return job.id;
    },

    getStatus,

    cancel(id) {
      const job = jobs.get(id);

      if (!job || finished.has(job.status)) {
        return false;
      }

      job.controller.abort();

      finishJob(job, "cancelled", abortError());

      return true;
    },

    waitFor(id) {
      const job = jobs.get(id);

      if (!job) {
        return Promise.reject(new Error("Unknown job ID"));
      }

      return job.done;
    },

    subscribe,
  };
}

async function main() {
  const engine = createAgentEngine({
    concurrency: 2,
    maxRetries: 2,
    baseDelay: 250,
    timeout: 700,
  });

  engine.subscribe("job.completed", (job) => {
    console.log("Completed:", job.task);
  });

  engine.subscribe("job.failed", (job) => {
    console.log("Failed:", job.task, job.error);
  });

  engine.subscribe("job.cancelled", (job) => {
    console.log("Cancelled:", job.task);
  });

  const d = engine.submit({
    task: "cancel this task",
  });

  const a = engine.submit({
    task: "AI news",
  });

  const b = engine.submit({
    task: "slow search",
  });

  const c = engine.submit({
    task: "JavaScript news",
  });

  setTimeout(() => {
    engine.cancel(d);
  }, 100);

  const results = await Promise.all(
    [a, b, c, d].map((id) => engine.waitFor(id)),
  );

  console.log(
    "Final statuses:",
    results.map((job) => ({
      task: job.task,
      status: job.status,
      attempts: job.attempts,
    })),
  );

  console.log("Saved reports:", [...reportDatabase.values()]);
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  await main();
}
