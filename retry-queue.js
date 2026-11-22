import { pathToFileURL } from "node:url";
function createAsyncQueue() {
  const msg = [];
  const waiting = [];

  return {
    push(item) {
      msg.push(item);

      if (waiting.length > 0) {
        const resolve = waiting.shift();
        resolve(msg.shift());
      }
    },

    next() {
      if (msg.length > 0) {
        return Promise.resolve(msg.shift());
      }

      return new Promise((resolve) => {
        waiting.push(resolve);
      });
    },
  };
}

function delay(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

export function createRetryQueue(processJob, options = {}) {
  const { concurrency = 2, maxRetries = 3, baseDelay = 1000 } = options;

  if (typeof processJob !== "function")
    throw new TypeError("Processor must be a function");
  if (
    !Number.isInteger(concurrency) ||
    concurrency < 1 ||
    !Number.isInteger(maxRetries) ||
    maxRetries < 0 ||
    !Number.isFinite(baseDelay) ||
    baseDelay < 0
  )
    throw new RangeError("Invalid retry options");

  const queue = createAsyncQueue();
  const deadLetterQueue = [];

  function retry(job) {
    const expTime = Math.min(2147483647, baseDelay * 2 ** (job.retries - 1));

    console.log(`Retrying ${job.id} after ${expTime}ms`);

    setTimeout(() => {
      queue.push(job);
    }, expTime);
  }

  async function worker() {
    while (true) {
      const job = await queue.next();

      try {
        await processJob(job);

        console.log(`Job ${job.id} succeeded`);
      } catch (error) {
        job.retries += 1;

        if (job.retries <= maxRetries) {
          retry(job);
        } else {
          deadLetterQueue.push({
            ...job,
            error: error instanceof Error ? error.message : String(error),
          });

          console.log(`Job ${job.id} moved to dead-letter queue`);
        }
      }
    }
  }

  for (let i = 0; i < concurrency; i++) {
    worker();
  }

  return {
    push(data) {
      const job = {
        ...data,
        retries: 0,
      };

      queue.push(job);
    },

    getDeadLetterQueue() {
      return deadLetterQueue.map((job) => ({ ...job }));
    },
  };
}

const attempts = new Map();

async function processJob(job) {
  const count = (attempts.get(job.id) || 0) + 1;
  attempts.set(job.id, count);

  console.log(`Processing ${job.id}, attempt ${count}`);

  await delay(30);

  if (job.id === "A" && count <= 2) {
    throw new Error("Temporary API failure");
  }

  if (job.id === "B") {
    throw new Error("Permanent API failure");
  }

  console.log(`Completed ${job.id}`);
  return `Result for ${job.id}`;
}

async function main() {
  const processor = createRetryQueue(processJob, {
    concurrency: 2,
    maxRetries: 3,
    baseDelay: 50,
  });

  processor.push({
    id: "A",
    message: "Send notification",
  });

  processor.push({
    id: "B",
    message: "Generate report",
  });

  processor.push({
    id: "C",
    message: "Save analytics",
  });

  await delay(900);

  console.log("Dead-letter queue:", processor.getDeadLetterQueue());
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  await main();
}
