import { pathToFileURL } from "node:url";

export class QueueClosedError extends Error {
  constructor() {
    super("Queue is closed");
    this.name = "QueueClosedError";
  }
}

export function createAsyncQueue() {
  const messages = [];
  const waitingConsumers = [];
  let closed = false;

  return {
    push(item) {
      if (closed) throw new QueueClosedError();

      if (waitingConsumers.length > 0) {
        waitingConsumers.shift().resolve(item);
      } else {
        messages.push(item);
      }
    },

    next() {
      if (messages.length > 0) return Promise.resolve(messages.shift());
      if (closed) return Promise.reject(new QueueClosedError());

      return new Promise((resolve, reject) => {
        waitingConsumers.push({ resolve, reject });
      });
    },

    close() {
      if (closed) return;
      closed = true;

      for (const consumer of waitingConsumers.splice(0)) {
        consumer.reject(new QueueClosedError());
      }
    },
  };
}

export function createMessageProcessor(limit, processMessage) {
  if (!Number.isInteger(limit) || limit < 1) {
    throw new RangeError("Concurrency limit must be a positive integer");
  }
  if (typeof processMessage !== "function") {
    throw new TypeError("Message handler must be a function");
  }

  const queue = createAsyncQueue();

  async function startWorker() {
    while (true) {
      let message;
      try {
        message = await queue.next();
      } catch (error) {
        if (error instanceof QueueClosedError) return;
        throw error;
      }

      try {
        await processMessage(message);
      } catch (error) {
        console.error("Message processing failed:", error);
      }
    }
  }

  const workers = Array.from({ length: limit }, () => startWorker());

  return {
    push(message) {
      queue.push(message);
    },

    async shutdown() {
      queue.close();
      await Promise.all(workers);
    },
  };
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const processor = createMessageProcessor(2, async (message) => {
    console.log("Started:", message);
    await new Promise((resolve) => setTimeout(resolve, 200));
    console.log("Completed:", message);
  });

  for (const message of ["A", "B", "C", "D"]) processor.push(message);

  console.log("Shutdown requested");
  await processor.shutdown();
  console.log("All accepted messages finished");

  try {
    processor.push("E");
  } catch (error) {
    console.log("New message rejected:", error.message);
  }
}
