import { pathToFileURL } from "node:url";

function createAsyncQueue() {
  const messages = [];
  const waitingConsumers = [];

  return {
    push(item) {
      if (waitingConsumers.length > 0) {
        waitingConsumers.shift()(item);
      } else {
        messages.push(item);
      }
    },

    next() {
      if (messages.length > 0) return Promise.resolve(messages.shift());
      return new Promise((resolve) => waitingConsumers.push(resolve));
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
      const message = await queue.next();
      try {
        await processMessage(message);
      } catch (error) {
        console.error("Message processing failed:", error);
      }
    }
  }

  for (let i = 0; i < limit; i++) {
    void startWorker();
  }

  return {
    push(message) {
      queue.push(message);
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

  processor.push("A");
  processor.push("B");
  processor.push("C");
  setTimeout(() => processor.push("D"), 500);
}
