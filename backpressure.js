import { pathToFileURL } from "node:url";

export class QueueClosedError extends Error {
  constructor() {
    super("Queue is closed");
    this.name = "QueueClosedError";
  }
}

export function createAsyncQueue(capacity, maxPendingProducers = capacity) {
  if (!Number.isInteger(capacity) || capacity < 1) {
    throw new RangeError("Queue capacity must be a positive integer");
  }
  if (!Number.isInteger(maxPendingProducers) || maxPendingProducers < 0) {
    throw new RangeError(
      "Pending producer limit must be a non-negative integer",
    );
  }

  const messages = [];
  const waitingConsumers = [];
  const waitingProducers = [];
  let closed = false;

  return {
    push(item) {
      if (closed) return Promise.reject(new QueueClosedError());

      if (waitingConsumers.length > 0) {
        waitingConsumers.shift().resolve(item);
        return Promise.resolve();
      }

      if (messages.length < capacity) {
        messages.push(item);
        return Promise.resolve();
      }

      if (waitingProducers.length >= maxPendingProducers) {
        return Promise.reject(
          new Error("Queue is full; too many waiting producers"),
        );
      }

      return new Promise((resolve, reject) => {
        waitingProducers.push({ item, resolve, reject });
      });
    },

    next() {
      if (messages.length > 0) {
        const item = messages.shift();

        if (waitingProducers.length > 0) {
          const producer = waitingProducers.shift();
          messages.push(producer.item);
          producer.resolve();
        }

        return Promise.resolve(item);
      }

      if (closed) return Promise.reject(new QueueClosedError());

      return new Promise((resolve, reject) => {
        waitingConsumers.push({ resolve, reject });
      });
    },

    close() {
      if (closed) return;
      closed = true;

      // Pending producers have not been admitted; only accepted messages drain.
      for (const waiter of [
        ...waitingConsumers.splice(0),
        ...waitingProducers.splice(0),
      ]) {
        waiter.reject(new QueueClosedError());
      }
    },
  };
}

export function createMessageProcessor(
  limit,
  capacity,
  processMessage,
  maxPendingProducers = capacity,
) {
  if (!Number.isInteger(limit) || limit < 1) {
    throw new RangeError("Concurrency limit must be a positive integer");
  }
  if (typeof processMessage !== "function") {
    throw new TypeError("Message handler must be a function");
  }

  const queue = createAsyncQueue(capacity, maxPendingProducers);

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
      return queue.push(message);
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
  const processor = createMessageProcessor(2, 3, async (message) => {
    console.log("Started:", message);
    await new Promise((resolve) => setTimeout(resolve, 200));
    console.log("Completed:", message);
  });

  for (const message of ["A", "B", "C", "D", "E", "F", "G", "H"]) {
    console.log("Submitting:", message);
    await processor.push(message);
    console.log("Accepted:", message);
  }

  await processor.shutdown();
  console.log("All accepted messages finished");
}
